package server

import (
	"bufio"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/mababaNiubi/qv-lite/tsdb"
)

type queryRequest struct {
	Table          string          `json:"table"`
	Tag            string          `json:"tag"`
	StartTime      int64           `json:"start"`
	EndTime        int64           `json:"end"`
	WindowSize     int64           `json:"window,omitempty"`      // <=0 返回原始点
	Polymerization uint8           `json:"aggregation,omitempty"` // 0 avg,1 min,2 max
	Condition      json.RawMessage `json:"condition,omitempty"`
	Limit          int             `json:"limit,omitempty"`  // >0 时最多返回 limit 个点
	Offset         int64           `json:"offset,omitempty"` // 跳过前 offset 个点（仅与 limit 配合有意义）
}

type pointResult struct {
	Timestamp int64           `json:"timestamp"`
	Value     json.RawMessage `json:"value"`           // 原生 JSON 值
	VType     string          `json:"vtype,omitempty"` // 仅 int/uint 时输出（超 2^53 时 value 为字符串）
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	var req queryRequest
	if err := decodeBody(r, &req); err != nil {
		badRequest(w, err)
		return
	}
	cond, err := decodeCondition(req.Condition)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if !s.flushOr500(w) {
		return
	}
	if req.WindowSize <= 0 {
		s.streamRawQuery(w, r, &req, cond)
		return
	}
	s.streamWindowQuery(w, r, &req, cond)
}

// streamRawQuery streams raw points straight from the engine iterator to the
// HTTP response, so a large result never materializes as []Point or
// []pointResult in memory. The response body keeps the historical shape
// {"points":[...],"count":N}; N is written last once the total is known.
func (s *Server) streamRawQuery(w http.ResponseWriter, r *http.Request, req *queryRequest, cond any) {
	it, err := s.db.QueryIter(r.Context(), req.Table, req.Tag, req.StartTime, req.EndTime, cond,
		&tsdb.QueryOptions{Limit: req.Limit, Offset: req.Offset})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if it == nil { // table does not exist: historical behavior returns an empty result
		writeJSON(w, http.StatusOK, map[string]any{"points": []any{}, "count": 0})
		return
	}
	defer it.Close()
	s.writePointStream(w, it)
}

// streamWindowQuery runs the window aggregation (bounded output: at most
// range/windowSize points) and streams the result with the same shape.
func (s *Server) streamWindowQuery(w http.ResponseWriter, r *http.Request, req *queryRequest, cond any) {
	raw, err := s.db.Query(req.Table, req.Tag, req.StartTime, req.EndTime, req.WindowSize, req.Polymerization, cond)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if req.Offset > 0 {
		if int64(len(raw)) <= req.Offset {
			raw = nil
		} else {
			raw = raw[req.Offset:]
		}
	}
	if req.Limit > 0 && len(raw) > req.Limit {
		raw = raw[:req.Limit]
	}
	s.writePointStream(w, &slicePointIter{points: raw})
}

// slicePointIter adapts a materialized slice to the streaming PointIter
// interface (used for window-aggregated results, which are already bounded).
type slicePointIter struct {
	points []tsdb.Point
	i      int
}

func (s *slicePointIter) Next() (tsdb.Point, bool, error) {
	if s.i >= len(s.points) {
		return tsdb.Point{}, false, nil
	}
	p := s.points[s.i]
	s.i++
	return p, true, nil
}

func (s *slicePointIter) Close() error { return nil }

// writePointStream writes {"points":[...],"count":N} incrementally, encoding
// each point on the fly and flushing in batches. Mid-stream engine errors can
// no longer change the HTTP status; the stream is simply terminated (the
// client observes a truncated body).
func (s *Server) writePointStream(w http.ResponseWriter, it tsdb.PointIter) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	bw := bufio.NewWriterSize(w, 32*1024)
	write := func(b []byte) error { _, err := bw.Write(b); return err }

	write([]byte(`{"points":[`))
	count := 0
	scratch := make([]byte, 0, 256)
	var first = true
	for {
		p, ok, err := it.Next()
		if err != nil || !ok {
			break
		}
		val, vtype, e := VariantToRawJSON(p.V)
		if e != nil {
			break
		}
		if !first {
			if err := write([]byte(",")); err != nil {
				return
			}
		}
		first = false
		scratch = scratch[:0]
		scratch = append(scratch, `{"timestamp":`...)
		scratch = strconv.AppendInt(scratch, p.Tms, 10)
		scratch = append(scratch, `,"value":`...)
		scratch = append(scratch, val...)
		if vtype != "" {
			scratch = append(scratch, `,"vtype":"`...)
			scratch = append(scratch, vtype...)
			scratch = append(scratch, '"')
		}
		scratch = append(scratch, '}')
		if err := write(scratch); err != nil {
			return
		}
		count++
		if count%1024 == 0 && flusher != nil {
			if err := bw.Flush(); err != nil {
				return
			}
			flusher.Flush()
		}
	}
	scratch = scratch[:0]
	scratch = append(scratch, `],"count":`...)
	scratch = strconv.AppendInt(scratch, int64(count), 10)
	scratch = append(scratch, '}')
	_ = write(scratch)
	_ = bw.Flush()
}

type latestRequest struct {
	Table string `json:"table"`
	Tag   string `json:"tag"`
}

func (s *Server) handleQueryLatest(w http.ResponseWriter, r *http.Request) {
	var req latestRequest
	if err := decodeBody(r, &req); err != nil {
		badRequest(w, err)
		return
	}
	if !s.flushOr500(w) {
		return
	}
	p, err := s.db.QueryLatest(req.Table, req.Tag)
	if err != nil {
		writeError(w, err)
		return
	}
	if p == nil {
		writeErr(w, http.StatusNotFound, tsdb.ErrorTagNotFound)
		return
	}
	val, vtype, err := VariantToRawJSON(p.V)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, pointResult{Timestamp: p.Tms, Value: val, VType: vtype})
}
