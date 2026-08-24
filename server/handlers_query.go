package server

import (
	"encoding/json"
	"net/http"

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
	raw, err := s.db.Query(req.Table, req.Tag, req.StartTime, req.EndTime, req.WindowSize, req.Polymerization, cond)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	out := make([]pointResult, 0, len(raw))
	for _, p := range raw {
		val, vtype, e := VariantToRawJSON(p.V)
		if e != nil {
			writeErr(w, http.StatusInternalServerError, e)
			return
		}
		out = append(out, pointResult{Timestamp: p.Tms, Value: val, VType: vtype})
	}
	writeJSON(w, http.StatusOK, map[string]any{"points": out, "count": len(out)})
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
