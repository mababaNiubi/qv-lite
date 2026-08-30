package server

import (
	"errors"
	"net/http"
	"time"

	"github.com/mababaNiubi/qv-lite/tsdb"
)

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"uptime":  time.Since(s.start).String(),
		"tables":  s.db.TableNames(),
		"version": "1",
	})
}

func (s *Server) handleListTables(w http.ResponseWriter, r *http.Request) {
	infos := s.db.TableInfos()
	type tv struct {
		Name           string                 `json:"name"`
		Desc           string                 `json:"desc"`
		Type           tsdb.ColumnType        `json:"type"`
		FloatPrecision uint8                  `json:"float_precision"`
		Structure      []tsdb.ColumnAttribute `json:"structure,omitempty"`
	}
	out := make([]tv, 0, len(infos))
	for _, info := range infos {
		out = append(out, tv{
			Name:           info.Name,
			Desc:           info.Desc,
			Type:           info.Type,
			FloatPrecision: info.FloatPrecision,
			Structure:      info.Structure,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"tables": out})
}

type createTableRequest struct {
	Name           string       `json:"name"`
	Desc           string       `json:"desc"`
	FloatPrecision uint8        `json:"float_precision"`
	Columns        []columnSpec `json:"columns,omitempty"`
}

type columnSpec struct {
	Name string `json:"name"`
	Desc string `json:"desc"`
	Type uint8  `json:"type"`
}

func (s *Server) handleCreateTable(w http.ResponseWriter, r *http.Request) {
	var req createTableRequest
	if err := decodeBody(r, &req); err != nil {
		badRequest(w, err)
		return
	}
	if req.Name == "" {
		writeErr(w, http.StatusBadRequest, errors.New("table name is required"))
		return
	}
	// 建表与写入同序：先排空流水线缓冲再改元数据。
	if !s.flushOr500(w) {
		return
	}
	attr := tsdb.ColumnAttribute{
		Name:           req.Name,
		Desc:           req.Desc,
		FloatPrecision: req.FloatPrecision,
	}
	if attr.FloatPrecision == 0 {
		attr.FloatPrecision = 4
	}
	for _, c := range req.Columns {
		attr.Structure = append(attr.Structure, tsdb.ColumnAttribute{
			Name: c.Name, Desc: c.Desc, Type: tsdb.ColumnType(c.Type),
		})
	}
	if err := s.db.CreateTable(tsdb.TableInfo{ColumnAttribute: attr}); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"created": true, "name": req.Name})
}
