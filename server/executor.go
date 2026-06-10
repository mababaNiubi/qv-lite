package server

import (
	"fmt"
	"qvdb/tsdb"
)

type Executor struct {
	db *tsdb.DB
}

func NewExecutor(db *tsdb.DB) *Executor {
	return &Executor{db: db}
}

type Result struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
	Count   int         `json:"count,omitempty"`
}

func (e *Executor) Execute(stmt Stmt) (*Result, error) {
	switch s := stmt.(type) {
	case *CreateTableStmt:
		return e.execCreateTable(s)
	case *InsertStmt:
		return e.execInsert(s)
	case *SelectStmt:
		return e.execSelect(s)
	case *SelectLatestStmt:
		return e.execSelectLatest(s)
	default:
		return nil, fmt.Errorf("unknown statement type")
	}
}

func (e *Executor) execCreateTable(stmt *CreateTableStmt) (*Result, error) {
	err := e.db.CreateTable(tsdb.TableInfo{
		ColumnAttribute: tsdb.ColumnAttribute{
			Name:           stmt.Name,
			Type:           stmt.Type,
			FloatPrecision: stmt.Precision,
		},
	})
	if err != nil {
		return &Result{Success: false, Message: err.Error()}, nil
	}
	return &Result{Success: true, Message: fmt.Sprintf("table %s created", stmt.Name)}, nil
}

func (e *Executor) execInsert(stmt *InsertStmt) (*Result, error) {
	inserted := 0
	for _, row := range stmt.Rows {
		_, err := e.db.Write(stmt.Table, row.Tag, row.Timestamp, row.Value)
		if err != nil {
			return &Result{Success: false, Message: err.Error()}, nil
		}
		inserted++
	}
	return &Result{Success: true, Message: fmt.Sprintf("inserted %d", inserted)}, nil
}

func (e *Executor) execSelect(stmt *SelectStmt) (*Result, error) {
	var polymerization uint8
	switch stmt.Polymerization {
	case "avg":
		polymerization = tsdb.AvgFusion
	case "min":
		polymerization = tsdb.MinFusion
	case "max":
		polymerization = tsdb.MaxFusion
	}

	points, err := e.db.Query(stmt.Table, stmt.Tag, stmt.StartTime, stmt.EndTime, stmt.Limit, polymerization, stmt.Having)
	if err != nil {
		return &Result{Success: false, Message: err.Error()}, nil
	}

	formatted := FormatPoints(points)
	return &Result{
		Success: true,
		Data:    formatted,
		Count:   len(points),
	}, nil
}

func (e *Executor) execSelectLatest(stmt *SelectLatestStmt) (*Result, error) {
	point, err := e.db.QueryLatest(stmt.Table, stmt.Tag)
	if err != nil {
		return &Result{Success: false, Message: err.Error()}, nil
	}
	formatted := FormatPoints([]tsdb.Point{*point})
	return &Result{
		Success: true,
		Data:    formatted,
		Count:   1,
	}, nil
}

func mapPolymerization(name string) uint8 {
	switch name {
	case "avg":
		return tsdb.AvgFusion
	case "min":
		return tsdb.MinFusion
	case "max":
		return tsdb.MaxFusion
	default:
		return tsdb.AvgFusion
	}
}

// Helper to expose underlying DB for the server
func (e *Executor) DB() *tsdb.DB {
	return e.db
}

func (e *Executor) Close() error {
	return e.db.Close()
}
