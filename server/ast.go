package server

import (
	"github.com/mababaNiubi/qv-lite/tsdb"

	"github.com/mababaNiubi/variant"
)

type Stmt interface {
	stmtNode()
}

type CreateTableStmt struct {
	Name      string
	Type      tsdb.ColumnType
	Precision uint8
}

func (*CreateTableStmt) stmtNode() {}

type InsertRow struct {
	Tag       string
	Timestamp int64
	Value     variant.Variant
}

type InsertStmt struct {
	Table string
	Rows  []InsertRow
}

func (*InsertStmt) stmtNode() {}

type SelectStmt struct {
	Table          string
	Tag            string
	StartTime      int64
	EndTime        int64
	Limit          int64
	Polymerization string // "avg", "min", "max", "" for none
	Having         any    // *tsdb.Condition or *tsdb.LogicalCondition or nil
}

func (*SelectStmt) stmtNode() {}

type SelectLatestStmt struct {
	Table string
	Tag   string
}

func (*SelectLatestStmt) stmtNode() {}
