package server

import (
	"qvdb/tsdb"
	"testing"

	"github.com/mababaNiubi/variant"
)

func TestParseCreateTable(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		want    *CreateTableStmt
		wantErr bool
	}{
		{
			name: "float with precision",
			sql:  "CREATE TABLE metrics TYPE float PRECISION 3",
			want: &CreateTableStmt{Name: "metrics", Type: tsdb.ColumnTypeFloat, Precision: 3},
		},
		{
			name: "float without precision",
			sql:  "CREATE TABLE metrics TYPE float",
			want: &CreateTableStmt{Name: "metrics", Type: tsdb.ColumnTypeFloat, Precision: 0},
		},
		{
			name: "int type",
			sql:  "CREATE TABLE counts TYPE int",
			want: &CreateTableStmt{Name: "counts", Type: tsdb.ColumnTypeInt},
		},
		{
			name: "integer type",
			sql:  "CREATE TABLE counts TYPE integer",
			want: &CreateTableStmt{Name: "counts", Type: tsdb.ColumnTypeInt},
		},
		{
			name: "string type",
			sql:  "CREATE TABLE logs TYPE string",
			want: &CreateTableStmt{Name: "logs", Type: tsdb.ColumnTypeString},
		},
		{
			name: "bool type",
			sql:  "CREATE TABLE flags TYPE bool",
			want: &CreateTableStmt{Name: "flags", Type: tsdb.ColumnTypeBool},
		},
		{
			name: "json type",
			sql:  "CREATE TABLE events TYPE json",
			want: &CreateTableStmt{Name: "events", Type: tsdb.ColumnTypeJson},
		},
		{
			name:    "unknown type",
			sql:     "CREATE TABLE bad TYPE unknown",
			wantErr: true,
		},
		{
			name:    "missing table name",
			sql:     "CREATE TABLE TYPE float",
			wantErr: true,
		},
		{
			name:    "garbage input",
			sql:     "NOT A SQL STATEMENT",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			got, ok := stmt.(*CreateTableStmt)
			if !ok {
				t.Fatalf("expected *CreateTableStmt, got %T", stmt)
			}
			if got.Name != tt.want.Name {
				t.Errorf("Name: got %q, want %q", got.Name, tt.want.Name)
			}
			if got.Type != tt.want.Type {
				t.Errorf("Type: got %v, want %v", got.Type, tt.want.Type)
			}
			if got.Precision != tt.want.Precision {
				t.Errorf("Precision: got %v, want %v", got.Precision, tt.want.Precision)
			}
		})
	}
}

func TestParseInsert(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		want    *InsertStmt
		wantErr bool
	}{
		{
			name: "insert float",
			sql:  "INSERT INTO metrics (tag, time, value) VALUES ('cpu', 1000, 42.5)",
			want: &InsertStmt{
				Table: "metrics",
				Rows:  []InsertRow{{Tag: "cpu", Timestamp: 1000, Value: variant.NewFloat64(42.5)}},
			},
		},
		{
			name: "insert integer",
			sql:  "INSERT INTO counts (tag, time, value) VALUES ('sensor', 2000, 100)",
			want: &InsertStmt{
				Table: "counts",
				Rows:  []InsertRow{{Tag: "sensor", Timestamp: 2000, Value: variant.NewInt64(100)}},
			},
		},
		{
			name: "insert string",
			sql:  "INSERT INTO logs (tag, time, value) VALUES ('dev1', 3000, 'hello world')",
			want: &InsertStmt{
				Table: "logs",
				Rows:  []InsertRow{{Tag: "dev1", Timestamp: 3000, Value: variant.NewString("hello world")}},
			},
		},
		{
			name: "insert true",
			sql:  "INSERT INTO flags (tag, time, value) VALUES ('sw1', 4000, true)",
			want: &InsertStmt{
				Table: "flags",
				Rows:  []InsertRow{{Tag: "sw1", Timestamp: 4000, Value: variant.NewBool(true)}},
			},
		},
		{
			name: "insert false",
			sql:  "INSERT INTO flags (tag, time, value) VALUES ('sw2', 5000, false)",
			want: &InsertStmt{
				Table: "flags",
				Rows:  []InsertRow{{Tag: "sw2", Timestamp: 5000, Value: variant.NewBool(false)}},
			},
		},
		{
			name: "negative timestamp",
			sql:  "INSERT INTO test (tag, time, value) VALUES ('t1', -1000, 1.0)",
			want: &InsertStmt{
				Table: "test",
				Rows:  []InsertRow{{Tag: "t1", Timestamp: -1000, Value: variant.NewFloat64(1.0)}},
			},
		},
		{
			name: "batch insert multiple rows",
			sql:  "INSERT INTO t (tag, time, value) VALUES ('a', 1000, 1.0), ('b', 2000, 2.0), ('c', 3000, 3.0)",
			want: &InsertStmt{
				Table: "t",
				Rows: []InsertRow{
					{Tag: "a", Timestamp: 1000, Value: variant.NewFloat64(1.0)},
					{Tag: "b", Timestamp: 2000, Value: variant.NewFloat64(2.0)},
					{Tag: "c", Timestamp: 3000, Value: variant.NewFloat64(3.0)},
				},
			},
		},
		{
			name:    "unquoted tag",
			sql:     "INSERT INTO test (tag, time, value) VALUES (tag1, 1000, 1.0)",
			wantErr: true,
		},
		{
			name:    "missing values",
			sql:     "INSERT INTO test (tag, time, value)",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			got, ok := stmt.(*InsertStmt)
			if !ok {
				t.Fatalf("expected *InsertStmt, got %T", stmt)
			}
			if got.Table != tt.want.Table {
				t.Errorf("Table: got %q, want %q", got.Table, tt.want.Table)
			}
			if len(got.Rows) != len(tt.want.Rows) {
				t.Fatalf("Rows count: got %d, want %d", len(got.Rows), len(tt.want.Rows))
			}
			for i := range got.Rows {
				if got.Rows[i].Tag != tt.want.Rows[i].Tag {
					t.Errorf("Rows[%d].Tag: got %q, want %q", i, got.Rows[i].Tag, tt.want.Rows[i].Tag)
				}
				if got.Rows[i].Timestamp != tt.want.Rows[i].Timestamp {
					t.Errorf("Rows[%d].Timestamp: got %v, want %v", i, got.Rows[i].Timestamp, tt.want.Rows[i].Timestamp)
				}
				if !got.Rows[i].Value.IsEqual(tt.want.Rows[i].Value) {
					t.Errorf("Rows[%d].Value: got %v, want %v", i, got.Rows[i].Value.AsInterface(), tt.want.Rows[i].Value.AsInterface())
				}
			}
		})
	}
}

func TestParseSelect(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		want    *SelectStmt
		wantErr bool
	}{
		{
			name: "basic select",
			sql:  "SELECT * FROM metrics WHERE tag = 'cpu' AND time >= 1000 AND time <= 5000",
			want: &SelectStmt{
				Table:     "metrics",
				Tag:       "cpu",
				StartTime: 1000,
				EndTime:   5000,
			},
		},
		{
			name: "select with limit",
			sql:  "SELECT * FROM metrics WHERE tag = 'cpu' AND time >= 0 AND time <= 9999999 LIMIT 100",
			want: &SelectStmt{
				Table:     "metrics",
				Tag:       "cpu",
				StartTime: 0,
				EndTime:   9999999,
				Limit:     100,
			},
		},
		{
			name: "select with polymerization avg",
			sql:  "SELECT * FROM metrics WHERE tag = 'cpu' AND time >= 0 AND time <= 9999999 POLYMERIZATION avg",
			want: &SelectStmt{
				Table:          "metrics",
				Tag:            "cpu",
				StartTime:      0,
				EndTime:        9999999,
				Polymerization: "avg",
			},
		},
		{
			name: "select with polymerization min",
			sql:  "SELECT * FROM metrics WHERE tag = 'cpu' AND time >= 0 AND time <= 9999999 POLYMERIZATION min",
			want: &SelectStmt{
				Table:          "metrics",
				Tag:            "cpu",
				StartTime:      0,
				EndTime:        9999999,
				Polymerization: "min",
			},
		},
		{
			name: "select with polymerization max",
			sql:  "SELECT * FROM metrics WHERE tag = 'cpu' AND time >= 0 AND time <= 9999999 POLYMERIZATION max",
			want: &SelectStmt{
				Table:          "metrics",
				Tag:            "cpu",
				StartTime:      0,
				EndTime:        9999999,
				Polymerization: "max",
			},
		},
		{
			name: "select with having condition",
			sql:  "SELECT * FROM metrics WHERE tag = 'cpu' AND time >= 0 AND time <= 9999999 HAVING value > 100",
			want: &SelectStmt{
				Table:     "metrics",
				Tag:       "cpu",
				StartTime: 0,
				EndTime:   9999999,
				Having: tsdb.Condition{
					ColumnAttributeName: "",
					Type:                tsdb.GreaterThanQueryCondition,
					Value:               variant.NewInt64(100),
				},
			},
		},
		{
			name: "select with having equals string",
			sql:  "SELECT * FROM logs WHERE tag = 'dev1' AND time >= 0 AND time <= 9999999 HAVING value = 'active'",
			want: &SelectStmt{
				Table:     "logs",
				Tag:       "dev1",
				StartTime: 0,
				EndTime:   9999999,
				Having: tsdb.Condition{
					ColumnAttributeName: "",
					Type:                tsdb.EqualQueryCondition,
					Value:               variant.NewString("active"),
				},
			},
		},
		{
			name: "select with limit and polymerizaton",
			sql:  "SELECT * FROM metrics WHERE tag = 'cpu' AND time >= 0 AND time <= 9999999 LIMIT 50 POLYMERIZATION avg",
			want: &SelectStmt{
				Table:          "metrics",
				Tag:            "cpu",
				StartTime:      0,
				EndTime:        9999999,
				Limit:          50,
				Polymerization: "avg",
			},
		},
		{
			name:    "empty",
			sql:     "",
			wantErr: true,
		},
		{
			name:    "unknown polymerization",
			sql:     "SELECT * FROM m WHERE tag = 'x' AND time >= 0 AND time <= 100 POLYMERIZATION median",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			got, ok := stmt.(*SelectStmt)
			if !ok {
				t.Fatalf("expected *SelectStmt, got %T", stmt)
			}
			if got.Table != tt.want.Table {
				t.Errorf("Table: got %q, want %q", got.Table, tt.want.Table)
			}
			if got.Tag != tt.want.Tag {
				t.Errorf("Tag: got %q, want %q", got.Tag, tt.want.Tag)
			}
			if got.StartTime != tt.want.StartTime {
				t.Errorf("StartTime: got %v, want %v", got.StartTime, tt.want.StartTime)
			}
			if got.EndTime != tt.want.EndTime {
				t.Errorf("EndTime: got %v, want %v", got.EndTime, tt.want.EndTime)
			}
			if got.Limit != tt.want.Limit {
				t.Errorf("Limit: got %v, want %v", got.Limit, tt.want.Limit)
			}
			if got.Polymerization != tt.want.Polymerization {
				t.Errorf("Polymerization: got %q, want %q", got.Polymerization, tt.want.Polymerization)
			}
		})
	}
}

func TestParseCaseInsensitive(t *testing.T) {
	sqls := []string{
		"create table t1 type float precision 2",
		"Create Table t2 Type Int",
		"CREATE TABLE t3 TYPE string",
		"insert into t1 (tag, time, value) values ('x', 1, 2.5)",
		"Insert Into t1 (tag, time, value) Values ('y', 2, 3)",
		"INSERT INTO t1 (tag, time, value) VALUES ('z', 3, true)",
		"select * from t1 where tag = 'cpu' and time >= 0 and time <= 100",
		"Select * From t1 Where tag = 'cpu' and time >= 0 and time <= 100",
		"SELECT * FROM t1 WHERE tag = 'cpu' AND time >= 0 AND time <= 100",
	}
	for _, sql := range sqls {
		_, err := Parse(sql)
		if err != nil {
			t.Errorf("case-insensitive parse failed for %q: %v", sql, err)
		}
	}
}

func TestLexer(t *testing.T) {
	l := newLexer("CREATE TABLE test TYPE float PRECISION 2")
	tokens := []token{}
	for {
		tok, err := l.nextToken()
		if err != nil {
			t.Fatal(err)
		}
		tokens = append(tokens, tok)
		if tok.kind == tokEOF {
			break
		}
	}
	expected := []struct {
		kind  tokenKind
		value string
	}{
		{tokKeyword, "create"},
		{tokKeyword, "table"},
		{tokIdent, "test"},
		{tokKeyword, "type"},
		{tokKeyword, "float"},
		{tokKeyword, "precision"},
		{tokNumber, "2"},
		{tokEOF, ""},
	}
	if len(tokens) != len(expected) {
		t.Fatalf("token count: got %d, want %d", len(tokens), len(expected))
	}
	for i, e := range expected {
		if tokens[i].kind != e.kind {
			t.Errorf("token[%d] kind: got %v, want %v", i, tokens[i].kind, e.kind)
		}
		if tokens[i].value != e.value {
			t.Errorf("token[%d] value: got %q, want %q", i, tokens[i].value, e.value)
		}
	}
}

func TestFormatPoints(t *testing.T) {
	points := []tsdb.Point{
		{Tms: 1000, V: variant.NewFloat64(42.5)},
		{Tms: 2000, V: variant.NewString("hello")},
		{Tms: 3000, V: variant.NewBool(true)},
	}
	result := FormatPoints(points)
	if len(result) != 3 {
		t.Fatalf("expected 3 points, got %d", len(result))
	}
	if result[0].Time != 1000 {
		t.Errorf("result[0].Time: got %v, want 1000", result[0].Time)
	}
	if result[0].Value != 42.5 {
		t.Errorf("result[0].Value: got %v, want 42.5", result[0].Value)
	}
	if result[1].Value != "hello" {
		t.Errorf("result[1].Value: got %v, want hello", result[1].Value)
	}
	if result[2].Value != true {
		t.Errorf("result[2].Value: got %v, want true", result[2].Value)
	}
}

func TestFormatResultJSON(t *testing.T) {
	r := &Result{Success: true, Message: "ok", Count: 3}
	json := FormatResultJSON(r)
	if len(json) == 0 {
		t.Error("empty json result")
	}
}
