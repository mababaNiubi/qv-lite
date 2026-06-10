package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"qvdb/tsdb"
	"testing"
	"time"
)

func startTestServer(t *testing.T) (string, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "tsdb_test_*")
	if err != nil {
		t.Fatal(err)
	}

	db, err := tsdb.Open(tsdb.Config{
		Path:           dir,
		WalConfig:      tsdb.WalConfig{MaxCacheSize: 64 * 1024 * 1024},
		MaxStorageTime: 24 * 60 * 60 * 365,
	}, context.Background())
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}

	srv, err := NewServer(db, ":0") // random port
	if err != nil {
		db.Close()
		os.RemoveAll(dir)
		t.Fatal(err)
	}

	go srv.Start()
	addr := srv.Addr().String()

	cleanup := func() {
		srv.Close()
		db.Close()
		os.RemoveAll(dir)
	}
	return addr, cleanup
}

type sqlClient struct {
	conn   net.Conn
	reader *bufio.Reader
}

func newSQLClient(addr string) (*sqlClient, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	return &sqlClient{conn: conn, reader: bufio.NewReader(conn)}, nil
}

func (c *sqlClient) Exec(sql string) (*Result, error) {
	_, err := fmt.Fprintln(c.conn, sql)
	if err != nil {
		return nil, err
	}
	line, err := c.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	var r Result
	if err := json.Unmarshal([]byte(line), &r); err != nil {
		return nil, fmt.Errorf("json decode: %w (raw: %q)", err, line)
	}
	return &r, nil
}

func (c *sqlClient) Close() error {
	return c.conn.Close()
}

func TestServerIntegration(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	client, err := newSQLClient(addr)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	// 1. Create table
	r, err := client.Exec("CREATE TABLE test TYPE float PRECISION 2")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	if !r.Success {
		t.Fatalf("create table failed: %s", r.Message)
	}

	// 2. Insert data points
	for i, v := range []float64{10.5, 25.3, 50.7, 100.0, 200.1} {
		sql := fmt.Sprintf("INSERT INTO test (tag, time, value) VALUES ('cpu', %d, %v)", (i+1)*1000, v)
		r, err := client.Exec(sql)
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
		if !r.Success {
			t.Fatalf("insert %d failed: %s", i, r.Message)
		}
	}

	// 3. Select all
	r, err = client.Exec("SELECT * FROM test WHERE tag = 'cpu' AND time >= 0 AND time <= 9999999")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if !r.Success {
		t.Fatalf("select failed: %s", r.Message)
	}
	if r.Count != 5 {
		t.Errorf("expected 5 points, got %d", r.Count)
	}

	// 4. Select with HAVING filter
	r, err = client.Exec("SELECT * FROM test WHERE tag = 'cpu' AND time >= 0 AND time <= 9999999 HAVING value > 30")
	if err != nil {
		t.Fatalf("select with having: %v", err)
	}
	if r.Count != 3 {
		t.Errorf("HAVING value > 30: expected 3 points, got %d", r.Count)
	}

	// 5. Select with time range (partial)
	r, err = client.Exec("SELECT * FROM test WHERE tag = 'cpu' AND time >= 2000 AND time <= 4000")
	if err != nil {
		t.Fatalf("select time range: %v", err)
	}
	if r.Count == 0 {
		t.Error("time range query returned 0 points, expected some")
	}
}

func TestServerCreateTableDuplicate(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	client, err := newSQLClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	r, _ := client.Exec("CREATE TABLE dup TYPE float")
	if !r.Success {
		t.Fatalf("first create failed: %s", r.Message)
	}
	r, _ = client.Exec("CREATE TABLE dup TYPE int")
	if r.Success {
		t.Error("duplicate table should fail")
	}
}

func TestServerInsertNonExistentTable(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	client, err := newSQLClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	r, _ := client.Exec("INSERT INTO no_such_table (tag, time, value) VALUES ('x', 1, 2.0)")
	if r.Success {
		t.Error("insert into non-existent table should fail")
	}
}

func TestServerAllTypes(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	client, err := newSQLClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Create and test each type
	type testCase struct {
		createSQL string
		insertSQL string
		selectSQL string
	}

	tests := []testCase{
		{
			createSQL: "CREATE TABLE ft TYPE float PRECISION 3",
			insertSQL: "INSERT INTO ft (tag, time, value) VALUES ('t', 1000, 3.14159)",
			selectSQL: "SELECT * FROM ft WHERE tag = 't' AND time >= 0 AND time <= 9999999",
		},
		{
			createSQL: "CREATE TABLE it TYPE int",
			insertSQL: "INSERT INTO it (tag, time, value) VALUES ('t', 1000, 42)",
			selectSQL: "SELECT * FROM it WHERE tag = 't' AND time >= 0 AND time <= 9999999",
		},
		{
			createSQL: "CREATE TABLE st TYPE string",
			insertSQL: "INSERT INTO st (tag, time, value) VALUES ('t', 1000, 'hello')",
			selectSQL: "SELECT * FROM st WHERE tag = 't' AND time >= 0 AND time <= 9999999",
		},
		{
			createSQL: "CREATE TABLE bt TYPE bool",
			insertSQL: "INSERT INTO bt (tag, time, value) VALUES ('t', 1000, true)",
			selectSQL: "SELECT * FROM bt WHERE tag = 't' AND time >= 0 AND time <= 9999999",
		},
	}

	for _, tc := range tests {
		r, err := client.Exec(tc.createSQL)
		if err != nil || !r.Success {
			t.Errorf("create %s: err=%v success=%v msg=%s", tc.createSQL, err, r.Success, r.Message)
			continue
		}
		r, err = client.Exec(tc.insertSQL)
		if err != nil || !r.Success {
			t.Errorf("insert %s: err=%v success=%v msg=%s", tc.insertSQL, err, r.Success, r.Message)
			continue
		}
		r, err = client.Exec(tc.selectSQL)
		if err != nil || !r.Success || r.Count != 1 {
			t.Errorf("select: err=%v success=%v count=%d (want 1)", err, r.Success, r.Count)
		}
	}
}

func TestServerPolymerization(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	client, err := newSQLClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	client.Exec("CREATE TABLE poly TYPE float")
	for i := 0; i < 10; i++ {
		client.Exec(fmt.Sprintf("INSERT INTO poly (tag, time, value) VALUES ('cpu', %d, %v)", i*1000, float64(i)*10.0))
	}

	// Test each polymerization type
	for _, poly := range []string{"avg", "min", "max"} {
		sql := fmt.Sprintf("SELECT * FROM poly WHERE tag = 'cpu' AND time >= 0 AND time <= 9999999 POLYMERIZATION %s", poly)
		r, err := client.Exec(sql)
		if err != nil || !r.Success {
			t.Errorf("%s polymerization failed: err=%v", poly, err)
		}
	}
}

func TestServerLimit(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	client, err := newSQLClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	client.Exec("CREATE TABLE lim TYPE float")
	for i := 0; i < 20; i++ {
		client.Exec(fmt.Sprintf("INSERT INTO lim (tag, time, value) VALUES ('cpu', %d, %v)", i*1000, float64(i)))
	}

	// LIMIT in this system controls time-interval sampling granularity,
	// not a simple row count limit. Just verify it doesn't error.
	r, err := client.Exec("SELECT * FROM lim WHERE tag = 'cpu' AND time >= 0 AND time <= 9999999 LIMIT 5")
	if err != nil {
		t.Fatal(err)
	}
	if !r.Success {
		t.Errorf("LIMIT query failed: %s", r.Message)
	}
}

func TestServerHavingString(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	client, err := newSQLClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	client.Exec("CREATE TABLE status TYPE string")
	client.Exec("INSERT INTO status (tag, time, value) VALUES ('dev1', 1000, 'active')")
	client.Exec("INSERT INTO status (tag, time, value) VALUES ('dev1', 2000, 'idle')")

	r, err := client.Exec("SELECT * FROM status WHERE tag = 'dev1' AND time >= 0 AND time <= 9999999 HAVING value = 'active'")
	if err != nil {
		t.Fatal(err)
	}
	if r.Count != 1 {
		t.Errorf("HAVING value = 'active': expected 1, got %d", r.Count)
	}
}

func TestServerParseErrors(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	client, err := newSQLClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	badSQLs := []string{
		"INVALID",
		"CREATE",
		"INSERT INTO x VALUES (1)",
		"SELECT FROM x",
		"CREATE TABLE x TYPE unknown",
	}

	for _, sql := range badSQLs {
		r, err := client.Exec(sql)
		if err != nil {
			t.Errorf("unexpected connection error for %q: %v", sql, err)
			continue
		}
		if r.Success {
			t.Errorf("expected failure for %q, got success", sql)
		}
	}
}

func TestServerBatchInsert(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	client, err := newSQLClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	r, err := client.Exec("CREATE TABLE batch TYPE float")
	if err != nil || !r.Success {
		t.Fatalf("create table failed: err=%v msg=%s", err, r.Message)
	}

	r, err = client.Exec("INSERT INTO batch (tag, time, value) VALUES ('d1', 1000, 10.0), ('d2', 2000, 20.0), ('d3', 3000, 30.0)")
	if err != nil || !r.Success {
		t.Fatalf("batch insert failed: err=%v msg=%s", err, r.Message)
	}

	for _, tag := range []string{"d1", "d2", "d3"} {
		r, err = client.Exec(fmt.Sprintf("SELECT * FROM batch WHERE tag = '%s' AND time >= 0 AND time <= 9999999", tag))
		if err != nil || !r.Success {
			t.Errorf("select for %s failed: err=%v", tag, err)
			continue
		}
		if r.Count != 1 {
			t.Errorf("expected 1 point for %s, got count=%d", tag, r.Count)
		}
	}
}
