package client_test

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/mababaNiubi/qv-lite/server"
	"github.com/mababaNiubi/qv-lite/server/client"
)

func newTestClient(t *testing.T) *client.Client {
	t.Helper()
	cfg := &server.Config{}
	cfg.ApplyDefaults()
	cfg.DB.Path = filepath.Join(t.TempDir(), "data")
	srv, err := server.New(cfg, nil)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		ts.Close()
		_ = srv.Shutdown(context.Background())
	})
	c, err := client.New(ts.URL)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	return c
}

func TestClientFullRoundTrip(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	if err := c.Health(ctx); err != nil {
		t.Fatalf("health: %v", err)
	}

	if err := c.CreateTable(ctx, "metrics", nil, 4); err != nil {
		t.Fatalf("create table: %v", err)
	}
	tables, err := c.ListTables(ctx)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	if len(tables) != 1 || tables[0].Name != "metrics" {
		t.Fatalf("tables = %+v", tables)
	}

	base := time.Now().UnixMilli() - 60_000

	// single write with exact int64
	ok, err := c.Write(ctx, "metrics", "counter", base, client.Int(9007199254740993))
	if err != nil || !ok {
		t.Fatalf("write: ok=%v err=%v", ok, err)
	}

	// batch write
	pts := make([]client.TagPoint, 5)
	for i := range pts {
		pts[i] = client.TagPoint{
			Tag:       "cpu",
			Timestamp: base + int64(i)*1000,
			Value:     client.Float(float64(36 + i)),
		}
	}
	n, err := c.WriteBatch(ctx, "metrics", pts)
	if err != nil || n != 5 {
		t.Fatalf("batch: n=%d err=%v", n, err)
	}

	// query raw
	got, err := c.Query(ctx, "metrics", "cpu", base-1, base+10_000, 0, 0, nil)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("query points = %d, want 5", len(got))
	}

	// int64 exactness over the wire
	gotInt, err := c.Query(ctx, "metrics", "counter", base-1, base+1, 0, 0, nil)
	if err != nil {
		t.Fatalf("query int: %v", err)
	}
	if len(gotInt) != 1 {
		t.Fatalf("int points = %d", len(gotInt))
	}
	if gotInt[0].VType != "int" {
		t.Fatalf("int vtype = %q", gotInt[0].VType)
	}
	if v, ok := gotInt[0].AsInt64(); !ok || v != 9007199254740993 {
		t.Fatalf("int roundtrip = %d (ok=%v), want 9007199254740993", v, ok)
	}

	// latest
	latest, err := c.QueryLatest(ctx, "metrics", "cpu")
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	if latest.Timestamp != base+4000 {
		t.Fatalf("latest ts = %d, want %d", latest.Timestamp, base+4000)
	}

	// aggregation
	agg, err := c.Query(ctx, "metrics", "cpu", base-1, base+10_000, 3000, 0, nil)
	if err != nil {
		t.Fatalf("agg query: %v", err)
	}
	if len(agg) == 0 {
		t.Fatal("agg returned no windows")
	}
}

// TestClientWriteLine 验证 Line Protocol 写入通道（客户端 → 服务端）。
func TestClientWriteLine(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	if err := c.CreateTable(ctx, "sensor", nil, 4); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := c.CreateTable(ctx, "other", nil, 4); err != nil {
		t.Fatalf("create table other: %v", err)
	}

	lines := "sensor,tag=cpu value=36.5 1700000000000\n" +
		"sensor,tag=cpu value=42i 1700000001000\n" +
		`other,tag=meter a=1.5,b=2i 1700000002000` + "\n"
	n, err := c.WriteLine(ctx, lines)
	if err != nil {
		t.Fatalf("WriteLine: %v", err)
	}
	if n != 3 {
		t.Fatalf("written = %d, want 3", n)
	}

	got, err := c.Query(ctx, "sensor", "cpu", 1700000000000, 1700000002000, 0, 0, nil)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("points = %d, want 2", len(got))
	}
	if f, ok := got[0].AsFloat64(); !ok || f != 36.5 {
		t.Fatalf("float = %v (ok=%v), want 36.5", f, ok)
	}
	if i, ok := got[1].AsInt64(); !ok || i != 42 {
		t.Fatalf("int = %v (ok=%v), want 42", i, ok)
	}
}

func TestClientAuth(t *testing.T) {
	cfg := &server.Config{Token: "sekrit"}
	cfg.ApplyDefaults()
	cfg.DB.Path = filepath.Join(t.TempDir(), "data")
	srv, err := server.New(cfg, nil)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		ts.Close()
		_ = srv.Shutdown(context.Background())
	})

	// without token -> error
	c, _ := client.New(ts.URL)
	if err := c.Health(context.Background()); err == nil {
		t.Fatal("health without token should fail")
	}

	// with token -> ok
	c2, err := client.New(ts.URL, client.WithToken("sekrit"))
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	if err := c2.Health(context.Background()); err != nil {
		t.Fatalf("health with token: %v", err)
	}
}
