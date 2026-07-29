package tsdb

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/mababaNiubi/variant"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "tsdb_test_*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})
	return dir
}

func TestDB_CreateTable(t *testing.T) {
	db, err := Open(Config{
		Path:           tempDir(t),
		WalConfig:      WalConfig{MaxFileSize: 64 * 1024 * 1024},
		MaxStorageTime: 24 * 60 * 60 * 365,
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{
			Name: "test_table",
			Type: ColumnTypeFloat,
		},
	})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	err = db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{
			Name: "test_table",
			Type: ColumnTypeFloat,
		},
	})
	if err != ErrorTableExists {
		t.Errorf("duplicate table: expected ErrorTableExists, got %v", err)
	}
}

func TestDB_WriteAndQuery(t *testing.T) {
	db, err := Open(Config{
		Path:           tempDir(t),
		WalConfig:      WalConfig{MaxFileSize: 64 * 1024 * 1024},
		MaxStorageTime: 24 * 60 * 60 * 365,
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{Name: "test", Type: ColumnTypeFloat},
	}); err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now().UnixNano()
	n := 24 * 60 * 60 * 100
	for i := 0; i < n; i++ {
		_, err = db.Write("test", "cpu", baseTime+int64(i), variant.NewFloat64(float64(i)*0.5))
		if err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
	}

	points, err := db.QueryAll("test", "cpu", baseTime-100, baseTime+int64(n)+100, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != n {
		t.Fatalf("expected %d points, got %d", n, len(points))
	}
	if points[0].Tms != baseTime {
		t.Errorf("first point Tms: got %d, want %d", points[0].Tms, baseTime)
	}
	if points[n-1].Tms != baseTime+int64(n-1) {
		t.Errorf("last point Tms: got %d, want %d", points[n-1].Tms, baseTime+int64(n-1))
	}
}

func TestDB_WriteToNonExistentTable(t *testing.T) {
	db, err := Open(Config{
		Path:           tempDir(t),
		WalConfig:      WalConfig{MaxFileSize: 64 * 1024 * 1024},
		MaxStorageTime: 24 * 60 * 60 * 365,
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Write("no_such_table", "tag", time.Now().UnixMilli(), variant.NewInt(1))
	if err != ErrorTableNotExists {
		t.Errorf("expected ErrorTableNotExists, got %v", err)
	}
}

func TestDB_QueryEmpty(t *testing.T) {
	db, err := Open(Config{
		Path:           tempDir(t),
		WalConfig:      WalConfig{MaxFileSize: 64 * 1024 * 1024},
		MaxStorageTime: 24 * 60 * 60 * 365,
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{Name: "empty_table", Type: ColumnTypeFloat},
	}); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UnixMilli()
	points, err := db.QueryAll("empty_table", "cpu", now-1000, now+1000, nil)
	if err != nil {
		t.Logf("QueryAll on empty table returned error: %v (acceptable)", err)
		return
	}
	if len(points) != 0 {
		t.Errorf("expected 0 points for empty table, got %d", len(points))
	}
}

func TestDB_MultiTagWrite(t *testing.T) {
	db, err := Open(Config{
		Path:           tempDir(t),
		WalConfig:      WalConfig{MaxFileSize: 64 * 1024 * 1024},
		MaxStorageTime: 24 * 60 * 60 * 365,
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{Name: "multi_tag", Type: ColumnTypeInt},
	}); err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now().UnixNano()
	tags := []string{"cpu0", "cpu1", "cpu2"}
	for i, tag := range tags {
		if _, err := db.Write("multi_tag", tag, baseTime, variant.NewInt(i*100)); err != nil {
			t.Fatalf("Write %s failed: %v", tag, err)
		}
	}

	for _, tag := range tags {
		points, err := db.QueryAll("multi_tag", tag, baseTime-100, baseTime+100, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(points) != 1 {
			t.Errorf("tag %s: expected 1 point, got %d", tag, len(points))
		}
	}
}

func TestDB_StructTableWrite(t *testing.T) {
	db, err := Open(Config{
		Path:           tempDir(t),
		WalConfig:      WalConfig{MaxFileSize: 64 * 1024 * 1024},
		MaxStorageTime: 24 * 60 * 60 * 365,
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{
			Name: "struct_table",
			Type: ColumnTypeStructure,
			Structure: []ColumnAttribute{
				{Name: "name", Type: ColumnTypeString},
				{Name: "value", Type: ColumnTypeFloat},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now().UnixNano()
	number := 24 * 60 * 60 * 100
	for i := 0; i < number; i++ {
		v := variant.New(map[string]variant.Variant{
			"name":  variant.NewString("test" + strconv.Itoa(i)),
			"value": variant.NewFloat64(float64(i) * 0.5),
		})
		_, err = db.Write("struct_table", "sensor", baseTime+int64(i)*int64(time.Millisecond), v)
		if err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
	}
	points, err := db.QueryAll("struct_table", "sensor", baseTime-100, baseTime+int64(number)*int64(time.Millisecond)+100, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != number {
		t.Fatalf("expected %d points, got %d", number, len(points))
	}
	if points[0].Tms != baseTime {
		t.Errorf("first point Tms: got %d, want %d", points[0].Tms, baseTime)
	}
	if points[number-1].Tms != baseTime+int64(number-1)*int64(time.Millisecond) {
		t.Errorf("last point Tms: got %d, want %d", points[number-1].Tms, baseTime+int64(number-1))
	}
}
