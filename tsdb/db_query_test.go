package tsdb

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/mababaNiubi/variant"
)

func TestDB_QueryWithCondition(t *testing.T) {
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
			Name: "cond_table",
			Type: ColumnTypeStructure,
			Structure: []ColumnAttribute{
				{Name: "score", Type: ColumnTypeInt},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now().UnixNano()
	for i := 0; i < 100; i++ {
		v := variant.New(map[string]variant.Variant{
			"score": variant.NewInt(i),
		})
		if _, err := db.Write("cond_table", "student", baseTime+int64(i), v); err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
	}

	cond := LogicalCondition{
		Op: LogicalAnd,
		Cond: []any{
			Condition{
				ColumnAttributeName: "score",
				Operator:            OpGreaterThan,
				Value:               variant.NewInt(50),
			},
		},
	}
	points, err := db.QueryAll("cond_table", "student", baseTime-100, baseTime+200, cond)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 49 {
		t.Errorf("condition score>50: expected 49 points, got %d", len(points))
	}
}

func TestDB_Query_WithUnsortedWAL(t *testing.T) {
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
		ColumnAttribute: ColumnAttribute{Name: "unsorted_table", Type: ColumnTypeInt},
	}); err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now().UnixNano()
	for _, offset := range []int64{100, 50, 75, 200, 150} {
		if _, err := db.Write("unsorted_table", "tag1", baseTime+offset, variant.NewInt(int(offset))); err != nil {
			t.Fatalf("Write offset %d failed: %v", offset, err)
		}
	}

	points, err := db.QueryAll("unsorted_table", "tag1", baseTime, baseTime+300, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 5 {
		t.Fatalf("expected 5 points, got %d", len(points))
	}
	for i := 1; i < len(points); i++ {
		if points[i].Tms < points[i-1].Tms {
			t.Errorf("points not sorted: points[%d].Tms=%d > points[%d].Tms=%d",
				i-1, points[i-1].Tms, i, points[i].Tms)
		}
	}
	expected := []int64{50, 75, 100, 150, 200}
	for i := range expected {
		if v, _ := points[i].V.AsInt64(); v != expected[i] {
			t.Errorf("points[%d].V=%d, want %d", i, v, expected[i])
		}
	}
}

func TestDB_ColumnQuery_WhyNot10000(t *testing.T) {
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
		ColumnAttribute: ColumnAttribute{Name: "test_col", FloatPrecision: 2},
	}); err != nil {
		t.Fatal(err)
	}

	const totalPoints = 20000
	baseTime := time.Now().UnixNano()
	for i := 0; i < totalPoints; i++ {
		mp := map[string]any{
			"value": float64(i),
			"name":  "sensor_" + strconv.Itoa(i%100),
		}
		if _, err := db.Write("test_col", "tag1", baseTime+int64(i)*int64(time.Second), variant.New(mp)); err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
	}

	points, err := db.QueryAll("test_col", "tag1",
		baseTime, baseTime+int64(totalPoints)*int64(time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != totalPoints {
		t.Fatalf("QueryAll: expected %d points, got %d", totalPoints, len(points))
	}
}

func TestDB_QueryLimitNumber_WithUnsortedWAL(t *testing.T) {
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
		ColumnAttribute: ColumnAttribute{Name: "limit_unsorted", Type: ColumnTypeInt},
	}); err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now().UnixNano()
	for i := 0; i < 10; i++ {
		if _, err := db.Write("limit_unsorted", "tag1", baseTime+int64(i)*int64(time.Second), variant.NewInt(i)); err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
	}
	extraBase := baseTime + 5*int64(time.Second)
	for _, offset := range []int64{300, 100, 400, 200, 500} {
		if _, err := db.Write("limit_unsorted", "tag1", extraBase+offset*int64(time.Millisecond), variant.NewInt(int(offset))); err != nil {
			t.Fatalf("Write extra offset %d failed: %v", offset, err)
		}
	}

	points, err := db.Query("limit_unsorted", "tag1", baseTime, baseTime+2*int64(time.Hour), 5*int64(time.Second), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) == 0 {
		t.Fatal("expected at least 1 point, got 0")
	}
	for i := 1; i < len(points); i++ {
		if points[i].Tms < points[i-1].Tms {
			t.Errorf("points not sorted: points[%d].Tms=%d > points[%d].Tms=%d",
				i-1, points[i-1].Tms, i, points[i].Tms)
		}
	}
}
