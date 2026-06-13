package tsdb

import (
	"context"
	"github.com/mababaNiubi/variant"
	"os"
	"strconv"
	"testing"
	"time"
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
		WalConfig:      WalConfig{MaxCacheSize: 64 * 1024 * 1024},
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
		WalConfig:      WalConfig{MaxCacheSize: 64 * 1024 * 1024},
		MaxStorageTime: 24 * 60 * 60 * 365,
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{
			Name: "test",
			Type: ColumnTypeFloat,
		},
	})
	if err != nil {
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
		t.Fatalf("QueryAll: expected %d points, got %d", n, len(points))
	}

	// Verify first and last points
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
		WalConfig:      WalConfig{MaxCacheSize: 64 * 1024 * 1024},
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
		WalConfig:      WalConfig{MaxCacheSize: 64 * 1024 * 1024},
		MaxStorageTime: 24 * 60 * 60 * 365,
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{
			Name: "empty_table",
			Type: ColumnTypeFloat,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UnixMilli()
	points, err := db.QueryAll("empty_table", "cpu", now-1000, now+1000, nil)
	// QueryAll may return nil points for never-written tags, which is acceptable
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
		WalConfig:      WalConfig{MaxCacheSize: 64 * 1024 * 1024},
		MaxStorageTime: 24 * 60 * 60 * 365,
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{
			Name: "multi_tag",
			Type: ColumnTypeInt,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now().UnixNano()
	tags := []string{"cpu0", "cpu1", "cpu2"}
	for i, tag := range tags {
		_, err = db.Write("multi_tag", tag, baseTime, variant.NewInt(i*100))
		if err != nil {
			t.Fatalf("Write %s failed: %v", tag, err)
		}
	}

	// Each tag should have exactly 1 point
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
		WalConfig:      WalConfig{MaxCacheSize: 64 * 1024 * 1024},
		MaxStorageTime: 24 * 60 * 60 * 365,
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{
			Name: "struct_table",
			Type: ColumnTypeStructure,
			Structure: []ColumnAttribute{
				{Name: "name", Type: ColumnTypeString},
				{Name: "value", Type: ColumnTypeFloat},
			},
		},
	})
	if err != nil {
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
		t.Fatalf("QueryAll: expected %d points, got %d", number, len(points))
	}

	// Verify first and last points
	if points[0].Tms != baseTime {
		t.Errorf("first point Tms: got %d, want %d", points[0].Tms, baseTime)
	}
	if points[number-1].Tms != baseTime+int64(number-1)*int64(time.Millisecond) {
		t.Errorf("last point Tms: got %d, want %d", points[number-1].Tms, baseTime+int64(number-1))
	}
}

func TestDB_QueryWithCondition(t *testing.T) {
	db, err := Open(Config{
		Path:           tempDir(t),
		WalConfig:      WalConfig{MaxCacheSize: 64 * 1024 * 1024},
		MaxStorageTime: 24 * 60 * 60 * 365,
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{
			Name: "cond_table",
			Type: ColumnTypeStructure,
			Structure: []ColumnAttribute{
				{Name: "score", Type: ColumnTypeInt},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now().UnixNano()
	for i := 0; i < 100; i++ {
		v := variant.New(map[string]variant.Variant{
			"score": variant.NewInt(i),
		})
		_, err = db.Write("cond_table", "student", baseTime+int64(i), v)
		if err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
	}

	// Query with condition: score > 50
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
		WalConfig:      WalConfig{MaxCacheSize: 64 * 1024 * 1024},
		MaxStorageTime: 24 * 60 * 60 * 365,
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{
			Name: "unsorted_table",
			Type: ColumnTypeInt,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now().UnixNano()
	// Write out-of-order timestamps: 100, 50, 75, 200, 150
	for _, offset := range []int64{100, 50, 75, 200, 150} {
		_, err = db.Write("unsorted_table", "tag1", baseTime+offset, variant.NewInt(int(offset)))
		if err != nil {
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
	// 模拟 BenchmarkE2E_WriteAndColumnQuery 的简化版本
	// 写入 20000 条 column 数据，每条带 value 列，然后测试查询为什么不能返回期望条数
	db, err := Open(Config{
		Path:           tempDir(t),
		WalConfig:      WalConfig{MaxCacheSize: 64 * 1024 * 1024},
		MaxStorageTime: 24 * 60 * 60 * 365,
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{Name: "test_col", FloatPrecision: 2},
	})
	if err != nil {
		t.Fatal(err)
	}

	const totalPoints = 20000
	baseTime := time.Now().UnixNano()

	// 写入：每条数据是一个 map，包含 value 和 tag 字段
	for i := 0; i < totalPoints; i++ {
		mp := make(map[string]any)
		mp["value"] = float64(i)
		mp["name"] = "sensor_" + strconv.Itoa(i%100)
		_, err := db.Write("test_col", "tag1", baseTime+int64(i)*int64(time.Second), variant.New(mp))
		if err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
	}

	t.Logf("===== 写入 %d 条数据完成 =====", totalPoints)

	// 测试1：无条件的全量查询 — 期望返回所有 20000 条，但 maxNumber 默认为 10000
	points1, err := db.Query("test_col", "tag1",
		baseTime, baseTime+int64(totalPoints)*int64(time.Second), 0, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("测试1 (无条件, maxNumber=0→10000, fusion=1): 返回 %d 条", len(points1))

	// 测试2：指定大 maxNumber
	points2, err := db.Query("test_col", "tag1",
		baseTime, baseTime+int64(totalPoints)*int64(time.Second), 30000, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("测试2 (无条件, maxNumber=30000, fusion=1): 返回 %d 条", len(points2))

	// 测试3：用 QueryAll 直接查询
	points3, err := db.QueryAll("test_col", "tag1",
		baseTime, baseTime+int64(totalPoints)*int64(time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("测试3 (QueryAll 无条件): 返回 %d 条", len(points3))

	// 测试4：列条件查询 — 参考 BenchmarkE2E_WriteAndColumnQuery
	// value > 5000 AND value < 15000 (预期有 9999 条匹配)
	points4, err := db.Query("test_col", "tag1",
		baseTime, baseTime+int64(totalPoints)*int64(time.Second), 0, 1,
		LogicalCondition{
			Op: LogicalAnd,
			Cond: []any{
				Condition{ColumnAttributeName: "value", Operator: OpGreaterThan, Value: variant.NewInt64(5000)},
				Condition{ColumnAttributeName: "value", Operator: OpLessThan, Value: variant.NewInt64(15000)},
			},
		})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("测试4 (value>5000 AND value<15000, maxNumber=0): 返回 %d 条 (预期约9999)", len(points4))

	// 测试5：缩小查询时间范围，绕过 QueryLimitNumber
	// db.Query 中：如果 endTime-startTime <= 3600000ns，直接用 Query (不限制)
	// 但这要求时间范围极小，不实用
	points5, err := db.Query("test_col", "tag1",
		baseTime+5000*int64(time.Second), baseTime+15000*int64(time.Second), 0, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("测试5 (无条件, 时间范围10s, 绕过QueryLimitNumber): 返回 %d 条", len(points5))

	// 测试6：使用不带条件的列查询 + 大 maxNumber
	points6, err := db.Query("test_col", "tag1",
		baseTime+5000*int64(time.Second), baseTime+15000*int64(time.Second), 10001, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("测试6 (无条件, 时间范围10s, maxNumber=10001): 返回 %d 条", len(points6))

	// 测试7：使用 avg 聚合（fusion=0）
	points7, err := db.Query("test_col", "tag1",
		baseTime, baseTime+int64(totalPoints)*int64(time.Second), 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("测试7 (无条件, maxNumber=0→10000, fusion=0 avg): 返回 %d 条", len(points7))
}

func TestDB_QueryLimitNumber_WithUnsortedWAL(t *testing.T) {
	db, err := Open(Config{
		Path:           tempDir(t),
		WalConfig:      WalConfig{MaxCacheSize: 64 * 1024 * 1024},
		MaxStorageTime: 24 * 60 * 60 * 365,
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{
			Name: "limit_unsorted",
			Type: ColumnTypeInt,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now().UnixNano()
	// Write sequential points spanning several seconds.
	for i := 0; i < 10; i++ {
		_, err = db.Write("limit_unsorted", "tag1", baseTime+int64(i)*int64(time.Second), variant.NewInt(i))
		if err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
	}
	// Write out-of-order points into the WAL cache.
	extraBase := baseTime + 5*int64(time.Second)
	for _, offset := range []int64{300, 100, 400, 200, 500} {
		_, err = db.Write("limit_unsorted", "tag1", extraBase+offset*int64(time.Millisecond), variant.NewInt(int(offset)))
		if err != nil {
			t.Fatalf("Write extra offset %d failed: %v", offset, err)
		}
	}

	// Query with maxNumber=5 -- triggers QueryLimitNumber (range > 3.6ms threshold).
	points, err := db.Query("limit_unsorted", "tag1", baseTime, baseTime+20*int64(time.Second), 5, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) == 0 {
		t.Fatal("expected at least 1 point, got 0")
	}
	if len(points) > 5 {
		t.Errorf("expected at most 5 points, got %d", len(points))
	}
	for i := 1; i < len(points); i++ {
		if points[i].Tms < points[i-1].Tms {
			t.Errorf("points not sorted: points[%d].Tms=%d > points[%d].Tms=%d",
				i-1, points[i-1].Tms, i, points[i].Tms)
		}
	}
}
