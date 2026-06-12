package tsdb

import (
	"context"
	"fmt"
	"io/fs"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mababaNiubi/variant"
)

func benchDir(b *testing.B) string {
	dir, err := os.MkdirTemp("", "tsdb_bench_*")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		os.RemoveAll(dir)
	})
	return dir
}

func openBenchDB(b *testing.B, dir string, walCacheSize int64) *DB {
	db, err := Open(Config{
		Path:           dir,
		WalConfig:      WalConfig{MaxCacheSize: walCacheSize},
		MaxStorageTime: 24 * 60 * 60 * 365,
	}, context.Background())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		db.Close()
	})
	return db
}

// ==================== E2E: 写后即查 ====================

// BenchmarkE2E_WriteAndQuery 模拟 main.go 的完整流程：
// 写入 86400 秒 * 1000 点/秒 = 86,400,000 个数据点，然后全量查询。
// 使用 -benchtime=1x 确保只运行一次。
func BenchmarkE2E_WriteAndQuery(b *testing.B) {
	go func() {
		http.ListenAndServe(":6060", nil)
	}()
	const totalPoints = 24 * 60 * 60 * 1000 // 86,400,000
	dir := benchDir(b)
	tableName := "eu12"
	tag := "CPU"
	db := openBenchDB(b, dir, 256*1024*1024)
	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{Name: tableName, FloatPrecision: 2},
	}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	// 写入阶段
	writeStart := time.Now()
	baseTime := writeStart.UnixNano()
	for i := 0; i < totalPoints; i++ {
		_, err := db.Write(tableName, tag, baseTime+int64(i)*int64(time.Millisecond), variant.NewFloat64(123+float64(i)*0.01))
		if err != nil {
			b.Fatalf("write %d failed: %v", i, err)
		}
	}
	writeElapsed := time.Since(writeStart)
	// 查询阶段
	queryStart := time.Now()
	//all, err := db.Query(tableName, tag, baseTime-100, baseTime+int64(totalPoints)*int64(time.Millisecond)+100, 0, 1, nil)
	all, err := db.QueryAll(tableName, tag, baseTime-100, baseTime+int64(totalPoints)*int64(time.Millisecond)+100, nil)
	if err != nil {
		b.Fatal(err)
	}
	if len(all) >= 2 {
		b.Log(all[0], all[len(all)-1], all[len(all)-2])
	}
	queryElapsed := time.Since(queryStart)
	_ = db.Close()
	b.Logf("write: %v (%.0f pts/s), read: %v (%.0f pts/s), count: %d,size: %v",
		writeElapsed, float64(totalPoints)/writeElapsed.Seconds(),
		queryElapsed, float64(len(all))/queryElapsed.Seconds(),
		len(all), fileDirSize(dir, tableName))
}

func BenchmarkE2E_WriteAndColumnQuery(b *testing.B) {
	const totalPoints = 12 * 60 * 60 * 1000 // 86,400,000
	dir := benchDir(b)
	tableName := "eu12"
	tag := "CPU"
	db := openBenchDB(b, dir, 256*1024*1024)
	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{Name: tableName, FloatPrecision: 2},
	}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	// 写入阶段
	writeStart := time.Now()
	baseTime := writeStart.UnixNano()
	for i := 0; i < totalPoints; i++ {
		mp := make(map[string]any)
		mp["value"] = float64(i) * 0.01
		mp["tag"] = fmt.Sprintf("AX%v", i)
		_, err := db.Write(tableName, tag, baseTime+int64(i)*int64(time.Millisecond), variant.New(mp))
		if err != nil {
			b.Fatalf("write %d failed: %v", i, err)
		}
	}
	writeElapsed := time.Since(writeStart)
	// 查询阶段
	queryStart := time.Now()
	all, err := db.Query(tableName, tag, baseTime-100, baseTime+int64(totalPoints)*int64(time.Millisecond)+100, 0, 1, LogicalCondition{
		Op: LogicalAnd,
		Cond: []any{
			Condition{
				ColumnAttributeName: "value",
				Operator:            OpGreaterThan,
				Value:               variant.NewFloat64(60 * 60 * 10),
			},
			Condition{
				ColumnAttributeName: "value",
				Operator:            OpLessThan,
				Value:               variant.NewFloat64(60 * 60 * 10 * 2),
			},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	if len(all) >= 2 {
		b.Log(all[0], all[1], all[len(all)-1], all[len(all)-2])
	}
	queryElapsed := time.Since(queryStart)
	_ = db.Close()
	b.Logf("write: %v (%.0f pts/s), read: %v (%.0f pts/s), count: %d,size: %v",
		writeElapsed, float64(totalPoints)/writeElapsed.Seconds(),
		queryElapsed, float64(len(all))/queryElapsed.Seconds(),
		len(all), fileDirSize(dir, tableName))
}

func BenchmarkWriteParallel(b *testing.B) {
	const totalPoints = 24 * 60 * 60 * 1000 // 86,400,000
	dir := benchDir(b)
	tableName := "eu12"
	tag := "CPU"
	db := openBenchDB(b, dir, 256*1024*1024)
	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{Name: tableName, FloatPrecision: 2},
	}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	// 写入阶段
	writeStart := time.Now()
	baseTime := writeStart.UnixNano()
	for i := 0; i < totalPoints/1000; i++ {
		var wg sync.WaitGroup
		for j := 0; j < 1000; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := db.Write(tableName, tag, baseTime+int64(i*j)*int64(time.Millisecond), variant.NewFloat64(float64(123)+float64(i*j)*0.01))
				if err != nil {
					b.Fatalf("write %d failed: %v", i, err)
					return
				}
			}()
		}
		wg.Wait()
	}
	writeElapsed := time.Since(writeStart)
	// 查询阶段
	queryStart := time.Now()
	all, err := db.QueryAll(tableName, tag, baseTime-100, baseTime+int64(totalPoints)*int64(time.Millisecond)+100, nil)
	if err != nil {
		b.Fatal(err)
	}
	queryElapsed := time.Since(queryStart)
	_ = db.Close()
	b.Logf("write: %v (%.0f pts/s), read: %v (%.0f pts/s), count: %d,size: %v",
		writeElapsed, float64(totalPoints)/writeElapsed.Seconds(),
		queryElapsed, float64(len(all))/queryElapsed.Seconds(),
		len(all), fileDirSize(dir, tableName))
}

func fileDirSize(dir string, tableName string) string {
	var total int64
	_ = filepath.WalkDir(filepath.Join(dir, tableName, "data"), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// 遇到权限错误等问题时，打印错误但继续执行
			fmt.Fprintf(os.Stderr, "访问 %s 出错: %v\n", path, err)
			return nil
		}
		if !d.IsDir() {
			info, err := d.Info()
			if err != nil {
				fmt.Fprintf(os.Stderr, "获取 %s 信息出错: %v\n", path, err)
				return nil
			}
			total += info.Size()
		}
		return nil
	})
	if total < 1024 {
		return fmt.Sprintf("%v b", total)
	} else if total < 1024*1024 {
		return fmt.Sprintf("%.2fkB", float64(total)/1024)
	} else if total < 1024*1024*1024 {
		return fmt.Sprintf("%.2fMB", float64(total)/(1024*1024))
	} else {
		return fmt.Sprintf("%.2fGB", float64(total)/(1024*1024*1024))
	}
}
