package benchmark

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/mababaNiubi/qv-lite/tsdb"
	"github.com/mababaNiubi/variant"
)

// TestProfileWritePath captures a CPU profile of the write phase (single-write
// with a small WAL, forcing inline flushes) so the hot functions can be located
// with go tool pprof. Gated by PROFILE_WRITE=1 because it is slow.
//
//	PROFILE_WRITE=1 WAL_SIZE=524288 go test ./doc/benchmark/ -run TestProfileWritePath -v -count=1
//	go tool pprof -top doc/benchmark/write_cpu.out
//
// WAL_SIZE overrides WalConfig.MaxFileSize in bytes (default 512KB, which
// forces many inline flushes; set 0 for the default 64MB to isolate the
// per-point write cost from flush cost).
func TestProfileWritePath(t *testing.T) {
	if os.Getenv("PROFILE_WRITE") == "" {
		t.Skip("set PROFILE_WRITE=1 to capture the write-path CPU profile")
	}
	walSize := int64(512 * 1024)
	if v := os.Getenv("WAL_SIZE"); v != "" {
		if _, err := fmt.Sscanf(v, "%d", &walSize); err != nil {
			t.Fatalf("invalid WAL_SIZE %q", v)
		}
	}
	pts := 2_000_000
	dir, err := os.MkdirTemp("", "qv_profile_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := tsdb.Open(tsdb.Config{
		Path: dir,
		WalConfig: tsdb.WalConfig{
			MaxFileSize: walSize,
		},
		MaxStorageTime: 100 * 365 * 24 * 3600,
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTable(tsdb.TableInfo{ColumnAttribute: tsdb.ColumnAttribute{
		Name: "profile", FloatPrecision: 4,
	}}); err != nil {
		t.Fatal(err)
	}

	out := "write_cpu.out" // written to the package dir (cwd of go test)
	base := time.Now().UnixNano()
	err = ProfileFn(out, func() {
		for i := 0; i < pts; i++ {
			tag := fmt.Sprintf("tag_%d", i%10)
			if _, err := db.Write("profile", tag, base+int64(i)*int64(time.Millisecond),
				variant.NewFloat64(20+float64(i%1000)/100)); err != nil {
				t.Fatalf("write %d: %v", i, err)
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	t.Logf("wrote %d pts; cpu profile -> %s (analyze: go tool pprof -top %s)", pts, out, out)
}
