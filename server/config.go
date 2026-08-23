package server

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/mababaNiubi/qv-lite/tsdb"
)

// Config holds both the network listener settings and the embedded engine
// tuning knobs. It can be supplied via command-line flags, a JSON config file
// (via -config, values there override flags), or programmatically.
type Config struct {
	// Listen is the host:port the HTTP server binds to. Default ":8686".
	Listen string `json:"listen"`

	// DB mirrors the engine's tsdb.Config (data path + performance knobs).
	DB tsdb.Config `json:"db"`

	// MaxBodyBytes protects the server from oversized requests. Default 64 MiB.
	MaxBodyBytes int64 `json:"max_body_bytes"`

	// ReadTimeout / WriteTimeout for the HTTP server (Go duration strings).
	ReadTimeout  string `json:"read_timeout"`
	WriteTimeout string `json:"write_timeout"`

	// EnablePprof exposes /debug/pprof endpoints when true (useful for
	// profiling write/read hot paths). Default false.
	EnablePprof bool `json:"enable_pprof"`

	// Token, when non-empty, requires every API request to carry an
	// "X-Auth-Token" header matching this value. Default empty (no auth).
	Token string `json:"token,omitempty"`

	// WriteBufferMs enables the decode→ingest pipeline: writes are buffered
	// for up to this many milliseconds then coalesced into larger engine
	// batches by a single background writer goroutine. This decouples
	// decoding (CPU, on the request goroutine) from ingestion (engine WAL
	// lock, on the writer goroutine) — deserialization no longer blocks
	// engine writes — and coalesces small concurrent request batches into
	// one engine WriteBatch per table per flush cycle. Default 5.
	// 0 disables the pipeline (immediate write on the request goroutine).
	// Queries, single-point writes and shutdown always flush the buffer
	// first, so read-after-write stays consistent.
	WriteBufferMs int64 `json:"write_buffer_ms"`

	// WriteBatchSize is the buffered point count that triggers a coalesced
	// engine write while the pipeline is enabled. Default 100000.
	WriteBatchSize int `json:"write_batch_size"`
}

// DefaultConfig returns a server Config tuned for throughput: the decode→ingest
// pipeline is on (5ms buffer) so streaming writes run on a single background
// writer goroutine, async flush keeps writes off the segment-encoding path,
// async cleanup handles expired data, and the WAL buffer is reasonably large.
func DefaultConfig() Config {
	return Config{
		Listen:       ":8686",
		MaxBodyBytes: 64 << 20,
		ReadTimeout:  "30s",
		WriteTimeout: "60s",
		// 流水线默认开启：流式写入由单个后台 goroutine 合并入库，
		// 反序列化在请求 goroutine 上进行，互不阻塞。
		WriteBufferMs: 5,
		DB: tsdb.Config{
			Path:                     "./qvLite-data",
			MaxSegmentSize:           64 << 20,
			MaxSegmentTimeInterval:   0,
			AsyncFlush:               true,
			AsyncCleanup:             true,
			CleanupIntervalSeconds:   60,
			SecondaryCompressionName: "zstd",
		},
	}
}

// ApplyDefaults fills zero-valued fields with sensible defaults.
func (c *Config) ApplyDefaults() {
	def := DefaultConfig()
	if c.Listen == "" {
		c.Listen = def.Listen
	}
	if c.MaxBodyBytes <= 0 {
		c.MaxBodyBytes = def.MaxBodyBytes
	}
	if c.ReadTimeout == "" {
		c.ReadTimeout = def.ReadTimeout
	}
	if c.WriteTimeout == "" {
		c.WriteTimeout = def.WriteTimeout
	}
	if c.DB.Path == "" {
		c.DB.Path = def.DB.Path
	}
	if c.DB.MaxSegmentSize <= 0 {
		c.DB.MaxSegmentSize = def.DB.MaxSegmentSize
	}
	if c.DB.SecondaryCompressionName == "" {
		c.DB.SecondaryCompressionName = def.DB.SecondaryCompressionName
	}
	if c.DB.CleanupIntervalSeconds <= 0 {
		c.DB.CleanupIntervalSeconds = def.DB.CleanupIntervalSeconds
	}
	if c.WriteBatchSize <= 0 {
		c.WriteBatchSize = 100_000
	}
}

// Flags registers the supported command-line flags onto fs. Values set here
// become the base config that a -config JSON file (if any) overrides.
func (c *Config) Flags(fs *flag.FlagSet) {
	fs.StringVar(&c.Listen, "listen", c.Listen, "listener address, e.g. :8686 or 0.0.0.0:8686")
	fs.StringVar(&c.DB.Path, "db-path", c.DB.Path, "path to the tsdb data directory")
	fs.BoolVar(&c.DB.AsyncFlush, "async-flush", c.DB.AsyncFlush, "encode/flush WAL data in a background goroutine (higher write throughput)")
	fs.BoolVar(&c.DB.AsyncCleanup, "async-cleanup", c.DB.AsyncCleanup, "run periodic cleanup of expired segments in the background")
	fs.Int64Var(&c.DB.MaxSegmentSize, "max-segment-size", c.DB.MaxSegmentSize, "max segment (column block) size in bytes before a new file is started")
	fs.Int64Var(&c.DB.MaxSegmentTimeInterval, "max-segment-interval", c.DB.MaxSegmentTimeInterval, "max segment time span in seconds (0 = unlimited)")
	fs.Int64Var(&c.DB.ExpirationMinuteTime, "data-expiration", c.DB.ExpirationMinuteTime, "data retention in minutes (0 = never expire)")
	fs.Int64Var(&c.DB.DedupWindowMs, "dedup-window", c.DB.DedupWindowMs, "deduplication window in milliseconds (0 = disabled)")
	fs.Int64Var(&c.DB.MinIntervalMs, "min-interval", c.DB.MinIntervalMs, "minimum interval between writes in milliseconds (0 = disabled)")
	fs.Int64Var(&c.DB.MaxStorageTime, "max-storage-time", c.DB.MaxStorageTime, "max allowed age of stored timestamps vs now, seconds (0 = loose)")
	fs.StringVar(&c.DB.SecondaryCompressionName, "compression", c.DB.SecondaryCompressionName, "block compression: zstd, lz4, snappy, gzip, none")
	fs.Int64Var(&c.MaxBodyBytes, "max-body", c.MaxBodyBytes, "max request body bytes")
	fs.BoolVar(&c.EnablePprof, "pprof", c.EnablePprof, "enable /debug/pprof endpoints")
	fs.StringVar(&c.Token, "token", c.Token, "require this X-Auth-Token on every API request (empty = no auth)")
	fs.Int64Var(&c.WriteBufferMs, "write-buffer-ms", c.WriteBufferMs, "decode->ingest pipeline buffer period in ms (default 5; 0 = immediate writes)")
	fs.IntVar(&c.WriteBatchSize, "write-batch-size", c.WriteBatchSize, "coalesced engine batch size for the write pipeline")
}

// Load parses the given arguments. It first applies defaults, then command
// line flags, then a -config JSON file if provided (file takes precedence for
// any key it sets) so that server configs can be saved to disk.
func Load(args []string) (*Config, error) {
	cfg := DefaultConfig()
	fs := flag.NewFlagSet("tsdb-server", flag.ContinueOnError)
	var configPath string
	fs.StringVar(&configPath, "config", "", "path to a JSON config file")
	cfg.Flags(fs)

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	if configPath != "" {
		if err := cfg.applyFile(configPath); err != nil {
			return nil, err
		}
	}

	cfg.ApplyDefaults()
	return &cfg, nil
}

func (c *Config) applyFile(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config file %s: %w", path, err)
	}
	if err := json.Unmarshal(raw, c); err != nil {
		return fmt.Errorf("parse config file %s: %w", path, err)
	}
	return nil
}

// Validate checks structural constraints and makes the data directory absolute
// if it is relative to the current working directory.
func (c *Config) Validate() error {
	if c.DB.Path == "" {
		return errors.New("db path must not be empty")
	}
	abs, err := filepath.Abs(c.DB.Path)
	if err != nil {
		return fmt.Errorf("resolve db path: %w", err)
	}
	c.DB.Path = abs
	switch c.DB.SecondaryCompressionName {
	case "zstd", "lz4", "snappy", "gzip", "none":
	default:
		return fmt.Errorf("unsupported compression %q", c.DB.SecondaryCompressionName)
	}
	return nil
}
