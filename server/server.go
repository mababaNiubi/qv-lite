package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mababaNiubi/qv-lite/tsdb"
)

// Server wraps an embedded tsdb engine and exposes it over a JSON HTTP API.
type Server struct {
	cfg    *Config
	db     *tsdb.DB
	http   *http.Server
	mux    *http.ServeMux
	start  time.Time
	logger Logger
	writer *PipelinedWriter // 非 nil 时开启 decode→入库流水线
}

// Logger is the minimal logging interface used by the server. It defaults to
// a simple stderr logger when nil.
type Logger interface {
	Printf(format string, v ...any)
}

type stdLogger struct{}

func (stdLogger) Printf(format string, v ...any) {
	fmt.Fprintf(os.Stderr, "[tsdb-server] "+format+"\n", v...)
}

// New builds a Server from a config and opens the underlying tsdb database.
func New(cfg *Config, logger Logger) (*Server, error) {
	cfg.ApplyDefaults()
	if logger == nil {
		logger = stdLogger{}
	}
	db, err := tsdb.Open(cfg.DB, context.Background())
	if err != nil {
		return nil, fmt.Errorf("open tsdb at %s: %w", cfg.DB.Path, err)
	}
	s := &Server{
		cfg:    cfg,
		db:     db,
		mux:    http.NewServeMux(),
		start:  time.Now(),
		logger: logger,
	}
	// 可选：decode→入库流水线（编解码与入库并行，合并小批提高吞吐）。
	if cfg.WriteBufferMs > 0 {
		s.writer = NewPipelinedWriter(db, int(cfg.WriteBufferMs), cfg.WriteBatchSize)
	}
	s.routes()
	// 生产向 http.Server：合理的读写超时。
	s.http = &http.Server{
		Addr:         cfg.Listen,
		Handler:      s.Handler(),
		ReadTimeout:  mustDuration(cfg.ReadTimeout, 30*time.Second),
		WriteTimeout: mustDuration(cfg.WriteTimeout, 60*time.Second),
	}
	return s, nil
}

// Handler returns the root http.Handler (useful for embedding in tests or
// mounting under another router). It applies auth (when configured) and panic
// recovery.
func (s *Server) Handler() http.Handler {
	return s.withAuth(s.withRecovery(s.mux))
}

// DB exposes the underlying engine for advanced use.
func (s *Server) DB() *tsdb.DB { return s.db }

func (s *Server) routes() {
	if s.cfg.EnablePprof {
		registerPprof(s.mux)
	}
	s.mux.HandleFunc("GET /api/v1/health", s.handleHealth)
	s.mux.HandleFunc("POST /api/v1/tables", s.handleCreateTable)
	s.mux.HandleFunc("GET /api/v1/tables", s.handleListTables)
	s.mux.HandleFunc("POST /api/v1/write", s.handleWrite)
	s.mux.HandleFunc("POST /api/v1/write/line", s.handleWriteLine)
	s.mux.HandleFunc("POST /api/v1/batch", s.handleBatch)
	s.mux.HandleFunc("POST /api/v1/query", s.handleQuery)
	s.mux.HandleFunc("POST /api/v1/query/latest", s.handleQueryLatest)
}

// ListenAndServe starts the HTTP listener. It blocks until the context is
// cancelled or the server fails. On ctx cancellation it performs graceful
// shutdown, waiting for in-flight requests and then closing the engine.
func (s *Server) ListenAndServe(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		s.logger.Printf("listening on %s, data path %s", s.cfg.Listen, s.cfg.DB.Path)
		if err := s.http.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		return s.Shutdown(context.Background())
	case err := <-errCh:
		return err
	}
}

// Shutdown gracefully stops accepting new connections, gives in-flight
// requests up to timeout to finish, then closes the engine.
func (s *Server) Shutdown(ctx context.Context) error {
	if err := s.http.Shutdown(ctx); err != nil {
		s.logger.Printf("http shutdown: %v", err)
	}
	if s.writer != nil {
		if err := s.writer.Close(); err != nil {
			s.logger.Printf("pipeline writer close: %v", err)
		}
	}
	return s.db.Close()
}

// RunListenAndServeSignal is a convenience that wires SIGINT/SIGTERM to a
// graceful shutdown. Call it from main.
func (s *Server) RunListenAndServeSignal() error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	err := s.ListenAndServe(ctx)
	if ctx.Err() != nil {
		return nil // clean shutdown via signal
	}
	return err
}
