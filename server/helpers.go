package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/pprof"
	"runtime/debug"
	"time"

	"github.com/mababaNiubi/qv-lite/tsdb"
)

type apiError struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, apiError{Error: err.Error()})
}

func decodeBody(r *http.Request, dst any) error {
	return json.NewDecoder(r.Body).Decode(dst)
}

// badRequest 以统一的 "bad request: <err>" 文本写 400。
func badRequest(w http.ResponseWriter, err error) {
	writeErr(w, http.StatusBadRequest, fmt.Errorf("bad request: %w", err))
}

// writeError 按引擎错误类型映射 HTTP 状态并写出；无法识别的错误写 500。
func writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, tsdb.ErrorTableExists):
		status = http.StatusConflict
	case errors.Is(err, tsdb.ErrorTableNotExists),
		errors.Is(err, tsdb.ErrorTagNotFound),
		errors.Is(err, tsdb.ErrorNoDataForTag):
		status = http.StatusNotFound
	case errors.Is(err, tsdb.ErrorTimeOut),
		errors.Is(err, tsdb.ErrorValueIsEmpty):
		status = http.StatusBadRequest
	}
	writeErr(w, status, err)
}

// mustDuration parses a Go duration string, falling back to def on any error.
func mustDuration(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return def
	}
	return d
}

// registerPprof exposes the standard pprof endpoints under /debug/pprof.
func registerPprof(mux *http.ServeMux) {
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("/debug/pprof/block", pprof.Handler("block"))
	mux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
}

// withAuth enforces the configured X-Auth-Token, if any.
func (s *Server) withAuth(next http.Handler) http.Handler {
	if s.cfg.Token == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Auth-Token") != s.cfg.Token {
			writeJSON(w, http.StatusUnauthorized, apiError{Error: "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// withRecovery recovers panics from handlers and returns a 500 instead of
// crashing the process, while logging a stack trace.
func (s *Server) withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.logger.Printf("panic serving %s: %v\n%s", r.URL.Path, rec, debug.Stack())
				writeJSON(w, http.StatusInternalServerError, apiError{Error: "internal server error"})
			}
		}()
		next.ServeHTTP(w, r)
	})
}
