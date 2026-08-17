package server

import (
	"net/http"
	"net/http/pprof"
	"runtime/debug"
	"time"
)

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
				status := http.StatusInternalServerError
				writeJSON(w, status, apiError{Error: "internal server error"})
			}
		}()
		next.ServeHTTP(w, r)
	})
}
