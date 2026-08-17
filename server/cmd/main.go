// Command tsdb-server runs the qv-lite time-series database as a standalone
// network service, exposing write/query/table-management APIs over HTTP.
//
// Build:
//
//	go build ./cmd/tsdb-server
//
// Run with defaults:
//
//	./tsdb-server
//
// Flags and a JSON config file are supported; see -h for details.
package main

import (
	"fmt"
	"os"

	"github.com/mababaNiubi/qv-lite/server"
)

func main() {
	cfg, err := server.Load(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(2)
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(2)
	}

	srv, err := server.New(cfg, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start error: %v\n", err)
		os.Exit(1)
	}

	if err := srv.RunListenAndServeSignal(); err != nil {
		fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintln(os.Stderr, "server stopped")
}
