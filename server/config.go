package server

import "github.com/mababaNiubi/qv-lite/tsdb"

type Config struct {
	// HttpAddr is the address for the HTTP API server (e.g., ":8080").
	// Empty means the HTTP server is not started.
	HttpAddr string `json:"http_addr"`
	// TcpAddr is the address for the TCP SQL server (e.g., ":9876").
	// Empty means the TCP server is not started.
	TcpAddr string `json:"tcp_addr"`

	DBConfig tsdb.Config `json:"db_config"`
}
