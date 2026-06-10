package server

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"

	"github.com/mababaNiubi/qv-lite/tsdb"
)

type Server struct {
	db       *tsdb.DB
	listener net.Listener
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewServer(db *tsdb.DB, addr string) (*Server, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		db:       db,
		listener: listener,
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

func (s *Server) Addr() net.Addr {
	return s.listener.Addr()
}

func (s *Server) Start() error {
	fmt.Fprintf(os.Stderr, "TSDB SQL server listening on %s\n", s.listener.Addr())

	executor := NewExecutor(s.db)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return nil
			default:
				fmt.Fprintf(os.Stderr, "accept error: %v\n", err)
				continue
			}
		}
		go s.handleConn(conn, executor)
	}
}

func (s *Server) handleConn(conn net.Conn, executor *Executor) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		stmt, err := Parse(line)
		if err != nil {
			resp := FormatResultJSON(&Result{Success: false, Message: fmt.Sprintf("parse error: %v", err)})
			fmt.Fprintln(conn, resp)
			continue
		}

		result, err := executor.Execute(stmt)
		if err != nil {
			resp := FormatResultJSON(&Result{Success: false, Message: fmt.Sprintf("exec error: %v", err)})
			fmt.Fprintln(conn, resp)
			continue
		}

		fmt.Fprintln(conn, FormatResultJSON(result))
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "connection read error: %v\n", err)
	}
}

func (s *Server) Close() {
	s.cancel()
	s.listener.Close()
}
