package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/mababaNiubi/qv-lite/server"
	"github.com/mababaNiubi/qv-lite/tsdb"
)

func main() {
	configFile := flag.String("config", "", "Path to JSON config file")
	cli := flag.Bool("cli", false, "Run in command-line mode (stdin/stdout)")
	flag.Parse()

	config, err := loadConfig(*configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	db, err := tsdb.Open(config.DBConfig, context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	if *cli {
		runCLI(db)
		return
	}

	// Start TCP server
	tcpSrv, err := server.NewServer(db, config.TcpAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create TCP server: %v\n", err)
		os.Exit(1)
	}
	go func() {
		if err := tcpSrv.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "TCP server error: %v\n", err)
		}
	}()
	defer tcpSrv.Close()

	// Start HTTP server
	httpSrv, err := server.NewHttpServer(db, config.HttpAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create HTTP server: %v\n", err)
		os.Exit(1)
	}
	httpSrv.StartAsync()
	defer httpSrv.Close()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	fmt.Fprintln(os.Stderr, "\nShutting down...")
}

func loadConfig(path string) (server.Config, error) {
	def := server.Config{
		TcpAddr:  ":8811",
		HttpAddr: ":8822",
	}
	if path == "" {
		return def, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return def, fmt.Errorf("read config file: %w", err)
	}
	var config server.Config
	if err := json.Unmarshal(data, &config); err != nil {
		return def, fmt.Errorf("parse config file: %w", err)
	}
	return config, nil
}

func runCLI(db *tsdb.DB) {
	executor := server.NewExecutor(db)
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("TSDB CLI — type SQL statements, 'quit' or 'exit' to quit, 'help' for help")
	fmt.Print("> ")

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			fmt.Print("> ")
			continue
		}

		switch strings.ToLower(line) {
		case "quit", "exit":
			fmt.Println("bye")
			return
		case "help":
			fmt.Println("Commands:")
			fmt.Println("  CREATE TABLE <name> [TYPE] [PRECISION n]")
			fmt.Println("    types: float/int/integer/string/bool/json")
			fmt.Println("  INSERT INTO <table> <tag> <timestamp> <value>")
			fmt.Println("  INSERT INTO <table> (tag, time, value) VALUES (...), (...), ...")
			fmt.Println("  SELECT <table> <tag> <start> <end> [LIMIT n] [POLYMERIZATION avg|min|max] [HAVING col op val]")
			fmt.Println("  SELECT LATEST <table> <tag>")
			fmt.Println("  quit / exit    — exit")
			fmt.Println("  help           — show this help")
			fmt.Print("> ")
			continue
		}

		stmt, err := server.Parse(line)
		if err != nil {
			fmt.Fprintf(os.Stderr, "parse error: %v\n", err)
			fmt.Print("> ")
			continue
		}

		result, err := executor.Execute(stmt)
		if err != nil {
			fmt.Fprintf(os.Stderr, "exec error: %v\n", err)
			fmt.Print("> ")
			continue
		}

		fmt.Println(server.FormatResultJSON(result))
		fmt.Print("> ")
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "input error: %v\n", err)
	}
}
