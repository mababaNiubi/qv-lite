// Command bench runs the qv-lite TSDB benchmark suite and compares runs.
//
// Usage:
//
//	bench run [-label NAME] [-out DIR] [-scenarios a,b] [-pprof :6060] [-quick] [-version V] [-keep]
//	    Run the scenario matrix, writing one JSON report per scenario plus
//	    <label>_all.json. Output dir defaults to ./results.
//
//	bench compare [-write F] [-read F] [-ratio F] [-heap F] OLD.json NEW.json
//	    Diff two run results (any JSON array of reports) with PASS/FAIL verdicts.
//
//	bench pprof :6060
//	    Start only the net/http/pprof server (for attaching to a long run).
//
// Examples:
//
//	go run ./doc/cmd/bench run -label baseline -out results -version head
//	go run ./doc/cmd/bench run -label v2-m1    -out results -version m1 -pprof :6060
//	go run ./doc/cmd/bench compare -write 0 -ratio 0 results/baseline_all.json results/v2-m1_all.json
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mababaNiubi/qv-lite/doc/benchmark"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	var err error
	switch os.Args[1] {
	case "run":
		err = run(os.Args[2:])
	case "compare":
		err = compare(os.Args[2:])
	case "pprof":
		err = servePprof(os.Args[2:])
	default:
		usage()
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `bench — qv-lite TSDB benchmark suite

subcommands:
  run [flags]                 run scenarios, write JSON reports
      -label NAME             run label (default "baseline")
      -out DIR                output dir (default "results")
      -scenarios a,b          comma-separated scenario names; empty = all
      -pprof :6060            start net/http/pprof server on addr
      -quick                  use the fast smoke suite
      -version V              implementation version tag (e.g. head, m1)
      -repeat N               run each scenario N times, report the median
      -keep                   keep temp data dirs (populated in report)
  compare [flags] OLD.json NEW.json
      -write F  -read F  -ratio F  -heap F   max allowed regression (default 0.2)
  pprof :6060                 start only the pprof server
`)
}

func run(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	label := fs.String("label", "baseline", "run label")
	out := fs.String("out", "results", "output dir")
	filter := fs.String("scenarios", "", "comma-separated scenario names")
	pprofAddr := fs.String("pprof", "", "pprof http addr, e.g. :6060")
	quick := fs.Bool("quick", false, "use the quick smoke suite")
	version := fs.String("version", "", "implementation version tag")
	keep := fs.Bool("keep", false, "keep temp data dirs")
	repeat := fs.Int("repeat", 3, "run each scenario N times, report the median (disk-busy noise; use 5+ for authoritative numbers)")
	fs.Parse(args)

	scenarios := benchmark.DefaultSuite()
	if *quick {
		scenarios = benchmark.QuickSuite()
	}
	if *filter != "" {
		want := map[string]bool{}
		for _, n := range strings.Split(*filter, ",") {
			want[strings.TrimSpace(n)] = true
		}
		var sel []benchmark.Scenario
		for _, sc := range scenarios {
			if want[sc.Name] {
				sel = append(sel, sc)
			}
		}
		scenarios = sel
	}
	if len(scenarios) == 0 {
		return fmt.Errorf("no scenarios selected")
	}

	var stop func()
	if *pprofAddr != "" {
		var err error
		stop, err = benchmark.StartPprofServer(*pprofAddr)
		if err != nil {
			return err
		}
		defer stop()
	}

	if err := os.MkdirAll(*out, 0o755); err != nil {
		return err
	}
	hc := benchmark.HarnessConfig{Label: *label, Version: *version, KeepDir: *keep}

	reports := make([]benchmark.RunReport, 0, len(scenarios))
	for _, sc := range scenarios {
		r, err := benchmark.RunScenario(sc, hc)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%-40s FAILED: %v\n", sc.Name, err)
			continue
		}
		if *repeat > 1 {
			rs := []benchmark.RunReport{*r}
			for k := 1; k < *repeat; k++ {
				rr, err := benchmark.RunScenario(sc, hc)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%-40s repeat %d FAILED: %v\n", sc.Name, k, err)
					continue
				}
				rs = append(rs, *rr)
			}
			med := benchmark.MedianReports(rs)
			r = &med
		}
		reports = append(reports, *r)
		writeJSON(filepath.Join(*out, fmt.Sprintf("%s_%s.json", *label, sc.Name)), r)
		fmt.Printf("%-40s %s\n", sc.Name, formatLine(r))
	}
	if err := writeJSON(filepath.Join(*out, fmt.Sprintf("%s_all.json", *label)), reports); err != nil {
		return err
	}
	fmt.Printf("saved %d/%d reports to %s\n", len(reports), len(scenarios), *out)
	return nil
}

func compare(args []string) error {
	fs := flag.NewFlagSet("compare", flag.ExitOnError)
	th := benchmark.DefaultThresholds()
	fs.Float64Var(&th.WriteRateMaxRegress, "write", th.WriteRateMaxRegress, "max write-rate regression (0 forbids)")
	fs.Float64Var(&th.ReadRateMaxRegress, "read", th.ReadRateMaxRegress, "max read-rate regression")
	fs.Float64Var(&th.RatioMaxRegress, "ratio", th.RatioMaxRegress, "max compression-ratio regression")
	fs.Float64Var(&th.HeapMaxRegress, "heap", th.HeapMaxRegress, "max peak-heap regression")
	fs.Parse(args)
	if fs.NArg() != 2 {
		return fmt.Errorf("compare needs OLD.json and NEW.json (got %d args)", fs.NArg())
	}

	old, err := loadReports(fs.Arg(0))
	if err != nil {
		return err
	}
	nw, err := loadReports(fs.Arg(1))
	if err != nil {
		return err
	}

	diffs := benchmark.Compare(old, nw, th)
	fmt.Printf("%-40s | %12s | %12s | %10s | %10s | %9s | %s\n",
		"scenario", "write(old->new)", "read(old->new)", "ratio", "heap", "B/pt", "verdict")
	for _, d := range diffs {
		if len(d.Metrics) == 0 {
			fmt.Printf("%-40s | (missing in one side)                                                     | FAIL\n", d.Scenario)
			continue
		}
		fmt.Printf("%-40s | %12s | %12s | %10s | %10s | %9s | %s\n",
			d.Scenario,
			pct(d.MetricByName("write_rate_pts_per_sec")),
			pct(d.MetricByName("read_rate_pts_per_sec")),
			pct(d.MetricByName("compression_ratio")),
			pct(d.MetricByName("peak_heap_bytes")),
			pct(d.MetricByName("bytes_per_point")),
			verdict(d.Pass),
		)
	}
	return nil
}

func servePprof(args []string) error {
	fs := flag.NewFlagSet("pprof", flag.ExitOnError)
	fs.Parse(args)
	addr := ":6060"
	if fs.NArg() > 0 {
		addr = fs.Arg(0)
	}
	stop, err := benchmark.StartPprofServer(addr)
	if err != nil {
		return err
	}
	defer stop()
	fmt.Println("pprof server running; press Ctrl-C to stop")
	select {}
}

func loadReports(path string) ([]benchmark.RunReport, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var rs []benchmark.RunReport
	if err := json.Unmarshal(data, &rs); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return rs, nil
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func formatLine(r *benchmark.RunReport) string {
	line := fmt.Sprintf("write=%8.0f pts/s (%6.2f MB/s) read=%8.0f pts/s ratio=%5.2f bytes/pt=%5.2f peakHeap=%6.1fMB segs=%d",
		r.WriteRate, r.WriteMBps, r.ReadRate, r.Ratio, r.BytesPerPoint,
		float64(r.PeakHeap)/1e6, r.SegmentCount)
	if r.QueryMode == "full" {
		if r.Correct {
			line += " ok"
		} else {
			line += " MISMATCH"
		}
	}
	if r.DiskStalled {
		line += " STALLED"
	}
	if r.StalledRuns > 0 {
		line += fmt.Sprintf(" (%d stalled runs dropped)", r.StalledRuns)
	}
	return line
}

func pct(m *benchmark.Metric) string {
	if m == nil {
		return "   n/a  "
	}
	if m.Old == 0 {
		return "   n/a  "
	}
	return fmt.Sprintf("%+8.1f%%", m.ChangePct)
}

func verdict(pass bool) string {
	if pass {
		return "PASS"
	}
	return "FAIL"
}
