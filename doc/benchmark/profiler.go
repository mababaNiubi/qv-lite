package benchmark

import (
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"time"
)

// StartPprofServer starts a net/http/pprof server on addr (e.g. ":6060") on
// the DefaultServeMux. It returns a stop func. Only the first call in a process
// actually listens; later calls are no-ops returning the same stop func.
func StartPprofServer(addr string) (stop func(), err error) {
	if addr == "" {
		return func() {}, nil
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("pprof listen %s: %w", addr, err)
	}
	srv := &http.Server{Addr: ln.Addr().String()}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "pprof server stopped: %v\n", err)
		}
	}()
	fmt.Printf("pprof server on http://%s/debug/pprof/\n", ln.Addr())
	return func() { _ = srv.Close() }, nil
}

// CPUProfile writes a CPU profile of the next dur while fn runs, to path.
// fn is expected to block for at least dur.
func CPUProfile(path string, dur time.Duration) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		_ = f.Close()
		return err
	}
	time.Sleep(dur)
	pprof.StopCPUProfile()
	return f.Close()
}

// ProfileFn writes a CPU profile to path covering the execution of fn. It is
// the building block for phase-targeted profiling (e.g. profile only the write
// phase of a benchmark). The caller keeps the returned file handle; Close it
// after StopCPUProfile semantics (ProfileFn already stops the profile).
func ProfileFn(path string, fn func()) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		_ = f.Close()
		return err
	}
	fn()
	pprof.StopCPUProfile()
	return f.Close()
}

// HeapProfile writes the current heap profile to path.
func HeapProfile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return pprof.WriteHeapProfile(f)
}

// GoroutineProfile writes the current goroutine profile to path.
func GoroutineProfile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return pprof.Lookup("goroutine").WriteTo(f, 0)
}

// MemSample is one memory snapshot taken during a run.
type MemSample struct {
	At          time.Time `json:"at"`
	HeapAlloc   uint64    `json:"heap_alloc"`
	HeapInuse   uint64    `json:"heap_inuse"`
	Sys         uint64    `json:"sys"`
	NumGoroutine int      `json:"num_goroutine"`
}

// MemorySampler periodically records MemStats; drain() returns the samples.
type MemorySampler struct {
	interval time.Duration
	stopCh   chan struct{}
	done     chan struct{}
	samples  []MemSample
}

// NewMemorySampler starts sampling every interval. Call Drain to stop and get
// the samples, then Peak for the max HeapAlloc.
func NewMemorySampler(interval time.Duration) *MemorySampler {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	ms := &MemorySampler{interval: interval, stopCh: make(chan struct{}), done: make(chan struct{})}
	go ms.run()
	return ms
}

func (ms *MemorySampler) run() {
	defer close(ms.done)
	t := time.NewTicker(ms.interval)
	defer t.Stop()
	for {
		select {
		case <-ms.stopCh:
			return
		case now := <-t.C:
			var st runtime.MemStats
			runtime.ReadMemStats(&st)
			ms.samples = append(ms.samples, MemSample{
				At:           now,
				HeapAlloc:    st.HeapAlloc,
				HeapInuse:    st.HeapInuse,
				Sys:          st.Sys,
				NumGoroutine: runtime.NumGoroutine(),
			})
		}
	}
}

// Drain stops sampling and returns the collected samples.
func (ms *MemorySampler) Drain() []MemSample {
	close(ms.stopCh)
	<-ms.done
	return ms.samples
}

// Peak returns the maximum HeapAlloc observed so far.
func (ms *MemorySampler) Peak() uint64 {
	var peak uint64
	for _, s := range ms.samples {
		if s.HeapAlloc > peak {
			peak = s.HeapAlloc
		}
	}
	return peak
}
