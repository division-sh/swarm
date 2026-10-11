// Package sqlite2353 contains only the disposable, unshipped #2353 observer.
package sqlite2353

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var epoch = time.Now()
var outputMu sync.Mutex
var firstRequest atomic.Bool
var identityOnce sync.Once

func Enabled() bool {
	row := os.Getenv("TEST_2353_ROW")
	if row != "S01" && row != "S02" && row != "S03" && row != "S04" {
		return false
	}
	if os.Getenv("TEST_2353_DIR") == "" {
		return false
	}
	return row != "S03" || os.Getenv("SWARM_TEST_MAILBOX_PROJECTION_OVERLAY") == "1"
}

func Record(phase string, facts any) {
	if !Enabled() {
		return
	}
	item := struct {
		Row       string `json:"row"`
		PID       int    `json:"pid"`
		UTC       string `json:"utc"`
		ElapsedNS int64  `json:"process_observer_elapsed_ns"`
		Phase     string `json:"phase"`
		Facts     any    `json:"facts,omitempty"`
	}{os.Getenv("TEST_2353_ROW"), os.Getpid(), time.Now().UTC().Format(time.RFC3339Nano), time.Since(epoch).Nanoseconds(), phase, facts}
	outputMu.Lock()
	defer outputMu.Unlock()
	path := filepath.Join(os.Getenv("TEST_2353_DIR"), fmt.Sprintf("%s-%d.jsonl", item.Row, item.PID))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err == nil {
		err = json.NewEncoder(f).Encode(item)
		err = joinWriteError(err, f.Close())
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "2353_OBSERVER_INCOMPLETE phase=%s error=%v\n", phase, err)
	}
}

func joinWriteError(first, second error) error {
	if first != nil {
		return first
	}
	return second
}

func Phase(ctx context.Context, name string) func() {
	if !Enabled() {
		return func() {}
	}
	start := time.Now()
	Record(name+".enter", nil)
	return func() {
		Record(name+".exit", map[string]any{"elapsed_ns": time.Since(start).Nanoseconds(), "cause": fmt.Sprint(context.Cause(ctx))})
	}
}

// Sampling runs in the actual owner, not in an outer go-test or HTTP client.
func Sample(name string) {
	if !Enabled() {
		return
	}
	started := time.Now()
	var usage syscall.Rusage
	err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	Record(name+".cpu", map[string]any{"usage": usage, "error": fmt.Sprint(err)})
	for _, path := range []string{"/proc/self/schedstat", "/proc/self/io", "/proc/stat", "/proc/loadavg", "/proc/pressure/cpu", "/proc/pressure/io"} {
		raw, err := os.ReadFile(path)
		Record(name+".proc", map[string]any{"path": path, "raw": string(raw), "error": fmt.Sprint(err)})
	}
	tasks, taskErr := os.ReadDir("/proc/self/task")
	taskStats := make([]map[string]any, 0, len(tasks))
	for i, task := range tasks {
		if i == 256 {
			break
		}
		raw, err := os.ReadFile(filepath.Join("/proc/self/task", task.Name(), "schedstat"))
		taskStats = append(taskStats, map[string]any{"tid": task.Name(), "raw": string(raw), "error": fmt.Sprint(err)})
	}
	Record(name+".thread_schedstat", map[string]any{"error": fmt.Sprint(taskErr), "tasks": taskStats, "truncated": len(tasks) > 256})
	outputMu.Lock()
	path := filepath.Join(os.Getenv("TEST_2353_DIR"), fmt.Sprintf("%s-%d.stacks", os.Getenv("TEST_2353_ROW"), os.Getpid()))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err == nil {
		_, err = fmt.Fprintf(f, "\n2353_SAMPLE name=%s utc=%s elapsed_ns=%d pid=%d\n", name, time.Now().UTC().Format(time.RFC3339Nano), time.Since(epoch).Nanoseconds(), os.Getpid())
		if err == nil {
			err = pprof.Lookup("goroutine").WriteTo(f, 2)
		}
		err = joinWriteError(err, f.Close())
	}
	outputMu.Unlock()
	Record(name+".sample_done", map[string]any{"observer_elapsed_ns": time.Since(started).Nanoseconds(), "error": fmt.Sprint(err)})
	// Private backend observers use the same non-fatal signal for typed snapshots.
	if err := syscall.Kill(os.Getpid(), syscall.SIGUSR2); err != nil {
		Record(name+".backend_signal_error", fmt.Sprint(err))
	}
}

// The signal receiver is owned/joined by this exact serve lifetime.
func Lifetime(ctx context.Context) func() {
	if !Enabled() {
		return func() {}
	}
	firstRequest.Store(false)
	requests := make(chan os.Signal, 1)
	signal.Notify(requests, syscall.SIGUSR2)
	stop, joined := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(joined)
		for {
			select {
			case <-stop:
				return
			case <-requests:
				Record("owner.signal_received", nil)
			}
		}
	}()
	Record("serve.lifetime.enter", map[string]any{"go_build": buildIdentity(), "gomaxprocs": os.Getenv("GOMAXPROCS")})
	var once sync.Once
	return func() {
		once.Do(func() {
			signal.Stop(requests)
			close(stop)
			<-joined
			Record("serve.lifetime.exit", map[string]any{"cause": fmt.Sprint(context.Cause(ctx))})
			identityOnce.Do(recordExecutableIdentity)
		})
	}
}

func buildIdentity() any {
	info, ok := debug.ReadBuildInfo()
	return map[string]any{"available": ok, "build_info": info}
}

// Hash after the lifetime so identity collection does not warm the startup path.
func recordExecutableIdentity() {
	path, err := os.Executable()
	if err != nil {
		Record("executable_identity", fmt.Sprint(err))
		return
	}
	f, err := os.Open(path)
	if err != nil {
		Record("executable_identity", fmt.Sprint(err))
		return
	}
	h := sha256.New()
	_, err = io.Copy(h, f)
	err = joinWriteError(err, f.Close())
	Record("executable_identity", map[string]any{"path": path, "sha256": hex.EncodeToString(h.Sum(nil)), "error": fmt.Sprint(err)})
}

func Window(ctx context.Context, name string, points []time.Duration) func() {
	if !Enabled() {
		return func() {}
	}
	safety := make(chan os.Signal, 1)
	signal.Notify(safety, syscall.SIGUSR2)
	start := time.Now()
	var initialUsage syscall.Rusage
	initialErr := syscall.Getrusage(syscall.RUSAGE_SELF, &initialUsage)
	Record(name+".window_enter", map[string]any{"usage": initialUsage, "error": fmt.Sprint(initialErr)})
	stop, joined := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(joined)
		for _, at := range points {
			timer := time.NewTimer(at - time.Since(start))
			select {
			case <-stop:
				timer.Stop()
				return
			case <-ctx.Done():
				timer.Stop()
				Record(name+".context_cancelled", fmt.Sprint(context.Cause(ctx)))
				return
			case <-timer.C:
				if ctx.Err() != nil {
					Record(name+".sample_not_taken", fmt.Sprint(context.Cause(ctx)))
					return
				}
				Record(name+".sample_point", map[string]any{"scheduled_ns": at.Nanoseconds(), "actual_elapsed_ns": time.Since(start).Nanoseconds(), "cause": fmt.Sprint(context.Cause(ctx))})
				Sample(name + "." + strconv.FormatInt(at.Milliseconds(), 10) + "ms")
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			close(stop)
			<-joined
			signal.Stop(safety)
			var finalUsage syscall.Rusage
			finalErr := syscall.Getrusage(syscall.RUSAGE_SELF, &finalUsage)
			Record(name+".sampler_joined", map[string]any{"elapsed_ns": time.Since(start).Nanoseconds(), "usage": finalUsage, "error": fmt.Sprint(finalErr)})
		})
	}
}

func Request(ctx context.Context, method string) func(error) {
	row := os.Getenv("TEST_2353_ROW")
	if !Enabled() || (row == "S01" && method != "event.publish") || (row == "S02" && method != "run.fork") || row == "S03" || row == "S04" || !firstRequest.CompareAndSwap(false, true) {
		return func(error) {}
	}
	finish := Phase(ctx, "server."+method)
	joined := Window(ctx, "server."+method, []time.Duration{time.Second, 3 * time.Second, 4500 * time.Millisecond})
	return func(err error) {
		Record("server."+method+".raw_result", map[string]any{"error": fmt.Sprint(err), "caller_cause": fmt.Sprint(context.Cause(ctx))})
		finish()
		joined()
	}
}
