package conformance

import (
	"encoding/json"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/storetest"
)

// This disposable branch measures the unchanged hosted unit, never permissions.
func beginHostedFanOutPhase(t *testing.T) func() {
	t.Helper()
	restore := storetest.SetGuardDiagnosticPhase(t.Name())
	started := time.Now()
	logHostedFanOutEnvironment(t, "begin")
	return func() {
		logHostedFanOutEnvironment(t, "end")
		t.Logf("hosted attribution phase=%s elapsed=%s", t.Name(), time.Since(started))
		restore()
	}
}

func logHostedFanOutEnvironment(t *testing.T, point string) {
	t.Helper()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	files := make(map[string]string)
	for _, path := range []string{
		"/sys/fs/cgroup/cpu.max", "/sys/fs/cgroup/cpu.stat",
		"/sys/fs/cgroup/cpuset.cpus.effective", "/sys/fs/cgroup/memory.max",
		"/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/cpu/cpu.cfs_quota_us",
		"/sys/fs/cgroup/cpu/cpu.cfs_period_us", "/proc/self/cgroup",
		"/proc/self/stat", "/proc/loadavg",
	} {
		if raw, err := os.ReadFile(path); err == nil {
			files[path] = strings.TrimSpace(string(raw))
		}
	}
	if raw, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.HasPrefix(line, "Cpus_allowed") || strings.HasPrefix(line, "VmRSS:") || strings.HasPrefix(line, "Threads:") {
				files["/proc/self/status"] += line + "\n"
			}
		}
	}
	encoded, err := json.Marshal(map[string]any{
		"point": point, "phase": t.Name(), "utc": time.Now().UTC(),
		"num_cpu": runtime.NumCPU(), "gomaxprocs": runtime.GOMAXPROCS(0),
		"goroutines": runtime.NumGoroutine(), "heap_alloc": mem.HeapAlloc,
		"heap_sys": mem.HeapSys, "total_alloc": mem.TotalAlloc,
		"gc_count": mem.NumGC, "gc_pause_ns": mem.PauseTotalNs,
		"gc_cpu_fraction": mem.GCCPUFraction, "host": files,
	})
	if err != nil {
		t.Errorf("encode hosted environment: %v", err)
		return
	}
	t.Logf("hosted attribution environment %s", encoded)
}

func logHostedFanOutTransactions(t *testing.T, collector *storetest.TransactionCollector, phase string) {
	t.Helper()
	snapshot := collector.Snapshot()
	encoded, err := json.Marshal(map[string]any{
		"phase": phase, "test": t.Name(), "guard": storetest.GuardDiagnosticSnapshot(),
		"transactions": snapshot.ByOperation, "total": snapshot.Total, "active": snapshot.Active,
	})
	if err != nil {
		t.Errorf("encode attribution: %v", err)
		return
	}
	t.Logf("guard diagnostic metrics %s", encoded)
}
