package transactiontest

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"time"
)

// Disposable attribution instrumentation, never a production permission cache.
type GuardDiagnosticCounts struct {
	Calls                              uint64
	Wall, RunSQL, StandingSQL, Maximum time.Duration
	RunQueries, StandingQueries        uint64
}

type guardDiagnosticAttempt struct {
	site    string
	phase   string
	started time.Time
	counts  GuardDiagnosticCounts
}

type guardDiagnosticKey struct{}

var guardDiagnostics = struct {
	sync.Mutex
	phase  string
	counts map[string]GuardDiagnosticCounts
}{counts: make(map[string]GuardDiagnosticCounts)}

func BeginGuardDiagnostic(ctx context.Context) (context.Context, func()) {
	var pcs [8]uintptr
	n := runtime.Callers(3, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	var sites []string
	for len(sites) < 3 {
		frame, more := frames.Next()
		if !strings.HasSuffix(frame.Function, ".normalDispatchParked") {
			sites = append(sites, strings.TrimPrefix(frame.Function, "github.com/division-sh/swarm/internal/store/internal/backend/"))
		}
		if !more {
			break
		}
	}
	guardDiagnostics.Lock()
	phase := guardDiagnostics.phase
	guardDiagnostics.Unlock()
	a := &guardDiagnosticAttempt{site: strings.Join(sites, " <- "), phase: phase, started: time.Now()}
	return context.WithValue(ctx, guardDiagnosticKey{}, a), func() {
		a.counts.Calls = 1
		a.counts.Wall = time.Since(a.started)
		guardDiagnostics.Lock()
		defer guardDiagnostics.Unlock()
		key := a.phase + " | " + a.site
		c := guardDiagnostics.counts[key]
		c.Calls++
		c.Wall += a.counts.Wall
		c.RunSQL += a.counts.RunSQL
		c.StandingSQL += a.counts.StandingSQL
		c.RunQueries += a.counts.RunQueries
		c.StandingQueries += a.counts.StandingQueries
		if a.counts.Wall > c.Maximum {
			c.Maximum = a.counts.Wall
		}
		guardDiagnostics.counts[key] = c
	}
}

func RecordGuardDiagnosticSQL(ctx context.Context, standing bool, duration time.Duration) {
	a, _ := ctx.Value(guardDiagnosticKey{}).(*guardDiagnosticAttempt)
	if a == nil {
		return
	}
	if standing {
		a.counts.StandingSQL += duration
		a.counts.StandingQueries++
	} else {
		a.counts.RunSQL += duration
		a.counts.RunQueries++
	}
}

func GuardDiagnosticSnapshot() map[string]GuardDiagnosticCounts {
	guardDiagnostics.Lock()
	defer guardDiagnostics.Unlock()
	result := make(map[string]GuardDiagnosticCounts, len(guardDiagnostics.counts))
	for site, counts := range guardDiagnostics.counts {
		if strings.HasPrefix(site, guardDiagnostics.phase+" | ") {
			result[site] = counts
		}
	}
	return result
}

func SetGuardDiagnosticPhase(phase string) func() {
	guardDiagnostics.Lock()
	previous := guardDiagnostics.phase
	guardDiagnostics.phase = phase
	guardDiagnostics.Unlock()
	return func() {
		guardDiagnostics.Lock()
		guardDiagnostics.phase = previous
		guardDiagnostics.Unlock()
	}
}
