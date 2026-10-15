package worklifetime

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"
)

// Diagnostic-only checkpoint: retain acquisition and settlement evidence without
// changing the original M25 deadline, workload or work-counter decisions.
type m25LeaseTrace struct {
	started, finished time.Time
	transient, public bool
	acquire, settle   string
}

func m25LeaseCaller() string {
	var pcs [24]uintptr
	n := runtime.Callers(3, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	var out strings.Builder
	for {
		frame, more := frames.Next()
		fmt.Fprintf(&out, "%s:%d;", frame.Function, frame.Line)
		if !more {
			return out.String()
		}
	}
}

func newM25LeaseTrace(transient, public bool) *m25LeaseTrace {
	return &m25LeaseTrace{started: time.Now(), transient: transient, public: public, acquire: m25LeaseCaller()}
}

func (t *m25LeaseTrace) finish() {
	t.finished, t.settle = time.Now(), m25LeaseCaller()
}

func (g *gate) reportM25LeaseTraces(ctx context.Context, transientOnly bool) {
	g.mu.Lock()
	var out strings.Builder
	now := time.Now()
	active := g.active
	if transientOnly {
		active = g.activeTransient
	}
	fmt.Fprintf(&out, "M25-WAIT gate=%p transient_only=%t active=%d observed=%s context_error=%v\n", g, transientOnly, active, now.UTC().Format(time.RFC3339Nano), ctx.Err())
	for index, trace := range g.m25Traces {
		if transientOnly && !trace.transient {
			continue
		}
		end := trace.finished
		if end.IsZero() {
			end = now
		}
		fmt.Fprintf(&out, "M25-LEASE gate=%p id=%d acquired=%s settled=%t finished=%s observed=%s elapsed=%s public=%t acquire=%s settle=%s\n",
			g, index+1, trace.started.UTC().Format(time.RFC3339Nano), !trace.finished.IsZero(), trace.finished.UTC().Format(time.RFC3339Nano), now.UTC().Format(time.RFC3339Nano), end.Sub(trace.started), trace.public, trace.acquire, trace.settle)
	}
	g.mu.Unlock()
	fmt.Fprint(os.Stderr, out.String())
	if ctx.Err() != nil && active != 0 {
		stack := make([]byte, 1<<20)
		n := runtime.Stack(stack, true)
		fmt.Fprintf(os.Stderr, "M25-FAILED-WAIT-STACK gate=%p bytes=%d truncated=%t\n%s", g, n, n == len(stack), stack[:n])
	}
}
