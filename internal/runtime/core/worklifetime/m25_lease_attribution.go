package worklifetime

import (
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

func (g *gate) reportM25LeaseTraces(transientOnly bool) {
	g.mu.Lock()
	var out strings.Builder
	now := time.Now()
	for index, trace := range g.m25Traces {
		if transientOnly && !trace.transient {
			continue
		}
		end := trace.finished
		if end.IsZero() {
			end = now
		}
		fmt.Fprintf(&out, "M25-LEASE gate=%p id=%d acquired=%s settled=%t elapsed=%s public=%t acquire=%s settle=%s\n",
			g, index+1, trace.started.UTC().Format(time.RFC3339Nano), !trace.finished.IsZero(), end.Sub(trace.started), trace.public, trace.acquire, trace.settle)
	}
	g.mu.Unlock()
	fmt.Fprint(os.Stderr, out.String())
}
