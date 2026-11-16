package runforkexecution

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"
)

// ForkCancelProbe is temporary, invocation-scoped test instrumentation.
type ForkCancelProbe struct {
	mu         sync.Mutex
	operations []forkCancelOperation
	children   []forkCancelChild
	records    []forkCancelRecord
}

type forkCancelProbeKey struct{}

type forkCancelOperation struct {
	id        uint64
	request   context.Context
	process   context.Context
	preparing context.Context
	owned     context.Context
}

type forkCancelChild struct {
	operationID uint64
	runID       string
	selected    context.Context
	child       context.Context
}

type forkCancelRecord struct {
	at       time.Time
	owner    string
	supplied error
	observed error
	stack    []uintptr
}

func WithForkCancelProbe(ctx context.Context, probe *ForkCancelProbe) context.Context {
	return context.WithValue(ctx, forkCancelProbeKey{}, probe)
}

func forkCancelProbeFromContext(ctx context.Context) *ForkCancelProbe {
	probe, _ := ctx.Value(forkCancelProbeKey{}).(*ForkCancelProbe)
	return probe
}

func (p *ForkCancelProbe) bindOperation(request, process, preparing, owned context.Context) uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	id := uint64(len(p.operations) + 1)
	if len(p.operations) < 4 {
		p.operations = append(p.operations, forkCancelOperation{id: id, request: request, process: process, preparing: preparing, owned: owned})
	}
	return id
}

func (p *ForkCancelProbe) bindChild(operationID uint64, runID string, selected, child context.Context) {
	p.mu.Lock()
	if len(p.children) < 4 {
		p.children = append(p.children, forkCancelChild{operationID: operationID, runID: runID, selected: selected, child: child})
	}
	p.mu.Unlock()
}

func (p *ForkCancelProbe) wrap(owner string, ctx context.Context, cancel context.CancelCauseFunc) context.CancelCauseFunc {
	return func(cause error) {
		pcs := make([]uintptr, 16)
		pcs = pcs[:runtime.Callers(2, pcs)]
		record := forkCancelRecord{at: time.Now().UTC(), owner: owner, supplied: cause, stack: pcs}
		p.mu.Lock()
		index := -1
		if len(p.records) < 12 {
			index = len(p.records)
			p.records = append(p.records, record)
		}
		p.mu.Unlock()
		cancel(cause)
		if index >= 0 {
			p.mu.Lock()
			p.records[index].observed = context.Cause(ctx)
			p.mu.Unlock()
		}
	}
}

// Snapshot is called only from the executor's existing failure branch.
func (p *ForkCancelProbe) Snapshot() string {
	p.mu.Lock()
	operations := append([]forkCancelOperation(nil), p.operations...)
	children := append([]forkCancelChild(nil), p.children...)
	records := append([]forkCancelRecord(nil), p.records...)
	p.mu.Unlock()
	var b strings.Builder
	for _, op := range operations {
		fmt.Fprintf(&b, "operation=%d request=(%v,%v) process=(%v,%v) preparing=(%v,%v) owned=(%v,%v)\n",
			op.id, contextError(op.request), contextCause(op.request), contextError(op.process), contextCause(op.process),
			contextError(op.preparing), contextCause(op.preparing), contextError(op.owned), contextCause(op.owned))
	}
	for _, child := range children {
		fmt.Fprintf(&b, "child operation=%d run=%q selected=(%v,%v) child=(%v,%v)\n",
			child.operationID, child.runID, contextError(child.selected), contextCause(child.selected),
			contextError(child.child), contextCause(child.child))
	}
	fmt.Fprintf(&b, "cancel wrapper invocations=%d\n", len(records))
	for _, record := range records {
		fmt.Fprintf(&b, "%s owner=%s supplied=%v observed-after=%v\n",
			record.at.Format(time.RFC3339Nano), record.owner, record.supplied, record.observed)
		frames := runtime.CallersFrames(record.stack)
		for i := 0; i < 8; i++ {
			frame, more := frames.Next()
			fmt.Fprintf(&b, "  %s %s:%d\n", frame.Function, frame.File, frame.Line)
			if !more {
				break
			}
		}
	}
	return b.String()
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func contextCause(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return context.Cause(ctx)
}
