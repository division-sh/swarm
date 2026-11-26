package forkcancelcapture

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"time"
)

// Link records the actual context returned by an existing owner. It does not
// substitute a context, register a cancel callback, or observe with a goroutine.
func (p *Capture) Link(label string, child, parent context.Context) {
	recordContext(p, label, child, parent)
}

func recordContext(p *Capture, label string, child, parent context.Context) {
	if p == nil || child == nil {
		return
	}
	pc, file, line, _ := runtime.Caller(2)
	if fn := runtime.FuncForPC(pc); fn != nil {
		label = fmt.Sprintf("%s at %s %s:%d", label, fn.Name(), filepath.Base(file), line)
	}
	if !p.linkLocked(label, child, parent) {
		return
	}
	// The issuing parent's values may predate arming. Bind only a new child
	// lacking the marker; the original context and its values stay intact.
	if From(child) == nil {
		p.Bind(child)
	}
}

func (p *Capture) linkLocked(label string, child, parent context.Context) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	id := p.id(child)
	if _, known := p.origins[id]; id > 0 && !known && p.used < limit {
		p.origins[id] = p.id(parent)
		// In particular a custom context's Err may perform owner work.
		// Do not sample it merely because a value/authority layer was added.
		p.noteLocked(label, child, parent, nil, nil, false)
	} else if !known {
		p.incomplete = true
	}
	return true
}

func (p *Capture) checkReadOrigin(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	for id, depth := p.id(ctx), 0; id != 0; depth++ {
		parent, known := p.origins[id]
		if !known || depth == limit {
			if depth == 0 {
				p.incomplete = true
			}
			p.noteLocked(fmt.Sprintf("generation read lineage reaches unrecorded origin #%d", id), ctx, nil, nil, nil, false)
			return
		}
		id = parent
	}
}

func (p *Capture) WrapPlain(label string, ctx context.Context, cancel context.CancelFunc) context.CancelFunc {
	if p == nil {
		return cancel
	}
	return func() {
		p.Note(label+" invoke", ctx, nil, ctx, nil, true)
		cancel()
		p.Note(label+" return", ctx, nil, ctx, nil, false)
	}
}

// RecordValue observes an already-created context without owning its key or value.
func RecordValue(parent, child context.Context) context.Context {
	recordContext(Resolve(parent, parent), "context.WithValue", child, parent)
	return child
}

func WithoutCancel(parent context.Context) context.Context {
	child := context.WithoutCancel(parent)
	recordContext(Resolve(parent, parent), "context.WithoutCancel (values only)", child, parent)
	return child
}

func WithCancel(parent context.Context) (context.Context, context.CancelFunc) {
	child, cancel := context.WithCancel(parent)
	p := Resolve(parent, parent)
	recordContext(p, "context.WithCancel", child, parent)
	return child, p.WrapPlain("context.WithCancel", child, cancel)
}

func WithCancelCause(parent context.Context) (context.Context, context.CancelCauseFunc) {
	child, cancel := context.WithCancelCause(parent)
	p := Resolve(parent, parent)
	recordContext(p, "context.WithCancelCause", child, parent)
	return child, p.Wrap("context.WithCancelCause", child, cancel)
}

func WithDeadline(parent context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	child, cancel := context.WithDeadline(parent, deadline)
	p := Resolve(parent, parent)
	recordContext(p, "context.WithDeadline", child, parent)
	noteDeadline(p, child, parent)
	return child, p.WrapPlain("context.WithDeadline", child, cancel)
}

func WithTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	child, cancel := context.WithTimeout(parent, timeout)
	p := Resolve(parent, parent)
	recordContext(p, "context.WithTimeout", child, parent)
	noteDeadline(p, child, parent)
	return child, p.WrapPlain("context.WithTimeout", child, cancel)
}

func noteDeadline(p *Capture, child, parent context.Context) {
	if p == nil {
		return
	}
	if deadline, ok := child.Deadline(); ok {
		p.Note("effective deadline offset "+deadline.Sub(p.start).String(), child, parent, child, nil, false)
	}
}

func (p *Capture) MirroredDeadline(label string, child, source context.Context, deadline time.Time) {
	if p != nil {
		p.Note(label+" offset "+deadline.Sub(p.start).String(), child, source, nil, nil, false)
	}
}
