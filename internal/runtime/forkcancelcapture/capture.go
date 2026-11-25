// Package forkcancelcapture is a temporary, non-merging fork diagnostic.
package forkcancelcapture

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"
)

const limit = 128

type contextKey struct{}

var owners sync.Map

type Capture struct {
	start              time.Time
	mu                 sync.Mutex
	closed, incomplete bool
	used               int
	bound              map[any]struct{}
	ids                map[any]int
	rows               [limit]record
}

type record struct {
	label                    string
	at                       time.Duration
	subject, parent, context int
	state, cause, err        string
	stack                    [8]uintptr
	frames                   int
}

func Start(ctx context.Context) (context.Context, *Capture) {
	p := &Capture{start: time.Now(), ids: make(map[any]int), bound: make(map[any]struct{})}
	if deadline, ok := ctx.Deadline(); ok {
		p.Note("request deadline offset "+deadline.Sub(p.start).String(), ctx, nil, ctx, nil, false)
	} else {
		p.Note("request without deadline", ctx, nil, ctx, nil, false)
	}
	marked := context.WithValue(ctx, contextKey{}, p)
	p.Note("fixture trace context from request", marked, ctx, marked, nil, false)
	return marked, p
}

func From(ctx context.Context) *Capture {
	if ctx == nil {
		return nil
	}
	p, _ := ctx.Value(contextKey{}).(*Capture)
	return p
}

func Owner(key any) *Capture {
	if key == nil || !reflect.ValueOf(key).Comparable() {
		return nil
	}
	p, _ := owners.Load(key)
	trace, _ := p.(*Capture)
	return trace
}

func Resolve(ctx context.Context, key any) *Capture {
	if p := From(ctx); p != nil {
		return p
	}
	return Owner(key)
}

// Bind correlates diagnostics only. It never installs or changes an owner callback.
func (p *Capture) Bind(key any) func() {
	if p == nil || key == nil || !reflect.ValueOf(key).Comparable() {
		return func() {}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || len(p.bound) == limit {
		p.incomplete = true
		return func() {}
	}
	current, exists := owners.LoadOrStore(key, p)
	if exists {
		if current != p {
			p.incomplete = true
		}
		return func() {}
	}
	p.bound[key] = struct{}{}
	return func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		owners.CompareAndDelete(key, p)
		delete(p.bound, key)
	}
}

func (p *Capture) Close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	for key := range p.bound {
		owners.CompareAndDelete(key, p)
	}
}

func (p *Capture) Missing(label string, key any) {
	if p != nil {
		p.Note(label, key, nil, nil, nil, false)
		p.mu.Lock()
		p.incomplete = true
		p.mu.Unlock()
	}
}

func (p *Capture) id(value any) int {
	if value == nil {
		return 0
	}
	if !reflect.ValueOf(value).Comparable() {
		p.incomplete = true
		return -1
	}
	if id := p.ids[value]; id != 0 {
		return id
	}
	if len(p.ids) == 2*limit {
		p.incomplete = true
		return -1
	}
	id := len(p.ids) + 1
	p.ids[value] = id
	return id
}

func (p *Capture) Note(label string, subject, parent any, ctx context.Context, err error, stack bool) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	if p.used == limit {
		p.incomplete = true
		return
	}
	r := &p.rows[p.used]
	p.used++
	r.label, r.at = label, time.Since(p.start)
	r.subject, r.parent, r.context = p.id(subject), p.id(parent), p.id(ctx)
	if ctx != nil {
		r.state, r.cause = summarize(ctx.Err()), summarize(context.Cause(ctx))
	}
	r.err = summarize(err)
	if stack {
		r.frames = runtime.Callers(2, r.stack[:])
	}
}

func (p *Capture) Wrap(label string, ctx context.Context, cancel context.CancelCauseFunc) context.CancelCauseFunc {
	if p == nil {
		return cancel
	}
	return func(cause error) {
		p.Cancel(label, ctx, ctx, cancel, cause)
	}
}

func (p *Capture) Cancel(label string, subject any, ctx context.Context, cancel context.CancelCauseFunc, cause error) {
	p.Note(label+" invoke", subject, nil, ctx, cause, true)
	cancel(cause)
	p.Note(label+" return", subject, nil, ctx, cause, false)
}

type ReadStart struct {
	at           time.Duration
	state, cause string
}

func (p *Capture) ReadStart(ctx context.Context) ReadStart {
	if p == nil {
		return ReadStart{}
	}
	return ReadStart{at: time.Since(p.start), state: summarize(ctx.Err()), cause: summarize(context.Cause(ctx))}
}

func (p *Capture) ReadFailure(label string, before ReadStart, subject any, ctx context.Context, err error) {
	if p != nil {
		p.Note(label+" entry_at="+before.at.String()+" entry_state="+before.state+" entry_cause="+before.cause,
			subject, nil, ctx, err, true)
	}
}

// Keep the chain shape and known sentinel/SQLSTATE tags, not arbitrary error text.
func summarize(err error) string {
	if err == nil {
		return "-"
	}
	var b strings.Builder
	nodes := 0
	var visit func(error)
	visit = func(e error) {
		if e == nil || nodes >= 10 {
			return
		}
		nodes++
		if nodes > 1 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%T", e)
		if reflect.ValueOf(e).Comparable() {
			switch e {
			case context.Canceled:
				b.WriteString(":canceled")
			case context.DeadlineExceeded:
				b.WriteString(":deadline")
			case driver.ErrBadConn:
				b.WriteString(":bad_connection")
			case io.EOF:
				b.WriteString(":eof")
			}
		}
		if sqlErr, ok := e.(interface{ SQLState() string }); ok {
			code := sqlErr.SQLState()
			if len(code) == 5 && strings.Trim(code, "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") == "" {
				b.WriteString(":" + code)
			}
		}
		if joined, ok := e.(interface{ Unwrap() []error }); ok {
			for _, next := range joined.Unwrap() {
				visit(next)
			}
		} else {
			visit(errors.Unwrap(e))
		}
	}
	visit(err)
	if nodes == 10 {
		b.WriteString(",chain-may-be-truncated")
	}
	return b.String()
}

// Output is read at the existing failure assertion, not at fixture teardown.
func (p *Capture) Output() string {
	if p == nil {
		return ""
	}
	p.mu.Lock()
	records := append([]record(nil), p.rows[:p.used]...)
	incomplete := p.incomplete
	p.mu.Unlock()
	var b strings.Builder
	fmt.Fprintf(&b, "fork cancellation capture records=%d capped_or_incomplete=%t; order is observed, missing or racing parent means unattributed\n", len(records), incomplete)
	for i, r := range records {
		fmt.Fprintf(&b, "%03d +%s %s subject=%d parent=%d context=%d state=%s cause=%s error=%s\n",
			i+1, r.at, r.label, r.subject, r.parent, r.context, r.state, r.cause, r.err)
		if r.frames == 0 {
			continue
		}
		frames := runtime.CallersFrames(r.stack[:r.frames])
		for {
			frame, more := frames.Next()
			fmt.Fprintf(&b, "  %s %s:%d\n", frame.Function, filepath.Base(frame.File), frame.Line)
			if !more {
				break
			}
		}
	}
	return b.String()
}
