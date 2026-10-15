//go:build linux || darwin

package serveapp

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/sessionprovider"
)

type constructionDiscriminatorRead struct {
	channelonboarding.Store
	entered, release, released, publish chan struct{}
	armed                               atomic.Bool
}

func (s *constructionDiscriminatorRead) GetChannelOnboarding(ctx context.Context, id string) (channelonboarding.Operation, error) {
	if owned, _ := ctx.Value(sessionConstructionCutKey{}).(bool); owned && s.armed.CompareAndSwap(true, false) {
		close(s.entered)
		<-s.release
		close(s.released)
		<-s.publish
	}
	return s.Store.GetChannelOnboarding(ctx, id)
}

type constructionWaitObservation struct {
	context.Context
	attempt *serveSessionBootstrapAttempt
	entered chan struct{}
	once    sync.Once
}

func (c *constructionWaitObservation) Done() <-chan struct{} {
	// Observe only the owner's construction-select evaluation, not a SQL
	// cancellation check. Context behavior and production code are unchanged.
	var pcs [2]uintptr
	n := runtime.Callers(2, pcs[:])
	frame, _ := runtime.CallersFrames(pcs[:n]).Next()
	if frame.Function == "github.com/division-sh/swarm/internal/serveapp.(*serveSessionBootstrap).AwaitSessionBootstrap" {
		select {
		case <-c.attempt.done:
		default:
			c.once.Do(func() { close(c.entered) })
		}
	}
	return c.Context.Done()
}

func constructionDiagnosticErrorClass(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, channelonboarding.ErrSessionPending):
		return "pending"
	default:
		return "other_refusal"
	}
}

func constructionDiagnosticSignalReady(signal <-chan struct{}) bool {
	select {
	case <-signal:
		return true
	default:
		return false
	}
}

func awaitConstructionDiagnosticSignal(t *testing.T, ctx context.Context, signal <-chan struct{}) {
	t.Helper()
	wait, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	select {
	case <-signal:
	case <-wait.Done():
		t.Fatal("controlled construction diagnostic did not reach its existing bound", constructionDiagnosticErrorClass(context.Cause(wait)))
	}
}

func logConstructionDiagnostic(t *testing.T, f *serveBootstrapTestFixture, attempt *serveSessionBootstrapAttempt, phase string, first error,
	originalWait, reuseWait *constructionWaitObservation, closeRan, closeJoined bool,
) {
	t.Helper()
	f.adapter.mu.Lock()
	published := attempt.connection != nil
	f.adapter.mu.Unlock()
	attempt.mu.Lock()
	connecting, connected, cached := attempt.connecting, attempt.connected, constructionDiagnosticErrorClass(attempt.connectErr)
	attempt.mu.Unlock()
	t.Logf("phase=%s first_pending=%t first_direct_canceled=%t original_wait=%t reuse_wait=%t done=%t published=%t connecting=%t connected=%t cache_class=%s close_ran=%t close_joined=%t one_selection=%t",
		phase, errors.Is(first, channelonboarding.ErrSessionPending), errors.Is(first, context.Canceled),
		constructionDiagnosticSignalReady(originalWait.entered), constructionDiagnosticSignalReady(reuseWait.entered),
		constructionDiagnosticSignalReady(attempt.done), published, connecting, connected, cached, closeRan, closeJoined, f.selections.Load() == 1)
}

// Diagnostic only: the original cancellation test and all production owners
// remain untouched. Cross the two independently possible fixture interleavings.
func TestServeConstructionCancellationDiscriminatorBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []string{"bootstrap", "resume"} {
			for _, cancellation := range []string{"inside_wait", "before_wait"} {
				for _, publication := range []string{"after_probe", "before_probe"} {
					t.Run(backend+"/"+mode+"/"+cancellation+"/"+publication, func(t *testing.T) {
						runServeConstructionDiscriminator(t, backend, mode, cancellation == "inside_wait", publication == "before_probe")
					})
				}
			}
		}
	}
}

func runServeConstructionDiscriminator(t *testing.T, backend, mode string, insideWait, publishBeforeProbe bool) {
	t.Helper()
	f := newServeBootstrapTestFixture(t, backend)
	// Any final cache-use dial stays on the existing real SDK's local Noise
	// peer; this diagnostic never contacts WhatsApp or produces pairing material.
	serveBootstrapWireFixture(t, "healthy", f.owner)
	cut := &constructionDiscriminatorRead{Store: f.adapter.store, entered: make(chan struct{}), release: make(chan struct{}), released: make(chan struct{}), publish: make(chan struct{})}
	var releaseOnce, publishOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(cut.release) }) }
	publish := func() { publishOnce.Do(func() { close(cut.publish) }) }
	t.Cleanup(func() { release(); publish() })
	selectRuntime := f.adapter.selectRuntime
	f.adapter.selectRuntime = func(ctx context.Context, candidate channelonboarding.Candidate) (context.Context, *sessionprovider.RuntimeIncomingOptions, processbinding.Binding, func() error, error) {
		owned, incoming, binding, release, err := selectRuntime(ctx, candidate)
		if err != nil {
			return nil, nil, processbinding.Binding{}, nil, err
		}
		use, err := f.owner.Begin(ctx)
		if err != nil {
			return nil, nil, processbinding.Binding{}, nil, errors.Join(err, release())
		}
		cut.armed.Store(true)
		return context.WithValue(owned, sessionConstructionCutKey{}, true), incoming, binding,
			func() error { return errors.Join(release(), use.Done()) }, nil
	}
	f.adapter.store = cut
	caller, cancel := context.WithCancel(f.ctx)
	defer cancel()
	start := f.adapter.BootstrapSession
	if mode == "resume" {
		start = f.adapter.ResumeSession
	}
	first := start(caller, f.op, f.candidate)
	if !errors.Is(first, channelonboarding.ErrSessionPending) {
		t.Fatal("controlled initial constructor did not return pending", constructionDiagnosticErrorClass(first))
	}
	awaitConstructionDiagnosticSignal(t, f.ctx, cut.entered)
	f.adapter.mu.Lock()
	attempt := f.adapter.connections[f.op.OperationID]
	f.adapter.mu.Unlock()
	originalWait := &constructionWaitObservation{Context: caller, attempt: attempt, entered: make(chan struct{})}
	reuseWait := &constructionWaitObservation{Context: f.ctx, attempt: attempt, entered: make(chan struct{})}
	logConstructionDiagnostic(t, f, attempt, "before_cancellation", first, originalWait, reuseWait, false, false)
	var originalErr error
	if insideWait {
		finished := make(chan error, 1)
		go func() { finished <- f.adapter.AwaitSessionBootstrap(originalWait, f.op) }()
		awaitConstructionDiagnosticSignal(t, f.ctx, originalWait.entered)
		cancel()
		select {
		case originalErr = <-finished:
		case <-time.After(time.Second):
			t.Fatal("controlled original caller did not return within the original prompt bound")
		}
	} else {
		// Order cancellation in the existing helper's gap between Pending and
		// Await admission, without altering either production entry point.
		cancel()
		originalErr = f.adapter.AwaitSessionBootstrap(originalWait, f.op)
	}
	if !errors.Is(originalErr, context.Canceled) || f.owner.ActiveCount() == 0 {
		t.Fatal("controlled original result lost cancellation or construction ownership", constructionDiagnosticErrorClass(originalErr))
	}
	logConstructionDiagnostic(t, f, attempt, "after_original_result", first, originalWait, reuseWait, false, false)
	t.Logf("phase=original_result error_class=%s", constructionDiagnosticErrorClass(originalErr))
	if constructionDiagnosticSignalReady(originalWait.entered) != insideWait {
		t.Fatal("diagnostic did not distinguish the original construction-wait branch")
	}
	reuse, stop := context.WithTimeout(f.ctx, 50*time.Millisecond)
	defer stop()
	reuseWait.Context = reuse
	reuseFirst := f.adapter.BootstrapSession(reuseWait, f.op, f.candidate)
	reuseErr := reuseFirst
	if errors.Is(reuseFirst, channelonboarding.ErrSessionPending) {
		if !insideWait {
			// Use the same real 50ms deadline, not a sleep or an earlier cancellation.
			<-reuse.Done()
		}
		reuseErr = f.adapter.AwaitSessionBootstrap(reuseWait, f.op)
	}
	if !errors.Is(reuseErr, context.DeadlineExceeded) || f.selections.Load() != 1 {
		t.Fatal("50ms pending reuse lost its original no-successor negative", constructionDiagnosticErrorClass(reuseErr))
	}
	t.Logf("phase=reuse_result first_class=%s error_class=%s", constructionDiagnosticErrorClass(reuseFirst), constructionDiagnosticErrorClass(reuseErr))
	stop()
	logConstructionDiagnostic(t, f, attempt, "before_release", first, originalWait, reuseWait, false, false)
	release()
	awaitConstructionDiagnosticSignal(t, f.ctx, cut.released)
	publish()
	if err := f.owner.WaitForQuiescence(f.ctx); err != nil {
		t.Fatal("original construction did not publish before cleanup", constructionDiagnosticErrorClass(err))
	}
	select {
	case <-attempt.done:
	case <-f.ctx.Done():
		t.Fatal("exact original attempt did not settle", constructionDiagnosticErrorClass(context.Cause(f.ctx)))
	}
	f.adapter.mu.Lock()
	retained := f.adapter.connections[f.op.OperationID]
	f.adapter.mu.Unlock()
	if retained != attempt {
		t.Fatal("cleanup replaced the original attempt")
	}
	connection := f.adapter.connection(f.op.OperationID)
	closeRan, closeJoined := connection != nil, false
	if closeRan {
		closeErr := connection.Close(context.Background())
		closeJoined = closeErr == nil
		if closeErr != nil {
			t.Fatal("controlled original close failed", constructionDiagnosticErrorClass(closeErr))
		}
	}
	logConstructionDiagnostic(t, f, attempt, "fixture_probe", first, originalWait, reuseWait, closeRan, closeJoined)
	if !closeRan || !closeJoined {
		t.Fatal("controlled original connection did not join before cache reuse")
	}
	if err := f.owner.WaitForQuiescence(f.ctx); err != nil {
		t.Fatal("original construction did not join", constructionDiagnosticErrorClass(err))
	}
	logConstructionDiagnostic(t, f, attempt, "constructor_complete", first, originalWait, reuseWait, closeRan, closeJoined)
	finalErr := awaitServeSessionBootstrapFixture(f.ctx, f.adapter, f.op, f.candidate, false)
	t.Logf("phase=final_original_cache_use error_class=%s nil_result=%t close_ran=%t close_joined=%t one_selection=%t",
		constructionDiagnosticErrorClass(finalErr), finalErr == nil, closeRan, closeJoined, f.selections.Load() == 1)
	if finalErr == nil || errors.Is(finalErr, channelonboarding.ErrSessionPending) || f.selections.Load() != 1 {
		t.Fatal("canceled original construction did not retain a settled refusal")
	}
	if connection := f.adapter.connection(f.op.OperationID); connection != nil {
		if err := connection.Close(context.Background()); err != nil {
			t.Fatal("post-observation diagnostic cleanup did not join", constructionDiagnosticErrorClass(err))
		}
	}
}
