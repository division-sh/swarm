package serveapp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
)

func TestInboundStopRetainsOriginalDeadlineThroughOuterFinalization(t *testing.T) {
	for _, expired := range []bool{false, true} {
		name, grace := "live", 5*time.Second
		if expired {
			name, grace = "expired", time.Nanosecond
		}
		t.Run(name, func(t *testing.T) {
			s := newProcessLifecycleSupervisor(nil, nil)
			s.runtimeContexts = &runtime.RuntimeContextManager{}
			s.shutdownOptions = runtime.ShutdownOptions{Grace: grace}
			ctx := s.prepareInboundStop()
			defer s.inboundStopCancel()
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Fatal("missing original stop deadline")
			}
			if expired {
				<-ctx.Done()
			}
			stop := s.beginInboundStop()
			if err := s.runtimeContexts.CloseInboundRequests(t.Context()); err != nil {
				t.Fatal(err)
			}
			result := stop.Finish()
			if expired {
				if !errors.Is(result, context.DeadlineExceeded) || ctx.Err() != context.DeadlineExceeded {
					t.Fatalf("shared expiry hidden or replaced: stop=%v ctx=%v", result, ctx.Err())
				}
			} else if result != nil || ctx.Err() != nil {
				t.Fatalf("outer finalization received premature cancellation: stop=%v ctx=%v", result, ctx.Err())
			}
			again := s.prepareInboundStop()
			if next, _ := again.Deadline(); again != ctx || next != deadline {
				t.Fatal("later stop phase renewed or replaced the original deadline")
			}
		})
	}
}

func TestInboundStopJoinsOwnedCurrentRuntimeBeforeRegistration(t *testing.T) {
	definition := serveExecutionContextForTest(t)
	rt := definition.Runtime
	subscription, err := rt.Bus.SubscribeInternal(t.Context(), "partial-startup")
	if err != nil {
		t.Fatal(err)
	}
	completed := make(chan error, 1)
	go func() {
		<-subscription.Retiring()
		completed <- subscription.Complete(false)
	}()
	t.Cleanup(func() {
		if err := rt.Bus.ResetInMemoryState(); err != nil {
			t.Error(err)
		}
		if err := <-completed; err != nil {
			t.Error(err)
		}
	})
	s := newProcessLifecycleSupervisor(nil, rt)
	s.runtimeContexts = &runtime.RuntimeContextManager{}
	s.shutdownOptions = runtime.DefaultShutdownOptions()
	stop := s.beginInboundStop()
	defer s.inboundStopCancel()
	if err := s.runtimeContexts.CloseInboundRequests(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := stop.Finish(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-subscription.Retiring():
	default:
		t.Fatal("owned unregistered runtime escaped the process stop tail")
	}
}

func TestInboundStopPreparationFencesFreshDispatchWithoutCancelingAcceptedBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			_, start := clockDeploymentHarness(t, backend, canonicalrouting.CopyRootInputFeed(t, canonicalrouting.DeploymentFeedSameFlowRoot))
			process, _ := start()
			t.Cleanup(func() {
				if code := process.stop(); code != 0 {
					t.Errorf("joined native stop exit=%d", code)
				}
			})
			rt := servedTestProcessRuntime(t, process)
			accepted, err := rt.WorkOccurrence().Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := accepted.Done(); err != nil {
					t.Error(err)
				}
			}()
			s := newProcessLifecycleSupervisor(nil, rt)
			s.runtimeContexts = &runtime.RuntimeContextManager{}
			s.shutdownOptions = runtime.DefaultShutdownOptions()
			_ = s.prepareInboundStop()
			defer s.inboundStopCancel()
			fresh, err := rt.WorkOccurrence().Begin(t.Context())
			if fresh != nil {
				if err := fresh.Done(); err != nil {
					t.Error(err)
				}
			}
			if fresh != nil || !errors.Is(err, worklifetime.ErrAdmissionFenced) {
				t.Fatalf("preparation admitted fresh dispatch before dependency drain: %v", err)
			}
			if accepted.Context().Err() != nil {
				t.Fatal("fresh-dispatch seal canceled previously accepted work")
			}
			if grant, err := rt.CurrentStartupGrantEvidence(); err != nil || grant.State != startupownership.GrantAdmitted {
				t.Fatalf("preparation withdrew the native generation: %+v err=%v", grant, err)
			}
		})
	}
}
