package serveapp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime"
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
