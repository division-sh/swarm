package startupownership

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/google/uuid"
)

type ordinaryInspectionSession struct {
	*retainedSessionProbe
	sourceLoads atomic.Uint64
	inspect     func() error
}

func (s *ordinaryInspectionSession) LoadSourceSet(ctx context.Context) (agenttopology.SourceSetPlan, bool, error) {
	s.sourceLoads.Add(1)
	return s.retainedSessionProbe.LoadSourceSet(ctx)
}

func (s *ordinaryInspectionSession) InspectRunExecutionOwnership(context.Context, GrantEvidence, string) (manager.RunExecutionOwnership, error) {
	if err := s.inspect(); err != nil {
		return 0, err
	}
	return manager.RunExecutionOwned, nil
}

func TestOrdinaryRunInspectionLocalFenceJoinsBlockedConsumer(t *testing.T) {
	for _, independentFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "late_success", true: "independent_error"}[independentFailure], func(t *testing.T) {
			probe, plan := testRetainedSession(t)
			session := &ordinaryInspectionSession{retainedSessionProbe: probe}
			capability, err := newProcessCapability(session, time.Hour, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer capability.Release(context.Background())
			grant, err := capability.IssueGenerationGrant(context.Background(), GrantRequest{
				BundleHash: startupBundleHashA, RuntimeInstanceID: probe.authority.RuntimeInstanceID,
				RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
			})
			if err != nil {
				t.Fatal(err)
			}
			loads := session.sourceLoads.Load()
			entered, resume := make(chan struct{}), make(chan struct{})
			defer close(resume)
			failure := errors.New("independent inspection SQL failure")
			session.inspect = func() error {
				probe.callback(TerminalResult{Cause: TerminalOwnershipUnprovable})
				close(entered)
				<-resume
				if independentFailure {
					return failure
				}
				return nil
			}
			done := make(chan error, 1)
			go func() {
				_, err := grant.InspectRunExecutionOwnership(context.Background(), uuid.NewString())
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("ordinary inspection did not reach the session owner")
			}
			if session.sourceLoads.Load() != loads {
				t.Fatal("ordinary inspection retained the separate source-set transaction")
			}
			for _, signal := range []<-chan struct{}{capability.Done(), grant.Done()} {
				select {
				case <-signal:
				default:
					t.Fatal("local fence waited for blocked inspection SQL")
				}
			}
			if _, err := grant.Evidence(); err == nil {
				t.Fatal("locally fenced ordinary grant still supplied authority")
			}
			select {
			case <-done:
				t.Fatal("local fencing detached outstanding inspection SQL")
			default:
			}
			resume <- struct{}{}
			if err := <-done; err == nil || (independentFailure && !errors.Is(err, failure)) {
				t.Fatalf("late inspection lost local fence or independent error: %v", err)
			}
		})
	}
}
