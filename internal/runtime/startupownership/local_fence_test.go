package startupownership

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fenceTransitionSession struct {
	*retainedSessionProbe
	record func(GrantEvidence) error
}

func (s *fenceTransitionSession) RecordGenerationGrantTransition(ctx context.Context, previous *GrantEvidence, next GrantEvidence) error {
	if s.record != nil {
		if err := s.record(next); err != nil {
			return err
		}
	}
	return s.retainedSessionProbe.RecordGenerationGrantTransition(ctx, previous, next)
}

func TestGenerationGrantLocalFenceDuringTransition(t *testing.T) {
	for _, phase := range []string{"transition", "transition_error", "retirement"} {
		t.Run(phase, func(t *testing.T) {
			probe, plan := testRetainedSession(t)
			session := &fenceTransitionSession{retainedSessionProbe: probe}
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
			entered, resume := make(chan struct{}), make(chan struct{})
			defer close(resume)
			failure := errors.New("independent transition failure")
			session.record = func(GrantEvidence) error {
				probe.callback(TerminalResult{Cause: TerminalOwnershipUnprovable})
				close(entered)
				<-resume
				if phase == "transition_error" {
					return failure
				}
				return nil
			}
			done := make(chan error, 1)
			go func() {
				if phase == "retirement" {
					done <- grant.Retire(context.Background())
					return
				}
				_, err := grant.MarkProbesSettled(context.Background(), nil)
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("terminal callback blocked behind transition/grant lock")
			}
			for _, signal := range []<-chan struct{}{capability.Done(), grant.Done()} {
				select {
				case <-signal:
				default:
					t.Fatal("local grant fencing awaited SQL completion")
				}
			}
			if _, err := grant.Evidence(); err == nil {
				t.Fatal("retired grant still exposes execution evidence")
			}
			resume <- struct{}{}
			err = <-done
			if phase == "transition" && err == nil {
				t.Fatal("late successful transition revived a retired grant")
			}
			if phase == "transition_error" && !errors.Is(err, failure) {
				t.Fatalf("independent transition failure lost: %v", err)
			}
			g := grant.(*liveGenerationGrant).generationGrant
			g.mu.Lock()
			state := g.evidence.State
			g.mu.Unlock()
			if state != GrantRetired {
				t.Fatalf("late transition state=%s", state)
			}
		})
	}
}

func TestProcessCapabilityLateLineageOnlyEnrichesTerminal(t *testing.T) {
	capability, session, _ := testCapability(t)
	defer capability.Release(context.Background())
	session.callback(TerminalResult{Cause: TerminalOwnershipUnprovable})
	session.callback(TerminalResult{Cause: TerminalOwnershipSuperseded, SuccessorAuthorityID: "successor"})
	if _, err := capability.Evidence(); err == nil {
		t.Fatal("late lineage restored executable authority")
	}
	result, terminal := capability.TerminalResult()
	if !terminal || result.Cause != TerminalOwnershipSuperseded || result.SuccessorAuthorityID != "successor" {
		t.Fatalf("late diagnostic evidence lost: %#v", result)
	}
}

func TestProcessCapabilityRecheckFencesBeforeOperationJoin(t *testing.T) {
	for _, phase := range []string{"operation_error", "failed_release"} {
		t.Run(phase, func(t *testing.T) {
			probe, _ := testRetainedSession(t)
			capability, err := newProcessCapability(probe, time.Hour, 20*time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			defer capability.Release(context.Background())
			failure := errors.New("independent operation or release failure")
			if phase == "operation_error" {
				probe.loadSourceErr = failure
			} else {
				probe.releaseErr = failure
			}
			entered, resume := make(chan struct{}), make(chan struct{})
			defer close(resume)
			calls := 0
			probe.monitorProve = func(context.Context, time.Duration) error {
				calls++
				if phase == "operation_error" && calls == 1 {
					return nil
				}
				// The backend socket tests qualify the deadline decision. Here its
				// notification must not block on the enclosing process operation.
				probe.callback(TerminalResult{Cause: TerminalOwnershipUnprovable})
				close(entered)
				<-resume
				return context.DeadlineExceeded
			}
			done := make(chan error, 1)
			go func() {
				if phase == "failed_release" {
					done <- capability.Release(context.Background())
					return
				}
				_, _, err := capability.CurrentSourceSet(context.Background())
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("independent recheck notification blocked behind operation lock")
			}
			select {
			case <-capability.Done():
			default:
				t.Fatal("independent recheck did not fence locally")
			}
			select {
			case err := <-done:
				t.Fatalf("operation returned before proof cleanup joined: %v", err)
			default:
			}
			resume <- struct{}{}
			if err := <-done; !errors.Is(err, failure) || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("operation/recheck evidence lost: %v", err)
			}
		})
	}
}
