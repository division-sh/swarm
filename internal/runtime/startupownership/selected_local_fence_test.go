package startupownership

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/google/uuid"
)

type selectedFenceSession struct {
	*retainedSessionProbe
	proof   func() error
	inspect func() error
}

func (s *selectedFenceSession) ProveSelectedForkGenerationGrant(context.Context, GrantEvidence) error {
	if s.proof != nil {
		return s.proof()
	}
	return nil
}

func (s *selectedFenceSession) InspectRunExecutionOwnership(context.Context, GrantEvidence, string) (manager.RunExecutionOwnership, error) {
	return 0, s.inspect()
}

func TestSelectedGenerationGrantLocalFenceJoinsBlockedConsumer(t *testing.T) {
	for _, phase := range []string{"selected_proof", "ownership_read"} {
		for _, independentFailure := range []bool{false, true} {
			t.Run(phase+map[bool]string{false: "/success", true: "/independent_error"}[independentFailure], func(t *testing.T) {
				probe, _ := testRetainedSession(t)
				session := &selectedFenceSession{retainedSessionProbe: probe}
				capability, err := newProcessCapability(session, time.Hour, time.Second)
				if err != nil {
					t.Fatal(err)
				}
				defer capability.Release(context.Background())
				fingerprint := "sha256:" + strings.Repeat("1", 64)
				grant, err := capability.IssueSelectedForkGenerationGrant(context.Background(), SelectedForkGrantRequest{
					BundleHash: startupBundleHashA, RuntimeInstanceID: probe.authority.RuntimeInstanceID,
					Binding: SelectedForkGrantBinding{
						BindingID: uuid.NewString(), ForkRunID: uuid.NewString(), ExecutionID: uuid.NewString(),
						ExecutionGeneration: 1, FenceGeneration: 1, ExecutionOwner: "selected-owner",
						AdmissionFingerprint: fingerprint, ContainerPlanFingerprint: fingerprint,
						ActorCensusFingerprint: fingerprint, EffectiveConfigFingerprint: fingerprint,
						DeclarationPlanFingerprint: fingerprint, PreparationFingerprint: fingerprint,
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				entered, resume := make(chan struct{}), make(chan struct{})
				defer close(resume)
				failure := errors.New("independent selected SQL failure")
				block := func() error {
					probe.callback(TerminalResult{Cause: TerminalOwnershipUnprovable})
					close(entered)
					<-resume
					if independentFailure {
						return failure
					}
					return nil
				}
				if phase == "selected_proof" {
					session.proof = block
				} else {
					session.inspect = block
				}
				done := make(chan error, 1)
				go func() {
					_, err := grant.InspectRunExecutionOwnership(context.Background(), uuid.NewString())
					done <- err
				}()
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("selected proof blocked local fencing")
				}
				for _, signal := range []<-chan struct{}{capability.Done(), grant.Done()} {
					select {
					case <-signal:
					default:
						t.Fatal("selected authority remained live during blocked SQL")
					}
				}
				if _, err := grant.Evidence(); err == nil {
					t.Fatal("retired selected grant supplied execution authority")
				}
				select {
				case <-done:
					t.Fatal("local fencing detached the outstanding SQL operation")
				default:
				}
				resume <- struct{}{}
				if err := <-done; err == nil || (independentFailure && !errors.Is(err, failure)) {
					t.Fatalf("late SQL result lost retirement or independent failure: %v", err)
				}
			})
		}
	}
}
