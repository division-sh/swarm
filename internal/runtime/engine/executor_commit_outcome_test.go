package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

type outcomeMutationOwner struct {
	committed      bool
	malformedClaim bool
	err            error
	calls          int
}

func (o *outcomeMutationOwner) CommitEngineMutation(_ context.Context, mutation EngineMutation) (CommittedEngineMutation, error) {
	o.calls++
	if !o.committed {
		return CommittedEngineMutation{}, o.err
	}
	result := CommittedEngineMutation{Committed: true, ActivityIntents: mutation.ActivityIntents,
		EmitIntents: []EmitIntent{{Event: eventtest.RunCreatingRootIngress("child", "source.requested", "", "", []byte(`{}`), 0, "run-1", "", events.EventEnvelope{}, time.Time{})}}}
	if o.malformedClaim {
		result.SettledDeliveryClaim = &runtimedelivery.Claim{}
	}
	return result, o.err
}

func TestExecutorRetainsAcknowledgedIntentsWithIndependentError(t *testing.T) {
	for _, phase := range []string{"uncommitted", "committed", "committed_malformed_claim"} {
		t.Run(phase, func(t *testing.T) {
			injected := errors.New("mutation cleanup failure")
			owner := &outcomeMutationOwner{committed: phase != "uncommitted", malformedClaim: phase == "committed_malformed_claim", err: injected}
			executor, err := NewExecutor(RuntimeDependencies{
				Source: sourceWithFixtureStages(sourceWithActivityTool(), "research", "pending", "pending"), StateRepo: stubStateRepo{}, MutationOwner: owner, Locker: stubLocker{},
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			result, err := executor.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
				EntityID: identity.NormalizeEntityID("entity-1"), Node: testFlowExecutableNode(t, "research", "scanner"), HandlerEventKey: "source.requested",
				Event:   eventtest.RunCreatingRootIngress("evt-1", "source.requested", "", "task-1", []byte(`{"url":"https://example.com"}`), 2, "run-1", "", events.EventEnvelope{}, time.Time{}),
				Handler: runtimecontracts.SystemNodeEventHandler{Activity: runtimecontracts.ActivitySpec{Tool: "source_scrape", Input: map[string]runtimecontracts.ExpressionValue{"url": runtimecontracts.CELExpression("payload.url")}}},
				State:   testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{})})
			if !errors.Is(err, injected) || owner.calls != 1 || result.Committed != owner.committed {
				t.Fatalf("result=%+v err=%v commits=%d", result, err, owner.calls)
			}
			if owner.committed {
				if len(result.EmitIntents) != 1 || len(result.ActivityIntents) != 1 || result.Status == OutcomeRejected || result.Failure != nil {
					t.Fatalf("lost acknowledged work or rejected commit: %+v", result)
				}
			} else if len(result.EmitIntents) != 0 || len(result.ActivityIntents) != 0 {
				t.Fatal("uncommitted intents escaped the executor")
			}
		})
	}
}
