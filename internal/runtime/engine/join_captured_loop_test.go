package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

// Retained evidence exercises evaluation ownership, not scheduler permission.
// Real loop supersession cancels pending outcomes in the pipeline integration
// fixture; historical ownership here must never mutate a replacement attempt.
func TestExecutorJoinCapturedContextFromRetainedEvidence(t *testing.T) {
	for _, disposition := range []string{"complete", "timeout"} {
		for _, history := range []string{"current", "repeated", "closed", "forked"} {
			for _, reference := range []string{"ref", "cel"} {
				t.Run(disposition+"/"+history+"/"+reference, func(t *testing.T) {
					repo := canonicalrouting.RepoRoot(t)
					bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopyForkLoopRetainedJoin(t), runtimecontracts.DefaultPlatformSpecFile(repo))
					if err != nil {
						t.Fatal(err)
					}
					node := identitytest.RootNode(t, "controller")
					handler := bundle.Nodes["controller"].EventHandlers["review.requested"]
					outcome := handler.Join.OnComplete
					outcome.AdvancesTo = ""
					value := runtimecontracts.RefExpression("loop.revision_id")
					if reference == "cel" {
						value = runtimecontracts.CELExpression("loop.revision_id")
					}
					outcome.Emit.Fields["revision_id"] = value
					outcome.DataAccumulation = runtimecontracts.WorkflowDataAccumulation{Writes: []runtimecontracts.WorkflowDataWrite{{TargetField: "window", Value: value}}}
					handler.Join.OnComplete, handler.Join.Timeout.Outcome = outcome, outcome
					handler, err = completeSemanticFixtureHandlerRuleIdentity(node, "review.requested", handler)
					if err != nil {
						t.Fatal(err)
					}
					bundle.Nodes["controller"].EventHandlers["review.requested"] = handler
					for i := range bundle.Semantics.Joins {
						if bundle.Semantics.Joins[i].Node.Equal(node) && bundle.Semantics.Joins[i].HandlerEvent == "review.requested" {
							bundle.Semantics.Joins[i].Spec = *handler.Join
						}
					}
					source := semanticview.Wrap(bundle)
					exec, err := NewExecutor(RuntimeDependencies{Source: source, StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{}, Dispatcher: stubDispatcher{}}, nil)
					if err != nil {
						t.Fatal(err)
					}
					now := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
					runID := eventtest.UUID("captured-join-source")
					owner, err := loopruntime.New(runID, runID, ".", "revision", "revision_id", eventtest.UUID("captured-start"), "working", 3, now)
					if err != nil {
						t.Fatal(err)
					}
					captured := owner.Generation()
					if history != "current" {
						if _, err := owner.Repeat("working", eventtest.UUID("captured-repeat"), now.Add(time.Second)); err != nil {
							t.Fatal(err)
						}
					}
					if history == "closed" {
						if err := owner.Close("approved", eventtest.UUID("captured-close"), now.Add(2*time.Second)); err != nil {
							t.Fatal(err)
						}
					}
					if history == "forked" {
						runID = eventtest.UUID("captured-join-child")
						correspondence, err := loopruntime.NewForkCorrespondence([]loopruntime.Activation{owner}, runID, runID)
						if err != nil {
							t.Fatal(err)
						}
						original, err := correspondence.AdmitSource(captured)
						if err != nil {
							t.Fatal(err)
						}
						child, err := correspondence.Bind(original)
						if err != nil {
							t.Fatal(err)
						}
						captured = child.Generation()
						owner = correspondence.ProjectedActivations()[0]
					}
					ref, err := timeridentity.NewJoinRefForGeneration(node, "review.requested", "working", "reviews", "business-window", captured)
					if err != nil {
						t.Fatal(err)
					}
					handle, err := timeridentity.JoinTimeoutHandle(ref)
					if disposition == "complete" {
						handle, err = timeridentity.JoinCompleteHandle(ref)
					}
					if err != nil {
						t.Fatal(err)
					}
					join, err := joinruntime.NewActivation(handle, []string{"unchanged-business-value"}, now, now.Add(time.Hour))
					if err != nil {
						t.Fatal(err)
					}
					if disposition == "complete" {
						if _, err := join.Add("unchanged-business-value", captured.RevisionID); err != nil {
							t.Fatal(err)
						}
						join.Close(joinruntime.CloseReasonComplete, true, false)
					}
					buckets := map[string]map[string]any{}
					if err := loopruntime.Store(buckets, owner); err != nil {
						t.Fatal(err)
					}
					if err := joinruntime.Store(buckets, join); err != nil {
						t.Fatal(err)
					}
					payload, _ := json.Marshal(handle.PayloadMetadata())
					request := ExecutionRequest{
						EntityID: identity.NormalizeEntityID(runID), Node: node, HandlerEventKey: "review.requested", Handler: handler, JoinDeclaration: ref.Declaration(),
						Event: eventtest.RunCreatingRootIngress(eventtest.UUID("captured-outcome"), events.EventType(handle.EventType()), "runtime", handle.TaskID(), payload, 0, runID, "", events.EnvelopeForEntityID(events.EventEnvelope{}, runID), now.Add(time.Hour)),
						State: testStateSnapshot("working", map[string]any{"members": []any{"unchanged-business-value"}, "window": "not-a-revision"}, nil, buckets),
					}
					result, err := exec.ExecuteSemanticFixture(context.Background(), request)
					if err != nil {
						t.Fatal(err)
					}
					if got := emittedLoopRevision(t, result); got != captured.RevisionID {
						t.Fatalf("got revision=%s captured=%s current=%s", got, captured.RevisionID, owner.RevisionID)
					}
					if got := result.StateMutation.Fields["window"]; got != captured.RevisionID {
						t.Fatalf("data write did not consume captured reference: %v", got)
					}
					persisted, found, err := loopruntime.Load(result.StateMutation.StateBuckets, ".", "revision")
					if err != nil || !found || !reflect.DeepEqual(persisted, owner) || !bytes.Equal(payload, request.Event.Payload()) {
						t.Fatalf("outcome changed owner/payload: owner=%+v err=%v", persisted, err)
					}
				})
			}
		}
	}
}
