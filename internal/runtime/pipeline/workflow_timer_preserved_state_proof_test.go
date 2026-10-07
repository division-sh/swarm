package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

type preservedTimerMutationWitness struct {
	WorkflowEngineMutationOwner
	test     *testing.T
	fault    string
	calls    int
	lastErr  error
	staleErr error
	onStale  func()
}

var errPreservedTimerCleanup = errors.New("acknowledged timer cleanup failure")

func (w *preservedTimerMutationWitness) CommitWorkflowEngineMutation(ctx context.Context, command WorkflowEngineMutationCommand) (CommittedWorkflowEngineMutation, error) {
	w.calls++
	if !command.State.Transition.PreservesState() || command.DeliverySuccess == nil || command.Lifecycle.StageEntry != nil {
		w.test.Fatalf("accepted empty handler lost its exact preservation/settlement command: %+v", command)
	}
	switch w.fault {
	case "stale_revision":
		if w.calls == 1 {
			command.State.ExpectedRevision++
		}
	case "changed_fields":
		command.State.Fields = json.RawMessage(`{"marker":"unapproved"}`)
	case "changed_config":
		command.State.Config = json.RawMessage(`{"config":{"unapproved":true}}`)
	case "foreign_event", "wrong_occurrence":
		cause := command.AcceptedEvent
		id, at := cause.EventID(), cause.OccurredAt()
		if w.fault == "foreign_event" {
			id = uuid.NewString()
		} else {
			at = at.Add(time.Second)
		}
		changed, err := workflowlifecycle.NewAcceptedEvent(cause.Route(), cause.EntityID(), id, cause.EventType(), cause.ExecutionMode(), at, nil)
		if err != nil {
			w.test.Fatal(err)
		}
		command.AcceptedEvent = &changed
	case "foreign_source":
		fact, err := runtimecorrelation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("b", 64))
		if err != nil {
			w.test.Fatal(err)
		}
		command.AcceptedEventSource = fact
	case "settlement_failure":
		// A valid but unrelated claim must fail after the timer effect, rolling
		// the entire transaction back rather than exposing that effect.
		claim := command.DeliverySuccess.Claim
		foreign, err := runtimedelivery.AdmitPersistedClaim(claim.DeliveryID(), claim.RunID(), claim.RouteIdentity(), uuid.NewString(), claim.Version(), claim.SubscriberClass(), claim.SubscriberID())
		if err != nil {
			w.test.Fatal(err)
		}
		command.DeliverySuccess.Claim = foreign
	}
	result, err := w.WorkflowEngineMutationOwner.CommitWorkflowEngineMutation(ctx, command)
	if w.fault == "stale_revision" && w.calls == 1 {
		w.staleErr = err
		if result.Committed || err == nil {
			w.test.Fatal("stale timer attempt committed")
		}
		if w.onStale != nil {
			w.onStale()
		}
	}
	if result.Committed {
		stage, stageErr := CommittedWorkflowStage(command.State)
		if stageErr != nil || result.Stage != stage || result.Stage.Revision != command.State.ExpectedRevision {
			w.test.Fatalf("preserving settlement changed the committed stage receipt: %+v want=%+v err=%v", result.Stage, stage, stageErr)
		}
	}
	if w.fault == "committed_cleanup" && result.Committed {
		err = errors.Join(err, errPreservedTimerCleanup)
	}
	w.lastErr = err
	return result, err
}

// The factory supplies native stores, construction, attachment and real delivery
// admission. This proof never substitutes a SQL fixture for the commit owner.
func VerifyMutationFreeAcceptedEventTimersBothStoresForTest(t *testing.T, factory WorkflowTimerCauseReplayFactoryForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, fields := range []bool{false, true} {
			for _, fault := range []string{"none", "committed_cleanup", "stale_revision", "changed_fields", "changed_config", "foreign_event", "wrong_occurrence", "foreign_source", "settlement_failure"} {
				t.Run(fmt.Sprintf("%s/fields=%t/%s", backend, fields, fault), func(t *testing.T) {
					files := map[string]string{
						"schema.yaml": "name: preserved-timer-proof\nstages:\n  waiting: {initial: true}\n",
						"events.yaml": "timer.arm:\ntimer.cancel:\ntimer.elapsed:\nwork.noted:\n",
						"nodes.yaml":  "observer:\n  execution_type: system_node\n  event_handlers:\n    timer.arm: {}\n    timer.cancel: {}\n    work.noted: {}\n  timers:\n    - {id: event.timeout, event: timer.elapsed, start_on: 'event:timer.arm', cancel_on: 'event:timer.cancel', delay: 1h}\n",
					}
					entityType := ""
					values := map[string]any{}
					if fields {
						files["entities.yaml"] = "work:\n  marker: {type: text, initial: stable}\n"
						entityType, values = "work", map[string]any{"marker": "stable"}
					}
					bundle := loadWorkflowTempBundle(t, files)
					f := factory(t, backend, bundle)
					ctx, pc := f.Context, f.Coordinator
					runID := runtimecorrelation.RunIDFromContext(ctx)
					owner := testRunScopedWorkflowInstanceFromContext(ctx, runID)
					entityID := identity.NormalizeEntityID(runID)
					at := canonicalWorkflowTimerTime(time.Now().Add(-2 * time.Hour))
					f.CommitConstruction(ctx, owner, WorkflowInstance{InstanceID: runID, StorageRef: runID, EntityID: runID,
						WorkflowName: ".", WorkflowVersion: semanticview.Wrap(bundle).WorkflowVersion(), Mode: "static",
						EntityType: entityType, Fields: values, CurrentState: "waiting", StageDefined: true, CreatedAt: at, EnteredStageAt: at}, at)
					load := func() WorkflowTargetPersistenceRecord {
						t.Helper()
						record, err := pc.workflowStore.LoadTargetPersistence(ctx, owner, entityID)
						if err != nil {
							t.Fatal(err)
						}
						return record
					}
					before := load()
					witness := &preservedTimerMutationWitness{WorkflowEngineMutationOwner: pc.workflowStore.engineMutations, test: t, fault: fault}
					witness.onStale = func() {
						if !reflect.DeepEqual(before, load()) || len(listTimerCauseReplayActivationsForTest(t, pc.workflowStore.timerActivations, ctx, runID)) != 0 {
							t.Fatal("stale timer attempt changed construction or leaked an activation")
						}
					}
					pc.workflowStore.engineMutations = witness
					publish := func(name string, offset time.Duration) events.Event {
						t.Helper()
						event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(name), "operator", "", []byte(`{}`), 0, runID,
							events.EnvelopeForEntityID(events.EventEnvelope{}, runID), eventtest.RootRoutingSource(runID), at.Add(offset))
						if err := f.Publish(runtimecorrelation.WithInboundEvent(ctx, event), event); err != nil && !(fault == "committed_cleanup" && errors.Is(err, errPreservedTimerCleanup)) {
							t.Fatal(err)
						}
						return event
					}
					arm := publish("timer.arm", time.Minute)
					if !reflect.DeepEqual(before, load()) {
						t.Fatal("accepted-event reaction or refusal changed exact header/field/stage-entry bytes")
					}
					rows := listTimerCauseReplayActivationsForTest(t, pc.workflowStore.timerActivations, ctx, runID)
					if fault != "none" && fault != "committed_cleanup" && fault != "stale_revision" {
						if len(rows) != 0 || witness.calls != 1 {
							t.Fatalf("refusal leaked timers or replayed settlement: rows=%+v calls=%d", rows, witness.calls)
						}
						return
					}
					armCalls := 1
					if fault == "stale_revision" {
						armCalls = 2
						if !runtimefailures.IsStateContention(witness.staleErr) {
							t.Fatalf("stale timer attempt did not retain exact contention: %v", witness.staleErr)
						}
					}
					if len(rows) != 1 || rows[0].Status != workflowTimerStatusActive || witness.calls != armCalls {
						t.Fatalf("accepted-event timer not committed exactly once: %+v calls=%d error=%v", rows, witness.calls, witness.lastErr)
					}
					assertSettled := func(event events.Event) {
						t.Helper()
						route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(pipelineNode(t, ".", "observer")),
							Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: runID})}
						proof, err := pc.deliveryStore.ProveHandoff(ctx, event.ID(), route)
						if err != nil {
							t.Fatal(err)
						}
						snapshot, err := pc.deliveryStore.Snapshot(ctx, proof.DeliveryID())
						outcomes, outcomesErr := pc.deliveryStore.Outcomes(ctx, proof.DeliveryID())
						if err != nil || outcomesErr != nil || snapshot.Status != runtimedelivery.StatusDelivered || len(outcomes) != 1 {
							t.Fatalf("exact timer delivery settlement: status=%s outcomes=%+v err=%v/%v", snapshot.Status, outcomes, err, outcomesErr)
						}
					}
					assertSettled(arm)
					assertSettled(publish("work.noted", 2*time.Minute))
					if witness.calls != armCalls {
						t.Fatal("no-reaction event acquired lifecycle commit authority")
					}
					assertSettled(publish("timer.cancel", 3*time.Minute))
					rows = listTimerCauseReplayActivationsForTest(t, pc.workflowStore.timerActivations, ctx, runID)
					if len(rows) != 1 || rows[0].Status != workflowTimerStatusCancelled || witness.calls != armCalls+1 || !reflect.DeepEqual(before, load()) {
						t.Fatalf("cancel changed construction or repeated state entry: %+v calls=%d", rows, witness.calls)
					}
					if err := f.Publish(runtimecorrelation.WithInboundEvent(ctx, arm), arm); err != nil {
						t.Fatal(err)
					}
					if got := listTimerCauseReplayActivationsForTest(t, pc.workflowStore.timerActivations, ctx, runID); !reflect.DeepEqual(rows, got) || !reflect.DeepEqual(before, load()) || witness.calls != armCalls+1 {
						t.Fatal("receipt replay resurrected cancelled timer or mutated construction")
					}
				})
			}
		}
	}
}
