package pipeline_test

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

type a2SameCommitScheduleWitness struct {
	kind runtimepipeline.WorkflowScheduleMutationKind
	task string
	hash string
}

type a2SameCommitMutationWitness struct {
	state        json.RawMessage
	entry        timeridentity.StageEntryRef
	schedules    []a2SameCommitScheduleWitness
	publications []string
	deliveryID   string
	nodeKey      string
	err          error
}

// This observer neither changes nor delays the command. The real selected-store
// writer owns the transaction, revision fence, and prospective-state validation.
type a2SameCommitObservedPersistence struct {
	runtimepipeline.WorkflowPersistenceOwner
	mu        sync.Mutex
	committed []a2SameCommitMutationWitness
}

func (p *a2SameCommitObservedPersistence) CommitWorkflowEngineMutation(ctx context.Context, command runtimepipeline.WorkflowEngineMutationCommand) (runtimepipeline.CommittedWorkflowEngineMutation, error) {
	var witness a2SameCommitMutationWitness
	witness.state, witness.err = json.Marshal(command.State)
	if command.Lifecycle.StageEntry != nil {
		witness.entry = *command.Lifecycle.StageEntry
	}
	for _, schedule := range command.Lifecycle.Schedules {
		hash, err := schedule.Command.ImmutableHash()
		if err != nil {
			witness.err = err
		}
		witness.schedules = append(witness.schedules, a2SameCommitScheduleWitness{kind: schedule.Kind, task: schedule.Command.TaskID, hash: hash})
	}
	for _, publication := range command.Publications {
		witness.publications = append(witness.publications, publication.DurablePublicationEventID())
	}
	if command.DeliverySuccess != nil {
		witness.deliveryID = command.DeliverySuccess.Claim.DeliveryID()
		witness.nodeKey = command.DeliverySuccess.Claim.SubscriberID()
	}
	result, err := p.WorkflowPersistenceOwner.CommitWorkflowEngineMutation(ctx, command)
	if err == nil {
		p.mu.Lock()
		p.committed = append(p.committed, witness)
		p.mu.Unlock()
	}
	return result, err
}

func (p *a2SameCommitObservedPersistence) witnesses() []a2SameCommitMutationWitness {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]a2SameCommitMutationWitness(nil), p.committed...)
}

// An existing root transitions, arms, and publishes in one business commit.
// This is not eager construction, C/E boot activation, or a publication race.
func TestA2SameBusinessCommitTransitionArmAndPublicationOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			runID := uuid.NewString()
			insertGateRecoveryRun(t, selected, runID)
			ctx := withLiveGateExecution(correlation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
			owner := testRunScopedWorkflowInstanceForRun(runID, runID)
			source := semanticview.Wrap(loadPipelineLifecycleFixtureBundle(t, a2SameCommitJoinBindingFiles()))
			dispatcher := externalPipelineSourceNode(t, source, ".", "dispatcher")
			collector := externalPipelineSourceNode(t, source, ".", "collector")
			module := proposedEffectProofModule{source: source, nodes: []runtimepipeline.WorkflowNode{
				{Node: dispatcher, Subscriptions: []events.EventType{"dispatch.completed"}, ExecutionType: runtimecontracts.SystemNodeExecutionType},
				{Node: collector, Subscriptions: []events.EventType{"item.completed"}, ExecutionType: runtimecontracts.SystemNodeExecutionType},
			}}
			observer := &a2SameCommitObservedPersistence{WorkflowPersistenceOwner: selected.events.(runtimepipeline.WorkflowPersistenceOwner)}
			selected.persistence = runtimepipeline.NewWorkflowPersistence(observer)
			logger := &exactJoinRuntimeLogger{}
			newBus := func(probe *lifecycleprobe.Probe) *runtimebus.EventBus {
				t.Helper()
				bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
					ContractBundle: source, TestLifecycleProbe: probe, Logger: logger,
				}, "platform.join_complete", "platform.join_timeout")
				if err != nil {
					t.Fatalf("new same-commit EventBus: %v", err)
				}
				return bus
			}
			probe := lifecycleprobe.New()
			bus := newBus(probe)
			schedules, _ := newExactJoinScheduleLifecycleForTest(t, ctx, selected, bus)
			pc := newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{
				Module: module, GenericSchedules: schedules, TestLifecycleProbe: probe,
			})
			bus.SetInterceptors(pc)
			now := time.Now().UTC()
			if _, err := pc.MaterializeInitialEntry(ctx, owner, runtimepipeline.WorkflowInstance{
				InstanceID: runID, StorageRef: runID, EntityID: runID, WorkflowName: source.WorkflowName(), WorkflowVersion: source.WorkflowVersion(),
				CurrentState: "dispatching", EntityType: "join_state", Fields: map[string]any{"expected": []any{"a", "b"}},
			}, now); err != nil {
				t.Fatalf("materialize existing unarmed root: %v", err)
			}
			load := func() runtimepipeline.WorkflowInstance {
				t.Helper()
				instance, found, err := pc.Load(ctx, owner)
				if err != nil || !found {
					t.Fatalf("load same-commit root: found=%v err=%v", found, err)
				}
				return instance
			}
			initial := load()
			initialEntry, found, err := workflowlifecycle.LoadStageEntry(initial.Bookkeeping)
			if err != nil || !found || initial.CurrentState != "dispatching" || initialEntry.Stage != "dispatching" ||
				initialEntry.Cause != "construction" || len(a2KnownTargetArms(t, initial)) != 0 {
				t.Fatalf("root was not initially unarmed: entry=%#v found=%v err=%v", initialEntry, found, err)
			}
			trigger := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "dispatch.completed", "operator", "", []byte(`{}`), 0, runID,
				events.EnvelopeForEntityID(events.EventEnvelope{}, runID), eventtest.RootRoutingSource(runID), now)
			if err := bus.PublishAcknowledged(ctx, trigger); err != nil {
				t.Fatalf("publish real transition-and-emission handler: %v", err)
			}
			a2KnownTargetWaitForSettlement(t, ctx, bus, probe, trigger, dispatcher.Key(), "completed", "delivered", logger)
			outputs := func() (int, string) {
				t.Helper()
				query := `SELECT COUNT(*), COALESCE(MIN(CAST(event_id AS TEXT)), '') FROM events WHERE run_id=? AND source_event_id=? AND event_name='item.completed'`
				if selected.postgres {
					query = `SELECT COUNT(*), COALESCE(MIN(event_id::text), '') FROM events WHERE run_id=$1::uuid AND source_event_id=$2::uuid AND event_name='item.completed'`
				}
				var count int
				var id string
				if err := selected.db.QueryRowContext(ctx, query, runID, trigger.ID()).Scan(&count, &id); err != nil {
					t.Fatalf("read actual same-commit outputs: %v", err)
				}
				return count, id
			}
			count, outputID := outputs()
			if count != 1 || outputID == "" {
				t.Fatalf("handler did not commit one real local arrival: count=%d id=%q", count, outputID)
			}
			publication, found, err := selected.events.LoadPreparedPublishEvent(ctx, outputID)
			if err != nil || !found || len(publication.DeliveryRoutes) != 1 {
				t.Fatalf("read generated first-publication route: found=%v routes=%#v err=%v", found, publication.DeliveryRoutes, err)
			}
			a2KnownTargetWaitForSettlement(t, ctx, bus, probe, publication.Event.Event(), collector.Key(), "completed", "delivered", logger)
			final := load()
			entry, found, err := workflowlifecycle.LoadStageEntry(final.Bookkeeping)
			arm := exactJoinPersistedArm(t, final)
			route := publication.DeliveryRoutes[0]
			if err != nil || !found || entry == initialEntry || entry.Stage != "awaiting" || entry.Cause != "delivery" || entry.EventID != trigger.ID() ||
				route.Recipient.ID() != collector.Key() || route.Target.Route() != (events.RouteIdentity{FlowID: semanticview.RootExecutionFlowID(source), FlowInstance: runID, EntityID: runID}) ||
				len(route.Context.Joins) != 1 || route.Context.Joins[0].Disposition != events.JoinAdmissionBound ||
				route.Context.Joins[0].Ref.StageEntry() != entry || !route.Context.Joins[0].Ref.Equal(arm.JoinRef()) {
				t.Fatalf("publication did not bind the same-commit persisted entry/arm: entry=%#v route=%#v err=%v", entry, route, err)
			}
			if final.CurrentState != "awaiting" || final.Revision != initial.Revision+2 || len(final.TransitionHistory) != 1 ||
				arm.Status != joinruntime.StatusOpen || arm.Completed() != 1 || !reflect.DeepEqual(arm.Members, []string{"a", "b"}) ||
				!reflect.DeepEqual(arm.Outputs["a"].Value, map[string]any{"value": "same-commit"}) {
				t.Fatalf("real arrival did not contribute exactly once after the transition: revision=%d initial=%d state=%s arm=%#v", final.Revision, initial.Revision, final.CurrentState, arm)
			}
			deadline := exactJoinPendingSchedule(t, selected, ctx, arm)
			deadlineHash, err := deadline.Command.ImmutableHash()
			if err != nil {
				t.Fatal(err)
			}
			witnesses := observer.witnesses()
			if len(witnesses) != 2 {
				t.Fatalf("expected separate source business and arrival commits, got %d", len(witnesses))
			}
			var sourceCommits, arrivalCommits int
			deliveryID := func(eventID, nodeKey string) string {
				t.Helper()
				query := `SELECT delivery_id FROM event_deliveries WHERE event_id=? AND subscriber_type='node' AND subscriber_id=?`
				if selected.postgres {
					query = `SELECT delivery_id FROM event_deliveries WHERE event_id=$1::uuid AND subscriber_type='node' AND subscriber_id=$2`
				}
				var id string
				if err := selected.db.QueryRowContext(ctx, query, eventID, nodeKey).Scan(&id); err != nil {
					t.Fatalf("read actual delivery claim owner: %v", err)
				}
				return id
			}
			sourceDeliveryID, arrivalDeliveryID := deliveryID(trigger.ID(), dispatcher.Key()), deliveryID(outputID, collector.Key())
			for _, witness := range witnesses {
				record, instance := a2SameCommitWitnessState(t, witness)
				witnessArm := exactJoinPersistedArm(t, instance)
				witnessEntry, present, err := workflowlifecycle.LoadStageEntry(instance.Bookkeeping)
				if err != nil || !present || witnessEntry != entry || !witnessArm.JoinRef().Equal(arm.JoinRef()) || witnessArm.Status != joinruntime.StatusOpen {
					t.Fatalf("committed command lacked exact entry/arm: entry=%#v arm=%#v err=%v", witnessEntry, witnessArm, err)
				}
				switch witness.deliveryID {
				case sourceDeliveryID:
					sourceCommits++
					if witness.nodeKey != dispatcher.Key() || record.ExpectedState != "dispatching" || record.ExpectedRevision != initial.Revision ||
						record.CurrentState != "awaiting" || witness.entry != entry || witnessArm.Completed() != 0 ||
						!reflect.DeepEqual(witness.publications, []string{outputID}) ||
						!reflect.DeepEqual(witness.schedules, []a2SameCommitScheduleWitness{{kind: runtimepipeline.WorkflowScheduleMutationUpsert, task: arm.TimerTaskID(), hash: deadlineHash}}) {
						t.Fatalf("transition, arm, schedule, and publication were not one actual source commit: %#v", witness)
					}
				case arrivalDeliveryID:
					arrivalCommits++
					if witness.nodeKey != collector.Key() || record.ExpectedState != "awaiting" || record.ExpectedRevision != initial.Revision+1 ||
						witnessArm.Completed() != 1 || len(witness.publications) != 0 || len(witness.schedules) != 0 {
						t.Fatalf("arrival was not the next actual member commit: %#v", witness)
					}
				default:
					t.Fatalf("unexpected business commit for delivery %q", witness.deliveryID)
				}
			}
			if sourceCommits != 1 || arrivalCommits != 1 {
				t.Fatalf("source/arrival commit counts=%d/%d", sourceCommits, arrivalCommits)
			}

			// A fresh EventBus/coordinator reads retained publications and state.
			// The schedule gate is the only execution hold; this is not a crash test.
			stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err = schedules.Stop(stopCtx)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			probe = lifecycleprobe.New()
			bus = newBus(probe)
			schedules, _ = newExactJoinScheduleLifecycleForTest(t, ctx, selected, bus)
			pc = newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{
				Module: module, GenericSchedules: schedules, TestLifecycleProbe: probe,
			})
			bus.SetInterceptors(pc)
			for _, replay := range []events.Event{publication.Event.Event(), trigger} {
				if err := bus.PublishAcknowledged(ctx, replay); err != nil {
					t.Fatalf("replay retained event through restarted EventBus: %v", err)
				}
			}
			waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err = bus.WaitForQuiescence(waitCtx)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			replayed, found, err := selected.events.LoadPreparedPublishEvent(ctx, outputID)
			if err != nil || !found || !reflect.DeepEqual(replayed.DeliveryRoutes, publication.DeliveryRoutes) || !reflect.DeepEqual(final, load()) || len(observer.witnesses()) != len(witnesses) {
				t.Fatalf("restart replay rebound the receipt or repeated a business commit: found=%v err=%v", found, err)
			}
			if count, id := outputs(); count != 1 || id != outputID {
				t.Fatalf("restart repeated the same-commit output: count=%d id=%q", count, id)
			}
			for _, delivery := range []struct{ eventID, nodeKey string }{{trigger.ID(), dispatcher.Key()}, {outputID, collector.Key()}} {
				assertExactJoinDeliveryCount(t, selected, ctx, delivery.eventID, delivery.nodeKey, 1)
				assertExactJoinDeliveryStatus(t, selected, ctx, delivery.eventID, delivery.nodeKey, "delivered")
				query := `SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries d ON a.delivery_id=d.delivery_id
					WHERE d.event_id=? AND d.subscriber_type='node' AND d.subscriber_id=? AND a.closure_kind='settled'`
				if selected.postgres {
					query = `SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries d ON a.delivery_id=d.delivery_id
						WHERE d.event_id=$1::uuid AND d.subscriber_type='node' AND d.subscriber_id=$2 AND a.closure_kind='settled'`
				}
				var attempts int
				if err := selected.db.QueryRowContext(ctx, query, delivery.eventID, delivery.nodeKey).Scan(&attempts); err != nil || attempts != 1 {
					t.Fatalf("restart repeated a settled delivery attempt: attempts=%d err=%v", attempts, err)
				}
			}
			restoredDeadline := exactJoinPendingSchedule(t, selected, ctx, exactJoinPersistedArm(t, load()))
			if restoredDeadline.ID != deadline.ID || restoredDeadline.ImmutableHash != deadline.ImmutableHash {
				t.Fatal("restart changed the exact pending deadline schedule")
			}
		})
	}
}

func a2SameCommitWitnessState(t *testing.T, witness a2SameCommitMutationWitness) (runtimepipeline.WorkflowEngineStateRecord, runtimepipeline.WorkflowInstance) {
	t.Helper()
	if witness.err != nil {
		t.Fatalf("capture actual commit: %v", witness.err)
	}
	var record runtimepipeline.WorkflowEngineStateRecord
	if err := json.Unmarshal(witness.state, &record); err != nil {
		t.Fatal(err)
	}
	instance, err := runtimepipeline.DecodeWorkflowEntityStatePersistenceRecord(runtimepipeline.WorkflowEntityStatePersistenceRecord{
		EntityID: record.EntityID, FlowInstance: record.Identity.Route.InstancePath, EntityType: record.EntityType,
		CurrentState: record.CurrentState, Revision: record.ExpectedRevision + 1, EnteredStageAt: record.EnteredStageAt,
		Fields: record.Fields, Bookkeeping: record.Bookkeeping, Gates: record.Gates, Accumulator: record.Accumulator,
		CreatedAt: record.CreatedAt, UpdatedAt: record.UpdatedAt,
	}, record.Identity.Route, record.WorkflowName, record.WorkflowVersion, record.Mode)
	if err != nil {
		t.Fatal(err)
	}
	return record, instance
}

func a2SameCommitJoinBindingFiles() map[string]string {
	return map[string]string{
		"schema.yaml": `name: a2-same-commit-join-binding
stages:
  dispatching: {initial: true}
  awaiting: {}
  ready: {terminal: true}
  attention: {terminal: true}
pins:
  inputs:
    - dispatch.completed
  outputs:
    - item.completed
`,
		"entities.yaml": "join_state:\n  expected: \"[text]\"\n",
		"types.yaml":    "types:\n  JoinResult:\n    value: text\n",
		"events.yaml":   "dispatch.completed:\nitem.completed:\n  member_id: text\n  result: JoinResult\n",
		"nodes.yaml": `dispatcher:
  execution_type: system_node
  event_handlers:
    dispatch.completed:
      advances_to: awaiting
      emit:
        event: item.completed
        fields:
          member_id: "a"
          result: {value: "same-commit"}
collector:
  execution_type: system_node
  event_handlers:
    item.completed:
      join:
        stage: awaiting
        members: {from: state.expected, by: payload.member_id}
        output: payload.result
        deadline: {after: 1h, from: stage_entry}
        on_complete: {advances_to: ready}
        on_deadline: {advances_to: attention}
`,
	}
}
