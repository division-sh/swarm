package pipeline

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

type stageTimerEntryProof struct {
	t       *testing.T
	fixture *PipelineDeliveryNativeFixtureForTest
	pc      *PipelineCoordinator
	ctx     context.Context
	bus     *nativePipelineDeliveryBusObservationForTest
	at      time.Time
	held    []engine.EmitIntent
}

func newStageTimerEntryProof(t *testing.T, backend string, open pipelineDeliveryNativeOpenerForTest) *stageTimerEntryProof {
	return newStageTimerEntryVariantProof(t, backend, "1h", "advance", open)
}

func newStageTimerEntryVariantProof(t *testing.T, backend, delay, action string, open pipelineDeliveryNativeOpenerForTest) *stageTimerEntryProof {
	t.Helper()
	left, right := "advances_to: s2", "advances_to: s1"
	switch action {
	case "self":
		left, right = "advances_to: s1", "advances_to: s2"
	case "emit_only":
		left, right = "emit: observed", "emit: observed"
	case "public":
		left += ", emit: observed"
		right += ", emit: observed"
	}
	files := map[string]string{
		"schema.yaml": fmt.Sprintf(`name: timer-entry-authority
stages:
  s1:
    timers:
      - {id: s1_to_s2, after: %s, %s}
  s2:
    timers:
      - {id: s2_to_s1, after: %s, %s}
`, delay, left, delay, right),
		"entities.yaml": "test_entity:\n  marker: text\n",
		"events.yaml":   "enter.s1:\nenter.s2:\nnote:\nobserved:\n",
		"nodes.yaml": `router:
  execution_type: system_node
  event_handlers:
    enter.s1: {advances_to: s1}
    enter.s2: {advances_to: s2}
    note:
      data_accumulation:
        writes:
          - target_field: marker
            value: |-
              'updated'
    observed:
      data_accumulation:
        writes:
          - target_field: marker
            value: |-
              'observed'
`,
	}
	if action == "connected_qualified" || action == "connected_renamed" {
		event, rename := "enter.s2", ""
		if action == "connected_renamed" {
			event, rename = "exported", ", rename: enter.s2"
		}
		files["schema.yaml"] += "pins:\n  inputs: [enter.s2]\nconnect:\n  - {event: " + event + ", from: producer, to: ." + rename + "}\n"
		files["events.yaml"] = "enter.s1:\nnote:\nobserved:\n"
		files["producer/schema.yaml"] = "name: producer\npins:\n  outputs: [" + event + "]\n"
		files["producer/events.yaml"] = event + ":\n"
		files["producer/entities.yaml"] = "test_entity: {}\n"
	}
	bundle := loadWorkflowTempBundle(t, files)
	fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
	f := &stageTimerEntryProof{t: t, fixture: fixture, pc: pc, ctx: ctx, at: canonicalWorkflowTimerTime(time.Now().Add(-2 * time.Hour))}
	f.observe()
	instance := constructedScenarioInstanceForTest(t, pc.SemanticSource(), ctx, ".")
	instance.CreatedAt, instance.EnteredStageAt = f.at, f.at
	prepared, lifecycle, err := pc.PrepareInitialEntryLifecycle(ctx, testRunScopedWorkflowInstanceFromContext(ctx, instance.StorageRef), instance, f.at)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := fixture.ConstructInitial(ctx, prepared, lifecycle)
	if err != nil {
		t.Fatal(err)
	}
	if err := pc.FinalizeInitialEntryLifecycle(ctx, committed); err != nil {
		t.Fatal(err)
	}
	return f
}

type stageTimerEntryEventReadFunc func(context.Context, string, string) (events.Event, error)

func (f stageTimerEntryEventReadFunc) LoadWorkflowStageEntryEvent(ctx context.Context, run, event string) (events.Event, error) {
	return f(ctx, run, event)
}

func VerifyStageTimerEntryReadFailuresConserveRowsBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, fault := range []string{"missing_capability", "missing_event", "foreign_run", "wrong_event", "read_error", "contradictory_entry"} {
			t.Run(backend+"/"+fault, func(t *testing.T) {
				f := newStageTimerEntryProof(t, backend, open)
				f.enter("s2")
				f.enter("s1")
				accepted := f.fire(true)
				before := f.snapshot()
				entry, _, err := workflowlifecycle.LoadStageEntry(before.Instance.Bookkeeping)
				if err != nil {
					t.Fatal(err)
				}
				original := f.pc.workflowTimers.entryEvents
				readError := errors.New("entry read interrupted independently")
				if fault == "missing_capability" {
					f.pc.workflowTimers.entryEvents = nil
				} else {
					f.pc.workflowTimers.entryEvents = stageTimerEntryEventReadFunc(func(ctx context.Context, run, id string) (events.Event, error) {
						switch fault {
						case "missing_event":
							id = uuid.NewString()
						case "foreign_run":
							run = uuid.NewString()
						case "wrong_event":
							id = accepted.ID()
						case "read_error":
							return events.Event{}, readError
						}
						return original.LoadWorkflowStageEntryEvent(ctx, run, id)
					})
				}
				if fault == "contradictory_entry" {
					// Component validation of contradictory decoded evidence; no
					// alternate store writer or corruption authority is introduced.
					instance := before.Instance
					instance.Bookkeeping = make(map[string]any, len(before.Instance.Bookkeeping))
					for key, value := range before.Instance.Bookkeeping {
						instance.Bookkeeping[key] = value
					}
					entry.TransitionID = "transition:foreign"
					if err := workflowlifecycle.StoreStageEntry(instance.Bookkeeping, entry); err != nil {
						t.Fatal(err)
					}
					activation := loadWorkflowTimerOwnerActivation(t, f.pc.workflowStore, f.ctx, acceptedActivationIDForStageEntryProof(t, accepted))
					declaration, found, err := f.pc.workflowTimers.workflowTimerDeclarationForActivation(f.ctx, activation)
					if err != nil || !found {
						t.Fatal("missing native timer declaration")
					}
					if current, err := f.pc.workflowTimers.stageTimerEntryCurrent(f.ctx, instance, activation, declaration); err == nil || current {
						t.Fatal("contradictory decoded entry was treated as applicable or harmlessly stale")
					}
				} else {
					recognized, advanced, err := f.pc.handleWorkflowStageTimerFire(f.ctx, accepted)
					if err == nil || !recognized || advanced || (fault == "read_error" && !errors.Is(err, readError)) {
						t.Fatalf("entry read refusal lost its failure: %t/%t/%v", recognized, advanced, err)
					}
				}
				if !reflect.DeepEqual(before, f.snapshot()) {
					t.Fatal("entry read refusal changed header, fields, history, timers or publication")
				}
			})
		}
	}
}

func acceptedActivationIDForStageEntryProof(t *testing.T, event events.Event) string {
	t.Helper()
	occurrence, ok := timeridentity.ParseWorkflowTimerOccurrenceTaskID(event.TaskID())
	if !ok {
		t.Fatal("missing native accepted timer occurrence")
	}
	return occurrence.Activation.ActivationID
}

func VerifyStageTimerConnectedEntryUsesOriginalPublicationBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, action := range []string{"connected_qualified", "connected_renamed"} {
			t.Run(backend+"/"+action, func(t *testing.T) {
				f := newStageTimerEntryVariantProof(t, backend, "1h", action, open)
				constructNativePipelineScenarioForTest(t, f.fixture, f.pc, f.ctx, "producer")
				run := correlation.RunIDFromContext(f.ctx)
				kind := "producer/enter.s2"
				if action == "connected_renamed" {
					kind = "producer/exported"
				}
				envelope := events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: ".", FlowInstance: run, EntityID: run})
				event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(kind), "operator", "", []byte(`{}`), 0, run, envelope,
					eventtest.StaticFlowRoutingSource("producer", "producer", FlowInstanceEntityID("producer")), f.at)
				route := workflowNodeStampedConnectRouteForHandlerEvent(t, f.pc.SemanticSource(), "enter.s2", "router")
				route.Target = events.MustExistingEntityTarget(event.TargetRoute())
				if err := f.fixture.PublishNode(f.ctx, event, route); err != nil {
					t.Fatal(err)
				}
				if handled, err := f.pc.executeNodeHandlerPlanResult(withWorkflowNodeDeliveryRoute(f.ctx, route), pipelineNode(t, ".", "router"), event); err != nil || !handled {
					t.Fatalf("native connected transition: %t/%v", handled, err)
				}
				before := f.snapshot()
				entry, _, err := workflowlifecycle.LoadStageEntry(before.Instance.Bookkeeping)
				if err != nil || entry.EventID != event.ID() || before.Instance.CurrentState != "s2" {
					t.Fatal("connected publication did not own its exact entry")
				}
				compiled, ok := before.Instance.TransitionHistory[len(before.Instance.TransitionHistory)-1].Evidence.Compiled()
				if !ok || compiled.Edge().EventType == string(event.Type()) {
					t.Fatal("control did not distinguish the actual publication from its handler key")
				}
				published, err := f.fixture.PublishedEvent(f.ctx, event.ID())
				if err != nil || published.ID() != event.ID() || published.RunID() != event.RunID() || published.Type() != event.Type() || !published.CreatedAt().Equal(event.CreatedAt()) || !reflect.DeepEqual(published.Payload(), event.Payload()) {
					t.Fatalf("native publication changed its immutable origin facts: %v", err)
				}
				// Origin-read component control only. The existing native A2 fork
				// projection proof establishes the supported inherited entry; this
				// does not reconstruct or execute a historical pending timer.
				for _, origin := range []string{entry.RunID, uuid.NewString()} {
					projected := entry
					projected.RunID, projected.OriginRunID = uuid.NewString(), origin
					projected.EntityID, projected.InstanceID, projected.InstancePath = projected.RunID, projected.RunID, projected.RunID
					read, err := f.pc.workflowTimers.stageEntryOriginEvent(f.ctx, projected)
					if origin == entry.RunID {
						if err != nil || !reflect.DeepEqual(published, read) {
							t.Fatalf("projected origin read lost original provenance: %v", err)
						}
					} else if err == nil {
						t.Fatal("foreign projected origin fell back to the current run")
					}
				}
				if !reflect.DeepEqual(before, f.snapshot()) {
					t.Fatal("entry origin read changed stored source facts")
				}
				timerEvent := f.fire(false)
				after := f.snapshot()
				if after.Instance.CurrentState != "s1" || after.Instance.TransitionHistory[len(after.Instance.TransitionHistory)-1].TriggerEventID != timerEvent.ID() {
					t.Fatal("qualified/renamed entry's genuine current timer did not advance")
				}
			})
		}
	}
}

func (f *stageTimerEntryProof) observe() {
	f.t.Helper()
	f.bus = observeNativePipelineDeliveryBusForTest(f.t, f.pc)
	f.pc.workflowTimers.publication, f.pc.workflowTimers.dispatcher, f.pc.workflowTimers.logger = f.bus, f.bus.EngineDispatcher(), f.bus
	f.pc.timerScheduler = newWorkflowTimerTestScheduler(f.t, f.pc.workOwner)
	if err := f.pc.timerScheduler.PrepareStartup(); err != nil {
		f.t.Fatal(err)
	}
	if err := f.pc.workflowTimers.bindScheduler(f.pc.timerScheduler); err != nil {
		f.t.Fatal(err)
	}
	pc := f.pc
	f.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := pc.StopWorkflowTimerLifecycle(ctx); err != nil {
			f.t.Error(err)
		}
	})
}

func (f *stageTimerEntryProof) enter(stage string) {
	f.t.Helper()
	run := correlation.RunIDFromContext(f.ctx)
	evt := nativeWorkflowJoinEventForTest(f.ctx, ".", run, run, "enter."+stage, []byte(`{}`), f.at)
	dispatchNativeWorkflowJoinEventForTest(f.t, f.fixture, f.pc, f.ctx, evt, "router")
}

func (f *stageTimerEntryProof) active() WorkflowTimerActivation {
	f.t.Helper()
	rows := listWorkflowTimerOwnerActivations(f.t, f.pc.workflowStore, f.ctx, correlation.RunIDFromContext(f.ctx), true)
	if len(rows) != 1 || rows[0].Ref.Generation.Valid() {
		f.t.Fatalf("expected one non-loop active timer: %+v", rows)
	}
	return rows[0]
}

func (f *stageTimerEntryProof) fire(held bool) events.Event {
	return f.fireAtCut(held, false)
}

func (f *stageTimerEntryProof) fireAtCut(held, holdDispatch bool) events.Event {
	f.t.Helper()
	before := f.bus.committedCount()
	originalInstances, originalTargets := f.pc.workflowStore.instanceReader, f.pc.workflowStore.targetReader
	cut := &stageTimerEntryReadCut{WorkflowInstancePersistenceReader: originalInstances, WorkflowTargetPersistenceReader: originalTargets}
	if held {
		f.pc.workflowStore.instanceReader, f.pc.workflowStore.targetReader = cut, cut
		f.bus.beforeDispatch = func(_ context.Context, intents []engine.EmitIntent) error {
			f.held = append([]engine.EmitIntent(nil), intents...)
			cut.hold = true
			return nil
		}
	}
	if holdDispatch {
		f.bus.beforeDispatch = func(_ context.Context, intents []engine.EmitIntent) error {
			f.held = append([]engine.EmitIntent(nil), intents...)
			return context.Canceled
		}
	}
	outcome, err := fireWorkflowTimerTestWakeup(f.ctx, f.pc, f.active())
	f.bus.beforeDispatch = nil
	f.pc.workflowStore.instanceReader, f.pc.workflowStore.targetReader = originalInstances, originalTargets
	if outcome != WorkflowTimerFireCommitted || (!held && !holdDispatch && err != nil) || (held && ((err != nil && !errors.Is(err, context.Canceled)) || cut.blocked == 0)) || (holdDispatch && !errors.Is(err, context.Canceled)) {
		f.t.Fatalf("durable fire held=%t: outcome=%s error=%v", held, outcome, err)
	}
	if f.bus.committedCount() != before+1 {
		f.t.Fatal("fire did not retain its exact committed publication")
	}
	return f.bus.persistedPublishedEvent(f.t, f.fixture, f.ctx, before)
}

// Authenticate the exact committed occurrence first, then interrupt its
// transition read. Native dispatch retains retry and joins publication ownership.
type stageTimerEntryReadCut struct {
	WorkflowInstancePersistenceReader
	WorkflowTargetPersistenceReader
	hold           bool
	reads, blocked int
}

func (c *stageTimerEntryReadCut) LoadWorkflowInstance(ctx context.Context, owner flowidentity.RunScopedFlowInstance) (WorkflowInstance, bool, error) {
	if c.hold {
		c.reads++
		// Declaration authorization and handler declaration resolution precede
		// the emit-only applicability read. Both retain their original reads.
		if c.reads >= 3 {
			c.blocked++
			return WorkflowInstance{}, false, context.Canceled
		}
	}
	return c.WorkflowInstancePersistenceReader.LoadWorkflowInstance(ctx, owner)
}

func (c *stageTimerEntryReadCut) LoadWorkflowTargetPersistence(ctx context.Context, owner flowidentity.RunScopedFlowInstance, entity identity.EntityID) (WorkflowTargetPersistenceRecord, error) {
	if c.hold {
		c.blocked++
		return WorkflowTargetPersistenceRecord{}, context.Canceled
	}
	return c.WorkflowTargetPersistenceReader.LoadWorkflowTargetPersistence(ctx, owner, entity)
}

func VerifyStageTimerUnacknowledgedCurrentEntryRecoveryBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, owner := range []string{"retained", "reopened"} {
			t.Run(backend+"/"+owner, func(t *testing.T) {
				f := newStageTimerEntryVariantProof(t, backend, "1ns", "advance", open)
				accepted := f.fire(true)
				before := f.snapshot()
				if owner == "reopened" {
					f.reopen()
				}
				if recognized, advanced, err := f.pc.handleWorkflowStageTimerFire(f.ctx, accepted); err != nil || !recognized || !advanced {
					t.Fatalf("current accepted entry was not recoverable: %t/%t/%v", recognized, advanced, err)
				}
				first := f.snapshot()
				if first.Instance.CurrentState != "s2" || first.Events[string(accepted.Type())] != before.Events[string(accepted.Type())] {
					t.Fatal("current recovery failed or republished the occurrence")
				}
				// Exercise a current activation whose entry was itself created by a
				// timer, including its immutable origin activation after reopen.
				second := f.fire(false)
				settled := f.snapshot()
				if settled.Instance.CurrentState != "s1" || settled.Instance.TransitionHistory[len(settled.Instance.TransitionHistory)-1].TriggerEventID != second.ID() {
					t.Fatal("timer-created entry lost its current timer authority")
				}
				if recognized, advanced, err := f.pc.handleWorkflowStageTimerFire(f.ctx, accepted); err != nil || !recognized || advanced || !reflect.DeepEqual(settled, f.snapshot()) {
					t.Fatalf("acknowledged occurrence regained authority after a cycle: %t/%t/%v", recognized, advanced, err)
				}
			})
		}
	}
}

type stageTimerEntryCommitRace struct {
	WorkflowEngineMutationOwner
	command WorkflowEngineMutationCommand
	calls   int
}

func (o *stageTimerEntryCommitRace) CommitWorkflowEngineMutation(ctx context.Context, command WorkflowEngineMutationCommand) (CommittedWorkflowEngineMutation, error) {
	o.command = command
	o.calls++
	return CommittedWorkflowEngineMutation{}, context.Canceled
}

func VerifyStageTimerPreparedEntryCASBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newStageTimerEntryProof(t, backend, open)
			accepted := f.fire(true)
			before := f.snapshot()
			owner := &stageTimerEntryCommitRace{WorkflowEngineMutationOwner: f.pc.workflowStore.engineMutations}
			f.pc.workflowStore.engineMutations = owner
			recognized, advanced, err := f.pc.handleWorkflowStageTimerFire(f.ctx, accepted)
			if !recognized || advanced || !errors.Is(err, context.Canceled) || owner.calls != 1 || !reflect.DeepEqual(before, f.snapshot()) {
				t.Fatalf("pre-commit cut changed its original rows: %t/%t/%v", recognized, advanced, err)
			}
			f.pc.workflowStore.engineMutations = owner.WorkflowEngineMutationOwner
			f.enter("s2")
			f.enter("s1")
			changed := f.snapshot()
			unlock := f.pc.lockWorkflowEntity(owner.command.State.EntityID)
			result, err := owner.WorkflowEngineMutationOwner.CommitWorkflowEngineMutation(f.ctx, owner.command)
			unlock()
			if result.Committed || !failures.IsStateContention(err) || !reflect.DeepEqual(changed, f.snapshot()) {
				t.Fatalf("old evaluated draft bypassed the exact native CAS or changed newer rows: committed=%t error=%v", result.Committed, err)
			}
			if recognized, advanced, err := f.pc.handleWorkflowStageTimerFire(f.ctx, accepted); err != nil || !recognized || advanced || !reflect.DeepEqual(changed, f.snapshot()) {
				t.Fatalf("reevaluation borrowed the newer entry: %t/%t/%v", recognized, advanced, err)
			}
		})
	}
}

func VerifyStageTimerSiblingApplicabilityBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, action := range []string{"self", "emit_only", "public"} {
			t.Run(backend+"/"+action, func(t *testing.T) {
				f := newStageTimerEntryVariantProof(t, backend, "1h", action, open)
				public := action == "public" || action == "emit_only"
				accepted := f.fireAtCut(!public, public)
				f.enter("s2")
				f.enter("s1")
				before := f.snapshot()
				if recognized, advanced, err := f.pc.handleWorkflowStageTimerFire(f.ctx, accepted); err != nil || !recognized || advanced || !reflect.DeepEqual(before, f.snapshot()) {
					t.Fatalf("stale sibling timer authorized a different entry: %t/%t/%v", recognized, advanced, err)
				}
				if public {
					if len(f.held) != 1 {
						t.Fatal("missing original committed output intent")
					}
					if err := f.bus.Bus.EngineDispatcher().DispatchPostCommit(f.ctx, f.held); err != nil {
						t.Fatal(err)
					}
					after := f.snapshot()
					entryBefore, _, beforeErr := workflowlifecycle.LoadStageEntry(before.Instance.Bookkeeping)
					entryAfter, _, afterErr := workflowlifecycle.LoadStageEntry(after.Instance.Bookkeeping)
					if beforeErr != nil || afterErr != nil || entryBefore != entryAfter || after.Instance.CurrentState != "s1" || !reflect.DeepEqual(before.Timers, after.Timers) || !reflect.DeepEqual(before.Instance.TransitionHistory, after.Instance.TransitionHistory) || after.Instance.Fields["marker"] != "observed" || !reflect.DeepEqual(before.Events, after.Events) {
						t.Fatalf("stale timed transition suppressed or changed ordinary accepted output: marker=%v state=%s timers_same=%t history_same=%t entry_same=%t events_same=%t", after.Instance.Fields["marker"], after.Instance.CurrentState, reflect.DeepEqual(before.Timers, after.Timers), reflect.DeepEqual(before.Instance.TransitionHistory, after.Instance.TransitionHistory), entryBefore == entryAfter, reflect.DeepEqual(before.Events, after.Events))
					}
				}
			})
		}
	}
}

func (f *stageTimerEntryProof) reopen() {
	f.t.Helper()
	run, module := correlation.RunIDFromContext(f.ctx), f.pc.module
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := f.pc.StopWorkflowTimerLifecycle(ctx); err != nil {
		f.t.Fatal(err)
	}
	f.fixture = f.fixture.ReopenExecution()
	f.pc = f.fixture.NewCoordinator(PipelineCoordinatorOptions{Module: module})
	f.ctx = correlation.WithRunID(f.fixture.Context, run)
	f.observe()
}

type stageTimerEntrySnapshot struct {
	Instance WorkflowInstance
	Timers   []WorkflowTimerActivation
	History  []PipelineNativeMutationRowForTest
	Events   map[string]int
}

func (f *stageTimerEntryProof) snapshot() stageTimerEntrySnapshot {
	f.t.Helper()
	run := correlation.RunIDFromContext(f.ctx)
	instance, found, err := f.pc.workflowStore.Load(f.ctx, testRunScopedWorkflowInstanceFromContext(f.ctx, run))
	if err != nil || !found {
		f.t.Fatalf("load exact instance: found=%t error=%v", found, err)
	}
	timers := listWorkflowTimerOwnerActivations(f.t, f.pc.workflowStore, f.ctx, run, false)
	counts := map[string]int{}
	for _, timer := range timers {
		kind := timer.EventType
		counts[kind], err = f.fixture.EventCount(f.ctx, run, kind)
		if err != nil {
			f.t.Fatal(err)
		}
	}
	return stageTimerEntrySnapshot{Instance: instance,
		Timers:  timers,
		History: f.fixture.MutationHistory(f.ctx, run, run), Events: counts}
}

func VerifyStageTimerAcceptedOccurrenceRequiresCurrentEntryBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, stage := range []string{"s1", "s2"} {
			for _, delivery := range []string{"already_applied", "delayed_first_handling"} {
				for _, owner := range []string{"retained", "reopened"} {
					t.Run(backend+"/"+stage+"/"+delivery+"/"+owner, func(t *testing.T) {
						f := newStageTimerEntryProof(t, backend, open)
						if stage == "s2" {
							f.enter(stage)
						}
						entry1, _, err := workflowlifecycle.LoadStageEntry(f.snapshot().Instance.Bookkeeping)
						if err != nil {
							t.Fatal(err)
						}
						accepted := f.fire(delivery == "delayed_first_handling")
						other := "s2"
						if stage == other {
							other = "s1"
						}
						if delivery == "delayed_first_handling" {
							f.enter(other)
						}
						f.enter(stage)
						if owner == "reopened" {
							f.reopen()
						}
						before := f.snapshot()
						entry2, _, err := workflowlifecycle.LoadStageEntry(before.Instance.Bookkeeping)
						if err != nil || entry1 == entry2 || !before.Instance.EnteredStageAt.Equal(f.at) {
							t.Fatalf("cycle did not create a distinct entry at the same clock: first=%+v second=%+v error=%v", entry1, entry2, err)
						}
						current := f.active()
						pass, emitted, outcome, err := f.pc.Intercept(f.ctx, accepted)
						after := f.snapshot()
						if err != nil || pass || len(emitted) != 0 || outcome.Committed || !reflect.DeepEqual(before, after) {
							t.Fatalf("stale accepted occurrence owns a different stage entry: pass=%t committed=%t error=%v old_event=%s current_activation=%s state=%s->%s history=%d->%d timers_before=%+v timers_after=%+v", pass, outcome.Committed, err, accepted.ID(), current.Ref.ActivationID, before.Instance.CurrentState, after.Instance.CurrentState, len(before.Instance.TransitionHistory), len(after.Instance.TransitionHistory), before.Timers, after.Timers)
						}
						// A field-only committed revision must not invalidate entry authority.
						run := correlation.RunIDFromContext(f.ctx)
						note := nativeWorkflowJoinEventForTest(f.ctx, ".", run, run, "note", []byte(`{}`), f.at)
						dispatchNativeWorkflowJoinEventForTest(t, f.fixture, f.pc, f.ctx, note, "router")
						if got, _, err := workflowlifecycle.LoadStageEntry(f.snapshot().Instance.Bookkeeping); err != nil || got != entry2 {
							t.Fatal("field-only write changed lifecycle entry")
						}
						currentEvent := f.fire(false)
						final := f.snapshot()
						if final.Instance.CurrentState != other || len(final.Instance.TransitionHistory) == 0 || final.Instance.TransitionHistory[len(final.Instance.TransitionHistory)-1].TriggerEventID != currentEvent.ID() || final.Instance.Fields["marker"] != "updated" {
							t.Fatalf("current-entry timer did not retain its exact transition and field write: state=%s marker=%v history=%+v", final.Instance.CurrentState, final.Instance.Fields["marker"], final.Instance.TransitionHistory)
						}
						if recognized, advanced, err := f.pc.handleWorkflowStageTimerFire(f.ctx, currentEvent); err != nil || !recognized || advanced || !reflect.DeepEqual(final, f.snapshot()) {
							t.Fatalf("current-entry duplicate changed its settled projection: %t/%t/%v", recognized, advanced, err)
						}
					})
				}
			}
		}
	}
}
