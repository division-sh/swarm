package pipeline_test

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
	"github.com/google/uuid"
)

type issue642JoinReaderScenario struct {
	name           string
	count          int
	deadline       bool
	noJoin         bool
	missingTimeout bool
	stageExit      bool
	emptyList      bool
}

type issue642JoinReaderFixture struct {
	ctx        context.Context
	selected   gateRecoveryStoreCase
	bus        *bus.EventBus
	pc         *pipeline.PipelineCoordinator
	owner      flowidentity.RunScopedFlowInstance
	node       identity.ExecutableNode
	probe      *lifecycleprobe.Probe
	logger     *exactJoinRuntimeLogger
	generic    genericschedule.Store
	initial    joinruntime.Activation
	completion *genericschedule.Activation
	timeout    *genericschedule.Activation
	unrelated  []genericschedule.Activation
}

// This proves native writer -> captured fixed cut -> public fork planner. The
// paused schedule driver prevents this reader proof from serving a continuation.
func TestIssue642JoinWriterFixedCutReaderBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			for _, scenario := range []issue642JoinReaderScenario{
				{name: "no_join_scoped_keys", noJoin: true},
				{name: "zero_without_deadline"},
				{name: "zero_with_deadline", deadline: true},
				{name: "nonempty_completed", count: 1, deadline: true},
				{name: "missing_timeout", count: 1, deadline: true, missingTimeout: true},
				{name: "zero_stage_exit_without_deadline", stageExit: true},
				{name: "zero_stage_exit_with_deadline", stageExit: true, deadline: true},
				{name: "empty_list_stage_exit_without_deadline", stageExit: true, emptyList: true},
				{name: "empty_list_stage_exit_with_deadline", stageExit: true, emptyList: true, deadline: true},
			} {
				t.Run(scenario.name, func(t *testing.T) {
					f := issue642NewJoinReaderFixture(t, selected, scenario)
					if scenario.count > 0 {
						issue642CloseJoinReaderArm(t, f)
					}
					if scenario.stageExit {
						issue642ExitEmptyJoinReaderArm(t, f)
					}
					issue642RequireJoinReaderCut(t, f, scenario)
				})
			}
		})
	}
}

func issue642NewJoinReaderFixture(t *testing.T, selected gateRecoveryStoreCase, scenario issue642JoinReaderScenario) *issue642JoinReaderFixture {
	t.Helper()
	files := a2CountJoinFiles(scenario.count)
	files["schema.yaml"] += "    - reader.cut\n"
	files["events.yaml"] += "reader.cut:\npoll.tick:\n"
	if scenario.deadline {
		files["nodes.yaml"] = strings.Replace(files["nodes.yaml"], "        output: payload.result\n", "        output: payload.result\n        deadline: {after: 1h, from: stage_entry}\n", 1)
		files["nodes.yaml"] += "        on_deadline: {advances_to: ready}\n"
	}
	if scenario.emptyList {
		files["entities.yaml"] += "  members_list: \"[text]\"\n"
		files["nodes.yaml"] = strings.Replace(files["nodes.yaml"], "members: {count: 0, by: payload.member_id}", "members: {from: state.members_list, by: payload.member_id}", 1)
	}
	subscriptions := []events.EventType{"item.completed"}
	if scenario.stageExit {
		files["schema.yaml"] = strings.Replace(files["schema.yaml"], "  awaiting: {}\n", "  awaiting: {}\n  aborted: {}\n", 1)
		files["schema.yaml"] += "    - manual.abort\n"
		files["events.yaml"] += "manual.abort:\n"
		files["nodes.yaml"] += "    manual.abort:\n      advances_to: aborted\n"
		subscriptions = append(subscriptions, "manual.abort")
	}
	if scenario.noJoin {
		files["nodes.yaml"] = "collector:\n  execution_type: system_node\n  event_handlers:\n    item.completed: {}\n"
	}
	bundle := loadPipelineLifecycleFixtureBundle(t, files)
	source := semanticview.Wrap(bundle)
	boot, err := contracts.BootBundleIdentity(bundle)
	if err != nil {
		t.Fatal(err)
	}
	fact, err := correlation.NewSourceArtifactFact(boot.BundleHash)
	if err != nil {
		t.Fatal(err)
	}
	runID := uuid.NewString()
	ctx := withLiveGateExecution(correlation.WithRunID(testAuthorActivityContextForSource(t, t.Context(), fact), runID))
	at := runlifecycle.CanonicalTimestamp(time.Now().UTC())
	storetest.RequireRun(t, ctx, selected.events.(storetest.RunFixtureStore), storetest.RunFixture{
		RunID: runID, Origin: storetest.ScenarioSetupOrigin(), Artifact: bundle.SourceArtifact, BundleHash: fact.BundleHash(), StartedAt: at,
	})
	f := &issue642JoinReaderFixture{ctx: ctx, selected: selected, probe: lifecycleprobe.New(), logger: &exactJoinRuntimeLogger{}, generic: selected.events.(genericschedule.Store)}
	f.node = externalPipelineSourceNode(t, source, ".", "collector")
	f.bus, err = newScopedTestEventBus(t, selected.events, bus.EventBusOptions{
		ContractBundle: source, SourceArtifactFact: fact, WorkOwner: pipelineExternalTestWorkOwnerForSource(t, fact),
		PayloadAdmitter: runtimepkg.NewRuntimePayloadAdmitter(nil, source, fact), TestLifecycleProbe: f.probe, Logger: f.logger,
	}, "platform.join_complete", "platform.join_timeout")
	if err != nil {
		t.Fatal(err)
	}
	schedules, _ := newExactJoinScheduleLifecycleForTest(t, ctx, selected, f.bus)
	f.pc = newGateRecoveryCoordinator(f.bus, selected, pipeline.PipelineCoordinatorOptions{
		Module: proposedEffectProofModule{source: source, nodes: []pipeline.WorkflowNode{
			{Node: f.node, Subscriptions: subscriptions, ExecutionType: contracts.SystemNodeExecutionType},
		}},
		SourceArtifactFact: fact, WorkOwner: pipelineExternalTestWorkOwnerForSource(t, fact),
		GenericSchedules: schedules, TestLifecycleProbe: f.probe,
	})
	f.bus.SetInterceptors(f.pc)
	f.owner = flowidentity.RunScopedFlowInstance{RunID: runID, Route: flowidentity.StoredRoute(".", runID, runID)}
	fields := map[string]any{"final_expected": int64(0), "final_completed": int64(0), "final_results": []any{}, "final_reason": ""}
	if scenario.emptyList {
		fields["members_list"] = []any{}
	}
	initial, lifecycle, err := f.pc.PrepareInitialEntryLifecycle(ctx, f.owner, pipeline.WorkflowInstance{
		InstanceID: runID, StorageRef: runID, EntityID: runID, WorkflowName: semanticview.RootExecutionFlowID(source), WorkflowVersion: source.WorkflowVersion(),
		CurrentState: "awaiting", StageDefined: true, EntityType: "count_state", EnteredStageAt: at, CreatedAt: at,
		Fields: fields,
	}, at)
	if err != nil {
		t.Fatal(err)
	}
	if !scenario.noJoin {
		f.initial = exactJoinPersistedArmForOwner(t, initial, f.owner)
		if len(lifecycle.Schedules) != 1 || f.initial.Expected() != scenario.count {
			t.Fatalf("canonical writer did not prepare its exact initial schedule/cardinality: schedules=%d arm=%+v", len(lifecycle.Schedules), f.initial)
		}
		if scenario.count == 0 && (!f.initial.EmptyCompletionWasArmed() || lifecycle.Schedules[0].Command.EventType != "platform.join_complete" || f.initial.DeadlineAt.IsZero() == scenario.deadline) {
			t.Fatal("zero fixture did not follow the real immediate-completion writer branch")
		}
		if (f.initial.MemberCount == nil) != scenario.emptyList || len(f.initial.Members) != 0 {
			t.Fatal("canonical writer changed explicit count/list membership evidence")
		}
	}
	if scenario.missingTimeout {
		if lifecycle.Schedules[0].Command.EventType != "platform.join_timeout" || f.initial.Status != joinruntime.StatusOpen {
			t.Fatal("hostile omission must remove exactly the initial nonempty timeout")
		}
		// Deliberately omit only this native construction write. Keep the real
		// prepared arm/receipt, then let normal arrival execution close that arm.
		lifecycle.Schedules = nil
	}
	command, err := flowactivationfixture.Command(ctx, initial, lifecycle, at)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := selected.events.(bus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(ctx, command)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("native construction was not acknowledged: created=%t acknowledged=%t err=%v", committed.Created, committed.Acknowledged, err)
	}
	if len(committed.Lifecycle.GenericScheduleActivations) != len(lifecycle.Schedules) {
		t.Fatal("native constructor changed its prepared schedule census")
	}
	if !scenario.noJoin && scenario.count == 0 {
		completion := committed.Lifecycle.GenericScheduleActivations[0]
		if completion.Command.TaskID != f.initial.TimerTaskID() || completion.Command.EventType != "platform.join_complete" || !completion.InitialDueAt.Equal(f.initial.ArmedAt) {
			t.Fatal("native constructor changed the exact immediate completion or invented a timeout")
		}
		f.completion = &completion
	}
	if scenario.count > 0 && !scenario.missingTimeout {
		timeout := committed.Lifecycle.GenericScheduleActivations[0]
		if timeout.Command.TaskID != f.initial.TimerTaskID() || !timeout.InitialDueAt.Equal(f.initial.DeadlineAt) {
			t.Fatal("native timeout lost the original arm or deadline")
		}
		f.timeout = &timeout
	}
	if err := f.pc.FinalizeInitialEntryLifecycle(ctx, committed.Lifecycle); err != nil {
		t.Fatal(err)
	}
	f.unrelated = issue642JoinReaderScopedSchedules(t, f, at)
	return f
}

func issue642JoinReaderScopedSchedules(t *testing.T, f *issue642JoinReaderFixture, at time.Time) []genericschedule.Activation {
	t.Helper()
	payload, err := canonicaljson.FromGo(map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	source, err := events.NewRootRoutingSource(f.owner.RunID)
	if err != nil {
		t.Fatal(err)
	}
	var activations []genericschedule.Activation
	var scopes []string
	for _, owner := range []string{"reader-health-a", "reader-health-b"} {
		command := genericschedule.AdmissionCommand{
			ScheduleKey: "poll", RunID: f.owner.RunID, EntityID: f.owner.RunID, OwnerKind: genericschedule.OwnerSystem, OwnerID: owner,
			EventType: "poll.tick", Payload: payload, RoutingSource: source, ExecutionMode: executionmode.Live, Due: genericschedule.AbsoluteDue(at.Add(time.Hour)),
		}
		scope, err := command.ScopeKey()
		if err != nil {
			t.Fatal(err)
		}
		scopes = append(scopes, scope)
		committed, err := f.generic.AdmitGenericScheduleOutcome(f.ctx, command)
		if err != nil || !committed.Acknowledged || committed.Result.Outcome != genericschedule.AdmissionCreated {
			t.Fatalf("canonical scoped schedule admission: acknowledged=%t outcome=%s err=%v", committed.Acknowledged, committed.Result.Outcome, err)
		}
		activations = append(activations, committed.Result.Activation)
	}
	if scopes[0] == scopes[1] || activations[0].ID == activations[1].ID {
		t.Fatal("same-key fixtures must have distinct canonical scopes and native identities")
	}
	return activations
}

func issue642CloseJoinReaderArm(t *testing.T, f *issue642JoinReaderFixture) {
	t.Helper()
	event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "item.completed", "operator", "", []byte(`{"member_id":"member-a","result":{"value":"member-a"}}`), 0, f.owner.RunID,
		events.EnvelopeForEntityID(events.EventEnvelope{}, f.owner.RunID), eventtest.RootRoutingSource(f.owner.RunID), time.Now().UTC())
	if err := f.bus.PublishAcknowledged(f.ctx, event); err != nil {
		t.Fatal(err)
	}
	a2KnownTargetWaitForSettlement(t, f.ctx, f.bus, f.probe, event, f.node.Key(), "completed", "delivered", f.logger)
	arm := exactJoinPersistedArmForOwner(t, issue642JoinReaderInstance(t, f), f.owner)
	if arm.Status != joinruntime.StatusClosed || arm.CloseReason != joinruntime.CloseReasonComplete || arm.Expected() != 1 || arm.Completed() != 1 || !arm.OutcomePending || arm.OutcomeFired || arm.EmptyCompletionWasArmed() || !arm.JoinRef().Equal(f.initial.JoinRef()) || !arm.DeadlineAt.Equal(f.initial.DeadlineAt) {
		t.Fatalf("real arrival did not preserve its nonempty arm and pending continuation: %+v", arm)
	}
	if f.timeout != nil {
		actual, found, err := f.generic.LoadGenericScheduleActivation(f.ctx, f.timeout.ID)
		if err != nil || !found || actual.Status != genericschedule.StatusCancelled || actual.CancelCause != "join_closed" || !actual.InitialDueAt.Equal(f.initial.DeadlineAt) {
			t.Fatalf("real completion did not retain the canceled original timeout: found=%t timer=%+v err=%v", found, actual, err)
		}
		f.timeout = &actual
	}
}

func issue642JoinReaderInstance(t *testing.T, f *issue642JoinReaderFixture) pipeline.WorkflowInstance {
	t.Helper()
	instance, found, err := f.pc.Load(f.ctx, f.owner)
	if err != nil || !found {
		t.Fatalf("load real join receiver: found=%t err=%v", found, err)
	}
	return instance
}

func issue642ExitEmptyJoinReaderArm(t *testing.T, f *issue642JoinReaderFixture) {
	t.Helper()
	beforeTimers := issue642JoinReaderTimerRows(t, f)
	event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "manual.abort", "operator", "", []byte(`{}`), 0, f.owner.RunID,
		events.EnvelopeForEntityID(events.EventEnvelope{}, f.owner.RunID), eventtest.RootRoutingSource(f.owner.RunID), time.Now().UTC())
	if err := f.bus.PublishAcknowledged(f.ctx, event); err != nil {
		t.Fatal(err)
	}
	a2KnownTargetWaitForSettlement(t, f.ctx, f.bus, f.probe, event, f.node.Key(), "completed", "delivered", f.logger)
	instance := issue642JoinReaderInstance(t, f)
	arm := exactJoinPersistedArmForOwner(t, instance, f.owner)
	if instance.CurrentState != "aborted" || len(instance.TransitionHistory) != 1 || instance.TransitionHistory[0].TriggerEventID != event.ID() ||
		instance.TransitionHistory[0].From != "awaiting" || instance.TransitionHistory[0].To != "aborted" ||
		arm.Status != joinruntime.StatusClosed || arm.CloseReason != joinruntime.CloseReasonStageExit || arm.OutcomePending || arm.OutcomeFired || !arm.TimerCancelled ||
		arm.Expected() != 0 || arm.Completed() != 0 || !arm.JoinRef().Equal(f.initial.JoinRef()) || arm.TimerHandle() != f.initial.TimerHandle() ||
		!arm.ArmedAt.Equal(f.initial.ArmedAt) || !arm.FireAt.Equal(f.initial.FireAt) || !arm.DeadlineAt.Equal(f.initial.DeadlineAt) ||
		!reflect.DeepEqual(arm.MemberCount, f.initial.MemberCount) || !reflect.DeepEqual(arm.Members, f.initial.Members) {
		t.Fatalf("actual stage-exit writer changed its empty completion admission evidence: instance=%+v arm=%+v", instance, arm)
	}
	if f.completion == nil {
		t.Fatal("stage exit lacks its original native completion schedule")
	}
	completion, found, err := f.generic.LoadGenericScheduleActivation(f.ctx, f.completion.ID)
	if err != nil || !found || completion.Status != genericschedule.StatusCancelled || completion.CancelCause != "join_stage_exit" ||
		completion.ImmutableHash != f.completion.ImmutableHash || !completion.InitialDueAt.Equal(f.completion.InitialDueAt) ||
		completion.CurrentEventID != "" || !completion.FiredAt.IsZero() || !completion.AcceptedAt.IsZero() {
		t.Fatalf("stage exit did not retain its exact canceled, unpublished completion: found=%t schedule=%+v err=%v", found, completion, err)
	}
	if err := genericschedule.ValidateWorkflowJoinScheduleRelation(arm, completion); err != nil {
		t.Fatal(err)
	}
	if after := issue642JoinReaderTimerRows(t, f); len(after.Rows) != len(beforeTimers.Rows) {
		t.Fatal("stage exit created a timeout or replaced its original completion row")
	}
	f.completion = &completion
}

func issue642JoinReaderTimerRows(t *testing.T, f *issue642JoinReaderFixture) storetest.SelectedForkStorageTableSnapshot {
	t.Helper()
	snapshot, err := storetest.ReadSelectedForkApplicationStorageSnapshot(f.ctx, f.selected.events)
	if err != nil {
		t.Fatal(err)
	}
	timers, found := snapshot["timers"]
	if !found {
		t.Fatal("canonical native observation omitted the timers table")
	}
	return timers
}

func issue642RequireJoinReaderCut(t *testing.T, f *issue642JoinReaderFixture, scenario issue642JoinReaderScenario) {
	t.Helper()
	before := issue642JoinReaderInstance(t, f)
	beforeTimers := issue642JoinReaderTimerRows(t, f)
	marker := eventtest.ExistingRunRootIngress(uuid.NewString(), "reader.cut", "operator", "", []byte(`{}`), 0, f.owner.RunID, events.EventEnvelope{}, time.Now().UTC())
	if err := f.bus.PublishAcknowledged(f.ctx, marker); err != nil {
		t.Fatal(err)
	}
	wait, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	if err := f.bus.WaitForQuiescence(wait); err != nil {
		t.Fatal(err)
	}
	storetest.CaptureRunForkSnapshot(t, f.ctx, f.selected.events, f.owner.RunID)
	planner := f.selected.events.(interface {
		PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
	})
	plan, err := planner.PlanRunFork(f.ctx, runfork.RunForkPlanRequest{SourceRunID: f.owner.RunID, At: marker.ID()})
	if scenario.missingTimeout {
		arm := exactJoinPersistedArmForOwner(t, before, f.owner)
		handle, handleErr := timeridentity.JoinTimeoutHandle(arm.JoinRef())
		if handleErr != nil || err == nil || !strings.Contains(err.Error(), "lacks schedule "+handle.TaskID()) {
			t.Fatalf("hostile native missing-timeout history did not reach its exact reader refusal: handle_err=%v err=%v", handleErr, err)
		}
	} else {
		if err != nil || plan.SourceRunID != f.owner.RunID || plan.ForkPoint.EventID != marker.ID() || plan.ForkPoint.EventName != "reader.cut" || plan.ForkPoint.Revision <= 0 {
			t.Fatalf("native fixed-cut planner: point=%+v err=%v", plan.ForkPoint, err)
		}
		issue642RequireCapturedJoinReaderState(t, f, scenario, before, plan)
	}
	if !reflect.DeepEqual(before, issue642JoinReaderInstance(t, f)) {
		t.Fatal("capture/planning changed the real receiver state")
	}
	if !reflect.DeepEqual(beforeTimers, issue642JoinReaderTimerRows(t, f)) {
		t.Fatal("capture/planning changed native timers or created a timeout")
	}
	for _, expected := range f.unrelated {
		actual, found, err := f.generic.LoadGenericScheduleActivation(f.ctx, expected.ID)
		if err != nil || !found || !reflect.DeepEqual(expected.Canonical(), actual.Canonical()) {
			t.Fatalf("join reader changed an unrelated same-key schedule: found=%t err=%v", found, err)
		}
	}
}

func issue642RequireCapturedJoinReaderState(t *testing.T, f *issue642JoinReaderFixture, scenario issue642JoinReaderScenario, before pipeline.WorkflowInstance, plan runfork.RunForkPlan) {
	t.Helper()
	want := 1
	if scenario.noJoin {
		want = 0
	} else if scenario.count > 0 {
		want = 2
	}
	if len(plan.JoinSchedules) != want || len(plan.Entities) != 1 || plan.Entities[0].EntityID != f.owner.RunID || plan.Entities[0].CurrentState != before.CurrentState {
		t.Fatalf("native planner lost its exact join/receiver census: schedules=%d entities=%d want_schedules=%d", len(plan.JoinSchedules), len(plan.Entities), want)
	}
	if scenario.noJoin {
		return
	}
	arm := exactJoinPersistedArmForOwner(t, before, f.owner)
	buckets, err := joinruntime.PersistedBuckets(plan.Entities[0].Accumulator)
	if err != nil {
		t.Fatal(err)
	}
	joins, err := joinruntime.List(buckets)
	if err != nil || len(joins) != 1 || !reflect.DeepEqual(arm, joins[0]) {
		t.Fatalf("fixed-cut reader changed retained members, outputs, deadline or arm: joins=%d err=%v", len(joins), err)
	}
	for _, captured := range plan.JoinSchedules {
		actual, found, err := f.generic.LoadGenericScheduleActivation(f.ctx, captured.ID)
		if err != nil || !found || !reflect.DeepEqual(captured.Canonical(), actual.Canonical()) {
			t.Fatalf("fixed-cut schedule differs from the real native writer: found=%t err=%v", found, err)
		}
		if err := genericschedule.ValidateWorkflowJoinScheduleRelation(arm, captured); err != nil {
			t.Fatal(err)
		}
		if captured.Command.ScheduleKey == "poll" || captured.CurrentEventID != "" || !captured.FiredAt.IsZero() {
			t.Fatal("reader substituted an unrelated schedule or served the retained continuation")
		}
		if captured.Command.EventType == "platform.join_timeout" && (f.timeout == nil || captured.ID != f.timeout.ID || captured.Status != genericschedule.StatusCancelled) {
			t.Fatal("reader substituted the original canceled timeout")
		}
		if scenario.stageExit && (f.completion == nil || !reflect.DeepEqual(captured.Canonical(), f.completion.Canonical()) || captured.Status != genericschedule.StatusCancelled || captured.CancelCause != "join_stage_exit") {
			t.Fatal("reader lost the original canceled empty completion or invented a timeout")
		}
	}
}
