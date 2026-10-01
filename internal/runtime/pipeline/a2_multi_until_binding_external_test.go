package pipeline_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/division-sh/swarm/internal/testutil/flowroutefixture"
	"github.com/google/uuid"
)

// Existing receivers use real lifecycle commits, EventBus delivery and selected
// stores. Fixture materialization/readiness is not eager-construction or E boot proof.
func TestA2MultiUntilIndependentRecipientEntriesAndRestartOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			runID, key := uuid.NewString(), uuid.NewString()
			insertGateRecoveryRun(t, selected, runID)
			ctx := withLiveGateExecution(correlation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
			source := semanticview.Wrap(loadPipelineLifecycleFixtureBundle(t, a2MultiUntilBindingFiles()))
			worker := externalPipelineSourceNode(t, source, ".", "worker")
			flows := []string{"orders", "mirror"}
			paths := []string{"orders/" + key, "mirror/" + key}
			collectors, dispatchers := make([]identity.ExecutableNode, 2), make([]identity.ExecutableNode, 2)
			for index, flow := range flows {
				collectors[index] = externalPipelineSourceNode(t, source, flow, "collector")
				dispatchers[index] = externalPipelineSourceNode(t, source, flow, "dispatcher")
			}
			nodes, err := pipeline.LoadWorkflowNodes(source)
			if err != nil || len(source.WorkflowJoins()) != 4 {
				t.Fatalf("compiled multi-until module: joins=%d err=%v", len(source.WorkflowJoins()), err)
			}
			module := proposedEffectProofModule{source: source, nodes: nodes}
			logger := &exactJoinRuntimeLogger{}
			probe := lifecycleprobe.New()
			newBus := func() *runtimebus.EventBus {
				t.Helper()
				bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
					ContractBundle: source, TestLifecycleProbe: probe, Logger: logger,
				}, "platform.join_complete", "platform.join_timeout")
				if err != nil {
					t.Fatal(err)
				}
				return bus
			}
			bus := newBus()
			schedules, _ := newExactJoinScheduleLifecycleForTest(t, ctx, selected, bus)
			pc := newGateRecoveryCoordinator(bus, selected, pipeline.PipelineCoordinatorOptions{
				Module: module, GenericSchedules: schedules, TestLifecycleProbe: probe,
			})
			bus.SetInterceptors(pc)
			now := time.Now().UTC()
			if _, err := pc.MaterializeInitialEntry(ctx, testRunScopedWorkflowInstanceForRun(runID, runID), pipeline.WorkflowInstance{
				InstanceID: runID, StorageRef: runID, EntityID: runID, WorkflowName: source.WorkflowName(), WorkflowVersion: source.WorkflowVersion(),
				CurrentState: "active", EntityType: "root_state", Fields: map[string]any{"work_count": int64(0)},
			}, now); err != nil {
				t.Fatal(err)
			}
			publishRoutes := func() {
				t.Helper()
				for _, path := range paths {
					if err := flowroutefixture.Publish(bus, runtimebus.FlowInstanceRouteMaterializationRequest{
						Identity: testRunScopedWorkflowInstanceForRun(runID, path),
					}); err != nil {
						t.Fatal(err)
					}
				}
			}
			for index, path := range paths {
				readiness := pipeline.DynamicFlowRuntimeReadinessPlan{
					Identity: flowidentity.Instance{TemplateID: flows[index], ScopeKey: flows[index], InstanceID: key,
						InstancePath: path, EntityID: flowidentity.EntityID(path), HasStoredPath: true},
					RunID: runID, BundleHash: authorActivityTestSourceArtifactFact.BundleHash(), WorkflowVersion: source.WorkflowVersion(), ExecutionMode: executionmode.Live,
				}
				if _, err := pc.MaterializeInitialEntry(ctx, testRunScopedWorkflowInstanceForRun(runID, path), pipeline.WorkflowInstance{
					InstanceID: key, StorageRef: path, EntityID: flowidentity.EntityID(path), WorkflowName: flows[index], WorkflowVersion: source.WorkflowVersion(),
					Mode: "template", RuntimeReadiness: &readiness, CurrentState: "awaiting", EntityType: "order_state",
					Fields: map[string]any{"order_id": key, "expected": []any{"a", "b"}, "halt_count": int64(0)},
				}, now); err != nil {
					t.Fatal(err)
				}
				markGateRecoveryTopologyReadyFixture(t, selected, readiness, now)
			}
			publishRoutes()
			load := func(index int) pipeline.WorkflowInstance {
				t.Helper()
				instance, found, err := pc.Load(ctx, testRunScopedWorkflowInstanceForRun(runID, paths[index]))
				if err != nil || !found {
					t.Fatalf("load actual receiver %d: found=%v err=%v", index, found, err)
				}
				return instance
			}
			entry := func(index int) timeridentity.StageEntryRef {
				t.Helper()
				ref, found, err := workflowlifecycle.LoadStageEntry(load(index).Bookkeeping)
				if err != nil || !found {
					t.Fatalf("actual receiver entry: found=%v err=%v", found, err)
				}
				return ref
			}
			publishDirected := func(index int, name string, payload any, node identity.ExecutableNode) events.Event {
				t.Helper()
				raw, err := json.Marshal(payload)
				if err != nil {
					t.Fatal(err)
				}
				event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(paths[index]+"/"+name), "operator", "", raw, 0,
					runID, events.EnvelopeForEntityID(events.EventEnvelope{}, flowidentity.EntityID(paths[index])),
					eventtest.ConcreteTemplateRoutingSource(flows[index], paths[index], flowidentity.EntityID(paths[index])), time.Now().UTC())
				if err := bus.PublishAcknowledged(ctx, event); err != nil {
					t.Fatal(err)
				}
				a2KnownTargetWaitForSettlement(t, ctx, bus, probe, event, node.Key(), "completed", "delivered", logger)
				return event
			}
			reenter := func(index int) {
				t.Helper()
				for _, name := range []string{"manual.abort", "dispatch.completed"} {
					publishDirected(index, name, map[string]any{}, dispatchers[index])
				}
			}
			original := []timeridentity.StageEntryRef{entry(0), entry(1)}
			reenter(0)
			captured := []timeridentity.StageEntryRef{entry(0), entry(1)}
			if captured[0] == original[0] || captured[1] != original[1] || captured[0] == captured[1] {
				t.Fatal("A E2/B E1 must be independent actual lifecycle entries")
			}
			oldA := a2MultiUntilEntryArms(t, load(0), original[0])
			for _, arm := range oldA {
				if arm.Status != joinruntime.StatusClosed || arm.OutcomePending {
					t.Fatalf("A's superseded entry was not closed before until publication: %#v", arm)
				}
			}
			var replay []events.Event
			for _, arrival := range []struct {
				index               int
				name, member, value string
			}{
				{0, "item.completed", "a", "A-E2-primary-a"}, {0, "alternate.completed", "z", "A-E2-alternate-z"},
				{1, "item.completed", "b", "B-E1-primary-b"}, {1, "alternate.completed", "z", "B-E1-alternate-z"},
				{1, "alternate.completed", "x", "B-E1-alternate-x"},
			} {
				event := publishDirected(arrival.index, arrival.name, map[string]any{
					"member_id": arrival.member, "result": map[string]any{"value": arrival.value},
				}, collectors[arrival.index])
				prepared, found, err := selected.events.LoadPreparedPublishEvent(ctx, event.ID())
				if err != nil || !found || len(prepared.DeliveryRoutes) != 1 || len(prepared.DeliveryRoutes[0].Context.Joins) != 1 ||
					prepared.DeliveryRoutes[0].Context.Joins[0].Disposition != events.JoinAdmissionBound ||
					prepared.DeliveryRoutes[0].Context.Joins[0].Ref.StageEntry() != captured[arrival.index] ||
					prepared.DeliveryRoutes[0].Context.Joins[0].Ref.HandlerEvent() != arrival.name {
					t.Fatalf("actual member lacks its per-join/per-recipient receipt: routes=%#v found=%v err=%v", prepared.DeliveryRoutes, found, err)
				}
				replay = append(replay, event)
			}
			before := []pipeline.WorkflowInstance{load(0), load(1)}
			arms := [][]joinruntime.Activation{
				a2MultiUntilEntryArms(t, before[0], captured[0]), a2MultiUntilEntryArms(t, before[1], captured[1]),
			}
			triggerRaw, _ := json.Marshal(map[string]any{"order_id": key})
			trigger := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "stop.requested", "operator", "", triggerRaw, 0, runID,
				events.EnvelopeForEntityID(events.EventEnvelope{}, runID), eventtest.RootRoutingSource(runID), time.Now().UTC())
			if err := bus.PublishAcknowledged(ctx, trigger); err != nil {
				t.Fatal(err)
			}
			a2KnownTargetWaitForSettlement(t, ctx, bus, probe, trigger, worker.Key(), "completed", "delivered", logger)
			haltEvents := a2MultiUntilChildren(t, selected, ctx, trigger.ID(), "halt.requested", 1)
			halt := haltEvents[0]
			prepared, found, err := selected.events.LoadPreparedPublishEvent(ctx, halt.ID())
			if err != nil || !found || len(prepared.DeliveryRoutes) != 2 {
				t.Fatalf("actual worker until publication: routes=%#v found=%v err=%v", prepared.DeliveryRoutes, found, err)
			}
			seen := map[string]bool{}
			for _, route := range prepared.DeliveryRoutes {
				index := 0
				if route.Recipient.ID() == collectors[1].Key() {
					index = 1
				}
				if seen[route.Recipient.ID()] || route.Recipient.ID() != collectors[index].Key() ||
					route.Target.Route() != (events.RouteIdentity{FlowID: flows[index], FlowInstance: paths[index], EntityID: flowidentity.EntityID(paths[index])}) || len(route.Context.Joins) != 2 {
					t.Fatalf("until was broadcast or lost its independent recipient: %#v", route)
				}
				seen[route.Recipient.ID()] = true
				for _, arm := range arms[index] {
					matches := 0
					for _, receipt := range route.Context.Joins {
						if receipt.Disposition == events.JoinAdmissionBound && receipt.Ref.Equal(arm.JoinRef()) && receipt.Ref.StageEntry() == captured[index] {
							matches++
						}
					}
					if matches != 1 {
						t.Fatalf("until lacks one exact receipt for join %s/recipient %d: %#v", arm.Key(), index, route.Context.Joins)
					}
				}
				a2KnownTargetWaitForSettlement(t, ctx, bus, probe, halt, collectors[index].Key(), "completed", "delivered", logger)
			}
			observed := a2MultiUntilChildren(t, selected, ctx, halt.ID(), "halt.observed", 2)
			observedRecipients := map[string]bool{}
			for _, event := range observed {
				var value map[string]any
				if err := json.Unmarshal(event.Payload(), &value); err != nil || value["count"] != float64(1) || value["order_id"] != key {
					t.Fatalf("ordinary until handler effect/output was suppressed or repeated: payload=%s err=%v", event.Payload(), err)
				}
				matched := false
				for index := range paths {
					if event.Producer().ID() == collectors[index].Key() && event.RoutingSource().Route() ==
						(events.RouteIdentity{FlowID: flows[index], FlowInstance: paths[index], EntityID: flowidentity.EntityID(paths[index])}) && !observedRecipients[paths[index]] {
						observedRecipients[paths[index]], matched = true, true
					}
				}
				if !matched {
					t.Fatalf("ordinary until output crossed or repeated its recipient: %#v", event.RoutingSource())
				}
			}
			closed := []pipeline.WorkflowInstance{load(0), load(1)}
			var pending []genericschedule.Activation
			for index := range paths {
				if closed[index].Fields["halt_count"] != int64(1) || closed[index].CurrentState != "awaiting" || closed[index].Revision != before[index].Revision+1 {
					t.Fatalf("until was not one ordinary-plus-two-closures commit: before=%#v after=%#v", before[index], closed[index])
				}
				arms[index] = a2MultiUntilEntryArms(t, closed[index], captured[index])
				for _, arm := range arms[index] {
					wantExpected, wantCompleted := 2, 1
					if arm.JoinRef().HandlerEvent() == "alternate.completed" {
						wantExpected, wantCompleted = 3, index+1
					}
					if arm.Status != joinruntime.StatusClosed || arm.CloseReason != joinruntime.CloseReasonUntil ||
						!arm.OutcomePending || arm.OutcomeFired || arm.Expected() != wantExpected || arm.Completed() != wantCompleted {
						t.Fatalf("until fabricated a full join or crossed entries: %#v", arm)
					}
					missing := []string{"b"}
					values := []any{map[string]any{"value": "A-E2-primary-a"}}
					if index == 1 {
						missing, values = []string{"a"}, []any{map[string]any{"value": "B-E1-primary-b"}}
					}
					if arm.JoinRef().HandlerEvent() == "alternate.completed" {
						missing, values = []string{}, []any{map[string]any{"value": "A-E2-alternate-z"}}
						if index == 1 {
							values = []any{map[string]any{"value": "B-E1-alternate-x"}, map[string]any{"value": "B-E1-alternate-z"}}
						}
					}
					results, err := arm.Results()
					if err != nil || !reflect.DeepEqual(arm.Missing(), missing) || !reflect.DeepEqual(results, values) {
						t.Fatalf("partial closure changed admitted business values/order or invented missing IDs: results=%#v missing=%#v err=%v", results, arm.Missing(), err)
					}
					pending = append(pending, exactJoinPendingSchedule(t, selected, ctx, arm))
				}
			}
			if !reflect.DeepEqual(oldA, a2MultiUntilEntryArms(t, closed[0], original[0])) {
				t.Fatal("until changed A's historical closed E1 arms")
			}
			if err := schedules.Stop(ctx); err != nil {
				t.Fatal(err)
			}
			probe = lifecycleprobe.New()
			bus = newBus()
			var driver *exactJoinScheduleDriver
			schedules, driver = newExactJoinScheduleLifecycleForTest(t, ctx, selected, bus)
			pc = newGateRecoveryCoordinator(bus, selected, pipeline.PipelineCoordinatorOptions{
				Module: module, GenericSchedules: schedules, TestLifecycleProbe: probe,
			})
			bus.SetInterceptors(pc)
			publishRoutes()
			if restored, err := schedules.Restore(ctx); err != nil || restored != 4 {
				t.Fatalf("restore exactly four captured continuations: count=%d err=%v", restored, err)
			}
			for index := range paths {
				if !reflect.DeepEqual(closed[index], load(index)) {
					t.Fatal("scheduler reconstruction changed closed business state before execution")
				}
			}
			if err := driver.Resume(ctx); err != nil {
				t.Fatal(err)
			}
			for ordinal, schedule := range pending {
				index, arm := ordinal/2, arms[ordinal/2][ordinal%2]
				var fired genericschedule.Activation
				deadline := time.Now().Add(10 * time.Second)
				for time.Now().Before(deadline) {
					var found bool
					fired, found, err = selected.events.(genericschedule.Store).LoadGenericScheduleActivation(ctx, schedule.ID)
					if err == nil && found && fired.Status == genericschedule.StatusFired {
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
				if err != nil || fired.CurrentEventID == "" || fired.Status != genericschedule.StatusFired {
					t.Fatalf("exact persisted continuation did not fire: %#v err=%v logs=%s", fired, err, logger.String())
				}
				publication, found, err := selected.events.LoadPreparedPublishEvent(ctx, fired.CurrentEventID)
				if err != nil || !found {
					t.Fatalf("real schedule publication: found=%v err=%v", found, err)
				}
				completion := publication.Event.Event()
				a2KnownTargetWaitForSettlement(t, ctx, bus, probe, completion, collectors[index].Key(), "completed", "delivered", logger)
				assertExactJoinFiredSchedule(t, selected, ctx, schedule, completion.ID())
				name := "first.closed"
				if arm.JoinRef().HandlerEvent() == "alternate.completed" {
					name = "second.closed"
				}
				output := a2MultiUntilChildren(t, selected, ctx, completion.ID(), name, 1)[0]
				wantContext, contextErr := arm.Context()
				wantRaw, marshalErr := json.Marshal(wantContext)
				if contextErr != nil || marshalErr != nil || output.Producer().ID() != collectors[index].Key() ||
					output.RoutingSource().Route() != (events.RouteIdentity{FlowID: flows[index], FlowInstance: paths[index], EntityID: flowidentity.EntityID(paths[index])}) ||
					a2AccumulatorJSONHash(t, output.Payload()) != a2AccumulatorJSONHash(t, wantRaw) {
					t.Fatalf("exact completion lost truthful partial summary/results: output=%s want=%s source=%#v err=%v marshal=%v", output.Payload(), wantRaw, output.RoutingSource(), contextErr, marshalErr)
				}
				replay = append(replay, completion)
			}
			for index := range paths {
				for _, arm := range a2MultiUntilEntryArms(t, load(index), captured[index]) {
					if !arm.OutcomeFired || arm.OutcomePending || arm.CloseReason != joinruntime.CloseReasonUntil {
						t.Fatalf("reconstructed completion did not finish its exact until arm: %#v", arm)
					}
				}
				if load(index).Fields["halt_count"] != int64(1) {
					t.Fatal("join completion re-ran the ordinary until handler")
				}
			}
			// New one-hour deadlines are lawful active work, not quiescent work.
			// Pause their driver while testing durable replay against later entries.
			if err := schedules.Stop(ctx); err != nil {
				t.Fatal(err)
			}
			probe = lifecycleprobe.New()
			bus = newBus()
			schedules, _ = newExactJoinScheduleLifecycleForTest(t, ctx, selected, bus)
			pc = newGateRecoveryCoordinator(bus, selected, pipeline.PipelineCoordinatorOptions{
				Module: module, GenericSchedules: schedules, TestLifecycleProbe: probe,
			})
			bus.SetInterceptors(pc)
			publishRoutes()
			for index := range paths {
				reenter(index)
				if entry(index) == captured[index] {
					t.Fatal("later arm reused the until-closed entry")
				}
			}
			beforeReplay := []pipeline.WorkflowInstance{load(0), load(1)}
			replay = append(replay, trigger, halt)
			var eventsBefore, revisionsBefore int
			if err := selected.db.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM events WHERE run_id=$1),
				(SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1)`, runID).Scan(&eventsBefore, &revisionsBefore); err != nil {
				t.Fatal(err)
			}
			for _, event := range replay {
				if err := bus.PublishAcknowledged(ctx, event); err != nil {
					t.Fatalf("duplicate real publication: %v", err)
				}
			}
			waitForGateRecoveryQuiescence(t, bus, ctx)
			var eventsAfter, revisionsAfter int
			if err := selected.db.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM events WHERE run_id=$1),
				(SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1)`, runID).Scan(&eventsAfter, &revisionsAfter); err != nil {
				t.Fatal(err)
			}
			if eventsAfter != eventsBefore || revisionsAfter != revisionsBefore {
				t.Fatalf("duplicate until/member/completion made new publications/history: events=%d/%d revisions=%d/%d", eventsAfter, eventsBefore, revisionsAfter, revisionsBefore)
			}
			for index := range paths {
				if !reflect.DeepEqual(beforeReplay[index], load(index)) {
					t.Fatal("duplicate old publication affected later arms or historical closures")
				}
				for _, arm := range a2MultiUntilEntryArms(t, load(index), entry(index)) {
					if arm.Status != joinruntime.StatusOpen || arm.Completed() != 0 {
						t.Fatalf("old replay acquired later join authority: %#v", arm)
					}
				}
				assertExactJoinDeliveryCount(t, selected, ctx, halt.ID(), collectors[index].Key(), 1)
			}
			retained, found, err := selected.events.LoadPreparedPublishEvent(ctx, halt.ID())
			if err != nil || !found || !reflect.DeepEqual(retained.DeliveryRoutes, prepared.DeliveryRoutes) {
				t.Fatalf("until replay rewrote its four original receipts: found=%v err=%v", found, err)
			}
			t.Log("real EventBus: A E2/B E1, four exact until receipts/continuations, truthful partial summaries, ordinary effects once, and later-arm replay isolation")
		})
	}
}

func a2MultiUntilEntryArms(t *testing.T, instance pipeline.WorkflowInstance, entry timeridentity.StageEntryRef) []joinruntime.Activation {
	t.Helper()
	var matches []joinruntime.Activation
	for _, arm := range a2KnownTargetArms(t, instance) {
		if arm.JoinRef().StageEntry() == entry {
			matches = append(matches, arm)
		}
	}
	if len(matches) != 2 {
		t.Fatalf("exact entry must own two distinct joins: entry=%#v arms=%#v", entry, matches)
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i].Key() < matches[j].Key() })
	return matches
}

func a2MultiUntilChildren(t *testing.T, selected gateRecoveryStoreCase, ctx context.Context, parentID, name string, want int) []events.Event {
	t.Helper()
	rows, err := selected.db.QueryContext(ctx, `SELECT CAST(event_id AS TEXT) FROM events
		WHERE source_event_id=$1 AND (event_name=$2 OR event_name LIKE $3) ORDER BY event_id`, parentID, name, "%/"+name)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(ids) != want {
		t.Fatalf("real parent %s produced %d %s children, want %d", parentID, len(ids), name, want)
	}
	var children []events.Event
	for _, id := range ids {
		prepared, found, err := selected.events.LoadPreparedPublishEvent(ctx, id)
		if err != nil || !found || prepared.Event.Event().ParentEventID() != parentID {
			t.Fatalf("real child publication %s: found=%v err=%v", id, found, err)
		}
		children = append(children, prepared.Event.Event())
	}
	return children
}

func a2MultiUntilBindingFiles() map[string]string {
	files := a2PayloadDirectedJoinFiles()
	files["schema.yaml"] = `name: a2-multi-until-binding
stages:
  active: {initial: true}
pins:
  inputs: {events: [stop.requested]}
  outputs: {events: [halt.requested]}
connect:
  - {event: halt.requested, from: ., to: orders, resolution: select}
  - {event: halt.requested, from: ., to: mirror, resolution: select}
`
	files["events.yaml"] = "stop.requested:\n  order_id: text\nhalt.requested:\n  order_id: text\n"
	files["nodes.yaml"] = `worker:
  execution_type: system_node
  event_handlers:
    stop.requested:
      emit:
        event: halt.requested
        fields: {order_id: "${payload.order_id}"}
`
	for _, flow := range []string{"orders", "mirror"} {
		files[flow+"/schema.yaml"] = fmt.Sprintf(`name: %s
instance: order_id
stages:
  awaiting: {initial: true}
  dispatching: {}
  ready: {terminal: true}
  attention: {terminal: true}
pins:
  inputs:
    events:
      - halt.requested
`, flow)
		files[flow+"/entities.yaml"] = "order_state:\n  order_id: {type: text, indexed: true}\n  expected: \"[text]\"\n  halt_count: {type: integer, initial: 0}\n"
		files[flow+"/events.yaml"] = "manual.abort:\ndispatch.completed:\nitem.completed:\n  member_id: text\n  result: JoinResult\nalternate.completed:\n  member_id: text\n  result: JoinResult\nhalt.observed:\n  order_id: text\n  count: integer\n"
		for _, name := range []string{"first.closed", "second.closed"} {
			files[flow+"/events.yaml"] += name + ":\n  expected: integer\n  completed: integer\n  missing: \"[text]\"\n  results: \"[JoinResult]\"\n  timed_out: boolean\n  close_reason: text\n"
		}
		files[flow+"/nodes.yaml"] = `collector:
  execution_type: system_node
  event_handlers:
    item.completed:
      join:
        id: primary
        stage: awaiting
        members: {from: state.expected, by: payload.member_id}
        output: payload.result
        deadline: {after: 1h, from: stage_entry}
        until: halt.requested
        on_deadline: {advances_to: attention}
        on_complete:
          emit:
            event: first.closed
            fields:
              expected: "${join.expected}"
              completed: "${join.completed}"
              missing: "${join.missing}"
              results: "${join.results}"
              timed_out: "${join.timed_out}"
              close_reason: "${join.close_reason}"
    alternate.completed:
      join:
        id: alternate
        stage: awaiting
        members: {count: 3, by: payload.member_id}
        output: payload.result
        deadline: {after: 1h, from: stage_entry}
        until: halt.requested
        on_deadline: {advances_to: attention}
        on_complete:
          emit:
            event: second.closed
            fields:
              expected: "${join.expected}"
              completed: "${join.completed}"
              missing: "${join.missing}"
              results: "${join.results}"
              timed_out: "${join.timed_out}"
              close_reason: "${join.close_reason}"
    halt.requested:
      data_accumulation:
        writes:
          - {target_field: halt_count, value: "${entity.halt_count + 1}"}
      emit:
        event: halt.observed
        fields: {order_id: "${entity.order_id}", count: "${entity.halt_count}"}
dispatcher:
  execution_type: system_node
  event_handlers:
    manual.abort: {advances_to: dispatching}
    dispatch.completed: {advances_to: awaiting}
`
	}
	return files
}
