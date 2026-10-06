package pipeline_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/eventschema"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/google/uuid"
)

func TestA2JoinResultTypesRealExecutionAndRestartOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		for _, result := range []struct {
			name, typeName         string
			kind                   contracts.CatalogTypeKind
			first, second, changed any
		}{
			{"text", "text", contracts.CatalogTypeText, "first", "second", "changed"},
			{"integer", "integer", contracts.CatalogTypeInteger, int64(11), int64(22), int64(33)},
			{"double", "double", contracts.CatalogTypeNumber, float64(1.25), float64(2.75), float64(3.5)},
			{"numeric", "numeric", contracts.CatalogTypeNumber, float64(4.25), float64(5.75), float64(6.5)},
			{"boolean", "boolean", contracts.CatalogTypeBoolean, true, false, false},
			{"named_scalar", "Score", contracts.CatalogTypeInteger, int64(7), int64(9), int64(8)},
			{"enum", "Decision", contracts.CatalogTypeText, "yes", "no", "maybe"},
			{"named_record", "JoinResult", contracts.CatalogTypeObject,
				map[string]any{"label": "first", "rank": int64(1), "weight": float64(1.25), "accepted": true, "decision": "yes"},
				map[string]any{"label": "second", "rank": int64(2), "weight": float64(2.75), "accepted": false, "decision": "no"},
				map[string]any{"label": "changed", "rank": int64(1), "weight": float64(1.25), "accepted": true, "decision": "yes"}},
		} {
			t.Run(backend.name+"/"+result.name, func(t *testing.T) {
				selected := backend.open(t)
				runID := uuid.NewString()
				insertGateRecoveryRun(t, selected, runID)
				ctx := withLiveGateExecution(correlation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
				bundle := loadPipelineLifecycleFixtureBundle(t, a2JoinResultTypeFiles(result.typeName))
				source := semanticview.Wrap(bundle)
				node := externalPipelineSourceNode(t, source, ".", "collector")
				plans := bundle.WorkflowJoins()
				if len(plans) != 1 || !plans[0].Node.Equal(node) || plans[0].ResultType.Type != result.typeName {
					t.Fatalf("compiled result owner = %#v, want %s", plans, result.typeName)
				}
				resolved, err := plans[0].ResultType.Resolve()
				if err != nil || resolved.Kind != result.kind {
					t.Fatalf("result catalog resolution = %#v err=%v, want %s", resolved, err, result.kind)
				}
				schema, found, err := bundle.ResolveCompiledFlowEventSchema(".", "item.completed")
				if err != nil || !found {
					t.Fatalf("compiled arrival schema: found=%v err=%v", found, err)
				}
				module := proposedEffectProofModule{source: source, nodes: []pipeline.WorkflowNode{
					{Node: node, Subscriptions: []events.EventType{"item.completed", "halt.requested"}, ExecutionType: contracts.SystemNodeExecutionType},
				}}
				probe := lifecycleprobe.New()
				logger := &exactJoinRuntimeLogger{}
				newBus := func() *runtimebus.EventBus {
					t.Helper()
					bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{ContractBundle: source, TestLifecycleProbe: probe, Logger: logger},
						"platform.join_complete", "platform.join_timeout")
					if err != nil {
						t.Fatal(err)
					}
					return bus
				}
				bus := newBus()
				schedules, _ := newExactJoinScheduleLifecycleForTest(t, ctx, selected, bus)
				pc := newGateRecoveryCoordinator(bus, selected, pipeline.PipelineCoordinatorOptions{Module: module, GenericSchedules: schedules, TestLifecycleProbe: probe})
				bus.SetInterceptors(pc)
				owner := testRunScopedWorkflowInstanceForRun(runID, runID)
				commitA2FixtureConstruction(t, pc, selected.events, ctx, owner, pipeline.WorkflowInstance{
					InstanceID: runID, StorageRef: runID, EntityID: runID, WorkflowName: source.WorkflowName(), WorkflowVersion: source.WorkflowVersion(),
					CurrentState: "awaiting", EntityType: "result_state", Fields: map[string]any{
						"final_expected": int64(0), "final_completed": int64(0), "final_results": []any{}, "final_reason": "",
					},
				}, time.Now().UTC())
				load := func() pipeline.WorkflowInstance {
					t.Helper()
					instance, found, err := pc.Load(ctx, owner)
					if err != nil || !found {
						t.Fatalf("typed result receiver load: found=%v err=%v", found, err)
					}
					return instance
				}
				initial := exactJoinPersistedArm(t, load())
				if initial.MemberCount == nil || *initial.MemberCount != 2 || initial.Status != joinruntime.StatusOpen || len(initial.Members) != 0 {
					t.Fatalf("typed count arm = %#v, want two unassigned contributors", initial)
				}
				publish := func(member string, value any, want failures.Class) events.Event {
					t.Helper()
					raw, err := canonicaljson.MarshalPreservingNumberKinds(map[string]any{"member_id": member, "result": value})
					if err != nil {
						t.Fatal(err)
					}
					admitted, err := canonicaljson.Decode(raw)
					if err != nil {
						t.Fatal(err)
					}
					payload, err := workflowexpr.ProjectSemanticValue(admitted)
					if err != nil {
						t.Fatal(err)
					}
					if err := eventschema.ValidatePayloadAgainstSchema(schema.AcceptanceSchema(), payload.(map[string]any)); err != nil {
						t.Fatalf("typed result input was not admitted by its compiled schema: %v", err)
					}
					event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "item.completed", "operator", "", raw, 0, runID,
						events.EnvelopeForEntityID(events.EventEnvelope{}, runID), eventtest.RootRoutingSource(runID), time.Now().UTC())
					if err := bus.PublishAcknowledged(ctx, event); err != nil {
						t.Fatal(err)
					}
					handlerStatus, deliveryStatus := "completed", "delivered"
					if want != "" {
						handlerStatus, deliveryStatus = "failed", "dead_letter"
					}
					a2KnownTargetWaitForSettlement(t, ctx, bus, probe, event, node.Key(), handlerStatus, deliveryStatus, logger)
					if want != "" {
						var rawFailure string
						if err := selected.db.QueryRowContext(ctx, `SELECT CAST(failure AS TEXT) FROM event_deliveries WHERE event_id=$1 AND subscriber_type='node' AND subscriber_id=$2`, event.ID(), node.Key()).Scan(&rawFailure); err != nil {
							t.Fatal(err)
						}
						var failure failures.Envelope
						if err := json.Unmarshal([]byte(rawFailure), &failure); err != nil || failure.Class != want || failure.Retryable {
							t.Fatalf("typed duplicate refusal=%s err=%v, want nonretryable %s", rawFailure, err, want)
						}
					}
					return event
				}
				assertResults := func(instance pipeline.WorkflowInstance, want []any) joinruntime.Activation {
					t.Helper()
					arm := exactJoinPersistedArm(t, instance)
					values, err := arm.Results()
					if err != nil || !reflect.DeepEqual(values, want) || !arm.JoinRef().Equal(initial.JoinRef()) {
						t.Fatalf("persisted %s results lost type/order/entry: got=%#v want=%#v err=%v", result.typeName, values, want, err)
					}
					return arm
				}
				first := publish("z", result.first, "")
				assertResults(load(), []any{result.first})
				duplicate := publish("z", result.first, "")
				if duplicate.ID() == first.ID() {
					t.Fatal("same-result contributor proof requires distinct admitted deliveries")
				}
				if arm := assertResults(load(), []any{result.first}); arm.Completed() != 1 || arm.Status != joinruntime.StatusOpen {
					t.Fatalf("same-result duplicate consumed another slot: %#v", arm)
				}
				beforeConflict := load()
				publish("z", result.changed, failures.ClassConflictingDuplicate)
				if after := load(); after.Revision != beforeConflict.Revision || !reflect.DeepEqual(after.Fields, beforeConflict.Fields) ||
					!reflect.DeepEqual(after.StateBuckets, beforeConflict.StateBuckets) {
					t.Fatal("changed-result conflict mutated persisted counter/results")
				}
				publish("a", result.second, "")
				closed := load()
				wantResults := []any{result.second, result.first}
				arm := assertResults(closed, wantResults)
				if arm.Status != joinruntime.StatusClosed || arm.CloseReason != joinruntime.CloseReasonComplete || !arm.OutcomePending || arm.OutcomeFired || arm.Completed() != 2 ||
					closed.CurrentState != "awaiting" || !reflect.DeepEqual(closed.Fields["final_results"], []any{}) ||
					closed.Fields["final_expected"] != int64(0) || closed.Fields["final_completed"] != int64(0) || len(closed.TransitionHistory) != 0 {
					t.Fatalf("typed results were not retained exclusively in pending closure: arm=%#v fields=%#v", arm, closed.Fields)
				}
				pending := exactJoinPendingSchedule(t, selected, ctx, arm)
				if err := schedules.Stop(ctx); err != nil {
					t.Fatal(err)
				}
				bus = newBus()
				var driver *exactJoinScheduleDriver
				schedules, driver = newExactJoinScheduleLifecycleForTest(t, ctx, selected, bus)
				pc = newGateRecoveryCoordinator(bus, selected, pipeline.PipelineCoordinatorOptions{Module: module, GenericSchedules: schedules, TestLifecycleProbe: probe})
				bus.SetInterceptors(pc)
				if restored, err := schedules.Restore(ctx); err != nil || restored != 1 {
					t.Fatalf("restore typed result continuation: count=%d err=%v", restored, err)
				}
				if reloaded := load(); !reflect.DeepEqual(reloaded, closed) {
					t.Fatal("restoration changed the persisted closure before actual execution")
				}
				assertResults(load(), wantResults)
				if err := driver.Resume(ctx); err != nil {
					t.Fatal(err)
				}
				completionID := exactJoinOccurrenceEventID(t, selected, ctx, runID, "platform.join_complete")
				waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				completion, err := probe.WaitForHandlerCompleted(waitCtx, completionID, node.Key())
				if err == nil {
					err = bus.WaitForQuiescence(waitCtx)
				}
				cancel()
				if err != nil || completion.Status != "completed" {
					t.Fatalf("actual typed continuation = %#v err=%v", completion, err)
				}
				assertExactJoinDeliveryStatus(t, selected, ctx, completionID, node.Key(), "delivered")
				final := waitForExactJoinState(t, ctx, pc, owner.Route, "ready")
				fired := assertResults(final, wantResults)
				if !reflect.DeepEqual(final.Fields["final_results"], wantResults) || final.Fields["final_expected"] != int64(2) ||
					final.Fields["final_completed"] != int64(2) || final.Fields["final_reason"] != string(joinruntime.CloseReasonComplete) ||
					len(final.TransitionHistory) != 1 || !fired.OutcomeFired || fired.OutcomePending {
					t.Fatalf("final %s fields lost exact native types/order/outcome: fields=%#v arm=%#v", result.typeName, final.Fields, fired)
				}
				assertExactJoinFiredSchedule(t, selected, ctx, pending, completionID)
				assertExactJoinDeliveryCount(t, selected, ctx, completionID, node.Key(), 1)
			})
		}
	}
}

func a2JoinResultTypeFiles(resultType string) map[string]string {
	return map[string]string{
		"schema.yaml":   "name: a2-result-types\nstages:\n  awaiting: {}\n  ready: {final: true}\npins:\n  inputs:\n    - item.completed\n    - halt.requested\n",
		"entities.yaml": fmt.Sprintf("result_state:\n  final_expected: integer\n  final_completed: integer\n  final_results: \"[%s]\"\n  final_reason: text\n", resultType),
		"types.yaml": `scalars:
  Score: integer
enums:
  Decision:
    values: [yes, no, maybe]
    default: yes
types:
  JoinResult:
    label: text
    rank: integer
    weight: double
    accepted: boolean
    decision: Decision
`,
		"events.yaml": fmt.Sprintf("item.completed:\n  member_id: text\n  result: %s\nhalt.requested:\n", resultType),
		"nodes.yaml": `collector:
  execution_type: system_node
  event_handlers:
    item.completed:
      join:
        stage: awaiting
        members: {count: 2, by: payload.member_id}
        output: payload.result
        until: halt.requested
        on_complete:
          advances_to: ready
          data_accumulation:
            writes:
              - {target_field: final_expected, value: join.expected}
              - {target_field: final_completed, value: join.completed}
              - {target_field: final_results, value: join.results}
              - {target_field: final_reason, value: join.close_reason}
`,
	}
}
