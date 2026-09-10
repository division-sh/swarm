package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
)

func TestPipelineTransitionRejectsCoherentEventSubstitutionOnBothStores(t *testing.T) {
	source := transitionMutationSource(t)
	node := pipelineNode(t, ".", "router")
	handler := source.ExecutableNodeEventHandlers(node)["advance"]
	graph, ok := semanticview.WorkflowStageTopology(source, ".")
	if !ok {
		t.Fatal("source did not compile root topology")
	}
	compiled, err := graph.AdmitTransition(contracts.WorkflowTransitionSite{
		Node: node, HandlerEvent: "advance", AdvanceCarrier: contracts.HandlerAdvanceCarrierHandler,
	}, "ready", "done")
	if err != nil {
		t.Fatal(err)
	}
	ref, ok := handler.Rules[0].DeclarationIdentity()
	if !ok {
		t.Fatal("source did not qualify selected rule")
	}
	selection, err := handlerselection.Selected(handlerselection.ContextRules, ref, "chosen")
	if err != nil {
		t.Fatal(err)
	}
	cause, err := workflowlifecycle.NewCompiledTransition(compiled, selection, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, variant := range []string{"accepted_control", "direct_execution_control", "other_persisted_id", "missing_id", "type", "time", "other_persisted_event", "foreign_accepted_handler", "missing_execution_event", "application_without_execution_event", "contradictory_inbound_event"} {
			t.Run(backend+"/"+variant, func(t *testing.T) {
				db, store := openHandlerEntityRequirementStore(t, backend)
				pc := newDurablePipelineCoordinatorForTest(&recordingPipelineBus{}, db, PipelineCoordinatorOptions{
					Module: staticSemanticWorkflowModule{source: source}, Persistence: workflowPersistenceForTest(store),
					PipelineObligations: unavailablePipelineTestObligationOwner{},
				})
				var ctx context.Context
				if backend == "sqlite" {
					ctx = sqliteExactOnceRunContext(t, db)
				} else {
					ctx = testPipelineRunContext(t, db)
				}
				entityID := eventtest.UUID("transition-event-authority-" + backend + "-" + variant)
				address := testEngineStateAddress(".", testPipelineRunID, entityID)
				instance := materializedWorkflowInstanceForTest(WorkflowInstance{
					InstanceID: testPipelineRunID, StorageRef: testPipelineRunID, EntityID: entityID,
					WorkflowName: ".", WorkflowVersion: "1", Mode: contracts.FlowModeStatic,
					CurrentState: "ready", Fields: map[string]any{"marker": "unchanged"}, EntityType: "test_entity",
				})
				if err := store.upsert(ctx, instance); err != nil {
					t.Fatal(err)
				}
				at := time.Now().UTC().Truncate(time.Microsecond)
				acceptedID := eventtest.UUID("accepted-" + backend + "-" + variant)
				otherID := eventtest.UUID("other-" + backend + "-" + variant)
				acceptedType := events.EventType("advance")
				if variant == "foreign_accepted_handler" {
					acceptedType = "foreign"
				}
				accepted := handlerTestRootIngress(acceptedID, acceptedType, "", "", nil, 0, testPipelineRunID, "",
					handlerTestWorkflowEnvelope(".", testPipelineRunID, entityID), at)
				other := handlerTestRootIngress(otherID, "foreign", "", "", nil, 0, testPipelineRunID, "",
					handlerTestWorkflowEnvelope(".", testPipelineRunID, entityID), at.Add(time.Second))
				dialect := authoractivityfixture.DialectPostgres
				if backend == "sqlite" {
					dialect = authoractivityfixture.DialectSQLite
				}
				seedPipelineEventRecordForDialect(t, ctx, db, dialect, accepted)
				seedPipelineEventRecordForDialect(t, ctx, db, dialect, other)
				target := events.RouteIdentity{FlowID: ".", FlowInstance: testPipelineRunID, EntityID: entityID}
				acceptedHandler := source.ExecutableNodeEventHandlers(node)[string(acceptedType)]
				application, err := pc.prepareDeliveryTargetApplication(ctx, node.Key(), MustDeliveryTargetHandler(node).ForEvent(acceptedType), acceptedHandler, accepted, events.MustExistingEntityTarget(target))
				if err != nil {
					t.Fatalf("prepare actual delivery application: %v", err)
				}
				if variant != "direct_execution_control" && variant != "missing_execution_event" {
					ctx = withDeliveryTargetApplication(ctx, application)
				}
				ctx = runtimecorrelation.WithInboundEvent(ctx, application.Event())
				if variant == "missing_execution_event" || variant == "application_without_execution_event" {
					ctx = runtimecorrelation.WithInboundEvent(ctx, events.Event{})
				} else if variant == "contradictory_inbound_event" {
					ctx = runtimecorrelation.WithInboundEvent(ctx, other)
				}
				state := testEngineStateMutation(map[string]any{"marker": "must-not-persist"}, nil, nil)
				state.NextState, state.Transition = "done", &cause
				state.TriggerEventID, state.TriggerEventType, state.TriggeredAt = acceptedID, string(acceptedType), at
				switch variant {
				case "other_persisted_id":
					state.TriggerEventID = otherID
				case "missing_id":
					state.TriggerEventID = eventtest.UUID("never-persisted")
				case "type":
					state.TriggerEventType = "foreign"
				case "time":
					state.TriggeredAt = at.Add(time.Second)
				case "other_persisted_event":
					state.TriggerEventID, state.TriggerEventType, state.TriggeredAt = otherID, "foreign", at.Add(time.Second)
				}
				// Deliberately keep both projections internally consistent. Their
				// agreement must not substitute for the accepted execution event.
				effect, err := workflowlifecycle.NewAcceptedEvent(address.Route, identity.NormalizeEntityID(entityID),
					state.TriggerEventID, state.TriggerEventType, accepted.ExecutionMode(), state.TriggeredAt, &cause)
				if err != nil {
					t.Fatal(err)
				}
				mutation := engine.EngineMutation{Address: address, State: state, HandlerRuleSelection: selection, LifecycleEffects: []workflowlifecycle.Effect{effect}}
				if err := mutation.ValidateTransitionEvidence(); err != nil {
					t.Fatalf("probe must reach independent accepted-event authority: %v", err)
				}
				before, found, err := store.Load(ctx, address.Route)
				if err != nil || !found {
					t.Fatalf("before snapshot: found=%v err=%v", found, err)
				}
				snapshotRows := func() map[string][]string {
					out := map[string][]string{}
					for _, table := range []string{
						"entity_state", "flow_instances", "entity_mutations", "workflow_instance_initial_materializations",
						"events", "event_deliveries", "event_receipts", "timers", "activity_attempts",
						"event_delivery_handler_rule_selections", "event_delivery_attempts", "event_delivery_outcomes",
						"author_activity_occurrences", "fan_out_intents", "fan_out_outcomes",
					} {
						out[table] = func() []string {
							rows, err := db.QueryContext(ctx, "SELECT * FROM "+table)
							if err != nil {
								t.Fatalf("snapshot %s: %v", table, err)
							}
							defer rows.Close()
							columns, err := rows.Columns()
							if err != nil {
								t.Fatalf("snapshot %s columns: %v", table, err)
							}
							var snapshot []string
							for rows.Next() {
								values := make([]any, len(columns))
								dest := make([]any, len(columns))
								for i := range values {
									dest[i] = &values[i]
								}
								if err := rows.Scan(dest...); err != nil {
									t.Fatalf("snapshot %s row: %v", table, err)
								}
								for i, value := range values {
									if stamp, ok := value.(time.Time); ok {
										value = stamp.UTC()
									}
									// Retain types: JSON alone conflates byte values with base64 text.
									values[i] = struct {
										Column string
										Type   string
										Value  any
									}{columns[i], fmt.Sprintf("%T", value), value}
								}
								raw, err := json.Marshal(values)
								if err != nil {
									t.Fatalf("snapshot %s canonical row: %v", table, err)
								}
								snapshot = append(snapshot, string(raw))
							}
							if err := rows.Err(); err != nil {
								t.Fatalf("snapshot %s rows: %v", table, err)
							}
							slices.Sort(snapshot)
							return snapshot
						}()
					}
					return out
				}
				beforeRows := snapshotRows()
				_, commitErr := (pipelineEngineMutationOwner{store: store, state: pipelineEngineStateRepo{coordinator: pc}}).CommitEngineMutation(ctx, mutation)
				after, found, err := store.Load(ctx, address.Route)
				if err != nil || !found {
					t.Fatalf("after snapshot: found=%v err=%v", found, err)
				}
				if variant == "accepted_control" || variant == "direct_execution_control" {
					if commitErr != nil || after.CurrentState != "done" || after.Revision != before.Revision+1 || len(after.TransitionHistory) != 1 || after.TransitionHistory[0].TriggerEventID != acceptedID {
						t.Fatalf("accepted control: err=%v state=%#v", commitErr, after)
					}
					return
				}
				if commitErr == nil {
					t.Errorf("coherent %s substitution COMMITTED: accepted=%s/%s cause_handler=%s trigger=%s/%s before_revision=%d after_revision=%d history=%#v",
						variant, accepted.ID(), accepted.Type(), compiled.Edge().HandlerEvent, state.TriggerEventID, state.TriggerEventType, before.Revision, after.Revision, after.TransitionHistory)
				} else {
					want := "transition trigger contradicts its accepted execution event"
					switch variant {
					case "foreign_accepted_handler":
						want = "transition handler contradicts its accepted execution selection"
					case "missing_execution_event", "application_without_execution_event":
						want = "transition requires its accepted execution event"
					case "contradictory_inbound_event":
						want = "transition execution event contradicts its admitted delivery application"
					}
					if !strings.Contains(commitErr.Error(), want) {
						t.Errorf("wrong rejection: got %v, want %q", commitErr, want)
					}
				}
				if !reflect.DeepEqual(before, after) {
					t.Error("hostile event substitution changed business state/history")
				}
				afterRows := snapshotRows()
				for table, rows := range beforeRows {
					if !slices.Equal(rows, afterRows[table]) {
						t.Errorf("hostile event substitution changed %s row contents: before=%q after=%q", table, rows, afterRows[table])
					}
				}
			})
		}
	}
}
