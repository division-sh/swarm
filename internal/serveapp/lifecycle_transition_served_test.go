package serveapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestServedCompiledTransitionSelectedCarrierEvidenceOnBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, reordered := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reordered_%t", backend, reordered), func(t *testing.T) {
				rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyLifecycleSelectedCarriers(t, reordered))
				for _, handler := range []string{"first", "second"} {
					for _, choice := range []string{"alpha", "beta"} {
						t.Run(handler+"/"+choice, func(t *testing.T) {
							key := handler + "/" + choice
							seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.seeded", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": key + "/seed"})
							// One root input reaches all four flows through explicit
							// connections; it grants no private API endpoint.
							recipients := []string{"", "child/", "sibling/", "child/nested/"}
							for _, recipient := range recipients {
								requireLifecycleFlowEntity(t, rt, seed.RunID, recipient, "waiting")
							}
							params := map[string]any{"event_name": "work." + handler, "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"choice": choice}, "idempotency_key": key}
							selected := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
							for _, recipient := range recipients {
								requireLifecycleFlowEntity(t, rt, seed.RunID, recipient, "done")
							}
							waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
							for _, recipient := range recipients {
								entityID := requireLifecycleFlowEntity(t, rt, seed.RunID, recipient, "done")
								history := readLifecycleTransitionHistory(t, rt, seed.RunID, entityID)
								if len(history) != 1 || history[0].From != "active" || history[0].To != "done" {
									t.Fatalf("history=%#v", history)
								}
								record := readLifecycleTransitionAtCut(t, rt, seed.RunID, entityID, history[0].TriggerEventID)
								flow := strings.TrimSuffix(recipient, "/")
								if flow == "" {
									flow = "."
								}
								guards := []string{handler + "_choice", handler + "_nonempty"}
								selection := record.Evidence.RuleSelection()
								index := 0
								if choice == "beta" {
									index = 1
								}
								path := fmt.Sprintf("nodes[\"controller\"].handlers[%q].rules[%d]", "work."+handler, index)
								if record.From != "waiting" || record.To != "active" || record.TriggerEventID != selected.EventID || record.Evidence.FlowID() != flow || selection.Ref().Flow().String() != flow || selection.Ref().Family() != "handler_rule" || selection.DisplayLabel() != choice || selection.Ref().SemanticPath() != path || !reflect.DeepEqual(record.GuardsEvaluated, guards) || !reflect.DeepEqual(record.Evidence.GuardsEvaluated(), guards) {
									t.Fatalf("wrong exact selected history: %#v selection=%#v guards=%#v want flow=%s event=%s path=%s\n%s\n%s", record, selection, record.GuardsEvaluated, flow, selected.EventID, path, lifecycleStoredSnapshot(t, rt, seed.RunID), servedEventPublishDebugSummary(t, rt.DB, rt.Backend, seed.RunID))
								}
								var entity operatorread.OperatorEntityFull
								requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": seed.RunID, "entity_id": entityID}, &entity)
								if entity.Fields["result"] != recipient+handler+"/"+choice {
									t.Fatalf("wrong public output: %#v", entity)
								}
								requireLifecycleEventCount(t, rt, seed.RunID, recipient+"work.completed", 1)
							}
							var entities int
							if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM entity_state WHERE run_id=$1`, seed.RunID).Scan(&entities); err != nil {
								t.Fatal(err)
							}
							if entities != len(recipients) {
								t.Fatalf("public input recipient mismatch: %d entity rows, want %d", entities, len(recipients))
							}
							before := lifecycleStoredSnapshot(t, rt, seed.RunID)
							duplicate := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
							if duplicate.EventID != selected.EventID || lifecycleStoredSnapshot(t, rt, seed.RunID) != before {
								t.Fatal("duplicate changed selected transition")
							}
						})
					}
				}
			})
		}
	}
}

func requireLifecycleFlowEntity(t *testing.T, rt servedControlProofRuntime, runID, prefix, state string) string {
	t.Helper()
	instance := strings.TrimSuffix(prefix, "/")
	if instance == "" {
		instance = runID
	}
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
		var entityID string
		err := rt.DB.QueryRow(`SELECT entity_id FROM flow_instances WHERE run_id=$1 AND instance_path=$2 AND current_state=$3`, runID, instance, state).Scan(&entityID)
		if err == nil {
			return entityID
		}
		if err != sql.ErrNoRows {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("flow %s did not reach %s\n%s", instance, state, servedEventPublishDebugSummary(t, rt.DB, rt.Backend, runID))
	return ""
}

func readLifecycleTransitionHistory(t *testing.T, rt servedControlProofRuntime, runID, entityID string) []pipeline.WorkflowTransitionRecord {
	t.Helper()
	var raw string
	if err := rt.DB.QueryRow(`SELECT CAST(config AS TEXT) FROM flow_instances WHERE run_id=$1 AND entity_id=$2`, runID, entityID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	return decodeLifecycleTransitionHistory(t, []byte(raw))
}

func decodeLifecycleTransitionHistory(t *testing.T, raw []byte) []pipeline.WorkflowTransitionRecord {
	t.Helper()
	var config struct {
		History []pipeline.WorkflowTransitionRecord `json:"transition_history"`
	}
	if err := json.Unmarshal(raw, &config); err != nil {
		t.Fatalf("history hydration: %v\n%s", err, raw)
	}
	for _, record := range config.History {
		if err := record.Evidence.Validate(); err != nil {
			t.Fatal(err)
		}
		if record.TransitionID != record.Evidence.ID() || record.From != record.Evidence.From() || record.To != record.Evidence.To() {
			t.Fatalf("contradictory record: %#v", record)
		}
	}
	return config.History
}

func requireLifecycleCurrentTransition(t *testing.T, history []pipeline.WorkflowTransitionRecord, eventID, from, to string) pipeline.WorkflowTransitionRecord {
	t.Helper()
	if len(history) != 1 || history[0].TriggerEventID != eventID || history[0].From != from || history[0].To != to {
		t.Fatalf("current transition did not match the exact committed step %s (%s -> %s): %+v", eventID, from, to, history)
	}
	return history[0]
}

// Inspect one explicit cut through the same fixed-snapshot owner used by fork;
// never reconstruct a trajectory from current headers or transition timestamps.
func readLifecycleTransitionAtCut(t *testing.T, rt servedControlProofRuntime, runID, entityID, eventID string) pipeline.WorkflowTransitionRecord {
	t.Helper()
	type planner interface {
		PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
	}
	var reader planner
	if rt.Postgres != nil {
		reader = rt.Postgres
	} else if rt.SQLite != nil {
		reader = rt.SQLite
	} else {
		reader, _ = rt.ReceiverStateReader.(planner)
	}
	if reader == nil || eventID == "" {
		t.Fatal("transition proof requires its selected fixed-cut reader and exact event")
	}
	plan, err := reader.PlanRunFork(context.Background(), runfork.RunForkPlanRequest{SourceRunID: runID, At: eventID})
	if err != nil || plan.ForkPoint.EventID != eventID {
		t.Fatalf("read exact transition cut %s: %v", eventID, err)
	}
	for _, entity := range plan.Entities {
		if entity.EntityID != entityID {
			continue
		}
		metadata := entity.MaterializationMetadata
		if metadata == nil {
			t.Fatal("historical entity has no construction header")
		}
		route := flowidentity.StoredRoute(metadata.FlowTemplate, flowidentity.LogicalInstanceID(metadata.FlowInstance), metadata.FlowInstance)
		if _, err := pipeline.DecodeWorkflowInstanceRecordedHeader(route, metadata.FlowConfig); err != nil {
			t.Fatal(err)
		}
		var config struct {
			History []pipeline.WorkflowTransitionRecord `json:"transition_history"`
		}
		if err := json.Unmarshal(metadata.FlowConfig, &config); err != nil || len(config.History) != 1 {
			t.Fatalf("cut %s lost bounded complete transition: %s, %v", eventID, metadata.FlowConfig, err)
		}
		return config.History[0]
	}
	t.Fatalf("cut %s lacks exact entity %s", eventID, entityID)
	return pipeline.WorkflowTransitionRecord{}
}

func TestServedCompiledLoopEscapeSuppressesOrdinaryRepeatOnBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			finishStarted, finishRelease := make(chan struct{}, 1), make(chan struct{})
			var finishOnce sync.Once
			unblockFinish := func() { finishOnce.Do(func() { close(finishRelease) }) }
			defer unblockFinish()
			backendName := "sqlite"
			if backend == servedparity.BackendExplicitPostgres {
				backendName = "postgres"
			}
			opts, start := issue2564ServeHarness(t, backendName, canonicalrouting.CopyLifecycleEmitter(t, canonicalrouting.LifecycleLoopRepeatEmitsUntilObserverFinish), true)
			opts.TestWorkflowNodeHandlerStartHook = func(ctx context.Context, _ string, evt events.Event) error {
				if evt.Type() != "ordinary.finish" {
					return nil
				}
				finishStarted <- struct{}{}
				select {
				case <-finishRelease:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			process, rt := start()
			t.Cleanup(func() {
				if code := process.stop(); code != 0 {
					t.Errorf("loop proof serve stop=%d", code)
				}
			})
			var finish servedEventPublishRPCResult
			var staleAdmitEventID string
			started := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "create"})
			entityID := rt.waitEntityStage(t, started.RunID, started.RunID, "waiting")
			currentHistory := func() []pipeline.WorkflowTransitionRecord {
				return decodeLifecycleTransitionHistory(t, storetest.ObserveWriterFlow(t, context.Background(), rt.selected, started.RunID, entityID).Config)
			}
			readLoop := func() loopruntime.PublicActivation {
				entity := issue2564Entity(t, rt, started.RunID, entityID)
				if len(entity.Loops) != 1 {
					t.Fatalf("expected one public loop: %#v", entity)
				}
				return entity.Loops[0]
			}
			snapshot := func() string {
				stored, err := storetest.ReadSelectedForkApplicationStorageSnapshot(context.Background(), rt.selected)
				if err != nil {
					t.Fatal(err)
				}
				// Compare every column of the same two business families,
				// including all fields, config, revision and accumulator bytes.
				business := make(map[string]storetest.SelectedForkStorageTableSnapshot, 2)
				for _, table := range []string{"flow_instances", "entity_state"} {
					rows, ok := stored[table]
					if !ok || len(rows.Rows) == 0 {
						t.Fatalf("missing lifecycle business evidence: %s", table)
					}
					business[table] = rows
				}
				raw, err := json.Marshal(business)
				if err != nil {
					t.Fatal(err)
				}
				return string(raw)
			}
			requireEventCount := func(event string, want int) {
				if count := len(rt.events(t, started.RunID, event)); count != want {
					t.Fatalf("%s count=%d, want %d", event, count, want)
				}
			}
			waitOtherDeliveries := func() {
				if finish.EventID == "" {
					rt.waitDeliveries(t, started.RunID)
					return
				}
				// The known stale admit is rejected; all other work settles
				// successfully except the one exact accepted finish delivery.
				var last []storetest.H2NodeDeliveriesEvidence
				stable := 0
				for deadline := time.Now().Add(servedProofPollDeadline); time.Now().Before(deadline); {
					var err error
					last, err = storetest.ObserveH2NodeDeliveries(context.Background(), rt.selected, started.RunID)
					if err != nil {
						t.Fatal(err)
					}
					held, rejected, unsettled := 0, 0, 0
					for _, row := range last {
						switch {
						case row.Event == finish.EventID && row.Name == "ordinary.finish" && row.Status == "in_progress":
							held++
						case row.Event == staleAdmitEventID && row.Status == "dead_letter":
							rejected++
						case row.Status == "pending" || row.Status == "in_progress":
							unsettled++
						case row.Status != "delivered":
							t.Fatalf("unexpected observer-held delivery outcome: %#v", row)
						}
					}
					all := storetest.ObserveWriterRunDelivery(t, context.Background(), rt.selected, started.RunID)
					if held == 1 && rejected == 1 && unsettled == 0 && len(last) == all.Total && all.SettledDelivered+2 == all.Total {
						stable++
						if stable == 4 {
							var event operatorread.OperatorEventFull
							requireServedJSONRPCResult(t, rt.Endpoint, "event.get", map[string]any{"event_id": finish.EventID}, &event)
							if event.RunID != started.RunID || event.EventName != "ordinary.finish" || len(event.Deliveries) != 1 || event.Deliveries[0].Status != "in_progress" || event.Deliveries[0].SubscriberType != "node" || event.Deliveries[0].Target.FlowID != "ordinary" {
								t.Fatalf("held observer delivery=%#v", event)
							}
							return
						}
					} else {
						stable = 0
					}
					time.Sleep(25 * time.Millisecond)
				}
				t.Fatalf("other deliveries did not settle with only observer held: %#v", last)
			}
			publish := func(event, key string, payload map[string]any) servedEventPublishRPCResult {
				return requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": event, "run_id": started.RunID, "source_event_id": started.EventID, "payload": payload, "idempotency_key": key})
			}
			loopStart := publish("loop.start", "start", map[string]any{"seed": true})
			rt.waitEntityStage(t, started.RunID, entityID, "drafting")
			requireLifecycleCurrentTransition(t, currentHistory(), loopStart.EventID, "waiting", "drafting")
			first := readLoop()
			var capRepeat servedEventPublishRPCResult
			var capPayload map[string]any
			var ordinaryID string
			for attempt := 1; attempt <= 2; attempt++ {
				current := readLoop()
				if current.Attempt != attempt || current.Status != loopruntime.StatusOpen {
					t.Fatalf("loop=%#v", current)
				}
				payload := map[string]any{"revision_id": current.RevisionID}
				if attempt == 2 {
					before := snapshot()
					staleAdmitEventID = publish("loop.admit", "stale-open-admit", map[string]any{"revision_id": first.RevisionID}).EventID
					rt.waitDeliveries(t, started.RunID)
					if snapshot() != before {
						t.Fatal("stale prior revision mutated open loop")
					}
				}
				admit := publish("loop.admit", fmt.Sprintf("admit-%d", attempt), payload)
				rt.waitEntityStage(t, started.RunID, entityID, "review")
				requireLifecycleCurrentTransition(t, currentHistory(), admit.EventID, "drafting", "review")
				if attempt == 2 {
					// Admit before the root becomes final. Hold actual work, not
					// completion timing, while probing receiver-final admission.
					finish = publish("ordinary.finish", "finish-observer", map[string]any{"seed": true})
					select {
					case <-finishStarted:
					case <-time.After(servedProofPollDeadline):
						t.Fatal("observer finish did not start")
					}
				}
				capRepeat = publish("loop.repeat", fmt.Sprintf("repeat-%d", attempt), payload)
				capPayload = payload
				state := "drafting"
				if attempt == 2 {
					state = "escaped"
				}
				rt.waitEntityStage(t, started.RunID, entityID, state)
				requireLifecycleCurrentTransition(t, currentHistory(), capRepeat.EventID, "review", state)
				ordinaryID = rt.waitEntityStage(t, started.RunID, "", "observed")
				var ordinary operatorread.OperatorEntityFull
				requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": started.RunID, "entity_id": ordinaryID}, &ordinary)
				if ordinary.Fields["token"] != "ordinary" {
					t.Fatalf("ordinary consumer=%#v", ordinary)
				}
				if ordinary.Fields["revision_id"] != readLoop().RevisionID {
					t.Fatalf("ordinary consumer lost next-attempt revision: %#v", ordinary)
				}
				waitOtherDeliveries()
				requireEventCount("ordinary.repeated", 1)
				requireEventCount("loop.escaped", attempt-1)
			}
			closed := readLoop()
			if closed.Status != loopruntime.StatusClosed || closed.CloseReason != "escaped" || closed.Attempt != 2 || closed.RevisionID == first.RevisionID {
				t.Fatalf("closed loop=%#v", closed)
			}
			receipt := rt.waitEntityStage(t, started.RunID, "", "done")
			var received operatorread.OperatorEntityFull
			requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": started.RunID, "entity_id": receipt}, &received)
			if receipt == entityID || received.Fields["revision_id"] != closed.RevisionID {
				t.Fatalf("escape consumer=%#v loop=%#v", received, closed)
			}
			var config struct {
				History []pipeline.WorkflowTransitionRecord `json:"transition_history"`
			}
			flow := storetest.ObserveWriterFlow(t, context.Background(), rt.selected, started.RunID, entityID)
			if err := json.Unmarshal(flow.Config, &config); err != nil {
				t.Fatalf("history hydration: %v\n%s", err, flow.Config)
			}
			history := config.History
			for _, record := range history {
				if err := record.Evidence.Validate(); err != nil {
					t.Fatal(err)
				}
				if record.TransitionID != record.Evidence.ID() || record.From != record.Evidence.From() || record.To != record.Evidence.To() {
					t.Fatalf("contradictory record: %#v", record)
				}
			}
			if len(history) != 1 || history[0].TriggerEventID != capRepeat.EventID {
				t.Fatalf("cap history=%#v", history)
			}
			compiled, ok := history[0].Evidence.Compiled()
			if !ok || compiled.FlowID() != "." || compiled.Edge().Source != "loop.escape" || compiled.Edge().LoopID != "revision" {
				t.Fatalf("cap selected cause=%#v", compiled)
			}
			requireServedRunStatus(t, rt.Endpoint, started.RunID, "running")
			before := snapshot()
			duplicate := publish("loop.repeat", "repeat-2", capPayload)
			if duplicate.EventID != capRepeat.EventID {
				t.Fatal("duplicate reminted cap event")
			}
			late := requestServedJSONRPC(t, rt.Endpoint, "event.publish", map[string]any{"event_name": "loop.repeat", "run_id": started.RunID, "source_event_id": started.EventID, "payload": map[string]any{"revision_id": first.RevisionID}, "idempotency_key": "stale-repeat"})
			if late.Error == nil || late.Error.Data["code"] != "EVENT_PUBLISH_FAILED" || late.Error.Data["retryable"] != false {
				t.Fatalf("late repeat admission=%#v", late.Error)
			}
			details, _ := late.Error.Data["details"].(map[string]any)
			if details["reason"] != "receiver_entity_terminal" || details["stage"] != "escaped" || details["flow_id"] != "." {
				t.Fatalf("late repeat reason=%#v", details)
			}
			waitOtherDeliveries()
			if after := snapshot(); after != before {
				t.Fatalf("duplicate/stale repeat mutated lifecycle:\nbefore=%s\nafter=%s", before, after)
			}
			requireEventCount("ordinary.repeated", 1)
			requireEventCount("loop.escaped", 1)
			unblockFinish()
			rt.waitEntityStage(t, started.RunID, ordinaryID, "done")
			rt.waitDeliveries(t, started.RunID)
			requireServedRunStatus(t, rt.Endpoint, started.RunID, "completed")
			completed := snapshot()
			terminal := requestServedJSONRPC(t, rt.Endpoint, "event.publish", map[string]any{"event_name": "loop.repeat", "run_id": started.RunID, "source_event_id": started.EventID, "payload": map[string]any{"revision_id": first.RevisionID}, "idempotency_key": "completed-stale-repeat"})
			if terminal.Error == nil || terminal.Error.Data["code"] != "RUN_ALREADY_TERMINAL" || terminal.Error.Data["retryable"] != false {
				t.Fatalf("completed repeat admission=%#v", terminal.Error)
			}
			terminalDetails, _ := terminal.Error.Data["details"].(map[string]any)
			if terminalDetails["current_status"] != "completed" || terminalDetails["run_id"] != started.RunID {
				t.Fatalf("completed repeat reason=%#v", terminalDetails)
			}
			if after := snapshot(); after != completed {
				t.Fatalf("completed stale repeat mutated lifecycle:\nbefore=%s\nafter=%s", completed, after)
			}
			requireEventCount("ordinary.repeated", 1)
			requireEventCount("loop.escaped", 1)
		})
	}
}

// SQL is read-only proof of all business rows and persisted transition evidence.
func lifecycleStoredSnapshot(t *testing.T, rt servedControlProofRuntime, runID string) string {
	t.Helper()
	rows, err := rt.DB.Query(`SELECT f.entity_id,f.current_state,f.revision,CAST(e.fields AS TEXT),CAST(f.config AS TEXT),CAST(f.accumulator AS TEXT)
		FROM flow_instances f LEFT JOIN entity_state e ON e.run_id=f.run_id AND e.flow_instance=f.instance_path AND e.entity_id=f.entity_id
		WHERE f.run_id=$1 ORDER BY f.entity_id`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	result := ""
	for rows.Next() {
		var id, state, config, loops string
		var fields sql.NullString
		var revision int
		if err := rows.Scan(&id, &state, &revision, &fields, &config, &loops); err != nil {
			t.Fatal(err)
		}
		result += fmt.Sprintf("%s/%s/%d/%t:%s/%s/%s\n", id, state, revision, fields.Valid, fields.String, config, loops)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if result == "" {
		t.Fatal("no persisted lifecycle proof rows")
	}
	return result
}
