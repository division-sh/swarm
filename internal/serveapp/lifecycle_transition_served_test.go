package serveapp

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestServedCompiledTransitionSelectedCarrierEvidenceOnBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, reordered := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reordered_%t", backend, reordered), func(t *testing.T) {
				rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyLifecycleSelectedCarriers(t, reordered))
				for _, prefix := range []string{"", "child/", "sibling/", "child/nested/"} {
					for _, handler := range []string{"first", "second"} {
						for _, choice := range []string{"alpha", "beta"} {
							t.Run(prefix+handler+"/"+choice, func(t *testing.T) {
								key := prefix + handler + "/" + choice
								seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": prefix + "work.seeded", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": key + "/seed"})
								recipients := []string{prefix}
								// The root public input explicitly admits all four same-name
								// external input pins; scoped HTTP inputs select one flow.
								if prefix == "" {
									recipients = []string{"", "child/", "sibling/", "child/nested/"}
								}
								for _, recipient := range recipients {
									requireLifecycleFlowEntity(t, rt, seed.RunID, recipient, "waiting")
								}
								params := map[string]any{"event_name": prefix + "work." + handler, "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"choice": choice}, "idempotency_key": key}
								selected := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
								for _, recipient := range recipients {
									requireLifecycleFlowEntity(t, rt, seed.RunID, recipient, "done")
								}
								waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
								for _, recipient := range recipients {
									entityID := requireLifecycleFlowEntity(t, rt, seed.RunID, recipient, "done")
									history := readLifecycleTransitionHistory(t, rt, seed.RunID, entityID)
									if len(history) != 2 {
										t.Fatalf("history=%#v", history)
									}
									record := history[0]
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
		err := rt.DB.QueryRow(`SELECT entity_id FROM entity_state WHERE run_id=$1 AND flow_instance=$2 AND current_state=$3`, runID, instance, state).Scan(&entityID)
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
	if err := rt.DB.QueryRow(`SELECT CAST(f.config AS TEXT) FROM flow_instances f JOIN entity_state e ON f.run_id=e.run_id AND f.instance_path=e.flow_instance WHERE e.run_id=$1 AND e.entity_id=$2`, runID, entityID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var config struct {
		History []pipeline.WorkflowTransitionRecord `json:"transition_history"`
	}
	if err := json.Unmarshal([]byte(raw), &config); err != nil {
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

func TestServedCompiledLoopEscapeSuppressesOrdinaryRepeatOnBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyLifecycleEmitter(t, canonicalrouting.LifecycleLoopRepeatEmits))
			started := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "create"})
			entityID := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, started.RunID, "", "waiting")
			publish := func(event, key string, payload map[string]any) servedEventPublishRPCResult {
				return requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": event, "run_id": started.RunID, "source_event_id": started.EventID, "payload": payload, "idempotency_key": key})
			}
			publish("loop.start", "start", map[string]any{"seed": true})
			requireServedEventPublishEntityState(t, rt.DB, rt.Backend, started.RunID, entityID, "drafting")
			first := readLifecycleLoop(t, rt, started.RunID, entityID)
			var capRepeat servedEventPublishRPCResult
			var capPayload map[string]any
			for attempt := 1; attempt <= 2; attempt++ {
				current := readLifecycleLoop(t, rt, started.RunID, entityID)
				if current.Attempt != attempt || current.Status != loopruntime.StatusOpen {
					t.Fatalf("loop=%#v", current)
				}
				payload := map[string]any{"revision_id": current.RevisionID}
				if attempt == 2 {
					before := lifecycleStoredSnapshot(t, rt, started.RunID)
					publish("loop.admit", "stale-open-admit", map[string]any{"revision_id": first.RevisionID})
					waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, started.RunID)
					if lifecycleStoredSnapshot(t, rt, started.RunID) != before {
						t.Fatal("stale prior revision mutated open loop")
					}
				}
				publish("loop.admit", fmt.Sprintf("admit-%d", attempt), payload)
				requireServedEventPublishEntityState(t, rt.DB, rt.Backend, started.RunID, entityID, "review")
				capRepeat = publish("loop.repeat", fmt.Sprintf("repeat-%d", attempt), payload)
				capPayload = payload
				state := "drafting"
				if attempt == 2 {
					state = "escaped"
				}
				requireServedEventPublishEntityState(t, rt.DB, rt.Backend, started.RunID, entityID, state)
				ordinaryID := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, started.RunID, "", "observed")
				var ordinary operatorread.OperatorEntityFull
				requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": started.RunID, "entity_id": ordinaryID}, &ordinary)
				if ordinary.Fields["token"] != "ordinary" {
					t.Fatalf("ordinary consumer=%#v", ordinary)
				}
				if ordinary.Fields["revision_id"] != readLifecycleLoop(t, rt, started.RunID, entityID).RevisionID {
					t.Fatalf("ordinary consumer lost next-attempt revision: %#v", ordinary)
				}
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, started.RunID)
				requireLifecycleEventCount(t, rt, started.RunID, "ordinary.repeated", 1)
				requireLifecycleEventCount(t, rt, started.RunID, "loop.escaped", attempt-1)
			}
			closed := readLifecycleLoop(t, rt, started.RunID, entityID)
			if closed.Status != loopruntime.StatusClosed || closed.CloseReason != "escaped" || closed.Attempt != 2 || closed.RevisionID == first.RevisionID {
				t.Fatalf("closed loop=%#v", closed)
			}
			receipt := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, started.RunID, "", "done")
			var received operatorread.OperatorEntityFull
			requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": started.RunID, "entity_id": receipt}, &received)
			if receipt == entityID || received.Fields["revision_id"] != closed.RevisionID {
				t.Fatalf("escape consumer=%#v loop=%#v", received, closed)
			}
			history := readLifecycleTransitionHistory(t, rt, started.RunID, entityID)
			if len(history) != 5 || history[4].TriggerEventID != capRepeat.EventID {
				t.Fatalf("cap history=%#v", history)
			}
			compiled, ok := history[4].Evidence.Compiled()
			if !ok || compiled.FlowID() != "." || compiled.Edge().Source != "loop.escape" || compiled.Edge().LoopID != "revision" {
				t.Fatalf("cap selected cause=%#v", compiled)
			}
			before := lifecycleStoredSnapshot(t, rt, started.RunID)
			duplicate := publish("loop.repeat", "repeat-2", capPayload)
			if duplicate.EventID != capRepeat.EventID {
				t.Fatal("duplicate reminted cap event")
			}
			late := requestServedJSONRPC(t, rt.Endpoint, "event.publish", map[string]any{"event_name": "loop.repeat", "run_id": started.RunID, "source_event_id": started.EventID, "payload": map[string]any{"revision_id": first.RevisionID}, "idempotency_key": "stale-repeat"})
			// Run completion can fence public admission before the loop owner.
			// If admitted, the closed revision must still settle without effects.
			if late.Error != nil && late.Error.Data["code"] != "RUN_ALREADY_TERMINAL" {
				t.Fatalf("late repeat admission=%#v", late.Error)
			}
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, started.RunID)
			if after := lifecycleStoredSnapshot(t, rt, started.RunID); after != before {
				t.Fatalf("duplicate/stale repeat mutated lifecycle:\nbefore=%s\nafter=%s", before, after)
			}
			requireLifecycleEventCount(t, rt, started.RunID, "ordinary.repeated", 1)
			requireLifecycleEventCount(t, rt, started.RunID, "loop.escaped", 1)
		})
	}
}

// SQL is read-only proof of all business rows and persisted transition evidence.
func lifecycleStoredSnapshot(t *testing.T, rt servedControlProofRuntime, runID string) string {
	t.Helper()
	rows, err := rt.DB.Query(`SELECT e.entity_id,e.current_state,e.revision,CAST(e.fields AS TEXT),CAST(f.config AS TEXT),CAST(e.accumulator AS TEXT) FROM entity_state e JOIN flow_instances f ON f.run_id=e.run_id AND f.instance_path=e.flow_instance WHERE e.run_id=$1 ORDER BY e.entity_id`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	result := ""
	for rows.Next() {
		var id, state, fields, config, loops string
		var revision int
		if err := rows.Scan(&id, &state, &revision, &fields, &config, &loops); err != nil {
			t.Fatal(err)
		}
		result += fmt.Sprintf("%s/%s/%d/%s/%s/%s\n", id, state, revision, fields, config, loops)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if result == "" {
		t.Fatal("no persisted lifecycle proof rows")
	}
	return result
}
