package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestServedLifecycleEmitterCompetingExitPublication(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			for _, exit := range []string{"ordinary", "timer"} {
				t.Run(exit, func(t *testing.T) {
					for _, order := range []string{"gate_first", "exit_first", "contended"} {
						t.Run(order, func(t *testing.T) {
							root := canonicalrouting.CopyLifecycleEmitterCompetingExit(t, exit == "timer")
							var rt servedControlProofRuntime
							var diagnostic *lifecycleTimerContenderDiagnostic
							if backend == servedparity.BackendExplicitPostgres && exit == "timer" && order == "contended" {
								rt, diagnostic = startLifecycleTimerContenderDiagnostic(t, root)
							} else {
								rt = startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)
							}
							seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"bundle_hash": rt.BundleHash, "event_name": "work.requested", "payload": map[string]any{"seed": true}, "idempotency_key": "competing-start"})
							if diagnostic != nil {
								diagnostic.runID, diagnostic.seedEventID = seed.RunID, seed.EventID
							}
							entityID := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, "", "review")
							if diagnostic != nil {
								diagnostic.entityID = entityID
							}
							decision := lifecycleGateDecisionParams(t, rt, seed.RunID, "approve")
							_, before := requireReceiverTargetState(t, rt.ReceiverStateReader, seed.RunID, ".", seed.RunID, entityID)
							if before.CurrentState != "review" {
								t.Fatalf("contenders did not start at review: %+v", before)
							}
							// The supported untargeted publication selects this run's
							// sole primary entity through the API route owner.
							cancel := map[string]any{"run_id": seed.RunID, "source_event_id": seed.EventID, "event_name": "work.cancelled", "payload": map[string]any{"seed": true}, "idempotency_key": "competing-cancel"}
							var due time.Time
							if exit == "timer" {
								due = lifecycleCompetingTimerDue(t, rt, seed.RunID, entityID)
							}
							var gateReply, exitReply servedJSONRPCEnvelope
							switch order {
							case "gate_first":
								gateReply = requestServedJSONRPC(t, rt.Endpoint, "mailbox.decide", decision)
								requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, "done")
								if exit == "ordinary" {
									exitReply = requestServedJSONRPC(t, rt.Endpoint, "event.publish", cancel)
								}
							case "exit_first":
								if exit == "ordinary" {
									exitReply = requestServedJSONRPC(t, rt.Endpoint, "event.publish", cancel)
									if exitReply.Error != nil {
										t.Fatalf("ordinary exit admission: %#v", exitReply.Error)
									}
								}
								requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, "cancelled")
								gateReply = requestServedJSONRPC(t, rt.Endpoint, "mailbox.decide", decision)
							case "contended":
								if exit == "ordinary" {
									// Both HTTP requests are released together, unlike the
									// ordered committed-verdict fencing control.
									start := make(chan struct{})
									gateDone, exitDone := make(chan lifecycleContenderResult, 1), make(chan lifecycleContenderResult, 1)
									go func() { <-start; gateDone <- lifecycleContenderRPC(rt.Endpoint, "mailbox.decide", decision) }()
									go func() { <-start; exitDone <- lifecycleContenderRPC(rt.Endpoint, "event.publish", cancel) }()
									close(start)
									gateResult, exitResult := <-gateDone, <-exitDone
									if gateResult.err != nil || exitResult.err != nil {
										t.Fatalf("contender transport: gate=%v exit=%v", gateResult.err, exitResult.err)
									}
									gateReply, exitReply = gateResult.reply, exitResult.reply
								} else {
									// Compete with the real scheduler at its persisted due
									// instant; no forged timer occurrence or clock mutation.
									if time.Until(due) <= 0 {
										t.Fatal("timer already due before contention setup")
									}
									time.Sleep(time.Until(due))
									if diagnostic != nil {
										diagnostic.due = due
										gateReply = diagnostic.decide(t, rt.Endpoint, decision)
									} else {
										gateReply = requestServedJSONRPC(t, rt.Endpoint, "mailbox.decide", decision)
									}
								}
							}
							entity := lifecycleCompetingTerminalEntity(t, rt, seed.RunID, entityID)
							if exit == "timer" && time.Now().Before(due.Add(100*time.Millisecond)) {
								time.Sleep(time.Until(due.Add(100 * time.Millisecond)))
							}
							waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
							requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": seed.RunID, "entity_id": entityID}, &entity)
							won := entity.Entity.CurrentState == "done"
							if (order == "gate_first" && !won) || (order == "exit_first" && won) {
								t.Fatalf("ordered winner changed: %s -> %s", order, entity.Entity.CurrentState)
							}
							count, target := 0, "cancelled"
							if won {
								count, target = 1, "approved"
								if gateReply.Error != nil || entity.Fields["result"] != "approved" {
									t.Errorf("winning verdict lost acknowledgment/consumer effect: error=%#v result=%s entity=%#v", gateReply.Error, gateReply.Result, entity)
								}
							} else if gateReply.Error == nil || gateReply.Error.Data["code"] != "MAILBOX_CARD_SUPERSEDED" || entity.Fields["result"] == "approved" {
								t.Errorf("losing gate acknowledged or mutated consumer: error=%#v result=%s entity=%#v", gateReply.Error, gateReply.Result, entity)
							}
							if exitReply.Error != nil {
								code := exitReply.Error.Data["code"]
								details, _ := exitReply.Error.Data["details"].(map[string]any)
								lateTerminal := code == apiv1.EventPublishFailedCode && details["phase"] == "publish" && details["reason"] == "run is not active: run_id="+seed.RunID+" state=completed"
								if !won || (code != apiv1.RunAlreadyTerminalCode && !lateTerminal) {
									t.Fatalf("unexpected ordinary exit rejection: %#v", exitReply.Error)
								}
								requireLifecycleEventCount(t, rt, seed.RunID, "work.cancelled", 0)
							}
							// Publication, durable subscriber delivery, and public entity
							// state must agree on the same winning gate outcome.
							requireLifecycleEventCount(t, rt, seed.RunID, "work.completed", count)
							var deliveries int
							if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id = d.event_id WHERE e.run_id = $1 AND e.event_name = 'work.completed'`, seed.RunID).Scan(&deliveries); err != nil {
								t.Fatal(err)
							}
							if deliveries != count {
								t.Fatalf("outcome deliveries=%d, want %d", deliveries, count)
							}
							history := readLifecycleTransitionHistory(t, rt.ReceiverStateReader, seed.RunID, entityID)
							if len(history) != 1 {
								t.Fatalf("winner lost bounded current transition: %+v", history)
							}
							winner := history[0]
							if won {
								if winner.From != "approved" || winner.To != "done" {
									t.Fatalf("winning verdict lost consumer transition: %+v", winner)
								}
								// The emitted outcome pins the gate commit before its
								// consumer replaces the current header with approved -> done.
								winner = readLifecycleTransitionAtCut(t, rt.ReceiverStateReader, seed.RunID, entityID, winner.TriggerEventID)
							}
							if winner.From != "review" || winner.To != target {
								t.Fatalf("losing exit committed historical evidence: %+v", winner)
							}
							// Count physical state changes independently: one retained
							// header record cannot prove exactly one historical exit.
							mutations := storetest.ObserveEntityMutationHistory(t, t.Context(), rt.ReceiverStateReader, seed.RunID)
							if err := lifecycleCompetingExitMutationEvidence(mutations, entityID, winner); err != nil {
								t.Fatal(err)
							}
						})
					}
				})
			}
		})
	}
}

func lifecycleCompetingExitMutationEvidence(rows []storetest.EntityMutationEvidence, entityID string, winner pipeline.WorkflowTransitionRecord) error {
	exits := 0
	for _, row := range rows {
		if row.EntityID != entityID || row.Domain != string(mutationlog.DomainLifecycleState) {
			continue
		}
		var from, to string
		if err := json.Unmarshal(row.OldValue, &from); err != nil {
			return fmt.Errorf("decode committed lifecycle source: %w", err)
		}
		if from != "review" {
			continue
		}
		exits++
		if err := json.Unmarshal(row.NewValue, &to); err != nil {
			return fmt.Errorf("decode committed lifecycle target: %w", err)
		}
		if row.Path != "" || to != winner.To || row.CausedByEvent != winner.TriggerEventID {
			return fmt.Errorf("losing or unattributed exit committed mutation: %+v; winner=%+v", row, winner)
		}
	}
	if exits != 1 {
		return fmt.Errorf("review committed %d exits, want exactly one", exits)
	}
	return nil
}

func TestLifecycleCompetingExitMutationEvidenceRequiresOneWinner(t *testing.T) {
	winner := pipeline.WorkflowTransitionRecord{From: "review", To: "approved", TriggerEventID: "gate"}
	row := storetest.EntityMutationEvidence{RunDebugMutation: operatorread.RunDebugMutation{
		EntityID: "receiver", Domain: string(mutationlog.DomainLifecycleState),
		OldValue: json.RawMessage(`"review"`), NewValue: json.RawMessage(`"approved"`), CausedByEvent: "gate",
	}}
	loser, foreign, wrongPath, wrongCause := row, row, row, row
	loser.NewValue = json.RawMessage(`"cancelled"`)
	foreign.EntityID = "other"
	wrongPath.Path = "not-state"
	wrongCause.CausedByEvent = "other"
	for _, test := range []struct {
		name string
		rows []storetest.EntityMutationEvidence
		ok   bool
	}{
		{"one", []storetest.EntityMutationEvidence{row}, true},
		{"foreign", []storetest.EntityMutationEvidence{row, foreign}, true},
		{"missing", nil, false},
		{"duplicate", []storetest.EntityMutationEvidence{row, row}, false},
		{"losing", []storetest.EntityMutationEvidence{row, loser}, false},
		{"wrong_path", []storetest.EntityMutationEvidence{wrongPath}, false},
		{"wrong_cause", []storetest.EntityMutationEvidence{wrongCause}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := lifecycleCompetingExitMutationEvidence(test.rows, "receiver", winner); (err == nil) != test.ok {
				t.Fatalf("exit evidence error=%v, want accepted=%v", err, test.ok)
			}
		})
	}
}

type lifecycleContenderResult struct {
	reply servedJSONRPCEnvelope
	err   error
}

func lifecycleContenderRPC(endpoint, method string, params map[string]any) (result lifecycleContenderResult) {
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": method, "method": method, "params": params})
	if err != nil {
		result.err = err
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		result.err = err
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		result.err = err
		return
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		result.err = fmt.Errorf("%s HTTP status %d", method, response.StatusCode)
		return
	}
	result.err = json.NewDecoder(response.Body).Decode(&result.reply)
	return
}

func lifecycleCompetingTerminalEntity(t *testing.T, rt servedControlProofRuntime, runID, entityID string) operatorread.OperatorEntityFull {
	t.Helper()
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
		var entity operatorread.OperatorEntityFull
		requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": entityID}, &entity)
		if entity.Entity.CurrentState == "done" || entity.Entity.CurrentState == "cancelled" {
			return entity
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("neither competing exit settled: %s", servedEventPublishDebugSummary(t, rt.DB, rt.Backend, runID))
	return operatorread.OperatorEntityFull{}
}

func lifecycleCompetingTimerDue(t *testing.T, rt servedControlProofRuntime, runID, entityID string) time.Time {
	t.Helper()
	var raw any
	if err := rt.DB.QueryRow(`SELECT fire_at FROM timers WHERE run_id = $1 AND entity_id = $2 AND task_type = 'workflow_timer'`, runID, entityID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if due, ok := raw.(time.Time); ok {
		return due
	}
	value := fmt.Sprint(raw)
	if data, ok := raw.([]byte); ok {
		value = string(data)
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999-07:00", "2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05.999999999 -0700 MST", "2006-01-02 15:04:05.999999999"} {
		if due, err := time.Parse(layout, value); err == nil {
			return due
		}
	}
	t.Fatalf("unreadable persisted due time: %T %v", raw, raw)
	return time.Time{}
}
