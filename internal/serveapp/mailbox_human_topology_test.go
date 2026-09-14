package serveapp

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestHumanTaskRealRequesterTopologyBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, mode := range []string{"root", "static", "singleton", "template"} {
			t.Run(string(backend)+"/"+mode, func(t *testing.T) {
				root := canonicalrouting.CopyHumanTaskOwnership(t, mode)
				rt, owner, restart := newRetainedMailboxCompletionRuntime(t, backend, root)
				type completedRequest struct {
					method, runID    string
					params, response map[string]any
				}
				var completed []completedRequest
				for _, method := range []string{"mailbox.defer", "mailbox.decide", "expiry_restart"} {
					t.Run(method, func(t *testing.T) {
						f := mailboxCompletionFixtureInRuntime(t, rt, owner)
						eventName, flowID, instance := "observers/observer.requested", "observers", "observers"
						payload := map[string]any{"seed": true}
						if mode == "root" {
							eventName, flowID, instance = "observer.requested", ".", f.base.RunID
						} else if mode == "template" {
							key := uuid.NewString()
							eventName, instance = "observer.seed", "observers/"+key
							payload = map[string]any{"case_id": key}
						}
						deadline := time.Now().UTC().Add(24 * time.Hour)
						if method == "expiry_restart" {
							deadline = time.Now().UTC().Add(2 * time.Second)
						}
						payload["deadline_at"] = deadline.Format(time.RFC3339Nano)
						requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": eventName, "run_id": f.base.RunID, "source_event_id": f.eventID, "payload": payload, "idempotency_key": "human-topology-" + f.base.RunID})
						waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, f.base.RunID)
						expectedEntity := ""
						if mode == "template" {
							var fields string
							if err := rt.DB.QueryRow(`SELECT e.flow_instance,e.entity_id,CAST(e.fields AS TEXT) FROM entity_state e JOIN flow_instances f ON f.run_id=e.run_id AND f.instance_path=e.flow_instance WHERE e.run_id=$1 AND f.flow_template='observers'`, f.base.RunID).Scan(&instance, &expectedEntity, &fields); err != nil {
								t.Fatal(err)
							}
							var values map[string]any
							if err := json.Unmarshal([]byte(fields), &values); err != nil || values["case_id"] != payload["case_id"] {
								t.Fatalf("materialized requester lost authored case key: %s, %v", fields, err)
							}
						}
						var id string
						if err := rt.DB.QueryRow(`SELECT card_id FROM decision_cards WHERE run_id=$1 AND anchor_kind='human_task'`, f.base.RunID).Scan(&id); err != nil {
							t.Fatalf("real ask_human card: %v\n%s", err, servedEventPublishDebugSummary(t, rt.DB, rt.Backend, f.base.RunID))
						}
						card, err := owner.GetDecisionCard(f.ctx, id)
						if err != nil {
							t.Fatal(err)
						}
						anchor, err := card.Anchor.HumanTask()
						if err != nil {
							t.Fatal(err)
						}
						route := anchor.Source.Route()
						agentScope, agentPath := flowID, instance
						if mode == "root" {
							// Root agent identity is run-bound without a child route.
							agentScope, agentPath = "", ""
						}
						if route.FlowID != flowID || route.FlowInstance != instance || route.EntityID != expectedEntity {
							t.Fatalf("requester ownership for %s: %+v", mode, route)
						}
						control, err := card.Anchor.ControlRoutingSource()
						if err != nil || control.Kind() != events.RoutingSourceFlowOwnedControl || control.Route() != route {
							t.Fatalf("control changed requester ownership: %v %v", control, err)
						}
						params := map[string]any{"card_id": id, "idempotency_key": "human-outcome-" + id}
						outcome := "human_task.deferred"
						if method == "mailbox.decide" {
							params["verdict"], params["observed_content_hash"] = "approve", card.CardContentHash
							outcome = "human_task.approved"
						} else if method == "mailbox.defer" {
							params["until"] = time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano)
						}
						if method != "expiry_restart" {
							requireHumanTaskForeignContinuationRefusal(t, rt, card, method, params)
						}
						if method == "expiry_restart" {
							// Preserve the tool-authored deadline. Startup maintenance,
							// not an injected expiry event or edited row, expires it.
							if remaining := time.Until(deadline); remaining > 0 {
								time.Sleep(remaining)
							}
							rt, owner = restart()
							recovered, err := owner.GetDecisionCard(servedControlProofAuthorActivityContext(t, rt), id)
							if err != nil || recovered.Status != decisioncard.StatusExpired || !reflect.DeepEqual(recovered.Anchor, card.Anchor) {
								t.Fatalf("expiry restart lost canonical card: %+v, %v", recovered, err)
							}
							outcome = "human_task.expired"
							for _, request := range completed {
								var replay map[string]any
								waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, request.runID)
								before := mailboxCompletionRunEffects(t, rt, request.runID)
								requireServedJSONRPCResult(t, rt.Endpoint, request.method, request.params, &replay)
								want := make(map[string]any, len(request.response))
								for key, value := range request.response {
									want[key] = value
								}
								want["idempotency_replayed"] = true
								if !reflect.DeepEqual(replay, want) {
									t.Fatalf("restart response changed: got=%#v want=%#v", replay, want)
								}
								if after := mailboxCompletionRunEffects(t, rt, request.runID); !reflect.DeepEqual(before, after) {
									t.Fatalf("completed retry changed run effects: %v -> %v", before, after)
								}
							}
						} else {
							var response map[string]any
							requireServedJSONRPCResult(t, rt.Endpoint, method, params, &response)
							completed = append(completed, completedRequest{method, f.base.RunID, params, response})
						}
						waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, f.base.RunID)
						rows, err := rt.DB.Query(`SELECT d.subscriber_id,d.status,CAST(d.delivery_target_route AS TEXT),d.agent_flow_scope_key,d.agent_flow_instance_path,e.event_id,e.source_event_id FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE e.run_id=$1 AND e.event_name=$2`, f.base.RunID, outcome)
						if err != nil {
							t.Fatal(err)
						}
						defer rows.Close()
						count := 0
						for rows.Next() {
							var recipient, status, raw, scope, path, eventID, sourceID string
							if err := rows.Scan(&recipient, &status, &raw, &scope, &path, &eventID, &sourceID); err != nil {
								t.Fatal(err)
							}
							var target events.DeliveryTargetOwnership
							if err := json.Unmarshal([]byte(raw), &target); err != nil {
								t.Fatal(err)
							}
							if target.Route() != route || target.ExistingEntity() != (mode == "template") || target.EntitylessReceiver() != (mode != "template") || recipient != anchor.RequesterAgentID || status != "delivered" || scope != agentScope || path != agentPath {
								t.Fatalf("wrong final requester delivery: %s %s %s %s/%s", recipient, status, raw, scope, path)
							}
							if method != "mailbox.defer" && eventID != decisioncard.HumanTaskOutcomeEventID(id, sourceID) {
								t.Fatalf("final outcome did not use canonical identity: %s from %s", eventID, sourceID)
							}
							count++
						}
						if err := rows.Err(); err != nil {
							t.Fatal(err)
						}
						if count != 1 {
							t.Fatalf("requester outcome deliveries=%d, want exactly one\n%s", count, servedEventPublishDebugSummary(t, rt.DB, rt.Backend, f.base.RunID))
						}
					})
				}
			})
		}
	}
}

func requireHumanTaskForeignContinuationRefusal(t *testing.T, rt servedControlProofRuntime, card decisioncard.Card, method string, params map[string]any) {
	t.Helper()
	for _, tc := range []struct{ column, value string }{
		{"requester_flow_id", "foreign"},
		{"requester_flow_instance", "foreign/instance"},
		{"requester_entity_id", uuid.NewString()},
	} {
		t.Run("reject_"+tc.column, func(t *testing.T) {
			restore, err := storetest.CorruptHumanTaskRequester(context.Background(), selectedMailboxFixtureStore(rt), card.RunID, card.CardID, tc.column, tc.value)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := restore(context.Background()); err != nil {
					t.Error(err)
				}
			})
			before := mailboxCompletionRunEffects(t, rt, card.RunID)
			if response := requestServedJSONRPC(t, rt.Endpoint, method, params); response.Error == nil {
				t.Fatalf("foreign requester %s was admitted", tc.column)
			}
			if after := mailboxCompletionRunEffects(t, rt, card.RunID); !reflect.DeepEqual(before, after) {
				t.Fatalf("foreign requester mutation changed run effects: %v -> %v", before, after)
			}
			var status string
			if err := rt.DB.QueryRow(`SELECT status FROM decision_cards WHERE card_id=$1`, card.CardID).Scan(&status); err != nil || status != string(card.Status) {
				t.Fatalf("foreign requester changed card: %s, %v", status, err)
			}
			var completions int
			if err := rt.DB.QueryRow(`SELECT count(*) FROM api_idempotency WHERE idempotency_key=$1`, params["idempotency_key"]).Scan(&completions); err != nil || completions != 0 {
				t.Fatalf("foreign requester committed success: %d, %v", completions, err)
			}
		})
	}
}

func selectedMailboxFixtureStore(rt servedControlProofRuntime) any {
	if rt.Postgres != nil {
		return rt.Postgres
	}
	return rt.SQLite
}
