package serveapp

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

type servedForkDeliveryEvidence struct {
	eventID, identity string
	route             events.DeliveryRoute
}

func readServedForkDeliveryEvidence(t *testing.T, rt servedControlProofRuntime, runID string) map[string]servedForkDeliveryEvidence {
	t.Helper()
	rows, err := rt.DB.Query(`SELECT delivery_id,event_id,route_identity,subscriber_type,subscriber_id,CAST(delivery_target_route AS TEXT),CAST(delivery_context AS TEXT),CAST(delivery_payload_projection AS TEXT),CAST(connect_execution_claim AS TEXT) FROM event_deliveries WHERE run_id=$1 ORDER BY delivery_id`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]servedForkDeliveryEvidence{}
	for rows.Next() {
		var id, eventID, identity, kind, recipient, target, contextJSON, projection, claim string
		if err := rows.Scan(&id, &eventID, &identity, &kind, &recipient, &target, &contextJSON, &projection, &claim); err != nil {
			t.Fatal(err)
		}
		if kind != "node" {
			t.Fatalf("ordinary node fixture unexpectedly produced %s delivery", kind)
		}
		wire, err := json.Marshal(map[string]any{
			"subscriber_type": kind, "subscriber_id": recipient,
			"delivery_target_ownership": json.RawMessage(target), "delivery_context": json.RawMessage(contextJSON),
			"delivery_payload_projection": json.RawMessage(projection), "connect_execution_claim": json.RawMessage(claim),
		})
		if err != nil {
			t.Fatal(err)
		}
		var route events.DeliveryRoute
		if err := json.Unmarshal(wire, &route); err != nil {
			t.Fatal(err)
		}
		identityFact, err := route.Identity()
		if err != nil || events.EncodeDeliveryRouteIdentity(identityFact) != identity {
			t.Fatalf("live delivery %s full route disagrees with stored identity: %v", id, err)
		}
		out[id] = servedForkDeliveryEvidence{eventID: eventID, identity: identity, route: route.Normalized()}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func requireServedForkPlanDeliveryEvidence(t *testing.T, plan runfork.RunForkPlan, live map[string]servedForkDeliveryEvidence) {
	t.Helper()
	seen := map[string]bool{}
	connected, ordinary := 0, 0
	for _, item := range plan.PendingWork {
		if item.DeliveryID == "" {
			continue
		}
		want, ok := live[item.DeliveryID]
		if !ok || seen[item.DeliveryID] || item.EventID != want.eventID {
			t.Fatalf("historical delivery membership differs: %#v", item)
		}
		seen[item.DeliveryID] = true
		identity, err := item.DeliveryRoute.Identity()
		if err != nil || events.EncodeDeliveryRouteIdentity(identity) != want.identity || !reflect.DeepEqual(item.DeliveryRoute.Normalized(), want.route) {
			t.Fatalf("historical delivery %s lost complete route evidence: got=%#v want=%#v error=%v", item.DeliveryID, item.DeliveryRoute, want.route, err)
		}
		if item.DeliveryRoute.ConnectClaim.Empty() {
			ordinary++
		} else {
			connected++
		}
	}
	if len(seen) != len(live) || connected != 2 || ordinary != 1 {
		t.Fatalf("historical coverage: seen=%d live=%d connected=%d ordinary=%d", len(seen), len(live), connected, ordinary)
	}
}

func TestServedForkConnectedDeliveryRouteEvidenceOnBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			var selected *selectedStoreOwner
			previous := projectRuntimePersistenceForServe
			projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
				selected = owner
				return previous(owner)
			}
			t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyForkDeliveryRouteEvidence(t))
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "parent.seeded", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"work_id": "fork-connected-history"}, "idempotency_key": "fork-history-seed",
			})
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
			live := readServedForkDeliveryEvidence(t, rt, seed.RunID)
			if selected == nil {
				t.Fatal("served runtime did not expose its selected persistence owner")
			}
			planner, ok := selected.RunFork()
			if !ok {
				t.Fatal("served runtime has no selected fork owner")
			}
			ctx := servedControlProofAuthorActivityContext(t, rt)
			plan, err := planner.Plan(ctx, runfork.RunForkPlanRequest{SourceRunID: seed.RunID})
			if err != nil {
				t.Fatalf("plan real connected source: %v", err)
			}
			if plan.SourceRunID != seed.RunID || plan.ForkPoint.EventID == "" {
				t.Fatalf("plan lost source/fixed event identity: %#v", plan)
			}
			if !t.Run("historical_route_evidence", func(t *testing.T) {
				requireServedForkPlanDeliveryEvidence(t, plan, live)
				fixed, err := planner.Plan(ctx, runfork.RunForkPlanRequest{SourceRunID: seed.RunID, At: plan.ForkPoint.EventID})
				if err != nil || fixed.ForkPoint.EventID != plan.ForkPoint.EventID {
					t.Fatalf("fixed revision plan changed: %v", err)
				}
				requireServedForkPlanDeliveryEvidence(t, fixed, live)
				source := rt.Runtime.Options.WorkflowModule.SemanticSource()
				frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{Plan: fixed, Source: source})
				if err != nil {
					t.Fatal(err)
				}
				history, err := runforkadmission.AdmitSelectedContractRouteHistory(runforkadmission.SelectedContractRouteHistoryRequest{Plan: fixed, Source: source, FrontierAdmission: frontier})
				if err != nil {
					t.Fatal(err)
				}
				for _, want := range live {
					if want.route.ConnectClaim.Empty() {
						continue
					}
					matches := 0
					// The fixed event revision, not today's settled status, decides
					// whether the exact stamped route belongs to frontier or history.
					for _, event := range frontier.FrontierEvents {
						if event.SourceEventID != want.eventID {
							continue
						}
						for _, route := range event.HistoricalDeliveryRoutes {
							if reflect.DeepEqual(route, want.route) {
								matches++
							}
						}
					}
					for _, event := range history.SelectedRouteEvents {
						if event.SourceEventID != want.eventID {
							continue
						}
						for _, route := range event.HistoricalDeliveryRoutes {
							if reflect.DeepEqual(route, want.route) {
								matches++
							}
						}
					}
					if matches != 1 {
						t.Fatalf("historical event %s did not reach exact stamped recipient: matches=%d history=%#v", want.eventID, matches, history)
					}
				}
			}) {
				return
			}
			t.Run("http_fork", func(t *testing.T) {
				var fork apiv1.RunForkExecutionResult
				requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", map[string]any{
					"source_run_id": seed.RunID, "fork_event_id": plan.ForkPoint.EventID,
					"confirm_source_freeze": true, "idempotency_key": "fork-history",
				}, &fork)
				if fork.ForkRunID == "" || fork.ForkRunID == seed.RunID || fork.SourceRunID != seed.RunID || fork.ForkEventID != plan.ForkPoint.EventID {
					t.Fatalf("invalid fork identity: %+v", fork)
				}
				if got := readServedForkDeliveryEvidence(t, rt, seed.RunID); !reflect.DeepEqual(got, live) {
					t.Fatalf("fork changed source delivery authority: got=%#v want=%#v", got, live)
				}
			})
		})
	}
}
