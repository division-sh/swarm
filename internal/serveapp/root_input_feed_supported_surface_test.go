package serveapp

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestServedRootInputFeedsExecuteAndSettleBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, tc := range []struct {
				name                   string
				kind                   canonicalrouting.DeploymentFeedRoot
				rows                   string
				wantRows, wantEntities int
			}{
				{"connected-child", canonicalrouting.DeploymentFeedConnectedRoot, "{\"item_id\":\"one\"}\n{\"item_id\":\"two\"}\n", 2, 2},
				{"same-flow", canonicalrouting.DeploymentFeedSameFlowRoot, "{\"item_id\":\"one\"}\n", 1, 1},
				{"keyed-root-opening-event", canonicalrouting.DeploymentFeedKeyedRoot, "{\"item_id\":\"one\"}\n", 1, 1},
			} {
				t.Run(tc.name, func(t *testing.T) {
					_, rt := startServedBoundaryFixture(t, backend, canonicalrouting.CopyRootInputFeed(t, tc.kind))
					owner := rt.Selected.RuntimeDeps().EventStore
					params := map[string]any{
						"bundle_hash": rt.BundleHash, "run_id": uuid.NewString(), "idempotency_key": uuid.NewString(),
						"data": map[string]any{"imports": []any{map[string]any{
							"source_invocation_id": uuid.NewString(), "declaration": map[string]any{"flow_path": ".", "event": "item.registered"},
							"expected_head": map[string]any{"state": "absent"},
							"input":         map[string]any{"format": "jsonl", "content_base64": base64.StdEncoding.EncodeToString([]byte(tc.rows))},
						}}, "pins": []any{}},
					}
					if tc.kind == canonicalrouting.DeploymentFeedKeyedRoot {
						before, err := storetest.ReadFiniteRunStartStorageCounts(t.Context(), owner)
						if err != nil {
							t.Fatal(err)
						}
						refused := publicationCompletionHTTPMethod(t.Context(), rt.Endpoint, "run.start", params)
						after, err := storetest.ReadFiniteRunStartStorageCounts(t.Context(), owner)
						if err != nil || refused.err != nil || len(refused.rpcError) == 0 || before != after {
							t.Fatalf("data-only feed constructed keyed root or changed storage: before=%+v after=%+v rpc=%s transport=%v read=%v", before, after, refused.rpcError, refused.err, err)
						}
						params["idempotency_key"] = uuid.NewString()
						params["event_name"] = "account.opened"
						params["payload"] = map[string]any{"account_id": "account"}
					}
					first := publicationCompletionHTTPMethod(t.Context(), rt.Endpoint, "run.start", params)
					if first.err != nil || len(first.rpcError) != 0 {
						t.Fatalf("root input run.start: transport=%v rpc=%s", first.err, first.rpcError)
					}
					runID := params["run_id"].(string)
					requireServedRunStatus(t, rt.Endpoint, runID, "completed")
					waitServedBoundaryPublicationCompletion(t, rt, runID)
					var rows operatorread.OperatorEventListResult
					requireServedJSONRPCResult(t, rt.Endpoint, "event.list", map[string]any{
						"filter": map[string]any{"run_id": runID, "event_name": "item.registered"}, "limit": 100,
					}, &rows)
					if len(rows.Events) != tc.wantRows || rows.NextCursor != "" {
						t.Fatalf("feed row publications=%+v, want %d", rows, tc.wantRows)
					}
					for _, row := range rows.Events {
						if row.EventName != "item.registered" || row.RunID != runID || row.ProducerType != events.EventProducerExternal || row.Source != "deployment-feed" || len(row.DeadLetters) != 0 || len(row.Deliveries) != 1 || row.Payload["item_id"] == nil {
							t.Fatalf("feed lost declaration-owned source or business row: %+v", row)
						}
						stored, found, err := storetest.ReadCanonicalEventRecord(t.Context(), owner, row.EventID)
						if err != nil || !found || stored.RunID() != runID || stored.RoutingSource().Kind() != events.RoutingSourceDeploymentFeed || stored.RoutingSource().Route() != (events.RouteIdentity{FlowID: "."}) {
							t.Fatalf("feed acquired concrete sender authority: source=%+v found=%t err=%v", stored.RoutingSource(), found, err)
						}
						delivery := row.Deliveries[0]
						flow := "."
						if tc.kind == canonicalrouting.DeploymentFeedConnectedRoot {
							flow = "registry"
						}
						if delivery.Target.FlowID != flow || delivery.Target.EntityID == "" || delivery.Target.FlowInstance == "" || delivery.Status != "delivered" || !delivery.Terminal || delivery.SubscriberType != "node" {
							t.Fatalf("feed did not execute and settle its exact receiver: %+v", delivery)
						}
					}
					var entities operatorread.OperatorEntityListResult
					requireServedJSONRPCResult(t, rt.Endpoint, "entity.list", map[string]any{"run_id": runID}, &entities)
					if len(entities.Entities) != tc.wantEntities {
						t.Fatalf("feed fabricated a producer or lost a receiver: %+v", entities)
					}
					for _, entity := range entities.Entities {
						if entity.CurrentState != "done" || entity.RunID != runID {
							t.Fatalf("feed receiver did not reach its lawful final state: %+v", entity)
						}
					}
					before, err := storetest.ReadFiniteRunStartStorageCounts(t.Context(), owner)
					if err != nil {
						t.Fatal(err)
					}
					replay := publicationCompletionHTTPMethod(t.Context(), rt.Endpoint, "run.start", params)
					if replay.err != nil || len(replay.rpcError) != 0 || !bytes.Equal(first.result, replay.result) {
						t.Fatalf("exact replay changed result: first=%s replay=%s rpc=%s err=%v", first.result, replay.result, replay.rpcError, replay.err)
					}
					changed := make(map[string]any, len(params))
					for key, value := range params {
						changed[key] = value
					}
					changed["data"] = map[string]any{"imports": []any{}, "pins": []any{}}
					conflict := publicationCompletionHTTPMethod(t.Context(), rt.Endpoint, "run.start", changed)
					var refusal struct {
						Data struct {
							Code string `json:"code"`
						} `json:"data"`
					}
					if conflict.err != nil || json.Unmarshal(conflict.rpcError, &refusal) != nil || refusal.Data.Code != apiv1.IdempotencyConflictCode {
						t.Fatalf("changed feed content did not conflict: rpc=%s err=%v", conflict.rpcError, conflict.err)
					}
					after, err := storetest.ReadFiniteRunStartStorageCounts(t.Context(), owner)
					if err != nil || before != after {
						t.Fatalf("replay/conflict mutated durable state: before=%+v after=%+v err=%v", before, after, err)
					}
				})
			}
		})
	}
}
