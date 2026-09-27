package serveapp

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestServedForkRootSourceIngressBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyRootIngressServedFollowUp(t))
			var started struct {
				RunID string `json:"run_id"`
			}
			requireServedJSONRPCResult(t, rt.Endpoint, "run.start", map[string]any{
				"bundle_hash": rt.BundleHash, "event_name": "item.received", "payload": map[string]any{"item_id": "ordinary-start"},
			}, &started)
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, started.RunID)
			published := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"bundle_hash": rt.BundleHash, "run_id": started.RunID, "event_name": "item.processed",
				"payload": map[string]any{"item_id": "review"}, "idempotency_key": "root-source-ingress",
			})
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, started.RunID)
			requireServedRunStatus(t, rt.Endpoint, started.RunID, "completed")
			var kind, sourceRoute string
			if err := rt.DB.QueryRow(`SELECT routing_source_kind, CAST(source_route AS TEXT) FROM events WHERE event_id=$1`, published.EventID).Scan(&kind, &sourceRoute); err != nil {
				t.Fatal(err)
			}
			var source events.RouteIdentity
			if err := json.Unmarshal([]byte(sourceRoute), &source); err != nil {
				t.Fatal(err)
			}
			if kind != "root" || source != (events.RouteIdentity{EntityID: started.RunID}) {
				t.Fatalf("ordinary API admission lost selected-root authority: %s %s", kind, sourceRoute)
			}
			before := readServedForkRecipientSourceDomain(t, rt, started.RunID)
			params := map[string]any{"source_run_id": started.RunID, "fork_event_id": published.EventID, "allow_source_freeze": true, "idempotency_key": "root-source-fork"}
			var fork apiv1.RunForkExecutionResult
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &fork)
			if fork.ExecutedEventCount != 1 || fork.ForkRunID == "" || fork.SourceFrozen || fork.SourceRunID != started.RunID || fork.ForkEventID != published.EventID {
				t.Fatalf("fork result: %+v", fork)
			}
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, fork.ForkRunID)
			requireServedRunStatus(t, rt.Endpoint, fork.ForkRunID, "completed")
			var replay apiv1.RunForkExecutionResult
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &replay)
			if !reflect.DeepEqual(fork, replay) || !reflect.DeepEqual(before, readServedForkRecipientSourceDomain(t, rt, started.RunID)) {
				t.Fatal("repeated fork changed the result or source business state")
			}
			var replayKind, replayRoute string
			if err := rt.DB.QueryRow(`SELECT e.routing_source_kind, CAST(e.source_route AS TEXT) FROM events e JOIN run_fork_selected_contract_executions x ON x.fork_run_id=e.run_id AND x.fork_event_id=e.event_id WHERE x.fork_run_id=$1 AND x.source_event_id=$2 AND e.event_name='item.processed'`, fork.ForkRunID, published.EventID).Scan(&replayKind, &replayRoute); err != nil {
				t.Fatal(err)
			}
			var replaySource events.RouteIdentity
			if err := json.Unmarshal([]byte(replayRoute), &replaySource); err != nil {
				t.Fatal(err)
			}
			if replayKind != "root" || replaySource != (events.RouteIdentity{EntityID: fork.ForkRunID}) {
				t.Fatalf("selected execution lost fork-local root ownership: %s %s", replayKind, replayRoute)
			}
			for method, params := range map[string]map[string]any{
				"run.get":     {"run_id": fork.ForkRunID},
				"event.list":  {"filter": map[string]any{"run_id": fork.ForkRunID}, "limit": 500},
				"entity.list": {"run_id": fork.ForkRunID, "limit": 500},
			} {
				var result map[string]any
				requireServedJSONRPCResult(t, rt.Endpoint, method, params, &result)
				if len(result) == 0 {
					t.Fatalf("%s returned no public readback", method)
				}
			}
		})
	}
}
