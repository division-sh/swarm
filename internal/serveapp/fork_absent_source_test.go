package serveapp

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestServedForkRootSourceIngressBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			publication := newExactOriginPublicationProbe(t)
			rt := startServedLiveFlowProofRuntimeFromSource(t, backend, canonicalrouting.CopyRootIngressServedFollowUp(t))
			var started struct {
				RunID   string `json:"run_id"`
				EventID string `json:"event_id"`
			}
			requireServedJSONRPCResult(t, rt.Endpoint, "run.start", map[string]any{
				"bundle_hash": rt.BundleHash, "event_name": "item.received", "payload": map[string]any{"item_id": "ordinary-start"},
			}, &started)
			if started.RunID == "" || started.EventID == "" {
				t.Fatal("run.start lacks exact acknowledged publication identity")
			}
			initialEventID := started.EventID
			var rootEventOwner any = rt.SQLite
			if rt.Postgres != nil {
				rootEventOwner = rt.Postgres
			}
			requireServedLiveRootMode(t, rt.Runtime.ExecutionPosture, storetest.LoadCanonicalEventRecord(t, t.Context(), rootEventOwner, initialEventID), started.RunID)
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, started.RunID)
			published := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"bundle_hash": rt.BundleHash, "run_id": started.RunID, "event_name": "item.processed",
				"payload": map[string]any{"item_id": "review"}, "idempotency_key": "root-source-ingress",
			})
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, started.RunID)
			requireServedRunStatus(t, rt.Endpoint, started.RunID, "live")
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
			if err := waitExactOriginPublications(t.Context(), publication, initialEventID, published.EventID); err != nil {
				t.Fatal(err)
			}
			before := readServedForkRecipientSourceDomain(t, rt, started.RunID)
			requireSourceConserved := func() {
				t.Helper()
				if !reflect.DeepEqual(before, readServedForkRecipientSourceDomain(t, rt, started.RunID)) {
					t.Fatal("live switching changed source business state")
				}
			}
			params := map[string]any{"source_run_id": started.RunID, "fork_event_id": published.EventID, "idempotency_key": "root-source-fork"}
			var fork apiv1.RunForkExecutionResult
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &fork)
			if fork.ExecutedEventCount != 1 || fork.ForkRunStatus != "live" || fork.ForkRunID == "" || fork.SourceFrozen || fork.SourceRunID != started.RunID || fork.ForkEventID != published.EventID {
				t.Fatalf("fork result: %+v", fork)
			}
			waitServedSelectedForkFrontier(t, rt.ReceiverStateReader, rt.ReadRunDeliveries, fork.ForkRunID, published.EventID)
			requireServedRunStatus(t, rt.Endpoint, fork.ForkRunID, "live")
			requireServedRunStatus(t, rt.Endpoint, started.RunID, "paused")
			var replay apiv1.RunForkExecutionResult
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &replay)
			if !reflect.DeepEqual(fork, replay) {
				t.Fatal("repeated fork changed the result")
			}
			requireSourceConserved()
			refusal := requireServedJSONRPCError(t, rt.Endpoint, "run.continue", map[string]any{"run_id": started.RunID})
			if refusal.Data["code"] != apiv1.RunFlowAlreadyLiveCode {
				t.Fatalf("continue did not name its live sibling: %+v", refusal)
			}
			var control map[string]any
			requireServedJSONRPCResult(t, rt.Endpoint, "run.pause", map[string]any{"run_id": fork.ForkRunID}, &control)
			requireServedRunStatus(t, rt.Endpoint, fork.ForkRunID, "paused")
			requireServedJSONRPCResult(t, rt.Endpoint, "run.continue", map[string]any{"run_id": fork.ForkRunID}, &control)
			requireServedRunStatus(t, rt.Endpoint, fork.ForkRunID, "live")
			params["idempotency_key"] = "root-source-second-fork"
			var sibling apiv1.RunForkExecutionResult
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &sibling)
			if sibling.ForkRunID == fork.ForkRunID || sibling.ForkRunStatus != "live" || sibling.SourceFrozen || sibling.SourceRunStatus != "paused" || sibling.ForkEventID != fork.ForkEventID || sibling.ForkRevision != fork.ForkRevision {
				t.Fatalf("older source did not select a distinct same-cut sibling: %+v", sibling)
			}
			requireServedRunStatus(t, rt.Endpoint, fork.ForkRunID, "paused")
			requireServedRunStatus(t, rt.Endpoint, sibling.ForkRunID, "live")
			requireSourceConserved()
			var latest apiv1.RunForkExecutionResult
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", map[string]any{
				"source_run_id": sibling.ForkRunID, "idempotency_key": "root-latest-state-fork",
			}, &latest)
			if latest.ForkPointKind != "state_revision" || latest.ForkEventID != "" || latest.ForkRevision <= 0 || latest.ExecutedEventCount != 0 || latest.ForkRunStatus != "live" || latest.SourceRunStatus != "paused" || latest.SourceFrozen {
				t.Fatalf("latest did not use an input-free committed state cut: %+v", latest)
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
