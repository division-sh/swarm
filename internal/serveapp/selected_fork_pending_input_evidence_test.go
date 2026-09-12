package serveapp

import (
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/google/uuid"
)

func TestSelectedForkPendingInputEvidenceBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, fault := range []string{"payload", "source_run", "event_identity", "schema", "targets", "foreign_target", "foreign_artifact", "operator_reference", "live_payload"} {
			t.Run(string(backend)+"/"+fault, func(t *testing.T) {
				var selected *selectedStoreOwner
				previous := projectRuntimePersistenceForServe
				projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence { selected = owner; return previous(owner) }
				t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
				rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopySelectedForkPendingInput(t, canonicalrouting.PendingInputOriginal))
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.seeded", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "seed"})
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
				requireServedOKJSONRPC(t, rt.Endpoint, "run.pause", map[string]any{"run_id": seed.RunID, "idempotency_key": "pause"})
				point := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.first", "run_id": seed.RunID, "payload": map[string]any{"token": "proof"}, "idempotency_key": "pending"})
				family, ok := selected.RunFork()
				if !ok {
					t.Fatal("missing selected fork owner")
				}
				ctx := servedControlProofAuthorActivityContext(t, rt)
				plan, err := family.Plan(ctx, runfork.RunForkPlanRequest{SourceRunID: seed.RunID, At: point.EventID})
				if err != nil {
					t.Fatal(err)
				}
				loader := runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repoRootForTest(), PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath), Store: selected.SourceArtifactStore()}
				request, _ := admitForkHistoricalSelectedRequest(t, ctx, family, loader, plan, rt.BundleHash, rt.ForkRuntime)
				var revision int64
				var original []byte
				if err := rt.DB.QueryRow(`SELECT revision,fact FROM run_fork_fact_revisions WHERE run_id=$1 AND family='events' AND fact_key=$2 AND revision<=$3 ORDER BY revision DESC LIMIT 1`, seed.RunID, point.EventID, plan.ForkPoint.Revision).Scan(&revision, &original); err != nil {
					t.Fatal(err)
				}
				var fact map[string]any
				if err := json.Unmarshal(original, &fact); err != nil {
					t.Fatal(err)
				}
				if fault == "live_payload" {
					changed, err := rt.DB.Exec(`UPDATE events SET payload_bytes=$1 WHERE event_id=$2`, []byte(`{"token":"changed-after-preparation"}`), point.EventID)
					if err != nil {
						t.Fatal(err)
					}
					if n, err := changed.RowsAffected(); n != 1 || err != nil {
						t.Fatalf("live evidence fault affected %d rows: %v", n, err)
					}
					var mutated []byte
					if err := rt.DB.QueryRow(`SELECT payload_bytes FROM events WHERE event_id=$1`, point.EventID).Scan(&mutated); err != nil {
						t.Fatal(err)
					}
					if string(mutated) != `{"token":"changed-after-preparation"}` {
						t.Fatalf("canonical byte fault did not apply: %s", mutated)
					}
					response := requestServedJSONRPC(t, rt.Endpoint, "run.fork", map[string]any{"source_run_id": seed.RunID, "fork_event_id": point.EventID, "allow_source_freeze": true, "idempotency_key": "stale-input"})
					if response.Error == nil {
						t.Fatal("live canonical payload divergence passed bound source validation")
					}
					var executed int
					if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id<>$1 AND event_name='work.first'`, seed.RunID).Scan(&executed); err != nil {
						t.Fatal(err)
					}
					if executed != 0 {
						t.Fatal("stale input reached fork delivery")
					}
					requirePendingInputStateCount(t, rt, seed.RunID, "ready", 2)
					return
				}
				switch fault {
				case "payload":
					fact["payload_base64"] = base64.StdEncoding.EncodeToString([]byte(`{"token":"changed-after-preparation"}`))
				case "source_run":
					fact["run_id"] = uuid.NewString()
				case "event_identity":
					fact["event_id"] = uuid.NewString()
				case "schema":
					fact["payload_schema_flow_id"] = "child"
				case "targets":
					fact["target_set"] = nil
				case "foreign_target":
					fact["target_set"] = []events.RouteIdentity{{FlowID: "child", FlowInstance: "child", EntityID: uuid.NewString()}}
				case "foreign_artifact":
					fact["payload_schema_bundle_hash"] = "bundle-v2:sha256:" + strings.Repeat("a", 64)
				case "operator_reference":
					fact["operator_reference_event_id"] = uuid.NewString()
				}
				changed, err := json.Marshal(fact)
				if err != nil {
					t.Fatal(err)
				}
				if string(changed) == string(original) {
					t.Fatal("fault did not change fixed input evidence")
				}
				if _, err := rt.DB.Exec(`UPDATE run_fork_fact_revisions SET fact=$1 WHERE run_id=$2 AND family='events' AND fact_key=$3 AND revision=$4`, string(changed), seed.RunID, point.EventID, revision); err != nil {
					t.Fatal(err)
				}
				before := snapshotForkReceiverApplication(t, rt)
				direct, ok := family.Availability().(forkHistoricalSelectedStore)
				if !ok {
					t.Fatal("missing direct store fixture")
				}
				materialized, err := direct.MaterializeRunForkForSelectedContractExecution(ctx, request)
				if err == nil || materialized.ForkRunID != "" {
					t.Fatalf("stale preparation admitted: %+v %v", materialized, err)
				}
				if !reflect.DeepEqual(before, snapshotForkReceiverApplication(t, rt)) {
					t.Fatal("stale preparation changed application state")
				}
			})
		}
	}
}
