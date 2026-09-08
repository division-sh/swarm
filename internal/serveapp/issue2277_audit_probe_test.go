package serveapp

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"gopkg.in/yaml.v3"
)

// These are baseline classification probes, not assertions of the proposed
// unloaded-target behavior. No execution admission is bypassed.
func TestAudit2277ForkCompletionLoss(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rt := startServedControlProofRuntimeWithFixture(t, backend, func(t *testing.T) string { return canonicalrouting.CopyRootIngressServedFollowUp(t) })
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"bundle_hash": rt.BundleHash, "event_name": "item.received", "payload": map[string]any{"item_id": "hold"}, "idempotency_key": "audit2277-source"})
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
			point := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"bundle_hash": rt.BundleHash, "run_id": seed.RunID, "event_name": "item.processed", "payload": map[string]any{"item_id": "hold"}, "idempotency_key": "audit2277-point"})
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
			if rt.Backend == "sqlite" {
				if _, err := rt.DB.Exec(`CREATE TRIGGER audit2277_completion BEFORE INSERT ON api_idempotency WHEN NEW.method='run.fork' BEGIN SELECT RAISE(ABORT, 'audit2277 post-domain completion loss'); END`); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := rt.DB.Exec(`CREATE FUNCTION audit2277_completion_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'audit2277 post-domain completion loss'; END $$`); err != nil {
					t.Fatal(err)
				}
				if _, err := rt.DB.Exec(`CREATE TRIGGER audit2277_completion BEFORE INSERT ON api_idempotency FOR EACH ROW WHEN (NEW.method='run.fork') EXECUTE FUNCTION audit2277_completion_failure()`); err != nil {
					t.Fatal(err)
				}
			}
			params := map[string]any{"source_run_id": seed.RunID, "fork_event_id": point.EventID, "allow_source_freeze": true, "idempotency_key": "audit2277-fork"}
			first := requestServedJSONRPCWithTimeout(t, rt.Endpoint, "run.fork", params, 30*time.Second)
			var forks, completions int
			if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM runs WHERE forked_from_run_id=$1`, seed.RunID).Scan(&forks); err != nil {
				t.Fatal(err)
			}
			if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM api_idempotency WHERE method='run.fork'`).Scan(&completions); err != nil {
				t.Fatal(err)
			}
			t.Logf("AFTER LOSS response=%+v error=%+v forks=%d completions=%d", first, first.Error, forks, completions)
			if first.Error == nil || forks != 1 || completions != 0 {
				t.Fatal("probe did not reach post-domain completion-loss cut")
			}
			var forkID, forkStatus, executionState string
			if err := rt.DB.QueryRow(`SELECT run_id,status FROM runs WHERE forked_from_run_id=$1`, seed.RunID).Scan(&forkID, &forkStatus); err != nil {
				t.Fatal(err)
			}
			if err := rt.DB.QueryRow(`SELECT state FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1`, forkID).Scan(&executionState); err != nil {
				t.Fatal(err)
			}
			t.Logf("COMMITTED DOMAIN fork=%s status=%s execution=%s", forkID, forkStatus, executionState)
			if executionState != "closed" {
				t.Fatal("selected execution did not finish before injected completion loss")
			}
			before := repeatedStaticRunSnapshot(t, rt.DB, forkID)
			if rt.Backend == "sqlite" {
				if _, err := rt.DB.Exec(`DROP TRIGGER audit2277_completion`); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := rt.DB.Exec(`DROP TRIGGER audit2277_completion ON api_idempotency`); err != nil {
					t.Fatal(err)
				}
			}
			retry := requestServedJSONRPCWithTimeout(t, rt.Endpoint, "run.fork", params, 30*time.Second)
			if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM runs WHERE forked_from_run_id=$1`, seed.RunID).Scan(&forks); err != nil {
				t.Fatal(err)
			}
			if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM api_idempotency WHERE method='run.fork'`).Scan(&completions); err != nil {
				t.Fatal(err)
			}
			t.Logf("SAME KEY RETRY response=%+v error=%+v forks=%d completions=%d", retry, retry.Error, forks, completions)
			if !reflect.DeepEqual(before, repeatedStaticRunSnapshot(t, rt.DB, forkID)) {
				t.Fatal("same-key retry changed fork domain evidence")
			}
			if retry.Error == nil || forks != 1 || completions != 0 {
				t.Fatal("completion-loss baseline disposition changed; reclassify")
			}
		})
	}
}

func TestAudit2277StoppedRunReadinessRestart(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := canonicalrouting.CopyRootIngressLegacyTemplateTargetRoute(t)
			bundle := servedEventPublishFixtureBundleHash(t, root)
			unsetStoreSelectorEnv(t)
			stubServeRuntimeWorkspaceLifecycle(t)
			opts := cliapp.ServeOptions{SourceRoot: root, PlatformSpecPath: defaultPlatformSpecPath, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig()}
			var db *sql.DB
			if backend == "sqlite" {
				opts.ConfigPath = writeStoreBackendRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "audit2277.sqlite"))
				captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { db, _, _ = selectedRuntimeStoreForTest(t, p) })
			} else {
				dsn, _, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				original := buildStoresForServe
				buildStoresForServe = func(_ context.Context, _ storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
					pg, err := store.NewPostgresStore(dsn)
					if err != nil {
						return nil, err
					}
					storetest.BootstrapPostgresRuntimeStore(t, pg)
					db = storetest.DatabaseForTest(pg)
					return openSelectedPostgresOwner(t, dsn, db, cfg), nil
				}
				t.Cleanup(func() { buildStoresForServe = original })
				opts.ConfigPath = writeServeRuntimeTestConfig(t)
				opts.StoreMode, opts.StoreModeSet = "postgres", true
			}
			first := startServeRuntimeTestProcess(t, opts)
			first.waitForReadyLine()
			endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, first.outputString()) + "/v1/rpc"
			seed := requireServedEventPublishRPCResult(t, endpoint, map[string]any{"bundle_hash": bundle, "event_name": "opco.bootstrap_requested", "payload": map[string]any{"owner": "operator"}, "idempotency_key": "audit2277-stop-source"})
			requireServedEventPublishEntityState(t, db, backend, seed.RunID, "", "waiting")
			requireServedEventPublishRPCResult(t, endpoint, map[string]any{"run_id": seed.RunID, "source_event_id": seed.EventID, "event_name": "opco.spinup_requested", "payload": map[string]any{"instance_id": "11111111-1111-4111-8111-111111111111", "product_id": "product-1"}, "idempotency_key": "audit2277-spinup"})
			waitServedEventPublishEventID(t, db, backend, seed.RunID, "operating/11111111-1111-4111-8111-111111111111/opco.product_initialization_requested")
			waitServedRunDeliveryQuiescence(t, db, backend, seed.RunID)
			var readiness int
			if err := db.QueryRow(`SELECT COUNT(*) FROM flow_instance_runtime_readiness WHERE run_id=$1`, seed.RunID).Scan(&readiness); err != nil {
				t.Fatal(err)
			}
			t.Logf("BEFORE STOP readiness rows=%d", readiness)
			if readiness == 0 {
				t.Fatal("fixture has no readiness evidence")
			}
			var stopped any
			requireServedJSONRPCResult(t, endpoint, "run.stop", map[string]any{"run_id": seed.RunID, "idempotency_key": "audit2277-stop"}, &stopped)
			requireServedRunStatusWithDebug(t, endpoint, db, backend, seed.RunID, "cancelled")
			before := repeatedStaticRunSnapshot(t, db, seed.RunID)
			var readinessBefore string
			if err := db.QueryRow(`SELECT CAST(plan AS TEXT) FROM flow_instance_runtime_readiness WHERE run_id=$1`, seed.RunID).Scan(&readinessBefore); err != nil {
				t.Fatal(err)
			}
			if code := first.stop(); code != 0 {
				t.Fatalf("first exit=%d\n%s", code, first.outputString())
			}
			setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
			path := filepath.Join(root, "schema.yaml")
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var schema map[string]any
			if err := yaml.Unmarshal(raw, &schema); err != nil {
				t.Fatal(err)
			}
			schema["name"] = "audit2277-new-source"
			raw, err = yaml.Marshal(schema)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			newBundle := servedEventPublishFixtureBundleHash(t, root)
			if bundle == newBundle {
				t.Fatal("restart did not replace the source coordinate")
			}
			t.Logf("SOURCE REPLACEMENT old=%s new=%s", bundle, newBundle)
			second := startServeRuntimeTestProcess(t, opts)
			second.waitForReadyLine()
			endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, second.outputString()) + "/v1/rpc"
			requireServedRunStatusWithDebug(t, endpoint, db, backend, seed.RunID, "cancelled")
			var readinessAfter string
			if err := db.QueryRow(`SELECT CAST(plan AS TEXT) FROM flow_instance_runtime_readiness WHERE run_id=$1`, seed.RunID).Scan(&readinessAfter); err != nil {
				t.Fatal(err)
			}
			if readinessBefore != readinessAfter || !reflect.DeepEqual(before, repeatedStaticRunSnapshot(t, db, seed.RunID)) {
				t.Fatal("restart mutated stopped predecessor evidence")
			}
			t.Logf("RESTART reached readiness and preserved stopped run\n%s", second.outputString())
			if code := second.stop(); code != 0 {
				t.Fatalf("second exit=%d", code)
			}
		})
	}
}
