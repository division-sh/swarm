package serveapp

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/testutil"
)

// This is a tripwire against restoring generic run.fork completion authority.
// Current run.fork must never fire the trigger; this is not INSERT-failure recovery.
func TestAudit2277ForkDurableResultBypassesGenericCompletionBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rt := startServedControlProofRuntimeWithFixture(t, backend, func(t *testing.T) string { return canonicalrouting.CopyRootIngressServedFollowUp(t) })
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"bundle_hash": rt.BundleHash, "event_name": "item.received", "payload": map[string]any{"item_id": "hold"}, "idempotency_key": "audit2277-source"})
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
			point := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"bundle_hash": rt.BundleHash, "run_id": seed.RunID, "event_name": "item.processed", "payload": map[string]any{"item_id": "hold"}, "idempotency_key": "audit2277-point"})
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
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
			{
				var err error
				forks, err = readServedSourceForkCardinality(rt.ReceiverStateReader, seed.RunID)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM api_idempotency WHERE method='run.fork'`).Scan(&completions); err != nil {
				t.Fatal(err)
			}
			t.Logf("GENERIC COMPLETION TRIPWIRE response=%+v error=%+v forks=%d completions=%d", first, first.Error, forks, completions)
			if first.Error != nil || forks != 1 || completions != 0 {
				t.Fatal("durable fork result depended on forbidden generic completion authority")
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
				t.Fatal("selected execution did not finish before the first response")
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
			{
				var err error
				forks, err = readServedSourceForkCardinality(rt.ReceiverStateReader, seed.RunID)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM api_idempotency WHERE method='run.fork'`).Scan(&completions); err != nil {
				t.Fatal(err)
			}
			t.Logf("SAME KEY RETRY response=%+v error=%+v forks=%d completions=%d", retry, retry.Error, forks, completions)
			if !reflect.DeepEqual(before, repeatedStaticRunSnapshot(t, rt.DB, forkID)) {
				t.Fatal("same-key retry changed fork domain evidence")
			}
			if retry.Error != nil || forks != 1 || completions != 0 || !reflect.DeepEqual(first.Result, retry.Result) {
				t.Fatal("durable same-key fork replay lost its original result or changed domain state")
			}
		})
	}
}

func TestAudit2277StoppedRunReadinessRestart(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			probe := lifecycletest.New(t)
			root := canonicalrouting.CopyRootIngressLegacyTemplateTargetRoute(t)
			bundle := servedEventPublishFixtureBundleHash(t, root)
			unsetStoreSelectorEnv(t)
			stubServeRuntimeWorkspaceLifecycle(t)
			opts := cliapp.ServeOptions{SourceRoot: root, PlatformSpecPath: defaultPlatformSpecPath, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig()}
			opts.TestLifecycleProbe = probe
			var db *sql.DB
			var reader receiverProofStateReader
			var deliveryReader servedRunDeliveryReader
			captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { deliveryReader = p.deps.DeliveryStore.SummarizeRun })
			captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) {
				db, _, _ = selectedRuntimeStoreForTest(t, p)
				reader = requireReceiverProofStateReader(t, p.deps.EventStore)
			})
			if backend == "sqlite" {
				opts.ConfigPath = writeStoreBackendRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "audit2277.sqlite"))
			} else {
				dsn, _, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				original := buildStoresForServe
				buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
					return storeselected.OpenRuntime(ctx, storeselected.RuntimeRequest{Selection: selection, PostgresDSN: dsn, SessionLockTTL: runtimeSessionLockTTL(cfg)})
				}
				t.Cleanup(func() { buildStoresForServe = original })
				opts.ConfigPath = writeServeRuntimeTestConfig(t)
				opts.StoreMode, opts.StoreModeSet = "postgres", true
			}
			first := startServeRuntimeTestProcess(t, opts)
			first.waitForReadyLine()
			endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, first.outputString()) + "/v1/rpc"
			seed := requireServedEventPublishRPCResult(t, endpoint, map[string]any{"bundle_hash": bundle, "event_name": "opco.bootstrap_requested", "payload": map[string]any{"owner": "operator"}, "idempotency_key": "audit2277-stop-source"})
			requireServedEventPublishEntityState(t, reader, backend, seed.RunID, "", "waiting")
			spinup := requireServedEventPublishRPCResult(t, endpoint, map[string]any{"run_id": seed.RunID, "source_event_id": seed.EventID, "event_name": "opco.spinup_requested", "payload": map[string]any{"instance_id": "11111111-1111-4111-8111-111111111111", "product_id": "product-1"}, "idempotency_key": "audit2277-spinup"})
			waitServedEventPublishEventID(t, reader, backend, seed.RunID, servedTypedCreationInstancePath+"/opco.product_initialization_requested")
			waitServedRunDeliveryQuiescence(t, deliveryReader, seed.RunID)
			probe.Expect(seed.EventID).PostCommitDispatchStarted().PostCommitDispatchCompleted().Within(servedEventPublishLifecycleProbeWaitTimeout)
			probe.Expect(spinup.EventID).PostCommitDispatchStarted().PostCommitDispatchCompleted().Within(servedEventPublishLifecycleProbeWaitTimeout)
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
			requireServedRunStatusWithDebug(t, endpoint, reader, backend, seed.RunID, "cancelled")
			before := repeatedStaticRunSnapshot(t, db, seed.RunID)
			var readinessBefore string
			if err := db.QueryRow(`SELECT CAST(plan AS TEXT) FROM flow_instance_runtime_readiness WHERE run_id=$1`, seed.RunID).Scan(&readinessBefore); err != nil {
				t.Fatal(err)
			}
			if code := first.stop(); code != 0 {
				t.Fatalf("first exit=%d\n%s", code, first.outputString())
			}
			setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
			canonicalrouting.RenameStoppedRunReadinessSource(t, root)
			newBundle := servedEventPublishFixtureBundleHash(t, root)
			if bundle == newBundle {
				t.Fatal("restart did not replace the source coordinate")
			}
			t.Logf("SOURCE REPLACEMENT old=%s new=%s", bundle, newBundle)
			second := startServeRuntimeTestProcess(t, opts)
			second.waitForReadyLine()
			endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, second.outputString()) + "/v1/rpc"
			requireServedRunStatusWithDebug(t, endpoint, reader, backend, seed.RunID, "cancelled")
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
