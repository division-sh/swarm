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
	"github.com/division-sh/swarm/internal/events"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestReceiverCompositionRestartBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := canonicalrouting.CopyReceiverCreatingChild(t, false)
			bundle := servedEventPublishFixtureBundleHash(t, root)
			unsetStoreSelectorEnv(t)
			stubServeRuntimeWorkspaceLifecycle(t)
			opts := cliapp.ServeOptions{SourceRoot: root, PlatformSpecPath: defaultPlatformSpecPath, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig()}
			var db *sql.DB
			if backend == "sqlite" {
				opts.ConfigPath = writeStoreBackendRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "receiver.sqlite"))
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
			reached := make(chan runtimedelivery.Claim, 1)
			opts.TestWorkflowNodeHandlerStartHook = func(ctx context.Context, _ string, evt events.Event) error {
				if evt.Type() != "work.completed" {
					return nil
				}
				claim, ok := runtimedelivery.ClaimFromContext(ctx)
				if !ok {
					t.Error("restart barrier has no actual child claim")
					return context.Canceled
				}
				reached <- claim
				<-ctx.Done()
				return ctx.Err()
			}
			first := startServeRuntimeTestProcess(t, opts)
			first.waitForReadyLine()
			endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, first.outputString()) + "/v1/rpc"
			params := map[string]any{"event_name": "work.requested", "bundle_hash": bundle, "payload": map[string]any{"seed": true}, "idempotency_key": "receiver-restart"}
			published := requireServedEventPublishRPCResult(t, endpoint, params)
			var claim runtimedelivery.Claim
			select {
			case claim = <-reached:
			case <-time.After(10 * time.Second):
				t.Fatal("real child execution never reached the restart barrier")
			}
			var before string
			if err := db.QueryRow(`SELECT CAST(delivery_target_route AS TEXT) FROM event_deliveries WHERE delivery_id=$1`, claim.DeliveryID()).Scan(&before); err != nil {
				t.Fatal(err)
			}
			constructionBefore := receiverConstructionReceipts(t, db, published.RunID)
			if len(constructionBefore) != 2 {
				t.Fatalf("creating publication did not construct exactly root and child: %+v", constructionBefore)
			}
			if code := first.stop(); code != 0 {
				t.Fatalf("first serve exit=%d\n%s", code, first.outputString())
			}
			if err := os.RemoveAll(root); err != nil {
				t.Fatal(err)
			}
			opts.SourceRoot, opts.BundleHash = "", bundle
			opts.TestWorkflowNodeHandlerStartHook = nil
			setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
			second := startServeRuntimeTestProcess(t, opts)
			second.waitForReadyLine()
			endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, second.outputString()) + "/v1/rpc"
			waitServedRunDeliveryQuiescence(t, db, backend, published.RunID)
			var after, status string
			if err := db.QueryRow(`SELECT CAST(delivery_target_route AS TEXT),status FROM event_deliveries WHERE delivery_id=$1`, claim.DeliveryID()).Scan(&after, &status); err != nil {
				t.Fatal(err)
			}
			if before != after || status != "delivered" {
				t.Fatalf("restart changed/lost exact child: before=%s after=%s status=%s", before, after, status)
			}
			if receipts := receiverConstructionReceipts(t, db, published.RunID); !reflect.DeepEqual(constructionBefore, receipts) {
				t.Fatalf("hash-only restart changed immutable construction: before=%+v after=%+v", constructionBefore, receipts)
			}
			var entities int
			if err := db.QueryRow(`SELECT count(*) FROM entity_state WHERE run_id=$1 AND current_state='done'`, published.RunID).Scan(&entities); err != nil {
				t.Fatal(err)
			}
			if entities != 2 {
				t.Fatalf("restart did not create exactly parent and child: %d", entities)
			}
			duplicate := requireServedEventPublishRPCResult(t, endpoint, params)
			if duplicate.EventID != published.EventID {
				t.Fatal("restart duplicate reminted source")
			}
			requireReceiverPublicReadback(t, servedControlProofRuntime{Endpoint: endpoint, DB: db, Backend: backend}, published.RunID)
			if code := second.stop(); code != 0 {
				t.Fatalf("second serve exit=%d\n%s", code, second.outputString())
			}
		})
	}
}

func receiverConstructionReceipts(t *testing.T, db *sql.DB, runID string) map[string]string {
	t.Helper()
	rows, err := db.Query(`SELECT instance_path, CAST(projection AS TEXT) FROM workflow_instance_initial_materializations WHERE run_id=$1`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	receipts := make(map[string]string)
	for rows.Next() {
		var path, projection string
		if err := rows.Scan(&path, &projection); err != nil {
			t.Fatal(err)
		}
		receipts[path] = projection
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return receipts
}
