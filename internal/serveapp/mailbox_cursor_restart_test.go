package serveapp

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestServedMailboxCursorRetainedRestartParity(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.RootIngress)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := canonicalrouting.CopyMailboxEntityFilter(t)
			bundle := servedEventPublishFixtureBundleHash(t, root)
			unsetStoreSelectorEnv(t)
			stubServeRuntimeWorkspaceLifecycle(t)
			opts := cliapp.ServeOptions{SourceRoot: root, PlatformSpecPath: defaultPlatformSpecPath, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig()}
			var db *sql.DB
			if backend == "sqlite" {
				opts.ConfigPath = writeStoreBackendRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "mailbox.sqlite"))
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
			firstProcess := startServeRuntimeTestProcess(t, opts)
			firstProcess.waitForReadyLine()
			endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, firstProcess.outputString()) + "/v1/rpc"
			for i := 0; i < 3; i++ {
				published := requireServedEventPublishRPCResult(t, endpoint, map[string]any{"event_name": "review.requested", "bundle_hash": bundle, "payload": map[string]any{"item_id": uuid.NewString()}, "idempotency_key": uuid.NewString()})
				waitServedRunDeliveryQuiescence(t, db, backend, published.RunID)
			}
			var before, first cursorMailboxPage
			requireServedJSONRPCResult(t, endpoint, "mailbox.list", map[string]any{}, &before)
			requireServedJSONRPCResult(t, endpoint, "mailbox.list", map[string]any{"limit": 1}, &first)
			if len(before.Items) != 3 || len(first.Items) != 1 || first.Next == "" {
				t.Fatalf("before=%+v first=%+v", before, first)
			}
			if code := firstProcess.stop(); code != 0 {
				t.Fatalf("stop=%d\n%s", code, firstProcess.outputString())
			}
			setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
			secondProcess := startServeRuntimeTestProcess(t, opts)
			secondProcess.waitForReadyLine()
			endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, secondProcess.outputString()) + "/v1/rpc"
			var fresh cursorMailboxPage
			requireServedJSONRPCResult(t, endpoint, "mailbox.list", map[string]any{}, &fresh)
			if !reflect.DeepEqual(cursorRowKeys(before.Items), cursorRowKeys(fresh.Items)) {
				t.Fatalf("retained identities changed: before=%v after=%v", cursorRowKeys(before.Items), cursorRowKeys(fresh.Items))
			}
			var continued []cursorMailboxRow
			cursor := first.Next
			for i := 0; i < 3; i++ {
				var page cursorMailboxPage
				requireServedJSONRPCResult(t, endpoint, "mailbox.list", map[string]any{"limit": 1, "cursor": cursor}, &page)
				continued = append(continued, page.Items...)
				if page.Next == "" {
					break
				}
				cursor = page.Next
			}
			if !reflect.DeepEqual(cursorRowKeys(continued), cursorRowKeys(fresh.Items[1:])) {
				t.Fatalf("continued=%v fresh=%v", cursorRowKeys(continued), cursorRowKeys(fresh.Items))
			}
			if code := secondProcess.stop(); code != 0 {
				t.Fatalf("stop=%d\n%s", code, secondProcess.outputString())
			}
		})
	}
}
