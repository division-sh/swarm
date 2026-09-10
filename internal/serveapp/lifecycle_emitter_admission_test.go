package serveapp

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestServedLifecycleEmitterDanglingAdmission(t *testing.T) {
	testServedLifecycleEmitterDanglingAdmission(t, servedparity.BackendDefaultSQLite)
}

func TestServedLifecycleEmitterDanglingAdmissionPostgres(t *testing.T) {
	testServedLifecycleEmitterDanglingAdmission(t, servedparity.BackendExplicitPostgres)
}

func testServedLifecycleEmitterDanglingAdmission(t *testing.T, backend servedparity.Backend) {
	t.Helper()
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	for _, tc := range []struct {
		name, event string
		variant     canonicalrouting.LifecycleEmitterStaticVariant
	}{
		{"gate", "work.completed", canonicalrouting.LifecycleStaticGateDanglingStrict},
		{"loop", "loop.escaped", canonicalrouting.LifecycleStaticLoopDangling},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := canonicalrouting.CopyLifecycleEmitterStatic(t, tc.variant)
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			if err := cliapp.VerifyBundle(context.Background(), semanticview.Wrap(bundle), executionposture.Live); err == nil || !strings.Contains(err.Error(), "event_consumer_exists") || !strings.Contains(err.Error(), tc.event) {
				t.Fatalf("strict verify did not reject the dangling emitter: %v", err)
			}
			isolateCLIAPIConfigEnv(t)
			unsetStoreSelectorEnv(t)
			config := ""
			if backend == servedparity.BackendExplicitPostgres {
				installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle { return serveRuntimeWorkspaceStub{} })
				config = writeServeRuntimeTestConfig(t)
			} else {
				stubServeRuntimeWorkspaceLifecycle(t)
				config = writeStoreBackendRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "runtime.db"))
			}
			var published atomic.Bool
			var out lockedBuffer
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			opts := cliapp.ServeOptions{
				ConfigPath:       config,
				SourceRoot:       root,
				PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repo),
				APIListenAddr:    "127.0.0.1:0",
				MCPListenAddr:    "127.0.0.1:0",
				SelfCheck:        true,
				Verbose:          true,
				Output:           &out,
				TestRuntimeContextsReadyHook: func(*runtimepkg.RuntimeContextManager) {
					published.Store(true)
				},
			}
			if backend == servedparity.BackendExplicitPostgres {
				opts.StoreMode, opts.StoreModeSet = "postgres", true
			}
			code := runFrom(ctx, repo, opts)
			if code == 0 || !strings.Contains(out.String(), "event_consumer_exists") || !strings.Contains(out.String(), tc.event) {
				t.Fatalf("serve did not reject the dangling emitter: code=%d\n%s", code, out.String())
			}
			if ctx.Err() != nil || published.Load() || strings.Contains(out.String(), "ready in ") {
				t.Fatalf("serve did not fail before readiness: context=%v published=%v\n%s", ctx.Err(), published.Load(), out.String())
			}
		})
	}
}

func TestServedLifecycleEmitterPublicInputAdmissionNoMutation(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			for _, tc := range []struct {
				name, event, site, state string
				variant                  canonicalrouting.LifecycleEmitterVariant
				payload                  map[string]any
			}{
				{"gate", "work.completed", "stages.review.gate.outcomes.approve.emit", "review", canonicalrouting.LifecycleGateLocal, map[string]any{"result": "approved"}},
				{"loop", "loop.escaped", "loops.revision.escape.emit", "waiting", canonicalrouting.LifecycleLoopConnected, map[string]any{"revision_id": "not-a-runtime-revision"}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyLifecycleEmitter(t, tc.variant))
					before := lifecycleAdmissionDomainCounts(t, rt.DB)
					// run.start, unlike low-level event.publish, requires an exact
					// declared root input. A producer fact must not widen it.
					err := requireServedJSONRPCError(t, rt.Endpoint, "run.start", map[string]any{
						"bundle_hash": rt.BundleHash, "event_name": tc.event, "payload": tc.payload, "idempotency_key": "lifecycle-not-ingress",
					})
					details, ok := err.Data["details"].(map[string]any)
					if err.Data["code"] != apiv1.EventNotDeclaredCode || !ok || details["reason"] != "not_declared_root_input" || details["event_name"] != tc.event {
						t.Fatalf("wrong non-ingress rejection: %#v", err)
					}
					if got := lifecycleAdmissionDomainCounts(t, rt.DB); !reflect.DeepEqual(got, before) {
						t.Fatalf("rejected run.start mutated domain: before=%v after=%v", before, got)
					}
					// Internal lifecycle site coordinates are not public event names.
					err = requireServedJSONRPCError(t, rt.Endpoint, "event.publish", map[string]any{
						"bundle_hash": rt.BundleHash, "event_name": tc.site, "payload": tc.payload, "idempotency_key": "lifecycle-site-not-event",
					})
					if err.Data["code"] != apiv1.EventNotDeclaredCode {
						t.Fatalf("wrong lifecycle-site rejection: %#v", err)
					}
					if got := lifecycleAdmissionDomainCounts(t, rt.DB); !reflect.DeepEqual(got, before) {
						t.Fatalf("rejected event.publish mutated domain: before=%v after=%v", before, got)
					}
					for _, key := range []struct{ method, key string }{{"run.start", "lifecycle-not-ingress"}, {"event.publish", "lifecycle-site-not-event"}} {
						if n := servedEventPublishAPIIdempotencyCount(t, rt.DB, rt.Backend, key.method, key.key); n != 0 {
							t.Fatalf("rejected %s stored %d idempotency completions", key.method, n)
						}
					}
					started := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
						"bundle_hash": rt.BundleHash, "event_name": "work.requested", "payload": map[string]any{"seed": true}, "idempotency_key": "lifecycle-valid-ingress",
					})
					requireServedEventPublishEntityState(t, rt.DB, rt.Backend, started.RunID, "", tc.state)
				})
			}
		})
	}
}

func lifecycleAdmissionDomainCounts(t *testing.T, db *sql.DB) map[string]int {
	t.Helper()
	counts := map[string]int{}
	for _, table := range []string{"runs", "events", "event_deliveries", "entity_state"} {
		var count int
		if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count); err != nil {
			t.Fatalf("read %s count: %v", table, err)
		}
		counts[table] = count
	}
	return counts
}
