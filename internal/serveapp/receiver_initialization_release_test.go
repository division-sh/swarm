package serveapp

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/testutil"
)

// Unlike the in-process served proofs, this exercises the actual CLI verify
// and serve commands, host workspace, and HTTP boundary in a separate process.
func TestReleaseReceiverInitializationBothStores(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "swarm")
	build := exec.Command("go", "build", "-race", "-o", binary, "./cmd/swarm")
	build.Dir = repoRootForTest()
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build race-instrumented release executable: %v\n%s", err, output)
	}
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			t.Run("connected_typed_creation", func(t *testing.T) {
				root := canonicalrouting.CopyServedReceiverInitialization(t)
				rt := startLifecycleReleaseProcess(t, binary, backend, root)
				rt.WaitReceiverOrigin = func(t *testing.T, seed servedEventPublishRPCResult) {
					waitServedEventPublishedLog(t, rt.Endpoint, seed.RunID, seed.EventID)
				}
				requireTypedReceiverInitializationCases(t, rt)
			})
			t.Run("direct_provider_schema", func(t *testing.T) {
				root := canonicalrouting.CopyProviderReceiverInitialization(t)
				rt := startLifecycleReleaseProcess(t, binary, backend, root)
				rt.WaitReceiverOrigin = func(t *testing.T, seed servedEventPublishRPCResult) {
					waitServedEventPublishedLog(t, rt.Endpoint, seed.RunID, seed.EventID)
				}
				requireReceiverInitializationPublicProviderIngressCases(t, rt)
			})
			t.Run("connected_restart", func(t *testing.T) {
				requireR7ReleaseReceiverRestart(t, binary, backend)
			})
		})
	}
}

func requireR7ReleaseReceiverRestart(t *testing.T, binary string, backend servedparity.Backend) {
	t.Helper()
	unsetStoreSelectorEnv(t)
	root := canonicalrouting.CopyServedReceiverInitialization(t)
	var config string
	if backend == servedparity.BackendDefaultSQLite {
		config = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", filepath.Join(t.TempDir(), "restart.sqlite"), channelOnboardingHostWorkspaceFields())
	} else {
		config = writeChannelOnboardingPostgresRuntimeConfig(t, testutil.StartEmptyPostgresDSN(t))
	}
	setServeRuntimeRecovery(t, config, false, true)
	env := append(releaseProviderTriggerProcessEnv(), "PGPASSWORD="+os.Getenv("PGPASSWORD"), "ANTHROPIC_API_KEY=", "OPENAI_API_KEY=")
	params := map[string]any{
		"event_name": "work.requested", "bundle_hash": servedEventPublishFixtureBundleHash(t, root), "idempotency_key": "r7-release-restart",
		"payload": map[string]any{"account_id": "preserved-account", "values": map[string]any{"active": true, "label": "original", "attributes": map[string]any{"nested": []any{1, 2.5}}}},
	}
	var seed servedEventPublishRPCResult
	var originalEvent operatorread.OperatorEventFull
	var originalEntity operatorread.OperatorEntityFull
	if !t.Run("before_restart", func(t *testing.T) {
		endpoint := startReleaseServeEndpoint(t, binary, backend, root, config, env)
		seed = requireServedEventPublishRPCResult(t, endpoint, params)
		originalEvent, originalEntity = requireR7ReleaseReceiverResult(t, endpoint, seed, 1)
		if originalEvent.Deliveries[0].Target.Kind != "materializing_entity" {
			t.Fatalf("first connection did not retain its creating target authority: %+v", originalEvent)
		}
		if duplicate := requireServedEventPublishRPCResult(t, endpoint, params); duplicate.EventID != seed.EventID || duplicate.RunID != seed.RunID {
			t.Fatalf("release duplicate changed the acknowledged publication: %+v -> %+v", seed, duplicate)
		}
		if event, entity := requireR7ReleaseReceiverResult(t, endpoint, seed, 1); !reflect.DeepEqual(event, originalEvent) || !reflect.DeepEqual(entity, originalEntity) {
			t.Fatal("release duplicate changed delivery or receiver evidence")
		}
	}) {
		return
	}
	// The existing process helper joins shutdown at the subtest boundary before
	// the next process opens the same store. No in-process owner is injected.
	t.Run("after_restart", func(t *testing.T) {
		endpoint := startReleaseServeEndpoint(t, binary, backend, root, config, env)
		if duplicate := requireServedEventPublishRPCResult(t, endpoint, params); duplicate.EventID != seed.EventID || duplicate.RunID != seed.RunID {
			t.Fatalf("restart changed the original publication receipt: %+v -> %+v", seed, duplicate)
		}
		if event, entity := requireR7ReleaseReceiverResult(t, endpoint, seed, 1); !reflect.DeepEqual(event, originalEvent) || !reflect.DeepEqual(entity, originalEntity) {
			t.Fatal("restart changed exact public delivery or receiver evidence")
		}
		reuse := map[string]any{
			"event_name": "work.requested", "run_id": seed.RunID, "idempotency_key": "r7-release-reuse",
			"payload": map[string]any{"account_id": "preserved-account", "values": map[string]any{}},
		}
		selected := requireServedEventPublishRPCResult(t, endpoint, reuse)
		event, entity := requireR7ReleaseReceiverResult(t, endpoint, selected, 2)
		target, original := event.Deliveries[0].Target, originalEvent.Deliveries[0].Target
		if selected.RunID != seed.RunID || selected.EventID == seed.EventID || entity.Entity.EntityID != originalEntity.Entity.EntityID || entity.Entity.FlowInstance != originalEntity.Entity.FlowInstance || target.Kind != "existing_entity" || target.FlowID != original.FlowID || target.FlowInstance != original.FlowInstance || target.EntityID != original.EntityID {
			t.Fatalf("post-restart selection created or redirected the stored receiver: seed=%+v selected=%+v original_entity=%+v entity=%+v original_target=%+v target=%+v", seed, selected, originalEntity.Entity, entity.Entity, originalEvent.Deliveries[0].Target, event.Deliveries[0].Target)
		}
	})
	t.Log("proof_surface=production verify/serve binary, public root input -> compiled connection -> private keyed handler, exact public event/entity/delivery readback, duplicate admission and joined process restart; no private persistence observer or live provider")
}

func requireR7ReleaseReceiverResult(t *testing.T, endpoint string, seed servedEventPublishRPCResult, executions int) (operatorread.OperatorEventFull, operatorread.OperatorEntityFull) {
	t.Helper()
	a9RequireIngressDeliveries(t, endpoint, seed.EventID, 1)
	emission := r7ObserverPublicEmission(t, endpoint, seed.RunID, seed.EventID, "work.ready")
	event := a9RequireIngressDeliveries(t, endpoint, emission.EventID, 1)
	target := event.Deliveries[0].Target
	r7RequireObserverDelivery(t, event, "account", "collector", target.FlowInstance, target.EntityID)
	var entity operatorread.OperatorEntityFull
	requireServedJSONRPCResult(t, endpoint, "entity.get", map[string]any{"run_id": seed.RunID, "entity_id": target.EntityID}, &entity)
	want := map[string]any{"account_id": "preserved-account", "processed_count": float64(executions), "values": map[string]any{"active": true, "label": "original", "attributes": map[string]any{"nested": []any{float64(1), 2.5}}}}
	if event.RunID != seed.RunID || event.SourceEventID != seed.EventID || target.EntityID == seed.RunID || target.FlowInstance == seed.RunID || entity.Entity.EntityID != target.EntityID || entity.Entity.RunID != seed.RunID || entity.Entity.FlowInstance != target.FlowInstance || !reflect.DeepEqual(entity.Fields, want) {
		t.Fatalf("release connected receiver lost identity, original fields or exact execution count: event=%+v entity=%+v", event, entity)
	}
	return event, entity
}
