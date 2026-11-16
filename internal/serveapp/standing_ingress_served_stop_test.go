package serveapp

import (
	"context"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
)

func TestStoredInboundServedStopRetainsNativePendingBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "credentials.json")
			t.Setenv("SWARM_CREDENTIALS_FILE", path)
			file, err := credentials.NewFileStore(path)
			if err != nil {
				t.Fatal(err)
			}
			const secret = "served-stop-pending"
			if err := file.Set(t.Context(), "partner.webhook_signing_secret", secret); err != nil {
				t.Fatal(err)
			}
			opts, start := clockDeploymentHarness(t, backend, canonicalrouting.CopyConcurrentKeyedRootRawIngress(t))
			enableChannelOnboardingRecoveryOnStartup(t, opts.ConfigPath)
			managers := make(chan *runtimepkg.RuntimeContextManager, 2)
			opts.TestRuntimeContextsReadyHook = func(m *runtimepkg.RuntimeContextManager) { managers <- m }
			process, served := start()
			t.Cleanup(func() {
				if code := process.stop(); code != 0 {
					t.Errorf("joined shutdown=%d", code)
				}
			})
			manager := <-managers
			rt := servedTestProcessRuntime(t, process)
			base := strings.TrimSuffix(served.Endpoint, "/v1/rpc")
			endpoint := base + "/webhooks/keyed-shop/partner"
			code, first := a9PostSignedRawIngress(t, endpoint, secret, `{"delivery_id":"before-stop"}`)
			if code != http.StatusAccepted {
				t.Fatalf("ordinary intake status=%d body=%s", code, first)
			}
			initial := a9AliasReceipt(t, first)
			runID := initial["run_id"].(string)
			a9RequireRootIngressSettlement(t, served.Endpoint, runID, initial["event_ids"].([]any)[0].(string))
			accepted, lookup, err := manager.AcquireBundleHash(t.Context(), served.BundleHash)
			if err != nil || accepted == nil || !lookup.Loaded() {
				t.Fatalf("hold exact accepted runtime: loaded=%t err=%v", lookup.Loaded(), err)
			}
			defer func() {
				if err := accepted.Done(); err != nil {
					t.Error(err)
				}
			}()
			process.cancel()
			cut, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			for {
				request, err := http.NewRequestWithContext(cut, http.MethodGet, base+"/readyz", nil)
				if err != nil {
					t.Fatal(err)
				}
				response, err := http.DefaultClient.Do(request)
				if err != nil {
					t.Fatalf("read existing stop readiness: %v", err)
				}
				_ = response.Body.Close()
				if response.StatusCode == http.StatusServiceUnavailable {
					break
				}
				if response.StatusCode != http.StatusOK {
					t.Fatalf("unexpected readiness response=%d", response.StatusCode)
				}
			}
			const body = `{"delivery_id":"during-stop"}`
			code, raw := a9PostSignedRawIngress(t, endpoint, secret, body)
			if code != http.StatusAccepted {
				t.Fatalf("ordinary call was not stored during actual stop: status=%d body=%s", code, raw)
			}
			receipt := a9AliasReceipt(t, raw)
			if receipt["run_id"] != runID {
				t.Fatalf("stop changed landing identity: %v", receipt)
			}
			owner, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.StoredRoute(".", runID, runID))
			if err != nil {
				t.Fatal(err)
			}
			instance, found, err := rt.Pipeline.LoadConstructedFlowInstance(t.Context(), owner, identity.NormalizeEntityID(runID))
			if err != nil || !found || instance.Fields["processed_count"] != int64(1) {
				t.Fatalf("stopping process started stored input: found=%t instance=%+v err=%v", found, instance, err)
			}
			if err := accepted.Done(); err != nil {
				t.Fatal(err)
			}
			if code := process.stop(); code != 0 {
				t.Fatalf("actual stop exit=%d", code)
			}
			process, served = start()
			a9RequireRootIngressSettlement(t, served.Endpoint, runID, receipt["event_ids"].([]any)[0].(string))
			var entity operatorread.OperatorEntityFull
			requireServedJSONRPCResult(t, served.Endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": runID}, &entity)
			if entity.Fields["processed_count"] != float64(2) {
				t.Fatalf("restart did not recover stored input once: %+v", entity)
			}
			endpoint = strings.TrimSuffix(served.Endpoint, "/v1/rpc") + "/webhooks/keyed-shop/partner"
			if code, duplicate := a9PostSignedRawIngress(t, endpoint, secret, body); code != http.StatusOK || !reflect.DeepEqual(receipt, a9AliasReceipt(t, duplicate)) {
				t.Fatalf("restart changed ordinary deduplication: status=%d body=%s", code, duplicate)
			}
		})
	}
}
