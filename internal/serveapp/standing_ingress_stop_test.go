package serveapp

import (
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
)

func TestStoredInboundProcessHandlerKeepsPendingUntilRestartBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			credentialPath := filepath.Join(t.TempDir(), "credentials.json")
			t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
			file, err := credentials.NewFileStore(credentialPath)
			if err != nil {
				t.Fatal(err)
			}
			const secret = "pending-during-stop"
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
					t.Errorf("joined serve shutdown=%d", code)
				}
			})
			manager := <-managers
			rt := servedTestProcessRuntime(t, process)
			endpoint := strings.TrimSuffix(served.Endpoint, "/v1/rpc") + "/webhooks/keyed-shop/partner"
			code, first := a9PostSignedRawIngress(t, endpoint, secret, `{"delivery_id":"before-stop","account_id":"account-7"}`)
			if code != http.StatusAccepted {
				t.Fatalf("ordinary intake status=%d body=%s", code, first)
			}
			firstReceipt := a9AliasReceipt(t, first)
			runID := firstReceipt["run_id"].(string)
			firstID := firstReceipt["event_ids"].([]any)[0].(string)
			a9RequireRootIngressSettlement(t, served.Endpoint, runID, firstID)
			if err := manager.SealInboundExecution(t.Context()); err != nil {
				t.Fatal(err)
			}
			rt.CloseAdmission()
			if err := manager.OpenStoredInbound(); err != nil {
				t.Fatal(err)
			}
			const body = `{"delivery_id":"during-stop","account_id":"account-7"}`
			if code, raw := a9PostSignedRawIngress(t, endpoint, "wrong-secret", body); code != http.StatusUnauthorized {
				t.Fatalf("stored intake bypassed authentication: status=%d body=%s", code, raw)
			}
			code, stored := a9PostSignedRawIngress(t, endpoint, secret, body)
			if code != http.StatusAccepted {
				t.Fatalf("stored intake status=%d body=%s", code, stored)
			}
			receipt := a9AliasReceipt(t, stored)
			eventID := receipt["event_ids"].([]any)[0].(string)
			if receipt["run_id"] != runID || eventID == firstID {
				t.Fatalf("stored intake changed ordinary identity: %v", receipt)
			}
			code, duplicate := a9PostSignedRawIngress(t, endpoint, secret, body)
			if code != http.StatusOK || !reflect.DeepEqual(receipt, a9AliasReceipt(t, duplicate)) {
				t.Fatalf("stored retry changed receipt: status=%d body=%s", code, duplicate)
			}
			var pending operatorread.OperatorEventFull
			requireServedJSONRPCResult(t, served.Endpoint, "event.get", map[string]any{"event_id": eventID}, &pending)
			if len(pending.Deliveries) != 1 || pending.Deliveries[0].Status != "pending" || pending.Deliveries[0].Terminal || len(pending.DeadLetters) != 0 {
				t.Fatalf("stored event did not retain its pending obligation: %+v", pending)
			}
			var entity operatorread.OperatorEntityFull
			requireServedJSONRPCResult(t, served.Endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": runID}, &entity)
			if entity.Fields["processed_count"] != float64(1) {
				t.Fatalf("stored intake executed in the stopping runtime: %+v", entity)
			}
			if err := manager.CloseInboundRequests(t.Context()); err != nil {
				t.Fatal(err)
			}
			if code := process.stop(); code != 0 {
				t.Fatalf("stop with pending input exit=%d", code)
			}
			process, served = start()
			a9RequireRootIngressSettlement(t, served.Endpoint, runID, eventID)
			requireServedJSONRPCResult(t, served.Endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": runID}, &entity)
			if entity.Fields["processed_count"] != float64(2) {
				t.Fatalf("restart did not recover the exact pending input once: %+v", entity)
			}
		})
	}
}
