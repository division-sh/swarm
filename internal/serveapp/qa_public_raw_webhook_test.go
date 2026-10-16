package serveapp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestQAPublicRawWebhookReadyRouteBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := canonicalrouting.CopyKeyedRootRawIngress(t)
			credentialPath := filepath.Join(t.TempDir(), "credentials.json")
			t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
			file, err := credentials.NewFileStore(credentialPath)
			if err != nil {
				t.Fatal(err)
			}
			const secret = "public-raw-loopback-no-spend"
			if err := file.Set(t.Context(), "webhook_signing.partner", secret); err != nil {
				t.Fatal(err)
			}
			publicListener := reserveChannelOnboardingListener(t)
			publicAddress := publicListener.Addr().String()
			publicTarget, err := url.Parse("http://" + publicAddress)
			if err != nil {
				t.Fatal(err)
			}
			proxy := httputil.NewSingleHostReverseProxy(publicTarget)
			proxy.Transport = http.DefaultTransport
			origin := httptest.NewTLSServer(proxy)
			t.Cleanup(origin.Close)
			priorTransport := http.DefaultTransport
			http.DefaultTransport = origin.Client().Transport
			t.Cleanup(func() { http.DefaultTransport = priorTransport })
			opts, start := clockDeploymentHarness(t, backend, root)
			opts.PublicWebhookBaseURL = origin.URL
			opts.PublicWebhookListen = publicAddress
			opts.PublicWebhookListener = publicListener
			process, served := start()
			t.Cleanup(func() {
				if code := process.stop(); code != 0 {
					t.Errorf("serve cleanup=%d\n%s", code, process.outputString())
				}
			})
			const route = "/webhooks/keyed-shop/partner"
			foundReady := false
			lines := strings.Split(process.outputString(), "\n")
			for index, line := range lines {
				if strings.Contains(line, origin.URL+route) {
					t.Logf("advertised route: %s", line)
					if index+1 < len(lines) {
						capability := strings.TrimSpace(lines[index+1])
						t.Logf("route capability: %s", capability)
						foundReady = strings.HasPrefix(capability, "capability") && strings.Contains(capability, "READY") && strings.Contains(capability, "HMAC_SHA256")
					}
				}
			}
			if !foundReady {
				t.Fatalf("expected declared public HMAC route READY\n%s", process.outputString())
			}
			api := strings.TrimSuffix(served.Endpoint, "/v1/rpc") + route
			const body = `{"delivery_id":"public-route-proof","account_id":"account-7"}`
			for name, endpoint := range map[string]string{"api": api, "public": "http://" + publicAddress + route} {
				code, response := a9PostSignedRawIngress(t, endpoint, "wrong-secret", body)
				t.Logf("%s invalid-signature status=%d body=%s", name, code, response)
				if code != http.StatusUnauthorized {
					t.Errorf("READY route %s bypassed authentication handler: status=%d, want 401", name, code)
				}
			}
			tlsCode, tlsReceipt := a9PostSignedRawIngress(t, origin.URL+route, secret, body)
			t.Logf("advertised TLS origin signed status=%d body=%s", tlsCode, tlsReceipt)
			if tlsCode != http.StatusAccepted {
				t.Errorf("READY public route refused a valid signed request: status=%d, want 202", tlsCode)
			}
			publicCode, publicReceipt := a9PostSignedRawIngress(t, "http://"+publicAddress+route, secret, body)
			wantPublic := http.StatusAccepted
			if tlsCode == http.StatusAccepted {
				wantPublic = http.StatusOK
			}
			t.Logf("direct public listener signed status=%d body=%s", publicCode, publicReceipt)
			if publicCode != wantPublic {
				t.Errorf("public listener did not preserve signed route/retry: status=%d, want %d", publicCode, wantPublic)
			}
			apiCode, apiReceipt := a9PostSignedRawIngress(t, api, secret, body)
			wantAPI := http.StatusAccepted
			if tlsCode == http.StatusAccepted || publicCode == http.StatusAccepted {
				wantAPI = http.StatusOK
			}
			t.Logf("API listener signed control status=%d body=%s", apiCode, apiReceipt)
			if apiCode != wantAPI {
				t.Fatalf("API control is not valid: status=%d, want %d", apiCode, wantAPI)
			}
			var receipt struct {
				RunID    string   `json:"run_id"`
				EventIDs []string `json:"event_ids"`
			}
			if err := json.Unmarshal(apiReceipt, &receipt); err != nil || receipt.RunID == "" || len(receipt.EventIDs) != 1 {
				t.Fatalf("real publication receipt=%s err=%v", apiReceipt, err)
			}
			a9RequireRootIngressSettlement(t, served.Endpoint, receipt.RunID, receipt.EventIDs[0])
		})
	}
}
