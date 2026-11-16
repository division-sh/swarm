package serveapp

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
)

func TestQAPublicRawWebhookReadyRouteBothStores(t *testing.T) {
	runPublicRawWebhookProof(t, false, false)
	t.Run("native_tls", func(t *testing.T) { runPublicRawWebhookProof(t, false, true) })
}

func TestPublicRawWebhookWithdrawalRefusesBeforePublicationBothStores(t *testing.T) {
	runPublicRawWebhookProof(t, true, false)
}

func runPublicRawWebhookProof(t *testing.T, withdrawAfterAcquisition, nativeTLS bool) {
	t.Helper()
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := canonicalrouting.CopyKeyedRootRawIngress(t)
			writeWorkflowValidationFixtureFile(t, filepath.Join(root, "partner/manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\ncapabilities:\n  can:\n    emit_events: [raw]\n    verify_secret: webhook_signing_secret\n  cannot: [execute code]\n")
			writeWorkflowValidationFixtureFile(t, filepath.Join(root, "partner/trigger.yaml"), "transport: webhook\npayload_object_required: true\nsecret: {required: true}\nsignature: {type: hmac_sha256, header: X-Signature, prefix: 'sha256=', signed_payload: raw_body}\ndelivery_id: {json_path: $.delivery_id, required: true}\nevent_type: {literal: update, required: true}\nevent_name: {literal: raw}\n")
			credentialPath := filepath.Join(t.TempDir(), "credentials.json")
			t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
			file, err := credentials.NewFileStore(credentialPath)
			if err != nil {
				t.Fatal(err)
			}
			const secret = "public-raw-loopback-no-spend"
			if err := file.Set(t.Context(), "partner.webhook_signing_secret", secret); err != nil {
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
			publicOrigin, publicURL := origin.URL, "http://"+publicAddress
			opts, start := clockDeploymentHarness(t, backend, root)
			opts.PublicWebhookBaseURL = publicOrigin
			opts.PublicWebhookListen, opts.PublicWebhookListener = publicAddress, publicListener
			priorTransport := http.DefaultTransport
			transport := origin.Client().Transport.(*http.Transport).Clone()
			if nativeTLS {
				port := origin.Listener.Addr().(*net.TCPAddr).Port
				publicAddress = origin.Listener.Addr().String()
				publicOrigin = fmt.Sprintf("https://example.com:%d", port)
				publicURL = publicOrigin
				pair := origin.TLS.Certificates[0]
				key, err := x509.MarshalPKCS8PrivateKey(pair.PrivateKey)
				if err != nil {
					t.Fatal(err)
				}
				certPath, keyPath := filepath.Join(t.TempDir(), "cert.pem"), filepath.Join(t.TempDir(), "key.pem")
				for path, block := range map[string]*pem.Block{
					certPath: {Type: "CERTIFICATE", Bytes: pair.Certificate[0]},
					keyPath:  {Type: "PRIVATE KEY", Bytes: key},
				} {
					if err := os.WriteFile(path, pem.EncodeToMemory(block), 0600); err != nil {
						t.Fatal(err)
					}
				}
				body, err := os.ReadFile(opts.ConfigPath)
				if err != nil {
					t.Fatal(err)
				}
				body = append(body, fmt.Appendf(nil, "\nserve:\n  public_webhook:\n    hostname: example.com\n    port: %d\n    certificate_file: %q\n    private_key_file: %q\n", port, certPath, keyPath)...)
				if err := os.WriteFile(opts.ConfigPath, body, 0600); err != nil {
					t.Fatal(err)
				}
				origin.Close()
				if err := publicListener.Close(); err != nil {
					t.Fatal(err)
				}
				opts.PublicWebhookBaseURL, opts.PublicWebhookListen, opts.PublicWebhookListener = "", "", nil
				transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
					if address == fmt.Sprintf("example.com:%d", port) {
						address = publicAddress
					}
					return (&net.Dialer{}).DialContext(ctx, network, address)
				}
			}
			http.DefaultTransport = transport
			t.Cleanup(func() { http.DefaultTransport = priorTransport })
			t.Cleanup(transport.CloseIdleConnections)
			var inbound inboundpublication.Runner
			captureSelectedRuntimePersistence(t, func(persistence serveRuntimePersistence) { inbound = persistence.deps.InboundStore })
			runtimes := make(chan *runtimepkg.Runtime, 1)
			managers := make(chan *runtimepkg.RuntimeContextManager, 1)
			opts.TestRuntimeReadyHook = func(rt *runtimepkg.Runtime) { runtimes <- rt }
			opts.TestRuntimeContextsReadyHook = func(manager *runtimepkg.RuntimeContextManager) { managers <- manager }
			process, served := start()
			t.Cleanup(func() {
				if code := process.stop(); code != 0 {
					t.Errorf("serve cleanup=%d\n%s", code, process.outputString())
				}
				if nativeTLS {
					listener, err := net.Listen("tcp", publicAddress)
					if err != nil {
						t.Errorf("native webhook cleanup retained its socket: %v", err)
					} else {
						_ = listener.Close()
					}
				}
			})
			const route = "/webhooks/keyed-shop/partner"
			foundReady := false
			lines := strings.Split(process.outputString(), "\n")
			for index, line := range lines {
				if strings.Contains(line, publicOrigin+route) {
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
			if nativeTLS {
				response, err := http.Post("http://"+publicAddress+route, "application/json", strings.NewReader(body))
				if err == nil {
					_ = response.Body.Close()
					if response.StatusCode != http.StatusBadRequest {
						t.Fatalf("native webhook accepted plaintext: HTTP %d", response.StatusCode)
					}
				}
			}
			publicClient := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
			for _, path := range []string{
				"/v1/rpc", "/healthz", "/readyz", "/mcp", "/tools/", "/webhooks/foreign/partner", "/webhooks/keyed-shop/foreign",
				"/webhooks/keyed-shop%2fpartner", route + "/", "/" + route, "/webhooks/../v1/rpc",
			} {
				response, err := publicClient.Post(publicURL+path, "application/json", strings.NewReader(body))
				if err != nil {
					t.Fatal(err)
				}
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
				if response.StatusCode != http.StatusNotFound || response.Header.Get("Location") != "" {
					t.Errorf("public listener exposed foreign route %s: %d", path, response.StatusCode)
				}
			}
			unsigned, err := http.Post(publicURL+route, "application/json", strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, unsigned.Body)
			_ = unsigned.Body.Close()
			if unsigned.StatusCode != http.StatusUnauthorized {
				t.Errorf("public route accepted missing HMAC: %d", unsigned.StatusCode)
			}
			for name, endpoint := range map[string]string{"api": api, "public": publicURL + route} {
				code, response := a9PostSignedRawIngress(t, endpoint, "wrong-secret", body)
				t.Logf("%s invalid-signature status=%d body=%s", name, code, response)
				if code != http.StatusUnauthorized {
					t.Errorf("READY route %s bypassed authentication handler: status=%d, want 401", name, code)
				}
			}
			var manager *runtimepkg.RuntimeContextManager
			var rt *runtimepkg.Runtime
			select {
			case manager = <-managers:
			case <-time.After(2 * time.Second):
				t.Fatal("serve did not expose its actual context manager")
			}
			select {
			case rt = <-runtimes:
			case <-time.After(2 * time.Second):
				t.Fatal("serve did not expose its actual runtime")
			}
			withdrawn := make(chan error, 1)
			before := manager.LookupIngress("keyed-shop", "partner")
			if !before.Loaded() {
				t.Fatalf("fixture has no exact loaded ingress publication: %+v", before)
			}
			beforeInstances, err := rt.Pipeline.ListWorkflowInstances(t.Context(), before.Target.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if withdrawAfterAcquisition {
				var once sync.Once
				rt.InboundGateway.SetCredentialAdmission(func(ctx context.Context, target runtimepkg.InboundTarget) (credentials.SecretBinding, func(context.Context) error, error) {
					binding, validate, err := rt.AdmitInboundCredentials(ctx, target)
					if err == nil {
						once.Do(func() {
							go func() {
								result := manager.DeactivateBundleHash(target.BundleHash, runtimepkg.RuntimeContextCauseUnloaded)
								withdrawn <- result.ShutdownErr
							}()
							select {
							case <-ctx.Done():
							case <-time.After(2 * time.Second):
								err = context.DeadlineExceeded
							}
						})
					}
					return binding, validate, err
				})
			}
			tlsCode, tlsReceipt := a9PostSignedRawIngress(t, publicOrigin+route, secret, body)
			t.Logf("advertised TLS origin signed status=%d body=%s", tlsCode, tlsReceipt)
			if withdrawAfterAcquisition {
				if tlsCode != http.StatusServiceUnavailable {
					t.Fatalf("withdrawn publication was not refused: %d %s", tlsCode, tlsReceipt)
				}
				select {
				case err := <-withdrawn:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("request did not reach its acquired-runtime credential boundary")
				}
				lookup := manager.LookupIngress("keyed-shop", "partner")
				if !lookup.Found || lookup.Loaded() || lookup.Cause != runtimepkg.RuntimeContextCauseUnloaded {
					t.Fatalf("interleaving did not withdraw the actual ingress publication: %+v", lookup)
				}
				code, _ := a9PostSignedRawIngress(t, publicURL+route, secret, body)
				if code != http.StatusNotFound {
					t.Fatalf("unloaded publication still exposed its route: %d", code)
				}
				if inbound == nil {
					t.Fatal("fixture did not retain its real inbound reader")
				}
				if _, found, err := inbound.LoadInboundPublicationByIdentity(t.Context(), inboundpublication.Identity{
					RunID:    before.Target.RunID,
					Provider: "partner", ProviderEventID: "public-route-proof",
				}); err != nil || found {
					t.Fatalf("withdrawal left a publication receipt: found=%t err=%v", found, err)
				}
				afterInstances, err := rt.Pipeline.ListWorkflowInstances(t.Context(), before.Target.RunID)
				if err != nil || !reflect.DeepEqual(beforeInstances, afterInstances) {
					t.Fatalf("withdrawal changed native construction: before=%+v after=%+v err=%v", beforeInstances, afterInstances, err)
				}
				return
			}
			if tlsCode != http.StatusAccepted {
				t.Errorf("READY public route refused a valid signed request: status=%d, want 202", tlsCode)
			}
			publicCode, publicReceipt := a9PostSignedRawIngress(t, publicURL+route, secret, body)
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
				t.Fatalf("API control is not valid: status=%d, want %d\n%s", apiCode, wantAPI, process.outputString())
			}
			var receipt struct {
				RunID    string   `json:"run_id"`
				EventIDs []string `json:"event_ids"`
			}
			if err := json.Unmarshal(apiReceipt, &receipt); err != nil || receipt.RunID == "" || len(receipt.EventIDs) != 1 {
				t.Fatalf("real publication receipt=%s err=%v", apiReceipt, err)
			}
			a9RequireRootIngressSettlement(t, served.Endpoint, receipt.RunID, receipt.EventIDs[0])
			var accepted map[string]any
			if err := json.Unmarshal(tlsReceipt, &accepted); err != nil || accepted["status"] != "accepted" {
				t.Fatalf("accepted public receipt=%s err=%v", tlsReceipt, err)
			}
			delete(accepted, "status")
			for name, raw := range map[string][]byte{"public": publicReceipt, "api": apiReceipt} {
				var duplicate map[string]any
				if err := json.Unmarshal(raw, &duplicate); err != nil || duplicate["status"] != "duplicate" {
					t.Fatalf("%s duplicate receipt=%s err=%v", name, raw, err)
				}
				delete(duplicate, "status")
				if !reflect.DeepEqual(accepted, duplicate) {
					t.Fatalf("%s replay changed receipt identity: public=%#v replay=%#v", name, accepted, duplicate)
				}
			}
			var entity struct {
				Fields map[string]any `json:"fields"`
			}
			requireServedJSONRPCResult(t, served.Endpoint, "entity.get", map[string]any{"run_id": receipt.RunID, "entity_id": receipt.RunID}, &entity)
			if entity.Fields["processed_count"] != float64(1) || entity.Fields["provider_event_id"] != "public-route-proof" {
				t.Fatalf("cross-listener retries changed receiver effects: %+v", entity.Fields)
			}
		})
	}
}
