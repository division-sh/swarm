package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
)

const providerCrashDiagnosticEnv = "TEST_PROVIDER_CRASH_STARTUP_DIAGNOSTIC"

type providerCrashDiagnosticRequest struct {
	Source, Config, Backend string
	Abandon                 bool
}

func TestProviderCrashStartupDiagnosticProcess(t *testing.T) {
	raw := os.Getenv(providerCrashDiagnosticEnv)
	if raw == "" {
		t.Skip("parent-owned #2701 diagnostic process")
	}
	var request providerCrashDiagnosticRequest
	if err := json.Unmarshal([]byte(raw), &request); err != nil {
		t.Fatal(err)
	}
	opts := cliapp.DefaultServeOptions()
	opts.SourceRoot, opts.ConfigPath = request.Source, request.Config
	opts.StoreMode, opts.StoreModeSet = request.Backend, true
	opts.PlatformSpecPath = defaultPlatformSpecPath
	opts.APIListenAddr, opts.MCPListenAddr = "127.0.0.1:0", "127.0.0.1:0"
	opts.WorkspaceBackend, opts.WorkspaceBackendSet = "host", true
	opts.SelfCheck, opts.Verbose = true, true
	opts.AbandonActiveRuns = request.Abandon
	opts.Output, opts.ErrorOutput = os.Stdout, os.Stderr
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if code := runFrom(ctx, repoRootForTest(), opts); code != 0 {
		t.Fatalf("provider-crash diagnostic serve exit=%d", code)
	}
}

func TestProviderCrashStartupDiagnosticBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			unsetStoreSelectorEnv(t)
			root := canonicalrouting.CopyRootIngressServedLiveAgent(t)
			setDoctorProviderSecret(t, "OPENAI_COMPATIBLE_API_KEY", "issue-2701-loopback-only")
			t.Setenv("OPENAI_COMPATIBLE_API_KEY", "")
			var entered atomic.Int32
			release := make(chan struct{})
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					w.Header().Set("Content-Type", "application/json")
					fmt.Fprint(w, `{"object":"list","data":[{"id":"gpt-compatible","object":"model"}]}`)
					return
				}
				entered.Add(1)
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"model":"gpt-compatible","choices":[{"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":3,"total_tokens":8}}`)
			}))
			t.Cleanup(provider.Close)
			t.Cleanup(func() { close(release) })
			var configPath, location string
			if backend == "postgres" {
				location = storetest.PostgresFixtureLocation(t)
				configPath = writeChannelOnboardingPostgresRuntimeConfig(t, location)
			} else {
				location = filepath.Join(t.TempDir(), "provider-crash.sqlite")
				configPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend, location, channelOnboardingHostWorkspaceFields())
			}
			raw, err := os.ReadFile(configPath)
			if err != nil {
				t.Fatal(err)
			}
			updated := strings.Replace(string(raw), "  backend: anthropic\n", "  backend: openai_compatible\n  openai_compatible:\n    base_url: "+provider.URL+"\n", 1)
			if updated == string(raw) {
				t.Fatal("diagnostic config has no canonical backend line")
			}
			if err := os.WriteFile(configPath, []byte(updated), 0600); err != nil {
				t.Fatal(err)
			}
			setServeRuntimeRecovery(t, configPath, false, true)
			start := func(abandon bool) *channelOnboardingCrashServeProcess {
				t.Helper()
				request, err := json.Marshal(providerCrashDiagnosticRequest{Source: root, Config: configPath, Backend: backend, Abandon: abandon})
				if err != nil {
					t.Fatal(err)
				}
				return startServedCrashProcessBeforeReadiness(t, "TestProviderCrashStartupDiagnosticProcess", []string{providerCrashDiagnosticEnv + "=" + string(request)})
			}
			original := start(false)
			endpoint := original.endpoint(t) + "/v1/rpc"
			for i := 0; i < 3; i++ {
				ack := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
					"event_name": "item.received", "bundle_hash": servedEventPublishFixtureBundleHash(t, root),
					"payload": map[string]any{"item_id": "emit"}, "idempotency_key": fmt.Sprintf("provider-crash-%d", i),
				})
				t.Logf("original ACK run=%s event=%s", ack.RunID, ack.EventID)
			}
			observations := storetest.OpenIssue2564WorkloadObservation(t, backend, location)
			deadline := time.Now().Add(20 * time.Second)
			for {
				attempts, err := storetest.ReadExternalAttemptStorage(context.Background(), observations.Reader)
				if err != nil {
					t.Fatal(err)
				}
				launched := 0
				for _, attempt := range attempts {
					if attempt.State == "launched" {
						launched++
					}
				}
				if launched == 3 && entered.Load() == 3 {
					t.Logf("abrupt-stop cut: attempts=%+v provider_entered=%d", attempts, entered.Load())
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("never reached three launched attempts: attempts=%+v provider_entered=%d\n%s", attempts, entered.Load(), original.output.String())
				}
				time.Sleep(10 * time.Millisecond)
			}
			if err := original.kill(); err != nil {
				t.Fatal(err)
			}
			for restart, want := range []string{"requires exact bundle scope in context", "provider_attempt_drain_successor_mismatch", "provider_attempt_drain_successor_mismatch"} {
				process := start(restart == 2)
				select {
				case <-process.exited:
				case <-time.After(30 * time.Second):
					_ = process.kill()
					t.Fatalf("restart %d did not exit\n%s", restart+1, process.output.String())
				}
				t.Logf("restart=%d abandon=%t exit=%v transcript:\n%s", restart+1, restart == 2, process.waitError(), process.output.String())
				if process.waitError() == nil || !strings.Contains(process.output.String(), want) {
					t.Fatalf("restart %d did not reproduce %q", restart+1, want)
				}
			}
			t.Errorf("MASTER RED #2701: launched attempts prevent first restart; persisted drain successor prevents later restarts")
		})
	}
}
