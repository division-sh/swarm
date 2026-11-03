package releasee2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/testutil/llmapi"
	"gopkg.in/yaml.v3"
)

func TestProviderGracefulStopAndStandbyHandoffBothStores(t *testing.T) {
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []struct{ expires, selected bool }{{}, {expires: true}, {selected: true}} {
			t.Run(fmt.Sprintf("%s/expired_%t/selected_%t", backend, cut.expires, cut.selected), func(t *testing.T) {
				expires := cut.expires
				base := filepath.Join(root, backend, fmt.Sprintf("%t-%t", expires, cut.selected))
				entered := make(chan struct{}, 1)
				release := make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				wantCalls := 1
				var forkDone chan error
				forkPending := false
				if cut.selected {
					wantCalls++
				}
				provider := llmapi.New(t, llmapi.OpenAICompatible, "/v1/chat/completions", "stop-test-key", func(request llmapi.Request) (llmapi.Reply, error) {
					if request.Number == wantCalls {
						entered <- struct{}{}
						<-release
						if expires {
							return llmapi.Reply{StatusCode: http.StatusServiceUnavailable}, nil
						}
					}
					return llmapi.Reply{ID: fmt.Sprint(request.Number), Model: "stop-model", Text: "completed",
						TextID: fmt.Sprint(request.Number), InputTokens: 1, OutputTokens: 1, TotalTokens: 2}, nil
				})
				defer unblock()
				config, env := clockReleaseConfig(t, base, backend)
				var document map[string]any
				raw, err := os.ReadFile(config)
				if err != nil {
					t.Fatal(err)
				}
				if err := yaml.Unmarshal(raw, &document); err != nil {
					t.Fatal(err)
				}
				document["workspace"] = map[string]any{"backend": "host", "allow_exec_on_host": true}
				document["llm"] = map[string]any{"backend": "openai_compatible",
					"openai_compatible": map[string]any{"base_url": provider.URL + "/v1"},
					"models":            map[string]any{"stop-model": map[string]any{"openai_compatible": "stop-model"}}}
				raw, err = yaml.Marshal(document)
				if err != nil {
					t.Fatal(err)
				}
				writeReleaseFile(t, config, string(raw))
				env = append(env, "SWARM_CREDENTIALS_FILE="+filepath.Join(base, "credentials.json"))
				credential := runReleaseCommand(t, goldenStartupTimeout, base, env, "stop-test-key\n", binary,
					"--config", config, "secrets", "set", "OPENAI_COMPATIBLE_API_KEY", "--stdin")
				if credential.err != nil {
					t.Fatalf("configure provider: %v\n%s", credential.err, credential.output)
				}
				source := canonicalrouting.CopyRootIngressServedSessionCleanup(t)
				writeReleaseFile(t, filepath.Join(source, "hold", "agents.yaml"), `load-agent:
  role: load_agent
  intent: prompts/load-agent.md
  model: stop-model
  subscriptions: [item.agent_hold]
  tools: []
`)
				token := filepath.Join(base, "api-token")
				writeReleaseFile(t, token, goldenAPIToken+"\n")
				spec := releaseProcessSpec{BinaryPath: binary, WorkingDir: base, ConfigPath: config, Source: source,
					Store: backend, TokenFile: token, Token: goldenAPIToken, Env: env,
					DefaultExecutionSelection: true, ShutdownGrace: 2 * time.Second, RedactValues: []string{"stop-test-key"}}
				first := startReleaseServe(t, spec)
				ctx, cancel := context.WithTimeout(t.Context(), goldenStartupTimeout)
				defer cancel()
				if err := first.waitReady(ctx); err != nil {
					t.Fatalf("holder readiness: %v\n%s", err, first.output.String())
				}
				var seed, business struct {
					RunID   string `json:"run_id"`
					EventID string `json:"event_id"`
				}
				if err := first.rpc.call(ctx, "event.publish", map[string]any{"event_name": "item.received",
					"bundle_hash": goldenServedBundleHash(t, first.rpc, "live"), "payload": map[string]any{"item_id": "grace"}, "idempotency_key": "stop-seed"}, &seed); err != nil {
					t.Fatal(err)
				}
				if cut.selected {
					for {
						var event goldenEvent
						if err := first.rpc.call(ctx, "event.get", map[string]any{"event_id": seed.EventID}, &event); err != nil {
							t.Fatal(err)
						}
						settled := false
						for _, delivery := range event.Deliveries {
							if delivery.SubscriberType != "node" {
								continue
							}
							if delivery.Terminal && delivery.Status != "delivered" {
								t.Fatalf("source seed did not settle: %+v", delivery)
							}
							settled = delivery.Status == "delivered"
						}
						if settled {
							break
						}
						select {
						case <-ctx.Done():
							t.Fatal("source seed did not complete")
						case <-time.After(10 * time.Millisecond):
						}
					}
				}
				if err := first.rpc.call(ctx, "event.publish", map[string]any{"event_name": "item.agent_hold",
					"run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"note": "grace"}, "idempotency_key": "stop-business"}, &business); err != nil {
					t.Fatal(err)
				}
				if cut.selected {
					for {
						var event goldenEvent
						if err := first.rpc.call(ctx, "event.get", map[string]any{"event_id": business.EventID}, &event); err != nil {
							t.Fatal(err)
						}
						settled := false
						for _, delivery := range event.Deliveries {
							if delivery.SubscriberType == "agent" {
								if delivery.Terminal && delivery.Status != "delivered" {
									t.Fatalf("source provider did not settle: %+v", delivery)
								}
								settled = delivery.Status == "delivered"
							}
						}
						if settled {
							break
						}
						select {
						case <-ctx.Done():
							t.Fatal("source provider did not complete")
						case <-time.After(10 * time.Millisecond):
						}
					}
					var result any
					if err := first.rpc.call(ctx, "run.pause", map[string]any{"run_id": seed.RunID, "idempotency_key": "pause-source"}, &result); err != nil {
						t.Fatal(err)
					}
					forkCtx, cancelFork := context.WithCancel(ctx)
					forkDone = make(chan error, 1)
					forkPending = true
					forkEventID := business.EventID
					go func() {
						var result any
						forkDone <- first.rpc.call(forkCtx, "run.fork", map[string]any{"source_run_id": seed.RunID,
							"fork_event_id": forkEventID, "idempotency_key": "selected-stop"}, &result)
					}()
					defer func() {
						cancelFork()
						if forkPending {
							<-forkDone
						}
					}()
				}
				select {
				case <-entered:
				case forkErr := <-forkDone:
					forkPending = false
					t.Fatalf("selected fork completed before provider request: %v\n%s", forkErr, first.output.String())
				case <-ctx.Done():
					t.Fatalf("provider request was not reached\n%s", first.output.String())
				}
				if cut.selected {
					var list struct {
						Runs []struct {
							RunID  string `json:"run_id"`
							Origin struct {
								SourceRunID string `json:"source_run_id"`
							} `json:"origin"`
						} `json:"runs"`
					}
					if err := first.rpc.call(ctx, "run.list", map[string]any{}, &list); err != nil {
						t.Fatal(err)
					}
					business.RunID = ""
					for _, run := range list.Runs {
						if run.Origin.SourceRunID == seed.RunID {
							if business.RunID != "" {
								t.Fatal("more than one selected child")
							}
							business.RunID = run.RunID
						}
					}
					var listEvents struct {
						Events []goldenEvent `json:"events"`
					}
					if err := first.rpc.call(ctx, "event.list", map[string]any{"filter": map[string]any{
						"run_id": business.RunID, "event_name": "item.agent_hold"}, "limit": 100}, &listEvents); err != nil {
						t.Fatal(err)
					}
					if business.RunID == "" || len(listEvents.Events) != 1 {
						t.Fatal("selected child input is not exact")
					}
					business.EventID = listEvents.Events[0].EventID
				}
				status := runReleaseCommand(t, goldenStartupTimeout, base, env, "", binary, "store", "status", "--config", config, "--store", backend)
				const heldPrefix = "Project owner: selected-store lease is held; "
				if status.err != nil || !strings.HasPrefix(status.output, heldPrefix) || strings.Contains(status.output, "ready") {
					t.Fatalf("held read-only store status: %v\n%s", status.err, status.output)
				}
				lastAdmitted := strings.TrimSuffix(strings.TrimPrefix(status.output, heldPrefix), "\n")
				if !strings.HasSuffix(lastAdmitted, "; interrupted: 0 provider turns to resume, 0 provider turns to dead-letter, 0 HTTP tool calls outcome-unknown") {
					t.Fatalf("live unexpired work counted as interrupted: %s", lastAdmitted)
				}
				spec.APIPort = freeReleaseTCPPort(t)
				standby := startReleaseServe(t, spec)
				standbyURL := fmt.Sprintf("http://127.0.0.1:%d/readyz", spec.APIPort)
				for {
					request, err := http.NewRequestWithContext(ctx, http.MethodGet, standbyURL, nil)
					if err != nil {
						t.Fatal(err)
					}
					response, err := http.DefaultClient.Do(request)
					if err == nil {
						body, readErr := io.ReadAll(response.Body)
						closeErr := response.Body.Close()
						if response.StatusCode != http.StatusServiceUnavailable {
							t.Fatalf("standby ready before holder exit: HTTP %d", response.StatusCode)
						}
						if readErr != nil || closeErr != nil || string(body) != "waiting: store held; "+lastAdmitted+"\n" {
							t.Fatalf("standby lacks last-admitted readout: %q read=%v close=%v", body, readErr, closeErr)
						}
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal("standby did not expose its read-only probe")
					case <-time.After(10 * time.Millisecond):
					}
				}
				health := runReleaseCommand(t, goldenStartupTimeout, base, env, "", binary, "health", "--api-server", strings.TrimSuffix(standbyURL, "/readyz"), "--api-token-file", token)
				if health.err == nil || !strings.Contains(health.output, "waiting: store held; "+lastAdmitted) {
					t.Fatalf("standby CLI health: %v\n%s", health.err, health.output)
				}
				if !strings.Contains(standby.output.String(), "waiting: store held; "+lastAdmitted) {
					t.Fatalf("standby omitted last-admitted startup line:\n%s", standby.output.String())
				}
				stopped := make(chan error, 1)
				go func() { stopped <- first.signalAndWait(syscall.SIGTERM, 5*time.Second) }()
				if !expires {
					select {
					case err := <-stopped:
						t.Fatalf("holder exited before accepted provider work completed: %v\n%s", err, first.output.String())
					case <-time.After(100 * time.Millisecond):
					}
					unblock()
				}
				stopErr := <-stopped
				if expires {
					if stopErr == nil || strings.Contains(first.output.String(), "shutdown \xc2\xb7 complete") {
						t.Fatalf("expired grace reported successful shutdown: %v\n%s", stopErr, first.output.String())
					}
					unblock()
				} else if stopErr != nil {
					t.Fatalf("within-grace shutdown: %v\n%s", stopErr, first.output.String())
				}
				if err := standby.waitReady(ctx); err != nil {
					t.Fatalf("handoff readiness: %v; provider requests=%d\nholder:\n%s\nstandby:\n%s", err, len(provider.Requests()), first.output.String(), standby.output.String())
				}
				for {
					var event goldenEvent
					if err := standby.rpc.call(ctx, "event.get", map[string]any{"event_id": business.EventID}, &event); err != nil {
						t.Fatal(err)
					}
					complete := false
					for _, delivery := range event.Deliveries {
						if delivery.SubscriberType != "agent" {
							continue
						}
						if delivery.Terminal && delivery.Status != "delivered" {
							t.Fatalf("grace/handoff lost accepted work: %+v", delivery)
						}
						if delivery.Status == "delivered" {
							wantRetry := 0
							wantTotal := wantCalls
							if expires {
								wantTotal++
								wantRetry++
							}
							if len(provider.Requests()) != wantTotal || delivery.RetryCount != wantRetry {
								t.Fatalf("unexpected provider replay: calls=%d delivery=%+v", len(provider.Requests()), delivery)
							}
							complete = true
						}
					}
					if complete {
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal("delivery did not complete after handoff")
					case <-time.After(10 * time.Millisecond):
					}
				}
				if cut.selected {
					var readback struct {
						Run struct {
							RunID   string         `json:"run_id"`
							Status  string         `json:"status"`
							Failure map[string]any `json:"failure"`
						} `json:"run"`
					}
					if err := standby.rpc.call(ctx, "run.get", map[string]any{"run_id": business.RunID}, &readback); err != nil {
						t.Fatal(err)
					}
					if readback.Run.RunID != business.RunID || readback.Run.Status == "failed" || readback.Run.Status == "cancelled" || readback.Run.Failure != nil {
						t.Fatalf("selected handoff did not preserve its accepted child: %+v", readback.Run)
					}
				}
				if err := standby.stopAndWait(5 * time.Second); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
