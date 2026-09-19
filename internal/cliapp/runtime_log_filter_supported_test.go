package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// Real diagnostic writer -> selected reader -> authenticated HTTP/WS -> CLI.
// This deliberately does not claim serve boot or SQLite cursor-rich replay.
func TestRuntimeLogFilterSupportedConsumersBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			var selected interface {
				apiv1.ObservabilityReadStore
				runlifecycle.OperationOwner
				runlifecycle.CandidateStore
				CommitRuntimeLogEvent(context.Context, events.AdmittedEvent) (runtimebus.EventAppendOutcome, error)
			}
			if backend == "sqlite" {
				selected = storetest.StartSQLiteRuntimeStore(t)
			} else {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected = storetest.AdmitPostgresRuntimeStore(t, db)
			}
			instance := uuid.NewString()
			fact := sourceartifactfixture.Fact()
			ctx := correlation.WithSourceArtifactFact(correlation.WithRuntimeInstanceID(context.Background(), instance), fact)
			ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(instance, fact.BundleHash()))
			base := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
			runA, runB := uuid.NewString(), uuid.NewString()
			artifactB := sourceartifactfixture.New("agents.yaml", []byte("agents: {}\n# runtime-log-consumer-foreign\n"))
			storetest.RequireRun(t, ctx, selected, storetest.RunFixture{RunID: runA, Origin: storetest.ScenarioSetupOrigin(), StartedAt: base.Add(-time.Minute)})
			storetest.RequireRun(t, ctx, selected, storetest.RunFixture{RunID: runB, Origin: storetest.ScenarioSetupOrigin(), Artifact: artifactB, StartedAt: base.Add(-time.Minute)})
			insert := func(run, code, message string, at time.Time) string {
				t.Helper()
				failure := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassInternalFailure, code, "filter-probe", "probe", nil), "filter-probe", "probe")
				payload, err := json.Marshal(map[string]any{"log_level": "warn", "message": message,
					"details": map[string]any{"component": "filter-probe", "action": "probe", "failure": failure}})
				if err != nil {
					t.Fatal(err)
				}
				id := uuid.NewString()
				event := eventtest.DiagnosticDirect(id, events.EventTypePlatformRuntimeLog, "runtime", "", payload, 0, run, "", events.EventEnvelope{}, at)
				event, err = eventtest.AdmitPayload(event, "", string(event.Type()))
				if err != nil {
					t.Fatal(err)
				}
				admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := selected.CommitRuntimeLogEvent(ctx, admitted); err != nil {
					t.Fatal(err)
				}
				return id
			}
			insert(runA, "other", "excluded-older", base)
			wanted := insert(runA, "wanted", "included-replay", base.Add(time.Second))
			insert(runA, "other", "excluded-newer", base.Add(2*time.Second))
			insert(runB, "wanted", "excluded-foreign", base.Add(3*time.Second))
			work := worklifetime.NewProcess()
			handler, err := apiv1.NewHandler(apiv1.Options{
				PlatformSpecPath: ResolvePath(RepoRoot(), defaultPlatformSpecPath), AuthTokens: []string{"test-token"}, ProcessWorkOwner: work,
				Handlers:      apiv1.OperatorObservabilityHandlers(apiv1.ObservabilityHandlerOptions{Observability: selected}),
				Subscriptions: apiv1.OperatorSubscriptions(apiv1.SubscriptionOptions{Observability: selected}, apiv1.SubscriptionRuntimeOptions{PollInterval: 10 * time.Millisecond}),
			})
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(handler)
			defer server.Close()
			defer func() {
				work.Retire()
				joinCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := work.Wait(joinCtx); err != nil {
					t.Error(err)
				}
			}()
			client, err := newCLIAPIClientForTest(t, testRootCommandOptions(server))
			if err != nil {
				t.Fatal(err)
			}
			t.Run("authenticated_http", func(t *testing.T) {
				for _, tc := range []struct {
					name   string
					params map[string]any
					want   []string
				}{
					{"error_before_limit", map[string]any{"run_id": runA, "error_code": "wanted", "limit": 1}, []string{wanted}},
					{"bundle_and_code_before_limit", map[string]any{"bundle_hash": fact.BundleHash(), "error_code": "wanted", "limit": 1}, []string{wanted}},
					{"contradictory_bundle", map[string]any{"run_id": runA, "bundle_hash": artifactB.BundleHash()}, nil},
					{"missing_code", map[string]any{"run_id": runA, "error_code": "absent"}, nil},
					{"exclusive_since", map[string]any{"run_id": runA, "error_code": "wanted", "since": base.Add(time.Second).Format(time.RFC3339Nano)}, nil},
				} {
					t.Run(tc.name, func(t *testing.T) {
						var result operatorread.OperatorRuntimeLogListResult
						if err := client.call(ctx, "runtime.logs", tc.params, &result); err != nil {
							t.Fatal(err)
						}
						var ids []string
						for _, log := range result.Logs {
							ids = append(ids, log.LogID)
						}
						if !reflect.DeepEqual(ids, tc.want) {
							t.Fatalf("logs=%v want=%v", ids, tc.want)
						}
					})
				}
				if backend == "sqlite" {
					var result operatorread.OperatorRuntimeLogListResult
					err := client.call(ctx, "runtime.logs", map[string]any{"cursor": "unsupported"}, &result)
					var rpcErr *jsonRPCError
					if !errors.As(err, &rpcErr) || rpcErr.Code != -32602 || !strings.Contains(string(rpcErr.Data), `"cursor"`) {
						t.Fatalf("SQLite cursor refusal = %v, want invalid params identifying cursor", err)
					}
				}
			})
			t.Run("authenticated_subscription", func(t *testing.T) {
				for _, foreign := range []bool{false, true} {
					bundle := fact.BundleHash()
					if foreign {
						bundle = artifactB.BundleHash()
					}
					conn, _, err := websocket.DefaultDialer.Dial(strings.Replace(server.URL, "http", "ws", 1)+"/v1/ws", http.Header{"Authorization": []string{"Bearer test-token"}})
					if err != nil {
						t.Fatal(err)
					}
					defer conn.Close()
					if err := conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "runtime.subscribe_logs", "params": map[string]any{"run_id": runA, "bundle_hash": bundle, "error_code": "wanted", "replay_since": base.Format(time.RFC3339Nano)}}); err != nil {
						t.Fatal(err)
					}
					if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
						t.Fatal(err)
					}
					var response struct {
						Error  json.RawMessage `json:"error"`
						Result json.RawMessage `json:"result"`
					}
					if err := conn.ReadJSON(&response); err != nil {
						t.Fatal(err)
					}
					if len(response.Error) != 0 && string(response.Error) != "null" {
						t.Fatalf("subscribe: %s", response.Error)
					}
					if !foreign {
						var notification struct {
							Params struct {
								Result operatorread.OperatorRuntimeLogEntry `json:"result"`
							} `json:"params"`
						}
						if err := conn.ReadJSON(&notification); err != nil {
							t.Fatal(err)
						}
						if notification.Params.Result.LogID != wanted {
							t.Fatalf("notification=%+v", notification)
						}
					}
					if err := conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
						t.Fatal(err)
					}
					_, _, err = conn.ReadMessage()
					if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
						t.Fatalf("unexpected extra/foreign notification or close: %v", err)
					}
					conn.Close()
				}
			})
			t.Run("cli_snapshot", func(t *testing.T) {
				for _, code := range []string{"wanted", "absent"} {
					var stdout, stderr bytes.Buffer
					exit := executeRootCommandWithOptions(ctx, t.TempDir(), []string{"logs", "--run-id", runA, "--error-code", code, "--limit", "1"}, &stdout, &stderr, testRootCommandOptions(server))
					if exit != 0 {
						t.Fatalf("exit=%d stderr=%s", exit, stderr.String())
					}
					if code == "wanted" && !strings.Contains(stdout.String(), "included-replay") {
						t.Fatalf("missing match: %s", stdout.String())
					}
					if code == "absent" && !strings.Contains(stdout.String(), "No log entries match") {
						t.Fatalf("missing no-match: %s", stdout.String())
					}
					if strings.Contains(stdout.String(), "excluded-") {
						t.Fatalf("unfiltered CLI: %s", stdout.String())
					}
				}
			})
			t.Run("cli_follow_replay_and_live", func(t *testing.T) {
				followCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				output := &runtimeLogFilterOutput{chunks: make(chan string, 32)}
				var stderr bytes.Buffer
				done := make(chan int, 1)
				root := t.TempDir()
				opts := testRootCommandOptions(server)
				go func() {
					done <- executeRootCommandWithOptions(followCtx, root, []string{"logs", "--follow", "--run-id", runA, "--error-code", "wanted", "--replay-since", base.Format(time.RFC3339Nano)}, output, &stderr, opts)
				}()
				var all strings.Builder
				live := ""
				deadline := time.NewTimer(5 * time.Second)
				defer deadline.Stop()
				for {
					select {
					case chunk := <-output.chunks:
						all.WriteString(chunk)
						if strings.Contains(all.String(), "excluded-") {
							cancel()
							<-done
							t.Fatalf("unfiltered follow: %s", all.String())
						}
						if live == "" && strings.Contains(all.String(), wanted) {
							insert(runA, "other", "excluded-live", time.Now().UTC())
							live = insert(runA, "wanted", "included-live", time.Now().UTC())
						}
						if live != "" && strings.Contains(all.String(), live) {
							cancel()
							if code := <-done; code != 130 {
								t.Fatalf("cancel exit=%d stderr=%s", code, stderr.String())
							}
							return
						}
					case code := <-done:
						t.Fatalf("follow ended before live row: exit=%d stderr=%s", code, stderr.String())
					case <-deadline.C:
						cancel()
						<-done
						t.Fatalf("follow did not deliver replay/live rows: %s stderr=%s", all.String(), stderr.String())
					}
				}
			})
		})
	}
}

type runtimeLogFilterOutput struct{ chunks chan string }

func (w *runtimeLogFilterOutput) Write(p []byte) (int, error) {
	w.chunks <- string(p)
	return len(p), nil
}
