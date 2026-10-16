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
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
)

const providerRestartRecoveryEnv = "TEST_PROVIDER_RESTART_RECOVERY"

type providerRestartRecoveryRequest struct {
	Source, Config, Backend string
}

func TestProviderRestartRecoveryProcess(t *testing.T) {
	raw := os.Getenv(providerRestartRecoveryEnv)
	if raw == "" {
		t.Skip("parent-owned #2701 restart-recovery process")
	}
	var request providerRestartRecoveryRequest
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
	opts.Output, opts.ErrorOutput = os.Stdout, os.Stderr
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if code := runFrom(ctx, repoRootForTest(), opts); code != 0 {
		t.Fatalf("provider restart recovery serve exit=%d", code)
	}
}

func TestProviderRestartRecoveryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, completed := range []bool{false, true} {
				t.Run(fmt.Sprintf("completed_%t", completed), func(t *testing.T) {
					proveProviderRestartRecovery(t, backend, completed)
				})
			}
		})
	}
}

func proveProviderRestartRecovery(t *testing.T, backend string, completed bool) {
	t.Helper()
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
		if !completed {
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
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
		location = filepath.Join(t.TempDir(), "provider-restart.sqlite")
		configPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend, location, channelOnboardingHostWorkspaceFields())
	}
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	updated := strings.Replace(string(raw), "  backend: anthropic\n", "  backend: openai_compatible\n  openai_compatible:\n    base_url: "+provider.URL+"\n", 1)
	if updated == string(raw) {
		t.Fatal("restart config has no canonical backend line")
	}
	updated += "\nbudget:\n  system_monthly_cap: 1\n"
	if err := os.WriteFile(configPath, []byte(updated), 0600); err != nil {
		t.Fatal(err)
	}
	setServeRuntimeRecovery(t, configPath, false, true)
	start := func() *channelOnboardingCrashServeProcess {
		t.Helper()
		request, err := json.Marshal(providerRestartRecoveryRequest{Source: root, Config: configPath, Backend: backend})
		if err != nil {
			t.Fatal(err)
		}
		return startServedCrashProcessBeforeReadiness(t, "TestProviderRestartRecoveryProcess", []string{providerRestartRecoveryEnv + "=" + string(request)})
	}
	process := start()
	endpoint := process.endpoint(t) + "/v1/rpc"
	observations := storetest.OpenIssue2564WorkloadObservation(t, backend, location)
	for cycle := 0; cycle < 2; cycle++ {
		for i := 0; i < 3; i++ {
			ack := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
				"event_name": "item.received", "bundle_hash": servedEventPublishFixtureBundleHash(t, root),
				"payload": map[string]any{"item_id": "emit"}, "idempotency_key": fmt.Sprintf("provider-restart-%d-%d", cycle, i),
			})
			t.Logf("original ACK cycle=%d run=%s event=%s", cycle, ack.RunID, ack.EventID)
		}
		deadline := time.Now().Add(20 * time.Second)
		for {
			attempts, err := storetest.ReadExternalAttemptStorage(context.Background(), observations.Reader)
			if err != nil {
				t.Fatal(err)
			}
			launched, active := 0, 0
			for _, attempt := range attempts {
				if attempt.State == "launched" {
					launched++
				}
				if attempt.State == "authorized" || attempt.State == "launched" || attempt.State == "response_observed" {
					active++
				}
			}
			ready := launched == 3 && entered.Load() == int32((cycle+1)*3)
			if completed {
				ready = active == 0 && len(attempts) >= (cycle+1)*3 && entered.Load() >= int32((cycle+1)*3)
				if ready {
					for _, attempt := range providerRestartAttempts(t, providerRestartSnapshot(t, observations.Reader)) {
						if attempt.state != "settled" || attempt.projection != "response_consumed" {
							ready = false
						}
					}
				}
			}
			if ready {
				t.Logf("restart recovery cut: completed=%t cycle=%d attempts=%d provider_entered=%d", completed, cycle, len(attempts), entered.Load())
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("restart recovery cut not reached: completed=%t attempts=%+v provider_entered=%d\n%s", completed, attempts, entered.Load(), process.output.String())
			}
			time.Sleep(10 * time.Millisecond)
		}
		before := providerRestartSnapshot(t, observations.Reader)
		expected := providerRestartAttempts(t, before)
		requestsBefore := entered.Load()
		if err := process.kill(); err != nil {
			t.Fatal(err)
		}
		process = start()
		endpoint = process.endpoint(t) + "/v1/rpc"
		after := providerRestartSnapshot(t, observations.Reader)
		requireProviderRestartEvidence(t, before, after, expected, completed)
		if entered.Load() != requestsBefore {
			t.Fatalf("restart repeated provider requests: before=%d after=%d", requestsBefore, entered.Load())
		}
		t.Logf("restart %d: PASS; attempts=%d; provider_requests=%d; completed_control=%t", cycle+1, len(expected), requestsBefore, completed)
	}
	if err := process.stop(); err != nil {
		t.Fatalf("recovered serve shutdown: %v", err)
	}
}

type providerRestartAttempt struct {
	id, operation, target, delivery, state, projection string
}

func providerRestartSnapshot(t *testing.T, selected any) map[string]storetest.SelectedForkStorageTableSnapshot {
	t.Helper()
	snapshot, err := storetest.ReadSelectedForkApplicationStorageSnapshot(context.Background(), selected)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func providerRestartAttempts(t *testing.T, snapshot map[string]storetest.SelectedForkStorageTableSnapshot) []providerRestartAttempt {
	t.Helper()
	rows, err := storetest.DecodeSelectedForkSnapshotRows(snapshot["runtime_external_effect_attempts"])
	if err != nil {
		t.Fatal(err)
	}
	var out []providerRestartAttempt
	for _, row := range rows {
		text := func(column string) string {
			value, err := row[column].Text()
			if err != nil {
				t.Fatal(err)
			}
			return value
		}
		out = append(out, providerRestartAttempt{text("attempt_id"), text("operation_id"), text("usage_target_id"), text("origin_delivery_id"), text("state"), text("completion_projection_phase")})
	}
	return out
}

func requireProviderRestartEvidence(t *testing.T, before, after map[string]storetest.SelectedForkStorageTableSnapshot, attempts []providerRestartAttempt, completed bool) {
	t.Helper()
	count := func(table, column, value string, predicate func(map[string]storetest.SelectedForkSnapshotCell) bool) int {
		rows, err := storetest.DecodeSelectedForkSnapshotRows(after[table])
		if err != nil {
			t.Fatal(err)
		}
		matched := 0
		for _, row := range rows {
			text, err := row[column].Text()
			if err != nil {
				t.Fatal(err)
			}
			if text == value && (predicate == nil || predicate(row)) {
				matched++
			}
		}
		return matched
	}
	stateIs := func(state string) func(map[string]storetest.SelectedForkSnapshotCell) bool {
		return func(row map[string]storetest.SelectedForkSnapshotCell) bool {
			value, err := row["state"].Text()
			return err == nil && value == state
		}
	}
	for _, attempt := range attempts {
		wantState := "outcome_uncertain"
		if completed {
			wantState = attempt.state
		}
		for _, table := range []string{"runtime_external_effect_attempts", "runtime_external_effect_operations"} {
			column, value := "attempt_id", attempt.id
			if table == "runtime_external_effect_operations" {
				column, value = "operation_id", attempt.operation
			}
			if got := count(table, column, value, stateIs(wantState)); got != 1 {
				t.Fatalf("restart %s exact terminal row count=%d want=1 state=%s", table, got, wantState)
			}
		}
		for _, evidence := range [][2]string{{"agent_turns", "completion_attempt_id"}, {"spend_ledger", "external_effect_attempt_id"}} {
			if got := count(evidence[0], evidence[1], attempt.id, nil); got != 1 {
				t.Fatalf("restart %s count=%d want=1", evidence[0], got)
			}
		}
		if !completed {
			if got := count("runtime_provider_attempt_drains", "attempt_id", attempt.id, stateIs("settled")); got != 1 {
				t.Fatalf("restart exact settled drain count=%d want=1", got)
			}
			if got := count("author_activity_occurrences", "source_identity", attempt.target+":failed", nil); got != 1 {
				t.Fatalf("restart failed turn story count=%d want=1", got)
			}
			if got := count("author_activity_occurrences", "source_identity", attempt.id+":outcome_uncertain", nil); got != 1 {
				t.Fatalf("restart uncertain attempt story count=%d want=1", got)
			}
			terminal := func(row map[string]storetest.SelectedForkSnapshotCell) bool {
				status, err := row["status"].Text()
				return err == nil && status == "dead_letter"
			}
			if got := count("event_deliveries", "delivery_id", attempt.delivery, terminal); got != 1 {
				t.Fatalf("restart original delivery terminal count=%d want=1", got)
			}
		}
	}
	if completed {
		for _, table := range []string{"runtime_external_effect_attempts", "runtime_external_effect_operations", "agent_turns", "spend_ledger", "runtime_provider_attempt_drains"} {
			if !reflect.DeepEqual(before[table], after[table]) {
				left, _ := json.Marshal(before[table])
				right, _ := json.Marshal(after[table])
				t.Fatalf("completed-attempt restart changed retained %s rows: before=%s after=%s", table, left, right)
			}
		}
	}
}
