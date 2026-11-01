package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type issue2564ServedObservationOwner interface {
	operatorread.EntityReader
	operatorread.RunReader
	operatorread.ObservabilityReader
	pipeline.WorkflowPersistenceOwner
	SummarizeRun(context.Context, string) (deliverylifecycle.RunSummary, error)
}

type issue2564ServedFixture struct {
	Endpoint, Backend, BundleHash string
	selected                      issue2564ServedObservationOwner
	persistence                   serveRuntimePersistence
	Runtime                       *runtimepkg.Runtime
}

// This boots the real public serve owner directly from its native selected
// location. The mock variant retains the existing mailbox lifecycle purpose.
func issue2564ServeHarness(t *testing.T, backend, root string, mock bool) (*cliapp.ServeOptions, func() (*serveRuntimeTestProcess, issue2564ServedFixture)) {
	t.Helper()
	return issue2564ServeHarnessAtLocation(t, backend, root, mock, "")
}

// An explicit native location lets process-death proofs reopen the same store.
// Ordinary harness calls retain their original allocation and configuration.
func issue2564ServeHarnessAtLocation(t *testing.T, backend, root string, mock bool, location string) (*cliapp.ServeOptions, func() (*serveRuntimeTestProcess, issue2564ServedFixture)) {
	return issue2564ServeHarnessWithPosture(t, backend, root, mock, location, executionposture.MockOnly)
}

func issue2564LiveServeHarness(t *testing.T, backend, root, location string) (*cliapp.ServeOptions, func() (*serveRuntimeTestProcess, issue2564ServedFixture)) {
	t.Helper()
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/messages" || request.Header.Get("x-api-key") != "live-flow-fixture-key" {
			http.Error(w, "unexpected controlled provider request", http.StatusBadRequest)
			return
		}
		var turn struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
			Messages []struct {
				Content json.RawMessage `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(request.Body).Decode(&turn); err != nil {
			t.Errorf("decode live-flow fixture turn: %v", err)
			http.Error(w, "invalid provider turn", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		var payload map[string]any
		for _, message := range turn.Messages {
			var text string
			if json.Unmarshal(message.Content, &text) != nil {
				continue
			}
			if text == "ok" {
				_, _ = w.Write([]byte(`{"model":"claude-test","usage":{"input_tokens":1,"output_tokens":1},"content":[{"type":"text","text":"Work finished."}]}`))
				return
			}
			var input map[string]any
			if json.Unmarshal([]byte(text), &input) == nil && input["work_id"] != nil {
				payload = input
			}
		}
		for _, tool := range turn.Tools {
			name := "emit_work_finished"
			arguments := map[string]any{"work_id": payload["work_id"], "result": "finished"}
			if payload["sequence"] == float64(1) {
				name, arguments = "emit_work_progress", map[string]any{"work_id": payload["work_id"]}
			}
			if tool.Name == "emit_work_finish" {
				name, arguments = tool.Name, map[string]any{}
			}
			if tool.Name != name {
				continue
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"model": "claude-test", "usage": map[string]int{"input_tokens": 1, "output_tokens": 1},
				"content": []map[string]any{{"type": "tool_use", "id": "live-flow-finish", "name": name, "input": arguments}},
			})
			return
		}
		_, _ = w.Write([]byte(`{"model":"claude-test","usage":{"input_tokens":1,"output_tokens":1},"content":[{"type":"text","text":"Observed delivery."}]}`))
	}))
	t.Cleanup(provider.Close)
	redirectExternalHosts(t, map[string]string{"api.anthropic.com": provider.URL})
	setDoctorProviderSecret(t, "ANTHROPIC_API_KEY", "live-flow-fixture-key")
	return issue2564ServeHarnessWithPosture(t, backend, root, true, location, executionposture.Live)
}

func issue2564ServeHarnessWithPosture(t *testing.T, backend, root string, mock bool, location string, posture executionposture.Posture) (*cliapp.ServeOptions, func() (*serveRuntimeTestProcess, issue2564ServedFixture)) {
	t.Helper()
	unsetStoreSelectorEnv(t)
	stubServeRuntimeWorkspaceLifecycle(t)
	opts := &cliapp.ServeOptions{SourceRoot: root, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig()}
	if mock {
		configureHostWorkspaceProofLifecycle(t)
		opts.WorkspaceBackend, opts.WorkspaceBackendSet = workspace.BackendHost, true
	}
	if backend == "sqlite" {
		if mock {
			if location == "" {
				location = filepath.Join(t.TempDir(), "completion.sqlite")
			}
			if posture == executionposture.Live {
				opts.ConfigPath = writeStoreBackendRuntimeConfig(t, backend, location)
			} else {
				opts.ConfigPath = writeMockAgentRuntimeConfig(t, backend, location)
			}
		} else {
			if location == "" {
				location = filepath.Join(t.TempDir(), "lifecycle.sqlite")
			}
			opts.ConfigPath = writeStoreBackendRuntimeConfig(t, backend, location)
		}
	} else {
		dsn := location
		if dsn == "" {
			dsn = storetest.PostgresFixtureLocation(t)
		}
		original := buildStoresForServe
		buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
			owner, err := storeselected.OpenRuntime(ctx, storeselected.RuntimeRequest{Selection: storebackend.Selection{Backend: storebackend.BackendPostgres}, PostgresDSN: dsn, SessionLockTTL: runtimeSessionLockTTL(cfg)})
			if err != nil {
				return nil, err
			}
			if _, err := initializeServePlatformStateStores(ctx, owner.Schema()); err != nil {
				return nil, errors.Join(err, owner.CloseUnactivated())
			}
			return owner, nil
		}
		t.Cleanup(func() { buildStoresForServe = original })
		if mock && posture != executionposture.Live {
			opts.ConfigPath = writeMockAgentRuntimeConfig(t, backend, "")
		} else {
			opts.ConfigPath = writeServeRuntimeTestConfig(t)
		}
		opts.StoreMode, opts.StoreModeSet = "postgres", true
	}
	var persistence serveRuntimePersistence
	captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { persistence = p })
	retained := ownedMockLifecycleRoot(t)
	return opts, func() (*serveRuntimeTestProcess, issue2564ServedFixture) {
		var process *serveRuntimeTestProcess
		if mock {
			t.Logf("proof_surface=H in-process retained owned lifecycle posture=%s with real authenticated HTTP/WS; not public serve/test", posture)
			process = startRuntimeTestProcessWithRunner(t, repoRootForTest(), *opts, func(ctx context.Context, repo string, opts cliapp.ServeOptions) int {
				code, err := runOwnedLifecycle(ctx, repo, retained, opts, apiv1.AuthTokenResolution{Tokens: []string{apiv1.DefaultLoopbackAPIToken}, Explicit: true, Source: "internal-lifecycle-parent"}, posture, "")
				if err != nil {
					fmt.Fprintf(opts.ErrorOutput, "internal mailbox lifecycle setup: %v\n", err)
				}
				return code
			})
		} else {
			process = startServeRuntimeTestProcess(t, *opts)
		}
		process.waitForReadyLine()
		selected, ok := persistence.deps.EventStore.(issue2564ServedObservationOwner)
		if !ok {
			t.Fatalf("selected serve construction lacks exact observation owners: %T", persistence.deps.EventStore)
		}
		fixture := issue2564ServedFixture{Endpoint: "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc", Backend: backend, BundleHash: servedEventPublishFixtureBundleHash(t, opts.SourceRoot), selected: selected, persistence: persistence}
		if mock {
			fixture.Runtime = servedTestProcessRuntime(t, process)
		}
		return process, fixture
	}
}

func requireIssue2564LiveRootMode(t *testing.T, rt issue2564ServedFixture, runID, eventID string) {
	t.Helper()
	event := storetest.LoadCanonicalEventRecord(t, t.Context(), rt.selected, eventID)
	if rt.Runtime == nil || rt.Runtime.ExecutionPosture != executionposture.Live || event.RunID() != runID || event.ExecutionMode() != executionposture.Live.RootMode() {
		t.Fatalf("live lifecycle fixture changed its exact root: run=%s mode=%s", event.RunID(), event.ExecutionMode())
	}
}

func (f issue2564ServedFixture) waitEntityStage(t *testing.T, runID, entityID, stage string) string {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	lastState := ""
	for time.Now().Before(deadline) {
		state := storetest.ObserveWriterStage(t, context.Background(), f.selected, runID, entityID, stage)
		lastState = state.State
		if state.State == stage && state.EntityID != "" {
			return state.EntityID
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("entity %s/%s did not reach %s; last state=%q\n%s", runID, entityID, stage, lastState, f.debug(t, runID))
	return ""
}

func (f issue2564ServedFixture) waitDeliveries(t *testing.T, runID string) {
	t.Helper()
	deadline := time.Now().Add(servedProofPollDeadline)
	stable := 0
	for time.Now().Before(deadline) {
		summary, err := f.selected.SummarizeRun(context.Background(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if summary.Pending+summary.InProgress == 0 {
			stable++
			if stable == 4 {
				return
			}
		} else {
			stable = 0
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("run delivery quiescence timed out: %s", runID)
}

func (f issue2564ServedFixture) events(t *testing.T, runID, name string) []operatorread.OperatorEventFull {
	t.Helper()
	options := operatorread.OperatorEventListOptions{Filter: operatorread.OperatorEventListFilter{RunID: runID, EventName: name}, Limit: 100}
	var out []operatorread.OperatorEventFull
	for {
		page, err := f.selected.ListOperatorEvents(context.Background(), options)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, page.Events...)
		if page.NextCursor == "" {
			return out
		}
		options.Cursor = page.NextCursor
	}
}

func (f issue2564ServedFixture) debug(t *testing.T, runID string) string {
	t.Helper()
	report, err := f.selected.LoadRunDebugReport(context.Background(), runID, operatorread.RunDebugQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%+v", report)
}
