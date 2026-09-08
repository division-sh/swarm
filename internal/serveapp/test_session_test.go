package serveapp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

func admittedPrivateTestRequest(t *testing.T) cliapp.TestSessionRequest {
	t.Helper()
	isolateCLIAPIConfigEnv(t)
	unsetStoreSelectorEnv(t)
	source := scaffoldConformanceArchetype(t, "zero-agent-automation")
	var admitted cliapp.TestSessionRequest
	var out, errOut bytes.Buffer
	code := executeCLIFromWithRunners(context.Background(), source, []string{"test", source}, &out, &errOut, nil,
		func(_ context.Context, req cliapp.TestSessionRequest, _ func(context.Context, cliapp.TestSessionEndpoint) error) error {
			admitted = req
			return nil
		})
	if code != 0 || admitted.Bundle == nil {
		t.Fatalf("admit private session: code=%d stderr=%s", code, errOut.String())
	}
	return admitted
}

func TestPrivateTestSessionJoinedCleanupAndPrivateTransport(t *testing.T) {
	var previousToken string
	for _, outcome := range []string{"success", "callback_failure", "cancelled", "deadline"} {
		t.Run(outcome, func(t *testing.T) {
			req := admittedPrivateTestRequest(t)
			original := buildStoresForServe
			var db *sql.DB
			var privateRoot string
			buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
				if selection.Backend != storebackend.BackendSQLite {
					t.Fatalf("private backing store = %v", selection.Backend)
				}
				stores, err := original(ctx, selection, cfg)
				if err == nil {
					db = selectedStoreDatabaseForTest(t, stores)
					privateRoot = filepath.Dir(selection.SQLitePath)
				}
				return stores, err
			}
			t.Cleanup(func() { buildStoresForServe = original })
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			sentinel := errors.New("scenario callback failed")
			var endpoint cliapp.TestSessionEndpoint
			client := &http.Client{Timeout: 2 * time.Second}
			err := RunTestSession(ctx, req, func(ctx context.Context, ready cliapp.TestSessionEndpoint) error {
				endpoint = ready
				if ready.Token == previousToken {
					t.Fatal("private session reused predecessor authentication")
				}
				for _, token := range []string{"", previousToken, ready.Token} {
					request, err := http.NewRequestWithContext(ctx, http.MethodPost, ready.APIServer+"/v1/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"runtime.identity","params":{}}`))
					if err != nil {
						return err
					}
					request.Header.Set("Content-Type", "application/json")
					if token != "" {
						request.Header.Set("Authorization", "Bearer "+token)
					}
					response, err := client.Do(request)
					if err != nil {
						return err
					}
					body, readErr := io.ReadAll(response.Body)
					response.Body.Close()
					if readErr != nil {
						return readErr
					}
					if token != ready.Token && response.StatusCode != http.StatusUnauthorized {
						t.Fatalf("unauthenticated private API status=%d body=%s", response.StatusCode, body)
					}
					if token == ready.Token && (response.StatusCode != http.StatusOK || !bytes.Contains(body, []byte(`"source_artifacts"`))) {
						t.Fatalf("authenticated private API status=%d body=%s", response.StatusCode, body)
					}
				}
				previousToken = ready.Token
				request, err := http.NewRequestWithContext(ctx, http.MethodPost, ready.APIServer+"/v1/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"health.check","params":{}}`))
				if err != nil {
					return err
				}
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("Authorization", "Bearer "+ready.Token)
				healthResponse, err := client.Do(request)
				if err != nil {
					return err
				}
				var health struct {
					Result struct {
						Ready            bool   `json:"ready"`
						ExecutionPosture string `json:"execution_posture"`
					} `json:"result"`
				}
				err = json.NewDecoder(healthResponse.Body).Decode(&health)
				healthResponse.Body.Close()
				if err != nil || !health.Result.Ready || health.Result.ExecutionPosture != "mock_only" {
					t.Fatalf("private callback preceded mock readiness: health=%#v error=%v", health, err)
				}
				response, err := client.Get(ready.APIServer + "/webhooks/private/telegram")
				if err != nil {
					return err
				}
				response.Body.Close()
				if response.StatusCode != http.StatusNotFound {
					t.Fatalf("private ingress route status=%d", response.StatusCode)
				}
				switch outcome {
				case "callback_failure":
					return sentinel
				case "cancelled":
					cancel()
					return ctx.Err()
				case "deadline":
					scenarioCtx, stop := context.WithTimeout(ctx, time.Nanosecond)
					defer stop()
					<-scenarioCtx.Done()
					return scenarioCtx.Err()
				default:
					return nil
				}
			})
			switch outcome {
			case "success":
				if err != nil {
					t.Fatal(err)
				}
			case "callback_failure":
				if !errors.Is(err, sentinel) {
					t.Fatalf("error = %v", err)
				}
			case "cancelled":
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("error = %v", err)
				}
			case "deadline":
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("error = %v", err)
				}
			}
			if db == nil || privateRoot == "" || endpoint.Token == "" {
				t.Fatal("session never reached its real resources")
			}
			if err := db.Ping(); err == nil {
				t.Fatal("returned before closing store")
			}
			if _, err := os.Stat(privateRoot); !os.IsNotExist(err) {
				t.Fatalf("private root survived: %v", err)
			}
			if response, err := client.Get(endpoint.APIServer + "/healthz"); err == nil {
				response.Body.Close()
				t.Fatal("returned before joining HTTP listener shutdown")
			}
		})
	}
}

func TestPrivateTestStartupFailureUnwindsAcquiredResources(t *testing.T) {
	for _, stage := range []string{"store_acquisition", "schema_after_store", "workspace_prepare", "workspace_release_failure", "runtime_construction"} {
		t.Run(stage, func(t *testing.T) {
			req := admittedPrivateTestRequest(t)
			if stage == "schema_after_store" {
				req.PlatformSpecPath = filepath.Join(t.TempDir(), "missing-platform-spec.yaml")
			}
			original := buildStoresForServe
			t.Cleanup(func() { buildStoresForServe = original })
			var privateRoot string
			var db *sql.DB
			if stage == "runtime_construction" {
				originalProjection := projectRuntimePersistenceForServe
				t.Cleanup(func() { projectRuntimePersistenceForServe = originalProjection })
				projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
					projection := originalProjection(owner)
					projection.deps.EventStore = nil
					return projection
				}
			}
			workspaceReleased := 0
			if strings.HasPrefix(stage, "workspace_") {
				originalWorkspace := cliapp.ConfiguredWorkspaceLifecycleForServe
				t.Cleanup(func() { cliapp.ConfiguredWorkspaceLifecycleForServe = originalWorkspace })
				cliapp.ConfiguredWorkspaceLifecycleForServe = func(*config.Config, *sourceartifact.RuntimeProjection, semanticview.Source, cliapp.WorkspaceMountSources, cliapp.WorkspaceBackendSelection) (cliapp.ServeWorkspaceLifecycle, error) {
					return serveRuntimeWorkspaceStub{
						stubWorkspaceLifecycle: stubWorkspaceLifecycle{prereqErr: errors.New("injected workspace preparation failure")},
						release: func(context.Context) error {
							workspaceReleased++
							if stage == "workspace_release_failure" {
								return errors.New("injected workspace release failure")
							}
							return nil
						},
					}, nil
				}
			}
			buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
				privateRoot = filepath.Dir(selection.SQLitePath)
				if stage == "store_acquisition" {
					return nil, errors.New("injected store acquisition failure")
				}
				owner, err := original(ctx, selection, cfg)
				if err == nil {
					db = selectedStoreDatabaseForTest(t, owner)
				}
				return owner, err
			}
			called := false
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			err := RunTestSession(ctx, req, func(context.Context, cliapp.TestSessionEndpoint) error {
				called = true
				return nil
			})
			if err == nil || called || privateRoot == "" {
				t.Fatalf("startup failure: err=%v callback=%t root=%q", err, called, privateRoot)
			}
			if stage != "store_acquisition" && db == nil {
				t.Fatal("startup failure did not exercise the acquired store")
			}
			if strings.HasPrefix(stage, "workspace_") && workspaceReleased != 1 {
				t.Fatalf("workspace release calls = %d, want one", workspaceReleased)
			}
			if stage == "workspace_release_failure" && !strings.Contains(err.Error(), "injected workspace release failure") {
				t.Fatalf("cleanup error was discarded: %v", err)
			}
			if db != nil && db.Ping() == nil {
				t.Fatal("failed startup returned before closing its store")
			}
			if _, err := os.Stat(privateRoot); !os.IsNotExist(err) {
				t.Fatalf("failed startup leaked its private root: %v", err)
			}
			if _, err := os.Stat(filepath.Join(req.SourceRoot, ".swarm")); !os.IsNotExist(err) {
				t.Fatalf("failed private startup mutated project state: %v", err)
			}
		})
	}
}

func TestPrivateTestAdmissionPrecedesResourceAcquisition(t *testing.T) {
	for _, input := range []string{"invalid_variables", "invalid_event", "explicit_target"} {
		t.Run(input, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			unsetStoreSelectorEnv(t)
			source := scaffoldConformanceArchetype(t, "zero-agent-automation")
			args := []string{"test", source}
			switch input {
			case "invalid_variables":
				if err := os.WriteFile(filepath.Join(source, "tests", "smoke.yaml"), []byte("name: invalid\nvars: {broken: '${missing_identifier}'}\nsteps: []\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "invalid_event":
				if err := os.WriteFile(filepath.Join(source, "tests", "smoke.yaml"), []byte("name: invalid\nsteps:\n  - publish: nonexistent.event\n    payload: {}\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "explicit_target":
				args = append(args, "--api-server", "http://127.0.0.1:1")
			}
			called := false
			var out, errOut bytes.Buffer
			code := executeCLIFromWithRunners(context.Background(), source, args, &out, &errOut, nil,
				func(context.Context, cliapp.TestSessionRequest, func(context.Context, cliapp.TestSessionEndpoint) error) error {
					called = true
					return nil
				})
			if code != 2 || called {
				t.Fatalf("code=%d acquired=%t stderr=%s", code, called, errOut.String())
			}
			if _, err := os.Stat(filepath.Join(source, ".swarm")); !os.IsNotExist(err) {
				t.Fatalf("project state created: %v", err)
			}
		})
	}
}

func TestPrivateTestRootCleanupRemovesImmutableProjectionsWithoutFollowingLinks(t *testing.T) {
	root := filepath.Join(t.TempDir(), "private")
	projection := filepath.Join(root, "data-projections", "actor", ".swarm")
	if err := os.MkdirAll(projection, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(projection, "access.v1.json"), []byte("{}"), 0o444); err != nil {
		t.Fatal(err)
	}
	external := t.TempDir()
	if err := os.WriteFile(filepath.Join(external, "untouched"), []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(external, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, filepath.Join(root, "outside")); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(projection, 0o555); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Dir(projection), 0o555); err != nil {
		t.Fatal(err)
	}
	if err := removePrivateTestRoot(root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("private root survived: %v", err)
	}
	info, err := os.Stat(external)
	if err != nil || info.Mode().Perm() != 0o755 {
		t.Fatalf("external directory permissions changed: %v %v", info, err)
	}
	if contents, err := os.ReadFile(filepath.Join(external, "untouched")); err != nil || string(contents) != "keep" {
		t.Fatalf("external file changed: %q %v", contents, err)
	}
}

func TestPrivateTestDoesNotSelectConcurrentLiveRuntime(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	unsetStoreSelectorEnv(t)
	source := canonicalrouting.CopyExample(t, canonicalrouting.RootIngress)
	liveStore := filepath.Join(t.TempDir(), "live.sqlite")
	configPath := writeStoreBackendRuntimeConfig(t, "sqlite", liveStore)
	live := startServeRuntimeTestProcessAtRepo(t, source, cliapp.ServeOptions{
		ConfigPath: configPath, SourceRoot: source, PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath),
		APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true,
	})
	live.waitForReadyLine()
	endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, live.outputString())
	var before, after map[string]any
	requireServedJSONRPCResult(t, endpoint+"/v1/rpc", "run.list", map[string]any{}, &before)
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, []byte(fmt.Sprintf("\nconnection:\n  api_server: %q\n", endpoint))...)
	if err := os.WriteFile(configPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	code := executeCLIFrom(context.Background(), source, []string{"test", source, "--config", configPath, "--timeout", "10s", "--poll-interval", "10ms"}, &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("private test code=%d stdout=%s stderr=%s", code, out.String(), errOut.String())
	}
	requireServedJSONRPCResult(t, endpoint+"/v1/rpc", "run.list", map[string]any{}, &after)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("private test mutated concurrent live runs: before=%v after=%v", before, after)
	}
	if code := live.stop(); code != 0 {
		t.Fatalf("live process stop=%d: %s", code, live.outputString())
	}
}
