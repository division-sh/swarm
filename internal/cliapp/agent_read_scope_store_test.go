package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type readProofAgentStore interface {
	storetest.AgentFixtureStore
	apiv1.AgentReadStore
	apiv1.AgentIdentityResolver
	apiv1.AgentDeliveryLifecycleReadStore
	apiv1.ConversationReadStore
}

// Canonical lifecycle fixtures provide two real exact identities, not mocked
// resolver answers. This earns compiled reader proof, not serve lifecycle credit.
func TestReadProofFactoringCompiledAgentScopeBothStores(t *testing.T) {
	root := t.TempDir()
	binary := filepath.Join(root, "swarm")
	build := exec.Command("go", "build", "-o", binary, "./cmd/swarm")
	build.Dir = RepoRoot()
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build compiled scope reader: %v\n%s", err, output)
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected readProofAgentStore
			if backend == "sqlite" {
				selected = storetest.StartSQLiteRuntimeStore(t)
			} else {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected = storetest.AdmitPostgresRuntimeStore(t, db)
			}
			fact, err := correlation.NewSourceArtifactFact(storetest.SemanticFixtureBundleHash)
			if err != nil {
				t.Fatal(err)
			}
			instanceID := uuid.NewString()
			ctx := correlation.WithSourceArtifactFact(correlation.WithRuntimeInstanceID(context.Background(), instanceID), fact)
			ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(instanceID, fact.BundleHash()))
			runID := uuid.NewString()
			profile, err := selection.ResolveLiveBackend(selection.BackendAnthropic)
			if err != nil {
				t.Fatal(err)
			}
			intent, err := agentintent.Resolve(agentintent.SourceInline, "inline", "agents.yaml#agents.reader.intent", "Read proof fixture.")
			if err != nil {
				t.Fatal(err)
			}
			prompt, err := agentintent.IntentOnlyPrompt(intent)
			if err != nil {
				t.Fatal(err)
			}
			for _, instance := range []string{"one", "two"} {
				actor := actors.AgentConfig{ID: "reader", Identity: agentidentitytest.RuntimeForRun(t, runID, "reader", "read-proof", "flow", instance, "flow/"+instance), Role: "researcher", Type: "managed", Model: "cheap", Intent: intent, Prompt: prompt, FlowPath: "flow/" + instance, Config: json.RawMessage(`{}`)}
				execution, err := llm.ResolveAgentExecution(executionposture.Live, profile, nil, actor)
				if err != nil {
					t.Fatal(err)
				}
				storetest.RequireStaticAgentFixture(t, ctx, selected, manager.PersistedAgent{Config: execution.Actor, Status: "active", StartedAt: time.Now().UTC()})
			}
			registry, err := apiv1.LoadRegistry(filepath.Join(RepoRoot(), "platform-spec.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			const token = "read-proof-token"
			handler, err := apiv1.NewHandler(apiv1.Options{Registry: registry, AuthTokens: []string{token}, Handlers: apiv1.OperatorAgentConversationHandlers(apiv1.AgentConversationHandlerOptions{Agents: selected, Conversations: selected, DeliveryLifecycle: selected})})
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(handler)
			defer server.Close()
			cwd := t.TempDir()
			if err := os.WriteFile(filepath.Join(cwd, "api-token"), []byte(token+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			env := readProofCompiledScopeEnv(cwd)
			for _, command := range [][]string{{"agent", "view", "reader"}, {"agent", "diagnose", "reader"}, {"agent", "deliveries", "reader"}, {"conversation", "list", "--agent-id", "reader"}} {
				for _, mode := range []string{"", "--json", "--quiet"} {
					base := append(append([]string{}, command...), "--api-server", server.URL, "--api-token-file", "api-token")
					if mode != "" {
						base = append(base, mode)
					}
					t.Run(strings.Join(command[:2], "-")+mode, func(t *testing.T) {
						foreignExit := 5
						if command[0] == "conversation" {
							foreignExit = 3
						}
						for _, scope := range []struct {
							name string
							args []string
							code int
							why  string
						}{
							{"missing", nil, 2, "--run-id"},
							{"blank", []string{"--run-id", " "}, 2, "--run-id"},
							{"malformed", []string{"--run-id", "bad scope!"}, 2, "OpaqueId"},
							{"ambiguous", []string{"--run-id", runID}, 3, "flow_instance"},
							{"foreign", []string{"--run-id", uuid.NewString(), "--flow-instance", "flow/one"}, foreignExit, "AGENT_NOT_FOUND"},
						} {
							t.Run(scope.name, func(t *testing.T) {
								readProofCompiledRefusal(t, binary, cwd, env, scope.code, scope.why, append(append([]string{}, base...), scope.args...)...)
							})
						}
						// Both exact routes disambiguate through the same strict owner.
						for _, instance := range []string{"flow/one", "flow/two"} {
							args := append(append([]string{}, base...), "--run-id", runID, "--flow-instance", instance)
							out := readProofCompiledScopeSuccess(t, binary, cwd, env, args...)
							if mode == "--json" && !json.Valid([]byte(out)) {
								t.Fatalf("invalid public JSON: %s", out)
							}
						}
					})
				}
			}
		})
	}
}

func readProofCompiledScopeEnv(cwd string) []string {
	var env []string
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if key == "HOME" || key == "PATH" || key == "GOMAXPROCS" ||
			strings.HasPrefix(key, "SWARM_") || strings.HasPrefix(key, "ANTHROPIC_") ||
			strings.HasPrefix(key, "CLAUDE_") || strings.HasPrefix(key, "OPENAI_") || strings.HasPrefix(key, "PG") {
			continue
		}
		env = append(env, entry)
	}
	return append(env, "HOME="+cwd, "PATH=", "XDG_CONFIG_HOME="+filepath.Join(cwd, ".config"),
		"XDG_CACHE_HOME="+filepath.Join(cwd, ".cache"), "XDG_DATA_HOME="+filepath.Join(cwd, ".local/share"), "NO_COLOR=1")
}

func readProofCompiledScopeSuccess(t *testing.T, binary, cwd string, env []string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Dir, cmd.Env = cwd, env
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	if err := cmd.Run(); err != nil || errOut.Len() != 0 {
		t.Fatalf("compiled scope read %v: err=%v stdout=%s stderr=%s", args, err, &out, &errOut)
	}
	return out.String()
}

func readProofCompiledRefusal(t *testing.T, binary, cwd string, env []string, code int, why string, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Dir, cmd.Env = cwd, env
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	var exit *exec.ExitError
	err := cmd.Run()
	if !errors.As(err, &exit) || exit.ExitCode() != code || out.Len() != 0 || !strings.Contains(errOut.String(), why) {
		t.Fatalf("compiled refusal %v: err=%v stdout=%s stderr=%s; want exit=%d containing %q", args, err, &out, &errOut, code, why)
	}
}
