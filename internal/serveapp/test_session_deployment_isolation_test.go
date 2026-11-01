package serveapp

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

func TestSwarmTestUsesCompleteDeploymentLLMConfig(t *testing.T) {
	const completeLLM = `llm:
  backend: openai_compatible
  models:
    regular: {openai_compatible: configured-model}
  session:
    lock_ttl: 17s
    rotate_after_turns: 19
    rotate_on_parse_failures: 2
  provider_limits:
    openai_compatible:
      max_concurrency: 2
      max_concurrency_max_wait: 3s
  claude_cli:
    command: configured-claude
    timeout: 23s
    output_format: json
  openai_compatible:
    base_url: http://127.0.0.1:9/v1
  openai_responses:
    base_url: http://127.0.0.1:9/responses
`
	for _, test := range []struct {
		name, config, refusal string
		explicit              bool
	}{
		{name: "defaults", config: "{}\n"},
		{name: "configured_endpoint", config: "llm:\n  backend: openai_compatible\n  openai_compatible:\n    base_url: http://127.0.0.1:9/v1\n"},
		{name: "complete_discovered", config: completeLLM},
		{name: "complete_explicit", config: completeLLM, explicit: true},
		{name: "responses_endpoint", config: "llm:\n  backend: openai_responses\n  openai_responses:\n    base_url: http://127.0.0.1:9/v1\n"},
		{name: "missing_endpoint", config: "llm:\n  backend: openai_compatible\n", refusal: "llm.openai_compatible.base_url is required"},
		{name: "invalid_endpoint", config: "llm:\n  backend: openai_compatible\n  openai_compatible:\n    base_url: localhost:9/v1\n", refusal: "llm.openai_compatible.base_url must be an http(s) URL"},
	} {
		t.Run(test.name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			unsetStoreSelectorEnv(t)
			source := scaffoldConformanceArchetype(t, "zero-agent-automation")
			configPath := filepath.Join(source, ".swarm", "swarm.yaml")
			if err := os.WriteFile(configPath, []byte(test.config), 0o600); err != nil {
				t.Fatal(err)
			}
			defer unchangedPrivateTestDeployment(t, source)()
			loadOptions := cliapp.RuntimeConfigLoadOptions{RepoRoot: source}
			args := []string{"test", ".", "tests/smoke.yaml", "--timeout", "10s", "--poll-interval", "10ms"}
			if test.explicit {
				loadOptions.ExplicitPath = configPath
				args = append(args, "--config", configPath)
			}
			expected, loadErr := cliapp.LoadRuntimeConfigWithOptions(loadOptions)
			if test.refusal == "" && loadErr != nil {
				t.Fatal(loadErr)
			}
			original := buildStoresForServe
			t.Cleanup(func() { buildStoresForServe = original })
			acquired := false
			buildStoresForServe = func(ctx context.Context, selected storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
				acquired = true
				if test.refusal != "" {
					t.Fatal("invalid endpoint reached private resource acquisition")
				}
				if !reflect.DeepEqual(cfg.LLM, expected.Config.LLM) {
					t.Fatalf("private LLM config differs from canonical reader: got=%#v want=%#v", cfg.LLM, expected.Config.LLM)
				}
				if selected.Backend != storebackend.BackendSQLite || strings.HasPrefix(selected.SQLitePath, source+string(os.PathSeparator)) {
					t.Fatalf("test did not keep its private store: %#v", selected)
				}
				return original(ctx, selected, cfg)
			}
			var out, errOut bytes.Buffer
			code := executeCLIFrom(t.Context(), source, args, &out, &errOut, nil)
			if test.refusal != "" {
				if code != 2 || acquired || !strings.Contains(errOut.String(), test.refusal) {
					t.Fatalf("endpoint refusal code=%d acquired=%t stdout=%s stderr=%s", code, acquired, &out, &errOut)
				}
				return
			}
			if code != 0 || !acquired || !strings.Contains(out.String(), "swarm test ok: scenarios=1") {
				t.Fatalf("public test code=%d acquired=%t stdout=%s stderr=%s", code, acquired, &out, &errOut)
			}
		})
	}
}

func TestPrivateTestIgnoresDeploymentResources(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			unsetStoreSelectorEnv(t)
			source := scaffoldConformanceArchetype(t, "zero-agent-automation")
			defer unchangedPrivateTestDeployment(t, source)()
			var requests atomic.Int32
			trap := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				http.Error(w, "deployment endpoint must not be used by test", http.StatusServiceUnavailable)
			}))
			defer trap.Close()
			deployment := t.TempDir()
			storePath := filepath.Join(deployment, "retained.sqlite")
			if err := os.WriteFile(storePath, []byte("retained-store-sentinel"), 0o600); err != nil {
				t.Fatal(err)
			}
			unreadableDocument := t.TempDir() // A directory is never a valid token/credential document.
			t.Setenv("SWARM_CREDENTIALS_FILE", unreadableDocument)
			t.Setenv("PATH", t.TempDir())
			configPath := filepath.Join(deployment, "config.yaml")
			text := fmt.Sprintf(`store:
  backend: %s
  sqlite:
    path: %q
database:
  host: 127.0.0.1
  port: 1
  name: unreachable
  user: unreachable
  password_file: %q
workspace:
  backend: docker
  image: unavailable-test-image
  docker_bin: %q
  host_root: %q
connection:
  api_server: %q
  api_token_file: %q
serve:
  api_listen_addr: %q
  mcp_listen_addr: %q
  api_token_file: %q
`, backend, storePath, unreadableDocument,
				filepath.Join(deployment, "missing-docker"), filepath.Join(deployment, "must-not-create"),
				trap.URL, unreadableDocument, trap.Listener.Addr().String(), trap.Listener.Addr().String(), unreadableDocument)
			if err := os.WriteFile(configPath, []byte(text), 0o600); err != nil {
				t.Fatal(err)
			}
			original := buildStoresForServe
			t.Cleanup(func() { buildStoresForServe = original })
			acquired := false
			buildStoresForServe = func(ctx context.Context, selected storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
				acquired = true
				if selected.Backend != storebackend.BackendSQLite || selected.SQLitePath == storePath ||
					strings.HasPrefix(selected.SQLitePath, deployment+string(os.PathSeparator)) {
					t.Fatalf("private store selected deployment resource: %#v", selected)
				}
				if cfg.Database.PasswordFile != "" || cfg.Workspace.HostRoot == filepath.Join(deployment, "must-not-create") {
					t.Fatal("deployment resource configuration crossed private construction boundary")
				}
				return original(ctx, selected, cfg)
			}
			var out, errOut bytes.Buffer
			code := executeCLIFrom(context.Background(), source,
				[]string{"test", source, "--config", configPath, "--timeout", "10s", "--poll-interval", "10ms"}, &out, &errOut, nil)
			if code != 0 || !acquired || !strings.Contains(out.String(), "swarm test ok:") {
				t.Fatalf("private test code=%d acquired=%t stdout=%s stderr=%s", code, acquired, out.String(), errOut.String())
			}
			if requests.Load() != 0 {
				t.Fatalf("private test contacted deployment endpoint %d times", requests.Load())
			}
			if raw, err := os.ReadFile(storePath); err != nil || string(raw) != "retained-store-sentinel" {
				t.Fatalf("retained store changed: %q %v", raw, err)
			}
			for _, path := range []string{filepath.Join(deployment, "must-not-create")} {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("private test created deployment/project state at %s: %v", path, err)
				}
			}
		})
	}
}

func TestPrivateTestRejectsEachExplicitDeploymentTargetBeforeAcquisition(t *testing.T) {
	for _, flag := range []string{"--api-server", "--context", "--api-token-file"} {
		t.Run(flag, func(t *testing.T) {
			req := admittedPrivateTestRequest(t)
			var out, errOut bytes.Buffer
			code := executeCLIFromWithRunners(context.Background(), req.SourceRoot,
				[]string{"test", req.SourceRoot, flag, "must-not-resolve"}, &out, &errOut, nil,
				func(context.Context, cliapp.TestSessionRequest, func(context.Context, cliapp.TestSessionEndpoint) error) error {
					t.Fatal("explicit target reached private resource acquisition")
					return nil
				})
			if code != 2 || !strings.Contains(errOut.String(), "fresh private session") {
				t.Fatalf("target refusal code=%d stderr=%s", code, errOut.String())
			}
		})
	}
}
