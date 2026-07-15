package cliapp

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/requiredagentsparentconnect"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/store"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/testpostgres"
	"github.com/google/uuid"
)

type delayedRunStatusAgent struct {
	id            string
	subscriptions []events.EventType
	started       chan struct{}
	release       chan struct{}
}

func chdirForTest(t *testing.T, dir string) {
	t.Helper()
	previous, err := os.Getwd()
	if err != nil {
		t.Fatalf("get current working directory: %v", err)
	}
	previousPWD, hadPreviousPWD := os.LookupEnv("PWD")
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir %q: %v", dir, err)
	}
	if err := os.Setenv("PWD", dir); err != nil {
		t.Fatalf("set PWD for chdir %q: %v", dir, err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(previous); err != nil {
			t.Fatalf("restore working directory %q: %v", previous, err)
		}
		if hadPreviousPWD {
			if err := os.Setenv("PWD", previousPWD); err != nil {
				t.Fatalf("restore PWD %q: %v", previousPWD, err)
			}
			return
		}
		if err := os.Unsetenv("PWD"); err != nil {
			t.Fatalf("unset PWD after chdir: %v", err)
		}
	})
}

func (a delayedRunStatusAgent) ID() string { return a.id }
func (delayedRunStatusAgent) Type() string { return "test" }
func (a delayedRunStatusAgent) Subscriptions() []events.EventType {
	return append([]events.EventType(nil), a.subscriptions...)
}

func (a delayedRunStatusAgent) OnEvent(ctx context.Context, evt events.Event) ([]events.Event, error) {
	select {
	case a.started <- struct{}{}:
	default:
	}
	select {
	case <-a.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return []events.Event{eventtest.RootIngress(uuid.NewString(), events.EventType("scan.completed"), a.id, "", []byte(`{}`), 0, evt.RunID(), "", events.EnvelopeForEntityID(events.EventEnvelope{}, evt.EntityID()), time.Now().UTC())}, nil
}

func publishRunStatusRootEvent(t *testing.T, bus *runtimebus.EventBus, runID, entityID string) string {
	t.Helper()
	eventID := uuid.NewString()
	if err := bus.Publish(context.Background(), eventtest.RootIngress(
		eventID,
		events.EventType("scan.requested"),
		"api.v1",
		"",
		[]byte(`{"topic":"sample"}`),
		0,
		runID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("publish root event: %v", err)
	}
	return eventID
}

func seedRunStatusEntityState(t *testing.T, db *sql.DB, runID, entityID string) {
	t.Helper()
	now := time.Now().UTC()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, slug, name, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, 'run-status-test', 'default', 'status-entity', 'Status Entity', 'ready',
			'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, $3, $3, $3
		)
	`, runID, entityID, now); err != nil {
		t.Fatalf("seed run status entity_state: %v", err)
	}
	if err := storerunlifecycle.SyncCounts(context.Background(), db, runID); err != nil {
		t.Fatalf("sync run status entity_count: %v", err)
	}
}

func markRunStatusCompleted(t *testing.T, pg *store.PostgresStore, eventID string) {
	t.Helper()
	if err := pg.ConvergeNormalRunCompletion(context.Background(), eventID, []string{"ready"}, map[string][]string{"run-status-test": {"ready"}}); err != nil {
		t.Fatalf("converge normal run completion: %v", err)
	}
}

func TestCLI_RootNoArgsPrintsHelpAndDoesNotStartRuntime(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := executeRootCommand(context.Background(), t.TempDir(), nil, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("root code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{"Swarm runs event-driven agent workflows", "Getting started:", "Observe & debug:", "serve", "verify", "completion", "version"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("root help missing %q:\n%s", want, stdout.String())
		}
	}
	for _, retired := range []string{"investigate"} {
		if strings.Contains(stdout.String(), "\n  "+retired+" ") {
			t.Fatalf("root help advertises retired command %q:\n%s", retired, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("root stderr = %q, want empty", stderr.String())
	}
}

func TestCLI_HelpCommandPrintsRootHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := executeRootCommand(context.Background(), t.TempDir(), []string{"help"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("help code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !strings.Contains(stdout.String(), "Swarm runs event-driven agent workflows") || !strings.Contains(stdout.String(), "serve") {
		t.Fatalf("help output missing root command help:\n%s", stdout.String())
	}
}

func TestCLI_CompletionCommandSupportsCobraShells(t *testing.T) {
	for _, shell := range []string{"bash", "zsh", "fish", "powershell"} {
		t.Run(shell, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := executeRootCommand(context.Background(), t.TempDir(), []string{"completion", shell}, &stdout, &stderr)
			if code != 0 {
				t.Fatalf("completion %s code = %d stderr=%s", shell, code, stderr.String())
			}
			if got := stdout.String(); !strings.Contains(got, "swarm") {
				t.Fatalf("completion %s output missing swarm command:\n%s", shell, got)
			}
		})
	}
}

func TestCLI_VerifyHelpAndCompletionOwnedByCobra(t *testing.T) {
	for _, args := range [][]string{
		{"verify", "--help"},
		{"verify", "-h"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := executeRootCommand(context.Background(), t.TempDir(), args, &stdout, &stderr)
			if code != 0 {
				t.Fatalf("%s code = %d stderr=%s stdout=%s", strings.Join(args, " "), code, stderr.String(), stdout.String())
			}
			for _, want := range []string{"Usage:", "--contracts", "--platform-spec", "--json", "--quiet", "--no-color", "--log-level"} {
				if !strings.Contains(stdout.String(), want) {
					t.Fatalf("%s help missing %q:\n%s", strings.Join(args, " "), want, stdout.String())
				}
			}
			if strings.Contains(stdout.String(), "flag_parsing_disabled") || strings.Contains(stderr.String(), "flag: help requested") {
				t.Fatalf("%s leaked old flag parser state stdout=%q stderr=%q", strings.Join(args, " "), stdout.String(), stderr.String())
			}
		})
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommand(context.Background(), t.TempDir(), []string{"completion", "bash"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("completion bash code = %d stderr=%s", code, stderr.String())
	}
	verifySectionStart := strings.Index(stdout.String(), "_swarm_verify()")
	if verifySectionStart < 0 {
		t.Fatalf("completion output missing _swarm_verify section")
	}
	verifySection := stdout.String()[verifySectionStart:]
	if verifySectionEnd := strings.Index(verifySection[len("_swarm_verify()"):], "\n_swarm_"); verifySectionEnd >= 0 {
		verifySection = verifySection[:len("_swarm_verify()")+verifySectionEnd]
	}
	for _, want := range []string{"--contracts", "--platform-spec", "--json", "--quiet", "--no-color", "--log-level"} {
		if !strings.Contains(verifySection, want) {
			t.Fatalf("_swarm_verify completion missing %q:\n%s", want, verifySection)
		}
	}
	if strings.Contains(verifySection, "flag_parsing_disabled") {
		t.Fatalf("_swarm_verify completion still disables Cobra flag parsing:\n%s", verifySection)
	}

	stdout.Reset()
	stderr.Reset()
	code = executeRootCommand(context.Background(), t.TempDir(), []string{"__complete", "verify", "--"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("__complete verify -- code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{"--contracts", "--platform-spec", "--json", "--quiet", "--no-color", "--log-level"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("__complete verify -- missing %q:\n%s", want, stdout.String())
		}
	}
}

func TestCLI_RetiredCommandsHiddenFromHelpAndCompletion(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"help"},
	} {
		t.Run("root "+strings.Join(args, " "), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := executeRootCommand(context.Background(), t.TempDir(), args, &stdout, &stderr)
			if code != 0 {
				t.Fatalf("root help code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
			}
			for _, retired := range []string{"investigate"} {
				if strings.Contains(stdout.String(), "\n  "+retired+" ") {
					t.Fatalf("root help advertises retired command %q:\n%s", retired, stdout.String())
				}
			}
		})
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommand(context.Background(), t.TempDir(), []string{"__complete", ""}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("__complete root code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, retired := range []string{"investigate\t"} {
		if strings.Contains(stdout.String(), retired) {
			t.Fatalf("__complete root advertises retired command %q:\n%s", retired, stdout.String())
		}
	}

	stdout.Reset()
	stderr.Reset()
	code = executeRootCommand(context.Background(), t.TempDir(), []string{"control", "--help"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("control help code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if strings.Contains(stdout.String(), "\n  mailbox") {
		t.Fatalf("control help advertises retired mailbox command:\n%s", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = executeRootCommand(context.Background(), t.TempDir(), []string{"__complete", "control", ""}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("__complete control code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if strings.Contains(stdout.String(), "mailbox\t") {
		t.Fatalf("__complete control advertises retired mailbox command:\n%s", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = executeRootCommand(context.Background(), t.TempDir(), []string{"__complete", "investigate", ""}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("__complete investigate code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, retired := range []string{"runs\t", "run\t", "trace\t", "health\t"} {
		if strings.Contains(stdout.String(), retired) {
			t.Fatalf("__complete investigate advertises retired subcommand %q:\n%s", retired, stdout.String())
		}
	}
}

func TestCLI_VersionPrintsLocalBinaryIdentity(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := executeRootCommand(context.Background(), t.TempDir(), []string{"version"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("version code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{"Swarm dev", "Commit: unknown", "Built: unknown", "Go:"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("version output missing %q:\n%s", want, stdout.String())
		}
	}
}

func TestCLI_ServeOwnsRuntimeStartupFlags(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := executeRootCommand(context.Background(), t.TempDir(), []string{"serve", "--help"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("serve help code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{"Start the Swarm runtime", "--config", "--backend", "openai_responses", "--contracts", "--data", "--workspace-backend", "--bundle-hash", "--api-listen-addr", "API, WebSocket, health, and readiness routes", "--mcp-listen-addr", "MCP and tools routes", "--platform-spec", "--store", "--self-check", "--dev", "--require-bundle-match", "--no-require-bundle-match", "--abandon-active-runs", "--shutdown-grace", "--verbose"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("serve help missing %q:\n%s", want, stdout.String())
		}
	}
	for _, notWant := range []string{"--health-addr", "unified serve listener", "--api-port", "--mcp-port", "--api ", "--no-api", "--mcp ", "--no-mcp", "--log-level"} {
		if strings.Contains(stdout.String(), notWant) {
			t.Fatalf("serve help exposed unpromoted listener/topology flag %q:\n%s", notWant, stdout.String())
		}
	}
}

func TestCLI_ServeBundleHashValidationAndSerialScope(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantStderr string
		wantHash   string
	}{
		{
			name:       "blank bundle hash rejected",
			args:       []string{"serve", "--bundle-hash", "  "},
			wantCode:   2,
			wantStderr: "--bundle-hash must be non-empty",
		},
		{
			name:       "legacy fingerprint shape rejected",
			args:       []string{"serve", "--bundle-hash", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			wantCode:   2,
			wantStderr: "--bundle-hash must be bundle-v1:sha256:<64 lowercase hex>",
		},
		{
			name:       "contracts conflict rejected",
			args:       []string{"serve", "--bundle-hash", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--contracts", "contracts"},
			wantCode:   2,
			wantStderr: "--bundle-hash is mutually exclusive with --contracts",
		},
		{
			name:       "dev conflict rejected",
			args:       []string{"serve", "--bundle-hash", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--dev"},
			wantCode:   2,
			wantStderr: "--bundle-hash is mutually exclusive with --dev",
		},
		{
			name:       "sqlite conflict rejected",
			args:       []string{"serve", "--bundle-hash", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--store", "sqlite"},
			wantCode:   2,
			wantStderr: "--bundle-hash requires --store postgres",
		},
		{
			name:     "canonical bundle hash accepted",
			args:     []string{"serve", "--bundle-hash", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--api-listen-addr", "127.0.0.1:0", "--mcp-listen-addr", "127.0.0.1:0"},
			wantCode: 0,
			wantHash: "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
		{
			name:       "duplicate pinned bundle hash rejected",
			args:       []string{"serve", "--bundle-hash", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--bundle-hash", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			wantCode:   2,
			wantStderr: "--bundle-hash values must be unique",
		},
		{
			name:     "repeated canonical bundle hashes accepted",
			args:     []string{"serve", "--bundle-hash", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--bundle-hash", "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "--api-listen-addr", "127.0.0.1:0", "--mcp-listen-addr", "127.0.0.1:0"},
			wantCode: 0,
			wantHash: "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var captured ServeOptions
			called := false
			opts := defaultRootCommandOptions()
			opts.runServe = func(_ context.Context, _ string, serveOpts ServeOptions) int {
				called = true
				captured = serveOpts
				return 0
			}

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, opts)
			if code != tc.wantCode {
				t.Fatalf("serve code = %d, want %d\nstdout=%s\nstderr=%s", code, tc.wantCode, stdout.String(), stderr.String())
			}
			if tc.wantStderr != "" {
				if !strings.Contains(stderr.String(), tc.wantStderr) {
					t.Fatalf("serve stderr missing %q:\n%s", tc.wantStderr, stderr.String())
				}
				if called {
					t.Fatal("serve runtime started despite invalid bundle hash configuration")
				}
				return
			}
			if !called {
				t.Fatal("serve runtime was not called for valid bundle hash")
			}
			if captured.BundleHash != tc.wantHash {
				t.Fatalf("BundleHash = %q, want %q", captured.BundleHash, tc.wantHash)
			}
			if tc.name == "repeated canonical bundle hashes accepted" {
				wantExtra := []string{"bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
				if !reflect.DeepEqual(captured.BundleHashes, wantExtra) {
					t.Fatalf("BundleHashes = %#v, want %#v", captured.BundleHashes, wantExtra)
				}
			}
		})
	}
}

func TestCLI_ServeRetiresHealthAddrAndValidatesListenAddresses(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{
			name:       "health addr retired",
			args:       []string{"serve", "--health-addr", "127.0.0.1:0"},
			wantStderr: "unknown flag: --health-addr",
		},
		{
			name:       "api bare port rejected",
			args:       []string{"serve", "--api-listen-addr", "8081"},
			wantStderr: "--api-listen-addr must be a host:port listen address",
		},
		{
			name:       "mcp host without port rejected",
			args:       []string{"serve", "--mcp-listen-addr", "127.0.0.1"},
			wantStderr: "--mcp-listen-addr must be a host:port listen address",
		},
		{
			name:       "api url rejected",
			args:       []string{"serve", "--api-listen-addr", "http://127.0.0.1:8081"},
			wantStderr: "--api-listen-addr must be a host:port listen address, not a URL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := executeRootCommand(context.Background(), t.TempDir(), tt.args, &stdout, &stderr)
			if code != 2 {
				t.Fatalf("serve code = %d, want 2\nstdout=%s\nstderr=%s", code, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tt.wantStderr) {
				t.Fatalf("serve stderr missing %q:\n%s", tt.wantStderr, stderr.String())
			}
		})
	}
}

func TestCLI_ServeListenAddrFlagsConsumeIndependentOwners(t *testing.T) {
	isolateCLIAPIConfigEnv(t)

	var captured ServeOptions
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ string, serveOpts ServeOptions) int {
		captured = serveOpts
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", "--api-listen-addr", "0.0.0.0:9001"}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("serve code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.APIListenAddr != "0.0.0.0:9001" {
		t.Fatalf("api listen addr = %q, want override", captured.APIListenAddr)
	}
	if captured.MCPListenAddr != defaultMCPListenAddr {
		t.Fatalf("mcp listen addr = %q, want default %q", captured.MCPListenAddr, defaultMCPListenAddr)
	}

	captured = ServeOptions{}
	stdout.Reset()
	stderr.Reset()
	code = executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", "--mcp-listen-addr", "127.0.0.1:9002"}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("serve code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.APIListenAddr != defaultAPIListenAddr {
		t.Fatalf("api listen addr = %q, want default %q", captured.APIListenAddr, defaultAPIListenAddr)
	}
	if captured.MCPListenAddr != "127.0.0.1:9002" {
		t.Fatalf("mcp listen addr = %q, want override", captured.MCPListenAddr)
	}
}

func TestCLI_ServeListenAddrSourcePrecedence(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		config  map[string]string
		wantAPI string
		wantMCP string
	}{
		{
			name: "config beats defaults",
			config: map[string]string{
				"serve_api_listen_addr": "127.0.0.1:9101",
				"serve_mcp_listen_addr": "127.0.0.1:9102",
			},
			wantAPI: "127.0.0.1:9101",
			wantMCP: "127.0.0.1:9102",
		},
		{
			name: "flag beats config for that listener only",
			args: []string{"--api-listen-addr", "127.0.0.1:9301"},
			config: map[string]string{
				"serve_api_listen_addr": "127.0.0.1:9101",
				"serve_mcp_listen_addr": "127.0.0.1:9102",
			},
			wantAPI: "127.0.0.1:9301",
			wantMCP: "127.0.0.1:9102",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			if len(tc.config) > 0 {
				t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, tc.config))
			}
			var captured ServeOptions
			opts := defaultRootCommandOptions()
			opts.runServe = func(_ context.Context, _ string, serveOpts ServeOptions) int {
				captured = serveOpts
				return 0
			}

			var stdout, stderr bytes.Buffer
			args := append([]string{"serve"}, tc.args...)
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, opts)
			if code != 0 {
				t.Fatalf("serve code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
			}
			if captured.APIListenAddr != tc.wantAPI {
				t.Fatalf("api listen addr = %q, want %q", captured.APIListenAddr, tc.wantAPI)
			}
			if captured.MCPListenAddr != tc.wantMCP {
				t.Fatalf("mcp listen addr = %q, want %q", captured.MCPListenAddr, tc.wantMCP)
			}
		})
	}
}

func TestCLI_ServeListenAddrEnvConfigValidation(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(t *testing.T)
		wantStderr string
	}{
		{
			name: "retired api environment address",
			setup: func(t *testing.T) {
				t.Setenv("SWARM_API_LISTEN_ADDR", "8081")
			},
			wantStderr: "SWARM_API_LISTEN_ADDR",
		},
		{
			name: "invalid mcp config address",
			setup: func(t *testing.T) {
				t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
					"serve_mcp_listen_addr": "http://127.0.0.1:8082",
				}))
			},
			wantStderr: "--mcp-listen-addr must be a host:port listen address, not a URL",
		},
		{
			name: "bare listener config key stays unsupported",
			setup: func(t *testing.T) {
				configPath := filepath.Join(t.TempDir(), "config.yaml")
				if err := os.WriteFile(configPath, []byte("api_listen_addr: \"127.0.0.1:9101\"\n"), 0o600); err != nil {
					t.Fatalf("write config: %v", err)
				}
				t.Setenv("SWARM_CONFIG", configPath)
			},
			wantStderr: `unknown config key "api_listen_addr"`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			tc.setup(t)
			ran := false
			opts := defaultRootCommandOptions()
			opts.runServe = func(context.Context, string, ServeOptions) int {
				ran = true
				return 0
			}

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve"}, &stdout, &stderr, opts)
			if code != 2 {
				t.Fatalf("serve code = %d, want 2\nstdout=%s\nstderr=%s", code, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("serve stderr missing %q:\n%s", tc.wantStderr, stderr.String())
			}
			if ran {
				t.Fatal("serve runtime started after invalid listener source")
			}
		})
	}
}

func TestResolveServeAPIAuthSourceAuthority(t *testing.T) {
	t.Run("default loopback when no explicit source", func(t *testing.T) {
		isolateCLIAPIConfigEnv(t)
		auth, err := ResolveServeAPIAuth("", DefaultServeOptions())
		if err != nil {
			t.Fatalf("ResolveServeAPIAuth: %v", err)
		}
		if !auth.UsesDefaultLoopbackToken() || !reflect.DeepEqual(auth.Tokens, []string{apiv1.DefaultLoopbackAPIToken}) {
			t.Fatalf("auth = %#v, want built-in loopback default", auth)
		}
	})

	t.Run("flag token file wins over config", func(t *testing.T) {
		isolateCLIAPIConfigEnv(t)
		configTokenFile := writeCLIAPITokenFile(t, "config-token")
		flagTokenFile := writeCLIAPITokenFile(t, "flag-token")
		t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
			"serve_api_token_file": configTokenFile,
		}))
		auth, err := ResolveServeAPIAuth("", ServeOptions{APITokenFile: flagTokenFile, APITokenFileFlagSet: true})
		if err != nil {
			t.Fatalf("ResolveServeAPIAuth: %v", err)
		}
		if got := auth.Tokens; !reflect.DeepEqual(got, []string{"flag-token"}) {
			t.Fatalf("tokens = %#v, want flag token", got)
		}
		if auth.Source != apiv1.AuthTokenSource(serveAPITokenFileFlagSource) || !auth.Explicit || auth.TokenFile != flagTokenFile {
			t.Fatalf("auth = %#v, want flag token-file source", auth)
		}
	})

	t.Run("config token file used when flag absent", func(t *testing.T) {
		isolateCLIAPIConfigEnv(t)
		configTokenFile := writeCLIAPITokenFile(t, "config-token")
		t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
			"serve_api_token_file": configTokenFile,
		}))
		auth, err := ResolveServeAPIAuth("", DefaultServeOptions())
		if err != nil {
			t.Fatalf("ResolveServeAPIAuth: %v", err)
		}
		if got := auth.Tokens; !reflect.DeepEqual(got, []string{"config-token"}) {
			t.Fatalf("tokens = %#v, want config token", got)
		}
		if auth.Source != apiv1.AuthTokenSource(serveAPITokenFileConfigSource) || !auth.Explicit || auth.TokenFile != configTokenFile {
			t.Fatalf("auth = %#v, want config token-file source", auth)
		}
	})

	t.Run("raw env source rejected even with token file", func(t *testing.T) {
		isolateCLIAPIConfigEnv(t)
		tokenFile := writeCLIAPITokenFile(t, "flag-token")
		t.Setenv("SWARM_API_TOKEN", "env-token")
		_, err := ResolveServeAPIAuth("", ServeOptions{APITokenFile: tokenFile, APITokenFileFlagSet: true})
		if err == nil || !strings.Contains(err.Error(), "server-side API environment source is no longer accepted") || !strings.Contains(err.Error(), "serve.api_token_file") {
			t.Fatalf("err = %v, want removed-env diagnostic", err)
		}
	})

	t.Run("blank and missing token files fail closed", func(t *testing.T) {
		isolateCLIAPIConfigEnv(t)
		blank := writeCLIAPITokenFile(t, "  \n")
		if _, err := ResolveServeAPIAuth("", ServeOptions{APITokenFile: blank, APITokenFileFlagSet: true}); err == nil || !strings.Contains(err.Error(), "--api-token-file is blank") {
			t.Fatalf("blank token err = %v, want blank token failure", err)
		}
		missing := filepath.Join(t.TempDir(), "missing-token")
		if _, err := ResolveServeAPIAuth("", ServeOptions{APITokenFile: missing, APITokenFileFlagSet: true}); err == nil || !strings.Contains(err.Error(), "read --api-token-file") {
			t.Fatalf("missing token err = %v, want read failure", err)
		}
	})
}

func TestCLI_ServeUnifiedConfigFeedsListenerConfig(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	runtimeConfig := filepath.Join(t.TempDir(), "runtime.yaml")
	if err := os.WriteFile(runtimeConfig, []byte("serve:\n  api_listen_addr: \"127.0.0.1:9999\"\n  mcp_listen_addr: \"127.0.0.1:9998\"\n"), 0o600); err != nil {
		t.Fatalf("write runtime config: %v", err)
	}

	var captured ServeOptions
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ string, serveOpts ServeOptions) int {
		captured = serveOpts
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", "--config", runtimeConfig}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("serve code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.ConfigPath != runtimeConfig {
		t.Fatalf("runtime config path = %q, want %q", captured.ConfigPath, runtimeConfig)
	}
	if captured.APIListenAddr != "127.0.0.1:9999" {
		t.Fatalf("api listen addr = %q, want unified config value", captured.APIListenAddr)
	}
	if captured.MCPListenAddr != "127.0.0.1:9998" {
		t.Fatalf("mcp listen addr = %q, want unified config value", captured.MCPListenAddr)
	}
}

func TestCLI_ServeDataFlagFeedsServeOptions(t *testing.T) {
	dataDir := t.TempDir()
	var captured ServeOptions
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ string, serveOpts ServeOptions) int {
		captured = serveOpts
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", "--data", dataDir}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("serve code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.DataSource != dataDir {
		t.Fatalf("data source = %q, want %q", captured.DataSource, dataDir)
	}
}

func TestCLI_ServeDataFlagRejectsEmptySource(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", "--data", ""}, &stdout, &stderr, defaultRootCommandOptions())
	if code == 0 {
		t.Fatalf("serve code = 0, want failure stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "--data must be non-empty") {
		t.Fatalf("serve stderr = %q, want --data validation error", stderr.String())
	}
}

func TestCLI_ServeWorkspaceBackendFlagFeedsServeOptions(t *testing.T) {
	var captured ServeOptions
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ string, serveOpts ServeOptions) int {
		captured = serveOpts
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", "--workspace-backend", "host"}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("serve code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.WorkspaceBackend != workspace.BackendHost || !captured.WorkspaceBackendSet {
		t.Fatalf("workspace backend opts = backend %q set %v, want host set=true", captured.WorkspaceBackend, captured.WorkspaceBackendSet)
	}
}

func TestCLI_ServeWorkspaceBackendFlagRejectsUnsupportedBackend(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", "--workspace-backend", "none"}, &stdout, &stderr, defaultRootCommandOptions())
	if code == 0 {
		t.Fatalf("serve code = 0, want failure stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "--workspace-backend") || !strings.Contains(stderr.String(), "docker or host") {
		t.Fatalf("serve stderr = %q, want workspace backend validation error", stderr.String())
	}
}

func TestCLI_ServeListenAddrHigherPrecedenceSourcesSkipCLIConfig(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		setup     func(t *testing.T)
		wantAPI   string
		wantMCP   string
		wantRan   bool
		wantError string
	}{
		{
			name: "both flags skip missing explicit cli config",
			args: []string{"serve", "--api-listen-addr", "127.0.0.1:9401", "--mcp-listen-addr", "127.0.0.1:9402"},
			setup: func(t *testing.T) {
				t.Setenv("SWARM_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))
			},
			wantAPI: "127.0.0.1:9401",
			wantMCP: "127.0.0.1:9402",
			wantRan: true,
		},
		{
			name: "retired env vars fail before malformed cli config",
			args: []string{"serve"},
			setup: func(t *testing.T) {
				configPath := filepath.Join(t.TempDir(), "config.yaml")
				if err := os.WriteFile(configPath, []byte("serve_api_listen_addr: [\n"), 0o600); err != nil {
					t.Fatalf("write config: %v", err)
				}
				t.Setenv("SWARM_CONFIG", configPath)
				t.Setenv("SWARM_API_LISTEN_ADDR", "127.0.0.1:9501")
				t.Setenv("SWARM_MCP_LISTEN_ADDR", "127.0.0.1:9502")
			},
			wantError: "SWARM_API_LISTEN_ADDR",
		},
		{
			name: "partial retired env fails closed",
			args: []string{"serve"},
			setup: func(t *testing.T) {
				t.Setenv("SWARM_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))
				t.Setenv("SWARM_API_LISTEN_ADDR", "127.0.0.1:9601")
			},
			wantError: "SWARM_API_LISTEN_ADDR",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			tc.setup(t)
			var captured ServeOptions
			ran := false
			opts := defaultRootCommandOptions()
			opts.runServe = func(_ context.Context, _ string, serveOpts ServeOptions) int {
				captured = serveOpts
				ran = true
				return 0
			}

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, opts)
			if tc.wantError != "" {
				if code != 2 {
					t.Fatalf("serve code = %d, want 2\nstdout=%s\nstderr=%s", code, stdout.String(), stderr.String())
				}
				if !strings.Contains(stderr.String(), tc.wantError) {
					t.Fatalf("serve stderr missing %q:\n%s", tc.wantError, stderr.String())
				}
				if ran {
					t.Fatal("serve runtime started after required CLI config load failed")
				}
				return
			}
			if code != 0 {
				t.Fatalf("serve code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
			}
			if ran != tc.wantRan {
				t.Fatalf("runtime ran = %v, want %v", ran, tc.wantRan)
			}
			if captured.APIListenAddr != tc.wantAPI {
				t.Fatalf("api listen addr = %q, want %q", captured.APIListenAddr, tc.wantAPI)
			}
			if captured.MCPListenAddr != tc.wantMCP {
				t.Fatalf("mcp listen addr = %q, want %q", captured.MCPListenAddr, tc.wantMCP)
			}
		})
	}
}

func TestCLI_ServeDevFlagComposesClosedServeOwners(t *testing.T) {
	var captured ServeOptions
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ string, serveOpts ServeOptions) int {
		captured = serveOpts
		return 0
	}

	var stdout, stderr bytes.Buffer
	wantGrace := 42 * time.Second
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", "--dev", "--shutdown-grace", wantGrace.String()}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("serve code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !captured.Dev {
		t.Fatal("serve dev = false, want true")
	}
	if !captured.AbandonActiveRuns {
		t.Fatal("serve abandon active runs = false, want dev composition")
	}
	if !captured.NoRequireBundleMatch || captured.RequireBundleMatch {
		t.Fatalf("serve bundle match = require:%t no-require:%t, want dev no-require composition", captured.RequireBundleMatch, captured.NoRequireBundleMatch)
	}
	if captured.Verbose {
		t.Fatal("serve verbose = true, want dev and presentation verbosity to remain independent")
	}
	if captured.ShutdownGrace != wantGrace {
		t.Fatalf("serve shutdown grace = %s, want explicit %s", captured.ShutdownGrace, wantGrace)
	}
}

func TestCLI_ServeDevAndExplicitVerboseComposeIndependently(t *testing.T) {
	var captured ServeOptions
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ string, serveOpts ServeOptions) int {
		captured = serveOpts
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", "--dev", "--verbose"}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("serve code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !captured.Dev || !captured.Verbose {
		t.Fatalf("serve dev/verbose = %t/%t, want both explicit modes", captured.Dev, captured.Verbose)
	}
}

func TestCLI_ServeDevRejectsRequireBundleMatchBeforeOwner(t *testing.T) {
	var called atomic.Bool
	opts := defaultRootCommandOptions()
	opts.runServe = func(context.Context, string, ServeOptions) int {
		called.Store(true)
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", "--dev", "--require-bundle-match"}, &stdout, &stderr, opts)
	if code != 2 {
		t.Fatalf("serve code = %d stderr=%s stdout=%s, want 2", code, stderr.String(), stdout.String())
	}
	if !strings.Contains(stderr.String(), "--dev cannot be combined with --require-bundle-match") {
		t.Fatalf("stderr = %q, want dev conflict error", stderr.String())
	}
	if called.Load() {
		t.Fatal("serve owner was called despite dev/require-bundle-match conflict")
	}
}

func TestCLI_ServeDevAcceptsExplicitRequireBundleMatchFalse(t *testing.T) {
	var captured ServeOptions
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ string, serveOpts ServeOptions) int {
		captured = serveOpts
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", "--dev", "--require-bundle-match=false"}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("serve code = %d stderr=%s stdout=%s, want 0", code, stderr.String(), stdout.String())
	}
	if !captured.Dev {
		t.Fatal("serve dev = false, want true")
	}
	if !captured.NoRequireBundleMatch || captured.RequireBundleMatch {
		t.Fatalf("serve bundle match = require:%t no-require:%t, want dev no-require composition", captured.RequireBundleMatch, captured.NoRequireBundleMatch)
	}
}

func TestPlatformSpecServeDevModeCompositionPromoted(t *testing.T) {
	spec := loadServeDevModeSpec(t)
	if !strings.Contains(spec.ImplementedBy, "#830") || !strings.Contains(spec.ImplementedBy, "#2010") {
		t.Fatalf("dev mode implemented_by = %q, want #830/#2010 migration", spec.ImplementedBy)
	}
	if strings.TrimSpace(spec.Flag) != "--dev" {
		t.Fatalf("dev mode flag = %q, want --dev", spec.Flag)
	}
	for _, want := range []string{
		"`--abandon-active-runs`",
		"`--no-require-bundle-match`",
		"without setting `--verbose`",
		"dev entity-container cleanup",
	} {
		if !stringSliceContains(spec.Composition, want) {
			t.Fatalf("dev mode composition missing %q: %#v", want, spec.Composition)
		}
	}
	for _, want := range []string{"workspace", "containeridentity"} {
		if !strings.Contains(spec.Owner, want) {
			t.Fatalf("dev mode owner missing %q:\n%s", want, spec.Owner)
		}
	}
	for _, want := range []string{"--dev --require-bundle-match", "fails before runtime boot"} {
		if !stringSliceContains(spec.ConflictRules, want) {
			t.Fatalf("dev mode conflict rules missing %q: %#v", want, spec.ConflictRules)
		}
	}
	for _, want := range []string{"--dev --require-bundle-match=false", "redundant but valid"} {
		if !stringSliceContains(spec.ConflictRules, want) {
			t.Fatalf("dev mode conflict rules missing %q: %#v", want, spec.ConflictRules)
		}
	}
	for _, want := range []string{"--dev --verbose", "not redundant"} {
		if !stringSliceContains(spec.ConflictRules, want) {
			t.Fatalf("dev mode conflict rules missing %q: %#v", want, spec.ConflictRules)
		}
	}
	for _, want := range []string{"runtime shutdown admission", "Cleanup still runs after a shutdown timeout/error", "joined shutdown and cleanup errors"} {
		if !strings.Contains(spec.ShutdownOrdering, want) {
			t.Fatalf("dev mode shutdown ordering missing %q:\n%s", want, spec.ShutdownOrdering)
		}
	}
	for _, want := range []string{"identity-proven runtime-owned", "`kind=entity`", "MUST NOT infer ownership from names"} {
		if !strings.Contains(spec.CleanupScope, want) {
			t.Fatalf("dev mode cleanup scope missing %q:\n%s", want, spec.CleanupScope)
		}
	}
	for _, want := range []string{"Scaffold/system", "operator-managed", "unlabeled", "`kind=agent`", "`kind=flow`"} {
		if !strings.Contains(spec.PreservationBoundary, want) {
			t.Fatalf("dev mode preservation boundary missing %q:\n%s", want, spec.PreservationBoundary)
		}
	}
}

func TestPlatformSpecServeUnifiedListenerBindContractPromoted(t *testing.T) {
	spec := loadServeUnifiedListenerSpec(t)
	if strings.TrimSpace(spec.ImplementedBy) != "#853" {
		t.Fatalf("listener contract implemented_by = %q, want #853", spec.ImplementedBy)
	}
	if !strings.Contains(spec.SupersededBy, "#992") {
		t.Fatalf("listener contract superseded_by = %q, want #992", spec.SupersededBy)
	}
	if strings.TrimSpace(spec.Flag) != "--health-addr <addr>" {
		t.Fatalf("listener contract flag = %q, want --health-addr <addr>", spec.Flag)
	}
	for _, want := range []string{"historical", "single HTTP listener", "health-specific name", "superseded by `listener_topology_v2_1`", "MUST NOT be accepted"} {
		if !strings.Contains(spec.Semantics, want) {
			t.Fatalf("listener semantics missing %q:\n%s", want, spec.Semantics)
		}
	}
	for _, want := range []string{"/healthz", "/readyz"} {
		if !stringSliceContains(spec.Routes.Always, want) {
			t.Fatalf("listener always routes missing %q: %#v", want, spec.Routes.Always)
		}
	}
	for _, want := range []string{"/v1/rpc", "/v1/ws"} {
		if !stringSliceContains(spec.Routes.WhenAPIHandlerInstalled, want) {
			t.Fatalf("listener API routes missing %q: %#v", want, spec.Routes.WhenAPIHandlerInstalled)
		}
	}
	for _, want := range []string{"/mcp", "/tools/"} {
		if !stringSliceContains(spec.Routes.WhenMCPGatewayInstalled, want) {
			t.Fatalf("listener MCP routes missing %q: %#v", want, spec.Routes.WhenMCPGatewayInstalled)
		}
	}
	for _, want := range []string{"swarm run start --api-port", "consumer", "second bind owner"} {
		if !strings.Contains(spec.ConsumerBoundaries.SwarmRunAPIPort, want) {
			t.Fatalf("api-port boundary missing %q:\n%s", want, spec.ConsumerBoundaries.SwarmRunAPIPort)
		}
	}
	for _, want := range []string{"swarm run start --mcp-port", "fail before API/WS calls", "local foreground MCP listener control"} {
		if !strings.Contains(spec.ConsumerBoundaries.SwarmRunMCPPort, want) {
			t.Fatalf("mcp-port boundary missing %q:\n%s", want, spec.ConsumerBoundaries.SwarmRunMCPPort)
		}
	}
	for _, want := range []string{"--api/--no-api", "--mcp/--no-mcp", "serve --api-port", "serve --mcp-port", "--log-level"} {
		if !stringSliceContains(spec.UnpromotedReviewControls, want) {
			t.Fatalf("unpromoted review controls missing %q: %#v", want, spec.UnpromotedReviewControls)
		}
	}
}

func TestPlatformSpecServeListenerTopologyRuntimeBindingPromoted(t *testing.T) {
	spec := loadServeListenerTopologySpec(t)
	if strings.TrimSpace(spec.PromotedBy) != "#884" {
		t.Fatalf("listener topology promoted_by = %q, want #884", spec.PromotedBy)
	}
	if strings.TrimSpace(spec.RuntimeBindImplementedBy) != "#992" {
		t.Fatalf("listener topology runtime_bind_implemented_by = %q, want #992", spec.RuntimeBindImplementedBy)
	}
	if strings.TrimSpace(spec.EnvConfigPrecedenceImplementedBy) != "#844" {
		t.Fatalf("listener topology env_config_precedence_implemented_by = %q, want #844", spec.EnvConfigPrecedenceImplementedBy)
	}
	if strings.TrimSpace(spec.ServerAPIAuthImplementedBy) != "#1647" {
		t.Fatalf("listener topology server_api_auth_implemented_by = %q, want #1647", spec.ServerAPIAuthImplementedBy)
	}
	if strings.TrimSpace(spec.ImplementationStatus) != "runtime_bind_env_config_and_server_api_auth_implemented_enable_disable_pending" {
		t.Fatalf("listener topology status = %q", spec.ImplementationStatus)
	}
	if spec.Listeners.API.BindFlag != "--api-listen-addr <host:port>" {
		t.Fatalf("api bind flag = %q", spec.Listeners.API.BindFlag)
	}
	if spec.Listeners.MCP.BindFlag != "--mcp-listen-addr <host:port>" {
		t.Fatalf("mcp bind flag = %q", spec.Listeners.MCP.BindFlag)
	}
	if spec.Defaults.APIListenAddr != defaultAPIListenAddr || spec.Listeners.API.DefaultListenAddr != defaultAPIListenAddr {
		t.Fatalf("api default = defaults:%q listener:%q want %q", spec.Defaults.APIListenAddr, spec.Listeners.API.DefaultListenAddr, defaultAPIListenAddr)
	}
	if spec.Defaults.MCPListenAddr != defaultMCPListenAddr || spec.Listeners.MCP.DefaultListenAddr != defaultMCPListenAddr {
		t.Fatalf("mcp default = defaults:%q listener:%q want %q", spec.Defaults.MCPListenAddr, spec.Listeners.MCP.DefaultListenAddr, defaultMCPListenAddr)
	}
	wantSourceOrder := []string{"flag", "unified_config", "built_in_default"}
	if len(spec.SourcePrecedence.SourceOrder) != len(wantSourceOrder) {
		t.Fatalf("listener source order = %#v, want %#v", spec.SourcePrecedence.SourceOrder, wantSourceOrder)
	}
	for i, want := range wantSourceOrder {
		if spec.SourcePrecedence.SourceOrder[i] != want {
			t.Fatalf("listener source order[%d] = %q, want %q", i, spec.SourcePrecedence.SourceOrder[i], want)
		}
	}
	if spec.SourcePrecedence.APIListenAddr.ConfigKey != "serve.api_listen_addr" {
		t.Fatalf("api listener source precedence = %#v", spec.SourcePrecedence.APIListenAddr)
	}
	if spec.SourcePrecedence.MCPListenAddr.ConfigKey != "serve.mcp_listen_addr" {
		t.Fatalf("mcp listener source precedence = %#v", spec.SourcePrecedence.MCPListenAddr)
	}
	if spec.SourcePrecedence.ServerAPIAuth.AcceptedSources["flag_file"] != "--api-token-file <path>" {
		t.Fatalf("server api auth flag source = %#v", spec.SourcePrecedence.ServerAPIAuth.AcceptedSources)
	}
	if spec.SourcePrecedence.ServerAPIAuth.AcceptedSources["config_file_key"] != "serve.api_token_file" {
		t.Fatalf("server api auth config source = %#v", spec.SourcePrecedence.ServerAPIAuth.AcceptedSources)
	}
	wantServeAuthOrder := []string{"--api-token-file", "config serve.api_token_file", "built-in loopback default"}
	if !reflect.DeepEqual(spec.SourcePrecedence.ServerAPIAuth.SourceOrder, wantServeAuthOrder) {
		t.Fatalf("server api auth source order = %#v, want %#v", spec.SourcePrecedence.ServerAPIAuth.SourceOrder, wantServeAuthOrder)
	}
	for key, want := range map[string]string{
		"SWARM_API_TOKEN":                  "#1647",
		"SWARM_API_TOKEN_FILE":             "Not promoted",
		"config connection.api_token_file": "Client-side API auth only",
		"config api_token":                 "Inline bearer tokens",
	} {
		if !strings.Contains(spec.SourcePrecedence.ServerAPIAuth.RejectedSources[key], want) {
			t.Fatalf("server api auth rejected source %q missing %q:\n%s", key, want, spec.SourcePrecedence.ServerAPIAuth.RejectedSources[key])
		}
	}
	for _, want := range []string{"Missing", "blank", "MUST NOT fall back"} {
		if !strings.Contains(spec.SourcePrecedence.ServerAPIAuth.TokenFileRule, want) {
			t.Fatalf("server token_file_rule missing %q:\n%s", want, spec.SourcePrecedence.ServerAPIAuth.TokenFileRule)
		}
	}
	for _, want := range []string{"--api-token-file", "serve.api_token_file"} {
		if !strings.Contains(spec.SourcePrecedence.APIAuthCouplingRule, want) {
			t.Fatalf("api auth coupling rule missing %q:\n%s", want, spec.SourcePrecedence.APIAuthCouplingRule)
		}
		if !strings.Contains(spec.InteractionRules.APIDefaultTokenExposure.Rule, want) {
			t.Fatalf("api default token exposure rule missing %q:\n%s", want, spec.InteractionRules.APIDefaultTokenExposure.Rule)
		}
	}
	if strings.Contains(spec.SourcePrecedence.APIAuthCouplingRule, "explicit `SWARM_API_TOKEN`") {
		t.Fatalf("api auth coupling still names explicit SWARM_API_TOKEN:\n%s", spec.SourcePrecedence.APIAuthCouplingRule)
	}
	for _, key := range []string{"SWARM_API_PORT", "SWARM_MCP_PORT", "api_listen_addr", "mcp_listen_addr"} {
		if strings.TrimSpace(spec.SourcePrecedence.RejectedSources[key]) == "" {
			t.Fatalf("listener source precedence rejected source %q missing: %#v", key, spec.SourcePrecedence.RejectedSources)
		}
	}
	for _, want := range []string{"/healthz", "/readyz", "/v1/rpc", "/v1/ws"} {
		if !stringSliceContains(spec.Listeners.API.Routes, want) {
			t.Fatalf("api routes missing %q: %#v", want, spec.Listeners.API.Routes)
		}
	}
	for _, want := range []string{"/mcp", "/tools/"} {
		if !stringSliceContains(spec.Listeners.MCP.Routes, want) {
			t.Fatalf("mcp routes missing %q: %#v", want, spec.Listeners.MCP.Routes)
		}
	}
	for _, want := range []string{"#992 implements", "`--health-addr` retirement", "`swarm run start --mcp-port` remains fail-closed", "#1891 implements `swarm serve` listener source precedence"} {
		if !stringSliceContains(spec.ImplementationBoundaries, want) {
			t.Fatalf("implementation boundaries missing %q: %#v", want, spec.ImplementationBoundaries)
		}
	}
}

func TestPlatformSpecCLIAPIConnectionAuthConfigPrecedencePromoted(t *testing.T) {
	spec := loadCLIAPIConnectionAuthConfigSpec(t)
	platformSpecData, err := os.ReadFile(filepath.Join(RepoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	platformSpecText := string(platformSpecData)
	for _, stale := range []string{
		"Runtime-state commands use v1 bearer auth through `SWARM_API_TOKEN`",
		"SWARM_API_TOKEN is the only user-facing API bearer-token source",
		"API token source is required.",
	} {
		if strings.Contains(platformSpecText, stale) {
			t.Fatalf("platform spec still contains stale SWARM_API_TOKEN-only bearer-token authority %q", stale)
		}
	}
	for _, want := range []string{
		"User-facing API bearer-token sources are governed by",
		"cli_specification.foundations.api_connection_auth_config_precedence",
		"`SWARM_BUILDER_AUTH_TOKEN` and `SWARM_OPERATOR_AUTH_TOKEN` fallback is invalid",
	} {
		if !strings.Contains(platformSpecText, want) {
			t.Fatalf("platform spec missing auth-boundary repair proof %q", want)
		}
	}
	if strings.TrimSpace(spec.PromotedBy) != "#844" {
		t.Fatalf("api connection/auth/config promoted_by = %q, want #844", spec.PromotedBy)
	}
	if strings.TrimSpace(spec.ClientEnvRemovedBy) != "#1636" {
		t.Fatalf("api connection/auth/config client_env_removed_by = %q, want #1636", spec.ClientEnvRemovedBy)
	}
	if strings.TrimSpace(spec.ImplementationStatus) != "implemented_loopback_default_no_client_env_sources" {
		t.Fatalf("api connection/auth/config implementation_status = %q, want implemented_loopback_default_no_client_env_sources", spec.ImplementationStatus)
	}
	if !strings.Contains(spec.CanonicalOwner, "cli_specification.foundations.api_connection_auth_config_precedence") {
		t.Fatalf("canonical owner does not point at promoted section: %s", spec.CanonicalOwner)
	}
	for _, want := range []string{"API-backed command leaves consume", "OpenRPC", "unified `swarm.yaml` connection config"} {
		if !strings.Contains(spec.Scope, want) {
			t.Fatalf("scope missing boundary %q:\n%s", want, spec.Scope)
		}
	}
	wantPrecedence := []string{"flag", "context_descriptor", "config_file", "built_in_default"}
	if len(spec.PrecedenceOrder) != len(wantPrecedence) {
		t.Fatalf("precedence order = %#v, want %#v", spec.PrecedenceOrder, wantPrecedence)
	}
	for i, want := range wantPrecedence {
		if spec.PrecedenceOrder[i] != want {
			t.Fatalf("precedence order[%d] = %q, want %q", i, spec.PrecedenceOrder[i], want)
		}
	}
	if spec.APIServer.AcceptedSources.Flag != "--api-server <url>" {
		t.Fatalf("api_server flag = %q, want --api-server <url>", spec.APIServer.AcceptedSources.Flag)
	}
	if !strings.Contains(spec.APIServer.AcceptedSources.ContextDescriptor, "context descriptor") {
		t.Fatalf("api_server context_descriptor = %q, want context descriptor source", spec.APIServer.AcceptedSources.ContextDescriptor)
	}
	if spec.APIServer.AcceptedSources.ConfigKey != "connection.api_server" {
		t.Fatalf("api_server config key = %q, want connection.api_server", spec.APIServer.AcceptedSources.ConfigKey)
	}
	if spec.APIServer.AcceptedSources.BuiltInDefault != "http://127.0.0.1:8081" {
		t.Fatalf("api_server default = %q, want http://127.0.0.1:8081", spec.APIServer.AcceptedSources.BuiltInDefault)
	}
	if !strings.Contains(spec.APIServer.RejectedSources["SWARM_API_SERVER"], "#1636") {
		t.Fatalf("api_server SWARM_API_SERVER rejection missing #1636:\n%s", spec.APIServer.RejectedSources["SWARM_API_SERVER"])
	}
	for _, want := range []string{"base URL", "`/v1/rpc`", "`/v1/ws`"} {
		if !strings.Contains(spec.APIServer.ValueModel, want) {
			t.Fatalf("api_server value model missing %q:\n%s", want, spec.APIServer.ValueModel)
		}
	}
	for _, want := range []string{"base URL", "127.0.0.1"} {
		if !strings.Contains(spec.APIServer.Rationale, want) {
			t.Fatalf("api_server rationale missing %q:\n%s", want, spec.APIServer.Rationale)
		}
	}
	for _, want := range []string{"flag_file", "context_descriptor", "config_file_key"} {
		if strings.TrimSpace(spec.APIToken.AcceptedSources[want]) == "" {
			t.Fatalf("api_token accepted sources missing %q: %#v", want, spec.APIToken.AcceptedSources)
		}
	}
	for _, removed := range []string{"environment_token", "environment_file"} {
		if strings.TrimSpace(spec.APIToken.AcceptedSources[removed]) != "" {
			t.Fatalf("api_token accepted source %q should be removed: %#v", removed, spec.APIToken.AcceptedSources)
		}
	}
	wantTokenSourceOrder := []string{"--api-token-file", "context descriptor auth", "config connection.api_token_file", "built-in loopback default"}
	if len(spec.APIToken.SourceOrder) != len(wantTokenSourceOrder) {
		t.Fatalf("api_token source order = %#v, want %#v", spec.APIToken.SourceOrder, wantTokenSourceOrder)
	}
	for i, want := range wantTokenSourceOrder {
		if spec.APIToken.SourceOrder[i] != want {
			t.Fatalf("api_token source_order[%d] = %q, want %q", i, spec.APIToken.SourceOrder[i], want)
		}
	}
	for key, want := range map[string]string{
		"--api-token":          "shell history",
		"config api_token":     "inline",
		"SWARM_API_TOKEN":      "#1636",
		"SWARM_API_TOKEN_FILE": "config `connection.api_token_file`",
	} {
		if !strings.Contains(spec.APIToken.RejectedSources[key], want) {
			t.Fatalf("api_token rejected source %q missing %q:\n%s", key, want, spec.APIToken.RejectedSources[key])
		}
	}
	if spec.APIToken.BuiltInLoopbackDefault.TokenValue != apiv1.DefaultLoopbackAPIToken {
		t.Fatalf("built-in default token = %q, want %q", spec.APIToken.BuiltInLoopbackDefault.TokenValue, apiv1.DefaultLoopbackAPIToken)
	}
	for _, want := range []string{"127.0.0.0/8", "::1"} {
		if !stringSliceContains(spec.APIToken.BuiltInLoopbackDefault.AllowedTargetHosts, want) {
			t.Fatalf("built-in default allowed hosts missing %q: %#v", want, spec.APIToken.BuiltInLoopbackDefault.AllowedTargetHosts)
		}
	}
	for _, want := range []string{"localhost", "0.0.0.0"} {
		if !stringSliceContains(spec.APIToken.BuiltInLoopbackDefault.RejectedWithoutExplicitToken, want) {
			t.Fatalf("built-in default rejected hosts missing %q: %#v", want, spec.APIToken.BuiltInLoopbackDefault.RejectedWithoutExplicitToken)
		}
	}
	for _, stale := range []string{"SWARM_API_TOKEN", "SWARM_API_TOKEN_FILE"} {
		if strings.Contains(spec.APIToken.BuiltInLoopbackDefault.AppliesWhen, stale) {
			t.Fatalf("built-in default applies_when still names removed client env %q:\n%s", stale, spec.APIToken.BuiltInLoopbackDefault.AppliesWhen)
		}
	}
	if !strings.Contains(spec.APIToken.BuiltInLoopbackDefault.AppliesWhen, "numeric loopback") {
		t.Fatalf("built-in default applies_when missing loopback boundary:\n%s", spec.APIToken.BuiltInLoopbackDefault.AppliesWhen)
	}
	for _, want := range []string{"no-auth", "Authorization: Bearer"} {
		if !strings.Contains(spec.APIToken.BuiltInLoopbackDefault.NoAuthBypassRule, want) {
			t.Fatalf("built-in default no-auth rule missing %q:\n%s", want, spec.APIToken.BuiltInLoopbackDefault.NoAuthBypassRule)
		}
	}
	for key, want := range map[string]string{
		"flag":        "--config <path>",
		"environment": "SWARM_CONFIG",
		"xdg_default": "swarm/swarm.yaml",
	} {
		if !strings.Contains(spec.CLIConfigFile.AcceptedSources[key], want) {
			t.Fatalf("cli config accepted source %q missing %q:\n%s", key, want, spec.CLIConfigFile.AcceptedSources[key])
		}
	}
	for key, want := range map[string]string{
		"${XDG_CONFIG_HOME:-$HOME/.config}/swarm/config.yaml": "retired",
		"executable_adjacent_config.yaml":                     "retired",
	} {
		if !strings.Contains(spec.CLIConfigFile.RejectedSources[key], want) {
			t.Fatalf("cli config rejected source %q missing %q:\n%s", key, want, spec.CLIConfigFile.RejectedSources[key])
		}
	}
	for _, key := range []string{"connection.api_server", "connection.api_token_file"} {
		if strings.TrimSpace(spec.CLIConfigFile.AcceptedKeys[key]) == "" {
			t.Fatalf("cli config accepted key %q missing: %#v", key, spec.CLIConfigFile.AcceptedKeys)
		}
	}
	for _, key := range []string{"api_token", "output_format", "no_color", "log_level", "retry"} {
		if strings.TrimSpace(spec.CLIConfigFile.RejectedKeys[key]) == "" {
			t.Fatalf("cli config rejected key %q missing: %#v", key, spec.CLIConfigFile.RejectedKeys)
		}
	}
	for _, key := range []string{"paths.contracts_path", "paths.platform_spec_path"} {
		if !strings.Contains(spec.CLIConfigFile.SharedNonAPIKeys[key], "contract_platform_spec_path_resolution") {
			t.Fatalf("cli config shared non-API key %q missing contract path owner: %#v", key, spec.CLIConfigFile.SharedNonAPIKeys)
		}
	}
	for _, key := range []string{"serve.api_listen_addr", "serve.mcp_listen_addr"} {
		if !strings.Contains(spec.CLIConfigFile.SharedNonAPIKeys[key], "listener_topology_v2_1.source_precedence") {
			t.Fatalf("cli config shared non-API key %q missing listener source owner: %#v", key, spec.CLIConfigFile.SharedNonAPIKeys)
		}
	}
	if !strings.Contains(spec.CLIConfigFile.SharedNonAPIKeys["serve.api_token_file"], "server_api_auth") ||
		!strings.Contains(spec.CLIConfigFile.SharedNonAPIKeys["serve.api_token_file"], "server-side `swarm serve` auth only") ||
		!strings.Contains(spec.CLIConfigFile.SharedNonAPIKeys["serve.api_token_file"], "MUST NOT") {
		t.Fatalf("cli config serve.api_token_file missing server/client boundary: %#v", spec.CLIConfigFile.SharedNonAPIKeys["serve.api_token_file"])
	}
	for _, key := range []string{"SWARM_API_PORT", "SWARM_MCP_PORT"} {
		if !strings.Contains(spec.ServeListenerEnvConfigBoundary.RejectedPorts[key], "Not promoted") {
			t.Fatalf("serve listener rejected port %q missing not-promoted rule:\n%s", key, spec.ServeListenerEnvConfigBoundary.RejectedPorts[key])
		}
	}
	for _, key := range []string{"SWARM_API_LISTEN_ADDR", "SWARM_MCP_LISTEN_ADDR"} {
		if !strings.Contains(spec.ServeListenerEnvConfigBoundary.RejectedListenerEnvironment[key], "Retired by #1891") {
			t.Fatalf("serve listener rejected env %q missing retirement rule:\n%s", key, spec.ServeListenerEnvConfigBoundary.RejectedListenerEnvironment[key])
		}
	}
	for _, want := range []string{"#848", "#884/#750", "#743", "#1636", "#1647", "`--no-retry`"} {
		if !stringSliceContains(spec.SplitSiblings, want) {
			t.Fatalf("split siblings missing %q: %#v", want, spec.SplitSiblings)
		}
	}
	for _, want := range []string{"API-backed command leaves consume", "SWARM_API_SERVER", "#1647", "OpenRPC"} {
		if !strings.Contains(strings.Join(spec.ImplementationBoundaries, "\n"), want) {
			t.Fatalf("implementation boundaries missing %q: %#v", want, spec.ImplementationBoundaries)
		}
	}
}

func TestRootNoAssetCommandsDoNotRequireRepoRoot(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "bare root help", args: nil, want: "Usage:"},
		{name: "root help", args: []string{"--help"}, want: "Usage:"},
		{name: "version", args: []string{"version"}, want: "Swarm dev"},
		{name: "completion", args: []string{"completion", "bash"}, want: "swarm"},
		{name: "serve help", args: []string{"serve", "--help"}, want: "Start the Swarm runtime"},
		{name: "verify help", args: []string{"verify", "--help"}, want: "Validate contract files before boot"},
		{name: "run help", args: []string{"run", "start", "--help"}, want: "Start a workflow run on a running runtime"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			chdirForTest(t, t.TempDir())

			var stdout, stderr bytes.Buffer
			code := executeRootCommand(context.Background(), "", tc.args, &stdout, &stderr)
			if code != 0 {
				t.Fatalf("code = %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
			}
			if !strings.Contains(stdout.String(), tc.want) {
				t.Fatalf("stdout missing %q:\n%s", tc.want, stdout.String())
			}
			if strings.Contains(stderr.String(), "locate repo root") {
				t.Fatalf("stderr leaked repo root discovery failure: %q", stderr.String())
			}
		})
	}
}

func TestAssetCommandsDiscoverRepoRootAtExecution(t *testing.T) {
	repo := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(repo, "go.mod"), "module testrepo\n")
	chdirForTest(t, repo)

	var capturedRepo string
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, repo string, _ ServeOptions) int {
		capturedRepo = repo
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), "", []string{"serve"}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("serve code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if capturedRepo != repo {
		t.Fatalf("serve repo = %q, want discovered repo %q", capturedRepo, repo)
	}
}

func TestVerifyCommandIgnoresRepoDotEnvAfterLazyRepoDiscovery(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	_ = os.Unsetenv("SWARM_CONTRACTS_PATH")
	repo := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(repo, "go.mod"), "module testrepo\n")
	contractsRoot := writeEnvAuthorityContractsFixture(t, "dot-env-contracts")
	configPath := writeTestVerifyRuntimeConfig(t)
	writeWorkflowValidationFixtureFile(t, filepath.Join(repo, ".env"), "SWARM_CONTRACTS_PATH="+contractsRoot+"\nBROKEN\n")
	chdirForTest(t, repo)

	var stdout, stderr bytes.Buffer
	code := executeRootCommand(context.Background(), "", []string{"verify", "--config", configPath}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("verify unexpectedly consumed contracts path from repo .env: stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String()+stderr.String(), contractsRoot) {
		t.Fatalf("verify output referenced repo .env contracts path stdout=%s stderr=%s", stdout.String(), stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = executeRootCommand(context.Background(), "", []string{"verify", "--contracts", contractsRoot, "--config", configPath}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("verify with explicit contracts code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !strings.Contains(stdout.String(), "verify ok: contracts="+contractsRoot) {
		t.Fatalf("verify explicit contracts output missing success marker:\n%s", stdout.String())
	}
}

func TestLocalRunDiscoversRepoRootWithoutLoadingDotEnv(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	_ = os.Unsetenv("SWARM_API_TOKEN")
	repo := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(repo, "go.mod"), "module testrepo\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(repo, ".env"), "SWARM_API_TOKEN=test-token\nBROKEN\n")
	payloadPath := filepath.Join(t.TempDir(), "payload.json")
	writeWorkflowValidationFixtureFile(t, payloadPath, "{}\n")
	chdirForTest(t, repo)

	var capturedRepo string
	opts := runCommandOptions{
		apiOptions:   defaultRootCommandOptions(),
		eventName:    "scan.requested",
		payloadPath:  payloadPath,
		changedFlags: map[string]bool{},
	}
	opts.apiOptions.runServe = func(ctx context.Context, repo string, _ ServeOptions) int {
		capturedRepo = repo
		<-ctx.Done()
		return 1
	}
	opts.apiOptions.runReadyTimeout = time.Millisecond
	opts.apiOptions.runReadyPoll = time.Millisecond

	var stdout, stderr bytes.Buffer
	_ = runRunCommand(context.Background(), "", &stdout, &stderr, opts)
	if capturedRepo != repo {
		t.Fatalf("local run serve repo = %q, want discovered repo %q; stderr=%s stdout=%s", capturedRepo, repo, stderr.String(), stdout.String())
	}
	if got := os.Getenv("SWARM_API_TOKEN"); got != "" {
		t.Fatalf("local run loaded SWARM_API_TOKEN from repo .env: %q", got)
	}
}

func TestRunVerifyCommandUsesEmbeddedPlatformSpecWithoutRepoRoot(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	chdirForTest(t, t.TempDir())
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: embedded-platform-spec
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: embedded-platform-spec\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")

	var buf bytes.Buffer
	code := runVerifyCommandWithContractsForTest(t, context.Background(), "", root, &buf)
	if code != 0 {
		t.Fatalf("runVerifyCommand exit code = %d, output = %q", code, buf.String())
	}
	if !strings.Contains(buf.String(), "verify ok: contracts=") {
		t.Fatalf("verify output missing success marker:\n%s", buf.String())
	}
}

func TestRunVerifyCommandFailsClosedForIncompatiblePlatformVersion(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	chdirForTest(t, t.TempDir())
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: incompatible-platform-spec
version: "1.0.0"
platform_version: ">=0.8.0"
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: incompatible-platform-spec\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")

	var buf bytes.Buffer
	code := runVerifyCommandWithContractsForTest(t, context.Background(), "", root, &buf)
	if code == 0 {
		t.Fatalf("runVerifyCommand exit code = 0, output = %q", buf.String())
	}
	for _, want := range []string{
		"platform_version_compatibility",
		`platform_version range ">=0.8.0" does not include running platform "0.7.0"`,
		"remediation: update package.yaml platform_version after re-verifying",
	} {
		if !strings.Contains(buf.String(), want) {
			t.Fatalf("verify output missing %q:\n%s", want, buf.String())
		}
	}
}

func TestRunVerifyCommandAcceptsPackageManifestSelfFacts(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	chdirForTest(t, t.TempDir())
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: package-self-facts-verify
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
keywords: [dedup-index, catalog]
license: MIT
repository: https://github.com/division-sh/swarm
extra:
  colony.division.sh/display_name: Package Self Facts Verify
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: package-self-facts-verify\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")

	var buf bytes.Buffer
	code := runVerifyCommandWithContractsForTest(t, context.Background(), "", root, &buf)
	if code != 0 {
		t.Fatalf("runVerifyCommand exit code = %d, output = %q", code, buf.String())
	}
	if !strings.Contains(buf.String(), "verify ok: contracts=") {
		t.Fatalf("verify output missing success marker:\n%s", buf.String())
	}
}

func TestRunVerifyCommandFailsClosedForUnknownPackageManifestField(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	chdirForTest(t, t.TempDir())
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: package-unknown-field-verify
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
homepage: https://division.sh
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: package-unknown-field-verify\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")

	var buf bytes.Buffer
	code := runVerifyCommandWithContractsForTest(t, context.Background(), "", root, &buf)
	if code == 0 {
		t.Fatalf("runVerifyCommand exit code = 0, output = %q", buf.String())
	}
	for _, want := range []string{"ERROR: package.yaml field \"homepage\" is not supported.", "Valid options:", "Remediation:"} {
		if !strings.Contains(buf.String(), want) {
			t.Fatalf("verify output missing %q:\n%s", want, buf.String())
		}
	}
}

func TestConfiguredWorkspaceLifecycleDoesNotInventSourceRootDataSource(t *testing.T) {
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}

	manager, err := configuredWorkspaceLifecycle(nil, nil, contractsDir, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), WorkspaceMountSources{})
	if err != nil {
		t.Fatalf("configuredWorkspaceLifecycle: %v", err)
	}
	err = manager.ValidateSource(context.Background(), semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
	if err == nil || !strings.Contains(err.Error(), "/data source is not configured") {
		t.Fatalf("ValidateSource error = %v, want explicit /data source requirement", err)
	}
}

func TestConfiguredWorkspaceLifecycleUsesExplicitDataAndContractsSources(t *testing.T) {
	dataDir := t.TempDir()
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}

	manager, err := configuredWorkspaceLifecycle(nil, nil, contractsDir, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), WorkspaceMountSources{
		DataSource:       dataDir,
		DataSourceSource: "--data",
	})
	if err != nil {
		t.Fatalf("configuredWorkspaceLifecycle: %v", err)
	}
	if err := manager.ValidateSource(context.Background(), semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})); err != nil {
		t.Fatalf("ValidateSource: %v", err)
	}
}

func TestConfiguredWorkspaceLifecycleFailsClosedForUnreadableDataSource(t *testing.T) {
	missingDataDir := filepath.Join(t.TempDir(), "missing-data")
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}

	manager, err := configuredWorkspaceLifecycle(nil, nil, contractsDir, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), WorkspaceMountSources{
		DataSource:       missingDataDir,
		DataSourceSource: "--data",
	})
	if err != nil {
		t.Fatalf("configuredWorkspaceLifecycle: %v", err)
	}
	err = manager.ValidateSource(context.Background(), semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
	if err == nil || !strings.Contains(err.Error(), "/data source") || !strings.Contains(err.Error(), missingDataDir) {
		t.Fatalf("ValidateSource error = %v, want missing explicit /data source", err)
	}
}

func TestConfiguredWorkspaceLifecycleRejectsExplicitDataSourceWithVolumesFrom(t *testing.T) {
	cfg := &config.Config{Workspace: config.WorkspaceConfig{VolumesFrom: "swarm-orchestrator"}}
	_, err := configuredWorkspaceLifecycle(nil, cfg, t.TempDir(), semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), WorkspaceMountSources{
		DataSource:       t.TempDir(),
		DataSourceSource: "workspace.data_source",
	})
	if err == nil || !strings.Contains(err.Error(), "cannot be combined with workspace.volumes_from") {
		t.Fatalf("configuredWorkspaceLifecycle error = %v, want volumes-from conflict", err)
	}
}

func TestConfiguredWorkspaceLifecycleForBackendSelectsHostWithoutDocker(t *testing.T) {
	cfg := &config.Config{Workspace: config.WorkspaceConfig{HostRoot: filepath.Join(t.TempDir(), "host-workspaces")}}
	dataDir := t.TempDir()
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}
	lifecycle, err := ConfiguredWorkspaceLifecycleForBackend(nil, cfg, contractsDir, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), WorkspaceMountSources{
		DataSource:       dataDir,
		DataSourceSource: "--data",
	}, WorkspaceBackendSelection{Backend: workspace.BackendHost, Source: "--workspace-backend"})
	if err != nil {
		t.Fatalf("ConfiguredWorkspaceLifecycleForBackend: %v", err)
	}
	manager, ok := lifecycle.(*workspace.HostManager)
	if !ok {
		t.Fatalf("lifecycle = %T, want *workspace.HostManager", lifecycle)
	}
	if err := manager.ValidateSource(context.Background(), semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})); err != nil {
		t.Fatalf("ValidateSource: %v", err)
	}
	if err := manager.EnsureSystemWorkspaces(context.Background()); err != nil {
		t.Fatalf("EnsureSystemWorkspaces: %v", err)
	}
}

func TestConfiguredWorkspaceLifecycleForBackendRejectsHostVolumesFrom(t *testing.T) {
	cfg := &config.Config{Workspace: config.WorkspaceConfig{VolumesFrom: "swarm-orchestrator"}}
	_, err := ConfiguredWorkspaceLifecycleForBackend(nil, cfg, t.TempDir(), semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), WorkspaceMountSources{
		DataSource:       t.TempDir(),
		DataSourceSource: "--data",
	}, WorkspaceBackendSelection{Backend: workspace.BackendHost, Source: "--workspace-backend"})
	if err == nil || !strings.Contains(err.Error(), "host workspace backend cannot consume workspace.volumes_from") {
		t.Fatalf("ConfiguredWorkspaceLifecycleForBackend error = %v, want host volumes-from rejection", err)
	}
}

func TestResolveWorkspaceMountSourcesPrecedence(t *testing.T) {
	RepoRoot := t.TempDir()
	flagDir := filepath.Join(RepoRoot, "flag-data")
	configDir := filepath.Join(RepoRoot, "config-data")
	for _, dir := range []string{flagDir, configDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}

	result, err := resolveWorkspaceMountSourcesFromInput(workspaceDataSourceInput{
		RepoRoot:         RepoRoot,
		FlagDataSource:   "flag-data",
		ConfigDataSource: "config-data",
	})
	if err != nil {
		t.Fatalf("resolve workspace mount sources: %v", err)
	}
	if result.DataSource != flagDir || result.DataSourceSource != "--data" {
		t.Fatalf("flag precedence result = %#v, want source %q from --data", result, flagDir)
	}

	result, err = resolveWorkspaceMountSourcesFromInput(workspaceDataSourceInput{
		RepoRoot:            RepoRoot,
		ConfigDataSource:    "config-data",
		ConfigDataSourceSet: true,
	})
	if err != nil {
		t.Fatalf("resolve config workspace mount source: %v", err)
	}
	if result.DataSource != configDir || result.DataSourceSource != "workspace.data_source" {
		t.Fatalf("config precedence result = %#v, want source %q from workspace.data_source", result, configDir)
	}

	result, err = resolveWorkspaceMountSourcesFromInput(workspaceDataSourceInput{
		RepoRoot:                RepoRoot,
		DefaultDataSource:       filepath.Join(RepoRoot, defaultWorkspaceDataSourceRelativePath),
		DefaultDataSourceSource: defaultWorkspaceDataSourceSource,
		CreateDefaultDataSource: true,
	})
	if err != nil {
		t.Fatalf("resolve default workspace mount source: %v", err)
	}
	defaultDir := filepath.Join(RepoRoot, defaultWorkspaceDataSourceRelativePath)
	if result.DataSource != defaultDir || result.DataSourceSource != defaultWorkspaceDataSourceSource {
		t.Fatalf("default result = %#v, want source %q from %s", result, defaultDir, defaultWorkspaceDataSourceSource)
	}
	if info, err := os.Stat(defaultDir); err != nil || !info.IsDir() {
		t.Fatalf("default data source stat = (%v, %v), want created directory", info, err)
	}
}

func TestResolveWorkspaceMountSourcesRejectsEmptyConfigBeforeAlternateOrDefault(t *testing.T) {
	RepoRoot := t.TempDir()
	result, err := resolveWorkspaceMountSourcesFromInput(workspaceDataSourceInput{
		RepoRoot:                RepoRoot,
		ConfigDataSource:        " \t ",
		ConfigDataSourceSet:     true,
		VolumesFrom:             "swarm-orchestrator",
		VolumesFromSet:          true,
		DefaultDataSource:       filepath.Join(RepoRoot, defaultWorkspaceDataSourceRelativePath),
		DefaultDataSourceSource: defaultWorkspaceDataSourceSource,
		CreateDefaultDataSource: true,
	})
	if err == nil || !strings.Contains(err.Error(), "workspace.data_source") || !strings.Contains(err.Error(), "must be non-empty") {
		t.Fatalf("resolve workspace mount sources error = %v, want empty workspace.data_source rejection", err)
	}
	if result.DataSource != "" || result.DataSourceSource != "workspace.data_source" {
		t.Fatalf("workspace mount sources = %#v, want no alternate/default fallback and workspace.data_source source label", result)
	}
	if _, err := os.Stat(filepath.Join(RepoRoot, defaultWorkspaceDataSourceRelativePath)); !os.IsNotExist(err) {
		t.Fatalf("default data source stat error = %v, want not created", err)
	}
}

func TestResolveWorkspaceMountSourcesDefaultsToSwarmDataNotRepoData(t *testing.T) {
	RepoRoot := t.TempDir()
	repoDataDir := filepath.Join(RepoRoot, "data")
	if err := os.MkdirAll(repoDataDir, 0o755); err != nil {
		t.Fatalf("mkdir repo data: %v", err)
	}
	result, err := resolveWorkspaceMountSourcesFromInput(workspaceDataSourceInput{
		RepoRoot:                RepoRoot,
		DefaultDataSource:       filepath.Join(RepoRoot, defaultWorkspaceDataSourceRelativePath),
		DefaultDataSourceSource: defaultWorkspaceDataSourceSource,
		CreateDefaultDataSource: true,
	})
	if err != nil {
		t.Fatalf("resolve workspace mount sources: %v", err)
	}
	defaultDir := filepath.Join(RepoRoot, defaultWorkspaceDataSourceRelativePath)
	if result.DataSource != defaultDir || result.DataSourceSource != defaultWorkspaceDataSourceSource {
		t.Fatalf("workspace mount sources = %#v, want default %q", result, defaultDir)
	}
	if result.DataSource == repoDataDir {
		t.Fatalf("workspace mount source = repo data dir %q, want managed .swarm/data default", repoDataDir)
	}
	if info, err := os.Stat(defaultDir); err != nil || !info.IsDir() {
		t.Fatalf("default data source stat = (%v, %v), want created directory", info, err)
	}
}

func TestResolveWorkspaceMountSourcesRejectsDefaultWithoutProjectSource(t *testing.T) {
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("get cwd: %v", err)
	}
	tmp := t.TempDir()
	if err := os.Chdir(tmp); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(oldWD); err != nil {
			t.Fatalf("restore cwd: %v", err)
		}
	})

	result, err := resolveWorkspaceMountSourcesFromInput(workspaceDataSourceInput{})
	if err == nil || !strings.Contains(err.Error(), "workspace data source is required") {
		t.Fatalf("resolve workspace mount sources error = %v, want missing data source failure", err)
	}
	if result.DataSource != "" || result.DataSourceSource != "" {
		t.Fatalf("workspace mount sources = %#v, want no default source", result)
	}
	if _, err := os.Stat(filepath.Join(tmp, defaultWorkspaceDataSourceRelativePath)); !os.IsNotExist(err) {
		t.Fatalf("default data source stat error = %v, want not created", err)
	}
}

func TestResolveWorkspaceMountSourcesUsesVolumesFromAlternateWithoutDefault(t *testing.T) {
	RepoRoot := t.TempDir()
	result, err := resolveWorkspaceMountSourcesFromInput(workspaceDataSourceInput{
		RepoRoot:       RepoRoot,
		VolumesFrom:    "swarm-orchestrator",
		VolumesFromSet: true,
	})
	if err != nil {
		t.Fatalf("resolve workspace mount sources: %v", err)
	}
	if result.DataSource != "" || result.DataSourceSource != "" {
		t.Fatalf("workspace mount sources = %#v, want volumes-from alternate without path source", result)
	}
	if _, err := os.Stat(filepath.Join(RepoRoot, defaultWorkspaceDataSourceRelativePath)); !os.IsNotExist(err) {
		t.Fatalf("default data source stat error = %v, want not created", err)
	}
}

func TestResolveWorkspaceMountSourcesReadsRuntimeConfigAndRejectsEmptyConfig(t *testing.T) {
	RepoRoot := t.TempDir()
	configDir := t.TempDir()

	result, err := resolveWorkspaceMountSources(RepoRoot, "", &config.Config{
		Workspace: config.WorkspaceConfig{DataSource: configDir},
	})
	if err != nil {
		t.Fatalf("resolve config workspace mount sources: %v", err)
	}
	if result.DataSource != configDir || result.DataSourceSource != "workspace.data_source" {
		t.Fatalf("config-backed workspace mount sources = %#v, want %q from workspace.data_source", result, configDir)
	}

	configPath := filepath.Join(t.TempDir(), "swarm.yaml")
	writeRuntimeConfigText(t, configPath, strings.Join([]string{
		"runtime:",
		"  recovery_on_startup: false",
		"workspace:",
		"  data_source: \"   \"",
		"llm:",
		"  backend: anthropic",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
	}, "\n")+"\n")
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("load config with empty workspace.data_source: %v", err)
	}
	result, err = resolveWorkspaceMountSources(RepoRoot, "", cfg)
	if err == nil || !strings.Contains(err.Error(), "workspace.data_source") || !strings.Contains(err.Error(), "must be non-empty") {
		t.Fatalf("resolve empty configured workspace source error = %v, want fail-closed config rejection", err)
	}
	if result.DataSource != "" || result.DataSourceSource != "workspace.data_source" {
		t.Fatalf("empty configured workspace source result = %#v, want no env fallback", result)
	}
}

func TestResolveWorkspaceBackendPrecedence(t *testing.T) {
	result, err := resolveWorkspaceBackendFromInput(workspaceBackendInput{
		FlagBackend:   "host",
		FlagSet:       true,
		ConfigBackend: workspace.BackendDocker,
		ConfigSet:     true,
	})
	if err != nil {
		t.Fatalf("resolve workspace backend flag precedence: %v", err)
	}
	if result.Backend != workspace.BackendHost || result.Source != "--workspace-backend" {
		t.Fatalf("flag precedence result = %#v, want host from --workspace-backend", result)
	}

	result, err = resolveWorkspaceBackendFromInput(workspaceBackendInput{
		ConfigBackend: workspace.BackendHost,
		ConfigSet:     true,
	})
	if err != nil {
		t.Fatalf("resolve workspace backend config precedence: %v", err)
	}
	if result.Backend != workspace.BackendHost || result.Source != "workspace.backend" {
		t.Fatalf("config precedence result = %#v, want host from workspace.backend", result)
	}

	result, err = resolveWorkspaceBackendFromInput(workspaceBackendInput{})
	if err != nil {
		t.Fatalf("resolve workspace backend default: %v", err)
	}
	if result.Backend != "" || result.Source != "capability-derived" || result.PreferenceExplicit {
		t.Fatalf("default preference result = %#v, want capability-derived no explicit backend", result)
	}
}

func TestResolveWorkspaceBackendRejectsEmptyConfigBeforeEnvFallback(t *testing.T) {
	result, err := resolveWorkspaceBackendFromInput(workspaceBackendInput{
		ConfigBackend: " \t ",
		ConfigSet:     true,
	})
	if err == nil || !strings.Contains(err.Error(), "workspace.backend") || !strings.Contains(err.Error(), "must be non-empty") {
		t.Fatalf("resolve empty configured workspace backend error = %v, want fail-closed config rejection", err)
	}
	if result.Backend != "" || result.Source != "workspace.backend" {
		t.Fatalf("empty configured workspace backend result = %#v, want no env fallback", result)
	}
}

func TestPlatformSpecSecretsCLISurfacePromoted(t *testing.T) {
	var spec struct {
		ToolModel struct {
			CredentialStore struct {
				Interface struct {
					List string `yaml:"list"`
				} `yaml:"interface"`
				ResolutionModel struct {
					Tiers []struct {
						Tier        string `yaml:"tier"`
						Description string `yaml:"description"`
					} `yaml:"tiers"`
					ListRule string `yaml:"list_rule"`
				} `yaml:"resolution_model"`
				Metadata struct {
					Fields map[string]string `yaml:"fields"`
				} `yaml:"metadata"`
				StorageBackends struct {
					LocalDev string `yaml:"local_dev"`
				} `yaml:"storage_backends"`
				WriteCoordination struct {
					Rule string `yaml:"rule"`
				} `yaml:"write_coordination"`
				LocalCLISurface struct {
					Command string   `yaml:"command"`
					Rule    string   `yaml:"rule"`
					Split   []string `yaml:"split_scope"`
				} `yaml:"local_cli_surface"`
			} `yaml:"credential_store"`
		} `yaml:"tool_model"`
		CLISpecification struct {
			CommandCatalog struct {
				Secrets struct {
					Command     string `yaml:"command"`
					Status      string `yaml:"implementation_status"`
					Owner       string `yaml:"owner"`
					StoragePath struct {
						Default  string `yaml:"default"`
						Override string `yaml:"override"`
						Examples struct {
							MacOS string `yaml:"macos"`
							Linux string `yaml:"linux"`
						} `yaml:"examples"`
					} `yaml:"storage_path"`
					Subcommands map[string]struct {
						Command  string `yaml:"command"`
						Behavior string `yaml:"behavior"`
					} `yaml:"subcommands"`
				} `yaml:"secrets"`
			} `yaml:"command_catalog"`
		} `yaml:"cli_specification"`
	}
	decodeAuthoritativeYAMLFileForTest(t, filepath.Join(RepoRoot(), defaultPlatformSpecPath), &spec)
	store := spec.ToolModel.CredentialStore
	for _, want := range []string{"shadowed", "required_by"} {
		if !strings.Contains(store.Interface.List, want) {
			t.Fatalf("credential store list interface missing %q: %q", want, store.Interface.List)
		}
		if _, ok := store.Metadata.Fields[want]; !ok {
			t.Fatalf("credential store metadata missing %q: %#v", want, store.Metadata.Fields)
		}
	}
	if !strings.Contains(store.ResolutionModel.ListRule, "shadowed") {
		t.Fatalf("credential store list rule missing shadowing: %q", store.ResolutionModel.ListRule)
	}
	joinedTierDescriptions := ""
	for _, tier := range store.ResolutionModel.Tiers {
		joinedTierDescriptions += tier.Tier + " " + tier.Description + "\n"
	}
	for _, want := range []string{"os.UserConfigDir()/swarm/credentials.json", "SWARM_CREDENTIALS_FILE", "Library/Application Support", ".config/swarm/credentials.json", "uppercased normalized"} {
		if !strings.Contains(joinedTierDescriptions+store.StorageBackends.LocalDev, want) {
			t.Fatalf("credential store path/env docs missing %q:\n%s\n%s", want, joinedTierDescriptions, store.StorageBackends.LocalDev)
		}
	}
	for _, want := range []string{"advisory lock", "Lock contention fails closed", "CLI is the only supported local writer"} {
		if !strings.Contains(store.WriteCoordination.Rule, want) {
			t.Fatalf("credential store write coordination missing %q: %q", want, store.WriteCoordination.Rule)
		}
	}
	if store.LocalCLISurface.Command != "swarm secrets set|list|check|rm" || !strings.Contains(store.LocalCLISurface.Rule, "plaintext argv values") || !strings.Contains(store.LocalCLISurface.Rule, "rm") {
		t.Fatalf("credential store local CLI surface = %#v", store.LocalCLISurface)
	}

	secrets := spec.CLISpecification.CommandCatalog.Secrets
	if secrets.Command != "swarm secrets set|list|check|rm" || secrets.Status != "implemented" || secrets.Owner != "tool_model.credential_store" {
		t.Fatalf("secrets command catalog = %#v", secrets)
	}
	if secrets.StoragePath.Default != "os.UserConfigDir()/swarm/credentials.json" || secrets.StoragePath.Override != "SWARM_CREDENTIALS_FILE" {
		t.Fatalf("secrets storage path = %#v", secrets.StoragePath)
	}
	if !strings.Contains(secrets.StoragePath.Examples.MacOS, "Library/Application Support") || !strings.Contains(secrets.StoragePath.Examples.Linux, ".config/swarm/credentials.json") {
		t.Fatalf("secrets storage examples = %#v", secrets.StoragePath.Examples)
	}
	for _, name := range []string{"set", "list", "check", "rm"} {
		sub, ok := secrets.Subcommands[name]
		if !ok {
			t.Fatalf("secrets command missing subcommand %q: %#v", name, secrets.Subcommands)
		}
		if !strings.Contains(sub.Command, "swarm secrets "+name) {
			t.Fatalf("secrets subcommand %q = %#v", name, sub)
		}
	}
	if !strings.Contains(secrets.Subcommands["set"].Behavior, "Plaintext positional values") || !strings.Contains(secrets.Subcommands["set"].Behavior, "--value") {
		t.Fatalf("secrets set behavior = %q", secrets.Subcommands["set"].Behavior)
	}
}

func TestPlatformSpecWorkspaceBackendSelectionPromoted(t *testing.T) {
	var spec struct {
		WorkspaceModel struct {
			WorkspaceBackendSelection struct {
				PromotedBy           string   `yaml:"promoted_by"`
				ImplementationStatus string   `yaml:"implementation_status"`
				CanonicalOwner       string   `yaml:"canonical_owner"`
				Scope                string   `yaml:"scope"`
				CLIFlag              string   `yaml:"cli_flag"`
				ConfigKey            string   `yaml:"config_key"`
				UnsafeConfigKey      string   `yaml:"unsafe_config_key"`
				RetiredEnvVar        string   `yaml:"retired_env_var"`
				SourceOrder          []string `yaml:"source_order"`
				DefaultBackend       string   `yaml:"default_backend"`
				CapabilityReasonRule string   `yaml:"capability_reason_rule"`
				CapabilityClasses    map[string]struct {
					Rule string `yaml:"rule"`
				} `yaml:"capability_classes"`
				Backends map[string]struct {
					Behavior      string `yaml:"behavior"`
					WorkspaceRoot struct {
						ConfigKey string `yaml:"config_key"`
						Default   string `yaml:"default"`
						Rule      string `yaml:"rule"`
					} `yaml:"workspace_root"`
				} `yaml:"backends"`
				FailureBehavior []string `yaml:"failure_behavior"`
				Consumers       []string `yaml:"consumers"`
				SplitScope      []string `yaml:"split_scope"`
			} `yaml:"workspace_backend_selection"`
		} `yaml:"workspace_model"`
		CLISpecification struct {
			CommandCatalog struct {
				Serve struct {
					WorkspaceBackendSelection struct {
						PromotedBy      string   `yaml:"promoted_by"`
						Owner           string   `yaml:"owner"`
						Flag            string   `yaml:"flag"`
						ConfigKey       string   `yaml:"config_key"`
						UnsafeConfigKey string   `yaml:"unsafe_config_key"`
						RetiredEnvVar   string   `yaml:"retired_env_var"`
						DefaultBackend  string   `yaml:"default_backend"`
						Consumers       []string `yaml:"consumers"`
					} `yaml:"workspace_backend_selection"`
				} `yaml:"serve"`
			} `yaml:"command_catalog"`
		} `yaml:"cli_specification"`
	}
	decodeAuthoritativeYAMLFileForTest(t, filepath.Join(RepoRoot(), defaultPlatformSpecPath), &spec)
	authority := spec.WorkspaceModel.WorkspaceBackendSelection
	if strings.TrimSpace(authority.PromotedBy) != "#1138" || strings.TrimSpace(authority.ImplementationStatus) != "implemented_capability_driven_first_slice" {
		t.Fatalf("workspace backend authority status = promoted_by:%q implementation_status:%q", authority.PromotedBy, authority.ImplementationStatus)
	}
	if !strings.Contains(authority.CanonicalOwner, "workspace_model.workspace_backend_selection") {
		t.Fatalf("workspace backend canonical owner = %q", authority.CanonicalOwner)
	}
	if authority.CLIFlag != "--workspace-backend <docker|host>" || authority.ConfigKey != "workspace.backend" || authority.UnsafeConfigKey != "workspace.allow_exec_on_host" || authority.RetiredEnvVar != "SWARM_WORKSPACE_BACKEND" {
		t.Fatalf("workspace backend selectors = %#v", authority)
	}
	for _, want := range []string{"loaded contract execution capability", "--workspace-backend", "workspace.backend"} {
		if !stringSliceContains(authority.SourceOrder, want) {
			t.Fatalf("workspace backend order missing %q: %#v", want, authority.SourceOrder)
		}
	}
	if authority.DefaultBackend != "capability-derived" {
		t.Fatalf("workspace backend default = %q, want capability-derived", authority.DefaultBackend)
	}
	for _, want := range []string{"typed capability reasons", "MUST NOT parse", "llm.backend: anthropic", "every remaining exec reason", "workspace.allow_exec_on_host"} {
		if !strings.Contains(authority.CapabilityReasonRule, want) {
			t.Fatalf("workspace backend capability reason rule missing %q:\n%s", want, authority.CapabilityReasonRule)
		}
	}
	for _, want := range []string{"none", "workspace_write", "exec"} {
		if _, ok := authority.CapabilityClasses[want]; !ok {
			t.Fatalf("workspace backend capability class %q missing: %#v", want, authority.CapabilityClasses)
		}
	}
	noneBackend, ok := authority.Backends[WorkspaceBackendNone]
	if !ok || !strings.Contains(noneBackend.Behavior, "No workspace lifecycle") || !strings.Contains(noneBackend.Behavior, "forkchat") {
		t.Fatalf("none backend spec missing no-workspace forkchat behavior: %#v", authority.Backends)
	}
	dockerBackend, ok := authority.Backends[workspace.BackendDocker]
	if !ok || !strings.Contains(dockerBackend.Behavior, "exec-capable") || !strings.Contains(dockerBackend.Behavior, "configured workspace image") {
		t.Fatalf("docker backend spec missing default fail-closed behavior: %#v", authority.Backends)
	}
	hostBackend, ok := authority.Backends[workspace.BackendHost]
	if !ok || !strings.Contains(hostBackend.Behavior, "workspace.allow_exec_on_host") || !strings.Contains(hostBackend.Behavior, "MUST NOT require Docker") {
		t.Fatalf("host backend spec missing local-dev no-Docker behavior: %#v", authority.Backends)
	}
	if hostBackend.WorkspaceRoot.ConfigKey != "workspace.host_root" || hostBackend.WorkspaceRoot.Default != "~/.swarm/workspaces" {
		t.Fatalf("host workspace root spec = %#v", hostBackend.WorkspaceRoot)
	}
	for _, want := range []string{"canonical/evaluated paths", "Every host lifecycle consumer", "symlink escapes"} {
		if !strings.Contains(hostBackend.WorkspaceRoot.Rule, want) {
			t.Fatalf("host workspace root rule missing %q:\n%s", want, hostBackend.WorkspaceRoot.Rule)
		}
	}
	for _, want := range []string{"Unsupported backend", "Empty explicit backend", "workspace.volumes_from", "Claude CLI", "SWARM_WORKSPACE_BACKEND=host", "conversation.fork_chat", "native command execution"} {
		if !joinedContains(authority.FailureBehavior, want) {
			t.Fatalf("workspace backend failure behavior missing %q: %#v", want, authority.FailureBehavior)
		}
	}
	for _, want := range []string{"serve boot", "run start", "Builder project reload", "selected-contract run-fork", "swarm verify", "swarm describe", "swarm doctor", "conversation.fork_chat"} {
		if !joinedContains(authority.Consumers, want) {
			t.Fatalf("workspace backend consumers missing %q: %#v", want, authority.Consumers)
		}
	}
	for _, want := range []string{"#1137", "#1136", "full host Claude CLI/provider", "production SQLite"} {
		if !joinedContains(authority.SplitScope, want) {
			t.Fatalf("workspace backend split scope missing %q: %#v", want, authority.SplitScope)
		}
	}
	command := spec.CLISpecification.CommandCatalog.Serve.WorkspaceBackendSelection
	if command.PromotedBy != "#1138" || command.Owner != "workspace_model.workspace_backend_selection" || command.Flag != "--workspace-backend <docker|host>" {
		t.Fatalf("serve command workspace backend authority = %#v", command)
	}
	if command.ConfigKey != "workspace.backend" || command.UnsafeConfigKey != "workspace.allow_exec_on_host" || command.RetiredEnvVar != "SWARM_WORKSPACE_BACKEND" || command.DefaultBackend != "capability-derived" {
		t.Fatalf("serve command workspace backend selectors = %#v", command)
	}
	for _, want := range []string{"serve boot", "run start", "Builder project reload", "selected-contract run-fork", "verify/describe/doctor", "conversation.fork_chat"} {
		if !joinedContains(command.Consumers, want) {
			t.Fatalf("serve command workspace backend consumers missing %q: %#v", want, command.Consumers)
		}
	}
}

func TestPlatformSpecWorkspaceExecutionTargetCapabilityPromoted(t *testing.T) {
	var spec struct {
		WorkspaceModel struct {
			WorkspaceExecutionTargetCapability struct {
				PromotedBy           string `yaml:"promoted_by"`
				ImplementationStatus string `yaml:"implementation_status"`
				CanonicalOwner       string `yaml:"canonical_owner"`
				Scope                string `yaml:"scope"`
				ImplementationOwner  string `yaml:"implementation_owner"`
				TargetModes          map[string]struct {
					Rule         string   `yaml:"rule"`
					Capabilities []string `yaml:"capabilities"`
				} `yaml:"target_modes"`
				LogicalMountAuthority struct {
					Writable []string `yaml:"writable"`
					ReadOnly []string `yaml:"read_only"`
					Rules    []string `yaml:"rules"`
				} `yaml:"logical_mount_authority"`
				Consumers                           []string `yaml:"consumers"`
				RetiredNonAuthoritativeInterpreters []string `yaml:"retired_non_authoritative_interpreters"`
				FailureBehavior                     []string `yaml:"failure_behavior"`
				SplitScope                          []string `yaml:"split_scope"`
			} `yaml:"workspace_execution_target_capability"`
		} `yaml:"workspace_model"`
		CLISpecification struct {
			CommandCatalog struct {
				Serve struct {
					WorkspaceExecutionTargetCapability struct {
						PromotedBy string   `yaml:"promoted_by"`
						Owner      string   `yaml:"owner"`
						Rule       string   `yaml:"rule"`
						Consumers  []string `yaml:"consumers"`
					} `yaml:"workspace_execution_target_capability"`
				} `yaml:"serve"`
			} `yaml:"command_catalog"`
		} `yaml:"cli_specification"`
	}
	decodeAuthoritativeYAMLFileForTest(t, filepath.Join(RepoRoot(), defaultPlatformSpecPath), &spec)
	authority := spec.WorkspaceModel.WorkspaceExecutionTargetCapability
	if strings.TrimSpace(authority.PromotedBy) != "#1213/#1235/#1286/#1356" || strings.TrimSpace(authority.ImplementationStatus) != "implemented_host_file_relay_and_trusted_command_slice" {
		t.Fatalf("workspace execution target authority status = promoted_by:%q implementation_status:%q", authority.PromotedBy, authority.ImplementationStatus)
	}
	if !strings.Contains(authority.CanonicalOwner, "workspace_model.workspace_execution_target_capability") || !strings.Contains(authority.ImplementationOwner, "internal/runtime/workspace.ExecutionTarget") {
		t.Fatalf("workspace execution target owners = canonical:%q implementation:%q", authority.CanonicalOwner, authority.ImplementationOwner)
	}
	for _, want := range []string{"Docker container execution", "explicit host-local targets", "unsupported targets", "trusted/unsafe", "tool-result relay"} {
		if !strings.Contains(authority.Scope, want) {
			t.Fatalf("workspace execution target scope missing %q:\n%s", want, authority.Scope)
		}
	}
	dockerMode := authority.TargetModes[string(workspace.ExecutionModeDockerContainer)]
	for _, want := range []string{
		string(workspace.ExecutionCapabilityNativeCommand),
		string(workspace.ExecutionCapabilityFileRead),
		string(workspace.ExecutionCapabilityFileWrite),
		string(workspace.ExecutionCapabilityToolResultRelay),
		string(workspace.ExecutionCapabilityClaudeCLI),
	} {
		if !stringSliceContains(dockerMode.Capabilities, want) {
			t.Fatalf("docker execution capabilities missing %q: %#v", want, dockerMode.Capabilities)
		}
	}
	hostMode := authority.TargetModes[string(workspace.ExecutionModeHostLocal)]
	if !strings.Contains(hostMode.Rule, "MUST NOT infer execution support from an empty container") || !strings.Contains(hostMode.Rule, "trusted/unsafe native_command") {
		t.Fatalf("host execution mode rule = %q", hostMode.Rule)
	}
	for _, want := range []string{string(workspace.ExecutionCapabilityNativeCommand), string(workspace.ExecutionCapabilityFileRead), string(workspace.ExecutionCapabilityFileWrite), string(workspace.ExecutionCapabilityToolResultRelay)} {
		if !stringSliceContains(hostMode.Capabilities, want) {
			t.Fatalf("host execution capabilities missing %q: %#v", want, hostMode.Capabilities)
		}
	}
	for _, forbidden := range []string{string(workspace.ExecutionCapabilityClaudeCLI)} {
		if stringSliceContains(hostMode.Capabilities, forbidden) {
			t.Fatalf("host execution capabilities include forbidden %q: %#v", forbidden, hostMode.Capabilities)
		}
	}
	unsupportedMode := authority.TargetModes[string(workspace.ExecutionModeUnsupported)]
	if !strings.Contains(unsupportedMode.Rule, "process cwd") || len(unsupportedMode.Capabilities) != 0 {
		t.Fatalf("unsupported execution mode = %#v, want fail-closed no fallback", unsupportedMode)
	}
	if !stringSliceContains(authority.LogicalMountAuthority.Writable, workspace.LogicalWorkspaceMount) {
		t.Fatalf("logical writable mounts = %#v, want %s", authority.LogicalMountAuthority.Writable, workspace.LogicalWorkspaceMount)
	}
	for _, want := range []string{workspace.LogicalDataMount, workspace.LogicalContractsMount} {
		if !stringSliceContains(authority.LogicalMountAuthority.ReadOnly, want) {
			t.Fatalf("logical read-only mounts missing %q: %#v", want, authority.LogicalMountAuthority.ReadOnly)
		}
	}
	for _, want := range []string{"Relative execution paths", "Writes outside logical `/workspace`", "MUST NOT leak raw host backing paths", "symlink escapes", "data_source_authority"} {
		if !joinedContains(authority.LogicalMountAuthority.Rules, want) {
			t.Fatalf("logical mount authority rules missing %q: %#v", want, authority.LogicalMountAuthority.Rules)
		}
	}
	for _, want := range []string{"Host workspace target production", "native executor command", "native fallback tool definitions", "OpenAI-compatible/API fallback-tool routing", "tool-result relay", "Claude CLI process", "runtime Claude managed-agent startup"} {
		if !joinedContains(authority.Consumers, want) {
			t.Fatalf("workspace execution target consumers missing %q: %#v", want, authority.Consumers)
		}
	}
	for _, want := range []string{"Target.Container", "Target.Enabled()", "raw Target.Workdir", "cwd fallback", "HostBackend()"} {
		if !joinedContains(authority.RetiredNonAuthoritativeInterpreters, want) {
			t.Fatalf("retired interpreters missing %q: %#v", want, authority.RetiredNonAuthoritativeInterpreters)
		}
	}
	for _, want := range []string{"explicit host backend selection", "native_tools.bash", "Docker target failures", "Host-local tool-result relay writes", "Host-local file operations fail closed", "Empty-container targets", "Docker behavior"} {
		if !joinedContains(authority.FailureBehavior, want) {
			t.Fatalf("failure behavior missing %q: %#v", want, authority.FailureBehavior)
		}
	}
	for _, want := range []string{"Claude CLI", "Docker-equivalent host isolation", "#1137"} {
		if !joinedContains(authority.SplitScope, want) {
			t.Fatalf("split scope missing %q: %#v", want, authority.SplitScope)
		}
	}
	command := spec.CLISpecification.CommandCatalog.Serve.WorkspaceExecutionTargetCapability
	if command.PromotedBy != "#1213/#1235/#1286/#1356" || command.Owner != "workspace_model.workspace_execution_target_capability" {
		t.Fatalf("serve command workspace execution authority = %#v", command)
	}
	if !strings.Contains(command.Rule, "trusted/unsafe native command execution") || !strings.Contains(command.Rule, "Claude/provider execution remains fail closed") {
		t.Fatalf("serve command workspace execution rule = %q", command.Rule)
	}
	for _, want := range []string{"native executor", "fallback-tool routing", "tool-result relay", "Claude CLI"} {
		if !joinedContains(command.Consumers, want) {
			t.Fatalf("serve command workspace execution consumers missing %q: %#v", want, command.Consumers)
		}
	}
}

func TestPlatformSpecInstalledBinaryPortabilityPromoted(t *testing.T) {
	var spec struct {
		CLISpecification struct {
			Foundations struct {
				InstalledBinaryPortability struct {
					PromotedBy             string            `yaml:"promoted_by"`
					ImplementationStatus   string            `yaml:"implementation_status"`
					CanonicalOwner         string            `yaml:"canonical_owner"`
					Scope                  string            `yaml:"scope"`
					Rule                   string            `yaml:"rule"`
					EmbeddedAssets         map[string]string `yaml:"embedded_assets"`
					RuntimeWorkspaceAssets map[string]string `yaml:"runtime_workspace_assets"`
					SplitTail              []string          `yaml:"split_tail"`
				} `yaml:"installed_binary_portability"`
			} `yaml:"foundations"`
		} `yaml:"cli_specification"`
		WorkspaceModel struct {
			RuntimeImagePackaging struct {
				PromotedBy           string            `yaml:"promoted_by"`
				ImplementationStatus string            `yaml:"implementation_status"`
				CanonicalOwner       string            `yaml:"canonical_owner"`
				Rule                 string            `yaml:"rule"`
				MountSourceRule      string            `yaml:"mount_source_rule"`
				SplitScope           string            `yaml:"split_scope"`
				RecoveryDiagnostics  map[string]string `yaml:"recovery_diagnostics"`
			} `yaml:"runtime_image_packaging"`
		} `yaml:"workspace_model"`
	}
	decodeAuthoritativeYAMLFileForTest(t, filepath.Join(RepoRoot(), defaultPlatformSpecPath), &spec)
	portability := spec.CLISpecification.Foundations.InstalledBinaryPortability
	if strings.TrimSpace(portability.PromotedBy) != "#1002" {
		t.Fatalf("installed binary portability promoted_by = %q, want #1002", portability.PromotedBy)
	}
	if strings.TrimSpace(portability.ImplementationStatus) != "implemented" {
		t.Fatalf("installed binary portability implementation_status = %q, want implemented", portability.ImplementationStatus)
	}
	if !strings.Contains(portability.CanonicalOwner, "cli_specification.foundations.installed_binary_portability") {
		t.Fatalf("canonical owner does not point at installed_binary_portability: %s", portability.CanonicalOwner)
	}
	for _, want := range []string{"help", "completion", "local `swarm version`", "contract_platform_spec_path_resolution", "workspace_model.runtime_image_packaging"} {
		if !strings.Contains(portability.Scope, want) {
			t.Fatalf("installed binary portability scope missing %q:\n%s", want, portability.Scope)
		}
	}
	for _, want := range []string{"without a source checkout", "go.mod", "MUST NOT block help"} {
		if !strings.Contains(portability.Rule, want) {
			t.Fatalf("installed binary portability rule missing %q:\n%s", want, portability.Rule)
		}
	}
	if !strings.Contains(portability.EmbeddedAssets["platform_spec"], defaultPlatformSpecPath) {
		t.Fatalf("platform_spec embedded asset missing tracked path:\n%s", portability.EmbeddedAssets["platform_spec"])
	}
	if !strings.Contains(portability.RuntimeWorkspaceAssets["workspace_image"], "MUST NOT use") || !strings.Contains(portability.RuntimeWorkspaceAssets["workspace_image"], "Dockerfile.workspace") {
		t.Fatalf("workspace_image embedded portability rule missing source-root prohibition:\n%s", portability.RuntimeWorkspaceAssets["workspace_image"])
	}
	if stringSliceContains(portability.SplitTail, "Dockerfile.workspace") {
		t.Fatalf("installed binary portability still lists Dockerfile.workspace as split tail: %#v", portability.SplitTail)
	}
	for _, want := range []string{"#996 Docker Compose", "#997 local cli_test"} {
		if !stringSliceContains(portability.SplitTail, want) {
			t.Fatalf("installed binary portability split_tail missing %q: %#v", want, portability.SplitTail)
		}
	}
	packaging := spec.WorkspaceModel.RuntimeImagePackaging
	if strings.TrimSpace(packaging.PromotedBy) != "#1002" {
		t.Fatalf("runtime image packaging promoted_by = %q, want #1002", packaging.PromotedBy)
	}
	if strings.TrimSpace(packaging.ImplementationStatus) != "implemented" {
		t.Fatalf("runtime image packaging implementation_status = %q, want implemented", packaging.ImplementationStatus)
	}
	if !strings.Contains(packaging.CanonicalOwner, "workspace_model.runtime_image_packaging") {
		t.Fatalf("runtime image packaging canonical owner = %q, want workspace_model.runtime_image_packaging", packaging.CanonicalOwner)
	}
	for _, want := range []string{"prebuilt runtime dependency", "fail closed", "MUST NOT build", "Dockerfile.workspace", "runtime.Caller"} {
		if !strings.Contains(packaging.Rule, want) {
			t.Fatalf("runtime image packaging rule missing %q:\n%s", want, packaging.Rule)
		}
	}
	for _, want := range []string{"/data", "/opt/swarm/contracts", "MUST NOT derive"} {
		if !strings.Contains(packaging.MountSourceRule, want) {
			t.Fatalf("runtime image packaging mount source rule missing %q:\n%s", want, packaging.MountSourceRule)
		}
	}
	if !strings.Contains(packaging.RecoveryDiagnostics["docker_daemon"], "<configured-docker-bin> info") || !strings.Contains(packaging.RecoveryDiagnostics["workspace_image"], "swarm workspace build --backend claude_cli") || !strings.Contains(packaging.RecoveryDiagnostics["workspace_image"], "MUST NOT advertise pulling") {
		t.Fatalf("runtime image recovery diagnostics incomplete: %#v", packaging.RecoveryDiagnostics)
	}
	for _, want := range []string{"#996", "#997"} {
		if !strings.Contains(packaging.SplitScope, want) {
			t.Fatalf("runtime image packaging split scope missing %q:\n%s", want, packaging.SplitScope)
		}
	}
}

func TestPlatformSpecLocalCLITestGatewayStartupPromoted(t *testing.T) {
	var spec struct {
		CLISpecification struct {
			Foundations struct {
				LocalCLITestGatewayStartup struct {
					PromotedBy            string   `yaml:"promoted_by"`
					ImplementationStatus  string   `yaml:"implementation_status"`
					CanonicalOwner        string   `yaml:"canonical_owner"`
					Scope                 string   `yaml:"scope"`
					GatewayTokenRule      string   `yaml:"gateway_token_rule"`
					GatewayTokenConsumers []string `yaml:"gateway_token_consumers"`
					StartupProbeRule      string   `yaml:"startup_probe_rule"`
					SplitTail             []string `yaml:"split_tail"`
				} `yaml:"local_cli_test_gateway_startup"`
			} `yaml:"foundations"`
		} `yaml:"cli_specification"`
	}
	decodeAuthoritativeYAMLFileForTest(t, filepath.Join(RepoRoot(), defaultPlatformSpecPath), &spec)
	startup := spec.CLISpecification.Foundations.LocalCLITestGatewayStartup
	if strings.TrimSpace(startup.PromotedBy) != "#997" {
		t.Fatalf("local cli_test gateway startup promoted_by = %q, want #997", startup.PromotedBy)
	}
	if strings.TrimSpace(startup.ImplementationStatus) != "implemented_first_slice" {
		t.Fatalf("local cli_test gateway startup implementation_status = %q, want implemented_first_slice", startup.ImplementationStatus)
	}
	if !strings.Contains(startup.CanonicalOwner, "cli_specification.foundations.local_cli_test_gateway_startup") {
		t.Fatalf("canonical owner does not point at local_cli_test_gateway_startup: %s", startup.CanonicalOwner)
	}
	for _, want := range []string{"narrowed #997", "MCP gateway token derivation", "MCP-only managed-agent startup proof", "does not close full #997", "local_tool_gateway_binding"} {
		if !strings.Contains(startup.Scope, want) {
			t.Fatalf("local cli_test gateway startup scope missing %q:\n%s", want, startup.Scope)
		}
	}
	for _, want := range []string{"SWARM_TOOL_GATEWAY_TOKEN", "removed", "per-boot", "binding token", "SWARM_TOOL_GATEWAY_URL", "not source authority", "Local foreground `swarm run start`"} {
		if !strings.Contains(startup.GatewayTokenRule, want) {
			t.Fatalf("gateway token rule missing %q:\n%s", want, startup.GatewayTokenRule)
		}
	}
	for _, want := range []string{"RuntimeOptions.ToolGatewayBinding", "runtime MCP gateway auth", "ValidateClaudeCLIRuntimeConfig", "MCP HTTP binding"} {
		if !stringSliceContains(startup.GatewayTokenConsumers, want) {
			t.Fatalf("gateway token consumers missing %q: %#v", want, startup.GatewayTokenConsumers)
		}
	}
	for _, want := range []string{"startup validation MUST execute", "every managed agent", "MCP-only", "Agent-free `cli_test`"} {
		if !strings.Contains(startup.StartupProbeRule, want) {
			t.Fatalf("startup probe rule missing %q:\n%s", want, startup.StartupProbeRule)
		}
	}
	for _, want := range []string{"#997 local cli_test workspace image contents", "#996 Docker Compose", "#1568 selected-contract", "#979/#1012 are completed historical source-authority slices", "#1002 runtime workspace source-root image packaging is closed"} {
		if !stringSliceContains(startup.SplitTail, want) {
			t.Fatalf("local cli_test gateway startup split_tail missing %q: %#v", want, startup.SplitTail)
		}
	}
}

func TestPlatformSpecLocalToolGatewayBindingPromoted(t *testing.T) {
	var spec struct {
		CLISpecification struct {
			Foundations struct {
				LocalToolGatewayBinding struct {
					PromotedBy           string   `yaml:"promoted_by"`
					ImplementationStatus string   `yaml:"implementation_status"`
					CanonicalOwner       string   `yaml:"canonical_owner"`
					Scope                string   `yaml:"scope"`
					BindingFields        []string `yaml:"binding_fields"`
					SourceRule           string   `yaml:"source_rule"`
					EndpointEnvRule      string   `yaml:"endpoint_env_rule"`
					AuthRule             string   `yaml:"auth_rule"`
					MultiContextRule     string   `yaml:"multi_context_rule"`
					Consumers            []string `yaml:"consumers"`
					SplitTail            []string `yaml:"split_tail"`
				} `yaml:"local_tool_gateway_binding"`
			} `yaml:"foundations"`
		} `yaml:"cli_specification"`
	}
	decodeAuthoritativeYAMLFileForTest(t, filepath.Join(RepoRoot(), defaultPlatformSpecPath), &spec)
	binding := spec.CLISpecification.Foundations.LocalToolGatewayBinding
	if strings.TrimSpace(binding.PromotedBy) != "#1568" {
		t.Fatalf("local tool gateway binding promoted_by = %q, want #1568", binding.PromotedBy)
	}
	if strings.TrimSpace(binding.ImplementationStatus) != "implemented_first_slice" {
		t.Fatalf("local tool gateway binding implementation_status = %q, want implemented_first_slice", binding.ImplementationStatus)
	}
	if !strings.Contains(binding.CanonicalOwner, "cli_specification.foundations.local_tool_gateway_binding") {
		t.Fatalf("canonical owner does not point at local_tool_gateway_binding: %s", binding.CanonicalOwner)
	}
	for _, want := range []string{"local `serve`", "foreground `run`", "actual bound MCP listener", "stale URL-env", "public/operator", "non-dev serve", "production Claude runtime factory", "MUST NOT mutate process-global"} {
		if !strings.Contains(binding.Scope, want) {
			t.Fatalf("local tool gateway binding scope missing %q:\n%s", want, binding.Scope)
		}
	}
	for _, want := range []string{"transport", "host_endpoint", "workspace_endpoint", "auth_token", "lifecycle_owner", "source"} {
		if !stringSliceContains(binding.BindingFields, want) {
			t.Fatalf("binding fields missing %q: %#v", want, binding.BindingFields)
		}
	}
	for _, want := range []string{"bind the MCP/tool listener first", "ToolGatewayBinding", "workspace-backend projection", "serve boot", "Selected-fork ephemeral gateways", "actual ephemeral gateway listener"} {
		if !strings.Contains(binding.SourceRule, want) {
			t.Fatalf("source rule missing %q:\n%s", want, binding.SourceRule)
		}
	}
	for _, want := range []string{"SWARM_TOOL_GATEWAY_URL", "SWARM_TOOL_GATEWAY_CONTAINER_URL", "not public/operator source", "generated final-boundary compatibility", "derived from `ToolGatewayBinding`", "Selected-fork ephemeral gateway startup MUST NOT set", "retired public input", "MUST fail closed", "unset it", "MUST NOT be rendered as accepted configuration"} {
		if !strings.Contains(binding.EndpointEnvRule, want) {
			t.Fatalf("endpoint env rule missing %q:\n%s", want, binding.EndpointEnvRule)
		}
	}
	for _, want := range []string{"SWARM_TOOL_GATEWAY_TOKEN", "removed as public/operator token source", "per-boot token", "binding token", "selected-fork ephemeral gateways MUST generate their own binding token", "fail closed"} {
		if !strings.Contains(binding.AuthRule, want) {
			t.Fatalf("auth rule missing %q:\n%s", want, binding.AuthRule)
		}
	}
	for _, want := range []string{"multi-context", "claude_cli", "ToolGatewayBinding", "MCP `/mcp` and `/tools/`", "forkchat sandbox", "MUST fail closed before readiness", "MUST NOT rely on primary"} {
		if !strings.Contains(binding.MultiContextRule, want) {
			t.Fatalf("multi context rule missing %q:\n%s", want, binding.MultiContextRule)
		}
	}
	for _, want := range []string{"serve listener binding", "RuntimeOptions.ToolGatewayBinding", "runtime MCP gateway auth", "ValidateClaudeCLIRuntimeConfig", "MCP HTTP config", "Docker exec", "fork-chat sandbox", "selected-fork ephemeral gateway", "non-dev serve retired URL-env admission"} {
		if !stringSliceContains(binding.Consumers, want) {
			t.Fatalf("binding consumers missing %q: %#v", want, binding.Consumers)
		}
	}
	for _, want := range []string{"#1568 broader selected-contract", "no longer uses process-global URL env", "#979/#1012 are completed historical source-authority slices", "#1138/#1213", "#1567", "IPC/unix socket"} {
		if !stringSliceContains(binding.SplitTail, want) {
			t.Fatalf("binding split_tail missing %q: %#v", want, binding.SplitTail)
		}
	}
}

func TestPlatformSpecLocalCLITestWorkspaceCLIAvailabilityPromoted(t *testing.T) {
	var spec struct {
		CLISpecification struct {
			Foundations struct {
				LocalCLITestWorkspaceCLIAvailability struct {
					PromotedBy           string   `yaml:"promoted_by"`
					ImplementationStatus string   `yaml:"implementation_status"`
					CanonicalOwner       string   `yaml:"canonical_owner"`
					Scope                string   `yaml:"scope"`
					WorkspaceCLIRule     string   `yaml:"workspace_cli_rule"`
					Consumers            []string `yaml:"consumers"`
					SplitTail            []string `yaml:"split_tail"`
				} `yaml:"local_cli_test_workspace_cli_availability"`
			} `yaml:"foundations"`
		} `yaml:"cli_specification"`
	}
	decodeAuthoritativeYAMLFileForTest(t, filepath.Join(RepoRoot(), defaultPlatformSpecPath), &spec)
	availability := spec.CLISpecification.Foundations.LocalCLITestWorkspaceCLIAvailability
	if strings.TrimSpace(availability.PromotedBy) != "#997" {
		t.Fatalf("local cli_test workspace cli availability promoted_by = %q, want #997", availability.PromotedBy)
	}
	if strings.TrimSpace(availability.ImplementationStatus) != "implemented" {
		t.Fatalf("local cli_test workspace cli availability implementation_status = %q, want implemented", availability.ImplementationStatus)
	}
	if !strings.Contains(availability.CanonicalOwner, "cli_specification.foundations.local_cli_test_workspace_cli_availability") {
		t.Fatalf("canonical owner does not point at local_cli_test_workspace_cli_availability: %s", availability.CanonicalOwner)
	}
	for _, want := range []string{"remaining #997", "workspace image/default-agent Claude CLI availability", "local `swarm serve`", "local foreground `swarm run start`"} {
		if !strings.Contains(availability.Scope, want) {
			t.Fatalf("local cli_test workspace cli availability scope missing %q:\n%s", want, availability.Scope)
		}
	}
	for _, want := range []string{"startup validation MUST prove", "before readiness or event delivery", "Docker image inspection", "existing container reuse", "workspace.image"} {
		if !strings.Contains(availability.WorkspaceCLIRule, want) {
			t.Fatalf("workspace cli rule missing %q:\n%s", want, availability.WorkspaceCLIRule)
		}
	}
	for _, want := range []string{"local `swarm serve`", "local foreground `swarm run start`", "managed-agent startup validation", "Claude CLI startup probe", "configured workspace image/container targets"} {
		if !stringSliceContains(availability.Consumers, want) {
			t.Fatalf("local cli_test workspace cli availability consumers missing %q: %#v", want, availability.Consumers)
		}
	}
	for _, want := range []string{"#996 Docker Compose", "#995 schema migration", "#1568 selected-contract", "#979/#1012 are completed historical source-authority slices", "#1002 runtime workspace source-root image packaging is closed"} {
		if !stringSliceContains(availability.SplitTail, want) {
			t.Fatalf("local cli_test workspace cli availability split_tail missing %q: %#v", want, availability.SplitTail)
		}
	}
}

func TestPlatformSpecLLMProviderModelSelectionSourceAuthorityPromoted(t *testing.T) {
	var spec struct {
		Engine struct {
			AgentSessionManagement struct {
				Selection struct {
					PromotedBy           string            `yaml:"promoted_by"`
					ImplementationStatus string            `yaml:"implementation_status"`
					Owner                string            `yaml:"owner"`
					BehaviorChildren     map[string]string `yaml:"behavior_children"`
					CanonicalSelector    struct {
						CLIFlag                          string   `yaml:"cli_flag"`
						ConfigKey                        string   `yaml:"config_key"`
						EnvVar                           string   `yaml:"env_var"`
						DefaultBackendProfile            string   `yaml:"default_backend_profile"`
						SourceOrder                      []string `yaml:"source_order"`
						RetiredNonAuthoritativeSelectors []string `yaml:"retired_non_authoritative_selectors"`
						Rules                            []string `yaml:"rules"`
					} `yaml:"canonical_selector"`
					BackendProfileIdentity struct {
						ActiveBackendProfiles map[string]struct {
							Provider                    string   `yaml:"provider"`
							Transport                   string   `yaml:"transport"`
							ProviderContractRuntimeMode string   `yaml:"provider_contract_runtime_mode"`
							LegacyBackendIDs            []string `yaml:"legacy_backend_ids"`
							CredentialSource            struct {
								SecretKey      string `yaml:"secret_key"`
								Owner          string `yaml:"owner"`
								LegacyEnvVar   string `yaml:"legacy_env_var"`
								Required       bool   `yaml:"required"`
								ResolutionRule string `yaml:"resolution_rule"`
							} `yaml:"credential_source"`
							EndpointSource struct {
								ConfigKey      string `yaml:"config_key"`
								EnvVar         string `yaml:"env_var"`
								Required       bool   `yaml:"required"`
								BuiltInDefault string `yaml:"built_in_default"`
								Rule           string `yaml:"rule"`
							} `yaml:"endpoint_source"`
						} `yaml:"active_backend_profiles"`
						PendingBackendProfiles map[string]struct {
							Status                      string `yaml:"status"`
							Provider                    string `yaml:"provider"`
							Transport                   string `yaml:"transport"`
							ProviderContractRuntimeMode string `yaml:"provider_contract_runtime_mode"`
							Protocol                    string `yaml:"protocol"`
							CredentialSource            struct {
								SecretKey      string `yaml:"secret_key"`
								Owner          string `yaml:"owner"`
								LegacyEnvVar   string `yaml:"legacy_env_var"`
								Required       bool   `yaml:"required"`
								ResolutionRule string `yaml:"resolution_rule"`
							} `yaml:"credential_source"`
							EndpointSource struct {
								ConfigKey      string `yaml:"config_key"`
								EnvVar         string `yaml:"env_var"`
								Required       bool   `yaml:"required"`
								BuiltInDefault string `yaml:"built_in_default"`
								Rule           string `yaml:"rule"`
							} `yaml:"endpoint_source"`
							ActivationRule string `yaml:"activation_rule"`
							ProofBoundary  string `yaml:"proof_boundary"`
						} `yaml:"pending_backend_profiles"`
						RejectedTargetNames map[string]string `yaml:"rejected_target_names"`
					} `yaml:"backend_profile_identity"`
					NativeOpenAIResponsesSourceAuthority struct {
						PromotedBy             string   `yaml:"promoted_by"`
						ImplementationStatus   string   `yaml:"implementation_status"`
						TargetBackendProfileID string   `yaml:"target_backend_profile_id"`
						ProviderIdentity       string   `yaml:"provider_identity"`
						ActivationStatus       string   `yaml:"activation_status"`
						AuthoritativeRefs      []string `yaml:"authoritative_refs"`
						ActiveSelectorPolicy   []string `yaml:"active_selector_policy"`
						ProtocolFamily         struct {
							Name              string `yaml:"name"`
							EndpointPath      string `yaml:"endpoint_path"`
							Transport         string `yaml:"transport"`
							RequestBodyOwner  string `yaml:"request_body_owner"`
							InputScope        string `yaml:"input_scope"`
							ToolScope         string `yaml:"tool_scope"`
							StreamingScope    string `yaml:"streaming_scope"`
							SessionContinuity string `yaml:"session_continuity"`
						} `yaml:"protocol_family"`
						CredentialAndEndpointPolicy struct {
							CredentialSource struct {
								SecretKey      string `yaml:"secret_key"`
								Owner          string `yaml:"owner"`
								LegacyEnvVar   string `yaml:"legacy_env_var"`
								Required       bool   `yaml:"required"`
								ResolutionRule string `yaml:"resolution_rule"`
							} `yaml:"credential_source"`
							EndpointSource struct {
								ConfigKey      string `yaml:"config_key"`
								EnvVar         string `yaml:"env_var"`
								Required       bool   `yaml:"required"`
								BuiltInDefault string `yaml:"built_in_default"`
							} `yaml:"endpoint_source"`
							Rules []string `yaml:"rules"`
						} `yaml:"credential_and_endpoint_policy"`
						ModelAliasDefaults           map[string]string `yaml:"model_alias_defaults"`
						ModelAliasRule               string            `yaml:"model_alias_rule"`
						ProviderContractExpectations struct {
							ToolSchema            []string `yaml:"tool_schema"`
							ResponseNormalization []string `yaml:"response_normalization"`
							SessionLifecycle      []string `yaml:"session_lifecycle"`
							Persistence           []string `yaml:"persistence"`
							Budget                []string `yaml:"budget"`
						} `yaml:"provider_contract_expectations"`
						StoreProfileImplication string   `yaml:"store_profile_implication"`
						SplitBoundaries         []string `yaml:"split_boundaries"`
					} `yaml:"native_openai_responses_source_authority"`
					ModelAliasAuthority struct {
						ContractField              string                       `yaml:"contract_field"`
						Replaces                   string                       `yaml:"replaces"`
						AliasConfigKey             string                       `yaml:"alias_config_key"`
						BuiltInAliases             []string                     `yaml:"built_in_aliases"`
						BuiltInModelDefaults       map[string]map[string]string `yaml:"built_in_model_defaults"`
						AliasVocabularyDeclaration string                       `yaml:"alias_vocabulary_declaration"`
						ResolutionRule             string                       `yaml:"resolution_rule"`
						VerifyRule                 string                       `yaml:"verify_rule"`
						AuditRule                  string                       `yaml:"audit_rule"`
					} `yaml:"model_alias_authority"`
					CredentialAndConfigPolicy struct {
						ProviderSecretKeys                 []string `yaml:"provider_secret_keys"`
						DeprecatedProviderEnvShadowSources []string `yaml:"deprecated_provider_env_shadow_sources"`
						SecretEnvSources                   []string `yaml:"secret_env_sources"`
						RuntimeConfigCanonicalFor          []string `yaml:"runtime_config_canonical_for"`
						InfraConnectionOverridePolicy      string   `yaml:"infra_connection_override_policy"`
					} `yaml:"credential_and_config_policy"`
					PersistenceRules []string `yaml:"persistence_rules"`
					SplitBoundaries  []string `yaml:"split_boundaries"`
				} `yaml:"llm_provider_selection_config_authority"`
			} `yaml:"agent_session_management"`
		} `yaml:"engine"`
	}
	decodeAuthoritativeYAMLFileForTest(t, filepath.Join(RepoRoot(), defaultPlatformSpecPath), &spec)
	authority := spec.Engine.AgentSessionManagement.Selection
	if strings.TrimSpace(authority.PromotedBy) != "#1127" {
		t.Fatalf("llm provider selection promoted_by = %q, want #1127", authority.PromotedBy)
	}
	if strings.TrimSpace(authority.ImplementationStatus) != "backend_selection_and_model_alias_authority_implemented_docs_split" {
		t.Fatalf("llm provider selection implementation_status = %q", authority.ImplementationStatus)
	}
	if !strings.Contains(authority.Owner, "backend profile and model alias resolver") {
		t.Fatalf("llm provider selection owner missing alias resolver: %s", authority.Owner)
	}
	for _, want := range []string{"#1128", "#1129", "#1130"} {
		if !mapValueContains(authority.BehaviorChildren, want) {
			t.Fatalf("behavior children missing %q: %#v", want, authority.BehaviorChildren)
		}
	}
	selector := authority.CanonicalSelector
	if selector.CLIFlag != "--backend" || selector.ConfigKey != "llm.backend" || selector.EnvVar != "" || selector.DefaultBackendProfile != "anthropic" {
		t.Fatalf("selector = %#v, want --backend > llm.backend > default anthropic with no env selector", selector)
	}
	for _, want := range []string{"--backend", "runtime config llm.backend", "built-in default anthropic"} {
		if !stringSliceContains(selector.SourceOrder, want) {
			t.Fatalf("selector source order missing %q: %#v", want, selector.SourceOrder)
		}
	}
	for _, want := range []string{"SWARM_LLM_BACKEND", "llm.runtime_mode", "SWARM_LLM_RUNTIME_MODE"} {
		if !stringSliceContains(selector.RetiredNonAuthoritativeSelectors, want) {
			t.Fatalf("retired selectors missing %q: %#v", want, selector.RetiredNonAuthoritativeSelectors)
		}
	}
	if !joinedContains(selector.Rules, "Environment variables never select the backend profile") {
		t.Fatalf("selector rules do not retire env backend selection: %#v", selector.Rules)
	}
	profiles := authority.BackendProfileIdentity.ActiveBackendProfiles
	for _, oldID := range []string{"api", "cli_test"} {
		if _, ok := profiles[oldID]; ok {
			t.Fatalf("old backend id %q still active in source authority profiles: %#v", oldID, profiles)
		}
	}
	if got := profiles["anthropic"]; got.Provider != "anthropic" || got.ProviderContractRuntimeMode != "api" || !stringSliceContains(got.LegacyBackendIDs, "api") {
		t.Fatalf("anthropic profile = %#v", got)
	}
	if got := profiles["claude_cli"]; got.Provider != "claude" || got.ProviderContractRuntimeMode != "cli_test" || !stringSliceContains(got.LegacyBackendIDs, "cli_test") {
		t.Fatalf("claude_cli profile = %#v", got)
	}
	if got := profiles["openai_compatible"]; got.Provider != "openai_compatible" || got.ProviderContractRuntimeMode != "openai_compatible" || got.EndpointSource.EnvVar != "" || !strings.Contains(got.EndpointSource.Rule, "SWARM_OPENAI_COMPATIBLE_BASE_URL is retired") {
		t.Fatalf("openai_compatible profile = %#v", got)
	}
	for backend, wantKey := range map[string]string{
		"anthropic":         "ANTHROPIC_API_KEY",
		"claude_cli":        "CLAUDE_CODE_OAUTH_TOKEN",
		"openai_compatible": "OPENAI_COMPATIBLE_API_KEY",
		"openai_responses":  "OPENAI_API_KEY",
	} {
		source := profiles[backend].CredentialSource
		if source.SecretKey != wantKey || source.Owner != "swarm secrets" || source.LegacyEnvVar != wantKey || !source.Required || !strings.Contains(source.ResolutionRule, "process env is deprecated diagnostic shadow") {
			t.Fatalf("%s credential source = %#v, want swarm secrets source key %s with deprecated env shadow only", backend, source, wantKey)
		}
	}
	openAIResponses := profiles["openai_responses"]
	if openAIResponses.Provider != "openai" || openAIResponses.ProviderContractRuntimeMode != "openai_responses" || openAIResponses.Transport != "api" {
		t.Fatalf("openai_responses profile = %#v", openAIResponses)
	}
	if openAIResponses.CredentialSource.SecretKey != "OPENAI_API_KEY" || openAIResponses.CredentialSource.Owner != "swarm secrets" || openAIResponses.CredentialSource.LegacyEnvVar != "OPENAI_API_KEY" || !openAIResponses.CredentialSource.Required || !strings.Contains(openAIResponses.CredentialSource.ResolutionRule, "must not beat a stored secret") {
		t.Fatalf("openai_responses credential source = %#v", openAIResponses.CredentialSource)
	}
	if openAIResponses.EndpointSource.ConfigKey != "llm.openai_responses.base_url" || openAIResponses.EndpointSource.EnvVar != "" || openAIResponses.EndpointSource.Required || openAIResponses.EndpointSource.BuiltInDefault != "https://api.openai.com/v1" {
		t.Fatalf("openai_responses endpoint source = %#v", openAIResponses.EndpointSource)
	}
	if _, ok := authority.BackendProfileIdentity.PendingBackendProfiles["openai_responses"]; ok {
		t.Fatalf("openai_responses must not remain pending after #1224 activation: %#v", authority.BackendProfileIdentity.PendingBackendProfiles)
	}
	openAIRejectedTarget := authority.BackendProfileIdentity.RejectedTargetNames["openai"]
	for _, want := range []string{"must not be accepted", "openai_responses", "#1224"} {
		if !strings.Contains(strings.ToLower(openAIRejectedTarget), strings.ToLower(want)) {
			t.Fatalf("openai rejected target missing %q: %#v", want, authority.BackendProfileIdentity.RejectedTargetNames)
		}
	}
	if !strings.Contains(strings.ToLower(authority.BackendProfileIdentity.RejectedTargetNames["openai"]), "provider identity") {
		t.Fatalf("openai rejected target missing design decision: %#v", authority.BackendProfileIdentity.RejectedTargetNames)
	}
	responses := authority.NativeOpenAIResponsesSourceAuthority
	if responses.PromotedBy != "#1229" || responses.ImplementationStatus != "runtime_adapter_activated_first_subset" {
		t.Fatalf("native responses source authority status = %#v", responses)
	}
	if responses.TargetBackendProfileID != "openai_responses" || responses.ProviderIdentity != "openai" || responses.ActivationStatus != "active_backend_profile_runtime_adapter_promoted_by_1224" {
		t.Fatalf("native responses identity = %#v", responses)
	}
	for _, want := range []string{"api-reference/responses", "docs/models", "function-calling", "streaming-responses"} {
		if !joinedContains(responses.AuthoritativeRefs, want) {
			t.Fatalf("native responses authoritative refs missing %q: %#v", want, responses.AuthoritativeRefs)
		}
	}
	for _, want := range []string{"openai_responses is accepted", "openai remains a rejected", "openai_compatible remains Chat Completions-compatible"} {
		if !joinedContains(responses.ActiveSelectorPolicy, want) {
			t.Fatalf("native responses selector policy missing %q: %#v", want, responses.ActiveSelectorPolicy)
		}
	}
	protocol := responses.ProtocolFamily
	if protocol.Name != "native_openai_responses" || protocol.EndpointPath != "/responses" || protocol.Transport != "api" {
		t.Fatalf("native responses protocol = %#v", protocol)
	}
	for _, want := range []string{"Platform-managed text transcript", "function", "server-sent events", "previous_response_id"} {
		if !strings.Contains(protocol.InputScope+protocol.ToolScope+protocol.StreamingScope+protocol.SessionContinuity, want) {
			t.Fatalf("native responses protocol scope missing %q: %#v", want, protocol)
		}
	}
	if responses.CredentialAndEndpointPolicy.CredentialSource.SecretKey != "OPENAI_API_KEY" || responses.CredentialAndEndpointPolicy.CredentialSource.Owner != "swarm secrets" || responses.CredentialAndEndpointPolicy.CredentialSource.LegacyEnvVar != "OPENAI_API_KEY" || !responses.CredentialAndEndpointPolicy.CredentialSource.Required {
		t.Fatalf("native responses credential policy = %#v", responses.CredentialAndEndpointPolicy.CredentialSource)
	}
	if responses.CredentialAndEndpointPolicy.EndpointSource.ConfigKey != "llm.openai_responses.base_url" || responses.CredentialAndEndpointPolicy.EndpointSource.EnvVar != "" || responses.CredentialAndEndpointPolicy.EndpointSource.BuiltInDefault != "https://api.openai.com/v1" {
		t.Fatalf("native responses endpoint policy = %#v", responses.CredentialAndEndpointPolicy.EndpointSource)
	}
	for alias, model := range map[string]string{"cheap": "gpt-5.4-nano", "regular": "gpt-5.4", "frontier": "gpt-5.5"} {
		if got := strings.TrimSpace(responses.ModelAliasDefaults[alias]); got != model {
			t.Fatalf("native responses model alias %s = %q, want %q; aliases=%#v", alias, got, model, responses.ModelAliasDefaults)
		}
	}
	if !strings.Contains(responses.ModelAliasRule, "consumed by") || !strings.Contains(responses.ModelAliasRule, "openai_responses") {
		t.Fatalf("native responses model alias rule does not record active consumption: %s", responses.ModelAliasRule)
	}
	for _, want := range []string{"function tools", "function-call output", "raw Responses payloads", "previous_response_id", "backend profile id openai_responses", "exact provider-reported usage"} {
		combined := strings.Join(responses.ProviderContractExpectations.ToolSchema, " ") +
			strings.Join(responses.ProviderContractExpectations.ResponseNormalization, " ") +
			strings.Join(responses.ProviderContractExpectations.SessionLifecycle, " ") +
			strings.Join(responses.ProviderContractExpectations.Persistence, " ") +
			strings.Join(responses.ProviderContractExpectations.Budget, " ")
		if !strings.Contains(combined, want) {
			t.Fatalf("native responses provider contract expectations missing %q: %#v", want, responses.ProviderContractExpectations)
		}
	}
	if !strings.Contains(responses.StoreProfileImplication, "schema/check admission") || !strings.Contains(responses.StoreProfileImplication, "openai_responses") {
		t.Fatalf("native responses store implication must record active persisted admission: %s", responses.StoreProfileImplication)
	}
	for _, want := range []string{"#1224", "previous_response_id", "Provider matrix"} {
		if !joinedContains(responses.SplitBoundaries, want) {
			t.Fatalf("native responses split boundaries missing %q: %#v", want, responses.SplitBoundaries)
		}
	}
	models := authority.ModelAliasAuthority
	if models.ContractField != "model" || models.Replaces != "model_tier" || models.AliasConfigKey != "llm.models" {
		t.Fatalf("model alias authority = %#v", models)
	}
	for _, want := range []string{"cheap", "regular", "frontier"} {
		if !stringSliceContains(models.BuiltInAliases, want) {
			t.Fatalf("model aliases missing %q: %#v", want, models.BuiltInAliases)
		}
	}
	for alias, backendModels := range map[string]map[string]string{
		"cheap":    {"anthropic": "claude-3-5-haiku", "claude_cli": "haiku", "openai_compatible": "gpt-compatible-mini", "openai_responses": "gpt-5.4-nano"},
		"regular":  {"anthropic": "claude-3-5-sonnet", "claude_cli": "sonnet", "openai_compatible": "gpt-compatible", "openai_responses": "gpt-5.4"},
		"frontier": {"anthropic": "claude-3-opus", "claude_cli": "opus", "openai_compatible": "gpt-compatible-frontier", "openai_responses": "gpt-5.5"},
	} {
		for backend, model := range backendModels {
			if got := strings.TrimSpace(models.BuiltInModelDefaults[alias][backend]); got != model {
				t.Fatalf("built-in model default %s/%s = %q, want %q; all defaults=%#v", alias, backend, got, model, models.BuiltInModelDefaults)
			}
		}
	}
	for _, want := range []string{"free-form", "well-formedness", "selected-backend", "write time", "MUST NOT reconstruct"} {
		if !strings.Contains(models.AliasVocabularyDeclaration+models.ResolutionRule+models.VerifyRule+models.AuditRule, want) {
			t.Fatalf("model alias authority missing %q:\n%#v", want, models)
		}
	}
	for _, want := range []string{"ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN", "OPENAI_COMPATIBLE_API_KEY", "OPENAI_API_KEY"} {
		if !stringSliceContains(authority.CredentialAndConfigPolicy.ProviderSecretKeys, want) {
			t.Fatalf("provider secret keys missing %q: %#v", want, authority.CredentialAndConfigPolicy.ProviderSecretKeys)
		}
		if !stringSliceContains(authority.CredentialAndConfigPolicy.DeprecatedProviderEnvShadowSources, want) {
			t.Fatalf("deprecated provider env shadow sources missing %q: %#v", want, authority.CredentialAndConfigPolicy.DeprecatedProviderEnvShadowSources)
		}
		if stringSliceContains(authority.CredentialAndConfigPolicy.SecretEnvSources, want) {
			t.Fatalf("provider key %q still promoted as secret env source: %#v", want, authority.CredentialAndConfigPolicy.SecretEnvSources)
		}
	}
	if stringSliceContains(authority.CredentialAndConfigPolicy.SecretEnvSources, "SWARM_TOOL_GATEWAY_TOKEN") {
		t.Fatalf("secret env sources still promote retired gateway token env: %#v", authority.CredentialAndConfigPolicy.SecretEnvSources)
	}
	for _, want := range []string{"backend selection", "provider model alias maps"} {
		if !stringSliceContains(authority.CredentialAndConfigPolicy.RuntimeConfigCanonicalFor, want) {
			t.Fatalf("runtime config canonical list missing %q: %#v", want, authority.CredentialAndConfigPolicy.RuntimeConfigCanonicalFor)
		}
	}
	for _, want := range []string{"anthropic", "claude_cli", "openai_compatible", "openai_responses", "api and cli_test", "model", "write time"} {
		if !joinedContains(authority.PersistenceRules, want) {
			t.Fatalf("persistence rules missing %q: %#v", want, authority.PersistenceRules)
		}
	}
	for _, want := range []string{"#1127", "#1128", "#1129", "#1130", "#1229", "#1224", "active backend runtime implementation"} {
		if !joinedContains(authority.SplitBoundaries, want) {
			t.Fatalf("split boundaries missing %q: %#v", want, authority.SplitBoundaries)
		}
	}
}

func TestPlatformSpecContractPlatformSpecPathResolutionPromoted(t *testing.T) {
	spec := loadCLIContractPlatformSpecPathResolutionSpec(t)
	if strings.TrimSpace(spec.PromotedBy) != "#844" {
		t.Fatalf("contract path promoted_by = %q, want #844", spec.PromotedBy)
	}
	if strings.TrimSpace(spec.ImplementationStatus) != "implemented" {
		t.Fatalf("contract path implementation_status = %q, want implemented", spec.ImplementationStatus)
	}
	if !strings.Contains(spec.CanonicalOwner, "cli_specification.foundations.contract_platform_spec_path_resolution") {
		t.Fatalf("canonical owner does not point at promoted section: %s", spec.CanonicalOwner)
	}
	for _, want := range []string{"swarm verify", "swarm serve", "local foreground `swarm run start`"} {
		if !stringSliceContains(spec.AppliesTo, want) {
			t.Fatalf("applies_to missing %q: %#v", want, spec.AppliesTo)
		}
	}
	for _, want := range []string{"swarm run start --connect", "swarm run start --reattach"} {
		if !stringSliceContains(spec.NotAppliesTo, want) {
			t.Fatalf("not_applies_to missing %q: %#v", want, spec.NotAppliesTo)
		}
	}
	wantContractsOrder := []string{"--contracts", "SWARM_CONTRACTS_PATH", "config paths.contracts_path", "repo contracts/package.yaml"}
	if len(spec.ContractsPath.SourceOrder) != len(wantContractsOrder) {
		t.Fatalf("contracts source order = %#v, want %#v", spec.ContractsPath.SourceOrder, wantContractsOrder)
	}
	for i, want := range wantContractsOrder {
		if spec.ContractsPath.SourceOrder[i] != want {
			t.Fatalf("contracts source order[%d] = %q, want %q", i, spec.ContractsPath.SourceOrder[i], want)
		}
	}
	if spec.ContractsPath.AcceptedSources.Flag != "--contracts <path>" {
		t.Fatalf("contracts flag = %q", spec.ContractsPath.AcceptedSources.Flag)
	}
	if spec.ContractsPath.AcceptedSources.Environment != "SWARM_CONTRACTS_PATH" {
		t.Fatalf("contracts env = %q", spec.ContractsPath.AcceptedSources.Environment)
	}
	if spec.ContractsPath.AcceptedSources.ConfigKey != "paths.contracts_path" {
		t.Fatalf("contracts config key = %q", spec.ContractsPath.AcceptedSources.ConfigKey)
	}
	if !strings.Contains(spec.ContractsPath.RejectedSources["SWARM_CONTRACTS_DIR"], "Not a CLI source") {
		t.Fatalf("SWARM_CONTRACTS_DIR rejection missing CLI-source rule:\n%s", spec.ContractsPath.RejectedSources["SWARM_CONTRACTS_DIR"])
	}
	wantPlatformOrder := []string{"--platform-spec", "config paths.platform_spec_path", "embedded tracked platform spec"}
	if len(spec.PlatformSpecPath.SourceOrder) != len(wantPlatformOrder) {
		t.Fatalf("platform source order = %#v, want %#v", spec.PlatformSpecPath.SourceOrder, wantPlatformOrder)
	}
	for i, want := range wantPlatformOrder {
		if spec.PlatformSpecPath.SourceOrder[i] != want {
			t.Fatalf("platform source order[%d] = %q, want %q", i, spec.PlatformSpecPath.SourceOrder[i], want)
		}
	}
	if spec.PlatformSpecPath.AcceptedSources.Flag != "--platform-spec <path>" {
		t.Fatalf("platform flag = %q", spec.PlatformSpecPath.AcceptedSources.Flag)
	}
	if spec.PlatformSpecPath.AcceptedSources.ConfigKey != "paths.platform_spec_path" {
		t.Fatalf("platform config key = %q", spec.PlatformSpecPath.AcceptedSources.ConfigKey)
	}
	if spec.PlatformSpecPath.AcceptedSources.BuiltInDefault != defaultPlatformSpecPath {
		t.Fatalf("platform default = %q, want %q", spec.PlatformSpecPath.AcceptedSources.BuiltInDefault, defaultPlatformSpecPath)
	}
	if !strings.Contains(spec.PlatformSpecPath.RejectedSources["SWARM_PLATFORM_SPEC_PATH"], "Not promoted") {
		t.Fatalf("SWARM_PLATFORM_SPEC_PATH rejection missing not-promoted rule:\n%s", spec.PlatformSpecPath.RejectedSources["SWARM_PLATFORM_SPEC_PATH"])
	}
	if !strings.Contains(spec.PlatformSpecPath.DefaultRule, "embedded") || !strings.Contains(spec.PlatformSpecPath.DefaultRule, "MUST NOT require source checkout") {
		t.Fatalf("platform default rule missing embedded portability semantics:\n%s", spec.PlatformSpecPath.DefaultRule)
	}
	for _, want := range []string{"verify", "serve", "local foreground run", "API auth/config", "connected run"} {
		if !stringSliceContains(spec.ImplementationBoundaries, want) {
			t.Fatalf("implementation boundaries missing %q: %#v", want, spec.ImplementationBoundaries)
		}
	}
}

func TestCLI_ServeVerboseFlagConsumesServeOwner(t *testing.T) {
	var captured ServeOptions
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ string, serveOpts ServeOptions) int {
		captured = serveOpts
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", "--verbose"}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("serve code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !captured.Verbose {
		t.Fatalf("serve verbose = false, want true")
	}
	if captured.Output == nil {
		t.Fatalf("serve output writer was not passed to serve owner")
	}
}

func TestCLI_ServeAbandonActiveRunsFlagConsumesServeOwner(t *testing.T) {
	var captured ServeOptions
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ string, serveOpts ServeOptions) int {
		captured = serveOpts
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", "--abandon-active-runs"}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("serve code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !captured.AbandonActiveRuns {
		t.Fatalf("serve abandon active runs = false, want true")
	}
}

func TestCLI_ServeShutdownGraceFlagConsumesServeOwner(t *testing.T) {
	var captured ServeOptions
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ string, serveOpts ServeOptions) int {
		captured = serveOpts
		return 0
	}

	var stdout, stderr bytes.Buffer
	wantGrace := 42 * time.Second
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", "--shutdown-grace", wantGrace.String()}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("serve code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.ShutdownGrace != wantGrace {
		t.Fatalf("serve shutdown grace = %s, want %s", captured.ShutdownGrace, wantGrace)
	}
}

func TestCLI_ServeShutdownGraceRejectsNonPositiveDurationBeforeOwner(t *testing.T) {
	for _, args := range [][]string{
		{"serve", "--shutdown-grace", "0s"},
		{"serve", "--shutdown-grace", "-1s"},
		{"serve", "--shutdown-grace", "not-a-duration"},
	} {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			var called atomic.Bool
			opts := defaultRootCommandOptions()
			opts.runServe = func(context.Context, string, ServeOptions) int {
				called.Store(true)
				return 0
			}

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, opts)
			if code != 2 {
				t.Fatalf("serve code = %d stderr=%s stdout=%s, want 2", code, stderr.String(), stdout.String())
			}
			if args[2] == "not-a-duration" {
				if !strings.Contains(stderr.String(), "invalid argument") {
					t.Fatalf("stderr = %q, want invalid duration parse error", stderr.String())
				}
			} else if !strings.Contains(stderr.String(), "--shutdown-grace must be a positive duration") {
				t.Fatalf("stderr = %q, want shutdown-grace validation error", stderr.String())
			}
			if called.Load() {
				t.Fatal("serve owner was called despite invalid shutdown grace")
			}
		})
	}
}

func writeServedEventPublishFollowUpFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyRootIngressServedFollowUp(t)
}

// Keep the selected-fork proof independent from periodic recovery replay.

func servedEventPublishFixtureBundleHash(t *testing.T, contractsPath string) string {
	t.Helper()
	bundle := loadWorkflowValidationBundleAt(t, contractsPath)
	bundleHash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		t.Fatalf("BundleHash(%s): %v", contractsPath, err)
	}
	return bundleHash
}

// servedProofPollDeadline bounds poll-until-state helpers in served-path
// proofs. Success exits early; the margin absorbs full-suite load where
// Postgres-served runs are green in seconds isolated but can lag under
// concurrent package load.

func TestCLI_NoArgCommandsRejectUnexpectedArgs(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{name: "serve", args: []string{"serve", "unexpected"}},
		{name: "version", args: []string{"version", "unexpected"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := executeRootCommand(context.Background(), t.TempDir(), tc.args, &stdout, &stderr)
			if code != 2 {
				t.Fatalf("%s code = %d, want 2 stdout=%s stderr=%s", tc.name, code, stdout.String(), stderr.String())
			}
			if strings.TrimSpace(stdout.String()) != "" {
				t.Fatalf("%s stdout = %q, want empty", tc.name, stdout.String())
			}
			if !strings.Contains(stderr.String(), "unknown command") {
				t.Fatalf("%s stderr = %q, want Cobra arg validation error", tc.name, stderr.String())
			}
		})
	}
}

func TestCLI_VerifyPreservesLocalContractCarveOut(t *testing.T) {
	var stdout, stderr bytes.Buffer
	missingContracts := filepath.Join(t.TempDir(), "missing")
	code := executeRootCommand(context.Background(), t.TempDir(), []string{"verify", "--contracts", missingContracts}, &stdout, &stderr)
	if code != CLIExitValidation {
		t.Fatalf("verify code = %d, want %d stderr=%s stdout=%s", code, CLIExitValidation, stderr.String(), stdout.String())
	}
	if strings.TrimSpace(stdout.String()) != "" {
		t.Fatalf("verify stdout = %q, want empty on error", stdout.String())
	}
	if !strings.Contains(stderr.String(), "ERROR: no Swarm package manifest was found") {
		t.Fatalf("verify stderr = %q, want local contract resolution failure", stderr.String())
	}
}

func waitRunStatusEventSettlement(t *testing.T, db *sql.DB, runID string, wantEvents int) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var (
			eventCount       int
			activeDeliveries int
		)
		err := db.QueryRowContext(ctx, `
			SELECT
				(SELECT COUNT(*) FROM events WHERE run_id = $1::uuid),
				(SELECT COUNT(*) FROM event_deliveries WHERE run_id = $1::uuid AND status IN ('pending', 'in_progress'))
		`, runID).Scan(&eventCount, &activeDeliveries)
		if err == nil && eventCount >= wantEvents && activeDeliveries == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s did not settle after release: last err=%v event_count=%d want_events=%d active_deliveries=%d", runID, err, eventCount, wantEvents, activeDeliveries)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestProjectRunOperationalStatus_UsesDeliveryLifecycleWhenRunIsOperationallyStalled(t *testing.T) {
	report := store.RunDebugReport{
		RunTableStatus: "running",
		LastEventAt:    time.Unix(1700000000, 0).UTC(),
		Deliveries: []store.RunDebugDeliveryCount{
			{SubscriberID: "agent-1", Status: "delivered", Count: 2},
			{SubscriberID: "agent-2", Status: "failed", Count: 1},
		},
	}

	got := store.ProjectRunOperationalStatus(report)
	if got.State != "stalled" {
		t.Fatalf("state = %q, want stalled", got.State)
	}
	if got.BlockingLayer != "delivery_lifecycle" {
		t.Fatalf("blocking_layer = %q, want delivery_lifecycle", got.BlockingLayer)
	}
	if got.BlockingReason != "no_active_deliveries" {
		t.Fatalf("blocking_reason = %q, want no_active_deliveries", got.BlockingReason)
	}
}

func TestProjectRunOperationalStatus_UsesScoringOutcomeBlockingLayer(t *testing.T) {
	report := store.RunDebugReport{
		RunTableStatus: "running",
		LastEventAt:    time.Unix(1700000000, 0).UTC(),
		EventCounts: []store.RunDebugEventCount{
			{EventName: "scoring/scoring.requested", Count: 1},
		},
		Deliveries: []store.RunDebugDeliveryCount{
			{SubscriberID: "agent-1", Status: "delivered", Count: 1},
		},
	}

	got := store.ProjectRunOperationalStatus(report)
	if got.State != "stalled" {
		t.Fatalf("state = %q, want stalled", got.State)
	}
	if got.BlockingLayer != "scoring_terminal_outcome" {
		t.Fatalf("blocking_layer = %q, want scoring_terminal_outcome", got.BlockingLayer)
	}
	if got.BlockingReason != "terminal_scoring_outcome_missing" {
		t.Fatalf("blocking_reason = %q, want terminal_scoring_outcome_missing", got.BlockingReason)
	}
}

func TestProjectRunOperationalStatus_TreatsScopedShortlistAsTerminalScoringOutcome(t *testing.T) {
	report := store.RunDebugReport{
		RunTableStatus: "running",
		LastEventAt:    time.Unix(1700000000, 0).UTC(),
		EventCounts: []store.RunDebugEventCount{
			{EventName: "scoring/scoring.requested", Count: 1},
			{EventName: "scoring/vertical.shortlisted", Count: 1},
		},
		Deliveries: []store.RunDebugDeliveryCount{
			{SubscriberID: "agent-1", Status: "delivered", Count: 1},
		},
	}

	got := store.ProjectRunOperationalStatus(report)
	if got.State != "stalled" {
		t.Fatalf("state = %q, want stalled", got.State)
	}
	if got.BlockingLayer != "delivery_lifecycle" {
		t.Fatalf("blocking_layer = %q, want delivery_lifecycle", got.BlockingLayer)
	}
	if got.BlockingReason != "no_active_deliveries" {
		t.Fatalf("blocking_reason = %q, want no_active_deliveries", got.BlockingReason)
	}
}

func TestProjectRunOperationalStatus_PreservesHealthyRunningWhenActiveDeliveriesRemain(t *testing.T) {
	report := store.RunDebugReport{
		RunTableStatus: "running",
		LastEventAt:    time.Unix(1700000000, 0).UTC(),
		Deliveries: []store.RunDebugDeliveryCount{
			{SubscriberID: "agent-1", Status: "in_progress", Count: 1},
			{SubscriberID: "agent-2", Status: "delivered", Count: 1},
		},
	}

	got := store.ProjectRunOperationalStatus(report)
	if got.State != "running" {
		t.Fatalf("state = %q, want running", got.State)
	}
	if got.BlockingLayer != "" || got.BlockingReason != "" {
		t.Fatalf("unexpected blocking projection: %#v", got)
	}
}

func setPostgresEnvFromDSN(t *testing.T, dsn string) {
	t.Helper()
	connection, err := testpostgres.ParseConnection(dsn)
	if err != nil {
		t.Fatalf("parse canonical test Postgres DSN: %v", err)
	}
	parsed := connection.Parameters()
	t.Setenv("PGPASSWORD", parsed.Password)
	configPath := filepath.Join(t.TempDir(), "swarm.yaml")
	t.Setenv("SWARM_CONFIG", configPath)
	writeRuntimeConfigText(t, configPath, fmt.Sprintf(`store:
  backend: postgres
database:
  host: %q
  port: %d
  name: %q
  user: %q
  password_env: PGPASSWORD
  sslmode: %q
  pool_size: 5
llm:
  backend: claude_cli
  session:
    lock_ttl: 10s
    rotate_after_turns: 40
    rotate_on_parse_failures: 3
  claude_cli:
    command: true
    timeout: 2s
    output_format: json
`, parsed.Host, parsed.Port, parsed.Database, parsed.User, parsed.SSLMode))
}

func TestSetPostgresEnvFromDSNConsumesCanonicalTypedConnection(t *testing.T) {
	for _, raw := range []string{
		`host=127.0.0.1 port=55432 user='swarm user' password='slash\\ quote\' space' dbname='swarm db' sslmode=disable`,
		`postgres://swarm%20user:slash%5C%20quote%27%20space@127.0.0.1:55432/swarm%20db?sslmode=disable`,
	} {
		t.Run(raw[:8], func(t *testing.T) {
			setPostgresEnvFromDSN(t, raw)
			cfg, err := loadRuntimeConfig(os.Getenv("SWARM_CONFIG"))
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Database.Host != "127.0.0.1" || cfg.Database.Port != 55432 || cfg.Database.Name != "swarm db" || cfg.Database.User != "swarm user" || cfg.Database.SSLMode != "disable" {
				t.Fatalf("database config = %#v", cfg.Database)
			}
			if got := os.Getenv("PGPASSWORD"); got != `slash\ quote' space` {
				t.Fatalf("PGPASSWORD = %q", got)
			}
		})
	}
}

func TestDefaultRuntimeConfig_RejectsUnsupportedRuntimeControlEnv(t *testing.T) {
	t.Setenv("SWARM_RUNTIME_MAX_CONCURRENT_AGENTS", "4")
	cfg, err := defaultRuntimeConfig()
	if err == nil || !strings.Contains(err.Error(), "SWARM_RUNTIME_MAX_CONCURRENT_AGENTS") {
		t.Fatalf("defaultRuntimeConfig error = %v, want unsupported env rejection", err)
	}
	if cfg != nil {
		t.Fatalf("defaultRuntimeConfig cfg = %#v, want nil on unsupported env", cfg)
	}
}

func TestDefaultRuntimeConfig_RejectsRetiredLLMRuntimeModeEnv(t *testing.T) {
	t.Setenv("SWARM_LLM_RUNTIME_MODE", "api")
	cfg, err := defaultRuntimeConfig()
	if err == nil || !strings.Contains(err.Error(), "--backend") || !strings.Contains(err.Error(), "llm.backend") {
		t.Fatalf("defaultRuntimeConfig error = %v, want retired runtime mode env guidance", err)
	}
	if cfg != nil {
		t.Fatalf("defaultRuntimeConfig cfg = %#v, want nil on retired env", cfg)
	}
}

func TestDefaultRuntimeConfig_RejectsRetiredLLMBackendEnv(t *testing.T) {
	t.Setenv("SWARM_LLM_BACKEND", "cli_test")
	cfg, err := defaultRuntimeConfig()
	if err == nil || !strings.Contains(err.Error(), "SWARM_LLM_BACKEND") || !strings.Contains(err.Error(), "--backend") {
		t.Fatalf("defaultRuntimeConfig error = %v, want retired backend env rejection", err)
	}
	if cfg != nil {
		t.Fatalf("defaultRuntimeConfig cfg = %#v, want nil on retired env", cfg)
	}
}

func TestDefaultRuntimeConfig_DoesNotInferLLMBackendFromCredentials(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "test-key")
	t.Setenv("OPENAI_API_KEY", "openai-test-key")
	t.Setenv("OPENAI_COMPATIBLE_API_KEY", "compatible-test-key")
	cfg, err := defaultRuntimeConfig()
	if err != nil {
		t.Fatalf("defaultRuntimeConfig: %v", err)
	}
	if cfg.LLM.Backend != "anthropic" {
		t.Fatalf("llm backend = %q, want canonical default anthropic", cfg.LLM.Backend)
	}
}

func TestDefaultRuntimeConfig_IgnoresRetiredRuntimeLLMConfigEnv(t *testing.T) {
	for key, value := range map[string]string{
		"SWARM_RUNTIME_RECOVERY_ON_STARTUP":          "true",
		"SWARM_LLM_SESSION_LOCK_TTL":                 "1s",
		"SWARM_LLM_SESSION_ROTATE_AFTER_TURNS":       "2",
		"SWARM_LLM_SESSION_ROTATE_ON_PARSE_FAILURES": "2",
		"SWARM_CLAUDE_API_MAX_RETRIES":               "7",
		"SWARM_CLAUDE_API_RETRY_BACKOFF":             "7s",
		"SWARM_CLAUDE_CLI_COMMAND":                   "false",
		"SWARM_CLAUDE_CLI_TIMEOUT":                   "1s",
		"SWARM_CLAUDE_CLI_OUTPUT_FORMAT":             "bad",
		"SWARM_CLAUDE_CLI_RETRIES":                   "7",
		"SWARM_CLAUDE_CLI_NO_SESSION_PERSISTENCE":    "true",
		"SWARM_CLAUDE_CLI_USE_TMUX":                  "true",
		"SWARM_CLAUDE_TIMEOUT_SECONDS":               "1",
	} {
		t.Setenv(key, value)
	}

	cfg, err := defaultRuntimeConfig()
	if err != nil {
		t.Fatalf("defaultRuntimeConfig: %v", err)
	}
	if cfg.Runtime.RecoveryOnStartup {
		t.Fatalf("recovery_on_startup = true, want built-in false")
	}
	if cfg.LLM.Session.LockTTL != 10*time.Second || cfg.LLM.Session.RotateAfterTurns != 40 || cfg.LLM.Session.RotateOnParseFailures != 3 {
		t.Fatalf("session defaults = %#v, want built-ins", cfg.LLM.Session)
	}
	if cfg.LLM.ClaudeCLI.Command != "claude" ||
		cfg.LLM.ClaudeCLI.Timeout != time.Hour ||
		cfg.LLM.ClaudeCLI.OutputFormat != "stream-json" ||
		cfg.LLM.ClaudeCLI.Retries != 1 ||
		cfg.LLM.ClaudeCLI.NoSessionPersistence ||
		cfg.LLM.ClaudeCLI.UseTMux {
		t.Fatalf("claude_cli defaults = %#v, want built-ins", cfg.LLM.ClaudeCLI)
	}
}

func TestDefaultRuntimeConfig_RejectsRetiredOpenAICompatibleBaseURLEnv(t *testing.T) {
	t.Setenv("SWARM_OPENAI_COMPATIBLE_BASE_URL", "https://example.test/v1")
	cfg, err := defaultRuntimeConfig()
	if err == nil || !strings.Contains(err.Error(), "SWARM_OPENAI_COMPATIBLE_BASE_URL") || !strings.Contains(err.Error(), "llm.openai_compatible.base_url") {
		t.Fatalf("defaultRuntimeConfig error = %v, want base URL env retirement", err)
	}
	if cfg != nil {
		t.Fatalf("defaultRuntimeConfig cfg = %#v, want nil on retired env", cfg)
	}
}

func TestLoadRuntimeConfigWithOptions_PreservesRuntimeLLMTypedConfigValues(t *testing.T) {
	for key, value := range map[string]string{
		"SWARM_RUNTIME_RECOVERY_ON_STARTUP":          "false",
		"SWARM_LLM_SESSION_LOCK_TTL":                 "1s",
		"SWARM_LLM_SESSION_ROTATE_AFTER_TURNS":       "2",
		"SWARM_LLM_SESSION_ROTATE_ON_PARSE_FAILURES": "2",
		"SWARM_CLAUDE_CLI_COMMAND":                   "false",
		"SWARM_CLAUDE_CLI_TIMEOUT":                   "1s",
		"SWARM_CLAUDE_CLI_OUTPUT_FORMAT":             "stream-json",
		"SWARM_CLAUDE_TIMEOUT_SECONDS":               "1",
	} {
		t.Setenv(key, value)
	}

	repo := t.TempDir()
	configPath := filepath.Join(repo, "runtime.yaml")
	writeRuntimeConfigText(t, configPath, strings.Join([]string{
		"runtime:",
		"  recovery_on_startup: true",
		"llm:",
		"  backend: claude_cli",
		"  session:",
		"    lock_ttl: 22s",
		"    rotate_after_turns: 55",
		"    rotate_on_parse_failures: 9",
		"  claude_cli:",
		"    command: echo",
		"    timeout: 44s",
		"    output_format: json",
	}, "\n")+"\n")

	result, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: configPath})
	if err != nil {
		t.Fatalf("LoadRuntimeConfigWithOptions: %v", err)
	}
	cfg := result.Config
	if !cfg.Runtime.RecoveryOnStartup {
		t.Fatalf("runtime.recovery_on_startup = false, want true from config")
	}
	if cfg.LLM.Session.LockTTL != 22*time.Second || cfg.LLM.Session.RotateAfterTurns != 55 || cfg.LLM.Session.RotateOnParseFailures != 9 {
		t.Fatalf("session config = %#v, want typed config values", cfg.LLM.Session)
	}
	if cfg.LLM.ClaudeCLI.Command != "echo" || cfg.LLM.ClaudeCLI.Timeout != 44*time.Second || cfg.LLM.ClaudeCLI.OutputFormat != "json" {
		t.Fatalf("claude_cli config = %#v, want typed config values", cfg.LLM.ClaudeCLI)
	}
}

func TestLoadRuntimeConfigWithOptions_UsesSharedDiscoveryAndBackendPrecedence(t *testing.T) {
	repo := t.TempDir()
	localDir := filepath.Join(repo, ".swarm")
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		t.Fatalf("mkdir local config dir: %v", err)
	}
	writeRuntimeConfigText(t, filepath.Join(localDir, "swarm.yaml"), strings.Join([]string{
		"llm:",
		"  backend: claude_cli",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
		"  claude_cli:",
		"    command: true",
		"    timeout: 2s",
		"    output_format: json",
	}, "\n")+"\n")
	explicitPath := filepath.Join(repo, "runtime.yaml")
	writeRuntimeConfigText(t, explicitPath, strings.Join([]string{
		"llm:",
		"  backend: anthropic",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
		"  openai_compatible:",
		"    base_url: https://example.test/v1",
	}, "\n")+"\n")

	result, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo})
	if err != nil {
		t.Fatalf("load discovered local-operator config: %v", err)
	}
	if result.Config.LLM.Backend != "claude_cli" || result.Source != string(unifiedLayerLocalOperator) {
		t.Fatalf("local-operator config result = source=%s backend=%s, want local_operator_config claude_cli", result.Source, result.Config.LLM.Backend)
	}

	result, err = LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: "runtime.yaml", BackendOverride: "openai_compatible"})
	if err != nil {
		t.Fatalf("load explicit config with backend override: %v", err)
	}
	if result.Config.LLM.Backend != "openai_compatible" || result.Source != string(unifiedLayerExplicit) || filepath.Clean(result.Path) != filepath.Clean(explicitPath) {
		t.Fatalf("explicit config result = %#v backend=%s", result, result.Config.LLM.Backend)
	}
}

func TestLoadRuntimeConfigWithOptions_BackendOverrideSkipsOverriddenProfileValidation(t *testing.T) {
	for _, tt := range []struct {
		name string
		body []string
	}{
		{
			name: "openai-compatible-without-required-fields",
			body: []string{
				"llm:",
				"  backend: openai_compatible",
				"  session:",
				"    lock_ttl: 10s",
				"    rotate_after_turns: 40",
				"    rotate_on_parse_failures: 3",
			},
		},
		{
			name: "claude-cli-without-command",
			body: []string{
				"llm:",
				"  backend: claude_cli",
				"  session:",
				"    lock_ttl: 10s",
				"    rotate_after_turns: 40",
				"    rotate_on_parse_failures: 3",
				"  claude_cli:",
				"    output_format: json",
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			repo := t.TempDir()
			writeRuntimeConfigText(t, filepath.Join(repo, "runtime.yaml"), strings.Join(tt.body, "\n")+"\n")

			result, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{
				RepoRoot:        repo,
				ExplicitPath:    "runtime.yaml",
				BackendOverride: "anthropic",
			})
			if err != nil {
				t.Fatalf("LoadRuntimeConfigWithOptions: %v", err)
			}
			if result.Config.LLM.Backend != "anthropic" {
				t.Fatalf("llm backend = %q, want anthropic override", result.Config.LLM.Backend)
			}
		})
	}
}

func TestLoadRuntimeConfigWithOptions_RejectsRetiredModelEnvForConfiguredPaths(t *testing.T) {
	originalExecutablePath := runtimeConfigExecutablePath
	t.Cleanup(func() { runtimeConfigExecutablePath = originalExecutablePath })
	t.Setenv("SWARM_CLAUDE_DEFAULT_MODEL", "claude-test")

	configBody := strings.Join([]string{
		"llm:",
		"  backend: anthropic",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
	}, "\n") + "\n"

	for _, tt := range []struct {
		name  string
		setup func(t *testing.T) RuntimeConfigLoadOptions
	}{
		{
			name: "explicit",
			setup: func(t *testing.T) RuntimeConfigLoadOptions {
				t.Helper()
				repo := t.TempDir()
				writeRuntimeConfigText(t, filepath.Join(repo, "runtime.yaml"), configBody)
				return RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: "runtime.yaml"}
			},
		},
		{
			name: "executable-adjacent",
			setup: func(t *testing.T) RuntimeConfigLoadOptions {
				t.Helper()
				exeDir := t.TempDir()
				runtimeConfigExecutablePath = func() (string, error) {
					return filepath.Join(exeDir, "swarm"), nil
				}
				writeRuntimeConfigText(t, filepath.Join(exeDir, "config.yaml"), configBody)
				return RuntimeConfigLoadOptions{RepoRoot: t.TempDir()}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadRuntimeConfigWithOptions(tt.setup(t))
			if err == nil || !strings.Contains(err.Error(), "SWARM_CLAUDE_DEFAULT_MODEL") || !strings.Contains(err.Error(), "llm.models") {
				t.Fatalf("LoadRuntimeConfigWithOptions error = %v, want retired model env guidance", err)
			}
		})
	}
}

func TestLoadRuntimeConfigWithOptions_RejectsLegacyBackendBeforeOverride(t *testing.T) {
	repo := t.TempDir()
	configPath := filepath.Join(repo, "runtime.yaml")
	writeRuntimeConfigText(t, configPath, strings.Join([]string{
		"llm:",
		"  backend: api",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
	}, "\n")+"\n")
	_, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: "runtime.yaml", BackendOverride: "claude_cli"})
	if err == nil || !strings.Contains(err.Error(), "unsupported llm backend profile") {
		t.Fatalf("LoadRuntimeConfigWithOptions error = %v, want legacy backend rejection", err)
	}
}

func TestLoadRuntimeConfig_RejectsUnsupportedRuntimeControlsFromFile(t *testing.T) {
	cfgText := strings.Join([]string{
		"runtime:",
		"  max_concurrent_agents: 4",
		"llm:",
		"  backend: anthropic",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
	}, "\n") + "\n"
	p := filepath.Join(t.TempDir(), "swarm.yaml")
	if err := os.WriteFile(p, []byte(cfgText), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if _, err := loadRuntimeConfig(p); err == nil || !strings.Contains(err.Error(), "runtime.max_concurrent_agents") {
		t.Fatalf("loadRuntimeConfig error = %v, want unsupported runtime control rejection", err)
	}
}

func runVerifyCommandWithContractsForTest(t *testing.T, ctx context.Context, repo, contractsPath string, out *bytes.Buffer) int {
	t.Helper()
	return runVerifyCommandWithContractsOutputForTest(t, ctx, repo, contractsPath, out, out)
}

func runVerifyCommandWithContractsOutputForTest(t *testing.T, ctx context.Context, repo, contractsPath string, out, errOut *bytes.Buffer) int {
	t.Helper()
	opts := defaultVerifyCommandOptions()
	opts.contractsPath = contractsPath
	opts.configPath = writeTestVerifyRuntimeConfig(t)
	return runVerifyCommandWithOutput(ctx, repo, opts, out, errOut)
}

func TestRunVerifyCommand_BadContractsPath(t *testing.T) {
	cases := []struct {
		name string
		path string
	}{
		{name: "missing absolute path", path: filepath.Join(t.TempDir(), "missing")},
		{name: "explicit child path under bundle", path: filepath.Join(RepoRoot(), "tests", "tier8-boot-verification", "test-boot-success", "zzz-not-a-real-dir")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			code := runVerifyCommandWithContractsForTest(t, context.Background(), RepoRoot(), tc.path, &buf)
			if code == 0 {
				t.Fatal("expected non-zero exit code")
			}
			out := buf.String()
			if !strings.Contains(out, "ERROR: no Swarm package manifest was found") {
				t.Fatalf("output = %q", out)
			}
			if !strings.Contains(out, tc.path) {
				t.Fatalf("output does not name explicit path %q:\n%s", tc.path, out)
			}
		})
	}
}

func TestRunVerifyCommandFormatsPreBootLoaderDiagnostics(t *testing.T) {
	tests := []struct {
		name     string
		write    func(t *testing.T, root string)
		wants    []string
		notWants []string
	}{
		{
			name: "package yaml syntax",
			write: func(t *testing.T, root string) {
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: invalid-yaml
flows:
  - [broken
`)
			},
			wants: []string{
				"ERROR: contract YAML could not be parsed.",
				"Location:",
				"package.yaml",
				"Remediation: Fix the YAML syntax, then run the command again.",
			},
			notWants: []string{"yaml: line", "did not find expected", "parse ", "load Swarm contracts", "resolve contracts"},
		},
		{
			name: "package document mapping shape",
			write: func(t *testing.T, root string) {
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `invalid-package-shape`)
			},
			wants: []string{
				"ERROR: package.yaml must be a mapping.",
				"Location:",
				"package.yaml:package.yaml",
				"Remediation: Use a package.yaml mapping with fields like name, version, flows, and packages.",
			},
			notWants: []string{"package.yaml must be a mapping\n", "parse ", "load Swarm contracts", "resolve contracts"},
		},
		{
			name: "schema document mapping shape",
			write: func(t *testing.T, root string) {
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: invalid-schema-shape
version: "1.0.0"
flows: []
`)
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `invalid-schema-shape`)
			},
			wants: []string{
				"ERROR: schema.yaml must be a mapping.",
				"Location:",
				"schema.yaml:schema.yaml",
				"Remediation: Use a schema.yaml mapping with fields like name, states, pins, and entity.",
			},
			notWants: []string{"flow schema document must be a mapping", "parse ", "load Swarm contracts", "resolve contracts"},
		},
		{
			name: "schema field valid options",
			write: func(t *testing.T, root string) {
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: invalid-schema-field
version: "1.0.0"
flows: []
`)
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: invalid-schema-field
bogus: true
`)
			},
			wants: []string{
				"ERROR: schema field \"bogus\" is not supported.",
				"Location:",
				"schema.yaml:schema",
				"Valid options:",
				"auto_emit_on_create",
				"terminal_states",
				"Remediation: Use one of the supported schema fields.",
			},
			notWants: []string{"UNDEFINED-FIELD", "not in platform spec", "load Swarm contracts", "resolve contracts"},
		},
		{
			name: "stage field valid options",
			write: func(t *testing.T, root string) {
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: invalid-stage-field
version: "1.0.0"
flows: []
`)
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: invalid-stage-field
stages:
  queued:
    surprise: true
`)
			},
			wants: []string{
				"ERROR: stage field \"surprise\" is not supported.",
				"Location:",
				"schema.yaml:stage",
				"Valid options:",
				"description",
				"initial",
				"terminal",
				"Remediation: Use one of the supported stage fields.",
			},
			notWants: []string{"UNDEFINED-FIELD", "not in platform spec", "load Swarm contracts", "resolve contracts"},
		},
		{
			name: "package flows entries shape",
			write: func(t *testing.T, root string) {
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: invalid-package-flow-shape
version: "1.0.0"
flows:
  - child
`)
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `name: invalid-package-flow-shape`)
			},
			wants: []string{
				"ERROR: package.yaml flows entries must be mappings with id, flow, and optional mode.",
				"Location:",
				"package.yaml:package.yaml.flows",
				"Remediation: Use entries like `flows: [{id: child, flow: child, mode: child}]`.",
			},
			notWants: []string{"yaml: unmarshal errors", "cannot unmarshal", "contracts.ProjectFlowRef", "load Swarm contracts", "resolve contracts"},
		},
		{
			name: "package requires field valid options",
			write: func(t *testing.T, root string) {
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: invalid-package-requires-field
version: "1.0.0"
requires:
  surprise: true
flows: []
`)
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `name: invalid-package-requires-field`)
			},
			wants: []string{
				"ERROR: requires field \"surprise\" is not supported.",
				"Location:",
				"package.yaml:requires",
				"Valid options:",
				"inputs",
				"platform_version",
				"Remediation: Use one of the supported requires fields.",
			},
			notWants: []string{"UNDEFINED-FIELD", "not in platform spec", "load Swarm contracts", "resolve contracts"},
		},
		{
			name: "agent field valid options",
			write: func(t *testing.T, root string) {
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: invalid-agent-field
version: "1.0.0"
flows: []
`)
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `name: invalid-agent-field`)
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), `
reviewer:
  role: reviewer
  model: regular
  subscriptions: [item.received]
  surprise: true
`)
			},
			wants: []string{
				"ERROR: agent field \"surprise\" is not supported.",
				"Location:",
				"agents.yaml:agent",
				"Valid options:",
				"model",
				"subscriptions",
				"Remediation: Use one of the supported agent fields.",
			},
			notWants: []string{"parse ", "UNDEFINED-FIELD", "not in platform spec", "load Swarm contracts", "resolve contracts"},
		},
		{
			name: "handler field valid options",
			write: func(t *testing.T, root string) {
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: invalid-handler-mailbox-write
version: "1.0.0"
flows: []
`)
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: invalid-handler-mailbox-write
initial_state: pending
terminal_states: [done]
states: [pending, done]
pins:
  inputs:
    events: [item.received]
  outputs:
    events: [item.processed]
`)
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), "item.received:\nitem.processed: {}\n")
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
test-node:
  id: test-node
  execution_type: system_node
  subscribes_to: [item.received]
  produces: [item.processed]
  event_handlers:
    item.received:
      mailbox_write: {}
      advances_to: done
`)
			},
			wants: []string{
				"ERROR: handler field \"mailbox_write\" is not supported.",
				"Valid options:",
				"action",
				"on_success",
				"Remediation: Use the supported action field",
			},
			notWants: []string{"UNDEFINED-FIELD", "load Swarm contracts", "resolve contracts"},
		},
		{
			name: "guard on fail field valid options",
			write: func(t *testing.T, root string) {
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: invalid-guard-on-fail-field
version: "1.0.0"
flows: []
`)
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: invalid-guard-on-fail-field
initial_state: pending
terminal_states: [done]
states: [pending, done]
pins:
  inputs:
    events: [item.received]
  outputs:
    events: [item.processed]
`)
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), "item.received:\nitem.processed: {}\n")
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
test-node:
  id: test-node
  execution_type: system_node
  subscribes_to: [item.received]
  produces: [item.processed]
  event_handlers:
    item.received:
      guard:
        id: gatekeeper
        on_fail:
          reject: true
      advances_to: done
`)
			},
			wants: []string{
				"ERROR: guard.on_fail field \"reject\" is not supported.",
				"Location:",
				"nodes.yaml:guard.on_fail",
				"Valid options:",
				"escalate",
				"Remediation: Use one of the supported guard.on_fail fields.",
			},
			notWants: []string{"action", "emit", "reason", "UNDEFINED-FIELD", "not in platform spec", "load Swarm contracts", "resolve contracts"},
		},
		{
			name: "node field valid options",
			write: func(t *testing.T, root string) {
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: invalid-node-field
version: "1.0.0"
flows: []
`)
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: invalid-node-field
initial_state: pending
terminal_states: [done]
states: [pending, done]
pins:
  inputs:
    events: [item.received]
  outputs:
    events: [item.processed]
`)
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), "item.received:\nitem.processed: {}\n")
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
test-node:
  id: test-node
  execution_type: system_node
  subscribes_to: [item.received]
  produces: [item.processed]
  bogus: true
`)
			},
			wants: []string{
				"ERROR: node field \"bogus\" is not supported.",
				"Location:",
				"nodes.yaml:node",
				"Valid options:",
				"event_handlers",
				"execution_type",
				"Remediation: Use one of the supported node fields.",
			},
			notWants: []string{"UNDEFINED-FIELD", "not in platform spec", "load Swarm contracts", "resolve contracts"},
		},
		{
			name: "input event pin required shape",
			write: func(t *testing.T, root string) {
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: invalid-input-pin
version: "1.0.0"
flows: []
`)
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: invalid-input-pin
pins:
  inputs:
    events:
      - event: item.received
`)
			},
			wants: []string{
				"ERROR: input event pins must name the pin or use a scalar event name.",
				"Location:",
				"schema.yaml.pins.inputs.events",
				"Remediation: Use `events: [item.received]`",
			},
			notWants: []string{"input event pin name is required", "load Swarm contracts", "resolve contracts"},
		},
		{
			name: "output event pin required shape",
			write: func(t *testing.T, root string) {
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: invalid-output-pin
version: "1.0.0"
flows: []
`)
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: invalid-output-pin
pins:
  outputs:
    events:
      - event: item.processed
`)
			},
			wants: []string{
				"ERROR: output event pins must name the pin or use a scalar event name.",
				"Location:",
				"schema.yaml.pins.outputs.events",
				"Remediation: Use `events: [item.processed]`",
			},
			notWants: []string{"output event pin name is required", "load Swarm contracts", "resolve contracts"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			tt.write(t, root)
			var buf bytes.Buffer
			code := runVerifyCommandWithContractsForTest(t, context.Background(), RepoRoot(), root, &buf)
			if code != CLIExitValidation {
				t.Fatalf("code = %d, want %d output=%s", code, CLIExitValidation, buf.String())
			}
			out := buf.String()
			for _, want := range tt.wants {
				if !strings.Contains(out, want) {
					t.Fatalf("output missing %q:\n%s", want, out)
				}
			}
			for _, notWant := range tt.notWants {
				if strings.Contains(out, notWant) {
					t.Fatalf("output contains %q:\n%s", notWant, out)
				}
			}
		})
	}
}

func TestNormalizeContractsRootExplicitPathValidation(t *testing.T) {
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `name: explicit-root`)

	got, err := NormalizeContractsRoot(root)
	if err != nil {
		t.Fatalf("normalize directory root: %v", err)
	}
	if got != root {
		t.Fatalf("root = %q, want %q", got, root)
	}

	got, err = NormalizeContractsRoot(filepath.Join(root, "package.yaml"))
	if err != nil {
		t.Fatalf("normalize package file shorthand: %v", err)
	}
	if got != root {
		t.Fatalf("root from package file = %q, want %q", got, root)
	}

	explicitChild := filepath.Join(root, "zzz-not-a-real-dir")
	if got, err := NormalizeContractsRoot(explicitChild); err == nil {
		t.Fatalf("normalize explicit child returned %q, want fail-closed error", got)
	} else if !strings.Contains(err.Error(), explicitChild) {
		t.Fatalf("error = %q, want explicit child path %q", err.Error(), explicitChild)
	}
}

func TestRunVerifyCommand_SurfacesLintEvidence(t *testing.T) {
	root := writeVerifyLintEvidenceFixture(t)

	var stdout, stderr bytes.Buffer
	code := runVerifyCommandWithContractsOutputForTest(t, context.Background(), RepoRoot(), root, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("runVerifyCommand exit code = %d, stdout = %q stderr = %q", code, stdout.String(), stderr.String())
	}
	if out := stdout.String(); !strings.Contains(out, "verify ok: contracts=") {
		t.Fatalf("verify stdout missing success marker:\n%s", out)
	} else if strings.Contains(out, "entity_reader_coverage") || strings.Contains(out, "lint_evidence") {
		t.Fatalf("verify stdout contains advisory diagnostics:\n%s", out)
	}
	errText := stderr.String()
	if !strings.Contains(errText, "[INFO] entity_reader_coverage @ root: flow root entity_type case declares field untouched with no detected internal reader coverage") {
		t.Fatalf("verify stderr missing lint evidence:\n%s", errText)
	}
	if strings.Contains(errText, "lint_evidence:") {
		t.Fatalf("verify stderr used legacy lint_evidence prefix:\n%s", errText)
	}

	opts := defaultVerifyCommandOptions()
	opts.contractsPath = root
	opts.configPath = writeTestVerifyRuntimeConfig(t)
	opts.output.asJSON = true
	stdout.Reset()
	stderr.Reset()
	code = runVerifyCommandWithOutput(context.Background(), RepoRoot(), opts, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("runVerifyCommand --json exit code = %d, stdout = %q stderr = %q", code, stdout.String(), stderr.String())
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("verify --json stderr = %q, want empty", stderr.String())
	}
	verifyJSON := decodeOutputJSON[verifyCommandResult](t, stdout.String())
	if len(verifyJSON.LintEvidence) == 0 || verifyJSON.LintEvidence[0].Severity != "lint_evidence" {
		t.Fatalf("verify --json lint evidence = %#v, want canonical severity", verifyJSON.LintEvidence)
	}
}

func TestRunVerifyCommand_JSONDoesNotHideLaterValidationErrorBehindAdvisoryBootFindings(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "false")
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")

	root := writeVerifyLintEvidenceWithMissingEmitSchemaFixture(t)
	opts := defaultVerifyCommandOptions()
	opts.contractsPath = root
	opts.configPath = writeTestVerifyRuntimeConfig(t)
	opts.output.asJSON = true

	var stdout, stderr bytes.Buffer
	code := runVerifyCommandWithOutput(context.Background(), RepoRoot(), opts, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("runVerifyCommand --json exit code = 0, stdout = %q stderr = %q", stdout.String(), stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != "" {
		t.Fatalf("verify --json stdout = %q, want empty for non-boot validation failure", stdout.String())
	}
	if errText := stderr.String(); !strings.Contains(errText, "verify failed: emit schema strict mode enabled") {
		t.Fatalf("verify --json stderr = %q, want strict emit schema validation failure", errText)
	}
}

func writeVerifyLintEvidenceFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyVerifyLintEvidence(t, false)
}

func writeVerifyLintEvidenceWithMissingEmitSchemaFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyVerifyLintEvidence(t, true)
}

func TestRunVerifyCommand_AllowsBootTimerWithoutCancelOn(t *testing.T) {
	root := writeVerifyBootTimerCommandFixture(t, "")

	var stdout, stderr bytes.Buffer
	code := runVerifyCommandWithContractsOutputForTest(t, context.Background(), RepoRoot(), root, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("runVerifyCommand exit code = %d, stdout = %q stderr = %q", code, stdout.String(), stderr.String())
	}
	if out := stdout.String(); !strings.Contains(out, "verify ok: contracts=") {
		t.Fatalf("verify stdout missing success marker:\n%s", out)
	}
}

func TestRunVerifyCommand_RejectsBootTimerWithCancelOn(t *testing.T) {
	root := writeVerifyBootTimerCommandFixture(t, "state:done")

	var stdout, stderr bytes.Buffer
	code := runVerifyCommandWithContractsOutputForTest(t, context.Background(), RepoRoot(), root, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("runVerifyCommand exit code = 0, stdout = %q stderr = %q", stdout.String(), stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != "" {
		t.Fatalf("verify stdout = %q, want empty for hard invalidity", stdout.String())
	}
	errText := stderr.String()
	for _, want := range []string{
		"verify failed: boot verification failed:",
		"[BLOCKER] timer_validation @",
		"start_on boot does not support cancel_on state:done",
		"remediation:",
	} {
		if !strings.Contains(errText, want) {
			t.Fatalf("verify stderr missing %q:\n%s", want, errText)
		}
	}

	opts := defaultVerifyCommandOptions()
	opts.contractsPath = root
	opts.configPath = writeTestVerifyRuntimeConfig(t)
	opts.output.asJSON = true
	stdout.Reset()
	stderr.Reset()
	code = runVerifyCommandWithOutput(context.Background(), RepoRoot(), opts, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("runVerifyCommand --json exit code = 0, stdout = %q stderr = %q", stdout.String(), stderr.String())
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("verify --json stderr = %q, want empty structured failure", stderr.String())
	}
	verifyJSON := decodeOutputJSON[verifyCommandResult](t, stdout.String())
	if verifyJSON.OK {
		t.Fatalf("verify --json ok = true, want false: %#v", verifyJSON)
	}
	if strings.TrimSpace(verifyJSON.Contracts) != root {
		t.Fatalf("verify --json contracts = %q, want %q", verifyJSON.Contracts, root)
	}
	if len(verifyJSON.Errors) == 0 {
		t.Fatalf("verify --json errors = %#v, want timer_validation", verifyJSON.Errors)
	}
	if got := verifyJSON.Errors[0]; got.CheckID != "timer_validation" || got.Severity != "hard_invalidity" || !strings.Contains(got.Message, "start_on boot does not support cancel_on state:done") {
		t.Fatalf("verify --json first error = %#v, want structured timer_validation hard invalidity", got)
	}
	if strings.TrimSpace(verifyJSON.Errors[0].Remediation) == "" {
		t.Fatalf("verify --json first error missing remediation: %#v", verifyJSON.Errors[0])
	}
	if len(verifyJSON.Errors[0].Evidence) == 0 || !strings.Contains(strings.Join(verifyJSON.Errors[0].Evidence, "\n"), "cancel_on: state:done") {
		t.Fatalf("verify --json first error evidence = %#v, want timer cancel_on evidence", verifyJSON.Errors[0].Evidence)
	}
}

func TestRunVerifyCommand_EscalatedWarningUsesBlockingAnalyzerOutput(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")

	root := filepath.Join(RepoRoot(), "tests", "tier8-boot-verification", "test-boot-state-machine-unreachable")
	var stdout, stderr bytes.Buffer
	code := runVerifyCommandWithContractsOutputForTest(t,
		context.Background(),
		RepoRoot(),
		root,
		&stdout,
		&stderr,
	)
	if code == 0 {
		t.Fatalf("runVerifyCommand exit code = 0, stdout = %q stderr = %q", stdout.String(), stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != "" {
		t.Fatalf("verify stdout = %q, want empty for blocking analyzer failure", stdout.String())
	}
	errText := stderr.String()
	for _, want := range []string{
		"verify failed: boot verification blocked by policy-escalated findings:",
		"[BLOCKER] semantic_drift_unreachable_state @",
		"declares state review but no transition path",
	} {
		if !strings.Contains(errText, want) {
			t.Fatalf("verify stderr missing %q:\n%s", want, errText)
		}
	}
	if strings.Contains(errText, "boot verification warnings:") {
		t.Fatalf("verify stderr used legacy fatal warning banner:\n%s", errText)
	}

	opts := defaultVerifyCommandOptions()
	opts.contractsPath = root
	opts.configPath = writeTestVerifyRuntimeConfig(t)
	opts.output.asJSON = true
	stdout.Reset()
	stderr.Reset()
	code = runVerifyCommandWithOutput(context.Background(), RepoRoot(), opts, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("runVerifyCommand --json exit code = 0, stdout = %q stderr = %q", stdout.String(), stderr.String())
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("verify --json stderr = %q, want empty structured warning failure", stderr.String())
	}
	verifyJSON := decodeOutputJSON[verifyCommandResult](t, stdout.String())
	if verifyJSON.OK {
		t.Fatalf("verify --json ok = true, want false: %#v", verifyJSON)
	}
	if len(verifyJSON.Errors) != 0 {
		t.Fatalf("verify --json errors = %#v, want warning-only structured failure", verifyJSON.Errors)
	}
	if len(verifyJSON.Warnings) == 0 {
		t.Fatalf("verify --json warnings = %#v, want semantic_drift_unreachable_state", verifyJSON.Warnings)
	}
	if !verifyFindingOutputsContain(verifyJSON.Warnings, "semantic_drift_unreachable_state", "semantic_drift_warning", "declares state review but no transition path") {
		t.Fatalf("verify --json warnings = %#v, want structured semantic_drift_unreachable_state warning", verifyJSON.Warnings)
	}
}

func verifyFindingOutputsContain(findings []verifyFindingOutput, checkID, severity, messageContains string) bool {
	for _, finding := range findings {
		if strings.TrimSpace(finding.CheckID) != checkID {
			continue
		}
		if strings.TrimSpace(finding.Severity) != severity {
			continue
		}
		if !strings.Contains(finding.Message, messageContains) {
			continue
		}
		return true
	}
	return false
}

func writeVerifyBootTimerCommandFixture(t *testing.T, cancelOn string) string {
	t.Helper()
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: verify-boot-timer
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows: []
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: verify-boot-timer
initial_state: waiting
terminal_states: [done]
states: [waiting, done]
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "entities.yaml"), `
ticket:
  ticket_id:
    type: string
    initial: ""
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), `
timer.reminder: {}
`)
	timerBlock := `
    - id: reminder
      owner: support-node
      event: timer.reminder
      delay: 1m
      start_on: boot
`
	if strings.TrimSpace(cancelOn) != "" {
		timerBlock += "      cancel_on: " + cancelOn + "\n"
	}
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
support-node:
  id: support-node
  execution_type: system_node
  subscribes_to: [timer.reminder]
  timers:
`+timerBlock+`  event_handlers:
    timer.reminder:
      advances_to: done
`)
	return root
}

func TestRunVerifyCommand_FirstFlowEquivalentSuppressesTutorialLintEvidence(t *testing.T) {
	root := canonicalrouting.CopyFirstFlowTutorial(t)
	var buf bytes.Buffer
	code := runVerifyCommandWithContractsForTest(t, context.Background(), RepoRoot(), root, &buf)
	if code != 0 {
		t.Fatalf("runVerifyCommand exit code = %d, output = %q", code, buf.String())
	}
	out := buf.String()
	if strings.Contains(out, "lint_evidence: cross_surface_named_type_use") {
		t.Fatalf("verify output should not contain tutorial cross-surface lint evidence:\n%s", out)
	}
	if strings.Contains(out, "lint_evidence: entity_reader_coverage") {
		t.Fatalf("verify output should not contain tutorial reader coverage lint evidence:\n%s", out)
	}
	if !strings.Contains(out, "verify ok: contracts=") {
		t.Fatalf("verify output missing success marker:\n%s", out)
	}
}

func TestRunVerifyCommand_FailsForUndefinedSelectedBackendModelAlias(t *testing.T) {
	root := canonicalrouting.CopyVerifyModelAlias(t, canonicalrouting.VerifyModelAliasUndefined)

	var buf bytes.Buffer
	code := runVerifyCommandWithContractsForTest(t, context.Background(), RepoRoot(), root, &buf)
	if code == 0 {
		t.Fatalf("expected non-zero exit code, output = %q", buf.String())
	}
	out := buf.String()
	for _, want := range []string{"model alias resolution failed", "llm.models alias \"not_configured\" is not configured"} {
		if !strings.Contains(out, want) {
			t.Fatalf("verify output missing %q:\n%s", want, out)
		}
	}
}

func TestRunVerifyCommand_UsesUnifiedRuntimeConfigModelAliases(t *testing.T) {
	root := canonicalrouting.CopyVerifyModelAlias(t, canonicalrouting.VerifyModelAliasConfigured)

	configPath := filepath.Join(t.TempDir(), "swarm.yaml")
	writeRuntimeConfigText(t, configPath, withTestProviderTriggerPlatformInventory(t, strings.Join([]string{
		"llm:",
		"  backend: anthropic",
		"  models:",
		"    audit.custom:",
		"      anthropic: claude-custom",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
	}, "\n")+"\n"))
	t.Setenv("SWARM_CONFIG", configPath)

	var buf bytes.Buffer
	opts := defaultVerifyCommandOptions()
	opts.contractsPath = root
	code := runVerifyCommandWithOutput(context.Background(), RepoRoot(), opts, &buf, &buf)
	if code != 0 {
		t.Fatalf("runVerifyCommand exit code = %d, output = %q", code, buf.String())
	}
	if out := buf.String(); !strings.Contains(out, "verify ok: contracts=") {
		t.Fatalf("verify output missing success marker:\n%s", out)
	}
}

func TestRunVerifyCommand_FailsForPromptDeclaredSaveWithoutEntityWrites(t *testing.T) {
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: verify-prompt-writer-coverage
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: child
    flow: child
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `name: verify-prompt-writer-coverage`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "schema.yaml"), `
name: child
initial_state: idle
terminal_states: [done]
states: [idle, done]
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "entities.yaml"), `
case:
  business_brief:
    type: text
    _unused_reason: verify prompt writer proof field
  research_context:
    type: text
    _unused_reason: verify prompt writer proof field
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "agents.yaml"), `
writer:
  id: writer
  type: factory
  role: writer
  prompt_ref: writer
  model: regular
  subscriptions: []
  entity_writes:
    case:
      save:
      - research_context
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "events.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "nodes.yaml"), `
closer:
  id: closer
  execution_type: system_node
  subscribes_to: [task.assigned]
  event_handlers:
    task.assigned:
      advances_to: done
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "prompts", "writer.md"), `
Use save_entity_field for `+"`business_brief`"+`.
`)

	var stdout, stderr bytes.Buffer
	code := runVerifyCommandWithContractsOutputForTest(t, context.Background(), RepoRoot(), root, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("expected non-zero exit code, stdout = %q stderr = %q", stdout.String(), stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != "" {
		t.Fatalf("verify stdout = %q, want empty for hard invalidity", stdout.String())
	}
	errText := stderr.String()
	for _, want := range []string{
		"verify failed: boot verification failed:",
		"[BLOCKER] entity_writer_coverage @",
		"business_brief",
	} {
		if !strings.Contains(errText, want) {
			t.Fatalf("verify stderr missing %q:\n%s", want, errText)
		}
	}
}

func TestRunVerifyCommand_FailsForPseudoStateSchemaTypes(t *testing.T) {
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: verify-state-schema-pseudo-types
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `name: verify-state-schema-pseudo-types`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
accumulator:
  id: accumulator
  execution_type: system_node
  state_schema:
    fields:
      dimensions_received: dimension score receipts keyed by dimension name
`)

	var buf bytes.Buffer
	code := runVerifyCommandWithContractsForTest(t, context.Background(), RepoRoot(), root, &buf)
	if code == 0 {
		t.Fatalf("expected non-zero exit code, output = %q", buf.String())
	}
	if out := buf.String(); !strings.Contains(out, "state_schema field type") {
		t.Fatalf("unexpected output = %q", out)
	}
}

func TestRunVerifyCommand_AllowsCanonicalStateSchemaFloat(t *testing.T) {
	root := canonicalrouting.CopyVerifyStateSchemaFloat(t)

	var buf bytes.Buffer
	code := runVerifyCommandWithContractsForTest(t, context.Background(), RepoRoot(), root, &buf)
	if code != 0 {
		t.Fatalf("runVerifyCommand exit code = %d, output = %q", code, buf.String())
	}
	if out := buf.String(); !strings.Contains(out, "verify ok: contracts=") {
		t.Fatalf("verify output missing success marker:\n%s", out)
	}
}

func TestRunVerifyCommand_AllowsAccumulatorEntityProjection(t *testing.T) {

	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "false")

	root := canonicalrouting.CopyVerifyAccumulatorEntityProjection(t)

	var buf bytes.Buffer
	code := runVerifyCommandWithContractsForTest(t, context.Background(), RepoRoot(), root, &buf)
	if code != 0 {
		t.Fatalf("runVerifyCommand exit code = %d, output = %q", code, buf.String())
	}
	if out := buf.String(); !strings.Contains(out, "verify ok: contracts=") {
		t.Fatalf("verify output missing success marker:\n%s", out)
	}
}

func TestRunVerifyCommand_AllowsOpenStreamAccumulatorWithExternalSource(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "false")

	root := writeVerifyAccumulatorSafetyCommandFixture(t, verifyAccumulatorSafetyCommandFixtureOptions{
		eventSource: "external (verify accumulator safety proof)",
	})

	var stdout, stderr bytes.Buffer
	code := runVerifyCommandWithContractsOutputForTest(t, context.Background(), RepoRoot(), root, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("runVerifyCommand exit code = %d, stdout = %q stderr = %q", code, stdout.String(), stderr.String())
	}
	if out := stdout.String(); !strings.Contains(out, "verify ok: contracts=") {
		t.Fatalf("verify stdout missing success marker:\n%s", out)
	}
	errText := stderr.String()
	if strings.Contains(errText, "accumulator_input_producer_path") {
		t.Fatalf("verify stderr reported no-producer error despite external source:\n%s", errText)
	}
}

func TestRunVerifyCommand_FailsForAccumulatorInputWithoutProducerPath(t *testing.T) {
	root := writeVerifyAccumulatorSafetyCommandFixture(t, verifyAccumulatorSafetyCommandFixtureOptions{})

	var stdout, stderr bytes.Buffer
	code := runVerifyCommandWithContractsOutputForTest(t, context.Background(), RepoRoot(), root, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("expected non-zero exit code, stdout = %q stderr = %q", stdout.String(), stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != "" {
		t.Fatalf("verify stdout = %q, want empty for hard invalidity", stdout.String())
	}
	errText := stderr.String()
	for _, want := range []string{
		"verify failed: boot verification failed:",
		"[BLOCKER] accumulator_input_producer_path @",
		"no accepted producer/source path",
		"remediation:",
	} {
		if !strings.Contains(errText, want) {
			t.Fatalf("verify stderr missing %q:\n%s", want, errText)
		}
	}
}

type verifyAccumulatorSafetyCommandFixtureOptions struct {
	eventSource string
}

func writeVerifyAccumulatorSafetyCommandFixture(t *testing.T, opts verifyAccumulatorSafetyCommandFixtureOptions) string {
	t.Helper()
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: verify-accumulator-safety
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows: []
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: verify-accumulator-safety
initial_state: collecting
terminal_states: [done]
states: [collecting, done]
pins:
  inputs:
    events: [item.arrived]
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), `{}`)
	sourceBlock := ""
	if strings.TrimSpace(opts.eventSource) != "" {
		sourceBlock = "\n  swarm:\n    source: " + opts.eventSource
	}
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), `
item.arrived:`+sourceBlock+`
  expected_count: integer
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
accumulator:
  id: accumulator
  execution_type: system_node
  subscribes_to: [item.arrived]
  event_handlers:
    item.arrived:
      accumulate:
        into: items
        from: payload
      advances_to: done
`)
	return root
}

func testWorkflowValidationBundle() *runtimecontracts.WorkflowContractBundle {
	bundle := &runtimecontracts.WorkflowContractBundle{}
	bundle.Platform.Platform.Name = "swarm"
	bundle.Platform.Platform.Version = "test"
	return bundle
}

func loadWorkflowValidationFixtureBundle(t *testing.T, relativeRoot string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	RepoRoot := runtimepipeline.WorkflowRepoRoot()
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(RepoRoot)
	fixtureRoot := filepath.Join(RepoRoot, relativeRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(RepoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides(%s): %v", fixtureRoot, err)
	}
	return bundle
}

func loadWorkflowValidationBundleAt(t *testing.T, fixtureRoot string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	RepoRoot := runtimepipeline.WorkflowRepoRoot()
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(RepoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(RepoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides(%s): %v", fixtureRoot, err)
	}
	return bundle
}

func writeWorkflowValidationFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func writeServeRuntimeAgentSlugFixture(t *testing.T, workflowName, agentID string) string {
	t.Helper()
	return writeServeRuntimeAgentSlugFixtureWithKey(t, workflowName, agentID, agentID)
}

func writeServeRuntimeAgentSlugFixtureWithKey(t *testing.T, workflowName, agentKey, agentID string) string {
	t.Helper()
	return canonicalrouting.CopyAgentSlugAdmission(t, workflowName, agentKey, agentID)
}

func writeServeRuntimeNativeBashFixture(t *testing.T) string {
	t.Helper()
	const agentID = "native-bash-worker"
	root := writeServeRuntimeAgentSlugFixture(t, "native-bash-docker-required", agentID)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), fmt.Sprintf(`
%s:
  id: %s
  role: %s
  prompt_ref: %s
  model: regular
  native_tools:
    bash: true
  subscriptions: [agent.requested]
`, agentID, agentID, agentID, agentID))
	return root
}

func writeWorkflowValidationDeadEventSchemaFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: dead-event-schema
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: dead-event-schema\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), `
root.unused: {}
`)
	return root
}

func TestVerifyBundle_AgreesWithRuntimeValidationOnTouchedToolAndEventClasses(t *testing.T) {
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	cases := []struct {
		name        string
		bundle      *runtimecontracts.WorkflowContractBundle
		errContains string
		wantErr     bool
	}{
		{
			name: "missing tool reference",
			bundle: func() *runtimecontracts.WorkflowContractBundle {
				bundle := testWorkflowValidationBundle()
				bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
					"agent-1": {ID: "agent-1", Tools: []string{"missing_tool"}},
				}
				return bundle
			}(),
			wantErr: false,
		},
		{
			name: "builtin runtime tool reference",
			bundle: func() *runtimecontracts.WorkflowContractBundle {
				bundle := testWorkflowValidationBundle()
				bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
					"agent-1": {ID: "agent-1", Tools: []string{"schedule"}, Permissions: []string{"schedule"}},
				}
				return bundle
			}(),
			wantErr: false,
		},
		{
			name: "missing emitted event schema warning",
			bundle: func() *runtimecontracts.WorkflowContractBundle {
				bundle := testWorkflowValidationBundle()
				bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
					"agent-1": {ID: "agent-1", EmitEvents: []string{"missing.event"}},
				}
				return bundle
			}(),
			errContains: "'missing.event' emitted but no schema in events.yaml",
			wantErr:     true,
		},
		{
			name: "tool implementation warning",
			bundle: func() *runtimecontracts.WorkflowContractBundle {
				bundle := testWorkflowValidationBundle()
				bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
					"legacy_call": {
						HandlerType: "api_call",
					},
				}
				return bundle
			}(),
			errContains: "tool implementation warnings",
			wantErr:     true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := semanticview.Wrap(tc.bundle)
			verifyErr := VerifyBundle(context.Background(), source)
			if tc.wantErr {
				if verifyErr == nil || !strings.Contains(verifyErr.Error(), tc.errContains) {
					t.Fatalf("VerifyBundle error = %v, want substring %q", verifyErr, tc.errContains)
				}
			} else if verifyErr != nil {
				t.Fatalf("VerifyBundle error = %v, want nil", verifyErr)
			}

			result, runtimeErr := runtimepkg.ValidateWorkflowContractSurface(context.Background(), source, runtimepkg.DefaultWorkflowContractValidationOptions(nil))
			if tc.wantErr {
				if runtimeErr == nil || !strings.Contains(runtimeErr.Error(), tc.errContains) {
					t.Fatalf("ValidateWorkflowContractSurface error = %v, want substring %q", runtimeErr, tc.errContains)
				}
				return
			}
			if runtimeErr != nil {
				t.Fatalf("ValidateWorkflowContractSurface error = %v, want nil", runtimeErr)
			}
			switch tc.name {
			case "missing tool reference":
				if warnings := result.BootReport.Warnings(); len(warnings) == 0 || !strings.Contains(warnings[0].Message, "missing tool missing_tool") {
					t.Fatalf("BootReport warnings = %#v, want tool_resolution warning", warnings)
				}
			case "builtin runtime tool reference":
				for _, warning := range result.BootReport.Warnings() {
					if strings.TrimSpace(warning.CheckID) == "tool_resolution" && strings.Contains(warning.Message, "schedule") {
						t.Fatalf("BootReport warnings = %#v, unexpected builtin tool_resolution warning", result.BootReport.Warnings())
					}
				}
			}
		})
	}
}

func TestSummarizeServeSchemaPlansOmitsTableBreakdown(t *testing.T) {
	plans := []store.SchemaTableDDL{
		{TableName: "events", ColumnCount: 17},
		{TableName: "runs", ColumnCount: 14},
	}

	summary := SummarizeServeSchemaPlans(plans)

	if summary != "verified 2 generated tables" {
		t.Fatalf("summary = %q, want concise generated-table count", summary)
	}
	for _, forbidden := range []string{"events(17)", "runs(14)", "detail="} {
		if strings.Contains(summary, forbidden) {
			t.Fatalf("serve schema summary leaked %q: %s", forbidden, summary)
		}
	}
}

func TestDoctorSchemaInventoryRetainsTypedSortedTableBreakdown(t *testing.T) {
	plans := []store.SchemaTableDDL{
		{TableName: "runs", ColumnCount: 14},
		{TableName: "events", ColumnCount: 17},
	}

	summary := newServeSchemaPlanSummary(plans)
	if summary.tableCount != 2 || summary.columnCount != 31 {
		t.Fatalf("summary counts = %d/%d, want 2/31", summary.tableCount, summary.columnCount)
	}
	if len(summary.tables) != 2 || summary.tables[0].Name != "events" || summary.tables[0].ColumnCount != 17 || summary.tables[1].Name != "runs" || summary.tables[1].ColumnCount != 14 {
		t.Fatalf("typed inventory = %#v, want sorted events/runs rows", summary.tables)
	}
}

func TestSummarizeServeSchemaPlansZeroPlans(t *testing.T) {
	summary := SummarizeServeSchemaPlans(nil)

	if summary != "verified 0 generated tables" {
		t.Fatalf("summary = %q, want zero generated-table summary", summary)
	}
}

type serveDevModeSpec struct {
	ImplementedBy        string   `yaml:"implemented_by"`
	Flag                 string   `yaml:"flag"`
	Owner                string   `yaml:"owner"`
	Composition          []string `yaml:"composition"`
	ConflictRules        []string `yaml:"conflict_rules"`
	ShutdownOrdering     string   `yaml:"shutdown_ordering"`
	CleanupScope         string   `yaml:"cleanup_scope"`
	PreservationBoundary string   `yaml:"preservation_boundary"`
	SiblingBoundaries    string   `yaml:"sibling_boundaries"`
}

type serveUnifiedListenerSpec struct {
	ImplementedBy string `yaml:"implemented_by"`
	SupersededBy  string `yaml:"superseded_by"`
	Flag          string `yaml:"flag"`
	Owner         string `yaml:"owner"`
	Semantics     string `yaml:"semantics"`
	Routes        struct {
		Always                  []string `yaml:"always"`
		WhenAPIHandlerInstalled []string `yaml:"when_api_handler_installed"`
		WhenMCPGatewayInstalled []string `yaml:"when_mcp_gateway_installed"`
	} `yaml:"routes"`
	BindRules          []string `yaml:"bind_rules"`
	ConsumerBoundaries struct {
		SwarmRunAPIPort string `yaml:"swarm_run_api_port"`
		SwarmRunMCPPort string `yaml:"swarm_run_mcp_port"`
	} `yaml:"consumer_boundaries"`
	UnpromotedReviewControls []string `yaml:"unpromoted_review_controls"`
}

type serveListenerTopologySpec struct {
	PromotedBy                       string `yaml:"promoted_by"`
	RuntimeBindImplementedBy         string `yaml:"runtime_bind_implemented_by"`
	EnvConfigPrecedenceImplementedBy string `yaml:"env_config_precedence_implemented_by"`
	ServerAPIAuthImplementedBy       string `yaml:"server_api_auth_implemented_by"`
	ImplementationStatus             string `yaml:"implementation_status"`
	CanonicalOwner                   string `yaml:"canonical_owner"`
	Summary                          string `yaml:"summary"`
	Listeners                        struct {
		API struct {
			BindFlag          string   `yaml:"bind_flag"`
			DefaultListenAddr string   `yaml:"default_listen_addr"`
			Routes            []string `yaml:"routes"`
		} `yaml:"api"`
		MCP struct {
			BindFlag          string   `yaml:"bind_flag"`
			DefaultListenAddr string   `yaml:"default_listen_addr"`
			Routes            []string `yaml:"routes"`
		} `yaml:"mcp"`
	} `yaml:"listeners"`
	Defaults struct {
		APIListenAddr string `yaml:"api_listen_addr"`
		MCPListenAddr string `yaml:"mcp_listen_addr"`
	} `yaml:"defaults"`
	SourcePrecedence struct {
		SourceOrder   []string `yaml:"source_order"`
		APIListenAddr struct {
			Flag           string `yaml:"flag"`
			ConfigKey      string `yaml:"config_key"`
			BuiltInDefault string `yaml:"built_in_default"`
		} `yaml:"api_listen_addr"`
		MCPListenAddr struct {
			Flag           string `yaml:"flag"`
			ConfigKey      string `yaml:"config_key"`
			BuiltInDefault string `yaml:"built_in_default"`
		} `yaml:"mcp_listen_addr"`
		ServerAPIAuth struct {
			AcceptedSources map[string]string `yaml:"accepted_sources"`
			SourceOrder     []string          `yaml:"source_order"`
			RejectedSources map[string]string `yaml:"rejected_sources"`
			TokenFileRule   string            `yaml:"token_file_rule"`
		} `yaml:"server_api_auth"`
		APIAuthCouplingRule string            `yaml:"api_auth_coupling_rule"`
		RejectedSources     map[string]string `yaml:"rejected_sources"`
	} `yaml:"source_precedence"`
	InteractionRules struct {
		APIDefaultTokenExposure struct {
			Rule string `yaml:"rule"`
		} `yaml:"api_default_token_exposure"`
	} `yaml:"interaction_rules"`
	ImplementationBoundaries []string `yaml:"implementation_boundaries"`
}

type cliAPIConnectionAuthConfigSpec struct {
	PromotedBy           string   `yaml:"promoted_by"`
	ClientEnvRemovedBy   string   `yaml:"client_env_removed_by"`
	ImplementationStatus string   `yaml:"implementation_status"`
	CanonicalOwner       string   `yaml:"canonical_owner"`
	Scope                string   `yaml:"scope"`
	AppliesTo            []string `yaml:"applies_to"`
	NotAppliesTo         []string `yaml:"not_applies_to"`
	PrecedenceOrder      []string `yaml:"precedence_order"`
	APIServer            struct {
		AcceptedSources struct {
			Flag              string `yaml:"flag"`
			ContextDescriptor string `yaml:"context_descriptor"`
			ConfigKey         string `yaml:"config_key"`
			BuiltInDefault    string `yaml:"built_in_default"`
		} `yaml:"accepted_sources"`
		RejectedSources map[string]string `yaml:"rejected_sources"`
		ValueModel      string            `yaml:"value_model"`
		Rationale       string            `yaml:"rationale"`
	} `yaml:"api_server"`
	APIToken struct {
		AcceptedSources        map[string]string `yaml:"accepted_sources"`
		SourceOrder            []string          `yaml:"source_order"`
		RejectedSources        map[string]string `yaml:"rejected_sources"`
		TokenFileRule          string            `yaml:"token_file_rule"`
		BuiltInLoopbackDefault struct {
			TokenValue                   string   `yaml:"token_value"`
			Source                       string   `yaml:"source"`
			AppliesWhen                  string   `yaml:"applies_when"`
			AllowedTargetHosts           []string `yaml:"allowed_target_hosts"`
			RejectedWithoutExplicitToken []string `yaml:"rejected_without_explicit_token"`
			NoAuthBypassRule             string   `yaml:"no_auth_bypass_rule"`
		} `yaml:"built_in_loopback_default"`
	} `yaml:"api_token"`
	CLIConfigFile struct {
		AcceptedSources  map[string]string `yaml:"accepted_sources"`
		RejectedSources  map[string]string `yaml:"rejected_sources"`
		AcceptedKeys     map[string]string `yaml:"accepted_keys"`
		SharedNonAPIKeys map[string]string `yaml:"shared_non_api_keys"`
		RejectedKeys     map[string]string `yaml:"rejected_keys"`
	} `yaml:"cli_config_file"`
	ServeListenerEnvConfigBoundary struct {
		RejectedPorts               map[string]string `yaml:"rejected_ports"`
		RejectedListenerEnvironment map[string]string `yaml:"rejected_listener_environment"`
		Rule                        string            `yaml:"rule"`
	} `yaml:"serve_listener_env_config_boundary"`
	SplitSiblings            []string `yaml:"split_siblings"`
	ImplementationBoundaries []string `yaml:"implementation_boundaries"`
}

type cliContractPlatformSpecPathResolutionSpec struct {
	PromotedBy           string   `yaml:"promoted_by"`
	ImplementationStatus string   `yaml:"implementation_status"`
	CanonicalOwner       string   `yaml:"canonical_owner"`
	Scope                string   `yaml:"scope"`
	AppliesTo            []string `yaml:"applies_to"`
	NotAppliesTo         []string `yaml:"not_applies_to"`
	ContractsPath        struct {
		AcceptedSources struct {
			Flag              string `yaml:"flag"`
			Environment       string `yaml:"environment"`
			ConfigKey         string `yaml:"config_key"`
			DiscoveredDefault string `yaml:"discovered_default"`
		} `yaml:"accepted_sources"`
		SourceOrder     []string          `yaml:"source_order"`
		RejectedSources map[string]string `yaml:"rejected_sources"`
		MissingSource   string            `yaml:"missing_source_rule"`
	} `yaml:"contracts_path"`
	PlatformSpecPath struct {
		AcceptedSources struct {
			Flag           string `yaml:"flag"`
			ConfigKey      string `yaml:"config_key"`
			BuiltInDefault string `yaml:"built_in_default"`
		} `yaml:"accepted_sources"`
		SourceOrder     []string          `yaml:"source_order"`
		RejectedSources map[string]string `yaml:"rejected_sources"`
		DefaultRule     string            `yaml:"default_rule"`
	} `yaml:"platform_spec_path"`
	ImplementationBoundaries []string `yaml:"implementation_boundaries"`
}

func loadServeDevModeSpec(t *testing.T) serveDevModeSpec {
	t.Helper()
	var spec struct {
		CLISpecification struct {
			CommandCatalog struct {
				Serve struct {
					DevMode serveDevModeSpec `yaml:"dev_mode_lifecycle_composition"`
				} `yaml:"serve"`
			} `yaml:"command_catalog"`
		} `yaml:"cli_specification"`
	}
	decodeAuthoritativeYAMLFileForTest(t, filepath.Join(RepoRoot(), defaultPlatformSpecPath), &spec)
	if strings.TrimSpace(spec.CLISpecification.CommandCatalog.Serve.DevMode.Flag) == "" {
		t.Fatal("platform spec missing serve dev_mode_lifecycle_composition")
	}
	return spec.CLISpecification.CommandCatalog.Serve.DevMode
}

func loadServeUnifiedListenerSpec(t *testing.T) serveUnifiedListenerSpec {
	t.Helper()
	var spec struct {
		CLISpecification struct {
			CommandCatalog struct {
				Serve struct {
					Listener serveUnifiedListenerSpec `yaml:"unified_listener_bind_contract"`
				} `yaml:"serve"`
			} `yaml:"command_catalog"`
		} `yaml:"cli_specification"`
	}
	decodeAuthoritativeYAMLFileForTest(t, filepath.Join(RepoRoot(), defaultPlatformSpecPath), &spec)
	if strings.TrimSpace(spec.CLISpecification.CommandCatalog.Serve.Listener.Flag) == "" {
		t.Fatal("platform spec missing serve unified_listener_bind_contract")
	}
	return spec.CLISpecification.CommandCatalog.Serve.Listener
}

func loadServeListenerTopologySpec(t *testing.T) serveListenerTopologySpec {
	t.Helper()
	var spec struct {
		CLISpecification struct {
			CommandCatalog struct {
				Serve struct {
					ListenerTopology serveListenerTopologySpec `yaml:"listener_topology_v2_1"`
				} `yaml:"serve"`
			} `yaml:"command_catalog"`
		} `yaml:"cli_specification"`
	}
	decodeAuthoritativeYAMLFileForTest(t, filepath.Join(RepoRoot(), defaultPlatformSpecPath), &spec)
	if strings.TrimSpace(spec.CLISpecification.CommandCatalog.Serve.ListenerTopology.CanonicalOwner) == "" {
		t.Fatal("platform spec missing serve listener_topology_v2_1")
	}
	return spec.CLISpecification.CommandCatalog.Serve.ListenerTopology
}

func loadCLIAPIConnectionAuthConfigSpec(t *testing.T) cliAPIConnectionAuthConfigSpec {
	t.Helper()
	var spec struct {
		CLISpecification struct {
			Foundations struct {
				APIConnectionAuthConfig cliAPIConnectionAuthConfigSpec `yaml:"api_connection_auth_config_precedence"`
			} `yaml:"foundations"`
		} `yaml:"cli_specification"`
	}
	decodeAuthoritativeYAMLFileForTest(t, filepath.Join(RepoRoot(), defaultPlatformSpecPath), &spec)
	if strings.TrimSpace(spec.CLISpecification.Foundations.APIConnectionAuthConfig.CanonicalOwner) == "" {
		t.Fatal("platform spec missing api_connection_auth_config_precedence")
	}
	return spec.CLISpecification.Foundations.APIConnectionAuthConfig
}

func loadCLIContractPlatformSpecPathResolutionSpec(t *testing.T) cliContractPlatformSpecPathResolutionSpec {
	t.Helper()
	var spec struct {
		CLISpecification struct {
			Foundations struct {
				ContractPlatformSpecPathResolution cliContractPlatformSpecPathResolutionSpec `yaml:"contract_platform_spec_path_resolution"`
			} `yaml:"foundations"`
		} `yaml:"cli_specification"`
	}
	decodeAuthoritativeYAMLFileForTest(t, filepath.Join(RepoRoot(), defaultPlatformSpecPath), &spec)
	if strings.TrimSpace(spec.CLISpecification.Foundations.ContractPlatformSpecPathResolution.CanonicalOwner) == "" {
		t.Fatal("platform spec missing contract_platform_spec_path_resolution")
	}
	return spec.CLISpecification.Foundations.ContractPlatformSpecPathResolution
}

func stringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if strings.Contains(value, want) {
			return true
		}
	}
	return false
}

func joinedContains(values []string, want string) bool {
	return strings.Contains(strings.Join(values, "\n"), want)
}

func mapValueContains(values map[string]string, want string) bool {
	for _, value := range values {
		if strings.Contains(value, want) {
			return true
		}
	}
	return false
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

const (
	serveRuntimeReadyTimeout = 30 * time.Second
	serveRuntimeStopTimeout  = runtimepkg.DefaultShutdownGrace + 15*time.Second
)

type serveRuntimeTestProcess struct {
	t      *testing.T
	cancel context.CancelFunc
	done   <-chan int
	out    *lockedBuffer

	mu      sync.Mutex
	stopped bool
	code    int
}

func (p *serveRuntimeTestProcess) outputString() string {
	return p.out.String()
}

func (p *serveRuntimeTestProcess) stop() int {
	p.t.Helper()
	p.cancel()
	code, ok := p.waitForExit(serveRuntimeStopTimeout)
	if !ok {
		p.t.Fatalf("timed out stopping Run\noutput:\n%s", p.outputString())
	}
	return code
}

func (p *serveRuntimeTestProcess) waitForExit(timeout time.Duration) (int, bool) {
	p.t.Helper()
	p.mu.Lock()
	if p.stopped {
		code := p.code
		p.mu.Unlock()
		return code, true
	}
	p.mu.Unlock()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case code := <-p.done:
		p.recordStopped(code)
		return code, true
	case <-timer.C:
		return 0, false
	}
}

func (p *serveRuntimeTestProcess) recordStopped(code int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopped = true
	p.code = code
}

func writeStoreBackendRuntimeConfigWithWorkspaceFields(t *testing.T, backend string, sqlitePath string, workspaceFields []string) string {
	t.Helper()
	lines := []string{
		"runtime:",
		"  recovery_on_startup: false",
		"workspace:",
		"  data_source: " + t.TempDir(),
	}
	lines = append(lines, workspaceFields...)
	if strings.TrimSpace(backend) != "" || strings.TrimSpace(sqlitePath) != "" {
		lines = append(lines,
			"store:",
			"  backend: "+backend,
			"  sqlite:",
			"    path: "+sqlitePath,
		)
	}
	lines = append(lines,
		"llm:",
		"  backend: anthropic",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
	)
	path := filepath.Join(t.TempDir(), "swarm.yaml")
	configText := withTestProviderTriggerPlatformInventory(t, strings.Join(lines, "\n")+"\n")
	if err := os.WriteFile(path, []byte(configText), 0o644); err != nil {
		t.Fatalf("write runtime config: %v", err)
	}
	return path
}

func writeRuntimeConfigText(t *testing.T, path, configText string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(configText), 0o644); err != nil {
		t.Fatalf("write runtime config %s: %v", path, err)
	}
}

func TestVerifyBundle_DoesNotWarnForFlowLocalEmittedEventsWithOwningFlowSchemas(t *testing.T) {
	source := semanticview.Wrap(loadWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier11-flow-composition", "test-child-flow-local-events")))

	err := VerifyBundle(context.Background(), source)
	if err == nil {
		t.Fatal("VerifyBundle error = nil, want warning-only failure from unrelated fixture warnings")
	}
	if strings.Contains(err.Error(), "'child/child.internal' emitted but no schema in events.yaml") {
		t.Fatalf("unexpected flow-local no-schema warning: %v", err)
	}
	if strings.Contains(err.Error(), "'child/child.done' emitted but no schema in events.yaml") {
		t.Fatalf("unexpected flow-local no-schema warning: %v", err)
	}
}

func TestVerifyBundle_DoesNotWarnForFlowOwnedAgentOutputEvents(t *testing.T) {
	source := semanticview.Wrap(requiredagentsparentconnect.LoadBundle(t))

	err := VerifyBundle(context.Background(), source)
	if err == nil {
		t.Fatal("VerifyBundle error = nil, want warning-only failure from unrelated fixture warnings")
	}
	if strings.Contains(err.Error(), "'work.ready' emitted but nobody subscribes") {
		t.Fatalf("unexpected flow-owned agent output warning: %v", err)
	}
}

func TestVerifyBundle_CreateEntityAccumulatePreemptsDynamicComputeWarningSurface(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	bundle := loadWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier8-boot-verification", "test-boot-success"))
	bundle.RootEntities = runtimecontracts.EntityContractsDocument{
		"tracking": {
			Fields: map[string]runtimecontracts.EntityFieldDecl{
				"expected_count":  {Type: "integer", Initial: 1},
				"composite_score": {Type: "numeric"},
			},
		},
	}
	nodeID := "complete-task"
	eventType := "task.requested"
	node, ok := bundle.Nodes[nodeID]
	if !ok {
		t.Fatalf("node %s missing from test fixture bundle", nodeID)
	}
	handler := node.EventHandlers[eventType]
	handler.CreateEntity = true
	handler.Accumulate = &runtimecontracts.AccumulateSpec{Into: "items"}
	handler.Compute = &runtimecontracts.ComputeSpec{
		Operation: runtimecontracts.ComputeOpCount,
		StoreAs:   "entity.composite_score",
	}
	node.EventHandlers[eventType] = handler
	bundle.Nodes[nodeID] = node
	if bundle.Semantics.NodeHandlers == nil {
		bundle.Semantics.NodeHandlers = map[string]map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	if bundle.Semantics.NodeHandlers[nodeID] == nil {
		bundle.Semantics.NodeHandlers[nodeID] = map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	bundle.Semantics.NodeHandlers[nodeID][eventType] = handler

	err := VerifyBundle(context.Background(), semanticview.Wrap(bundle))
	if err == nil || !strings.Contains(err.Error(), "declares both create_entity and accumulate") {
		t.Fatalf("VerifyBundle error = %v, want create_entity/accumulate boot error", err)
	}
}

func TestVerifyBundle_EmittedPayloadCompletenessReturnsWarningSurface(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")

	bundle := &runtimecontracts.WorkflowContractBundle{
		Platform: runtimecontracts.PlatformSpecDocument{},
		RootEntities: runtimecontracts.EntityContractsDocument{
			"scan": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"scan_id":   {Type: "string"},
					"geography": {Type: "string"},
				},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"dispatcher": {
					"scan.corpus_dispatch": {
						Emit: runtimecontracts.EmitSpec{Event: "market_research.scan_assigned"},
					},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"dispatcher": {
				SubscribesTo: []string{"scan.corpus_dispatch"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"scan.corpus_dispatch": {
						Emit: runtimecontracts.EmitSpec{Event: "market_research.scan_assigned"},
					},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"scan.corpus_dispatch": {
				Swarm: runtimecontracts.EventSwarmMetadata{Source: "external"},
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"scan_id":   {Type: "string"},
						"geography": {Type: "string"},
					},
				},
				Required: []string{"scan_id", "geography"},
			},
			"market_research.scan_assigned": {
				ConsumerType: []string{"dashboard"},
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"entity_id":          {Type: "string"},
						"current_state":      {Type: "string"},
						"trigger_event_type": {Type: "string"},
						"scan_id":            {Type: "string"},
					},
				},
				Required: []string{"entity_id", "scan_id"},
			},
		},
	}
	bundle.Platform.Platform.Name = "test"
	bundle.Platform.Platform.Version = "1.0.0"

	err := VerifyBundle(context.Background(), semanticview.Wrap(bundle))
	if err == nil {
		t.Fatal("VerifyBundle error = nil, want emitted payload completeness invalidity")
	}
	if !strings.Contains(err.Error(), "scan_id is not statically provable") {
		t.Fatalf("VerifyBundle error = %v, want emitted payload completeness invalidity", err)
	}
	if strings.Contains(err.Error(), "definitely missing") {
		t.Fatalf("VerifyBundle error = %v, want approved warning wording only", err)
	}
}

func TestVerifyBundle_InputPinProducerPathReturnsHardInvaliditySurface(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")

	err := VerifyBundle(context.Background(), semanticview.Wrap(loadWorkflowValidationBundleAt(t, writeVerifyMissingPinWarningFixture(t))))
	if err == nil {
		t.Fatal("VerifyBundle error = nil, want hard invalidity from missing producer path")
	}
	for _, want := range []string{
		"no accepted producer source was found in the authored bundle",
		"Boundary external ingress: not found",
		"Intrinsic ingress input pin: not found",
		"Parent connect: not found",
		"Validation-only harness input: not found",
		"Platform source: not found",
		"Internal topology producer: not found",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("VerifyBundle error = %v, want substring %q", err, want)
		}
	}
}

func writeVerifyMissingPinWarningFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyVerifyMissingPin(t)
}

func TestVerifyBundle_UnreachableStateReturnsWarningSurface(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")

	err := VerifyBundle(context.Background(), semanticview.Wrap(loadWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier8-boot-verification", "test-boot-state-machine-unreachable"))))
	if err == nil {
		t.Fatal("VerifyBundle error = nil, want warning-only failure from unreachable declared state")
	}
	for _, want := range []string{
		"semantic_drift_unreachable_state",
		"declares state review but no transition path from initial_state waiting reaches review",
		"Reachable states: active, done, waiting",
		"Unreachable states: review",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("VerifyBundle error = %v, want substring %q", err, want)
		}
	}
}

func TestVerifyBundle_DeadDeclaredEventSchemaReturnsWarningSurface(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")

	source := semanticview.Wrap(loadWorkflowValidationBundleAt(t, writeWorkflowValidationDeadEventSchemaFixture(t)))

	verifyErr := VerifyBundle(context.Background(), source)
	if verifyErr == nil {
		t.Fatal("VerifyBundle error = nil, want warning-only failure from dead declared event schema")
	}
	for _, want := range []string{
		"semantic_drift_dead_event_schema",
		"root.unused",
		"has no active role in the authored bundle",
	} {
		if !strings.Contains(verifyErr.Error(), want) {
			t.Fatalf("VerifyBundle error = %v, want substring %q", verifyErr, want)
		}
	}

	result, runtimeErr := runtimepkg.ValidateWorkflowContractSurface(context.Background(), source, runtimepkg.DefaultWorkflowContractValidationOptions(nil))
	if runtimeErr == nil {
		t.Fatal("ValidateWorkflowContractSurface error = nil, want warning-only failure from dead declared event schema")
	}
	for _, want := range []string{
		"semantic_drift_dead_event_schema",
		"root.unused",
		"has no active role in the authored bundle",
	} {
		if !strings.Contains(runtimeErr.Error(), want) {
			t.Fatalf("ValidateWorkflowContractSurface error = %v, want substring %q", runtimeErr, want)
		}
	}
	if !strings.Contains(strings.TrimSpace(result.BootReport.Warnings()[0].CheckID), "semantic_drift_dead_event_schema") {
		t.Fatalf("BootReport warnings = %#v, want semantic_drift_dead_event_schema", result.BootReport.Warnings())
	}
}

func TestVerifyBundle_CreateEntityAccumulateReturnsBootError(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")

	err := VerifyBundle(context.Background(), semanticview.Wrap(loadWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier8-boot-verification", "test-boot-create-entity-plus-accumulate"))))
	if err == nil {
		t.Fatal("VerifyBundle error = nil, want create_entity/accumulate boot error")
	}
	if !strings.Contains(err.Error(), "declares both create_entity and accumulate") {
		t.Fatalf("VerifyBundle error = %v, want create_entity/accumulate boot error", err)
	}
}
