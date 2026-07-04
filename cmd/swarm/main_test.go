package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
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
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/preservationcleanup"
	runtimerunforkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	runtimerunquiescence "github.com/division-sh/swarm/internal/runtime/runquiescence"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

type delayedRunStatusAgent struct {
	id            string
	subscriptions []events.EventType
	started       chan struct{}
	release       chan struct{}
}

type servedEventPublishBlockingLLMRuntime struct {
	started chan<- struct{}
	release <-chan struct{}
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
	out := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("scan.completed"),
		a.id,
		"",
		[]byte(`{}`),
		0,
		evt.RunID(),
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, evt.EntityID()),
		time.Now().UTC(),
	)

	return []events.Event{out}, nil
}

func (r servedEventPublishBlockingLLMRuntime) StartSession(_ context.Context, agentID string, systemPrompt string, tools []runtimellm.ToolDefinition) (*runtimellm.Session, error) {
	return &runtimellm.Session{
		ID:           uuid.NewString(),
		AgentID:      agentID,
		SystemPrompt: systemPrompt,
		Tools:        append([]runtimellm.ToolDefinition(nil), tools...),
	}, nil
}

func (r servedEventPublishBlockingLLMRuntime) ContinueSession(ctx context.Context, session *runtimellm.Session, message runtimellm.Message) (*runtimellm.Response, error) {
	if r.started != nil {
		select {
		case r.started <- struct{}{}:
		default:
		}
	}
	select {
	case <-r.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	sessionID := ""
	if session != nil {
		sessionID = session.ID
	}
	return &runtimellm.Response{
		Message:   runtimellm.Message{Role: "assistant", Content: "acknowledged"},
		SessionID: sessionID,
	}, nil
}

func publishRunStatusRootEvent(t *testing.T, bus *runtimebus.EventBus, runID, entityID string) {
	t.Helper()
	if err := bus.Publish(context.Background(), eventtest.RootIngress(
		uuid.NewString(),
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

func markRunStatusCompleted(t *testing.T, pg *store.PostgresStore, runID string) {
	t.Helper()
	if err := pg.MarkRunTerminal(context.Background(), runID, "completed", "", time.Now().UTC()); err != nil {
		t.Fatalf("mark run completed: %v", err)
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
			var captured serveOptions
			called := false
			opts := defaultRootCommandOptions()
			opts.runServe = func(_ context.Context, _ string, serveOpts serveOptions) int {
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

	var captured serveOptions
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ string, serveOpts serveOptions) int {
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

	captured = serveOptions{}
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
		env     map[string]string
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
			name: "environment beats config",
			config: map[string]string{
				"serve_api_listen_addr": "127.0.0.1:9101",
				"serve_mcp_listen_addr": "127.0.0.1:9102",
			},
			env: map[string]string{
				"SWARM_API_LISTEN_ADDR": "0.0.0.0:9201",
				"SWARM_MCP_LISTEN_ADDR": "0.0.0.0:9202",
			},
			wantAPI: "0.0.0.0:9201",
			wantMCP: "0.0.0.0:9202",
		},
		{
			name: "flag beats environment for that listener only",
			args: []string{"--api-listen-addr", "127.0.0.1:9301"},
			config: map[string]string{
				"serve_api_listen_addr": "127.0.0.1:9101",
				"serve_mcp_listen_addr": "127.0.0.1:9102",
			},
			env: map[string]string{
				"SWARM_API_LISTEN_ADDR": "0.0.0.0:9201",
				"SWARM_MCP_LISTEN_ADDR": "0.0.0.0:9202",
			},
			wantAPI: "127.0.0.1:9301",
			wantMCP: "0.0.0.0:9202",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			if len(tc.config) > 0 {
				t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, tc.config))
			}
			for key, value := range tc.env {
				t.Setenv(key, value)
			}

			var captured serveOptions
			opts := defaultRootCommandOptions()
			opts.runServe = func(_ context.Context, _ string, serveOpts serveOptions) int {
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
			name: "invalid api environment address",
			setup: func(t *testing.T) {
				t.Setenv("SWARM_API_LISTEN_ADDR", "8081")
			},
			wantStderr: "--api-listen-addr must be a host:port listen address",
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
			wantStderr: `unsupported CLI config key "api_listen_addr"`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			tc.setup(t)
			ran := false
			opts := defaultRootCommandOptions()
			opts.runServe = func(context.Context, string, serveOptions) int {
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

func TestValidateServeAPIAuthBindingDefaultTokenLoopbackBoundary(t *testing.T) {
	defaultAuth := apiv1.AuthTokenResolution{Tokens: []string{apiv1.DefaultLoopbackAPIToken}, Source: apiv1.AuthTokenSourceBuiltInLoopbackToken}
	explicitAuth := apiv1.AuthTokenResolution{Tokens: []string{"operator-token"}, Source: apiv1.AuthTokenSource(serveAPITokenFileFlagSource), Explicit: true, TokenFile: filepath.Join(t.TempDir(), "token")}
	tests := []struct {
		name    string
		addr    string
		auth    apiv1.AuthTokenResolution
		wantErr string
	}{
		{name: "default token allowed on ipv4 loopback", addr: "127.0.0.1:8081", auth: defaultAuth},
		{name: "default token allowed on ipv6 loopback", addr: "[::1]:8081", auth: defaultAuth},
		{name: "default token rejects localhost", addr: "localhost:8081", auth: defaultAuth, wantErr: "non-loopback API bind localhost:8081 requires --api-token-file or config serve_api_token_file"},
		{name: "default token rejects wildcard", addr: "0.0.0.0:8081", auth: defaultAuth, wantErr: "non-loopback API bind 0.0.0.0:8081 requires --api-token-file or config serve_api_token_file"},
		{name: "default token rejects routable", addr: "192.0.2.10:8081", auth: defaultAuth, wantErr: "non-loopback API bind 192.0.2.10:8081 requires --api-token-file or config serve_api_token_file"},
		{name: "explicit token allows wildcard", addr: "0.0.0.0:8081", auth: explicitAuth},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateServeAPIAuthBinding(tc.addr, tc.auth)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validateServeAPIAuthBinding: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestResolveServeAPIAuthSourceAuthority(t *testing.T) {
	t.Run("default loopback when no explicit source", func(t *testing.T) {
		isolateCLIAPIConfigEnv(t)
		auth, err := resolveServeAPIAuth(defaultServeOptions())
		if err != nil {
			t.Fatalf("resolveServeAPIAuth: %v", err)
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
		auth, err := resolveServeAPIAuth(serveOptions{APITokenFile: flagTokenFile, APITokenFileFlagSet: true})
		if err != nil {
			t.Fatalf("resolveServeAPIAuth: %v", err)
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
		auth, err := resolveServeAPIAuth(defaultServeOptions())
		if err != nil {
			t.Fatalf("resolveServeAPIAuth: %v", err)
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
		_, err := resolveServeAPIAuth(serveOptions{APITokenFile: tokenFile, APITokenFileFlagSet: true})
		if err == nil || !strings.Contains(err.Error(), "server-side API environment source is no longer accepted") || !strings.Contains(err.Error(), "serve_api_token_file") {
			t.Fatalf("err = %v, want removed-env diagnostic", err)
		}
	})

	t.Run("blank and missing token files fail closed", func(t *testing.T) {
		isolateCLIAPIConfigEnv(t)
		blank := writeCLIAPITokenFile(t, "  \n")
		if _, err := resolveServeAPIAuth(serveOptions{APITokenFile: blank, APITokenFileFlagSet: true}); err == nil || !strings.Contains(err.Error(), "--api-token-file is blank") {
			t.Fatalf("blank token err = %v, want blank token failure", err)
		}
		missing := filepath.Join(t.TempDir(), "missing-token")
		if _, err := resolveServeAPIAuth(serveOptions{APITokenFile: missing, APITokenFileFlagSet: true}); err == nil || !strings.Contains(err.Error(), "read --api-token-file") {
			t.Fatalf("missing token err = %v, want read failure", err)
		}
	})
}

func TestCLI_ServeRuntimeConfigDoesNotFeedListenerConfig(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	runtimeConfig := filepath.Join(t.TempDir(), "runtime.yaml")
	if err := os.WriteFile(runtimeConfig, []byte("serve_api_listen_addr: \"0.0.0.0:9999\"\nserve_mcp_listen_addr: \"0.0.0.0:9998\"\n"), 0o600); err != nil {
		t.Fatalf("write runtime config: %v", err)
	}

	var captured serveOptions
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ string, serveOpts serveOptions) int {
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
	if captured.APIListenAddr != defaultAPIListenAddr {
		t.Fatalf("api listen addr = %q, want default %q", captured.APIListenAddr, defaultAPIListenAddr)
	}
	if captured.MCPListenAddr != defaultMCPListenAddr {
		t.Fatalf("mcp listen addr = %q, want default %q", captured.MCPListenAddr, defaultMCPListenAddr)
	}
}

func TestCLI_ServeDataFlagFeedsServeOptions(t *testing.T) {
	dataDir := t.TempDir()
	var captured serveOptions
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ string, serveOpts serveOptions) int {
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
	var captured serveOptions
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ string, serveOpts serveOptions) int {
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
				t.Setenv("SWARM_API_LISTEN_ADDR", "8081")
				t.Setenv("SWARM_MCP_LISTEN_ADDR", "http://127.0.0.1:8082")
			},
			wantAPI: "127.0.0.1:9401",
			wantMCP: "127.0.0.1:9402",
			wantRan: true,
		},
		{
			name: "both env vars skip malformed cli config",
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
			wantAPI: "127.0.0.1:9501",
			wantMCP: "127.0.0.1:9502",
			wantRan: true,
		},
		{
			name: "partial env still loads cli config for unresolved listener",
			args: []string{"serve"},
			setup: func(t *testing.T) {
				t.Setenv("SWARM_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))
				t.Setenv("SWARM_API_LISTEN_ADDR", "127.0.0.1:9601")
			},
			wantError: "read SWARM_CONFIG",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			tc.setup(t)
			var captured serveOptions
			ran := false
			opts := defaultRootCommandOptions()
			opts.runServe = func(_ context.Context, _ string, serveOpts serveOptions) int {
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
	var captured serveOptions
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ string, serveOpts serveOptions) int {
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
	if !captured.Verbose {
		t.Fatal("serve verbose = false, want dev composition")
	}
	if captured.ShutdownGrace != wantGrace {
		t.Fatalf("serve shutdown grace = %s, want explicit %s", captured.ShutdownGrace, wantGrace)
	}
}

func TestCLI_ServeDevRejectsRequireBundleMatchBeforeOwner(t *testing.T) {
	var called atomic.Bool
	opts := defaultRootCommandOptions()
	opts.runServe = func(context.Context, string, serveOptions) int {
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
	var captured serveOptions
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ string, serveOpts serveOptions) int {
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
	if strings.TrimSpace(spec.ImplementedBy) != "#830" {
		t.Fatalf("dev mode implemented_by = %q, want #830", spec.ImplementedBy)
	}
	if strings.TrimSpace(spec.Flag) != "--dev" {
		t.Fatalf("dev mode flag = %q, want --dev", spec.Flag)
	}
	for _, want := range []string{
		"`--abandon-active-runs`",
		"`--no-require-bundle-match`",
		"`--verbose`",
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
	for _, want := range []string{"swarm run --api-port", "consumer", "second bind owner"} {
		if !strings.Contains(spec.ConsumerBoundaries.SwarmRunAPIPort, want) {
			t.Fatalf("api-port boundary missing %q:\n%s", want, spec.ConsumerBoundaries.SwarmRunAPIPort)
		}
	}
	for _, want := range []string{"swarm run --mcp-port", "fail before API/WS calls", "local foreground MCP listener control"} {
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
	wantSourceOrder := []string{"flag", "environment", "cli_config_file", "built_in_default"}
	if len(spec.SourcePrecedence.SourceOrder) != len(wantSourceOrder) {
		t.Fatalf("listener source order = %#v, want %#v", spec.SourcePrecedence.SourceOrder, wantSourceOrder)
	}
	for i, want := range wantSourceOrder {
		if spec.SourcePrecedence.SourceOrder[i] != want {
			t.Fatalf("listener source order[%d] = %q, want %q", i, spec.SourcePrecedence.SourceOrder[i], want)
		}
	}
	if spec.SourcePrecedence.APIListenAddr.Environment != "SWARM_API_LISTEN_ADDR" || spec.SourcePrecedence.APIListenAddr.ConfigKey != "serve_api_listen_addr" {
		t.Fatalf("api listener source precedence = %#v", spec.SourcePrecedence.APIListenAddr)
	}
	if spec.SourcePrecedence.MCPListenAddr.Environment != "SWARM_MCP_LISTEN_ADDR" || spec.SourcePrecedence.MCPListenAddr.ConfigKey != "serve_mcp_listen_addr" {
		t.Fatalf("mcp listener source precedence = %#v", spec.SourcePrecedence.MCPListenAddr)
	}
	if spec.SourcePrecedence.ServerAPIAuth.AcceptedSources["flag_file"] != "--api-token-file <path>" {
		t.Fatalf("server api auth flag source = %#v", spec.SourcePrecedence.ServerAPIAuth.AcceptedSources)
	}
	if spec.SourcePrecedence.ServerAPIAuth.AcceptedSources["config_file_key"] != "serve_api_token_file" {
		t.Fatalf("server api auth config source = %#v", spec.SourcePrecedence.ServerAPIAuth.AcceptedSources)
	}
	wantServeAuthOrder := []string{"--api-token-file", "config serve_api_token_file", "built-in loopback default"}
	if !reflect.DeepEqual(spec.SourcePrecedence.ServerAPIAuth.SourceOrder, wantServeAuthOrder) {
		t.Fatalf("server api auth source order = %#v, want %#v", spec.SourcePrecedence.ServerAPIAuth.SourceOrder, wantServeAuthOrder)
	}
	for key, want := range map[string]string{
		"SWARM_API_TOKEN":       "#1647",
		"SWARM_API_TOKEN_FILE":  "Not promoted",
		"config api_token_file": "Client-side API auth only",
		"config api_token":      "Inline bearer tokens",
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
	for _, want := range []string{"--api-token-file", "serve_api_token_file"} {
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
	for _, want := range []string{"#992 implements", "`--health-addr` retirement", "`swarm run --mcp-port` remains fail-closed", "#844 implements `swarm serve` listener source precedence"} {
		if !stringSliceContains(spec.ImplementationBoundaries, want) {
			t.Fatalf("implementation boundaries missing %q: %#v", want, spec.ImplementationBoundaries)
		}
	}
}

func TestPlatformSpecCLIAPIConnectionAuthConfigPrecedencePromoted(t *testing.T) {
	spec := loadCLIAPIConnectionAuthConfigSpec(t)
	platformSpecData, err := os.ReadFile(filepath.Join(repoRoot(), defaultPlatformSpecPath))
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
	for _, want := range []string{"API-backed command leaves consume", "OpenRPC", "root/global `--config`"} {
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
	if spec.APIServer.AcceptedSources.ConfigKey != "api_server" {
		t.Fatalf("api_server config key = %q, want api_server", spec.APIServer.AcceptedSources.ConfigKey)
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
	wantTokenSourceOrder := []string{"--api-token-file", "context descriptor auth", "config api_token_file", "built-in loopback default"}
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
		"SWARM_API_TOKEN_FILE": "config `api_token_file`",
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
		"environment": "SWARM_CONFIG",
		"xdg_default": "swarm/config.yaml",
	} {
		if !strings.Contains(spec.CLIConfigFile.AcceptedSources[key], want) {
			t.Fatalf("cli config accepted source %q missing %q:\n%s", key, want, spec.CLIConfigFile.AcceptedSources[key])
		}
	}
	if !strings.Contains(spec.CLIConfigFile.RejectedSources["--config"], "dual semantic ownership") {
		t.Fatalf("cli config --config rejection missing dual-ownership rule:\n%s", spec.CLIConfigFile.RejectedSources["--config"])
	}
	for _, key := range []string{"api_server", "api_token_file"} {
		if strings.TrimSpace(spec.CLIConfigFile.AcceptedKeys[key]) == "" {
			t.Fatalf("cli config accepted key %q missing: %#v", key, spec.CLIConfigFile.AcceptedKeys)
		}
	}
	for _, key := range []string{"api_token", "output_format", "no_color", "log_level", "retry"} {
		if strings.TrimSpace(spec.CLIConfigFile.RejectedKeys[key]) == "" {
			t.Fatalf("cli config rejected key %q missing: %#v", key, spec.CLIConfigFile.RejectedKeys)
		}
	}
	for _, key := range []string{"contracts_path", "platform_spec_path"} {
		if !strings.Contains(spec.CLIConfigFile.SharedNonAPIKeys[key], "contract_platform_spec_path_resolution") {
			t.Fatalf("cli config shared non-API key %q missing contract path owner: %#v", key, spec.CLIConfigFile.SharedNonAPIKeys)
		}
	}
	for _, key := range []string{"serve_api_listen_addr", "serve_mcp_listen_addr"} {
		if !strings.Contains(spec.CLIConfigFile.SharedNonAPIKeys[key], "listener_topology_v2_1.source_precedence") {
			t.Fatalf("cli config shared non-API key %q missing listener source owner: %#v", key, spec.CLIConfigFile.SharedNonAPIKeys)
		}
	}
	if !strings.Contains(spec.CLIConfigFile.SharedNonAPIKeys["serve_api_token_file"], "server_api_auth") ||
		!strings.Contains(spec.CLIConfigFile.SharedNonAPIKeys["serve_api_token_file"], "server-side `swarm serve` auth only") ||
		!strings.Contains(spec.CLIConfigFile.SharedNonAPIKeys["serve_api_token_file"], "MUST NOT") {
		t.Fatalf("cli config serve_api_token_file missing server/client boundary: %#v", spec.CLIConfigFile.SharedNonAPIKeys["serve_api_token_file"])
	}
	for _, key := range []string{"SWARM_API_PORT", "SWARM_MCP_PORT"} {
		if !strings.Contains(spec.ServeListenerEnvConfigBoundary.RejectedPorts[key], "Not promoted") {
			t.Fatalf("serve listener rejected port %q missing not-promoted rule:\n%s", key, spec.ServeListenerEnvConfigBoundary.RejectedPorts[key])
		}
	}
	for _, key := range []string{"SWARM_API_LISTEN_ADDR", "SWARM_MCP_LISTEN_ADDR"} {
		if !strings.Contains(spec.ServeListenerEnvConfigBoundary.AcceptedListenerEnvironment[key], "listener_topology_v2_1") {
			t.Fatalf("serve listener accepted env %q missing listener owner:\n%s", key, spec.ServeListenerEnvConfigBoundary.AcceptedListenerEnvironment[key])
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
		{name: "run help", args: []string{"run", "--help"}, want: "Start a workflow run on a running runtime"},
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
	opts.runServe = func(_ context.Context, repo string, _ serveOptions) int {
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
	writeWorkflowValidationFixtureFile(t, filepath.Join(repo, ".env"), "SWARM_CONTRACTS_PATH="+contractsRoot+"\nBROKEN\n")
	chdirForTest(t, repo)

	var stdout, stderr bytes.Buffer
	code := executeRootCommand(context.Background(), "", []string{"verify"}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("verify unexpectedly consumed contracts path from repo .env: stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String()+stderr.String(), contractsRoot) {
		t.Fatalf("verify output referenced repo .env contracts path stdout=%s stderr=%s", stdout.String(), stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = executeRootCommand(context.Background(), "", []string{"verify", "--contracts", contractsRoot}, &stdout, &stderr)
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
	opts.apiOptions.runServe = func(ctx context.Context, repo string, _ serveOptions) int {
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
	code := runVerifyCommandWithContractsForTest(context.Background(), "", root, &buf)
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
	code := runVerifyCommandWithContractsForTest(context.Background(), "", root, &buf)
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
	code := runVerifyCommandWithContractsForTest(context.Background(), "", root, &buf)
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
	code := runVerifyCommandWithContractsForTest(context.Background(), "", root, &buf)
	if code == 0 {
		t.Fatalf("runVerifyCommand exit code = 0, output = %q", buf.String())
	}
	for _, want := range []string{"verify failed: load Swarm contracts:", "UNDEFINED-FIELD", "homepage"} {
		if !strings.Contains(buf.String(), want) {
			t.Fatalf("verify output missing %q:\n%s", want, buf.String())
		}
	}
}

func TestConfiguredWorkspaceLifecycleDoesNotInventSourceRootDataSource(t *testing.T) {
	t.Setenv("SWARM_WORKSPACE_DATA_SOURCE", "")
	t.Setenv("SWARM_WORKSPACE_CONTRACTS_SOURCE", "")
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}

	manager, err := configuredWorkspaceLifecycle(nil, contractsDir, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), workspaceMountSources{})
	if err != nil {
		t.Fatalf("configuredWorkspaceLifecycle: %v", err)
	}
	err = manager.ValidateSource(context.Background(), semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
	if err == nil || !strings.Contains(err.Error(), "/data source is not configured") {
		t.Fatalf("ValidateSource error = %v, want explicit /data source requirement", err)
	}
}

func TestConfiguredWorkspaceLifecycleUsesExplicitDataAndContractsSources(t *testing.T) {
	t.Setenv("SWARM_WORKSPACE_DATA_SOURCE", "")
	t.Setenv("SWARM_WORKSPACE_CONTRACTS_SOURCE", "")
	dataDir := t.TempDir()
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}

	manager, err := configuredWorkspaceLifecycle(nil, contractsDir, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), workspaceMountSources{
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
	t.Setenv("SWARM_WORKSPACE_DATA_SOURCE", "")
	t.Setenv("SWARM_WORKSPACE_CONTRACTS_SOURCE", "")
	missingDataDir := filepath.Join(t.TempDir(), "missing-data")
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}

	manager, err := configuredWorkspaceLifecycle(nil, contractsDir, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), workspaceMountSources{
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
	t.Setenv("SWARM_WORKSPACE_VOLUMES_FROM", "swarm-orchestrator")
	_, err := configuredWorkspaceLifecycle(nil, t.TempDir(), semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), workspaceMountSources{
		DataSource:       t.TempDir(),
		DataSourceSource: "workspace.data_source",
	})
	if err == nil || !strings.Contains(err.Error(), "cannot be combined with SWARM_WORKSPACE_VOLUMES_FROM") {
		t.Fatalf("configuredWorkspaceLifecycle error = %v, want volumes-from conflict", err)
	}
}

func TestConfiguredWorkspaceLifecycleForBackendSelectsHostWithoutDocker(t *testing.T) {
	t.Setenv("SWARM_WORKSPACE_VOLUMES_FROM", "")
	t.Setenv("SWARM_WORKSPACE_HOST_ROOT", filepath.Join(t.TempDir(), "host-workspaces"))
	t.Setenv("SWARM_DOCKER_BIN", filepath.Join(t.TempDir(), "missing-docker"))
	dataDir := t.TempDir()
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}
	lifecycle, err := configuredWorkspaceLifecycleForBackend(nil, contractsDir, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), workspaceMountSources{
		DataSource:       dataDir,
		DataSourceSource: "--data",
	}, workspaceBackendSelection{Backend: workspace.BackendHost, Source: "--workspace-backend"})
	if err != nil {
		t.Fatalf("configuredWorkspaceLifecycleForBackend: %v", err)
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
	t.Setenv("SWARM_WORKSPACE_VOLUMES_FROM", "swarm-orchestrator")
	_, err := configuredWorkspaceLifecycleForBackend(nil, t.TempDir(), semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), workspaceMountSources{
		DataSource:       t.TempDir(),
		DataSourceSource: "--data",
	}, workspaceBackendSelection{Backend: workspace.BackendHost, Source: "--workspace-backend"})
	if err == nil || !strings.Contains(err.Error(), "host workspace backend cannot consume SWARM_WORKSPACE_VOLUMES_FROM") {
		t.Fatalf("configuredWorkspaceLifecycleForBackend error = %v, want host volumes-from rejection", err)
	}
}

func TestResolveWorkspaceMountSourcesPrecedence(t *testing.T) {
	repoRoot := t.TempDir()
	flagDir := filepath.Join(repoRoot, "flag-data")
	configDir := filepath.Join(repoRoot, "config-data")
	envDir := filepath.Join(repoRoot, "env-data")
	for _, dir := range []string{flagDir, configDir, envDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}

	result, err := resolveWorkspaceMountSourcesFromInput(workspaceDataSourceInput{
		RepoRoot:         repoRoot,
		FlagDataSource:   "flag-data",
		ConfigDataSource: "config-data",
		EnvDataSource:    "env-data",
		EnvDataSourceSet: true,
	})
	if err != nil {
		t.Fatalf("resolve workspace mount sources: %v", err)
	}
	if result.DataSource != flagDir || result.DataSourceSource != "--data" {
		t.Fatalf("flag precedence result = %#v, want source %q from --data", result, flagDir)
	}

	result, err = resolveWorkspaceMountSourcesFromInput(workspaceDataSourceInput{
		RepoRoot:            repoRoot,
		ConfigDataSource:    "config-data",
		ConfigDataSourceSet: true,
		EnvDataSource:       "env-data",
		EnvDataSourceSet:    true,
	})
	if err != nil {
		t.Fatalf("resolve config workspace mount source: %v", err)
	}
	if result.DataSource != configDir || result.DataSourceSource != "workspace.data_source" {
		t.Fatalf("config precedence result = %#v, want source %q from workspace.data_source", result, configDir)
	}

	result, err = resolveWorkspaceMountSourcesFromInput(workspaceDataSourceInput{
		RepoRoot:         repoRoot,
		EnvDataSource:    "env-data",
		EnvDataSourceSet: true,
		ConfigDataSource: " ",
		FlagDataSource:   " ",
	})
	if err != nil {
		t.Fatalf("resolve env workspace mount source: %v", err)
	}
	if result.DataSource != envDir || result.DataSourceSource != envWorkspaceDataSource {
		t.Fatalf("env precedence result = %#v, want source %q from %s", result, envDir, envWorkspaceDataSource)
	}

	result, err = resolveWorkspaceMountSourcesFromInput(workspaceDataSourceInput{
		RepoRoot:                repoRoot,
		DefaultDataSource:       filepath.Join(repoRoot, defaultWorkspaceDataSourceRelativePath),
		DefaultDataSourceSource: defaultWorkspaceDataSourceSource,
		CreateDefaultDataSource: true,
	})
	if err != nil {
		t.Fatalf("resolve default workspace mount source: %v", err)
	}
	defaultDir := filepath.Join(repoRoot, defaultWorkspaceDataSourceRelativePath)
	if result.DataSource != defaultDir || result.DataSourceSource != defaultWorkspaceDataSourceSource {
		t.Fatalf("default result = %#v, want source %q from %s", result, defaultDir, defaultWorkspaceDataSourceSource)
	}
	if info, err := os.Stat(defaultDir); err != nil || !info.IsDir() {
		t.Fatalf("default data source stat = (%v, %v), want created directory", info, err)
	}
}

func TestResolveWorkspaceMountSourcesRejectsEmptyConfigBeforeEnvFallback(t *testing.T) {
	repoRoot := t.TempDir()
	envDir := t.TempDir()
	result, err := resolveWorkspaceMountSourcesFromInput(workspaceDataSourceInput{
		RepoRoot:            repoRoot,
		ConfigDataSource:    " \t ",
		ConfigDataSourceSet: true,
		EnvDataSource:       envDir,
		EnvDataSourceSet:    true,
	})
	if err == nil || !strings.Contains(err.Error(), "workspace.data_source") || !strings.Contains(err.Error(), "must be non-empty") {
		t.Fatalf("resolve workspace mount sources error = %v, want empty workspace.data_source rejection", err)
	}
	if result.DataSource != "" || result.DataSourceSource != "workspace.data_source" {
		t.Fatalf("workspace mount sources = %#v, want no env fallback and workspace.data_source source label", result)
	}
}

func TestResolveWorkspaceMountSourcesRejectsEmptyEnvBeforeDefault(t *testing.T) {
	repoRoot := t.TempDir()
	result, err := resolveWorkspaceMountSourcesFromInput(workspaceDataSourceInput{
		RepoRoot:         repoRoot,
		EnvDataSource:    " \t ",
		EnvDataSourceSet: true,
		VolumesFrom:      "swarm-orchestrator",
		VolumesFromSet:   true,
	})
	if err == nil || !strings.Contains(err.Error(), envWorkspaceDataSource) || !strings.Contains(err.Error(), "must be non-empty") {
		t.Fatalf("resolve workspace mount sources error = %v, want empty env rejection", err)
	}
	if result.DataSource != "" || result.DataSourceSource != envWorkspaceDataSource {
		t.Fatalf("workspace mount sources = %#v, want no default or volumes-from fallback and env source label", result)
	}
	if _, err := os.Stat(filepath.Join(repoRoot, defaultWorkspaceDataSourceRelativePath)); !os.IsNotExist(err) {
		t.Fatalf("default data source stat error = %v, want not created", err)
	}
}

func TestResolveWorkspaceMountSourcesDefaultsToSwarmDataNotRepoData(t *testing.T) {
	repoRoot := t.TempDir()
	repoDataDir := filepath.Join(repoRoot, "data")
	if err := os.MkdirAll(repoDataDir, 0o755); err != nil {
		t.Fatalf("mkdir repo data: %v", err)
	}
	result, err := resolveWorkspaceMountSourcesFromInput(workspaceDataSourceInput{
		RepoRoot:                repoRoot,
		DefaultDataSource:       filepath.Join(repoRoot, defaultWorkspaceDataSourceRelativePath),
		DefaultDataSourceSource: defaultWorkspaceDataSourceSource,
		CreateDefaultDataSource: true,
	})
	if err != nil {
		t.Fatalf("resolve workspace mount sources: %v", err)
	}
	defaultDir := filepath.Join(repoRoot, defaultWorkspaceDataSourceRelativePath)
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
	repoRoot := t.TempDir()
	result, err := resolveWorkspaceMountSourcesFromInput(workspaceDataSourceInput{
		RepoRoot:       repoRoot,
		VolumesFrom:    "swarm-orchestrator",
		VolumesFromSet: true,
	})
	if err != nil {
		t.Fatalf("resolve workspace mount sources: %v", err)
	}
	if result.DataSource != "" || result.DataSourceSource != "" {
		t.Fatalf("workspace mount sources = %#v, want volumes-from alternate without path source", result)
	}
	if _, err := os.Stat(filepath.Join(repoRoot, defaultWorkspaceDataSourceRelativePath)); !os.IsNotExist(err) {
		t.Fatalf("default data source stat error = %v, want not created", err)
	}
}

func TestResolveWorkspaceMountSourcesReadsRuntimeConfigAndEnvFallback(t *testing.T) {
	repoRoot := t.TempDir()
	configDir := t.TempDir()
	envDir := t.TempDir()
	t.Setenv(envWorkspaceDataSource, envDir)

	result, err := resolveWorkspaceMountSources(repoRoot, "", &config.Config{
		Workspace: config.WorkspaceConfig{DataSource: configDir},
	})
	if err != nil {
		t.Fatalf("resolve config workspace mount sources: %v", err)
	}
	if result.DataSource != configDir || result.DataSourceSource != "workspace.data_source" {
		t.Fatalf("config-backed workspace mount sources = %#v, want %q from workspace.data_source", result, configDir)
	}

	result, err = resolveWorkspaceMountSources(repoRoot, "", &config.Config{})
	if err != nil {
		t.Fatalf("resolve env workspace mount sources: %v", err)
	}
	if result.DataSource != envDir || result.DataSourceSource != envWorkspaceDataSource {
		t.Fatalf("env-backed workspace mount sources = %#v, want %q from %s", result, envDir, envWorkspaceDataSource)
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
	result, err = resolveWorkspaceMountSources(repoRoot, "", cfg)
	if err == nil || !strings.Contains(err.Error(), "workspace.data_source") || !strings.Contains(err.Error(), "must be non-empty") {
		t.Fatalf("resolve empty configured workspace source error = %v, want fail-closed config rejection before env fallback", err)
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
		EnvBackend:    workspace.BackendDocker,
		EnvSet:        true,
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
		EnvBackend:    workspace.BackendDocker,
		EnvSet:        true,
	})
	if err != nil {
		t.Fatalf("resolve workspace backend config precedence: %v", err)
	}
	if result.Backend != workspace.BackendHost || result.Source != "workspace.backend" {
		t.Fatalf("config precedence result = %#v, want host from workspace.backend", result)
	}

	result, err = resolveWorkspaceBackendFromInput(workspaceBackendInput{
		EnvBackend: workspace.BackendHost,
		EnvSet:     true,
	})
	if err != nil {
		t.Fatalf("resolve workspace backend env precedence: %v", err)
	}
	if result.Backend != workspace.BackendHost || result.Source != envWorkspaceBackend {
		t.Fatalf("env precedence result = %#v, want host from %s", result, envWorkspaceBackend)
	}

	result, err = resolveWorkspaceBackendFromInput(workspaceBackendInput{})
	if err != nil {
		t.Fatalf("resolve workspace backend default: %v", err)
	}
	if result.Backend != workspace.BackendDocker || result.Source != "default" {
		t.Fatalf("default result = %#v, want docker default", result)
	}
}

func TestResolveWorkspaceBackendRejectsEmptyConfigBeforeEnvFallback(t *testing.T) {
	result, err := resolveWorkspaceBackendFromInput(workspaceBackendInput{
		ConfigBackend: " \t ",
		ConfigSet:     true,
		EnvBackend:    workspace.BackendHost,
		EnvSet:        true,
	})
	if err == nil || !strings.Contains(err.Error(), "workspace.backend") || !strings.Contains(err.Error(), "must be non-empty") {
		t.Fatalf("resolve empty configured workspace backend error = %v, want fail-closed config rejection before env fallback", err)
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
	data, err := os.ReadFile(filepath.Join(repoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	if err := yaml.Unmarshal(data, &spec); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
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

func TestPlatformSpecWorkspaceDataSourceAuthorityPromoted(t *testing.T) {
	var spec struct {
		WorkspaceModel struct {
			DataSourceAuthority struct {
				PromotedBy                     string   `yaml:"promoted_by"`
				ImplementationStatus           string   `yaml:"implementation_status"`
				CanonicalOwner                 string   `yaml:"canonical_owner"`
				CLIFlag                        string   `yaml:"cli_flag"`
				ConfigKey                      string   `yaml:"config_key"`
				EnvVar                         string   `yaml:"env_var"`
				SourceOrder                    []string `yaml:"source_order"`
				DefaultBehavior                string   `yaml:"default_behavior"`
				FailureBehavior                string   `yaml:"failure_behavior"`
				VolumesFromConflictRule        string   `yaml:"volumes_from_conflict_rule"`
				ReadPolicy                     string   `yaml:"read_policy"`
				RetiredNonAuthoritativeSources []string `yaml:"retired_non_authoritative_sources"`
				SplitScope                     []string `yaml:"split_scope"`
			} `yaml:"data_source_authority"`
		} `yaml:"workspace_model"`
		CLISpecification struct {
			CommandCatalog struct {
				Serve struct {
					WorkspaceDataSourceAuthority struct {
						PromotedBy string   `yaml:"promoted_by"`
						Owner      string   `yaml:"owner"`
						Flag       string   `yaml:"flag"`
						ConfigKey  string   `yaml:"config_key"`
						EnvVar     string   `yaml:"env_var"`
						Consumers  []string `yaml:"consumers"`
					} `yaml:"workspace_data_source_authority"`
				} `yaml:"serve"`
				Run struct {
					Flags []string `yaml:"flags"`
					Modes struct {
						ForegroundLocalStart struct {
							Invocation string `yaml:"invocation"`
							Behavior   string `yaml:"behavior"`
						} `yaml:"foreground_local_start"`
					} `yaml:"modes"`
				} `yaml:"run"`
			} `yaml:"command_catalog"`
		} `yaml:"cli_specification"`
	}
	data, err := os.ReadFile(filepath.Join(repoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	if err := yaml.Unmarshal(data, &spec); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
	authority := spec.WorkspaceModel.DataSourceAuthority
	if !strings.Contains(authority.PromotedBy, "#1139") || !strings.Contains(authority.PromotedBy, "#1223") || strings.TrimSpace(authority.ImplementationStatus) != "implemented" {
		t.Fatalf("workspace data source authority status = promoted_by:%q implementation_status:%q", authority.PromotedBy, authority.ImplementationStatus)
	}
	if !strings.Contains(authority.CanonicalOwner, "workspace_model.data_source_authority") {
		t.Fatalf("workspace data source canonical owner = %q", authority.CanonicalOwner)
	}
	if authority.CLIFlag != "--data" || authority.ConfigKey != "workspace.data_source" || authority.EnvVar != envWorkspaceDataSource {
		t.Fatalf("workspace data source selectors = %#v", authority)
	}
	for _, want := range []string{"--data", "workspace.data_source", envWorkspaceDataSource, defaultWorkspaceDataSourceSource} {
		if !stringSliceContains(authority.SourceOrder, want) {
			t.Fatalf("workspace data source order missing %q: %#v", want, authority.SourceOrder)
		}
	}
	for _, want := range []string{defaultWorkspaceDataSourceRelativePath, "0755", "repo-local `data/`", "fail closed", "SWARM_WORKSPACE_VOLUMES_FROM", "read-only", "opaque"} {
		if !strings.Contains(authority.DefaultBehavior+authority.FailureBehavior+authority.VolumesFromConflictRule+authority.ReadPolicy+strings.Join(authority.RetiredNonAuthoritativeSources, "\n"), want) {
			t.Fatalf("workspace data source spec missing %q:\n%#v", want, authority)
		}
	}
	for _, want := range []string{"#1137", "#1138", "#1214", "workspace-init", "SWARM_WORKSPACE_DATA_MOUNT"} {
		if !joinedContains(authority.SplitScope, want) {
			t.Fatalf("workspace data source split scope missing %q: %#v", want, authority.SplitScope)
		}
	}
	command := spec.CLISpecification.CommandCatalog.Serve.WorkspaceDataSourceAuthority
	if !strings.Contains(command.PromotedBy, "#1139") || !strings.Contains(command.PromotedBy, "#1223") || command.Owner != "workspace_model.data_source_authority" || command.Flag != "--data <path>" {
		t.Fatalf("serve command data authority = %#v", command)
	}
	for _, want := range []string{"serve boot", "local foreground swarm run", "Builder project reload", "selected-contract run-fork"} {
		if !joinedContains(command.Consumers, want) {
			t.Fatalf("serve command data authority consumers missing %q: %#v", want, command.Consumers)
		}
	}
	run := spec.CLISpecification.CommandCatalog.Run
	if !stringSliceContains(run.Flags, "--data <path>") {
		t.Fatalf("run command flags missing --data <path>: %#v", run.Flags)
	}
	if !strings.Contains(run.Modes.ForegroundLocalStart.Invocation, "--data <path>") || !strings.Contains(run.Modes.ForegroundLocalStart.Behavior, "workspace_model.data_source_authority") || !strings.Contains(run.Modes.ForegroundLocalStart.Behavior, "--connect") || !strings.Contains(run.Modes.ForegroundLocalStart.Behavior, "--reattach") {
		t.Fatalf("run local foreground data authority missing from spec: %#v", run.Modes.ForegroundLocalStart)
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
				EnvVar               string   `yaml:"env_var"`
				SourceOrder          []string `yaml:"source_order"`
				DefaultBackend       string   `yaml:"default_backend"`
				Backends             map[string]struct {
					Behavior      string `yaml:"behavior"`
					WorkspaceRoot struct {
						EnvVar  string `yaml:"env_var"`
						Default string `yaml:"default"`
						Rule    string `yaml:"rule"`
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
						PromotedBy     string   `yaml:"promoted_by"`
						Owner          string   `yaml:"owner"`
						Flag           string   `yaml:"flag"`
						ConfigKey      string   `yaml:"config_key"`
						EnvVar         string   `yaml:"env_var"`
						DefaultBackend string   `yaml:"default_backend"`
						Consumers      []string `yaml:"consumers"`
					} `yaml:"workspace_backend_selection"`
				} `yaml:"serve"`
			} `yaml:"command_catalog"`
		} `yaml:"cli_specification"`
	}
	data, err := os.ReadFile(filepath.Join(repoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	if err := yaml.Unmarshal(data, &spec); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
	authority := spec.WorkspaceModel.WorkspaceBackendSelection
	if strings.TrimSpace(authority.PromotedBy) != "#1138" || strings.TrimSpace(authority.ImplementationStatus) != "implemented_first_slice" {
		t.Fatalf("workspace backend authority status = promoted_by:%q implementation_status:%q", authority.PromotedBy, authority.ImplementationStatus)
	}
	if !strings.Contains(authority.CanonicalOwner, "workspace_model.workspace_backend_selection") {
		t.Fatalf("workspace backend canonical owner = %q", authority.CanonicalOwner)
	}
	if authority.CLIFlag != "--workspace-backend <docker|host>" || authority.ConfigKey != "workspace.backend" || authority.EnvVar != envWorkspaceBackend {
		t.Fatalf("workspace backend selectors = %#v", authority)
	}
	for _, want := range []string{"--workspace-backend", "workspace.backend", envWorkspaceBackend, "docker default"} {
		if !stringSliceContains(authority.SourceOrder, want) {
			t.Fatalf("workspace backend order missing %q: %#v", want, authority.SourceOrder)
		}
	}
	if authority.DefaultBackend != workspace.BackendDocker {
		t.Fatalf("workspace backend default = %q, want docker", authority.DefaultBackend)
	}
	dockerBackend, ok := authority.Backends[workspace.BackendDocker]
	if !ok || !strings.Contains(dockerBackend.Behavior, "Docker fail-closed") || !strings.Contains(dockerBackend.Behavior, "configured workspace image") {
		t.Fatalf("docker backend spec missing default fail-closed behavior: %#v", authority.Backends)
	}
	hostBackend, ok := authority.Backends[workspace.BackendHost]
	if !ok || !strings.Contains(hostBackend.Behavior, "Explicit local-dev opt-in") || !strings.Contains(hostBackend.Behavior, "MUST NOT require Docker") {
		t.Fatalf("host backend spec missing local-dev no-Docker behavior: %#v", authority.Backends)
	}
	if hostBackend.WorkspaceRoot.EnvVar != workspace.EnvHostWorkspaceRoot || hostBackend.WorkspaceRoot.Default != "~/.swarm/workspaces" {
		t.Fatalf("host workspace root spec = %#v", hostBackend.WorkspaceRoot)
	}
	for _, want := range []string{"canonical/evaluated paths", "Every host lifecycle consumer", "symlink escapes"} {
		if !strings.Contains(hostBackend.WorkspaceRoot.Rule, want) {
			t.Fatalf("host workspace root rule missing %q:\n%s", want, hostBackend.WorkspaceRoot.Rule)
		}
	}
	for _, want := range []string{"Unsupported backend", "Empty explicit backend", "SWARM_WORKSPACE_VOLUMES_FROM", "Claude CLI", "native tool execution"} {
		if !joinedContains(authority.FailureBehavior, want) {
			t.Fatalf("workspace backend failure behavior missing %q: %#v", want, authority.FailureBehavior)
		}
	}
	for _, want := range []string{"serve boot", "Builder project reload", "selected-contract run-fork"} {
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
	if command.ConfigKey != "workspace.backend" || command.EnvVar != envWorkspaceBackend || command.DefaultBackend != workspace.BackendDocker {
		t.Fatalf("serve command workspace backend selectors = %#v", command)
	}
	for _, want := range []string{"serve boot", "Builder project reload", "selected-contract run-fork"} {
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
	data, err := os.ReadFile(filepath.Join(repoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	if err := yaml.Unmarshal(data, &spec); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
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
				PromotedBy           string `yaml:"promoted_by"`
				ImplementationStatus string `yaml:"implementation_status"`
				CanonicalOwner       string `yaml:"canonical_owner"`
				Rule                 string `yaml:"rule"`
				MountSourceRule      string `yaml:"mount_source_rule"`
				SplitScope           string `yaml:"split_scope"`
			} `yaml:"runtime_image_packaging"`
		} `yaml:"workspace_model"`
	}
	data, err := os.ReadFile(filepath.Join(repoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	if err := yaml.Unmarshal(data, &spec); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
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
	data, err := os.ReadFile(filepath.Join(repoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	if err := yaml.Unmarshal(data, &spec); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
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
	for _, want := range []string{"SWARM_TOOL_GATEWAY_TOKEN", "removed", "per-boot", "binding token", "SWARM_TOOL_GATEWAY_URL", "not source authority", "Local foreground `swarm run`"} {
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
	data, err := os.ReadFile(filepath.Join(repoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	if err := yaml.Unmarshal(data, &spec); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
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
	data, err := os.ReadFile(filepath.Join(repoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	if err := yaml.Unmarshal(data, &spec); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
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
	for _, want := range []string{"remaining #997", "workspace image/default-agent Claude CLI availability", "local `swarm serve`", "local foreground `swarm run`"} {
		if !strings.Contains(availability.Scope, want) {
			t.Fatalf("local cli_test workspace cli availability scope missing %q:\n%s", want, availability.Scope)
		}
	}
	for _, want := range []string{"startup validation MUST prove", "before readiness or event delivery", "Docker image inspection", "existing container reuse", "SWARM_WORKSPACE_IMAGE"} {
		if !strings.Contains(availability.WorkspaceCLIRule, want) {
			t.Fatalf("workspace cli rule missing %q:\n%s", want, availability.WorkspaceCLIRule)
		}
	}
	for _, want := range []string{"local `swarm serve`", "local foreground `swarm run`", "managed-agent startup validation", "Claude CLI startup probe", "configured workspace image/container targets"} {
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
	data, err := os.ReadFile(filepath.Join(repoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	if err := yaml.Unmarshal(data, &spec); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
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
	for _, want := range []string{"swarm verify", "swarm serve", "local foreground `swarm run`"} {
		if !stringSliceContains(spec.AppliesTo, want) {
			t.Fatalf("applies_to missing %q: %#v", want, spec.AppliesTo)
		}
	}
	for _, want := range []string{"swarm run --connect", "swarm run --reattach", "root/global `--config`"} {
		if !stringSliceContains(spec.NotAppliesTo, want) {
			t.Fatalf("not_applies_to missing %q: %#v", want, spec.NotAppliesTo)
		}
	}
	wantContractsOrder := []string{"--contracts", "SWARM_CONTRACTS_PATH", "config contracts_path", "repo contracts/package.yaml"}
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
	if spec.ContractsPath.AcceptedSources.ConfigKey != "contracts_path" {
		t.Fatalf("contracts config key = %q", spec.ContractsPath.AcceptedSources.ConfigKey)
	}
	if !strings.Contains(spec.ContractsPath.RejectedSources["SWARM_CONTRACTS_DIR"], "Not a CLI source") {
		t.Fatalf("SWARM_CONTRACTS_DIR rejection missing CLI-source rule:\n%s", spec.ContractsPath.RejectedSources["SWARM_CONTRACTS_DIR"])
	}
	wantPlatformOrder := []string{"--platform-spec", "config platform_spec_path", "embedded tracked platform spec"}
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
	if spec.PlatformSpecPath.AcceptedSources.ConfigKey != "platform_spec_path" {
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
	var captured serveOptions
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ string, serveOpts serveOptions) int {
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
	var captured serveOptions
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ string, serveOpts serveOptions) int {
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
	var captured serveOptions
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ string, serveOpts serveOptions) int {
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
			opts.runServe = func(context.Context, string, serveOptions) int {
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

func TestCloseServeRuntimeDevCleanupRunsAfterShutdownAndJoinsErrors(t *testing.T) {
	shutdownErr := fmt.Errorf("shutdown timed out")
	cleanupErr := fmt.Errorf("cleanup failed")
	var order []string
	supervisor := &runtimeProjectSupervisor{
		currentRT: &runtimepkg.Runtime{},
	}
	supervisor.shutdownRuntime = func(context.Context, *runtimepkg.Runtime, runtimepkg.ShutdownOptions) error {
		order = append(order, "shutdown")
		return shutdownErr
	}
	workspaces := serveRuntimeWorkspaceStub{
		cleanup: func(context.Context) (runtimedestructivereset.ContainerResetResult, error) {
			order = append(order, "cleanup")
			return runtimedestructivereset.ContainerResetResult{}, cleanupErr
		},
	}

	err := closeServeRuntime(context.Background(), supervisor, serveOptions{
		Dev:           true,
		ShutdownGrace: runtimepkg.DefaultShutdownGrace,
	}, workspaces)
	if err == nil || !strings.Contains(err.Error(), shutdownErr.Error()) || !strings.Contains(err.Error(), cleanupErr.Error()) {
		t.Fatalf("closeServeRuntime err = %v, want joined shutdown and cleanup errors", err)
	}
	if got := strings.Join(order, ","); got != "shutdown,cleanup" {
		t.Fatalf("order = %s, want shutdown,cleanup", got)
	}
	if got := supervisor.CurrentRuntime(); got != nil {
		t.Fatalf("CurrentRuntime after close = %p, want nil", got)
	}
}

func TestServeBootReporterEmitsNumberedProgressOnlyWhenVerbose(t *testing.T) {
	var quiet bytes.Buffer
	newServeBootReporter(false, &quiet).emit(1, "process_start", "ok", "")
	if quiet.Len() != 0 {
		t.Fatalf("quiet reporter output = %q, want empty", quiet.String())
	}

	var verbose bytes.Buffer
	newServeBootReporter(true, &verbose).emit(1, "process_start", "ok", "contracts=contracts")
	out := verbose.String()
	for _, want := range []string{"[1/22]", "process_start", "ok", "contracts=contracts"} {
		if !strings.Contains(out, want) {
			t.Fatalf("verbose output missing %q:\n%s", want, out)
		}
	}
}

func TestServeBundleMatchAdmissionRejectsActiveAvailabilityConflicts(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	bootFingerprint := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	persistedMissingRunID := uuid.NewString()
	legacyRunID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source, started_at)
		VALUES ($1::uuid, 'running', 'bundle-v1:sha256:2222222222222222222222222222222222222222222222222222222222222222', 'persisted', now())
	`, persistedMissingRunID); err != nil {
		t.Fatalf("seed active persisted missing run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_source, bundle_fingerprint, started_at)
		VALUES ($1::uuid, 'paused', 'legacy', $2, now())
	`, legacyRunID, bootFingerprint); err != nil {
		t.Fatalf("seed active legacy run: %v", err)
	}

	err := enforceServeBundleMatchAdmission(ctx, pg, bootFingerprint, true, "")
	if err == nil {
		t.Fatal("enforceServeBundleMatchAdmission error = nil, want availability conflict")
	}
	got := err.Error()
	for _, want := range []string{
		"active run bundle availability conflict",
		persistedMissingRunID,
		"BUNDLE_DATA_INTEGRITY_ERROR",
		"persisted_missing_bundle_row",
		legacyRunID,
		"BUNDLE_UNAVAILABLE",
		"cause=legacy",
		"legacy_bundle_fingerprint=" + bootFingerprint,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("admission error = %q, want detail %q", got, want)
		}
	}

	pinnedHash := "bundle-v1:sha256:3333333333333333333333333333333333333333333333333333333333333333"
	err = enforceServeBundleMatchAdmission(ctx, pg, pinnedHash, false, pinnedHash)
	if err == nil || !strings.Contains(err.Error(), "active run bundle availability conflict") {
		t.Fatalf("DB-loaded disabled legacy admission error = %v, want active availability conflict", err)
	}
}

func TestServeBundleMatchAdmissionAllowsPersistedPresentAndDisabled(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	bootFingerprint := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	persistedHash := "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111"
	missingHash := "bundle-v1:sha256:2222222222222222222222222222222222222222222222222222222222222222"

	if _, err := db.ExecContext(ctx, `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json)
		VALUES ($1, 'name: serve-test', '{}'::jsonb)
	`, persistedHash); err != nil {
		t.Fatalf("seed bundle row: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source, started_at)
		VALUES
			($1::uuid, 'running', $2, 'persisted', now()),
			($3::uuid, 'completed', $4, 'persisted', now())
	`, uuid.NewString(), persistedHash, uuid.NewString(), missingHash); err != nil {
		t.Fatalf("seed persisted-present and completed-missing runs: %v", err)
	}
	if err := enforceServeBundleMatchAdmission(ctx, pg, bootFingerprint, true, ""); err != nil {
		t.Fatalf("enforceServeBundleMatchAdmission persisted-present/completed-missing: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source, started_at)
		VALUES ($1::uuid, 'running', $2, 'persisted', now())
	`, uuid.NewString(), missingHash); err != nil {
		t.Fatalf("seed disabled persisted-missing run: %v", err)
	}
	if err := enforceServeBundleMatchAdmission(ctx, pg, bootFingerprint, false, ""); err != nil {
		t.Fatalf("enforceServeBundleMatchAdmission disabled: %v", err)
	}
}

func TestServeBundleMatchAdmissionRejectsDifferentPersistedActiveRunInDBLoadedMode(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	pinnedHash := "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111"
	otherHash := "bundle-v1:sha256:2222222222222222222222222222222222222222222222222222222222222222"
	otherRunID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json)
		VALUES
			($1, 'name: pinned', '{}'::jsonb),
			($2, 'name: other', '{}'::jsonb)
	`, pinnedHash, otherHash); err != nil {
		t.Fatalf("seed bundle rows: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source, started_at)
		VALUES
			($1::uuid, 'running', $2, 'persisted', now()),
			($3::uuid, 'paused', $4, 'persisted', now()),
			($5::uuid, 'completed', $4, 'persisted', now())
	`, uuid.NewString(), pinnedHash, otherRunID, otherHash, uuid.NewString()); err != nil {
		t.Fatalf("seed active persisted runs: %v", err)
	}

	err := enforceServeBundleMatchAdmission(ctx, pg, pinnedHash, true, pinnedHash)
	if err == nil {
		t.Fatal("enforceServeBundleMatchAdmission error = nil, want pinned bundle_hash conflict")
	}
	got := err.Error()
	for _, want := range []string{
		"active run pinned bundle_hash conflict",
		pinnedHash,
		otherRunID,
		otherHash,
		"bundle_source=persisted",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("admission error = %q, want detail %q", got, want)
		}
	}

	err = enforceServeBundleMatchAdmission(ctx, pg, pinnedHash, false, pinnedHash)
	if err == nil || !strings.Contains(err.Error(), "active run pinned bundle_hash conflict") {
		t.Fatalf("disabled legacy admission error = %v, want DB-loaded pinned conflict", err)
	}
}

func TestLoadServeRuntimeBundleFromCatalogLoadsPersistedRuntimeSource(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	if _, err := pg.BindSchemaCapabilities(ctx); err != nil {
		t.Fatalf("BindSchemaCapabilities: %v", err)
	}
	bundle := loadWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier12-runtime-tools", "test-flow-data-access"))
	projection, err := runtimecontracts.BuildBundleCatalogProjection(bundle)
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection: %v", err)
	}
	if _, err := pg.UpsertBundleCatalog(ctx, store.BundleCatalogUpsert{
		BundleHash:  projection.BundleHash,
		ContentYAML: projection.ContentYAML,
		ParsedJSON:  projection.ParsedJSON,
		DataBlob:    projection.DataBlob,
		Metadata:    projection.Metadata,
	}); err != nil {
		t.Fatalf("UpsertBundleCatalog: %v", err)
	}
	runningPlatformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot())
	if _, err := loadServeRuntimeBundleFromCatalog(ctx, repoRoot(), storeBundle{}, projection.BundleHash, runningPlatformSpecPath); err == nil || !strings.Contains(err.Error(), "requires selected bundle catalog store") {
		t.Fatalf("loadServeRuntimeBundleFromCatalog without selected catalog err = %v, want selected-owner failure", err)
	}

	stores := selectedPostgresStoreBundle(pg, &config.Config{})
	if stores.InboundStore == nil || stores.runtimeStores().InboundStore == nil {
		t.Fatal("selected Postgres store bundle missing InboundStore for served webhook ingress")
	}
	loaded, err := loadServeRuntimeBundleFromCatalog(ctx, repoRoot(), stores, projection.BundleHash, runningPlatformSpecPath)
	if err != nil {
		t.Fatalf("loadServeRuntimeBundleFromCatalog: %v", err)
	}
	defer func() {
		if loaded.cleanup != nil {
			if err := loaded.cleanup(); err != nil {
				t.Fatalf("cleanup DB-loaded runtime source: %v", err)
			}
		}
	}()

	if !loaded.dbLoaded {
		t.Fatal("dbLoaded = false, want true")
	}
	if loaded.serveIdentityDetail() != projection.BundleHash {
		t.Fatalf("serve identity = %q, want bundle hash %q", loaded.serveIdentityDetail(), projection.BundleHash)
	}
	if loaded.bundleSourceFact.BundleHash != projection.BundleHash || loaded.bundleSourceFact.BundleSource != storerunlifecycle.BundleSourcePersisted {
		t.Fatalf("bundle source fact = %#v, want persisted %s", loaded.bundleSourceFact, projection.BundleHash)
	}
	if strings.Contains(loaded.contractsRoot, filepath.Join("tests", "tier12-runtime-tools", "test-flow-data-access")) {
		t.Fatalf("DB-loaded contracts root leaked local fixture path: %s", loaded.contractsRoot)
	}
	if _, err := os.Stat(filepath.Join(loaded.contractsRoot, "flows", "support", "data", "exclusions.yaml")); err != nil {
		t.Fatalf("DB-loaded source missing reconstructed data file: %v", err)
	}
	prepared, err := prepareLoadedServeBundleSource(ctx, stores, loaded, false)
	if err != nil {
		t.Fatalf("prepareLoadedServeBundleSource: %v", err)
	}
	if prepared.BundleHash != projection.BundleHash || prepared.BundleSource != storerunlifecycle.BundleSourcePersisted {
		t.Fatalf("prepared source fact = %#v, want persisted %s", prepared, projection.BundleHash)
	}
	if _, err := prepareLoadedServeBundleSource(ctx, stores, loaded, true); err == nil || !strings.Contains(err.Error(), "--bundle-hash is mutually exclusive with --dev") {
		t.Fatalf("prepareLoadedServeBundleSource dev error = %v", err)
	}
}

func TestRunServeRuntimeDBLoadedUsesEmbeddedSpecBeforeCatalogRead(t *testing.T) {
	_, _, pg := installServeRuntimePostgresTestStores(t, func() serveWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	ctx := context.Background()
	bundle := loadWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier8-boot-verification", "test-boot-success"))
	projection, err := runtimecontracts.BuildBundleCatalogProjection(bundle)
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection: %v", err)
	}
	if _, err := pg.UpsertBundleCatalog(ctx, store.BundleCatalogUpsert{
		BundleHash:  projection.BundleHash,
		ContentYAML: projection.ContentYAML,
		ParsedJSON:  projection.ParsedJSON,
		DataBlob:    projection.DataBlob,
		Metadata:    projection.Metadata,
	}); err != nil {
		t.Fatalf("UpsertBundleCatalog: %v", err)
	}

	serve := startServeRuntimeTestProcess(t, serveOptions{
		ConfigPath:         writeServeRuntimeTestConfig(t),
		BundleHash:         projection.BundleHash,
		PlatformSpecPath:   filepath.Join(t.TempDir(), "missing-platform-spec.yaml"),
		StoreMode:          "postgres",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: true,
		Verbose:            true,
	})

	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("runServeRuntime code = %d\noutput:\n%s", code, serve.outputString())
	}
	if strings.Contains(serve.outputString(), "missing-platform-spec.yaml") || strings.Contains(serve.outputString(), "read platform spec") {
		t.Fatalf("DB-loaded serve used missing local platform spec before catalog read:\n%s", serve.outputString())
	}
}

func TestRunServeRuntimeDBLoadedRunForkSupportedSurfaceExecutesAndStampsPersistedIdentity(t *testing.T) {
	_, db, pg := installServeRuntimePostgresTestStores(t, func() serveWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	ctx := context.Background()
	bundle := loadWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier8-boot-verification", "test-boot-success"))
	projection, err := runtimecontracts.BuildBundleCatalogProjection(bundle)
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection: %v", err)
	}
	if _, err := pg.UpsertBundleCatalog(ctx, store.BundleCatalogUpsert{
		BundleHash:  projection.BundleHash,
		ContentYAML: projection.ContentYAML,
		ParsedJSON:  projection.ParsedJSON,
		DataBlob:    projection.DataBlob,
		Metadata:    projection.Metadata,
	}); err != nil {
		t.Fatalf("UpsertBundleCatalog: %v", err)
	}

	serve := startServeRuntimeTestProcess(t, serveOptions{
		ConfigPath:         writeServeRuntimeTestConfig(t),
		BundleHash:         projection.BundleHash,
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "postgres",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: true,
		Verbose:            true,
	})
	serve.waitForReadyLine()
	apiAddr := serveRuntimeAPIListenerFromOutput(t, serve.outputString())

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700000340, 0).UTC()
	seedRunForkSelectedExecutionSourceEvent(t, db, sourceRunID, entityID, sourceEventID, "task.requested", "complete-task", "pending", "Serve DB Loaded Entity", "serve-db-loaded-test", at)
	if _, err := db.ExecContext(ctx, `
		UPDATE runs
		SET bundle_hash = $2,
		    bundle_source = $3
		WHERE run_id = $1::uuid
	`, sourceRunID, projection.BundleHash, storerunlifecycle.BundleSourcePersisted); err != nil {
		t.Fatalf("stamp source run bundle identity: %v", err)
	}

	body := fmt.Sprintf(
		`{"jsonrpc":"2.0","id":"fork","method":"run.fork","params":{"source_run_id":%q,"fork_event_id":%q,"idempotency_key":"db-loaded-serve-fork"}}`,
		sourceRunID,
		sourceEventID,
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+apiAddr+"/v1/rpc", strings.NewReader(body))
	if err != nil {
		t.Fatalf("build run.fork request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/rpc run.fork: %v\nserve output:\n%s", err, serve.outputString())
	}
	defer resp.Body.Close()
	var rpc struct {
		Result apiv1.RunForkExecutionResult `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
			Data    any    `json:"data,omitempty"`
		} `json:"error,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rpc); err != nil {
		t.Fatalf("decode run.fork response: %v", err)
	}
	if resp.StatusCode != http.StatusOK || rpc.Error != nil {
		t.Fatalf("run.fork status=%d error=%#v result=%#v\nserve output:\n%s", resp.StatusCode, rpc.Error, rpc.Result, serve.outputString())
	}
	if rpc.Result.SourceRunID != sourceRunID || rpc.Result.BundleHash != projection.BundleHash || rpc.Result.ExecutedEventCount != 1 {
		t.Fatalf("run.fork result = %#v, want source=%s bundle_hash=%s executed=1", rpc.Result, sourceRunID, projection.BundleHash)
	}
	if rpc.Result.ForkRunID == "" || rpc.Result.ForkEventID != sourceEventID {
		t.Fatalf("run.fork fork identity = %#v, want fork run and source fork event %s", rpc.Result, sourceEventID)
	}

	var forkBundleHash, forkBundleSource, forkBundleFingerprint string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(bundle_hash, ''), COALESCE(bundle_source, ''), COALESCE(bundle_fingerprint, '')
		FROM runs
		WHERE run_id = $1::uuid
	`, rpc.Result.ForkRunID).Scan(&forkBundleHash, &forkBundleSource, &forkBundleFingerprint); err != nil {
		t.Fatalf("load fork run bundle identity: %v", err)
	}
	if forkBundleHash != projection.BundleHash || forkBundleSource != storerunlifecycle.BundleSourcePersisted || forkBundleFingerprint != "" {
		t.Fatalf("fork bundle identity = hash:%q source:%q fingerprint:%q, want persisted %s without legacy fingerprint", forkBundleHash, forkBundleSource, forkBundleFingerprint, projection.BundleHash)
	}
	var lineageRows int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM run_fork_selected_contract_executions
		WHERE fork_run_id = $1::uuid
		  AND source_run_id = $2::uuid
		  AND source_event_id = $3::uuid
	`, rpc.Result.ForkRunID, sourceRunID, sourceEventID).Scan(&lineageRows); err != nil {
		t.Fatalf("count selected-contract execution lineage: %v", err)
	}
	if lineageRows != 1 {
		t.Fatalf("selected-contract execution lineage rows = %d, want 1", lineageRows)
	}

	if code := serve.stop(); code != 0 {
		t.Fatalf("runServeRuntime code = %d\noutput:\n%s", code, serve.outputString())
	}
}

func TestRunServeRuntimeDBLoadedRunForkCrossBundleTargetExecutesAndStampsTargetIdentity(t *testing.T) {
	_, db, pg := installServeRuntimePostgresTestStores(t, func() serveWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	ctx := context.Background()
	sourceBundle := loadWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier8-boot-verification", "test-boot-success"))
	sourceProjection, err := runtimecontracts.BuildBundleCatalogProjection(sourceBundle)
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection(source): %v", err)
	}
	targetRoot := filepath.Join(t.TempDir(), "target-contracts")
	writeWorkflowValidationFixtureFile(t, filepath.Join(targetRoot, "package.yaml"), `
name: cross-bundle-target
version: 1.0.0
description: Cross-bundle target fixture for run.fork.
platform_version: ">=0.7.0 <0.8.0"
flows: []
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(targetRoot, "schema.yaml"), `
initial_state: pending
terminal_states: [done]
states: [pending, done]
pins:
  inputs:
    events: [task.requested]
  outputs:
    events: [task.completed]
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(targetRoot, "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(targetRoot, "tools.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(targetRoot, "agents.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(targetRoot, "nodes.yaml"), `
test-node:
  id: test-node
  execution_type: system_node
  subscribes_to: [task.requested]
  produces: [task.completed]
  event_handlers:
    task.requested:
      advances_to: done
      emit:
        event: task.completed
        broadcast: true
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(targetRoot, "events.yaml"), `
task.requested:
  swarm:
    source: external
task.completed:
  swarm:
    source: external
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(targetRoot, "entities.yaml"), `{}`)
	targetBundle := loadWorkflowValidationBundleAt(t, targetRoot)
	targetProjection, err := runtimecontracts.BuildBundleCatalogProjection(targetBundle)
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection(target): %v", err)
	}
	for label, projection := range map[string]runtimecontracts.BundleCatalogProjection{
		"source": sourceProjection,
		"target": targetProjection,
	} {
		if _, err := pg.UpsertBundleCatalog(ctx, store.BundleCatalogUpsert{
			BundleHash:  projection.BundleHash,
			ContentYAML: projection.ContentYAML,
			ParsedJSON:  projection.ParsedJSON,
			DataBlob:    projection.DataBlob,
			Metadata:    projection.Metadata,
		}); err != nil {
			t.Fatalf("UpsertBundleCatalog(%s): %v", label, err)
		}
	}
	if sourceProjection.BundleHash == targetProjection.BundleHash {
		t.Fatalf("source and target projections unexpectedly share hash %s", sourceProjection.BundleHash)
	}

	serve := startServeRuntimeTestProcess(t, serveOptions{
		ConfigPath:         writeServeRuntimeTestConfig(t),
		BundleHash:         sourceProjection.BundleHash,
		BundleHashes:       []string{targetProjection.BundleHash},
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "postgres",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: true,
		Verbose:            true,
	})
	serve.waitForReadyLine()
	apiAddr := serveRuntimeAPIListenerFromOutput(t, serve.outputString())

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700000345, 0).UTC()
	seedRunForkSelectedExecutionSourceEvent(t, db, sourceRunID, entityID, sourceEventID, "task.requested", "complete-task", "pending", "Serve Cross Bundle Entity", "serve-cross-bundle-test", at)
	if _, err := db.ExecContext(ctx, `
		UPDATE runs
		SET bundle_hash = $2,
		    bundle_source = $3
		WHERE run_id = $1::uuid
	`, sourceRunID, sourceProjection.BundleHash, storerunlifecycle.BundleSourcePersisted); err != nil {
		t.Fatalf("stamp source run bundle identity: %v", err)
	}

	var stdout, stderr bytes.Buffer
	cliOpts := defaultRootCommandOptions()
	cliOpts.apiRPCEndpointOverride = "http://" + apiAddr + "/v1/rpc"
	code := executeRootCommandWithOptions(ctx, t.TempDir(), []string{
		"fork", sourceRunID,
		"--bundle-hash", targetProjection.BundleHash,
		"--at-event", sourceEventID,
		"--idempotency-key", "db-loaded-cross-bundle-serve-fork",
		"--json",
	}, &stdout, &stderr, cliOpts)
	if code != 0 {
		t.Fatalf("swarm fork code=%d stderr=%s stdout=%s\nserve output:\n%s", code, stderr.String(), stdout.String(), serve.outputString())
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("swarm fork stderr=%q, want empty", stderr.String())
	}
	var rpcResult apiv1.RunForkExecutionResult
	if err := json.Unmarshal(stdout.Bytes(), &rpcResult); err != nil {
		t.Fatalf("decode swarm fork json: %v\n%s", err, stdout.String())
	}
	if rpcResult.SourceRunID != sourceRunID || rpcResult.BundleHash != targetProjection.BundleHash || rpcResult.ExecutedEventCount != 1 {
		t.Fatalf("run.fork result = %#v, want source=%s target=%s executed=1", rpcResult, sourceRunID, targetProjection.BundleHash)
	}

	var forkBundleHash, forkBundleSource, forkBundleFingerprint string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(bundle_hash, ''), COALESCE(bundle_source, ''), COALESCE(bundle_fingerprint, '')
		FROM runs
		WHERE run_id = $1::uuid
	`, rpcResult.ForkRunID).Scan(&forkBundleHash, &forkBundleSource, &forkBundleFingerprint); err != nil {
		t.Fatalf("load fork run bundle identity: %v", err)
	}
	if forkBundleHash != targetProjection.BundleHash || forkBundleSource != storerunlifecycle.BundleSourcePersisted || forkBundleFingerprint != "" {
		t.Fatalf("fork bundle identity = hash:%q source:%q fingerprint:%q, want persisted target %s without legacy fingerprint", forkBundleHash, forkBundleSource, forkBundleFingerprint, targetProjection.BundleHash)
	}
	var mode, selectedHash, contractsRoot string
	if err := db.QueryRowContext(ctx, `
		SELECT mode, COALESCE(bundle_hash, ''), COALESCE(contracts_root, '')
		FROM run_fork_selected_contract_bindings
		WHERE fork_run_id = $1::uuid
	`, rpcResult.ForkRunID).Scan(&mode, &selectedHash, &contractsRoot); err != nil {
		t.Fatalf("load selected-contract binding: %v", err)
	}
	if mode != store.RunForkContractSelectionModeBundleHash || selectedHash != targetProjection.BundleHash || contractsRoot != "" {
		t.Fatalf("selected-contract binding = mode:%q hash:%q root:%q, want target bundle_hash selection", mode, selectedHash, contractsRoot)
	}
	var routeMode, routeHash string
	if err := db.QueryRowContext(ctx, `
		SELECT mode, COALESCE(bundle_hash, '')
		FROM run_fork_selected_contract_route_recoveries
		WHERE fork_run_id = $1::uuid
	`, rpcResult.ForkRunID).Scan(&routeMode, &routeHash); err != nil {
		t.Fatalf("load selected-contract route recovery: %v", err)
	}
	if routeMode != store.RunForkContractSelectionModeBundleHash || routeHash != targetProjection.BundleHash {
		t.Fatalf("route recovery = mode:%q hash:%q, want target bundle_hash selection", routeMode, routeHash)
	}
	var forkEventID string
	if err := db.QueryRowContext(ctx, `
		SELECT fork_event_id::text
		FROM run_fork_selected_contract_executions
		WHERE fork_run_id = $1::uuid AND source_event_id = $2::uuid
	`, rpcResult.ForkRunID, sourceEventID).Scan(&forkEventID); err != nil {
		t.Fatalf("load selected-contract execution lineage: %v", err)
	}
	var targetSubscriberDeliveries, sourceSubscriberDeliveries int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid AND event_id = $2::uuid AND subscriber_id = 'test-node'
	`, rpcResult.ForkRunID, forkEventID).Scan(&targetSubscriberDeliveries); err != nil {
		t.Fatalf("count target subscriber deliveries: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid AND event_id = $2::uuid AND subscriber_id = 'complete-task'
	`, rpcResult.ForkRunID, forkEventID).Scan(&sourceSubscriberDeliveries); err != nil {
		t.Fatalf("count source subscriber deliveries: %v", err)
	}
	if targetSubscriberDeliveries == 0 || sourceSubscriberDeliveries != 0 {
		t.Fatalf("fork delivery recipients target=%d source=%d, want target bundle route only", targetSubscriberDeliveries, sourceSubscriberDeliveries)
	}

	if code := serve.stop(); code != 0 {
		t.Fatalf("runServeRuntime exit code = %d\noutput:\n%s", code, serve.outputString())
	}
}

func TestRunServeRuntimeEventPublishRunIDFollowUpServedPathDefaultSQLite(t *testing.T) {
	unsetStoreSelectorEnv(t)
	stubServeRuntimeWorkspaceLifecycle(t)
	sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	t.Setenv(storebackend.EnvSQLitePath, sqlitePath)
	contractsPath := writeServedEventPublishFollowUpFixture(t)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
	probe := lifecycletest.New(t, lifecycletest.WithTimeout(servedEventPublishLifecycleProbeWaitTimeout))
	oldBuildStores := buildStoresForServe
	t.Cleanup(func() {
		buildStoresForServe = oldBuildStores
	})
	var servedDB *sql.DB
	buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (storeBundle, error) {
		stores, err := oldBuildStores(ctx, selection, cfg)
		if err == nil {
			servedDB = stores.SQLDB
		}
		return stores, err
	}
	endpoint, _ := startServedEventPublishFollowUpRuntime(t, serveOptions{
		ConfigPath:              writeServeRuntimeTestConfig(t),
		ContractsPath:           contractsPath,
		PlatformSpecPath:        defaultPlatformSpecPath,
		APIListenAddr:           "127.0.0.1:0",
		MCPListenAddr:           "127.0.0.1:0",
		SelfCheck:               true,
		RequireBundleMatch:      false,
		NoRequireBundleMatch:    true,
		Verbose:                 true,
		TestLifecycleProbe:      probe,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	})

	if servedDB == nil {
		t.Fatal("served sqlite SQLDB is required for event.publish proof")
	}
	runServedEventPublishFollowUpProof(t, endpoint, servedDB, "sqlite", bundleHash, probe)
}

func TestRunServeRuntimeEventPublishRunIDFollowUpServedPathPostgres(t *testing.T) {
	_, db, _ := installServeRuntimeEmptyPostgresTestStores(t, func() serveWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	contractsPath := writeServedEventPublishFollowUpFixture(t)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
	probe := lifecycletest.New(t, lifecycletest.WithTimeout(servedEventPublishLifecycleProbeWaitTimeout))
	endpoint, _ := startServedEventPublishFollowUpRuntime(t, serveOptions{
		ConfigPath:              writeServeRuntimeTestConfig(t),
		ContractsPath:           contractsPath,
		PlatformSpecPath:        defaultPlatformSpecPath,
		StoreMode:               "postgres",
		StoreModeSet:            true,
		APIListenAddr:           "127.0.0.1:0",
		MCPListenAddr:           "127.0.0.1:0",
		SelfCheck:               true,
		RequireBundleMatch:      false,
		Verbose:                 true,
		TestLifecycleProbe:      probe,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	})

	runServedEventPublishFollowUpProof(t, endpoint, db, "postgres", bundleHash, probe)
}

func TestRunServeRuntimeEventPublishTargetRouteServedPathDefaultSQLite(t *testing.T) {
	unsetStoreSelectorEnv(t)
	stubServeRuntimeWorkspaceLifecycle(t)
	sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	t.Setenv(storebackend.EnvSQLitePath, sqlitePath)
	contractsPath := writeServedEventPublishTargetRouteFixture(t)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
	probe := lifecycletest.New(t, lifecycletest.WithTimeout(servedEventPublishLifecycleProbeWaitTimeout))
	oldBuildStores := buildStoresForServe
	t.Cleanup(func() {
		buildStoresForServe = oldBuildStores
	})
	var servedDB *sql.DB
	buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (storeBundle, error) {
		stores, err := oldBuildStores(ctx, selection, cfg)
		if err == nil {
			servedDB = stores.SQLDB
		}
		return stores, err
	}
	endpoint, _ := startServedEventPublishFollowUpRuntime(t, serveOptions{
		ConfigPath:              writeServeRuntimeTestConfig(t),
		ContractsPath:           contractsPath,
		PlatformSpecPath:        defaultPlatformSpecPath,
		APIListenAddr:           "127.0.0.1:0",
		MCPListenAddr:           "127.0.0.1:0",
		SelfCheck:               true,
		RequireBundleMatch:      false,
		NoRequireBundleMatch:    true,
		Verbose:                 true,
		TestLifecycleProbe:      probe,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	})

	if servedDB == nil {
		t.Fatal("served sqlite SQLDB is required for event.publish target-route proof")
	}
	runServedEventPublishTargetRouteProof(t, endpoint, servedDB, "sqlite", bundleHash, probe)
}

func TestRunServeRuntimeEventPublishTargetRouteServedPathPostgres(t *testing.T) {
	_, db, _ := installServeRuntimeEmptyPostgresTestStores(t, func() serveWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	contractsPath := writeServedEventPublishTargetRouteFixture(t)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
	probe := lifecycletest.New(t, lifecycletest.WithTimeout(servedEventPublishLifecycleProbeWaitTimeout))
	endpoint, _ := startServedEventPublishFollowUpRuntime(t, serveOptions{
		ConfigPath:              writeServeRuntimeTestConfig(t),
		ContractsPath:           contractsPath,
		PlatformSpecPath:        defaultPlatformSpecPath,
		StoreMode:               "postgres",
		StoreModeSet:            true,
		APIListenAddr:           "127.0.0.1:0",
		MCPListenAddr:           "127.0.0.1:0",
		SelfCheck:               true,
		RequireBundleMatch:      false,
		Verbose:                 true,
		TestLifecycleProbe:      probe,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	})

	runServedEventPublishTargetRouteProof(t, endpoint, db, "postgres", bundleHash, probe)
}

func TestRunServeRuntimeEventPublishExistingRunActiveLoadServedPathDefaultSQLite(t *testing.T) {
	unsetStoreSelectorEnv(t)
	stubServeRuntimeWorkspaceLifecycle(t)
	sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	t.Setenv(storebackend.EnvSQLitePath, sqlitePath)
	contractsPath := writeServedEventPublishActiveLoadFixture(t)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
	probe := lifecycletest.New(t, lifecycletest.WithTimeout(servedEventPublishLifecycleProbeWaitTimeout))
	agentStarted := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	oldBuildStores := buildStoresForServe
	t.Cleanup(func() {
		buildStoresForServe = oldBuildStores
	})
	var servedDB *sql.DB
	buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (storeBundle, error) {
		stores, err := oldBuildStores(ctx, selection, cfg)
		if err == nil {
			servedDB = stores.SQLDB
		}
		return stores, err
	}
	endpoint, _ := startServedEventPublishFollowUpRuntime(t, serveOptions{
		ConfigPath:              writeServeRuntimeTestConfig(t),
		ContractsPath:           contractsPath,
		PlatformSpecPath:        defaultPlatformSpecPath,
		APIListenAddr:           "127.0.0.1:0",
		MCPListenAddr:           "127.0.0.1:0",
		SelfCheck:               true,
		RequireBundleMatch:      false,
		NoRequireBundleMatch:    true,
		Verbose:                 true,
		TestLifecycleProbe:      probe,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
		TestLLMRuntime:          servedEventPublishBlockingLLMRuntime{started: agentStarted, release: release},
	})
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	if servedDB == nil {
		t.Fatal("served sqlite SQLDB is required for active-load event.publish proof")
	}
	runServedEventPublishActiveLoadProof(t, endpoint, servedDB, "sqlite", bundleHash, probe, agentStarted, release, &releaseOnce)
}

func TestRunServeRuntimeEventPublishExistingRunActiveLoadServedPathPostgres(t *testing.T) {
	_, db, _ := installServeRuntimeEmptyPostgresTestStores(t, func() serveWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	contractsPath := writeServedEventPublishActiveLoadFixture(t)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
	probe := lifecycletest.New(t, lifecycletest.WithTimeout(servedEventPublishLifecycleProbeWaitTimeout))
	agentStarted := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	endpoint, _ := startServedEventPublishFollowUpRuntime(t, serveOptions{
		ConfigPath:              writeServeRuntimeTestConfig(t),
		ContractsPath:           contractsPath,
		PlatformSpecPath:        defaultPlatformSpecPath,
		StoreMode:               "postgres",
		StoreModeSet:            true,
		APIListenAddr:           "127.0.0.1:0",
		MCPListenAddr:           "127.0.0.1:0",
		SelfCheck:               true,
		RequireBundleMatch:      false,
		Verbose:                 true,
		TestLifecycleProbe:      probe,
		TestLLMRuntime:          servedEventPublishBlockingLLMRuntime{started: agentStarted, release: release},
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	})
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	runServedEventPublishActiveLoadProof(t, endpoint, db, "postgres", bundleHash, probe, agentStarted, release, &releaseOnce)
}

func TestRunServeRuntimeEventPublishDynamicAutoEmitServedPathDefaultSQLite(t *testing.T) {
	unsetStoreSelectorEnv(t)
	stubServeRuntimeWorkspaceLifecycle(t)
	sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	t.Setenv(storebackend.EnvSQLitePath, sqlitePath)
	contractsPath := writeServedDynamicAutoEmitFixture(t)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
	blocked := make(chan servedEventPublishPreHandlerProof, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	oldBuildStores := buildStoresForServe
	t.Cleanup(func() {
		buildStoresForServe = oldBuildStores
	})
	var servedDB *sql.DB
	buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (storeBundle, error) {
		stores, err := oldBuildStores(ctx, selection, cfg)
		if err == nil {
			servedDB = stores.SQLDB
		}
		return stores, err
	}
	var (
		hookMu sync.Mutex
		hook   runtimepipeline.WorkflowNodeHandlerStartHook
	)
	endpoint, _ := startServedEventPublishFollowUpRuntime(t, serveOptions{
		ConfigPath:              writeServeRuntimeTestConfig(t),
		ContractsPath:           contractsPath,
		PlatformSpecPath:        defaultPlatformSpecPath,
		APIListenAddr:           "127.0.0.1:0",
		MCPListenAddr:           "127.0.0.1:0",
		SelfCheck:               true,
		RequireBundleMatch:      false,
		NoRequireBundleMatch:    true,
		Verbose:                 true,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
		TestWorkflowNodeHandlerStartHook: func(ctx context.Context, nodeID string, evt events.Event) error {
			if !servedEventPublishMatchesNodeEvent(nodeID, evt, "portfolio-node", "opco.spinup_requested") {
				return nil
			}
			hookMu.Lock()
			if hook == nil {
				if servedDB == nil {
					hookMu.Unlock()
					return fmt.Errorf("served sqlite SQLDB is required for dynamic event.publish proof")
				}
				hook = servedEventPublishBlockingHandlerAuthorityHook(servedDB, "sqlite", "portfolio-node", "opco.spinup_requested", blocked, release)
			}
			h := hook
			hookMu.Unlock()
			return h(ctx, nodeID, evt)
		},
	})
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	if servedDB == nil {
		t.Fatal("served sqlite SQLDB is required for dynamic event.publish proof")
	}
	runServedDynamicAutoEmitProof(t, endpoint, servedDB, "sqlite", bundleHash, blocked, release, &releaseOnce)
}

func TestRunServeRuntimeEventPublishDynamicAutoEmitServedPathPostgres(t *testing.T) {
	_, db, _ := installServeRuntimeEmptyPostgresTestStores(t, func() serveWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	contractsPath := writeServedDynamicAutoEmitFixture(t)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
	blocked := make(chan servedEventPublishPreHandlerProof, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	endpoint, _ := startServedEventPublishFollowUpRuntime(t, serveOptions{
		ConfigPath:                       writeServeRuntimeTestConfig(t),
		ContractsPath:                    contractsPath,
		PlatformSpecPath:                 defaultPlatformSpecPath,
		StoreMode:                        "postgres",
		StoreModeSet:                     true,
		APIListenAddr:                    "127.0.0.1:0",
		MCPListenAddr:                    "127.0.0.1:0",
		SelfCheck:                        true,
		RequireBundleMatch:               false,
		Verbose:                          true,
		TestWorkflowNodeHandlerStartHook: servedEventPublishBlockingHandlerAuthorityHook(db, "postgres", "portfolio-node", "opco.spinup_requested", blocked, release),
		TestOutboxSweeperConfig:          servedEventPublishProofOutboxSweeperConfig(),
	})
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	runServedDynamicAutoEmitProof(t, endpoint, db, "postgres", bundleHash, blocked, release, &releaseOnce)
}

func runServedDynamicAutoEmitProof(t *testing.T, endpoint string, db *sql.DB, backend, bundleHash string, blocked <-chan servedEventPublishPreHandlerProof, release chan struct{}, releaseOnce *sync.Once) {
	t.Helper()
	bootstrap := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
		"event_name":      "opco.bootstrap_requested",
		"bundle_hash":     bundleHash,
		"payload":         map[string]any{"owner": "operator"},
		"idempotency_key": "issue-1384-" + backend + "-bootstrap",
	})
	if !bootstrap.NewRunCreated || bootstrap.EventID == "" || bootstrap.RunID == "" {
		t.Fatalf("bootstrap event.publish result = %#v, want new run and event id", bootstrap)
	}
	runID := bootstrap.RunID
	parentEntityID := requireServedEventPublishEntityState(t, db, backend, runID, "", "waiting")

	instanceID := "11111111-1111-4111-8111-111111111111"
	start := time.Now()
	spinupEnvelope := requestServedJSONRPC(t, endpoint, "event.publish", map[string]any{
		"event_name":      "opco.spinup_requested",
		"run_id":          runID,
		"source_event_id": bootstrap.EventID,
		"payload": map[string]any{
			"instance_id": instanceID,
			"product_id":  "product-1",
		},
		"idempotency_key": "issue-1384-" + backend + "-spinup",
	})
	elapsed := time.Since(start)
	if spinupEnvelope.Error != nil {
		t.Fatalf("spinup event.publish error = %#v", spinupEnvelope.Error)
	}
	if elapsed > time.Second {
		t.Fatalf("spinup event.publish returned after %s, want durable ack before blocked create_flow_instance handler completes", elapsed)
	}
	var spinup servedEventPublishRPCResult
	if err := json.Unmarshal(spinupEnvelope.Result, &spinup); err != nil {
		t.Fatalf("decode spinup event.publish result: %v\n%s", err, string(spinupEnvelope.Result))
	}
	if spinup.RunID != runID || spinup.SourceEventID != bootstrap.EventID || spinup.NewRunCreated || spinup.EventID == "" {
		t.Fatalf("spinup event.publish result = %#v, want existing run with source lineage", spinup)
	}
	requireServedEventPublishPreHandlerProof(t, db, backend, blocked, runID, spinup.EventID, "portfolio-node")
	assertServedEventPublishDeliveriesContainStatus(t, spinup.Deliveries, "node", "portfolio-node", "pending", "in_progress")
	if got := servedEventPublishDeliveryStatusCount(t, db, backend, spinup.EventID, "node", "portfolio-node", "in_progress"); got != 1 {
		t.Fatalf("%s parent delivery in_progress count = %d, want 1\n%s", backend, got, servedEventPublishDebugSummary(t, db, backend, runID))
	}
	if got := servedEventPublishDeliveryStatusCount(t, db, backend, spinup.EventID, "node", "__runtime_replay_scope__"); got != 1 {
		t.Fatalf("%s parent replay-scope delivery count = %d, want 1\n%s", backend, got, servedEventPublishDebugSummary(t, db, backend, runID))
	}
	if got := servedEventPublishDeliveryStatusCount(t, db, backend, spinup.EventID, "", "workflow-runtime"); got != 0 {
		t.Fatalf("%s parent workflow-runtime delivery count = %d, want 0\n%s", backend, got, servedEventPublishDebugSummary(t, db, backend, runID))
	}
	requireServedReplayNoDeliveryHistoryNoMutation(t, endpoint, db, backend, spinup.EventID, "issue-1384-"+backend+"-replay-pending-parent")

	releaseOnce.Do(func() { close(release) })
	waitServedEventPublishDeliveryStatusCount(t, db, backend, spinup.EventID, "node", "portfolio-node", "delivered", 1)
	waitServedEventPublishReceiptOutcomeCount(t, db, backend, spinup.EventID, "node", "portfolio-node", "no_op", 1)
	requireServedEventReadback(t, endpoint, spinup.EventID, runID, parentEntityID, "opco.spinup_requested", "portfolio-node")
	requireServedTraceReadback(t, endpoint, runID, spinup.EventID, "opco.spinup_requested", "portfolio-node")

	autoEventName := "operating/" + instanceID + "/opco.product_initialization_requested"
	autoEventID := waitServedEventPublishEventID(t, db, backend, runID, autoEventName)
	autoEntityID := servedEventPublishEventEntityID(t, db, backend, autoEventID)
	assertServedDynamicAutoEmitPayloadProductOnly(t, db, backend, autoEventID)
	requireServedEventReadback(t, endpoint, autoEventID, runID, autoEntityID, autoEventName, "lifecycle-orchestrator")
	requireServedTraceReadback(t, endpoint, runID, autoEventID, autoEventName, "lifecycle-orchestrator")
	waitServedEventPublishReceiptOutcomeCount(t, db, backend, autoEventID, "node", "lifecycle-orchestrator", "no_op", 1)
	if got := servedEventPublishDeliveryStatusCount(t, db, backend, autoEventID, "node", "__runtime_replay_scope__"); got != 1 {
		t.Fatalf("%s child replay-scope delivery count = %d, want 1\n%s", backend, got, servedEventPublishDebugSummary(t, db, backend, runID))
	}
	if got := servedEventPublishDeliveryStatusCount(t, db, backend, autoEventID, "", "workflow-runtime"); got != 0 {
		t.Fatalf("%s child workflow-runtime delivery count = %d, want 0\n%s", backend, got, servedEventPublishDebugSummary(t, db, backend, runID))
	}
	if got := servedEventPublishReceiptCountBySubscribers(t, db, backend, autoEventID, "workflow-runtime", "__runtime_replay_scope__"); got != 0 {
		t.Fatalf("%s child runtime/replay receipt count = %d, want 0\n%s", backend, got, servedEventPublishDebugSummary(t, db, backend, runID))
	}

	componentEventID := waitServedEventPublishEventID(t, db, backend, runID, "operating/component_scaffold.spawn_requested")
	assertServedDynamicAutoEmitPayloadProductOnly(t, db, backend, componentEventID)
	componentEntityID := servedEventPublishEventEntityID(t, db, backend, componentEventID)
	requireServedEventReadback(t, endpoint, componentEventID, runID, componentEntityID, "operating/component_scaffold.spawn_requested", "component-scaffold")
	requireServedTraceReadback(t, endpoint, runID, componentEventID, "operating/component_scaffold.spawn_requested", "component-scaffold")
	requireServedRunStatus(t, endpoint, runID, "completed")
	requireServedRunDiagnoseOperationalState(t, endpoint, runID, "completed")
	requireServedStatusCLIReadback(t, endpoint, runID, "status=completed")
	requireServedReplayNoDeliveryHistoryNoMutation(t, endpoint, db, backend, autoEventID, "issue-1384-"+backend+"-replay-child-node-only")
}

func writeServedEventPublishFollowUpFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: served-event-publish-followup
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `
initial_state: new
terminal_states: [done]
states: [new, waiting, done]
pins:
  inputs:
    events: [thing.created]
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "entities.yaml"), `
widget:
  amount:
    type: integer
    initial: 0
  who: text
  note: text
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), `
thing.created:
  swarm:
    source: external
  amount: integer
  who: text

thing.reviewed:
  swarm:
    source: external
  note: text

thing.agent_hold:
  swarm:
    source: external
  note: text

thing.unhandled:
  swarm:
    source: external
  note: text
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
entity-writer:
  id: entity-writer
  execution_type: system_node
  subscribes_to: [thing.created, thing.reviewed]
  event_handlers:
    thing.created:
      data_accumulation:
        source_event: thing.created
        writes:
          - source_field: amount
            target_field: amount
          - source_field: who
            target_field: who
      advances_to: waiting
    thing.reviewed:
      data_accumulation:
        source_event: thing.reviewed
        writes:
          - source_field: note
            target_field: note
      advances_to: done
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), `{}`)
	return root
}

func writeServedEventPublishTargetRouteFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: served-event-publish-target-route
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: operating
    flow: operating
    mode: template
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: served-event-publish-target-route
initial_state: new
terminal_states: [done]
states: [new, waiting, done]
pins:
  inputs:
    events: [opco.bootstrap_requested]
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "entities.yaml"), `
portfolio:
  owner: text
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), `
opco.bootstrap_requested:
  swarm:
    source: external
  owner: text

opco.spinup_requested:
  swarm:
    source: external
  instance_id: string
  product_id: string
  required:
    - instance_id
    - product_id
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
portfolio-bootstrap:
  id: portfolio-bootstrap
  execution_type: system_node
  subscribes_to: [opco.bootstrap_requested]
  event_handlers:
    opco.bootstrap_requested:
      data_accumulation:
        source_event: opco.bootstrap_requested
        writes:
          - source_field: owner
            target_field: owner
      advances_to: waiting
portfolio-node:
  id: portfolio-node
  execution_type: system_node
  subscribes_to: [opco.spinup_requested]
  event_handlers:
    opco.spinup_requested:
      action: create_flow_instance
      template: operating
      instance_id_from: payload.instance_id
      config_from:
        product_id: payload.product_id
      advances_to: done
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "operating", "schema.yaml"), `
name: operating
mode: template
instance:
  by: product_id
  on_missing: create
  on_conflict: reject
initial_state: initializing
terminal_states: [ready]
states: [initializing, waiting, ready]
pins:
  inputs:
    events: [opco.product_initialization_requested, opco.product_review_requested]
auto_emit_on_create:
  event: opco.product_initialization_requested
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "operating", "entities.yaml"), `
product:
  product_id: text
  note: text
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "operating", "events.yaml"), `
opco.product_initialization_requested:
  swarm:
    source: external
  product_id: string
opco.product_review_requested:
  swarm:
    source: external
  note: string
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "operating", "nodes.yaml"), `
lifecycle-orchestrator:
  id: lifecycle-orchestrator
  execution_type: system_node
  subscribes_to: [opco.product_initialization_requested, opco.product_review_requested]
  event_handlers:
    opco.product_initialization_requested:
      data_accumulation:
        source_event: opco.product_initialization_requested
        writes:
          - source_field: product_id
            target_field: product_id
      advances_to: waiting
    opco.product_review_requested:
      data_accumulation:
        source_event: opco.product_review_requested
        writes:
          - source_field: note
            target_field: note
      advances_to: ready
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "operating", "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "operating", "tools.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "operating", "agents.yaml"), `{}`)
	return root
}

func writeServedEventPublishActiveLoadFixture(t *testing.T) string {
	t.Helper()
	root := writeServedEventPublishFollowUpFixture(t)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), `
load-agent:
  id: load-agent
  role: load_agent
  prompt_ref: load-agent
  model: regular
  mode: task
  subscriptions:
    - thing.agent_hold
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "prompts", "load-agent.md"), `
Handle the active-load event and wait for test release.
`)
	return root
}

func writeServedDynamicAutoEmitFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: served-dynamic-auto-emit
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: operating
    flow: operating
    mode: template
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: served-dynamic-auto-emit
initial_state: new
terminal_states: [done]
states: [new, waiting, done]
pins:
  inputs:
    events: [opco.bootstrap_requested]
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "entities.yaml"), `
portfolio:
  owner: text
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), `
opco.bootstrap_requested:
  swarm:
    source: external
  owner: text

opco.spinup_requested:
  swarm:
    source: external
  instance_id: string
  product_id: string
  required:
    - instance_id
    - product_id
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
portfolio-bootstrap:
  id: portfolio-bootstrap
  execution_type: system_node
  subscribes_to: [opco.bootstrap_requested]
  event_handlers:
    opco.bootstrap_requested:
      data_accumulation:
        source_event: opco.bootstrap_requested
        writes:
          - source_field: owner
            target_field: owner
      advances_to: waiting
portfolio-node:
  id: portfolio-node
  execution_type: system_node
  subscribes_to: [opco.spinup_requested]
  event_handlers:
    opco.spinup_requested:
      action: create_flow_instance
      template: operating
      instance_id_from: payload.instance_id
      config_from:
        product_id: payload.product_id
      advances_to: done
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "operating", "schema.yaml"), `
name: operating
mode: template
instance:
  by: product_id
  on_missing: create
  on_conflict: reject
initial_state: initializing
terminal_states: [ready]
states: [initializing, spawning, ready]
auto_emit_on_create:
  event: opco.product_initialization_requested
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "operating", "entities.yaml"), `
product:
  product_id: text
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "operating", "events.yaml"), `
opco.product_initialization_requested:
  product_id: string
component_scaffold.spawn_requested:
  product_id: string
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "operating", "nodes.yaml"), `
lifecycle-orchestrator:
  id: lifecycle-orchestrator
  execution_type: system_node
  subscribes_to: [opco.product_initialization_requested]
  produces: [component_scaffold.spawn_requested]
  event_handlers:
    opco.product_initialization_requested:
      data_accumulation:
        source_event: opco.product_initialization_requested
        writes:
          - source_field: product_id
            target_field: product_id
      emit:
        event: component_scaffold.spawn_requested
        fields:
          product_id: payload.product_id
      advances_to: spawning
component-scaffold:
  id: component-scaffold
  execution_type: system_node
  subscribes_to: [component_scaffold.spawn_requested]
  event_handlers:
    component_scaffold.spawn_requested:
      advances_to: ready
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "operating", "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "operating", "tools.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "operating", "agents.yaml"), `{}`)
	return root
}

func servedEventPublishFixtureBundleHash(t *testing.T, contractsPath string) string {
	t.Helper()
	bundle := loadWorkflowValidationBundleAt(t, contractsPath)
	bundleHash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		t.Fatalf("BundleHash(%s): %v", contractsPath, err)
	}
	return bundleHash
}

func servedEventPublishProofOutboxSweeperConfig() runtimebus.OutboxSweeperConfig {
	cfg := runtimebus.DefaultOutboxSweeperConfig()
	cfg.Interval = 25 * time.Millisecond
	return cfg
}

func startServedEventPublishFollowUpRuntime(t *testing.T, opts serveOptions) (string, *runtimepkg.Runtime) {
	t.Helper()
	serveCtx, cancelServe := context.WithCancel(context.Background())
	var out lockedBuffer
	done := make(chan int, 1)
	runtimeReady := make(chan *runtimepkg.Runtime, 1)
	priorRuntimeReadyHook := opts.TestRuntimeReadyHook
	opts.TestRuntimeReadyHook = func(rt *runtimepkg.Runtime) {
		if priorRuntimeReadyHook != nil {
			priorRuntimeReadyHook(rt)
		}
		select {
		case runtimeReady <- rt:
		default:
		}
	}
	opts.Output = &out
	go func() {
		done <- runServeRuntime(serveCtx, repoRoot(), opts)
	}()
	stopped := false
	waitForServeReadyLine(t, &out, done)
	var rt *runtimepkg.Runtime
	select {
	case rt = <-runtimeReady:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for serve runtime test hook\noutput:\n%s", out.String())
	}
	t.Cleanup(func() {
		if stopped {
			return
		}
		cancelServe()
		select {
		case code := <-done:
			if code != 0 {
				t.Errorf("runServeRuntime exit code = %d\noutput:\n%s", code, out.String())
			}
		case <-time.After(5 * time.Second):
			t.Errorf("timed out stopping runServeRuntime\noutput:\n%s", out.String())
		}
		stopped = true
	})
	return "http://" + serveRuntimeAPIListenerFromOutput(t, out.String()) + "/v1/rpc", rt
}

type servedEventPublishPreHandlerProof struct {
	RunID   string
	EventID string
	NodeID  string
	Count   int
	Err     error
}

func servedEventPublishBlockingHandlerAuthorityHook(
	db *sql.DB,
	backend string,
	wantNodeID string,
	wantEventSuffix string,
	proofs chan<- servedEventPublishPreHandlerProof,
	release <-chan struct{},
) runtimepipeline.WorkflowNodeHandlerStartHook {
	var once sync.Once
	wantNodeID = strings.TrimSpace(wantNodeID)
	wantEventSuffix = strings.Trim(strings.TrimSpace(wantEventSuffix), "/")
	return func(ctx context.Context, nodeID string, evt events.Event) error {
		if !servedEventPublishMatchesNodeEvent(nodeID, evt, wantNodeID, wantEventSuffix) {
			return nil
		}
		once.Do(func() {
			runID := strings.TrimSpace(evt.RunID())
			if runID == "" {
				runID = runtimecorrelation.RunIDFromContext(ctx)
			}
			eventID := strings.TrimSpace(evt.ID())
			count, err := servedEventPublishNodeDeliveryCountValue(ctx, db, backend, runID, eventID, wantNodeID)
			proof := servedEventPublishPreHandlerProof{
				RunID:   runID,
				EventID: eventID,
				NodeID:  wantNodeID,
				Count:   count,
				Err:     err,
			}
			select {
			case proofs <- proof:
			default:
			}
			<-release
		})
		return nil
	}
}

func servedEventPublishMatchesNodeEvent(nodeID string, evt events.Event, wantNodeID, wantEventSuffix string) bool {
	wantNodeID = strings.TrimSpace(wantNodeID)
	wantEventSuffix = strings.Trim(strings.TrimSpace(wantEventSuffix), "/")
	if strings.TrimSpace(nodeID) != wantNodeID {
		return false
	}
	eventType := strings.Trim(strings.TrimSpace(string(evt.Type())), "/")
	return eventType == wantEventSuffix || strings.HasSuffix(eventType, "/"+wantEventSuffix)
}

func requireServedEventPublishPreHandlerProof(t *testing.T, db *sql.DB, backend string, proofs <-chan servedEventPublishPreHandlerProof, runID, eventID, nodeID string) {
	t.Helper()
	select {
	case proof := <-proofs:
		if proof.Err != nil {
			t.Fatalf("%s pre-handler node delivery authority query failed: %v\n%s", backend, proof.Err, servedEventPublishDebugSummary(t, db, backend, runID))
		}
		if proof.RunID != runID || proof.EventID != eventID || proof.NodeID != nodeID {
			t.Fatalf("%s pre-handler node delivery authority proof = %#v, want run=%s event=%s node=%s\n%s", backend, proof, runID, eventID, nodeID, servedEventPublishDebugSummary(t, db, backend, runID))
		}
		if proof.Count == 0 {
			t.Fatalf("%s pre-handler root-input node deliveries = %d, want committed node/%s authority before handler execution\n%s", backend, proof.Count, nodeID, servedEventPublishDebugSummary(t, db, backend, runID))
		}
	case <-time.After(45 * time.Second):
		t.Fatalf("%s pre-handler root-input node delivery authority hook did not run for run=%s event=%s node=%s\n%s", backend, runID, eventID, nodeID, servedEventPublishDebugSummary(t, db, backend, runID))
	}
}

type servedEventPublishRPCResult struct {
	EventID       string `json:"event_id"`
	RunID         string `json:"run_id"`
	SourceEventID string `json:"source_event_id,omitempty"`
	NewRunCreated bool   `json:"new_run_created"`
	Deliveries    []struct {
		SubscriberType string `json:"subscriber_type"`
		SubscriberID   string `json:"subscriber_id"`
		Status         string `json:"status"`
	} `json:"deliveries"`
}

func requireServedEventPublishRPCResult(t *testing.T, endpoint string, params map[string]any) servedEventPublishRPCResult {
	t.Helper()
	var result servedEventPublishRPCResult
	requireServedJSONRPCResult(t, endpoint, "event.publish", params, &result)
	return result
}

func assertServedEventPublishDeliveriesContain(t *testing.T, deliveries []struct {
	SubscriberType string `json:"subscriber_type"`
	SubscriberID   string `json:"subscriber_id"`
	Status         string `json:"status"`
}, subscriberType, subscriberID, status string) {
	t.Helper()
	assertServedEventPublishDeliveriesContainStatus(t, deliveries, subscriberType, subscriberID, status)
}

func assertServedEventPublishDeliveriesContainStatus(t *testing.T, deliveries []struct {
	SubscriberType string `json:"subscriber_type"`
	SubscriberID   string `json:"subscriber_id"`
	Status         string `json:"status"`
}, subscriberType, subscriberID string, statuses ...string) {
	t.Helper()
	allowed := map[string]bool{}
	for _, status := range statuses {
		allowed[status] = true
	}
	for _, delivery := range deliveries {
		if delivery.SubscriberType == subscriberType && delivery.SubscriberID == subscriberID && allowed[delivery.Status] {
			return
		}
	}
	t.Fatalf("event.publish deliveries = %#v, want %s/%s status in %v", deliveries, subscriberType, subscriberID, statuses)
}

func waitForServedEventPublishNodeDeliveryLifecycle(t *testing.T, db *sql.DB, backend, runID, eventID string, probe *lifecycletest.Probe) {
	t.Helper()
	waitForServedEventPublishNodeDeliveryLifecycleForNode(t, db, backend, runID, eventID, "entity-writer", probe)
}

func waitForServedEventPublishNodeDeliveryLifecycleForNode(t *testing.T, db *sql.DB, backend, runID, eventID, nodeID string, probe *lifecycletest.Probe) {
	t.Helper()
	if probe == nil {
		t.Fatalf("%s lifecycle probe is required for event %s", backend, eventID)
	}
	probe.RequireNodePending(eventID, nodeID)
	probe.Expect(eventID).
		PostCommitDispatchStarted().
		NodeInProgress(nodeID).
		HandlerStarted(nodeID).
		HandlerCompleted(nodeID).
		NodeDelivered(nodeID).
		PostCommitDispatchCompleted().
		Within(servedEventPublishLifecycleProbeWaitTimeout)
	if count := servedEventPublishNodeDeliveryCount(t, db, backend, runID, eventID, nodeID); count != 1 {
		t.Fatalf("%s node/%s delivery count for event %s = %d, want 1\n%s", backend, nodeID, eventID, count, servedEventPublishDebugSummary(t, db, backend, runID))
	}
}

const (
	servedEventPublishLifecycleProbeWaitTimeout = 45 * time.Second
)

func runServedEventPublishFollowUpProof(t *testing.T, endpoint string, db *sql.DB, backend, bundleHash string, probe *lifecycletest.Probe) {
	t.Helper()
	initialStdout, initialStderr, code := runServedCLICommand(t, endpoint, []string{
		"event", "publish", "thing.created",
		"--bundle-hash", bundleHash,
		"--payload-json", `{"amount":7,"who":"operator"}`,
		"--idempotency-key", "issue-1255-" + backend + "-initial",
	})
	if code != 0 {
		t.Fatalf("initial event publish code=%d stderr=%s stdout=%s", code, initialStderr, initialStdout)
	}
	initial := parseServedEventPublishOutput(t, initialStdout)
	runID := initial["run_id"]
	initialEventID := initial["event_id"]
	if initial["new_run_created"] != "true" || initial["deliveries"] == "0" || runID == "" || initialEventID == "" {
		t.Fatalf("initial event publish fields = %#v, want new run with delivery", initial)
	}
	if got := servedEventPublishRowCount(t, db, backend, "runs", runID, ""); got != 1 {
		t.Fatalf("%s runs for initial run = %d, want 1", backend, got)
	}
	if got := servedEventPublishNodeDeliveryCount(t, db, backend, runID, initialEventID, "entity-writer"); got == 0 {
		t.Fatalf("%s initial root-input node deliveries = %d, want persisted node/entity-writer authority", backend, got)
	}
	waitForServedEventPublishNodeDeliveryLifecycle(t, db, backend, runID, initialEventID, probe)
	entityID := requireServedEventPublishEntityState(t, db, backend, runID, "", "waiting")
	requireServedEventReadback(t, endpoint, initialEventID, runID, entityID, "thing.created", "entity-writer")
	requireServedEntityReadback(t, endpoint, runID, entityID, "waiting")

	followUpStdout, followUpStderr, code := runServedCLICommand(t, endpoint, []string{
		"event", "publish", "thing.reviewed",
		"--run-id", runID,
		"--payload-json", `{"note":"approved"}`,
		"--idempotency-key", "issue-1255-" + backend + "-follow-up",
	})
	if code != 0 {
		t.Fatalf("follow-up event publish code=%d stderr=%s stdout=%s", code, followUpStderr, followUpStdout)
	}
	followUp := parseServedEventPublishOutput(t, followUpStdout)
	followUpEventID := followUp["event_id"]
	if followUp["run_id"] != runID || followUp["new_run_created"] != "false" || followUp["deliveries"] == "0" || followUpEventID == "" {
		t.Fatalf("follow-up event publish fields = %#v, want selected existing run with delivery", followUp)
	}
	if got := servedEventPublishRowCount(t, db, backend, "runs", runID, ""); got != 1 {
		t.Fatalf("%s runs for selected run after follow-up = %d, want 1", backend, got)
	}
	if got := servedEventPublishScalarCount(t, db, backend, "application_events_by_run", runID, ""); got != 2 {
		t.Fatalf("%s application events for selected run = %d, want 2\n%s", backend, got, servedEventPublishDebugSummary(t, db, backend, runID))
	}
	if got := servedEventPublishScalarCount(t, db, backend, "event_deliveries", runID, followUpEventID); got == 0 {
		t.Fatalf("%s follow-up deliveries = %d, want non-empty persisted evidence", backend, got)
	}
	waitForServedEventPublishNodeDeliveryLifecycle(t, db, backend, runID, followUpEventID, probe)
	requireServedEventPublishEntityState(t, db, backend, runID, entityID, "done")
	requireServedEntityReadback(t, endpoint, runID, entityID, "done")
	requireServedRunStatus(t, endpoint, runID, "completed")
	requireServedEventReadback(t, endpoint, followUpEventID, runID, entityID, "thing.reviewed", "entity-writer")
	requireServedTraceReadback(t, endpoint, runID, followUpEventID, "thing.reviewed", "entity-writer")

	traceStdout, traceStderr, traceCode := runServedCLICommand(t, endpoint, []string{
		"trace", runID,
		"--event-name", "thing.reviewed",
		"--entity-id", entityID,
		"--limit", "10",
	})
	if traceCode != 0 {
		t.Fatalf("trace readback code=%d stderr=%s stdout=%s", traceCode, traceStderr, traceStdout)
	}
	for _, want := range []string{"thing.reviewed", followUpEventID, "delivered", "node/entity-writer"} {
		if !strings.Contains(traceStdout, want) {
			t.Fatalf("trace readback missing %q:\n%s", want, traceStdout)
		}
	}
	requireServedStatusCLIReadback(t, endpoint, runID, "status=completed")
	entityListStdout, entityListStderr, entityListCode := runServedCLICommand(t, endpoint, []string{"entities", "list", "--run-id", runID, "--limit", "10"})
	if entityListCode != 0 {
		t.Fatalf("entities list readback code=%d stderr=%s stdout=%s", entityListCode, entityListStderr, entityListStdout)
	}
	for _, want := range []string{entityID, runID, "done"} {
		if !strings.Contains(entityListStdout, want) {
			t.Fatalf("entities list readback missing %q:\n%s", want, entityListStdout)
		}
	}
	entityViewStdout, entityViewStderr, entityViewCode := runServedCLICommand(t, endpoint, []string{"entity", "view", entityID, "--run-id", runID})
	if entityViewCode != 0 {
		t.Fatalf("entity view readback code=%d stderr=%s stdout=%s", entityViewCode, entityViewStderr, entityViewStdout)
	}
	for _, want := range []string{entityID, "state=done", `"note":"approved"`} {
		if !strings.Contains(entityViewStdout, want) {
			t.Fatalf("entity view readback missing %q:\n%s", want, entityViewStdout)
		}
	}

	unhandledIdempotencyKey := "issue-1255-" + backend + "-unhandled"
	errResp := requireServedJSONRPCError(t, endpoint, "event.publish", map[string]any{
		"event_name":      "thing.unhandled",
		"run_id":          runID,
		"payload":         map[string]any{"note": "lost"},
		"idempotency_key": unhandledIdempotencyKey,
	})
	if errResp.Data["code"] != apiv1.RunAlreadyTerminalCode {
		t.Fatalf("unhandled follow-up error data = %#v, want %s", errResp.Data, apiv1.RunAlreadyTerminalCode)
	}
	details, ok := errResp.Data["details"].(map[string]any)
	if !ok || details["current_status"] != "completed" {
		t.Fatalf("unhandled follow-up details = %#v", errResp.Data["details"])
	}
	if got := servedEventPublishEventCountByIdempotencyKey(t, db, backend, unhandledIdempotencyKey); got != 0 {
		t.Fatalf("%s event rows for rejected follow-up idempotency key = %d, want 0", backend, got)
	}
	if got := servedEventPublishAPIIdempotencyCount(t, db, backend, "event.publish", unhandledIdempotencyKey); got != 0 {
		t.Fatalf("%s idempotency rows for rejected follow-up = %d, want 0", backend, got)
	}
}

func runServedEventPublishTargetRouteProof(t *testing.T, endpoint string, db *sql.DB, backend, bundleHash string, probe *lifecycletest.Probe) {
	t.Helper()
	bootstrapStdout, bootstrapStderr, code := runServedCLICommand(t, endpoint, []string{
		"event", "publish", "opco.bootstrap_requested",
		"--bundle-hash", bundleHash,
		"--payload-json", `{"owner":"operator"}`,
		"--idempotency-key", "issue-1438-" + backend + "-bootstrap",
	})
	if code != 0 {
		t.Fatalf("bootstrap target-route event publish code=%d stderr=%s stdout=%s", code, bootstrapStderr, bootstrapStdout)
	}
	bootstrap := parseServedEventPublishOutput(t, bootstrapStdout)
	runID := bootstrap["run_id"]
	bootstrapEventID := bootstrap["event_id"]
	if bootstrap["new_run_created"] != "true" || bootstrap["deliveries"] == "0" || runID == "" || bootstrapEventID == "" {
		t.Fatalf("bootstrap target-route event publish fields = %#v, want new run with delivery", bootstrap)
	}
	requireServedEventPublishEntityState(t, db, backend, runID, "", "waiting")

	instanceID := "11111111-1111-4111-8111-111111111111"
	spinupStdout, spinupStderr, code := runServedCLICommand(t, endpoint, []string{
		"event", "publish", "opco.spinup_requested",
		"--run-id", runID,
		"--source-event-id", bootstrapEventID,
		"--payload-json", fmt.Sprintf(`{"instance_id":%q,"product_id":"product-1"}`, instanceID),
		"--idempotency-key", "issue-1438-" + backend + "-spinup",
	})
	if code != 0 {
		t.Fatalf("spinup target-route event publish code=%d stderr=%s stdout=%s", code, spinupStderr, spinupStdout)
	}
	spinup := parseServedEventPublishOutput(t, spinupStdout)
	spinupEventID := spinup["event_id"]
	if spinup["run_id"] != runID || spinup["new_run_created"] != "false" || spinup["deliveries"] == "0" || spinupEventID == "" {
		t.Fatalf("spinup target-route event publish fields = %#v, want selected existing run with delivery", spinup)
	}
	waitServedEventPublishDeliveryStatusCount(t, db, backend, spinupEventID, "node", "portfolio-node", "delivered", 1)

	autoEventName := "operating/" + instanceID + "/opco.product_initialization_requested"
	autoEventID := waitServedEventPublishEventID(t, db, backend, runID, autoEventName)
	waitServedEventPublishDeliveryStatusCount(t, db, backend, autoEventID, "node", "lifecycle-orchestrator", "delivered", 1)
	entityID := servedEventPublishEventEntityID(t, db, backend, autoEventID)
	if entityID == "" {
		t.Fatalf("%s child auto event %s has no entity_id\n%s", backend, autoEventID, servedEventPublishDebugSummary(t, db, backend, runID))
	}
	requireServedEventPublishEntityState(t, db, backend, runID, entityID, "waiting")
	targetFlowInstance := requireServedEventPublishEntityFlowInstance(t, db, backend, runID, entityID)

	targetStdout, targetStderr, code := runServedCLICommand(t, endpoint, []string{
		"event", "publish", "operating/opco.product_review_requested",
		"--run-id", runID,
		"--source-event-id", autoEventID,
		"--target-flow-instance", targetFlowInstance,
		"--target-entity-id", entityID,
		"--payload-json", `{"note":"approved-target"}`,
		"--idempotency-key", "issue-1438-" + backend + "-target",
	})
	if code != 0 {
		t.Fatalf("target-route event publish code=%d stderr=%s stdout=%s", code, targetStderr, targetStdout)
	}
	targeted := parseServedEventPublishOutput(t, targetStdout)
	targetEventID := targeted["event_id"]
	if targeted["run_id"] != runID || targeted["new_run_created"] != "false" || targeted["deliveries"] == "0" || targetEventID == "" {
		t.Fatalf("target-route event publish fields = %#v, want selected existing run with delivery", targeted)
	}
	requireServedEventPublishTargetRouteRow(t, db, backend, targetEventID, "operating/opco.product_review_requested", targetFlowInstance, entityID)
	requireServedEventPublishDeliveryTargetRoute(t, db, backend, targetEventID, "node", "lifecycle-orchestrator", targetFlowInstance, entityID)
	waitServedEventPublishDeliveryStatusCount(t, db, backend, targetEventID, "node", "lifecycle-orchestrator", "delivered", 1)
	requireServedEventPublishEntityState(t, db, backend, runID, entityID, "ready")
	requireServedEntityReadback(t, endpoint, runID, entityID, "ready")
	requireServedRunStatus(t, endpoint, runID, "completed")
	requireServedEventReadback(t, endpoint, targetEventID, runID, entityID, "operating/opco.product_review_requested", "lifecycle-orchestrator")
	requireServedTraceReadback(t, endpoint, runID, targetEventID, "operating/opco.product_review_requested", "lifecycle-orchestrator")

	if got := servedEventPublishAPIIdempotencyCount(t, db, backend, "event.publish", "issue-1438-"+backend+"-target"); got != 1 {
		t.Fatalf("%s target-route idempotency rows = %d, want 1", backend, got)
	}
}

func runServedEventPublishActiveLoadProof(
	t *testing.T,
	endpoint string,
	db *sql.DB,
	backend string,
	bundleHash string,
	probe *lifecycletest.Probe,
	agentStarted <-chan struct{},
	release chan struct{},
	releaseOnce *sync.Once,
) {
	t.Helper()
	initial := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
		"event_name":      "thing.created",
		"bundle_hash":     bundleHash,
		"payload":         map[string]any{"amount": 7, "who": "operator"},
		"idempotency_key": "issue-1434-" + backend + "-initial",
	})
	runID := initial.RunID
	initialEventID := initial.EventID
	if !initial.NewRunCreated || runID == "" || initialEventID == "" {
		t.Fatalf("%s initial event.publish result = %#v, want new run", backend, initial)
	}
	waitForServedEventPublishNodeDeliveryLifecycle(t, db, backend, runID, initialEventID, probe)
	entityID := requireServedEventPublishEntityState(t, db, backend, runID, "", "waiting")

	holdStart := time.Now()
	holdEnvelope := requestServedJSONRPC(t, endpoint, "event.publish", map[string]any{
		"event_name":      "thing.agent_hold",
		"run_id":          runID,
		"source_event_id": initialEventID,
		"payload":         map[string]any{"note": "hold active agent delivery"},
		"idempotency_key": "issue-1434-" + backend + "-agent-hold",
	})
	holdElapsed := time.Since(holdStart)
	if holdEnvelope.Error != nil {
		t.Fatalf("%s agent-hold event.publish error = %#v", backend, holdEnvelope.Error)
	}
	if holdElapsed > time.Second {
		t.Fatalf("%s agent-hold event.publish returned after %s, want durable ACK before held agent completes", backend, holdElapsed)
	}
	var hold servedEventPublishRPCResult
	if err := json.Unmarshal(holdEnvelope.Result, &hold); err != nil {
		t.Fatalf("%s decode agent-hold event.publish result: %v\n%s", backend, err, string(holdEnvelope.Result))
	}
	if hold.RunID != runID || hold.SourceEventID != initialEventID || hold.NewRunCreated || hold.EventID == "" {
		t.Fatalf("%s agent-hold event.publish result = %#v, want existing run with source lineage", backend, hold)
	}
	if got := servedEventPublishScalarCount(t, db, backend, "event_deliveries", runID, hold.EventID); got == 0 {
		t.Fatalf("%s agent-hold deliveries = %d, want persisted delivery authority\n%s", backend, got, servedEventPublishDebugSummary(t, db, backend, runID))
	}
	requireServedEventPublishCommittedReplayScope(t, db, backend, runID, hold.EventID, "subscribed")
	assertServedEventPublishDeliveriesContainStatus(t, hold.Deliveries, "agent", "load-agent", "pending", "in_progress")
	select {
	case <-agentStarted:
	case <-time.After(servedEventPublishLifecycleProbeWaitTimeout):
		t.Fatalf("%s timed out waiting for active-load agent to start\n%s", backend, servedEventPublishDebugSummary(t, db, backend, runID))
	}
	if got := servedEventPublishDeliveryStatusCount(t, db, backend, hold.EventID, "agent", "load-agent", "in_progress"); got != 1 {
		t.Fatalf("%s agent-hold delivery in_progress count = %d, want active agent load before follow-up\n%s", backend, got, servedEventPublishDebugSummary(t, db, backend, runID))
	}

	unhandledKey := "issue-1434-" + backend + "-unhandled-active"
	unhandledStart := time.Now()
	unhandledEnvelope := requestServedJSONRPC(t, endpoint, "event.publish", map[string]any{
		"event_name":      "thing.unhandled",
		"run_id":          runID,
		"source_event_id": hold.EventID,
		"payload":         map[string]any{"note": "unhandled under active load"},
		"idempotency_key": unhandledKey,
	})
	unhandledElapsed := time.Since(unhandledStart)
	if unhandledEnvelope.Error == nil {
		t.Fatalf("%s unhandled event.publish error = nil, result=%s", backend, string(unhandledEnvelope.Result))
	}
	if unhandledElapsed > time.Second {
		t.Fatalf("%s unhandled event.publish returned after %s, want typed fail-closed error before client timeout", backend, unhandledElapsed)
	}
	if unhandledEnvelope.Error.Data["code"] != apiv1.EventNotDeclaredCode {
		t.Fatalf("%s unhandled event.publish error data = %#v, want %s", backend, unhandledEnvelope.Error.Data, apiv1.EventNotDeclaredCode)
	}
	if got := servedEventPublishEventCountByIdempotencyKey(t, db, backend, unhandledKey); got != 0 {
		t.Fatalf("%s event rows for unhandled active-load idempotency key = %d, want 0", backend, got)
	}
	if got := servedEventPublishAPIIdempotencyCount(t, db, backend, "event.publish", unhandledKey); got != 0 {
		t.Fatalf("%s idempotency rows for unhandled active-load publish = %d, want 0", backend, got)
	}
	if got := servedEventPublishDeliveryStatusCount(t, db, backend, hold.EventID, "agent", "load-agent", "in_progress"); got != 1 {
		t.Fatalf("%s agent-hold delivery in_progress after fail-closed publish = %d, want still active\n%s", backend, got, servedEventPublishDebugSummary(t, db, backend, runID))
	}

	followStart := time.Now()
	followEnvelope := requestServedJSONRPC(t, endpoint, "event.publish", map[string]any{
		"event_name":      "thing.reviewed",
		"run_id":          runID,
		"source_event_id": hold.EventID,
		"payload":         map[string]any{"note": "approved under active load"},
		"idempotency_key": "issue-1434-" + backend + "-follow-up",
	})
	followElapsed := time.Since(followStart)
	if followEnvelope.Error != nil {
		t.Fatalf("%s follow-up event.publish error = %#v", backend, followEnvelope.Error)
	}
	if followElapsed > time.Second {
		t.Fatalf("%s follow-up event.publish returned after %s, want durable ACK while unrelated delivery remains active", backend, followElapsed)
	}
	var followUp servedEventPublishRPCResult
	if err := json.Unmarshal(followEnvelope.Result, &followUp); err != nil {
		t.Fatalf("%s decode follow-up event.publish result: %v\n%s", backend, err, string(followEnvelope.Result))
	}
	if followUp.RunID != runID || followUp.SourceEventID != hold.EventID || followUp.NewRunCreated || followUp.EventID == "" {
		t.Fatalf("%s follow-up event.publish result = %#v, want existing run with hold event source lineage", backend, followUp)
	}
	if got := servedEventPublishScalarCount(t, db, backend, "event_deliveries", runID, followUp.EventID); got == 0 {
		t.Fatalf("%s follow-up deliveries = %d, want persisted delivery authority\n%s", backend, got, servedEventPublishDebugSummary(t, db, backend, runID))
	}
	requireServedEventPublishCommittedReplayScope(t, db, backend, runID, followUp.EventID, "subscribed")
	assertServedEventPublishDeliveriesContainStatus(t, followUp.Deliveries, "node", "entity-writer", "pending", "in_progress", "delivered")
	if got := servedEventPublishDeliveryStatusCount(t, db, backend, hold.EventID, "agent", "load-agent", "in_progress"); got != 1 {
		t.Fatalf("%s agent-hold delivery in_progress after follow-up ACK = %d, want ACK before unrelated agent delivery release\n%s", backend, got, servedEventPublishDebugSummary(t, db, backend, runID))
	}
	requireServedEventReadback(t, endpoint, followUp.EventID, runID, entityID, "thing.reviewed", "entity-writer")

	releaseOnce.Do(func() { close(release) })
	waitServedEventPublishDeliveryStatusCount(t, db, backend, hold.EventID, "agent", "load-agent", "delivered", 1)
	waitForServedEventPublishNodeDeliveryLifecycle(t, db, backend, runID, followUp.EventID, probe)
	requireServedEventPublishEntityState(t, db, backend, runID, entityID, "done")
	requireServedRunStatus(t, endpoint, runID, "completed")
	requireServedTraceReadback(t, endpoint, runID, followUp.EventID, "thing.reviewed", "entity-writer")
}

func runServedCLICommand(t *testing.T, endpoint string, args []string) (string, string, int) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	cliOpts := defaultRootCommandOptions()
	cliOpts.apiRPCEndpointOverride = endpoint
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, cliOpts)
	return stdout.String(), stderr.String(), code
}

func parseServedEventPublishOutput(t *testing.T, output string) map[string]string {
	t.Helper()
	for _, line := range strings.Split(output, "\n") {
		if !strings.HasPrefix(line, "event publish ok:") {
			continue
		}
		out := map[string]string{}
		for _, field := range strings.Fields(strings.TrimPrefix(line, "event publish ok:")) {
			key, value, ok := strings.Cut(field, "=")
			if ok {
				out[key] = value
			}
		}
		return out
	}
	t.Fatalf("event publish output missing success line:\n%s", output)
	return nil
}

type servedJSONRPCError struct {
	Code    int            `json:"code"`
	Message string         `json:"message"`
	Data    map[string]any `json:"data"`
}

type servedJSONRPCEnvelope struct {
	JSONRPC string              `json:"jsonrpc"`
	ID      string              `json:"id"`
	Result  json.RawMessage     `json:"result"`
	Error   *servedJSONRPCError `json:"error"`
}

func requireServedJSONRPCResult(t *testing.T, endpoint, method string, params map[string]any, out any) {
	t.Helper()
	resp := requestServedJSONRPC(t, endpoint, method, params)
	if resp.Error != nil {
		t.Fatalf("%s error = %#v", method, resp.Error)
	}
	if err := json.Unmarshal(resp.Result, out); err != nil {
		t.Fatalf("decode %s result: %v\n%s", method, err, string(resp.Result))
	}
}

func requireServedJSONRPCError(t *testing.T, endpoint, method string, params map[string]any) *servedJSONRPCError {
	t.Helper()
	resp := requestServedJSONRPC(t, endpoint, method, params)
	if resp.Error == nil {
		t.Fatalf("%s error = nil, result=%s", method, string(resp.Result))
	}
	return resp.Error
}

func requestServedJSONRPC(t *testing.T, endpoint, method string, params map[string]any) servedJSONRPCEnvelope {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      method + "-proof",
		"method":  method,
		"params":  params,
	})
	if err != nil {
		t.Fatalf("marshal %s request: %v", method, err)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build %s request: %v", method, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("post %s request: %v", method, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s HTTP status = %d, want 200", method, resp.StatusCode)
	}
	var envelope servedJSONRPCEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode %s envelope: %v", method, err)
	}
	return envelope
}

func requireServedRunStatus(t *testing.T, endpoint, runID, want string) {
	t.Helper()
	var last string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var result struct {
			Run struct {
				RunID  string `json:"run_id"`
				Status string `json:"status"`
			} `json:"run"`
		}
		requireServedJSONRPCResult(t, endpoint, "run.get", map[string]any{"run_id": runID}, &result)
		last = result.Run.Status
		if result.Run.RunID == runID && result.Run.Status == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("run.get status for %s = %q, want %q", runID, last, want)
}

func requireServedRunDiagnoseOperationalState(t *testing.T, endpoint, runID, want string) {
	t.Helper()
	type servedRunDiagnose struct {
		OperationalState string `json:"operational_state"`
		Run              struct {
			RunID  string `json:"run_id"`
			Status string `json:"status"`
		} `json:"run"`
	}
	var last servedRunDiagnose
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var result servedRunDiagnose
		requireServedJSONRPCResult(t, endpoint, "run.diagnose", map[string]any{"run_id": runID}, &result)
		last = result
		if result.Run.RunID == runID && result.Run.Status == want && result.OperationalState == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("run.diagnose for %s = %#v, want run/status and operational_state %q", runID, last, want)
}

func requireServedStatusCLIReadback(t *testing.T, endpoint, runID, want string) {
	t.Helper()
	var lastStdout, lastStderr string
	var lastCode int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		lastStdout, lastStderr, lastCode = runServedCLICommand(t, endpoint, []string{"status", runID, "--no-diagnose"})
		if lastCode == 0 && strings.Contains(lastStdout, want) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("status readback code=%d stderr=%s stdout=%s\nmissing %q", lastCode, lastStderr, lastStdout, want)
}

func requireServedEventReadback(t *testing.T, endpoint, eventID, runID, entityID, eventName, subscriberID string) {
	t.Helper()
	type servedEventReadback struct {
		EventID    string `json:"event_id"`
		EventName  string `json:"event_name"`
		EntityID   string `json:"entity_id"`
		RunID      string `json:"run_id"`
		Deliveries []struct {
			SubscriberType string `json:"subscriber_type"`
			SubscriberID   string `json:"subscriber_id"`
			Status         string `json:"status"`
		} `json:"deliveries"`
	}

	var last servedEventReadback
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var event servedEventReadback
		requireServedJSONRPCResult(t, endpoint, "event.get", map[string]any{"event_id": eventID}, &event)
		last = event
		if event.EventID != eventID || event.RunID != runID || event.EntityID != entityID || event.EventName != eventName {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		for _, delivery := range event.Deliveries {
			if delivery.SubscriberType == "node" && delivery.SubscriberID == subscriberID && delivery.Status == "delivered" {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("event.get result = %#v, want delivered node/%s for event %s on selected run", last, subscriberID, eventID)
}

func requireServedTraceReadback(t *testing.T, endpoint, runID, eventID, eventName, subscriberID string) {
	t.Helper()
	type servedTraceReadback struct {
		Trace []struct {
			EventID        string `json:"event_id"`
			EventName      string `json:"event_name"`
			DeliveryStatus string `json:"delivery_status"`
			SubscriberType string `json:"subscriber_type"`
			SubscriberID   string `json:"subscriber_id"`
		} `json:"trace"`
	}

	var last servedTraceReadback
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var trace servedTraceReadback
		requireServedJSONRPCResult(t, endpoint, "run.trace", map[string]any{
			"run_id": runID,
			"filter": map[string]any{
				"event_name": []string{eventName},
			},
			"limit": 10,
		}, &trace)
		last = trace
		for _, row := range trace.Trace {
			if row.EventID == eventID && row.EventName == eventName && row.DeliveryStatus == "delivered" &&
				row.SubscriberType == "node" && row.SubscriberID == subscriberID {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("run.trace rows = %#v, want delivered node/%s row for %s", last.Trace, subscriberID, eventID)
}

func requireServedEntityReadback(t *testing.T, endpoint, runID, entityID, wantState string) {
	t.Helper()
	var result struct {
		Entity struct {
			EntityID     string `json:"entity_id"`
			RunID        string `json:"run_id"`
			CurrentState string `json:"current_state"`
		} `json:"entity"`
	}
	requireServedJSONRPCResult(t, endpoint, "entity.get", map[string]any{"entity_id": entityID, "run_id": runID}, &result)
	if result.Entity.EntityID != entityID || result.Entity.RunID != runID || result.Entity.CurrentState != wantState {
		t.Fatalf("entity.get result = %#v, want %s/%s state %s", result.Entity, runID, entityID, wantState)
	}
}

func requireServedEventPublishEntityState(t *testing.T, db *sql.DB, backend, runID, entityID, wantState string) string {
	t.Helper()
	var lastState, lastEntityID string
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		gotEntityID, state := servedEventPublishEntityState(t, db, backend, runID, entityID, wantState)
		if state == wantState && gotEntityID != "" {
			return gotEntityID
		}
		lastEntityID = gotEntityID
		lastState = state
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s entity_state for run %s entity %q = %q (entity %q), want %q\n%s", backend, runID, entityID, lastState, lastEntityID, wantState, servedEventPublishDebugSummary(t, db, backend, runID))
	return ""
}

func requireServedEventPublishEntityFlowInstance(t *testing.T, db *sql.DB, backend, runID, entityID string) string {
	t.Helper()
	var (
		query        string
		flowInstance string
		args         []any
	)
	switch backend {
	case "postgres":
		query = `SELECT COALESCE(flow_instance, '') FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`
		args = []any{runID, entityID}
	case "sqlite":
		query = `SELECT COALESCE(flow_instance, '') FROM entity_state WHERE run_id = ? AND entity_id = ?`
		args = []any{runID, entityID}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&flowInstance); err != nil {
		t.Fatalf("%s load entity flow_instance run=%s entity=%s: %v", backend, runID, entityID, err)
	}
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	if flowInstance == "" {
		t.Fatalf("%s entity flow_instance for run=%s entity=%s is empty", backend, runID, entityID)
	}
	return flowInstance
}

func requireServedEventPublishTargetRouteRow(t *testing.T, db *sql.DB, backend, eventID, eventName, flowInstance, entityID string) {
	t.Helper()
	var (
		query           string
		gotEventName    string
		gotEntityID     string
		gotFlowInstance string
		targetRoute     string
		args            []any
	)
	switch backend {
	case "postgres":
		query = `
			SELECT event_name, COALESCE(entity_id::text, ''), COALESCE(flow_instance, ''), COALESCE(target_route::text, '{}')
			FROM events
			WHERE event_id = $1::uuid
		`
		args = []any{eventID}
	case "sqlite":
		query = `
			SELECT event_name, COALESCE(entity_id, ''), COALESCE(flow_instance, ''), COALESCE(target_route, '{}')
			FROM events
			WHERE event_id = ?
		`
		args = []any{eventID}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&gotEventName, &gotEntityID, &gotFlowInstance, &targetRoute); err != nil {
		t.Fatalf("%s load target-route event row: %v", backend, err)
	}
	if gotEventName != eventName || gotEntityID != entityID || gotFlowInstance != flowInstance {
		t.Fatalf("%s target event row = event:%q entity:%q flow:%q, want %q/%q/%q", backend, gotEventName, gotEntityID, gotFlowInstance, eventName, entityID, flowInstance)
	}
	requireServedEventPublishRouteJSON(t, backend, "event.target_route", targetRoute, flowInstance, entityID)
}

func requireServedEventPublishDeliveryTargetRoute(t *testing.T, db *sql.DB, backend, eventID, subscriberType, subscriberID, flowInstance, entityID string) {
	t.Helper()
	var (
		query       string
		targetRoute string
		args        []any
	)
	switch backend {
	case "postgres":
		query = `
			SELECT COALESCE(delivery_target_route::text, '{}')
			FROM event_deliveries
			WHERE event_id = $1::uuid
			  AND subscriber_type = $2
			  AND subscriber_id = $3
			LIMIT 1
		`
		args = []any{eventID, subscriberType, subscriberID}
	case "sqlite":
		query = `
			SELECT COALESCE(delivery_target_route, '{}')
			FROM event_deliveries
			WHERE event_id = ?
			  AND subscriber_type = ?
			  AND subscriber_id = ?
			LIMIT 1
		`
		args = []any{eventID, subscriberType, subscriberID}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&targetRoute); err != nil {
		t.Fatalf("%s load delivery target route: %v", backend, err)
	}
	requireServedEventPublishRouteJSON(t, backend, "event_deliveries.delivery_target_route", targetRoute, flowInstance, entityID)
}

func requireServedEventPublishRouteJSON(t *testing.T, backend, field, raw, flowInstance, entityID string) {
	t.Helper()
	var decoded map[string]any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatalf("%s decode %s route JSON %q: %v", backend, field, raw, err)
	}
	if decoded["flow_instance"] != flowInstance || decoded["entity_id"] != entityID {
		t.Fatalf("%s %s route = %#v, want flow/entity %s/%s", backend, field, decoded, flowInstance, entityID)
	}
}

func servedEventPublishDebugSummary(t *testing.T, db *sql.DB, backend, runID string) string {
	t.Helper()
	sections := []string{
		servedEventPublishDebugQuery(t, db, backend, "entity_state", runID),
		servedEventPublishDebugQuery(t, db, backend, "events", runID),
		servedEventPublishDebugQuery(t, db, backend, "event_deliveries", runID),
		servedEventPublishDebugQuery(t, db, backend, "event_receipts", runID),
		servedEventPublishDebugQuery(t, db, backend, "dead_letters", runID),
	}
	return strings.Join(sections, "\n")
}

func servedEventPublishDebugQuery(t *testing.T, db *sql.DB, backend, scope, runID string) string {
	t.Helper()
	sqlText := ""
	args := []any{runID}
	switch backend {
	case "postgres":
		switch scope {
		case "entity_state":
			sqlText = `SELECT entity_id::text, COALESCE(flow_instance, ''), COALESCE(current_state, '') FROM entity_state WHERE run_id = $1::uuid ORDER BY created_at, entity_id LIMIT 5`
		case "events":
			sqlText = `SELECT event_id::text, event_name, COALESCE(entity_id::text, ''), COALESCE(flow_instance, '') FROM events WHERE run_id = $1::uuid ORDER BY created_at, event_id LIMIT 5`
		case "event_deliveries":
			sqlText = `SELECT event_id::text, subscriber_type, subscriber_id, status, COALESCE(reason_code, '') FROM event_deliveries WHERE run_id = $1::uuid ORDER BY created_at, event_id LIMIT 8`
		case "event_receipts":
			sqlText = `SELECT r.event_id::text, r.subscriber_type, r.subscriber_id, r.outcome, COALESCE(r.reason_code, ''), COALESCE(r.side_effects::text, '') FROM event_receipts r JOIN events e ON e.event_id = r.event_id WHERE e.run_id = $1::uuid ORDER BY r.processed_at, r.event_id LIMIT 8`
		case "dead_letters":
			sqlText = `SELECT d.original_event, COALESCE(d.entity_id::text, ''), d.failure_type, COALESCE(d.error_message, '') FROM dead_letters d JOIN events e ON e.event_id = d.original_event_id WHERE e.run_id = $1::uuid ORDER BY d.created_at LIMIT 5`
		}
	case "sqlite":
		switch scope {
		case "entity_state":
			sqlText = `SELECT entity_id, COALESCE(flow_instance, ''), COALESCE(current_state, '') FROM entity_state WHERE run_id = ? ORDER BY created_at, entity_id LIMIT 5`
		case "events":
			sqlText = `SELECT event_id, event_name, COALESCE(entity_id, ''), COALESCE(flow_instance, '') FROM events WHERE run_id = ? ORDER BY created_at, event_id LIMIT 5`
		case "event_deliveries":
			sqlText = `SELECT event_id, subscriber_type, subscriber_id, status, COALESCE(reason_code, '') FROM event_deliveries WHERE run_id = ? ORDER BY created_at, event_id LIMIT 8`
		case "event_receipts":
			sqlText = `SELECT r.event_id, r.subscriber_type, r.subscriber_id, r.outcome, COALESCE(r.reason_code, ''), COALESCE(r.side_effects, '') FROM event_receipts r JOIN events e ON e.event_id = r.event_id WHERE e.run_id = ? ORDER BY r.processed_at, r.event_id LIMIT 8`
		case "dead_letters":
			sqlText = `SELECT d.original_event, COALESCE(d.entity_id, ''), d.failure_type, COALESCE(d.error_message, '') FROM dead_letters d JOIN events e ON e.event_id = d.original_event_id WHERE e.run_id = ? ORDER BY d.created_at LIMIT 5`
		}
	}
	if sqlText == "" {
		return scope + ": unsupported debug query"
	}
	rows, err := db.QueryContext(context.Background(), sqlText, args...)
	if err != nil {
		return fmt.Sprintf("%s: %v", scope, err)
	}
	defer rows.Close()
	out := []string{}
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Sprintf("%s columns: %v", scope, err)
	}
	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		scan := make([]any, len(values))
		for i := range values {
			scan[i] = &values[i]
		}
		if err := rows.Scan(scan...); err != nil {
			return fmt.Sprintf("%s scan: %v", scope, err)
		}
		cols := make([]string, len(values))
		for i, value := range values {
			if value.Valid {
				cols[i] = value.String
			}
		}
		out = append(out, fmt.Sprintf("%v", cols))
	}
	if err := rows.Err(); err != nil {
		return fmt.Sprintf("%s rows: %v", scope, err)
	}
	if len(out) == 0 {
		return scope + ": []"
	}
	return scope + ": " + strings.Join(out, "; ")
}

func servedEventPublishRowCount(t *testing.T, db *sql.DB, backend, scope, runID, eventID string) int {
	t.Helper()
	return servedEventPublishScalarCount(t, db, backend, scope, runID, eventID)
}

func servedEventPublishScalarCount(t *testing.T, db *sql.DB, backend, scope, runID, eventID string) int {
	t.Helper()
	var sqlText string
	var args []any
	switch backend {
	case "postgres":
		switch scope {
		case "runs":
			sqlText = `SELECT COUNT(*) FROM runs WHERE run_id = $1::uuid`
			args = []any{runID}
		case "events_by_run":
			sqlText = `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid`
			args = []any{runID}
		case "application_events_by_run":
			sqlText = `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid AND event_name NOT LIKE 'platform.%'`
			args = []any{runID}
		case "event_deliveries":
			sqlText = `SELECT COUNT(*) FROM event_deliveries WHERE run_id = $1::uuid AND event_id = $2::uuid`
			args = []any{runID, eventID}
		default:
			t.Fatalf("unknown postgres proof count scope %q", scope)
		}
	case "sqlite":
		switch scope {
		case "runs":
			sqlText = `SELECT COUNT(*) FROM runs WHERE run_id = ?`
			args = []any{runID}
		case "events_by_run":
			sqlText = `SELECT COUNT(*) FROM events WHERE run_id = ?`
			args = []any{runID}
		case "application_events_by_run":
			sqlText = `SELECT COUNT(*) FROM events WHERE run_id = ? AND event_name NOT LIKE 'platform.%'`
			args = []any{runID}
		case "event_deliveries":
			sqlText = `SELECT COUNT(*) FROM event_deliveries WHERE run_id = ? AND event_id = ?`
			args = []any{runID, eventID}
		default:
			t.Fatalf("unknown sqlite proof count scope %q", scope)
		}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var count int
	if err := db.QueryRowContext(context.Background(), sqlText, args...).Scan(&count); err != nil {
		t.Fatalf("%s count %s: %v", backend, scope, err)
	}
	return count
}

func servedEventPublishEventCountByIdempotencyKey(t *testing.T, db *sql.DB, backend, idempotencyKey string) int {
	t.Helper()
	var (
		query string
		args  []any
	)
	switch backend {
	case "postgres":
		query = `SELECT COUNT(*) FROM events WHERE idempotency_key = $1`
		args = []any{idempotencyKey}
	case "sqlite":
		query = `SELECT COUNT(*) FROM events WHERE idempotency_key = ?`
		args = []any{idempotencyKey}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var count int
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatalf("%s count events by idempotency key %q: %v", backend, idempotencyKey, err)
	}
	return count
}

func servedEventPublishAPIIdempotencyCount(t *testing.T, db *sql.DB, backend, method, idempotencyKey string) int {
	t.Helper()
	var (
		query string
		args  []any
	)
	switch backend {
	case "postgres":
		query = `SELECT COUNT(*) FROM api_idempotency WHERE method = $1 AND idempotency_key = $2`
		args = []any{method, idempotencyKey}
	case "sqlite":
		query = `SELECT COUNT(*) FROM api_idempotency WHERE method = ? AND idempotency_key = ?`
		args = []any{method, idempotencyKey}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var count int
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatalf("%s count api idempotency method=%q key=%q: %v", backend, method, idempotencyKey, err)
	}
	return count
}

func waitServedEventPublishDeliveryStatusCount(t *testing.T, db *sql.DB, backend, eventID, subscriberType, subscriberID, status string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var got int
	for time.Now().Before(deadline) {
		got = servedEventPublishDeliveryStatusCount(t, db, backend, eventID, subscriberType, subscriberID, status)
		if got == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("%s delivery count for event=%s subscriber=%s/%s status=%q = %d, want %d", backend, eventID, subscriberType, subscriberID, status, got, want)
}

func servedEventPublishDeliveryStatusCount(t *testing.T, db *sql.DB, backend, eventID, subscriberType, subscriberID string, statuses ...string) int {
	t.Helper()
	where := []string{}
	args := []any{}
	switch backend {
	case "postgres":
		where = append(where, fmt.Sprintf("event_id = $%d::uuid", len(args)+1))
		args = append(args, eventID)
		if strings.TrimSpace(subscriberType) != "" {
			where = append(where, fmt.Sprintf("subscriber_type = $%d", len(args)+1))
			args = append(args, subscriberType)
		}
		where = append(where, fmt.Sprintf("subscriber_id = $%d", len(args)+1))
		args = append(args, subscriberID)
		for _, status := range statuses {
			if strings.TrimSpace(status) == "" {
				continue
			}
			where = append(where, fmt.Sprintf("status = $%d", len(args)+1))
			args = append(args, status)
		}
	case "sqlite":
		where = append(where, "event_id = ?")
		args = append(args, eventID)
		if strings.TrimSpace(subscriberType) != "" {
			where = append(where, "subscriber_type = ?")
			args = append(args, subscriberType)
		}
		where = append(where, "subscriber_id = ?")
		args = append(args, subscriberID)
		for _, status := range statuses {
			if strings.TrimSpace(status) == "" {
				continue
			}
			where = append(where, "status = ?")
			args = append(args, status)
		}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	query := "SELECT COUNT(*) FROM event_deliveries WHERE " + strings.Join(where, " AND ")
	var count int
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatalf("%s delivery count query failed: %v\n%s", backend, err, query)
	}
	return count
}

func requireServedEventPublishCommittedReplayScope(t *testing.T, db *sql.DB, backend, runID, eventID, wantScope string) {
	t.Helper()
	wantReason := "replay_scope_" + strings.TrimSpace(wantScope)
	var (
		query string
		args  []any
	)
	switch backend {
	case "postgres":
		query = `
			SELECT COUNT(*)
			FROM event_deliveries
			WHERE run_id = $1::uuid
			  AND event_id = $2::uuid
			  AND subscriber_type = 'node'
			  AND subscriber_id = '__runtime_replay_scope__'
			  AND status = 'delivered'
			  AND reason_code = $3
			  AND delivered_at IS NOT NULL
		`
		args = []any{runID, eventID, wantReason}
	case "sqlite":
		query = `
			SELECT COUNT(*)
			FROM event_deliveries
			WHERE run_id = ?
			  AND event_id = ?
			  AND subscriber_type = 'node'
			  AND subscriber_id = '__runtime_replay_scope__'
			  AND status = 'delivered'
			  AND reason_code = ?
			  AND delivered_at IS NOT NULL
		`
		args = []any{runID, eventID, wantReason}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var count int
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatalf("%s committed replay scope query failed: %v\n%s", backend, err, query)
	}
	if count != 1 {
		t.Fatalf("%s committed replay scope rows for run=%s event=%s reason=%q = %d, want 1\n%s", backend, runID, eventID, wantReason, count, servedEventPublishDebugSummary(t, db, backend, runID))
	}
}

func waitServedEventPublishReceiptOutcomeCount(t *testing.T, db *sql.DB, backend, eventID, subscriberType, subscriberID, outcome string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var got int
	for time.Now().Before(deadline) {
		got = servedEventPublishReceiptOutcomeCount(t, db, backend, eventID, subscriberType, subscriberID, outcome)
		if got == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("%s receipt count for event=%s subscriber=%s/%s outcome=%q = %d, want %d", backend, eventID, subscriberType, subscriberID, outcome, got, want)
}

func servedEventPublishReceiptOutcomeCount(t *testing.T, db *sql.DB, backend, eventID, subscriberType, subscriberID, outcome string) int {
	t.Helper()
	var query string
	var args []any
	switch backend {
	case "postgres":
		query = `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = $2 AND subscriber_id = $3 AND outcome = $4`
		args = []any{eventID, subscriberType, subscriberID, outcome}
	case "sqlite":
		query = `SELECT COUNT(*) FROM event_receipts WHERE event_id = ? AND subscriber_type = ? AND subscriber_id = ? AND outcome = ?`
		args = []any{eventID, subscriberType, subscriberID, outcome}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var count int
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatalf("%s receipt count query failed: %v\n%s", backend, err, query)
	}
	return count
}

func servedEventPublishReceiptCountBySubscribers(t *testing.T, db *sql.DB, backend, eventID string, subscriberIDs ...string) int {
	t.Helper()
	if len(subscriberIDs) == 0 {
		t.Fatal("subscriberIDs is required")
	}
	var query string
	args := []any{eventID}
	switch backend {
	case "postgres":
		placeholders := make([]string, 0, len(subscriberIDs))
		for _, subscriberID := range subscriberIDs {
			placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)+1))
			args = append(args, subscriberID)
		}
		query = `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid AND subscriber_id IN (` + strings.Join(placeholders, ", ") + `)`
	case "sqlite":
		placeholders := make([]string, 0, len(subscriberIDs))
		for _, subscriberID := range subscriberIDs {
			placeholders = append(placeholders, "?")
			args = append(args, subscriberID)
		}
		query = `SELECT COUNT(*) FROM event_receipts WHERE event_id = ? AND subscriber_id IN (` + strings.Join(placeholders, ", ") + `)`
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var count int
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatalf("%s receipt subscriber count query failed: %v\n%s", backend, err, query)
	}
	return count
}

func waitServedEventPublishEventID(t *testing.T, db *sql.DB, backend, runID, eventName string) string {
	t.Helper()
	var query string
	var args []any
	switch backend {
	case "postgres":
		query = `SELECT event_id::text FROM events WHERE run_id = $1::uuid AND event_name = $2`
		args = []any{runID, eventName}
	case "sqlite":
		query = `SELECT event_id FROM events WHERE run_id = ? AND event_name = ?`
		args = []any{runID, eventName}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		var eventID string
		err := db.QueryRowContext(context.Background(), query, args...).Scan(&eventID)
		if err == nil && strings.TrimSpace(eventID) != "" {
			return strings.TrimSpace(eventID)
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("%s event id query failed: %v\n%s", backend, err, query)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s timed out waiting for event %q in run %s\n%s", backend, eventName, runID, servedEventPublishDebugSummary(t, db, backend, runID))
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func servedEventPublishEventEntityID(t *testing.T, db *sql.DB, backend, eventID string) string {
	t.Helper()
	var query string
	var args []any
	switch backend {
	case "postgres":
		query = `SELECT COALESCE(entity_id::text, '') FROM events WHERE event_id = $1::uuid`
		args = []any{eventID}
	case "sqlite":
		query = `SELECT COALESCE(entity_id, '') FROM events WHERE event_id = ?`
		args = []any{eventID}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var entityID string
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&entityID); err != nil {
		t.Fatalf("%s load event entity_id %s: %v", backend, eventID, err)
	}
	return strings.TrimSpace(entityID)
}

func assertServedDynamicAutoEmitPayloadProductOnly(t *testing.T, db *sql.DB, backend, eventID string) {
	t.Helper()
	var raw string
	var query string
	var args []any
	switch backend {
	case "postgres":
		query = `SELECT payload::text FROM events WHERE event_id = $1::uuid`
		args = []any{eventID}
	case "sqlite":
		query = `SELECT payload FROM events WHERE event_id = ?`
		args = []any{eventID}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&raw); err != nil {
		t.Fatalf("%s load event payload %s: %v", backend, eventID, err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("decode event payload %s: %v\n%s", eventID, err, raw)
	}
	if got := payload["product_id"]; got != "product-1" {
		t.Fatalf("payload product_id = %#v, want product-1: %#v", got, payload)
	}
	for _, key := range []string{"instance_id", "template_id", "flow_path", "parent_entity_id"} {
		if _, ok := payload[key]; ok {
			t.Fatalf("payload includes hidden activation context %q: %#v", key, payload)
		}
	}
}

func requireServedReplayNoDeliveryHistoryNoMutation(t *testing.T, endpoint string, db *sql.DB, backend, eventID, idempotencyKey string) {
	t.Helper()
	beforeSourcedEvents := servedEventPublishSourcedEventCount(t, db, backend, eventID)
	errResp := requireServedJSONRPCError(t, endpoint, "event.replay", map[string]any{
		"event_id":        eventID,
		"idempotency_key": idempotencyKey,
	})
	if errResp.Data["code"] != apiv1.EventReplayNoDeliveryHistoryCode {
		t.Fatalf("event.replay data = %#v, want %s", errResp.Data, apiv1.EventReplayNoDeliveryHistoryCode)
	}
	if got := servedEventPublishSourcedEventCount(t, db, backend, eventID); got != beforeSourcedEvents {
		t.Fatalf("%s events sourced from rejected node-only replay target = %d, want %d", backend, got, beforeSourcedEvents)
	}
	if got := servedEventPublishAPIIdempotencyCount(t, db, backend, "event.replay", idempotencyKey); got != 0 {
		t.Fatalf("%s api_idempotency rows for rejected node-only replay = %d, want 0", backend, got)
	}
}

func servedEventPublishSourcedEventCount(t *testing.T, db *sql.DB, backend, sourceEventID string) int {
	t.Helper()
	var query string
	var args []any
	switch backend {
	case "postgres":
		query = `SELECT COUNT(*) FROM events WHERE source_event_id = $1::uuid`
		args = []any{sourceEventID}
	case "sqlite":
		query = `SELECT COUNT(*) FROM events WHERE source_event_id = ?`
		args = []any{sourceEventID}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var count int
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatalf("%s sourced event count query failed: %v\n%s", backend, err, query)
	}
	return count
}

func servedEventPublishNodeDeliveryCount(t *testing.T, db *sql.DB, backend, runID, eventID, subscriberID string) int {
	t.Helper()
	count, err := servedEventPublishNodeDeliveryCountValue(context.Background(), db, backend, runID, eventID, subscriberID)
	if err != nil {
		t.Fatalf("%s count node delivery %s/%s: %v", backend, eventID, subscriberID, err)
	}
	return count
}

func servedEventPublishNodeDeliveryCountValue(ctx context.Context, db *sql.DB, backend, runID, eventID, subscriberID string) (int, error) {
	var sqlText string
	var args []any
	switch backend {
	case "postgres":
		sqlText = `
			SELECT COUNT(*)
			FROM event_deliveries
			WHERE run_id = $1::uuid
			  AND event_id = $2::uuid
			  AND subscriber_type = 'node'
			  AND subscriber_id = $3
		`
		args = []any{runID, eventID, subscriberID}
	case "sqlite":
		sqlText = `
			SELECT COUNT(*)
			FROM event_deliveries
			WHERE run_id = ?
			  AND event_id = ?
			  AND subscriber_type = 'node'
			  AND subscriber_id = ?
		`
		args = []any{runID, eventID, subscriberID}
	default:
		return 0, fmt.Errorf("unknown proof backend %q", backend)
	}
	var count int
	if err := db.QueryRowContext(ctx, sqlText, args...).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func servedEventPublishEntityState(t *testing.T, db *sql.DB, backend, runID, entityID, wantState string) (string, string) {
	t.Helper()
	var sqlText string
	var args []any
	switch backend {
	case "postgres":
		if strings.TrimSpace(entityID) != "" {
			sqlText = `SELECT COALESCE(current_state, '') FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`
			args = []any{runID, entityID}
		} else {
			sqlText = `SELECT entity_id::text, COALESCE(current_state, '') FROM entity_state WHERE run_id = $1::uuid AND current_state = $2 ORDER BY created_at, entity_id LIMIT 1`
			args = []any{runID, wantState}
		}
	case "sqlite":
		if strings.TrimSpace(entityID) != "" {
			sqlText = `SELECT COALESCE(current_state, '') FROM entity_state WHERE run_id = ? AND entity_id = ?`
			args = []any{runID, entityID}
		} else {
			sqlText = `SELECT entity_id, COALESCE(current_state, '') FROM entity_state WHERE run_id = ? AND current_state = ? ORDER BY created_at, entity_id LIMIT 1`
			args = []any{runID, wantState}
		}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var gotEntityID, state string
	if strings.TrimSpace(entityID) != "" {
		gotEntityID = strings.TrimSpace(entityID)
		err := db.QueryRowContext(context.Background(), sqlText, args...).Scan(&state)
		if err == sql.ErrNoRows {
			return gotEntityID, ""
		}
		if err != nil {
			t.Fatalf("%s entity_state query: %v", backend, err)
		}
		return gotEntityID, state
	}
	err := db.QueryRowContext(context.Background(), sqlText, args...).Scan(&gotEntityID, &state)
	if err == sql.ErrNoRows {
		return "", ""
	}
	if err != nil {
		t.Fatalf("%s entity_state query: %v", backend, err)
	}
	return gotEntityID, state
}

func TestRunServeRuntimePassesDataFlagToWorkspaceLifecycle(t *testing.T) {
	dataDir := t.TempDir()
	var capturedMountSources workspaceMountSources
	_, _, _ = installServeRuntimePostgresTestStoresWithWorkspaceFactory(t, func(mountSources workspaceMountSources) serveWorkspaceLifecycle {
		capturedMountSources = mountSources
		return serveRuntimeWorkspaceStub{}
	})

	serve := startServeRuntimeTestProcess(t, serveOptions{
		ConfigPath:         writeServeRuntimeTestConfig(t),
		ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		DataSource:         dataDir,
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "postgres",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: false,
		Verbose:            true,
	})
	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("runServeRuntime code = %d\noutput:\n%s", code, serve.outputString())
	}
	if capturedMountSources.DataSource != dataDir || capturedMountSources.DataSourceSource != "--data" {
		t.Fatalf("workspace mount sources = %#v, want %q from --data", capturedMountSources, dataDir)
	}
}

func TestRunServeRuntimeHostWorkspaceBackendBootsWithoutDockerForSystemOnlyFlow(t *testing.T) {
	t.Setenv("SWARM_DOCKER_BIN", filepath.Join(t.TempDir(), "missing-docker"))
	t.Setenv("SWARM_WORKSPACE_HOST_ROOT", filepath.Join(t.TempDir(), "host-workspaces"))
	t.Setenv(storebackend.EnvSQLitePath, filepath.Join(t.TempDir(), "runtime.db"))
	dataDir := t.TempDir()
	configPath := writeServeRuntimeTestConfig(t)

	serve := startServeRuntimeTestProcess(t, serveOptions{
		ConfigPath:           configPath,
		ContractsPath:        filepath.Join("tests", "tier1-primitives", "test-emits-single"),
		DataSource:           dataDir,
		WorkspaceBackend:     workspace.BackendHost,
		WorkspaceBackendSet:  true,
		PlatformSpecPath:     defaultPlatformSpecPath,
		StoreMode:            storebackend.ActiveDefaultBackend().String(),
		APIListenAddr:        "127.0.0.1:0",
		MCPListenAddr:        "127.0.0.1:0",
		SelfCheck:            true,
		RequireBundleMatch:   false,
		ShutdownGrace:        runtimepkg.DefaultShutdownGrace,
		Verbose:              true,
		NoRequireBundleMatch: true,
	})
	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("runServeRuntime code = %d\noutput:\n%s", code, serve.outputString())
	}
	if strings.Contains(serve.outputString(), "workspace image") || strings.Contains(serve.outputString(), "docker is not available") {
		t.Fatalf("host workspace serve output shows Docker dependency despite host backend:\n%s", serve.outputString())
	}
}

func TestRunServeRuntimeFreshEmptyPostgresBootstrapsSchemaBeforeDiskContractsServe(t *testing.T) {
	_, db, _ := installServeRuntimeEmptyPostgresTestStores(t, func() serveWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	serve := startServeRuntimeTestProcess(t, serveOptions{
		ConfigPath:         writeServeRuntimeTestConfig(t),
		ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "postgres",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: true,
		Verbose:            true,
	})

	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("runServeRuntime code = %d\noutput:\n%s", code, serve.outputString())
	}
	for _, table := range []string{"bundles", "runs", "events", "event_deliveries"} {
		assertPostgresTableExists(t, db, table)
	}
	if !strings.Contains(serve.outputString(), "state_stores=verified") {
		t.Fatalf("serve output missing state store proof:\n%s", serve.outputString())
	}
}

func TestRunServeRuntimeFreshEmptyPostgresBootstrapsSchemaBeforeDevAbandon(t *testing.T) {
	_, db, _ := installServeRuntimeEmptyPostgresTestStores(t, func() serveWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	serve := startServeRuntimeTestProcess(t, serveOptions{
		ConfigPath:           writeServeRuntimeTestConfig(t),
		ContractsPath:        filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		PlatformSpecPath:     defaultPlatformSpecPath,
		StoreMode:            "postgres",
		APIListenAddr:        "127.0.0.1:0",
		MCPListenAddr:        "127.0.0.1:0",
		SelfCheck:            true,
		Dev:                  true,
		RequireBundleMatch:   false,
		NoRequireBundleMatch: true,
		AbandonActiveRuns:    true,
		Verbose:              true,
	})

	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("runServeRuntime code = %d\noutput:\n%s", code, serve.outputString())
	}
	for _, table := range []string{"bundles", "runs", "run_control_state", "event_receipts"} {
		assertPostgresTableExists(t, db, table)
	}
	if strings.Contains(serve.outputString(), "relation") && strings.Contains(serve.outputString(), "does not exist") {
		t.Fatalf("serve output contains missing-table failure:\n%s", serve.outputString())
	}
}

func TestRunServeRuntimeFreshEmptySQLiteBootsWithDevAbandon(t *testing.T) {
	runServeRuntimeFreshEmptySQLiteBootsWithAbandon(t, true)
}

func TestRunServeRuntimeFreshEmptySQLiteBootsWithDirectAbandon(t *testing.T) {
	runServeRuntimeFreshEmptySQLiteBootsWithAbandon(t, false)
}

func runServeRuntimeFreshEmptySQLiteBootsWithAbandon(t *testing.T, dev bool) {
	t.Helper()
	stubServeRuntimeWorkspaceLifecycle(t)
	unsetStoreSelectorEnv(t)
	sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	t.Setenv(storebackend.EnvSQLitePath, sqlitePath)
	requireBundleMatch := !dev
	noRequireBundleMatch := dev
	serve := startServeRuntimeTestProcess(t, serveOptions{
		ConfigPath:           writeServeRuntimeTestConfig(t),
		ContractsPath:        filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		PlatformSpecPath:     defaultPlatformSpecPath,
		APIListenAddr:        "127.0.0.1:0",
		MCPListenAddr:        "127.0.0.1:0",
		SelfCheck:            true,
		Dev:                  dev,
		RequireBundleMatch:   requireBundleMatch,
		NoRequireBundleMatch: noRequireBundleMatch,
		AbandonActiveRuns:    true,
		Verbose:              true,
	})

	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("runServeRuntime code = %d\noutput:\n%s", code, serve.outputString())
	}
	if strings.Contains(serve.outputString(), "postgres store is required") {
		t.Fatalf("serve output contains stale postgres-only abandon failure:\n%s", serve.outputString())
	}
	if _, err := os.Stat(sqlitePath); err != nil {
		t.Fatalf("sqlite dev db not created at %s: %v", sqlitePath, err)
	}
}

func TestRunServeRuntimeArtifactRepoCommitFailsBeforeReadinessForUnusableArtifactRoot(t *testing.T) {
	stubServeRuntimeWorkspaceLifecycle(t)
	unsetStoreSelectorEnv(t)
	sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	t.Setenv(storebackend.EnvSQLitePath, sqlitePath)
	rootFile := filepath.Join(t.TempDir(), "artifact-root")
	if err := os.WriteFile(rootFile, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("write unusable artifact root: %v", err)
	}
	t.Setenv("SWARM_ARTIFACT_ROOT", rootFile)

	var out lockedBuffer
	code := runServeRuntime(context.Background(), repoRoot(), serveOptions{
		ConfigPath:           writeServeRuntimeTestConfig(t),
		ContractsPath:        writeArtifactRepoCommitServeFixture(t),
		PlatformSpecPath:     defaultPlatformSpecPath,
		StoreMode:            "sqlite",
		StoreModeSet:         true,
		APIListenAddr:        "127.0.0.1:0",
		MCPListenAddr:        "127.0.0.1:0",
		SelfCheck:            true,
		Dev:                  true,
		RequireBundleMatch:   false,
		NoRequireBundleMatch: true,
		Verbose:              true,
		Output:               &out,
	})
	if code == 0 {
		t.Fatalf("runServeRuntime code = 0, want startup failure\noutput:\n%s", out.String())
	}
	for _, want := range []string{
		"[5/22] runtime_context",
		"artifact repo root startup validation failed",
		rootFile,
		"SWARM_ARTIFACT_ROOT=<writable runtime-private absolute path>",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("serve output missing %q:\n%s", want, out.String())
		}
	}
	if strings.Contains(out.String(), "[22/22]") {
		t.Fatalf("serve reached readiness despite unusable artifact root:\n%s", out.String())
	}
}

func TestRunServeRuntimeArtifactRepoCommitFailsBeforeReadinessForBlockedRepoStorageBase(t *testing.T) {
	stubServeRuntimeWorkspaceLifecycle(t)
	unsetStoreSelectorEnv(t)
	sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	t.Setenv(storebackend.EnvSQLitePath, sqlitePath)
	artifactRoot := t.TempDir()
	reposFile := filepath.Join(artifactRoot, "repos")
	if err := os.WriteFile(reposFile, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("write unusable repos base: %v", err)
	}
	t.Setenv("SWARM_ARTIFACT_ROOT", artifactRoot)

	var out lockedBuffer
	code := runServeRuntime(context.Background(), repoRoot(), serveOptions{
		ConfigPath:           writeServeRuntimeTestConfig(t),
		ContractsPath:        writeArtifactRepoCommitServeFixture(t),
		PlatformSpecPath:     defaultPlatformSpecPath,
		StoreMode:            "sqlite",
		StoreModeSet:         true,
		APIListenAddr:        "127.0.0.1:0",
		MCPListenAddr:        "127.0.0.1:0",
		SelfCheck:            true,
		Dev:                  true,
		RequireBundleMatch:   false,
		NoRequireBundleMatch: true,
		Verbose:              true,
		Output:               &out,
	})
	if code == 0 {
		t.Fatalf("runServeRuntime code = 0, want startup failure\noutput:\n%s", out.String())
	}
	for _, want := range []string{
		"[5/22] runtime_context",
		"artifact repo root startup validation failed",
		artifactRoot,
		reposFile,
		"SWARM_ARTIFACT_ROOT=<writable runtime-private absolute path>",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("serve output missing %q:\n%s", want, out.String())
		}
	}
	if strings.Contains(out.String(), "[22/22]") {
		t.Fatalf("serve reached readiness despite blocked artifact repo storage base:\n%s", out.String())
	}
}

func TestRunServeRuntimeNonArtifactBundleDoesNotExerciseUnusableArtifactRoot(t *testing.T) {
	stubServeRuntimeWorkspaceLifecycle(t)
	unsetStoreSelectorEnv(t)
	sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	t.Setenv(storebackend.EnvSQLitePath, sqlitePath)
	rootFile := filepath.Join(t.TempDir(), "artifact-root")
	if err := os.WriteFile(rootFile, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("write unusable artifact root: %v", err)
	}
	t.Setenv("SWARM_ARTIFACT_ROOT", rootFile)

	serve := startServeRuntimeTestProcess(t, serveOptions{
		ConfigPath:           writeServeRuntimeTestConfig(t),
		ContractsPath:        filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		PlatformSpecPath:     defaultPlatformSpecPath,
		StoreMode:            "sqlite",
		StoreModeSet:         true,
		APIListenAddr:        "127.0.0.1:0",
		MCPListenAddr:        "127.0.0.1:0",
		SelfCheck:            true,
		Dev:                  true,
		RequireBundleMatch:   false,
		NoRequireBundleMatch: true,
		Verbose:              true,
	})
	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("runServeRuntime code = %d\noutput:\n%s", code, serve.outputString())
	}
	if strings.Contains(serve.outputString(), "artifact repo root startup validation failed") {
		t.Fatalf("non-artifact bundle hit artifact root admission:\n%s", serve.outputString())
	}
}

func TestRunServeRuntimeSQLiteAbandonActiveRunsQuiescesBeforeReadiness(t *testing.T) {
	stubServeRuntimeWorkspaceLifecycle(t)
	unsetStoreSelectorEnv(t)
	sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	runID, eventID := seedServeRuntimeSQLiteAbandonWork(t, sqlitePath)
	t.Setenv(storebackend.EnvSQLitePath, sqlitePath)
	ctx := context.Background()
	serve := startServeRuntimeTestProcess(t, serveOptions{
		ConfigPath:         writeServeRuntimeTestConfig(t),
		ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		PlatformSpecPath:   defaultPlatformSpecPath,
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: true,
		AbandonActiveRuns:  true,
		Verbose:            true,
	})

	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("runServeRuntime code = %d\noutput:\n%s", code, serve.outputString())
	}
	sqliteStore, err := store.NewSQLiteRuntimeStore(sqlitePath)
	if err != nil {
		t.Fatalf("reopen sqlite store: %v", err)
	}
	defer func() {
		if err := sqliteStore.Close(); err != nil {
			t.Fatalf("close sqlite store: %v", err)
		}
	}()
	ctx = context.Background()
	var runStatus, controlStatus, reason, controlledBy string
	if err := sqliteStore.DB.QueryRowContext(ctx, `
		SELECT r.status, rc.control_status, COALESCE(rc.reason, ''), COALESCE(rc.controlled_by, '')
		FROM runs r
		JOIN run_control_state rc ON rc.run_id = r.run_id
		WHERE r.run_id = ?
	`, runID).Scan(&runStatus, &controlStatus, &reason, &controlledBy); err != nil {
		t.Fatalf("load sqlite serve-abandoned run/control: %v", err)
	}
	if runStatus != "cancelled" || controlStatus != "stopped" || reason != runtimerunquiescence.ServeAbandonReasonCode || controlledBy != runtimerunquiescence.ServeAbandonControlledBy {
		t.Fatalf("sqlite serve run/control = %s/%s/%s/%s, want cancelled/stopped/%s/%s", runStatus, controlStatus, reason, controlledBy, runtimerunquiescence.ServeAbandonReasonCode, runtimerunquiescence.ServeAbandonControlledBy)
	}
	var deliveryStatus, deliveryReason string
	var activeSession sql.NullString
	if err := sqliteStore.DB.QueryRowContext(ctx, `
		SELECT status, COALESCE(reason_code, ''), active_session_id
		FROM event_deliveries
		WHERE event_id = ?
		  AND subscriber_type = 'agent'
		  AND subscriber_id = 'agent-a'
	`, eventID).Scan(&deliveryStatus, &deliveryReason, &activeSession); err != nil {
		t.Fatalf("load sqlite serve-abandoned delivery: %v", err)
	}
	if deliveryStatus != "dead_letter" || deliveryReason != runtimerunquiescence.ServeAbandonReasonCode || activeSession.Valid {
		t.Fatalf("sqlite serve delivery = %s/%s active=%v, want dead_letter/%s inactive", deliveryStatus, deliveryReason, activeSession.Valid, runtimerunquiescence.ServeAbandonReasonCode)
	}
	for _, subscriberID := range []string{"agent-a", "pipeline"} {
		var outcome, receiptReason string
		if err := sqliteStore.DB.QueryRowContext(ctx, `
			SELECT outcome, COALESCE(reason_code, '')
			FROM event_receipts
			WHERE event_id = ?
			  AND subscriber_id = ?
		`, eventID, subscriberID).Scan(&outcome, &receiptReason); err != nil {
			t.Fatalf("load sqlite serve receipt %s: %v", subscriberID, err)
		}
		if outcome != "dead_letter" || receiptReason != runtimerunquiescence.ServeAbandonReasonCode {
			t.Fatalf("sqlite serve receipt %s = %s/%s, want dead_letter/%s", subscriberID, outcome, receiptReason, runtimerunquiescence.ServeAbandonReasonCode)
		}
	}
}

func TestRunServeRuntimeBundleHashMissingFailsBeforeReadiness(t *testing.T) {
	_, _, _ = installServeRuntimeEmptyPostgresTestStores(t, func() serveWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	missingHash := "bundle-v1:sha256:2222222222222222222222222222222222222222222222222222222222222222"
	var out lockedBuffer
	code := runServeRuntime(context.Background(), repoRoot(), serveOptions{
		ConfigPath:         writeServeRuntimeTestConfig(t),
		BundleHash:         missingHash,
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "postgres",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: true,
		Verbose:            true,
		Output:             &out,
	})
	if code == 0 {
		t.Fatalf("runServeRuntime code = 0, want startup failure\noutput:\n%s", out.String())
	}
	for _, want := range []string{
		"bundle_hash=" + missingHash,
		"BUNDLE_UNAVAILABLE",
		"bundle_hash " + missingHash + " is not present in bundles",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("serve output missing %q:\n%s", want, out.String())
		}
	}
	if strings.Contains(out.String(), "ready                      ok") || strings.Contains(out.String(), "[22/22]") {
		t.Fatalf("serve reached readiness after missing DB-loaded bundle:\n%s", out.String())
	}
	if strings.Contains(out.String(), "bundle catalog read surface requires bundles columns") || strings.Contains(out.String(), "relation \"bundles\" does not exist") {
		t.Fatalf("serve reported schema/bootstrap failure instead of typed bundle unavailability:\n%s", out.String())
	}
}

func TestValidateServeMultiContextToolGatewayAdmission(t *testing.T) {
	claudeCfg := &config.Config{}
	claudeCfg.LLM.Backend = "claude_cli"
	anthropicCfg := &config.Config{}
	anthropicCfg.LLM.Backend = "anthropic"
	twoContexts := []serveRuntimeBundle{{}, {}}

	tests := []struct {
		name        string
		cfg         *config.Config
		loaded      []serveRuntimeBundle
		wantErr     string
		wantDetails bool
	}{
		{
			name:   "single claude context allowed",
			cfg:    claudeCfg,
			loaded: []serveRuntimeBundle{{}},
		},
		{
			name:   "multi context non claude backend allowed",
			cfg:    anthropicCfg,
			loaded: twoContexts,
		},
		{
			name:        "multi context claude backend fails closed",
			cfg:         claudeCfg,
			loaded:      twoContexts,
			wantErr:     "multi-context swarm serve --bundle-hash with llm.backend=claude_cli is unsupported",
			wantDetails: true,
		},
		{
			name:    "multi context needs config",
			cfg:     nil,
			loaded:  twoContexts,
			wantErr: "runtime config is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateServeMultiContextToolGatewayAdmission(tt.cfg, tt.loaded)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validateServeMultiContextToolGatewayAdmission: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validateServeMultiContextToolGatewayAdmission err = %v, want %q", err, tt.wantErr)
			}
			if tt.wantDetails {
				for _, want := range []string{"ToolGatewayBinding", "MCP /mcp and /tools routes", "forkchat sandbox runtime", "context-aware gateway router"} {
					if !strings.Contains(err.Error(), want) {
						t.Fatalf("admission error missing %q:\n%s", want, err.Error())
					}
				}
			}
		})
	}
}

func TestRunServeRuntimeDuplicateAgentSlugFailsBeforeReadiness(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	_, _, pg := installServeRuntimePostgresTestStores(t, func() serveWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	ctx := context.Background()
	firstRoot := writeServeRuntimeAgentSlugFixtureWithKey(t, "duplicate-agent-slug-a", "alpha", "shared-worker")
	secondRoot := writeServeRuntimeAgentSlugFixtureWithKey(t, "duplicate-agent-slug-b", "beta", "shared-worker")
	firstHash := seedServeRuntimeBundleCatalogRoot(t, ctx, pg, firstRoot)
	secondHash := seedServeRuntimeBundleCatalogRoot(t, ctx, pg, secondRoot)
	if firstHash == secondHash {
		t.Fatalf("test fixtures produced duplicate bundle hash %s", firstHash)
	}

	var out lockedBuffer
	code := runServeRuntime(ctx, repoRoot(), serveOptions{
		ConfigPath:         writeServeRuntimeTestConfig(t),
		BundleHash:         firstHash,
		BundleHashes:       []string{secondHash},
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "postgres",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: false,
		Verbose:            true,
		Output:             &out,
		TestLLMRuntime:     runtimellm.NoopRuntime{},
	})
	if code == 0 {
		t.Fatalf("runServeRuntime code = 0, want startup failure\noutput:\n%s", out.String())
	}
	for _, want := range []string{
		"[5/22] runtime_context",
		`duplicate runtime context agent_id "shared-worker"`,
		firstHash,
		secondHash,
		"bundle_source=persisted",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("serve output missing %q:\n%s", want, out.String())
		}
	}
	for _, notWant := range []string{"[22/22]", "ready                      ok", "manager_event_loop_start", "platform_boot_event_published"} {
		if strings.Contains(out.String(), notWant) {
			t.Fatalf("serve reached %q after duplicate agent slug admission failure:\n%s", notWant, out.String())
		}
	}
}

func TestRunServeRuntimeDistinctAgentSlugsBootPinnedContextsReachReadiness(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	_, _, pg := installServeRuntimePostgresTestStores(t, func() serveWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	ctx := context.Background()
	firstRoot := writeServeRuntimeAgentSlugFixture(t, "distinct-agent-slug-a", "alpha-worker")
	secondRoot := writeServeRuntimeAgentSlugFixture(t, "distinct-agent-slug-b", "beta-worker")
	firstHash := seedServeRuntimeBundleCatalogRoot(t, ctx, pg, firstRoot)
	secondHash := seedServeRuntimeBundleCatalogRoot(t, ctx, pg, secondRoot)
	if firstHash == secondHash {
		t.Fatalf("test fixtures produced duplicate bundle hash %s", firstHash)
	}

	serve := startServeRuntimeTestProcess(t, serveOptions{
		ConfigPath:              writeServeRuntimeTestConfig(t),
		BundleHash:              firstHash,
		BundleHashes:            []string{secondHash},
		PlatformSpecPath:        defaultPlatformSpecPath,
		StoreMode:               "postgres",
		APIListenAddr:           "127.0.0.1:0",
		MCPListenAddr:           "127.0.0.1:0",
		SelfCheck:               true,
		RequireBundleMatch:      false,
		Verbose:                 true,
		TestLLMRuntime:          runtimellm.NoopRuntime{},
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	})
	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("runServeRuntime code = %d\noutput:\n%s", code, serve.outputString())
	}
	for _, want := range []string{firstHash, secondHash, "[22/22] ready"} {
		if !strings.Contains(serve.outputString(), want) {
			t.Fatalf("serve output missing %q:\n%s", want, serve.outputString())
		}
	}
}

func TestRunServeRuntimeMultiContextClaudeCLIFailsClosedBeforePrimaryGatewayOrForkchat(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")

	var workspaceConfigured atomic.Bool
	_, _, pg := installServeRuntimePostgresTestStores(t, func() serveWorkspaceLifecycle {
		workspaceConfigured.Store(true)
		return serveRuntimeWorkspaceStub{}
	})
	ctx := context.Background()
	firstHash := seedServeRuntimeBundleCatalog(t, ctx, pg, filepath.Join("tests", "tier8-boot-verification", "test-boot-success"))
	secondHash := seedServeRuntimeBundleCatalog(t, ctx, pg, filepath.Join("tests", "tier1-primitives", "test-emits-single"))
	if firstHash == secondHash {
		t.Fatalf("test fixtures produced duplicate bundle hash %s", firstHash)
	}

	var out lockedBuffer
	code := runServeRuntime(ctx, repoRoot(), serveOptions{
		ConfigPath:         writeDoctorClaudeConfig(t),
		Backend:            "claude_cli",
		BundleHash:         firstHash,
		BundleHashes:       []string{secondHash},
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "postgres",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: false,
		Verbose:            true,
		Output:             &out,
	})
	if code != 3 {
		t.Fatalf("runServeRuntime code = %d, want 3\noutput:\n%s", code, out.String())
	}
	for _, want := range []string{
		"[4/22] bundle_load",
		"[5/22] runtime_context",
		"multi-context swarm serve --bundle-hash",
		"llm.backend=claude_cli",
		"ToolGatewayBinding",
		"MCP /mcp and /tools routes",
		"forkchat sandbox runtime",
		"context-aware gateway router",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("serve output missing %q:\n%s", want, out.String())
		}
	}
	for _, notWant := range []string{"http_listener_bind", "[20/22]", "[22/22]", "ready                      ok", "init forkchat sandbox runtime"} {
		if strings.Contains(out.String(), notWant) {
			t.Fatalf("serve reached %q after multi-context claude_cli admission failure:\n%s", notWant, out.String())
		}
	}
	if workspaceConfigured.Load() {
		t.Fatalf("workspace lifecycle was configured despite fail-closed admission:\n%s", out.String())
	}
}

func TestRunServeRuntimeUnavailableBundleStartupRecoveryFailsPersistedMissingBeforeCleanup(t *testing.T) {
	_, db, _ := installServeRuntimePostgresTestStores(t, func() serveWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	ctx := context.Background()
	persistedMissingRunID := uuid.NewString()
	legacyRunID := uuid.NewString()
	missingHash := "bundle-v1:sha256:2222222222222222222222222222222222222222222222222222222222222222"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source, started_at)
		VALUES
			($1::uuid, 'running', $2, 'persisted', now()),
			($3::uuid, 'running', NULL, 'legacy', now())
	`, persistedMissingRunID, missingHash, legacyRunID); err != nil {
		t.Fatalf("seed mixed active runs: %v", err)
	}

	var out lockedBuffer
	code := runServeRuntime(context.Background(), repoRoot(), serveOptions{
		ConfigPath:         writeServeRuntimeTestConfig(t),
		ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "postgres",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: false,
		Verbose:            true,
		Output:             &out,
	})
	if code != serveExitDataIntegrity {
		t.Fatalf("runServeRuntime code = %d, want %d\noutput:\n%s", code, serveExitDataIntegrity, out.String())
	}
	assertServeRuntimeRunStillActive(t, ctx, &store.PostgresStore{DB: db}, persistedMissingRunID)
	assertServeRuntimeRunStillActive(t, ctx, &store.PostgresStore{DB: db}, legacyRunID)
	if strings.Contains(out.String(), "ready") {
		t.Fatalf("serve reached readiness despite persisted-missing startup recovery failure:\n%s", out.String())
	}
}

func TestRunServeRuntimeUnavailableBundleStartupRecoveryOrphansExpectedUnavailableRuns(t *testing.T) {
	stoppedContainers := []string{}
	_, db, _ := installServeRuntimePostgresTestStores(t, func() serveWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{
			managedContainers: []runtimedestructivereset.ContainerRef{
				{Name: "swarm-legacy-agent", RunID: serveRuntimeLegacyRunIDForTest, Kind: "agent"},
				{Name: "swarm-unrelated-agent", RunID: uuid.NewString(), Kind: "agent"},
			},
			stoppedContainers: &stoppedContainers,
		}
	})
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (agent_id, role, model, conversation_mode)
		VALUES ('agent-a', 'operator', 'default', 'task')
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	contractsRoot, err := normalizeContractsRoot(resolvePath(repoRoot(), filepath.Join("tests", "tier8-boot-verification", "test-boot-success")))
	if err != nil {
		t.Fatalf("contracts root: %v", err)
	}
	_, bundle, err := newSwarmWorkflowModule(repoRoot(), contractsRoot, resolvePath(repoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("load test workflow bundle: %v", err)
	}
	bootIdentity, err := runtimecontracts.BootBundleIdentity(bundle)
	if err != nil {
		t.Fatalf("boot bundle identity: %v", err)
	}
	persistedRunID := uuid.NewString()
	persistedHash := "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json)
		VALUES ($1, 'name: serve-test', '{}'::jsonb)
	`, persistedHash); err != nil {
		t.Fatalf("seed bundle row: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source, started_at)
		VALUES ($1::uuid, 'running', $2, 'persisted', now())
	`, persistedRunID, persistedHash); err != nil {
		t.Fatalf("seed persisted-present run: %v", err)
	}

	orphanTargets := []struct {
		runID       string
		source      string
		cause       string
		fingerprint string
	}{
		{runID: uuid.NewString(), source: storerunlifecycle.BundleSourceEphemeral, cause: preservationcleanup.BundleEphemeralOrphanedReason},
		{runID: uuid.NewString(), source: storerunlifecycle.BundleSourceDeleted, cause: preservationcleanup.BundleDeletedOrphanedReason},
		{runID: serveRuntimeLegacyRunIDForTest, source: storerunlifecycle.BundleSourceLegacy, cause: preservationcleanup.BundleLegacyOrphanedReason, fingerprint: bootIdentity.Fingerprint},
	}
	for _, target := range orphanTargets {
		seedServeRuntimeUnavailableBundleRunState(t, ctx, db, target.runID, target.source, target.fingerprint)
	}

	serve := startServeRuntimeTestProcess(t, serveOptions{
		ConfigPath:         writeServeRuntimeTestConfig(t),
		ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "postgres",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: true,
		Verbose:            true,
	})

	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("runServeRuntime code = %d\noutput:\n%s", code, serve.outputString())
	}
	assertServeRuntimeRunStillActive(t, context.Background(), &store.PostgresStore{DB: db}, persistedRunID)
	for _, target := range orphanTargets {
		assertServeRuntimeUnavailableBundleRunOrphaned(t, context.Background(), &store.PostgresStore{DB: db}, target.runID, target.cause)
	}
	if len(stoppedContainers) != 1 || stoppedContainers[0] != "swarm-legacy-agent" {
		t.Fatalf("stopped containers = %#v, want only legacy run container", stoppedContainers)
	}
}

const serveRuntimeLegacyRunIDForTest = "11111111-2222-3333-4444-555555555555"

func installServeRuntimePostgresTestStores(t *testing.T, workspaceFactory func() serveWorkspaceLifecycle) (string, *sql.DB, *store.PostgresStore) {
	t.Helper()
	return installServeRuntimePostgresTestStoresWithWorkspaceFactory(t, func(workspaceMountSources) serveWorkspaceLifecycle {
		return workspaceFactory()
	})
}

func installServeRuntimeEmptyPostgresTestStores(t *testing.T, workspaceFactory func() serveWorkspaceLifecycle) (string, *sql.DB, *store.PostgresStore) {
	t.Helper()
	return installServeRuntimePostgresTestStoresForDatabase(t, func(workspaceMountSources) serveWorkspaceLifecycle {
		return workspaceFactory()
	}, false)
}

func seedServeRuntimeSQLiteAbandonWork(t *testing.T, sqlitePath string) (string, string) {
	t.Helper()
	spec, err := loadServePlatformSpecDocument(filepath.Join(repoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("load platform spec: %v", err)
	}
	plans, err := store.GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs: %v", err)
	}
	sqliteStore, err := store.NewSQLiteRuntimeStore(sqlitePath)
	if err != nil {
		t.Fatalf("NewSQLiteRuntimeStore: %v", err)
	}
	if err := sqliteStore.EnsureSchemaTables(context.Background(), plans); err != nil {
		_ = sqliteStore.Close()
		t.Fatalf("EnsureSchemaTables: %v", err)
	}
	ctx := context.Background()
	now := time.Date(2026, 5, 18, 4, 30, 0, 0, time.UTC)
	runID := uuid.NewString()
	eventID := uuid.NewString()
	activeSessionID := uuid.NewString()
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_fingerprint, started_at)
		VALUES (?, 'running', ?, ?)
	`, runID, "sha256:2222222222222222222222222222222222222222222222222222222222222222", now.Add(-time.Hour)); err != nil {
		_ = sqliteStore.Close()
		t.Fatalf("seed sqlite active run: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO events (
			event_id, run_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		) VALUES (
			?, ?, 'serve.abandon.test', 'global', '{}', 'test', 'agent', ?
		)
	`, eventID, runID, now); err != nil {
		_ = sqliteStore.Close()
		t.Fatalf("seed sqlite active delivery event: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, active_session_id, reason_code, created_at
		) VALUES (
			?, ?, ?, 'agent', 'agent-a', 'in_progress', ?, 'agent_processing', ?
		)
	`, uuid.NewString(), runID, eventID, activeSessionID, now); err != nil {
		_ = sqliteStore.Close()
		t.Fatalf("seed sqlite active delivery: %v", err)
	}
	if err := sqliteStore.Close(); err != nil {
		t.Fatalf("close seeded sqlite store: %v", err)
	}
	return runID, eventID
}

func stubServeRuntimeWorkspaceLifecycle(t *testing.T) {
	t.Helper()
	oldWorkspaceLifecycle := configuredWorkspaceLifecycleForServe
	configuredWorkspaceLifecycleForServe = func(*sql.DB, string, semanticview.Source, workspaceMountSources, workspaceBackendSelection) (serveWorkspaceLifecycle, error) {
		return serveRuntimeWorkspaceStub{}, nil
	}
	t.Cleanup(func() {
		configuredWorkspaceLifecycleForServe = oldWorkspaceLifecycle
	})
}

func installServeRuntimePostgresTestStoresWithWorkspaceFactory(t *testing.T, workspaceFactory func(workspaceMountSources) serveWorkspaceLifecycle) (string, *sql.DB, *store.PostgresStore) {
	t.Helper()
	return installServeRuntimePostgresTestStoresForDatabase(t, workspaceFactory, true)
}

func installServeRuntimePostgresTestStoresForDatabase(t *testing.T, workspaceFactory func(workspaceMountSources) serveWorkspaceLifecycle, useTemplate bool) (string, *sql.DB, *store.PostgresStore) {
	t.Helper()
	oldBuildStores := buildStoresForServe
	oldWorkspaceLifecycle := configuredWorkspaceLifecycleForServe
	var dsn string
	var db *sql.DB
	var cleanup func()
	if useTemplate {
		dsn, db, cleanup = testutil.StartPostgres(t)
	} else {
		dsn, db, cleanup = testutil.StartEmptyPostgres(t)
	}
	t.Cleanup(cleanup)
	runtimePG, err := store.NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = runtimePG.DB.Close() })
	buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (storeBundle, error) {
		if _, err := runtimePG.BindSchemaCapabilities(ctx); err != nil {
			return storeBundle{}, err
		}
		return selectedPostgresStoreBundle(runtimePG, cfg), nil
	}
	configuredWorkspaceLifecycleForServe = func(_ *sql.DB, _ string, _ semanticview.Source, mountSources workspaceMountSources, _ workspaceBackendSelection) (serveWorkspaceLifecycle, error) {
		return workspaceFactory(mountSources), nil
	}
	t.Cleanup(func() {
		buildStoresForServe = oldBuildStores
		configuredWorkspaceLifecycleForServe = oldWorkspaceLifecycle
	})
	return dsn, db, runtimePG
}

func assertPostgresTableExists(t *testing.T, db *sql.DB, tableName string) {
	t.Helper()
	var found sql.NullString
	if err := db.QueryRowContext(context.Background(), `SELECT to_regclass($1)::text`, "public."+tableName).Scan(&found); err != nil {
		t.Fatalf("check table %s exists: %v", tableName, err)
	}
	if !found.Valid || strings.TrimSpace(found.String) == "" {
		t.Fatalf("table %s was not bootstrapped", tableName)
	}
}

func seedServeRuntimeUnavailableBundleRunState(t *testing.T, ctx context.Context, db *sql.DB, runID, source, fingerprint string) {
	t.Helper()
	sessionID := uuid.NewString()
	timerID := uuid.NewString()
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_source, bundle_fingerprint, started_at)
		VALUES ($1::uuid, 'running', $2, NULLIF($3, ''), now())
	`, runID, source, fingerprint); err != nil {
		t.Fatalf("seed unavailable bundle run %s: %v", source, err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			event_id, run_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		) VALUES (
			$1::uuid, $2::uuid, $3, 'global', '{}'::jsonb, 'test', 'agent', now()
		)
	`, eventID, runID, "startup."+source+".event"); err != nil {
		t.Fatalf("seed event %s: %v", source, err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, active_session_id, reason_code, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'agent', 'agent-a', 'in_progress', $3::uuid, 'agent_processing', now()
		)
	`, runID, eventID, sessionID); err != nil {
		t.Fatalf("seed delivery %s: %v", source, err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (session_id, run_id, agent_id, scope_key, scope, runtime_mode, status)
		VALUES ($1::uuid, $2::uuid, 'agent-a', $2::text, 'flow', 'session', 'active')
	`, sessionID, runID); err != nil {
		t.Fatalf("seed session %s: %v", source, err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO timers (timer_id, timer_name, run_id, fire_event, fire_at, status)
		VALUES ($1::uuid, $2, $3::uuid, 'timer.fired', now() + interval '1 hour', 'active')
	`, timerID, "timer-"+source, runID); err != nil {
		t.Fatalf("seed timer %s: %v", source, err)
	}
}

func assertServeRuntimeRunStillActive(t *testing.T, ctx context.Context, pg *store.PostgresStore, runID string) {
	t.Helper()
	var status string
	if err := pg.DB.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, runID).Scan(&status); err != nil {
		t.Fatalf("load run %s: %v", runID, err)
	}
	if status != "running" {
		t.Fatalf("run %s status = %s, want running", runID, status)
	}
	var controlRows int
	if err := pg.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_control_state WHERE run_id = $1::uuid`, runID).Scan(&controlRows); err != nil {
		t.Fatalf("count control rows %s: %v", runID, err)
	}
	if controlRows != 0 {
		t.Fatalf("run %s control rows = %d, want none", runID, controlRows)
	}
}

func assertServeRuntimeUnavailableBundleRunOrphaned(t *testing.T, ctx context.Context, pg *store.PostgresStore, runID, reason string) {
	t.Helper()
	var runStatus, errorSummary, controlStatus, controlReason string
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT r.status, COALESCE(r.error_summary, ''), rc.control_status, COALESCE(rc.reason, '')
		FROM runs r
		JOIN run_control_state rc ON rc.run_id = r.run_id
		WHERE r.run_id = $1::uuid
	`, runID).Scan(&runStatus, &errorSummary, &controlStatus, &controlReason); err != nil {
		t.Fatalf("load orphaned run %s: %v", runID, err)
	}
	if runStatus != "cancelled" || errorSummary != reason || controlStatus != "stopped" || controlReason != reason {
		t.Fatalf("orphaned run %s = %s/%s/%s/%s, want cancelled/%s/stopped/%s", runID, runStatus, errorSummary, controlStatus, controlReason, reason, reason)
	}
	var deadLetters int
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND status = 'dead_letter'
		  AND reason_code = $2
		  AND active_session_id IS NULL
	`, runID, reason).Scan(&deadLetters); err != nil {
		t.Fatalf("count orphaned deliveries %s: %v", runID, err)
	}
	if deadLetters != 1 {
		t.Fatalf("orphaned run %s dead-letter deliveries = %d, want 1", runID, deadLetters)
	}
	var receipts int
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts er
		JOIN events e ON e.event_id = er.event_id
		WHERE e.run_id = $1::uuid
		  AND er.outcome = 'dead_letter'
		  AND er.reason_code = $2
		  AND er.subscriber_id IN ('agent-a', 'pipeline')
	`, runID, reason).Scan(&receipts); err != nil {
		t.Fatalf("count orphaned receipts %s: %v", runID, err)
	}
	if receipts != 2 {
		t.Fatalf("orphaned run %s receipts = %d, want agent and pipeline", runID, receipts)
	}
	var sessions int
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM agent_sessions
		WHERE run_id = $1::uuid
		  AND status = 'terminated'
		  AND termination_reason = 'orphaned'
		  AND termination_detail = $2
	`, runID, reason).Scan(&sessions); err != nil {
		t.Fatalf("count orphaned sessions %s: %v", runID, err)
	}
	if sessions != 1 {
		t.Fatalf("orphaned run %s sessions = %d, want 1", runID, sessions)
	}
	var timers int
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM timers
		WHERE run_id = $1::uuid
		  AND status = 'cancelled'
	`, runID).Scan(&timers); err != nil {
		t.Fatalf("count orphaned timers %s: %v", runID, err)
	}
	if timers != 1 {
		t.Fatalf("orphaned run %s timers = %d, want 1", runID, timers)
	}
}

func TestPrepareServeBundleSourcePersistsCatalogForContractsServe(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	bundle := loadWorkflowValidationFixtureBundle(t, "tests/tier12-runtime-tools/test-flow-data-access")
	identity, err := runtimecontracts.BootBundleIdentity(bundle)
	if err != nil {
		t.Fatalf("BootBundleIdentity: %v", err)
	}

	fact, err := prepareServeBundleSource(ctx, selectedPostgresStoreBundle(pg, &config.Config{}), bundle, identity.Fingerprint, false)
	if err != nil {
		t.Fatalf("prepareServeBundleSource: %v", err)
	}
	if fact.BundleSource != storerunlifecycle.BundleSourcePersisted || fact.BundleHash == "" || fact.BundleFingerprint != identity.Fingerprint {
		t.Fatalf("source fact = %#v", fact)
	}
	detail, err := pg.LoadBundleCatalog(ctx, fact.BundleHash)
	if err != nil {
		t.Fatalf("LoadBundleCatalog(%s): %v", fact.BundleHash, err)
	}
	if detail.BundleHash != fact.BundleHash || detail.AgentCount == 0 || detail.ContentYAML == "" {
		t.Fatalf("bundle catalog detail = %#v", detail)
	}
}

func TestPrepareServeBundleSourceDevStampsEphemeralWithoutCatalogRow(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	bundle := loadWorkflowValidationFixtureBundle(t, "tests/tier12-runtime-tools/test-flow-data-access")
	identity, err := runtimecontracts.BootBundleIdentity(bundle)
	if err != nil {
		t.Fatalf("BootBundleIdentity: %v", err)
	}

	fact, err := prepareServeBundleSource(ctx, selectedPostgresStoreBundle(pg, &config.Config{}), bundle, identity.Fingerprint, true)
	if err != nil {
		t.Fatalf("prepareServeBundleSource(dev): %v", err)
	}
	if fact.BundleSource != storerunlifecycle.BundleSourceEphemeral || fact.BundleHash == "" || fact.BundleFingerprint != identity.Fingerprint {
		t.Fatalf("source fact = %#v", fact)
	}
	if _, err := pg.LoadBundleCatalog(ctx, fact.BundleHash); err != store.ErrBundleNotFound {
		t.Fatalf("LoadBundleCatalog(dev hash) error = %v, want ErrBundleNotFound", err)
	}
}

func TestPrepareServeBundleSourceSQLiteStampsEphemeralWithoutPostgresCatalog(t *testing.T) {
	ctx := context.Background()
	bundle := loadWorkflowValidationFixtureBundle(t, "tests/tier1-primitives/test-emits-single")
	identity, err := runtimecontracts.BootBundleIdentity(bundle)
	if err != nil {
		t.Fatalf("BootBundleIdentity: %v", err)
	}

	fact, err := prepareServeBundleSource(ctx, storeBundle{}, bundle, identity.Fingerprint, false)
	if err != nil {
		t.Fatalf("prepareServeBundleSource(sqlite local): %v", err)
	}
	if fact.BundleSource != storerunlifecycle.BundleSourceEphemeral || fact.BundleHash == "" || fact.BundleFingerprint != identity.Fingerprint {
		t.Fatalf("source fact = %#v", fact)
	}
}

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
	if code != 1 {
		t.Fatalf("verify code = %d, want 1 stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if strings.TrimSpace(stdout.String()) != "" {
		t.Fatalf("verify stdout = %q, want empty on error", stdout.String())
	}
	if !strings.Contains(stderr.String(), "verify failed: resolve contracts") {
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

func TestServeListenerServersPartitionAPIAndMCPRoutes(t *testing.T) {
	var ready atomic.Bool
	var apiHit atomic.Bool
	apiHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiHit.Store(true)
		if r.URL.Path != "/v1/rpc" && r.URL.Path != "/v1/ws" {
			t.Errorf("api path = %q, want /v1/rpc or /v1/ws", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":"test","result":{"ok":true}}`))
	})
	var inboundHit atomic.Bool
	inboundHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inboundHit.Store(true)
		if r.URL.Path != "/webhooks/customer-a/github" {
			t.Errorf("inbound path = %q, want /webhooks/customer-a/github", r.URL.Path)
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"accepted"}`))
	})
	toolGateway := runtimemcp.NewGateway(nil, "", runtimemcp.GatewayHooks{})
	apiHandlerMux := newAPIServer(&ready, apiHandler, inboundHandler).Handler
	mcpHandlerMux := newMCPServer(toolGateway).Handler

	rec := httptest.NewRecorder()
	apiHandlerMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != "ok" {
		t.Fatalf("/healthz status/body = %d/%q, want 200 ok", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	apiHandlerMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz before ready status = %d, want 503", rec.Code)
	}
	ready.Store(true)
	rec = httptest.NewRecorder()
	apiHandlerMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != "ready" {
		t.Fatalf("/readyz ready status/body = %d/%q, want 200 ready", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	apiHandlerMux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"test","method":"rpc.unsubscribe","params":{"subscription_id":"sub"}}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("/v1/rpc status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}
	if !apiHit.Load() {
		t.Fatal("/v1/rpc did not route to v1 API handler")
	}

	rec = httptest.NewRecorder()
	apiHandlerMux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/ws", strings.NewReader(`{"jsonrpc":"2.0","id":"test","method":"rpc.unsubscribe","params":{"subscription_id":"sub"}}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("/v1/ws status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	apiHandlerMux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/github", strings.NewReader(`{"zen":"ok"}`)))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("/webhooks/ status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if !inboundHit.Load() {
		t.Fatal("/webhooks/ did not route to inbound handler")
	}

	rec = httptest.NewRecorder()
	apiHandlerMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/mcp", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("api listener /mcp status = %d, want 404", rec.Code)
	}
	rec = httptest.NewRecorder()
	apiHandlerMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/tools/query_entities", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("api listener /tools/ status = %d, want 404", rec.Code)
	}

	rec = httptest.NewRecorder()
	mcpHandlerMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/mcp", nil))
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != "ok" {
		t.Fatalf("/mcp status/body = %d/%q, want 200 ok", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	mcpHandlerMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/tools/query_entities", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("/tools/ route status = %d, want mounted gateway 405", rec.Code)
	}

	for _, path := range []string{"/healthz", "/readyz", "/v1/rpc", "/v1/ws", "/webhooks/customer-a/github"} {
		rec = httptest.NewRecorder()
		mcpHandlerMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("mcp listener %s status = %d, want 404", path, rec.Code)
		}
	}

	for _, path := range []string{"/api", "/api/healthz", "/api/rpc", "/rpc", "/api/ws", "/ws"} {
		rec = httptest.NewRecorder()
		apiHandlerMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s status = %d, want 404", path, rec.Code)
		}
	}
}

func TestPrintRunStatusReport(t *testing.T) {
	var buf bytes.Buffer
	started := time.Unix(1700000000, 0).UTC()
	last := started.Add(5 * time.Minute)
	report := runStatusReport{
		RunID:             "run-123",
		RunTableStatus:    "running",
		OperationalState:  "stalled",
		BlockingLayer:     "scoring_terminal_outcome",
		BlockingReason:    "terminal_scoring_outcome_missing",
		RootEventID:       "evt-1",
		RootEventType:     "scan.requested",
		StartedAt:         started,
		LastEventAt:       last,
		EventCount:        7,
		WarnErrorLogCount: 1,
		Heuristics: []string{
			"run appears settled after scoring started but no terminal scoring outcome was emitted",
		},
		EventCounts: []runStatusEventCount{
			{EventName: "scan.requested", Count: 1},
			{EventName: "vertical.discovered", Count: 2},
		},
		Deliveries: []runStatusDeliveryCount{
			{SubscriberID: "analysis-agent", Status: "delivered", Count: 2},
		},
		AgentTurns: []runStatusAgentTurn{
			{AgentID: "analysis-agent", Turns: 2, ErrorCount: 0, LastAt: last},
		},
		RuntimeLogSummary: []runStatusRuntimeSummary{
			{Level: "warn", Component: "mcp-gateway", Action: "mcp.context.fallback_used", Count: 3},
		},
		RuntimeLogs: []runStatusRuntimeLog{
			{Level: "warn", Component: "mcp-gateway", Action: "mcp.context.fallback_used", Error: "missing or invalid mcp context token", CreatedAt: last},
		},
		RecentEvents: []runStatusEvent{
			{EventName: "vertical.discovered", EntityID: "ent-1", CreatedAt: last},
		},
		Mutations: []runStatusMutation{
			{Field: "current_state", EntityID: "ent-1", WriterType: "workflow", WriterID: "router", HandlerStep: "step-1", CreatedAt: last},
		},
	}

	printRunStatusReport(&buf, report)
	out := buf.String()
	for _, want := range []string{
		"Run run-123",
		"Root: scan.requested (evt-1)",
		"Run Table Status: running",
		"Operational State: stalled",
		"Blocking Layer: scoring_terminal_outcome",
		"Blocking Reason: terminal_scoring_outcome_missing",
		"Summary: events=7 deliveries=1 dead_letters=0 agent_turns=1 runtime_warn_errors=1",
		"Heuristics:",
		"run appears settled after scoring started but no terminal scoring outcome was emitted",
		"Event Counts:",
		"analysis-agent  status=delivered  count=2",
		"Runtime Log Summary:",
		"WARN  mcp-gateway/mcp.context.fallback_used  count=3",
		"Runtime Warnings/Errors:",
		"WARN  mcp-gateway/mcp.context.fallback_used",
		"Recent Events:",
		"vertical.discovered  entity=ent-1",
		"Recent Mutations:",
		"current_state  entity=ent-1  writer=workflow/router  step=step-1",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("status output missing %q:\n%s", want, out)
		}
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

func TestLoadRunStatusReport_UsesCanonicalPersistedRunIDForRuntimeLogsAndMutations(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	targetRunID := uuid.NewString()
	otherRunID := uuid.NewString()
	targetEntityID := uuid.NewString()
	otherEntityID := uuid.NewString()
	targetEventID := uuid.NewString()
	otherEventID := uuid.NewString()
	now := time.Unix(1700000000, 0).UTC()

	insertRuntimeLog := func(runID string, payloadRunID string, component string, action string, createdAt time.Time) {
		t.Helper()
		payload, err := json.Marshal(map[string]any{
			"log_level": "warn",
			"message":   action,
			"details": map[string]any{
				"run_id":    payloadRunID,
				"component": component,
				"action":    action,
				"error":     action + "-error",
			},
		})
		if err != nil {
			t.Fatalf("marshal runtime log payload: %v", err)
		}
		if _, err := db.Exec(`
			INSERT INTO events (
				run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
			)
			VALUES (
				$1::uuid, gen_random_uuid(), 'platform.runtime_log', NULL, NULL, 'global', $2::jsonb, 'test', 'agent', $3
			)
		`, runID, string(payload), createdAt); err != nil {
			t.Fatalf("insert runtime log for run %s: %v", runID, err)
		}
	}

	for _, runID := range []string{targetRunID, otherRunID} {
		if _, err := db.Exec(`
			INSERT INTO runs (run_id, status, started_at)
			VALUES ($1::uuid, 'running', $2)
			ON CONFLICT (run_id) DO NOTHING
		`, runID, now.Add(-5*time.Minute)); err != nil {
			t.Fatalf("insert run %s: %v", runID, err)
		}
	}
	if _, err := db.Exec(`
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'scan.requested', $3::uuid, NULL, 'global', '{}'::jsonb, 'test', 'agent', $4),
			($5::uuid, $6::uuid, 'scan.requested', $7::uuid, NULL, 'global', '{}'::jsonb, 'test', 'agent', $8)
	`, targetRunID, targetEventID, targetEntityID, now.Add(-4*time.Minute), otherRunID, otherEventID, otherEntityID, now.Add(-3*time.Minute)); err != nil {
		t.Fatalf("insert root events: %v", err)
	}
	insertRuntimeLog(targetRunID, otherRunID, "scheduler", "canonical-owner", now)
	insertRuntimeLog(otherRunID, targetRunID, "scheduler", "payload-only", now.Add(1*time.Minute))
	if _, err := db.Exec(`
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', $4::jsonb, $5::jsonb, $3::uuid, 'platform', 'runner', 'step-a', $6),
			($7::uuid, $8::uuid, 'current_state', $10::jsonb, $11::jsonb, $9::uuid, 'platform', 'runner', 'step-b', $12)
	`, targetRunID, targetEntityID, targetEventID, `"queued"`, `"running"`, now.Add(2*time.Minute), otherRunID, otherEntityID, otherEventID, `"queued"`, `"failed"`, now.Add(3*time.Minute)); err != nil {
		t.Fatalf("insert mutations: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, delivered_at, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent', 'agent-1', 'delivered', $3, $4)
	`, targetRunID, targetEventID, now.Add(10*time.Second), now.Add(5*time.Second)); err != nil {
		t.Fatalf("insert delivery: %v", err)
	}
	if err := runtimedeadletters.Insert(context.Background(), db, runtimedeadletters.Record{
		OriginalEventID: targetEventID,
		OriginalEvent:   "scan.requested",
		EntityID:        targetEntityID,
		FailureType:     "handler_error",
		ErrorMessage:    "boom",
		HandlerNode:     "node-a",
		Timestamp:       now.Add(4 * time.Minute).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("insert dead letter: %v", err)
	}

	report, err := loadRunStatusReport(context.Background(), pg, targetRunID, runStatusOptions{
		LogsOnly:  true,
		Component: "scheduler",
	})
	if err != nil {
		t.Fatalf("loadRunStatusReport: %v", err)
	}
	if report.WarnErrorLogCount != 1 {
		t.Fatalf("WarnErrorLogCount = %d, want 1", report.WarnErrorLogCount)
	}
	if len(report.RuntimeLogSummary) != 1 {
		t.Fatalf("RuntimeLogSummary len = %d, want 1", len(report.RuntimeLogSummary))
	}
	if got := report.RuntimeLogSummary[0]; got.Component != "scheduler" || got.Action != "canonical-owner" || got.Count != 1 {
		t.Fatalf("RuntimeLogSummary[0] = %#v", got)
	}
	if len(report.RuntimeLogs) != 1 {
		t.Fatalf("RuntimeLogs len = %d, want 1", len(report.RuntimeLogs))
	}
	if got := report.RuntimeLogs[0]; got.Component != "scheduler" || got.Action != "canonical-owner" || got.Error != "canonical-owner-error" {
		t.Fatalf("RuntimeLogs[0] = %#v", got)
	}
	if len(report.Mutations) != 1 {
		t.Fatalf("Mutations len = %d, want 1", len(report.Mutations))
	}
	if got := report.Mutations[0]; got.EntityID != targetEntityID || got.Field != "current_state" || got.WriterType != "platform" || got.WriterID != "runner" {
		t.Fatalf("Mutations[0] = %#v", got)
	}
	if len(report.DeadLetters) != 1 {
		t.Fatalf("DeadLetters len = %d, want 1", len(report.DeadLetters))
	}
}

func TestRunForkRuntimeOwnerHarness_DryRunUsesCanonicalPlannerJSON(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	runID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000300, 0).UTC()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'fork.cli', 'global', '{}'::jsonb, 'test', 'platform', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	var buf bytes.Buffer
	code := runForkRuntimeOwnerHarness(ctx, t.TempDir(), []string{
		"--store", "postgres",
		"--dry-run",
		"--run", runID,
		"--at", eventID,
		"--json",
	}, &buf)
	if code != 0 {
		t.Fatalf("runForkRuntimeOwnerHarness code=%d output=%s", code, buf.String())
	}
	var plan store.RunForkPlan
	if err := json.Unmarshal(buf.Bytes(), &plan); err != nil {
		t.Fatalf("decode fork plan json: %v\n%s", err, buf.String())
	}
	if plan.SourceRunID != runID {
		t.Fatalf("SourceRunID = %q, want %q", plan.SourceRunID, runID)
	}
	if plan.ForkPoint.EventID != eventID {
		t.Fatalf("ForkPoint.EventID = %q, want %q", plan.ForkPoint.EventID, eventID)
	}
	if plan.PendingWorkCount != 0 || len(plan.PendingWork) != 0 {
		t.Fatalf("pending work = %#v, want none", plan.PendingWork)
	}
	if !plan.ExecutionReady {
		t.Fatalf("ExecutionReady = false, want true for state-only dry-run; blockers=%#v", plan.UnsupportedBlockers)
	}
	if plan.UnsupportedBlockerCount != 0 {
		t.Fatalf("UnsupportedBlockerCount = %d, want 0; blockers=%#v", plan.UnsupportedBlockerCount, plan.UnsupportedBlockers)
	}
	if plan.ReplayResumeAdmission.Owner != store.RunForkReplayResumeAdmissionOwner || !plan.ReplayResumeAdmission.StateOnlyExecutionReady {
		t.Fatalf("taxonomy = %#v, want canonical owner and state-only ready", plan.ReplayResumeAdmission)
	}
}

func TestRunForkRuntimeOwnerHarness_DryRunJSONReportsDeliveryEventReplayReady(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	runID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000305, 0).UTC()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'fork.cli.pending', 'global', '{}'::jsonb, 'test', 'platform', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent', 'cli-agent', 'pending', 0, 'matched_agent_subscription', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed pending delivery: %v", err)
	}

	var buf bytes.Buffer
	code := runForkRuntimeOwnerHarness(ctx, t.TempDir(), []string{
		"--store", "postgres",
		"--dry-run",
		"--run", runID,
		"--at", eventID,
		"--json",
	}, &buf)
	if code != 0 {
		t.Fatalf("runForkRuntimeOwnerHarness code=%d output=%s", code, buf.String())
	}
	var plan store.RunForkPlan
	if err := json.Unmarshal(buf.Bytes(), &plan); err != nil {
		t.Fatalf("decode fork plan json: %v\n%s", err, buf.String())
	}
	if !plan.ExecutionReady || !plan.ReplayResumeAdmission.DeliveryEventReplayReady || !plan.ReplayResumeAdmission.HistoricalReplaySupported {
		t.Fatalf("dry-run replay admission = execution:%v admission:%#v", plan.ExecutionReady, plan.ReplayResumeAdmission)
	}
	if plan.ReplayResumeAdmission.StateOnlyExecutionReady {
		t.Fatalf("StateOnlyExecutionReady = true, want false for delivery/event replay dry-run")
	}
}

func TestRunForkRuntimeOwnerHarness_DryRunContractsAddsContractFrontierAdmissionJSON(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	runID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000307, 0).UTC()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'flow-a/work.begin', 'global', '{}'::jsonb, 'test', 'platform', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'node', 'source-node', 'pending', 0, 'matched_node_subscription', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed pending node delivery: %v", err)
	}

	repo := repoRoot()
	var buf bytes.Buffer
	code := runForkRuntimeOwnerHarness(ctx, repo, []string{
		"--store", "postgres",
		"--dry-run",
		"--run", runID,
		"--at", eventID,
		"--contracts", filepath.Join(repo, "tests", "tier11-flow-composition", "test-sibling-both-instantiated-isolated"),
		"--json",
	}, &buf)
	if code != 0 {
		t.Fatalf("runForkRuntimeOwnerHarness code=%d output=%s", code, buf.String())
	}
	var plan store.RunForkPlan
	if err := json.Unmarshal(buf.Bytes(), &plan); err != nil {
		t.Fatalf("decode fork plan json: %v\n%s", err, buf.String())
	}
	if plan.ContractFrontierAdmission == nil {
		t.Fatalf("ContractFrontierAdmission = nil; output=%s", buf.String())
	}
	admission := plan.ContractFrontierAdmission
	if admission.Owner != store.RunForkContractFrontierAdmissionOwner || !admission.NonMutating || admission.HistoricalExecutionSupported {
		t.Fatalf("contract frontier admission = %#v", admission)
	}
	if admission.FrontierEventCount != 1 || len(admission.FrontierEvents) != 1 {
		t.Fatalf("frontier events = %#v", admission.FrontierEvents)
	}
	if !runForkPlanHasString(admission.FrontierEvents[0].RuntimeEventOwners, "alpha-intake") {
		t.Fatalf("runtime event owners = %#v, want alpha-intake from selected contract", admission.FrontierEvents[0].RuntimeEventOwners)
	}
	if !runForkPlanHasBlocker(admission.UnsupportedBlockers, store.RunForkBlockerContractFrontierExecutionUnsupported) {
		t.Fatalf("contract frontier blockers = %#v, want execution unsupported", admission.UnsupportedBlockers)
	}
	model := plan.SelectedContractExecution
	if model == nil {
		t.Fatalf("SelectedContractExecution = nil; output=%s", buf.String())
	}
	if model.Owner != store.RunForkSelectedContractExecutionModelOwner ||
		model.FutureExecutionOwner != store.RunForkSelectedContractExecutionOwner ||
		!model.NonMutating ||
		model.ExecutionSupported {
		t.Fatalf("selected-contract execution model = %#v", model)
	}
	if model.AdmissionOwner != store.RunForkContractFrontierAdmissionOwner ||
		model.AdmissionUse != store.RunForkSelectedContractExecutionAdmissionUseEvidenceOnly {
		t.Fatalf("selected-contract execution admission use = %#v", model)
	}
	if model.RouteTopology == nil ||
		model.RouteTopology.Owner != store.RunForkSelectedContractRouteTopologyOwner ||
		model.RouteTopology.RouteAdmissionOwner != store.RunForkSelectedContractRouteAdmissionOwner ||
		!model.RouteTopology.NonMutating ||
		model.RouteTopology.RoutePersistenceSupported ||
		model.RouteTopology.ExecutableRecipientsSupported {
		t.Fatalf("selected-contract route topology = %#v", model.RouteTopology)
	}
	if !runForkPlanHasBoundary(model.RouteTopology.InvalidPaths, "copy_source_routing_rules", store.RunForkSelectedContractDispositionInvalid) {
		t.Fatalf("selected-contract route invalid paths = %#v", model.RouteTopology.InvalidPaths)
	}
	if model.RecipientPlanning == nil ||
		model.RecipientPlanning.Owner != store.RunForkSelectedContractRecipientPlanningOwner ||
		!model.RecipientPlanning.NonMutating ||
		!model.RecipientPlanning.RecipientPlanningSupported ||
		model.RecipientPlanning.DeliveryWritesSupported {
		t.Fatalf("selected-contract recipient planning = %#v", model.RecipientPlanning)
	}
	if !runForkPlanHasBoundary(model.RecipientPlanning.RequiredConsumers, "eventbus_publish_recipient_guard", store.RunForkSelectedContractDispositionPrerequisite) {
		t.Fatalf("selected-contract recipient planning consumers = %#v", model.RecipientPlanning.RequiredConsumers)
	}
	if !runForkPlanHasBlocker(model.UnsupportedBlockers, store.RunForkBlockerSelectedContractRouteTopologyNonMutating) {
		t.Fatalf("selected-contract execution blockers = %#v, want route topology non-mutating blocker", model.UnsupportedBlockers)
	}
	if !runForkPlanHasBlocker(model.UnsupportedBlockers, store.RunForkBlockerSelectedContractRouteAdmissionNonMutating) {
		t.Fatalf("selected-contract execution blockers = %#v, want route admission non-mutating blocker", model.UnsupportedBlockers)
	}
	if !runForkPlanHasBoundary(model.InvalidPaths, "copy_source_event_deliveries", store.RunForkSelectedContractDispositionInvalid) {
		t.Fatalf("selected-contract execution invalid paths = %#v", model.InvalidPaths)
	}
	if !runForkPlanHasBoundary(model.RequiredConsumers, "fork_local_runtime_container", store.RunForkSelectedContractDispositionPrerequisite) ||
		!runForkPlanHasBoundary(model.RequiredConsumers, "fork_run_id_runtime_context", store.RunForkSelectedContractDispositionPrerequisite) ||
		!runForkPlanHasBoundary(model.RequiredConsumers, "fork_local_event_delivery_writes", store.RunForkSelectedContractDispositionPrerequisite) ||
		!runForkPlanHasBoundary(model.RequiredConsumers, "handler_execution", store.RunForkSelectedContractDispositionPrerequisite) ||
		!runForkPlanHasBoundary(model.RequiredConsumers, "emitted_follow_up_events", store.RunForkSelectedContractDispositionPrerequisite) {
		t.Fatalf("selected-contract execution consumers = %#v", model.RequiredConsumers)
	}
	if !runForkPlanHasBlocker(model.UnsupportedBlockers, store.RunForkBlockerSelectedContractExecutionModelNonMutating) {
		t.Fatalf("selected-contract execution blockers = %#v", model.UnsupportedBlockers)
	}
	readiness := plan.SelectedContractReadiness
	if readiness == nil {
		t.Fatalf("SelectedContractReadiness = nil; output=%s", buf.String())
	}
	if readiness.Owner != store.RunForkSelectedContractReadinessClassifierOwner ||
		!readiness.NonMutating ||
		readiness.PlannerOwner != store.RunForkPlanningOwner ||
		readiness.ReplayResumeAdmissionOwner != store.RunForkReplayResumeAdmissionOwner ||
		readiness.ContractFrontierAdmissionOwner != store.RunForkContractFrontierAdmissionOwner ||
		readiness.RouteTopologyOwner != store.RunForkSelectedContractRouteTopologyOwner ||
		readiness.RecipientPlanningOwner != store.RunForkSelectedContractRecipientPlanningOwner ||
		readiness.SelectedExecutionModelOwner != store.RunForkSelectedContractExecutionModelOwner ||
		readiness.FutureExecutionOwner != store.RunForkSelectedContractExecutionOwner {
		t.Fatalf("selected-contract readiness = %#v", readiness)
	}
	if len(readiness.FactMatrix) != 20 {
		t.Fatalf("readiness facts = %d, want complete matrix; facts=%#v", len(readiness.FactMatrix), readiness.FactMatrix)
	}
	for _, fact := range []string{
		store.RunForkSelectedContractReadinessFactSourceEvents,
		store.RunForkSelectedContractReadinessFactForkEvents,
		store.RunForkSelectedContractReadinessFactSourceDeliveries,
		store.RunForkSelectedContractReadinessFactForkDeliveries,
		store.RunForkSelectedContractReadinessFactSelectedRecipientsRoutes,
		store.RunForkSelectedContractReadinessFactTimers,
		store.RunForkSelectedContractReadinessFactSessions,
		store.RunForkSelectedContractReadinessFactTurns,
		store.RunForkSelectedContractReadinessFactAudits,
		store.RunForkSelectedContractReadinessFactCommittedReplayScopeMarkers,
		store.RunForkSelectedContractReadinessFactPlatformRuntimeDiagnostics,
		store.RunForkSelectedContractReadinessFactReceipts,
		store.RunForkSelectedContractReadinessFactDeadLetters,
		store.RunForkSelectedContractReadinessFactRetryIdempotency,
		store.RunForkSelectedContractReadinessFactEmittedFollowUps,
		store.RunForkSelectedContractReadinessFactSourcePostTFacts,
		store.RunForkSelectedContractReadinessFactCurrentStateSnapshots,
		store.RunForkSelectedContractReadinessFactNonAgentNodeSystemWork,
		store.RunForkSelectedContractReadinessFactRestartRecovery,
		store.RunForkSelectedContractReadinessFactOperatorConsumers,
	} {
		if !runForkReadinessFactHas(readiness.FactMatrix, fact) {
			t.Fatalf("readiness fact %q missing from %#v", fact, readiness.FactMatrix)
		}
	}
	if !runForkReadinessFactHasDisposition(readiness.FactMatrix, store.RunForkSelectedContractReadinessFactSourceDeliveries, store.RunForkSelectedContractReadinessDispositionFailClosedBlocker) {
		t.Fatalf("source delivery readiness = %#v, want fail-closed blocker for source node delivery", readiness.FactMatrix)
	}
	if !runForkReadinessFactHasDisposition(readiness.FactMatrix, store.RunForkSelectedContractReadinessFactSelectedRecipientsRoutes, store.RunForkSelectedContractReadinessDispositionReconstructedForkState) {
		t.Fatalf("route/recipient readiness = %#v, want reconstructed fork-local evidence", readiness.FactMatrix)
	}
}

func TestRunForkRuntimeOwnerHarness_ActivateWithContractsReachesSelectedActivationGate(t *testing.T) {
	var buf bytes.Buffer
	code := runForkRuntimeOwnerHarness(context.Background(), t.TempDir(), []string{
		"--activate",
		"--contracts", t.TempDir(),
		"--run", uuid.NewString(),
	}, &buf)
	if code != 1 {
		t.Fatalf("runForkRuntimeOwnerHarness code=%d, want runtime failure after parsing; output=%s", code, buf.String())
	}
	if strings.Contains(buf.String(), "--contracts is only supported") {
		t.Fatalf("output = %q, want --activate to consume canonical selected activation gate rather than parse-level contract rejection", buf.String())
	}
}

func TestRunForkRuntimeOwnerHarness_SelectedContractsBorrowedRequireExplicitData(t *testing.T) {
	dsn, _, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	repo := repoRoot()
	borrowedRoot := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(borrowedRoot, "package.yaml"), `
name: borrowed-selected-contracts
version: 1.0.0
flows: []
`)

	var buf bytes.Buffer
	code := runForkRuntimeOwnerHarness(context.Background(), repo, []string{
		"--store", "postgres",
		"--contracts", borrowedRoot,
		"--run", uuid.NewString(),
		"--at", uuid.NewString(),
	}, &buf)
	if code != 1 {
		t.Fatalf("runForkRuntimeOwnerHarness code=%d, want missing data-source failure; output=%s", code, buf.String())
	}
	if !strings.Contains(buf.String(), "resolve workspace data source") ||
		!strings.Contains(buf.String(), "workspace data source is required") {
		t.Fatalf("output = %q, want borrowed selected contracts to require explicit workspace data", buf.String())
	}
	if _, err := os.Stat(filepath.Join(borrowedRoot, defaultWorkspaceDataSourceRelativePath)); !os.IsNotExist(err) {
		t.Fatalf("borrowed contracts data stat error = %v, want no default data source created", err)
	}
}

func TestRunForkRuntimeOwnerHarness_SelectedContractsExecuteThroughCanonicalOwnerJSON(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	repo := repoRoot()
	contractsRoot := filepath.Join(repo, "tests/tier1-primitives/test-emits-multiple")
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	diagnosticEventID := uuid.NewString()
	at := time.Unix(1700000312, 0).UTC()
	seedRunForkCLISelectedExecutionSource(t, db, sourceRunID, entityID, sourceEventID, at)
	seedRunForkCLISelectedExecutionDiagnosticPlatformDeadLetter(t, db, sourceRunID, diagnosticEventID, at.Add(-time.Second))

	var buf bytes.Buffer
	code := runForkRuntimeOwnerHarness(context.Background(), repo, []string{
		"--store", "postgres",
		"--contracts", contractsRoot,
		"--run", sourceRunID,
		"--at", sourceEventID,
		"--json",
	}, &buf)
	if code != 0 {
		t.Fatalf("runForkRuntimeOwnerHarness code=%d output=%s", code, buf.String())
	}
	var result runtimerunforkexecution.SelectedContractExecutionResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("decode selected execution json: %v\n%s", err, buf.String())
	}
	if result.Owner != store.RunForkSelectedContractExecutionOwner || result.ExecutedEventCount != 1 || len(result.ForkEvents) != 1 {
		t.Fatalf("selected execution result = %#v", result)
	}
	if result.SelectedContractExecutionAdmission == nil ||
		result.SelectedContractExecutionAdmission.RecipientPlanning == nil ||
		result.SelectedContractExecutionAdmission.RecipientPlanning.Owner != store.RunForkSelectedContractRecipientPlanningOwner {
		t.Fatalf("selected execution recipient planning admission = %#v", result.SelectedContractExecutionAdmission)
	}
	var lineageRows int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM run_fork_selected_contract_executions
		WHERE fork_run_id = $1::uuid
		  AND source_event_id = $2::uuid
		  AND fork_event_id = $3::uuid
	`, result.Materialization.ForkRunID, sourceEventID, result.ForkEvents[0].ForkEventID).Scan(&lineageRows); err != nil {
		t.Fatalf("count selected execution lineage: %v", err)
	}
	if lineageRows != 1 {
		t.Fatalf("selected execution lineage rows = %d, want 1", lineageRows)
	}
	var diagnosticCopies int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND (
			event_id = $2::uuid
			OR COALESCE(source_event_id::text, '') = $2::text
		  )
	`, result.Materialization.ForkRunID, diagnosticEventID).Scan(&diagnosticCopies); err != nil {
		t.Fatalf("count copied diagnostic platform events: %v", err)
	}
	if diagnosticCopies != 0 {
		t.Fatalf("diagnostic platform events copied into fork = %d, want 0", diagnosticCopies)
	}
	var typedRuntimeDiagnostics int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND event_name = 'platform.runtime_log'
		  AND source_event_id = $2::uuid
		  AND payload->'details'->>'runtime_lineage_owner' = $3
	`, result.Materialization.ForkRunID, result.ForkEvents[0].ForkEventID, store.RunForkSelectedContractForkLocalRuntimeTypedLineageOwner).Scan(&typedRuntimeDiagnostics); err != nil {
		t.Fatalf("count typed fork-local runtime diagnostics: %v", err)
	}
	if typedRuntimeDiagnostics == 0 {
		t.Fatalf("typed fork-local runtime diagnostics = 0, want selected execution runtime logs parented to fork event")
	}
	var diagnosticLineageRows int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM run_fork_selected_contract_executions
		WHERE fork_run_id = $1::uuid
		  AND source_event_id = $2::uuid
	`, result.Materialization.ForkRunID, diagnosticEventID).Scan(&diagnosticLineageRows); err != nil {
		t.Fatalf("count diagnostic selected execution lineage: %v", err)
	}
	if diagnosticLineageRows != 0 {
		t.Fatalf("diagnostic platform execution lineage rows = %d, want 0", diagnosticLineageRows)
	}
}

func TestRunForkRuntimeOwnerHarness_SelectedContractsExecuteReportsSourceAdvancedBranchJSON(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	repo := repoRoot()
	contractsRoot := filepath.Join(repo, "tests/tier1-primitives/test-emits-multiple")
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	afterEventID := uuid.NewString()
	at := time.Unix(1700000313, 0).UTC()
	seedRunForkCLISelectedExecutionSource(t, db, sourceRunID, entityID, sourceEventID, at)
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'source.after', $3::uuid, 'flow-a/1', 'entity', '{}'::jsonb, 'test', 'platform', $4)
	`, sourceRunID, afterEventID, entityID, at.Add(time.Second)); err != nil {
		t.Fatalf("seed post-fork source event: %v", err)
	}

	var buf bytes.Buffer
	code := runForkRuntimeOwnerHarness(context.Background(), repo, []string{
		"--store", "postgres",
		"--contracts", contractsRoot,
		"--run", sourceRunID,
		"--at", sourceEventID,
		"--json",
	}, &buf)
	if code != 0 {
		t.Fatalf("runForkRuntimeOwnerHarness code=%d output=%s", code, buf.String())
	}
	var result runtimerunforkexecution.SelectedContractExecutionResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("decode selected branch execution json: %v\n%s", err, buf.String())
	}
	if result.Activation.BranchDivergence == nil || result.Activation.SourceFrozen {
		t.Fatalf("branch activation = %#v", result.Activation)
	}
	if result.Activation.BranchDivergence.Owner != store.RunForkSelectedContractBranchDivergenceOwner ||
		result.Activation.BranchDivergence.Policy != store.RunForkSelectedContractSourceAdvancedBranchPolicy {
		t.Fatalf("branch divergence = %#v", result.Activation.BranchDivergence)
	}
	var sourceStatus string
	if err := db.QueryRowContext(context.Background(), `SELECT status FROM runs WHERE run_id = $1::uuid`, sourceRunID).Scan(&sourceStatus); err != nil {
		t.Fatalf("load source status: %v", err)
	}
	if sourceStatus != "running" {
		t.Fatalf("source status = %q, want unchanged running", sourceStatus)
	}
}

func TestRunForkRuntimeOwnerHarness_MaterializeOnlyUsesCanonicalStoreOwnerJSON(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	runID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000310, 0).UTC()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'fork.cli.materialize', $3::uuid, 'flow-a/1', 'entity', '{}'::jsonb, 'test', 'platform', $4)
	`, runID, eventID, entityID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"ready"'::jsonb, $3::uuid, 'platform', 'cli-test', 'seed', $4),
			($1::uuid, $2::uuid, 'name', 'null'::jsonb, '"CLI Entity"'::jsonb, $3::uuid, 'platform', 'cli-test', 'seed', $4)
	`, runID, entityID, eventID, at); err != nil {
		t.Fatalf("seed mutations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'flow-a/1', 'default', 'CLI Entity',
			'ready', '{}'::jsonb, '{"name":"CLI Entity"}'::jsonb, '{}'::jsonb, 1,
			$3, $3, $3
		)
	`, runID, entityID, at); err != nil {
		t.Fatalf("seed entity_state: %v", err)
	}
	repo := repoRoot()
	contractsRoot := filepath.Join(repo, "tests", "tier11-flow-composition", "test-sibling-both-instantiated-isolated")
	var buf bytes.Buffer
	code := runForkRuntimeOwnerHarness(ctx, repo, []string{
		"--store", "postgres",
		"--materialize-only",
		"--run", runID,
		"--at", eventID,
		"--contracts", contractsRoot,
		"--json",
	}, &buf)
	if code != 0 {
		t.Fatalf("runForkRuntimeOwnerHarness code=%d output=%s", code, buf.String())
	}
	var result store.RunForkMaterialization
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("decode fork materialization json: %v\n%s", err, buf.String())
	}
	if result.SourceRunID != runID || result.ForkRunID == "" || result.ForkRunStatus != store.RunForkMaterializedStatus {
		t.Fatalf("materialization result = %#v", result)
	}
	if result.ReplayResumeAdmission.Owner != store.RunForkReplayResumeAdmissionOwner || !result.ReplayResumeAdmission.StateOnlyExecutionReady {
		t.Fatalf("materialization taxonomy = %#v, want canonical owner and state-only ready", result.ReplayResumeAdmission)
	}
	if result.SelectedContractBinding == nil {
		t.Fatalf("SelectedContractBinding = nil; output=%s", buf.String())
	}
	if result.SelectedContractBinding.Owner != store.RunForkSelectedContractBindingOwner ||
		result.SelectedContractBinding.ForkRunID != result.ForkRunID ||
		result.SelectedContractBinding.SourceRunID != runID ||
		result.SelectedContractBinding.ForkEventID != eventID ||
		result.SelectedContractBinding.ContractSelection.ContractsRoot != contractsRoot {
		t.Fatalf("selected contract binding = %#v", result.SelectedContractBinding)
	}
	var forkState string
	if err := db.QueryRowContext(ctx, `
		SELECT current_state
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, result.ForkRunID, entityID).Scan(&forkState); err != nil {
		t.Fatalf("load fork entity_state: %v", err)
	}
	if forkState != "ready" {
		t.Fatalf("fork state = %q, want ready", forkState)
	}
	var persistedBindingMode string
	if err := db.QueryRowContext(ctx, `
		SELECT mode
		FROM run_fork_selected_contract_bindings
		WHERE fork_run_id = $1::uuid
		  AND source_run_id = $2::uuid
		  AND fork_event_id = $3::uuid
	`, result.ForkRunID, runID, eventID).Scan(&persistedBindingMode); err != nil {
		t.Fatalf("load selected contract binding row: %v", err)
	}
	if persistedBindingMode != "selected_contracts" {
		t.Fatalf("binding mode = %q, want selected_contracts", persistedBindingMode)
	}
}

func TestRunForkRuntimeOwnerHarness_ActivateUsesCanonicalStoreOwnerJSON(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	pg := &store.PostgresStore{DB: db}
	runID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000320, 0).UTC()
	ctx := context.Background()
	seedRunForkCLIActivationSource(t, db, runID, entityID, eventID, at)
	materialized, err := pg.MaterializeRunFork(ctx, store.RunForkMaterializeRequest{SourceRunID: runID, At: eventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}

	var buf bytes.Buffer
	code := runForkRuntimeOwnerHarness(ctx, t.TempDir(), []string{
		"--store", "postgres",
		"--activate",
		"--run", materialized.ForkRunID,
		"--json",
	}, &buf)
	if code != 0 {
		t.Fatalf("runForkRuntimeOwnerHarness code=%d output=%s", code, buf.String())
	}
	var result store.RunForkActivation
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("decode fork activation json: %v\n%s", err, buf.String())
	}
	if !result.Activated || !result.SourceFrozen || !result.HistoricalReplayBlocked {
		t.Fatalf("activation result = %#v", result)
	}
	if result.ReplayResumeAdmission.Owner != store.RunForkReplayResumeAdmissionOwner || !result.ReplayResumeAdmission.StateOnlyExecutionReady {
		t.Fatalf("activation taxonomy = %#v, want canonical owner and state-only ready", result.ReplayResumeAdmission)
	}
	if result.SourceRunID != runID || result.ForkRunID != materialized.ForkRunID {
		t.Fatalf("activation lineage = %#v", result)
	}
	var sourceStatus, forkStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, runID).Scan(&sourceStatus); err != nil {
		t.Fatalf("load source status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, materialized.ForkRunID).Scan(&forkStatus); err != nil {
		t.Fatalf("load fork status: %v", err)
	}
	if sourceStatus != store.RunForkSourceFrozenStatus || forkStatus != store.RunForkActivatedStatus {
		t.Fatalf("source/fork status = %s/%s, want forked/running", sourceStatus, forkStatus)
	}
}

func TestRunForkRuntimeOwnerHarness_ActivateNonSelectedDoesNotRequireSelectedBindingSchema(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	pg := &store.PostgresStore{DB: db}
	runID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000325, 0).UTC()
	ctx := context.Background()
	seedRunForkCLIActivationSource(t, db, runID, entityID, eventID, at)
	materialized, err := pg.MaterializeRunFork(ctx, store.RunForkMaterializeRequest{SourceRunID: runID, At: eventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DROP TABLE run_fork_selected_contract_bindings`); err != nil {
		t.Fatalf("drop selected binding table: %v", err)
	}

	var buf bytes.Buffer
	code := runForkRuntimeOwnerHarness(ctx, t.TempDir(), []string{
		"--store", "postgres",
		"--activate",
		"--run", materialized.ForkRunID,
		"--json",
	}, &buf)
	if code != 0 {
		t.Fatalf("runForkRuntimeOwnerHarness code=%d output=%s", code, buf.String())
	}
	var result store.RunForkActivation
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("decode fork activation json: %v\n%s", err, buf.String())
	}
	if !result.Activated || !result.SourceFrozen || result.ForkRunID != materialized.ForkRunID {
		t.Fatalf("activation result = %#v", result)
	}
}

func TestRunForkRuntimeOwnerHarness_ActivateSelectedBindingConsumesRuntimeAdmission(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	runID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000330, 0).UTC()
	ctx := context.Background()
	seedRunForkCLIActivationSource(t, db, runID, entityID, eventID, at)
	repo := repoRoot()
	contractsRoot := filepath.Join(repo, "tests", "tier11-flow-composition", "test-sibling-both-instantiated-isolated")

	var materializeOut bytes.Buffer
	materializeCode := runForkRuntimeOwnerHarness(ctx, repo, []string{
		"--store", "postgres",
		"--materialize-only",
		"--run", runID,
		"--at", eventID,
		"--contracts", contractsRoot,
		"--json",
	}, &materializeOut)
	if materializeCode != 0 {
		t.Fatalf("materialize code=%d output=%s", materializeCode, materializeOut.String())
	}
	var materialized store.RunForkMaterialization
	if err := json.Unmarshal(materializeOut.Bytes(), &materialized); err != nil {
		t.Fatalf("decode materialization: %v\n%s", err, materializeOut.String())
	}

	var activateOut bytes.Buffer
	activateCode := runForkRuntimeOwnerHarness(ctx, repo, []string{
		"--store", "postgres",
		"--activate",
		"--run", materialized.ForkRunID,
		"--json",
	}, &activateOut)
	if activateCode != 0 {
		t.Fatalf("activate code=%d output=%s", activateCode, activateOut.String())
	}
	var result struct {
		store.RunForkActivation
		Owner     string                                           `json:"selected_contract_activation_gate_owner"`
		Admission *store.RunForkSelectedContractExecutionAdmission `json:"selected_contract_execution_admission"`
	}
	if err := json.Unmarshal(activateOut.Bytes(), &result); err != nil {
		t.Fatalf("decode activation: %v\n%s", err, activateOut.String())
	}
	if !result.Activated || !result.SourceFrozen || result.ForkRunID != materialized.ForkRunID {
		t.Fatalf("activation = %#v", result.RunForkActivation)
	}
	if result.Owner != store.RunForkSelectedContractExecutionActivationGateOwner {
		t.Fatalf("selected activation owner = %q, want %q", result.Owner, store.RunForkSelectedContractExecutionActivationGateOwner)
	}
	if result.Admission == nil ||
		result.Admission.Owner != store.RunForkSelectedContractExecutionAdmissionOwner ||
		result.Admission.FrontierEventCount != 0 ||
		result.Admission.ContractBindingOwner != store.RunForkSelectedContractBindingOwner {
		t.Fatalf("selected admission = %#v", result.Admission)
	}
}

func TestRunForkRuntimeOwnerHarness_ActivateSelectedBindingBlocksReplayWithoutSelectedRecipientPlan(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	runID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000340, 0).UTC()
	ctx := context.Background()
	seedRunForkCLIActivationSource(t, db, runID, entityID, eventID, at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent', 'safe-agent', 'pending', 0, 'matched_agent_subscription', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed safe pending delivery: %v", err)
	}
	repo := repoRoot()
	contractsRoot := filepath.Join(repo, "tests", "tier11-flow-composition", "test-sibling-both-instantiated-isolated")

	var materializeOut bytes.Buffer
	materializeCode := runForkRuntimeOwnerHarness(ctx, repo, []string{
		"--store", "postgres",
		"--materialize-only",
		"--run", runID,
		"--at", eventID,
		"--contracts", contractsRoot,
		"--json",
	}, &materializeOut)
	if materializeCode != 0 {
		t.Fatalf("materialize code=%d output=%s", materializeCode, materializeOut.String())
	}
	var materialized store.RunForkMaterialization
	if err := json.Unmarshal(materializeOut.Bytes(), &materialized); err != nil {
		t.Fatalf("decode materialization: %v\n%s", err, materializeOut.String())
	}

	var activateOut bytes.Buffer
	activateCode := runForkRuntimeOwnerHarness(ctx, repo, []string{
		"--store", "postgres",
		"--activate",
		"--run", materialized.ForkRunID,
		"--json",
	}, &activateOut)
	if activateCode != 1 {
		t.Fatalf("activate code=%d, want 1; output=%s", activateCode, activateOut.String())
	}
	if !strings.Contains(activateOut.String(), "no selected recipients") {
		t.Fatalf("activate output = %q, want selected recipient-plan blocker", activateOut.String())
	}
	var sourceStatus, forkStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, runID).Scan(&sourceStatus); err != nil {
		t.Fatalf("load source status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, materialized.ForkRunID).Scan(&forkStatus); err != nil {
		t.Fatalf("load fork status: %v", err)
	}
	if sourceStatus != "running" || forkStatus != store.RunForkMaterializedStatus {
		t.Fatalf("source/fork status = %s/%s, want running/paused", sourceStatus, forkStatus)
	}
	var replayRows, forkEvents, forkDeliveries int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_delivery_event_replays WHERE fork_run_id = $1::uuid`, materialized.ForkRunID).Scan(&replayRows); err != nil {
		t.Fatalf("count replay rows: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid`, materialized.ForkRunID).Scan(&forkEvents); err != nil {
		t.Fatalf("count fork events: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id = $1::uuid`, materialized.ForkRunID).Scan(&forkDeliveries); err != nil {
		t.Fatalf("count fork deliveries: %v", err)
	}
	if replayRows != 0 || forkEvents != 0 || forkDeliveries != 0 {
		t.Fatalf("selected-bound replay mutation counts = replay:%d events:%d deliveries:%d, want 0/0/0", replayRows, forkEvents, forkDeliveries)
	}
}

func TestRunForkRuntimeOwnerHarness_NonDryRunWithoutMaterializeOnlyStaysFailClosed(t *testing.T) {
	var buf bytes.Buffer
	code := runForkRuntimeOwnerHarness(context.Background(), t.TempDir(), []string{
		"--run", uuid.NewString(),
		"--at", uuid.NewString(),
	}, &buf)
	if code != 2 {
		t.Fatalf("runForkRuntimeOwnerHarness code=%d, want 2; output=%s", code, buf.String())
	}
	if !strings.Contains(buf.String(), "mutating fork execution without --contracts is not implemented") {
		t.Fatalf("output = %q, want fail-closed fork execution message", buf.String())
	}
}

func seedRunForkCLIActivationSource(t *testing.T, db interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, runID, entityID, eventID string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'fork.cli.activate', $3::uuid, 'flow-a/1', 'entity', '{}'::jsonb, 'test', 'platform', $4)
	`, runID, eventID, entityID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"ready"'::jsonb, $3::uuid, 'platform', 'cli-test', 'seed', $4),
			($1::uuid, $2::uuid, 'name', 'null'::jsonb, '"CLI Entity"'::jsonb, $3::uuid, 'platform', 'cli-test', 'seed', $4)
	`, runID, entityID, eventID, at); err != nil {
		t.Fatalf("seed mutations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'flow-a/1', 'default', 'CLI Entity',
			'ready', '{}'::jsonb, '{"name":"CLI Entity"}'::jsonb, '{}'::jsonb, 1,
			$3, $3, $3
		)
	`, runID, entityID, at); err != nil {
		t.Fatalf("seed entity_state: %v", err)
	}
}

func seedRunForkCLISelectedExecutionSource(t *testing.T, db interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, runID, entityID, eventID string, at time.Time) {
	t.Helper()
	seedRunForkSelectedExecutionSourceEvent(t, db, runID, entityID, eventID, "item.received", "test-node", "pending", "CLI Selected Execution Entity", "cli-selected-execution-test", at)
}

func seedRunForkSelectedExecutionSourceEvent(t *testing.T, db interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, runID, entityID, eventID, eventName, subscriberID, currentState, entityName, writerID string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, $3, $4::uuid, 'flow-a/1', 'entity', $5::jsonb, 'test', 'platform', $6)
	`, runID, eventID, eventName, entityID, fmt.Sprintf(`{"entity_id":%q}`, entityID), at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, reason_code, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'node', $3, 'pending', 'source_pending_node_delivery', $4)
	`, runID, eventID, subscriberID, at); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, to_jsonb($5::text), $3::uuid, 'platform', $6, 'seed', $4),
			($1::uuid, $2::uuid, 'name', 'null'::jsonb, to_jsonb($7::text), $3::uuid, 'platform', $6, 'seed', $4)
	`, runID, entityID, eventID, at, currentState, writerID, entityName); err != nil {
		t.Fatalf("seed mutations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'flow-a/1', 'default', $4,
			$5, '{}'::jsonb, jsonb_build_object('name', $4::text), '{}'::jsonb, 1,
			$3, $3, $3
		)
	`, runID, entityID, at, entityName, currentState); err != nil {
		t.Fatalf("seed entity_state: %v", err)
	}
}

func seedRunForkCLISelectedExecutionDiagnosticPlatformDeadLetter(t *testing.T, db interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, runID, eventID string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'platform.runtime_log', NULL, NULL, 'global',
			'{"level":"info","message":"diagnostic platform row must remain lineage-only"}'::jsonb,
			'pipeline', 'platform', $3
		)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed diagnostic platform event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance, outcome, reason_code, side_effects, processed_at
		)
		VALUES (
			$1::uuid, 'platform', 'pipeline', NULL, NULL,
			'dead_letter', 'runtime_log_pipeline_dead_letter', '{}'::jsonb, $2
		)
	`, eventID, at); err != nil {
		t.Fatalf("seed diagnostic platform receipt: %v", err)
	}
}

func runForkPlanHasBlocker(blockers []store.RunForkUnsupportedBlocker, code string) bool {
	for _, blocker := range blockers {
		if blocker.Code == code {
			return true
		}
	}
	return false
}

func runForkPlanHasString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func runForkPlanHasBoundary(values []store.RunForkSelectedContractExecutionBoundary, concept, disposition string) bool {
	for _, value := range values {
		if value.Concept == concept && value.Disposition == disposition {
			return true
		}
	}
	return false
}

func runForkReadinessFactHas(values []store.RunForkSelectedContractReadinessFact, fact string) bool {
	for _, value := range values {
		if value.Fact == fact {
			return true
		}
	}
	return false
}

func runForkReadinessFactHasDisposition(values []store.RunForkSelectedContractReadinessFact, fact, disposition string) bool {
	for _, value := range values {
		if value.Fact == fact && value.Disposition == disposition {
			return true
		}
	}
	return false
}

func setPostgresEnvFromDSN(t *testing.T, dsn string) {
	t.Helper()
	values := map[string]string{}
	for _, part := range strings.Fields(dsn) {
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		values[key] = value
	}
	for _, item := range []struct {
		env string
		key string
	}{
		{"PGHOST", "host"},
		{"PGPORT", "port"},
		{"PGDATABASE", "dbname"},
		{"PGUSER", "user"},
		{"PGPASSWORD", "password"},
		{"SWARM_DB_SSLMODE", "sslmode"},
	} {
		if value := strings.TrimSpace(values[item.key]); value != "" {
			t.Setenv(item.env, value)
		}
	}
	exeDir := t.TempDir()
	originalExecutablePath := runtimeConfigExecutablePath
	runtimeConfigExecutablePath = func() (string, error) {
		return filepath.Join(exeDir, "swarm"), nil
	}
	t.Cleanup(func() { runtimeConfigExecutablePath = originalExecutablePath })
	writeRuntimeConfigText(t, filepath.Join(exeDir, "config.yaml"), fmt.Sprintf(`store:
  backend: postgres
database:
  host: %s
  port: %s
  name: %s
  user: %s
  password_env: PGPASSWORD
  sslmode: %s
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
    retries: 1
    no_session_persistence: false
`, values["host"], values["port"], values["dbname"], values["user"], values["sslmode"]))
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

func TestLoadRuntimeConfigWithOptions_UsesSharedDiscoveryAndBackendPrecedence(t *testing.T) {
	originalExecutablePath := runtimeConfigExecutablePath
	t.Cleanup(func() { runtimeConfigExecutablePath = originalExecutablePath })

	repo := t.TempDir()
	exeDir := t.TempDir()
	exePath := filepath.Join(exeDir, "swarm")
	runtimeConfigExecutablePath = func() (string, error) {
		return exePath, nil
	}
	writeRuntimeConfigText(t, filepath.Join(exeDir, "config.yaml"), strings.Join([]string{
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
		"    retries: 1",
		"    no_session_persistence: false",
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

	result, err := loadRuntimeConfigWithOptions(runtimeConfigLoadOptions{RepoRoot: repo})
	if err != nil {
		t.Fatalf("load executable config: %v", err)
	}
	if result.Config.LLM.Backend != "claude_cli" || result.Source != "executable" {
		t.Fatalf("executable config result = source=%s backend=%s, want executable claude_cli", result.Source, result.Config.LLM.Backend)
	}

	result, err = loadRuntimeConfigWithOptions(runtimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: "runtime.yaml", BackendOverride: "openai_compatible"})
	if err != nil {
		t.Fatalf("load explicit config with backend override: %v", err)
	}
	if result.Config.LLM.Backend != "openai_compatible" || result.Source != "explicit" || filepath.Clean(result.Path) != filepath.Clean(explicitPath) {
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

			result, err := loadRuntimeConfigWithOptions(runtimeConfigLoadOptions{
				RepoRoot:        repo,
				ExplicitPath:    "runtime.yaml",
				BackendOverride: "anthropic",
			})
			if err != nil {
				t.Fatalf("loadRuntimeConfigWithOptions: %v", err)
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
		setup func(t *testing.T) runtimeConfigLoadOptions
	}{
		{
			name: "explicit",
			setup: func(t *testing.T) runtimeConfigLoadOptions {
				t.Helper()
				repo := t.TempDir()
				writeRuntimeConfigText(t, filepath.Join(repo, "runtime.yaml"), configBody)
				return runtimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: "runtime.yaml"}
			},
		},
		{
			name: "executable-adjacent",
			setup: func(t *testing.T) runtimeConfigLoadOptions {
				t.Helper()
				exeDir := t.TempDir()
				runtimeConfigExecutablePath = func() (string, error) {
					return filepath.Join(exeDir, "swarm"), nil
				}
				writeRuntimeConfigText(t, filepath.Join(exeDir, "config.yaml"), configBody)
				return runtimeConfigLoadOptions{RepoRoot: t.TempDir()}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadRuntimeConfigWithOptions(tt.setup(t))
			if err == nil || !strings.Contains(err.Error(), "SWARM_CLAUDE_DEFAULT_MODEL") || !strings.Contains(err.Error(), "llm.models") {
				t.Fatalf("loadRuntimeConfigWithOptions error = %v, want retired model env guidance", err)
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
	_, err := loadRuntimeConfigWithOptions(runtimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: "runtime.yaml", BackendOverride: "claude_cli"})
	if err == nil || !strings.Contains(err.Error(), "unsupported llm backend profile") {
		t.Fatalf("loadRuntimeConfigWithOptions error = %v, want legacy backend rejection", err)
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

func TestLoadRunStatusReport_UsesDurableCompletedRunState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	eb, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	runID := uuid.NewString()
	entityID := uuid.NewString()
	publishRunStatusRootEvent(t, eb, runID, entityID)
	seedRunStatusEntityState(t, db, runID, entityID)
	markRunStatusCompleted(t, pg, runID)

	ctx := context.Background()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var status string
		err := db.QueryRowContext(ctx, `
			SELECT COALESCE(status, '')
			FROM runs
			WHERE run_id = $1::uuid
		`, runID).Scan(&status)
		if err == nil && status == "completed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s did not reach durable completed state: last err=%v", runID, err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	report, err := loadRunStatusReport(ctx, pg, runID, runStatusOptions{})
	if err != nil {
		t.Fatalf("loadRunStatusReport: %v", err)
	}
	if report.RunTableStatus != "completed" {
		t.Fatalf("RunTableStatus = %q, want completed", report.RunTableStatus)
	}
	if report.EndedAt == nil || report.EndedAt.IsZero() {
		t.Fatal("expected durable ended_at in run status report")
	}
	for _, heuristic := range report.Heuristics {
		if strings.Contains(heuristic, "runs table still says running") {
			t.Fatalf("unexpected running heuristic after durable completion: %#v", report.Heuristics)
		}
	}
}

func TestLoadRunStatusReport_KeepsSupportedRunRunningUntilManagerWorkSettles(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	eb, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	agentStarted := make(chan struct{}, 1)
	releaseAgent := make(chan struct{})
	testAgent := delayedRunStatusAgent{
		id:            "agent-1",
		subscriptions: []events.EventType{"scan.requested"},
		started:       agentStarted,
		release:       releaseAgent,
	}
	am := runtimemanager.NewAgentManager(eb, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		if cfg.ID != testAgent.id {
			t.Fatalf("unexpected agent id: %q", cfg.ID)
		}
		return testAgent, nil
	}, pg)
	if err := am.SpawnAgent(runtimeactors.AgentConfig{ID: testAgent.id, Model: "regular"}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	am.Run(context.Background())
	defer func() { _ = am.Shutdown() }()

	runID := uuid.NewString()
	entityID := uuid.NewString()
	publishRunStatusRootEvent(t, eb, runID, entityID)
	seedRunStatusEntityState(t, db, runID, entityID)

	select {
	case <-agentStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for agent work to start")
	}

	ctx := context.Background()
	var (
		status           string
		eventCount       int
		entityCount      int
		activeDeliveries int
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), event_count, entity_count
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&status, &eventCount, &entityCount); err != nil {
		t.Fatalf("load in-flight run row: %v", err)
	}
	if status != "running" {
		t.Fatalf("in-flight run status = %q, want running", status)
	}
	if eventCount != 1 {
		t.Fatalf("in-flight event_count = %d, want 1 root event", eventCount)
	}
	if entityCount != 1 {
		t.Fatalf("in-flight entity_count = %d, want 1", entityCount)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND status IN ('pending', 'in_progress')
	`, runID).Scan(&activeDeliveries); err != nil {
		t.Fatalf("count active deliveries: %v", err)
	}
	if activeDeliveries == 0 {
		t.Fatal("expected active delivery while agent work is blocked")
	}

	close(releaseAgent)
	waitRunStatusEventSettlement(t, db, runID, 2)
	markRunStatusCompleted(t, pg, runID)

	deadline := time.Now().Add(3 * time.Second)
	for {
		err := db.QueryRowContext(ctx, `
			SELECT COALESCE(status, ''), event_count, entity_count
			FROM runs
			WHERE run_id = $1::uuid
		`, runID).Scan(&status, &eventCount, &entityCount)
		if err == nil && status == "completed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s did not reach coherent completed state: last err=%v status=%q event_count=%d entity_count=%d", runID, err, status, eventCount, entityCount)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if eventCount < 2 {
		t.Fatalf("completed event_count = %d, want downstream event activity", eventCount)
	}
	if entityCount != 1 {
		t.Fatalf("completed entity_count = %d, want 1", entityCount)
	}
	var extraRunningRows int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM runs
		WHERE run_id <> $1::uuid
		  AND status = 'running'
	`, runID).Scan(&extraRunningRows); err != nil {
		t.Fatalf("count extra running rows: %v", err)
	}
	if extraRunningRows != 0 {
		t.Fatalf("extra running rows = %d, want 0", extraRunningRows)
	}

	report, err := loadRunStatusReport(ctx, pg, runID, runStatusOptions{})
	if err != nil {
		t.Fatalf("loadRunStatusReport: %v", err)
	}
	if report.RunTableStatus != "completed" {
		t.Fatalf("RunTableStatus = %q, want completed", report.RunTableStatus)
	}
}

func TestLoadRunStatusReport_PreservesRunningTruthWhileManagerWorkIsActive(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	eb, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	agentStarted := make(chan struct{}, 1)
	releaseAgent := make(chan struct{})
	testAgent := delayedRunStatusAgent{
		id:            "agent-1",
		subscriptions: []events.EventType{"scan.requested"},
		started:       agentStarted,
		release:       releaseAgent,
	}
	am := runtimemanager.NewAgentManager(eb, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		if cfg.ID != testAgent.id {
			t.Fatalf("unexpected agent id: %q", cfg.ID)
		}
		return testAgent, nil
	}, pg)
	if err := am.SpawnAgent(runtimeactors.AgentConfig{ID: testAgent.id, Model: "regular"}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	am.Run(context.Background())
	defer func() { _ = am.Shutdown() }()

	runID := uuid.NewString()
	entityID := uuid.NewString()
	publishRunStatusRootEvent(t, eb, runID, entityID)
	seedRunStatusEntityState(t, db, runID, entityID)

	select {
	case <-agentStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for agent work to start")
	}

	time.Sleep(120 * time.Millisecond)

	ctx := context.Background()
	var (
		status           string
		activeDeliveries int
		endedAt          sql.NullTime
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), ended_at
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&status, &endedAt); err != nil {
		t.Fatalf("load timed-out run row: %v", err)
	}
	if status != "running" {
		t.Fatalf("timed-out run status = %q, want running", status)
	}
	if endedAt.Valid {
		t.Fatalf("timed-out run ended_at = %s, want NULL while same-run work remains active", endedAt.Time)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND status IN ('pending', 'in_progress')
	`, runID).Scan(&activeDeliveries); err != nil {
		t.Fatalf("count active deliveries after timeout window: %v", err)
	}
	if activeDeliveries == 0 {
		t.Fatal("expected same-run active delivery after builder timeout window")
	}
	if got := am.InFlightCount(); got == 0 {
		t.Fatal("expected live in-flight manager work after builder timeout window")
	}

	report, err := loadRunStatusReport(ctx, pg, runID, runStatusOptions{})
	if err != nil {
		t.Fatalf("loadRunStatusReport: %v", err)
	}
	if report.RunTableStatus != "running" {
		t.Fatalf("RunTableStatus after timeout window = %q, want running", report.RunTableStatus)
	}
	if report.OperationalState != "running" {
		t.Fatalf("OperationalState after timeout window = %q, want running", report.OperationalState)
	}
	foundActiveDelivery := false
	for _, item := range report.Deliveries {
		if item.SubscriberID == "agent-1" && item.Status == "in_progress" && item.Count > 0 {
			foundActiveDelivery = true
			break
		}
	}
	if !foundActiveDelivery {
		t.Fatalf("expected supported status report to preserve active same-run delivery, got %#v", report.Deliveries)
	}

	close(releaseAgent)
	waitRunStatusEventSettlement(t, db, runID, 2)
	markRunStatusCompleted(t, pg, runID)

	deadline := time.Now().Add(3 * time.Second)
	for {
		err := db.QueryRowContext(ctx, `
			SELECT COALESCE(status, ''), ended_at
			FROM runs
			WHERE run_id = $1::uuid
		`, runID).Scan(&status, &endedAt)
		if err == nil && status == "completed" && endedAt.Valid {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s did not reach coherent completed state after release: last err=%v status=%q ended_at_valid=%v", runID, err, status, endedAt.Valid)
		}
		time.Sleep(10 * time.Millisecond)
	}

}

func TestLoadRunStatusReport_ProjectsExplicitStalledRunState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()

	runID := uuid.NewString()
	rootEventID := uuid.NewString()
	deliveredEventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', now() - interval '10 minutes')
	`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	for _, eventID := range []string{rootEventID, deliveredEventID} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO events (
				run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
			) VALUES (
				$1::uuid, $2::uuid, 'scan.requested', 'global', '{}'::jsonb, 'runtime', 'agent', now() - interval '5 minutes'
			)
		`, runID, eventID); err != nil {
			t.Fatalf("seed event %s: %v", eventID, err)
		}
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, delivered_at, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'agent', 'agent-1', 'delivered', now() - interval '2 minutes', now() - interval '4 minutes'
		)
	`, runID, deliveredEventID); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}

	report, err := loadRunStatusReport(ctx, pg, runID, runStatusOptions{})
	if err != nil {
		t.Fatalf("loadRunStatusReport: %v", err)
	}
	if report.RunTableStatus != "running" {
		t.Fatalf("run_table_status = %q, want running", report.RunTableStatus)
	}
	if report.OperationalState != "stalled" {
		t.Fatalf("operational_state = %q, want stalled", report.OperationalState)
	}
	if report.BlockingLayer != "delivery_lifecycle" {
		t.Fatalf("blocking_layer = %q, want delivery_lifecycle", report.BlockingLayer)
	}
	if report.BlockingReason != "no_active_deliveries" {
		t.Fatalf("blocking_reason = %q, want no_active_deliveries", report.BlockingReason)
	}
	for _, heuristic := range report.Heuristics {
		if strings.Contains(heuristic, "runs table still says running") {
			t.Fatalf("unexpected stalled heuristic fallback: %#v", report.Heuristics)
		}
	}
}

func TestLoadRunStatusReport_ProjectsScoringOutcomeStall(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()

	runID := uuid.NewString()
	rootEventID := uuid.NewString()
	scoringEventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', now() - interval '10 minutes')
	`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		) VALUES
			($1::uuid, $2::uuid, 'scan.requested', 'global', '{}'::jsonb, 'runtime', 'agent', now() - interval '9 minutes'),
			($1::uuid, $3::uuid, 'scoring/scoring.requested', 'global', '{}'::jsonb, 'runtime', 'agent', now() - interval '2 minutes')
	`, runID, rootEventID, scoringEventID); err != nil {
		t.Fatalf("seed events: %v", err)
	}

	report, err := loadRunStatusReport(ctx, pg, runID, runStatusOptions{})
	if err != nil {
		t.Fatalf("loadRunStatusReport: %v", err)
	}
	if report.OperationalState != "stalled" {
		t.Fatalf("operational_state = %q, want stalled", report.OperationalState)
	}
	if report.BlockingLayer != "scoring_terminal_outcome" {
		t.Fatalf("blocking_layer = %q, want scoring_terminal_outcome", report.BlockingLayer)
	}
	if report.BlockingReason != "terminal_scoring_outcome_missing" {
		t.Fatalf("blocking_reason = %q, want terminal_scoring_outcome_missing", report.BlockingReason)
	}
	for _, heuristic := range report.Heuristics {
		if strings.Contains(heuristic, "run appears settled after scoring started") {
			t.Fatalf("unexpected scoring heuristic fallback: %#v", report.Heuristics)
		}
	}
}

func runVerifyCommandWithContractsForTest(ctx context.Context, repo, contractsPath string, out *bytes.Buffer) int {
	return runVerifyCommandWithContractsOutputForTest(ctx, repo, contractsPath, out, out)
}

func runVerifyCommandWithContractsOutputForTest(ctx context.Context, repo, contractsPath string, out, errOut *bytes.Buffer) int {
	opts := defaultVerifyCommandOptions()
	opts.contractsPath = contractsPath
	return runVerifyCommandWithOutput(ctx, repo, opts, out, errOut)
}

func TestRunVerifyCommand_BadContractsPath(t *testing.T) {
	cases := []struct {
		name string
		path string
	}{
		{name: "missing absolute path", path: filepath.Join(t.TempDir(), "missing")},
		{name: "explicit child path under bundle", path: filepath.Join(repoRoot(), "tests", "tier8-boot-verification", "test-boot-success", "zzz-not-a-real-dir")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			code := runVerifyCommandWithContractsForTest(context.Background(), repoRoot(), tc.path, &buf)
			if code == 0 {
				t.Fatal("expected non-zero exit code")
			}
			out := buf.String()
			if !strings.Contains(out, "verify failed: resolve contracts") {
				t.Fatalf("output = %q", out)
			}
			if !strings.Contains(out, tc.path) {
				t.Fatalf("output does not name explicit path %q:\n%s", tc.path, out)
			}
		})
	}
}

func TestNormalizeContractsRootExplicitPathValidation(t *testing.T) {
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `name: explicit-root`)

	got, err := normalizeContractsRoot(root)
	if err != nil {
		t.Fatalf("normalize directory root: %v", err)
	}
	if got != root {
		t.Fatalf("root = %q, want %q", got, root)
	}

	got, err = normalizeContractsRoot(filepath.Join(root, "package.yaml"))
	if err != nil {
		t.Fatalf("normalize package file shorthand: %v", err)
	}
	if got != root {
		t.Fatalf("root from package file = %q, want %q", got, root)
	}

	explicitChild := filepath.Join(root, "zzz-not-a-real-dir")
	if got, err := normalizeContractsRoot(explicitChild); err == nil {
		t.Fatalf("normalize explicit child returned %q, want fail-closed error", got)
	} else if !strings.Contains(err.Error(), explicitChild) {
		t.Fatalf("error = %q, want explicit child path %q", err.Error(), explicitChild)
	}
}

func TestRunVerifyCommand_SurfacesLintEvidence(t *testing.T) {
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: verify-lint-evidence
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: child
    flow: child
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `name: verify-lint-evidence`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "entities.yaml"), `
case:
  untouched:
    type: integer
    _unused_reason: verify command lint evidence proof field
  priority:
    type: integer
    _unused_reason: child read-pin coverage proof field
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "schema.yaml"), `
name: child
initial_state: idle
terminal_states: [done]
states: [idle, done]
pins:
  inputs:
    events: [task.assigned]
    reads: [priority]
  outputs:
    events: []
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "agents.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "entities.yaml"), `
case:
  priority:
    type: integer
    _unused_reason: verify lint evidence child primary entity proof field
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "events.yaml"), `
task.assigned:
  swarm:
    source: external (verify lint evidence test)
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "nodes.yaml"), `
reader:
  id: reader
  execution_type: system_node
  subscribes_to: [task.assigned]
  event_handlers:
    task.assigned:
      guard:
        check: "entity.priority >= 0"
      advances_to: done
`)

	var stdout, stderr bytes.Buffer
	code := runVerifyCommandWithContractsOutputForTest(context.Background(), repoRoot(), root, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("runVerifyCommand exit code = %d, stdout = %q stderr = %q", code, stdout.String(), stderr.String())
	}
	if out := stdout.String(); !strings.Contains(out, "verify ok: contracts=") {
		t.Fatalf("verify stdout missing success marker:\n%s", out)
	} else if strings.Contains(out, "entity_reader_coverage") || strings.Contains(out, "lint_evidence") {
		t.Fatalf("verify stdout contains advisory diagnostics:\n%s", out)
	}
	errText := stderr.String()
	if !strings.Contains(errText, "INFO: entity_reader_coverage [root] flow root entity_type case declares field untouched with no detected internal reader coverage") {
		t.Fatalf("verify stderr missing lint evidence:\n%s", errText)
	}
	if strings.Contains(errText, "lint_evidence:") {
		t.Fatalf("verify stderr used legacy lint_evidence prefix:\n%s", errText)
	}

	opts := defaultVerifyCommandOptions()
	opts.contractsPath = root
	opts.output.asJSON = true
	stdout.Reset()
	stderr.Reset()
	code = runVerifyCommandWithOutput(context.Background(), repoRoot(), opts, &stdout, &stderr)
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

func TestRunVerifyCommand_AllowsBootTimerWithoutCancelOn(t *testing.T) {
	root := writeVerifyBootTimerCommandFixture(t, "")

	var stdout, stderr bytes.Buffer
	code := runVerifyCommandWithContractsOutputForTest(context.Background(), repoRoot(), root, &stdout, &stderr)
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
	code := runVerifyCommandWithContractsOutputForTest(context.Background(), repoRoot(), root, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("runVerifyCommand exit code = 0, stdout = %q stderr = %q", stdout.String(), stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != "" {
		t.Fatalf("verify stdout = %q, want empty for hard invalidity", stdout.String())
	}
	errText := stderr.String()
	for _, want := range []string{
		"verify failed: boot verification failed:",
		"ERROR: timer_validation",
		"start_on boot does not support cancel_on state:done",
	} {
		if !strings.Contains(errText, want) {
			t.Fatalf("verify stderr missing %q:\n%s", want, errText)
		}
	}
}

func TestRunVerifyCommand_EscalatedWarningUsesBlockingAnalyzerOutput(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")

	var stdout, stderr bytes.Buffer
	code := runVerifyCommandWithContractsOutputForTest(
		context.Background(),
		repoRoot(),
		writeVerifyMissingPinWarningFixture(t),
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
		"ERROR: input_pin_wiring",
		"no producer path was found in the authored bundle",
	} {
		if !strings.Contains(errText, want) {
			t.Fatalf("verify stderr missing %q:\n%s", want, errText)
		}
	}
	if strings.Contains(errText, "boot verification warnings:") {
		t.Fatalf("verify stderr used legacy fatal warning banner:\n%s", errText)
	}
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
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: ticket-flow
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows: []
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: ticket-flow
initial_state: open
terminal_states: [resolved]
states: [open, assigned, resolved]
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "entities.yaml"), `
ticket:
  category:
    type: text
    initial: ""
  priority:
    type: text
    initial: ""
  resolution:
    type: text
    initial: ""
    _unused_reader_reason: External operator readout from the persisted ticket record
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), `
ticket.classified:
  swarm:
    source: external (first-flow verify proof)
  category: text
  priority: text
ticket.assigned:
  category: text
  priority: text
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
classifier:
  id: classifier
  execution_type: system_node
  subscribes_to: [ticket.classified]
  produces: [ticket.assigned]
  event_handlers:
    ticket.classified:
      guard:
        check: "entity.category != '' && entity.priority != ''"
      emit:
        event: ticket.assigned
        broadcast: true
        fields:
          category: entity.category
          priority: entity.priority
      advances_to: assigned
assignee:
  id: assignee
  execution_type: system_node
  subscribes_to: [ticket.assigned]
  event_handlers:
    ticket.assigned:
      guard:
        check: "entity.category != ''"
      advances_to: resolved
`)

	var buf bytes.Buffer
	code := runVerifyCommandWithContractsForTest(context.Background(), repoRoot(), root, &buf)
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
	root := t.TempDir()
	writeVerifyModelAliasFixture(t, root, "not_configured")

	var buf bytes.Buffer
	code := runVerifyCommandWithContractsForTest(context.Background(), repoRoot(), root, &buf)
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

func TestRunVerifyCommand_UsesExecutableRuntimeConfigModelAliases(t *testing.T) {
	originalExecutablePath := runtimeConfigExecutablePath
	t.Cleanup(func() { runtimeConfigExecutablePath = originalExecutablePath })

	root := t.TempDir()
	writeVerifyModelAliasFixture(t, root, "audit.custom")

	exeDir := t.TempDir()
	runtimeConfigExecutablePath = func() (string, error) {
		return filepath.Join(exeDir, "swarm"), nil
	}
	writeRuntimeConfigText(t, filepath.Join(exeDir, "config.yaml"), strings.Join([]string{
		"llm:",
		"  backend: anthropic",
		"  models:",
		"    audit.custom:",
		"      anthropic: claude-custom",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
	}, "\n")+"\n")

	var buf bytes.Buffer
	code := runVerifyCommandWithContractsForTest(context.Background(), repoRoot(), root, &buf)
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
  mode: task
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
	code := runVerifyCommandWithContractsOutputForTest(context.Background(), repoRoot(), root, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("expected non-zero exit code, stdout = %q stderr = %q", stdout.String(), stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != "" {
		t.Fatalf("verify stdout = %q, want empty for hard invalidity", stdout.String())
	}
	errText := stderr.String()
	for _, want := range []string{
		"verify failed: boot verification failed:",
		"ERROR: entity_writer_coverage",
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
	code := runVerifyCommandWithContractsForTest(context.Background(), repoRoot(), root, &buf)
	if code == 0 {
		t.Fatalf("expected non-zero exit code, output = %q", buf.String())
	}
	if out := buf.String(); !strings.Contains(out, "verify failed: load Swarm contracts:") || !strings.Contains(out, "state_schema field type") {
		t.Fatalf("unexpected output = %q", out)
	}
}

func TestRunVerifyCommand_AllowsCanonicalStateSchemaFloat(t *testing.T) {
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: verify-state-schema-float
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: child
    flow: child
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `name: verify-state-schema-float`)
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
case: {}
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "agents.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "events.yaml"), `
task.assigned:
  swarm:
    source: external (state schema float verify test)
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "nodes.yaml"), `
accumulator:
  id: accumulator
  execution_type: system_node
  subscribes_to: [task.assigned]
  event_handlers:
    task.assigned:
      advances_to: done
  state_schema:
    fields:
      composite: float
`)

	var buf bytes.Buffer
	code := runVerifyCommandWithContractsForTest(context.Background(), repoRoot(), root, &buf)
	if code != 0 {
		t.Fatalf("runVerifyCommand exit code = %d, output = %q", code, buf.String())
	}
	if out := buf.String(); !strings.Contains(out, "verify ok: contracts=") {
		t.Fatalf("verify output missing success marker:\n%s", out)
	}
}

func TestRunVerifyCommand_AllowsAccumulatorEntityProjection(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "false")

	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: verify-accumulator-entity-projection
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows: []
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: verify-accumulator-entity-projection
initial_state: collecting
terminal_states: [complete]
states: [collecting, complete]
pins:
  inputs:
    events: [score.dimension_complete]
  outputs:
    events: [score.completed]
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "types.yaml"), `
types:
  DimensionScore:
    dimension: text
    tier: integer
    score: integer
    evidence: text
    confidence: text
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "entities.yaml"), `
vertical:
  scores:
    type: list<DimensionScore>
    materialize_from: scorer.dimensions_received
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), `
score.dimension_complete:
  swarm:
    source: external (verify accumulator projection fixture)
  expected_dimensions: integer
  vertical_id: string
  dimension: text
  tier: integer
  score: integer
  evidence: text
  confidence: text
score.completed: {}
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
scorer:
  id: scorer
  execution_type: system_node
  subscribes_to: [score.dimension_complete]
  produces: [score.completed]
  event_handlers:
    score.dimension_complete:
      accumulate:
        into: dimensions_received
        expected_from: payload.expected_dimensions
        completion: all
      on_complete:
        - condition: "true"
          emit:
            event: score.completed
            broadcast: true
      advances_to: complete
  state_schema:
    fields:
      dimensions_received: list<DimensionScore>
`)

	var buf bytes.Buffer
	code := runVerifyCommandWithContractsForTest(context.Background(), repoRoot(), root, &buf)
	if code != 0 {
		t.Fatalf("runVerifyCommand exit code = %d, output = %q", code, buf.String())
	}
	if out := buf.String(); !strings.Contains(out, "verify ok: contracts=") {
		t.Fatalf("verify output missing success marker:\n%s", out)
	}
}

func TestRunVerifyCommand_WarnsForAccumulateAllWithoutBoundedEscape(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "false")

	root := writeVerifyAccumulatorSafetyCommandFixture(t, verifyAccumulatorSafetyCommandFixtureOptions{
		eventSource: "external (verify accumulator safety proof)",
		completion:  "all",
	})

	var stdout, stderr bytes.Buffer
	code := runVerifyCommandWithContractsOutputForTest(context.Background(), repoRoot(), root, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("runVerifyCommand exit code = %d, stdout = %q stderr = %q", code, stdout.String(), stderr.String())
	}
	if out := stdout.String(); !strings.Contains(out, "verify ok: contracts=") {
		t.Fatalf("verify stdout missing success marker:\n%s", out)
	}
	errText := stderr.String()
	if !strings.Contains(errText, "WARN: accumulate_all_bounded_escape") ||
		!strings.Contains(errText, "without a bounded timeout escape") {
		t.Fatalf("verify stderr missing accumulator bounded-escape warning:\n%s", errText)
	}
	if strings.Contains(errText, "accumulator_input_producer_path") {
		t.Fatalf("verify stderr reported no-producer error despite external source:\n%s", errText)
	}
}

func TestRunVerifyCommand_FailsForAccumulateTimeoutWithoutTimeoutMS(t *testing.T) {
	root := writeVerifyAccumulatorSafetyCommandFixture(t, verifyAccumulatorSafetyCommandFixtureOptions{
		eventSource: "external (verify accumulator safety proof)",
		completion:  "timeout",
	})

	var stdout, stderr bytes.Buffer
	code := runVerifyCommandWithContractsOutputForTest(context.Background(), repoRoot(), root, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("expected non-zero exit code, stdout = %q stderr = %q", stdout.String(), stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != "" {
		t.Fatalf("verify stdout = %q, want empty for hard invalidity", stdout.String())
	}
	errText := stderr.String()
	for _, want := range []string{
		"verify failed: boot verification failed:",
		"ERROR: accumulator_timeout_requires_timeout_ms",
		"without positive timeout_ms",
	} {
		if !strings.Contains(errText, want) {
			t.Fatalf("verify stderr missing %q:\n%s", want, errText)
		}
	}
	if strings.Contains(errText, "accumulator_input_producer_path") {
		t.Fatalf("verify stderr reported no-producer error despite external source:\n%s", errText)
	}
}

func TestRunVerifyCommand_FailsForAccumulatorInputWithoutProducerPath(t *testing.T) {
	root := writeVerifyAccumulatorSafetyCommandFixture(t, verifyAccumulatorSafetyCommandFixtureOptions{
		completion: "timeout",
		timeoutMS:  5000,
	})

	var stdout, stderr bytes.Buffer
	code := runVerifyCommandWithContractsOutputForTest(context.Background(), repoRoot(), root, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("expected non-zero exit code, stdout = %q stderr = %q", stdout.String(), stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != "" {
		t.Fatalf("verify stdout = %q, want empty for hard invalidity", stdout.String())
	}
	errText := stderr.String()
	for _, want := range []string{
		"verify failed: boot verification failed:",
		"ERROR: accumulator_input_producer_path",
		"no accepted producer/source path",
	} {
		if !strings.Contains(errText, want) {
			t.Fatalf("verify stderr missing %q:\n%s", want, errText)
		}
	}
}

type verifyAccumulatorSafetyCommandFixtureOptions struct {
	eventSource string
	completion  string
	timeoutMS   int
}

func writeVerifyAccumulatorSafetyCommandFixture(t *testing.T, opts verifyAccumulatorSafetyCommandFixtureOptions) string {
	t.Helper()
	root := t.TempDir()
	completion := strings.TrimSpace(opts.completion)
	if completion == "" {
		completion = "all"
	}
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
	timeoutLine := ""
	if opts.timeoutMS > 0 {
		timeoutLine = fmt.Sprintf("        timeout_ms: %d\n", opts.timeoutMS)
	}
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
accumulator:
  id: accumulator
  execution_type: system_node
  subscribes_to: [item.arrived]
  event_handlers:
    item.arrived:
      accumulate:
        expected_from: entity.expected_count
        completion: `+completion+`
`+timeoutLine+`      advances_to: done
  state_schema:
    fields:
      expected_count: integer
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
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	fixtureRoot := filepath.Join(repoRoot, relativeRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides(%s): %v", fixtureRoot, err)
	}
	return bundle
}

func seedServeRuntimeBundleCatalog(t *testing.T, ctx context.Context, pg *store.PostgresStore, relativeRoot string) string {
	t.Helper()
	return seedServeRuntimeBundleCatalogFromBundle(t, ctx, pg, relativeRoot, loadWorkflowValidationFixtureBundle(t, relativeRoot))
}

func seedServeRuntimeBundleCatalogRoot(t *testing.T, ctx context.Context, pg *store.PostgresStore, root string) string {
	t.Helper()
	return seedServeRuntimeBundleCatalogFromBundle(t, ctx, pg, root, loadWorkflowValidationBundleAt(t, root))
}

func seedServeRuntimeBundleCatalogFromBundle(t *testing.T, ctx context.Context, pg *store.PostgresStore, label string, bundle *runtimecontracts.WorkflowContractBundle) string {
	t.Helper()
	if pg == nil {
		t.Fatal("postgres store is required")
	}
	if _, err := pg.BindSchemaCapabilities(ctx); err != nil {
		t.Fatalf("BindSchemaCapabilities: %v", err)
	}
	projection, err := runtimecontracts.BuildBundleCatalogProjection(bundle)
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection(%s): %v", label, err)
	}
	if _, err := pg.UpsertBundleCatalog(ctx, store.BundleCatalogUpsert{
		BundleHash:  projection.BundleHash,
		ContentYAML: projection.ContentYAML,
		ParsedJSON:  projection.ParsedJSON,
		DataBlob:    projection.DataBlob,
		Metadata:    projection.Metadata,
	}); err != nil {
		t.Fatalf("UpsertBundleCatalog(%s): %v", label, err)
	}
	return projection.BundleHash
}

func loadWorkflowValidationBundleAt(t *testing.T, fixtureRoot string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
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
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), fmt.Sprintf(`
name: %s
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows: []
`, workflowName))
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `
initial_state: pending
terminal_states: [done]
states: [pending, done]
pins:
  inputs:
    events: [agent.requested]
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), `
agent.requested:
  swarm:
    source: external
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), fmt.Sprintf(`
%s:
  id: %s
  role: %s
  prompt_ref: %s
  model: regular
  mode: task
  subscriptions: [agent.requested]
`, agentKey, agentID, agentID, agentID))
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "prompts", agentID+".md"), "Handle assigned work.\n")
	return root
}

func writeArtifactRepoCommitServeFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: artifact-root-startup
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows: []
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `
initial_state: ready
terminal_states: [done]
states: [ready, done]
pins:
  inputs:
    events: [artifact.requested]
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "entities.yaml"), `
core:
  repo_url:
    type: text
    _unused_reason: artifact startup admission proof output field
  current_ref:
    type: text
    _unused_reason: artifact startup admission proof output field
  file_manifest:
    type: text
    _unused_reason: artifact startup admission proof output field
  status:
    type: text
    _unused_reason: artifact startup admission proof output field
  failure_reason:
    type: text
    _unused_reason: artifact startup admission proof output field
  last_request_id:
    type: text
    _unused_reason: artifact startup admission proof output field
  last_source_event_id:
    type: text
    _unused_reason: artifact startup admission proof output field
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), `
artifact.requested:
  swarm:
    source: external
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
artifact-writer:
  id: artifact-writer
  execution_type: system_node
  subscribes_to: [artifact.requested]
  event_handlers:
    artifact.requested:
      action:
        id: artifact_repo_commit
        artifact_repo:
          provider: local_git
          repo_id:
            literal: "11111111-1111-1111-1111-111111111111"
          namespace:
            literal: local-proof
          request_id:
            literal: "22222222-2222-2222-2222-222222222222"
          allowed_paths:
            - readme.md
          files:
            - path:
                literal: readme.md
              content:
                literal: "# Demo\n"
              content_type: markdown
          output:
            repo_url: repo_url
            current_ref: current_ref
            file_manifest: file_manifest
            status: status
            failure_reason: failure_reason
            last_request_id: last_request_id
            last_source_event_id: last_source_event_id
`)
	return root
}

func writeVerifyModelAliasFixture(t *testing.T, root, model string) {
	t.Helper()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: verify-model-alias
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: child
    flow: child
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `name: verify-model-alias`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "entities.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "schema.yaml"), `
name: child
initial_state: idle
terminal_states: [done]
states: [idle, done]
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "entities.yaml"), `
case: {}
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "agents.yaml"), fmt.Sprintf(`
worker:
  id: worker
  type: factory
  role: worker
  prompt_ref: worker
  model: %s
  mode: task
  subscriptions: [task.assigned]
`, model))
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "events.yaml"), `
task.assigned:
  swarm:
    source: external (verify model alias test)
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "nodes.yaml"), `
closer:
  id: closer
  execution_type: system_node
  subscribes_to: [task.assigned]
  event_handlers:
    task.assigned:
      advances_to: done
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "prompts", "worker.md"), `Handle the task.`)
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

func firstWorkflowValidationFlowHandler(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle) (string, string, string, runtimecontracts.SystemNodeEventHandler) {
	t.Helper()
	for _, view := range bundle.FlowViews() {
		flowID := strings.TrimSpace(view.Paths.ID)
		for nodeID, node := range view.Nodes {
			for eventType, handler := range node.EventHandlers {
				return flowID, nodeID, eventType, handler
			}
		}
	}
	t.Fatal("expected fixture to include at least one flow handler")
	return "", "", "", runtimecontracts.SystemNodeEventHandler{}
}

func writeWorkflowValidationFlowHandler(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle, flowID, nodeID, eventType string, handler runtimecontracts.SystemNodeEventHandler) {
	t.Helper()
	flowView, ok := bundle.FlowViewByID(flowID)
	if !ok || flowView == nil {
		t.Fatalf("flow view %s missing", flowID)
	}
	node := flowView.Nodes[nodeID]
	node.EventHandlers[eventType] = handler
	flowView.Nodes[nodeID] = node
	if bundle.Nodes == nil {
		bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{}
	}
	bundle.Nodes[nodeID] = node
	if bundle.Semantics.NodeHandlers == nil {
		bundle.Semantics.NodeHandlers = map[string]map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	if bundle.Semantics.NodeHandlers[nodeID] == nil {
		bundle.Semantics.NodeHandlers[nodeID] = map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	bundle.Semantics.NodeHandlers[nodeID][eventType] = handler
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
			verifyErr := verifyBundle(context.Background(), source)
			if tc.wantErr {
				if verifyErr == nil || !strings.Contains(verifyErr.Error(), tc.errContains) {
					t.Fatalf("verifyBundle error = %v, want substring %q", verifyErr, tc.errContains)
				}
			} else if verifyErr != nil {
				t.Fatalf("verifyBundle error = %v, want nil", verifyErr)
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

func TestServeBootRegistryDetail_UsesRuntimeToolInventoryCount(t *testing.T) {
	source := semanticview.Wrap(testWorkflowValidationBundle())
	wantTools := len(runtimetools.RuntimeAvailableToolNamesForSource(source))
	if wantTools == 0 {
		t.Fatal("runtime tool inventory unexpectedly empty")
	}

	out := serveBootBundleLoadDetail("sha256:test", source)
	if !strings.Contains(out, "sha256:test") {
		t.Fatalf("boot progress detail missing fingerprint:\n%s", out)
	}
	if !strings.Contains(out, fmt.Sprintf("tools=%d", wantTools)) {
		t.Fatalf("log output missing runtime tool count %d:\n%s", wantTools, out)
	}
	if strings.Contains(out, "tools=0") {
		t.Fatalf("log output still reports zero tools:\n%s", out)
	}
}

func TestSummarizeServeSchemaPlansDefaultOmitsTableBreakdown(t *testing.T) {
	plans := []store.SchemaTableDDL{
		{TableName: "events", ColumnCount: 17},
		{TableName: "runs", ColumnCount: 14},
	}

	summary, logOutput := captureServeSchemaPlanSummary(t, plans, false)

	if summary != "verified 2 generated tables" {
		t.Fatalf("summary = %q, want concise generated-table count", summary)
	}
	for _, forbidden := range []string{"events(17)", "runs(14)", "detail="} {
		if strings.Contains(summary, forbidden) || strings.Contains(logOutput, forbidden) {
			t.Fatalf("default schema summary leaked %q\nsummary=%s\nlog=%s", forbidden, summary, logOutput)
		}
	}
	for _, want := range []string{"swarm boot state stores", "tables=2", "columns=31"} {
		if !strings.Contains(logOutput, want) {
			t.Fatalf("default schema log missing %q:\n%s", want, logOutput)
		}
	}
}

func TestSummarizeServeSchemaPlansVerboseRetainsTableBreakdown(t *testing.T) {
	plans := []store.SchemaTableDDL{
		{TableName: "runs", ColumnCount: 14},
		{TableName: "events", ColumnCount: 17},
	}

	summary, logOutput := captureServeSchemaPlanSummary(t, plans, true)

	if summary != "verified 2 generated tables (events(17), runs(14))" {
		t.Fatalf("summary = %q, want sorted verbose table breakdown", summary)
	}
	for _, forbidden := range []string{"events(17)", "runs(14)", "detail="} {
		if strings.Contains(logOutput, forbidden) {
			t.Fatalf("process log leaked verbose schema detail %q:\n%s", forbidden, logOutput)
		}
	}
}

func TestSummarizeServeSchemaPlansZeroPlans(t *testing.T) {
	summary, logOutput := captureServeSchemaPlanSummary(t, nil, true)

	if summary != "verified 0 generated tables" {
		t.Fatalf("summary = %q, want zero generated-table summary", summary)
	}
	for _, want := range []string{"tables=0", "columns=0"} {
		if !strings.Contains(logOutput, want) {
			t.Fatalf("zero-plan schema log missing %q:\n%s", want, logOutput)
		}
	}
	if strings.Contains(logOutput, "detail=") {
		t.Fatalf("zero-plan schema log emitted empty detail:\n%s", logOutput)
	}
}

func TestInitializeServeSchemaStateStoresConsumeVerboseOwner(t *testing.T) {
	ctx := context.Background()
	bundle := loadStoreBackendSelectionWorkflowBundle(t)
	stores := storeBundle{SchemaBootstrapper: recordingSchemaBootstrapper{}}

	defaultSummary, err := initializeStateStores(ctx, stores, bundle, false)
	if err != nil {
		t.Fatalf("initializeStateStores(default): %v", err)
	}
	if strings.Contains(defaultSummary, "(") {
		t.Fatalf("default loaded-bundle state store summary leaked table detail:\n%s", defaultSummary)
	}

	verboseSummary, err := initializeStateStores(ctx, stores, bundle, true)
	if err != nil {
		t.Fatalf("initializeStateStores(verbose): %v", err)
	}
	if !strings.Contains(verboseSummary, "(") || !strings.Contains(verboseSummary, ")") {
		t.Fatalf("verbose loaded-bundle state store summary missing table detail:\n%s", verboseSummary)
	}

	loadedDefaultSummaries, err := initializeLoadedServeRuntimeStateStores(ctx, stores, []serveRuntimeBundle{{bundle: bundle}, {bundle: bundle}}, false)
	if err != nil {
		t.Fatalf("initializeLoadedServeRuntimeStateStores(default): %v", err)
	}
	for _, summary := range loadedDefaultSummaries {
		if strings.Contains(summary, "(") {
			t.Fatalf("default loaded runtime state store summary leaked table detail:\n%v", loadedDefaultSummaries)
		}
	}

	loadedVerboseSummaries, err := initializeLoadedServeRuntimeStateStores(ctx, stores, []serveRuntimeBundle{{bundle: bundle}}, true)
	if err != nil {
		t.Fatalf("initializeLoadedServeRuntimeStateStores(verbose): %v", err)
	}
	if len(loadedVerboseSummaries) != 1 || !strings.Contains(loadedVerboseSummaries[0], "(") || !strings.Contains(loadedVerboseSummaries[0], ")") {
		t.Fatalf("verbose loaded runtime state store summary missing table detail:\n%v", loadedVerboseSummaries)
	}

	defaultPlatformSummary, err := initializeServePlatformStateStores(ctx, stores, filepath.Join(repoRoot(), defaultPlatformSpecPath), false)
	if err != nil {
		t.Fatalf("initializeServePlatformStateStores(default): %v", err)
	}
	if strings.Contains(defaultPlatformSummary, "(") {
		t.Fatalf("default pre-catalog platform state store summary leaked table detail:\n%s", defaultPlatformSummary)
	}

	verbosePlatformSummary, err := initializeServePlatformStateStores(ctx, stores, filepath.Join(repoRoot(), defaultPlatformSpecPath), true)
	if err != nil {
		t.Fatalf("initializeServePlatformStateStores(verbose): %v", err)
	}
	if !strings.Contains(verbosePlatformSummary, "(") || !strings.Contains(verbosePlatformSummary, ")") {
		t.Fatalf("verbose pre-catalog platform state store summary missing table detail:\n%s", verbosePlatformSummary)
	}
}

func TestInitializeStateStoresDoesNotPlanGeneratedEntityTables(t *testing.T) {
	ctx := context.Background()
	bundle := workflowBundleWithGeneratedEntitySchemaForStateStoreTest(t)
	recorder := &capturingSchemaBootstrapper{}

	summary, err := initializeStateStores(ctx, storeBundle{SchemaBootstrapper: recorder}, bundle, true)
	if err != nil {
		t.Fatalf("initializeStateStores: %v", err)
	}
	if !schemaPlanContainsTable(recorder.plans, "entity_state") {
		t.Fatal("normal boot schema plan missing canonical entity_state table")
	}
	if schemaPlanContainsTable(recorder.plans, "products") {
		t.Fatalf("normal boot schema plan included generated typed entity table products: %s", summary)
	}
}

func TestInitializeStateStoresSQLiteDoesNotCreateGeneratedEntityTables(t *testing.T) {
	ctx := context.Background()
	bundle := workflowBundleWithGeneratedEntitySchemaForStateStoreTest(t)
	sqliteStore, err := store.NewSQLiteRuntimeStore(filepath.Join(t.TempDir(), "dev.db"))
	if err != nil {
		t.Fatalf("NewSQLiteRuntimeStore: %v", err)
	}
	t.Cleanup(func() {
		if err := sqliteStore.Close(); err != nil {
			t.Fatalf("close sqlite store: %v", err)
		}
	})

	if _, err := initializeStateStores(ctx, storeBundle{SchemaBootstrapper: sqliteStore}, bundle, false); err != nil {
		t.Fatalf("initializeStateStores(sqlite): %v", err)
	}
	if !sqliteMainTestTableExists(t, sqliteStore.DB, "entity_state") {
		t.Fatal("sqlite normal boot missing canonical entity_state table")
	}
	if sqliteMainTestTableExists(t, sqliteStore.DB, "products") {
		t.Fatal("sqlite normal boot created misleading generated typed entity table products")
	}
}

func TestInitializeStateStoresPostgresDoesNotCreateGeneratedEntityTables(t *testing.T) {
	ctx := context.Background()
	bundle := workflowBundleWithGeneratedEntitySchemaForStateStoreTest(t)
	dsn, db, cleanup := testutil.StartEmptyPostgres(t)
	t.Cleanup(cleanup)
	pg, err := store.NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() {
		if err := pg.DB.Close(); err != nil {
			t.Fatalf("close postgres store: %v", err)
		}
	})

	if _, err := initializeStateStores(ctx, selectedPostgresStoreBundle(pg, &config.Config{}), bundle, false); err != nil {
		t.Fatalf("initializeStateStores(postgres): %v", err)
	}
	if !postgresMainTestTableExists(t, db, "entity_state") {
		t.Fatal("postgres normal boot missing canonical entity_state table")
	}
	if postgresMainTestTableExists(t, db, "products") {
		t.Fatal("postgres normal boot created misleading generated typed entity table products")
	}
}

func TestServeRuntimeStateStoreSummaryDedupesSchemaSummaries(t *testing.T) {
	got := serveRuntimeStateStoreSummary([]serveRuntimeBundleContext{
		{stateStoreSummary: "verified 2 generated tables"},
		{stateStoreSummary: "verified 2 generated tables"},
		{stateStoreSummary: "verified 2 generated tables (events(17), runs(14))"},
		{stateStoreSummary: " "},
	})

	if strings.Count(got, "verified 2 generated tables") != 2 {
		t.Fatalf("summary = %q, want one concise and one verbose summary after de-dupe", got)
	}
	if strings.Count(got, "events(17)") != 1 {
		t.Fatalf("summary = %q, want one verbose table detail after de-dupe", got)
	}
}

func captureServeSchemaPlanSummary(t *testing.T, plans []store.SchemaTableDDL, verbose bool) (string, string) {
	t.Helper()
	var logOutput bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() {
		slog.SetDefault(previous)
	})
	return summarizeServeSchemaPlans(plans, verbose), logOutput.String()
}

type recordingSchemaBootstrapper struct{}

func (recordingSchemaBootstrapper) EnsureSchemaTables(context.Context, []store.SchemaTableDDL) error {
	return nil
}

func (recordingSchemaBootstrapper) ResolveSchemaCapabilities(context.Context) (store.StoreSchemaCapabilities, error) {
	return store.StoreSchemaCapabilities{}, nil
}

type capturingSchemaBootstrapper struct {
	plans []store.SchemaTableDDL
}

func (c *capturingSchemaBootstrapper) EnsureSchemaTables(_ context.Context, plans []store.SchemaTableDDL) error {
	c.plans = append([]store.SchemaTableDDL{}, plans...)
	return nil
}

func (c *capturingSchemaBootstrapper) ResolveSchemaCapabilities(context.Context) (store.StoreSchemaCapabilities, error) {
	return store.StoreSchemaCapabilities{}, nil
}

func workflowBundleWithGeneratedEntitySchemaForStateStoreTest(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	spec, err := loadServePlatformSpecDocument(filepath.Join(repoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("load platform spec: %v", err)
	}
	return &runtimecontracts.WorkflowContractBundle{
		Platform: spec,
		Semantics: runtimecontracts.WorkflowSemanticView{
			EntitySchema: runtimecontracts.EntitySchema{
				Groups: []runtimecontracts.EntitySchemaGroup{{
					Name: "products",
					Fields: []runtimecontracts.EntitySchemaField{
						{Name: "slug", Type: "text", Indexed: true},
						{Name: "score", Type: "numeric(12,2)", Nullable: true},
					},
				}},
			},
		},
	}
}

func schemaPlanContainsTable(plans []store.SchemaTableDDL, tableName string) bool {
	for _, plan := range plans {
		if strings.EqualFold(strings.TrimSpace(plan.TableName), tableName) {
			return true
		}
	}
	return false
}

func sqliteMainTestTableExists(t *testing.T, db *sql.DB, tableName string) bool {
	t.Helper()
	var name string
	err := db.QueryRowContext(context.Background(), `
		SELECT name
		FROM sqlite_master
		WHERE type='table' AND name=?
	`, tableName).Scan(&name)
	if err == sql.ErrNoRows {
		return false
	}
	if err != nil {
		t.Fatalf("check sqlite table %s: %v", tableName, err)
	}
	return strings.TrimSpace(name) != ""
}

func postgresMainTestTableExists(t *testing.T, db *sql.DB, tableName string) bool {
	t.Helper()
	var exists bool
	if err := db.QueryRowContext(context.Background(), `SELECT to_regclass($1)::text IS NOT NULL`, "public."+tableName).Scan(&exists); err != nil {
		t.Fatalf("check postgres table %s: %v", tableName, err)
	}
	return exists
}

func TestWaitForServeHealthEndpointsProvesHealthAndReadyRoutes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz", "/readyz":
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	if err := waitForServeHealthEndpoints(context.Background(), server.Listener.Addr()); err != nil {
		t.Fatalf("waitForServeHealthEndpoints: %v", err)
	}
}

func TestRunServeRuntimeVerboseEmitsPlatformSpecBootSequence(t *testing.T) {
	steps := loadServeBootProgressSequenceFromSpec(t)
	if got, want := len(steps), runtimepkg.BootProgressTotalSteps; got != want {
		t.Fatalf("serve boot progress spec step count = %d, want %d", got, want)
	}

	oldBuildStores := buildStoresForServe
	oldWorkspaceLifecycle := configuredWorkspaceLifecycleForServe
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	runtimePG, err := store.NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (storeBundle, error) {
		if _, err := runtimePG.BindSchemaCapabilities(ctx); err != nil {
			return storeBundle{}, err
		}
		return selectedPostgresStoreBundle(runtimePG, cfg), nil
	}
	configuredWorkspaceLifecycleForServe = func(*sql.DB, string, semanticview.Source, workspaceMountSources, workspaceBackendSelection) (serveWorkspaceLifecycle, error) {
		return serveRuntimeWorkspaceStub{}, nil
	}
	t.Cleanup(func() {
		buildStoresForServe = oldBuildStores
		configuredWorkspaceLifecycleForServe = oldWorkspaceLifecycle
	})

	serve := startServeRuntimeTestProcess(t, serveOptions{
		ConfigPath:         writeServeRuntimeTestConfig(t),
		ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "postgres",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: true,
		Verbose:            true,
	})

	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("runServeRuntime code = %d\noutput:\n%s", code, serve.outputString())
	}

	rows := parseServeBootProgressRows(t, serve.outputString())
	if got, want := len(rows), len(steps); got != want {
		t.Fatalf("serve boot progress rows = %d, want %d\noutput:\n%s", got, want, serve.outputString())
	}
	for i, want := range steps {
		got := rows[i]
		if got.Step != want.Step || got.Total != runtimepkg.BootProgressTotalSteps || got.Name != want.Name {
			t.Fatalf("row %d = step=%d total=%d name=%q, want step=%d total=%d name=%q\noutput:\n%s", i, got.Step, got.Total, got.Name, want.Step, runtimepkg.BootProgressTotalSteps, want.Name, serve.outputString())
		}
	}
	if strings.Contains(serve.outputString(), "health_endpoints_respond       ok  (/healthz /readyz /v1/rpc /v1/ws)") {
		t.Fatalf("serve verbose output still claims unproven v1 endpoint response:\n%s", serve.outputString())
	}
	for _, want := range []string{"http_listener_bind", "api_listener=", "api_routes=" + serveAPIRoutes, "mcp_listener=", "mcp_routes=" + serveMCPRoutes, "health_endpoints_respond", serveReadinessRoutes} {
		if !strings.Contains(serve.outputString(), want) {
			t.Fatalf("serve verbose output missing %q:\n%s", want, serve.outputString())
		}
	}
	if strings.Contains(serve.outputString(), "health=127.") {
		t.Fatalf("serve verbose output still labels the unified listener as health-only:\n%s", serve.outputString())
	}
}

func TestRunServeRuntimeListenerBindFailuresExitBeforeReadiness(t *testing.T) {
	for _, tt := range []struct {
		name      string
		occupyAPI bool
	}{
		{name: "api listener", occupyAPI: true},
		{name: "mcp listener", occupyAPI: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
			t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
			oldBuildStores := buildStoresForServe
			oldWorkspaceLifecycle := configuredWorkspaceLifecycleForServe
			dsn, _, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			runtimePG, err := store.NewPostgresStore(dsn)
			if err != nil {
				t.Fatalf("NewPostgresStore: %v", err)
			}
			buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (storeBundle, error) {
				if _, err := runtimePG.BindSchemaCapabilities(ctx); err != nil {
					return storeBundle{}, err
				}
				return selectedPostgresStoreBundle(runtimePG, cfg), nil
			}
			configuredWorkspaceLifecycleForServe = func(*sql.DB, string, semanticview.Source, workspaceMountSources, workspaceBackendSelection) (serveWorkspaceLifecycle, error) {
				return serveRuntimeWorkspaceStub{}, nil
			}
			t.Cleanup(func() {
				buildStoresForServe = oldBuildStores
				configuredWorkspaceLifecycleForServe = oldWorkspaceLifecycle
			})

			occupied, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("occupy listener: %v", err)
			}
			defer occupied.Close()
			apiAddr := "127.0.0.1:0"
			mcpAddr := "127.0.0.1:0"
			if tt.occupyAPI {
				apiAddr = occupied.Addr().String()
			} else {
				mcpAddr = occupied.Addr().String()
			}

			var out lockedBuffer
			code := runServeRuntime(context.Background(), repoRoot(), serveOptions{
				ConfigPath:         writeServeRuntimeTestConfig(t),
				ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
				PlatformSpecPath:   defaultPlatformSpecPath,
				StoreMode:          "postgres",
				APIListenAddr:      apiAddr,
				MCPListenAddr:      mcpAddr,
				SelfCheck:          true,
				RequireBundleMatch: true,
				Verbose:            true,
				Output:             &out,
			})
			if code != 3 {
				t.Fatalf("runServeRuntime code = %d, want 3\noutput:\n%s", code, out.String())
			}
			if !strings.Contains(out.String(), "http_listener_bind") || !strings.Contains(out.String(), "FAILED") {
				t.Fatalf("serve output missing bind failure proof:\n%s", out.String())
			}
			if strings.Contains(out.String(), "ready                      ok") {
				t.Fatalf("serve reported readiness after listener bind failure:\n%s", out.String())
			}
		})
	}
}

func TestCreateServeToolGatewayBindingAlignsToMCPListenerWithoutMutatingURLEnv(t *testing.T) {
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen mcp: %v", err)
	}
	defer listener.Close()
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split listener addr: %v", err)
	}

	binding, err := createServeToolGatewayBinding(listener.Addr())
	if err != nil {
		t.Fatalf("create gateway binding: %v", err)
	}
	if got, want := binding.HostEndpoint, "http://127.0.0.1:"+port; got != want {
		t.Fatalf("HostEndpoint = %q, want %q", got, want)
	}
	if got, want := binding.WorkspaceEndpoint, "http://host.docker.internal:"+port; got != want {
		t.Fatalf("WorkspaceEndpoint = %q, want %q", got, want)
	}
	if strings.TrimSpace(binding.Token) == "" {
		t.Fatal("binding token was not generated")
	}
	if got, want := len(binding.Token), base64.RawURLEncoding.EncodedLen(toolgateway.AuthTokenBytes); got != want {
		t.Fatalf("binding token length = %d, want %d", got, want)
	}
	if got := os.Getenv("SWARM_TOOL_GATEWAY_URL"); got != "" {
		t.Fatalf("SWARM_TOOL_GATEWAY_URL = %q, want empty", got)
	}
	if got := os.Getenv("SWARM_TOOL_GATEWAY_CONTAINER_URL"); got != "" {
		t.Fatalf("SWARM_TOOL_GATEWAY_CONTAINER_URL = %q, want empty", got)
	}
	if got := os.Getenv("SWARM_TOOL_GATEWAY_TOKEN"); got != "" {
		t.Fatalf("SWARM_TOOL_GATEWAY_TOKEN = %q, want empty", got)
	}
}

func TestCreateServeToolGatewayBindingRejectsRetiredGatewayTokenEnv(t *testing.T) {
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "operator-token")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen mcp: %v", err)
	}
	defer listener.Close()

	_, err = createServeToolGatewayBinding(listener.Addr())
	if err == nil || !strings.Contains(err.Error(), "SWARM_TOOL_GATEWAY_TOKEN is retired") || !strings.Contains(err.Error(), "ToolGatewayBinding") {
		t.Fatalf("create gateway binding error = %v, want retired token env rejection", err)
	}
}

func TestRunServeRuntimeDevClaudeCLIStaleGatewayEnvUsesTypedBinding(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	stubServeWorkspaceLifecycleForTest(t)
	mcpPort := freeDoctorTCPPort(t)
	mcpAddr := "127.0.0.1:" + mcpPort
	stalePort := staleGatewayTestPort(mcpPort)
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "http://127.0.0.1:"+stalePort)
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "http://host.docker.internal:"+stalePort)
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")

	bindingCh := make(chan toolgateway.Binding, 1)
	opts := serveOptions{
		ConfigPath:         writeDoctorClaudeConfig(t),
		Backend:            "claude_cli",
		ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		DataSource:         t.TempDir(),
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "sqlite",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      mcpAddr,
		SelfCheck:          true,
		RequireBundleMatch: false,
		Verbose:            true,
		Dev:                true,
		TestRuntimeReadyHook: func(rt *runtimepkg.Runtime) {
			bindingCh <- rt.Options.ToolGatewayBinding
		},
	}
	assertServePreflightStaleGatewayWarning(t, opts, "serve_dev")

	process := startServeRuntimeTestProcess(t, opts)
	process.waitForReadyLine()
	binding := receiveToolGatewayBinding(t, bindingCh, process.outputString())
	assertToolGatewayBindingUsesMCPPort(t, binding, mcpPort, stalePort)
	if code := process.stop(); code != 0 {
		t.Fatalf("serve exited with code %d, want 0\noutput:\n%s", code, process.outputString())
	}
}

func TestStartLocalRunServeClaudeCLIStaleGatewayEnvUsesTypedBinding(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	stubServeWorkspaceLifecycleForTest(t)
	apiPortText := freeDoctorTCPPort(t)
	apiPort, err := strconv.Atoi(apiPortText)
	if err != nil {
		t.Fatalf("parse api port %q: %v", apiPortText, err)
	}
	mcpPort := freeDoctorTCPPort(t)
	mcpAddr := "127.0.0.1:" + mcpPort
	stalePort := staleGatewayTestPort(mcpPort)
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "http://127.0.0.1:"+stalePort)
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "http://host.docker.internal:"+stalePort)
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")

	bindingCh := make(chan toolgateway.Binding, 1)
	serveStarted := make(chan serveOptions, 1)
	apiOpts := defaultRootCommandOptions()
	apiOpts.apiRPCEndpointOverride = "http://127.0.0.1:" + apiPortText + "/v1/rpc"
	apiOpts.runReadyTimeout = serveRuntimeReadyTimeout
	apiOpts.runReadyPoll = 10 * time.Millisecond
	apiOpts.runServe = func(ctx context.Context, repo string, serveOpts serveOptions) int {
		if !serveOpts.LocalRun {
			t.Errorf("startLocalRunServe produced LocalRun = false, want shared run_local preflight consumer")
		}
		if serveOpts.APIListenAddr != "127.0.0.1:"+apiPortText {
			t.Errorf("startLocalRunServe APIListenAddr = %q, want 127.0.0.1:%s", serveOpts.APIListenAddr, apiPortText)
		}
		serveOpts.MCPListenAddr = mcpAddr
		serveOpts.Verbose = true
		serveOpts.TestRuntimeReadyHook = func(rt *runtimepkg.Runtime) {
			bindingCh <- rt.Options.ToolGatewayBinding
		}
		assertServePreflightStaleGatewayWarning(t, serveOpts, "run_local")
		serveStarted <- serveOpts
		return runServeRuntime(ctx, repo, serveOpts)
	}

	stop, err := startLocalRunServe(context.Background(), repoRoot(), runCommandOptions{
		apiOptions:       apiOpts,
		configPath:       writeDoctorClaudeConfig(t),
		backend:          "claude_cli",
		contractsPath:    filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		dataSource:       t.TempDir(),
		platformSpecPath: defaultPlatformSpecPath,
		apiPort:          apiPort,
	})
	if err != nil {
		t.Fatalf("startLocalRunServe: %v", err)
	}
	defer stop()
	select {
	case <-serveStarted:
	case <-time.After(serveRuntimeReadyTimeout):
		t.Fatal("startLocalRunServe did not invoke the serve owner")
	}
	binding := receiveToolGatewayBinding(t, bindingCh, "")
	assertToolGatewayBindingUsesMCPPort(t, binding, mcpPort, stalePort)
}

func TestCreateServeToolGatewayBindingIgnoresStaleLocalURLEnv(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen mcp: %v", err)
	}
	defer listener.Close()
	_, mcpPort, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split listener addr: %v", err)
	}
	oldUnifiedPort := "8081"
	if mcpPort == oldUnifiedPort {
		oldUnifiedPort = "8080"
	}
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "http://127.0.0.1:"+oldUnifiedPort)
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "http://host.docker.internal:"+oldUnifiedPort)

	binding, err := createServeToolGatewayBinding(listener.Addr())
	if err != nil {
		t.Fatalf("create gateway binding: %v", err)
	}
	if strings.Contains(binding.HostEndpoint, oldUnifiedPort) || strings.Contains(binding.WorkspaceEndpoint, oldUnifiedPort) {
		t.Fatalf("binding = %#v, stale URL env leaked into binding", binding)
	}
}

func TestRunServeRuntimeNonDevClaudeCLIRetiredGatewayURLEnvFailsClosed(t *testing.T) {
	for _, tt := range []struct {
		name  string
		env   string
		value string
	}{
		{name: "host-url", env: "SWARM_TOOL_GATEWAY_URL", value: "http://127.0.0.1:" + freeDoctorTCPPort(t)},
		{name: "container-url", env: "SWARM_TOOL_GATEWAY_CONTAINER_URL", value: "http://host.docker.internal:" + freeDoctorTCPPort(t)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
			t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
			t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")
			t.Setenv(tt.env, tt.value)

			assertRunServeRuntimeRetiredGatewayURLAdmissionFailure(t, tt.env, serveOptions{
				ConfigPath:         writeDoctorClaudeConfig(t),
				Backend:            "claude_cli",
				ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
				DataSource:         t.TempDir(),
				PlatformSpecPath:   defaultPlatformSpecPath,
				StoreMode:          "not-a-store",
				APIListenAddr:      "127.0.0.1:0",
				MCPListenAddr:      "127.0.0.1:0",
				SelfCheck:          true,
				RequireBundleMatch: false,
			})
		})
	}
}

func TestRunServeRuntimeBundleHashRetiredGatewayURLEnvFailsBeforeStartupSideEffects(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "http://127.0.0.1:"+freeDoctorTCPPort(t))
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")

	assertRunServeRuntimeRetiredGatewayURLAdmissionFailure(t, "SWARM_TOOL_GATEWAY_URL", serveOptions{
		ConfigPath:         writeDoctorClaudeConfig(t),
		Backend:            "claude_cli",
		ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		DataSource:         t.TempDir(),
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "not-a-store",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: false,
		BundleHash:         "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	})
}

func TestRunServeRuntimeNonClaudeRetiredGatewayURLEnvFailsBeforeStartupSideEffects(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "http://host.docker.internal:"+freeDoctorTCPPort(t))
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")

	assertRunServeRuntimeRetiredGatewayURLAdmissionFailure(t, "SWARM_TOOL_GATEWAY_CONTAINER_URL", serveOptions{
		ConfigPath:         writeServeRuntimeTestConfig(t),
		Backend:            "anthropic",
		ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		DataSource:         t.TempDir(),
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "not-a-store",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: false,
	})
}

func assertRunServeRuntimeRetiredGatewayURLAdmissionFailure(t *testing.T, envName string, opts serveOptions) {
	t.Helper()
	var out lockedBuffer
	opts.Verbose = true
	opts.Output = &out
	code := runServeRuntime(context.Background(), repoRoot(), opts)
	if code != cliExitRuntime {
		t.Fatalf("runServeRuntime code = %d, want %d\noutput:\n%s", code, cliExitRuntime, out.String())
	}
	for _, want := range []string{
		"config_load",
		"serve_admission",
		envName,
		"retired",
		"unset " + envName,
		"ToolGatewayBinding",
		"non-dev serve rejects retired gateway URL env",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("serve output missing %q:\n%s", want, out.String())
		}
	}
	for _, notWant := range []string{"local_preflight", "not-a-store", "db_connection", "bundle_load", "http_listener_bind", "ready"} {
		if strings.Contains(out.String(), notWant) {
			t.Fatalf("serve reached %q after retired gateway URL env failure:\n%s", notWant, out.String())
		}
	}
}

func assertServePreflightStaleGatewayWarning(t *testing.T, opts serveOptions, wantMode string) {
	t.Helper()
	cfgResult, err := loadRuntimeConfigWithOptions(runtimeConfigLoadOptions{
		RepoRoot:        repoRoot(),
		ExplicitPath:    opts.ConfigPath,
		BackendOverride: opts.Backend,
	})
	if err != nil {
		t.Fatalf("load config for preflight proof: %v", err)
	}
	resolvedPaths, err := resolveCLIContractPlatformSpecPaths(repoRoot(), cliContractPlatformSpecPathOptions{
		ContractsPath:    opts.ContractsPath,
		PlatformSpecPath: opts.PlatformSpecPath,
	})
	if err != nil {
		t.Fatalf("resolve preflight paths: %v", err)
	}
	workspaceBackend, err := resolveWorkspaceBackend(opts.WorkspaceBackend, opts.WorkspaceBackendSet, cfgResult.Config)
	if err != nil {
		t.Fatalf("resolve workspace backend for preflight proof: %v", err)
	}
	report := runServeLocalClaudeCLIPreflight(context.Background(), repoRoot(), opts, cfgResult.Config, resolvedPaths, workspaceBackend, workspaceMountSources{DataSource: t.TempDir(), DataSourceSource: "test"})
	if report.Mode != wantMode {
		t.Fatalf("preflight mode = %q, want %q", report.Mode, wantMode)
	}
	for _, code := range []string{"swarm_tool_gateway_url_retired", "swarm_tool_gateway_container_url_retired"} {
		if !localPreflightReportHasFinding(report, code, localPreflightSeverityWarning, localPreflightStatusFailed) {
			t.Fatalf("preflight report missing warning %q:\n%#v", code, report)
		}
	}
	if report.HasBlockers() {
		t.Fatalf("stale local gateway URL env produced blockers, want warnings only:\n%#v", report)
	}
}

func localPreflightReportHasFinding(report localPreflightReport, code string, severity localPreflightSeverity, status localPreflightFindingStatus) bool {
	for _, finding := range report.Findings {
		if finding.Code == code && finding.Severity == severity && finding.Status == status {
			return true
		}
	}
	return false
}

func stubServeWorkspaceLifecycleForTest(t *testing.T) {
	t.Helper()
	oldWorkspaceLifecycle := configuredWorkspaceLifecycleForServe
	configuredWorkspaceLifecycleForServe = func(*sql.DB, string, semanticview.Source, workspaceMountSources, workspaceBackendSelection) (serveWorkspaceLifecycle, error) {
		return serveRuntimeWorkspaceStub{}, nil
	}
	t.Cleanup(func() {
		configuredWorkspaceLifecycleForServe = oldWorkspaceLifecycle
	})
}

func receiveToolGatewayBinding(t *testing.T, ch <-chan toolgateway.Binding, output string) toolgateway.Binding {
	t.Helper()
	select {
	case binding := <-ch:
		return binding
	case <-time.After(serveRuntimeReadyTimeout):
		t.Fatalf("timed out waiting for runtime tool gateway binding\noutput:\n%s", output)
		return toolgateway.Binding{}
	}
}

func assertToolGatewayBindingUsesMCPPort(t *testing.T, binding toolgateway.Binding, mcpPort, stalePort string) {
	t.Helper()
	if err := binding.Validate(); err != nil {
		t.Fatalf("runtime tool gateway binding is invalid: %v\nbinding=%#v", err, binding)
	}
	if got, want := binding.HostEndpoint, "http://127.0.0.1:"+mcpPort; got != want {
		t.Fatalf("binding HostEndpoint = %q, want %q", got, want)
	}
	if got, want := binding.WorkspaceEndpoint, "http://host.docker.internal:"+mcpPort; got != want {
		t.Fatalf("binding WorkspaceEndpoint = %q, want %q", got, want)
	}
	if strings.Contains(binding.HostEndpoint, stalePort) || strings.Contains(binding.WorkspaceEndpoint, stalePort) {
		t.Fatalf("runtime binding leaked stale URL env port %s: %#v", stalePort, binding)
	}
	if strings.TrimSpace(binding.Token) == "" {
		t.Fatalf("runtime binding token is empty: %#v", binding)
	}
}

func staleGatewayTestPort(mcpPort string) string {
	if strings.TrimSpace(mcpPort) == "8081" {
		return "8080"
	}
	return "8081"
}

func TestValidateServeGatewayURLEnvForNonDevRejectsAnyRetiredURLEnv(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value string
	}{
		{name: "SWARM_TOOL_GATEWAY_URL", value: "http://127.0.0.1:8082"},
		{name: "SWARM_TOOL_GATEWAY_CONTAINER_URL", value: "http://host.docker.internal:8082"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
			t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
			t.Setenv(tt.name, tt.value)
			err := validateServeGatewayURLEnvForNonDev()
			if err == nil {
				t.Fatal("non-dev gateway env validation unexpectedly accepted retired URL env")
			}
			for _, want := range []string{tt.name, "retired", "unset " + tt.name, "ToolGatewayBinding"} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error = %v, want %q", err, want)
				}
			}
			if strings.Contains(err.Error(), "MCP listener port") {
				t.Fatalf("error still suggests port-matching URL env is valid: %v", err)
			}
		})
	}
}

func TestRunServeRuntimeAbandonActiveRunsQuiescesBeforeBundleMatchAdmission(t *testing.T) {
	oldBuildStores := buildStoresForServe
	oldWorkspaceLifecycle := configuredWorkspaceLifecycleForServe
	dsn, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	runtimePG, err := store.NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (storeBundle, error) {
		if _, err := runtimePG.BindSchemaCapabilities(ctx); err != nil {
			return storeBundle{}, err
		}
		return selectedPostgresStoreBundle(runtimePG, cfg), nil
	}
	configuredWorkspaceLifecycleForServe = func(*sql.DB, string, semanticview.Source, workspaceMountSources, workspaceBackendSelection) (serveWorkspaceLifecycle, error) {
		return serveRuntimeWorkspaceStub{}, nil
	}
	t.Cleanup(func() {
		buildStoresForServe = oldBuildStores
		configuredWorkspaceLifecycleForServe = oldWorkspaceLifecycle
	})

	ctx := context.Background()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	activeSessionID := uuid.NewString()
	mismatchFingerprint := "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_fingerprint, started_at)
		VALUES ($1::uuid, 'running', $2, now())
	`, runID, mismatchFingerprint); err != nil {
		t.Fatalf("seed active mismatched run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			event_id, run_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'serve.abandon.test', 'global', '{}'::jsonb, 'test', 'agent', now()
		)
	`, eventID, runID); err != nil {
		t.Fatalf("seed active delivery event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, active_session_id, reason_code, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'agent', 'agent-a', 'in_progress', $3::uuid, 'agent_processing', now()
		)
	`, runID, eventID, activeSessionID); err != nil {
		t.Fatalf("seed active delivery: %v", err)
	}

	serve := startServeRuntimeTestProcess(t, serveOptions{
		ConfigPath:         writeServeRuntimeTestConfig(t),
		ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "postgres",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: true,
		AbandonActiveRuns:  true,
		Verbose:            true,
	})

	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("runServeRuntime code = %d\noutput:\n%s", code, serve.outputString())
	}

	var runStatus, controlStatus, reason, controlledBy string
	if err := db.QueryRowContext(context.Background(), `
		SELECT r.status, rc.control_status, COALESCE(rc.reason, ''), COALESCE(rc.controlled_by, '')
		FROM runs r
		JOIN run_control_state rc ON rc.run_id = r.run_id
		WHERE r.run_id = $1::uuid
	`, runID).Scan(&runStatus, &controlStatus, &reason, &controlledBy); err != nil {
		t.Fatalf("load run/control state: %v", err)
	}
	if runStatus != "cancelled" || controlStatus != "stopped" || reason != runtimerunquiescence.ServeAbandonReasonCode || controlledBy != runtimerunquiescence.ServeAbandonControlledBy {
		t.Fatalf("run/control = %s/%s/%s/%s, want cancelled/stopped/%s/%s", runStatus, controlStatus, reason, controlledBy, runtimerunquiescence.ServeAbandonReasonCode, runtimerunquiescence.ServeAbandonControlledBy)
	}

	var deliveryStatus, deliveryReason string
	var deliveryActiveSession sql.NullString
	if err := db.QueryRowContext(context.Background(), `
		SELECT status, COALESCE(reason_code, ''), active_session_id::text
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = 'agent-a'
	`, eventID).Scan(&deliveryStatus, &deliveryReason, &deliveryActiveSession); err != nil {
		t.Fatalf("load delivery: %v", err)
	}
	if deliveryStatus != "dead_letter" || deliveryReason != runtimerunquiescence.ServeAbandonReasonCode || deliveryActiveSession.Valid {
		t.Fatalf("delivery = %s/%s active=%v, want serve abandon dead_letter", deliveryStatus, deliveryReason, deliveryActiveSession.Valid)
	}
	for _, subscriberID := range []string{"agent-a", "pipeline"} {
		var outcome, receiptReason string
		if err := db.QueryRowContext(context.Background(), `
			SELECT outcome, COALESCE(reason_code, '')
			FROM event_receipts
			WHERE event_id = $1::uuid
			  AND subscriber_id = $2
		`, eventID, subscriberID).Scan(&outcome, &receiptReason); err != nil {
			t.Fatalf("load receipt %s: %v", subscriberID, err)
		}
		if outcome != "dead_letter" || receiptReason != runtimerunquiescence.ServeAbandonReasonCode {
			t.Fatalf("receipt %s = %s/%s, want serve abandon dead_letter", subscriberID, outcome, receiptReason)
		}
	}
}

type serveRuntimeWorkspaceStub struct {
	stubWorkspaceLifecycle
	cleanup           func(context.Context) (runtimedestructivereset.ContainerResetResult, error)
	managedContainers []runtimedestructivereset.ContainerRef
	stoppedContainers *[]string
}

func (s serveRuntimeWorkspaceStub) CleanupDevEntityContainers(ctx context.Context) (runtimedestructivereset.ContainerResetResult, error) {
	if s.cleanup != nil {
		return s.cleanup(ctx)
	}
	return runtimedestructivereset.ContainerResetResult{}, nil
}

func (s serveRuntimeWorkspaceStub) ManagedResetContainerInventory(context.Context) ([]runtimedestructivereset.ContainerRef, error) {
	return append([]runtimedestructivereset.ContainerRef(nil), s.managedContainers...), nil
}

func (serveRuntimeWorkspaceStub) InspectManagedContainer(context.Context, string) (runtimedestructivereset.ManagedContainerInspection, error) {
	return runtimedestructivereset.ManagedContainerInspection{}, nil
}

func (s serveRuntimeWorkspaceStub) StopManagedContainer(_ context.Context, name string) error {
	if s.stoppedContainers != nil {
		*s.stoppedContainers = append(*s.stoppedContainers, name)
	}
	return nil
}

type serveBootProgressSpecStep struct {
	Step  int    `yaml:"step"`
	Name  string `yaml:"name"`
	Owner string `yaml:"owner"`
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
			Environment    string `yaml:"environment"`
			ConfigKey      string `yaml:"config_key"`
			BuiltInDefault string `yaml:"built_in_default"`
		} `yaml:"api_listen_addr"`
		MCPListenAddr struct {
			Flag           string `yaml:"flag"`
			Environment    string `yaml:"environment"`
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
		AcceptedListenerEnvironment map[string]string `yaml:"accepted_listener_environment"`
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

func loadServeBootProgressSequenceFromSpec(t *testing.T) []serveBootProgressSpecStep {
	t.Helper()
	var spec struct {
		CLISpecification struct {
			CommandCatalog struct {
				Serve struct {
					BootObservability struct {
						BootProgressSequence struct {
							TotalSteps int                         `yaml:"total_steps"`
							Steps      []serveBootProgressSpecStep `yaml:"steps"`
						} `yaml:"boot_progress_sequence"`
					} `yaml:"boot_observability"`
				} `yaml:"serve"`
			} `yaml:"command_catalog"`
		} `yaml:"cli_specification"`
	}
	data, err := os.ReadFile(filepath.Join(repoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	if err := yaml.Unmarshal(data, &spec); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
	sequence := spec.CLISpecification.CommandCatalog.Serve.BootObservability.BootProgressSequence
	if sequence.TotalSteps != runtimepkg.BootProgressTotalSteps {
		t.Fatalf("platform spec total_steps = %d, want %d", sequence.TotalSteps, runtimepkg.BootProgressTotalSteps)
	}
	for i, step := range sequence.Steps {
		if step.Step != i+1 {
			t.Fatalf("platform spec step index %d has step %d", i, step.Step)
		}
		if strings.TrimSpace(step.Name) == "" {
			t.Fatalf("platform spec step %d missing name", step.Step)
		}
		if strings.TrimSpace(step.Owner) == "" {
			t.Fatalf("platform spec step %d missing owner", step.Step)
		}
	}
	return sequence.Steps
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
	data, err := os.ReadFile(filepath.Join(repoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	if err := yaml.Unmarshal(data, &spec); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
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
	data, err := os.ReadFile(filepath.Join(repoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	if err := yaml.Unmarshal(data, &spec); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
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
	data, err := os.ReadFile(filepath.Join(repoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	if err := yaml.Unmarshal(data, &spec); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
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
	data, err := os.ReadFile(filepath.Join(repoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	if err := yaml.Unmarshal(data, &spec); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
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
	data, err := os.ReadFile(filepath.Join(repoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	if err := yaml.Unmarshal(data, &spec); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
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

func intSliceContains(values []int, want int) bool {
	for _, value := range values {
		if value == want {
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

func waitForServeReadyLine(t *testing.T, out *lockedBuffer, done <-chan int) {
	t.Helper()
	deadline := time.After(serveRuntimeReadyTimeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case code := <-done:
			t.Fatalf("runServeRuntime exited before ready line with code %d\noutput:\n%s", code, out.String())
		case <-deadline:
			t.Fatalf("timed out waiting for serve ready line\noutput:\n%s", out.String())
		case <-ticker.C:
			if strings.Contains(out.String(), "[22/22]") {
				return
			}
		}
	}
}

const (
	serveRuntimeReadyTimeout = 30 * time.Second
	serveRuntimeStopTimeout  = 5 * time.Second
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

func startServeRuntimeTestProcess(t *testing.T, opts serveOptions) *serveRuntimeTestProcess {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	out := &lockedBuffer{}
	opts.Output = out
	done := make(chan int, 1)
	process := &serveRuntimeTestProcess{
		t:      t,
		cancel: cancel,
		done:   done,
		out:    out,
	}
	t.Cleanup(process.cleanup)
	go func() {
		done <- runServeRuntime(ctx, repoRoot(), opts)
	}()
	return process
}

func (p *serveRuntimeTestProcess) outputString() string {
	return p.out.String()
}

func (p *serveRuntimeTestProcess) waitForReadyLine() {
	p.t.Helper()
	deadline := time.After(serveRuntimeReadyTimeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case code := <-p.done:
			p.recordStopped(code)
			p.t.Fatalf("runServeRuntime exited before ready line with code %d\noutput:\n%s", code, p.outputString())
		case <-deadline:
			p.cancel()
			if code, ok := p.waitForExit(serveRuntimeStopTimeout); ok {
				p.t.Fatalf("timed out waiting for serve ready line; runServeRuntime stopped after cancellation with code %d\noutput:\n%s", code, p.outputString())
			}
			p.t.Fatalf("timed out waiting for serve ready line and stopping runServeRuntime\noutput:\n%s", p.outputString())
		case <-ticker.C:
			if strings.Contains(p.outputString(), "[22/22]") {
				return
			}
		}
	}
}

func (p *serveRuntimeTestProcess) stop() int {
	p.t.Helper()
	p.cancel()
	code, ok := p.waitForExit(serveRuntimeStopTimeout)
	if !ok {
		p.t.Fatalf("timed out stopping runServeRuntime\noutput:\n%s", p.outputString())
	}
	return code
}

func (p *serveRuntimeTestProcess) cleanup() {
	p.t.Helper()
	p.mu.Lock()
	stopped := p.stopped
	p.mu.Unlock()
	if stopped {
		return
	}
	p.cancel()
	if _, ok := p.waitForExit(serveRuntimeStopTimeout); !ok {
		p.t.Errorf("timed out stopping runServeRuntime during cleanup\noutput:\n%s", p.outputString())
	}
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

func serveRuntimeAPIListenerFromOutput(t *testing.T, output string) string {
	t.Helper()
	for _, line := range strings.Split(output, "\n") {
		for _, field := range strings.Fields(line) {
			field = strings.Trim(field, "(),")
			if addr, ok := strings.CutPrefix(field, "api_listener="); ok && strings.TrimSpace(addr) != "" {
				return addr
			}
		}
	}
	t.Fatalf("serve output missing api_listener:\n%s", output)
	return ""
}

type serveBootProgressRow struct {
	Step  int
	Total int
	Name  string
}

func parseServeBootProgressRows(t *testing.T, output string) []serveBootProgressRow {
	t.Helper()
	rows := []serveBootProgressRow{}
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "[") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			t.Fatalf("malformed serve boot progress line: %q", line)
		}
		parts := strings.Split(strings.Trim(fields[0], "[]"), "/")
		if len(parts) != 2 {
			t.Fatalf("malformed serve boot progress marker %q in line %q", fields[0], line)
		}
		step, err := strconv.Atoi(parts[0])
		if err != nil {
			t.Fatalf("parse step from %q: %v", fields[0], err)
		}
		total, err := strconv.Atoi(parts[1])
		if err != nil {
			t.Fatalf("parse total from %q: %v", fields[0], err)
		}
		rows = append(rows, serveBootProgressRow{Step: step, Total: total, Name: fields[1]})
	}
	return rows
}

func writeServeRuntimeTestConfig(t *testing.T) string {
	t.Helper()
	configText := strings.Join([]string{
		"runtime:",
		"  recovery_on_startup: false",
		"workspace:",
		"  data_source: " + t.TempDir(),
		"llm:",
		"  backend: anthropic",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
	}, "\n") + "\n"
	path := filepath.Join(t.TempDir(), "swarm.yaml")
	if err := os.WriteFile(path, []byte(configText), 0o644); err != nil {
		t.Fatalf("write serve runtime config: %v", err)
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

	err := verifyBundle(context.Background(), source)
	if err == nil {
		t.Fatal("verifyBundle error = nil, want warning-only failure from unrelated fixture warnings")
	}
	if strings.Contains(err.Error(), "'child/child.internal' emitted but no schema in events.yaml") {
		t.Fatalf("unexpected flow-local no-schema warning: %v", err)
	}
	if strings.Contains(err.Error(), "'child/child.done' emitted but no schema in events.yaml") {
		t.Fatalf("unexpected flow-local no-schema warning: %v", err)
	}
}

func TestVerifyBundle_DoesNotWarnForFlowOwnedAgentOutputEvents(t *testing.T) {
	source := semanticview.Wrap(loadWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier11-flow-composition", "test-required-agents-child")))

	err := verifyBundle(context.Background(), source)
	if err == nil {
		t.Fatal("verifyBundle error = nil, want warning-only failure from unrelated fixture warnings")
	}
	if strings.Contains(err.Error(), "'analysis.done' emitted but nobody subscribes") {
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
	handler.Accumulate = &runtimecontracts.AccumulateSpec{ExpectedFrom: "entity.expected_count"}
	handler.Compute = &runtimecontracts.ComputeSpec{
		Operation: runtimecontracts.ComputeOpCount,
		StoreAs:   "entity.composite_score",
	}
	handler.OnComplete = []runtimecontracts.HandlerRuleEntry{{
		Condition: "entity.composite_score >= 0",
	}}
	node.EventHandlers[eventType] = handler
	bundle.Nodes[nodeID] = node
	if bundle.Semantics.NodeHandlers == nil {
		bundle.Semantics.NodeHandlers = map[string]map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	if bundle.Semantics.NodeHandlers[nodeID] == nil {
		bundle.Semantics.NodeHandlers[nodeID] = map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	bundle.Semantics.NodeHandlers[nodeID][eventType] = handler

	err := verifyBundle(context.Background(), semanticview.Wrap(bundle))
	if err == nil || !strings.Contains(err.Error(), "declares both create_entity and accumulate") {
		t.Fatalf("verifyBundle error = %v, want create_entity/accumulate boot error", err)
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

	err := verifyBundle(context.Background(), semanticview.Wrap(bundle))
	if err == nil {
		t.Fatal("verifyBundle error = nil, want emitted payload completeness invalidity")
	}
	if !strings.Contains(err.Error(), "scan_id is not statically provable") {
		t.Fatalf("verifyBundle error = %v, want emitted payload completeness invalidity", err)
	}
	if strings.Contains(err.Error(), "definitely missing") {
		t.Fatalf("verifyBundle error = %v, want approved warning wording only", err)
	}
}

func TestVerifyBundle_InputPinProducerPathReturnsWarningSurface(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")

	err := verifyBundle(context.Background(), semanticview.Wrap(loadWorkflowValidationBundleAt(t, writeVerifyMissingPinWarningFixture(t))))
	if err == nil {
		t.Fatal("verifyBundle error = nil, want warning-only failure from missing producer path")
	}
	for _, want := range []string{
		"no producer path was found in the authored bundle",
		"Sibling flow output pin: not found",
		"Root agent emit_events: not found",
		"Root node handler emits: not found",
		"Platform event catalog: not matched",
		"External source metadata (swarm.source): not found",
		"Same-flow timer declaration: not found",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("verifyBundle error = %v, want substring %q", err, want)
		}
	}
}

func writeVerifyMissingPinWarningFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: verify-missing-pin-warning
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: child
    flow: child
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: verify-missing-pin-warning
initial_state: pending
terminal_states: [done]
states: [pending, done]
pins:
  inputs:
    events: [task.requested]
  outputs:
    events: [task.completed]
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), `
task.requested:
  swarm:
    source: external
task.completed: {}
child/task.assigned: {}
child/task.result: {}
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
dispatcher:
  id: dispatcher
  execution_type: system_node
  subscribes_to: [task.requested, child/task.result]
  produces: [task.completed, child/task.assigned]
  event_handlers:
    task.requested:
      emit: child/task.assigned
    child/task.result:
      advances_to: done
      emit:
        event: task.completed
        broadcast: true
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "schema.yaml"), `
name: child
initial_state: idle
terminal_states: [done]
states: [idle, working, done]
pins:
  inputs:
    events: [task.assigned, task.feedback]
  outputs:
    events: [task.result]
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "entities.yaml"), `
work_item: {}
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "events.yaml"), `
task.assigned: {}
task.feedback:
  comment: string
task.result: {}
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "nodes.yaml"), `
worker:
  id: worker
  execution_type: system_node
  subscribes_to: [task.assigned, task.feedback]
  produces: [task.result]
  event_handlers:
    task.assigned:
      advances_to: working
    task.feedback:
      advances_to: done
      emit:
        event: task.result
        broadcast: true
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "agents.yaml"), `{}`)
	return root
}

func TestVerifyBundle_UnreachableStateReturnsWarningSurface(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")

	err := verifyBundle(context.Background(), semanticview.Wrap(loadWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier8-boot-verification", "test-boot-state-machine-unreachable"))))
	if err == nil {
		t.Fatal("verifyBundle error = nil, want warning-only failure from unreachable declared state")
	}
	for _, want := range []string{
		"semantic_drift_unreachable_state",
		"declares state review but no transition path from initial_state waiting reaches review",
		"Reachable states: active, done, waiting",
		"Unreachable states: review",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("verifyBundle error = %v, want substring %q", err, want)
		}
	}
}

func TestVerifyBundle_DeadDeclaredEventSchemaReturnsWarningSurface(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")

	source := semanticview.Wrap(loadWorkflowValidationBundleAt(t, writeWorkflowValidationDeadEventSchemaFixture(t)))

	verifyErr := verifyBundle(context.Background(), source)
	if verifyErr == nil {
		t.Fatal("verifyBundle error = nil, want warning-only failure from dead declared event schema")
	}
	for _, want := range []string{
		"semantic_drift_dead_event_schema",
		"root.unused",
		"has no active role in the authored bundle",
	} {
		if !strings.Contains(verifyErr.Error(), want) {
			t.Fatalf("verifyBundle error = %v, want substring %q", verifyErr, want)
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

	err := verifyBundle(context.Background(), semanticview.Wrap(loadWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier8-boot-verification", "test-boot-create-entity-plus-accumulate"))))
	if err == nil {
		t.Fatal("verifyBundle error = nil, want create_entity/accumulate boot error")
	}
	if !strings.Contains(err.Error(), "declares both create_entity and accumulate") {
		t.Fatalf("verifyBundle error = %v, want create_entity/accumulate boot error", err)
	}
}
