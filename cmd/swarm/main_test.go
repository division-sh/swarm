package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
	"swarm/internal/apiv1"
	"swarm/internal/config"
	"swarm/internal/events"
	runtimepkg "swarm/internal/runtime"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimedeadletters "swarm/internal/runtime/deadletters"
	runtimedestructivereset "swarm/internal/runtime/destructivereset"
	runtimemanager "swarm/internal/runtime/manager"
	runtimemcp "swarm/internal/runtime/mcp"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/preservationcleanup"
	runtimerunforkexecution "swarm/internal/runtime/runforkexecution"
	runtimerunquiescence "swarm/internal/runtime/runquiescence"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/runtime/sessions"
	runtimetools "swarm/internal/runtime/tools"
	"swarm/internal/store"
	storebackend "swarm/internal/store/backendselection"
	storerunlifecycle "swarm/internal/store/runlifecycle"
	"swarm/internal/testutil"
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
	out := (events.Event{
		ID:          uuid.NewString(),
		RunID:       evt.RunID,
		Type:        events.EventType("scan.completed"),
		SourceAgent: a.id,
		Payload:     []byte(`{}`),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(evt.EntityID())
	return []events.Event{out}, nil
}

func publishRunStatusRootEvent(t *testing.T, bus *runtimebus.EventBus, runID, entityID string) {
	t.Helper()
	if err := bus.Publish(context.Background(), (events.Event{
		ID:          uuid.NewString(),
		RunID:       runID,
		Type:        events.EventType("scan.requested"),
		SourceAgent: "api.v1",
		Payload:     []byte(`{"topic":"sample"}`),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(entityID)); err != nil {
		t.Fatalf("publish root event: %v", err)
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
	for _, want := range []string{"Run and inspect Swarm workflows.", "serve", "verify", "completion", "version"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("root help missing %q:\n%s", want, stdout.String())
		}
	}
	for _, retired := range []string{"fork", "investigate"} {
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
	if !strings.Contains(stdout.String(), "Run and inspect Swarm workflows.") || !strings.Contains(stdout.String(), "serve") {
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
			for _, retired := range []string{"fork", "investigate"} {
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
	for _, retired := range []string{"fork\t", "investigate\t"} {
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
	for _, want := range []string{"Start the Swarm runtime", "--config", "--backend", "--contracts", "--bundle-hash", "--api-listen-addr", "API, WebSocket, health, and readiness routes", "--mcp-listen-addr", "MCP and tools routes", "--platform-spec", "--store", "--self-check", "--dev", "--require-bundle-match", "--no-require-bundle-match", "--abandon-active-runs", "--shutdown-grace", "--verbose"} {
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
	explicitAuth := apiv1.AuthTokenResolution{Tokens: []string{"operator-token"}, Source: apiv1.AuthTokenSourceEnvironment, Explicit: true}
	tests := []struct {
		name    string
		addr    string
		auth    apiv1.AuthTokenResolution
		wantErr string
	}{
		{name: "default token allowed on ipv4 loopback", addr: "127.0.0.1:8081", auth: defaultAuth},
		{name: "default token allowed on ipv6 loopback", addr: "[::1]:8081", auth: defaultAuth},
		{name: "default token rejects localhost", addr: "localhost:8081", auth: defaultAuth, wantErr: "non-loopback API bind localhost:8081 requires an explicit SWARM_API_TOKEN"},
		{name: "default token rejects wildcard", addr: "0.0.0.0:8081", auth: defaultAuth, wantErr: "non-loopback API bind 0.0.0.0:8081 requires an explicit SWARM_API_TOKEN"},
		{name: "default token rejects routable", addr: "192.0.2.10:8081", auth: defaultAuth, wantErr: "non-loopback API bind 192.0.2.10:8081 requires an explicit SWARM_API_TOKEN"},
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
	if strings.TrimSpace(spec.ImplementationStatus) != "runtime_bind_and_env_config_precedence_implemented_enable_disable_pending" {
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
		"SWARM_API_TOKEN is required.",
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
	if strings.TrimSpace(spec.ImplementationStatus) != "implemented_loopback_default" {
		t.Fatalf("api connection/auth/config implementation_status = %q, want implemented_loopback_default", spec.ImplementationStatus)
	}
	if !strings.Contains(spec.CanonicalOwner, "cli_specification.foundations.api_connection_auth_config_precedence") {
		t.Fatalf("canonical owner does not point at promoted section: %s", spec.CanonicalOwner)
	}
	for _, want := range []string{"API-backed command leaves consume", "OpenRPC", "root/global `--config`"} {
		if !strings.Contains(spec.Scope, want) {
			t.Fatalf("scope missing boundary %q:\n%s", want, spec.Scope)
		}
	}
	wantPrecedence := []string{"flag", "environment", "config_file", "built_in_default"}
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
	if spec.APIServer.AcceptedSources.Environment != "SWARM_API_SERVER" {
		t.Fatalf("api_server environment = %q, want SWARM_API_SERVER", spec.APIServer.AcceptedSources.Environment)
	}
	if spec.APIServer.AcceptedSources.ConfigKey != "api_server" {
		t.Fatalf("api_server config key = %q, want api_server", spec.APIServer.AcceptedSources.ConfigKey)
	}
	if spec.APIServer.AcceptedSources.BuiltInDefault != "http://127.0.0.1:8081" {
		t.Fatalf("api_server default = %q, want http://127.0.0.1:8081", spec.APIServer.AcceptedSources.BuiltInDefault)
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
	for _, want := range []string{"flag_file", "environment_token", "environment_file", "config_file_key"} {
		if strings.TrimSpace(spec.APIToken.AcceptedSources[want]) == "" {
			t.Fatalf("api_token accepted sources missing %q: %#v", want, spec.APIToken.AcceptedSources)
		}
	}
	wantTokenSourceOrder := []string{"--api-token-file", "SWARM_API_TOKEN", "SWARM_API_TOKEN_FILE", "config api_token_file", "built-in loopback default"}
	if len(spec.APIToken.SourceOrder) != len(wantTokenSourceOrder) {
		t.Fatalf("api_token source order = %#v, want %#v", spec.APIToken.SourceOrder, wantTokenSourceOrder)
	}
	for i, want := range wantTokenSourceOrder {
		if spec.APIToken.SourceOrder[i] != want {
			t.Fatalf("api_token source_order[%d] = %q, want %q", i, spec.APIToken.SourceOrder[i], want)
		}
	}
	for key, want := range map[string]string{
		"--api-token":      "shell history",
		"config api_token": "inline",
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
	for _, want := range []string{"#848", "#884/#750", "#743", "`--no-retry`"} {
		if !stringSliceContains(spec.SplitSiblings, want) {
			t.Fatalf("split siblings missing %q: %#v", want, spec.SplitSiblings)
		}
	}
	for _, want := range []string{"API-backed command leaves consume", "OpenRPC", "Future API-backed CLI commands MUST consume this owner"} {
		if !stringSliceContains(spec.ImplementationBoundaries, want) {
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
		{name: "verify help", args: []string{"verify", "--help"}, want: "Validate local Swarm contract files"},
		{name: "run help", args: []string{"run", "--help"}, want: "Start or reattach to a Swarm run"},
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

func TestVerifyCommandLoadsRepoDotEnvAfterLazyRepoDiscovery(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	_ = os.Unsetenv("SWARM_CONTRACTS_PATH")
	repo := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(repo, "go.mod"), "module testrepo\n")
	contractsRoot := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(contractsRoot, "package.yaml"), `
name: dot-env-contracts
version: "1.0.0"
platform: ">=1.6.0"
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(contractsRoot, "schema.yaml"), "name: dot-env-contracts\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(contractsRoot, "policy.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(contractsRoot, "tools.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(contractsRoot, "agents.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(contractsRoot, "nodes.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(contractsRoot, "events.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(repo, ".env"), "SWARM_CONTRACTS_PATH="+contractsRoot+"\n")
	chdirForTest(t, repo)

	var stdout, stderr bytes.Buffer
	code := executeRootCommand(context.Background(), "", []string{"verify"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("verify code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !strings.Contains(stdout.String(), "verify ok: contracts="+contractsRoot) {
		t.Fatalf("verify did not consume contracts path from repo .env:\n%s", stdout.String())
	}
}

func TestLocalRunDiscoversRepoRootBeforeDotEnvAndServe(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	_ = os.Unsetenv("SWARM_API_TOKEN")
	repo := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(repo, "go.mod"), "module testrepo\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(repo, ".env"), "SWARM_API_TOKEN=test-token\n")
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
}

func TestRunVerifyCommandUsesEmbeddedPlatformSpecWithoutRepoRoot(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	chdirForTest(t, t.TempDir())
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: embedded-platform-spec
version: "1.0.0"
platform: ">=1.6.0"
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

func TestConfiguredWorkspaceLifecycleDoesNotInventSourceRootDataSource(t *testing.T) {
	t.Setenv("SWARM_WORKSPACE_DATA_SOURCE", "")
	t.Setenv("SWARM_WORKSPACE_CONTRACTS_SOURCE", "")
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}

	manager := configuredWorkspaceLifecycle(nil, "", contractsDir, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
	err := manager.ValidateSource(context.Background(), semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
	if err == nil || !strings.Contains(err.Error(), "/data source is not configured") {
		t.Fatalf("ValidateSource error = %v, want explicit /data source requirement", err)
	}
}

func TestConfiguredWorkspaceLifecycleUsesExplicitRepoAndContractsSources(t *testing.T) {
	t.Setenv("SWARM_WORKSPACE_DATA_SOURCE", "")
	t.Setenv("SWARM_WORKSPACE_CONTRACTS_SOURCE", "")
	repoRoot := t.TempDir()
	if err := os.Mkdir(filepath.Join(repoRoot, "data"), 0o755); err != nil {
		t.Fatalf("mkdir data: %v", err)
	}
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}

	manager := configuredWorkspaceLifecycle(nil, repoRoot, contractsDir, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
	if err := manager.ValidateSource(context.Background(), semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})); err != nil {
		t.Fatalf("ValidateSource: %v", err)
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
	for _, want := range []string{"narrowed #997", "MCP gateway token derivation", "MCP-only managed-agent startup proof", "does not close full #997"} {
		if !strings.Contains(startup.Scope, want) {
			t.Fatalf("local cli_test gateway startup scope missing %q:\n%s", want, startup.Scope)
		}
	}
	for _, want := range []string{"SWARM_TOOL_GATEWAY_TOKEN", "per-boot", "operator-provided", "Local foreground `swarm run`"} {
		if !strings.Contains(startup.GatewayTokenRule, want) {
			t.Fatalf("gateway token rule missing %q:\n%s", want, startup.GatewayTokenRule)
		}
	}
	for _, want := range []string{"RuntimeOptions.ToolGatewayToken", "runtime MCP gateway auth", "ValidateClaudeCLIRuntimeConfig", "docker exec", "MCP HTTP binding"} {
		if !stringSliceContains(startup.GatewayTokenConsumers, want) {
			t.Fatalf("gateway token consumers missing %q: %#v", want, startup.GatewayTokenConsumers)
		}
	}
	for _, want := range []string{"startup validation MUST execute", "every managed agent", "MCP-only", "Agent-free `cli_test`"} {
		if !strings.Contains(startup.StartupProbeRule, want) {
			t.Fatalf("startup probe rule missing %q:\n%s", want, startup.StartupProbeRule)
		}
	}
	for _, want := range []string{"#997 local cli_test workspace image contents", "#996 Docker Compose", "#979/#1012", "#1002 runtime workspace source-root image packaging is closed"} {
		if !stringSliceContains(startup.SplitTail, want) {
			t.Fatalf("local cli_test gateway startup split_tail missing %q: %#v", want, startup.SplitTail)
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
	for _, want := range []string{"#996 Docker Compose", "#995 schema migration", "#979/#1012", "#1002 runtime workspace source-root image packaging is closed"} {
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
								EnvVar   string `yaml:"env_var"`
								Required bool   `yaml:"required"`
							} `yaml:"credential_source"`
							EndpointSource struct {
								ConfigKey string `yaml:"config_key"`
								EnvVar    string `yaml:"env_var"`
								Rule      string `yaml:"rule"`
							} `yaml:"endpoint_source"`
						} `yaml:"active_backend_profiles"`
						RejectedTargetNames map[string]string `yaml:"rejected_target_names"`
					} `yaml:"backend_profile_identity"`
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
						SecretEnvSources              []string `yaml:"secret_env_sources"`
						RuntimeConfigCanonicalFor     []string `yaml:"runtime_config_canonical_for"`
						InfraConnectionOverridePolicy string   `yaml:"infra_connection_override_policy"`
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
	if !strings.Contains(strings.ToLower(authority.BackendProfileIdentity.RejectedTargetNames["openai"]), "not active") {
		t.Fatalf("openai rejected target missing design decision: %#v", authority.BackendProfileIdentity.RejectedTargetNames)
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
		"cheap":    {"anthropic": "claude-3-5-haiku", "claude_cli": "haiku", "openai_compatible": "gpt-compatible-mini"},
		"regular":  {"anthropic": "claude-3-5-sonnet", "claude_cli": "sonnet", "openai_compatible": "gpt-compatible"},
		"frontier": {"anthropic": "claude-3-opus", "claude_cli": "opus", "openai_compatible": "gpt-compatible-frontier"},
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
	for _, want := range []string{"ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN", "OPENAI_COMPATIBLE_API_KEY"} {
		if !stringSliceContains(authority.CredentialAndConfigPolicy.SecretEnvSources, want) {
			t.Fatalf("secret env sources missing %q: %#v", want, authority.CredentialAndConfigPolicy.SecretEnvSources)
		}
	}
	for _, want := range []string{"backend selection", "provider model alias maps"} {
		if !stringSliceContains(authority.CredentialAndConfigPolicy.RuntimeConfigCanonicalFor, want) {
			t.Fatalf("runtime config canonical list missing %q: %#v", want, authority.CredentialAndConfigPolicy.RuntimeConfigCanonicalFor)
		}
	}
	for _, want := range []string{"anthropic", "claude_cli", "openai_compatible", "api and cli_test", "model", "write time"} {
		if !joinedContains(authority.PersistenceRules, want) {
			t.Fatalf("persistence rules missing %q: %#v", want, authority.PersistenceRules)
		}
	}
	for _, want := range []string{"#1127", "#1128", "#1129", "#1130", "MUST NOT change runtime"} {
		if !joinedContains(authority.SplitBoundaries, want) {
			t.Fatalf("split boundaries missing %q: %#v", want, authority.SplitBoundaries)
		}
	}
}

func TestRuntimeOperationsWatchlistMapsLLMProviderModelSelectionSourceAuthority(t *testing.T) {
	var watchlist struct {
		ActiveIssues []struct {
			ID     int      `yaml:"id"`
			MapsTo []string `yaml:"maps_to"`
		} `yaml:"active_issues"`
		Nodes []struct {
			ID                    string   `yaml:"id"`
			CanonicalOwners       []string `yaml:"canonical_owners"`
			KnownManifestations   []string `yaml:"known_manifestations"`
			RepresentativeIssues  []int    `yaml:"representative_issues"`
			CommonFramingMistakes struct {
				TooBroad  []string `yaml:"too_broad"`
				TooNarrow []string `yaml:"too_narrow"`
			} `yaml:"common_framing_mistakes"`
		} `yaml:"nodes"`
	}
	data, err := os.ReadFile(filepath.Join(repoRoot(), "docs", "watchlists", "runtime-operations.yaml"))
	if err != nil {
		t.Fatalf("read runtime operations watchlist: %v", err)
	}
	if err := yaml.Unmarshal(data, &watchlist); err != nil {
		t.Fatalf("parse runtime operations watchlist: %v", err)
	}
	for _, issueID := range []int{1123, 1127, 1128, 1129, 1130} {
		if !watchlistIssueMapsTo(watchlist.ActiveIssues, issueID, "llm_provider_selection_config_authority") {
			t.Fatalf("issue %d does not map to llm_provider_selection_config_authority: %#v", issueID, watchlist.ActiveIssues)
		}
	}
	var node struct {
		ID                    string
		CanonicalOwners       []string
		KnownManifestations   []string
		RepresentativeIssues  []int
		CommonFramingMistakes struct {
			TooBroad  []string `yaml:"too_broad"`
			TooNarrow []string `yaml:"too_narrow"`
		}
	}
	for _, candidate := range watchlist.Nodes {
		if candidate.ID == "llm_provider_selection_config_authority" {
			node.ID = candidate.ID
			node.CanonicalOwners = candidate.CanonicalOwners
			node.KnownManifestations = candidate.KnownManifestations
			node.RepresentativeIssues = candidate.RepresentativeIssues
			node.CommonFramingMistakes = candidate.CommonFramingMistakes
			break
		}
	}
	if node.ID == "" {
		t.Fatalf("llm_provider_selection_config_authority node not found")
	}
	if !joinedContains(node.CanonicalOwners, "model alias resolver") {
		t.Fatalf("watchlist canonical owners missing model alias resolver: %#v", node.CanonicalOwners)
	}
	for _, want := range []string{"#1127", "--backend", "anthropic", "claude_cli", "model", "write time", "#1128", "#1129", "#1130"} {
		if !joinedContains(node.KnownManifestations, want) {
			t.Fatalf("watchlist known manifestations missing %q: %#v", want, node.KnownManifestations)
		}
	}
	for _, issueID := range []int{1123, 1127, 1128, 1129, 1130} {
		if !intSliceContains(node.RepresentativeIssues, issueID) {
			t.Fatalf("representative issues missing %d: %#v", issueID, node.RepresentativeIssues)
		}
	}
	if !joinedContains(node.CommonFramingMistakes.TooBroad, "#1128") || !joinedContains(node.CommonFramingMistakes.TooNarrow, "model alongside model") {
		t.Fatalf("watchlist framing mistakes do not guard #1127 split: %#v", node.CommonFramingMistakes)
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

func TestDBLoadedServeRunForkAvailabilityFailsClosed(t *testing.T) {
	hash := "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111"
	runID := uuid.NewString()

	availability, err := (dbLoadedServeRunForkAvailability{bundleHash: hash}).LoadRunBundleAvailability(context.Background(), runID)
	if err != nil {
		t.Fatalf("LoadRunBundleAvailability: %v", err)
	}
	if !availability.Unavailable() {
		t.Fatalf("availability = %#v, want unavailable", availability)
	}
	if availability.RunID != runID || availability.BundleHash != hash || availability.BundleSource != storerunlifecycle.BundleSourcePersisted {
		t.Fatalf("availability = %#v, want DB-loaded persisted unavailable for %s", availability, hash)
	}
	if availability.Cause != "db_loaded_same_bundle_fork_split_to_1024" {
		t.Fatalf("availability cause = %q", availability.Cause)
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
	if _, err := loadServeRuntimeBundleFromCatalog(ctx, repoRoot(), storeBundle{}, projection.BundleHash); err == nil || !strings.Contains(err.Error(), "requires postgres bundle catalog store") {
		t.Fatalf("loadServeRuntimeBundleFromCatalog without Postgres err = %v, want Postgres-only failure", err)
	}

	loaded, err := loadServeRuntimeBundleFromCatalog(ctx, repoRoot(), storeBundle{Postgres: pg}, projection.BundleHash)
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
	prepared, err := prepareLoadedServeBundleSource(ctx, storeBundle{Postgres: pg}, loaded, false)
	if err != nil {
		t.Fatalf("prepareLoadedServeBundleSource: %v", err)
	}
	if prepared.BundleHash != projection.BundleHash || prepared.BundleSource != storerunlifecycle.BundleSourcePersisted {
		t.Fatalf("prepared source fact = %#v, want persisted %s", prepared, projection.BundleHash)
	}
	if _, err := prepareLoadedServeBundleSource(ctx, storeBundle{Postgres: pg}, loaded, true); err == nil || !strings.Contains(err.Error(), "--bundle-hash is mutually exclusive with --dev") {
		t.Fatalf("prepareLoadedServeBundleSource dev error = %v", err)
	}
}

func TestRunServeRuntimeBundleHashMissingFailsBeforeReadiness(t *testing.T) {
	_, _, _ = installServeRuntimePostgresTestStores(t, func() serveWorkspaceLifecycle {
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var out lockedBuffer
	done := make(chan int, 1)
	go func() {
		done <- runServeRuntime(ctx, repoRoot(), serveOptions{
			ConfigPath:         writeServeRuntimeTestConfig(t),
			ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
			PlatformSpecPath:   defaultPlatformSpecPath,
			StoreMode:          "postgres",
			APIListenAddr:      "127.0.0.1:0",
			MCPListenAddr:      "127.0.0.1:0",
			SelfCheck:          true,
			RequireBundleMatch: true,
			Verbose:            true,
			Output:             &out,
		})
	}()

	waitForServeReadyLine(t, &out, done)
	cancel()
	if code := <-done; code != 0 {
		t.Fatalf("runServeRuntime code = %d\noutput:\n%s", code, out.String())
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
	oldBuildStores := buildStoresForServe
	oldWorkspaceLifecycle := configuredWorkspaceLifecycleForServe
	dsn, db, cleanup := testutil.StartPostgres(t)
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
		return storeBundle{
			Postgres:           runtimePG,
			SQLDB:              runtimePG.DB,
			SchemaBootstrapper: runtimePG,
			EventStore:         runtimePG,
			SessionRegistry:    sessions.NewPostgresRegistry(runtimePG.DB, cfg.LLM.Session.LockTTL),
			ConversationStore:  runtimePG,
			ManagerStore:       runtimePG,
			ScheduleStore:      runtimePG,
			TurnStore:          runtimePG,
		}, nil
	}
	configuredWorkspaceLifecycleForServe = func(*sql.DB, string, string, semanticview.Source) serveWorkspaceLifecycle {
		return workspaceFactory()
	}
	t.Cleanup(func() {
		buildStoresForServe = oldBuildStores
		configuredWorkspaceLifecycleForServe = oldWorkspaceLifecycle
	})
	return dsn, db, runtimePG
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

	fact, err := prepareServeBundleSource(ctx, storeBundle{Postgres: pg}, bundle, identity.Fingerprint, false)
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

	fact, err := prepareServeBundleSource(ctx, storeBundle{Postgres: pg}, bundle, identity.Fingerprint, true)
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

func TestCLI_ForkIsSpecifiedButNotImplemented(t *testing.T) {
	const expectedForkCommand = "swarm fork <source-run-id> [--bundle-hash <bundle_hash>] [--at-event <event-id>] [--idempotency-key <key>]"
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "fork", args: []string{"fork"}, want: "ERROR: `swarm fork` is specified but not implemented yet."},
		{name: "fork-help", args: []string{"fork", "--help"}, want: expectedForkCommand},
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
			if !strings.Contains(stderr.String(), tc.want) {
				t.Fatalf("%s stderr = %q, want %q", tc.name, stderr.String(), tc.want)
			}
			if !strings.Contains(stderr.String(), expectedForkCommand) {
				t.Fatalf("%s stderr = %q, want promoted fork command %q", tc.name, stderr.String(), expectedForkCommand)
			}
			if strings.Contains(stderr.String(), "swarm control run fork") || strings.Contains(stderr.String(), "was removed in v1") {
				t.Fatalf("%s stderr preserves stale fork authority: %q", tc.name, stderr.String())
			}
		})
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
	toolGateway := runtimemcp.NewGateway(nil, "", runtimemcp.GatewayHooks{})
	apiHandlerMux := newAPIServer(&ready, apiHandler).Handler
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

	for _, path := range []string{"/healthz", "/readyz", "/v1/rpc", "/v1/ws"} {
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
		VALUES ($1::uuid, $2::uuid, 'item.received', $3::uuid, 'flow-a/1', 'entity', $4::jsonb, 'test', 'platform', $5)
	`, runID, eventID, entityID, fmt.Sprintf(`{"entity_id":%q}`, entityID), at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, reason_code, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'node', 'test-node', 'pending', 'source_pending_node_delivery', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"pending"'::jsonb, $3::uuid, 'platform', 'cli-selected-execution-test', 'seed', $4),
			($1::uuid, $2::uuid, 'name', 'null'::jsonb, '"CLI Selected Execution Entity"'::jsonb, $3::uuid, 'platform', 'cli-selected-execution-test', 'seed', $4)
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
			$1::uuid, $2::uuid, 'flow-a/1', 'default', 'CLI Selected Execution Entity',
			'pending', '{}'::jsonb, '{"name":"CLI Selected Execution Entity"}'::jsonb, '{}'::jsonb, 1,
			$3, $3, $3
		)
	`, runID, entityID, at); err != nil {
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

func TestDockerComposeUsesBackendFlagNotRetiredLLMBackendEnv(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(repoRoot(), "docker-compose.yml"))
	if err != nil {
		t.Fatalf("read docker-compose.yml: %v", err)
	}
	text := string(raw)
	if strings.Contains(text, "SWARM_LLM_BACKEND") {
		t.Fatalf("docker-compose.yml still uses retired SWARM_LLM_BACKEND selector")
	}
	if !strings.Contains(text, "--backend claude_cli") {
		t.Fatalf("docker-compose.yml missing canonical --backend claude_cli selector")
	}
	if !strings.Contains(text, "SWARM_API_TOKEN: ${SWARM_API_TOKEN:-}") {
		t.Fatalf("docker-compose.yml must keep model rendering tokenless for postgres-only startup")
	}
	if !strings.Contains(text, "SWARM_API_TOKEN must be set because the orchestrator API binds to 0.0.0.0.") {
		t.Fatalf("docker-compose.yml must require explicit SWARM_API_TOKEN inside the orchestrator command")
	}
	if strings.Contains(text, "SWARM_API_TOKEN:-"+apiv1.DefaultLoopbackAPIToken) {
		t.Fatalf("docker-compose.yml must not default the non-loopback API to the built-in token")
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

func TestLoadDotEnvFileSetsMissingVarsOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	if err := os.WriteFile(path, []byte("ALPHA=one\nBETA=\"two words\"\nexport GAMMA='three'\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	t.Setenv("ALPHA", "shell")

	if err := loadDotEnvFile(path); err != nil {
		t.Fatalf("loadDotEnvFile: %v", err)
	}

	if got := os.Getenv("ALPHA"); got != "shell" {
		t.Fatalf("ALPHA = %q, want shell override", got)
	}
	if got := os.Getenv("BETA"); got != "two words" {
		t.Fatalf("BETA = %q", got)
	}
	if got := os.Getenv("GAMMA"); got != "three" {
		t.Fatalf("GAMMA = %q", got)
	}
}

func TestLoadDotEnvFileMissingIsNoop(t *testing.T) {
	if err := loadDotEnvFile(filepath.Join(t.TempDir(), ".env")); err != nil {
		t.Fatalf("loadDotEnvFile: %v", err)
	}
}

func TestLoadDotEnvFileRejectsMalformedLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	if err := os.WriteFile(path, []byte("BROKEN\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	err := loadDotEnvFile(path)
	if err == nil || !strings.Contains(err.Error(), "expected KEY=VALUE") {
		t.Fatalf("loadDotEnvFile error = %v", err)
	}
}

func runVerifyCommandWithContractsForTest(ctx context.Context, repo, contractsPath string, out *bytes.Buffer) int {
	opts := defaultVerifyCommandOptions()
	opts.contractsPath = contractsPath
	return runVerifyCommand(ctx, repo, opts, out)
}

func TestRunVerifyCommand_BadContractsPath(t *testing.T) {
	var buf bytes.Buffer
	code := runVerifyCommandWithContractsForTest(context.Background(), repoRoot(), filepath.Join(t.TempDir(), "missing"), &buf)
	if code == 0 {
		t.Fatal("expected non-zero exit code")
	}
	if out := buf.String(); !strings.Contains(out, "verify failed: resolve contracts") {
		t.Fatalf("output = %q", out)
	}
}

func TestRunVerifyCommand_SurfacesLintEvidence(t *testing.T) {
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: verify-lint-evidence
version: "1.0.0"
platform: ">=1.6.0"
flows:
  - id: child
    flow: child
    mode: static
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
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "events.yaml"), `
task.assigned:
  swarm:
    source: external (verify lint evidence test)
  entity_id: string
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "nodes.yaml"), `
reader:
  id: reader
  execution_type: system_node
  subscribes_to: [task.assigned]
  event_handlers:
    task.assigned:
      create_entity: true
      guard:
        check: "entity.priority >= 0"
      advances_to: done
`)

	var buf bytes.Buffer
	code := runVerifyCommandWithContractsForTest(context.Background(), repoRoot(), root, &buf)
	if code != 0 {
		t.Fatalf("runVerifyCommand exit code = %d, output = %q", code, buf.String())
	}
	out := buf.String()
	if !strings.Contains(out, "lint_evidence: entity_reader_coverage [root] flow root entity_type case declares field untouched with no detected internal reader coverage") {
		t.Fatalf("verify output missing lint evidence:\n%s", out)
	}
	if !strings.Contains(out, "verify ok: contracts=") {
		t.Fatalf("verify output missing success marker:\n%s", out)
	}
}

func TestRunVerifyCommand_FailsForPromptDeclaredSaveWithoutEntityWrites(t *testing.T) {
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: verify-prompt-writer-coverage
version: "1.0.0"
platform: ">=1.6.0"
flows:
  - id: child
    flow: child
    mode: static
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
  conversation_mode: task
  subscriptions: []
  entity_writes:
    case:
      save:
      - research_context
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "events.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "nodes.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "prompts", "writer.md"), `
Use save_entity_field for `+"`business_brief`"+`.
`)

	var buf bytes.Buffer
	code := runVerifyCommandWithContractsForTest(context.Background(), repoRoot(), root, &buf)
	if code == 0 {
		t.Fatalf("expected non-zero exit code, output = %q", buf.String())
	}
	out := buf.String()
	if !strings.Contains(out, "entity_writer_coverage") {
		t.Fatalf("verify output missing entity_writer_coverage:\n%s", out)
	}
	if !strings.Contains(out, "business_brief") {
		t.Fatalf("verify output missing offending field:\n%s", out)
	}
}

func TestRunVerifyCommand_FailsForPseudoStateSchemaTypes(t *testing.T) {
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: verify-state-schema-pseudo-types
version: "1.0.0"
platform: ">=1.6.0"
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
platform: ">=1.6.0"
flows:
  - id: child
    flow: child
    mode: static
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
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "agents.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "events.yaml"), `
task.assigned:
  swarm:
    source: external (state schema float verify test)
  entity_id: string
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "child", "nodes.yaml"), `
accumulator:
  id: accumulator
  execution_type: system_node
  subscribes_to: [task.assigned]
  event_handlers:
    task.assigned:
      create_entity: true
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
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: verify-accumulator-entity-projection
version: "1.0.0"
platform_version: ">=1.1.0"
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
  entity_id: string
  expected_dimensions: integer
  vertical_id: string
  dimension: text
  tier: integer
  score: integer
  evidence: text
  confidence: text
score.completed:
  entity_id: string
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
          emit: score.completed
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

func writeWorkflowValidationDeadEventSchemaFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: dead-event-schema
version: "1.0.0"
platform: ">=1.6.0"
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
		return storeBundle{
			Postgres:           runtimePG,
			SQLDB:              runtimePG.DB,
			SchemaBootstrapper: runtimePG,
			EventStore:         runtimePG,
			SessionRegistry:    sessions.NewPostgresRegistry(runtimePG.DB, cfg.LLM.Session.LockTTL),
			ConversationStore:  runtimePG,
			ManagerStore:       runtimePG,
			ScheduleStore:      runtimePG,
			TurnStore:          runtimePG,
		}, nil
	}
	configuredWorkspaceLifecycleForServe = func(*sql.DB, string, string, semanticview.Source) serveWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	}
	t.Cleanup(func() {
		buildStoresForServe = oldBuildStores
		configuredWorkspaceLifecycleForServe = oldWorkspaceLifecycle
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var out lockedBuffer
	done := make(chan int, 1)
	go func() {
		done <- runServeRuntime(ctx, repoRoot(), serveOptions{
			ConfigPath:         writeServeRuntimeTestConfig(t),
			ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
			PlatformSpecPath:   defaultPlatformSpecPath,
			StoreMode:          "postgres",
			APIListenAddr:      "127.0.0.1:0",
			MCPListenAddr:      "127.0.0.1:0",
			SelfCheck:          true,
			RequireBundleMatch: true,
			Verbose:            true,
			Output:             &out,
		})
	}()

	waitForServeReadyLine(t, &out, done)
	cancel()
	if code := <-done; code != 0 {
		t.Fatalf("runServeRuntime code = %d\noutput:\n%s", code, out.String())
	}

	rows := parseServeBootProgressRows(t, out.String())
	if got, want := len(rows), len(steps); got != want {
		t.Fatalf("serve boot progress rows = %d, want %d\noutput:\n%s", got, want, out.String())
	}
	for i, want := range steps {
		got := rows[i]
		if got.Step != want.Step || got.Total != runtimepkg.BootProgressTotalSteps || got.Name != want.Name {
			t.Fatalf("row %d = step=%d total=%d name=%q, want step=%d total=%d name=%q\noutput:\n%s", i, got.Step, got.Total, got.Name, want.Step, runtimepkg.BootProgressTotalSteps, want.Name, out.String())
		}
	}
	if strings.Contains(out.String(), "health_endpoints_respond       ok  (/healthz /readyz /v1/rpc /v1/ws)") {
		t.Fatalf("serve verbose output still claims unproven v1 endpoint response:\n%s", out.String())
	}
	for _, want := range []string{"http_listener_bind", "api_listener=", "api_routes=" + serveAPIRoutes, "mcp_listener=", "mcp_routes=" + serveMCPRoutes, "health_endpoints_respond", serveReadinessRoutes} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("serve verbose output missing %q:\n%s", want, out.String())
		}
	}
	if strings.Contains(out.String(), "health=127.") {
		t.Fatalf("serve verbose output still labels the unified listener as health-only:\n%s", out.String())
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
				return storeBundle{
					Postgres:           runtimePG,
					SQLDB:              runtimePG.DB,
					SchemaBootstrapper: runtimePG,
					EventStore:         runtimePG,
					SessionRegistry:    sessions.NewPostgresRegistry(runtimePG.DB, cfg.LLM.Session.LockTTL),
					ConversationStore:  runtimePG,
					ManagerStore:       runtimePG,
					ScheduleStore:      runtimePG,
					TurnStore:          runtimePG,
				}, nil
			}
			configuredWorkspaceLifecycleForServe = func(*sql.DB, string, string, semanticview.Source) serveWorkspaceLifecycle {
				return serveRuntimeWorkspaceStub{}
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

func TestConfigureServeMCPGatewayEnvAlignsToMCPListener(t *testing.T) {
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

	restore, err := configureServeMCPGatewayEnv(listener.Addr())
	if err != nil {
		t.Fatalf("configure gateway env: %v", err)
	}
	if got, want := os.Getenv("SWARM_TOOL_GATEWAY_URL"), "http://127.0.0.1:"+port; got != want {
		t.Fatalf("SWARM_TOOL_GATEWAY_URL = %q, want %q", got, want)
	}
	if got, want := os.Getenv("SWARM_TOOL_GATEWAY_CONTAINER_URL"), "http://host.docker.internal:"+port; got != want {
		t.Fatalf("SWARM_TOOL_GATEWAY_CONTAINER_URL = %q, want %q", got, want)
	}
	gatewayToken := os.Getenv("SWARM_TOOL_GATEWAY_TOKEN")
	if strings.TrimSpace(gatewayToken) == "" {
		t.Fatal("SWARM_TOOL_GATEWAY_TOKEN was not generated")
	}
	if got, want := len(gatewayToken), base64.RawURLEncoding.EncodedLen(serveGatewayTokenBytes); got != want {
		t.Fatalf("SWARM_TOOL_GATEWAY_TOKEN length = %d, want %d", got, want)
	}
	restore()
	if got := os.Getenv("SWARM_TOOL_GATEWAY_URL"); got != "" {
		t.Fatalf("SWARM_TOOL_GATEWAY_URL after restore = %q, want empty", got)
	}
	if got := os.Getenv("SWARM_TOOL_GATEWAY_CONTAINER_URL"); got != "" {
		t.Fatalf("SWARM_TOOL_GATEWAY_CONTAINER_URL after restore = %q, want empty", got)
	}
	if got := os.Getenv("SWARM_TOOL_GATEWAY_TOKEN"); got != "" {
		t.Fatalf("SWARM_TOOL_GATEWAY_TOKEN after restore = %q, want empty", got)
	}
}

func TestConfigureServeMCPGatewayEnvPreservesExplicitGatewayToken(t *testing.T) {
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "operator-token")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen mcp: %v", err)
	}
	defer listener.Close()

	restore, err := configureServeMCPGatewayEnv(listener.Addr())
	if err != nil {
		t.Fatalf("configure gateway env: %v", err)
	}
	if got := os.Getenv("SWARM_TOOL_GATEWAY_TOKEN"); got != "operator-token" {
		t.Fatalf("SWARM_TOOL_GATEWAY_TOKEN = %q, want explicit operator token", got)
	}
	restore()
	if got := os.Getenv("SWARM_TOOL_GATEWAY_TOKEN"); got != "operator-token" {
		t.Fatalf("SWARM_TOOL_GATEWAY_TOKEN after restore = %q, want explicit operator token", got)
	}
}

func TestConfigureServeMCPGatewayEnvRejectsOldUnifiedPort(t *testing.T) {
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

	restore, err := configureServeMCPGatewayEnv(listener.Addr())
	if err == nil {
		restore()
		t.Fatalf("configure gateway env unexpectedly accepted old unified API port")
	}
	if !strings.Contains(err.Error(), "must target the MCP listener port") {
		t.Fatalf("error = %v, want MCP listener port mismatch", err)
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
		return storeBundle{
			Postgres:           runtimePG,
			SQLDB:              runtimePG.DB,
			SchemaBootstrapper: runtimePG,
			EventStore:         runtimePG,
			SessionRegistry:    sessions.NewPostgresRegistry(runtimePG.DB, cfg.LLM.Session.LockTTL),
			ConversationStore:  runtimePG,
			ManagerStore:       runtimePG,
			ScheduleStore:      runtimePG,
			TurnStore:          runtimePG,
		}, nil
	}
	configuredWorkspaceLifecycleForServe = func(*sql.DB, string, string, semanticview.Source) serveWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var out lockedBuffer
	done := make(chan int, 1)
	go func() {
		done <- runServeRuntime(ctx, repoRoot(), serveOptions{
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
			Output:             &out,
		})
	}()

	waitForServeReadyLine(t, &out, done)
	cancel()
	if code := <-done; code != 0 {
		t.Fatalf("runServeRuntime code = %d\noutput:\n%s", code, out.String())
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
		RejectedSources map[string]string `yaml:"rejected_sources"`
	} `yaml:"source_precedence"`
	ImplementationBoundaries []string `yaml:"implementation_boundaries"`
}

type cliAPIConnectionAuthConfigSpec struct {
	PromotedBy           string   `yaml:"promoted_by"`
	ImplementationStatus string   `yaml:"implementation_status"`
	CanonicalOwner       string   `yaml:"canonical_owner"`
	Scope                string   `yaml:"scope"`
	AppliesTo            []string `yaml:"applies_to"`
	NotAppliesTo         []string `yaml:"not_applies_to"`
	PrecedenceOrder      []string `yaml:"precedence_order"`
	APIServer            struct {
		AcceptedSources struct {
			Flag           string `yaml:"flag"`
			Environment    string `yaml:"environment"`
			ConfigKey      string `yaml:"config_key"`
			BuiltInDefault string `yaml:"built_in_default"`
		} `yaml:"accepted_sources"`
		ValueModel string `yaml:"value_model"`
		Rationale  string `yaml:"rationale"`
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

func watchlistIssueMapsTo(issues []struct {
	ID     int      `yaml:"id"`
	MapsTo []string `yaml:"maps_to"`
}, issueID int, nodeID string) bool {
	for _, issue := range issues {
		if issue.ID == issueID && stringSliceContains(issue.MapsTo, nodeID) {
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
	deadline := time.After(5 * time.Second)
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

	err := verifyBundle(context.Background(), semanticview.Wrap(loadWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier8-boot-verification", "test-boot-missing-pin"))))
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
