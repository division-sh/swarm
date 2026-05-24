package main

import (
	"bytes"
	"context"
	"database/sql"
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
	runtimerunforkexecution "swarm/internal/runtime/runforkexecution"
	runtimerunquiescence "swarm/internal/runtime/runquiescence"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/runtime/sessions"
	runtimetools "swarm/internal/runtime/tools"
	"swarm/internal/store"
	"swarm/internal/testutil"
)

type delayedRunStatusAgent struct {
	id            string
	subscriptions []events.EventType
	started       chan struct{}
	release       chan struct{}
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
		if strings.Contains(stdout.String(), "\n  "+retired) {
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
				if strings.Contains(stdout.String(), "\n  "+retired) {
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
	for _, want := range []string{"Start the Swarm runtime", "--contracts", "--api-listen-addr", "API, WebSocket, health, and readiness routes", "--mcp-listen-addr", "MCP and tools routes", "--platform-spec", "--store", "--self-check", "--dev", "--require-bundle-match", "--no-require-bundle-match", "--abandon-active-runs", "--shutdown-grace", "--verbose"} {
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
	if strings.TrimSpace(spec.ImplementationStatus) != "runtime_bind_implemented_enable_disable_env_pending" {
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
	for _, want := range []string{"#992 implements", "`--health-addr` retirement", "`swarm run --mcp-port` remains fail-closed", "Environment/config precedence remains #844"} {
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
	if strings.TrimSpace(spec.ImplementationStatus) != "implemented" {
		t.Fatalf("api connection/auth/config implementation_status = %q, want implemented", spec.ImplementationStatus)
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
	wantTokenSourceOrder := []string{"--api-token-file", "SWARM_API_TOKEN", "SWARM_API_TOKEN_FILE", "config api_token_file"}
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
	for _, key := range []string{"api_token", "output_format", "no_color", "log_level", "retry", "serve_api_listen_addr", "serve_mcp_listen_addr"} {
		if strings.TrimSpace(spec.CLIConfigFile.RejectedKeys[key]) == "" {
			t.Fatalf("cli config rejected key %q missing: %#v", key, spec.CLIConfigFile.RejectedKeys)
		}
	}
	for _, key := range []string{"contracts_path", "platform_spec_path"} {
		if !strings.Contains(spec.CLIConfigFile.SharedNonAPIKeys[key], "contract_platform_spec_path_resolution") {
			t.Fatalf("cli config shared non-API key %q missing contract path owner: %#v", key, spec.CLIConfigFile.SharedNonAPIKeys)
		}
	}
	for _, key := range []string{"SWARM_API_PORT", "SWARM_MCP_PORT"} {
		if !strings.Contains(spec.ServeListenerEnvConfigBoundary.RejectedPorts[key], "Not promoted") {
			t.Fatalf("serve listener rejected port %q missing not-promoted rule:\n%s", key, spec.ServeListenerEnvConfigBoundary.RejectedPorts[key])
		}
	}
	for _, key := range []string{"SWARM_API_LISTEN_ADDR", "SWARM_MCP_LISTEN_ADDR"} {
		if !strings.Contains(spec.ServeListenerEnvConfigBoundary.ReservedCandidates[key], "later #884/#750") {
			t.Fatalf("serve listener reserved candidate %q missing split rule:\n%s", key, spec.ServeListenerEnvConfigBoundary.ReservedCandidates[key])
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
	wantPlatformOrder := []string{"--platform-spec", "config platform_spec_path", "built-in tracked platform spec"}
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

func TestServeBundleMatchAdmissionRejectsActiveMismatches(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	bootFingerprint := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	mismatchFingerprint := "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	runID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_fingerprint, started_at)
		VALUES ($1::uuid, 'running', $2, now())
	`, runID, mismatchFingerprint); err != nil {
		t.Fatalf("seed active mismatched run: %v", err)
	}

	err := enforceServeBundleMatchAdmission(ctx, pg, bootFingerprint, true)
	if err == nil {
		t.Fatal("enforceServeBundleMatchAdmission error = nil, want mismatch")
	}
	if got := err.Error(); !strings.Contains(got, "active run bundle mismatch") || !strings.Contains(got, runID) || !strings.Contains(got, mismatchFingerprint) {
		t.Fatalf("admission error = %q, want run/fingerprint detail", got)
	}
}

func TestServeBundleMatchAdmissionAllowsMatchingNullAndDisabled(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	bootFingerprint := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	mismatchFingerprint := "sha256:2222222222222222222222222222222222222222222222222222222222222222"

	for _, seed := range []struct {
		runID       string
		status      string
		fingerprint sql.NullString
	}{
		{runID: uuid.NewString(), status: "running", fingerprint: sql.NullString{String: bootFingerprint, Valid: true}},
		{runID: uuid.NewString(), status: "paused", fingerprint: sql.NullString{}},
		{runID: uuid.NewString(), status: "completed", fingerprint: sql.NullString{String: mismatchFingerprint, Valid: true}},
	} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO runs (run_id, status, bundle_fingerprint, started_at)
			VALUES ($1::uuid, $2, $3, now())
		`, seed.runID, seed.status, seed.fingerprint); err != nil {
			t.Fatalf("seed run %s: %v", seed.runID, err)
		}
	}
	if err := enforceServeBundleMatchAdmission(ctx, pg, bootFingerprint, true); err != nil {
		t.Fatalf("enforceServeBundleMatchAdmission matching/null/completed: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_fingerprint, started_at)
		VALUES ($1::uuid, 'running', $2, now())
	`, uuid.NewString(), mismatchFingerprint); err != nil {
		t.Fatalf("seed disabled mismatch: %v", err)
	}
	if err := enforceServeBundleMatchAdmission(ctx, pg, bootFingerprint, false); err != nil {
		t.Fatalf("enforceServeBundleMatchAdmission disabled: %v", err)
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

func TestCLI_ForkIsHardRetired(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "fork", args: []string{"fork"}, want: "ERROR: `swarm fork` was removed in v1."},
		{name: "fork-help", args: []string{"fork", "--help"}, want: "ERROR: `swarm fork` was removed in v1."},
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
	t.Setenv("SWARM_LLM_RUNTIME_MODE", "api")
	t.Setenv("SWARM_RUNTIME_MAX_CONCURRENT_AGENTS", "4")
	t.Setenv("SWARM_CLAUDE_DEFAULT_MODEL", "test-model")
	cfg, err := defaultRuntimeConfig()
	if err == nil || !strings.Contains(err.Error(), "SWARM_RUNTIME_MAX_CONCURRENT_AGENTS") {
		t.Fatalf("defaultRuntimeConfig error = %v, want unsupported env rejection", err)
	}
	if cfg != nil {
		t.Fatalf("defaultRuntimeConfig cfg = %#v, want nil on unsupported env", cfg)
	}
}

func TestLoadRuntimeConfig_RejectsUnsupportedRuntimeControlsFromFile(t *testing.T) {
	cfgText := strings.Join([]string{
		"runtime:",
		"  max_concurrent_agents: 4",
		"llm:",
		"  runtime_mode: api",
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
	if err := am.SpawnAgent(runtimeactors.AgentConfig{ID: testAgent.id}); err != nil {
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
	if err := am.SpawnAgent(runtimeactors.AgentConfig{ID: testAgent.id}); err != nil {
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
  model_tier: sonnet
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
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
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
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
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
	buildStoresForServe = func(ctx context.Context, _ string, cfg *config.Config) (storeBundle, error) {
		if _, err := runtimePG.BindSchemaCapabilities(ctx); err != nil {
			return storeBundle{}, err
		}
		return storeBundle{
			Postgres:          runtimePG,
			SQLDB:             runtimePG.DB,
			EventStore:        runtimePG,
			SessionRegistry:   sessions.NewPostgresRegistry(runtimePG.DB, cfg.LLM.Session.LockTTL),
			ConversationStore: runtimePG,
			ManagerStore:      runtimePG,
			ScheduleStore:     runtimePG,
			TurnStore:         runtimePG,
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
			buildStoresForServe = func(ctx context.Context, _ string, cfg *config.Config) (storeBundle, error) {
				if _, err := runtimePG.BindSchemaCapabilities(ctx); err != nil {
					return storeBundle{}, err
				}
				return storeBundle{
					Postgres:          runtimePG,
					SQLDB:             runtimePG.DB,
					EventStore:        runtimePG,
					SessionRegistry:   sessions.NewPostgresRegistry(runtimePG.DB, cfg.LLM.Session.LockTTL),
					ConversationStore: runtimePG,
					ManagerStore:      runtimePG,
					ScheduleStore:     runtimePG,
					TurnStore:         runtimePG,
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
	restore()
	if got := os.Getenv("SWARM_TOOL_GATEWAY_URL"); got != "" {
		t.Fatalf("SWARM_TOOL_GATEWAY_URL after restore = %q, want empty", got)
	}
	if got := os.Getenv("SWARM_TOOL_GATEWAY_CONTAINER_URL"); got != "" {
		t.Fatalf("SWARM_TOOL_GATEWAY_CONTAINER_URL after restore = %q, want empty", got)
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
	buildStoresForServe = func(ctx context.Context, _ string, cfg *config.Config) (storeBundle, error) {
		if _, err := runtimePG.BindSchemaCapabilities(ctx); err != nil {
			return storeBundle{}, err
		}
		return storeBundle{
			Postgres:          runtimePG,
			SQLDB:             runtimePG.DB,
			EventStore:        runtimePG,
			SessionRegistry:   sessions.NewPostgresRegistry(runtimePG.DB, cfg.LLM.Session.LockTTL),
			ConversationStore: runtimePG,
			ManagerStore:      runtimePG,
			ScheduleStore:     runtimePG,
			TurnStore:         runtimePG,
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
	cleanup func(context.Context) (runtimedestructivereset.ContainerResetResult, error)
}

func (s serveRuntimeWorkspaceStub) CleanupDevEntityContainers(ctx context.Context) (runtimedestructivereset.ContainerResetResult, error) {
	if s.cleanup != nil {
		return s.cleanup(ctx)
	}
	return runtimedestructivereset.ContainerResetResult{}, nil
}

func (serveRuntimeWorkspaceStub) ManagedResetContainerInventory(context.Context) ([]runtimedestructivereset.ContainerRef, error) {
	return nil, nil
}

func (serveRuntimeWorkspaceStub) InspectManagedContainer(context.Context, string) (runtimedestructivereset.ManagedContainerInspection, error) {
	return runtimedestructivereset.ManagedContainerInspection{}, nil
}

func (serveRuntimeWorkspaceStub) StopManagedContainer(context.Context, string) error {
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
	PromotedBy               string `yaml:"promoted_by"`
	RuntimeBindImplementedBy string `yaml:"runtime_bind_implemented_by"`
	ImplementationStatus     string `yaml:"implementation_status"`
	CanonicalOwner           string `yaml:"canonical_owner"`
	Summary                  string `yaml:"summary"`
	Listeners                struct {
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
		AcceptedSources map[string]string `yaml:"accepted_sources"`
		SourceOrder     []string          `yaml:"source_order"`
		RejectedSources map[string]string `yaml:"rejected_sources"`
		TokenFileRule   string            `yaml:"token_file_rule"`
	} `yaml:"api_token"`
	CLIConfigFile struct {
		AcceptedSources  map[string]string `yaml:"accepted_sources"`
		RejectedSources  map[string]string `yaml:"rejected_sources"`
		AcceptedKeys     map[string]string `yaml:"accepted_keys"`
		SharedNonAPIKeys map[string]string `yaml:"shared_non_api_keys"`
		RejectedKeys     map[string]string `yaml:"rejected_keys"`
	} `yaml:"cli_config_file"`
	ServeListenerEnvConfigBoundary struct {
		RejectedPorts      map[string]string `yaml:"rejected_ports"`
		ReservedCandidates map[string]string `yaml:"reserved_candidates"`
		Rule               string            `yaml:"rule"`
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
		"  runtime_mode: api",
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
