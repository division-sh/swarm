package cliapp

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	operatorread "github.com/division-sh/swarm/internal/operatorread"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/config"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testpostgres"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/requiredagentsparentconnect"
)

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
			for _, want := range []string{"Usage:", "[directory]", "--json", "--quiet", "--no-color", "--log-level"} {
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
	for _, want := range []string{"--json", "--quiet", "--no-color", "--log-level"} {
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
	for _, want := range []string{"--json", "--quiet", "--no-color", "--log-level"} {
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
	for _, want := range []string{"Swarm dev", "Commit: unknown", "Built: unknown", "Go:", "Platform spec digest: "} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("version output missing %q:\n%s", want, stdout.String())
		}
	}
}

func TestCLI_ServeOwnsRuntimeStartupFlags(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := executeRootCommand(context.Background(), t.TempDir(), []string{"serve", ".", "--help"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("serve help code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{"Start the Swarm runtime", "[directory]", "--config", "--backend", "openai_responses", "--workspace-backend", "--api-listen-addr", "API, WebSocket, health, and readiness routes", "--mcp-listen-addr", "MCP and tools routes", "--store", "--self-check", "--dev", "--shutdown-grace", "--verbose"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("serve help missing %q:\n%s", want, stdout.String())
		}
	}
	for _, notWant := range []string{"--platform-spec", "--health-addr", "unified serve listener", "--api-port", "--mcp-port", "--api ", "--no-api", "--mcp ", "--no-mcp", "--log-level", "--require-bundle-match", "--no-require-bundle-match", "--abandon-active-runs"} {
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
			args:       []string{"serve", ".", "--health-addr", "127.0.0.1:0"},
			wantStderr: "unknown flag: --health-addr",
		},
		{
			name:       "api bare port rejected",
			args:       []string{"serve", ".", "--api-listen-addr", "8081"},
			wantStderr: "--api-listen-addr must be a host:port listen address",
		},
		{
			name:       "mcp host without port rejected",
			args:       []string{"serve", ".", "--mcp-listen-addr", "127.0.0.1"},
			wantStderr: "--mcp-listen-addr must be a host:port listen address",
		},
		{
			name:       "api url rejected",
			args:       []string{"serve", ".", "--api-listen-addr", "http://127.0.0.1:8081"},
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
	opts.runServe = func(_ context.Context, _ InvocationRoot, serveOpts ServeOptions) int {
		captured = serveOpts
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", ".", "--api-listen-addr", "0.0.0.0:9001"}, &stdout, &stderr, opts)
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
	code = executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", ".", "--mcp-listen-addr", "127.0.0.1:9002"}, &stdout, &stderr, opts)
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
			opts.runServe = func(_ context.Context, _ InvocationRoot, serveOpts ServeOptions) int {
				captured = serveOpts
				return 0
			}

			var stdout, stderr bytes.Buffer
			args := append([]string{"serve", "."}, tc.args...)
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
			opts.runServe = func(context.Context, InvocationRoot, ServeOptions) int {
				ran = true
				return 0
			}

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", "."}, &stdout, &stderr, opts)
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
		auth, err := ResolveServeAPIAuth(mustInvocationRootForTest(t.TempDir()), DefaultServeOptions())
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
		auth, err := ResolveServeAPIAuth(mustInvocationRootForTest(t.TempDir()), ServeOptions{APITokenFile: flagTokenFile, APITokenFileFlagSet: true})
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
		auth, err := ResolveServeAPIAuth(mustInvocationRootForTest(t.TempDir()), DefaultServeOptions())
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
		_, err := ResolveServeAPIAuth(mustInvocationRootForTest(t.TempDir()), ServeOptions{APITokenFile: tokenFile, APITokenFileFlagSet: true})
		if err == nil || !strings.Contains(err.Error(), "unknown SWARM_* env is not accepted") || !strings.Contains(err.Error(), "SWARM_API_TOKEN") {
			t.Fatalf("err = %v, want removed-env diagnostic", err)
		}
	})

	t.Run("blank and missing token files fail closed", func(t *testing.T) {
		isolateCLIAPIConfigEnv(t)
		blank := writeCLIAPITokenFile(t, "  \n")
		if _, err := ResolveServeAPIAuth(mustInvocationRootForTest(t.TempDir()), ServeOptions{APITokenFile: blank, APITokenFileFlagSet: true}); err == nil || !strings.Contains(err.Error(), "--api-token-file is blank") {
			t.Fatalf("blank token err = %v, want blank token failure", err)
		}
		missing := filepath.Join(t.TempDir(), "missing-token")
		if _, err := ResolveServeAPIAuth(mustInvocationRootForTest(t.TempDir()), ServeOptions{APITokenFile: missing, APITokenFileFlagSet: true}); err == nil || !strings.Contains(err.Error(), "read --api-token-file") {
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
	opts.runServe = func(_ context.Context, _ InvocationRoot, serveOpts ServeOptions) int {
		captured = serveOpts
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", ".", "--config", runtimeConfig}, &stdout, &stderr, opts)
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

func TestCLI_ServeRejectsRetiredDataFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", ".", "--data", t.TempDir()}, &stdout, &stderr, defaultRootCommandOptions())
	if code == 0 {
		t.Fatalf("serve code = 0, want failure stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "unknown flag: --data") {
		t.Fatalf("serve stderr = %q, want hard retirement", stderr.String())
	}
}

func TestCLI_ServeWorkspaceBackendFlagFeedsServeOptions(t *testing.T) {
	var captured ServeOptions
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ InvocationRoot, serveOpts ServeOptions) int {
		captured = serveOpts
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", ".", "--workspace-backend", "host"}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("serve code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.WorkspaceBackend != workspace.BackendHost || !captured.WorkspaceBackendSet {
		t.Fatalf("workspace backend opts = backend %q set %v, want host set=true", captured.WorkspaceBackend, captured.WorkspaceBackendSet)
	}
}

func TestCLI_ServeWorkspaceBackendFlagRejectsUnsupportedBackend(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", ".", "--workspace-backend", "none"}, &stdout, &stderr, defaultRootCommandOptions())
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
			args: []string{"serve", ".", "--api-listen-addr", "127.0.0.1:9401", "--mcp-listen-addr", "127.0.0.1:9402"},
			setup: func(t *testing.T) {
				t.Setenv("SWARM_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))
			},
			wantAPI: "127.0.0.1:9401",
			wantMCP: "127.0.0.1:9402",
			wantRan: true,
		},
		{
			name: "retired env vars fail before malformed cli config",
			args: []string{"serve", "."},
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
			args: []string{"serve", "."},
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
			opts.runServe = func(_ context.Context, _ InvocationRoot, serveOpts ServeOptions) int {
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
	opts.runServe = func(_ context.Context, _ InvocationRoot, serveOpts ServeOptions) int {
		captured = serveOpts
		return 0
	}

	var stdout, stderr bytes.Buffer
	wantGrace := 42 * time.Second
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", ".", "--dev", "--shutdown-grace", wantGrace.String()}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("serve code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !captured.Dev {
		t.Fatal("serve dev = false, want true")
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
	opts.runServe = func(_ context.Context, _ InvocationRoot, serveOpts ServeOptions) int {
		captured = serveOpts
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", ".", "--dev", "--verbose"}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("serve code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !captured.Dev || !captured.Verbose {
		t.Fatalf("serve dev/verbose = %t/%t, want both explicit modes", captured.Dev, captured.Verbose)
	}
}

func TestCLI_ServeRejectsRetiredBundleMatchFlagsBeforeOwner(t *testing.T) {
	for _, flag := range []string{"--require-bundle-match", "--require-bundle-match=false", "--no-require-bundle-match"} {
		t.Run(flag, func(t *testing.T) {
			var called atomic.Bool
			opts := defaultRootCommandOptions()
			opts.runServe = func(context.Context, InvocationRoot, ServeOptions) int {
				called.Store(true)
				return 0
			}

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", ".", flag}, &stdout, &stderr, opts)
			if code != 2 || !strings.Contains(stderr.String(), "unknown flag") {
				t.Fatalf("serve %s code = %d stderr=%s stdout=%s, want unknown-flag rejection", flag, code, stderr.String(), stdout.String())
			}
			if called.Load() {
				t.Fatalf("serve owner was called for retired flag %s", flag)
			}
		})
	}
}

func TestSourceFreeCommandsDoNotRequireSourceCheckout(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "bare root help", args: nil, want: "Usage:"},
		{name: "root help", args: []string{"--help"}, want: "Usage:"},
		{name: "version", args: []string{"version"}, want: "Swarm dev"},
		{name: "completion", args: []string{"completion", "bash"}, want: "swarm"},
		{name: "serve help", args: []string{"serve", ".", "--help"}, want: "Start the Swarm runtime"},
		{name: "verify help", args: []string{"verify", "--help"}, want: "Validate contract files or compare a run's state with its history"},
		{name: "run help", args: []string{"run", "start", "--help"}, want: "Publish into a served flow's live run, or reattach to one."},
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

func TestCLISourceAwareLocalCommandsAcceptOmittedDirectoryAsInvocationRoot(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{name: "serve", args: []string{"serve"}},
		{name: "verify", args: []string{"verify"}},
		{name: "describe", args: []string{"describe"}},
		{name: "describe routes", args: []string{"describe", "routes"}},
		{name: "test", args: []string{"test"}},
		{name: "packs list", args: []string{"packs", "list"}},
		{name: "packs show", args: []string{"packs", "show", "telegram"}},
		{name: "import", args: []string{"import", "telegram"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			invocationRoot := t.TempDir()
			var ownerCalls atomic.Int32
			opts := defaultRootCommandOptions()
			opts.runServe = func(context.Context, InvocationRoot, ServeOptions) int {
				ownerCalls.Add(1)
				return 0
			}
			var stdout, stderr bytes.Buffer
			_ = executeRootCommandWithOptions(context.Background(), invocationRoot, tc.args, &stdout, &stderr, opts)
			if strings.Contains(stderr.String(), "source directory is required") || strings.Contains(stderr.String(), "requires at least") {
				t.Fatalf("omitted cwd was rejected: stdout=%s stderr=%s", stdout.String(), stderr.String())
			}
			if tc.name == "serve" && ownerCalls.Load() != 1 {
				t.Fatalf("serve owner calls = %d, want one cwd-selected invocation", ownerCalls.Load())
			}
		})
	}
}

func TestAssetCommandsCaptureInvocationRootAtExecution(t *testing.T) {
	repo := t.TempDir()
	chdirForTest(t, repo)

	var capturedRepo string
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, root InvocationRoot, _ ServeOptions) int {
		capturedRepo = root.Path()
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), "", []string{"serve", "."}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("serve code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if capturedRepo != repo {
		t.Fatalf("serve repo = %q, want discovered repo %q", capturedRepo, repo)
	}
}

func TestVerifyCommandIgnoresInvocationRootDotEnv(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	_ = os.Unsetenv("SWARM_CONTRACTS_PATH")
	repo := t.TempDir()
	sourceRoot := writeEnvAuthorityContractsFixture(t, "dot-env-contracts")
	configPath := writeTestVerifyRuntimeConfig(t)
	writeWorkflowValidationFixtureFile(t, filepath.Join(repo, ".env"), "SWARM_CONTRACTS_PATH="+sourceRoot+"\nBROKEN\n")
	chdirForTest(t, repo)

	var stdout, stderr bytes.Buffer
	code := executeRootCommand(context.Background(), "", []string{"verify", "--portable", "--config", configPath}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("verify unexpectedly consumed contracts path from repo .env: stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String()+stderr.String(), sourceRoot) {
		t.Fatalf("verify output referenced repo .env contracts path stdout=%s stderr=%s", stdout.String(), stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = executeRootCommand(context.Background(), "", []string{"verify", sourceRoot, "--portable", "--config", configPath}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("verify with explicit contracts code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !strings.Contains(stdout.String(), "verify ok: source="+filepath.Base(sourceRoot)+"@") {
		t.Fatalf("verify explicit contracts output missing success marker:\n%s", stdout.String())
	}
}

func TestRunVerifyCommandUsesEmbeddedPlatformSpecWithoutRepoRoot(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	chdirForTest(t, t.TempDir())
	root := t.TempDir()

	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), "stages: []\n")

	var buf bytes.Buffer
	code := runVerifyCommandWithContractsForTest(t, context.Background(), "", root, &buf)
	if code != 0 {
		t.Fatalf("runVerifyCommand exit code = %d, output = %q", code, buf.String())
	}
	if !strings.Contains(buf.String(), "verify ok: source=") {
		t.Fatalf("verify output missing success marker:\n%s", buf.String())
	}
}

func TestConfiguredWorkspaceLifecycleNeedsNoAmbientDataSource(t *testing.T) {
	projection := testRuntimeSourceProjection(t)

	manager, err := configuredWorkspaceLifecycle(nil, projection, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), WorkspaceMountSources{})
	if err != nil {
		t.Fatalf("configuredWorkspaceLifecycle: %v", err)
	}
	if err := manager.ValidateSource(context.Background(), semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})); err != nil {
		t.Fatalf("ValidateSource without ambient data: %v", err)
	}
}

func TestConfiguredWorkspaceLifecycleRejectsExplicitAmbientData(t *testing.T) {
	_, err := configuredWorkspaceLifecycle(nil, nil, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), WorkspaceMountSources{
		DataSource:       t.TempDir(),
		DataSourceSource: "--data",
	})
	if err == nil || !strings.Contains(err.Error(), "ambient workspace data sources are unsupported") {
		t.Fatalf("configuredWorkspaceLifecycle error = %v, want hard retirement", err)
	}
}

func TestConfiguredWorkspaceLifecycleRejectsUnreadableAmbientDataWithoutFallback(t *testing.T) {
	missingDataDir := filepath.Join(t.TempDir(), "missing-data")
	_, err := configuredWorkspaceLifecycle(nil, nil, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), WorkspaceMountSources{
		DataSource:       missingDataDir,
		DataSourceSource: "--data",
	})
	if err == nil || !strings.Contains(err.Error(), "ambient workspace data sources are unsupported") {
		t.Fatalf("configuredWorkspaceLifecycle error = %v, want hard retirement before filesystem fallback", err)
	}
}

func TestConfiguredWorkspaceLifecycleRejectsExplicitDataSourceWithVolumesFrom(t *testing.T) {
	var cfg config.Config
	err := yaml.Unmarshal([]byte("workspace: {data_source: /data, volumes_from: swarm-orchestrator}"), &cfg)
	if err == nil || !strings.Contains(err.Error(), "data_source") || !strings.Contains(err.Error(), "Valid fields:") {
		t.Fatalf("config admission error = %v, want rejection before workspace creation", err)
	}
}

func testRuntimeSourceProjection(t *testing.T) *sourceartifact.RuntimeProjection {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "manifest.yaml"), []byte("version: 1.0.0\nplatform_version: '*'\n"), 0o600); err != nil {
		t.Fatalf("write source schema: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("stages: []\n"), 0o600); err != nil {
		t.Fatalf("write source schema: %v", err)
	}
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatalf("admit source artifact: %v", err)
	}
	projection, err := sourceartifact.MaterializeRuntimeProjection(artifact)
	if err != nil {
		t.Fatalf("materialize source projection: %v", err)
	}
	t.Cleanup(func() {
		if err := projection.Release(); err != nil {
			t.Fatalf("release source projection: %v", err)
		}
	})
	return projection
}

func TestConfiguredWorkspaceLifecycleForBackendSelectsHostWithoutDocker(t *testing.T) {
	cfg := &config.Config{Workspace: config.WorkspaceConfig{HostRoot: filepath.Join(t.TempDir(), "host-workspaces")}}
	projection := testRuntimeSourceProjection(t)
	lifecycle, err := ConfiguredWorkspaceLifecycleForBackend(cfg, projection, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), WorkspaceMountSources{}, WorkspaceBackendSelection{Backend: workspace.BackendHost, Source: "--workspace-backend"})
	if err != nil {
		t.Fatalf("ConfiguredWorkspaceLifecycleForBackend: %v", err)
	}
	manager, ok := lifecycle.(*workspace.HostManager)
	if !ok {
		t.Fatalf("lifecycle = %T, want *workspace.HostManager", lifecycle)
	}
	if err := manager.BindSourceProjection(projection); err != nil {
		t.Fatalf("BindSourceProjection: %v", err)
	}
	if err := manager.ValidateSource(context.Background(), semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})); err != nil {
		t.Fatalf("ValidateSource: %v", err)
	}
	if err := manager.EnsureSystemWorkspaces(context.Background()); err != nil {
		t.Fatalf("EnsureSystemWorkspaces: %v", err)
	}
}

func TestConfiguredWorkspaceLifecycleForBackendRejectsHostVolumesFrom(t *testing.T) {
	var cfg config.Config
	err := yaml.Unmarshal([]byte("workspace: {backend: host, volumes_from: swarm-orchestrator}"), &cfg)
	if err == nil || !strings.Contains(err.Error(), "volumes_from") || !strings.Contains(err.Error(), "Valid fields:") {
		t.Fatalf("config admission error = %v, want rejection before host workspace creation", err)
	}
}

func TestWorkspaceSourceAdmissionRejectsUnsupportedAuthoritiesBeforeEffects(t *testing.T) {
	for _, key := range []string{"data_source", "volumes_from"} {
		for _, value := range []string{"null", "''", "'   '", "{}", "[]", "false", "some-data"} {
			for _, body := range []string{
				"workspace: {" + key + ": " + value + "}\n",
				"workspace: {<<: &source {" + key + ": " + value + "}}\n",
			} {
				t.Run(body, func(t *testing.T) {
					root := t.TempDir()
					configPath := filepath.Join(root, "swarm.yaml")
					writeRuntimeConfigText(t, configPath, body)
					if _, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root, ExplicitPath: configPath}); err == nil || !strings.Contains(err.Error(), key) || !strings.Contains(err.Error(), "Valid fields:") {
						t.Fatalf("source admission = %v, want current workspace vocabulary", err)
					}
					if _, err := os.Stat(filepath.Join(root, ".swarm", "data")); !os.IsNotExist(err) {
						t.Fatalf("invalid source created default data: %v", err)
					}
				})
			}
		}
	}
}

func TestWorkspaceMountAdmissionRejectsProgrammaticAmbientSource(t *testing.T) {
	root := t.TempDir()
	mount, err := resolveWorkspaceMountSourcesForLocalState(root, "some-data", nil, localRuntimeStateProject{}, true)
	if err == nil || !strings.Contains(err.Error(), "unsupported") || mount != (WorkspaceMountSources{}) {
		t.Fatalf("ambient source admitted or fell back: %#v, %v", mount, err)
	}
	if _, err := os.Stat(filepath.Join(root, ".swarm", "data")); !os.IsNotExist(err) {
		t.Fatalf("invalid source created default data: %v", err)
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

func TestCLI_ServeVerboseFlagConsumesServeOwner(t *testing.T) {
	var captured ServeOptions
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ InvocationRoot, serveOpts ServeOptions) int {
		captured = serveOpts
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", ".", "--verbose"}, &stdout, &stderr, opts)
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

func TestCLI_ServeRejectsAbandonActiveRunsBeforeOwner(t *testing.T) {
	for _, flag := range []string{"--abandon-active-runs", "--abandon-active-runs=false"} {
		t.Run(flag, func(t *testing.T) {
			var called bool
			opts := defaultRootCommandOptions()
			opts.runServe = func(context.Context, InvocationRoot, ServeOptions) int {
				called = true
				return 0
			}
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", ".", flag}, &stdout, &stderr, opts)
			if code != CLIExitValidation || !strings.Contains(stderr.String(), "swarm control stop --all") {
				t.Fatalf("serve %s code=%d stderr=%s, want refusal naming stop command", flag, code, stderr.String())
			}
			if called {
				t.Fatal("retired option reached the serve owner")
			}
		})
	}
}

func TestCLI_ServeShutdownGraceFlagConsumesServeOwner(t *testing.T) {
	var captured ServeOptions
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ InvocationRoot, serveOpts ServeOptions) int {
		captured = serveOpts
		return 0
	}

	var stdout, stderr bytes.Buffer
	wantGrace := 42 * time.Second
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", ".", "--shutdown-grace", wantGrace.String()}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("serve code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.ShutdownGrace != wantGrace {
		t.Fatalf("serve shutdown grace = %s, want %s", captured.ShutdownGrace, wantGrace)
	}
}

func TestCLI_ServeShutdownGraceRejectsNonPositiveDurationBeforeOwner(t *testing.T) {
	for _, args := range [][]string{
		{"serve", ".", "--shutdown-grace", "0s"},
		{"serve", ".", "--shutdown-grace", "-1s"},
		{"serve", ".", "--shutdown-grace", "not-a-duration"},
	} {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			var called atomic.Bool
			opts := defaultRootCommandOptions()
			opts.runServe = func(context.Context, InvocationRoot, ServeOptions) int {
				called.Store(true)
				return 0
			}

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, opts)
			if code != 2 {
				t.Fatalf("serve code = %d stderr=%s stdout=%s, want 2", code, stderr.String(), stdout.String())
			}
			if args[len(args)-1] == "not-a-duration" {
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

func servedEventPublishFixtureBundleHash(t *testing.T, sourceRoot string) string {
	t.Helper()
	bundle := loadWorkflowValidationBundleAt(t, sourceRoot)
	bundleHash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		t.Fatalf("BundleHash(%s): %v", sourceRoot, err)
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
	code := executeRootCommand(context.Background(), t.TempDir(), []string{"verify", missingContracts}, &stdout, &stderr)
	if code != CLIExitValidation {
		t.Fatalf("verify code = %d, want %d stderr=%s stdout=%s", code, CLIExitValidation, stderr.String(), stdout.String())
	}
	if !strings.Contains(stdout.String(), "admission validation failed; startup execution not performed") {
		t.Fatalf("verify stdout omitted the failed admission ledger: %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "source directory") {
		t.Fatalf("verify stderr = %q, want local contract resolution failure", stderr.String())
	}
}

func TestProjectRunOperationalStatus_UsesDeliveryLifecycleWhenRunIsOperationallyStalled(t *testing.T) {
	report := operatorread.RunDebugReport{
		OperationalFacts: &operatorread.RunOperationalFacts{Complete: true, Instances: []operatorread.RunInstanceFacts{{FlowInstance: "work/a", Complete: true}}},
		RunTableStatus:   "running",
		LastEventAt:      time.Unix(1700000000, 0).UTC(),
		Deliveries: []operatorread.RunDebugDeliveryCount{
			{SubscriberID: "agent-1", Status: "delivered", Count: 2},
			{SubscriberID: "agent-2", Status: "failed", Count: 1},
		},
	}

	got := operatorread.ProjectRunOperationalStatus(report)
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

func TestProjectRunOperationalStatus_DoesNotCreateScoringObligations(t *testing.T) {
	report := operatorread.RunDebugReport{
		OperationalFacts: &operatorread.RunOperationalFacts{Complete: true, Instances: []operatorread.RunInstanceFacts{{FlowInstance: "work/a", Complete: true}}},
		RunTableStatus:   "running",
		LastEventAt:      time.Unix(1700000000, 0).UTC(),
		EventCounts: []operatorread.RunDebugEventCount{
			{EventName: "scoring/scoring.requested", Count: 1},
		},
		Deliveries: []operatorread.RunDebugDeliveryCount{
			{SubscriberID: "agent-1", Status: "delivered", Count: 1},
		},
	}

	got := operatorread.ProjectRunOperationalStatus(report)
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

func TestProjectRunOperationalStatus_TreatsScopedShortlistAsTerminalScoringOutcome(t *testing.T) {
	report := operatorread.RunDebugReport{
		OperationalFacts: &operatorread.RunOperationalFacts{Complete: true, Instances: []operatorread.RunInstanceFacts{{FlowInstance: "work/a", Complete: true}}},
		RunTableStatus:   "running",
		LastEventAt:      time.Unix(1700000000, 0).UTC(),
		EventCounts: []operatorread.RunDebugEventCount{
			{EventName: "scoring/scoring.requested", Count: 1},
			{EventName: "scoring/vertical.shortlisted", Count: 1},
		},
		Deliveries: []operatorread.RunDebugDeliveryCount{
			{SubscriberID: "agent-1", Status: "delivered", Count: 1},
		},
	}

	got := operatorread.ProjectRunOperationalStatus(report)
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
	report := operatorread.RunDebugReport{
		RunTableStatus: "running",
		LastEventAt:    time.Unix(1700000000, 0).UTC(),
		Deliveries: []operatorread.RunDebugDeliveryCount{
			{SubscriberID: "agent-1", Status: "in_progress", Count: 1},
			{SubscriberID: "agent-2", Status: "delivered", Count: 1},
		},
	}

	got := operatorread.ProjectRunOperationalStatus(report)
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
    rotate_on_parse_failures: 3
  claude_cli:
    command: true
    timeout: 2s
    output_format: json
`,
		parsed.Host, parsed.Port, parsed.Database, parsed.User, parsed.SSLMode))
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
	cfg, err := DefaultRuntimeConfig()
	if err == nil || !strings.Contains(err.Error(), "SWARM_RUNTIME_MAX_CONCURRENT_AGENTS") {
		t.Fatalf("defaultRuntimeConfig error = %v, want unsupported env rejection", err)
	}
	if cfg != nil {
		t.Fatalf("defaultRuntimeConfig cfg = %#v, want nil on unsupported env", cfg)
	}
}

func TestDefaultRuntimeConfig_RejectsRetiredLLMRuntimeModeEnv(t *testing.T) {
	t.Setenv("SWARM_LLM_RUNTIME_MODE", "api")
	cfg, err := DefaultRuntimeConfig()
	if err == nil || !strings.Contains(err.Error(), "SWARM_LLM_RUNTIME_MODE") || !strings.Contains(err.Error(), "unknown SWARM_* env is not accepted") {
		t.Fatalf("DefaultRuntimeConfig error = %v, want unknown env rejection", err)
	}
	if cfg != nil {
		t.Fatalf("defaultRuntimeConfig cfg = %#v, want nil on retired env", cfg)
	}
}

func TestDefaultRuntimeConfig_RejectsRetiredLLMBackendEnv(t *testing.T) {
	t.Setenv("SWARM_LLM_BACKEND", "cli_test")
	cfg, err := DefaultRuntimeConfig()
	if err == nil || !strings.Contains(err.Error(), "SWARM_LLM_BACKEND") || !strings.Contains(err.Error(), "unknown SWARM_* env is not accepted") {
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
		"SWARM_RUNTIME_RECOVERY_ON_STARTUP":          "false",
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
	if !cfg.Runtime.RecoveryOnStartup {
		t.Fatalf("recovery_on_startup = false, want built-in true regardless of retired environment override")
	}
	if cfg.LLM.Session.LockTTL != 10*time.Second || cfg.LLM.Session.RotateOnParseFailures != 3 {
		t.Fatalf("session defaults = %#v, want built-ins", cfg.LLM.Session)
	}
	if cfg.LLM.ClaudeCLI.Command != "claude" ||
		cfg.LLM.ClaudeCLI.Timeout != time.Hour ||
		cfg.LLM.ClaudeCLI.OutputFormat != "stream-json" {
		t.Fatalf("claude_cli defaults = %#v, want built-ins", cfg.LLM.ClaudeCLI)
	}
}

func TestDefaultRuntimeConfig_RejectsRetiredOpenAICompatibleBaseURLEnv(t *testing.T) {
	t.Setenv("SWARM_OPENAI_COMPATIBLE_BASE_URL", "https://example.test/v1")
	cfg, err := DefaultRuntimeConfig()
	if err == nil || !strings.Contains(err.Error(), "SWARM_OPENAI_COMPATIBLE_BASE_URL") || !strings.Contains(err.Error(), "unknown SWARM_* env is not accepted") {
		t.Fatalf("defaultRuntimeConfig error = %v, want base URL env retirement", err)
	}
	if cfg != nil {
		t.Fatalf("defaultRuntimeConfig cfg = %#v, want nil on retired env", cfg)
	}
}

func TestLoadRuntimeConfigWithOptions_PreservesRuntimeLLMTypedConfigValues(t *testing.T) {
	repo := t.TempDir()
	configPath := filepath.Join(repo, "runtime.yaml")
	writeRuntimeConfigText(t, configPath, strings.Join([]string{
		"runtime:",
		"  recovery_on_startup: true",
		"llm:",
		"  backend: claude_cli",
		"  session:",
		"    lock_ttl: 22s",
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
	if cfg.LLM.Session.LockTTL != 22*time.Second || cfg.LLM.Session.RotateOnParseFailures != 9 {
		t.Fatalf("session config = %#v, want typed config values", cfg.LLM.Session)
	}
	if cfg.LLM.ClaudeCLI.Command != "echo" || cfg.LLM.ClaudeCLI.Timeout != 44*time.Second || cfg.LLM.ClaudeCLI.OutputFormat != "json" {
		t.Fatalf("claude_cli config = %#v, want typed config values", cfg.LLM.ClaudeCLI)
	}
	for _, key := range []string{"SWARM_RUNTIME_RECOVERY_ON_STARTUP", "SWARM_LLM_SESSION_LOCK_TTL", "SWARM_LLM_SESSION_ROTATE_AFTER_TURNS", "SWARM_LLM_SESSION_ROTATE_ON_PARSE_FAILURES", "SWARM_CLAUDE_CLI_COMMAND", "SWARM_CLAUDE_CLI_TIMEOUT", "SWARM_CLAUDE_CLI_OUTPUT_FORMAT", "SWARM_CLAUDE_TIMEOUT_SECONDS"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, "unsupported-source")
			if _, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: configPath}); err == nil || !strings.Contains(err.Error(), "env/unknown_stale @ "+key) {
				t.Fatalf("unsupported source must reject even with typed config: %v", err)
			}
		})
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
		"    rotate_on_parse_failures: 3",
		"  openai_compatible:",
		"    base_url: https://example.test/v1",
	}, "\n")+"\n")

	result, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo})
	if err != nil {
		t.Fatalf("load discovered local-operator config: %v", err)
	}
	if result.Config.LLM.Backend != "claude_cli" || result.Source != unifiedConfigSourceDeployment {
		t.Fatalf("deployment config result = source=%s backend=%s, want deployment_config claude_cli", result.Source, result.Config.LLM.Backend)
	}

	result, err = LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: "runtime.yaml", BackendOverride: "openai_compatible"})
	if err != nil {
		t.Fatalf("load explicit config with backend override: %v", err)
	}
	if result.Config.LLM.Backend != "openai_compatible" || result.Source != unifiedConfigSourceExplicit || filepath.Clean(result.Path) != filepath.Clean(explicitPath) {
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
	t.Setenv("SWARM_CLAUDE_DEFAULT_MODEL", "claude-test")

	configBody := strings.Join([]string{
		"llm:",
		"  backend: anthropic",
		"  session:",
		"    lock_ttl: 10s",
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
				writeIgnoredExecutableConfig(t, configBody)
				return RuntimeConfigLoadOptions{RepoRoot: t.TempDir()}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadRuntimeConfigWithOptions(tt.setup(t))
			if err == nil || !strings.Contains(err.Error(), "SWARM_CLAUDE_DEFAULT_MODEL") || !strings.Contains(err.Error(), "unknown_stale") {
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

func runVerifyCommandWithContractsForTest(t *testing.T, ctx context.Context, repo, sourceRoot string, out *bytes.Buffer) int {
	t.Helper()
	return runVerifyCommandWithContractsOutputForTest(t, ctx, repo, sourceRoot, out, out)
}

func runVerifyCommandWithContractsOutputForTest(t *testing.T, ctx context.Context, repo, sourceRoot string, out, errOut *bytes.Buffer) int {
	t.Helper()
	if repo == "" {
		repo = t.TempDir()
	}
	opts := defaultVerifyCommandOptions()
	// These fixtures prove declaration semantics, not deployment readiness.
	opts.portable = true
	opts.sourceRoot = sourceRoot
	opts.configPath = writeTestVerifyRuntimeConfig(t)
	return runVerifyCommandWithOutput(ctx, repo, opts, out, errOut)
}

func TestRunVerifyCommandFormatsSchemaLoaderDiagnostics(t *testing.T) {
	tests := []struct {
		name     string
		schema   string
		files    map[string]string
		wants    []string
		notWants []string
	}{
		{
			name:     "document mapping shape",
			schema:   "invalid-schema-shape",
			wants:    []string{"schema.yaml:1:1", "must be a mapping", "got scalar"},
			notWants: []string{"flow schema document must be a mapping", "load Swarm contracts", "resolve contracts"},
		},
		{
			name:     "field valid options",
			schema:   "bogus: true\n",
			wants:    []string{"ERROR: schema field \"bogus\" is not supported.", "Valid options:", "stages", "Remediation:"},
			notWants: []string{"UNDEFINED-FIELD", "not in platform spec", "load Swarm contracts", "resolve contracts", "terminal_states"},
		},
		{
			name:   "duplicate activity after connector import",
			schema: "stages: []\ninputs: [z.request, a.request]\n",
			files: map[string]string{
				"events.yaml": "z.request: {chat_id: text, text: text}\na.request: {chat_id: text, text: text}\n",
				"nodes.yaml":  "sender:\n  z.request: {activity: {id: send_reply, tool: telegram.send_message, input: {chat_id: payload.chat_id, text: payload.text}}}\n  a.request: {activity: {id: send_reply, tool: telegram.send_message, input: {chat_id: payload.chat_id, text: payload.text}}}\n",
			},
			wants:    []string{`duplicate activity id "send_reply"`, "nodes.yaml:3:30", `handlers "sender/z.request" and "sender/a.request"`, "give each activity in this flow a unique id"},
			notWants: []string{"provider connector pack import failed", "multiple declaration owners"},
		},
		{
			name:   "duplicate activity on one YAML line",
			schema: "stages: []\ninputs: [z.request, a.request]\n",
			files: map[string]string{
				"events.yaml": "z.request: {chat_id: text, text: text}\na.request: {chat_id: text, text: text}\n",
				"nodes.yaml":  "sender: {z.request: {activity: {id: send_reply, tool: telegram.send_message, input: {chat_id: payload.chat_id, text: payload.text}}}, a.request: {activity: {id: send_reply, tool: telegram.send_message, input: {chat_id: payload.chat_id, text: payload.text}}}}\n",
			},
			wants:    []string{`duplicate activity id "send_reply"`, "nodes.yaml:1:162", `handlers "sender/z.request" and "sender/a.request"`},
			notWants: []string{"provider connector pack import failed", "multiple declaration owners"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			writeWorkflowValidationFixtureFile(t, filepath.Join(root, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
			writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), test.schema)
			for name, body := range test.files {
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, name), body)
			}
			if strings.HasPrefix(test.name, "duplicate activity") {
				for _, member := range []string{"manifest.yaml", "trigger.yaml", "connector.yaml", "channel.yaml"} {
					body, err := os.ReadFile(filepath.Join(RepoRoot(), "packs/providers/telegram", member))
					if err != nil {
						t.Fatal(err)
					}
					writeWorkflowValidationFixtureFile(t, filepath.Join(root, "telegram", member), string(body))
				}
			}
			var output bytes.Buffer
			code := runVerifyCommandWithContractsForTest(t, context.Background(), RepoRoot(), root, &output)
			if code != CLIExitValidation {
				t.Fatalf("code = %d, want %d output=%s", code, CLIExitValidation, output.String())
			}
			for _, want := range test.wants {
				if !strings.Contains(output.String(), want) {
					t.Fatalf("output missing %q:\n%s", want, output.String())
				}
			}
			for _, notWant := range test.notWants {
				if strings.Contains(output.String(), notWant) {
					t.Fatalf("output contains internal detail %q:\n%s", notWant, output.String())
				}
			}
		})
	}
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
			if !strings.Contains(out, "source directory") {
				t.Fatalf("output = %q", out)
			}
			if !strings.Contains(out, tc.path) {
				t.Fatalf("output does not name explicit path %q:\n%s", tc.path, out)
			}
		})
	}
}

func TestNormalizeSourceRootExplicitPathValidation(t *testing.T) {
	root := t.TempDir()

	got, err := NormalizeSourceRoot(root)
	if err != nil {
		t.Fatalf("normalize directory root: %v", err)
	}
	if got != root {
		t.Fatalf("root = %q, want %q", got, root)
	}

	explicitChild := filepath.Join(root, "zzz-not-a-real-dir")
	if got, err := NormalizeSourceRoot(explicitChild); err == nil {
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
	if out := stdout.String(); !strings.Contains(out, "verify ok: source=") {
		t.Fatalf("verify stdout missing success marker:\n%s", out)
	} else if strings.Contains(out, "entity_reader_coverage") || strings.Contains(out, "lint_evidence") {
		t.Fatalf("verify stdout contains advisory diagnostics:\n%s", out)
	}
	errText := stderr.String()
	if !strings.Contains(errText, "[INFO] entity_reader_coverage @ root: flow root entity_type state declares field untouched with no detected internal reader coverage") {
		t.Fatalf("verify stderr missing lint evidence:\n%s", errText)
	}
	if strings.Contains(errText, "lint_evidence:") {
		t.Fatalf("verify stderr used legacy lint_evidence prefix:\n%s", errText)
	}

	opts := defaultVerifyCommandOptions()
	opts.portable = true
	opts.sourceRoot = root
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
	opts.portable = true
	opts.sourceRoot = root
	opts.configPath = writeTestVerifyRuntimeConfig(t)
	opts.output.asJSON = true

	var stdout, stderr bytes.Buffer
	code := runVerifyCommandWithOutput(context.Background(), RepoRoot(), opts, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("runVerifyCommand --json exit code = 0, stdout = %q stderr = %q", stdout.String(), stderr.String())
	}
	result := decodeOutputJSON[verifyCommandResult](t, stdout.String())
	if result.OK || len(result.Errors) != 1 || result.Errors[0].CheckID != "generated_tool_schema_closure" || !strings.Contains(result.Errors[0].Message, "agent strict-schema-agent emit missing.event has no exact schema in child") {
		t.Fatalf("verify --json lost strict emit schema failure: %#v", result)
	}
	if stderr.Len() != 0 {
		t.Fatalf("verify --json stderr = %q, want structured output only", stderr.String())
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
	if out := stdout.String(); !strings.Contains(out, "verify ok: source=") {
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
	if !strings.Contains(stdout.String(), "admission validation failed; startup execution not performed") {
		t.Fatalf("verify stdout omitted the failed admission ledger: %q", stdout.String())
	}
	errText := stderr.String()
	for _, want := range []string{
		"[BLOCKER] timer_validation @",
		"start_on boot does not support cancel_on state:done",
		"remediation:",
	} {
		if !strings.Contains(errText, want) {
			t.Fatalf("verify stderr missing %q:\n%s", want, errText)
		}
	}

	opts := defaultVerifyCommandOptions()
	opts.portable = true
	opts.sourceRoot = root
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
	if strings.TrimSpace(verifyJSON.SourceRoot) != root {
		t.Fatalf("verify --json source_root = %q, want %q", verifyJSON.SourceRoot, root)
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

func TestRunVerifyCommandRejectsAncestorEventNameWithoutReceiverLocalDeclarationOrConnect(t *testing.T) {
	root := canonicalrouting.CopyAncestorEventWithoutReceiverMembership(t)
	var stdout, stderr bytes.Buffer
	code := runVerifyCommandWithContractsOutputForTest(t, context.Background(), RepoRoot(), root, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("verify exit code = 0, stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "admission validation failed; startup execution not performed") {
		t.Fatalf("verify stdout omitted the failed admission ledger: %q", stdout.String())
	}
	for _, want := range []string{"undeclared event", "declare the event in this flow or bind its producer-owned input", "root.started"} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("verify stderr missing %q:\n%s", want, stderr.String())
		}
	}
}

func TestRunVerifyCommand_UnreachableStageUsesBlockingAnalyzerOutput(t *testing.T) {
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
	if !strings.Contains(stdout.String(), "admission validation failed; startup execution not performed") {
		t.Fatalf("verify stdout omitted the failed admission ledger: %q", stdout.String())
	}
	errText := stderr.String()
	for _, want := range []string{
		"[BLOCKER] semantic_drift_unreachable_state @",
		"declares stage review but no lawful lifecycle path",
	} {
		if !strings.Contains(errText, want) {
			t.Fatalf("verify stderr missing %q:\n%s", want, errText)
		}
	}
	if strings.Contains(errText, "boot verification warnings:") {
		t.Fatalf("verify stderr used legacy fatal warning banner:\n%s", errText)
	}

	opts := defaultVerifyCommandOptions()
	opts.portable = true
	opts.sourceRoot = root
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
	if !verifyFindingOutputsContain(verifyJSON.Errors, "semantic_drift_unreachable_state", "hard_invalidity", "declares stage review but no lawful lifecycle path") {
		t.Fatalf("verify --json errors = %#v, want structured unreachable-stage hard invalidity", verifyJSON.Errors)
	}
	if verifyFindingOutputsContain(verifyJSON.Warnings, "semantic_drift_unreachable_state", "semantic_drift_warning", "review") {
		t.Fatalf("verify --json downgraded unreachable-stage hard invalidity: %#v", verifyJSON.Warnings)
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

	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `stages:
  waiting: {}
  done: {final: true}
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "state.yaml"), `ticket_id: ""
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), `
timer.reminder:
`)
	timerBlock := `
    - owner: support-node
      event: timer.reminder
      delay: 1m
      start_on: boot
`
	if strings.TrimSpace(cancelOn) != "" {
		timerBlock += "      cancel_on: " + cancelOn + "\n"
	}
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
support-node:
  timers:
`+timerBlock+`  timer.reminder:
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
	if !strings.Contains(out, "verify ok: source=") {
		t.Fatalf("verify output missing success marker:\n%s", out)
	}
}

func TestRunVerifyCommand_FailsForUndefinedDeclaredModelAlias(t *testing.T) {
	root := canonicalrouting.CopyVerifyModelAlias(t, canonicalrouting.VerifyModelAliasUndefined)

	var buf bytes.Buffer
	code := runVerifyCommandWithContractsForTest(t, context.Background(), RepoRoot(), root, &buf)
	if code == 0 {
		t.Fatalf("expected non-zero exit code, output = %q", buf.String())
	}
	out := buf.String()
	for _, want := range []string{"model alias declaration failed", "llm.models alias \"not_configured\" is not configured"} {
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
		"    rotate_on_parse_failures: 3",
	}, "\n")+"\n"))
	t.Setenv("SWARM_CONFIG", configPath)

	var buf bytes.Buffer
	opts := defaultVerifyCommandOptions()
	opts.portable = true
	opts.sourceRoot = root
	code := runVerifyCommandWithOutput(context.Background(), RepoRoot(), opts, &buf, &buf)
	if code != 0 {
		t.Fatalf("runVerifyCommand exit code = %d, output = %q", code, buf.String())
	}
	if out := buf.String(); !strings.Contains(out, "verify ok: source=") {
		t.Fatalf("verify output missing success marker:\n%s", out)
	}
}

func TestRunVerifyCommand_AllowsOpenStreamAccumulatorWithRootInput(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "false")

	for _, before := range []bool{false, true} {
		t.Run(fmt.Sprintf("read_before_accumulate=%t", before), func(t *testing.T) {
			root := writeVerifyAccumulatorSafetyCommandFixture(t, verifyAccumulatorSafetyCommandFixtureOptions{
				publicInput: true,
			})
			read := "    rules:\n      - when: state.items.size() >= 1\n        advances_to: done\n"
			if before {
				read = "    guard: state.items.size() >= 1\n    advances_to: done\n"
			}
			writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), "accumulator:\n  item.arrived:\n    accumulate: {target: items, key: expected_count}\n"+read)

			var stdout, stderr bytes.Buffer
			code := runVerifyCommandWithContractsOutputForTest(t, context.Background(), RepoRoot(), root, &stdout, &stderr)
			if before {
				if code == 0 || !strings.Contains(stderr.String(), "not definitely assigned") {
					t.Fatalf("pre-accumulate read must refuse: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
				}
				return
			}
			if code != 0 {
				t.Fatalf("runVerifyCommand exit code = %d, stdout = %q stderr = %q", code, stdout.String(), stderr.String())
			}
			if out := stdout.String(); !strings.Contains(out, "verify ok: source=") {
				t.Fatalf("verify stdout missing success marker:\n%s", out)
			}
			if errText := stderr.String(); strings.Contains(errText, "accumulator_input_producer_path") {
				t.Fatalf("verify stderr reported no-producer error despite public root input:\n%s", errText)
			}
		})
	}
}

func TestRunVerifyCommand_FailsForAccumulatorInputWithoutProducerPath(t *testing.T) {
	root := writeVerifyAccumulatorSafetyCommandFixture(t, verifyAccumulatorSafetyCommandFixtureOptions{})

	var stdout, stderr bytes.Buffer
	code := runVerifyCommandWithContractsOutputForTest(t, context.Background(), RepoRoot(), root, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("expected non-zero exit code, stdout = %q stderr = %q", stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "admission validation failed; startup execution not performed") {
		t.Fatalf("verify stdout omitted the failed admission ledger: %q", stdout.String())
	}
	errText := stderr.String()
	for _, want := range []string{
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
	publicInput bool
}

func writeVerifyAccumulatorSafetyCommandFixture(t *testing.T, opts verifyAccumulatorSafetyCommandFixtureOptions) string {
	t.Helper()
	root := t.TempDir()

	schema := `stages:
  collecting: {}
  done: {final: true}
`

	if opts.publicInput {
		schema += "inputs: [item.arrived]\n"
	}
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), schema)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), `
item.arrived:
  expected_count: integer
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "state.yaml"), "items: [Arrival]\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "types.yaml"), "Arrival:\n  expected_count: integer\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
accumulator:
  item.arrived:
    accumulate:
      target: items
      key: expected_count
    advances_to: done
`)
	return root
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
	if isOptionalWorkflowDeclarationFixture(path) && strings.TrimSpace(contents) == "{}" {
		t.Fatalf("optional declaration fixture %s must be omitted instead of written as {}", path)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func isOptionalWorkflowDeclarationFixture(path string) bool {
	switch filepath.Base(path) {
	case "agents.yaml", "state.yaml", "events.yaml", "nodes.yaml", "policy.yaml", "tools.yaml", "types.yaml":
		return true
	default:
		return false
	}
}

func writeWorkflowValidationDeadEventSchemaFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()

	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), "stages: []\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), `
root.unused:
`)
	return root
}

func addTestAgentOwners(bundle *runtimecontracts.WorkflowContractBundle) {
	if bundle == nil {
		return
	}
	if bundle.URIRegistry.Agents == nil {
		bundle.URIRegistry.Agents = map[string]runtimecontracts.ContractURIRef{}
	}
	if bundle.URIRegistry.ByURI == nil {
		bundle.URIRegistry.ByURI = map[string]runtimecontracts.ContractURIRef{}
	}
	if bundle.Events == nil {
		bundle.Events = map[string]runtimecontracts.EventCatalogEntry{}
	}
	if _, ok := bundle.Events["test.input"]; !ok {
		bundle.Events["test.input"] = runtimecontracts.EventCatalogEntry{}
	}
	if bundle.RootSchema == nil {
		bundle.RootSchema = &runtimecontracts.FlowSchemaDocument{}
	}
	bundle.RootSchema.Pins.Inputs.EventPins = append(bundle.RootSchema.Pins.Inputs.EventPins, runtimecontracts.FlowInputEventPin{Event: "test.input"})
	for localID, entry := range bundle.Agents {
		uri := "swarm-test://root/agents/" + localID
		ref := runtimecontracts.ContractURIRef{Kind: "agent", LocalID: localID, Full: uri}
		bundle.URIRegistry.Agents[localID] = ref
		bundle.URIRegistry.ByURI[uri] = ref
		entry.Role = localID
		entry.Model = "regular"
		entry.Subscriptions = []string{"test.input"}
		entry.Intent = runtimeagentintent.Source{Kind: runtimeagentintent.SourceInline, Inline: "Validate this test declaration."}
		resolved, err := runtimeagentintent.Resolve(
			runtimeagentintent.SourceInline,
			"inline",
			"agents.yaml#agents."+localID+".intent",
			"Validate this test declaration.",
		)
		if err != nil {
			panic(fmt.Sprintf("resolve test agent intent: %v", err))
		}
		entry.ResolvedIntent = resolved
		bundle.Agents[localID] = entry
	}
	semanticviewtest.WrapRootAgents(bundle)
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		panic(err)
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

func writeRuntimeConfigText(t *testing.T, path, configText string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(configText), 0o644); err != nil {
		t.Fatalf("write runtime config %s: %v", path, err)
	}
}

func TestExecutionValidation_DoesNotWarnForFlowLocalEmittedEventsWithOwningFlowSchemas(t *testing.T) {
	source := semanticview.Wrap(loadWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier11-flow-composition", "test-child-flow-local-events")))

	err := validateExecutionFixture(context.Background(), source, executionposture.Live)
	if err == nil {
		t.Fatal("validateExecutionFixture error = nil, want warning-only failure from unrelated fixture warnings")
	}
	if strings.Contains(err.Error(), "'child/child.internal' emitted but no schema in events.yaml") {
		t.Fatalf("unexpected flow-local no-schema warning: %v", err)
	}
	if strings.Contains(err.Error(), "'child/child.done' emitted but no schema in events.yaml") {
		t.Fatalf("unexpected flow-local no-schema warning: %v", err)
	}
}

func TestExecutionValidation_DoesNotWarnForFlowOwnedAgentOutputEvents(t *testing.T) {
	source := semanticview.Wrap(requiredagentsparentconnect.LoadBundle(t))

	err := validateExecutionFixture(context.Background(), source, executionposture.Live)
	if err != nil && strings.Contains(err.Error(), "'work.ready' emitted but nobody subscribes") {
		t.Fatalf("unexpected flow-owned agent output warning: %v", err)
	}
}

func TestExecutionValidation_CreateEntityAccumulatePreemptsDynamicComputeWarningSurface(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	bundle := loadWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier8-boot-verification", "test-boot-success"))
	bundle.RootEntities = runtimecontracts.EntityContractsDocument{
		"state": {
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
	handler.Accumulate = &runtimecontracts.AccumulateSpec{Target: "items", Key: "id"}
	handler.Compute = &runtimecontracts.ComputeSpec{
		Operation: runtimecontracts.ComputeOpCount,
		StoreAs:   "state.composite_score",
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

	err := validateExecutionFixture(context.Background(), semanticview.Wrap(bundle), executionposture.Live)
	if err == nil || !strings.Contains(err.Error(), "uses retired create_entity; construction belongs to the canonical flow constructor") {
		t.Fatalf("validateExecutionFixture error = %v, want retired-constructor error before compute warnings", err)
	}
}

func TestExecutionValidation_EmittedPayloadCompletenessReturnsWarningSurface(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")

	bundle := &runtimecontracts.WorkflowContractBundle{
		Platform: runtimecontracts.PlatformSpecDocument{},
		RootEntities: runtimecontracts.EntityContractsDocument{
			"state": {
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

				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"scan.corpus_dispatch": {
						Emit: runtimecontracts.EmitSpec{Event: "market_research.scan_assigned"},
					},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"scan.corpus_dispatch": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"scan_id":   {Type: "string"},
						"geography": {Type: "string"},
					},
					Required: []string{"scan_id", "geography"},
				},
			},
			"market_research.scan_assigned": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"entity_id":          {Type: "string"},
						"current_state":      {Type: "string"},
						"trigger_event_type": {Type: "string"},
						"scan_id":            {Type: "string"},
					},
					Required: []string{"entity_id", "scan_id"},
				},
			},
		},
	}
	bundle.Platform.Platform.Name = "test"
	bundle.Platform.Platform.Version = "1.0.0"
	root := &runtimecontracts.FlowContractView{
		Path: ".", Paths: runtimecontracts.FlowContractPaths{FlowPath: "."},
		Events: bundle.Events, Nodes: bundle.Nodes,
	}
	bundle.FlowTree = runtimecontracts.FlowTree{Root: root, ByID: map[string]*runtimecontracts.FlowContractView{".": root}}
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatal(err)
	}

	err := validateExecutionFixture(context.Background(), semanticview.Wrap(bundle), executionposture.Live)
	if err == nil {
		t.Fatal("validateExecutionFixture error = nil, want emitted payload completeness invalidity")
	}
	if !strings.Contains(err.Error(), "scan_id is not statically provable") {
		t.Fatalf("validateExecutionFixture error = %v, want emitted payload completeness invalidity", err)
	}
	if strings.Contains(err.Error(), "definitely missing") {
		t.Fatalf("validateExecutionFixture error = %v, want approved warning wording only", err)
	}
}

func TestExecutionValidation_UnreachableStageReturnsHardInvalidity(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")

	err := validateExecutionFixture(context.Background(), semanticview.Wrap(loadWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier8-boot-verification", "test-boot-state-machine-unreachable"))), executionposture.Live)
	if err == nil {
		t.Fatal("validateExecutionFixture error = nil, want hard invalidity from unreachable declared stage")
	}
	for _, want := range []string{
		"semantic_drift_unreachable_state",
		"declares stage review but no lawful lifecycle path from initial stage waiting reaches review",
		"Reachable states: active, done, waiting",
		"Unreachable states: review",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("validateExecutionFixture error = %v, want substring %q", err, want)
		}
	}
}

func TestExecutionValidation_DeadDeclaredEventSchemaReturnsWarningSurface(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")

	source := semanticview.Wrap(loadWorkflowValidationBundleAt(t, writeWorkflowValidationDeadEventSchemaFixture(t)))

	verifyErr := validateExecutionFixture(context.Background(), source, executionposture.Live)
	if verifyErr == nil {
		t.Fatal("validateExecutionFixture error = nil, want warning-only failure from dead declared event schema")
	}
	for _, want := range []string{
		"semantic_drift_dead_event_schema",
		"root.unused",
		"has no active role in the authored bundle",
	} {
		if !strings.Contains(verifyErr.Error(), want) {
			t.Fatalf("validateExecutionFixture error = %v, want substring %q", verifyErr, want)
		}
	}

	result, runtimeErr := runtimepkg.ValidateWorkflowContractSurface(context.Background(), source, runtimepkg.DefaultWorkflowContractValidationOptions(nil, executionposture.Live))
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

func TestExecutionValidation_CreateEntityAccumulateReturnsBootError(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")

	// Retired grammar refuses at the public loader, before execution validation.
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"verify", filepath.Join(RepoRoot(), "tests", "tier8-boot-verification", "test-boot-create-entity-plus-accumulate")}, &stdout, &stderr, defaultRootCommandOptions())
	if code != 2 || !strings.Contains(stderr.String(), `ERROR: handler field "create_entity" is not supported.`) ||
		!strings.Contains(stderr.String(), `Location: nodes.yaml:$["test-node"]["task.started"]["create_entity"]`) ||
		!strings.Contains(stderr.String(), "Valid options:") {
		t.Fatalf("public verify code=%d stdout=%s stderr=%s, want current-vocabulary loader refusal", code, &stdout, &stderr)
	}
}
