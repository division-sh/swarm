package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/spf13/cobra"
)

func TestResolveCLIAPISettingsPrecedence(t *testing.T) {
	t.Run("flag sources beat config and defaults", func(t *testing.T) {
		isolateCLIAPIConfigEnv(t)
		configToken := writeCLIAPITokenFile(t, "config-token")
		flagToken := writeCLIAPITokenFile(t, "flag-token")
		configPath := writeCLIAPIConfigFile(t, map[string]string{
			"api_server":     "http://127.0.0.1:4444",
			"api_token_file": configToken,
		})
		t.Setenv("SWARM_CONFIG", configPath)

		client, err := newCLIAPIClient(rootCommandOptions{
			apiServer:    "http://127.0.0.1:6666",
			apiTokenFile: flagToken,
		})
		if err != nil {
			t.Fatalf("newCLIAPIClient: %v", err)
		}
		if client.endpoint != "http://127.0.0.1:6666/v1/rpc" {
			t.Fatalf("endpoint = %q", client.endpoint)
		}
		if client.token != "flag-token" {
			t.Fatalf("token = %q", client.token)
		}
	})

	t.Run("client environment sources fail closed instead of shadowing config", func(t *testing.T) {
		isolateCLIAPIConfigEnv(t)
		configToken := writeCLIAPITokenFile(t, "config-token")
		envToken := writeCLIAPITokenFile(t, "env-file-token")
		t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
			"api_server":     "http://127.0.0.1:4444",
			"api_token_file": configToken,
		}))
		t.Setenv("SWARM_API_SERVER", "http://127.0.0.1:5555")
		t.Setenv("SWARM_API_TOKEN", "env-token")
		t.Setenv("SWARM_API_TOKEN_FILE", envToken)

		_, err := newCLIAPIClient(rootCommandOptions{})
		if err == nil {
			t.Fatal("newCLIAPIClient returned nil error")
		}
		for _, want := range []string{"client-side API environment sources are no longer accepted", "SWARM_API_SERVER", "SWARM_API_TOKEN", "SWARM_API_TOKEN_FILE", "--api-server", "--api-token-file"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("err = %q, want %q", err.Error(), want)
			}
		}
	})

	t.Run("client environment token file fails closed instead of shadowing config", func(t *testing.T) {
		isolateCLIAPIConfigEnv(t)
		configToken := writeCLIAPITokenFile(t, "config-token")
		envToken := writeCLIAPITokenFile(t, "env-file-token")
		t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
			"api_server":     "http://127.0.0.1:4444",
			"api_token_file": configToken,
		}))
		t.Setenv("SWARM_API_TOKEN_FILE", envToken)

		_, err := newCLIAPIClient(rootCommandOptions{})
		if err == nil {
			t.Fatal("newCLIAPIClient returned nil error")
		}
		if !strings.Contains(err.Error(), "SWARM_API_TOKEN_FILE") || !strings.Contains(err.Error(), "config connection.api_token_file") {
			t.Fatalf("err = %q, want SWARM_API_TOKEN_FILE replacement guidance", err.Error())
		}
	})

	t.Run("config feeds endpoint and token file", func(t *testing.T) {
		isolateCLIAPIConfigEnv(t)
		configToken := writeCLIAPITokenFile(t, "config-token")
		t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
			"api_server":     "http://127.0.0.1:4444",
			"api_token_file": configToken,
		}))

		client, err := newCLIAPIClient(rootCommandOptions{})
		if err != nil {
			t.Fatalf("newCLIAPIClient: %v", err)
		}
		if client.endpoint != "http://127.0.0.1:4444/v1/rpc" {
			t.Fatalf("endpoint = %q", client.endpoint)
		}
		if client.token != "config-token" {
			t.Fatalf("token = %q", client.token)
		}
	})

	t.Run("built in API server default remains loopback base", func(t *testing.T) {
		isolateCLIAPIConfigEnv(t)

		client, err := newCLIAPIClient(rootCommandOptions{})
		if err != nil {
			t.Fatalf("newCLIAPIClient: %v", err)
		}
		if client.endpoint != "http://127.0.0.1:8081/v1/rpc" {
			t.Fatalf("endpoint = %q", client.endpoint)
		}
		if client.token != apiv1.DefaultLoopbackAPIToken {
			t.Fatalf("token = %q", client.token)
		}
	})
}

func TestCLIAPISettingsDefaultTokenLoopbackBoundary(t *testing.T) {
	for _, tc := range []struct {
		name       string
		apiServer  string
		wantToken  string
		wantErr    string
		wantSource string
	}{
		{
			name:       "numeric ipv4 loopback gets default token",
			apiServer:  "http://127.0.0.1:8081",
			wantToken:  apiv1.DefaultLoopbackAPIToken,
			wantSource: string(apiv1.AuthTokenSourceBuiltInLoopbackToken),
		},
		{
			name:       "numeric ipv6 loopback gets default token",
			apiServer:  "http://[::1]:8081",
			wantToken:  apiv1.DefaultLoopbackAPIToken,
			wantSource: string(apiv1.AuthTokenSourceBuiltInLoopbackToken),
		},
		{
			name:      "localhost needs explicit token",
			apiServer: "http://localhost:8081",
			wantErr:   "API token source is required",
		},
		{
			name:      "wildcard needs explicit token",
			apiServer: "http://0.0.0.0:8081",
			wantErr:   "API token source is required",
		},
		{
			name:      "routable address needs explicit token",
			apiServer: "http://192.0.2.10:8081",
			wantErr:   "API token source is required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			settings, err := resolveCLIAPISettings(rootCommandOptions{apiServer: tc.apiServer})
			if tc.wantErr != "" {
				if err == nil {
					t.Fatal("resolveCLIAPISettings returned nil error")
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %q, want %q", err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveCLIAPISettings: %v", err)
			}
			if settings.token != tc.wantToken {
				t.Fatalf("token = %q, want %q", settings.token, tc.wantToken)
			}
			if settings.tokenSource != tc.wantSource || settings.tokenExplicit {
				t.Fatalf("token source = %q explicit=%v, want source %q explicit=false", settings.tokenSource, settings.tokenExplicit, tc.wantSource)
			}
		})
	}
}

func TestCLIAPIResolverToleratesContractPathConfigKeys(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	tokenFile := writeCLIAPITokenFile(t, "config-token")
	t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
		"api_server":         "http://127.0.0.1:4444",
		"api_token_file":     tokenFile,
		"swarm_dir":          filepath.Join(t.TempDir(), "state"),
		"contracts_path":     "contracts-from-config",
		"platform_spec_path": "platform-from-config.yaml",
	}))

	client, err := newCLIAPIClient(rootCommandOptions{})
	if err != nil {
		t.Fatalf("newCLIAPIClient: %v", err)
	}
	if client.endpoint != "http://127.0.0.1:4444/v1/rpc" {
		t.Fatalf("endpoint = %q", client.endpoint)
	}
	if client.token != "config-token" {
		t.Fatalf("token = %q", client.token)
	}
}

func TestCLISwarmDirResolutionPrecedence(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	configDir := filepath.Join(t.TempDir(), "config-state")
	flagDir := filepath.Join(t.TempDir(), "flag-state")
	t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
		"swarm_dir": configDir,
	}))
	t.Setenv("SWARM_DIR", filepath.Join(t.TempDir(), "must-not-use"))
	t.Setenv("SWARM_HOME", filepath.Join(t.TempDir(), "must-not-use"))

	got, err := resolveCLISwarmDir(cliSwarmDirOptions{SwarmDir: flagDir, SwarmDirFlagSet: true})
	if err != nil {
		t.Fatalf("resolve flag swarm dir: %v", err)
	}
	if got.Path != flagDir || got.Source != "--swarm-dir" {
		t.Fatalf("flag swarm dir = %#v, want %q from --swarm-dir", got, flagDir)
	}

	got, err = resolveCLISwarmDir(cliSwarmDirOptions{})
	if err != nil {
		t.Fatalf("resolve config swarm dir: %v", err)
	}
	if got.Path != configDir || got.Source != "config paths.swarm_dir" {
		t.Fatalf("config swarm dir = %#v, want %q from config paths.swarm_dir", got, configDir)
	}

	t.Setenv("SWARM_CONFIG", "")
	got, err = resolveCLISwarmDir(cliSwarmDirOptions{})
	if err != nil {
		t.Fatalf("resolve default swarm dir: %v", err)
	}
	if !strings.HasSuffix(got.Path, filepath.Join(".swarm")) || got.Source != "default ~/.swarm" {
		t.Fatalf("default swarm dir = %#v, want ~/.swarm default", got)
	}
	if strings.Contains(got.Path, "must-not-use") {
		t.Fatalf("default swarm dir consumed retired env source: %#v", got)
	}
}

func TestCLISwarmDirConfigValidation(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	path := filepath.Join(t.TempDir(), "swarm.yaml")
	if err := os.WriteFile(path, []byte("paths:\n  swarm_dir: [bad]\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("SWARM_CONFIG", path)

	if _, err := resolveCLISwarmDir(cliSwarmDirOptions{}); err == nil || !strings.Contains(err.Error(), "decode swarm.yaml CLI config") {
		t.Fatalf("resolveCLISwarmDir err = %v, want swarm.yaml CLI decode validation", err)
	}
}

func TestCLISwarmDirBlankConfigFailsClosed(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	path := filepath.Join(t.TempDir(), "swarm.yaml")
	if err := os.WriteFile(path, []byte("paths:\n  swarm_dir: \"  \"\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("SWARM_CONFIG", path)

	if _, err := resolveCLISwarmDir(cliSwarmDirOptions{}); err == nil || !strings.Contains(err.Error(), "config paths.swarm_dir must be non-empty") {
		t.Fatalf("resolveCLISwarmDir err = %v, want blank config paths.swarm_dir validation", err)
	}
}

func TestCLIAPIResolverIgnoresServeListenerConfigKeys(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	tokenFile := writeCLIAPITokenFile(t, "config-token")
	t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
		"api_server":            "http://127.0.0.1:4444",
		"api_token_file":        tokenFile,
		"serve_api_listen_addr": "not-a-listen-address",
		"serve_mcp_listen_addr": "http://127.0.0.1:9002",
	}))

	client, err := newCLIAPIClient(rootCommandOptions{})
	if err != nil {
		t.Fatalf("newCLIAPIClient: %v", err)
	}
	if client.endpoint != "http://127.0.0.1:4444/v1/rpc" {
		t.Fatalf("endpoint = %q", client.endpoint)
	}
	if client.token != "config-token" {
		t.Fatalf("token = %q", client.token)
	}
}

func TestCLIAPISettingsFailClosed(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(t *testing.T) rootCommandOptions
		wantExit int
		wantErr  string
	}{
		{
			name: "explicit missing config",
			setup: func(t *testing.T) rootCommandOptions {
				t.Setenv("SWARM_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))
				t.Setenv("SWARM_API_TOKEN", "env-token")
				return rootCommandOptions{}
			},
			wantExit: cliExitValidation,
			wantErr:  "read swarm.yaml config",
		},
		{
			name: "malformed config",
			setup: func(t *testing.T) rootCommandOptions {
				path := filepath.Join(t.TempDir(), "config.yaml")
				if err := os.WriteFile(path, []byte("api_server: ["), 0o644); err != nil {
					t.Fatalf("write config: %v", err)
				}
				t.Setenv("SWARM_CONFIG", path)
				t.Setenv("SWARM_API_TOKEN", "env-token")
				return rootCommandOptions{}
			},
			wantExit: cliExitValidation,
			wantErr:  "parse swarm.yaml config",
		},
		{
			name: "unsupported inline config token",
			setup: func(t *testing.T) rootCommandOptions {
				path := filepath.Join(t.TempDir(), "config.yaml")
				if err := os.WriteFile(path, []byte("api_token: secret\n"), 0o644); err != nil {
					t.Fatalf("write config: %v", err)
				}
				t.Setenv("SWARM_CONFIG", path)
				return rootCommandOptions{}
			},
			wantExit: cliExitValidation,
			wantErr:  `unknown config key "api_token"`,
		},
		{
			name: "missing token file",
			setup: func(t *testing.T) rootCommandOptions {
				return rootCommandOptions{apiTokenFile: filepath.Join(t.TempDir(), "missing-token")}
			},
			wantExit: cliExitAuth,
			wantErr:  "read --api-token-file",
		},
		{
			name: "blank token file",
			setup: func(t *testing.T) rootCommandOptions {
				return rootCommandOptions{apiTokenFile: writeCLIAPITokenFile(t, "  \n")}
			},
			wantExit: cliExitAuth,
			wantErr:  "--api-token-file is blank",
		},
		{
			name: "invalid API server query",
			setup: func(t *testing.T) rootCommandOptions {
				return rootCommandOptions{apiServer: "http://127.0.0.1:8081?x=1"}
			},
			wantExit: cliExitValidation,
			wantErr:  "must not include query or fragment",
		},
		{
			name: "flag API server rejects direct RPC endpoint",
			setup: func(t *testing.T) rootCommandOptions {
				return rootCommandOptions{apiServer: "http://127.0.0.1:8081/v1/rpc"}
			},
			wantExit: cliExitValidation,
			wantErr:  "not a direct /v1/rpc endpoint",
		},
		{
			name: "flag API server rejects prefixed RPC endpoint",
			setup: func(t *testing.T) rootCommandOptions {
				return rootCommandOptions{apiServer: "http://127.0.0.1:8081/proxy/v1/rpc"}
			},
			wantExit: cliExitValidation,
			wantErr:  "not a direct /v1/rpc endpoint",
		},
		{
			name: "environment API server is removed before endpoint shape validation",
			setup: func(t *testing.T) rootCommandOptions {
				t.Setenv("SWARM_API_SERVER", "http://127.0.0.1:8081/v1/ws")
				t.Setenv("SWARM_API_TOKEN", "env-token")
				return rootCommandOptions{}
			},
			wantExit: cliExitValidation,
			wantErr:  "client-side API environment sources are no longer accepted",
		},
		{
			name: "environment API server with prefix is removed before endpoint shape validation",
			setup: func(t *testing.T) rootCommandOptions {
				t.Setenv("SWARM_API_SERVER", "http://127.0.0.1:8081/proxy/v1/ws")
				t.Setenv("SWARM_API_TOKEN", "env-token")
				return rootCommandOptions{}
			},
			wantExit: cliExitValidation,
			wantErr:  "client-side API environment sources are no longer accepted",
		},
		{
			name: "config API server rejects direct RPC endpoint",
			setup: func(t *testing.T) rootCommandOptions {
				t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
					"api_server": "http://127.0.0.1:8081/v1/rpc",
				}))
				return rootCommandOptions{}
			},
			wantExit: cliExitValidation,
			wantErr:  "not a direct /v1/rpc endpoint",
		},
		{
			name: "config API server rejects prefixed RPC endpoint",
			setup: func(t *testing.T) rootCommandOptions {
				t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
					"api_server": "http://127.0.0.1:8081/proxy/v1/rpc",
				}))
				return rootCommandOptions{}
			},
			wantExit: cliExitValidation,
			wantErr:  "not a direct /v1/rpc endpoint",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			opts := tc.setup(t)
			_, err := newCLIAPIClient(opts)
			if err == nil {
				t.Fatal("newCLIAPIClient returned nil error")
			}
			if got := cliAPIErrorExitCode(err, cliAPIErrorClassifier{}); got != tc.wantExit {
				t.Fatalf("exit = %d, want %d; err=%v", got, tc.wantExit, err)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %q, want %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestCLIAPIConnectionFlagsSurfaceAndIsolation(t *testing.T) {
	root := newRootCommandWithOptions(context.Background(), t.TempDir(), ioDiscard{}, ioDiscard{}, defaultRootCommandOptions())
	withFlags := []string{
		"run list", "run status", "run trace", "health", "logs", "incidents",
		"event list", "event follow", "event view", "event publish", "event replay",
		"bundle list", "bundle show", "bundle agents", "bundle register", "bundle delete",
		"agent list", "agent deliveries", "agent view", "agent diagnose", "agent restart", "agent replay", "agent replay-backlog", "agent directive",
		"conversation list", "conversation view", "conversation turn",
		"entity list", "entity view", "entity aggregate",
		"mailbox list", "mailbox view", "mailbox defer",
		"control pause", "control continue", "control stop", "control nuke",
		"run fork", "forkchat new", "forkchat resume", "forkchat list", "forkchat view", "forkchat delete",
		"version",
	}
	for _, path := range withFlags {
		cmd := mustFindCLICommand(t, root, path)
		if cmd.Flags().Lookup("api-server") == nil {
			t.Fatalf("%s missing --api-server", path)
		}
		if cmd.Flags().Lookup("api-token-file") == nil {
			t.Fatalf("%s missing --api-token-file", path)
		}
		if cmd.Flags().Lookup("context") == nil {
			t.Fatalf("%s missing --context", path)
		}
	}

	withoutFlags := []string{
		"", "verify", "completion", "run",
		"event", "bundle", "agent", "conversation", "entity", "mailbox", "control", "forkchat",
		"investigate", "investigate health",
	}
	for _, path := range withoutFlags {
		cmd := mustFindCLICommand(t, root, path)
		if cmd.Flags().Lookup("api-server") != nil || cmd.Flags().Lookup("api-token-file") != nil {
			t.Fatalf("%s unexpectedly accepts API connection flags", path)
		}
	}

	serve := mustFindCLICommand(t, root, "serve")
	if serve.Flags().Lookup("api-token-file") == nil {
		t.Fatal("serve missing server --api-token-file")
	}
	if serve.Flags().Lookup("api-server") != nil {
		t.Fatal("serve unexpectedly accepts client --api-server")
	}
}

func TestServeContextFlagPassesRootPrevalidation(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	var stdout, stderr bytes.Buffer
	called := false
	var got serveOptions
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ string, serveOpts serveOptions) int {
		called = true
		got = serveOpts
		return 0
	}

	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", "--dev", "--context", "named"}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("serve code = %d, want 0 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if !called {
		t.Fatal("serve runner was not called")
	}
	if got.ContextName != "named" || !got.ContextNameSet {
		t.Fatalf("serve context = %q set=%v, want named/set", got.ContextName, got.ContextNameSet)
	}
	if strings.Contains(stderr.String(), "unknown flag: --context") {
		t.Fatalf("serve context flag was rejected by prevalidation: %s", stderr.String())
	}
}

func TestServeAPITokenFileFlagPassesRootPrevalidation(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	tokenFile := writeCLIAPITokenFile(t, "serve-token")
	var stdout, stderr bytes.Buffer
	called := false
	var got serveOptions
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ string, serveOpts serveOptions) int {
		called = true
		got = serveOpts
		return 0
	}

	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"serve", "--api-token-file", tokenFile}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("serve code = %d, want 0 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if !called {
		t.Fatal("serve runner was not called")
	}
	if got.APITokenFile != tokenFile || !got.APITokenFileFlagSet {
		t.Fatalf("serve API token file = %q set=%v, want %q/set", got.APITokenFile, got.APITokenFileFlagSet, tokenFile)
	}
	if strings.Contains(stderr.String(), "unknown flag: --api-token-file") {
		t.Fatalf("serve api-token-file flag was rejected by prevalidation: %s", stderr.String())
	}
}

func TestAPIConnectionFlagsDriveRuntimeStateCommand(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	tokenFile := writeCLIAPITokenFile(t, "flag-token")
	var sawRequest bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawRequest = true
		if r.URL.Path != "/v1/rpc" {
			t.Errorf("path = %q, want /v1/rpc", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer flag-token" {
			t.Errorf("Authorization = %q, want bearer flag-token", got)
		}
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.Method != "run.list" {
			t.Errorf("method = %q, want run.list", req.Method)
		}
		writeJSONRPCResult(t, w, req.ID, map[string]any{"runs": []any{}})
	}))
	defer server.Close()

	opts := defaultRootCommandOptions()
	opts.httpClient = server.Client()
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "list", "--api-server", server.URL, "--api-token-file", tokenFile}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("exit = %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	if !sawRequest {
		t.Fatal("server did not receive request")
	}
}

func TestAPIConnectionDefaultLoopbackTokenDrivesRuntimeStateCommand(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	var sawRequest bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawRequest = true
		if got := r.Header.Get("Authorization"); got != "Bearer "+apiv1.DefaultLoopbackAPIToken {
			t.Errorf("Authorization = %q, want default loopback bearer", got)
		}
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.Method != "run.list" {
			t.Errorf("method = %q, want run.list", req.Method)
		}
		writeJSONRPCResult(t, w, req.ID, map[string]any{"runs": []any{}})
	}))
	defer server.Close()

	opts := defaultRootCommandOptions()
	opts.httpClient = server.Client()
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "list", "--api-server", server.URL}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("exit = %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	if !sawRequest {
		t.Fatal("server did not receive request")
	}
}

func TestAPIConnectionFlagsPreserveServerBasePathPrefix(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	tokenFile := writeCLIAPITokenFile(t, "flag-token")
	var sawRequest bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawRequest = true
		if r.URL.Path != "/proxy/v1/rpc" {
			t.Errorf("path = %q, want /proxy/v1/rpc", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer flag-token" {
			t.Errorf("Authorization = %q, want bearer flag-token", got)
		}
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.Method != "run.list" {
			t.Errorf("method = %q, want run.list", req.Method)
		}
		writeJSONRPCResult(t, w, req.ID, map[string]any{"runs": []any{}})
	}))
	defer server.Close()

	opts := defaultRootCommandOptions()
	opts.httpClient = server.Client()
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "list", "--api-server", server.URL + "/proxy", "--api-token-file", tokenFile}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("exit = %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	if !sawRequest {
		t.Fatal("server did not receive request")
	}
}

func TestCLIAPIConfigDrivesRuntimeStateCommand(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	tokenFile := writeCLIAPITokenFile(t, "config-token")
	var sawRequest bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawRequest = true
		if got := r.Header.Get("Authorization"); got != "Bearer config-token" {
			t.Errorf("Authorization = %q, want bearer config-token", got)
		}
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, req.ID, map[string]any{"runs": []any{}})
	}))
	defer server.Close()
	t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
		"api_server":     server.URL,
		"api_token_file": tokenFile,
	}))

	opts := defaultRootCommandOptions()
	opts.httpClient = server.Client()
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "list"}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("exit = %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	if !sawRequest {
		t.Fatal("server did not receive request")
	}
}

func TestCLIAPIProjectContextOutranksSelectedGlobal(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	project := writeCLIAPIProjectFixture(t)
	swarmDir := t.TempDir()
	projectServer := startCLIAPIRuntimeIdentityServer(t, "runtime-project")
	globalServer := startCLIAPIRuntimeIdentityServer(t, "runtime-global")
	registry := newLocalContextRegistry(swarmDir)
	writeCLIAPITestContext(t, registry, "global", "runtime-global", globalServer.URL, "")
	writeCLIAPITestContext(t, registry, localProjectContextName(project.canonicalRoot), "runtime-project", projectServer.URL, project.canonicalRoot)
	if err := registry.SetCurrent("global"); err != nil {
		t.Fatalf("set current: %v", err)
	}

	client, err := newCLIAPIClient(rootCommandOptions{
		repoRoot: project.root,
		rootFlags: &rootCommandFlagState{
			swarmDir:    swarmDir,
			swarmDirSet: true,
		},
	})
	if err != nil {
		t.Fatalf("newCLIAPIClient: %v", err)
	}
	if want := projectServer.URL + "/v1/rpc"; client.endpoint != want {
		t.Fatalf("endpoint = %q, want project endpoint %q", client.endpoint, want)
	}
	if client.target.source != "project context" || client.target.projectRoot != project.canonicalRoot {
		t.Fatalf("target = %#v, want project context for %s", client.target, project.canonicalRoot)
	}
}

func TestCLIAPIContextDescriptorAuthOutranksConfigTokenFile(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	project := writeCLIAPIProjectFixture(t)
	swarmDir := t.TempDir()
	configToken := writeCLIAPITokenFile(t, "config-token")
	t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
		"api_server":     "http://127.0.0.1:4444",
		"api_token_file": configToken,
	}))
	server := startCLIAPIRuntimeIdentityServer(t, "runtime-project")
	registry := newLocalContextRegistry(swarmDir)
	writeCLIAPITestContext(t, registry, localProjectContextName(project.canonicalRoot), "runtime-project", server.URL, project.canonicalRoot)

	client, err := newCLIAPIClient(rootCommandOptions{
		repoRoot: project.root,
		rootFlags: &rootCommandFlagState{
			swarmDir:    swarmDir,
			swarmDirSet: true,
		},
	})
	if err != nil {
		t.Fatalf("newCLIAPIClient: %v", err)
	}
	if client.target.source != "project context" {
		t.Fatalf("target source = %q, want project context", client.target.source)
	}
	if client.token != apiv1.DefaultLoopbackAPIToken {
		t.Fatalf("token = %q, want descriptor built-in loopback token", client.token)
	}

	flagToken := writeCLIAPITokenFile(t, "flag-token")
	settings, err := resolveCLIAPISettings(rootCommandOptions{
		apiTokenFile: flagToken,
		repoRoot:     project.root,
		rootFlags: &rootCommandFlagState{
			swarmDir:    swarmDir,
			swarmDirSet: true,
		},
	})
	if err != nil {
		t.Fatalf("resolveCLIAPISettings with flag token: %v", err)
	}
	if settings.target.source != "project context" {
		t.Fatalf("target source with flag token = %q, want project context", settings.target.source)
	}
	if settings.token != "flag-token" || settings.tokenSource != "--api-token-file" {
		t.Fatalf("token/source = %q/%q, want flag-token/--api-token-file", settings.token, settings.tokenSource)
	}
}

func TestCLIAPIExplicitAPIServerAndContextPrecedence(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	project := writeCLIAPIProjectFixture(t)
	swarmDir := t.TempDir()
	projectServer := startCLIAPIRuntimeIdentityServer(t, "runtime-project")
	explicitContextServer := startCLIAPIRuntimeIdentityServer(t, "runtime-explicit")
	apiServer := startCLIAPIRuntimeIdentityServer(t, "runtime-api-server")
	registry := newLocalContextRegistry(swarmDir)
	writeCLIAPITestContext(t, registry, localProjectContextName(project.canonicalRoot), "runtime-project", projectServer.URL, project.canonicalRoot)
	writeCLIAPITestContext(t, registry, "chosen", "runtime-explicit", explicitContextServer.URL, "")

	client, err := newCLIAPIClient(rootCommandOptions{
		repoRoot:    project.root,
		apiServer:   apiServer.URL,
		contextName: "chosen",
		rootFlags: &rootCommandFlagState{
			swarmDir:    swarmDir,
			swarmDirSet: true,
		},
	})
	if err != nil {
		t.Fatalf("newCLIAPIClient api-server: %v", err)
	}
	if want := apiServer.URL + "/v1/rpc"; client.endpoint != want {
		t.Fatalf("api-server endpoint = %q, want %q", client.endpoint, want)
	}
	if client.target.source != "--api-server" {
		t.Fatalf("api-server target source = %q", client.target.source)
	}

	client, err = newCLIAPIClient(rootCommandOptions{
		repoRoot:    project.root,
		contextName: "chosen",
		rootFlags: &rootCommandFlagState{
			swarmDir:    swarmDir,
			swarmDirSet: true,
		},
	})
	if err != nil {
		t.Fatalf("newCLIAPIClient context: %v", err)
	}
	if want := explicitContextServer.URL + "/v1/rpc"; client.endpoint != want {
		t.Fatalf("context endpoint = %q, want %q", client.endpoint, want)
	}
	if client.target.source != "--context" || client.target.contextName != "chosen" {
		t.Fatalf("context target = %#v", client.target)
	}
}

func TestCLIAPIProjectContextFailureClassesFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name      string
		setup     func(t *testing.T, registry localContextRegistry, project cliAPITestProject)
		wantError string
	}{
		{
			name: "stale descriptor",
			setup: func(t *testing.T, registry localContextRegistry, project cliAPITestProject) {
				writeCLIAPITestContext(t, registry, "stale", "runtime-stale", "http://127.0.0.1:1", project.canonicalRoot)
			},
			wantError: "project context",
		},
		{
			name: "identity mismatch",
			setup: func(t *testing.T, registry localContextRegistry, project cliAPITestProject) {
				server := startCLIAPIRuntimeIdentityServer(t, "runtime-live")
				writeCLIAPITestContext(t, registry, "mismatch", "runtime-descriptor", server.URL, project.canonicalRoot)
			},
			wantError: "identity_mismatch",
		},
		{
			name: "multiple live",
			setup: func(t *testing.T, registry localContextRegistry, project cliAPITestProject) {
				a := startCLIAPIRuntimeIdentityServer(t, "runtime-a")
				b := startCLIAPIRuntimeIdentityServer(t, "runtime-b")
				writeCLIAPITestContext(t, registry, "a", "runtime-a", a.URL, project.canonicalRoot)
				writeCLIAPITestContext(t, registry, "b", "runtime-b", b.URL, project.canonicalRoot)
			},
			wantError: "multiple live project contexts",
		},
		{
			name: "live plus stale",
			setup: func(t *testing.T, registry localContextRegistry, project cliAPITestProject) {
				server := startCLIAPIRuntimeIdentityServer(t, "runtime-live")
				writeCLIAPITestContext(t, registry, "live", "runtime-live", server.URL, project.canonicalRoot)
				writeCLIAPITestContext(t, registry, "stale", "runtime-stale", "http://127.0.0.1:1", project.canonicalRoot)
			},
			wantError: "no_server",
		},
		{
			name: "corrupt descriptor",
			setup: func(t *testing.T, registry localContextRegistry, project cliAPITestProject) {
				path, err := registry.descriptorPath("corrupt")
				if err != nil {
					t.Fatalf("descriptor path: %v", err)
				}
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatalf("mkdir: %v", err)
				}
				if err := os.WriteFile(path, []byte(`{bad-json`), 0o600); err != nil {
					t.Fatalf("write corrupt descriptor: %v", err)
				}
			},
			wantError: "corrupt_descriptor",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			project := writeCLIAPIProjectFixture(t)
			swarmDir := t.TempDir()
			registry := newLocalContextRegistry(swarmDir)
			tc.setup(t, registry, project)

			_, err := newCLIAPIClient(rootCommandOptions{
				repoRoot: project.root,
				rootFlags: &rootCommandFlagState{
					swarmDir:    swarmDir,
					swarmDirSet: true,
				},
			})
			if err == nil {
				t.Fatal("newCLIAPIClient returned nil error")
			}
			if !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("err = %q, want %q", err.Error(), tc.wantError)
			}
		})
	}
}

func TestCLIAPIMutatingCommandDoesNotFallThroughFromProjectWithoutContext(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	project := writeCLIAPIProjectFixture(t)
	swarmDir := t.TempDir()
	globalServer := startCLIAPIRuntimeIdentityServer(t, "runtime-global")
	registry := newLocalContextRegistry(swarmDir)
	writeCLIAPITestContext(t, registry, "global", "runtime-global", globalServer.URL, "")
	if err := registry.SetCurrent("global"); err != nil {
		t.Fatalf("set current: %v", err)
	}

	_, err := newCLIAPIClient(rootCommandOptions{
		repoRoot:        project.root,
		apiCommandClass: cliAPICommandClassMutating,
		rootFlags: &rootCommandFlagState{
			swarmDir:    swarmDir,
			swarmDirSet: true,
		},
	})
	if err == nil {
		t.Fatal("newCLIAPIClient returned nil error")
	}
	if !strings.Contains(err.Error(), "no live project context") {
		t.Fatalf("err = %q, want no live project context", err.Error())
	}
}

func TestCLIAPIProjectContextUsesRealpathForSymlinkedRoot(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	project := writeCLIAPIProjectFixture(t)
	parent := t.TempDir()
	link := filepath.Join(parent, "link-project")
	if err := os.Symlink(project.root, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	swarmDir := t.TempDir()
	server := startCLIAPIRuntimeIdentityServer(t, "runtime-project")
	registry := newLocalContextRegistry(swarmDir)
	writeCLIAPITestContext(t, registry, localProjectContextName(project.canonicalRoot), "runtime-project", server.URL, project.canonicalRoot)

	client, err := newCLIAPIClient(rootCommandOptions{
		repoRoot: link,
		rootFlags: &rootCommandFlagState{
			swarmDir:    swarmDir,
			swarmDirSet: true,
		},
	})
	if err != nil {
		t.Fatalf("newCLIAPIClient: %v", err)
	}
	if want := server.URL + "/v1/rpc"; client.endpoint != want {
		t.Fatalf("endpoint = %q, want %q", client.endpoint, want)
	}
}

func TestEndpointShapedAPIServerRejectedBeforeRequest(t *testing.T) {
	for _, tc := range []struct {
		name  string
		args  []string
		setup func(t *testing.T, serverURL string, tokenFile string)
		want  string
	}{
		{
			name: "flag direct RPC endpoint",
			args: []string{"run", "list", "--api-server", "{server}/v1/rpc", "--api-token-file", "{token}"},
			want: "not a direct /v1/rpc endpoint",
		},
		{
			name: "flag prefixed RPC endpoint",
			args: []string{"run", "list", "--api-server", "{server}/proxy/v1/rpc", "--api-token-file", "{token}"},
			want: "not a direct /v1/rpc endpoint",
		},
		{
			name: "flag prefixed websocket endpoint",
			args: []string{"run", "list", "--api-server", "{server}/proxy/v1/ws", "--api-token-file", "{token}"},
			want: "not a direct /v1/ws endpoint",
		},
		{
			name: "environment direct websocket endpoint is removed",
			args: []string{"run", "list"},
			setup: func(t *testing.T, serverURL string, _ string) {
				t.Setenv("SWARM_API_SERVER", serverURL+"/v1/ws")
				t.Setenv("SWARM_API_TOKEN", "env-token")
			},
			want: "client-side API environment sources are no longer accepted",
		},
		{
			name: "environment prefixed RPC endpoint is removed",
			args: []string{"run", "list"},
			setup: func(t *testing.T, serverURL string, _ string) {
				t.Setenv("SWARM_API_SERVER", serverURL+"/proxy/v1/rpc")
				t.Setenv("SWARM_API_TOKEN", "env-token")
			},
			want: "client-side API environment sources are no longer accepted",
		},
		{
			name: "config direct RPC endpoint",
			args: []string{"run", "list"},
			setup: func(t *testing.T, serverURL string, tokenFile string) {
				t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
					"api_server":     serverURL + "/v1/rpc",
					"api_token_file": tokenFile,
				}))
			},
			want: "not a direct /v1/rpc endpoint",
		},
		{
			name: "config prefixed RPC endpoint",
			args: []string{"run", "list"},
			setup: func(t *testing.T, serverURL string, tokenFile string) {
				t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
					"api_server":     serverURL + "/proxy/v1/rpc",
					"api_token_file": tokenFile,
				}))
			},
			want: "not a direct /v1/rpc endpoint",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			tokenFile := writeCLIAPITokenFile(t, "test-token")
			var sawRequest bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				sawRequest = true
				http.Error(w, "unexpected request", http.StatusInternalServerError)
			}))
			defer server.Close()
			args := append([]string(nil), tc.args...)
			for i, arg := range args {
				arg = strings.ReplaceAll(arg, "{server}", server.URL)
				arg = strings.ReplaceAll(arg, "{token}", tokenFile)
				args[i] = arg
			}
			if tc.setup != nil {
				tc.setup(t, server.URL, tokenFile)
			}
			opts := defaultRootCommandOptions()
			opts.httpClient = server.Client()
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, opts)
			if code != cliExitValidation {
				t.Fatalf("exit = %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
			}
			if !strings.Contains(stderr.String(), tc.want) {
				t.Fatalf("stderr = %q, want %q", stderr.String(), tc.want)
			}
			if sawRequest {
				t.Fatal("endpoint-shaped API server value reached the HTTP server")
			}
		})
	}
}

func TestAPIConnectionFlagsRejectedOnNonAPISurfaces(t *testing.T) {
	for _, tc := range []struct {
		args       []string
		wantStderr string
	}{
		{args: []string{"--api-server", "http://127.0.0.1:9"}, wantStderr: "unknown flag: --api-server"},
		{args: []string{"--api-server", "http://127.0.0.1:9", "runs"}, wantStderr: "unknown flag: --api-server"},
		{args: []string{"serve", "--api-server", "http://127.0.0.1:9"}, wantStderr: "unknown flag: --api-server"},
		{args: []string{"verify", "--api-server", "http://127.0.0.1:9"}},
		{args: []string{"run", "start", "--api-server", "http://127.0.0.1:9"}, wantStderr: "unknown flag: --api-server"},
		{args: []string{"event", "--api-server", "http://127.0.0.1:9", "list"}, wantStderr: "unknown flag: --api-server"},
	} {
		var stdout, stderr bytes.Buffer
		code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, defaultRootCommandOptions())
		if code != cliExitValidation {
			t.Fatalf("%v exit = %d stderr=%q", tc.args, code, stderr.String())
		}
		if tc.wantStderr != "" && !strings.Contains(stderr.String(), tc.wantStderr) {
			t.Fatalf("%v stderr = %q, want %q", tc.args, stderr.String(), tc.wantStderr)
		}
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"version", "--api-server", "http://127.0.0.1:9"}, &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitValidation {
		t.Fatalf("version exit = %d stderr=%q", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "require --server") {
		t.Fatalf("version stderr = %q, want --server requirement", stderr.String())
	}
}

type cliAPITestProject struct {
	root          string
	contracts     string
	canonicalRoot string
}

func writeCLIAPIProjectFixture(t *testing.T) cliAPITestProject {
	t.Helper()
	root := t.TempDir()
	contracts := filepath.Join(root, "contracts")
	if err := os.MkdirAll(contracts, 0o755); err != nil {
		t.Fatalf("mkdir contracts: %v", err)
	}
	if err := os.WriteFile(filepath.Join(contracts, "package.yaml"), []byte("name: test\nversion: 1.0.0\n"), 0o600); err != nil {
		t.Fatalf("write package: %v", err)
	}
	canonical, status := canonicalizeDoctorTargetPath(root)
	if status != "resolved" {
		t.Fatalf("canonicalize project root status = %q", status)
	}
	return cliAPITestProject{root: root, contracts: contracts, canonicalRoot: canonical}
}

func startCLIAPIRuntimeIdentityServer(t *testing.T, runtimeID string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+apiv1.DefaultLoopbackAPIToken {
			t.Fatalf("Authorization = %q, want built-in loopback bearer", got)
		}
		switch req.Method {
		case "runtime.identity":
			writeJSONRPCResult(t, w, req.ID, map[string]any{
				"runtime_instance_id":  runtimeID,
				"started_at":           "2026-07-02T00:00:00Z",
				"api_version":          "v1",
				"supported_transports": []string{"tcp"},
			})
		case "run.list":
			writeJSONRPCResult(t, w, req.ID, map[string]any{"runs": []any{}})
		default:
			writeJSONRPCResult(t, w, req.ID, map[string]any{})
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func writeCLIAPITestContext(t *testing.T, registry localContextRegistry, name, runtimeID, apiServer, projectRoot string) {
	t.Helper()
	now := localContextTimestamp()
	if err := registry.WriteDescriptor(localContextDescriptor{
		Version:           localContextDescriptorVersion,
		Name:              name,
		RuntimeInstanceID: runtimeID,
		Transport:         localContextTransportTCP,
		APIServer:         apiServer,
		Auth:              localContextDescriptorAuth{Mode: localContextAuthBuiltinLoopback},
		ProjectRoot:       projectRoot,
		CreatedAt:         now,
		UpdatedAt:         now,
	}); err != nil {
		t.Fatalf("write context %s: %v", name, err)
	}
}

func isolateCLIAPIConfigEnv(t *testing.T) {
	t.Helper()
	t.Setenv("SWARM_CONFIG", "")
	t.Setenv("SWARM_API_SERVER", "")
	t.Setenv("SWARM_API_TOKEN", "")
	t.Setenv("SWARM_API_TOKEN_FILE", "")
	t.Setenv("SWARM_API_LISTEN_ADDR", "")
	t.Setenv("SWARM_MCP_LISTEN_ADDR", "")
	t.Setenv("SWARM_CONTRACTS_PATH", "")
	t.Setenv("SWARM_CONTRACTS_DIR", "")
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("HOME", t.TempDir())
}

func setCLIAPITestToken(t *testing.T, token string) {
	t.Helper()
	if strings.TrimSpace(token) == "" {
		t.Setenv("SWARM_CONFIG", "")
		return
	}
	t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
		"api_token_file": writeCLIAPITokenFile(t, token),
	}))
}

func writeCLIAPITokenFile(t *testing.T, token string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte(token), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	return path
}

func writeCLIAPIConfigFile(t *testing.T, values map[string]string) string {
	t.Helper()
	var body strings.Builder
	writeSection := func(section string, keys []string, names map[string]string) {
		var sectionBody strings.Builder
		for _, key := range keys {
			value, ok := values[key]
			if !ok {
				continue
			}
			name := names[key]
			if name == "" {
				name = key
			}
			sectionBody.WriteString("  ")
			sectionBody.WriteString(name)
			sectionBody.WriteString(": ")
			sectionBody.WriteString(strconvQuoteYAML(value))
			sectionBody.WriteString("\n")
		}
		if sectionBody.Len() == 0 {
			return
		}
		body.WriteString(section)
		body.WriteString(":\n")
		body.WriteString(sectionBody.String())
	}
	writeSection("connection", []string{"api_server", "api_token_file"}, map[string]string{
		"api_server":     "api_server",
		"api_token_file": "api_token_file",
	})
	writeSection("paths", []string{"swarm_dir", "contracts_path", "platform_spec_path"}, map[string]string{
		"swarm_dir":          "swarm_dir",
		"contracts_path":     "contracts_path",
		"platform_spec_path": "platform_spec_path",
	})
	writeSection("serve", []string{"serve_api_listen_addr", "serve_mcp_listen_addr", "serve_api_token_file"}, map[string]string{
		"serve_api_listen_addr": "api_listen_addr",
		"serve_mcp_listen_addr": "mcp_listen_addr",
		"serve_api_token_file":  "api_token_file",
	})
	path := filepath.Join(t.TempDir(), "swarm.yaml")
	configText := withTestProviderTriggerPlatformInventory(t, body.String())
	if err := os.WriteFile(path, []byte(configText), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}
	return path
}

func strconvQuoteYAML(value string) string {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return string(raw)
}

func mustFindCLICommand(t *testing.T, root *cobra.Command, commandPath string) *cobra.Command {
	t.Helper()
	if strings.TrimSpace(commandPath) == "" {
		return root
	}
	cmd, _, err := root.Find(strings.Fields(commandPath))
	if err != nil {
		t.Fatalf("find %s: %v", commandPath, err)
	}
	if cmd == nil {
		t.Fatalf("find %s returned nil", commandPath)
	}
	return cmd
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) {
	return len(p), nil
}
