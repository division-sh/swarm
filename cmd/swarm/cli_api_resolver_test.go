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
	t.Run("flag sources beat env config and defaults", func(t *testing.T) {
		isolateCLIAPIConfigEnv(t)
		configToken := writeCLIAPITokenFile(t, "config-token")
		envToken := writeCLIAPITokenFile(t, "env-file-token")
		flagToken := writeCLIAPITokenFile(t, "flag-token")
		configPath := writeCLIAPIConfigFile(t, map[string]string{
			"api_server":     "http://127.0.0.1:4444",
			"api_token_file": configToken,
		})
		t.Setenv("SWARM_CONFIG", configPath)
		t.Setenv("SWARM_API_SERVER", "http://127.0.0.1:5555")
		t.Setenv("SWARM_API_TOKEN", "env-token")
		t.Setenv("SWARM_API_TOKEN_FILE", envToken)

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

	t.Run("environment token beats token file and config", func(t *testing.T) {
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

		client, err := newCLIAPIClient(rootCommandOptions{})
		if err != nil {
			t.Fatalf("newCLIAPIClient: %v", err)
		}
		if client.endpoint != "http://127.0.0.1:5555/v1/rpc" {
			t.Fatalf("endpoint = %q", client.endpoint)
		}
		if client.token != "env-token" {
			t.Fatalf("token = %q", client.token)
		}
	})

	t.Run("environment token file beats config", func(t *testing.T) {
		isolateCLIAPIConfigEnv(t)
		configToken := writeCLIAPITokenFile(t, "config-token")
		envToken := writeCLIAPITokenFile(t, "env-file-token")
		t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
			"api_server":     "http://127.0.0.1:4444",
			"api_token_file": configToken,
		}))
		t.Setenv("SWARM_API_TOKEN_FILE", envToken)

		client, err := newCLIAPIClient(rootCommandOptions{})
		if err != nil {
			t.Fatalf("newCLIAPIClient: %v", err)
		}
		if client.endpoint != "http://127.0.0.1:4444/v1/rpc" {
			t.Fatalf("endpoint = %q", client.endpoint)
		}
		if client.token != "env-file-token" {
			t.Fatalf("token = %q", client.token)
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
			wantErr:   "SWARM_API_TOKEN is required",
		},
		{
			name:      "wildcard needs explicit token",
			apiServer: "http://0.0.0.0:8081",
			wantErr:   "SWARM_API_TOKEN is required",
		},
		{
			name:      "routable address needs explicit token",
			apiServer: "http://192.0.2.10:8081",
			wantErr:   "SWARM_API_TOKEN is required",
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
	if got.Path != configDir || got.Source != "config swarm_dir" {
		t.Fatalf("config swarm dir = %#v, want %q from config swarm_dir", got, configDir)
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
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("swarm_dir: [bad]\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("SWARM_CONFIG", path)

	if _, err := resolveCLISwarmDir(cliSwarmDirOptions{}); err == nil || !strings.Contains(err.Error(), "CLI config swarm_dir must be a string") {
		t.Fatalf("resolveCLISwarmDir err = %v, want non-string swarm_dir validation", err)
	}
}

func TestCLISwarmDirBlankConfigFailsClosed(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("swarm_dir: \"  \"\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("SWARM_CONFIG", path)

	if _, err := resolveCLISwarmDir(cliSwarmDirOptions{}); err == nil || !strings.Contains(err.Error(), "config swarm_dir must be non-empty") {
		t.Fatalf("resolveCLISwarmDir err = %v, want blank config swarm_dir validation", err)
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
			wantErr:  "read SWARM_CONFIG",
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
			wantErr:  "parse CLI config",
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
			wantErr:  `unsupported CLI config key "api_token"`,
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
				t.Setenv("SWARM_API_TOKEN", "env-token")
				return rootCommandOptions{apiServer: "http://127.0.0.1:8081?x=1"}
			},
			wantExit: cliExitValidation,
			wantErr:  "must not include query or fragment",
		},
		{
			name: "flag API server rejects direct RPC endpoint",
			setup: func(t *testing.T) rootCommandOptions {
				t.Setenv("SWARM_API_TOKEN", "env-token")
				return rootCommandOptions{apiServer: "http://127.0.0.1:8081/v1/rpc"}
			},
			wantExit: cliExitValidation,
			wantErr:  "not a direct /v1/rpc endpoint",
		},
		{
			name: "flag API server rejects prefixed RPC endpoint",
			setup: func(t *testing.T) rootCommandOptions {
				t.Setenv("SWARM_API_TOKEN", "env-token")
				return rootCommandOptions{apiServer: "http://127.0.0.1:8081/proxy/v1/rpc"}
			},
			wantExit: cliExitValidation,
			wantErr:  "not a direct /v1/rpc endpoint",
		},
		{
			name: "environment API server rejects direct websocket endpoint",
			setup: func(t *testing.T) rootCommandOptions {
				t.Setenv("SWARM_API_SERVER", "http://127.0.0.1:8081/v1/ws")
				t.Setenv("SWARM_API_TOKEN", "env-token")
				return rootCommandOptions{}
			},
			wantExit: cliExitValidation,
			wantErr:  "not a direct /v1/ws endpoint",
		},
		{
			name: "environment API server rejects prefixed websocket endpoint",
			setup: func(t *testing.T) rootCommandOptions {
				t.Setenv("SWARM_API_SERVER", "http://127.0.0.1:8081/proxy/v1/ws")
				t.Setenv("SWARM_API_TOKEN", "env-token")
				return rootCommandOptions{}
			},
			wantExit: cliExitValidation,
			wantErr:  "not a direct /v1/ws endpoint",
		},
		{
			name: "config API server rejects direct RPC endpoint",
			setup: func(t *testing.T) rootCommandOptions {
				t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
					"api_server": "http://127.0.0.1:8081/v1/rpc",
				}))
				t.Setenv("SWARM_API_TOKEN", "env-token")
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
				t.Setenv("SWARM_API_TOKEN", "env-token")
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
		"runs", "status", "trace", "health", "logs", "incidents",
		"events list", "events follow", "event view", "event publish", "event replay",
		"bundle list", "bundle show", "bundle agents", "bundle register",
		"agents list", "agent view", "agent diagnose", "agent restart", "agent replay", "agent replay-backlog", "agent directive",
		"entities list", "entity view", "entity aggregate",
		"mailbox list", "mailbox view", "mailbox approve", "mailbox reject", "mailbox defer",
		"control pause", "control continue", "control stop", "control nuke",
		"fork", "version",
	}
	for _, path := range withFlags {
		cmd := mustFindCLICommand(t, root, path)
		if cmd.Flags().Lookup("api-server") == nil {
			t.Fatalf("%s missing --api-server", path)
		}
		if cmd.Flags().Lookup("api-token-file") == nil {
			t.Fatalf("%s missing --api-token-file", path)
		}
	}

	withoutFlags := []string{
		"", "serve", "verify", "completion", "run",
		"events", "event", "bundle", "agents", "agent", "entities", "entity", "mailbox", "control",
		"investigate", "investigate health",
	}
	for _, path := range withoutFlags {
		cmd := mustFindCLICommand(t, root, path)
		if cmd.Flags().Lookup("api-server") != nil || cmd.Flags().Lookup("api-token-file") != nil {
			t.Fatalf("%s unexpectedly accepts API connection flags", path)
		}
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
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"runs", "--api-server", server.URL, "--api-token-file", tokenFile}, &stdout, &stderr, opts)
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
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"runs", "--api-server", server.URL}, &stdout, &stderr, opts)
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
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"runs", "--api-server", server.URL + "/proxy", "--api-token-file", tokenFile}, &stdout, &stderr, opts)
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
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"runs"}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("exit = %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	if !sawRequest {
		t.Fatal("server did not receive request")
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
			args: []string{"runs", "--api-server", "{server}/v1/rpc", "--api-token-file", "{token}"},
			want: "not a direct /v1/rpc endpoint",
		},
		{
			name: "flag prefixed RPC endpoint",
			args: []string{"runs", "--api-server", "{server}/proxy/v1/rpc", "--api-token-file", "{token}"},
			want: "not a direct /v1/rpc endpoint",
		},
		{
			name: "flag prefixed websocket endpoint",
			args: []string{"runs", "--api-server", "{server}/proxy/v1/ws", "--api-token-file", "{token}"},
			want: "not a direct /v1/ws endpoint",
		},
		{
			name: "environment direct websocket endpoint",
			args: []string{"runs"},
			setup: func(t *testing.T, serverURL string, _ string) {
				t.Setenv("SWARM_API_SERVER", serverURL+"/v1/ws")
				t.Setenv("SWARM_API_TOKEN", "env-token")
			},
			want: "not a direct /v1/ws endpoint",
		},
		{
			name: "environment prefixed RPC endpoint",
			args: []string{"runs"},
			setup: func(t *testing.T, serverURL string, _ string) {
				t.Setenv("SWARM_API_SERVER", serverURL+"/proxy/v1/rpc")
				t.Setenv("SWARM_API_TOKEN", "env-token")
			},
			want: "not a direct /v1/rpc endpoint",
		},
		{
			name: "config direct RPC endpoint",
			args: []string{"runs"},
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
			args: []string{"runs"},
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
		{args: []string{"run", "--api-server", "http://127.0.0.1:9"}, wantStderr: "unknown flag: --api-server"},
		{args: []string{"events", "--api-server", "http://127.0.0.1:9", "list"}, wantStderr: "unknown flag: --api-server"},
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
	for _, key := range []string{"api_server", "api_token_file", "swarm_dir", "contracts_path", "platform_spec_path", "serve_api_listen_addr", "serve_mcp_listen_addr"} {
		if value, ok := values[key]; ok {
			body.WriteString(key)
			body.WriteString(": ")
			body.WriteString(strconvQuoteYAML(value))
			body.WriteString("\n")
		}
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body.String()), 0o600); err != nil {
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
