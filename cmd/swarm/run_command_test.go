package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/gorilla/websocket"
)

func TestRunCommandLocalForegroundConsumesServeOwnerAndV1API(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"entity_id": "entity-1"})
	tracePrinted := make(chan struct{})
	server, calls, wsRequests := newRunCommandServer(t, runCommandServerOptions{
		expectedToken: apiv1.DefaultLoopbackAPIToken,
		rpcResponder: func(req jsonRPCRequest, callIndex int) map[string]any {
			switch req.Method {
			case "health.check":
				return runCommandHealthResult()
			case "run.start":
				if got := req.Params["event_name"]; got != "scan.requested" {
					t.Fatalf("event_name = %#v, want scan.requested", got)
				}
				if got := req.Params["bundle_hash"]; got != "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
					t.Fatalf("bundle_hash = %#v, want health.check bundle hash", got)
				}
				if _, ok := req.Params["bundle_ref"]; ok {
					t.Fatalf("bundle_ref unexpectedly present without bundle flag: %#v", req.Params)
				}
				return map[string]any{"run_id": "run-local", "status": "running"}
			case "run.get":
				run := validDiagnosticRunHeader("run-local")
				select {
				case <-tracePrinted:
					run["status"] = "completed"
					run["ended_at"] = "2026-05-13T10:01:00Z"
				default:
				}
				return map[string]any{"run": run}
			default:
				t.Fatalf("unexpected method[%d] = %q", callIndex, req.Method)
			}
			return nil
		},
		wsRows: []map[string]any{validRunCommandTraceRow("evt-local")},
	})
	defer server.Close()

	var serveCalled atomic.Int32
	serveCanceled := make(chan struct{})
	opts := testRunCommandOptions(server)
	repo := t.TempDir()
	configPath := filepath.Join(repo, "runtime.yaml")
	if err := os.WriteFile(configPath, []byte("runtime:\n  recovery_on_startup: true\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	opts.runServe = func(ctx context.Context, repo string, serveOpts serveOptions) int {
		serveCalled.Add(1)
		if serveOpts.ConfigPath != configPath || serveOpts.Backend != "claude_cli" || serveOpts.ContractsPath != filepath.Join(repo, "contracts") || serveOpts.DataSource != "reference-data" || serveOpts.PlatformSpecPath != filepath.Join(repo, "platform.yaml") {
			t.Errorf("serve opts = %#v", serveOpts)
		}
		if serveOpts.Verbose {
			t.Errorf("local run serve opts Verbose = true, want default non-verbose schema summary owner")
		}
		if !serveOpts.LocalRun {
			t.Errorf("local run serve opts LocalRun = false, want shared local preflight consumer marker")
		}
		<-ctx.Done()
		close(serveCanceled)
		return 0
	}

	stdout := &notifyingBuffer{needle: "id=evt-local", notify: tracePrinted}
	var stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repo, []string{"run", "start", "--event", "scan.requested", "--payload", payloadPath, "--config", configPath, "--backend", "claude_cli", "--contracts", "contracts", "--data", "reference-data", "--platform-spec", "platform.yaml"}, stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if serveCalled.Load() != 1 {
		t.Fatalf("serve called = %d, want 1", serveCalled.Load())
	}
	select {
	case <-serveCanceled:
	case <-time.After(time.Second):
		t.Fatal("local serve hook was not canceled after terminal run")
	}
	assertRunCommandMethods(t, calls, []string{"health.check", "health.check", "run.start", "run.get"})
	assertRunCommandTraceSubscription(t, wsRequests, "run-local", true)
	for _, want := range []string{"run started: run_id=run-local", "id=evt-local", "run terminal: run_id=run-local status=completed"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestRunCommandLocalForegroundUsesServeAPITokenFileForEmbeddedClient(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	tokenFile := writeCLIAPITokenFile(t, "serve-token")
	t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
		"serve_api_token_file": tokenFile,
	}))
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"entity_id": "entity-1"})
	server, calls, wsRequests := newRunCommandServer(t, runCommandServerOptions{
		expectedToken: "serve-token",
		strictAuth:    true,
		rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
			switch req.Method {
			case "health.check":
				return runCommandHealthResult()
			case "run.start":
				return map[string]any{"run_id": "run-token-file", "status": "running"}
			case "run.get":
				run := validDiagnosticRunHeader("run-token-file")
				run["status"] = "completed"
				run["ended_at"] = "2026-05-13T10:01:00Z"
				return map[string]any{"run": run}
			default:
				t.Fatalf("unexpected method = %q", req.Method)
			}
			return nil
		},
		wsRows: []map[string]any{validRunCommandTraceRow("evt-token-file")},
	})
	defer server.Close()

	opts := testRunCommandOptions(server)
	var serveCalled atomic.Int32
	opts.runServe = func(ctx context.Context, repo string, serveOpts serveOptions) int {
		serveCalled.Add(1)
		if !serveOpts.LocalRun {
			t.Errorf("local run serve opts LocalRun = false, want true")
		}
		<-ctx.Done()
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--event", "scan.requested", "--payload", payloadPath}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if serveCalled.Load() != 1 {
		t.Fatalf("serve called = %d, want 1", serveCalled.Load())
	}
	assertRunCommandMethods(t, calls, []string{"health.check", "health.check", "run.start", "run.get"})
	assertRunCommandTraceSubscription(t, wsRequests, "run-token-file", true)
}

func TestStartLocalRunServeConsumesContractPathConfigResolver(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()
	configContracts := filepath.Join(t.TempDir(), "config-contracts")
	configPlatform := filepath.Join(t.TempDir(), "config-platform.yaml")
	t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
		"contracts_path":     configContracts,
		"platform_spec_path": configPlatform,
		"api_token_file":     writeCLIAPITokenFile(t, "test-token"),
	}))
	server, _, _ := newRunCommandServer(t, runCommandServerOptions{
		rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
			if req.Method != "health.check" {
				t.Fatalf("unexpected method = %q", req.Method)
			}
			return runCommandHealthResult()
		},
	})
	defer server.Close()

	opts := runCommandOptions{apiOptions: testRunCommandOptions(server), apiPort: 19001}
	serveStarted := make(chan serveOptions, 1)
	opts.apiOptions.runServe = func(ctx context.Context, repo string, serveOpts serveOptions) int {
		serveStarted <- serveOpts
		<-ctx.Done()
		return 0
	}

	stop, err := startLocalRunServe(context.Background(), repo, opts)
	if err != nil {
		t.Fatalf("startLocalRunServe: %v", err)
	}
	stop()
	serveOpts := <-serveStarted
	if serveOpts.ContractsPath != configContracts {
		t.Fatalf("contracts path = %q, want %q", serveOpts.ContractsPath, configContracts)
	}
	if serveOpts.PlatformSpecPath != configPlatform {
		t.Fatalf("platform spec path = %q, want %q", serveOpts.PlatformSpecPath, configPlatform)
	}
	if serveOpts.APIListenAddr != "127.0.0.1:19001" {
		t.Fatalf("api listen addr = %q, want API listener owner from --api-port", serveOpts.APIListenAddr)
	}
	if serveOpts.MCPListenAddr != defaultMCPListenAddr {
		t.Fatalf("mcp listen addr = %q, want unchanged default %q", serveOpts.MCPListenAddr, defaultMCPListenAddr)
	}
	if serveOpts.StoreMode != "sqlite" || serveOpts.StoreModeSet {
		t.Fatalf("store opts = mode %q set %v, want sqlite default with no flag source", serveOpts.StoreMode, serveOpts.StoreModeSet)
	}
	if serveOpts.DataSource != "" {
		t.Fatalf("data source = %q, want unset so serve resolver owns default selection", serveOpts.DataSource)
	}
	if serveOpts.Verbose {
		t.Fatalf("serve verbose = true, want local run default to consume non-verbose serve boot owner")
	}
}

func TestRunCommandHelpShowsDataFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--help"}, &stdout, &stderr, rootCommandOptions{})
	if code != 0 {
		t.Fatalf("run --help code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{"--data", "Path to agent-visible read-only /data reference directory", "--backend", "openai_responses"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("run help missing %q:\n%s", want, stdout.String())
		}
	}
}

func TestRunCommandConnectedNoFollowUsesHealthAndRunStartOnly(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	server, calls, wsRequests := newRunCommandServer(t, runCommandServerOptions{
		rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
			switch req.Method {
			case "health.check":
				return runCommandHealthResult()
			case "run.start":
				if got := req.Params["bundle_hash"]; got != "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
					t.Fatalf("bundle_hash = %#v, want health.check bundle hash", got)
				}
				return map[string]any{"run_id": "run-no-follow", "status": "running"}
			default:
				t.Fatalf("unexpected method = %q", req.Method)
			}
			return nil
		},
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath, "--no-follow"}, &stdout, &stderr, testRunCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	assertRunCommandMethods(t, calls, []string{"health.check", "run.start"})
	if len(*wsRequests) != 0 {
		t.Fatalf("websocket requests = %d, want 0", len(*wsRequests))
	}
	for _, want := range []string{"run started: run_id=run-no-follow", "reattach: swarm run start --connect " + server.URL + " --reattach run-no-follow"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
}

func TestRunCommandStartIncludesOptionalRunIDAndIdempotencyKey(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	server, calls, _ := newRunCommandServer(t, runCommandServerOptions{
		rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
			switch req.Method {
			case "health.check":
				return runCommandHealthResult()
			case "run.start":
				if got := req.Params["run_id"]; got != "run-explicit" {
					t.Fatalf("run_id = %#v, want run-explicit", got)
				}
				if got := req.Params["idempotency_key"]; got != "idem-start" {
					t.Fatalf("idempotency_key = %#v, want idem-start", got)
				}
				return map[string]any{"run_id": "run-explicit", "status": "running"}
			default:
				t.Fatalf("unexpected method = %q", req.Method)
			}
			return nil
		},
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath, "--run-id", "run-explicit", "--idempotency-key", "idem-start", "--no-follow"}, &stdout, &stderr, testRunCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	assertRunCommandMethods(t, calls, []string{"health.check", "run.start"})
}

func TestRunCommandBundleFingerprintMismatchFailsBeforeRunStart(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	server, calls, _ := newRunCommandServer(t, runCommandServerOptions{
		rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
			if req.Method != "health.check" {
				t.Fatalf("unexpected method = %q; run.start must not be called after bundle mismatch", req.Method)
			}
			return runCommandHealthResult()
		},
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath, "--bundle-fingerprint", "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "--no-follow"}, &stdout, &stderr, testRunCommandOptions(server))
	if code != 6 {
		t.Fatalf("code = %d, want 6 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	assertRunCommandMethods(t, calls, []string{"health.check"})
	if !strings.Contains(stderr.String(), "bundle fingerprint mismatch") {
		t.Fatalf("stderr = %q, want bundle mismatch", stderr.String())
	}
}

func TestRunCommandBundleHashSerializesCanonicalParamAndMapsUnsupported(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	var calls []jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/rpc" {
			t.Errorf("path = %q, want /v1/rpc", r.URL.Path)
		}
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		calls = append(calls, req)
		switch req.Method {
		case "health.check":
			writeJSONRPCResult(t, w, req.ID, runCommandHealthResult())
		case "run.start":
			if got := req.Params["bundle_hash"]; got != "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
				t.Fatalf("bundle_hash = %#v", got)
			}
			if _, ok := req.Params["bundle_ref"]; ok {
				t.Fatalf("bundle_ref unexpectedly present: %#v", req.Params)
			}
			writeRunCommandJSONRPCError(t, w, req.ID, "UNSUPPORTED_BUNDLE_HASH")
		default:
			t.Fatalf("unexpected method = %q", req.Method)
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath, "--bundle-hash", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--no-follow"}, &stdout, &stderr, testRunCommandOptions(server))
	if code != 6 {
		t.Fatalf("code = %d, want 6 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	assertRunCommandMethods(t, &calls, []string{"health.check", "run.start"})
	if !strings.Contains(stderr.String(), "UNSUPPORTED_BUNDLE_HASH") {
		t.Fatalf("stderr = %q, want UNSUPPORTED_BUNDLE_HASH", stderr.String())
	}
}

func TestRunCommandStartApplicationErrorsExitSixAndDoNotFollow(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	for _, codeName := range []string{"BUNDLE_SCOPE_REQUIRED", "BUNDLE_UNAVAILABLE", "BUNDLE_DATA_INTEGRITY_ERROR", "BUNDLE_MISMATCH", "EVENT_NOT_DECLARED", "PAYLOAD_VALIDATION_FAILED", "EVENT_PUBLISH_FAILED"} {
		t.Run(codeName, func(t *testing.T) {
			var calls []jsonRPCRequest
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/rpc":
					var req jsonRPCRequest
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Errorf("decode request: %v", err)
					}
					calls = append(calls, req)
					switch req.Method {
					case "health.check":
						writeJSONRPCResult(t, w, req.ID, runCommandHealthResult())
					case "run.start":
						writeRunCommandJSONRPCError(t, w, req.ID, codeName)
					default:
						t.Fatalf("unexpected method = %q", req.Method)
					}
				case "/v1/ws":
					t.Fatalf("run.subscribe_trace must not be opened after failed run.start")
				default:
					t.Fatalf("unexpected path = %q", r.URL.Path)
				}
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath}, &stdout, &stderr, testRunCommandOptions(server))
			if code != 6 {
				t.Fatalf("code = %d, want 6 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			assertRunCommandMethods(t, &calls, []string{"health.check", "run.start"})
			if strings.TrimSpace(stdout.String()) != "" {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			if !strings.Contains(stderr.String(), codeName) {
				t.Fatalf("stderr = %q, want %s", stderr.String(), codeName)
			}
		})
	}
}

func TestRunCommandBundleFingerprintSerializesLegacyBundleRef(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	server, calls, _ := newRunCommandServer(t, runCommandServerOptions{
		rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
			switch req.Method {
			case "health.check":
				return runCommandHealthResult()
			case "run.start":
				if got := req.Params["bundle_ref"]; !reflect.DeepEqual(got, map[string]any{"fingerprint": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}) {
					t.Fatalf("bundle_ref = %#v", got)
				}
				if _, ok := req.Params["bundle_hash"]; ok {
					t.Fatalf("bundle_hash unexpectedly present: %#v", req.Params)
				}
				return map[string]any{"run_id": "run-legacy", "status": "running"}
			default:
				t.Fatalf("unexpected method = %q", req.Method)
			}
			return nil
		},
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath, "--bundle-fingerprint", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--no-follow"}, &stdout, &stderr, testRunCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	assertRunCommandMethods(t, calls, []string{"health.check", "run.start"})
}

func TestRunCommandConnectedForegroundFollowsTraceAndExitsOnTerminalRunGet(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	tracePrinted := make(chan struct{})
	server, calls, wsRequests := newRunCommandServer(t, runCommandServerOptions{
		rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
			switch req.Method {
			case "health.check":
				return runCommandHealthResult()
			case "run.start":
				return map[string]any{"run_id": "run-foreground", "status": "running"}
			case "run.get":
				run := validDiagnosticRunHeader("run-foreground")
				select {
				case <-tracePrinted:
					run["status"] = "completed"
					run["ended_at"] = "2026-05-13T10:01:00Z"
				default:
				}
				return map[string]any{"run": run}
			default:
				t.Fatalf("unexpected method = %q", req.Method)
			}
			return nil
		},
		wsRows: []map[string]any{func() map[string]any {
			row := validRunCommandTraceRow("evt-foreground")
			row["delivery_status"] = "in_progress"
			return row
		}()},
	})
	defer server.Close()

	stdout := &notifyingBuffer{needle: "id=evt-foreground", notify: tracePrinted}
	var stderr bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	code := executeRootCommandWithOptions(ctx, t.TempDir(), []string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath}, stdout, &stderr, testRunCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	methods := runCommandMethodNames(*calls)
	if len(methods) < 3 || methods[0] != "health.check" || methods[1] != "run.start" {
		t.Fatalf("methods = %v, want health.check, run.start, then one or more run.get calls", methods)
	}
	for i, method := range methods[2:] {
		if method != "run.get" {
			t.Fatalf("method[%d] = %q, want run.get; all=%v", i+2, method, methods)
		}
	}
	assertRunCommandTraceSubscription(t, wsRequests, "run-foreground", true)
	for _, want := range []string{"id=evt-foreground", "delivery=in progress", "run terminal: run_id=run-foreground status=completed"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
}

type notifyingBuffer struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	needle string
	notify chan struct{}
	once   sync.Once
}

func (b *notifyingBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n, err := b.buf.Write(p)
	if b.notify != nil && strings.Contains(b.buf.String(), b.needle) {
		b.once.Do(func() {
			close(b.notify)
		})
	}
	return n, err
}

func (b *notifyingBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestRunCommandReattachTerminalUsesRunGetWithoutWebSocket(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	server, calls, wsRequests := newRunCommandServer(t, runCommandServerOptions{
		rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
			if req.Method != "run.get" {
				t.Fatalf("method = %q, want run.get", req.Method)
			}
			run := validDiagnosticRunHeader("run-terminal")
			run["status"] = "failed"
			run["ended_at"] = "2026-05-13T10:01:00Z"
			run["failure"] = testRuntimeFailureClass(runtimefailures.ClassInternalFailure, "workflow_failed")
			return map[string]any{"run": run}
		},
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--connect", server.URL, "--reattach", "run-terminal"}, &stdout, &stderr, testRunCommandOptions(server))
	if code != 7 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	assertRunCommandMethods(t, calls, []string{"run.get"})
	if len(*wsRequests) != 0 {
		t.Fatalf("websocket requests = %d, want 0", len(*wsRequests))
	}
	if !strings.Contains(stdout.String(), "run terminal: run_id=run-terminal status=failed") || !strings.Contains(stdout.String(), "platform.internal_failure/workflow_failed") {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestRunCommandReattachActiveCtrlCDetachesWithoutRunStop(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	wsSubscribed := make(chan struct{})
	server, calls, wsRequests := newRunCommandServer(t, runCommandServerOptions{
		rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
			if req.Method == "run.stop" {
				t.Fatal("reattach Ctrl-C must not call run.stop")
			}
			if req.Method != "run.get" {
				t.Fatalf("method = %q, want run.get", req.Method)
			}
			return map[string]any{"run": validDiagnosticRunHeader("run-active")}
		},
		wsSubscribed: wsSubscribed,
	})
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-wsSubscribed
		cancel()
	}()
	opts := testRunCommandOptions(server)
	opts.runStatusPoll = time.Hour
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(ctx, t.TempDir(), []string{"run", "start", "--connect", server.URL, "--reattach", "run-active"}, &stdout, &stderr, opts)
	if code != 130 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	assertRunCommandMethods(t, calls, []string{"run.get"})
	assertRunCommandTraceSubscription(t, wsRequests, "run-active", false)
	if !strings.Contains(stderr.String(), "detached from run trace") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestRunCommandStartCtrlCCallsRunStopAfterRunID(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	wsSubscribed := make(chan struct{})
	server, calls, _ := newRunCommandServer(t, runCommandServerOptions{
		rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
			switch req.Method {
			case "health.check":
				return runCommandHealthResult()
			case "run.start":
				return map[string]any{"run_id": "run-interrupt", "status": "running"}
			case "run.stop":
				if got := req.Params["run_id"]; got != "run-interrupt" {
					t.Fatalf("run.stop run_id = %#v", got)
				}
				return map[string]any{"ok": true}
			default:
				t.Fatalf("unexpected method = %q", req.Method)
			}
			return nil
		},
		wsSubscribed: wsSubscribed,
	})
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-wsSubscribed
		cancel()
	}()
	opts := testRunCommandOptions(server)
	opts.runStatusPoll = time.Hour
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(ctx, t.TempDir(), []string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath}, &stdout, &stderr, opts)
	if code != 130 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	assertRunCommandMethods(t, calls, []string{"health.check", "run.start", "run.stop"})
	if !strings.Contains(stderr.String(), "interrupted; requested run.stop") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestRunCommandLocalReadinessAuthFailureFailsFast(t *testing.T) {
	setCLIAPITestToken(t, "bad-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"invalid bearer token"}`))
	}))
	defer server.Close()

	var serveCanceled atomic.Bool
	opts := testRunCommandOptions(server)
	opts.runReadyTimeout = time.Minute
	opts.runReadyPoll = time.Millisecond
	opts.runServe = func(ctx context.Context, repo string, serveOpts serveOptions) int {
		<-ctx.Done()
		serveCanceled.Store(true)
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--event", "scan.requested", "--payload", payloadPath}, &stdout, &stderr, opts)
	if code != 4 {
		t.Fatalf("code = %d, want 4 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if !serveCanceled.Load() {
		t.Fatal("local serve hook was not canceled after readiness auth failure")
	}
	if !strings.Contains(stderr.String(), "rejected the request with status 401") {
		t.Fatalf("stderr = %q, want auth failure", stderr.String())
	}
}

func TestRunCommandClosedTraceChannelStillWaitsForRunGet(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	var runGetCalls atomic.Int32
	server, calls, _ := newRunCommandServer(t, runCommandServerOptions{
		rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
			switch req.Method {
			case "health.check":
				return runCommandHealthResult()
			case "run.start":
				return map[string]any{"run_id": "run-closed-ws", "status": "running"}
			case "run.get":
				run := validDiagnosticRunHeader("run-closed-ws")
				if runGetCalls.Add(1) == 1 {
					return map[string]any{"run": run}
				}
				run["status"] = "completed"
				run["ended_at"] = "2026-05-13T10:01:00Z"
				return map[string]any{"run": run}
			default:
				t.Fatalf("unexpected method = %q", req.Method)
			}
			return nil
		},
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath}, &stdout, &stderr, testRunCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	assertRunCommandMethods(t, calls, []string{"health.check", "run.start", "run.get", "run.get"})
	if !strings.Contains(stdout.String(), "run terminal: run_id=run-closed-ws status=completed") {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestRunCommandMalformedWebSocketFailuresExitThree(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	for _, tc := range []struct {
		name       string
		serverOpts runCommandServerOptions
		wantStderr string
	}{
		{
			name: "subscription response missing id",
			serverOpts: runCommandServerOptions{
				wsSubscriptionResult: map[string]any{},
			},
			wantStderr: "subscription_id is required",
		},
		{
			name: "notification missing event id",
			serverOpts: runCommandServerOptions{
				wsRows: []map[string]any{{
					"event_name":       "scan.requested",
					"event_created_at": "2026-05-13T10:00:01Z",
				}},
			},
			wantStderr: "event_id is required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.serverOpts.rpcResponder = func(req jsonRPCRequest, _ int) map[string]any {
				switch req.Method {
				case "health.check":
					return runCommandHealthResult()
				case "run.start":
					return map[string]any{"run_id": "run-bad-ws", "status": "running"}
				default:
					t.Fatalf("unexpected method = %q", req.Method)
				}
				return nil
			}
			server, _, _ := newRunCommandServer(t, tc.serverOpts)
			defer server.Close()

			opts := testRunCommandOptions(server)
			opts.runStatusPoll = time.Hour
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath}, &stdout, &stderr, opts)
			if code != 3 {
				t.Fatalf("code = %d, want 3 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func TestRunCommandWebSocketHandshakeHTTPErrorUsesSharedDiagnostic(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	server, _, _ := newRunCommandServer(t, runCommandServerOptions{
		rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
			switch req.Method {
			case "health.check":
				return runCommandHealthResult()
			case "run.start":
				return map[string]any{"run_id": "run-ws-auth", "status": "running"}
			default:
				t.Fatalf("unexpected method = %q", req.Method)
			}
			return nil
		},
		wsHTTPStatus: http.StatusUnauthorized,
		wsHTTPBody:   "invalid bearer token",
	})
	defer server.Close()

	opts := testRunCommandOptions(server)
	opts.runStatusPoll = time.Hour
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath}, &stdout, &stderr, opts)
	if code != 4 {
		t.Fatalf("code = %d, want 4 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	for _, want := range []string{
		"ERROR: the Swarm runtime at ",
		"rejected the request with status 401",
		"Check API credentials",
	} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr = %q, want substring %q", stderr.String(), want)
		}
	}
	for _, forbidden := range []string{"cannot reach", "runtime event stream dial failed", "/v1/ws"} {
		if strings.Contains(stderr.String(), forbidden) {
			t.Fatalf("stderr = %q, must not contain %q", stderr.String(), forbidden)
		}
	}
}

func TestRunCommandValidationAndAuthNoCallPaths(t *testing.T) {
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	for _, tc := range []struct {
		name       string
		token      string
		args       []string
		wantCode   int
		wantStderr string
	}{
		{name: "detach retired", token: "test-token", args: []string{"run", "start", "--detach", "--event", "scan.requested", "--payload", payloadPath}, wantCode: 2, wantStderr: "swarm run start --connect"},
		{name: "no follow requires connect", token: "test-token", args: []string{"run", "start", "--event", "scan.requested", "--payload", payloadPath, "--no-follow"}, wantCode: 2, wantStderr: "--no-follow requires --connect"},
		{name: "no follow reattach rejected", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--reattach", "run-1", "--no-follow"}, wantCode: 2, wantStderr: "--no-follow and --reattach are mutually exclusive"},
		{name: "invalid bundle hash rejected", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--event", "scan.requested", "--payload", payloadPath, "--bundle-hash", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, wantCode: 2, wantStderr: "--bundle-hash must be bundle-v1:sha256:<64 lowercase hex>"},
		{name: "blank bundle hash rejected", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--event", "scan.requested", "--payload", payloadPath, "--bundle-hash", "  "}, wantCode: 2, wantStderr: "--bundle-hash must be non-empty"},
		{name: "invalid bundle fingerprint rejected", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--event", "scan.requested", "--payload", payloadPath, "--bundle-fingerprint", "sha256:BAD"}, wantCode: 2, wantStderr: "--bundle-fingerprint must be sha256:<64 lowercase hex>"},
		{name: "bundle hash conflicts with legacy fingerprint", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--event", "scan.requested", "--payload", payloadPath, "--bundle-hash", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--bundle-fingerprint", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, wantCode: 2, wantStderr: "--bundle-hash is mutually exclusive with --bundle-fingerprint"},
		{name: "reattach rejects bundle hash", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--reattach", "run-1", "--bundle-hash", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, wantCode: 2, wantStderr: "--reattach is mutually exclusive with --bundle-hash"},
		{name: "reattach rejects bundle fingerprint", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--reattach", "run-1", "--bundle-fingerprint", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, wantCode: 2, wantStderr: "--reattach is mutually exclusive with --bundle-fingerprint"},
		{name: "reattach rejects config flag", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--reattach", "run-1", "--config", "swarm.yaml"}, wantCode: 2, wantStderr: "--reattach is mutually exclusive with --config"},
		{name: "reattach rejects backend flag", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--reattach", "run-1", "--backend", "claude_cli"}, wantCode: 2, wantStderr: "--reattach is mutually exclusive with --backend"},
		{name: "reattach rejects local startup flags", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--reattach", "run-1", "--contracts", "contracts"}, wantCode: 2, wantStderr: "--reattach is mutually exclusive with --contracts"},
		{name: "reattach rejects data flag", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--reattach", "run-1", "--data", "reference-data"}, wantCode: 2, wantStderr: "--reattach is mutually exclusive with --data"},
		{name: "reattach rejects platform spec flag", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--reattach", "run-1", "--platform-spec", "platform.yaml"}, wantCode: 2, wantStderr: "--reattach is mutually exclusive with --platform-spec"},
		{name: "reattach rejects api port flag", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--reattach", "run-1", "--api-port", "8081"}, wantCode: 2, wantStderr: "--reattach is mutually exclusive with --api-port"},
		{name: "blank data rejected", token: "test-token", args: []string{"run", "start", "--event", "scan.requested", "--payload", payloadPath, "--data", " "}, wantCode: 2, wantStderr: "--data must be non-empty"},
		{name: "api port zero rejected when explicit", token: "test-token", args: []string{"run", "start", "--event", "scan.requested", "--payload", payloadPath, "--api-port", "0"}, wantCode: 2, wantStderr: "--api-port must be between 1 and 65535"},
		{name: "api port rejects default mcp listener conflict", token: "test-token", args: []string{"run", "start", "--event", "scan.requested", "--payload", payloadPath, "--api-port", "8082"}, wantCode: 2, wantStderr: "--api-port 8082 conflicts with default MCP listener 127.0.0.1:8082"},
		{name: "mcp port unsupported", token: "test-token", args: []string{"run", "start", "--event", "scan.requested", "--payload", payloadPath, "--mcp-port", "9000"}, wantCode: 2, wantStderr: "--mcp-port is not supported"},
		{name: "connect rejects config local flag", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--event", "scan.requested", "--payload", payloadPath, "--config", "swarm.yaml"}, wantCode: 2, wantStderr: "--config requires local foreground mode"},
		{name: "connect rejects backend local flag", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--event", "scan.requested", "--payload", payloadPath, "--backend", "claude_cli"}, wantCode: 2, wantStderr: "--backend requires local foreground mode"},
		{name: "connect rejects contracts local flag", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--event", "scan.requested", "--payload", payloadPath, "--contracts", "contracts"}, wantCode: 2, wantStderr: "--contracts requires local foreground mode"},
		{name: "connect rejects data local flag", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--event", "scan.requested", "--payload", payloadPath, "--data", "reference-data"}, wantCode: 2, wantStderr: "--data requires local foreground mode"},
		{name: "connect rejects platform spec local flag", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--event", "scan.requested", "--payload", payloadPath, "--platform-spec", "platform.yaml"}, wantCode: 2, wantStderr: "--platform-spec requires local foreground mode"},
		{name: "connect rejects api port local flag", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--event", "scan.requested", "--payload", payloadPath, "--api-port", "8081"}, wantCode: 2, wantStderr: "--api-port requires local foreground mode"},
		{name: "connect rejects legacy path", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1/api/rpc", "--event", "scan.requested", "--payload", payloadPath}, wantCode: 2, wantStderr: "--connect path must be empty or /v1/rpc"},
		{name: "connect rejects unsupported scheme", token: "test-token", args: []string{"run", "start", "--connect", "ftp://127.0.0.1:1", "--event", "scan.requested", "--payload", payloadPath}, wantCode: 2, wantStderr: "--connect must use http or https"},
		{name: "missing explicit token for non-loopback exits four", args: []string{"run", "start", "--connect", "http://192.0.2.10:1", "--event", "scan.requested", "--payload", payloadPath}, wantCode: 4, wantStderr: "API token source is required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.token == "" {
				setCLIAPITestToken(t, "")
			} else {
				setCLIAPITestToken(t, tc.token)
			}
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRunCommandOptions(nil))
			if code != tc.wantCode {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, tc.wantCode, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func TestRunCommandMapsRPCAndMalformedFailures(t *testing.T) {
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	for _, tc := range []struct {
		name       string
		handler    http.HandlerFunc
		wantCode   int
		wantStderr string
	}{
		{
			name: "run not found exits five",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeRunCommandJSONRPCError(t, w, req.ID, "RUN_NOT_FOUND")
			},
			wantCode:   5,
			wantStderr: "RUN_NOT_FOUND",
		},
		{
			name: "malformed response exits three",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				if req.Method == "health.check" {
					writeJSONRPCResult(t, w, req.ID, runCommandHealthResult())
					return
				}
				writeJSONRPCResult(t, w, req.ID, map[string]any{"run_id": "run-1"})
			},
			wantCode:   3,
			wantStderr: "malformed run.start result",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			server := httptest.NewServer(tc.handler)
			defer server.Close()

			var stdout, stderr bytes.Buffer
			var code int
			if tc.name == "malformed response exits three" {
				code = executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath, "--no-follow"}, &stdout, &stderr, testRunCommandOptions(server))
			} else {
				code = executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--connect", server.URL, "--reattach", "run-1"}, &stdout, &stderr, testRunCommandOptions(server))
			}
			if code != tc.wantCode {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, tc.wantCode, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

type runCommandServerOptions struct {
	rpcResponder         func(jsonRPCRequest, int) map[string]any
	wsRows               []map[string]any
	wsSubscribed         chan struct{}
	wsSubscriptionResult map[string]any
	wsCloseAfterRows     bool
	wsHTTPStatus         int
	wsHTTPBody           string
	expectedToken        string
	strictAuth           bool
}

func newRunCommandServer(t *testing.T, opts runCommandServerOptions) (*httptest.Server, *[]jsonRPCRequest, *[]jsonRPCRequest) {
	t.Helper()
	expectedToken := strings.TrimSpace(opts.expectedToken)
	if expectedToken == "" {
		expectedToken = "test-token"
	}
	var mu sync.Mutex
	rpcRequests := []jsonRPCRequest{}
	wsRequests := []jsonRPCRequest{}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/rpc":
			if got := r.Header.Get("Authorization"); got != "Bearer "+expectedToken {
				if opts.strictAuth {
					http.Error(w, "invalid bearer token", http.StatusUnauthorized)
					return
				}
				t.Errorf("Authorization = %q, want bearer token", got)
			}
			var req jsonRPCRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode request: %v", err)
			}
			mu.Lock()
			callIndex := len(rpcRequests)
			rpcRequests = append(rpcRequests, req)
			mu.Unlock()
			if opts.rpcResponder == nil {
				t.Fatalf("unexpected RPC request %q", req.Method)
			}
			writeJSONRPCResult(t, w, req.ID, opts.rpcResponder(req, callIndex))
		case "/v1/ws":
			if got := r.Header.Get("Authorization"); got != "Bearer "+expectedToken {
				if opts.strictAuth {
					http.Error(w, "invalid bearer token", http.StatusUnauthorized)
					return
				}
				t.Errorf("WS Authorization = %q, want bearer token", got)
			}
			if opts.wsHTTPStatus != 0 {
				body := opts.wsHTTPBody
				if body == "" {
					body = http.StatusText(opts.wsHTTPStatus)
				}
				http.Error(w, body, opts.wsHTTPStatus)
				return
			}
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				t.Errorf("upgrade websocket: %v", err)
				return
			}
			defer conn.Close()
			var req jsonRPCRequest
			if err := conn.ReadJSON(&req); err != nil {
				t.Errorf("read ws request: %v", err)
				return
			}
			mu.Lock()
			wsRequests = append(wsRequests, req)
			mu.Unlock()
			if req.Method != "run.subscribe_trace" {
				t.Errorf("ws method = %q, want run.subscribe_trace", req.Method)
			}
			result := opts.wsSubscriptionResult
			if result == nil {
				result = map[string]any{"subscription_id": "sub-run"}
			}
			if err := conn.WriteJSON(map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"result":  result,
			}); err != nil {
				t.Errorf("write ws subscription response: %v", err)
				return
			}
			if opts.wsSubscribed != nil {
				close(opts.wsSubscribed)
			}
			for _, row := range opts.wsRows {
				if err := conn.WriteJSON(map[string]any{
					"jsonrpc": "2.0",
					"method":  "rpc.subscription",
					"params": map[string]any{
						"subscription": "sub-run",
						"result":       row,
					},
				}); err != nil {
					return
				}
			}
			if opts.wsCloseAfterRows {
				_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				return
			}
			<-r.Context().Done()
		default:
			t.Errorf("path = %q, want /v1/rpc or /v1/ws", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	return server, &rpcRequests, &wsRequests
}

func testRunCommandOptions(server *httptest.Server) rootCommandOptions {
	opts := defaultRootCommandOptions()
	if server != nil {
		opts.apiRPCEndpointOverride = server.URL + "/v1/rpc"
		opts.httpClient = server.Client()
	}
	opts.runReadyTimeout = time.Second
	opts.runReadyPoll = time.Millisecond
	opts.runStatusPoll = time.Millisecond
	opts.runServe = func(ctx context.Context, repo string, serveOpts serveOptions) int {
		<-ctx.Done()
		return 0
	}
	return opts
}

func writeRunCommandPayloadFile(t *testing.T, payload map[string]any) string {
	t.Helper()
	path := t.TempDir() + "/payload.json"
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	return path
}

func runCommandHealthResult() map[string]any {
	return map[string]any{
		"alive":      true,
		"ready":      true,
		"db_ok":      true,
		"runtime_ok": true,
		"bundle": map[string]any{
			"workflow_name":    "review",
			"workflow_version": "1.2.3",
			"fingerprint":      "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"bundle_hash":      "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
	}
}

func validRunCommandTraceRow(eventID string) map[string]any {
	return map[string]any{
		"event_id":         eventID,
		"event_name":       "scan.requested",
		"event_created_at": "2026-05-13T10:00:01Z",
		"entity_id":        "entity-1",
		"delivery_status":  "delivered",
	}
}

func assertRunCommandMethods(t *testing.T, calls *[]jsonRPCRequest, want []string) {
	t.Helper()
	if len(*calls) != len(want) {
		t.Fatalf("methods = %v, want %v", runCommandMethodNames(*calls), want)
	}
	for i, req := range *calls {
		if req.Method != want[i] {
			t.Fatalf("method[%d] = %q, want %q; all=%v", i, req.Method, want[i], runCommandMethodNames(*calls))
		}
	}
}

func assertRunCommandTraceSubscription(t *testing.T, requests *[]jsonRPCRequest, runID string, wantReplaySince bool) {
	t.Helper()
	assertRunCommandTraceSubscriptionParams(t, requests, runID, wantReplaySince, nil)
}

func assertRunCommandTraceSubscriptionWithFilter(t *testing.T, requests *[]jsonRPCRequest, runID string, wantReplaySince bool, wantFilter map[string]any) {
	t.Helper()
	assertRunCommandTraceSubscriptionParams(t, requests, runID, wantReplaySince, wantFilter)
}

func assertRunCommandTraceSubscriptionParams(t *testing.T, requests *[]jsonRPCRequest, runID string, wantReplaySince bool, wantFilter map[string]any) {
	t.Helper()
	if len(*requests) != 1 || (*requests)[0].Method != runCommandMethodSubscribeTrace {
		t.Fatalf("ws requests = %#v, want %s", *requests, runCommandMethodSubscribeTrace)
	}
	if got := (*requests)[0].Params["run_id"]; got != runID {
		t.Fatalf("ws run_id = %#v, want %s", got, runID)
	}
	rawFilter, hasFilter := (*requests)[0].Params["filter"]
	if wantFilter == nil {
		if hasFilter {
			t.Fatalf("ws filter = %#v, want omitted", rawFilter)
		}
	} else if !reflect.DeepEqual(rawFilter, wantFilter) {
		t.Fatalf("ws filter = %#v, want %#v", rawFilter, wantFilter)
	}
	rawReplaySince, ok := (*requests)[0].Params["replay_since"]
	if !wantReplaySince {
		if ok {
			t.Fatalf("ws replay_since = %#v, want omitted", rawReplaySince)
		}
		return
	}
	replaySince, ok := rawReplaySince.(string)
	if !ok || strings.TrimSpace(replaySince) == "" {
		t.Fatalf("ws replay_since = %#v, want RFC3339Nano string", rawReplaySince)
	}
	if _, err := time.Parse(time.RFC3339Nano, replaySince); err != nil {
		t.Fatalf("ws replay_since = %q, want RFC3339Nano: %v", replaySince, err)
	}
}

func runCommandMethodNames(calls []jsonRPCRequest) []string {
	out := make([]string, 0, len(calls))
	for _, req := range calls {
		out = append(out, req.Method)
	}
	return out
}

func writeRunCommandJSONRPCError(t *testing.T, w http.ResponseWriter, id string, code string) {
	t.Helper()
	w.Header().Set("content-type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error": map[string]any{
			"code":    -32003,
			"message": "Application error: " + code,
			"data": map[string]any{
				"code":           code,
				"retryable":      false,
				"correlation_id": "corr",
				"details":        map[string]any{},
			},
		},
	}); err != nil {
		t.Fatalf("encode error: %v", err)
	}
}
