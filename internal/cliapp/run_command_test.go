package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
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

	"github.com/division-sh/swarm/internal/operatorread"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/gorilla/websocket"
)

func TestRunCommandRequiresRunningServe(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_API_TOKEN", "")
	if err := os.Unsetenv("SWARM_API_TOKEN"); err != nil {
		t.Fatal(err)
	}
	repo := t.TempDir()
	if err := os.WriteFile(filepath.Join(repo, ".env"), []byte("SWARM_API_TOKEN=must-not-load\nBROKEN\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"entity_id": "entity-1"})
	server := httptest.NewServer(http.NotFoundHandler())
	server.Close()
	for _, extra := range [][]string{nil, {"--no-follow"}, {"--connect", server.URL}} {
		t.Run(strings.Join(extra, " "), func(t *testing.T) {
			opts := testRunCommandOptions(server)
			opts.runServe = func(context.Context, InvocationRoot, ServeOptions) int {
				t.Error("run start must not launch a serve")
				return 1
			}
			args := append([]string{"run", "start", "--event", "scan.requested", "--payload", payloadPath}, extra...)
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), repo, args, &stdout, &stderr, opts)
			if code != 6 || !strings.Contains(stderr.String(), "No serve running for this deployment: start `swarm serve` first") {
				t.Fatalf("code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if stdout.Len() != 0 {
				t.Fatalf("absent serve produced a start receipt: %s", stdout.String())
			}
			if got := os.Getenv("SWARM_API_TOKEN"); got != "" {
				t.Fatalf("run start loaded credential authority from .env: %q", got)
			}
		})
	}
}

func TestRunCommandRejectsUnverifiableLocalSelectorsBeforePublication(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	setCLIAPITestToken(t, "test-token")
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		http.Error(w, "must not contact an unrelated deployment", http.StatusInternalServerError)
	}))
	defer server.Close()
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"entity_id": "entity-1"})
	for _, selector := range [][]string{
		{"other-dir"},
		{"--backend", "claude_cli"},
		{"--backend="},
		{"--mcp-port", "9000"},
		{"other-dir", "--api-port", strings.TrimPrefix(server.URL, "http://127.0.0.1:")},
	} {
		t.Run(strings.Join(selector, " "), func(t *testing.T) {
			args := append([]string{"run", "start", "--event", "scan.requested", "--payload", payloadPath, "--no-follow"}, selector...)
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, testRunCommandOptions(server))
			if code != 2 || stdout.Len() != 0 {
				t.Fatalf("code=%d stdout=%s stderr=%s", code, &stdout, &stderr)
			}
			for _, want := range []string{"local hosting is no longer supported", "--connect", "--api-port"} {
				if !strings.Contains(stderr.String(), want) {
					t.Fatalf("stderr=%q, want %q", stderr.String(), want)
				}
			}
			if got := requests.Load(); got != 0 {
				t.Fatalf("unverifiable selector contacted an unrelated deployment: %d requests", got)
			}
		})
	}
}

func TestRunCommandUsesRunningServeAPITokenFile(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		name := "serve_config_fallback"
		if explicit {
			name = "explicit_client_token_wins"
		}
		t.Run(name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			tokenFile := writeCLIAPITokenFile(t, "serve-token")
			configuredTokenFile := tokenFile
			if explicit {
				configuredTokenFile = writeCLIAPITokenFile(t, "other-config-token")
			}
			t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
				"serve_api_token_file": configuredTokenFile,
			}))
			payloadPath := writeRunCommandPayloadFile(t, map[string]any{"entity_id": "entity-1"})
			requestRead := make(chan struct{})
			server, calls, wsRequests := newRunCommandServer(t, runCommandServerOptions{
				expectedToken: "serve-token",
				strictAuth:    true,
				rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
					switch req.Method {
					case "health.check":
						return runCommandHealthResult()
					case "run.start":
						return runStartCommandResult("9d4ee136-7965-50e5-9111-821facf546b7", "running")
					case "run.get", "run.diagnose":
						awaitRunCommandTraceRequest(t, requestRead)
						run := validDiagnosticRunHeader("9d4ee136-7965-50e5-9111-821facf546b7")
						run["status"] = "completed"
						run["ended_at"] = "2026-05-13T10:01:00Z"
						return runCommandReadResult(req.Method, run)
					default:
						t.Fatalf("unexpected method = %q", req.Method)
					}
					return nil
				},
				wsRows:        []map[string]any{validRunCommandTraceRow("evt-token-file")},
				wsRequestRead: requestRead,
			})
			defer server.Close()

			opts := testRunCommandOptions(server)
			var serveCalled atomic.Int32
			opts.runServe = func(ctx context.Context, root InvocationRoot, serveOpts ServeOptions) int {
				serveCalled.Add(1)
				t.Error("run start must use the existing serve")
				return 1
			}

			var stdout, stderr bytes.Buffer
			args := []string{"run", "start", "--event", "scan.requested", "--payload", payloadPath}
			if explicit {
				args = append(args, "--api-token-file", tokenFile)
			}
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, opts)
			if code != 0 {
				t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if serveCalled.Load() != 0 {
				t.Fatalf("serve called = %d, want 0", serveCalled.Load())
			}
			assertRunCommandMethods(t, calls, []string{"health.check", "run.start", "run.diagnose"})
			assertRunCommandTraceSubscription(t, wsRequests, "9d4ee136-7965-50e5-9111-821facf546b7", true)
		})
	}
}

func awaitRunCommandTraceRequest(t *testing.T, requestRead <-chan struct{}) {
	t.Helper()
	select {
	case <-requestRead:
	case <-time.After(5 * time.Second):
		t.Error("timed out waiting for subscription request read before terminal run.get")
	}
}

func TestStartLocalRunServeConsumesPositionalSourceOwner(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()
	configContracts := filepath.Join(t.TempDir(), "config-contracts")
	if err := os.Mkdir(configContracts, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
		"api_token_file": writeCLIAPITokenFile(t, "test-token"),
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

	opts := runCommandOptions{apiOptions: testRunCommandOptions(server), apiPort: 19001, sourceRoot: configContracts}
	serveStarted := make(chan ServeOptions, 1)
	opts.apiOptions.runServe = func(ctx context.Context, root InvocationRoot, serveOpts ServeOptions) int {
		serveStarted <- serveOpts
		<-ctx.Done()
		return 0
	}

	stop, err := startLocalRunServe(context.Background(), mustInvocationRootForTest(repo), opts, io.Discard)
	if err != nil {
		t.Fatalf("startLocalRunServe: %v", err)
	}
	stop()
	serveOpts := <-serveStarted
	if want := mustInvocationRootForTest(configContracts).Path(); serveOpts.SourceRoot != want {
		t.Fatalf("contracts path = %q, want %q", serveOpts.SourceRoot, want)
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

func TestRunStartLocalServeUsesMCPListenerOwner(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()
	configContracts := filepath.Join(t.TempDir(), "config-contracts")
	if err := os.Mkdir(configContracts, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
		"api_token_file": writeCLIAPITokenFile(t, "test-token"),
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

	opts := runCommandOptions{apiOptions: testRunCommandOptions(server), mcpPort: 19002, sourceRoot: configContracts}
	serveStarted := make(chan ServeOptions, 1)
	opts.apiOptions.runServe = func(ctx context.Context, root InvocationRoot, serveOpts ServeOptions) int {
		serveStarted <- serveOpts
		<-ctx.Done()
		return 0
	}

	stop, err := startLocalRunServe(context.Background(), mustInvocationRootForTest(repo), opts, io.Discard)
	if err != nil {
		t.Fatalf("startLocalRunServe: %v", err)
	}
	stop()
	serveOpts := <-serveStarted
	if serveOpts.MCPListenAddr != "127.0.0.1:19002" {
		t.Fatalf("mcp listen addr = %q, want MCP listener owner from --mcp-port", serveOpts.MCPListenAddr)
	}
	if serveOpts.APIListenAddr != defaultAPIListenAddr {
		t.Fatalf("api listen addr = %q, want unchanged default %q", serveOpts.APIListenAddr, defaultAPIListenAddr)
	}
}

func TestRunCommandHelpShowsDataFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--help"}, &stdout, &stderr, rootCommandOptions{invocationRoot: mustInvocationRootForTest(t.TempDir())})
	if code != 0 {
		t.Fatalf("run --help code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{"--data", "Fused immutable data import", "--pin", "Exact data version pin", "--backend", "openai_responses", "--timeout", "10m0s", "0 waits indefinitely"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("run help missing %q:\n%s", want, stdout.String())
		}
	}
}

func TestRunStartInitiationRequiresEventPairOrData(t *testing.T) {
	for _, tc := range []struct {
		name      string
		opts      runCommandOptions
		wantError bool
	}{
		{"event_only", runCommandOptions{eventName: "started", payloadPath: "payload.json"}, false},
		{"feed_only_import", runCommandOptions{dataImports: []string{"records=file.jsonl"}}, false},
		{"feed_only_pin", runCommandOptions{dataPins: []string{"records@head"}}, false},
		{"event_and_feed", runCommandOptions{eventName: "started", payloadPath: "payload.json", dataPins: []string{"records@head"}}, false},
		{"missing", runCommandOptions{}, true},
		{"event_without_payload", runCommandOptions{eventName: "started"}, true},
		{"payload_without_event", runCommandOptions{payloadPath: "payload.json"}, true},
		{"negative_timeout", runCommandOptions{eventName: "started", payloadPath: "payload.json", timeout: -time.Second}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.opts.validate()
			if (err != nil) != tc.wantError {
				t.Fatalf("validate() = %v; want error %v", err, tc.wantError)
			}
		})
	}
}

func TestConnectedFeedOnlyRunStartSendsNoInitialEvent(t *testing.T) {
	for _, tc := range []struct {
		name   string
		flag   string
		value  func(string) string
		field  string
		rows   bool
		reject bool
	}{
		{"import", "--data", func(path string) string { return "records.loaded=" + path }, "imports", false, false},
		{"pin", "--pin", func(string) string { return "records.loaded@head" }, "pins", false, false},
		{"row_receipts", "--data", func(path string) string { return "records.loaded=" + path }, "imports", true, false},
		{"row_refusal", "--data", func(path string) string { return "records.loaded=" + path }, "imports", true, true},
		{"row_delivery_refusal", "--data", func(path string) string { return "records.loaded=" + path }, "imports", true, false},
		{"row_delivery_pending", "--data", func(path string) string { return "records.loaded=" + path }, "imports", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			root := t.TempDir()
			input := filepath.Join(root, "rows.jsonl")
			content := ""
			if tc.rows {
				content = "{\"id\":\"one\"}\n{\"id\":\"two\"}\n"
			}
			if err := os.WriteFile(input, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			declaration := map[string]any{"flow_path": ".", "event": "records.loaded"}
			versionID := "resource-version-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
			feedID := "00000000-0000-4000-8000-000000000001"
			var runID string
			server, calls, wsRequests := newRunCommandServer(t, runCommandServerOptions{
				rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
					switch req.Method {
					case "health.check":
						return runCommandHealthResult()
					case "data.show":
						if req.Params["view"] == "version" {
							return map[string]any{"declaration": declaration, "version_id": versionID}
						}
						return map[string]any{
							"items": []any{map[string]any{
								"declaration": declaration,
								"local_name":  "records.loaded",
								"head":        map[string]any{"state": "version", "version_id": versionID},
							}},
							"item_count":   1,
							"continuation": map[string]any{"state": "end"},
						}
					case "run.start":
						if _, found := req.Params["event_name"]; found {
							t.Fatal("feed-only run.start sent event_name")
						}
						if _, found := req.Params["payload"]; found {
							t.Fatal("feed-only run.start sent payload")
						}
						data, ok := req.Params["data"].(map[string]any)
						if req.Params["run_id"] == "" || !ok || len(data[tc.field].([]any)) != 1 {
							t.Fatalf("feed-only run.start omitted durable run identity or %s: %#v", tc.field, req.Params)
						}
						runID = req.Params["run_id"].(string)
						result := runStartCommandResult(runID, "completed")
						result["deployment_feed_ids"] = []string{feedID}
						return result
					case "event.get":
						eventID := req.Params["event_id"].(string)
						result := validEventObservationEvent(eventID)
						result["run_id"] = runID
						delivery := result["deliveries"].([]any)[0].(map[string]any)
						if tc.name != "row_delivery_refusal" || eventID == "00000000-0000-4000-8000-000000000002" {
							delivery["status"] = "delivered"
							delete(delivery, "failure")
							delete(delivery, "reason_code")
						}
						if tc.name == "row_delivery_pending" && eventID == "00000000-0000-4000-8000-000000000003" {
							delivery["status"], delivery["terminal"] = "in_progress", false
						}
						return result
					case "run.fan_out.list":
						if req.Params["run_id"] != runID || req.Params["filter"].(map[string]any)["deployment_feed_id"] != feedID {
							t.Fatalf("wrong outcome scope: %#v", req.Params)
						}
						outcomes := []fanoutobligation.Outcome{}
						if tc.rows {
							for i := 0; i < 2; i++ {
								outcomes = append(outcomes, fanoutobligation.Outcome{Ordinal: i, Kind: fanoutobligation.OutcomeCommitted, EventID: []string{"00000000-0000-4000-8000-000000000002", "00000000-0000-4000-8000-000000000003"}[i], CreatedAt: time.Now().UTC()})
							}
							if tc.reject {
								failure := runtimeengine.NormalizeFailure(&runtimeengine.EmitPayloadContractError{
									Event: "records.loaded", Kind: runtimeengine.EmitPayloadSchemaMismatch, Path: "$.id", Constraint: "type", Expected: "string", Actual: "integer",
									Detail: "invalid input row",
								}, "test", "commit")
								raw, err := runtimefailures.MarshalEnvelope(failure.Failure)
								if err != nil {
									t.Fatal(err)
								}
								outcomes[1].Kind, outcomes[1].EventID, outcomes[1].Failure = fanoutobligation.OutcomeSemanticRejected, "", raw
							}
						}
						at := time.Now().UTC()
						return map[string]any{"run_id": runID, "run_status": "completed", "observed_at": at, "order": fanoutobligation.ListIdentityOrder,
							"intents":             []fanoutobligation.IntentReadback{{Key: fanoutobligation.IntentKey{RunID: runID, DeploymentFeedID: feedID}, BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), Status: fanoutobligation.StatusClosed, DurableState: "closed", Cardinality: len(outcomes), Cursor: len(outcomes), NextChunkSize: fanoutobligation.InitialChunkSize, CreatedAt: at, UpdatedAt: at, Runtime: fanoutobligation.UnavailableRuntimeReadback()}},
							"deployment_outcomes": fanoutobligation.DeploymentOutcomePage{RunID: runID, DeploymentFeedID: feedID, Outcomes: outcomes, NextOrdinal: len(outcomes), More: new(bool)},
						}
					default:
						t.Fatalf("unexpected method %q", req.Method)
						return nil
					}
				},
			})
			defer server.Close()
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), root,
				[]string{"run", "start", "--connect", server.URL, tc.flag, tc.value(input), "--no-follow"},
				&stdout, &stderr, testRunCommandOptions(server))
			wantCode := 0
			if tc.reject || tc.name == "row_delivery_refusal" {
				wantCode = 6
			}
			if code != wantCode {
				t.Fatalf("code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			want := []string{"health.check", "data.show", "run.start"}
			if tc.name == "pin" {
				want = []string{"health.check", "data.show", "data.show", "run.start"}
			}
			want = append(want, "run.fan_out.list")
			if tc.rows {
				want = append(want, "event.get")
				if !tc.reject {
					want = append(want, "event.get")
				}
			}
			assertRunCommandMethods(t, calls, want)
			if tc.rows && (!strings.Contains(stdout.String(), "row 0 ACCEPTED") || !strings.Contains(stdout.String(), "feed published: deployment_feed_id="+feedID)) {
				t.Fatalf("missing row/receipt readout: %s", stdout.String())
			}
			if (tc.reject || tc.name == "row_delivery_refusal") && !strings.Contains(stdout.String(), "row 1 REFUSED") {
				t.Fatalf("missing row refusal: %s", stdout.String())
			}
			if tc.name == "row_delivery_refusal" && !strings.Contains(stdout.String(), "review/instance-1") {
				t.Fatalf("missing refused instance: %s", stdout.String())
			}
			if tc.name == "row_delivery_pending" && strings.Contains(stdout.String(), "row 1") {
				t.Fatalf("pending delivery was assigned a final outcome: %s", stdout.String())
			}
			if strings.Contains(stdout.String(), "created") || tc.name == "row_receipts" && strings.Contains(stdout.String(), "review/instance-1") {
				t.Fatalf("readout claimed creation or named an accepted instance: %s", stdout.String())
			}
			if len(*wsRequests) != 0 {
				t.Fatalf("feed-only --no-follow opened websocket: %#v", *wsRequests)
			}
		})
	}
}

func TestRunCommandNoFollowUsesHealthAndRunStartOnly(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	root := t.TempDir()
	payloadPath := filepath.Join(root, "payload.json")
	payload, err := json.Marshal(map[string]any{"ok": true})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(payloadPath, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	server, calls, wsRequests := newRunCommandServer(t, runCommandServerOptions{
		rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
			switch req.Method {
			case "health.check":
				return runCommandHealthResult()
			case "run.start":
				if got := req.Params["bundle_hash"]; got != "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
					t.Fatalf("bundle_hash = %#v, want health.check bundle hash", got)
				}
				result := runStartCommandResult("9b84a4a3-41c0-5d3a-8ce8-e66c2afb4355", "running")
				result["event_id"] = "00000000-0000-4000-8000-000000000003"
				return result
			case "event.get":
				result := validEventObservationEvent("00000000-0000-4000-8000-000000000003")
				result["run_id"] = "9b84a4a3-41c0-5d3a-8ce8-e66c2afb4355"
				delivery := result["deliveries"].([]any)[0].(map[string]any)
				delivery["status"] = "delivered"
				delete(delivery, "failure")
				return result
			default:
				t.Fatalf("unexpected method = %q", req.Method)
			}
			return nil
		},
	})
	defer server.Close()

	chdirForTest(t, t.TempDir())
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), root, []string{"run", "start", "--event", "scan.requested", "--payload", filepath.Base(payloadPath), "--no-follow"}, &stdout, &stderr, testRunCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	assertRunCommandMethods(t, calls, []string{"health.check", "run.start", "event.get"})
	if len(*wsRequests) != 0 {
		t.Fatalf("websocket requests = %d, want 0", len(*wsRequests))
	}
	for _, want := range []string{"run started: run_id=9b84a4a3-41c0-5d3a-8ce8-e66c2afb4355", "input published: event_id=00000000-0000-4000-8000-000000000003", "reattach: swarm run start --connect " + server.URL + " --reattach 9b84a4a3-41c0-5d3a-8ce8-e66c2afb4355"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
}

func TestRunStartRemoteModesDoNotAcquireLocalOwnership(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	server, calls, _ := newRunCommandServer(t, runCommandServerOptions{
		rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
			switch req.Method {
			case "health.check":
				return runCommandHealthResult()
			case "run.start":
				return runStartCommandResult("7d4fb15d-b64e-578e-8769-d215bd7ca0ab", "running")
			case "run.get", "run.diagnose":
				run := validDiagnosticRunHeader("7d4fb15d-b64e-578e-8769-d215bd7ca0ab")
				run["status"] = "completed"
				run["ended_at"] = "2026-05-13T10:01:00Z"
				return runCommandReadResult(req.Method, run)
			default:
				t.Fatalf("unexpected method = %q", req.Method)
			}
			return nil
		},
	})
	defer server.Close()
	opts := testRunCommandOptions(server)
	opts.runServe = func(context.Context, InvocationRoot, ServeOptions) int {
		t.Fatal("remote run mode attempted to acquire local serve ownership")
		return 1
	}

	for _, test := range []struct {
		name string
		args []string
	}{
		{name: "connected start", args: []string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath, "--no-follow"}},
		{name: "reattach", args: []string{"run", "start", "--connect", server.URL, "--reattach", "7d4fb15d-b64e-578e-8769-d215bd7ca0ab"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := executeRootCommandWithOptions(context.Background(), t.TempDir(), test.args, &stdout, &stderr, opts); code != 0 {
				t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
		})
	}
	methods := runCommandMethodNames(*calls)
	if !reflect.DeepEqual(methods, []string{"health.check", "run.start", "run.get"}) {
		t.Fatalf("methods = %v, want remote API-only execution", methods)
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
				if got := req.Params["run_id"]; got != "310857e1-51d0-5d06-bce8-25778a85b448" {
					t.Fatalf("run_id = %#v, want 310857e1-51d0-5d06-bce8-25778a85b448", got)
				}
				if got := req.Params["idempotency_key"]; got != "idem-start" {
					t.Fatalf("idempotency_key = %#v, want idem-start", got)
				}
				return runStartCommandResult("310857e1-51d0-5d06-bce8-25778a85b448", "running")
			default:
				t.Fatalf("unexpected method = %q", req.Method)
			}
			return nil
		},
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath, "--run-id", "310857e1-51d0-5d06-bce8-25778a85b448", "--idempotency-key", "idem-start", "--no-follow"}, &stdout, &stderr, testRunCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	assertRunCommandMethods(t, calls, []string{"health.check", "run.start"})
}

func TestRunCommandRetiredBundleFlagFailsBeforeRPC(t *testing.T) {
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--connect", "http://127.0.0.1:1", "--event", "scan.requested", "--payload", payloadPath, retiredBundleIdentityFlag(), "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "--no-follow"}, &stdout, &stderr, testRunCommandOptions(nil))
	if code != 2 {
		t.Fatalf("code = %d, want 2 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "unknown flag") {
		t.Fatalf("stderr = %q, want unknown flag", stderr.String())
	}
}

func TestRunCommandServedSourceSerializesExactIdentityAndMapsUnsupported(t *testing.T) {
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
			if got := req.Params["bundle_hash"]; got != "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
				t.Fatalf("bundle_hash = %#v", got)
			}
			writeRunCommandJSONRPCError(t, w, req.ID, "UNSUPPORTED_BUNDLE_HASH")
		default:
			t.Fatalf("unexpected method = %q", req.Method)
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath, "--no-follow"}, &stdout, &stderr, testRunCommandOptions(server))
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
	for _, codeName := range []string{"BUNDLE_SCOPE_REQUIRED", "BUNDLE_UNAVAILABLE", "BUNDLE_DATA_INTEGRITY_ERROR", "BUNDLE_MISMATCH", "EVENT_NOT_DECLARED", "PAYLOAD_VALIDATION_FAILED", "EVENT_PUBLISH_FAILED", "RUN_NEVER_COMPLETES"} {
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

func TestRunCommandStartRendersRootInputRejectionFromServerFacts(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	var calls []jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/ws" {
			t.Fatal("run.subscribe_trace must not be opened after failed run.start")
		}
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		calls = append(calls, req)
		switch req.Method {
		case "health.check":
			writeJSONRPCResult(t, w, req.ID, runCommandHealthResult())
		case "run.start":
			w.Header().Set("content-type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"error": map[string]any{
					"code":    -32003,
					"message": "Application error: EVENT_NOT_DECLARED",
					"data": map[string]any{
						"code":      "EVENT_NOT_DECLARED",
						"retryable": false,
						"details": map[string]any{
							"event_name":      "scan.missing",
							"reason":          "not_declared_root_input",
							"declared_events": []string{"scan.requested"},
							"routable_events": []string{"scan.requested"},
						},
					},
				},
			}); err != nil {
				t.Fatalf("encode response: %v", err)
			}
		default:
			t.Fatalf("unexpected method = %q", req.Method)
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"run", "start", "--connect", server.URL, "--event", "scan.missing", "--payload", payloadPath, "--no-follow",
	}, &stdout, &stderr, testRunCommandOptions(server))
	if code != 6 {
		t.Fatalf("code = %d, want 6 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	assertRunCommandMethods(t, &calls, []string{"health.check", "run.start"})
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	for _, want := range []string{
		`ERROR: event "scan.missing" is not a declared root input.`,
		"A root input is an event declared in the root flow's `pins.inputs`.",
		"Declared root inputs: scan.requested.",
		"Routable root inputs: scan.requested.",
		"Remediation:",
		"Code: EVENT_NOT_DECLARED",
	} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr = %q, want substring %q", stderr.String(), want)
		}
	}
}

func TestRunCommandConnectedForegroundReturnsOnRunQuiescence(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	tracePrinted := make(chan struct{})
	server, calls, wsRequests := newRunCommandServer(t, runCommandServerOptions{
		rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
			switch req.Method {
			case "health.check":
				return runCommandHealthResult()
			case "run.start":
				return runStartCommandResult("09c3d378-ceed-5289-b4b3-028de530bb72", "running")
			case "run.get", "run.diagnose":
				run := validDiagnosticRunHeader("09c3d378-ceed-5289-b4b3-028de530bb72")
				result := runCommandReadResult(req.Method, run)
				result["next_wake_at"] = "2030-01-01T01:00:00Z"
				result["instances"] = []operatorread.RunInstanceOperationalStatus{
					{FlowInstance: "unrelated/one", Stage: "waiting", Revision: 1, Disposition: "waiting"},
					{FlowInstance: "unrelated/two", Stage: "waiting", Revision: 1, Disposition: "waiting"},
				}
				work := result["test_quiescence"].(map[string]any)
				select {
				case <-tracePrinted:
					work["ready"] = true
					work["active_deliveries"] = 0
				default:
				}
				return result
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
	code := executeRootCommandWithOptions(ctx, t.TempDir(), []string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath, "--timeout", "0"}, stdout, &stderr, testRunCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	methods := runCommandMethodNames(*calls)
	if len(methods) < 3 || methods[0] != "health.check" || methods[1] != "run.start" {
		t.Fatalf("methods = %v, want health.check, run.start, then run.diagnose", methods)
	}
	for i, method := range methods[2:] {
		if method != "run.diagnose" {
			t.Fatalf("method[%d] = %q, want run.diagnose; all=%v", i+2, method, methods)
		}
	}
	assertRunCommandTraceSubscription(t, wsRequests, "09c3d378-ceed-5289-b4b3-028de530bb72", true)
	for _, want := range []string{"id=evt-foreground", "delivery=in progress", "run settled: run_id=09c3d378-ceed-5289-b4b3-028de530bb72", "2 waiting (waiting) | 0 live", "next wake 2030-01-01T01:00:00Z"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.Contains(stdout.String(), "unrelated/") {
		t.Fatalf("run-wide readout named unrelated instances: %s", stdout.String())
	}
	t.Run("timeout_detaches_without_cancelling_work", func(t *testing.T) {
		server, calls, _ := newRunCommandServer(t, runCommandServerOptions{
			rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
				switch req.Method {
				case "health.check":
					return runCommandHealthResult()
				case "run.start":
					return runStartCommandResult("7e3709ac-9ed5-5d2d-afb2-da8ed843f2ab", "running")
				case "run.diagnose":
					result := runCommandReadResult(req.Method, validDiagnosticRunHeader("7e3709ac-9ed5-5d2d-afb2-da8ed843f2ab"))
					result["next_wake_at"] = "2030-01-01T01:00:00Z"
					result["instances"] = []operatorread.RunInstanceOperationalStatus{
						{FlowInstance: "unrelated/active", Stage: "processing", Revision: 1, Disposition: "active", Work: operatorread.RunInstanceWork{Deliveries: 1}},
					}
					return result
				default:
					t.Errorf("timeout must not mutate work; unexpected method %s", req.Method)
					return nil
				}
			},
		})
		defer server.Close()
		opts := testRunCommandOptions(server)
		opts.runStatusPoll = time.Hour
		var stdout, stderr bytes.Buffer
		code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
			"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath, "--timeout", "10ms",
		}, &stdout, &stderr, opts)
		if code != 8 || !strings.Contains(stderr.String(), "swarm run status 7e3709ac-9ed5-5d2d-afb2-da8ed843f2ab") || !strings.Contains(stderr.String(), "work continues") {
			t.Fatalf("code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
		}
		if !strings.Contains(stdout.String(), "1 processing (active) | 1 live") || strings.Contains(stdout.String(), "unrelated/active") {
			t.Fatalf("timeout lost run-wide counts or named another invocation's instance: %s", stdout.String())
		}
		if !strings.Contains(stdout.String(), "next wake 2030-01-01T01:00:00Z") {
			t.Fatalf("timeout lost the canonical future wake: %s", stdout.String())
		}
		assertRunCommandMethods(t, calls, []string{"health.check", "run.start", "run.diagnose"})
	})
	t.Run("continuation_keeps_resolved_port_and_auth", func(t *testing.T) {
		const token = "private-continuation-token"
		tokenFile := writeCLIAPITokenFile(t, token)
		config := writeCLIAPIConfigFile(t, map[string]string{"serve_api_token_file": tokenFile})
		const runID = "7e3709ac-9ed5-5d2d-afb2-da8ed843f2ab"
		server, _, _ := newRunCommandServer(t, runCommandServerOptions{
			expectedToken: token, strictAuth: true,
			rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
				switch req.Method {
				case "health.check":
					return runCommandHealthResult()
				case "run.start":
					return runStartCommandResult(runID, "running")
				case "run.get", "run.diagnose":
					run := validDiagnosticRunHeader(runID)
					run["status"], run["ended_at"] = "completed", "2026-05-13T10:01:00Z"
					return runCommandReadResult(req.Method, run)
				default:
					t.Errorf("unexpected method: %s", req.Method)
					return nil
				}
			},
		})
		defer server.Close()
		port := server.URL[strings.LastIndex(server.URL, ":")+1:]
		args := []string{"run", "start", "--api-port", port, "--config", config, "--event", "scan.requested", "--payload", payloadPath}
		for _, mode := range []string{"--timeout", "--no-follow"} {
			var stdout, stderr bytes.Buffer
			command := append([]string{}, args...)
			command = append(command, mode)
			wantCode, marker := 0, "reattach: "
			if mode == "--timeout" {
				command = append(command, "1ns")
				wantCode, marker = 8, "Check: "
			}
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), command, &stdout, &stderr, defaultRootCommandOptions())
			text := stdout.String() + stderr.String()
			if code != wantCode || strings.Contains(text, token) {
				t.Fatalf("code=%d, leaked token=%t", code, strings.Contains(text, token))
			}
			at := strings.Index(text, marker)
			if at < 0 {
				t.Fatalf("missing continuation: %s", text)
			}
			next := strings.SplitN(text[at+len(marker):], "\n", 2)[0]
			if !strings.Contains(next, server.URL) || !strings.Contains(next, tokenFile) || !strings.Contains(next, config) {
				t.Fatalf("continuation lost target/auth: %s", next)
			}
			stdout.Reset()
			stderr.Reset()
			code = executeRootCommandWithOptions(context.Background(), t.TempDir(), strings.Fields(next)[1:], &stdout, &stderr, defaultRootCommandOptions())
			if code != 0 {
				t.Fatalf("continuation code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
		}
	})
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
			run := validDiagnosticRunHeader("5b767b3b-aaf2-52f3-9581-0427cb7ee620")
			run["status"] = "failed"
			run["ended_at"] = "2026-05-13T10:01:00Z"
			run["failure"] = testRuntimeFailureClass(runtimefailures.ClassInternalFailure, "workflow_failed")
			return runCommandReadResult(req.Method, run)
		},
	})
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--connect", server.URL, "--reattach", "5b767b3b-aaf2-52f3-9581-0427cb7ee620"}, &stdout, &stderr, testRunCommandOptions(server))
	if code != 7 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	assertRunCommandMethods(t, calls, []string{"run.get"})
	if len(*wsRequests) != 0 {
		t.Fatalf("websocket requests = %d, want 0", len(*wsRequests))
	}
	if !strings.Contains(stdout.String(), "run terminal: run_id=5b767b3b-aaf2-52f3-9581-0427cb7ee620 status=failed") || !strings.Contains(stdout.String(), "platform.internal_failure/workflow_failed") {
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
			return map[string]any{"run": validDiagnosticRunHeader("0f53ca25-54c3-5dfb-81d3-aa0ba702d418")}
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
	code := executeRootCommandWithOptions(ctx, t.TempDir(), []string{"run", "start", "--connect", server.URL, "--reattach", "0f53ca25-54c3-5dfb-81d3-aa0ba702d418"}, &stdout, &stderr, opts)
	if code != 130 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	assertRunCommandMethods(t, calls, []string{"run.get"})
	assertRunCommandTraceSubscription(t, wsRequests, "0f53ca25-54c3-5dfb-81d3-aa0ba702d418", false)
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
				return runStartCommandResult("922697b4-366d-515c-976b-cd77b98f7ecd", "running")
			case "run.diagnose":
				return runCommandReadResult(req.Method, validDiagnosticRunHeader("922697b4-366d-515c-976b-cd77b98f7ecd"))
			case "run.stop":
				if got := req.Params["run_id"]; got != "922697b4-366d-515c-976b-cd77b98f7ecd" {
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
	wantMethods := []string{"health.check", "run.start", "run.stop"}
	if len(*calls) == 4 {
		wantMethods = []string{"health.check", "run.start", "run.diagnose", "run.stop"}
	}
	assertRunCommandMethods(t, calls, wantMethods)
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

	var serveCalled atomic.Bool
	opts := testRunCommandOptions(server)
	opts.runReadyTimeout = time.Minute
	opts.runReadyPoll = time.Millisecond
	opts.runServe = func(ctx context.Context, root InvocationRoot, serveOpts ServeOptions) int {
		serveCalled.Store(true)
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--event", "scan.requested", "--payload", payloadPath}, &stdout, &stderr, opts)
	if code != 4 {
		t.Fatalf("code = %d, want 4 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if serveCalled.Load() {
		t.Fatal("an authentication refusal must not launch a local serve")
	}
	if !strings.Contains(stderr.String(), "rejected the request with status 401") {
		t.Fatalf("stderr = %q, want auth failure", stderr.String())
	}
}

func TestRunCommandClosedTraceChannelStillWaitsForRunGet(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	warning := &notifyingBuffer{needle: string(runTraceDetachStreamClosed), notify: make(chan struct{})}
	server, calls, _ := newRunCommandServer(t, runCommandServerOptions{
		wsCloseAfterRows: true,
		rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
			switch req.Method {
			case "health.check":
				return runCommandHealthResult()
			case "run.start":
				return runStartCommandResult("8895c771-b861-5ee7-80d6-097af0895c0a", "running")
			case "run.get", "run.diagnose":
				run := validDiagnosticRunHeader("8895c771-b861-5ee7-80d6-097af0895c0a")
				select {
				case <-warning.notify:
					run["status"] = "completed"
					run["ended_at"] = "2026-05-13T10:01:00Z"
				default:
				}
				return runCommandReadResult(req.Method, run)
			default:
				t.Fatalf("unexpected method = %q", req.Method)
			}
			return nil
		},
	})
	defer server.Close()

	var stdout bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath}, &stdout, warning, testRunCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), warning.String())
	}
	methods := runCommandMethodNames(*calls)
	if len(methods) < 3 || methods[0] != "health.check" || methods[1] != "run.start" || methods[len(methods)-1] != "run.diagnose" {
		t.Fatalf("methods = %v", methods)
	}
	if !strings.Contains(stdout.String(), "run terminal: run_id=8895c771-b861-5ee7-80d6-097af0895c0a status=completed") {
		t.Fatalf("stdout = %q", stdout.String())
	}
	fact := requireSingleRunTraceDetachFact(t, warning.String())
	assertRunTraceDetachFact(t, fact, runTraceDetachStreamClosed, "8895c771-b861-5ee7-80d6-097af0895c0a", "swarm run start --connect "+server.URL+" --reattach 8895c771-b861-5ee7-80d6-097af0895c0a")
}

func TestRunCommandMalformedWebSocketFailuresDetachAndUseTerminalStatus(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	for _, tc := range []struct {
		name       string
		serverOpts runCommandServerOptions
		wantReason runTraceDetachReason
	}{
		{
			name: "subscription response missing id",
			serverOpts: runCommandServerOptions{
				wsSubscriptionResult: map[string]any{},
			},
			wantReason: runTraceDetachSubscriptionResponseInvalid,
		},
		{
			name: "notification missing event id",
			serverOpts: runCommandServerOptions{
				wsRows: []map[string]any{{
					"event_name":       "scan.requested",
					"event_created_at": "2026-05-13T10:00:01Z",
				}},
			},
			wantReason: runTraceDetachTraceRowInvalid,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			warning := &notifyingBuffer{needle: string(tc.wantReason), notify: make(chan struct{})}
			tc.serverOpts.rpcResponder = observerTerminalAfterWarningResponder(t, "2483d957-8a36-5465-9e77-e7de976898b6", warning.notify)
			server, _, _ := newRunCommandServer(t, tc.serverOpts)
			defer server.Close()

			var stdout bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath}, &stdout, warning, testRunCommandOptions(server))
			if code != 0 {
				t.Fatalf("code = %d, want 0 stdout=%s stderr=%s", code, stdout.String(), warning.String())
			}
			fact := requireSingleRunTraceDetachFact(t, warning.String())
			assertRunTraceDetachFact(t, fact, tc.wantReason, "2483d957-8a36-5465-9e77-e7de976898b6", "swarm run start --connect "+server.URL+" --reattach 2483d957-8a36-5465-9e77-e7de976898b6")
		})
	}
}

func TestRunCommandWebSocketHandshakeHTTPErrorDetachesObserver(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	warning := &notifyingBuffer{needle: string(runTraceDetachAttachFailed), notify: make(chan struct{})}
	server, _, _ := newRunCommandServer(t, runCommandServerOptions{
		rpcResponder: observerTerminalAfterWarningResponder(t, "390fe346-7ec6-58f0-a6a8-5f85cf7e384e", warning.notify),
		wsHTTPStatus: http.StatusUnauthorized,
		wsHTTPBody:   "invalid bearer token",
	})
	defer server.Close()

	var stdout bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath}, &stdout, warning, testRunCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d, want 0 stdout=%s stderr=%s", code, stdout.String(), warning.String())
	}
	fact := requireSingleRunTraceDetachFact(t, warning.String())
	assertRunTraceDetachFact(t, fact, runTraceDetachAttachFailed, "390fe346-7ec6-58f0-a6a8-5f85cf7e384e", "swarm run start --connect "+server.URL+" --reattach 390fe346-7ec6-58f0-a6a8-5f85cf7e384e")
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
		{name: "unknown detach", token: "test-token", args: []string{"run", "start", "--detach", "--event", "scan.requested", "--payload", payloadPath}, wantCode: 2, wantStderr: "unknown flag: --detach"},
		{name: "no follow reattach rejected", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--reattach", "run-1", "--no-follow"}, wantCode: 2, wantStderr: "--no-follow and --reattach are mutually exclusive"},
		{name: "invalid retired bundle hash rejected", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--event", "scan.requested", "--payload", payloadPath, "--bundle-hash", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, wantCode: 2, wantStderr: "unknown flag"},
		{name: "blank retired bundle hash rejected", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--event", "scan.requested", "--payload", payloadPath, "--bundle-hash", "  "}, wantCode: 2, wantStderr: "unknown flag"},
		{name: "retired bundle flag rejected", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--event", "scan.requested", "--payload", payloadPath, retiredBundleIdentityFlag(), "sha256:BAD"}, wantCode: 2, wantStderr: "unknown flag"},
		{name: "reattach rejects bundle hash", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--reattach", "run-1", "--bundle-hash", "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, wantCode: 2, wantStderr: "unknown flag: --bundle-hash"},
		{name: "reattach refuses missing config before effects", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--reattach", "run-1", "--config", "swarm.yaml"}, wantCode: 2, wantStderr: "read swarm.yaml config"},
		{name: "reattach rejects backend flag", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--reattach", "run-1", "--backend", "claude_cli"}, wantCode: 2, wantStderr: "--reattach is mutually exclusive with --backend"},
		{name: "reattach rejects local source", token: "test-token", args: []string{"run", "start", "contracts", "--connect", "http://127.0.0.1:1", "--reattach", "run-1"}, wantCode: 2, wantStderr: "a source directory cannot be used with --reattach"},
		{name: "reattach rejects data flag", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--reattach", "run-1", "--data", "reference-data"}, wantCode: 2, wantStderr: "--reattach is mutually exclusive with --data"},
		{name: "platform spec retired on reattach", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--reattach", "run-1", "--platform-spec", "platform.yaml"}, wantCode: 2, wantStderr: "unknown flag: --platform-spec"},
		{name: "reattach rejects api port flag", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--reattach", "run-1", "--api-port", "8081"}, wantCode: 2, wantStderr: "--reattach is mutually exclusive with --api-port"},
		{name: "blank data rejected", token: "test-token", args: []string{"run", "start", "--event", "scan.requested", "--payload", payloadPath, "--data", " "}, wantCode: 2, wantStderr: "--data must be name=file.jsonl"},
		{name: "api port zero rejected when explicit", token: "test-token", args: []string{"run", "start", "--event", "scan.requested", "--payload", payloadPath, "--api-port", "0"}, wantCode: 2, wantStderr: "--api-port must be between 1 and 65535"},
		{name: "api port rejects default mcp listener conflict", token: "test-token", args: []string{"run", "start", "--event", "scan.requested", "--payload", payloadPath, "--api-port", "8082"}, wantCode: 2, wantStderr: "--api-port 8082 conflicts with MCP listener 127.0.0.1:8082"},
		{name: "mcp port zero rejected when explicit", token: "test-token", args: []string{"run", "start", "--event", "scan.requested", "--payload", payloadPath, "--mcp-port", "0"}, wantCode: 2, wantStderr: "--mcp-port must be between 1 and 65535"},
		{name: "api port rejects custom mcp listener conflict", token: "test-token", args: []string{"run", "start", "--event", "scan.requested", "--payload", payloadPath, "--api-port", "9000", "--mcp-port", "9000"}, wantCode: 2, wantStderr: "--api-port 9000 conflicts with MCP listener 127.0.0.1:9000"},
		{name: "connect refuses missing config before effects", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--event", "scan.requested", "--payload", payloadPath, "--config", "swarm.yaml"}, wantCode: 2, wantStderr: "read swarm.yaml config"},
		{name: "connect rejects backend local flag", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--event", "scan.requested", "--payload", payloadPath, "--backend", "claude_cli"}, wantCode: 2, wantStderr: "--backend requires local foreground mode"},
		{name: "connect rejects local source", token: "test-token", args: []string{"run", "start", "contracts", "--connect", "http://127.0.0.1:1", "--event", "scan.requested", "--payload", payloadPath}, wantCode: 2, wantStderr: "a source directory requires local foreground mode"},
		{name: "platform spec retired on connect", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--event", "scan.requested", "--payload", payloadPath, "--platform-spec", "platform.yaml"}, wantCode: 2, wantStderr: "unknown flag: --platform-spec"},
		{name: "connect rejects api port local flag", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--event", "scan.requested", "--payload", payloadPath, "--api-port", "8081"}, wantCode: 2, wantStderr: "--api-port requires local foreground mode"},
		{name: "connect rejects mcp port local flag", token: "test-token", args: []string{"run", "start", "--connect", "http://127.0.0.1:1", "--event", "scan.requested", "--payload", payloadPath, "--mcp-port", "9000"}, wantCode: 2, wantStderr: "--mcp-port requires local foreground mode"},
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
	wsRequestRead        chan struct{}
	wsClosed             chan struct{}
	wsSubscriptionResult map[string]any
	wsSubscriptionError  *jsonRPCError
	wsCloseAfterRows     bool
	wsHoldResponse       bool
	wsAbruptClose        bool
	wsNotificationMethod string
	wsNotificationID     string
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
			if opts.wsClosed != nil {
				defer close(opts.wsClosed)
			}
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
			if opts.wsRequestRead != nil {
				close(opts.wsRequestRead)
			}
			if opts.wsHoldResponse {
				_, _, _ = conn.ReadMessage()
				return
			}
			response := map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
			}
			if opts.wsSubscriptionError != nil {
				response["error"] = opts.wsSubscriptionError
			} else {
				result := opts.wsSubscriptionResult
				if result == nil {
					result = map[string]any{"subscription_id": "sub-run"}
				}
				response["result"] = result
			}
			if err := conn.WriteJSON(response); err != nil {
				t.Errorf("write ws subscription response: %v", err)
				return
			}
			if opts.wsSubscribed != nil {
				close(opts.wsSubscribed)
			}
			if opts.wsAbruptClose {
				_ = conn.UnderlyingConn().Close()
				return
			}
			for _, row := range opts.wsRows {
				method := opts.wsNotificationMethod
				if method == "" {
					method = "rpc.subscription"
				}
				subscriptionID := opts.wsNotificationID
				if subscriptionID == "" {
					subscriptionID = "sub-run"
				}
				if err := conn.WriteJSON(map[string]any{
					"jsonrpc": "2.0",
					"method":  method,
					"params": map[string]any{
						"subscription": subscriptionID,
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
			_, _, _ = conn.ReadMessage()
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
	opts.runServe = func(ctx context.Context, root InvocationRoot, serveOpts ServeOptions) int {
		<-ctx.Done()
		return 0
	}
	return opts
}

func runCommandReadResult(method string, run map[string]any) map[string]any {
	if method != "run.diagnose" {
		return map[string]any{"run": run}
	}
	result := validDiagnosticRunDiagnosis(run["run_id"].(string), run["status"].(string), "none", "none", []any{})
	result["run"] = run
	if run["status"] == "running" {
		work := result["test_quiescence"].(map[string]any)
		work["ready"] = false
		work["active_deliveries"] = 1
	}
	return result
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
			"bundle_hash":      "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
	}
}

func runStartCommandResult(runID, status string) map[string]any {
	return map[string]any{"run_id": runID, "status": status, "data_binding": map[string]any{"state": "none"}}
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
