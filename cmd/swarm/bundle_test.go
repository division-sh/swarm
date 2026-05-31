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
	"sync/atomic"
	"testing"
)

func TestBundleCommandsUseCanonicalRPCAndRender(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	bundleHash := validBundleHash("a")
	var requests []jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/rpc" {
			t.Errorf("path = %q, want /v1/rpc", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("Authorization = %q, want bearer token", got)
		}
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		requests = append(requests, req)
		switch req.Method {
		case bundleListMethod:
			writeJSONRPCResult(t, w, req.ID, map[string]any{
				"bundles":     []map[string]any{validBundleSummary(bundleHash)},
				"next_cursor": "bundle-cursor-2",
			})
		case bundleGetMethod:
			writeJSONRPCResult(t, w, req.ID, validBundleDetail(bundleHash))
		case bundleAgentsMethod:
			writeJSONRPCResult(t, w, req.ID, map[string]any{
				"agents": []map[string]any{validBundleAgent("agent-alpha")},
			})
		default:
			t.Errorf("unexpected method %q", req.Method)
			writeJSONRPCResult(t, w, req.ID, map[string]any{})
		}
	}))
	defer server.Close()

	commands := []struct {
		args       []string
		wantStdout []string
	}{
		{args: []string{"bundle", "list", "--limit", "2", "--cursor", "bundle-cursor-1"}, wantStdout: []string{"bundle " + bundleHash, "agents=2", "next_cursor=bundle-cursor-2"}},
		{args: []string{"bundle", "show", bundleHash}, wantStdout: []string{"Bundle " + bundleHash, "content_yaml:", "agents:"}},
		{args: []string{"bundle", "agents", bundleHash}, wantStdout: []string{"agent agent-alpha", "role=researcher", "model=regular", "subscriptions=scan.requested"}},
	}
	for _, command := range commands {
		var stdout, stderr bytes.Buffer
		code := executeRootCommandWithOptions(context.Background(), t.TempDir(), command.args, &stdout, &stderr, testRootCommandOptions(server))
		if code != 0 {
			t.Fatalf("%v code = %d stderr=%s stdout=%s", command.args, code, stderr.String(), stdout.String())
		}
		for _, want := range command.wantStdout {
			if !strings.Contains(stdout.String(), want) {
				t.Fatalf("%v stdout missing %q:\n%s", command.args, want, stdout.String())
			}
		}
		if strings.TrimSpace(stderr.String()) != "" {
			t.Fatalf("%v stderr = %q, want empty", command.args, stderr.String())
		}
		if strings.Contains(stdout.String(), "model_tier") {
			t.Fatalf("%v stdout contains retired model_tier field:\n%s", command.args, stdout.String())
		}
	}

	if len(requests) != 3 {
		t.Fatalf("requests = %d, want 3", len(requests))
	}
	assertBundleRequest(t, requests[0], bundleListMethod, map[string]any{"limit": float64(2), "cursor": "bundle-cursor-1"})
	assertBundleRequest(t, requests[1], bundleGetMethod, map[string]any{"bundle_hash": bundleHash})
	assertBundleRequest(t, requests[2], bundleAgentsMethod, map[string]any{"bundle_hash": bundleHash})
}

func TestBundleCommandsJSONPreserveAPIShape(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	bundleHash := validBundleHash("b")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, validBundleDetail(bundleHash))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"bundle", "show", bundleHash, "--json"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	assertBundleRequest(t, captured, bundleGetMethod, map[string]any{"bundle_hash": bundleHash})
	var decoded map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &decoded); err != nil {
		t.Fatalf("decode stdout json: %v\n%s", err, stdout.String())
	}
	if decoded["bundle_hash"] != bundleHash || decoded["content_yaml"] == "" {
		t.Fatalf("json bundle detail = %#v", decoded)
	}
	if _, ok := decoded["parsed_json"].(map[string]any); !ok {
		t.Fatalf("json parsed_json = %#v, want object", decoded["parsed_json"])
	}
	for _, wrapper := range []string{"bundle", "detail"} {
		if _, ok := decoded[wrapper]; ok {
			t.Fatalf("json output contains CLI wrapper %q: %#v", wrapper, decoded)
		}
	}
}

func TestBundleAgentsJSONUsesCanonicalModelField(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	bundleHash := validBundleHash("d")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, map[string]any{
			"agents": []map[string]any{validBundleAgent("agent-alpha")},
		})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"bundle", "agents", bundleHash, "--json"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	assertBundleRequest(t, captured, bundleAgentsMethod, map[string]any{"bundle_hash": bundleHash})
	var decoded map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &decoded); err != nil {
		t.Fatalf("decode stdout json: %v\n%s", err, stdout.String())
	}
	agents, ok := decoded["agents"].([]any)
	if !ok || len(agents) != 1 {
		t.Fatalf("json agents = %#v, want one agent", decoded["agents"])
	}
	agent, ok := agents[0].(map[string]any)
	if !ok {
		t.Fatalf("json agent = %#v, want object", agents[0])
	}
	if agent["model"] != "regular" {
		t.Fatalf("json agent model = %#v, want regular; agent=%#v", agent["model"], agent)
	}
	if _, ok := agent["model_tier"]; ok {
		t.Fatalf("json agent contains retired model_tier field: %#v", agent)
	}
}

func TestBundleRegisterHelpDocumentsPreparedEnvelopeBoundary(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"bundle", "register", "--help"}, &stdout, &stderr, rootCommandOptions{})
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{
		"register <registration-envelope-yaml>",
		"--data-blob",
		"--idempotency-key",
		"--api-server",
		"--json",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("help missing %q:\n%s", want, stdout.String())
		}
	}
	for _, notWant := range []string{"contracts-directory", "archive"} {
		if strings.Contains(stdout.String(), notWant) {
			t.Fatalf("help contains unapproved packaging term %q:\n%s", notWant, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestBundleRegisterPreparedEnvelopeUsesCanonicalRPCAndRenders(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	bundleHash := validBundleHash("e")
	envelopePath := writeBundleRegisterFixture(t, "bundle-register.yaml", "api_version: swarm.bundle.register.v1\nfiles:\n  - path: package.yaml\n    text: \"name: demo\\n\"\n")
	dataBlobPath := writeBundleRegisterFixture(t, "bundle-data.json", `{"api_version":"swarm.bundle.data.v1","entries":[{"path":"flows/alpha/data/payload.bin","data_base64":"AQI="}]}`)
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/rpc" {
			t.Errorf("path = %q, want /v1/rpc", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("Authorization = %q, want bearer token", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, validBundleRegistrationResult(bundleHash))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"bundle", "register", envelopePath,
		"--data-blob", dataBlobPath,
		"--idempotency-key", "idem-register",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	assertBundleRequest(t, captured, bundleRegisterMethod, map[string]any{
		"content_yaml":    "api_version: swarm.bundle.register.v1\nfiles:\n  - path: package.yaml\n    text: \"name: demo\\n\"\n",
		"data_blob":       map[string]any{"api_version": "swarm.bundle.data.v1", "entries": []any{map[string]any{"path": "flows/alpha/data/payload.bin", "data_base64": "AQI="}}},
		"idempotency_key": "idem-register",
	})
	for _, want := range []string{"bundle " + bundleHash, "registered=true", "has_data=true", "data_size_bytes=7"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestBundleRegisterJSONPreservesAPIShape(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	bundleHash := validBundleHash("f")
	envelopePath := writeBundleRegisterFixture(t, "bundle-register.yaml", "api_version: swarm.bundle.register.v1\nfiles:\n  - path: package.yaml\n    text: \"name: demo\\n\"\n")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, validBundleRegistrationResult(bundleHash))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"bundle", "register", envelopePath, "--json"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	assertBundleRequest(t, captured, bundleRegisterMethod, map[string]any{
		"content_yaml": "api_version: swarm.bundle.register.v1\nfiles:\n  - path: package.yaml\n    text: \"name: demo\\n\"\n",
	})
	var decoded map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &decoded); err != nil {
		t.Fatalf("decode stdout json: %v\n%s", err, stdout.String())
	}
	if decoded["bundle_hash"] != bundleHash || decoded["registered"] != true || decoded["has_data"] != true || decoded["data_size_bytes"] != float64(7) {
		t.Fatalf("json registration result = %#v", decoded)
	}
}

func TestBundleCommandsRejectInvalidInputBeforeRequest(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{})
	}))
	defer server.Close()

	envelopePath := writeBundleRegisterFixture(t, "bundle-register.yaml", "api_version: swarm.bundle.register.v1\nfiles:\n  - path: package.yaml\n    text: \"name: demo\\n\"\n")
	emptyEnvelopePath := writeBundleRegisterFixture(t, "empty.yaml", " \n")
	dataBlobPath := writeBundleRegisterFixture(t, "bundle-data.json", `{"api_version":"swarm.bundle.data.v1","entries":[]}`)
	badDataBlobPath := writeBundleRegisterFixture(t, "bad-data.json", `{bad json`)
	multipleDataBlobPath := writeBundleRegisterFixture(t, "multi-data.json", `{"api_version":"swarm.bundle.data.v1","entries":[]}{}`)
	dirPath := filepath.Join(t.TempDir(), "bundle-dir")
	if err := os.Mkdir(dirPath, 0o700); err != nil {
		t.Fatalf("mkdir bundle dir: %v", err)
	}

	for _, tc := range []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{name: "list limit low", args: []string{"bundle", "list", "--limit", "0"}, wantStderr: "--limit must be between 1 and 500"},
		{name: "list limit high", args: []string{"bundle", "list", "--limit", "501"}, wantStderr: "--limit must be between 1 and 500"},
		{name: "blank cursor", args: []string{"bundle", "list", "--cursor", " "}, wantStderr: "--cursor must be non-empty"},
		{name: "show missing hash", args: []string{"bundle", "show"}, wantStderr: "accepts 1 arg(s)"},
		{name: "show invalid hash", args: []string{"bundle", "show", "sha256:abc"}, wantStderr: "bundle hash must match bundle-v1:sha256:<64 lowercase hex>"},
		{name: "agents invalid hash", args: []string{"bundle", "agents", "bad"}, wantStderr: "bundle hash must match bundle-v1:sha256:<64 lowercase hex>"},
		{name: "get alias not promoted", args: []string{"bundle", "get", validBundleHash("a")}, wantStderr: "unknown command"},
		{name: "register missing envelope", args: []string{"bundle", "register"}, wantStderr: "accepts 1 arg(s)"},
		{name: "register missing file", args: []string{"bundle", "register", filepath.Join(t.TempDir(), "missing.yaml")}, wantStderr: "read registration envelope"},
		{name: "register directory", args: []string{"bundle", "register", dirPath}, wantStderr: "registration envelope must be a file"},
		{name: "register empty envelope", args: []string{"bundle", "register", emptyEnvelopePath}, wantStderr: "registration envelope must be non-empty"},
		{name: "register missing data blob", args: []string{"bundle", "register", envelopePath, "--data-blob", filepath.Join(t.TempDir(), "missing.json")}, wantStderr: "read data blob"},
		{name: "register malformed data blob", args: []string{"bundle", "register", envelopePath, "--data-blob", badDataBlobPath}, wantStderr: "--data-blob must contain one BundleRegisterDataBlobV1 JSON object"},
		{name: "register multiple data blob documents", args: []string{"bundle", "register", envelopePath, "--data-blob", multipleDataBlobPath}, wantStderr: "--data-blob must contain one BundleRegisterDataBlobV1 JSON object"},
		{name: "register blank idempotency", args: []string{"bundle", "register", envelopePath, "--data-blob", dataBlobPath, "--idempotency-key", " "}, wantStderr: "--idempotency-key must be non-empty"},
		{name: "delete not promoted", args: []string{"bundle", "delete", validBundleHash("a")}, wantStderr: "unknown command"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls.Store(0)
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != cliExitValidation {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitValidation, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
			if calls.Load() != 0 {
				t.Fatalf("RPC calls = %d, want 0", calls.Load())
			}
		})
	}
}

func TestBundleCommandsFailClosedOnRPCAndMalformedResponses(t *testing.T) {
	bundleHash := validBundleHash("c")
	envelopePath := writeBundleRegisterFixture(t, "bundle-register.yaml", "api_version: swarm.bundle.register.v1\nfiles:\n  - path: package.yaml\n    text: \"name: demo\\n\"\n")
	for _, tc := range []struct {
		name       string
		args       []string
		handler    http.HandlerFunc
		wantCode   int
		wantStderr string
	}{
		{
			name: "bundle not found",
			args: []string{"bundle", "show", bundleHash},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeBundleJSONRPCError(t, w, req.ID, "BUNDLE_NOT_FOUND")
			},
			wantCode:   cliExitNotFound,
			wantStderr: "BUNDLE_NOT_FOUND: Application error: BUNDLE_NOT_FOUND",
		},
		{
			name: "malformed list missing bundles",
			args: []string{"bundle", "list"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{"next_cursor": "cursor-2"})
			},
			wantCode:   cliExitRuntime,
			wantStderr: "malformed bundle.list result: bundles is required",
		},
		{
			name: "malformed detail missing parsed json",
			args: []string{"bundle", "show", bundleHash},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := validBundleDetail(bundleHash)
				delete(result, "parsed_json")
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   cliExitRuntime,
			wantStderr: "malformed bundle.get result: parsed_json is required",
		},
		{
			name: "malformed agents missing agents",
			args: []string{"bundle", "agents", bundleHash},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{})
			},
			wantCode:   cliExitRuntime,
			wantStderr: "malformed bundle.agents result: agents is required",
		},
		{
			name: "bundle register conflict",
			args: []string{"bundle", "register", envelopePath},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeBundleJSONRPCError(t, w, req.ID, "BUNDLE_REGISTER_CONFLICT")
			},
			wantCode:   cliExitConflict,
			wantStderr: "BUNDLE_REGISTER_CONFLICT: Application error: BUNDLE_REGISTER_CONFLICT",
		},
		{
			name: "bundle register idempotency conflict",
			args: []string{"bundle", "register", envelopePath, "--idempotency-key", "idem-register"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeBundleJSONRPCError(t, w, req.ID, "IDEMPOTENCY_CONFLICT")
			},
			wantCode:   cliExitConflict,
			wantStderr: "IDEMPOTENCY_CONFLICT: Application error: IDEMPOTENCY_CONFLICT",
		},
		{
			name: "bundle register invalid params",
			args: []string{"bundle", "register", envelopePath},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeBundleInvalidParamsJSONRPCError(t, w, req.ID, "Invalid params: content_yaml")
			},
			wantCode:   cliExitRuntime,
			wantStderr: "Invalid params: content_yaml",
		},
		{
			name: "malformed register missing registered",
			args: []string{"bundle", "register", envelopePath},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := validBundleRegistrationResult(bundleHash)
				delete(result, "registered")
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   cliExitRuntime,
			wantStderr: "malformed bundle.register result: registered is required",
		},
		{
			name: "malformed register negative data size",
			args: []string{"bundle", "register", envelopePath},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := validBundleRegistrationResult(bundleHash)
				result["data_size_bytes"] = -1
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   cliExitRuntime,
			wantStderr: "malformed bundle.register result: data_size_bytes must be non-negative",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SWARM_API_TOKEN", "test-token")
			server := httptest.NewServer(tc.handler)
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != tc.wantCode {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, tc.wantCode, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func writeBundleRegisterFixture(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

func assertBundleRequest(t *testing.T, req jsonRPCRequest, wantMethod string, wantParams map[string]any) {
	t.Helper()
	if req.JSONRPC != "2.0" || req.Method != wantMethod {
		t.Fatalf("request jsonrpc/method = %s/%s, want 2.0/%s", req.JSONRPC, req.Method, wantMethod)
	}
	if !reflect.DeepEqual(req.Params, wantParams) {
		t.Fatalf("%s params = %#v, want %#v", wantMethod, req.Params, wantParams)
	}
}

func validBundleHash(hexDigit string) string {
	return "bundle-v1:sha256:" + strings.Repeat(hexDigit, 64)
}

func validBundleSummary(bundleHash string) map[string]any {
	return map[string]any{
		"bundle_hash":     bundleHash,
		"agent_count":     2,
		"has_data":        true,
		"data_size_bytes": 512,
		"metadata":        map[string]any{"source": "test"},
		"ingested_at":     "2026-05-31T10:00:00Z",
	}
}

func validBundleDetail(bundleHash string) map[string]any {
	result := validBundleSummary(bundleHash)
	result["content_yaml"] = "agents:\n  - id: agent-alpha\n"
	result["parsed_json"] = map[string]any{"agents": []map[string]any{{"id": "agent-alpha"}}}
	return result
}

func validBundleAgent(agentID string) map[string]any {
	return map[string]any{
		"agent_id":          agentID,
		"flow_instance":     "default",
		"role":              "researcher",
		"type":              "business",
		"model":             "regular",
		"llm_backend":       "openai_compatible",
		"conversation_mode": "task",
		"session_scope":     "run",
		"prompt_path":       "agents/researcher.md",
		"subscriptions":     []string{"scan.requested"},
		"tools":             []string{"read_file"},
	}
}

func validBundleRegistrationResult(bundleHash string) map[string]any {
	return map[string]any{
		"bundle_hash":     bundleHash,
		"registered":      true,
		"has_data":        true,
		"data_size_bytes": 7,
	}
}

func writeBundleJSONRPCError(t *testing.T, w http.ResponseWriter, id string, code string) {
	t.Helper()
	w.Header().Set("content-type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error": map[string]any{
			"code":    -32010,
			"message": "Application error: " + code,
			"data": map[string]any{
				"code":           code,
				"details":        map[string]any{"bundle_hash": "missing"},
				"retryable":      false,
				"correlation_id": "corr-bundle",
			},
		},
	}); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}

func writeBundleInvalidParamsJSONRPCError(t *testing.T, w http.ResponseWriter, id string, message string) {
	t.Helper()
	w.Header().Set("content-type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error": map[string]any{
			"code":    -32602,
			"message": message,
		},
	}); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}
