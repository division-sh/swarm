package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestBundleCommandsUseCanonicalRPCAndRender(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
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
	setCLIAPITestToken(t, "test-token")
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

func TestBundleDeleteUsesCanonicalRPCAndRenders(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	bundleHash := validBundleHash("d")
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
		force, _ := req.Params["force"].(bool)
		dryRun, _ := req.Params["dry_run"].(bool)
		status := "completed"
		deleted := true
		if dryRun {
			status = "dry_run"
			deleted = false
		}
		writeJSONRPCResult(t, w, req.ID, validBundleDeleteResult(bundleHash, force, dryRun, status, deleted))
	}))
	defer server.Close()

	commands := []struct {
		name       string
		args       []string
		wantParams map[string]any
		wantStdout []string
	}{
		{
			name:       "default",
			args:       []string{"bundle", "delete", bundleHash},
			wantParams: map[string]any{"bundle_hash": bundleHash},
			wantStdout: []string{"bundle " + bundleHash, "status=completed", "deleted=true", "force=false", "dry_run=false"},
		},
		{
			name:       "force",
			args:       []string{"bundle", "delete", bundleHash, "--force"},
			wantParams: map[string]any{"bundle_hash": bundleHash, "force": true},
			wantStdout: []string{"bundle " + bundleHash, "status=completed", "deleted=true", "force=true"},
		},
		{
			name:       "dry run idempotent",
			args:       []string{"bundle", "delete", bundleHash, "--dry-run", "--idempotency-key", "idem-delete"},
			wantParams: map[string]any{"bundle_hash": bundleHash, "dry_run": true, "idempotency_key": "idem-delete"},
			wantStdout: []string{"bundle " + bundleHash, "status=dry_run", "deleted=false", "dry_run=true"},
		},
		{
			name:       "explicit false flags",
			args:       []string{"bundle", "delete", bundleHash, "--force=false", "--dry-run=false"},
			wantParams: map[string]any{"bundle_hash": bundleHash, "force": false, "dry_run": false},
			wantStdout: []string{"bundle " + bundleHash, "status=completed", "deleted=true", "force=false", "dry_run=false"},
		},
	}
	for _, command := range commands {
		t.Run(command.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), command.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 0 {
				t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
			}
			for _, want := range command.wantStdout {
				if !strings.Contains(stdout.String(), want) {
					t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
				}
			}
			if strings.TrimSpace(stderr.String()) != "" {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
	if len(requests) != len(commands) {
		t.Fatalf("requests = %d, want %d", len(requests), len(commands))
	}
	for i, command := range commands {
		assertBundleRequest(t, requests[i], bundleDeleteMethod, command.wantParams)
	}
}

func TestBundleDeleteJSONPreservesAPIShape(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	bundleHash := validBundleHash("e")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, validBundleDeleteResult(bundleHash, true, true, "dry_run", false))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"bundle", "delete", bundleHash, "--force", "--dry-run", "--idempotency-key", "idem-delete", "--json"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	assertBundleRequest(t, captured, bundleDeleteMethod, map[string]any{
		"bundle_hash":     bundleHash,
		"force":           true,
		"dry_run":         true,
		"idempotency_key": "idem-delete",
	})
	var decoded map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &decoded); err != nil {
		t.Fatalf("decode stdout json: %v\n%s", err, stdout.String())
	}
	for _, want := range []string{"ok", "status", "operation_name", "bundle_hash", "force", "deleted", "dry_run", "plan", "cleanup", "containers", "final_mutation"} {
		if _, ok := decoded[want]; !ok {
			t.Fatalf("json output missing %q: %#v", want, decoded)
		}
	}
	if decoded["operation_name"] != bundleDeleteMethod || decoded["bundle_hash"] != bundleHash || decoded["force"] != true || decoded["dry_run"] != true || decoded["deleted"] != false {
		t.Fatalf("json delete result = %#v", decoded)
	}
	for _, wrapper := range []string{"bundle_delete", "delete_result", "result"} {
		if _, ok := decoded[wrapper]; ok {
			t.Fatalf("json output contains CLI wrapper %q: %#v", wrapper, decoded)
		}
	}
}

func TestBundleDeletePartialFailureRendersAndExitsRuntime(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	bundleHash := validBundleHash("9")
	for _, tc := range []struct {
		name string
		args []string
		json bool
	}{
		{name: "human", args: []string{"bundle", "delete", bundleHash, "--force"}},
		{name: "json", args: []string{"bundle", "delete", bundleHash, "--force", "--json"}, json: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var captured jsonRPCRequest
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
					t.Errorf("decode request: %v", err)
				}
				writeJSONRPCResult(t, w, captured.ID, partialBundleDeleteResult(bundleHash))
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != cliExitRuntime {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitRuntime, stdout.String(), stderr.String())
			}
			assertBundleRequest(t, captured, bundleDeleteMethod, map[string]any{
				"bundle_hash": bundleHash,
				"force":       true,
			})
			if !strings.Contains(stderr.String(), "bundle.delete failure: scope=managed_containers message=container stop failed") {
				t.Fatalf("stderr = %q, want partial failure details", stderr.String())
			}
			if tc.json {
				var decoded map[string]any
				if err := json.Unmarshal(stdout.Bytes(), &decoded); err != nil {
					t.Fatalf("decode stdout json: %v\n%s", err, stdout.String())
				}
				if decoded["ok"] != false || decoded["status"] != "partial_failure" || decoded["partial_failure"] != true {
					t.Fatalf("json partial result = %#v", decoded)
				}
				return
			}
			for _, want := range []string{"status=partial_failure", "deleted=false", "partial_failure=true", "errors="} {
				if !strings.Contains(stdout.String(), want) {
					t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
				}
			}
		})
	}
}

func TestBundleAgentsJSONUsesCanonicalModelField(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	bundleHash := validBundleHash("f")
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

func TestBundleDeleteHelpDocumentsCanonicalFlags(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"bundle", "delete", "--help"}, &stdout, &stderr, rootCommandOptions{})
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{
		"delete <bundle-hash>",
		"--force",
		"--dry-run",
		"--api-server",
		"--json",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("help missing %q:\n%s", want, stdout.String())
		}
	}
	for _, notWant := range []string{"archive", "contracts", "--idempotency-key string"} {
		if strings.Contains(stdout.String(), notWant) {
			t.Fatalf("help contains out-of-scope term %q:\n%s", notWant, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestBundleRegisterHelpDocumentsPreparedEnvelopeAndContractsBoundary(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"bundle", "register", "--help"}, &stdout, &stderr, rootCommandOptions{})
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{
		"register <registration-envelope-yaml>",
		"--contracts",
		"--data-blob",
		"--api-server",
		"--json",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("help missing %q:\n%s", want, stdout.String())
		}
	}
	for _, notWant := range []string{"archive", "--idempotency-key string"} {
		if strings.Contains(stdout.String(), notWant) {
			t.Fatalf("help contains unapproved packaging term %q:\n%s", notWant, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestBundleRegisterContractsDirectoryUsesCanonicalRPCAndRenders(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	bundleHash := validBundleHash("9")
	contractsDir := writeBundleRegisterContractsFixture(t)
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
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
		"bundle", "register",
		"--contracts", contractsDir,
		"--idempotency-key", "idem-contracts",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.Method != bundleRegisterMethod {
		t.Fatalf("method = %q, want %q", captured.Method, bundleRegisterMethod)
	}
	if _, ok := captured.Params["bundle_hash"]; ok {
		t.Fatalf("bundle_hash must not be sent by CLI: %#v", captured.Params)
	}
	contentYAML, ok := captured.Params["content_yaml"].(string)
	if !ok || strings.TrimSpace(contentYAML) == "" {
		t.Fatalf("content_yaml = %#v", captured.Params["content_yaml"])
	}
	var envelope bundleRegistrationEnvelopeForTest
	if err := yaml.Unmarshal([]byte(contentYAML), &envelope); err != nil {
		t.Fatalf("decode content_yaml: %v\n%s", err, contentYAML)
	}
	if envelope.APIVersion != "swarm.bundle.register.v1" {
		t.Fatalf("content_yaml api_version = %q", envelope.APIVersion)
	}
	var paths []string
	for _, file := range envelope.Files {
		paths = append(paths, file.Path)
		if strings.Contains(file.Text, "ignored") {
			t.Fatalf("ignored content leaked through %s", file.Path)
		}
	}
	wantPaths := []string{"flows/alpha/schema.yaml", "package.yaml", "prompts/root.md"}
	if !reflect.DeepEqual(paths, wantPaths) {
		t.Fatalf("content_yaml files = %#v, want %#v\n%s", paths, wantPaths, contentYAML)
	}
	wantDataBlob := map[string]any{
		"api_version": "swarm.bundle.data.v1",
		"entries": []any{
			map[string]any{
				"path":        "flows/alpha/data/empty.bin",
				"data_base64": "",
			},
			map[string]any{
				"path":        "flows/alpha/data/payload.bin",
				"data_base64": base64.StdEncoding.EncodeToString([]byte{0x01, 0x02, 0x03}),
			},
		},
	}
	if !reflect.DeepEqual(captured.Params["data_blob"], wantDataBlob) {
		t.Fatalf("data_blob = %#v, want %#v", captured.Params["data_blob"], wantDataBlob)
	}
	if captured.Params["idempotency_key"] != "idem-contracts" {
		t.Fatalf("idempotency_key = %#v", captured.Params["idempotency_key"])
	}
	for _, want := range []string{"bundle " + bundleHash, "registered=true", "has_data=true", "data_size_bytes=7"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestBundleRegisterContractsPackageFileShorthandUsesCanonicalRPC(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	bundleHash := validBundleHash("8")
	contractsDir := writeBundleRegisterContractsFixture(t)
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
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
		"bundle", "register",
		"--contracts", filepath.Join(contractsDir, "package.yaml"),
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.Method != bundleRegisterMethod {
		t.Fatalf("method = %q, want %q", captured.Method, bundleRegisterMethod)
	}
	contentYAML, ok := captured.Params["content_yaml"].(string)
	if !ok || !strings.Contains(contentYAML, "package.yaml") || !strings.Contains(contentYAML, "flows/alpha/schema.yaml") {
		t.Fatalf("content_yaml = %#v", captured.Params["content_yaml"])
	}
	if _, ok := captured.Params["data_blob"]; !ok {
		t.Fatalf("data_blob missing from params: %#v", captured.Params)
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestBundleRegisterPreparedEnvelopeUsesCanonicalRPCAndRenders(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
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
	setCLIAPITestToken(t, "test-token")
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
	setCLIAPITestToken(t, "test-token")
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
	contractsDir := writeBundleRegisterContractsFixture(t)
	invalidChildContractsDir := filepath.Join(contractsDir, "zzz-not-a-real-dir")
	invalidContractsDir := t.TempDir()
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
		{name: "delete missing hash", args: []string{"bundle", "delete"}, wantStderr: "accepts 1 arg(s)"},
		{name: "delete extra arg", args: []string{"bundle", "delete", validBundleHash("a"), "extra"}, wantStderr: "accepts 1 arg(s)"},
		{name: "delete invalid hash", args: []string{"bundle", "delete", "bad"}, wantStderr: "bundle hash must match bundle-v1:sha256:<64 lowercase hex>"},
		{name: "delete blank idempotency", args: []string{"bundle", "delete", validBundleHash("a"), "--idempotency-key", " "}, wantStderr: "--idempotency-key must be non-empty"},
		{name: "register missing envelope", args: []string{"bundle", "register"}, wantStderr: "register requires <registration-envelope-yaml> or --contracts <contracts-directory>"},
		{name: "register missing file", args: []string{"bundle", "register", filepath.Join(t.TempDir(), "missing.yaml")}, wantStderr: "read registration envelope"},
		{name: "register directory", args: []string{"bundle", "register", dirPath}, wantStderr: "registration envelope must be a file"},
		{name: "register empty envelope", args: []string{"bundle", "register", emptyEnvelopePath}, wantStderr: "registration envelope must be non-empty"},
		{name: "register missing data blob", args: []string{"bundle", "register", envelopePath, "--data-blob", filepath.Join(t.TempDir(), "missing.json")}, wantStderr: "read data blob"},
		{name: "register malformed data blob", args: []string{"bundle", "register", envelopePath, "--data-blob", badDataBlobPath}, wantStderr: "--data-blob must contain one BundleRegisterDataBlobV1 JSON object"},
		{name: "register multiple data blob documents", args: []string{"bundle", "register", envelopePath, "--data-blob", multipleDataBlobPath}, wantStderr: "--data-blob must contain one BundleRegisterDataBlobV1 JSON object"},
		{name: "register blank idempotency", args: []string{"bundle", "register", envelopePath, "--data-blob", dataBlobPath, "--idempotency-key", " "}, wantStderr: "--idempotency-key must be non-empty"},
		{name: "register contracts with envelope", args: []string{"bundle", "register", envelopePath, "--contracts", contractsDir}, wantStderr: "--contracts cannot be combined with a registration envelope argument"},
		{name: "register contracts with data blob", args: []string{"bundle", "register", "--contracts", contractsDir, "--data-blob", dataBlobPath}, wantStderr: "--data-blob cannot be used with --contracts"},
		{name: "register contracts typo child under bundle", args: []string{"bundle", "register", "--contracts", invalidChildContractsDir}, wantStderr: "resolve contracts"},
		{name: "register contracts missing package", args: []string{"bundle", "register", "--contracts", invalidContractsDir}, wantStderr: "resolve contracts"},
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

func TestBundleRegisterContractsDirectoryRejectsSymlinkBeforeRequest(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	contractsDir := writeBundleRegisterContractsFixture(t)
	if err := os.Symlink(filepath.Join(contractsDir, "prompts", "root.md"), filepath.Join(contractsDir, "prompts", "link.md")); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{"bundle", "register", "--contracts", contractsDir}, &stdout, &stderr, testRootCommandOptions(server))
	if code != cliExitValidation {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitValidation, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "symlink") {
		t.Fatalf("stderr = %q, want symlink rejection", stderr.String())
	}
	if calls.Load() != 0 {
		t.Fatalf("RPC calls = %d, want 0", calls.Load())
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
		{
			name: "bundle delete active runs conflict",
			args: []string{"bundle", "delete", bundleHash},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeBundleJSONRPCError(t, w, req.ID, "BUNDLE_HAS_ACTIVE_RUNS")
			},
			wantCode:   cliExitConflict,
			wantStderr: "BUNDLE_HAS_ACTIVE_RUNS: Application error: BUNDLE_HAS_ACTIVE_RUNS",
		},
		{
			name: "bundle delete in progress conflict",
			args: []string{"bundle", "delete", bundleHash, "--force"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeBundleJSONRPCError(t, w, req.ID, "BUNDLE_DELETE_IN_PROGRESS")
			},
			wantCode:   cliExitConflict,
			wantStderr: "BUNDLE_DELETE_IN_PROGRESS: Application error: BUNDLE_DELETE_IN_PROGRESS",
		},
		{
			name: "bundle delete idempotency conflict",
			args: []string{"bundle", "delete", bundleHash, "--idempotency-key", "idem-delete"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeBundleJSONRPCError(t, w, req.ID, "IDEMPOTENCY_CONFLICT")
			},
			wantCode:   cliExitConflict,
			wantStderr: "IDEMPOTENCY_CONFLICT: Application error: IDEMPOTENCY_CONFLICT",
		},
		{
			name: "bundle delete undeclared runtime nuke error fails closed",
			args: []string{"bundle", "delete", bundleHash, "--force"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeBundleJSONRPCError(t, w, req.ID, "RUNTIME_NUKE_IN_PROGRESS")
			},
			wantCode:   cliExitRuntime,
			wantStderr: "RUNTIME_NUKE_IN_PROGRESS: Application error: RUNTIME_NUKE_IN_PROGRESS",
		},
		{
			name: "malformed delete missing ok",
			args: []string{"bundle", "delete", bundleHash},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := validBundleDeleteResult(bundleHash, false, false, "completed", true)
				delete(result, "ok")
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   cliExitRuntime,
			wantStderr: "malformed bundle.delete result: ok is required",
		},
		{
			name: "malformed delete negative stopped count",
			args: []string{"bundle", "delete", bundleHash, "--force"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := validBundleDeleteResult(bundleHash, true, false, "completed", true)
				result["active_runs_stopped"] = -1
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   cliExitRuntime,
			wantStderr: "malformed bundle.delete result: active_runs_stopped must be non-negative",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
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

type bundleRegistrationEnvelopeForTest struct {
	APIVersion string                          `yaml:"api_version"`
	Files      []bundleRegistrationFileForTest `yaml:"files"`
}

type bundleRegistrationFileForTest struct {
	Path string `yaml:"path"`
	Text string `yaml:"text"`
}

func writeBundleRegisterContractsFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeBundleRegisterFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: cli-register-fixture
version: "1.0.0"
flows:
  - id: alpha
    flow: alpha
`)
	writeBundleRegisterFixtureFile(t, filepath.Join(root, "flows", "alpha", "schema.yaml"), `
initial_state: start
states:
  - start
  - done
`)
	writeBundleRegisterFixtureFile(t, filepath.Join(root, "prompts", "root.md"), "root prompt\n")
	writeBundleRegisterFixtureBytes(t, filepath.Join(root, "flows", "alpha", "data", "empty.bin"), nil)
	writeBundleRegisterFixtureBytes(t, filepath.Join(root, "flows", "alpha", "data", "payload.bin"), []byte{0x01, 0x02, 0x03})
	writeBundleRegisterFixtureFile(t, filepath.Join(root, ".DS_Store"), "ignored\n")
	writeBundleRegisterFixtureFile(t, filepath.Join(root, "prompts", ".#ignored.md"), "ignored\n")
	return root
}

func writeBundleRegisterFixtureFile(t *testing.T, path, content string) {
	t.Helper()
	writeBundleRegisterFixtureBytes(t, path, []byte(content))
}

func writeBundleRegisterFixtureBytes(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
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
		"agent_id":      agentID,
		"flow_instance": "default",
		"role":          "researcher",
		"type":          "business",
		"model":         "regular",
		"llm_backend":   "openai_compatible",
		"mode":          "task",
		"session_scope": "",
		"prompt_path":   "agents/researcher.md",
		"subscriptions": []string{"scan.requested"},
		"tools":         []string{"read_file"},
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

func validBundleDeleteResult(bundleHash string, force, dryRun bool, status string, deleted bool) map[string]any {
	return map[string]any{
		"ok":                   true,
		"status":               status,
		"operation_name":       bundleDeleteMethod,
		"bundle_hash":          bundleHash,
		"force":                force,
		"deleted":              deleted,
		"dry_run":              dryRun,
		"active_runs_stopped":  1,
		"deliveries_cancelled": 2,
		"containers_stopped":   3,
		"partial_failure":      false,
		"plan":                 map[string]any{"bundle_hash": bundleHash, "active_runs": []any{}, "non_active_runs": []any{}},
		"cleanup":              map[string]any{"runs": []any{}, "deliveries": []any{}},
		"containers":           map[string]any{"selected": []any{}, "stopped": []any{}},
		"final_mutation":       map[string]any{"bundle_hash": bundleHash, "deleted": deleted},
	}
}

func partialBundleDeleteResult(bundleHash string) map[string]any {
	result := validBundleDeleteResult(bundleHash, true, false, "partial_failure", false)
	result["ok"] = false
	result["partial_failure"] = true
	result["errors"] = []map[string]any{
		{"scope": "managed_containers", "message": "container stop failed"},
	}
	return result
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
