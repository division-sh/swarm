package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

func TestEventPublishUsesEventPublishV1RPCWithBoundParams(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
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
		writeJSONRPCResult(t, w, captured.ID, eventPublishTestResult(true))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"event", "publish", "scan.requested",
		"--payload-json", `{"topic":"sample","count":2}`,
		"--run-id", "run-1",
		"--source-event-id", "event-parent-1",
		"--bundle-fingerprint", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"--emitter", "cli:test",
		"--idempotency-key", "idem-1",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.JSONRPC != "2.0" || captured.Method != eventPublishMethod {
		t.Fatalf("request jsonrpc/method = %s/%s, want 2.0/%s", captured.JSONRPC, captured.Method, eventPublishMethod)
	}
	wantParams := map[string]any{
		"event_name": "scan.requested",
		"payload": map[string]any{
			"topic": "sample",
			"count": float64(2),
		},
		"run_id":          "run-1",
		"source_event_id": "event-parent-1",
		"bundle_ref":      map[string]any{"fingerprint": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		"emitter":         "cli:test",
		"idempotency_key": "idem-1",
	}
	if !reflect.DeepEqual(captured.Params, wantParams) {
		t.Fatalf("params = %#v, want %#v", captured.Params, wantParams)
	}
	for _, want := range []string{
		"event publish ok:",
		"event_id=event-1",
		"event_name=scan.requested",
		"run_id=run-1",
		"new_run_created=true",
		"deliveries=1",
		"delivery subscriber_id=agent-1",
		"session_id=session-1",
		"attempt=2",
		"source_event_id=event-parent-1",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestEventPublishPassesFlowScopedEventNameToV1RPC(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, eventPublishTestResult(true))
	}))
	defer server.Close()

	const eventName = "repo-scaffold/repo_scaffold.repo_commit_succeeded"
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"event", "publish", eventName,
		"--payload-json", `{"topic":"sample"}`,
		"--bundle-hash", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.Method != eventPublishMethod {
		t.Fatalf("method = %s, want %s", captured.Method, eventPublishMethod)
	}
	if captured.Params["event_name"] != eventName {
		t.Fatalf("event_name param = %#v, want %s", captured.Params["event_name"], eventName)
	}
	if !strings.Contains(stdout.String(), "event_name="+eventName) {
		t.Fatalf("stdout = %q, want flow-scoped event name", stdout.String())
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestEventPublishBundleHashSerializesCanonicalParamAndMapsUnsupported(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeEventPublishJSONRPCError(t, w, captured.ID, "UNSUPPORTED_BUNDLE_HASH")
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"event", "publish", "scan.requested",
		"--payload-json", `{}`,
		"--bundle-hash", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 6 {
		t.Fatalf("code = %d, want 6 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if got := captured.Params["bundle_hash"]; got != "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("bundle_hash = %#v", got)
	}
	if _, ok := captured.Params["bundle_ref"]; ok {
		t.Fatalf("bundle_ref unexpectedly present in canonical request: %#v", captured.Params)
	}
	if !strings.Contains(stderr.String(), "UNSUPPORTED_BUNDLE_HASH") {
		t.Fatalf("stderr = %q, want UNSUPPORTED_BUNDLE_HASH", stderr.String())
	}
}

func TestEventPublishPayloadEntityIDServerRejectionMapsSupportedCLISurface(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeEventPublishJSONRPCErrorWithDetails(t, w, captured.ID, "PAYLOAD_VALIDATION_FAILED", map[string]any{
			"event_name": "thing.created",
			"violations": []any{
				map[string]any{
					"field_path": "$.entity_id",
					"rule":       "create_entity_mints_entity_id",
				},
			},
		})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"event", "publish", "thing.created",
		"--payload-json", `{"entity_id":"11111111-1111-4111-8111-111111111111","amount":50}`,
		"--bundle-hash", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"--idempotency-key", "idem-cli-create-entity-supplied-id",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 6 {
		t.Fatalf("code = %d, want 6 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if captured.Method != eventPublishMethod {
		t.Fatalf("method = %s, want %s", captured.Method, eventPublishMethod)
	}
	payload := captured.Params["payload"].(map[string]any)
	if payload["entity_id"] != "11111111-1111-4111-8111-111111111111" || payload["amount"] != float64(50) {
		t.Fatalf("payload = %#v, want supplied entity_id and amount", payload)
	}
	if got := captured.Params["bundle_hash"]; got != "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("bundle_hash = %#v", got)
	}
	if got := captured.Params["idempotency_key"]; got != "idem-cli-create-entity-supplied-id" {
		t.Fatalf("idempotency_key = %#v", got)
	}
	if strings.TrimSpace(stdout.String()) != "" {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	if !strings.Contains(stderr.String(), "PAYLOAD_VALIDATION_FAILED: Application error: PAYLOAD_VALIDATION_FAILED") {
		t.Fatalf("stderr = %q, want server payload validation rejection", stderr.String())
	}
}

func TestEventPublishLegacyBundleFingerprintSerializesBundleRef(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, eventPublishTestResult(false))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"event", "publish", "scan.requested",
		"--payload-json", `{}`,
		"--bundle-fingerprint", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if got := captured.Params["bundle_ref"]; !reflect.DeepEqual(got, map[string]any{"fingerprint": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}) {
		t.Fatalf("bundle_ref = %#v", got)
	}
	if _, ok := captured.Params["bundle_hash"]; ok {
		t.Fatalf("bundle_hash unexpectedly present in legacy request: %#v", captured.Params)
	}
}

func TestEventPublishOmitsOptionalParamsWhenNotProvided(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, eventPublishTestResult(false))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"event", "publish", "scan.requested",
		"--payload-json", `{}`,
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	wantParams := map[string]any{
		"event_name": "scan.requested",
		"payload":    map[string]any{},
	}
	if !reflect.DeepEqual(captured.Params, wantParams) {
		t.Fatalf("params = %#v, want %#v", captured.Params, wantParams)
	}
	if !strings.Contains(stdout.String(), "new_run_created=false") {
		t.Fatalf("stdout = %q, want new_run_created=false", stdout.String())
	}
}

func TestEventPublishRejectsInvalidInputBeforeRequest(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", eventPublishTestResult(true))
	}))
	defer server.Close()

	for _, tc := range []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{name: "missing event name", args: []string{"event", "publish", "--payload-json", "{}"}, wantStderr: "accepts 1 arg(s)"},
		{name: "blank event name", args: []string{"event", "publish", "  ", "--payload-json", "{}"}, wantStderr: "event name is required"},
		{name: "missing payload", args: []string{"event", "publish", "scan.requested"}, wantStderr: "requires --payload-json"},
		{name: "blank payload", args: []string{"event", "publish", "scan.requested", "--payload-json", "  "}, wantStderr: "requires --payload-json"},
		{name: "invalid payload json", args: []string{"event", "publish", "scan.requested", "--payload-json", "{"}, wantStderr: "valid JSON object"},
		{name: "non object payload", args: []string{"event", "publish", "scan.requested", "--payload-json", "[]"}, wantStderr: "valid JSON object"},
		{name: "null payload", args: []string{"event", "publish", "scan.requested", "--payload-json", "null"}, wantStderr: "JSON object"},
		{name: "trailing json", args: []string{"event", "publish", "scan.requested", "--payload-json", `{} {}`}, wantStderr: "exactly one JSON object"},
		{name: "blank run id", args: []string{"event", "publish", "scan.requested", "--payload-json", "{}", "--run-id", "  "}, wantStderr: "--run-id must be non-empty"},
		{name: "blank source event id", args: []string{"event", "publish", "scan.requested", "--payload-json", "{}", "--source-event-id", "  "}, wantStderr: "--source-event-id must be non-empty"},
		{name: "source event without run id", args: []string{"event", "publish", "scan.requested", "--payload-json", "{}", "--source-event-id", "event-parent-1"}, wantStderr: "--source-event-id requires --run-id"},
		{name: "blank bundle hash", args: []string{"event", "publish", "scan.requested", "--payload-json", "{}", "--bundle-hash", "  "}, wantStderr: "--bundle-hash must be non-empty"},
		{name: "invalid bundle hash", args: []string{"event", "publish", "scan.requested", "--payload-json", "{}", "--bundle-hash", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, wantStderr: "--bundle-hash must be bundle-v1:sha256:<64 lowercase hex>"},
		{name: "blank bundle fingerprint", args: []string{"event", "publish", "scan.requested", "--payload-json", "{}", "--bundle-fingerprint", "  "}, wantStderr: "--bundle-fingerprint must be non-empty"},
		{name: "invalid bundle fingerprint", args: []string{"event", "publish", "scan.requested", "--payload-json", "{}", "--bundle-fingerprint", "sha256:BAD"}, wantStderr: "--bundle-fingerprint must be sha256:<64 lowercase hex>"},
		{name: "bundle hash conflicts with legacy fingerprint", args: []string{"event", "publish", "scan.requested", "--payload-json", "{}", "--bundle-hash", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--bundle-fingerprint", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, wantStderr: "--bundle-hash is mutually exclusive with --bundle-fingerprint"},
		{name: "blank emitter", args: []string{"event", "publish", "scan.requested", "--payload-json", "{}", "--emitter", "  "}, wantStderr: "--emitter must be non-empty"},
		{name: "blank idempotency key", args: []string{"event", "publish", "scan.requested", "--payload-json", "{}", "--idempotency-key", "  "}, wantStderr: "--idempotency-key must be non-empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls.Store(0)
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 2 {
				t.Fatalf("code = %d, want 2 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
			if calls.Load() != 0 {
				t.Fatalf("calls = %d, want 0", calls.Load())
			}
		})
	}
}

func TestEventPublishFailClosedWithoutTokenBeforeRequest(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", eventPublishTestResult(true))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"event", "publish", "scan.requested",
		"--payload-json", "{}",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 4 {
		t.Fatalf("code = %d, want 4 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "SWARM_API_TOKEN is required") {
		t.Fatalf("stderr = %q, want token failure", stderr.String())
	}
	if calls.Load() != 0 {
		t.Fatalf("calls = %d, want 0", calls.Load())
	}
}

func TestEventPublishMapsFailureExitCodes(t *testing.T) {
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
				writeEventPublishJSONRPCError(t, w, req.ID, "RUN_NOT_FOUND")
			},
			wantCode:   5,
			wantStderr: "RUN_NOT_FOUND",
		},
		{
			name: "source event not found exits five",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeEventPublishJSONRPCError(t, w, req.ID, "EVENT_NOT_FOUND")
			},
			wantCode:   5,
			wantStderr: "EVENT_NOT_FOUND",
		},
		{
			name: "bundle mismatch exits six",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeEventPublishJSONRPCError(t, w, req.ID, "BUNDLE_MISMATCH")
			},
			wantCode:   6,
			wantStderr: "BUNDLE_MISMATCH",
		},
		{
			name: "bundle scope required exits six",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeEventPublishJSONRPCError(t, w, req.ID, "BUNDLE_SCOPE_REQUIRED")
			},
			wantCode:   6,
			wantStderr: "BUNDLE_SCOPE_REQUIRED",
		},
		{
			name: "bundle unavailable exits six",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeEventPublishJSONRPCError(t, w, req.ID, "BUNDLE_UNAVAILABLE")
			},
			wantCode:   6,
			wantStderr: "BUNDLE_UNAVAILABLE",
		},
		{
			name: "bundle data integrity exits six",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeEventPublishJSONRPCError(t, w, req.ID, "BUNDLE_DATA_INTEGRITY_ERROR")
			},
			wantCode:   6,
			wantStderr: "BUNDLE_DATA_INTEGRITY_ERROR",
		},
		{
			name: "unsupported canonical bundle exits six",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeEventPublishJSONRPCError(t, w, req.ID, "UNSUPPORTED_BUNDLE_HASH")
			},
			wantCode:   6,
			wantStderr: "UNSUPPORTED_BUNDLE_HASH",
		},
		{
			name: "unsupported legacy bundle exits six",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeEventPublishJSONRPCError(t, w, req.ID, "UNSUPPORTED_BUNDLE_REF")
			},
			wantCode:   6,
			wantStderr: "UNSUPPORTED_BUNDLE_REF",
		},
		{
			name: "undeclared event exits six",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeEventPublishJSONRPCError(t, w, req.ID, "EVENT_NOT_DECLARED")
			},
			wantCode:   6,
			wantStderr: "EVENT_NOT_DECLARED",
		},
		{
			name: "payload validation exits six",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeEventPublishJSONRPCError(t, w, req.ID, "PAYLOAD_VALIDATION_FAILED")
			},
			wantCode:   6,
			wantStderr: "PAYLOAD_VALIDATION_FAILED",
		},
		{
			name: "terminal run exits six",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeEventPublishJSONRPCError(t, w, req.ID, "RUN_ALREADY_TERMINAL")
			},
			wantCode:   6,
			wantStderr: "RUN_ALREADY_TERMINAL",
		},
		{
			name: "idempotency conflict exits six",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeEventPublishJSONRPCError(t, w, req.ID, "IDEMPOTENCY_CONFLICT")
			},
			wantCode:   6,
			wantStderr: "IDEMPOTENCY_CONFLICT",
		},
		{
			name: "unauthorized rpc exits four",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeEventPublishJSONRPCError(t, w, req.ID, "UNAUTHORIZED")
			},
			wantCode:   4,
			wantStderr: "UNAUTHORIZED",
		},
		{
			name: "unknown rpc exits three",
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeEventPublishJSONRPCError(t, w, req.ID, "METHOD_UNAVAILABLE")
			},
			wantCode:   3,
			wantStderr: "METHOD_UNAVAILABLE",
		},
		{
			name: "http auth exits four",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
			},
			wantCode:   4,
			wantStderr: "v1 RPC HTTP 401",
		},
		{
			name: "http runtime exits three",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
			},
			wantCode:   3,
			wantStderr: "v1 RPC HTTP 503",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SWARM_API_TOKEN", "test-token")
			server := httptest.NewServer(tc.handler)
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
				"event", "publish", "scan.requested",
				"--payload-json", "{}",
			}, &stdout, &stderr, testRootCommandOptions(server))
			if code != tc.wantCode {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, tc.wantCode, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func TestEventPublishMalformedResultsFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mutate     func(map[string]any)
		wantStderr string
	}{
		{
			name:       "missing event id",
			mutate:     func(result map[string]any) { delete(result, "event_id") },
			wantStderr: "event_id is required",
		},
		{
			name:       "missing run id",
			mutate:     func(result map[string]any) { delete(result, "run_id") },
			wantStderr: "run_id is required",
		},
		{
			name:       "missing new run created",
			mutate:     func(result map[string]any) { delete(result, "new_run_created") },
			wantStderr: "new_run_created is required",
		},
		{
			name:       "missing deliveries",
			mutate:     func(result map[string]any) { delete(result, "deliveries") },
			wantStderr: "deliveries is required",
		},
		{
			name: "missing subscriber",
			mutate: func(result map[string]any) {
				delivery := result["deliveries"].([]any)[0].(map[string]any)
				delete(delivery, "subscriber_id")
			},
			wantStderr: "deliveries[0].subscriber_id is required",
		},
		{
			name: "invalid delivery status",
			mutate: func(result map[string]any) {
				delivery := result["deliveries"].([]any)[0].(map[string]any)
				delivery["status"] = "locally_published"
			},
			wantStderr: "deliveries[0].status",
		},
		{
			name: "invalid delivery attempt",
			mutate: func(result map[string]any) {
				delivery := result["deliveries"].([]any)[0].(map[string]any)
				delivery["attempt"] = 0
			},
			wantStderr: "deliveries[0].attempt must be >= 1",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SWARM_API_TOKEN", "test-token")
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := eventPublishTestResult(true)
				tc.mutate(result)
				writeJSONRPCResult(t, w, req.ID, result)
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
				"event", "publish", "scan.requested",
				"--payload-json", "{}",
			}, &stdout, &stderr, testRootCommandOptions(server))
			if code != 3 {
				t.Fatalf("code = %d, want 3 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func eventPublishTestResult(newRunCreated bool) map[string]any {
	return map[string]any{
		"event_id":        "event-1",
		"run_id":          "run-1",
		"source_event_id": "event-parent-1",
		"new_run_created": newRunCreated,
		"deliveries": []any{
			map[string]any{
				"subscriber_id": "agent-1",
				"session_id":    "session-1",
				"status":        "delivered",
				"attempt":       2,
			},
		},
	}
}

func writeEventPublishJSONRPCError(t *testing.T, w http.ResponseWriter, id string, code string) {
	t.Helper()
	writeEventPublishJSONRPCErrorWithDetails(t, w, id, code, map[string]any{"event_name": "scan.requested"})
}

func writeEventPublishJSONRPCErrorWithDetails(t *testing.T, w http.ResponseWriter, id string, code string, details map[string]any) {
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
				"details":        details,
				"retryable":      false,
				"correlation_id": "corr-event-publish",
			},
		},
	}); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}
