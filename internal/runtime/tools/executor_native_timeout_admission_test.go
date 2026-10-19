package tools

import (
	"io"
	"net/http"
	"sync/atomic"
	"testing"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
)

func TestNativeWebSearchCustomPolicyRejectsInvalidHTTP(t *testing.T) {
	for _, row := range []struct {
		name   string
		fields map[string]any
	}{
		{"zero", map[string]any{"timeout_seconds": 0}},
		{"null", map[string]any{"timeout_seconds": nil}},
		{"negative", map[string]any{"timeout_seconds": -1}},
		{"unrepresentable", map[string]any{"timeout_seconds": 9223372037}},
		{"string", map[string]any{"timeout_seconds": "1"}},
		{"boolean", map[string]any{"timeout_seconds": true}},
		{"fractional", map[string]any{"timeout_seconds": 1.5}},
		{"integral_float", map[string]any{"timeout_seconds": float64(1)}},
		{"sequence", map[string]any{"timeout_seconds": []any{1}}},
		{"mapping", map[string]any{"timeout_seconds": map[string]any{"seconds": 1}}},
		{"go_field_only", map[string]any{"TimeoutSeconds": 7}},
		{"mixed_spellings", map[string]any{"timeout_seconds": 1, "TimeoutSeconds": 7}},
		{"unknown_field", map[string]any{"timeuot_seconds": 1}},
	} {
		t.Run(row.name, func(t *testing.T) {
			httpValue := map[string]any{"method": http.MethodPost, "url": "https://search.test/"}
			for name, value := range row.fields {
				httpValue[name] = value
			}
			executor := nativeTimeoutExecutor("https://search.test/", 0)
			executor.workflowSource = nativeTimeoutPolicySource(httpValue, "", "")
			harness := effecttest.New()
			ctx := nativeTimeoutContext(harness, "native-policy-refusal")
			actor, _ := ActorFromContext(ctx)
			cfg, admissionErr := executor.resolveWebSearchProviderConfig(actor)
			if admissionErr == nil {
				prepared, prepareErr := cfg.HTTP.Prepare(map[string]any{"query": "swarm"}, nil)
				t.Errorf("invalid policy admitted: prepared timeout=%s prepare error=%v fields=%v", prepared.Timeout(), prepareErr, row.fields)
			}
			var calls atomic.Int32
			executor.httpClient.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls.Add(1)
				return jsonHTTPResponse(http.StatusOK, nativeTimeoutResponse), nil
			})
			out, err := executor.Execute(ctx, "web_search", map[string]any{"query": "swarm"})
			if out != nil || err == nil || calls.Load() != 0 || len(harness.Attempts) != 0 || len(harness.Settlements) != 0 {
				t.Fatalf("invalid HTTP reached execution: out=%#v err=%v calls=%d attempts=%d settlements=%d", out, err, calls.Load(), len(harness.Attempts), len(harness.Settlements))
			}
		})
	}
}

func TestNativeWebSearchCustomPolicyPreservesBodyPresence(t *testing.T) {
	for _, row := range []struct {
		name, wire string
		present    bool
		body       any
	}{
		{name: "omitted"},
		{name: "null", present: true, body: nil, wire: "null"},
		{name: "zero", present: true, body: 0, wire: "0"},
		{name: "false", present: true, body: false, wire: "false"},
		{name: "empty_text", present: true, body: "", wire: `""`},
		{name: "empty_list", present: true, body: []any{}, wire: "[]"},
		{name: "empty_mapping", present: true, body: map[string]any{}, wire: "{}"},
	} {
		t.Run(row.name, func(t *testing.T) {
			httpValue := map[string]any{
				"method": http.MethodPost, "url": "https://search.test/", "timeout_seconds": 10,
				"headers": map[string]any{"X-Search-Budget": "preserved"},
			}
			if row.present {
				httpValue["body"] = row.body
			}
			executor := nativeTimeoutExecutor("https://search.test/", 10)
			executor.workflowSource = nativeTimeoutPolicySource(httpValue, "", "")
			harness := effecttest.New()
			ctx := nativeTimeoutContext(harness, "native-body-presence")
			var calls atomic.Int32
			executor.httpClient.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
				calls.Add(1)
				actual, err := io.ReadAll(req.Body)
				if err != nil || string(actual) != row.wire || req.GetBody == nil || req.Header.Get("X-Search-Budget") != "preserved" {
					t.Errorf("body=%s err=%v GetBody=%v header=%q, want %q", actual, err, req.GetBody != nil, req.Header.Get("X-Search-Budget"), row.wire)
				}
				if req.GetBody != nil {
					copy, err := req.GetBody()
					if err != nil {
						return nil, err
					}
					replayed, readErr := io.ReadAll(copy)
					_ = copy.Close()
					if readErr != nil || string(replayed) != row.wire {
						t.Errorf("replay body=%s err=%v, want %q", replayed, readErr, row.wire)
					}
				}
				return jsonHTTPResponse(http.StatusOK, nativeTimeoutResponse), nil
			})
			out, err := executor.Execute(ctx, "web_search", map[string]any{"query": "swarm"})
			requireNativeTimeoutResult(t, out, err)
			requireNativeTimeoutAttempt(t, executor, ctx, harness, &calls, runtimeeffects.StateSettled)
		})
	}
}
