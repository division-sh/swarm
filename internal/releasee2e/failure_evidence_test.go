package releasee2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestReleaseRPCFailureEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, body, want string
		status           int
	}{
		{"missing", `{"jsonrpc":"2.0","id":"health.check"}`, "returned no result", 200},
		{"null", `{"jsonrpc":"2.0","id":"health.check","result":null}`, "returned no result", 200},
		{"wrong_id", `{"jsonrpc":"2.0","id":"foreign-call","result":{"ok":true}}`, "identity mismatch", 200},
		{"missing_id", `{"jsonrpc":"2.0","result":{"ok":true}}`, "identity mismatch", 200},
		{"error", `{"jsonrpc":"2.0","id":"health.check","error":{"code":-32000,"message":"provider-secret authorization-secret","data":{"secret":"unlisted-data-secret"}}}`, "failed", 200},
		{"extra_secrets", `{"jsonrpc":"2.0","id":"health.check","result":null,"Authorization":"authorization-secret","provider":{"token":"unlisted-data-secret"}}`, "returned no result", 200},
		{"non_json", `upstream proxy provider-secret unlisted-data-secret`, "HTTP 503", 503},
		{"malformed", `{"secret":"unlisted-data-secret"`, "decode health.check response", 200},
		{"bounded", `{"jsonrpc":"2.0","id":"health.check","error":{"code":-32000,"message":"` + strings.Repeat("x", 8192) + `provider-secret"}}`, "failed", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				var request struct {
					ID string `json:"id"`
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.ID != "health.check" {
					t.Errorf("request identity: %+v, %v", request, err)
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			rpc := &releaseRPCClient{endpoint: server.URL + "/rpc?token=endpoint-secret", token: "authorization-secret", client: server.Client(), processID: 1234, redactValues: []string{"provider-secret"}}
			var result any
			err := rpc.call(context.Background(), "health.check", nil, &result)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
			text := err.Error()
			for _, required := range []string{`"process_id":1234`, `"request_id":"health.check"`, `"http_status":` + fmt.Sprint(tc.status), `"body_sha256":`, `"body_bytes":`, `"response_envelope":`, server.URL + "/rpc"} {
				if !strings.Contains(text, required) {
					t.Errorf("missing %s in %s", required, text)
				}
			}
			for _, secret := range []string{"authorization-secret", "provider-secret", "endpoint-secret", "unlisted-data-secret"} {
				if strings.Contains(text, secret) {
					t.Errorf("failure evidence exposed secret %q", secret)
				}
			}
			if tc.name == "null" && !strings.Contains(text, `"result":null`) {
				t.Errorf("null result missing: %s", text)
			}
			if tc.name == "wrong_id" && !strings.Contains(text, `"id":"foreign-call"`) {
				t.Errorf("foreign response identity missing: %s", text)
			}
			if len(text) > 1600 {
				t.Errorf("unbounded evidence: %d bytes", len(text))
			}
			if calls.Load() != 1 {
				t.Errorf("failure was retried %d times", calls.Load())
			}
		})
	}
}

func TestGoldenFailureEvidenceSurvivesCollectorFailure(t *testing.T) {
	for _, scenario := range []string{"entity_error", "entity_timeout", "partial_events", "partial_entities", "budget_cancelled"} {
		t.Run(scenario, func(t *testing.T) {
			var eventsFlushed atomic.Bool
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					ID, Method string
					Params     map[string]any
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					return
				}
				envelope := map[string]any{"jsonrpc": "2.0", "id": request.ID}
				switch request.Method {
				case "run.diagnose":
					envelope["result"] = map[string]any{}
				case "event.list":
					if request.Params["cursor"] != nil {
						envelope["error"] = map[string]any{"code": -32000, "message": "event page failure"}
					} else {
						result := map[string]any{"events": []map[string]any{{"event_id": "analyzed-id", "event_name": "candidate.analyzed", "run_id": "run"}}}
						if scenario == "partial_events" {
							result["next_cursor"] = "next"
						}
						envelope["result"] = result
					}
				case "entity.list":
					if !eventsFlushed.Load() {
						t.Error("entity collection began before public event evidence was flushed")
					}
					if scenario == "budget_cancelled" {
						cancel()
						<-r.Context().Done()
						return
					}
					if scenario == "entity_timeout" {
						<-r.Context().Done()
						return
					}
					if scenario == "partial_entities" && request.Params["cursor"] == nil {
						envelope["result"] = map[string]any{"entities": []map[string]any{{"entity_id": "candidate-id", "run_id": "run"}}, "next_cursor": "next"}
					} else {
						envelope["error"] = map[string]any{"code": -32000, "message": "entity collection failure"}
					}
				case "runtime.logs":
					envelope["result"] = map[string]any{"logs": []map[string]any{{"log_id": "log-id", "action": "handled"}}}
				default:
					t.Errorf("unexpected method %s", request.Method)
				}
				_ = json.NewEncoder(w).Encode(envelope)
			}))
			defer server.Close()
			rpc := &releaseRPCClient{endpoint: server.URL, client: server.Client()}
			parts := map[string]any{}
			errs := map[string]error{}
			collectGoldenFailureEvidence(ctx, rpc, nil, "run", func(part string, value any, err error) {
				parts[part], errs[part] = value, err
				if part == "events" {
					eventsFlushed.Store(true)
				}
			})
			if len(parts) != 5 {
				t.Fatalf("collection stopped early: %#v", parts)
			}
			if errs["entities"] == nil {
				t.Fatal("entity failure was lost")
			}
			if scenario == "entity_timeout" && !errors.Is(errs["entities"], context.DeadlineExceeded) {
				t.Fatalf("timeout cause lost: %v", errs["entities"])
			}
			if scenario == "budget_cancelled" {
				if !errors.Is(errs["entities"], context.Canceled) || !errors.Is(errs["runtime_logs"], context.Canceled) {
					t.Fatalf("total-budget cancellation lost: entities=%v logs=%v", errs["entities"], errs["runtime_logs"])
				}
			}
			if scenario == "partial_events" && errs["events"] == nil {
				t.Fatal("event page failure was lost")
			}
			events := parts["events"].([]goldenEvent)
			if len(events) != 1 || events[0].EventID != "analyzed-id" {
				t.Fatalf("earlier event evidence lost: %#v", events)
			}
			if scenario == "partial_entities" && len(parts["entities"].([]goldenEntitySummary)) != 1 {
				t.Fatal("earlier entity page lost")
			}
			logs := parts["runtime_logs"].([]map[string]any)
			if scenario != "budget_cancelled" && (errs["runtime_logs"] != nil || len(logs) != 1 || logs[0]["log_id"] != "log-id") {
				t.Fatalf("later log collection aborted: %#v, %v", logs, errs["runtime_logs"])
			}
		})
	}
}
