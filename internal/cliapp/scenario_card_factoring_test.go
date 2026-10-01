package cliapp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestScenarioCardFactoringMatchCharacterization(t *testing.T) {
	fields := []string{"card_id", "entity_id", "flow_instance", "decision", "stage", "requester_agent_id", "category", "request_event_id", "activity_id", "scope"}
	for _, anchor := range []string{"stage_gate", "human_task", "proposed_effect"} {
		for _, field := range fields {
			t.Run(anchor+"/"+field, func(t *testing.T) {
				detail := scenarioCursorProjection(anchor, "selected", true)
				card := detail["decision_card"].(map[string]any)
				value, admitted := factoringCardMatchValue(card, field)
				if anchor == "stage_gate" && field == "scope" {
					admitted = false
				}
				for _, mismatch := range []bool{false, true} {
					gets := 0
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
						var rpc jsonRPCRequest
						if err := json.NewDecoder(req.Body).Decode(&rpc); err != nil {
							t.Error(err)
							return
						}
						switch rpc.Method {
						case "mailbox.list":
							if rpc.Params["limit"] != float64(200) || rpc.Params["run_id"] != "run-1" || rpc.Params["anchor_kind"] != anchor || rpc.Params["status"] != "pending" {
								t.Errorf("list params=%v", rpc.Params)
							}
							if field == "entity_id" && rpc.Params[field] != matchValueForFactoring(value, mismatch, admitted) {
								t.Errorf("entity pushdown=%v", rpc.Params)
							}
							writeJSONRPCResult(t, w, rpc.ID, map[string]any{"items": []any{scenarioCursorProjection(anchor, "selected", false)}, "next_cursor": "", "unread_informational_notices": 0})
						case "mailbox.get":
							gets++
							if rpc.Params["mailbox_id"] != "selected" {
								t.Errorf("detail params=%v", rpc.Params)
							}
							writeJSONRPCResult(t, w, rpc.ID, detail)
						default:
							t.Errorf("unexpected %s", rpc.Method)
						}
					}))
					client, err := newCLIAPIClientForTest(t, rootCommandOptions{invocationRoot: mustInvocationRootForTest(t.TempDir()), apiServer: server.URL})
					if err != nil {
						t.Fatal(err)
					}
					matchValue := value
					if mismatch || !admitted {
						matchValue = "does-not-match"
					}
					id, hash, err := (scenarioRunner{client: client}).findDecisionCard(context.Background(), mustScenarioExpressionEvaluator(t, nil), "run-1", map[string]any{"anchor_kind": anchor, field: matchValue})
					server.Close()
					if !admitted {
						var validation scenarioTestValidationError
						if !errors.As(err, &validation) || !strings.Contains(err.Error(), "not valid for anchor_kind") || gets != 0 {
							t.Fatalf("invalid selector admitted: id=%s err=%v gets=%d", id, err, gets)
						}
					} else if mismatch {
						if err == nil || !strings.Contains(err.Error(), "returned 0 items") || gets != 0 {
							t.Fatalf("mismatch accepted: id=%s err=%v gets=%d", id, err, gets)
						}
					} else if err != nil || id != "selected" || hash != card["card_content_hash"] || gets != 1 {
						t.Fatalf("exact selector: id=%s hash=%s err=%v gets=%d", id, hash, err, gets)
					}
				}
			})
		}
	}
}

func matchValueForFactoring(value string, mismatch, admitted bool) string {
	if mismatch || !admitted {
		return "does-not-match"
	}
	return value
}

func factoringCardMatchValue(card map[string]any, field string) (string, bool) {
	var value any
	switch field {
	case "entity_id", "flow_instance", "scope":
		key := field
		if field == "scope" {
			key = "kind"
		}
		value = card["scope"].(map[string]any)[key]
	case "stage", "requester_agent_id", "request_event_id", "activity_id":
		value = card["anchor"].(map[string]any)[field]
	default:
		value = card[field]
	}
	text, ok := value.(string)
	return text, ok && text != ""
}

func TestScenarioCardFactoringAdmissionCharacterization(t *testing.T) {
	for _, row := range []struct {
		name  string
		match map[string]any
		want  string
	}{
		{"absent", nil, "match.anchor_kind is required"},
		{"empty", map[string]any{"anchor_kind": " "}, "match.anchor_kind is required"},
		{"unknown_anchor", map[string]any{"anchor_kind": "unknown"}, "match.anchor_kind is required"},
		{"unknown_field", map[string]any{"anchor_kind": "stage_gate", "unknown": "value"}, "unsupported decision-card match field"},
	} {
		t.Run(row.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				t.Error("RPC before selector admission")
				w.WriteHeader(500)
			}))
			defer server.Close()
			client, err := newCLIAPIClientForTest(t, rootCommandOptions{invocationRoot: mustInvocationRootForTest(t.TempDir()), apiServer: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = (scenarioRunner{client: client}).findDecisionCard(context.Background(), mustScenarioExpressionEvaluator(t, nil), "run-1", row.match)
			var validation scenarioTestValidationError
			if !errors.As(err, &validation) || !strings.Contains(err.Error(), row.want) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestScenarioCardFactoringCancelledContinuationNeverReadsDetailOrMutates(t *testing.T) {
	for _, action := range []string{"mailbox.decide", "mailbox.defer"} {
		t.Run(action, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var mu sync.Mutex
			var calls []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				var rpc jsonRPCRequest
				if err := json.NewDecoder(req.Body).Decode(&rpc); err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				calls = append(calls, rpc.Method)
				n := len(calls)
				mu.Unlock()
				if rpc.Method != "mailbox.list" {
					t.Errorf("premature %s", rpc.Method)
					return
				}
				if n == 1 {
					writeJSONRPCResult(t, w, rpc.ID, map[string]any{"items": []any{scenarioCursorProjection("stage_gate", "selected", false)}, "next_cursor": "opaque+/=", "unread_informational_notices": 0})
					return
				}
				if rpc.Params["cursor"] != "opaque+/=" {
					t.Error("opaque cursor changed")
				}
				cancel()
				<-req.Context().Done()
			}))
			defer server.Close()
			client, err := newCLIAPIClientForTest(t, rootCommandOptions{invocationRoot: mustInvocationRootForTest(t.TempDir()), apiServer: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			step := scenarioStep{Action: action, Match: map[string]any{"anchor_kind": "stage_gate"}, Verdict: "approve", Until: "2030-01-01T00:00:00Z"}
			err = (scenarioRunner{client: client}).runMailboxStep(ctx, mustScenarioExpressionEvaluator(t, nil), &scenarioRunState{RunID: "run-1"}, step)
			mu.Lock()
			defer mu.Unlock()
			if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(calls, []string{"mailbox.list", "mailbox.list"}) {
				t.Fatalf("err=%v calls=%v", err, calls)
			}
		})
	}
}
