package cliapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestScenarioCardFactoringDetailAndMutationFailures(t *testing.T) {
	for _, anchor := range []string{"stage_gate", "human_task", "proposed_effect"} {
		for _, action := range []string{"mailbox.decide", "mailbox.defer"} {
			for _, failure := range []string{"cursor_type", "get_error", "missing_hash", "missing_snapshot", "bad_anchor", "cancel_get", "not_ok", "wrong_card", "missing_change"} {
				t.Run(anchor+"/"+action+"/"+failure, func(t *testing.T) {
					var gets, mutations atomic.Int32
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					enteredGet := make(chan struct{})
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						var req jsonRPCRequest
						if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
							t.Error(err)
							return
						}
						switch req.Method {
						case "mailbox.list":
							result := map[string]any{"items": []any{scenarioCursorProjection(anchor, "selected", false)}, "unread_informational_notices": 0}
							if failure == "cursor_type" {
								result["next_cursor"] = 3
							}
							writeJSONRPCResult(t, w, req.ID, result)
						case "mailbox.get":
							gets.Add(1)
							if failure == "cancel_get" {
								close(enteredGet)
								<-r.Context().Done()
								return
							}
							if failure == "get_error" {
								http.Error(w, "detail unavailable", http.StatusServiceUnavailable)
								return
							}
							result := scenarioCursorProjection(anchor, "selected", true)
							card := result["decision_card"].(map[string]any)
							switch failure {
							case "missing_hash":
								delete(card, "card_content_hash")
							case "missing_snapshot":
								delete(card, "snapshot")
							case "bad_anchor":
								card["anchor_kind"] = "unregistered"
							}
							writeJSONRPCResult(t, w, req.ID, result)
						case action:
							mutations.Add(1)
							result := map[string]any{"ok": true, "card_id": "selected", "change_id": 1}
							switch failure {
							case "not_ok":
								result["ok"] = false
							case "wrong_card":
								result["card_id"] = "foreign"
							case "missing_change":
								delete(result, "change_id")
							}
							writeJSONRPCResult(t, w, req.ID, result)
						default:
							t.Error("unexpected method " + req.Method)
						}
					}))
					defer func() {
						cancel()
						server.Close()
					}()
					client, err := newCLIAPIClientForTest(t, rootCommandOptions{invocationRoot: mustInvocationRootForTest(t.TempDir()), apiServer: server.URL})
					if err != nil {
						t.Fatal(err)
					}
					step := scenarioStep{Action: action, Match: map[string]any{"anchor_kind": anchor}, Verdict: "approve", Until: "2030-01-01T00:00:00Z"}
					evaluator := mustScenarioExpressionEvaluator(t, nil)
					result := make(chan error, 1)
					go func() {
						result <- (scenarioRunner{client: client}).runMailboxStep(ctx, evaluator, &scenarioRunState{RunID: "run-1"}, step)
					}()
					if failure == "cancel_get" {
						select {
						case <-enteredGet:
							cancel()
						case <-time.After(5 * time.Second):
							t.Fatal("detail read not reached")
						}
					}
					select {
					case err = <-result:
					case <-time.After(5 * time.Second):
						t.Fatal("failure did not settle")
					}
					wantGets, wantMutations := int32(1), int32(0)
					if failure == "cursor_type" {
						wantGets = 0
					}
					if failure == "not_ok" || failure == "wrong_card" || failure == "missing_change" {
						wantMutations = 1
						if err == nil || !strings.Contains(err.Error(), "malformed "+action+" result") {
							t.Fatalf("mutation error=%v", err)
						}
					}
					if err == nil || gets.Load() != wantGets || mutations.Load() != wantMutations {
						t.Fatal(fmt.Sprintf("failure=%s err=%v gets=%d/%d mutations=%d/%d", failure, err, gets.Load(), wantGets, mutations.Load(), wantMutations))
					}
				})
			}
		}
	}
}
