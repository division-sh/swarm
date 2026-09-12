package cliapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestScenarioMailboxConsumesCompleteMatchSet(t *testing.T) {
	for _, anchor := range []string{"stage_gate", "human_task", "proposed_effect"} {
		for _, action := range []string{"mailbox.decide", "mailbox.defer"} {
			for _, test := range []struct {
				name string
				want string
			}{
				{"unique_later", ""}, {"unique_first", ""}, {"ambiguous", "returned 2 items"},
				{"zero", "returned 0 items"}, {"later_error", "later page unavailable"},
				{"later_missing_items", "items is required"}, {"later_invalid_item", "decision_card is required"},
				{"later_wrong_type", "cannot unmarshal"}, {"repeat", "repeated next_cursor"},
				{"cycle", "repeated next_cursor"}, {"stale", "stale content"},
			} {
				t.Run(anchor+"/"+action+"/"+test.name, func(t *testing.T) {
					lists, gets, mutations := 0, 0, 0
					var filters map[string]any
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
						var rpc jsonRPCRequest
						if err := json.NewDecoder(req.Body).Decode(&rpc); err != nil {
							t.Error(err)
							return
						}
						switch rpc.Method {
						case "mailbox.list":
							lists++
							if lists > 4 {
								t.Error("unbounded continuation")
								w.WriteHeader(http.StatusInternalServerError)
								return
							}
							if lists == 1 {
								filters = cloneAnyMap(rpc.Params)
							} else {
								wantCursor := "opaque-A"
								if lists == 3 {
									wantCursor = "opaque-B"
								}
								if rpc.Params["cursor"] != wantCursor {
									t.Errorf("cursor changed: %#v", rpc.Params)
								}
								delete(rpc.Params, "cursor")
								if !reflect.DeepEqual(filters, rpc.Params) {
									t.Errorf("filters changed: %#v != %#v", filters, rpc.Params)
								}
							}
							items := []any{}
							next := ""
							if lists == 1 {
								next = "opaque-A"
								for i := 0; i < 200; i++ {
									id := fmt.Sprintf("other-%d", i)
									if i == 0 && test.name != "unique_later" && test.name != "zero" {
										id = "selected"
									}
									items = append(items, scenarioCursorProjection(anchor, id, false))
								}
							} else {
								switch test.name {
								case "unique_later":
									items = append(items, scenarioCursorProjection(anchor, "selected", false))
								case "ambiguous":
									items = append(items, scenarioCursorProjection(anchor, "selected-second", false))
								case "later_error":
									_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": rpc.ID, "error": map[string]any{"code": -32603, "message": "later page unavailable"}})
									return
								case "later_missing_items":
									items = nil
								case "later_invalid_item":
									items = append(items, map[string]any{"kind": "decision_card"})
								case "later_wrong_type":
									writeJSONRPCResult(t, w, rpc.ID, map[string]any{"items": "bad"})
									return
								case "repeat":
									next = "opaque-A"
								case "cycle":
									next = "opaque-B"
									if lists == 3 {
										next = "opaque-A"
									}
								}
							}
							writeJSONRPCResult(t, w, rpc.ID, map[string]any{"items": items, "next_cursor": next, "unread_informational_notices": 0})
						case "mailbox.get":
							gets++
							if lists != 2 {
								t.Errorf("detail fetched before complete selection: lists=%d", lists)
							}
							writeJSONRPCResult(t, w, rpc.ID, scenarioCursorProjection(anchor, "selected", true))
						case action:
							mutations++
							if lists != 2 || gets != 1 || rpc.Params["card_id"] != "selected" {
								t.Errorf("premature/wrong mutation: %+v", rpc)
							}
							if action == "mailbox.decide" && rpc.Params["observed_content_hash"] != scenarioCursorProjection(anchor, "selected", true)["decision_card"].(map[string]any)["card_content_hash"] {
								t.Error("lost stale-content fence")
							}
							if test.name == "stale" {
								_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": rpc.ID, "error": map[string]any{"code": -32602, "message": "stale content"}})
								return
							}
							writeJSONRPCResult(t, w, rpc.ID, map[string]any{"ok": true, "card_id": "selected", "change_id": 1})
						default:
							t.Errorf("unexpected method %s", rpc.Method)
						}
					}))
					defer server.Close()
					client, err := newCLIAPIClientForTest(t, rootCommandOptions{invocationRoot: mustInvocationRootForTest(t.TempDir()), apiServer: server.URL})
					if err != nil {
						t.Fatal(err)
					}
					step := scenarioStep{Action: action, Match: map[string]any{"anchor_kind": anchor, "flow_instance": "root", "entity_id": "entity-1"}, Verdict: "approve", Until: "2030-01-01T00:00:00Z"}
					err = (scenarioRunner{client: client}).runMailboxStep(context.Background(), mustScenarioExpressionEvaluator(t, nil), &scenarioRunState{RunID: "run-1"}, step)
					if test.want == "" {
						if err != nil || lists != 2 || gets != 1 || mutations != 1 {
							t.Fatalf("err=%v lists=%d gets=%d mutations=%d", err, lists, gets, mutations)
						}
					} else {
						if err == nil || !strings.Contains(err.Error(), test.want) {
							t.Fatalf("err=%v want %s", err, test.want)
						}
						if test.name != "stale" && (gets != 0 || mutations != 0) {
							t.Fatalf("action before full admission: gets=%d mutations=%d", gets, mutations)
						}
					}
				})
			}
		}
	}
}

func scenarioCursorProjection(anchor, id string, detail bool) map[string]any {
	var card map[string]any
	switch anchor {
	case "stage_gate":
		card = mailboxCardDetailResult(id)
	case "human_task":
		card = mailboxHumanTaskCardDetailResult(id)
		card["scope"].(map[string]any)["entity_id"] = "entity-1"
	case "proposed_effect":
		card = mailboxProposedEffectCardDetailResult(id)
	}
	if !detail {
		delete(card, "snapshot")
		delete(card, "card_content_hash")
	}
	if strings.HasPrefix(id, "other-") {
		card["scope"].(map[string]any)["flow_instance"] = "other"
	}
	result := map[string]any{"kind": "decision_card", "decision_card": card}
	if anchor == "proposed_effect" {
		result["effect"] = mailboxProposedEffectStateResult()
	}
	return result
}
