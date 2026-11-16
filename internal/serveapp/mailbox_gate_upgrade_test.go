package serveapp

import (
	"cmp"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

// The served mock lifecycle uses real authenticated API/CLI and native stores;
// it is not a standalone binary, provider or physical-channel proof.
func TestMailboxUnchangedGateUpgradeAnswerBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, transport := range []string{"api", "cli"} {
			t.Run(string(backend)+"/"+transport, func(t *testing.T) {
				root := canonicalrouting.CopyMailboxNoticeCompletion(t)
				rt, restart := newMailboxGateEpoch(t, backend, root)
				base := rt.Base
				until := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
				var deferred, draft map[string]any
				requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.defer", map[string]any{"card_id": base.CardID, "until": until.Format(time.RFC3339), "idempotency_key": uuid.NewString()}, &deferred)
				requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.begin_input", map[string]any{"card_id": base.CardID, "verdict": "reject", "observed_content_hash": base.CardContentHash, "idempotency_key": uuid.NewString()}, &draft)
				before, err := rt.Card(base.CardID)
				if err != nil || !before.DeferredUntil.Equal(until) || draft["input_draft_id"] == nil {
					t.Fatalf("pending card setup: %+v draft=%v err=%v", before, draft, err)
				}
				schema := filepath.Join(root, "schema.yaml")
				body, err := os.ReadFile(schema)
				if err != nil {
					t.Fatal(err)
				}
				upgrade := func(wantHash, wantStatus string) {
					t.Helper()
					body = append(body, []byte("\n# immutable source changes; offered contract does not\n")...)
					if err := os.WriteFile(schema, body, 0o600); err != nil {
						t.Fatal(err)
					}
					previous := rt.BundleHash
					oldCard := rt.Card
					rt = restart()
					if _, err := oldCard(base.CardID); err == nil {
						t.Fatal("old observation followed restart")
					}
					if rt.BundleHash == previous {
						t.Fatal("restart did not admit the new source")
					}
					wantHash = cmp.Or(wantHash, rt.BundleHash)
					var header struct {
						Run struct {
							BundleHash string `json:"bundle_hash"`
							Status     string `json:"status"`
						} `json:"run"`
					}
					requireServedJSONRPCResult(t, rt.Endpoint, "run.get", map[string]any{"run_id": base.RunID}, &header)
					if header.Run.BundleHash != wantHash || header.Run.Status != wantStatus {
						t.Fatalf("restart run header=%+v want_hash=%s want_status=%s new_primary=%s", header.Run, wantHash, wantStatus, rt.BundleHash)
					}
					if wantStatus == "completed" {
						rt.Unloaded(base.RunID, wantHash)
					}
				}
				upgrade("", "running")
				after, err := rt.Card(base.CardID)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("equal upgrade changed issuance or defer: %v", err)
				}
				requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.defer", map[string]any{"card_id": before.CardID, "until": until.Format(time.RFC3339), "idempotency_key": uuid.NewString()}, &deferred)
				var canceled map[string]any
				requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.cancel_input", map[string]any{"card_id": base.CardID, "input_draft_id": draft["input_draft_id"], "idempotency_key": uuid.NewString()}, &canceled)
				params := map[string]any{"card_id": base.CardID, "verdict": "approve", "observed_content_hash": before.CardContentHash, "idempotency_key": uuid.NewString()}
				config := writeMockAgentRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "unused.sqlite"))
				tokenFile := filepath.Join(t.TempDir(), "api-token")
				if err := os.WriteFile(tokenFile, []byte(apiv1.DefaultLoopbackAPIToken), 0o600); err != nil {
					t.Fatal(err)
				}
				retainOriginalReturn := func() {
					t.Helper()
					captured := rt.Capture()
					calls, result, cause, domain, cardID := captured.Calls, captured.Result, captured.Err, captured.Domain, captured.CardID
					t.Logf("GATE_UPGRADE_ORIGINAL_RETURN calls=%d result=%#v error_type=%T error=%v domain=%+v card_id=%s", calls, result, cause, cause, domain, cardID)
					logGateUpgradeCauseTree(t, cause)
					rt.Observe("original_error_sequential_readback", before.RunID, cardID, domain)
				}
				answer := func() {
					t.Helper()
					if transport == "api" {
						response := requestServedJSONRPC(t, rt.Endpoint, "mailbox.decide", params)
						if response.Error != nil {
							retainOriginalReturn()
							t.Fatalf("mailbox.decide error = %#v", response.Error)
						}
						var result map[string]any
						if err := json.Unmarshal(response.Result, &result); err != nil {
							t.Fatalf("decode mailbox.decide result: %v\n%s", err, response.Result)
						}
						return
					}
					out, errOut := &lockedBuffer{}, &lockedBuffer{}
					code := executeCLI(t.Context(), []string{"--config", config, "mailbox", "decide", before.CardID, "approve", "--idempotency-key", params["idempotency_key"].(string), "--api-server", strings.TrimSuffix(rt.Endpoint, "/v1/rpc"), "--api-token-file", tokenFile}, out, errOut, nil)
					if code != 0 {
						retainOriginalReturn()
						t.Fatalf("current-run CLI answer exit=%d out=%s err=%s", code, out.String(), errOut.String())
					}
				}
				answerSource, answerRuntime := rt.BundleHash, rt.RuntimeInstanceID
				answer()
				domain := rt.Capture().Domain
				rt.WaitStage(before.RunID, "done")
				rt.WaitDeliveries(before.RunID)
				decided, err := rt.Card(before.CardID)
				if err != nil || decided.Status != decisioncard.StatusDecided || decided.DecisionEventID == "" || decided.BundleHash != before.BundleHash || decided.CardContentHash != before.CardContentHash || !reflect.DeepEqual(decided.Snapshot, before.Snapshot) {
					t.Fatalf("current execution rewrote issuance or failed continuation: %+v %v", decided, err)
				}
				assertAttribution := func() {
					t.Helper()
					page, err := rt.Activity(authoractivity.ListOptions{RunID: before.RunID, Limit: 100})
					if err != nil || len(page.Occurrences) == 100 {
						t.Fatalf("complete bounded activity readback: %v", err)
					}
					decisions, deferrals, continuations := 0, 0, 0
					for _, occurrence := range page.Occurrences {
						if occurrence.Kind == authoractivity.KindCardLifecycle && occurrence.Projection.CardID == before.CardID {
							if occurrence.Scope.Kind != authoractivity.ScopeBundle || occurrence.Scope.BundleHash != before.BundleHash {
								t.Fatalf("card lifecycle lost immutable target: %+v", occurrence)
							}
							if occurrence.Transition == decisioncard.ChangeDecided {
								decisions++
								if occurrence.Scope.RuntimeInstanceID != answerRuntime || occurrence.Projection.DecisionID != decided.DecisionEventID {
									t.Fatalf("decided story lost exact target/runtime: %+v", occurrence)
								}
							} else if occurrence.Transition == decisioncard.ChangeDeferred {
								deferrals++
							}
						}
						if occurrence.Kind == authoractivity.KindEventEmitted && occurrence.Projection.EventType == "work.completed" && occurrence.Scope.BundleHash == answerSource {
							continuations++
						}
					}
					if decisions != 1 || deferrals != 2 || continuations != 1 || answerSource == before.BundleHash {
						t.Fatalf("target/current execution counts: decisions=%d deferrals=%d continuations=%d source=%s issued=%s", decisions, deferrals, continuations, answerSource, before.BundleHash)
					}
					event, found, err := rt.Prepared(decided.DecisionEventID)
					payload, admitted := event.Event.Event().PayloadAdmission()
					if err != nil || !found || !admitted || payload.Binding().BundleHash() != answerSource {
						t.Fatalf("answer publication did not use current source: found=%t admitted=%t source=%s err=%v", found, admitted, payload.Binding().BundleHash(), err)
					}
					snapshot := rt.Observe("target_story_current_execution", before.RunID, before.CardID, domain)
					completions := 0
					for _, row := range snapshot.API {
						if row.Key != domain.Key {
							continue
						}
						completions++
						var response map[string]any
						if err := json.Unmarshal([]byte(row.Response), &response); err != nil || row.Method != domain.Method || row.ActorKind != "operator_principal" || row.Actor != domain.Actor || row.Hash != domain.Hash || row.ResourceID != before.CardID || response["decision_event_id"] != decided.DecisionEventID {
							t.Fatalf("completion domain/answer drift: row=%+v err=%v", row, err)
						}
					}
					if completions != 1 {
						t.Fatalf("original answer completion count=%d", completions)
					}
				}
				assertAttribution()
				upgrade(answerSource, "completed")
				answer()
				captured := rt.Capture()
				calls, reply := captured.Calls, captured.Result
				wantCalls := 1
				if transport == "api" {
					wantCalls = 2
				}
				if calls != wantCalls {
					t.Fatalf("transport mutation calls=%d want=%d", calls, wantCalls)
				}
				if transport == "api" {
					raw, err := canonicaljson.Bytes(reply)
					if err != nil {
						t.Fatal(err)
					}
					var replay, original map[string]any
					if err := json.Unmarshal(raw, &replay); err != nil || replay["idempotency_replayed"] != true {
						t.Fatalf("API did not replay retained completion: %s error=%v", raw, err)
					}
					delete(replay, "idempotency_replayed")
					snapshot := rt.Observe("exact_replay_response", before.RunID, before.CardID, domain)
					for _, row := range snapshot.API {
						if row.Method == domain.Method && row.Actor == domain.Actor && row.Key == domain.Key && row.Hash == domain.Hash && row.ResourceID == before.CardID {
							if err := json.Unmarshal([]byte(row.Response), &original); err != nil {
								t.Fatal(err)
							}
						}
					}
					if original == nil || !reflect.DeepEqual(original, replay) {
						t.Fatal("API replay response differs from retained domain response")
					}
				}
				assertAttribution()
				seen := map[string]bool{}
				query := map[string]any{"filter": map[string]any{"run_id": before.RunID, "event_name": "mailbox.card_decided"}, "limit": 100}
				for {
					var page operatorread.OperatorEventListResult
					requireServedJSONRPCResult(t, rt.Endpoint, "event.list", query, &page)
					for _, event := range page.Events {
						if event.RunID != before.RunID || event.EventName != "mailbox.card_decided" || seen[event.EventID] {
							t.Fatalf("wrong/duplicate decision event: %+v", event)
						}
						seen[event.EventID] = true
					}
					if page.NextCursor == "" {
						break
					}
					query["cursor"] = page.NextCursor
				}
				if len(seen) != 1 {
					t.Fatalf("exact replay published another answer: events=%v", seen)
				}
				params["idempotency_key"] = uuid.NewString()
				if result := requestServedJSONRPC(t, rt.Endpoint, "mailbox.decide", params); result.Error == nil {
					t.Fatal("fresh request answered the completed wait")
				}
				rt.Join()
			})
		}
	}
}

func logGateUpgradeCauseTree(t *testing.T, cause error) {
	t.Helper()
	type branch struct {
		err  error
		path string
	}
	pending := []branch{{cause, "root"}}
	for visited := 0; len(pending) > 0 && visited < 16; visited++ {
		node := pending[0]
		pending = pending[1:]
		if node.err == nil {
			continue
		}
		line := fmt.Sprintf("branch=%s type=%T cause=%v", node.path, node.err, node.err)
		truncated := len(line) > 1024
		if truncated {
			line = line[:1024]
		}
		t.Logf("GATE_UPGRADE_CAUSE %s text_truncated=%t", line, truncated)
		var children []error
		switch wrapped := node.err.(type) {
		case interface{ Unwrap() []error }:
			children = wrapped.Unwrap()
		case interface{ Unwrap() error }:
			children = []error{wrapped.Unwrap()}
		}
		available := 15 - visited - len(pending)
		if len(children) > available {
			t.Logf("GATE_UPGRADE_CAUSE tree_truncated=true branch=%s omitted_children=%d", node.path, len(children)-available)
			children = children[:available]
		}
		for index, child := range children {
			pending = append(pending, branch{child, fmt.Sprintf("%s/%d", node.path, index)})
		}
	}
}
