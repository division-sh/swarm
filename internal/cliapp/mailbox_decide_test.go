package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

// Invariant: every card anchor uses the same authenticated CLI decision path,
// preserving the observed hash and declared input types without a chat account.
func TestMailboxDecideCommitsDeclaredOption(t *testing.T) {
	for _, kind := range []string{"stage_gate", "human_task", "proposed_effect"} {
		t.Run(kind, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			card := mailboxCardDetailResult("card-1")
			if kind == "human_task" {
				card = mailboxHumanTaskCardDetailResult("card-1")
			} else if kind == "proposed_effect" {
				card = mailboxProposedEffectCardDetailResult("card-1")
			}
			card["snapshot"] = map[string]any{
				"decision": "review", "title": "Review request", "context": map[string]any{"qa": "passed"},
				"outcomes": map[string]any{"reject": map[string]any{
					"Verdict": "reject", "Label": "Reject request", "InputOrder": []string{"reason", "count", "ratio", "confirmed", "at", "key"},
					"Input": map[string]any{
						"reason":    map[string]any{"type": "text", "label": "Reason", "required": true},
						"count":     map[string]any{"type": "integer", "label": "Count", "required": true},
						"ratio":     map[string]any{"type": "numeric", "label": "Ratio", "required": false},
						"confirmed": map[string]any{"type": "boolean", "label": "Confirmed", "required": false},
						"at":        map[string]any{"type": "timestamp", "label": "At", "required": false},
						"key":       map[string]any{"type": "uuid", "label": "Key", "required": false},
					},
				}},
			}
			detail := map[string]any{"kind": "decision_card", "decision_card": card}
			if kind == "proposed_effect" {
				detail["effect"] = mailboxProposedEffectStateResult()
			}
			var calls []string
			var params map[string]any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer test-token" {
					t.Errorf("missing authenticated operator request")
				}
				var req jsonRPCRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
					return
				}
				calls = append(calls, req.Method)
				if req.Method == "mailbox.list" {
					writeJSONRPCResult(t, w, req.ID, map[string]any{"items": []any{map[string]any{"kind": "decision_card", "decision_card": card, "effect": detail["effect"]}}})
					return
				}
				if req.Method == "mailbox.get" {
					writeJSONRPCResult(t, w, req.ID, detail)
					return
				}
				params = req.Params
				writeJSONRPCResult(t, w, req.ID, map[string]any{"ok": true, "card_id": "card-1", "status": "decided", "change_id": 1, "idempotency_replayed": false})
			}))
			defer server.Close()
			args := []string{"mailbox", "decide", "card-1", "reject", "--input", "reason=not today=x", "--input", "count=2", "--input", "ratio=1.5", "--input", "confirmed=false", "--input", "at=2026-10-22T12:00:00Z", "--input", "key=11111111-1111-4111-8111-111111111111"}
			args = append(args, "--api-server", server.URL)
			if err := validateCLIAPIConnectionFlagPlacement(args); err != nil {
				t.Fatal(err)
			}
			var out, errOut bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &out, &errOut, testRootCommandOptions(server))
			wantFields := map[string]any{"reason": "not today=x", "count": float64(2), "ratio": 1.5, "confirmed": false, "at": "2026-10-22T12:00:00Z", "key": "11111111-1111-4111-8111-111111111111"}
			if code != 0 || out.String() != "Decided: reject (card card-1)\n" || !reflect.DeepEqual(calls, []string{"mailbox.get", "mailbox.decide"}) || !reflect.DeepEqual(params["fields"], wantFields) || params["observed_content_hash"] != card["card_content_hash"] || params["verdict"] != "reject" {
				t.Fatalf("code=%d out=%q stderr=%s calls=%v params=%#v", code, out.String(), errOut.String(), calls, params)
			}
			for _, command := range [][]string{{"mailbox", "view", "card-1"}, {"mailbox", "list"}} {
				out.Reset()
				if code := executeRootCommandWithOptions(context.Background(), t.TempDir(), command, &out, &errOut, testRootCommandOptions(server)); code != 0 {
					t.Fatalf("view code=%d stderr=%s", code, errOut.String())
				}
				for _, want := range []string{"question=Review request", "qa", "passed", "Option: reject - Reject request", "--input reason=<text> (required)", "instance=root", "age="} {
					if !strings.Contains(out.String(), want) {
						t.Fatalf("view missing %q: %s", want, out.String())
					}
				}
			}
		})
	}
}

// Invariant: an already committed card reports its recorded verdict and time,
// without authorizing another decision, including a concurrent winning decision.
func TestMailboxDecideAlreadyDecided(t *testing.T) {
	for _, raced := range []bool{false, true} {
		t.Run(fmt.Sprint(raced), func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			reads, writes := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				if req.Method == "mailbox.decide" {
					writes++
					fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%q,"error":{"code":-32000,"message":"decided","data":{"code":"MAILBOX_ALREADY_DECIDED"}}}`, req.ID)
					return
				}
				reads++
				card := mailboxCardDetailResult("card-1")
				if !raced || reads > 1 {
					card["status"], card["verdict"], card["decided_at"] = "decided", "approve", "2026-10-22T12:00:00Z"
				}
				writeJSONRPCResult(t, w, req.ID, map[string]any{"kind": "decision_card", "decision_card": card})
			}))
			defer server.Close()
			var out, errOut bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"mailbox", "decide", "card-1", "approve"}, &out, &errOut, testRootCommandOptions(server))
			wantWrites := 0
			if raced {
				wantWrites = 1
			}
			if code != 0 || writes != wantWrites || out.String() != "already decided: approve at 2026-10-22T12:00:00Z\n" {
				t.Fatalf("code=%d writes=%d out=%q stderr=%s", code, writes, out.String(), errOut.String())
			}
		})
	}
}

// Invariant: stale content is never retried and invalid declared input cannot
// reach the mutation endpoint; both refusals teach the operator what to correct.
func TestMailboxDecideRefusesStaleAndInvalidInput(t *testing.T) {
	for _, tc := range []struct {
		name, option string
		input        []string
		want         string
		writes       int
	}{
		{"stale", "approve", nil, "the card changed, view it again", 1},
		{"option", "absent", nil, "absent", 0},
		{"unknown", "approve", []string{"other=x"}, "other", 0},
		{"duplicate", "approve", []string{"count=2", "count=3"}, "count", 0},
		{"invalid", "approve", []string{"count=no"}, "count", 0},
		{"missing", "reject", nil, "reason", 0},
		{"malformed", "approve", []string{"count"}, "key=value", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			writes := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				if req.Method == "mailbox.decide" {
					writes++
					fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%q,"error":{"code":-32000,"message":"stale","data":{"code":"MAILBOX_STALE_CARD"}}}`, req.ID)
					return
				}
				card := mailboxCardDetailResult("card-1")
				card["snapshot"].(map[string]any)["outcomes"] = map[string]any{
					"approve": map[string]any{"Verdict": "approve", "Label": "Approve", "Input": map[string]any{"count": map[string]any{"type": "integer", "label": "Count", "required": false}}, "InputOrder": []string{"count"}},
					"reject":  map[string]any{"Verdict": "reject", "Label": "Reject", "Input": map[string]any{"reason": map[string]any{"type": "text", "label": "Reason", "required": true}}, "InputOrder": []string{"reason"}},
				}
				writeJSONRPCResult(t, w, req.ID, map[string]any{"kind": "decision_card", "decision_card": card})
			}))
			defer server.Close()
			args := []string{"mailbox", "decide", "card-1", tc.option}
			for _, input := range tc.input {
				args = append(args, "--input", input)
			}
			var out, errOut bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &out, &errOut, testRootCommandOptions(server))
			if code == 0 || writes != tc.writes || !strings.Contains(errOut.String(), tc.want) || out.Len() != 0 {
				t.Fatalf("code=%d writes=%d out=%q stderr=%s", code, writes, out.String(), errOut.String())
			}
		})
	}
}
