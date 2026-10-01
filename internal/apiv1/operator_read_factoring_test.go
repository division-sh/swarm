package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

func TestOperatorReadFactoringCapabilityMatrix(t *testing.T) {
	for mask := 0; mask < 16; mask++ {
		t.Run(fmt.Sprint(mask), func(t *testing.T) {
			reads := &fakeAgentConversationReadStore{}
			opts := AgentConversationHandlerOptions{}
			want := []string{}
			if mask&1 != 0 {
				opts.Agents = reads
				want = append(want, "agent.list", "agent.get", "agent.diagnose", "agent.delivery_diagnostics")
			}
			if mask&2 != 0 {
				opts.Conversations = reads
				want = append(want, "conversation.list", "conversation.list_turns", "conversation.get_turn")
			}
			if mask&4 != 0 {
				opts.Usage = reads
				want = append(want, "agent.usage")
			}
			if mask&8 != 0 {
				opts.DeliveryLifecycle = reads
				want = append(want, "agent.delivery_lifecycle")
			}
			handlers := OperatorAgentConversationHandlers(opts)
			got := []string{}
			for name := range handlers {
				got = append(got, name)
			}
			sort.Strings(got)
			sort.Strings(want)
			if !reflect.DeepEqual(got, want) || (mask == 0 && handlers != nil) {
				t.Fatalf("mounted=%v want=%v nil=%v", got, want, handlers == nil)
			}
		})
	}
}

type factoringIdentityTrace struct {
	*fakeAgentConversationReadStore
	resolutions []string
}

func (s *factoringIdentityTrace) ResolveOperatorAgentIdentity(ctx context.Context, runID, agentID, instance string) (agentidentity.Identity, error) {
	s.resolutions = append(s.resolutions, runID+"/"+agentID+"/"+instance)
	return s.fakeAgentConversationReadStore.ResolveOperatorAgentIdentity(ctx, runID, agentID, instance)
}

func TestOperatorReadFactoringOrderCharacterization(t *testing.T) {
	const run = "11111111-1111-4111-8111-111111111111"
	for _, row := range []struct {
		method, invalidField string
		invalidValue         any
		identity             bool
	}{
		{"agent.list", "flow", 12, false},
		{"agent.get", "agent_id", "", true},
		{"agent.diagnose", "queue_limit", 0, true},
		{"agent.usage", "since", "not-a-time", true},
		{"agent.delivery_diagnostics", "failure_limit", 0, true},
		{"agent.delivery_lifecycle", "delivery_status", []string{"invalid"}, true},
		{"conversation.list", "limit", 0, true},
		{"conversation.list_turns", "limit", 0, false},
		{"conversation.get_turn", "turn_id", "", false},
	} {
		t.Run(row.method, func(t *testing.T) {
			readFailure := errors.New("selected read failed")
			reads := &factoringIdentityTrace{fakeAgentConversationReadStore: &fakeAgentConversationReadStore{
				listAgentsErr: readFailure, agentErr: readFailure, agentDiagnosisErr: readFailure,
				agentUsageErr: readFailure, agentDeliveryDiagnosticsErr: readFailure, agentDeliveryLifecycleErr: readFailure,
				listConversationsErr: readFailure, conversationTurnsErr: readFailure, conversationTurnErr: readFailure,
			}}
			h := OperatorAgentConversationHandlers(AgentConversationHandlerOptions{Agents: reads, Conversations: reads, Usage: reads, DeliveryLifecycle: reads})[row.method]
			params := map[string]any{"agent_id": "agent-1", "run_id": run, "flow_instance": "research/inst-1", "flow": "research", "role": "writer", "session_id": "session-1", "turn_id": "turn-1", "limit": 7, "queue_limit": 7, "failure_limit": 7, "dead_letter_limit": 8, "cursor": "opaque+/=", "queue_cursor": "queue+/=", "failure_cursor": "failure+/=", "dead_letter_cursor": "dead+/="}
			_, err := h(context.Background(), Request{Params: params})
			if !errors.Is(err, readFailure) {
				t.Fatalf("read failure was replaced: %v", err)
			}
			wantResolutions := []string(nil)
			if row.identity {
				wantResolutions = []string{run + "/agent-1/research/inst-1"}
			}
			if !reflect.DeepEqual(reads.resolutions, wantResolutions) {
				t.Fatalf("identity trace=%v want=%v", reads.resolutions, wantResolutions)
			}
			assertFactoringReadOptions(t, row.method, reads)
			reads.resolutions = nil
			params[row.invalidField] = row.invalidValue
			_, err = h(context.Background(), Request{Params: params})
			var invalid *InvalidParamsError
			if !errors.As(err, &invalid) || invalid.Details.(map[string]any)["field"] != row.invalidField {
				t.Fatalf("invalid field lost precedence: %v %#v", err, invalid)
			}
			// Agent-scoped options follow resolution; list filters and required IDs precede it.
			resolvedBeforeInvalid := row.identity && row.method != "agent.get" && row.method != "conversation.list"
			if (len(reads.resolutions) != 0) != resolvedBeforeInvalid {
				t.Fatalf("validation moved across identity: %v", reads.resolutions)
			}
		})
	}
}

func assertFactoringReadOptions(t *testing.T, method string, reads *factoringIdentityTrace) {
	t.Helper()
	switch method {
	case "agent.list":
		if reads.lastAgentList.Flow != "research" || reads.lastAgentList.Role != "writer" {
			t.Fatal(reads.lastAgentList)
		}
	case "agent.diagnose":
		if reads.lastAgentDiagnosisOptions.QueueLimit != 7 || reads.lastAgentDiagnosisOptions.QueueCursor != "queue+/=" {
			t.Fatal(reads.lastAgentDiagnosisOptions)
		}
	case "agent.delivery_diagnostics":
		if reads.lastAgentDeliveryDiagnosticsOptions != (operatorread.OperatorAgentDeliveryDiagnosticsOptions{FailureLimit: 7, DeadLetterLimit: 8, FailureCursor: "failure+/=", DeadLetterCursor: "dead+/="}) {
			t.Fatal(reads.lastAgentDeliveryDiagnosticsOptions)
		}
	case "agent.delivery_lifecycle":
		if reads.lastAgentDeliveryLifecycleOptions.Limit != 7 || reads.lastAgentDeliveryLifecycleOptions.Cursor != "opaque+/=" {
			t.Fatal(reads.lastAgentDeliveryLifecycleOptions)
		}
	case "conversation.list":
		if reads.lastConversationList.Limit != 7 || reads.lastConversationList.Cursor != "opaque+/=" || reads.lastConversationList.FlowInstance != "research/inst-1" {
			t.Fatal(reads.lastConversationList)
		}
	case "conversation.list_turns":
		if reads.lastConversationTurns != (operatorread.OperatorConversationTurnListOptions{SessionID: "session-1", Limit: 7, Cursor: "opaque+/="}) {
			t.Fatal(reads.lastConversationTurns)
		}
	case "conversation.get_turn":
		if reads.lastConversationTurnSessionID != "session-1" || reads.lastConversationTurnID != "turn-1" {
			t.Fatal("turn selection changed")
		}
	}
}

func TestOperatorReadFactoringIndependentConversationCapability(t *testing.T) {
	reads := &fakeAgentConversationReadStore{listConversationsResult: operatorread.OperatorConversationListResult{Conversations: []operatorread.OperatorConversationSummary{}}}
	h := OperatorAgentConversationHandlers(AgentConversationHandlerOptions{Conversations: reads})["conversation.list"]
	if _, err := h(context.Background(), Request{Params: map[string]any{}}); err != nil {
		t.Fatalf("unfiltered conversations require agents: %v", err)
	}
	_, err := h(context.Background(), Request{Params: map[string]any{"agent_id": "agent-1", "run_id": "11111111-1111-4111-8111-111111111111"}})
	if err == nil {
		t.Fatal("agent filter unexpectedly works without identity capability")
	}
}

func TestOperatorReadFactoringWireCharacterization(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	reads := &fakeAgentConversationReadStore{
		listAgentsResult:               operatorread.OperatorAgentListResult{Agents: []operatorread.OperatorAgentSummary{}},
		agentResult:                    operatorread.OperatorAgentDetail{Agent: operatorread.OperatorAgentSummary{AgentID: "reader", Status: "running"}, CurrentSessionRef: &operatorread.OperatorSessionRef{SessionID: "session", StartedAt: now}},
		agentDiagnosisResult:           operatorread.OperatorAgentDiagnosis{AgentID: "reader", Status: "running", Queue: operatorread.OperatorAgentDiagnosisQueue{PendingDeliveries: []operatorread.OperatorAgentPendingDelivery{}, NextCursor: "opaque+/="}},
		agentUsageResult:               operatorread.OperatorAgentUsage{AgentID: "reader", Breakdown: []operatorread.OperatorAgentUsageBreakdown{}},
		agentDeliveryDiagnosticsResult: operatorread.OperatorAgentDeliveryDiagnostics{AgentID: "reader", Failures: []operatorread.OperatorAgentDeliveryFailure{}, DeadLetters: []operatorread.OperatorAgentDeadLetterDelivery{}, FailuresNextCursor: "fail+/=", DeadLettersNextCursor: "dead+/="},
		agentDeliveryLifecycleResult:   operatorread.OperatorAgentDeliveryLifecycleList{AgentID: "reader", Deliveries: []operatorread.OperatorAgentDeliveryLifecycleRow{}, NextCursor: "delivery+/="},
		listConversationsResult:        operatorread.OperatorConversationListResult{Conversations: []operatorread.OperatorConversationSummary{}, NextCursor: "sessions+/="},
		conversationTurnsResult:        operatorread.OperatorConversationTurnListResult{Conversation: operatorread.OperatorConversationSummary{SessionID: "session", AgentID: "reader", StartedAt: now, Status: "active"}, Turns: []operatorread.OperatorConversationTurnListItem{}, NextCursor: "turns+/="},
		conversationTurnResult:         operatorread.OperatorPublicConversationTurnDetail{Session: operatorread.OperatorConversationSummary{SessionID: "session", AgentID: "reader", StartedAt: now, Status: "active"}, Turn: operatorread.OperatorPublicConversationTurn{TurnID: "turn", Activity: []operatorread.OperatorConversationActivity{}}},
	}
	handlers := OperatorAgentConversationHandlers(AgentConversationHandlerOptions{Agents: reads, Conversations: reads, Usage: reads, DeliveryLifecycle: reads})
	for method, want := range map[string]any{
		"agent.list": reads.listAgentsResult, "agent.get": reads.agentResult, "agent.diagnose": reads.agentDiagnosisResult,
		"agent.usage": reads.agentUsageResult, "agent.delivery_diagnostics": reads.agentDeliveryDiagnosticsResult,
		"agent.delivery_lifecycle": reads.agentDeliveryLifecycleResult, "conversation.list": reads.listConversationsResult,
		"conversation.list_turns": reads.conversationTurnsResult, "conversation.get_turn": reads.conversationTurnResult,
	} {
		t.Run(method, func(t *testing.T) {
			params := map[string]any{"run_id": "11111111-1111-4111-8111-111111111111", "agent_id": "reader", "session_id": "session", "turn_id": "turn"}
			got, err := handlers[method](context.Background(), Request{Params: params})
			if err != nil {
				t.Fatal(err)
			}
			gotJSON, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			wantJSON, err := json.Marshal(want)
			if err != nil || string(gotJSON) != string(wantJSON) || !reflect.DeepEqual(got, want) {
				t.Fatalf("owner wire changed: got=%s want=%s err=%v", gotJSON, wantJSON, err)
			}
		})
	}
}

func TestOperatorReadFactoringHTTPCharacterization(t *testing.T) {
	reads := &fakeAgentConversationReadStore{}
	handlers := OperatorAgentConversationHandlers(AgentConversationHandlerOptions{Agents: reads, Conversations: reads, Usage: reads, DeliveryLifecycle: reads})
	for method, original := range handlers {
		t.Run(method, func(t *testing.T) {
			hits := 0
			wrapped := map[string]MethodHandler{method: func(ctx context.Context, req Request) (any, error) {
				hits++
				return original(ctx, req)
			}}
			for _, auth := range []struct {
				name, header string
				tokens       []string
				status       int
			}{
				{"missing", "", []string{testToken}, http.StatusUnauthorized},
				{"wrong", "Bearer wrong", []string{testToken}, http.StatusUnauthorized},
				{"unconfigured", "Bearer " + testToken, nil, http.StatusServiceUnavailable},
			} {
				t.Run(auth.name, func(t *testing.T) {
					h := testHandler(t, Options{AuthTokens: auth.tokens, Handlers: wrapped})
					req := httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(fmt.Sprintf(`{"jsonrpc":"2.0","id":"read","method":%q,"params":{}}`, method)))
					req.Header.Set("Authorization", auth.header)
					rec := httptest.NewRecorder()
					h.ServeHTTP(rec, testAuthorActivityRequest(req))
					if rec.Code != auth.status || hits != 0 {
						t.Fatalf("auth admission: status=%d hits=%d body=%s", rec.Code, hits, rec.Body)
					}
				})
			}
			h := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: wrapped})
			response := rpcCall(t, h, fmt.Sprintf(`{"jsonrpc":"2.0","id":"read","method":%q,"params":{"unowned":"field"}}`, method))
			if response.Error == nil || response.Error.Code != codeInvalidParams || hits != 0 || response.JSONRPC != "2.0" || response.ID != "read" {
				t.Fatalf("schema must precede the owner: response=%#v hits=%d", response, hits)
			}
		})
	}
}
