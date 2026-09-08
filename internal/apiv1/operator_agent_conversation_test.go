package apiv1

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	operatorread "github.com/division-sh/swarm/internal/operatorread"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

type fakeAgentConversationReadStore struct {
	listAgentsResult                     operatorread.OperatorAgentListResult
	listAgentsErr                        error
	agentResult                          operatorread.OperatorAgentDetail
	agentErr                             error
	agentDiagnosisResult                 operatorread.OperatorAgentDiagnosis
	agentDiagnosisErr                    error
	agentUsageResult                     operatorread.OperatorAgentUsage
	agentUsageErr                        error
	agentDeliveryLifecycleResult         operatorread.OperatorAgentDeliveryLifecycleList
	agentDeliveryLifecycleErr            error
	agentDeliveryDiagnosticsResult       operatorread.OperatorAgentDeliveryDiagnostics
	agentDeliveryDiagnosticsErr          error
	listConversationsResult              operatorread.OperatorConversationListResult
	listConversationsErr                 error
	conversationTurnsResult              operatorread.OperatorConversationTurnListResult
	conversationTurnsErr                 error
	conversationTurnResult               operatorread.OperatorPublicConversationTurnDetail
	conversationTurnErr                  error
	lastAgentList                        operatorread.OperatorAgentListOptions
	lastConversationList                 operatorread.OperatorConversationListOptions
	lastResolveAgentID                   string
	lastResolveFlowInstance              string
	resolveIdentity                      agentidentity.Identity
	resolveErr                           error
	lastAgentIdentity                    agentidentity.Identity
	lastAgentDiagnosisIdentity           agentidentity.Identity
	lastAgentDiagnosisOptions            operatorread.OperatorAgentDiagnosisOptions
	lastAgentUsageIdentity               agentidentity.Identity
	lastAgentUsageOptions                operatorread.OperatorAgentUsageOptions
	lastAgentDeliveryLifecycleIdentity   agentidentity.Identity
	lastAgentDeliveryLifecycleOptions    operatorread.OperatorAgentDeliveryLifecycleOptions
	lastAgentDeliveryDiagnosticsIdentity agentidentity.Identity
	lastAgentDeliveryDiagnosticsOptions  operatorread.OperatorAgentDeliveryDiagnosticsOptions
	lastConversationTurns                operatorread.OperatorConversationTurnListOptions
	lastConversationTurnSessionID        string
	lastConversationTurnID               string
}

func (s *fakeAgentConversationReadStore) ResolveOperatorAgentIdentity(_ context.Context, runID, agentID, flowInstance string) (agentidentity.Identity, error) {
	s.lastResolveAgentID = agentID
	s.lastResolveFlowInstance = flowInstance
	if s.resolveErr != nil {
		return agentidentity.Identity{}, s.resolveErr
	}
	if !s.resolveIdentity.IsZero() {
		return s.resolveIdentity, nil
	}
	name, err := agentidentity.DeclaredName(agentID, "test-bundle")
	if err != nil {
		return agentidentity.Identity{}, err
	}
	route, err := agentidentity.PresentRoute("research", "inst-1", "research/inst-1")
	if err != nil {
		return agentidentity.Identity{}, err
	}
	return agentidentity.New(runID, name, route)
}

func (s *fakeAgentConversationReadStore) ListOperatorAgents(_ context.Context, opts operatorread.OperatorAgentListOptions) (operatorread.OperatorAgentListResult, error) {
	s.lastAgentList = opts
	return s.listAgentsResult, s.listAgentsErr
}

func (s *fakeAgentConversationReadStore) LoadOperatorAgent(_ context.Context, identity agentidentity.Identity) (operatorread.OperatorAgentDetail, error) {
	s.lastAgentIdentity = identity
	return s.agentResult, s.agentErr
}

func (s *fakeAgentConversationReadStore) LoadOperatorAgentDiagnosis(_ context.Context, identity agentidentity.Identity, opts operatorread.OperatorAgentDiagnosisOptions) (operatorread.OperatorAgentDiagnosis, error) {
	s.lastAgentDiagnosisIdentity = identity
	s.lastAgentDiagnosisOptions = opts
	return s.agentDiagnosisResult, s.agentDiagnosisErr
}

func (s *fakeAgentConversationReadStore) LoadOperatorAgentUsage(_ context.Context, identity agentidentity.Identity, opts operatorread.OperatorAgentUsageOptions) (operatorread.OperatorAgentUsage, error) {
	s.lastAgentUsageIdentity = identity
	s.lastAgentUsageOptions = opts
	return s.agentUsageResult, s.agentUsageErr
}

func (s *fakeAgentConversationReadStore) LoadOperatorAgentDeliveryLifecycle(_ context.Context, identity agentidentity.Identity, opts operatorread.OperatorAgentDeliveryLifecycleOptions) (operatorread.OperatorAgentDeliveryLifecycleList, error) {
	s.lastAgentDeliveryLifecycleIdentity = identity
	s.lastAgentDeliveryLifecycleOptions = opts
	return s.agentDeliveryLifecycleResult, s.agentDeliveryLifecycleErr
}

func (s *fakeAgentConversationReadStore) LoadOperatorAgentDeliveryDiagnostics(_ context.Context, identity agentidentity.Identity, opts operatorread.OperatorAgentDeliveryDiagnosticsOptions) (operatorread.OperatorAgentDeliveryDiagnostics, error) {
	s.lastAgentDeliveryDiagnosticsIdentity = identity
	s.lastAgentDeliveryDiagnosticsOptions = opts
	return s.agentDeliveryDiagnosticsResult, s.agentDeliveryDiagnosticsErr
}

func (s *fakeAgentConversationReadStore) ListOperatorConversations(_ context.Context, opts operatorread.OperatorConversationListOptions) (operatorread.OperatorConversationListResult, error) {
	s.lastConversationList = opts
	return s.listConversationsResult, s.listConversationsErr
}

func (s *fakeAgentConversationReadStore) ListOperatorConversationTurns(_ context.Context, opts operatorread.OperatorConversationTurnListOptions) (operatorread.OperatorConversationTurnListResult, error) {
	s.lastConversationTurns = opts
	return s.conversationTurnsResult, s.conversationTurnsErr
}

func (s *fakeAgentConversationReadStore) LoadOperatorPublicConversationTurn(_ context.Context, sessionID, turnID string) (operatorread.OperatorPublicConversationTurnDetail, error) {
	s.lastConversationTurnSessionID = sessionID
	s.lastConversationTurnID = turnID
	return s.conversationTurnResult, s.conversationTurnErr
}

func TestOperatorAgentConversationHandlersExposeReadOwner(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	reads := &fakeAgentConversationReadStore{
		listAgentsResult: operatorread.OperatorAgentListResult{Agents: []operatorread.OperatorAgentSummary{{
			AgentID:      "agent-1",
			Role:         "researcher",
			Type:         "managed",
			Model:        "cheap",
			Memory:       true,
			MemorySource: "authored",
			FlowInstance: "research/inst-1",
			Status:       "running",
		}}},
		agentResult: operatorread.OperatorAgentDetail{Agent: operatorread.OperatorAgentSummary{AgentID: "agent-1", Role: "researcher"}},
		agentDiagnosisResult: operatorread.OperatorAgentDiagnosis{
			AgentID: "agent-1",
			Status:  "running",
			Queue: operatorread.OperatorAgentDiagnosisQueue{
				PendingCount:            2,
				OldestPendingAgeSeconds: 45,
				PendingDeliveries: []operatorread.OperatorAgentPendingDelivery{{
					DeliveryID: "delivery-1",
					EventID:    "event-1",
					EventName:  "task.ready",
					EnqueuedAt: time.Date(2026, 5, 21, 10, 0, 0, 0, time.UTC),
					Attempts:   1,
				}},
				NextCursor: "cursor-2",
			},
			DeliveryLifecycle: &operatorread.OperatorAgentDeliveryLifecycle{
				State:         "active",
				BlockingLayer: "session_execution",
			},
			Active: &operatorread.OperatorAgentDiagnosisActive{
				TurnID:   "22222222-2222-2222-2222-222222222222",
				TaskID:   "task-1",
				EntityID: "33333333-3333-3333-3333-333333333333",
			},
			LastToolOutcome: &operatorread.OperatorAgentLastToolOutcome{
				TurnID:    "22222222-2222-2222-2222-222222222222",
				ToolName:  "selected_tool",
				ToolUseID: "toolu-selected",
				OK:        true,
			},
			RuntimeState: &operatorread.OperatorAgentDiagnosisRuntimeState{
				Watchdog: &operatorread.OperatorAgentDiagnosisWatchdog{
					State:         "no_output",
					BlockingLayer: "session_execution",
					Action:        "session_no_output",
					Outcome:       "warning_emitted",
					RecordedAt:    "2026-05-21T10:01:00Z",
				},
			},
		},
		agentUsageResult: operatorread.OperatorAgentUsage{
			AgentID: "agent-1",
			Window: operatorread.OperatorAgentUsageWindow{
				Since: ptrTime(now.Add(-time.Hour)),
				Until: ptrTime(now),
			},
			Usage: operatorread.OperatorAgentUsageByAccounting{
				Exact: operatorread.OperatorAgentUsageTotals{
					LedgerEntries:    1,
					InputTokens:      100,
					OutputTokens:     25,
					EstimatedCostUSD: 0.000675,
				},
				Estimated: operatorread.OperatorAgentUsageTotals{
					LedgerEntries:    1,
					InputTokens:      50,
					OutputTokens:     10,
					EstimatedCostUSD: 0.000300,
				},
			},
			Breakdown: []operatorread.OperatorAgentUsageBreakdown{{
				ExecutionMode:   "live",
				UsageAccounting: operatorread.AgentUsageAccountingExact,
				InvocationType:  "anthropic",
				Model:           "claude-3-5-sonnet",
				ModelAlias:      "regular",
				BackendProfile:  "anthropic",
				Provider:        "anthropic",
				Transport:       "api",
				ResolvedModel:   "claude-3-5-sonnet",
				CostDisplay:     "$0.000675 estimated",
				Totals: operatorread.OperatorAgentUsageTotals{
					LedgerEntries:    1,
					InputTokens:      100,
					OutputTokens:     25,
					EstimatedCostUSD: 0.000675,
				},
			}, {
				ExecutionMode:   "live",
				UsageAccounting: operatorread.AgentUsageAccountingEstimated,
				InvocationType:  "claude_cli",
				Model:           "sonnet",
				ModelAlias:      "regular",
				BackendProfile:  "claude_cli",
				Provider:        "claude",
				Transport:       "cli",
				ResolvedModel:   "sonnet",
				CostDisplay:     "$0.000300 estimated",
				Totals: operatorread.OperatorAgentUsageTotals{
					LedgerEntries:    1,
					InputTokens:      50,
					OutputTokens:     10,
					EstimatedCostUSD: 0.000300,
				},
			}},
		},
		agentDeliveryLifecycleResult: operatorread.OperatorAgentDeliveryLifecycleList{
			AgentID: "agent-1",
			Deliveries: []operatorread.OperatorAgentDeliveryLifecycleRow{{
				DeliveryID:        "delivery-lifecycle-1",
				EventID:           "event-lifecycle-1",
				EventName:         "task.pending",
				RunID:             "11111111-1111-1111-1111-111111111111",
				EntityID:          "33333333-3333-3333-3333-333333333333",
				Status:            "pending",
				RetryCount:        1,
				ReasonCode:        "retry_scheduled",
				Failure:           testFailure("temporary_failure"),
				DeliveryCreatedAt: now.Add(-3 * time.Minute),
			}},
			NextCursor: "lifecycle-next",
		},
		agentDeliveryDiagnosticsResult: operatorread.OperatorAgentDeliveryDiagnostics{
			AgentID: "agent-1",
			Summary: operatorread.OperatorAgentDeliveryDiagnosticsSummary{
				Failures24h:    1,
				DeadLetters24h: 1,
			},
			Failures: []operatorread.OperatorAgentDeliveryFailure{{
				DeliveryID: "delivery-failed-1",
				EventID:    "event-failed-1",
				EventName:  "task.failed",
				RunID:      "run-1",
				EntityID:   "entity-1",
				Status:     "failed",
				ReasonCode: "handler_error",
				Failure:    testFailure("handler_failed"),
				RetryCount: 2,
				OccurredAt: now.Add(-time.Minute),
			}},
			FailuresNextCursor: "failure-next",
			DeadLetters: []operatorread.OperatorAgentDeadLetterDelivery{{
				DeliveryID: "delivery-dead-1",
				EventID:    "event-dead-1",
				EventName:  "task.dead",
				RunID:      "run-1",
				EntityID:   "entity-1",
				Status:     "dead_letter",
				ReasonCode: "retry_exhausted",
				Failure:    testFailure("retry_exhausted"),
				RetryCount: 3,
				OccurredAt: now.Add(-2 * time.Minute),
				DeadLetterRecords: []operatorread.OperatorDeadLetterRecord{{
					DeadLetterID: "dead-letter-1",
					Failure:      *testFailure("retry_exhausted"),
					RetryCount:   3,
					ChainDepth:   0,
					HandlerNode:  "agent-1",
					CreatedAt:    now.Add(-time.Minute),
				}},
			}},
			DeadLettersNextCursor: "dead-letter-next",
		},
		listConversationsResult: operatorread.OperatorConversationListResult{
			Conversations: []operatorread.OperatorConversationSummary{{
				SessionID:    "sess-1",
				AgentID:      "agent-1",
				RunID:        "run-1",
				StartedAt:    now,
				TurnCount:    1,
				MessageCount: 2,
				Status:       "active",
			}},
			NextCursor: "next",
		},
		conversationTurnsResult: operatorread.OperatorConversationTurnListResult{
			Conversation: operatorread.OperatorConversationSummary{SessionID: "sess-1", AgentID: "agent-1", StartedAt: now, Status: "active"},
			Turns: []operatorread.OperatorConversationTurnListItem{{
				TurnID: "turn-1", Ordinal: 1, CompletedAt: now, DurationMS: 25,
				TriggerEventID: "evt-1", TriggerEventType: "task.started", ParseOK: true,
				ActivityCounts: operatorread.OperatorConversationActivityCounts{Tool: 1},
			}},
			NextCursor: "turn-cursor-2",
		},
		conversationTurnResult: operatorread.OperatorPublicConversationTurnDetail{
			Session: operatorread.OperatorConversationSummary{SessionID: "sess-1", AgentID: "agent-1", StartedAt: now, Status: "active"},
			Turn: operatorread.OperatorPublicConversationTurn{
				TurnID: "turn-1", Ordinal: 1, CompletedAt: now.Add(time.Second), ParseOK: true,
				Activity: []operatorread.OperatorConversationActivity{},
			},
		},
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			AgentConversations:     reads,
			AgentDeliveryLifecycle: reads,
			AgentUsage:             reads,
		}),
	})

	listAgents := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"agents","method":"agent.list","params":{"flow":"flow/a","role":"researcher"}}`)
	if listAgents.Error != nil {
		t.Fatalf("agent.list error = %#v", listAgents.Error)
	}
	if reads.lastAgentList.Flow != "flow/a" || reads.lastAgentList.Role != "researcher" {
		t.Fatalf("agent.list options = %#v", reads.lastAgentList)
	}
	listResult := asMap(t, listAgents.Result)
	agents, ok := listResult["agents"].([]any)
	if !ok || len(agents) != 1 {
		t.Fatalf("agent.list result = %#v", listResult)
	}
	assertUnsupportedAgentMetricStubsAbsent(t, asMap(t, agents[0]))

	getAgent := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"agent","method":"agent.get","params":{"run_id":"11111111-1111-4111-8111-111111111111","agent_id":"agent-1","flow_instance":"research/inst-1"}}`)
	if getAgent.Error != nil {
		t.Fatalf("agent.get error = %#v", getAgent.Error)
	}
	if reads.lastResolveAgentID != "agent-1" || reads.lastResolveFlowInstance != "research/inst-1" {
		t.Fatalf("agent.get selector = %q/%q", reads.lastResolveAgentID, reads.lastResolveFlowInstance)
	}
	if reads.lastAgentIdentity.AgentID() != "agent-1" || reads.lastAgentIdentity.FlowInstance() != "research/inst-1" {
		t.Fatalf("agent.get identity = %#v", reads.lastAgentIdentity)
	}
	agentResult := asMap(t, getAgent.Result)
	agent := asMap(t, agentResult["agent"])
	assertUnsupportedAgentMetricStubsAbsent(t, agent)
	for _, splitField := range []string{"queue", "delivery_lifecycle", "runtime_state", "last_tool_outcome"} {
		if _, ok := agent[splitField]; ok {
			t.Fatalf("agent.get unexpectedly exposed diagnosis field %q: %#v", splitField, agent)
		}
	}

	diagnoseAgent := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"diagnose","method":"agent.diagnose","params":{"run_id":"11111111-1111-4111-8111-111111111111","agent_id":"agent-1","queue_limit":1,"queue_cursor":"cursor-1"}}`)
	if diagnoseAgent.Error != nil {
		t.Fatalf("agent.diagnose error = %#v", diagnoseAgent.Error)
	}
	if reads.lastAgentDiagnosisIdentity.AgentID() != "agent-1" {
		t.Fatalf("agent.diagnose identity = %#v", reads.lastAgentDiagnosisIdentity)
	}
	if reads.lastAgentDiagnosisOptions.QueueLimit != 1 || reads.lastAgentDiagnosisOptions.QueueCursor != "cursor-1" {
		t.Fatalf("agent.diagnose options = %#v", reads.lastAgentDiagnosisOptions)
	}
	diagnosis := asMap(t, diagnoseAgent.Result)
	if diagnosis["agent_id"] != "agent-1" || diagnosis["status"] != "running" {
		t.Fatalf("agent.diagnose identity/status = %#v", diagnosis)
	}
	queue := asMap(t, diagnosis["queue"])
	if queue["pending_count"] != float64(2) || queue["oldest_pending_age_seconds"] != float64(45) {
		t.Fatalf("agent.diagnose queue = %#v", queue)
	}
	deliveries, ok := queue["pending_deliveries"].([]any)
	if !ok || len(deliveries) != 1 {
		t.Fatalf("agent.diagnose pending_deliveries = %#v", queue["pending_deliveries"])
	}
	delivery := asMap(t, deliveries[0])
	if delivery["event_id"] != "event-1" || delivery["event_name"] != "task.ready" || delivery["attempts"] != float64(1) {
		t.Fatalf("agent.diagnose pending delivery = %#v", delivery)
	}
	if queue["next_cursor"] != "cursor-2" {
		t.Fatalf("agent.diagnose next_cursor = %#v", queue["next_cursor"])
	}
	lifecycle := asMap(t, diagnosis["delivery_lifecycle"])
	if lifecycle["state"] != "active" || lifecycle["blocking_layer"] != "session_execution" {
		t.Fatalf("agent.diagnose lifecycle = %#v", lifecycle)
	}
	active := asMap(t, diagnosis["active"])
	if active["turn_id"] != "22222222-2222-2222-2222-222222222222" || active["task_id"] != "task-1" || active["entity_id"] != "33333333-3333-3333-3333-333333333333" {
		t.Fatalf("agent.diagnose active = %#v", active)
	}
	runtimeState := asMap(t, diagnosis["runtime_state"])
	watchdog := asMap(t, runtimeState["watchdog"])
	if watchdog["state"] != "no_output" || watchdog["blocking_layer"] != "session_execution" || watchdog["action"] != "session_no_output" || watchdog["outcome"] != "warning_emitted" {
		t.Fatalf("agent.diagnose runtime_state.watchdog = %#v", watchdog)
	}
	if watchdog["recorded_at"] != "2026-05-21T10:01:00Z" {
		t.Fatalf("agent.diagnose runtime_state.watchdog.recorded_at = %#v", watchdog["recorded_at"])
	}
	lastTool := asMap(t, diagnosis["last_tool_outcome"])
	if lastTool["turn_id"] != "22222222-2222-2222-2222-222222222222" || lastTool["tool_name"] != "selected_tool" || lastTool["tool_use_id"] != "toolu-selected" || lastTool["ok"] != true {
		t.Fatalf("agent.diagnose last_tool_outcome = %#v", lastTool)
	}
	if _, ok := lastTool["result"]; ok {
		t.Fatalf("agent.diagnose leaked raw last_tool_outcome.result: %#v", lastTool)
	}
	for _, splitField := range []string{"bundle_version", "watchdog", "token_usage", "failures_recent", "dead_letters_recent"} {
		if _, ok := diagnosis[splitField]; ok {
			t.Fatalf("agent.diagnose exposed split field %q: %#v", splitField, diagnosis)
		}
	}

	usageResp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"agent-usage","method":"agent.usage","params":{"run_id":"11111111-1111-4111-8111-111111111111","agent_id":"agent-1","since":"2026-05-21T09:00:00Z","until":"2026-05-21T10:00:00Z"}}`)
	if usageResp.Error != nil {
		t.Fatalf("agent.usage error = %#v", usageResp.Error)
	}
	if reads.lastAgentUsageIdentity.AgentID() != "agent-1" {
		t.Fatalf("agent.usage identity = %#v", reads.lastAgentUsageIdentity)
	}
	if reads.lastAgentUsageOptions.Since == nil || reads.lastAgentUsageOptions.Until == nil {
		t.Fatalf("agent.usage options missing window = %#v", reads.lastAgentUsageOptions)
	}
	usage := asMap(t, usageResp.Result)
	if usage["agent_id"] != "agent-1" {
		t.Fatalf("agent.usage agent_id = %#v", usage["agent_id"])
	}
	totals := asMap(t, usage["usage"])
	exactTotals := asMap(t, totals["exact"])
	estimatedTotals := asMap(t, totals["estimated"])
	if exactTotals["input_tokens"] != float64(100) || estimatedTotals["input_tokens"] != float64(50) {
		t.Fatalf("agent.usage exact/estimated totals = %#v", totals)
	}
	breakdown, ok := usage["breakdown"].([]any)
	if !ok || len(breakdown) != 2 {
		t.Fatalf("agent.usage breakdown = %#v", usage["breakdown"])
	}
	if first := asMap(t, breakdown[0]); first["usage_accounting"] != "exact" || first["invocation_type"] != "anthropic" || first["model_alias"] != "regular" || first["backend_profile"] != "anthropic" || first["provider"] != "anthropic" || first["transport"] != "api" || first["resolved_model"] != "claude-3-5-sonnet" {
		t.Fatalf("agent.usage first breakdown = %#v", first)
	}
	for _, forbidden := range []string{"token_usage", "run_id", "session_id", "turn_id"} {
		if _, ok := usage[forbidden]; ok {
			t.Fatalf("agent.usage exposed forbidden field %q: %#v", forbidden, usage)
		}
	}

	deliveryLifecycle := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"agent-delivery-lifecycle","method":"agent.delivery_lifecycle","params":{"agent_id":"agent-1","run_id":"11111111-1111-1111-1111-111111111111","delivery_status":["pending","delivered"],"limit":1,"cursor":"lifecycle-cursor-1"}}`)
	if deliveryLifecycle.Error != nil {
		t.Fatalf("agent.delivery_lifecycle error = %#v", deliveryLifecycle.Error)
	}
	if reads.lastAgentDeliveryLifecycleIdentity.AgentID() != "agent-1" {
		t.Fatalf("agent.delivery_lifecycle identity = %#v", reads.lastAgentDeliveryLifecycleIdentity)
	}
	if reads.lastAgentDeliveryLifecycleOptions.RunID != "11111111-1111-1111-1111-111111111111" || reads.lastAgentDeliveryLifecycleOptions.Limit != 1 || reads.lastAgentDeliveryLifecycleOptions.Cursor != "lifecycle-cursor-1" {
		t.Fatalf("agent.delivery_lifecycle options = %#v", reads.lastAgentDeliveryLifecycleOptions)
	}
	if got := reads.lastAgentDeliveryLifecycleOptions.Statuses; len(got) != 2 || got[0] != "pending" || got[1] != "delivered" {
		t.Fatalf("agent.delivery_lifecycle statuses = %#v", got)
	}
	lifecycleList := asMap(t, deliveryLifecycle.Result)
	if lifecycleList["agent_id"] != "agent-1" || lifecycleList["next_cursor"] != "lifecycle-next" {
		t.Fatalf("agent.delivery_lifecycle result identity/cursor = %#v", lifecycleList)
	}
	lifecycleRows, ok := lifecycleList["deliveries"].([]any)
	if !ok || len(lifecycleRows) != 1 {
		t.Fatalf("agent.delivery_lifecycle deliveries = %#v", lifecycleList["deliveries"])
	}
	lifecycleRow := asMap(t, lifecycleRows[0])
	if lifecycleRow["delivery_id"] != "delivery-lifecycle-1" || lifecycleRow["event_id"] != "event-lifecycle-1" || lifecycleRow["status"] != "pending" || lifecycleRow["retry_count"] != float64(1) {
		t.Fatalf("agent.delivery_lifecycle row = %#v", lifecycleRow)
	}
	for _, forbidden := range []string{"dead_letter_records", "failures", "dead_letters", "summary"} {
		if _, ok := lifecycleRow[forbidden]; ok {
			t.Fatalf("agent.delivery_lifecycle exposed diagnostics field %q: %#v", forbidden, lifecycleRow)
		}
	}

	deliveryDiagnostics := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"agent-delivery-diagnostics","method":"agent.delivery_diagnostics","params":{"run_id":"11111111-1111-1111-1111-111111111111","agent_id":"agent-1","failure_limit":1,"failure_cursor":"failure-cursor-1","dead_letter_limit":1,"dead_letter_cursor":"dead-letter-cursor-1"}}`)
	if deliveryDiagnostics.Error != nil {
		t.Fatalf("agent.delivery_diagnostics error = %#v", deliveryDiagnostics.Error)
	}
	if reads.lastAgentDeliveryDiagnosticsIdentity.AgentID() != "agent-1" {
		t.Fatalf("agent.delivery_diagnostics identity = %#v", reads.lastAgentDeliveryDiagnosticsIdentity)
	}
	if reads.lastAgentDeliveryDiagnosticsOptions.FailureLimit != 1 || reads.lastAgentDeliveryDiagnosticsOptions.FailureCursor != "failure-cursor-1" ||
		reads.lastAgentDeliveryDiagnosticsOptions.DeadLetterLimit != 1 || reads.lastAgentDeliveryDiagnosticsOptions.DeadLetterCursor != "dead-letter-cursor-1" {
		t.Fatalf("agent.delivery_diagnostics options = %#v", reads.lastAgentDeliveryDiagnosticsOptions)
	}
	diagnostics := asMap(t, deliveryDiagnostics.Result)
	if diagnostics["agent_id"] != "agent-1" {
		t.Fatalf("agent.delivery_diagnostics agent_id = %#v", diagnostics["agent_id"])
	}
	summary := asMap(t, diagnostics["summary"])
	if summary["failures_24h"] != float64(1) || summary["dead_letters_24h"] != float64(1) {
		t.Fatalf("agent.delivery_diagnostics summary = %#v", summary)
	}
	failures, ok := diagnostics["failures"].([]any)
	if !ok || len(failures) != 1 {
		t.Fatalf("agent.delivery_diagnostics failures = %#v", diagnostics["failures"])
	}
	failure := asMap(t, failures[0])
	if failure["delivery_id"] != "delivery-failed-1" || failure["status"] != "failed" || failure["retry_count"] != float64(2) {
		t.Fatalf("agent.delivery_diagnostics failure = %#v", failure)
	}
	deadLetters, ok := diagnostics["dead_letters"].([]any)
	if !ok || len(deadLetters) != 1 {
		t.Fatalf("agent.delivery_diagnostics dead_letters = %#v", diagnostics["dead_letters"])
	}
	deadLetter := asMap(t, deadLetters[0])
	if deadLetter["delivery_id"] != "delivery-dead-1" || deadLetter["status"] != "dead_letter" || deadLetter["retry_count"] != float64(3) {
		t.Fatalf("agent.delivery_diagnostics dead_letter = %#v", deadLetter)
	}
	records, ok := deadLetter["dead_letter_records"].([]any)
	if !ok || len(records) != 1 || asMap(t, records[0])["dead_letter_id"] != "dead-letter-1" {
		t.Fatalf("agent.delivery_diagnostics dead_letter_records = %#v", deadLetter["dead_letter_records"])
	}
	if diagnostics["failures_next_cursor"] != "failure-next" || diagnostics["dead_letters_next_cursor"] != "dead-letter-next" {
		t.Fatalf("agent.delivery_diagnostics cursors = %#v/%#v", diagnostics["failures_next_cursor"], diagnostics["dead_letters_next_cursor"])
	}

	listConversations := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"convs","method":"conversation.list","params":{"agent_id":"agent-1","run_id":"11111111-1111-1111-1111-111111111111","limit":10,"cursor":"abc"}}`)
	if listConversations.Error != nil {
		t.Fatalf("conversation.list error = %#v", listConversations.Error)
	}
	if reads.lastConversationList.AgentID != "agent-1" || reads.lastConversationList.RunID == "" || reads.lastConversationList.Limit != 10 || reads.lastConversationList.Cursor != "abc" {
		t.Fatalf("conversation.list options = %#v", reads.lastConversationList)
	}

	listTurns := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"turns","method":"conversation.list_turns","params":{"session_id":"sess-1","limit":25,"cursor":"turn-cursor-1"}}`)
	if listTurns.Error != nil {
		t.Fatalf("conversation.list_turns error = %#v", listTurns.Error)
	}
	if reads.lastConversationTurns.SessionID != "sess-1" || reads.lastConversationTurns.Limit != 25 || reads.lastConversationTurns.Cursor != "turn-cursor-1" {
		t.Fatalf("conversation.list_turns options = %#v", reads.lastConversationTurns)
	}
	conversationTurns, _ := asMap(t, listTurns.Result)["turns"].([]any)
	if len(conversationTurns) != 1 || asMap(t, conversationTurns[0])["ordinal"] != float64(1) {
		t.Fatalf("conversation.list_turns turns = %#v, want ordinal 1", asMap(t, listTurns.Result)["turns"])
	}

	getTurn := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"turn","method":"conversation.get_turn","params":{"session_id":"sess-1","turn_id":"turn-1"}}`)
	if getTurn.Error != nil {
		t.Fatalf("conversation.get_turn error = %#v", getTurn.Error)
	}
	if reads.lastConversationTurnSessionID != "sess-1" || reads.lastConversationTurnID != "turn-1" {
		t.Fatalf("conversation.get_turn owner args = %q/%q", reads.lastConversationTurnSessionID, reads.lastConversationTurnID)
	}
}

func TestOperatorAgentHandlersSerializeLifecycleStatusFromReadOwner(t *testing.T) {
	summary := operatorread.OperatorAgentSummary{
		AgentID:      "agent-1",
		Role:         "researcher",
		Type:         "managed",
		Model:        "cheap",
		Memory:       true,
		MemorySource: "authored",
		FlowInstance: "research/inst-1",
		Status:       "idle",
	}
	reads := &fakeAgentConversationReadStore{
		listAgentsResult: operatorread.OperatorAgentListResult{Agents: []operatorread.OperatorAgentSummary{summary}},
		agentResult:      operatorread.OperatorAgentDetail{Agent: summary},
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			AgentConversations: reads,
		}),
	})

	listAgents := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"agents","method":"agent.list","params":{}}`)
	if listAgents.Error != nil {
		t.Fatalf("agent.list error = %#v", listAgents.Error)
	}
	listResult := asMap(t, listAgents.Result)
	agents, ok := listResult["agents"].([]any)
	if !ok || len(agents) != 1 {
		t.Fatalf("agent.list result = %#v", listResult)
	}
	listAgent := asMap(t, agents[0])
	if listAgent["status"] != "idle" {
		t.Fatalf("agent.list status = %#v, want idle from read owner", listAgent["status"])
	}
	if _, ok := listAgent["in_flight_turn"]; ok {
		t.Fatalf("agent.list exposed dashboard lifecycle field: %#v", listAgent)
	}

	getAgent := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"agent","method":"agent.get","params":{"run_id":"11111111-1111-4111-8111-111111111111","agent_id":"agent-1"}}`)
	if getAgent.Error != nil {
		t.Fatalf("agent.get error = %#v", getAgent.Error)
	}
	agentResult := asMap(t, getAgent.Result)
	agent := asMap(t, agentResult["agent"])
	if agent["status"] != "idle" {
		t.Fatalf("agent.get status = %#v, want idle from read owner", agent["status"])
	}
	if _, ok := agent["in_flight_turn"]; ok {
		t.Fatalf("agent.get exposed dashboard lifecycle field: %#v", agent)
	}
}

func assertUnsupportedAgentMetricStubsAbsent(t *testing.T, payload map[string]any) {
	t.Helper()
	for _, key := range []string{"turns_24h", "in_flight_seconds"} {
		if _, ok := payload[key]; ok {
			t.Fatalf("agent read exposed unsupported metric stub %q: %#v", key, payload)
		}
	}
}

func TestOperatorAgentConversationHandlersSanitizeRunIDProjectionFailures(t *testing.T) {
	rawRunIDColumnErr := errors.New(`pq: column "run_id" does not exist at position 46:14 (42703)`)
	reader := &fakeAgentConversationReadStore{listConversationsErr: rawRunIDColumnErr}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			AgentConversations: reader,
		}),
	})

	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"convs","method":"conversation.list","params":{"limit":20}}`)
	assertConversationRunIDErrorSanitized(t, resp)
}

func assertConversationRunIDErrorSanitized(t *testing.T, resp rpcResponse) {
	t.Helper()
	if resp.Error == nil {
		t.Fatal("response error = nil, want sanitized run_id capability error")
	}
	if resp.Error.Code != codeInternalError {
		t.Fatalf("error code = %d, want %d", resp.Error.Code, codeInternalError)
	}
	requireRPCFailure(t, resp.Error, runtimefailures.ClassInternalFailure, "unclassified_runtime_error")
	text := strings.ToLower(resp.Error.Message + " " + fmt.Sprint(resp.Error.Data))
	for _, forbidden := range []string{"pq:", `column "run_id"`, "42703", "position"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("error data leaked %q: %s", forbidden, text)
		}
	}
}

func ptrTime(t time.Time) *time.Time {
	return &t
}

func TestOperatorAgentConversationHandlersTypedErrors(t *testing.T) {
	tests := []struct {
		name    string
		method  string
		body    string
		reads   *fakeAgentConversationReadStore
		wantApp string
	}{
		{
			name:    "agent missing",
			method:  "agent.get",
			body:    `{"jsonrpc":"2.0","id":"agent","method":"agent.get","params":{"run_id":"11111111-1111-4111-8111-111111111111","agent_id":"missing"}}`,
			reads:   &fakeAgentConversationReadStore{agentErr: operatorread.ErrAgentNotFound},
			wantApp: AgentNotFoundCode,
		},
		{
			name:    "agent diagnosis missing",
			method:  "agent.diagnose",
			body:    `{"jsonrpc":"2.0","id":"diagnose","method":"agent.diagnose","params":{"run_id":"11111111-1111-4111-8111-111111111111","agent_id":"missing"}}`,
			reads:   &fakeAgentConversationReadStore{agentDiagnosisErr: operatorread.ErrAgentNotFound},
			wantApp: AgentNotFoundCode,
		},
		{
			name:    "agent usage missing",
			method:  "agent.usage",
			body:    `{"jsonrpc":"2.0","id":"usage","method":"agent.usage","params":{"run_id":"11111111-1111-4111-8111-111111111111","agent_id":"missing"}}`,
			reads:   &fakeAgentConversationReadStore{agentUsageErr: operatorread.ErrAgentNotFound},
			wantApp: AgentNotFoundCode,
		},
		{
			name:    "agent delivery diagnostics missing",
			method:  "agent.delivery_diagnostics",
			body:    `{"jsonrpc":"2.0","id":"agent-delivery-diagnostics","method":"agent.delivery_diagnostics","params":{"run_id":"11111111-1111-4111-8111-111111111111","agent_id":"missing"}}`,
			reads:   &fakeAgentConversationReadStore{agentDeliveryDiagnosticsErr: operatorread.ErrAgentNotFound},
			wantApp: AgentNotFoundCode,
		},
		{
			name:    "agent delivery lifecycle missing",
			method:  "agent.delivery_lifecycle",
			body:    `{"jsonrpc":"2.0","id":"agent-delivery-lifecycle","method":"agent.delivery_lifecycle","params":{"run_id":"11111111-1111-4111-8111-111111111111","agent_id":"missing"}}`,
			reads:   &fakeAgentConversationReadStore{agentDeliveryLifecycleErr: operatorread.ErrAgentNotFound},
			wantApp: AgentNotFoundCode,
		},
		{
			name:    "conversation turn list missing session",
			method:  "conversation.list_turns",
			body:    `{"jsonrpc":"2.0","id":"turns","method":"conversation.list_turns","params":{"session_id":"missing"}}`,
			reads:   &fakeAgentConversationReadStore{conversationTurnsErr: operatorread.ErrSessionNotFound},
			wantApp: SessionNotFoundCode,
		},
		{
			name:    "conversation turn missing session",
			method:  "conversation.get_turn",
			body:    `{"jsonrpc":"2.0","id":"turn","method":"conversation.get_turn","params":{"session_id":"missing","turn_id":"turn-1"}}`,
			reads:   &fakeAgentConversationReadStore{conversationTurnErr: operatorread.ErrSessionNotFound},
			wantApp: SessionNotFoundCode,
		},
		{
			name:    "conversation turn missing turn",
			method:  "conversation.get_turn",
			body:    `{"jsonrpc":"2.0","id":"turn","method":"conversation.get_turn","params":{"session_id":"sess-1","turn_id":"missing-turn"}}`,
			reads:   &fakeAgentConversationReadStore{conversationTurnErr: operatorread.ErrTurnNotFound},
			wantApp: TurnNotFoundCode,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			handler := testHandler(t, Options{
				AuthTokens: []string{testToken},
				Handlers: testOperatorHandlers(testOperatorCapabilities{
					AgentConversations:     tc.reads,
					AgentDeliveryLifecycle: tc.reads,
					AgentUsage:             tc.reads,
				}),
			})
			resp := rpcCall(t, handler, tc.body)
			if resp.Error == nil {
				t.Fatalf("%s returned no error", tc.method)
			}
			data := asMap(t, resp.Error.Data)
			if data["code"] != tc.wantApp {
				t.Fatalf("error code = %#v, want %s", data["code"], tc.wantApp)
			}
		})
	}
}

func TestOperatorAgentUsageFailsClosedOnMalformedOwnerData(t *testing.T) {
	reads := &fakeAgentConversationReadStore{
		agentUsageResult: operatorread.OperatorAgentUsage{
			AgentID: "agent-1",
			Usage: operatorread.OperatorAgentUsageByAccounting{
				Exact: operatorread.OperatorAgentUsageTotals{},
				Estimated: operatorread.OperatorAgentUsageTotals{
					LedgerEntries: -1,
				},
			},
			Breakdown: []operatorread.OperatorAgentUsageBreakdown{},
		},
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			AgentConversations: reads,
			AgentUsage:         reads,
		}),
	})

	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"agent-usage","method":"agent.usage","params":{"run_id":"11111111-1111-4111-8111-111111111111","agent_id":"agent-1"}}`)
	if resp.Error == nil {
		t.Fatal("agent.usage returned success for malformed owner result")
	}
	if resp.Error.Code != codeInternalError {
		t.Fatalf("error code = %d, want %d", resp.Error.Code, codeInternalError)
	}
	requireRPCFailure(t, resp.Error, runtimefailures.ClassInternalFailure, "unclassified_runtime_error")
}

func TestOperatorAgentDiagnoseFailsClosedOnMalformedOwnerData(t *testing.T) {
	reads := &fakeAgentConversationReadStore{
		agentDiagnosisResult: operatorread.OperatorAgentDiagnosis{
			AgentID: "agent-1",
			Status:  "running",
			Queue: operatorread.OperatorAgentDiagnosisQueue{
				PendingDeliveries: []operatorread.OperatorAgentPendingDelivery{{
					DeliveryID: "delivery-1",
					EventName:  "task.ready",
					EnqueuedAt: time.Date(2026, 5, 21, 10, 0, 0, 0, time.UTC),
				}},
			},
		},
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			AgentConversations: reads,
		}),
	})

	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"diagnose","method":"agent.diagnose","params":{"run_id":"11111111-1111-4111-8111-111111111111","agent_id":"agent-1"}}`)
	if resp.Error == nil {
		t.Fatal("agent.diagnose returned success for malformed owner result")
	}
	if resp.Error.Code != codeInternalError {
		t.Fatalf("error code = %d, want %d", resp.Error.Code, codeInternalError)
	}
	requireRPCFailure(t, resp.Error, runtimefailures.ClassInternalFailure, "unclassified_runtime_error")
}

func TestOperatorAgentDeliveryDiagnosticsFailsClosedOnMalformedOwnerData(t *testing.T) {
	reads := &fakeAgentConversationReadStore{
		agentDeliveryDiagnosticsResult: operatorread.OperatorAgentDeliveryDiagnostics{
			AgentID: "agent-1",
			Summary: operatorread.OperatorAgentDeliveryDiagnosticsSummary{
				Failures24h:    1,
				DeadLetters24h: 1,
			},
			Failures: []operatorread.OperatorAgentDeliveryFailure{{
				DeliveryID: "delivery-failed-1",
				EventID:    "event-failed-1",
				EventName:  "task.failed",
				Status:     "failed",
				RetryCount: 1,
				OccurredAt: time.Date(2026, 5, 21, 10, 0, 0, 0, time.UTC),
			}},
			DeadLetters: []operatorread.OperatorAgentDeadLetterDelivery{{
				DeliveryID:        "delivery-dead-1",
				EventID:           "event-dead-1",
				EventName:         "task.dead",
				Status:            "dead_letter",
				RetryCount:        2,
				OccurredAt:        time.Date(2026, 5, 21, 10, 1, 0, 0, time.UTC),
				DeadLetterRecords: []operatorread.OperatorDeadLetterRecord{},
			}},
		},
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			AgentConversations: reads,
		}),
	})

	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"agent-delivery-diagnostics","method":"agent.delivery_diagnostics","params":{"run_id":"11111111-1111-4111-8111-111111111111","agent_id":"agent-1"}}`)
	if resp.Error == nil {
		t.Fatal("agent.delivery_diagnostics returned success for malformed owner result")
	}
	if resp.Error.Code != codeInternalError {
		t.Fatalf("error code = %d, want %d", resp.Error.Code, codeInternalError)
	}
	requireRPCFailure(t, resp.Error, runtimefailures.ClassInternalFailure, "unclassified_runtime_error")
}

func TestOperatorAgentDeliveryLifecycleFailsClosedOnMalformedOwnerData(t *testing.T) {
	reads := &fakeAgentConversationReadStore{
		agentDeliveryLifecycleResult: operatorread.OperatorAgentDeliveryLifecycleList{
			AgentID: "agent-1",
			Deliveries: []operatorread.OperatorAgentDeliveryLifecycleRow{{
				DeliveryID:        "delivery-1",
				EventID:           "event-1",
				EventName:         "task.ready",
				Status:            "not_a_delivery_status",
				RetryCount:        0,
				DeliveryCreatedAt: time.Date(2026, 5, 21, 10, 0, 0, 0, time.UTC),
			}},
		},
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			AgentDeliveryLifecycle: reads,
		}),
	})

	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"agent-delivery-lifecycle","method":"agent.delivery_lifecycle","params":{"run_id":"11111111-1111-4111-8111-111111111111","agent_id":"agent-1"}}`)
	if resp.Error == nil {
		t.Fatal("agent.delivery_lifecycle returned success for malformed owner result")
	}
	if resp.Error.Code != codeInternalError {
		t.Fatalf("error code = %d, want %d", resp.Error.Code, codeInternalError)
	}
	requireRPCFailure(t, resp.Error, runtimefailures.ClassInternalFailure, "unclassified_runtime_error")
}

func TestOperatorAgentDiagnoseFailsClosedOnMalformedWatchdogOwnerData(t *testing.T) {
	reads := &fakeAgentConversationReadStore{
		agentDiagnosisResult: operatorread.OperatorAgentDiagnosis{
			AgentID: "agent-1",
			Status:  "running",
			Queue: operatorread.OperatorAgentDiagnosisQueue{
				PendingDeliveries: []operatorread.OperatorAgentPendingDelivery{},
			},
			RuntimeState: &operatorread.OperatorAgentDiagnosisRuntimeState{
				Watchdog: &operatorread.OperatorAgentDiagnosisWatchdog{
					State:         "healthy_long_running",
					BlockingLayer: "session_execution",
					Action:        "turn_long_running",
					Outcome:       "observed",
					RecordedAt:    "2026-05-21T10:01:00Z",
				},
			},
		},
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			AgentConversations: reads,
		}),
	})

	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"diagnose","method":"agent.diagnose","params":{"run_id":"11111111-1111-4111-8111-111111111111","agent_id":"agent-1"}}`)
	if resp.Error == nil {
		t.Fatal("agent.diagnose returned success for malformed watchdog owner result")
	}
	if resp.Error.Code != codeInternalError {
		t.Fatalf("error code = %d, want %d", resp.Error.Code, codeInternalError)
	}
	requireRPCFailure(t, resp.Error, runtimefailures.ClassInternalFailure, "unclassified_runtime_error")
}

func TestOperatorAgentDiagnoseFailsClosedOnMalformedActiveOwnerData(t *testing.T) {
	reads := &fakeAgentConversationReadStore{
		agentDiagnosisResult: operatorread.OperatorAgentDiagnosis{
			AgentID: "agent-1",
			Status:  "running",
			Queue: operatorread.OperatorAgentDiagnosisQueue{
				PendingDeliveries: []operatorread.OperatorAgentPendingDelivery{},
			},
			Active: &operatorread.OperatorAgentDiagnosisActive{
				TaskID: "task-1",
			},
		},
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			AgentConversations: reads,
		}),
	})

	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"diagnose","method":"agent.diagnose","params":{"run_id":"11111111-1111-4111-8111-111111111111","agent_id":"agent-1"}}`)
	if resp.Error == nil {
		t.Fatal("agent.diagnose returned success for malformed active owner result")
	}
	if resp.Error.Code != codeInternalError {
		t.Fatalf("error code = %d, want %d", resp.Error.Code, codeInternalError)
	}
	requireRPCFailure(t, resp.Error, runtimefailures.ClassInternalFailure, "unclassified_runtime_error")
}

func TestOperatorAgentDiagnoseFailsClosedOnMalformedLastToolOutcomeOwnerData(t *testing.T) {
	for _, tc := range []struct {
		name   string
		active *operatorread.OperatorAgentDiagnosisActive
		item   *operatorread.OperatorAgentLastToolOutcome
		want   string
	}{
		{
			name: "missing turn id",
			item: &operatorread.OperatorAgentLastToolOutcome{ToolName: "read_file", OK: true},
			want: "last_tool_outcome.turn_id",
		},
		{
			name: "missing tool name",
			item: &operatorread.OperatorAgentLastToolOutcome{TurnID: "turn-1", OK: true},
			want: "last_tool_outcome.tool_name",
		},
		{
			name: "without active selected turn",
			item: &operatorread.OperatorAgentLastToolOutcome{TurnID: "turn-1", ToolName: "read_file", OK: true},
			want: "last_tool_outcome requires active",
		},
		{
			name:   "turn id does not match active turn id",
			active: &operatorread.OperatorAgentDiagnosisActive{TurnID: "turn-2"},
			item:   &operatorread.OperatorAgentLastToolOutcome{TurnID: "turn-1", ToolName: "read_file", OK: true},
			want:   "last_tool_outcome.turn_id",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reads := &fakeAgentConversationReadStore{
				agentDiagnosisResult: operatorread.OperatorAgentDiagnosis{
					AgentID: "agent-1",
					Status:  "running",
					Queue: operatorread.OperatorAgentDiagnosisQueue{
						PendingDeliveries: []operatorread.OperatorAgentPendingDelivery{},
					},
					Active:          tc.active,
					LastToolOutcome: tc.item,
				},
			}
			handler := testHandler(t, Options{
				AuthTokens: []string{testToken},
				Handlers: testOperatorHandlers(testOperatorCapabilities{
					AgentConversations: reads,
				}),
			})

			resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"diagnose","method":"agent.diagnose","params":{"run_id":"11111111-1111-4111-8111-111111111111","agent_id":"agent-1"}}`)
			if resp.Error == nil {
				t.Fatal("agent.diagnose returned success for malformed last_tool_outcome owner result")
			}
			if resp.Error.Code != codeInternalError {
				t.Fatalf("error code = %d, want %d", resp.Error.Code, codeInternalError)
			}
			requireRPCFailure(t, resp.Error, runtimefailures.ClassInternalFailure, "unclassified_runtime_error")
		})
	}
}

func TestOperatorAgentDiagnoseRejectsQueueLimit(t *testing.T) {
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			AgentConversations: &fakeAgentConversationReadStore{},
		}),
	})

	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"probe-agent-diagnose","method":"agent.diagnose","params":{"run_id":"11111111-1111-4111-8111-111111111111","agent_id":"agent-1","queue_limit":0}}`)
	if resp.Error == nil {
		t.Fatal("agent.diagnose returned success for invalid queue_limit")
	}
	assertReadOnlyProbeInvalidParams(t, "agent.diagnose", resp, "queue_limit")
}

func TestOperatorAgentDiagnoseRejectsBadQueueCursor(t *testing.T) {
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			AgentConversations: &fakeAgentConversationReadStore{agentDiagnosisErr: operatorread.ErrInvalidPendingAgentDeliveryCursor},
		}),
	})

	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"probe-agent-diagnose","method":"agent.diagnose","params":{"run_id":"11111111-1111-4111-8111-111111111111","agent_id":"agent-1","queue_cursor":"bad"}}`)
	if resp.Error == nil {
		t.Fatal("agent.diagnose returned success for invalid queue_cursor")
	}
	assertReadOnlyProbeInvalidParams(t, "agent.diagnose", resp, "queue_cursor")
}

func TestOperatorAgentUsageRejectsInvalidWindow(t *testing.T) {
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			AgentUsage: &fakeAgentConversationReadStore{},
		}),
	})

	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"probe-agent-usage","method":"agent.usage","params":{"run_id":"11111111-1111-4111-8111-111111111111","agent_id":"agent-1","since":"2026-05-21T10:00:00Z","until":"2026-05-21T10:00:00Z"}}`)
	if resp.Error == nil {
		t.Fatal("agent.usage returned success for invalid window")
	}
	assertReadOnlyProbeInvalidParams(t, "agent.usage", resp, "until")
}

func TestOperatorAgentDeliveryDiagnosticsRejectsLimits(t *testing.T) {
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			AgentConversations: &fakeAgentConversationReadStore{},
		}),
	})

	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"probe-agent-delivery_diagnostics","method":"agent.delivery_diagnostics","params":{"run_id":"11111111-1111-4111-8111-111111111111","agent_id":"agent-1","failure_limit":0}}`)
	if resp.Error == nil {
		t.Fatal("agent.delivery_diagnostics returned success for invalid failure_limit")
	}
	assertReadOnlyProbeInvalidParams(t, "agent.delivery_diagnostics", resp, "failure_limit")

	resp = rpcCall(t, handler, `{"jsonrpc":"2.0","id":"probe-agent-delivery_diagnostics","method":"agent.delivery_diagnostics","params":{"run_id":"11111111-1111-4111-8111-111111111111","agent_id":"agent-1","dead_letter_limit":0}}`)
	if resp.Error == nil {
		t.Fatal("agent.delivery_diagnostics returned success for invalid dead_letter_limit")
	}
	assertReadOnlyProbeInvalidParams(t, "agent.delivery_diagnostics", resp, "dead_letter_limit")
}

func TestOperatorAgentDeliveryLifecycleRejectsBadCursorAndStatuses(t *testing.T) {
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			AgentDeliveryLifecycle: &fakeAgentConversationReadStore{agentDeliveryLifecycleErr: operatorread.AgentDeliveryLifecycleCursorError{}},
		}),
	})

	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"probe-agent-delivery_lifecycle","method":"agent.delivery_lifecycle","params":{"run_id":"11111111-1111-4111-8111-111111111111","agent_id":"agent-1","cursor":"bad"}}`)
	if resp.Error == nil {
		t.Fatal("agent.delivery_lifecycle returned success for invalid cursor")
	}
	assertReadOnlyProbeInvalidParams(t, "agent.delivery_lifecycle", resp, "cursor")

	resp = rpcCall(t, handler, `{"jsonrpc":"2.0","id":"probe-agent-delivery_lifecycle","method":"agent.delivery_lifecycle","params":{"run_id":"11111111-1111-4111-8111-111111111111","agent_id":"agent-1","delivery_status":["unknown"]}}`)
	if resp.Error == nil {
		t.Fatal("agent.delivery_lifecycle returned success for invalid delivery_status")
	}
	assertReadOnlyProbeInvalidParams(t, "agent.delivery_lifecycle", resp, "delivery_status")

	resp = rpcCall(t, handler, `{"jsonrpc":"2.0","id":"probe-agent-delivery_lifecycle","method":"agent.delivery_lifecycle","params":{"run_id":"11111111-1111-4111-8111-111111111111","agent_id":"agent-1","limit":0}}`)
	if resp.Error == nil {
		t.Fatal("agent.delivery_lifecycle returned success for invalid limit")
	}
	assertReadOnlyProbeInvalidParams(t, "agent.delivery_lifecycle", resp, "limit")
}

func TestOperatorAgentDeliveryDiagnosticsRejectsBadCursor(t *testing.T) {
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			AgentConversations: &fakeAgentConversationReadStore{agentDeliveryDiagnosticsErr: operatorread.AgentDeliveryDiagnosticsCursorError{Field: "dead_letter_cursor"}},
		}),
	})

	resp := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"probe-agent-delivery_diagnostics","method":"agent.delivery_diagnostics","params":{"run_id":"11111111-1111-4111-8111-111111111111","agent_id":"agent-1","dead_letter_cursor":"bad"}}`)
	if resp.Error == nil {
		t.Fatal("agent.delivery_diagnostics returned success for invalid dead_letter_cursor")
	}
	assertReadOnlyProbeInvalidParams(t, "agent.delivery_diagnostics", resp, "dead_letter_cursor")
}
