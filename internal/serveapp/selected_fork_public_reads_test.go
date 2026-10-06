package serveapp

import (
	"context"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func requireSelectedForkDurablePublicReads(t *testing.T, endpoint string, owner runtimebus.EventStore, loadedBundleHash, runID string, completionCount int) {
	t.Helper()
	params := map[string]any{"run_id": runID}
	var get struct {
		Run operatorread.RunHeader `json:"run"`
	}
	requireServedJSONRPCResult(t, endpoint, "run.get", params, &get)
	if get.Run.RunID != runID || get.Run.Failure != nil || get.Run.EventCount == 0 {
		t.Fatalf("selected run header lost completed execution: %+v", get)
	}
	var diagnose struct {
		Run    operatorread.RunHeader                 `json:"run"`
		Failed []operatorread.RunDebugFailureDelivery `json:"failed_deliveries"`
	}
	requireServedJSONRPCResult(t, endpoint, "run.diagnose", params, &diagnose)
	if !reflect.DeepEqual(diagnose.Run, get.Run) || len(diagnose.Failed) != 0 {
		t.Fatalf("selected run diagnosis disagrees with exact run header: %+v", diagnose)
	}
	hash, err := storetest.ReadSelectedForkRunBundleHash(context.Background(), owner, runID)
	if err != nil || hash == loadedBundleHash {
		t.Fatalf("public read fixture must target an unloaded different artifact: hash=%s err=%v", hash, err)
	}
	var runs struct {
		Runs []operatorread.RunHeader `json:"runs"`
	}
	requireServedJSONRPCResult(t, endpoint, "run.list", map[string]any{"bundle_hash": hash}, &runs)
	if len(runs.Runs) != 1 || !reflect.DeepEqual(runs.Runs[0], get.Run) {
		t.Fatalf("unloaded target absent or replaced in run list: %+v", runs)
	}
	var agents operatorread.OperatorAgentListResult
	requireServedJSONRPCResult(t, endpoint, "agent.list", map[string]any{"flow": "consumer", "role": "observer"}, &agents)
	if len(agents.Agents) != 1 || agents.Agents[0].AgentID != "same-name" || agents.Agents[0].FlowInstance != "consumer" {
		t.Fatalf("unloaded target missing from agent list: %+v", agents)
	}
	var entities operatorread.OperatorEntityListResult
	requireServedJSONRPCResult(t, endpoint, "entity.list", params, &entities)
	if len(entities.Entities) != get.Run.EntityCount || len(entities.Entities) == 0 {
		t.Fatalf("selected entity census differs from header: %+v", entities)
	}
	counts := map[string]int{}
	for _, entity := range entities.Entities {
		if entity.RunID != runID {
			t.Fatalf("selected list borrowed another run: %+v", entity)
		}
		counts[entity.EntityType]++
		var full operatorread.OperatorEntityFull
		requireServedJSONRPCResult(t, endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": entity.EntityID}, &full)
		if !reflect.DeepEqual(full.Entity, entity) {
			t.Fatalf("selected entity get/list disagree: %+v / %+v", entity, full)
		}
		if entity.FlowInstance == "consumer" && full.Fields["processed_token"] != "receiver-proof" {
			t.Fatalf("public target lost actual execution result: %+v", full)
		}
	}
	var aggregate operatorread.OperatorEntityAggregateResult
	requireServedJSONRPCResult(t, endpoint, "entity.aggregate", map[string]any{"run_id": runID, "group_by": "entity_type"}, &aggregate)
	if !reflect.DeepEqual(aggregate.Counts, counts) {
		t.Fatalf("selected aggregate disagrees with exact entities: %+v / %v", aggregate, counts)
	}
	var conversations operatorread.OperatorConversationListResult
	requireServedJSONRPCResult(t, endpoint, "conversation.list", map[string]any{"run_id": runID, "agent_id": "same-name", "flow_instance": "consumer"}, &conversations)
	if len(conversations.Conversations) != 1 {
		t.Fatalf("selected conversation census: %+v", conversations)
	}
	conversation := conversations.Conversations[0]
	if conversation.RunID != runID || conversation.AgentID != "same-name" || conversation.TurnCount != completionCount {
		t.Fatalf("selected conversation lost its turn authority: %+v", conversation)
	}
	var turns operatorread.OperatorConversationTurnListResult
	requireServedJSONRPCResult(t, endpoint, "conversation.list_turns", map[string]any{"session_id": conversation.SessionID}, &turns)
	if len(turns.Turns) != completionCount || turns.Conversation.RunID != runID || turns.Conversation.SessionID != conversation.SessionID {
		t.Fatalf("selected turn list borrowed another conversation: %+v", turns)
	}
	for _, summary := range turns.Turns {
		var turn operatorread.OperatorPublicConversationTurnDetail
		requireServedJSONRPCResult(t, endpoint, "conversation.get_turn", map[string]any{"session_id": conversation.SessionID, "turn_id": summary.TurnID}, &turn)
		if turn.Turn.TurnID != summary.TurnID || turn.Session.RunID != runID || turn.Session.SessionID != conversation.SessionID || turn.Turn.Failure != nil || turn.Frame.FrameID == "" {
			t.Fatalf("selected turn detail lost execution/frame evidence: %+v", turn)
		}
		if turn.Turn.Tokens == nil || turn.Turn.Tokens.Input != 1 || turn.Turn.Tokens.Output != 1 {
			t.Fatalf("selected turn did not retain target mock usage: %+v", turn.Turn.Tokens)
		}
	}
	var eventList operatorread.OperatorEventListResult
	requireServedJSONRPCResult(t, endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": runID}, "limit": 1000}, &eventList)
	if len(eventList.Events) == 0 || eventList.NextCursor != "" {
		t.Fatalf("selected event census missing or unexpectedly paginated: %+v", eventList)
	}
	eventIDs := map[string]bool{}
	for _, event := range eventList.Events {
		if event.RunID != runID || eventIDs[event.EventID] {
			t.Fatalf("selected event list contains foreign/duplicate event: %+v", event)
		}
		eventIDs[event.EventID] = true
		var exact operatorread.OperatorEventFull
		requireServedJSONRPCResult(t, endpoint, "event.get", map[string]any{"event_id": event.EventID}, &exact)
		if !reflect.DeepEqual(exact, event) {
			t.Fatalf("selected event list/get disagree: %+v / %+v", event, exact)
		}
	}
	requireWorkspaceProofReceiverPublicReadback(t, endpoint, owner, runID)
	var trace struct {
		Trace []operatorread.RunDebugTraceRow `json:"trace"`
	}
	requireServedJSONRPCResult(t, endpoint, "run.trace", map[string]any{"run_id": runID, "limit": 2000}, &trace)
	var agentDelivery, nodeDelivery bool
	for _, row := range trace.Trace {
		if !eventIDs[row.EventID] {
			t.Fatalf("selected trace includes an event outside the selected run: %+v", row)
		}
		if row.EventName == "producer/work.ready" && row.DeliveryStatus == "delivered" {
			agentDelivery = agentDelivery || row.SubscriberType == "agent"
			nodeDelivery = nodeDelivery || row.SubscriberType == "node"
		}
	}
	if !agentDelivery || !nodeDelivery {
		t.Fatalf("selected trace omitted mixed recipient execution: %+v", trace)
	}
	var logs operatorread.OperatorRuntimeLogListResult
	requireServedJSONRPCResult(t, endpoint, "runtime.logs", map[string]any{"run_id": runID, "limit": 1000}, &logs)
	for _, log := range logs.Logs {
		if log.RunID != runID {
			t.Fatalf("selected log query borrowed another run: %+v", log)
		}
	}
	var incidents operatorread.OperatorRuntimeIncidentListResult
	requireServedJSONRPCResult(t, endpoint, "runtime.incidents", map[string]any{"bundle_hash": hash, "limit": 500}, &incidents)
	if len(incidents.Incidents) != 0 || incidents.NextCursor != "" {
		t.Fatalf("successful selected execution borrowed an incident or failed: %+v", incidents)
	}
}
