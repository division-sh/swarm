package runtimepersistence

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

func proveCompletionToolResultConsumption(t *testing.T, fixture completionSettlementFixture) {
	t.Helper()
	const adapter, request = "openai_compatible", "consume-terminal-tool-result"
	ctx := runtimeeffects.WithLifecycleToken(fixture.context, fixture.authority.Normal)
	ctx = runtimeeffects.WithLogicalOperationIdentity(ctx, request)
	handle := beginObservedCompletionForSettlementTest(t, ctx, adapter, request)
	settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, adapter, "", "")
	settlement.ProviderHead = nil
	frameID := "agent-frame:v1:" + uuid.NewString()
	payload, messages := completionSuccessorPayload(t, adapter, fixture, settlement, frameID)
	if err := runtimeeffects.AttachCompletionContinuationEvidence(settlement.Settlement.Evidence, []byte(request), payload); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.SettleCompletion(ctx, settlement); err != nil {
		t.Fatal(err)
	}
	controller := newCompletionControllerForTest(fixture.store)
	handle, found, err := controller.RecoverCompletionContinuation(managedExecutionStoreTestContext(t, fixture.context), fixture.sessionID, settlement.AgentTurn.Memory)
	if err != nil || !found {
		t.Fatalf("recover settled tool response: found=%v err=%v", found, err)
	}
	lease, err := fixture.store.(sessions.Registry).Acquire(ctx, fixture.authority.Target.AgentIdentity, fixture.leaseHolder)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.BindRecoveredContinuationGrant(runtimeeffects.SessionGrant{SessionID: lease.SessionID, GrantID: lease.GrantID, LockOwner: lease.LockOwner}); err != nil {
		t.Fatal(err)
	}
	snapshot, ok := handle.CompletionContinuation()
	if !ok {
		t.Fatal("recovered tool response has no immutable continuation")
	}
	projectionMessages, err := json.Marshal(messages)
	if err != nil {
		t.Fatal(err)
	}
	projection := runtimeeffects.CompletionConversationProjection{Payload: snapshot.Payload, SessionID: fixture.sessionID,
		Identity: settlement.AgentTurn.Identity, Memory: settlement.AgentTurn.Memory, ExpectedTurnCount: 0, TurnCount: 1, Messages: projectionMessages}
	if err := handle.ProjectCompletionConversation(ctx, projection); err != nil {
		t.Fatal(err)
	}
	result := json.RawMessage(`[{"tool_call_id":"call-1","name":"echo","ok":true,"result":{"value":"stable"}}]`)
	terminal, err := agentframe.NewToolContinuation(frameID, result)
	if err != nil {
		t.Fatal(err)
	}
	for _, hostile := range []struct {
		name, parent string
		payload      json.RawMessage
	}{
		{"parent", "agent-frame:v1:" + uuid.NewString(), result},
		{"call", frameID, json.RawMessage(`[{"tool_call_id":"foreign","name":"echo","ok":true,"result":{}}]`)},
		{"name", frameID, json.RawMessage(`[{"tool_call_id":"call-1","name":"other","ok":true,"result":{}}]`)},
		{"cardinality", frameID, json.RawMessage(`[{"tool_call_id":"call-1","name":"echo","ok":true,"result":{}},{"tool_call_id":"extra","name":"echo","ok":true,"result":{}}]`)},
	} {
		t.Run(hostile.name, func(t *testing.T) {
			candidate, err := agentframe.NewToolContinuation(hostile.parent, hostile.payload)
			if err != nil {
				t.Fatal(err)
			}
			before := historicalContextDatabaseRows(t, fixture.db, !fixture.sqlite)
			if err := handle.ConsumeCompletionResponse(ctx, nil, &candidate); err == nil {
				t.Fatal("hostile terminal tool result consumed")
			}
			historicalContextRequireUnchanged(t, before, historicalContextDatabaseRows(t, fixture.db, !fixture.sqlite))
		})
	}
	if err := handle.ConsumeCompletionResponse(ctx, nil, &terminal); err != nil {
		t.Fatal(err)
	}
	readback := fixture.store.(interface {
		LoadActiveConversation(context.Context, agentmemory.Identity) (runtimellm.ConversationRecord, bool, error)
	})
	record, found, err := readback.LoadActiveConversation(ctx, settlement.AgentTurn.Identity)
	if err != nil || !found || record.TurnCount != 1 || len(record.Messages) != len(messages)+1 || !reflect.DeepEqual(record.Messages[:len(messages)], messages) ||
		record.Messages[len(messages)].Role != "tool" || record.Messages[len(messages)].Content != string(terminal.ToolResult()) {
		t.Fatalf("consumed conversation=%+v found=%v error=%v", record, found, err)
	}
	requireCompletionProjectionPhase(t, fixture, handle.Attempt().AttemptID, runtimeeffects.CompletionProjectionResponseConsumed)
	before := historicalContextDatabaseRows(t, fixture.db, !fixture.sqlite)
	if err := handle.ConsumeCompletionResponse(ctx, nil, &terminal); err != nil {
		t.Fatalf("repeat exact tool result: %v", err)
	}
	historicalContextRequireUnchanged(t, before, historicalContextDatabaseRows(t, fixture.db, !fixture.sqlite))
	changed, err := agentframe.NewToolContinuation(frameID, json.RawMessage(`[{"tool_call_id":"call-1","name":"echo","ok":true,"result":{"value":"different"}}]`))
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.ConsumeCompletionResponse(ctx, nil, &changed); err == nil {
		t.Fatal("changed result replaced the committed consumed result")
	}
	historicalContextRequireUnchanged(t, before, historicalContextDatabaseRows(t, fixture.db, !fixture.sqlite))
	requireProviderAttemptCount(t, fixture, 1)
}
