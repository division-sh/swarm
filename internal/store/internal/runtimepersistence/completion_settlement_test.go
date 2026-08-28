package runtimepersistence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	agentfixture "github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type completionSettlementTestStore interface {
	completionControllerTestStore
	deliveryFixtureStore
	runtimeeffects.RecoveryStore
	agentfixture.Store
}

type completionControllerTestStore interface {
	runtimeeffects.Store
	runtimeeffects.CompletionStore
	runtimeeffects.CompletionHeartbeatStore
}

func newCompletionControllerForTest(store completionControllerTestStore) *runtimeeffects.Controller {
	return liveTestCompletionController(store, store, store, nil)
}

type completionSettlementFixture struct {
	store       completionSettlementTestStore
	lifecycle   runtimemanager.AgentLifecyclePersistence
	agentOwner  testing.TB
	db          *sql.DB
	sqlite      bool
	authority   runtimeeffects.Authority
	origin      runtimedelivery.Claim
	context     context.Context
	sessionID   string
	agentID     string
	leaseHolder string
}

func TestCompletionProviderHeadSettlementSQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveCompletionProviderHeadSettlement(t, newCompletionSettlementFixture(t, store, store.backend.ConstructionHandle(), true))
}

func TestCompletionProviderHeadSettlementPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveCompletionProviderHeadSettlement(t, newCompletionSettlementFixture(t, admitTestPostgresStore(t, db), db, false))
}

func TestCompletionOperationOwnsExactAgentFrameSQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveCompletionOperationOwnsExactAgentFrame(t, newCompletionSettlementFixture(t, store, store.backend.ConstructionHandle(), true))
}

func TestCompletionOperationOwnsExactAgentFramePostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveCompletionOperationOwnsExactAgentFrame(t, newCompletionSettlementFixture(t, admitTestPostgresStore(t, db), db, false))
}

func proveCompletionOperationOwnsExactAgentFrame(t *testing.T, fixture completionSettlementFixture) {
	t.Helper()
	const adapter = "claude_cli"
	authority := fixture.authority
	authority.BudgetScopes = nil
	ctx := runtimeeffects.WithLogicalOperationIdentity(fixture.contextFor(authority), "agent-frame:exact-owner")
	ctx = withManagedCompletionTestSurface(t, ctx, authority, adapter)
	event := managedCompletionTestEvent(authority)
	frame := managedCompletionTestFrameWithEvent(t, authority, adapter, event)
	want, err := agentframe.EncodeDurable(frame)
	if err != nil {
		t.Fatalf("encode expected operation frame: %v", err)
	}
	handle, err := runtimeeffects.BeginManagedCompletion(ctx, adapter, []byte("exact-frame-request"), frame, nil)
	if err != nil {
		t.Fatalf("authorize exact-frame completion: %v", err)
	}
	assertCompletionFrameBytes(t, fixture, "runtime_external_effect_operations", "operation_id", handle.Attempt().OperationID, want)

	failureErr := runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "claude_cli_process_start_failed", "completion-test", "start", map[string]any{"launch_rejected": true})
	failure, _ := runtimefailures.EnvelopeFromError(failureErr)
	settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, adapter, "", "")
	settlement.ProviderHead = nil
	settlement.Settlement = runtimeeffects.Settlement{State: runtimeeffects.StateTerminalFailure, Failure: &failure, Evidence: map[string]any{"launch_rejected": true}}
	settlement.Usage = runtimeeffects.CompletionUsage{ResolvedModel: "claude-test", Exactness: runtimeeffects.CompletionUsageUnavailable}
	settlement.AgentTurn.Failure = &failure
	if _, err := handle.SettleCompletion(ctx, settlement); err == nil {
		t.Fatal("authorized attempt materialized a completion turn")
	}
	requireCompletionRecoveryRows(t, fixture, handle.Attempt().AttemptID, 0, 0, 0)
	if err := handle.Settle(ctx, runtimeeffects.StateTerminalFailure, &failure, map[string]any{"launch_rejected": true}); err != nil {
		t.Fatalf("settle retryable prelaunch attempt: %v", err)
	}
	requireCompletionRecoveryRows(t, fixture, handle.Attempt().AttemptID, 0, 0, 0)

	retry, err := runtimeeffects.BeginManagedCompletion(ctx, adapter, []byte("exact-frame-request"), frame, nil)
	if err != nil {
		t.Fatalf("authorize byte-identical retry: %v", err)
	}
	if retry.Attempt().OperationID != handle.Attempt().OperationID || retry.Attempt().AttemptID == handle.Attempt().AttemptID || retry.Attempt().Ordinal != 2 {
		t.Fatalf("byte-identical retry did not reuse the operation with a second attempt: first=%+v retry=%+v", handle.Attempt(), retry.Attempt())
	}
	if err := retry.MarkLaunched(ctx); err != nil {
		t.Fatalf("launch byte-identical retry: %v", err)
	}
	if err := retry.MarkResponseObserved(ctx, map[string]any{"response_fingerprint": "exact-frame-response"}); err != nil {
		t.Fatalf("observe byte-identical retry response: %v", err)
	}
	settlement = completionSettlementForTest(t, retry.Attempt().Authority.Target, fixture, adapter, "", "")
	settlement.ProviderHead = nil
	if _, err := retry.SettleCompletion(ctx, settlement); err != nil {
		t.Fatalf("settle byte-identical retry completion: %v", err)
	}
	assertCompletionFrameBytes(t, fixture, "agent_turns", "turn_id", settlement.AgentTurn.TurnID, want)
	requireCompletionRecoveryRows(t, fixture, retry.Attempt().AttemptID, 1, 1, 0)

	changedEvent := eventtest.ExistingRunRootIngress(
		event.ID(), event.Type(), "gateway", "", []byte(`{ "request": "byte-distinct" }`), 0,
		event.RunID(), events.EventEnvelope{}, time.Unix(1, 0).UTC(),
	)
	changedFrame := managedCompletionTestFrameWithEvent(t, authority, adapter, changedEvent)
	changedCtx := runtimecorrelation.WithInboundEvent(ctx, changedEvent)
	if _, err := runtimeeffects.BeginManagedCompletion(changedCtx, adapter, []byte("exact-frame-request"), changedFrame, nil); err == nil {
		t.Fatal("same-operation retry accepted byte-distinct frame evidence")
	}
	assertCompletionFrameBytes(t, fixture, "runtime_external_effect_operations", "operation_id", handle.Attempt().OperationID, want)
}

func TestCompletionCorruptOperationFrameFailsClosedSQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveCompletionCorruptOperationFrameFailsClosed(t, newCompletionSettlementFixture(t, store, store.backend.ConstructionHandle(), true))
}

func TestCompletionCorruptOperationFrameFailsClosedPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveCompletionCorruptOperationFrameFailsClosed(t, newCompletionSettlementFixture(t, admitTestPostgresStore(t, db), db, false))
}

func proveCompletionCorruptOperationFrameFailsClosed(t *testing.T, fixture completionSettlementFixture) {
	t.Helper()
	ctx := runtimeeffects.WithLogicalOperationIdentity(fixture.context, "agent-frame:corrupt-owner")
	handle := beginObservedCompletionForSettlementTest(t, ctx, "claude_cli", "corrupt-frame")
	query := `UPDATE runtime_external_effect_operations SET agent_frame_bytes=? WHERE operation_id=?`
	if !fixture.sqlite {
		query = `UPDATE runtime_external_effect_operations SET agent_frame_bytes=$1 WHERE operation_id=$2::uuid`
	}
	if _, err := fixture.db.Exec(query, []byte(`{"corrupt":true}`), handle.Attempt().OperationID); err != nil {
		t.Fatalf("corrupt operation frame: %v", err)
	}
	settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, "claude_cli", "provider-head-current", "provider-head-next")
	if _, err := handle.SettleCompletion(ctx, settlement); err == nil {
		t.Fatal("corrupt operation frame settled an agent turn")
	}
	requireCompletionSettlementRows(t, fixture, handle.Attempt().AttemptID, settlement.AgentTurn.TurnID, runtimeeffects.StateResponseObserved, 0, 1)
}

func assertCompletionFrameBytes(t *testing.T, fixture completionSettlementFixture, table, idColumn, id string, want []byte) {
	t.Helper()
	got := loadCompletionFrameBytes(t, fixture, table, idColumn, id)
	if !bytes.Equal(got, want) {
		t.Fatalf("%s frame bytes changed: got %q want %q", table, got, want)
	}
	hydrated, err := agentframe.DecodeDurable(got)
	if err != nil {
		t.Fatalf("hydrate %s frame bytes: %v", table, err)
	}
	if hydrated.FrameID == "" {
		t.Fatalf("%s hydrated frame identity is empty", table)
	}
}

func loadCompletionFrameBytes(t *testing.T, fixture completionSettlementFixture, table, idColumn, id string) []byte {
	t.Helper()
	placeholder := "?"
	if !fixture.sqlite {
		placeholder = "$1::uuid"
	}
	var got []byte
	if err := fixture.db.QueryRow(`SELECT agent_frame_bytes FROM `+table+` WHERE `+idColumn+`=`+placeholder, id).Scan(&got); err != nil {
		t.Fatalf("load %s frame bytes: %v", table, err)
	}
	return got
}

func TestSettledProviderResponseContinuesExactReclaimedDeliverySQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveSettledProviderResponseContinuesExactReclaimedDelivery(t, newCompletionSettlementFixture(t, store, store.backend.ConstructionHandle(), true))
}

func TestSettledProviderResponseContinuesExactReclaimedDeliveryPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveSettledProviderResponseContinuesExactReclaimedDelivery(t, newCompletionSettlementFixture(t, admitTestPostgresStore(t, db), db, false))
}

func TestSettledProviderResponseRebindsStatelessSessionSQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveSettledProviderResponseRebindsStatelessSession(t, newCompletionSettlementFixtureWithMemory(t, store, store.backend.ConstructionHandle(), true, agentmemory.PlatformDefault()))
}

func TestSettledProviderResponseRebindsStatelessSessionPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveSettledProviderResponseRebindsStatelessSession(t, newCompletionSettlementFixtureWithMemory(t, admitTestPostgresStore(t, db), db, false, agentmemory.PlatformDefault()))
}

func TestSettledProviderResponseContinuationAdvancesExactActiveTurnSQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveSettledProviderResponseContinuationAdvancesExactActiveTurn(t, newCompletionSettlementFixture(t, store, store.backend.ConstructionHandle(), true))
}

func TestSettledProviderResponseContinuationAdvancesExactActiveTurnPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveSettledProviderResponseContinuationAdvancesExactActiveTurn(t, newCompletionSettlementFixture(t, admitTestPostgresStore(t, db), db, false))
}

func TestCompletionResponseSuccessorCheckpointMatrixSQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveCompletionResponseSuccessorCheckpointMatrix(t, store, store.backend.ConstructionHandle(), true)
}

func TestCompletionResponseSuccessorCheckpointMatrixPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveCompletionResponseSuccessorCheckpointMatrix(t, admitTestPostgresStore(t, db), db, false)
}

func proveCompletionResponseSuccessorCheckpointMatrix(t *testing.T, store completionSettlementTestStore, db *sql.DB, sqlite bool) {
	t.Helper()
	adapters := []string{"anthropic_api", "openai_compatible", "openai_responses", "claude_cli", "mock_python"}
	memoryPlans := []struct {
		name string
		plan agentmemory.Plan
	}{
		{name: "memory", plan: agentmemory.Authored(true)},
		{name: "stateless", plan: agentmemory.PlatformDefault()},
	}
	for _, memoryPlan := range memoryPlans {
		for adapterIndex, adapter := range adapters {
			t.Run(memoryPlan.name+"/"+adapter, func(t *testing.T) {
				fixture := newCompletionSettlementFixtureWithMemory(t, store, db, sqlite, memoryPlan.plan)
				fixture.authority.BudgetScopes = []runtimeeffects.BudgetAdmissionScope{{Kind: "system", CapUSD: 100}}
				fixture.context = fixture.contextFor(fixture.authority)
				request := "successor-checkpoint:" + memoryPlan.name + ":" + adapter
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
					t.Fatalf("settle completion: %v", err)
				}
				controller := newCompletionControllerForTest(store)
				recoveryCtx := managedExecutionStoreTestContext(t, fixture.context)
				settled, found, err := controller.RecoverCompletionContinuation(recoveryCtx, fixture.sessionID, settlement.AgentTurn.Memory)
				if err != nil || !found {
					t.Fatalf("recover settled response: found=%v err=%v", found, err)
				}
				snapshot, ok := settled.CompletionContinuation()
				if !ok {
					t.Fatal("settled completion continuation missing")
				}
				if settled.Attempt().AttemptID != handle.Attempt().AttemptID || snapshot.Phase != runtimeeffects.CompletionProjectionResponseSettled {
					t.Fatalf("settled recovery attempt=%s phase=%s", settled.Attempt().AttemptID, snapshot.Phase)
				}
				projectionMessages, err := json.Marshal(messages)
				if err != nil {
					t.Fatal(err)
				}
				if err := settled.ProjectCompletionConversation(ctx, runtimeeffects.CompletionConversationProjection{
					Payload: snapshot.Payload, SessionID: fixture.sessionID, Identity: settlement.AgentTurn.Identity,
					Memory: settlement.AgentTurn.Memory, ExpectedTurnCount: 0, TurnCount: 1, Messages: projectionMessages,
				}); err != nil {
					t.Fatalf("project completion: %v", err)
				}
				projected, found, err := controller.RecoverCompletionContinuation(recoveryCtx, fixture.sessionID, settlement.AgentTurn.Memory)
				if err != nil || !found {
					t.Fatalf("recover projected response: found=%v err=%v", found, err)
				}
				projectedSnapshot, ok := projected.CompletionContinuation()
				if !ok || projectedSnapshot.Phase != runtimeeffects.CompletionProjectionConversationProjected {
					t.Fatalf("projected recovery=%+v present=%v", projectedSnapshot, ok)
				}
				toolResult := json.RawMessage(`[{"name":"echo","ok":true,"result":{"value":"stable"}}]`)
				if adapterIndex%2 == 1 {
					toolResult = json.RawMessage(`[{"name":"read","ok":true,"result":{"value":1}},{"name":"write","ok":false,"failure":{"class":"dependency_unavailable","detail":{"code":"tool_failed"}}}]`)
				}
				successor, err := agentframe.NewToolContinuation(frameID, toolResult)
				if err != nil {
					t.Fatal(err)
				}
				if err := projected.ConsumeCompletionResponse(ctx, &successor); err != nil {
					t.Fatalf("consume response with exact successor: %v", err)
				}

				recovered, found, err := controller.RecoverCompletionContinuation(recoveryCtx, fixture.sessionID, settlement.AgentTurn.Memory)
				if err != nil || !found {
					t.Fatalf("recover consumed successor: found=%v err=%v", found, err)
				}
				recoveredSnapshot, ok := recovered.CompletionContinuation()
				committed, hasSuccessor := recoveredSnapshot.ToolContinuation()
				if recovered.Attempt().AttemptID != handle.Attempt().AttemptID || !ok || recoveredSnapshot.Phase != runtimeeffects.CompletionProjectionResponseConsumed || !hasSuccessor ||
					committed.ParentFrameID() != successor.ParentFrameID() || string(committed.ToolResult()) != string(successor.ToolResult()) {
					t.Fatalf("recovered checkpoint=%+v successor=%s present=%v", recoveredSnapshot, committed.ToolResult(), hasSuccessor)
				}
				if err := recovered.ConsumeCompletionResponse(ctx, &successor); err != nil {
					t.Fatalf("repeat exact successor consumption: %v", err)
				}
				foreign, err := agentframe.NewToolContinuation("agent-frame:v1:"+uuid.NewString(), successor.ToolResult())
				if err != nil {
					t.Fatal(err)
				}
				if err := recovered.ConsumeCompletionResponse(ctx, &foreign); err == nil {
					t.Fatal("consumed response accepted a different successor frame")
				}
				if err := requireCompletionPhaseDDLRejectsSuccessorRollback(fixture, handle.Attempt().AttemptID); err == nil {
					t.Fatal("fresh schema accepted a successor outside consumed phase")
				}

				successorAuthority := fixture.authority
				successorAuthority.Target.ID = uuid.NewString()
				successorCtx := runtimeeffects.WithLifecycleToken(fixture.contextFor(successorAuthority), successorAuthority.Normal)
				successorCtx = runtimeeffects.WithLogicalOperationIdentity(successorCtx, request+":successor")
				successorHandle := beginObservedCompletionForSettlementTest(t, successorCtx, adapter, request+":successor")
				successorSettlement := completionSettlementForTest(t, successorHandle.Attempt().Authority.Target, fixture, adapter, "", "")
				successorSettlement.ProviderHead = nil
				successorFrameID := "agent-frame:v1:" + uuid.NewString()
				successorPayload, _ := completionSuccessorPayload(t, adapter, fixture, successorSettlement, successorFrameID)
				if err := runtimeeffects.AttachCompletionContinuationEvidence(successorSettlement.Settlement.Evidence, []byte(request+":successor"), successorPayload); err != nil {
					t.Fatal(err)
				}
				if _, err := successorHandle.SettleCompletion(successorCtx, successorSettlement); err != nil {
					t.Fatalf("settle exact successor frame: %v", err)
				}
				next, found, err := controller.RecoverCompletionContinuation(recoveryCtx, fixture.sessionID, successorSettlement.AgentTurn.Memory)
				if err != nil || !found {
					t.Fatalf("recover successor settlement: found=%v err=%v", found, err)
				}
				if next.Attempt().AttemptID != successorHandle.Attempt().AttemptID {
					t.Fatalf("recovered successor attempt=%s, want %s", next.Attempt().AttemptID, successorHandle.Attempt().AttemptID)
				}
				requireActiveCompletionContinuation(t, fixture, handle.Attempt().AttemptID, false)
				requireActiveCompletionContinuation(t, fixture, successorHandle.Attempt().AttemptID, true)
			})
		}
	}
}

func completionSuccessorPayload(t testing.TB, adapter string, fixture completionSettlementFixture, settlement runtimeeffects.CompletionSettlement, frameID string) (json.RawMessage, []runtimellm.Message) {
	t.Helper()
	assistant := runtimellm.Message{
		Role: "assistant", Content: "use tool",
		ToolCalls: []runtimellm.ToolCall{{ID: "call-1", Name: "echo", Arguments: map[string]any{"value": "stable"}}},
	}
	messages := []runtimellm.Message{{Role: "user", Content: "start"}, assistant}
	document := struct {
		Version    string                         `json:"version"`
		Adapter    string                         `json:"adapter"`
		Response   runtimellm.Response            `json:"response"`
		Usage      runtimeeffects.CompletionUsage `json:"usage"`
		Projection struct {
			SessionID         string               `json:"session_id"`
			ExpectedTurnCount int                  `json:"expected_turn_count"`
			TurnCount         int                  `json:"turn_count"`
			Messages          []runtimellm.Message `json:"messages"`
			Identity          agentmemory.Identity `json:"identity"`
			Memory            agentmemory.Plan     `json:"memory"`
			FrameID           string               `json:"frame_id"`
		} `json:"projection"`
	}{
		Version: "provider-response-continuation.v1", Adapter: adapter,
		Response: runtimellm.Response{
			Message: assistant, ToolCalls: assistant.ToolCalls, Raw: []byte(`{"response":"settled"}`),
			ToolOutputAuthority: &runtimellm.ToolOutputAuthority{ProviderOperationID: uuid.NewString(), SettledAt: time.Unix(10, 0).UTC()},
		},
		Usage: runtimeeffects.CompletionUsage{ResolvedModel: "test-model", Exactness: runtimeeffects.CompletionUsageUnavailable},
	}
	document.Projection.SessionID = fixture.sessionID
	document.Projection.ExpectedTurnCount = 0
	document.Projection.TurnCount = 1
	document.Projection.Messages = messages
	document.Projection.Identity = settlement.AgentTurn.Identity
	document.Projection.Memory = settlement.AgentTurn.Memory
	document.Projection.FrameID = frameID
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return raw, messages
}

func requireCompletionPhaseDDLRejectsSuccessorRollback(fixture completionSettlementFixture, attemptID string) error {
	if fixture.sqlite {
		_, err := fixture.db.Exec(`UPDATE runtime_external_effect_attempts SET completion_projection_phase='conversation_projected' WHERE attempt_id=?`, attemptID)
		return err
	}
	_, err := fixture.db.Exec(`UPDATE runtime_external_effect_attempts SET completion_projection_phase='conversation_projected' WHERE attempt_id=$1::uuid`, attemptID)
	return err
}

func proveSettledProviderResponseContinuationAdvancesExactActiveTurn(t *testing.T, fixture completionSettlementFixture) {
	t.Helper()
	const adapter = "anthropic_api"

	firstCtx := runtimeeffects.WithLifecycleToken(fixture.context, fixture.authority.Normal)
	firstCtx = runtimeeffects.WithLogicalOperationIdentity(firstCtx, "active-completion-continuation:first")
	first := beginObservedCompletionForSettlementTest(t, firstCtx, adapter, "first-provider-request")
	firstSettlement := completionSettlementForTest(t, first.Attempt().Authority.Target, fixture, adapter, "", "")
	firstSettlement.ProviderHead = nil
	firstPayload := json.RawMessage(`{"version":"first-continuation"}`)
	if err := runtimeeffects.AttachCompletionContinuationEvidence(firstSettlement.Settlement.Evidence, []byte("first-provider-request"), firstPayload); err != nil {
		t.Fatal(err)
	}
	if _, err := first.SettleCompletion(firstCtx, firstSettlement); err != nil {
		t.Fatalf("settle first completion continuation: %v", err)
	}
	firstSnapshot, ok := first.CompletionContinuation()
	if !ok {
		t.Fatal("first committed completion continuation is missing")
	}
	if err := first.ProjectCompletionConversation(firstCtx, runtimeeffects.CompletionConversationProjection{
		Payload: firstSnapshot.Payload, SessionID: fixture.sessionID,
		Identity: firstSettlement.AgentTurn.Identity, Memory: firstSettlement.AgentTurn.Memory,
		ExpectedTurnCount: 0, TurnCount: 1,
		Messages: json.RawMessage(`[{"role":"user","content":"first"},{"role":"assistant","content":"first done"}]`),
	}); err != nil {
		t.Fatalf("project first completion continuation: %v", err)
	}
	if err := first.ConsumeCompletionResponse(firstCtx, nil); err != nil {
		t.Fatalf("consume first completion continuation: %v", err)
	}

	secondAuthority := fixture.authority
	secondAuthority.Target.ID = uuid.NewString()
	secondCtx := runtimeeffects.WithLifecycleToken(fixture.contextFor(secondAuthority), secondAuthority.Normal)
	secondCtx = runtimeeffects.WithLogicalOperationIdentity(secondCtx, "active-completion-continuation:second")
	second := beginObservedCompletionForSettlementTest(t, secondCtx, adapter, "second-provider-request")
	secondSettlement := completionSettlementForTest(t, second.Attempt().Authority.Target, fixture, adapter, "", "")
	secondSettlement.ProviderHead = nil
	secondPayload := json.RawMessage(`{"version":"second-continuation"}`)
	if err := runtimeeffects.AttachCompletionContinuationEvidence(secondSettlement.Settlement.Evidence, []byte("second-provider-request"), secondPayload); err != nil {
		t.Fatal(err)
	}
	if _, err := second.SettleCompletion(secondCtx, secondSettlement); err != nil {
		t.Fatalf("settle second completion continuation: %v", err)
	}

	recoveryCtx := managedExecutionStoreTestContext(t, fixture.contextFor(secondAuthority))
	controller := newCompletionControllerForTest(fixture.store)
	recovered, found, err := controller.RecoverCompletionContinuation(recoveryCtx, fixture.sessionID, secondSettlement.AgentTurn.Memory)
	if err != nil || !found {
		t.Fatalf("recover exact active completion continuation: found=%v err=%v", found, err)
	}
	if recovered.Attempt().AttemptID != second.Attempt().AttemptID {
		t.Fatalf("recovered attempt=%s, want active second attempt %s", recovered.Attempt().AttemptID, second.Attempt().AttemptID)
	}
	recoveredSnapshot, ok := recovered.CompletionContinuation()
	var recoveredDocument map[string]string
	if !ok || json.Unmarshal(recoveredSnapshot.Payload, &recoveredDocument) != nil || recoveredDocument["version"] != "second-continuation" {
		t.Fatalf("recovered active continuation=%s present=%v, want second payload", recoveredSnapshot.Payload, ok)
	}
	requireActiveCompletionContinuation(t, fixture, first.Attempt().AttemptID, false)
	requireActiveCompletionContinuation(t, fixture, second.Attempt().AttemptID, true)
}

func proveSettledProviderResponseRebindsStatelessSession(t *testing.T, fixture completionSettlementFixture) {
	t.Helper()
	const request = "stable-stateless-provider-request"
	ctx := runtimeeffects.WithLogicalOperationIdentity(fixture.context, "settled-stateless-provider-response-continuation")
	handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", request)
	settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, "anthropic_api", "", "")
	settlement.ProviderHead = nil
	if err := runtimeeffects.AttachCompletionContinuationEvidence(settlement.Settlement.Evidence, []byte(request), json.RawMessage(`{"version":"test-stateless-continuation"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.SettleCompletion(ctx, settlement); err != nil {
		t.Fatalf("settle stateless completion: %v", err)
	}
	requireCommittedCompletionContinuation(t, handle, "test-stateless-continuation")

	expireProviderOriginLease(t, fixture, fixture.origin)
	deliveryStore := fixture.store.(deliveryFixtureStore)
	snapshot, err := deliveryStore.Snapshot(testAuthorActivityContext(), fixture.origin.DeliveryID())
	if err != nil {
		t.Fatal(err)
	}
	event := loadCompletionOriginEvent(t, fixture, snapshot.EventID)
	claimed, err := deliveryStore.ClaimDelivery(testAuthorActivityContext(), snapshot.Authority, event, snapshot.Route)
	if err != nil {
		t.Fatal(err)
	}
	reclaimed, ok := claimed.Acquired()
	if !ok {
		t.Fatalf("stateless completion origin reclaim disposition=%s", claimed.Disposition)
	}

	successor := fixture.authority
	successor.Normal.Generation++
	successor.FenceGeneration = successor.Normal.Generation
	successor.ExecutionOwner = "stateless-completion-successor"
	successor.Target = runtimeeffects.UsageTarget{}
	setCompletionFixtureGeneration(t, fixture, int(successor.Normal.Generation))
	currentSessionID := uuid.NewString()
	successorCtx := runtimeeffects.WithLifecycleToken(testAuthorActivityContext(), successor.Normal)
	successorCtx = runtimeeffects.WithAuthority(successorCtx, successor)
	successorCtx = managedExecutionStoreTestContext(t, successorCtx)
	successorCtx = runtimedelivery.WithClaim(successorCtx, reclaimed.Claim)
	controller := newCompletionControllerForTest(fixture.store)
	successorCtx = runtimeeffects.WithController(successorCtx, controller)
	continuation, found, err := controller.RecoverCompletionContinuation(successorCtx, currentSessionID, agentmemory.PlatformDefault())
	if err != nil || !found {
		t.Fatalf("recover stateless completion continuation: found=%v err=%v", found, err)
	}
	got := continuation.Attempt()
	continuationSnapshot, present := continuation.CompletionContinuation()
	if !present || got.Authority.Target.SessionID != currentSessionID || continuationSnapshot.Surface.Authority.SessionID != currentSessionID {
		t.Fatalf("stateless continuation target=%#v surface=%#v present=%v", got.Authority.Target, continuationSnapshot.Surface.Authority, present)
	}
	projection := runtimeeffects.CompletionConversationProjection{
		Payload: continuationSnapshot.Payload, SessionID: currentSessionID,
		Identity: settlement.AgentTurn.Identity, Memory: agentmemory.PlatformDefault(),
		ExpectedTurnCount: 0, TurnCount: 1, Messages: json.RawMessage(`[{"role":"user","content":"hello"},{"role":"assistant","content":"done"}]`),
	}
	if err := continuation.ProjectCompletionConversation(successorCtx, projection); err != nil {
		t.Fatalf("project stateless completion continuation: %v", err)
	}
	if err := continuation.ConsumeCompletionResponse(successorCtx, nil); err != nil {
		t.Fatalf("consume stateless completion continuation: %v", err)
	}
	requireCompletionProjectionPhase(t, fixture, handle.Attempt().AttemptID, runtimeeffects.CompletionProjectionResponseConsumed)
	requireProviderAttemptCount(t, fixture, 1)
}

func proveSettledProviderResponseContinuesExactReclaimedDelivery(t *testing.T, fixture completionSettlementFixture) {
	t.Helper()
	const operation = "settled-provider-response-continuation"
	const request = "stable-provider-request"
	ctx := runtimeeffects.WithLogicalOperationIdentity(fixture.context, operation)
	handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", request)
	settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, "anthropic_api", "", "")
	settlement.ProviderHead = nil
	if err := runtimeeffects.AttachCompletionContinuationEvidence(settlement.Settlement.Evidence, []byte(request), json.RawMessage(`{"version":"test-continuation"}`)); err != nil {
		t.Fatalf("attach completion continuation evidence: %v", err)
	}
	if _, err := handle.SettleCompletion(ctx, settlement); err != nil {
		t.Fatalf("settle original completion: %v", err)
	}
	requireCommittedCompletionContinuation(t, handle, "test-continuation")

	expireProviderOriginLease(t, fixture, fixture.origin)
	deliveryStore := fixture.store.(deliveryFixtureStore)
	snapshot, err := deliveryStore.Snapshot(testAuthorActivityContext(), fixture.origin.DeliveryID())
	if err != nil {
		t.Fatalf("load expired origin snapshot: %v", err)
	}
	event := loadCompletionOriginEvent(t, fixture, snapshot.EventID)
	claimed, err := deliveryStore.ClaimDelivery(testAuthorActivityContext(), snapshot.Authority, event, snapshot.Route)
	if err != nil {
		t.Fatalf("reclaim exact completion origin: %v", err)
	}
	reclaimed, ok := claimed.Acquired()
	if !ok {
		t.Fatalf("completion origin reclaim disposition=%s, want acquired", claimed.Disposition)
	}

	successor := fixture.authority
	successor.Normal.Generation++
	successor.FenceGeneration = successor.Normal.Generation
	successor.ExecutionOwner = "completion-successor"
	successor.Target = runtimeeffects.UsageTarget{}
	setCompletionFixtureGeneration(t, fixture, int(successor.Normal.Generation))
	successorCtx := runtimeeffects.WithLifecycleToken(testAuthorActivityContext(), successor.Normal)
	successorCtx = runtimeeffects.WithController(runtimeeffects.WithAuthority(successorCtx, successor), newCompletionControllerForTest(fixture.store))
	successorCtx = managedExecutionStoreTestContext(t, successorCtx)
	successorCtx = runtimedelivery.WithClaim(successorCtx, reclaimed.Claim)
	controller := newCompletionControllerForTest(fixture.store)
	successorCtx = runtimeeffects.WithController(successorCtx, controller)
	if _, found, err := controller.RecoverCompletionContinuation(successorCtx, uuid.NewString(), settlement.AgentTurn.Memory); err == nil || found {
		t.Fatalf("memory continuation accepted a different session: found=%v err=%v", found, err)
	}
	continuation, found, err := controller.RecoverCompletionContinuation(successorCtx, fixture.sessionID, settlement.AgentTurn.Memory)
	if err != nil {
		t.Fatalf("recover settled completion continuation: %v", err)
	}
	if !found {
		t.Fatal("settled completion continuation was not found")
	}
	continuationSnapshot, ok := continuation.CompletionContinuation()
	var replayDocument map[string]string
	if !ok || json.Unmarshal(continuationSnapshot.Payload, &replayDocument) != nil || replayDocument["version"] != "test-continuation" || string(continuationSnapshot.Request) != request {
		t.Fatalf("completion continuation=%+v present=%v", continuationSnapshot, ok)
	}
	if continuation.Attempt().AttemptID != handle.Attempt().AttemptID || !continuation.Attempt().Origin.Same(runtimeeffects.CompletionOrigin{Kind: runtimeeffects.CompletionOriginDelivery, Delivery: reclaimed.Claim}) {
		t.Fatalf("completion continuation attempt=%+v does not carry current exact origin", continuation.Attempt())
	}
	if err := continuation.MarkLaunched(successorCtx); err == nil {
		t.Fatal("completion continuation admitted a second provider launch")
	}
	messages := json.RawMessage(`[{"role":"user","content":"hello"},{"role":"assistant","content":"done"}]`)
	projection := runtimeeffects.CompletionConversationProjection{
		Payload: continuationSnapshot.Payload, SessionID: fixture.sessionID,
		Identity: settlement.AgentTurn.Identity, Memory: settlement.AgentTurn.Memory,
		ExpectedTurnCount: 0, TurnCount: 1, Messages: messages,
	}
	if err := continuation.ConsumeCompletionResponse(successorCtx, nil); err == nil {
		t.Fatal("completion response was consumed before conversation projection")
	}
	hostileProjection := projection
	hostileProjection.Payload = json.RawMessage(`{"version":"foreign-continuation"}`)
	if err := continuation.ProjectCompletionConversation(successorCtx, hostileProjection); err == nil {
		t.Fatal("completion conversation accepted foreign immutable evidence")
	}
	requireCompletionProjectionState(t, fixture, handle.Attempt().AttemptID, runtimeeffects.CompletionProjectionResponseSettled, 0, json.RawMessage(`[]`))
	if err := continuation.ProjectCompletionConversation(successorCtx, projection); err != nil {
		t.Fatalf("project settled completion conversation: %v", err)
	}
	if err := continuation.ProjectCompletionConversation(successorCtx, projection); err != nil {
		t.Fatalf("repeat settled completion conversation projection: %v", err)
	}
	recovered, found, err := controller.RecoverCompletionContinuation(successorCtx, fixture.sessionID, settlement.AgentTurn.Memory)
	if err != nil || !found {
		t.Fatalf("recover projected completion continuation: found=%v err=%v", found, err)
	}
	projectedSnapshot, ok := recovered.CompletionContinuation()
	if !ok || projectedSnapshot.Phase != runtimeeffects.CompletionProjectionConversationProjected {
		t.Fatalf("projected continuation=%+v present=%v", projectedSnapshot, ok)
	}
	if err := recovered.ConsumeCompletionResponse(successorCtx, nil); err != nil {
		t.Fatalf("consume settled completion response: %v", err)
	}
	if err := recovered.ConsumeCompletionResponse(successorCtx, nil); err != nil {
		t.Fatalf("repeat settled completion response consumption: %v", err)
	}
	requireCompletionProjectionState(t, fixture, handle.Attempt().AttemptID, runtimeeffects.CompletionProjectionResponseConsumed, 1, messages)
	requireProviderAttemptCount(t, fixture, 1)
	requireCompletionSettlementRows(t, fixture, handle.Attempt().AttemptID, settlement.AgentTurn.TurnID, runtimeeffects.StateSettled, 1, 0)

}

func requireCommittedCompletionContinuation(t *testing.T, handle *runtimeeffects.Handle, wantVersion string) {
	t.Helper()
	snapshot, ok := handle.CompletionContinuation()
	if !ok || snapshot.Phase != runtimeeffects.CompletionProjectionResponseSettled {
		t.Fatalf("committed completion continuation=%+v present=%v", snapshot, ok)
	}
	var document map[string]string
	if err := json.Unmarshal(snapshot.Payload, &document); err != nil || document["version"] != wantVersion {
		t.Fatalf("committed completion payload=%s err=%v, want version %q", snapshot.Payload, err, wantVersion)
	}
}

func requireCompletionProjectionState(t *testing.T, fixture completionSettlementFixture, attemptID string, wantPhase runtimeeffects.CompletionProjectionPhase, wantTurnCount int, wantMessages json.RawMessage) {
	t.Helper()
	phaseQuery := `SELECT completion_projection_phase FROM runtime_external_effect_attempts WHERE attempt_id=?`
	conversationQuery := `SELECT turn_count,conversation FROM agent_sessions WHERE session_id=?`
	if !fixture.sqlite {
		phaseQuery = `SELECT completion_projection_phase FROM runtime_external_effect_attempts WHERE attempt_id=$1::uuid`
		conversationQuery = `SELECT turn_count,conversation FROM agent_sessions WHERE session_id=$1::uuid`
	}
	var phase string
	if err := fixture.db.QueryRow(phaseQuery, attemptID).Scan(&phase); err != nil || phase != string(wantPhase) {
		t.Fatalf("completion projection phase=%q err=%v, want %q", phase, err, wantPhase)
	}
	var turnCount int
	var raw []byte
	if err := fixture.db.QueryRow(conversationQuery, fixture.sessionID).Scan(&turnCount, &raw); err != nil {
		t.Fatalf("load projected completion conversation: %v", err)
	}
	var gotMessages, expectedMessages any
	if err := json.Unmarshal(raw, &gotMessages); err != nil {
		t.Fatalf("decode projected completion conversation: %v", err)
	}
	if err := json.Unmarshal(wantMessages, &expectedMessages); err != nil {
		t.Fatalf("decode expected completion conversation: %v", err)
	}
	if turnCount != wantTurnCount || !reflect.DeepEqual(gotMessages, expectedMessages) {
		t.Fatalf("projected completion conversation turn=%d messages=%s, want turn=%d messages=%s", turnCount, raw, wantTurnCount, wantMessages)
	}
}

func requireCompletionProjectionPhase(t *testing.T, fixture completionSettlementFixture, attemptID string, wantPhase runtimeeffects.CompletionProjectionPhase) {
	t.Helper()
	query := `SELECT completion_projection_phase FROM runtime_external_effect_attempts WHERE attempt_id=?`
	if !fixture.sqlite {
		query = `SELECT completion_projection_phase FROM runtime_external_effect_attempts WHERE attempt_id=$1::uuid`
	}
	var phase string
	if err := fixture.db.QueryRow(query, attemptID).Scan(&phase); err != nil || phase != string(wantPhase) {
		t.Fatalf("completion projection phase=%q err=%v, want %q", phase, err, wantPhase)
	}
}

func requireActiveCompletionContinuation(t *testing.T, fixture completionSettlementFixture, attemptID string, want bool) {
	t.Helper()
	query := `SELECT completion_continuation_active FROM runtime_external_effect_attempts WHERE attempt_id=?`
	if !fixture.sqlite {
		query = `SELECT completion_continuation_active FROM runtime_external_effect_attempts WHERE attempt_id=$1::uuid`
	}
	var got bool
	if err := fixture.db.QueryRow(query, attemptID).Scan(&got); err != nil || got != want {
		t.Fatalf("completion continuation active=%v err=%v, want %v", got, err, want)
	}
}

func loadCompletionOriginEvent(t *testing.T, fixture completionSettlementFixture, eventID string) events.Event {
	t.Helper()
	if fixture.sqlite {
		return loadSQLiteDeliveryFixtureEvent(t, testAuthorActivityContext(), fixture.db, eventID)
	}
	return loadPostgresDeliveryFixtureEvent(t, testAuthorActivityContext(), fixture.db, eventID)
}

func proveCompletionProviderHeadSettlement(t *testing.T, fixture completionSettlementFixture) {
	t.Helper()
	ctx := runtimeeffects.WithLogicalOperationIdentity(fixture.context, "provider-head:success")
	handle := beginObservedCompletionForSettlementTest(t, ctx, "claude_cli", "success")
	settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, "claude_cli", "provider-head-current", "provider-head-next")
	settled, err := handle.SettleCompletion(ctx, settlement)
	if err != nil {
		t.Fatalf("settle completion with provider head: %v", err)
	}
	if !settled.Committed || settled.Disposition != runtimeeffects.CompletionSettlementCurrent || settled.Drained() {
		t.Fatalf("current settlement result=%+v", settled)
	}
	requireProviderHead(t, fixture.db, fixture.sqlite, fixture.sessionID, "provider-head-next")
	requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, runtimeeffects.StateSettled)
	requireCompletionSettlementRows(t, fixture, handle.Attempt().AttemptID, settlement.AgentTurn.TurnID, runtimeeffects.StateSettled, 1, 0)
}

func TestCompletionProviderHeadConflictCommitsUncertaintySQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveCompletionProviderHeadConflictCommitsUncertainty(t, newCompletionSettlementFixture(t, store, store.backend.ConstructionHandle(), true))
}

func TestCompletionProviderHeadConflictCommitsUncertaintyPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveCompletionProviderHeadConflictCommitsUncertainty(t, newCompletionSettlementFixture(t, admitTestPostgresStore(t, db), db, false))
}

func proveCompletionProviderHeadConflictCommitsUncertainty(t *testing.T, fixture completionSettlementFixture) {
	t.Helper()
	ctx := runtimeeffects.WithLogicalOperationIdentity(fixture.context, "provider-head:conflict")
	handle := beginObservedCompletionForSettlementTest(t, ctx, "claude_cli", "conflict")
	settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, "claude_cli", "stale-provider-head", "provider-head-next")
	settled, err := handle.SettleCompletion(ctx, settlement)
	if err == nil {
		t.Fatal("provider-head conflict returned nil")
	}
	if !settled.Committed || settled.Disposition != runtimeeffects.CompletionSettlementCurrent {
		t.Fatalf("provider-head conflict result=%+v, want committed current uncertainty", settled)
	}
	failure, ok := runtimefailures.As(err)
	if !ok || failure.Failure.Detail.Code != "provider_head_cas_conflict" {
		t.Fatalf("provider-head conflict error=%v, want original provider_head_cas_conflict", err)
	}
	requireProviderHead(t, fixture.db, fixture.sqlite, fixture.sessionID, "provider-head-current")
	requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, runtimeeffects.StateOutcomeUncertain)
	requireCompletionSettlementRows(t, fixture, handle.Attempt().AttemptID, settlement.AgentTurn.TurnID, runtimeeffects.StateOutcomeUncertain, 1, 0)

	query := `SELECT COALESCE(json_extract(failure, '$.detail.code'), '') FROM agent_turns WHERE completion_attempt_id=?`
	if !fixture.sqlite {
		query = `SELECT COALESCE(failure->'detail'->>'code', '') FROM agent_turns WHERE completion_attempt_id=$1::uuid`
	}
	var code string
	if err := fixture.db.QueryRow(query, handle.Attempt().AttemptID).Scan(&code); err != nil || code != "provider_head_cas_conflict" {
		t.Fatalf("completion turn failure code=%q err=%v, want provider_head_cas_conflict", code, err)
	}
}

func TestCompletionProviderHeadStaleAuthorityCannotSettleSQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveCompletionProviderHeadStaleAuthorityCannotSettle(t, newCompletionSettlementFixture(t, store, store.backend.ConstructionHandle(), true))
}

func TestCompletionProviderHeadStaleAuthorityCannotSettlePostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveCompletionProviderHeadStaleAuthorityCannotSettle(t, newCompletionSettlementFixture(t, admitTestPostgresStore(t, db), db, false))
}

func TestCompletionPrelaunchFailureDoesNotSpendSQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveCompletionPrelaunchFailureDoesNotSpend(t, newCompletionSettlementFixture(t, store, store.backend.ConstructionHandle(), true))
}

func TestCompletionPrelaunchFailureDoesNotSpendPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveCompletionPrelaunchFailureDoesNotSpend(t, newCompletionSettlementFixture(t, admitTestPostgresStore(t, db), db, false))
}

func TestCompletionLaunchBoundaryFailureRemainsAttemptOnlySQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveCompletionLaunchBoundaryFailureRemainsAttemptOnly(t, newCompletionSettlementFixture(t, store, store.backend.ConstructionHandle(), true))
}

func TestCompletionLaunchBoundaryFailureRemainsAttemptOnlyPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveCompletionLaunchBoundaryFailureRemainsAttemptOnly(t, newCompletionSettlementFixture(t, admitTestPostgresStore(t, db), db, false))
}

func proveCompletionLaunchBoundaryFailureRemainsAttemptOnly(t *testing.T, fixture completionSettlementFixture) {
	t.Helper()
	adapters := []string{"anthropic_api", "openai_compatible", "openai_responses", "claude_cli", "mock_python"}
	launchStates := []struct {
		name            string
		durablyLaunched bool
	}{
		{name: "error_before_commit"},
		{name: "commit_then_error", durablyLaunched: true},
	}
	for _, adapter := range adapters {
		for _, launchState := range launchStates {
			t.Run(adapter+"/"+launchState.name, func(t *testing.T) {
				authority := fixture.authority
				authority.Target.ID = uuid.NewString()
				ctx := runtimeeffects.WithLogicalOperationIdentity(fixture.contextFor(authority), "no-provider-invocation:"+adapter+":"+launchState.name)
				ctx = withManagedCompletionTestSurface(t, ctx, authority, adapter)
				request := []byte("no-provider-invocation:" + adapter)
				handle, err := beginManagedCompletionForTest(t, ctx, adapter, request)
				if err != nil {
					t.Fatalf("authorize launch-boundary completion: %v", err)
				}
				if launchState.durablyLaunched {
					if err := handle.MarkLaunched(ctx); err != nil {
						t.Fatalf("commit launch marker before acknowledgement loss: %v", err)
					}
				}
				failureErr := runtimefailures.New(
					runtimefailures.ClassLifecycleConflict,
					"provider_launch_marker_failed",
					"completion-test",
					"launch_provider",
					map[string]any{"prelaunch": true},
				)
				failure, _ := runtimefailures.EnvelopeFromError(failureErr)
				if err := handle.Settle(ctx, runtimeeffects.StateTerminalFailure, &failure, map[string]any{"prelaunch": true}); err != nil {
					t.Fatalf("settle no-invocation attempt: %v", err)
				}
				requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, runtimeeffects.StateTerminalFailure)
				requireCompletionRecoveryRows(t, fixture, handle.Attempt().AttemptID, 0, 0, 0)
				requireCompletionOperationAttemptCount(t, fixture, handle.Attempt().OperationID, 1)

				if _, err := beginManagedCompletionForTest(t, ctx, adapter, request); err == nil {
					t.Fatal("non-allowlisted provider admitted a retry after proven zero invocation")
				}
				requireCompletionOperationAttemptCount(t, fixture, handle.Attempt().OperationID, 1)
			})
		}
	}
}

func proveCompletionPrelaunchFailureDoesNotSpend(t *testing.T, fixture completionSettlementFixture) {
	t.Helper()
	ctx := runtimeeffects.WithLogicalOperationIdentity(fixture.context, "completion-prelaunch-failure")
	ctx = withManagedCompletionTestSurface(t, ctx, fixture.authority, "claude_cli")
	handle, err := beginManagedCompletionForTest(t, ctx, "claude_cli", []byte("prelaunch"))
	if err != nil {
		t.Fatalf("authorize prelaunch completion: %v", err)
	}
	failure := runtimefailures.FromError(context.Canceled, "completion-test", "launch_rejected")
	if err := handle.Settle(ctx, runtimeeffects.StateTerminalFailure, &failure.Failure, map[string]any{"prelaunch": true}); err != nil {
		t.Fatalf("settle prelaunch attempt: %v", err)
	}
	requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, runtimeeffects.StateTerminalFailure)
	requireCompletionRecoveryRows(t, fixture, handle.Attempt().AttemptID, 0, 0, 0)
}

func TestCompletionRecoveryPreservesLiveOrdinaryAuthoritySQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveCompletionRecoveryPreservesLiveOrdinaryAuthority(t, newCompletionSettlementFixture(t, store, store.backend.ConstructionHandle(), true))
}

func TestCompletionRecoveryPreservesLiveOrdinaryAuthorityPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveCompletionRecoveryPreservesLiveOrdinaryAuthority(t, newCompletionSettlementFixture(t, admitTestPostgresStore(t, db), db, false))
}

func TestCompletionAttemptHeartbeatFencesRecoverySQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveCompletionAttemptHeartbeatFencesRecovery(t, newCompletionSettlementFixture(t, store, store.backend.ConstructionHandle(), true))
}

func TestCompletionAttemptHeartbeatFencesRecoveryPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveCompletionAttemptHeartbeatFencesRecovery(t, newCompletionSettlementFixture(t, admitTestPostgresStore(t, db), db, false))
}

func TestDeliveryClaimRenewalPublishesCompleteRunForkRevisionSQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveDeliveryClaimRenewalPublishesCompleteRunForkRevision(t, newCompletionSettlementFixture(t, store, store.backend.ConstructionHandle(), true))
}

func TestDeliveryClaimRenewalPublishesCompleteRunForkRevisionPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveDeliveryClaimRenewalPublishesCompleteRunForkRevision(t, newCompletionSettlementFixture(t, admitTestPostgresStore(t, db), db, false))
}

func proveDeliveryClaimRenewalPublishesCompleteRunForkRevision(t *testing.T, fixture completionSettlementFixture) {
	t.Helper()
	ctx := testAuthorActivityContext()
	setProviderOriginLease(t, fixture, fixture.origin, time.Now().UTC().Add(30*time.Second))
	before := providerOriginLease(t, fixture, fixture.origin)
	publishCompleteRunForkRevisionBaseline(t, ctx, fixture.db, !fixture.sqlite, fixture.authority.Target.RunID)
	if _, err := fixture.store.RenewClaim(ctx, fixture.origin); err != nil {
		t.Fatalf("renew delivery claim: %v", err)
	}
	after := providerOriginLease(t, fixture, fixture.origin)
	if !after.After(before) {
		t.Fatalf("renewed delivery lease=%s, want after prior lease %s", after, before)
	}
	revisionStore, ok := fixture.store.(authorActivityReceiptStore)
	if !ok {
		t.Fatalf("delivery store %T cannot validate run-fork revision", fixture.store)
	}
	requireCompleteRunForkRevision(t, ctx, authorActivityReceiptFixture{store: revisionStore, db: fixture.db}, fixture.authority.Target.RunID)
}

func proveCompletionAttemptHeartbeatFencesRecovery(t *testing.T, fixture completionSettlementFixture) {
	t.Helper()
	ctx := runtimeeffects.WithLogicalOperationIdentity(fixture.context, "completion-heartbeat")
	ctx = withManagedCompletionTestSurface(t, ctx, fixture.authority, "anthropic_api")
	handle, err := beginManagedCompletionForTest(t, ctx, "anthropic_api", []byte("heartbeat"))
	if err != nil {
		t.Fatalf("authorize heartbeat completion: %v", err)
	}
	before := time.Now().UTC().Add(-time.Minute)
	setCompletionAttemptLease(t, fixture, handle.Attempt().AttemptID, before)
	setProviderOriginLease(t, fixture, fixture.origin, time.Now().UTC().Add(30*time.Second))
	originLeaseBefore := providerOriginLease(t, fixture, fixture.origin)
	publishCompleteRunForkRevisionBaseline(t, ctx, fixture.db, !fixture.sqlite, fixture.authority.Target.RunID)
	heartbeatAt := time.Now().UTC()
	if err := fixture.store.HeartbeatCompletionAttempt(ctx, handle.Attempt(), heartbeatAt, 2*time.Minute); err != nil {
		t.Fatalf("heartbeat authorized completion: %v", err)
	}
	after := completionAttemptLease(t, fixture, handle.Attempt().AttemptID)
	if !after.After(heartbeatAt.Add(time.Minute)) {
		t.Fatalf("heartbeat lease=%s, want more than one minute of live authority", after)
	}
	originLeaseAfter := providerOriginLease(t, fixture, fixture.origin)
	if !originLeaseAfter.After(originLeaseBefore) {
		t.Fatalf("provider origin lease=%s, want after prior lease %s", originLeaseAfter, originLeaseBefore)
	}
	revisionStore, ok := fixture.store.(authorActivityReceiptStore)
	if !ok {
		t.Fatalf("completion store %T cannot validate run-fork revision", fixture.store)
	}
	requireCompleteRunForkRevision(t, ctx, authorActivityReceiptFixture{store: revisionStore, db: fixture.db}, fixture.authority.Target.RunID)
	if err := handle.MarkLaunched(ctx); err != nil {
		t.Fatalf("launch heartbeat completion: %v", err)
	}
	if err := handle.Heartbeat(ctx, 2*time.Minute); err != nil {
		t.Fatalf("heartbeat launched completion: %v", err)
	}
	stale := handle.Attempt()
	stale.Authority.FenceGeneration++
	if err := fixture.store.HeartbeatCompletionAttempt(ctx, stale, time.Now().UTC(), 2*time.Minute); err == nil {
		t.Fatal("stale completion fence renewed the attempt lease")
	}
	summary, err := fixture.store.ReconcileExternalEffectAttempts(testAuthorActivityContext(), liveExternalEffectRecoveryRequest(time.Now().UTC().Add(time.Minute)))
	if err != nil {
		t.Fatalf("reconcile heartbeating completion: %v", err)
	}
	if summary != (runtimeeffects.RecoverySummary{}) {
		t.Fatalf("heartbeating completion recovery summary=%+v, want empty", summary)
	}
	requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, runtimeeffects.StateLaunched)
}

func setCompletionAttemptLease(t *testing.T, fixture completionSettlementFixture, attemptID string, lease time.Time) {
	t.Helper()
	query := `UPDATE runtime_external_effect_attempts SET lease_expires_at=? WHERE attempt_id=?`
	if !fixture.sqlite {
		query = `UPDATE runtime_external_effect_attempts SET lease_expires_at=$1 WHERE attempt_id=$2::uuid`
	}
	if _, err := fixture.db.Exec(query, lease.UTC(), attemptID); err != nil {
		t.Fatalf("set completion attempt lease: %v", err)
	}
}

func completionAttemptLease(t *testing.T, fixture completionSettlementFixture, attemptID string) time.Time {
	t.Helper()
	if fixture.sqlite {
		var lease conversationForkTimeValue
		if err := fixture.db.QueryRow(`SELECT lease_expires_at FROM runtime_external_effect_attempts WHERE attempt_id=?`, attemptID).Scan(&lease); err != nil {
			t.Fatalf("load sqlite completion attempt lease: %v", err)
		}
		if !lease.Valid {
			t.Fatal("sqlite completion attempt lease is null")
		}
		return lease.Time.UTC()
	}
	var lease time.Time
	if err := fixture.db.QueryRow(`SELECT lease_expires_at FROM runtime_external_effect_attempts WHERE attempt_id=$1::uuid`, attemptID).Scan(&lease); err != nil {
		t.Fatalf("load completion attempt lease: %v", err)
	}
	return lease.UTC()
}

func proveCompletionRecoveryPreservesLiveOrdinaryAuthority(t *testing.T, fixture completionSettlementFixture) {
	t.Helper()
	ctx := runtimeeffects.WithLogicalOperationIdentity(fixture.context, "ordinary-recovery:authorized")
	ctx = withManagedCompletionTestSurface(t, ctx, fixture.authority, "anthropic_api")
	authorized, err := beginManagedCompletionForTest(t, ctx, "anthropic_api", []byte("authorized"))
	if err != nil {
		t.Fatalf("authorize live completion: %v", err)
	}
	now := time.Now().UTC()
	summary, err := fixture.store.ReconcileExternalEffectAttempts(testAuthorActivityContext(), liveExternalEffectRecoveryRequest(now))
	if err != nil {
		t.Fatalf("reconcile live completion: %v", err)
	}
	if summary != (runtimeeffects.RecoverySummary{}) {
		t.Fatalf("live completion recovery summary=%+v, want empty", summary)
	}
	requireExternalAttemptState(t, fixture.db, fixture.sqlite, authorized.Attempt().AttemptID, runtimeeffects.StateAuthorized)
	requireCompletionRecoveryRows(t, fixture, authorized.Attempt().AttemptID, 0, 0, 1)

	initialGeneration := int(fixture.authority.Normal.Generation)
	setCompletionFixtureGeneration(t, fixture, initialGeneration+1)
	summary, err = fixture.store.ReconcileExternalEffectAttempts(testAuthorActivityContext(), liveExternalEffectRecoveryRequest(now.Add(time.Second)))
	if err == nil {
		t.Fatal("reconcile stale prelaunch completion without a lifecycle drain succeeded")
	}
	if failure, ok := runtimefailures.As(err); !ok || failure.Failure.Detail.Code != "provider_attempt_drain_missing" {
		t.Fatalf("stale prelaunch recovery error=%v, want provider_attempt_drain_missing", err)
	}
	if summary != (runtimeeffects.RecoverySummary{}) {
		t.Fatalf("failed prelaunch recovery summary=%+v, want empty", summary)
	}
	requireExternalAttemptState(t, fixture.db, fixture.sqlite, authorized.Attempt().AttemptID, runtimeeffects.StateAuthorized)
	requireCompletionRecoveryRows(t, fixture, authorized.Attempt().AttemptID, 0, 0, 1)

	setCompletionFixtureGeneration(t, fixture, initialGeneration)
	failure := runtimefailures.FromError(context.Canceled, "completion-test", "cleanup")
	if err := authorized.Settle(ctx, runtimeeffects.StateTerminalFailure, &failure.Failure, map[string]any{"prelaunch": true}); err != nil {
		t.Fatalf("settle restored prelaunch attempt: %v", err)
	}
	secondAuthority := fixture.authority
	secondAuthority.Target.ID = uuid.NewString()
	ctx = runtimeeffects.WithLogicalOperationIdentity(runtimeeffects.WithAuthority(fixture.context, secondAuthority), "ordinary-recovery:launched")
	ctx = withManagedCompletionTestSurface(t, ctx, secondAuthority, "anthropic_api")
	launched, err := beginManagedCompletionForTest(t, ctx, "anthropic_api", []byte("launched"))
	if err != nil {
		t.Fatalf("authorize launched completion: %v", err)
	}
	if err := launched.MarkLaunched(ctx); err != nil {
		t.Fatalf("mark completion launched: %v", err)
	}
	setCompletionFixtureGeneration(t, fixture, initialGeneration+1)
	summary, err = fixture.store.ReconcileExternalEffectAttempts(testAuthorActivityContext(), liveExternalEffectRecoveryRequest(now.Add(2*time.Second)))
	if err == nil {
		t.Fatal("reconcile stale launched completion without a lifecycle drain succeeded")
	}
	if failure, ok := runtimefailures.As(err); !ok || failure.Failure.Detail.Code != "provider_attempt_drain_missing" {
		t.Fatalf("stale launched recovery error=%v, want provider_attempt_drain_missing", err)
	}
	if summary != (runtimeeffects.RecoverySummary{}) {
		t.Fatalf("failed launched recovery summary=%+v, want empty", summary)
	}
	requireExternalAttemptState(t, fixture.db, fixture.sqlite, launched.Attempt().AttemptID, runtimeeffects.StateLaunched)
	requireCompletionRecoveryRows(t, fixture, launched.Attempt().AttemptID, 0, 0, 1)
}

func proveCompletionProviderHeadStaleAuthorityCannotSettle(t *testing.T, fixture completionSettlementFixture) {
	t.Helper()
	ctx := runtimeeffects.WithLogicalOperationIdentity(fixture.context, "provider-head:stale-authority")
	handle := beginObservedCompletionForSettlementTest(t, ctx, "claude_cli", "stale-authority")
	settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, "claude_cli", "provider-head-current", "provider-head-next")
	stale := handle.Attempt()
	stale.Authority.Normal.Generation++
	stale.Authority.FenceGeneration++
	if _, err := fixture.store.SettleCompletion(ctx, stale, settlement); err == nil {
		t.Fatal("stale completion authority settled provider head")
	}
	requireProviderHead(t, fixture.db, fixture.sqlite, fixture.sessionID, "provider-head-current")
	requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, runtimeeffects.StateResponseObserved)
	requireCompletionSettlementRows(t, fixture, handle.Attempt().AttemptID, settlement.AgentTurn.TurnID, runtimeeffects.StateResponseObserved, 0, 1)
}

func newCompletionSettlementFixture(t *testing.T, store completionSettlementTestStore, db *sql.DB, sqlite bool) completionSettlementFixture {
	return newCompletionSettlementFixtureWithMemory(t, store, db, sqlite, agentmemory.Authored(true))
}

func newCompletionSettlementFixtureWithMemory(t *testing.T, store completionSettlementTestStore, db *sql.DB, sqlite bool, memory agentmemory.Plan) completionSettlementFixture {
	t.Helper()
	memory, err := memory.Normalize()
	if err != nil {
		t.Fatalf("normalize completion memory plan: %v", err)
	}
	ctx := testAuthorActivityContext()
	now := time.Now().UTC()
	agentID := "completion-settlement-agent"
	sessionID := uuid.NewString()
	runID := uuid.NewString()
	flowInstance := "global"
	leaseHolder := "completion-worker"
	identity := testAgentIdentity(t, agentID, flowInstance)
	identityFields, err := identity.StorageFields()
	if err != nil {
		t.Fatalf("completion agent identity: %v", err)
	}
	if err := agentfixture.UpsertStatic(t, ctx, store, runtimemanager.PersistedAgent{
		Config: withRuntimePersistenceTestIntent(t, runtimeactors.AgentConfig{
			ExecutionMode: "live", ID: agentID, Identity: identity, Role: "worker", Type: "managed",
			Model: "regular", LLMBackend: "claude_cli", ResolvedLLMBackend: "claude_cli",
			Memory: memory, FlowID: "global", FlowPath: flowInstance,
		}),
		Status: "active", StartedAt: now,
	}); err != nil {
		t.Fatalf("admit completion agent: %v", err)
	}
	lifecycle, found, err := store.LoadAgentLifecycleState(ctx, identity)
	if err != nil || !found {
		t.Fatalf("load admitted completion agent lifecycle: found=%v err=%v", found, err)
	}
	if sqlite {
		requireRunFixtureForTest(t, ctx, NewSQLiteRuntimeStoreForTest(db), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, StartedAt: now})
		if memory.Enabled {
			if _, err := db.ExecContext(ctx, `INSERT INTO agent_sessions (session_id,run_id,agent_id,agent_name_owner,agent_name_source,agent_route_presence,flow_scope_key,flow_instance_id,flow_instance,memory_enabled,memory_source,conversation,turn_count,runtime_state,lease_holder,lease_expires_at,status,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,1,'authored','[]',0,?,?,?,'active',?,?)`,
				sessionID, runID, identityFields.AgentID, identityFields.NameOwner, identityFields.NameSource,
				identityFields.RoutePresence, identityFields.FlowScopeKey, identityFields.FlowInstanceID, identityFields.FlowInstancePath,
				`{"provider_session_id":"provider-head-current"}`, leaseHolder, now.Add(10*time.Minute), now, now); err != nil {
				t.Fatalf("seed completion session: %v", err)
			}
		}
	} else {
		requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, StartedAt: now})
		if memory.Enabled {
			if _, err := db.ExecContext(ctx, `INSERT INTO agent_sessions (session_id,run_id,agent_id,agent_name_owner,agent_name_source,agent_route_presence,flow_scope_key,flow_instance_id,flow_instance,memory_enabled,memory_source,conversation,turn_count,runtime_state,lease_holder,lease_expires_at,status,created_at,updated_at) VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9,TRUE,'authored','[]'::jsonb,0,$10::jsonb,$11,$12,'active',$13,$13)`,
				sessionID, runID, identityFields.AgentID, identityFields.NameOwner, identityFields.NameSource,
				identityFields.RoutePresence, identityFields.FlowScopeKey, identityFields.FlowInstanceID, identityFields.FlowInstancePath,
				`{"provider_session_id":"provider-head-current"}`, leaseHolder, now.Add(10*time.Minute), now); err != nil {
				t.Fatalf("seed completion session: %v", err)
			}
		}
	}
	token := runtimeeffects.LifecycleToken{RuntimeEpoch: lifecycle.RuntimeEpoch, Identity: identity, AgentID: agentID, Generation: lifecycle.Generation}
	authority := runtimeeffects.NormalAgentAuthority(token, leaseHolder, now.Add(10*time.Minute))
	authority.Target = runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: uuid.NewString(), AgentID: agentID, AgentIdentity: identity,
		RunID: runID, SessionID: sessionID, Memory: memory, FlowInstance: flowInstance,
	}
	authority.BudgetScopes = []runtimeeffects.BudgetAdmissionScope{{Kind: "system", CapUSD: 1}}
	origin := claimCompletionOriginForTest(t, ctx, store, authority, now)
	completionCtx := runtimedelivery.WithClaim(runtimeeffects.WithController(runtimeeffects.WithAuthority(ctx, authority), newCompletionControllerForTest(store)), origin)
	return completionSettlementFixture{
		store: store, lifecycle: agentfixture.Lifecycle(t, store), agentOwner: t, db: db, sqlite: sqlite, authority: authority, origin: origin, context: completionCtx,
		sessionID: sessionID, agentID: agentID, leaseHolder: leaseHolder,
	}
}

func claimCompletionOriginForTest(t testing.TB, ctx context.Context, store completionSettlementTestStore, authority runtimeeffects.Authority, _ time.Time) runtimedelivery.Claim {
	t.Helper()
	originEvent := managedCompletionTestEvent(authority)
	return claimCompletionOriginEventForTest(t, ctx, store, authority, originEvent)
}

func claimCompletionOriginEventForTest(t testing.TB, ctx context.Context, store completionSettlementTestStore, authority runtimeeffects.Authority, originEvent events.Event) runtimedelivery.Claim {
	t.Helper()
	route := events.DeliveryRoute{
		Recipient:     events.MustAgentDeliveryRecipient(authority.Normal.Identity.AgentID()),
		AgentIdentity: authority.Normal.Identity,
	}
	if err := commitSemanticEventFixtureWithRoutes(ctx, store, originEvent, []events.DeliveryRoute{route}); err != nil {
		t.Fatalf("commit completion origin delivery: %v", err)
	}
	claimed, err := claimDeliveryFixture(ctx, store.(deliveryFixtureStore), originEvent, route)
	if err != nil {
		t.Fatalf("claim completion origin delivery: %v", err)
	}
	return claimed.Claim
}

func (f completionSettlementFixture) contextFor(authority runtimeeffects.Authority) context.Context {
	ctx := runtimeeffects.WithController(runtimeeffects.WithAuthority(testAuthorActivityContext(), authority), newCompletionControllerForTest(f.store))
	return runtimedelivery.WithClaim(ctx, f.origin)
}

func beginObservedCompletionForSettlementTest(t *testing.T, ctx context.Context, adapter, request string) *runtimeeffects.Handle {
	t.Helper()
	authority, ok := runtimeeffects.AuthorityFromContext(ctx)
	if !ok {
		t.Fatal("managed completion test authority is missing")
	}
	ctx = withManagedCompletionTestSurface(t, ctx, authority, adapter)
	handle, err := beginManagedCompletionForTest(t, ctx, adapter, []byte(request))
	if err != nil {
		t.Fatalf("authorize completion: %v", err)
	}
	if err := handle.MarkLaunched(ctx); err != nil {
		t.Fatalf("launch completion: %v", err)
	}
	if err := handle.MarkResponseObserved(ctx, map[string]any{"response_fingerprint": request}); err != nil {
		t.Fatalf("observe completion response: %v", err)
	}
	return handle
}

func completionSettlementForTest(t testing.TB, target runtimeeffects.UsageTarget, fixture completionSettlementFixture, adapter, expectedHead, newHead string) runtimeeffects.CompletionSettlement {
	t.Helper()
	input, output := int64(12), int64(4)
	settlement := runtimeeffects.CompletionSettlement{
		Settlement: runtimeeffects.Settlement{State: runtimeeffects.StateSettled, Evidence: map[string]any{"provider_result": true}},
		Usage: runtimeeffects.CompletionUsage{
			ResolvedModel: "claude-test", Exactness: runtimeeffects.CompletionUsageExact,
			InputTokens: &input, OutputTokens: &output,
		},
		AgentTurn: &runtimeeffects.CompletionAgentTurn{
			TurnID: target.ID, AgentID: target.AgentID, SessionID: target.SessionID,
			RunID: target.RunID, Identity: testAgentMemoryIdentity(t, target.RunID, target.AgentID, target.FlowInstance),
			Memory: target.Memory, FlowInstance: target.FlowInstance, EntityID: target.EntityID, ParseOK: true,
		},
		Spend: runtimeeffects.CompletionSpend{
			EntityID: target.EntityID, FlowInstance: target.FlowInstance, AgentID: target.AgentID, AgentIdentity: target.AgentIdentity,
			Model: "regular", ModelAlias: "regular",
			BackendProfile: "test", Provider: "anthropic", Transport: "process", ResolvedModel: "claude-test",
			CostUSD: 0.25, InvocationType: "agent_turn",
		},
		ProviderHead: &runtimeeffects.CompletionProviderHead{
			Identity:  testAgentMemoryIdentity(t, target.RunID, fixture.agentID, target.FlowInstance),
			SessionID: fixture.sessionID, LockOwner: fixture.leaseHolder,
			ExpectedProviderHead: expectedHead, NewProviderHead: newHead,
		},
		Now: time.Now().UTC(),
	}
	authority := fixture.authority
	authority.Target = target
	event := managedCompletionTestEvent(authority)
	settlement.AgentTurn.TriggerEventID = event.ID()
	settlement.AgentTurn.TriggerEventType = string(event.Type())
	applyManagedCompletionTestSurface(t, settlement.AgentTurn, authority, adapter)
	return settlement
}

func requireCompletionSettlementRows(t *testing.T, fixture completionSettlementFixture, attemptID, turnID string, wantState runtimeeffects.State, wantRows, wantReservations int) {
	t.Helper()
	placeholder := "?"
	turnQuery := `SELECT COUNT(*) FROM agent_turns WHERE turn_id=? AND completion_attempt_id=?`
	if !fixture.sqlite {
		placeholder = "$1::uuid"
		turnQuery = `SELECT COUNT(*) FROM agent_turns WHERE turn_id=$1::uuid AND completion_attempt_id=$2::uuid`
	}
	var turns, spend, reservations int
	if err := fixture.db.QueryRow(turnQuery, turnID, attemptID).Scan(&turns); err != nil {
		t.Fatalf("count completion turns: %v", err)
	}
	if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM spend_ledger WHERE external_effect_attempt_id=`+placeholder, attemptID).Scan(&spend); err != nil {
		t.Fatalf("count completion spend: %v", err)
	}
	if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM runtime_effect_budget_reservations WHERE attempt_id=`+placeholder, attemptID).Scan(&reservations); err != nil {
		t.Fatalf("count completion reservations: %v", err)
	}
	if turns != wantRows || spend != wantRows || reservations != wantReservations {
		t.Fatalf("completion rows turns=%d spend=%d reservations=%d, want %d/%d/%d after %s", turns, spend, reservations, wantRows, wantRows, wantReservations, wantState)
	}
}

func setCompletionFixtureGeneration(t *testing.T, fixture completionSettlementFixture, generation int) {
	t.Helper()
	query := `UPDATE agents SET lifecycle_generation=? WHERE agent_id=?`
	if !fixture.sqlite {
		query = `UPDATE agents SET lifecycle_generation=$1 WHERE agent_id=$2`
	}
	if _, err := fixture.db.Exec(query, generation, fixture.agentID); err != nil {
		t.Fatalf("set completion fixture generation: %v", err)
	}
}

func requireCompletionRecoveryRows(t *testing.T, fixture completionSettlementFixture, attemptID string, wantTurns, wantSpend, wantReservations int) {
	t.Helper()
	placeholder := "?"
	if !fixture.sqlite {
		placeholder = "$1::uuid"
	}
	var turns, spend, reservations int
	if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM agent_turns WHERE completion_attempt_id=`+placeholder, attemptID).Scan(&turns); err != nil {
		t.Fatalf("count recovered completion turns: %v", err)
	}
	if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM spend_ledger WHERE external_effect_attempt_id=`+placeholder, attemptID).Scan(&spend); err != nil {
		t.Fatalf("count recovered completion spend: %v", err)
	}
	if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM runtime_effect_budget_reservations WHERE attempt_id=`+placeholder, attemptID).Scan(&reservations); err != nil {
		t.Fatalf("count recovered completion reservations: %v", err)
	}
	if turns != wantTurns || spend != wantSpend || reservations != wantReservations {
		t.Fatalf("completion recovery rows turns=%d spend=%d reservations=%d, want %d/%d/%d", turns, spend, reservations, wantTurns, wantSpend, wantReservations)
	}
}

func requireCompletionOperationAttemptCount(t *testing.T, fixture completionSettlementFixture, operationID string, want int) {
	t.Helper()
	query := `SELECT COUNT(*) FROM runtime_external_effect_attempts WHERE operation_id=?`
	if !fixture.sqlite {
		query = `SELECT COUNT(*) FROM runtime_external_effect_attempts WHERE operation_id=$1::uuid`
	}
	var got int
	if err := fixture.db.QueryRow(query, operationID).Scan(&got); err != nil {
		t.Fatalf("count completion operation attempts: %v", err)
	}
	if got != want {
		t.Fatalf("completion operation attempts = %d, want %d", got, want)
	}
}
