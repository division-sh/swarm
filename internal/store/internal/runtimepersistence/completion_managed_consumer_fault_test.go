package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// The decorator reports a cleanup diagnostic only after the selected store
// has committed its real projection transaction.
type managedProjectionPostCommitFaultStore struct {
	completionSettlementTestStore
	runtimeeffects.CompletionContinuationStore
	faultAttempt string
	projectCalls int
	faults       int
	joined       bool
	independent  bool
}

func (s *managedProjectionPostCommitFaultStore) ProjectCompletionConversation(ctx context.Context, attempt runtimeeffects.Attempt, projection runtimeeffects.CompletionConversationProjection) error {
	if err := s.CompletionContinuationStore.ProjectCompletionConversation(ctx, attempt, projection); err != nil {
		return err
	}
	s.projectCalls++
	if attempt.AttemptID != s.faultAttempt || s.faults != 0 {
		return nil
	}
	s.faults++
	cleanup := runtimeeffects.NewPostCommitMutationError(runtimeeffects.MutationProjection, attempt, errors.New("injected projection acknowledgement cleanup"))
	if s.independent {
		return errors.Join(cleanup, errors.New("independent projection failure"))
	}
	if s.joined {
		return errors.Join(cleanup, runtimeeffects.NewPostCommitMutationError(runtimeeffects.MutationProjection, attempt, errors.New("second acknowledged cleanup")))
	}
	return cleanup
}

func TestManagedCompletionRealStorePostCommitProjectionConsumer(t *testing.T) {
	for _, variant := range []string{"plain", "joined", "independent"} {
		t.Run(variant, func(t *testing.T) {
			t.Run("sqlite", func(t *testing.T) {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				proveManagedCompletionRealStorePostCommitProjectionConsumer(t, store, store.backend.ConstructionHandle(), true, variant)
			})
			t.Run("postgres", func(t *testing.T) {
				_, db, _ := testutil.StartPostgres(t)
				proveManagedCompletionRealStorePostCommitProjectionConsumer(t, admitTestPostgresStore(t, db), db, false, variant)
			})
		})
	}
}

func proveManagedCompletionRealStorePostCommitProjectionConsumer(t *testing.T, store completionSettlementTestStore, db *sql.DB, sqlite bool, variant string) {
	t.Helper()
	const adapter = "mock_python"
	fixture := newCompletionSettlementFixture(t, store, db, sqlite)
	request := "managed-composed-projection-" + uuid.NewString()
	ctx := runtimeeffects.WithLifecycleToken(fixture.context, fixture.authority.Normal)
	ctx = runtimeeffects.WithLogicalOperationIdentity(ctx, request)
	handle := beginObservedCompletionForSettlementTest(t, ctx, adapter, request)
	settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, adapter, "", "")
	settlement.ProviderHead = nil
	payload, messages := terminalManagedCompletionPayload(t, fixture, settlement)
	if err := runtimeeffects.AttachCompletionContinuationEvidence(settlement.Settlement.Evidence, []byte(request), payload); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.SettleCompletion(ctx, settlement); err != nil {
		t.Fatalf("real selected-store completion settlement: %v", err)
	}
	requireProviderAttemptCount(t, fixture, 1)

	continuationStore, ok := store.(runtimeeffects.CompletionContinuationStore)
	if !ok {
		t.Fatal("selected store has no completion continuation writer")
	}
	faultStore := &managedProjectionPostCommitFaultStore{
		completionSettlementTestStore: store, CompletionContinuationStore: continuationStore,
		faultAttempt: handle.Attempt().AttemptID, joined: variant == "joined", independent: variant == "independent",
	}
	controller := liveTestCompletionController(faultStore, faultStore, faultStore, nil)
	runCtx := runtimeeffects.WithLifecycleToken(fixture.context, fixture.authority.Normal)
	runCtx = managedExecutionStoreTestContext(t, runCtx)
	draft := agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: managedCompletionTestEvent(fixture.authority)}
	for run := 0; run < 2; run++ {
		// Each pass uses a fresh runtime object against the same durable store.
		conversation := managedConsumerForCommittedCompletion(t, fixture, settlement, controller)
		response, err := conversation.RunManaged(runCtx, draft)
		if variant == "independent" && run == 0 {
			if err == nil || response != nil {
				t.Fatalf("independent projection failure was admitted: response=%+v err=%v", response, err)
			}
			requireProviderAttemptCount(t, fixture, 1)
			continue
		}
		if err != nil {
			t.Fatalf("managed consumer run %d: %v", run, err)
		}
		if response == nil || response.Message.Content != "committed terminal response" || len(response.ToolCalls) != 0 {
			t.Fatalf("managed consumer run %d lost terminal response: %+v", run, response)
		}
		if conversation.TurnCount != 1 {
			t.Fatalf("managed consumer run %d turn count=%d, want 1", run, conversation.TurnCount)
		}
		requireProviderAttemptCount(t, fixture, 1)
		requireCompletionProjectionState(t, fixture, handle.Attempt().AttemptID, runtimeeffects.CompletionProjectionResponseConsumed, 1, mustJSON(t, messages))
	}
	if faultStore.faults != 1 || faultStore.projectCalls < 2 {
		t.Fatalf("real projection writer calls=%d injected postcommit diagnostics=%d", faultStore.projectCalls, faultStore.faults)
	}
}

func terminalManagedCompletionPayload(t *testing.T, fixture completionSettlementFixture, settlement runtimeeffects.CompletionSettlement) (json.RawMessage, []runtimellm.Message) {
	t.Helper()
	payload, _ := completionSuccessorPayload(t, "mock_python", fixture, settlement, "agent-frame:v1:"+uuid.NewString())
	var document map[string]json.RawMessage
	if err := json.Unmarshal(payload, &document); err != nil {
		t.Fatal(err)
	}
	var response runtimellm.Response
	if err := json.Unmarshal(document["response"], &response); err != nil {
		t.Fatal(err)
	}
	response.Message = runtimellm.Message{Role: "assistant", Content: "committed terminal response"}
	response.ToolCalls = nil
	document["response"] = mustJSON(t, response)
	var projection map[string]json.RawMessage
	if err := json.Unmarshal(document["projection"], &projection); err != nil {
		t.Fatal(err)
	}
	messages := []runtimellm.Message{{Role: "user", Content: "start"}, response.Message}
	projection["messages"] = mustJSON(t, messages)
	document["projection"] = mustJSON(t, projection)
	return mustJSON(t, document), messages
}

func managedConsumerForCommittedCompletion(t *testing.T, fixture completionSettlementFixture, settlement runtimeeffects.CompletionSettlement, controller *runtimeeffects.Controller) *runtimellm.Conversation {
	t.Helper()
	runtime := runtimellm.NewMockRuntime(&config.Config{}, sessions.NewInMemoryRegistry(time.Minute), fixture.leaseHolder, nil, nil, controller)
	intent, err := agentintent.Resolve(agentintent.SourceInline, "inline", "agents.yaml#agents."+fixture.agentID+".intent", "Complete the admitted business work.")
	if err != nil {
		t.Fatal(err)
	}
	prompt, err := agentintent.IntentOnlyPrompt(intent)
	if err != nil {
		t.Fatal(err)
	}
	providerPrompt, err := agentintent.AssembleProviderPrompt(intent, nil, prompt, agentintent.RuntimeEnvironmentContext())
	if err != nil {
		t.Fatal(err)
	}
	contract := runtime.ProviderContract()
	seed := agentframe.SessionSeed{
		AgentIdentity: fixture.authority.Normal.Identity, Role: "worker", FlowID: "global", Intent: intent,
		ProviderPrompt: providerPrompt, RuntimeMode: contract.RuntimeMode, Provider: contract.Provider,
		Transport: string(contract.Transport), ModelAlias: "regular", Model: "test-model",
	}
	conversation, err := runtimellm.NewManagedConversation(seed, "task-1", nil, settlement.AgentTurn.Memory, 2, runtime)
	if err != nil {
		t.Fatal(err)
	}
	conversation.Session = &runtimellm.Session{
		ID: fixture.sessionID, AgentID: fixture.agentID, Memory: settlement.AgentTurn.Memory,
		MemoryIdentity: settlement.AgentTurn.Identity,
	}
	return conversation
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
