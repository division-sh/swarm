package runtimepersistence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
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

type blockedProviderNoTools struct{}

type recoveredAcquireFaultRegistry struct {
	sessions.Registry
	acquireErr   error
	releaseErr   error
	cancel       context.CancelFunc
	acquires     int
	releases     int
	acknowledged bool
}

func (r *recoveredAcquireFaultRegistry) Acquire(ctx context.Context, identity agentmemory.Identity, owner string) (*sessions.Lease, error) {
	lease, err := r.Registry.Acquire(ctx, identity, owner)
	if err != nil || lease == nil {
		return lease, err
	}
	r.acquires++
	if r.cancel != nil {
		r.cancel()
	}
	return lease, r.acquireErr
}

func (r *recoveredAcquireFaultRegistry) ReleaseOutcome(ctx context.Context, lease *sessions.Lease) (sessions.ReleaseResult, error) {
	r.releases++
	result, err := r.Registry.ReleaseOutcome(ctx, lease)
	r.acknowledged = result.Acknowledged
	if err != nil || !result.Acknowledged {
		return result, err
	}
	return result, r.releaseErr
}

func (blockedProviderNoTools) Execute(context.Context, string, any) (any, error) {
	return nil, errors.New("blocked provider test must not execute a tool")
}

func (blockedProviderNoTools) ToolCapabilitiesForActor(runtimeactors.AgentConfig, []string, map[string]struct{}) toolcapabilities.Set {
	return toolcapabilities.Set{}
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

func TestRecoveredAcquirePostCommitErrorReleasesExactGrantBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, canceled := range []bool{false, true} {
			for _, releasePostCommit := range []bool{false, true} {
				name := fmt.Sprintf("%s/canceled=%t/release_postcommit=%t", backend, canceled, releasePostCommit)
				t.Run(name, func(t *testing.T) {
					var store completionSettlementTestStore
					var db *sql.DB
					if backend == "sqlite" {
						sqlite := newBootstrappedSQLiteRuntimeStoreForTest(t)
						store, db = sqlite, sqlite.backend.ConstructionHandle()
					} else {
						_, db, _ = testutil.StartPostgres(t)
						store = admitTestPostgresStore(t, db)
					}
					fixture := newCompletionSettlementFixture(t, store, db, backend == "sqlite")
					request := "recovered-acquire-cleanup-" + uuid.NewString()
					ctx := runtimeeffects.WithLifecycleToken(fixture.context, fixture.authority.Normal)
					ctx = runtimeeffects.WithLogicalOperationIdentity(ctx, request)
					handle := beginObservedCompletionForSettlementTest(t, ctx, "mock_python", request)
					settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, "mock_python", "", "")
					settlement.ProviderHead = nil
					payload, _ := terminalManagedCompletionPayload(t, fixture, settlement)
					if err := runtimeeffects.AttachCompletionContinuationEvidence(settlement.Settlement.Evidence, []byte(request), payload); err != nil {
						t.Fatal(err)
					}
					if _, err := handle.SettleCompletion(ctx, settlement); err != nil {
						t.Fatal(err)
					}
					controller := liveTestCompletionController(store, store, store, nil)
					registry := &recoveredAcquireFaultRegistry{Registry: store.(sessions.Registry), acquireErr: errors.New("injected postcommit acquire failure")}
					if releasePostCommit {
						registry.releaseErr = errors.New("injected postcommit release failure")
					}
					runtime := runtimellm.NewMockRuntime(&config.Config{}, registry, fixture.leaseHolder, nil, nil, controller)
					conversation := managedConsumerWithRuntime(t, fixture, settlement, runtime)
					runCtx := managedExecutionStoreTestContext(t, runtimeeffects.WithLifecycleToken(fixture.context, fixture.authority.Normal))
					if canceled {
						var cancel context.CancelFunc
						runCtx, cancel = context.WithCancel(runCtx)
						defer cancel()
						registry.cancel = cancel
					}
					draft := agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: managedCompletionTestEvent(fixture.authority)}
					response, err := conversation.RunManaged(runCtx, draft)
					if response != nil || !errors.Is(err, registry.acquireErr) || (canceled && !errors.Is(err, context.Canceled)) || (releasePostCommit && !errors.Is(err, registry.releaseErr)) {
						t.Fatalf("recovered acquire failure displaced or lost causes: response=%+v err=%v", response, err)
					}
					if registry.acquires != 1 || registry.releases != 1 || !registry.acknowledged {
						t.Fatalf("acknowledged acquired grant not released: acquires=%d releases=%d acknowledged=%t", registry.acquires, registry.releases, registry.acknowledged)
					}
					requireProviderAttemptCount(t, fixture, 1)
					checkCtx := runtimeeffects.WithLifecycleToken(fixture.context, fixture.authority.Normal)
					current, err := store.(sessions.Registry).Acquire(checkCtx, settlement.AgentTurn.Identity, "different-owner")
					if err != nil || current == nil {
						t.Fatalf("recovered grant survived exact release: lease=%+v err=%v", current, err)
					}
					if _, err := store.(sessions.Registry).ReleaseOutcome(checkCtx, current); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
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
	registry, ok := fixture.store.(sessions.Registry)
	if !ok {
		t.Fatal("selected completion store does not expose its session registry")
	}
	runtime := runtimellm.NewMockRuntime(&config.Config{}, registry, fixture.leaseHolder, nil, nil, controller)
	return managedConsumerWithRuntime(t, fixture, settlement, runtime)
}

func managedConsumerWithRuntime(t *testing.T, fixture completionSettlementFixture, settlement runtimeeffects.CompletionSettlement, runtime runtimellm.Runtime) *runtimellm.Conversation {
	t.Helper()
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
	provider, ok := runtime.(runtimellm.ProviderContractProvider)
	if !ok {
		t.Fatal("managed test runtime has no provider contract")
	}
	contract := provider.ProviderContract()
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
		MemoryIdentity: settlement.AgentTurn.Identity, SystemPrompt: conversation.SystemPrompt,
	}
	return conversation
}

func TestManagedBlockedProviderGrantReplacementIsEvidenceOnlyBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, replaced := range []bool{false, true} {
			for _, canonicalTurnCount := range []int{0, 1} {
				name := backend + "/current"
				if replaced {
					name = backend + "/replaced"
				}
				name += fmt.Sprintf("/canonical_turn_%d", canonicalTurnCount)
				t.Run(name, func(t *testing.T) {
					var store completionSettlementTestStore
					var db *sql.DB
					if backend == "sqlite" {
						sqlite := newBootstrappedSQLiteRuntimeStoreForTest(t)
						store, db = sqlite, sqlite.backend.ConstructionHandle()
					} else {
						_, db, _ = testutil.StartPostgres(t)
						store = admitTestPostgresStore(t, db)
					}
					fixture := newCompletionSettlementFixture(t, store, db, backend == "sqlite")
					seedCtx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
					seedLease, _, err := store.(runtimellm.LiveSessionAcquirer).AcquireLiveSession(seedCtx, fixture.authority.Target.AgentIdentity, fixture.leaseHolder)
					if err != nil {
						t.Fatalf("acquire canonical conversation seed: %v", err)
					}
					if err := store.(runtimellm.ConversationPersistence).UpsertConversation(seedCtx, seedLease, runtimellm.ConversationRecord{
						SessionID: fixture.sessionID, AgentID: fixture.agentID, Identity: fixture.authority.Target.AgentIdentity,
						Memory: fixture.authority.Target.Memory, Messages: []runtimellm.Message{{Role: "user", Content: "canonical acquired history"}},
						TurnCount: canonicalTurnCount, Status: "active",
					}); err != nil {
						t.Fatalf("seed canonical conversation: %v", err)
					}
					if _, err := store.(sessions.Registry).ReleaseOutcome(seedCtx, seedLease); err != nil {
						t.Fatalf("release conversation seed: %v", err)
					}
					started := make(chan struct{})
					resume := make(chan struct{})
					observedRequest := make(chan []byte, 1)
					var providerCalls atomic.Int32
					defer func() {
						select {
						case <-resume:
						default:
							close(resume)
						}
					}()
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						payload, _ := io.ReadAll(r.Body)
						observedRequest <- payload
						if providerCalls.Add(1) == 1 {
							close(started)
						}
						<-resume
						w.Header().Set("Content-Type", "application/json")
						_, _ = w.Write([]byte(`{"id":"resp_1","model":"test-model","output":[{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"output_text","text":"late answer"}]}],"usage":{"input_tokens":2,"output_tokens":2,"total_tokens":4}}`))
					}))
					defer server.Close()
					cfg := &config.Config{LLM: config.LLMConfig{Backend: "openai_responses", OpenAIResponses: config.OpenAIResponsesConfig{BaseURL: server.URL}}}
					controller := liveTestCompletionController(store, store, store, publicationGroupSpendProjection{})
					runtime, err := (runtimellm.RuntimeFactory{
						Cfg: cfg, Sessions: store.(sessions.Registry), LiveSessions: store.(runtimellm.LiveSessionAcquirer),
						Conversations: store.(runtimellm.ConversationPersistence), LockOwner: fixture.leaseHolder,
						Credentials: mapCredentialStore{"OPENAI_API_KEY": "test-key"}, CompletionController: controller,
					}).Build()
					if err != nil {
						t.Fatal(err)
					}
					settlement := completionSettlementForTest(t, fixture.authority.Target, fixture, "openai_responses", "", "")
					conversation := managedConsumerWithRuntime(t, fixture, settlement, runtime)
					conversation.Session.Messages = []runtimellm.Message{{Role: "user", Content: "stale caller history"}}
					conversation.SetToolExecutor(blockedProviderNoTools{})
					process := worklifetime.NewProcess()
					root, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{RuntimeInstanceID: "grant-blocked-provider", BundleHash: "grant-blocked-provider"})
					if err != nil {
						t.Fatal(err)
					}
					defer func() {
						if _, err := root.RetireAndWait(context.Background()); err != nil {
							t.Errorf("join blocked-provider runtime: %v", err)
						}
						process.Retire()
						if _, err := process.Join(context.Background()); err != nil {
							t.Errorf("join blocked-provider process: %v", err)
						}
					}()
					work, err := root.Begin(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = work.Done() }()
					ctx := worklifetime.WithProcess(testAuthorActivityContext(), process)
					ctx = worklifetime.WithOccurrence(ctx, root)
					ctx = runtimeeffects.WithController(ctx, controller)
					ctx = runtimedelivery.WithClaim(ctx, fixture.origin)
					ctx = runtimeeffects.WithLifecycleToken(ctx, fixture.authority.Normal)
					ctx = managedExecutionStoreTestContext(t, ctx)
					ctx = runtimeactors.WithActor(ctx, runtimeactors.AgentConfig{
						ID: fixture.agentID, Identity: fixture.authority.Normal.Identity, Role: "worker", Type: "managed",
						ExecutionMode: runtimeeffects.ExecutionModeLive, Model: "regular", LLMBackend: "openai_responses",
						Memory: settlement.AgentTurn.Memory, FlowID: "global", FlowPath: "global",
					})
					ctx = agentmemory.WithExecution(ctx, settlement.AgentTurn.Memory, settlement.AgentTurn.Identity)
					ctx = runtimecorrelation.WithInboundEvent(ctx, managedCompletionTestEvent(fixture.authority))
					ctx = runtimeeffects.WithLogicalOperationIdentity(ctx, "blocked-provider-grant-replacement")
					type providerOutcome struct {
						response *runtimellm.Response
						err      error
					}
					outcome := make(chan providerOutcome, 1)
					go func() {
						response, err := conversation.RunManaged(ctx, agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: managedCompletionTestEvent(fixture.authority)})
						outcome <- providerOutcome{response: response, err: err}
					}()
					select {
					case <-started:
					case result := <-outcome:
						t.Fatalf("provider did not launch: %v", result.err)
					case <-time.After(15 * time.Second):
						t.Fatal("provider did not reach blocked HTTP request")
					}
					payload := <-observedRequest
					if !bytes.Contains(payload, []byte("canonical acquired history")) || bytes.Contains(payload, []byte("stale caller history")) {
						t.Fatalf("provider received stale rather than acquired conversation: %s", payload)
					}
					readCtx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
					sessionQuery := `SELECT * FROM agent_sessions WHERE run_id=$1 ORDER BY session_id`
					revisionQuery := `SELECT * FROM run_fork_fact_revisions WHERE run_id=$1 AND family='agent_sessions' ORDER BY revision,fact_key`
					var beforeSession, beforeRevision []string
					if replaced {
						var blockedGrant string
						if err := db.QueryRowContext(readCtx, `SELECT lease_grant_id FROM agent_sessions WHERE session_id=$1`, fixture.sessionID).Scan(&blockedGrant); err != nil || blockedGrant == "" {
							t.Fatalf("load blocked provider grant=%q err=%v", blockedGrant, err)
						}
						fresh, err := store.(sessions.Registry).Acquire(readCtx, fixture.authority.Target.AgentIdentity, fixture.leaseHolder)
						if err != nil || fresh == nil || fresh.GrantID == blockedGrant {
							t.Fatalf("replace blocked provider grant: lease=%+v blocked=%q err=%v", fresh, blockedGrant, err)
						}
						beforeSession = snapshotRotationTableRows(t, readCtx, db, sessionQuery, fixture.authority.Target.RunID)
						beforeRevision = snapshotRotationTableRows(t, readCtx, db, revisionQuery, fixture.authority.Target.RunID)
					}
					close(resume)
					var result providerOutcome
					select {
					case result = <-outcome:
					case <-time.After(15 * time.Second):
						t.Fatal("blocked provider did not settle")
					}
					if replaced {
						if result.err == nil || result.response != nil {
							t.Fatalf("replaced grant admitted blocked provider result: response=%+v err=%v", result.response, result.err)
						}
						failure, ok := runtimefailures.As(result.err)
						if !ok || failure.Failure.Class != runtimefailures.ClassOutcomeUncertain || failure.Failure.Detail.Code == "completion_projection_input_missing" {
							t.Fatalf("replaced grant failure=%v, want canonical postlaunch uncertainty", result.err)
						}
						if after := snapshotRotationTableRows(t, readCtx, db, sessionQuery, fixture.authority.Target.RunID); !reflect.DeepEqual(beforeSession, after) {
							t.Fatalf("blocked provider changed replacement session: before=%v after=%v", beforeSession, after)
						}
						if after := snapshotRotationTableRows(t, readCtx, db, revisionQuery, fixture.authority.Target.RunID); !reflect.DeepEqual(beforeRevision, after) {
							t.Fatalf("blocked provider changed replacement revision: before=%v after=%v", beforeRevision, after)
						}
					} else {
						if result.err != nil || result.response == nil || result.response.Message.Content != "late answer" {
							t.Fatalf("current grant did not project blocked provider response: response=%+v err=%v", result.response, result.err)
						}
						var turnCount int
						var conversationRaw []byte
						if err := db.QueryRowContext(readCtx, `SELECT turn_count,conversation FROM agent_sessions WHERE session_id=$1`, fixture.sessionID).Scan(&turnCount, &conversationRaw); err != nil || turnCount != canonicalTurnCount+1 || !strings.Contains(string(conversationRaw), "late answer") {
							t.Fatalf("current grant projected turn=%d conversation=%s err=%v", turnCount, conversationRaw, err)
						}
					}
					requireProviderAttemptCount(t, fixture, 1)
					var attemptState string
					wantState := runtimeeffects.StateSettled
					if replaced {
						wantState = runtimeeffects.StateOutcomeUncertain
					}
					if err := db.QueryRowContext(readCtx, `SELECT state FROM runtime_external_effect_attempts`).Scan(&attemptState); err != nil || attemptState != string(wantState) {
						t.Fatalf("blocked provider attempt state=%q err=%v, want %s", attemptState, err, wantState)
					}
					if got := providerCalls.Load(); got != 1 {
						t.Fatalf("blocked provider dispatched %d times, want exactly one", got)
					}
				})
			}
		}
	}
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
