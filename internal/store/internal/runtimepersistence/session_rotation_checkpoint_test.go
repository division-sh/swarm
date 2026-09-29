package runtimepersistence

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

func TestSelectedRotationCheckpointIsAcquiredBaseBothStores(t *testing.T) {
	for _, reason := range []string{"turn_limit", "parse_failure"} {
		t.Run(reason, func(t *testing.T) {
			eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
				fixture := newExactFactFixture(t, selected)
				identity := agentmemory.Identity(mustTestAgentIdentityForRun(fixture.runID, "checkpoint-agent", "support/instance-1"))
				seedTestAgentRow(t, testAuthorActivityContext(), selected.db, selected.postgres, identity, "active")
				ctx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
				store := selected.selected.(interface {
					sessions.Registry
					runtimellm.LiveSessionAcquirer
					runtimellm.ConversationPersistence
				})
				first, record, err := store.AcquireLiveSession(ctx, identity, "worker")
				if err != nil {
					t.Fatal(err)
				}
				record.Messages = []runtimellm.Message{{Role: "user", Content: "CHECKPOINT-MARKER"}, {Role: "assistant", Content: "prior answer"}}
				record.TurnCount = 1
				if err := store.UpsertConversation(ctx, first, record); err != nil {
					t.Fatal(err)
				}
				if _, err := store.ReleaseOutcome(ctx, first); err != nil {
					t.Fatal(err)
				}
				predecessor, hydrated, err := store.AcquireLiveSession(ctx, identity, "worker")
				if err != nil {
					t.Fatal(err)
				}
				session := &runtimellm.Session{ID: predecessor.SessionID, AgentID: identity.AgentID(), Memory: agentmemory.Authored(true), MemoryIdentity: identity, Messages: hydrated.Messages, TurnCount: hydrated.TurnCount}
				var successor *sessions.Lease
				if reason == "parse_failure" {
					session.ParseFailures = 1
					successor, err = runtimellm.MaybeRotateAfterParseFailures(ctx, session, store, predecessor, 1, nil)
				} else {
					successor, err = runtimellm.MaybeRotateAfterTurn(ctx, session, store, predecessor, 1, nil)
				}
				if err != nil || successor == nil || successor.SessionID == predecessor.SessionID {
					t.Fatalf("selected rotation: successor=%+v err=%v", successor, err)
				}
				if _, err := store.ReleaseOutcome(ctx, successor); err != nil {
					t.Fatal(err)
				}
				for acquire := 0; acquire < 2; acquire++ {
					current, base, err := store.AcquireLiveSession(ctx, identity, "worker")
					if err != nil {
						t.Fatal(err)
					}
					if current.SessionID != successor.SessionID || base.TurnCount != 0 || len(base.Messages) != 1 ||
						!reflect.DeepEqual(base.Messages[0], session.Messages[0]) || base.Messages[0].Role != "system" ||
						strings.Count(base.Messages[0].Content, "CHECKPOINT-MARKER") != 1 ||
						!strings.Contains(base.Messages[0].Content, map[string]string{"turn_limit": "turn_limit_reached:1", "parse_failure": "parse_failures_threshold:1"}[reason]) {
						t.Fatalf("acquired successor lost or duplicated checkpoint: lease=%+v base=%+v local=%+v", current, base, session)
					}
					if acquire == 1 {
						base.Messages = append(base.Messages, runtimellm.Message{Role: "assistant", Content: "successor answer"})
						base.TurnCount = 1
						if err := store.UpsertConversation(ctx, current, base); err != nil {
							t.Fatal(err)
						}
					}
					if _, err := store.ReleaseOutcome(ctx, current); err != nil {
						t.Fatal(err)
					}
				}
				current, base, err := store.AcquireLiveSession(ctx, identity, "worker")
				if err != nil {
					t.Fatal(err)
				}
				if len(base.Messages) != 2 || !reflect.DeepEqual(base.Messages[0], session.Messages[0]) || base.Messages[1].Content != "successor answer" {
					t.Fatalf("later turn did not retain exactly one checkpoint: %+v", base.Messages)
				}
				if _, err := store.ReleaseOutcome(context.WithoutCancel(ctx), current); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func TestSelectedTurnRotationCheckpointReachesManagedHTTPBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
		store := selected.selected.(completionSettlementTestStore)
		if selected.postgres {
			store = admitTestPostgresStore(t, selected.db)
		}
		fixture := newCompletionSettlementFixture(t, store, selected.db, !selected.postgres)
		requests := make(chan string, 1)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			requests <- string(body)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"checkpoint_response","model":"test-model","output":[{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"output_text","text":"successor answer"}]}],"usage":{"input_tokens":2,"output_tokens":2,"total_tokens":4}}`))
		}))
		defer server.Close()
		cfg := &config.Config{LLM: config.LLMConfig{Backend: "openai_responses", OpenAIResponses: config.OpenAIResponsesConfig{BaseURL: server.URL}}}
		cfg.LLM.Session.RotateAfterTurns = 1
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
		conversation.SetToolExecutor(blockedProviderNoTools{})
		ctx := runtimeeffects.WithLifecycleToken(runtimedelivery.WithClaim(testAuthorActivityContext(), fixture.origin), fixture.authority.Normal)
		ctx = managedExecutionStoreTestContext(t, ctx)
		ctx = storeTestWorkContext(t, ctx)
		workFixture, _ := storeTestWorkFixtures.Load(t)
		ctx = worklifetime.WithProcess(ctx, workFixture.(*storeTestWorkFixture).process)
		ctx = runtimeeffects.WithController(ctx, controller)
		ctx = runtimeactors.WithActor(ctx, runtimeactors.AgentConfig{ID: fixture.agentID, Identity: fixture.authority.Normal.Identity, Role: "worker", Type: "managed", ExecutionMode: "live", Model: "regular", LLMBackend: "openai_responses", Memory: settlement.AgentTurn.Memory, FlowID: "global", FlowPath: "global"})
		ctx = agentmemory.WithExecution(ctx, settlement.AgentTurn.Memory, settlement.AgentTurn.Identity)
		ctx = runtimecorrelation.WithInboundEvent(ctx, managedCompletionTestEvent(fixture.authority))
		ctx = runtimeeffects.WithLogicalOperationIdentity(ctx, "selected-checkpoint-proof")
		fresh, err := store.(sessions.Registry).Acquire(ctx, settlement.AgentTurn.Identity, fixture.leaseHolder)
		if err != nil {
			t.Fatal(err)
		}
		record := runtimellm.ConversationRecord{
			SessionID: fixture.sessionID, AgentID: fixture.agentID, Identity: settlement.AgentTurn.Identity,
			Memory: settlement.AgentTurn.Memory, Messages: []runtimellm.Message{{Role: "user", Content: "MUST-PRESERVE-NEW-BASE"}},
			TurnCount: 1, Status: "active",
		}
		if err := store.(runtimellm.ConversationPersistence).UpsertConversation(ctx, fresh, record); err != nil {
			t.Fatal(err)
		}
		if _, err := store.(sessions.Registry).ReleaseOutcome(ctx, fresh); err != nil {
			t.Fatal(err)
		}
		conversation.Session.TurnCount = 1
		conversation.Session.Messages = []runtimellm.Message{{Role: "user", Content: "CALLER-STALE"}}
		response, err := conversation.RunManaged(ctx, agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: managedCompletionTestEvent(fixture.authority)})
		if err != nil || response == nil || conversation.Session.ID == fixture.sessionID {
			t.Fatalf("selected rotation did not complete: response=%+v err=%v old=%s new=%s", response, err, fixture.sessionID, conversation.Session.ID)
		}
		select {
		case request := <-requests:
			if strings.Count(request, "MUST-PRESERVE-NEW-BASE") != 1 || strings.Contains(request, "CALLER-STALE") {
				t.Fatalf("provider request lost checkpoint or accepted stale caller history: %s", request)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("rotated successor never reached provider")
		}
		var raw string
		if err := selected.db.QueryRowContext(ctx, `SELECT conversation FROM agent_sessions WHERE session_id=$1`, conversation.Session.ID).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		if strings.Count(raw, "MUST-PRESERVE-NEW-BASE") != 1 || strings.Contains(raw, "CALLER-STALE") || !strings.Contains(raw, "successor answer") {
			t.Fatalf("persisted successor omitted checkpoint or accepted stale history: %s", raw)
		}
	})
}
