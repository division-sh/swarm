package apiv1

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type forkCommitOmissionProbeStore interface {
	ConversationForkLifecycleStore
	ConversationForkReadStore
	APIIdempotencyStore
	storetest.AgentFixtureStore
	storetest.ManagedAgentTurnFixtureStore
}

type forkCommitOmissionProbeOwner struct {
	ConversationForkLifecycleStore
	omit bool
	ids  []string
}

func (o *forkCommitOmissionProbeOwner) CreateAPIConversationFork(ctx context.Context, req runfork.APIConversationForkCreateRequest) (runfork.ConversationForkCreateResult, error) {
	created, err := o.ConversationForkLifecycleStore.CreateAPIConversationFork(ctx, req)
	if err != nil {
		return created, err
	}
	if !created.IdempotencyReplayed {
		o.ids = append(o.ids, created.Fork.ForkID)
	}
	if o.omit {
		// Both durable facts have committed before this return can be lost.
		return runfork.ConversationForkCreateResult{}, errors.New("injected omission after atomic fork and API completion commit")
	}
	return created, nil
}

func newConversationForkCommitFixture(t *testing.T, backend string) (context.Context, forkCommitOmissionProbeStore, *sql.DB, time.Time, string, string) {
	t.Helper()
	ctx := testAuthorActivityContext(context.Background())
	var selected forkCommitOmissionProbeStore
	var db *sql.DB
	runID, sessionID, turnID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	now := time.Now().UTC().Truncate(time.Second)
	runFixture := storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, StartedAt: now.Add(-time.Hour)}
	if backend == "sqlite" {
		s := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
		selected, db = s, storetest.DatabaseForTest(s)
		storetest.RequireSQLiteRun(t, ctx, db, runFixture)
	} else {
		_, pg, _ := testutil.StartPostgres(t)
		db = pg
		selected = storetest.AdmitPostgresRuntimeStore(t, db)
		storetest.RequirePostgresRun(t, ctx, db, runFixture)
	}
	const agentID = "fork-commit-omission-source"
	identity := sqliteAgentUsageIdentityForRun(t, runID, agentID)
	if err := storetest.UpsertStaticAgentFixture(t, ctx, selected, manager.PersistedAgent{
		Config: withAPITestIntent(t, runtimeactors.AgentConfig{
			Identity: identity, ID: agentID, Role: "researcher", Type: "managed", Model: "cheap",
			ExecutionMode: "live", ResolvedLLMBackend: "anthropic", FlowPath: "flow/a", Memory: agentmemory.Authored(true),
		}), Status: "active", StartedAt: now.Add(-5 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	history := storetest.SeedConversationForkSource(t, ctx, selected, storetest.ConversationForkSourceFixture{
		Identity: identity, RunID: runID, SessionID: sessionID, EntityID: uuid.NewString(),
		Event1ID: uuid.NewString(), Event2ID: uuid.NewString(), CreatedAt: now,
		Turn1At: now.Add(-2 * time.Minute), Turn2At: now.Add(-time.Minute),
	})
	storetest.PersistManagedAgentTurnFixture(t, ctx, storetest.ManagedAgentTurnFixture{
		Store: selected, Selected: selected, Identity: identity, RunID: runID, SessionID: sessionID,
		TurnID: turnID, Memory: agentmemory.Authored(true), Event: history[0], ParseOK: true, CreatedAt: now.Add(-2 * time.Minute),
	})
	return ctx, selected, db, now, sessionID, turnID
}

// The return-loss regression intercepts the named atomic owner, not the former
// generic callback. This is injection, not SIGKILL or wire ACK loss proof.
func TestConversationForkCommitBeforeAPICompletionProbe(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, omit := range []bool{false, true} {
			phase := "healthy"
			if omit {
				phase = "postcommit_omission"
			}
			t.Run(backend+"/"+phase, func(t *testing.T) {
				ctx, selected, db, now, sessionID, turnID := newConversationForkCommitFixture(t, backend)
				makeHandler := func(owner ConversationForkLifecycleStore) *Handler {
					return testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: testOperatorConversationForkHandlers(testOperatorCapabilities{
						ConversationForkLifecycle: owner, ConversationForks: selected, Idempotency: selected, Now: func() time.Time { return now },
					})})
				}
				const key = "fork-commit-omission-key"
				body := fmt.Sprintf(`{"jsonrpc":"2.0","id":"fork-probe","method":"conversation.fork","params":{"source_session_id":%q,"fork_point":{"kind":"turn","turn_id":%q},"idempotency_key":%q}}`, sessionID, turnID, key)
				firstOwner := &forkCommitOmissionProbeOwner{ConversationForkLifecycleStore: selected, omit: omit}
				first := rpcCall(t, makeHandler(firstOwner), body)
				if len(firstOwner.ids) != 1 {
					t.Fatalf("fixture did not reach durable fork commit: ids=%v rpc_error=%+v", firstOwner.ids, first.Error)
				}
				if omit && first.Error == nil {
					t.Fatal("injected omission did not reach RPC error")
				}
				if !omit && first.Error != nil {
					t.Fatalf("healthy create RPC: %+v", first.Error)
				}
				count := func(table string) int {
					t.Helper()
					var n int
					query := "SELECT count(*) FROM " + table
					if err := db.QueryRow(query).Scan(&n); err != nil {
						t.Fatal(err)
					}
					return n
				}
				beforeForks, beforeCompletions := count("conversation_forks"), count("api_idempotency")
				wantBeforeCompletions := 1
				if beforeForks != 1 || beforeCompletions != wantBeforeCompletions {
					t.Fatalf("post-first durable forks=%d completions=%d", beforeForks, beforeCompletions)
				}
				// A fresh HTTP handler has no predecessor result. Persistence, not the
				// first wrapper's observation, must resolve the same-key request.
				retryOwner := &forkCommitOmissionProbeOwner{ConversationForkLifecycleStore: selected}
				retryHandler := makeHandler(retryOwner)
				second := rpcCall(t, retryHandler, body)
				if second.Error != nil {
					t.Fatalf("same-key retry RPC: %+v", second.Error)
				}
				result := asMap(t, second.Result)
				fork := asMap(t, result["fork"])
				secondID := stringValue(t, fork["fork_id"], "retry fork_id")
				third := rpcCall(t, retryHandler, body)
				if third.Error != nil {
					t.Fatalf("completed replay RPC: %+v", third.Error)
				}
				thirdFork := asMap(t, asMap(t, third.Result)["fork"])
				if thirdFork["fork_id"] != secondID {
					t.Error("healthy completed response replay changed resource")
				}
				forks, completions := count("conversation_forks"), count("api_idempotency")
				listed, err := selected.ListOperatorConversationForks(ctx, runfork.ConversationForkListOptions{SourceSessionID: sessionID, Now: now, Limit: 100})
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("backend=%s injected_omission=%t first_fork=%s first_rpc_error=%+v before_retry_forks=%d before_retry_api_completions=%d retry_fork=%s retry_owner_calls=%d final_forks=%d listed_forks=%d final_api_completions=%d", backend, omit, firstOwner.ids[0], first.Error, beforeForks, beforeCompletions, secondID, len(retryOwner.ids), forks, len(listed.Forks), completions)
				if completions != 1 {
					t.Errorf("final API completions=%d want=1", completions)
				}
				if forks != 1 || len(listed.Forks) != 1 || secondID != firstOwner.ids[0] {
					t.Error("F1: same keyed RPC created a second durable fork after postcommit API completion omission")
				}
			})
		}
	}
}
