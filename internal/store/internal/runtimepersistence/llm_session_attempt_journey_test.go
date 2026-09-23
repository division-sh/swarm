package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/store/internal/backend/llmpersistence"
	"github.com/google/uuid"
)

type llmSessionAttemptJourneyOwner interface {
	AcquireLiveSession(context.Context, agentmemory.Identity, string) (*sessions.Lease, runtimellm.ConversationRecord, error)
	Rotate(context.Context, agentmemory.Identity, string, sessions.RotationMetadata) (*sessions.Lease, error)
	ReleaseOutcome(context.Context, *sessions.Lease) (sessions.ReleaseResult, error)
}

func TestLLMSessionAcquireRotateReleaseAttemptJourneyBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
		fixture := newExactFactFixture(t, selected)
		identity := agentmemory.Identity(mustTestAgentIdentityForRun(fixture.runID, "revision-matrix-agent", ""))
		ctx, cancel := context.WithTimeout(runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure), 15*time.Second)
		defer cancel()

		var bundleHash string
		if err := selected.db.QueryRowContext(ctx, `SELECT bundle_hash FROM runs WHERE run_id=$1`, fixture.runID).Scan(&bundleHash); err != nil {
			t.Fatal(err)
		}
		lateFailure := errors.New("injected session attempt failure after generated ID")
		postcommitFailure := errors.New("injected rotation candidate handoff failure")
		phase := "acquire"
		failAttempt := map[string]bool{"acquire": true, "rotate": true}
		observed := map[string][]string{}
		probe := llmCandidateRetryProbe{request: func(txctx context.Context, tx *sql.Tx, runID string, due *time.Time) (runlifecycle.CandidateRequestResult, error) {
			if phase == "acquire" || phase == "rotate" {
				var sessionID string
				if err := tx.QueryRowContext(txctx, `SELECT session_id FROM agent_sessions WHERE run_id=$1 AND status='active'`, runID).Scan(&sessionID); err != nil {
					return runlifecycle.CandidateRequestResult{}, err
				}
				observed[phase] = append(observed[phase], sessionID)
			}
			var result runlifecycle.CandidateRequestResult
			var err error
			if selected.postgres {
				result, err = selected.selected.(*PostgresStore).runLifecyclePostgresOwner.WriteCompletionCandidateTx(txctx, tx, runID, due)
			} else {
				result, err = selected.selected.(*SQLiteRuntimeStore).runLifecycleSQLiteOwner.WriteCompletionCandidateTx(txctx, tx, runID, due)
			}
			if err != nil {
				return result, err
			}
			if failAttempt[phase] {
				failAttempt[phase] = false
				return result, lateFailure
			}
			return result, nil
		}}

		var owner llmSessionAttemptJourneyOwner
		if selected.postgres {
			store := selected.selected.(*PostgresStore)
			constructed, err := llmpersistence.NewPostgres(store.backend, store.requireCurrentSchema, probe, store.runLifecycleCandidates)
			if err != nil {
				t.Fatal(err)
			}
			owner = constructed
		} else {
			store := selected.selected.(*SQLiteRuntimeStore)
			constructed, err := llmpersistence.NewSQLite(store.backend, store.requireCurrentSchema, probe, store.runLifecycleCandidates, store.now)
			if err != nil {
				t.Fatal(err)
			}
			owner = constructed
		}

		var submissions []runlifecycle.Candidate
		var handoffError error
		registrar := selected.selected.(runlifecycle.CandidateRegistrar)
		registration, err := registrar.RegisterCompletionCandidateSink(ctx, runlifecycle.CandidateScope{BundleHash: bundleHash}, &completionHandoffEvidenceProbeSink{submit: func(candidate runlifecycle.Candidate) error {
			submissions = append(submissions, candidate)
			if candidate.RunID != fixture.runID {
				t.Errorf("candidate run=%s want=%s", candidate.RunID, fixture.runID)
			}
			return handoffError
		}})
		if err != nil {
			t.Fatal(err)
		}
		defer registration.Release()

		failedAcquire, failedConversation, err := owner.AcquireLiveSession(ctx, identity, "journey-worker")
		if failedAcquire != nil || failedConversation.SessionID != "" || !errors.Is(err, lateFailure) || len(submissions) != 0 {
			t.Fatalf("failed acquire leaked result or handoff: lease=%+v conversation=%+v err=%v submissions=%d", failedAcquire, failedConversation, err, len(submissions))
		}
		acquired, conversation, err := owner.AcquireLiveSession(ctx, identity, "journey-worker")
		if err != nil || acquired == nil || conversation.SessionID != acquired.SessionID {
			t.Fatalf("acquire result: lease=%+v conversation=%+v err=%v", acquired, conversation, err)
		}
		assertLLMSessionJourneyAttemptIDs(t, selected.db, fixture.runID, observed["acquire"], acquired.SessionID)
		if len(submissions) != 1 {
			t.Fatalf("acquire candidate handoffs=%d want=1", len(submissions))
		}

		phase = "rotate"
		rotation := sessions.RotationMetadata{OperationID: uuid.NewString(), RetryReason: "session-attempt-journey"}
		failedRotation, err := owner.Rotate(ctx, identity, "journey-worker", rotation)
		if failedRotation != nil || !errors.Is(err, lateFailure) || len(submissions) != 1 {
			t.Fatalf("failed rotation leaked result or handoff: lease=%+v err=%v submissions=%d", failedRotation, err, len(submissions))
		}
		handoffError = postcommitFailure
		rotated, err := owner.Rotate(ctx, identity, "journey-worker", rotation)
		if rotated == nil || !errors.Is(err, postcommitFailure) || rotated.RetriesFromSessionID != acquired.SessionID {
			t.Fatalf("acknowledged rotation lost lease or cleanup error: lease=%+v err=%v", rotated, err)
		}
		assertLLMSessionJourneyAttemptIDs(t, selected.db, fixture.runID, observed["rotate"], rotated.SessionID)
		if len(submissions) != 2 {
			t.Fatalf("rotation candidate handoffs=%d want=2", len(submissions))
		}
		if selected.postgres {
			beforeReplay := len(submissions)
			replayed, err := owner.Rotate(ctx, identity, "journey-worker", rotation)
			if err != nil || replayed == nil || replayed.SessionID != rotated.SessionID || len(submissions) != beforeReplay {
				t.Fatalf("rotation replay duplicated effect: lease=%+v err=%v handoffs=%d", replayed, err, len(submissions))
			}
		}

		phase, handoffError = "release", nil
		released, err := owner.ReleaseOutcome(ctx, rotated)
		if err != nil || !released.Acknowledged {
			t.Fatalf("release result=%+v err=%v", released, err)
		}
		beforeDuplicate := len(submissions)
		released, err = owner.ReleaseOutcome(ctx, rotated)
		if err == nil || released.Acknowledged || len(submissions) != beforeDuplicate {
			t.Fatalf("duplicate release result=%+v err=%v handoffs=%d", released, err, len(submissions))
		}

		var reopened interface {
			LoadActiveConversation(context.Context, agentmemory.Identity) (runtimellm.ConversationRecord, bool, error)
		}
		var readDB *sql.DB
		if selected.postgres {
			reopened = newPostgresStoreWithBackend(mustPostgresBackend(selected.db))
			readDB = selected.db
		} else {
			store := newBootstrappedSQLiteRuntimeStoreForPath(t, selected.selected.(*SQLiteRuntimeStore).Path())
			reopened, readDB = store, store.backend.ConstructionHandle()
		}
		reloaded, found, err := reopened.LoadActiveConversation(ctx, identity)
		if err != nil || !found || reloaded.SessionID != rotated.SessionID {
			t.Fatalf("reconstructed selected-store readback: record=%+v found=%t err=%v", reloaded, found, err)
		}
		var active, total int
		var predecessorStatus, successor, holder string
		if err := readDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_sessions WHERE run_id=$1 AND status='active'`, fixture.runID).Scan(&active); err != nil {
			t.Fatal(err)
		}
		if err := readDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_sessions WHERE run_id=$1`, fixture.runID).Scan(&total); err != nil {
			t.Fatal(err)
		}
		if err := readDB.QueryRowContext(ctx, `SELECT status, successor_session_id FROM agent_sessions WHERE session_id=$1`, acquired.SessionID).Scan(&predecessorStatus, &successor); err != nil {
			t.Fatal(err)
		}
		if err := readDB.QueryRowContext(ctx, `SELECT COALESCE(lease_holder,'') FROM agent_sessions WHERE session_id=$1`, rotated.SessionID).Scan(&holder); err != nil {
			t.Fatal(err)
		}
		if active != 1 || total != 2 || predecessorStatus != "terminated" || successor != rotated.SessionID || holder != "" {
			t.Fatalf("durable session chain active=%d total=%d predecessor=%s successor=%s holder=%q", active, total, predecessorStatus, successor, holder)
		}
	})
}

func assertLLMSessionJourneyAttemptIDs(t *testing.T, db *sql.DB, runID string, observed []string, committed string) {
	t.Helper()
	if len(observed) != 2 || observed[0] == observed[1] || observed[1] != committed {
		t.Fatalf("generated attempt IDs=%v committed=%s", observed, committed)
	}
	for attempt, id := range observed {
		want := attempt
		var rows, facts int
		if err := db.QueryRow(`SELECT COUNT(*) FROM agent_sessions WHERE session_id=$1`, id).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1 AND family='agent_sessions' AND fact_key=$2`, runID, id).Scan(&facts); err != nil {
			t.Fatal(err)
		}
		if rows != want || facts != want {
			t.Fatalf("attempt %d ID=%s rows=%d facts=%d want=%d", attempt, id, rows, facts, want)
		}
	}
}
