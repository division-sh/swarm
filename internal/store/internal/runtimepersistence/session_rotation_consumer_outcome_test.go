package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/testutil"
)

// The store performs real SQL and acknowledges COMMIT. Only the subsequent
// candidate sink fails; no wrapper fabricates a committed session result.
func TestPostgresRuntimeSessionRotationPreservesCommittedHandoffOutcome(t *testing.T) {
	for _, operation := range []string{"turn", "parse", "prepare", "prepare_acquire"} {
		for _, fail := range []bool{false, true} {
			phase := "healthy"
			if fail {
				phase = "handoff_failed"
			}
			t.Run(operation+"/"+phase, func(t *testing.T) {
				_, db, _ := testutil.StartPostgres(t)
				store := admitTestPostgresStore(t, db)
				base := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
				seedSpecAgent(t, base, store, "a1", "", "")
				seedSpecMemoryRun(t, base, db)
				identity := specMemoryIdentity("a1", "global")
				memory := agentmemory.Authored(true)
				ctx := agentmemory.WithExecution(base, memory, identity)
				lease, err := store.Acquire(ctx, identity, "worker-1")
				if err != nil {
					t.Fatal(err)
				}
				if err := store.Release(ctx, lease); err != nil {
					t.Fatal(err)
				}
				oldID := lease.SessionID
				session := &runtimellm.Session{ID: oldID, ProviderSessionID: "provider-old", AgentID: identity.AgentID(), Memory: memory, MemoryIdentity: identity, TurnCount: 1, ParseFailures: 1, Messages: []runtimellm.Message{{Role: "assistant", Content: "done"}}}
				var bundleHash string
				if err := db.QueryRowContext(base, `SELECT bundle_hash FROM runs WHERE run_id=$1::uuid`, identity.RunID).Scan(&bundleHash); err != nil {
					t.Fatal(err)
				}
				failure, cleanup := errors.New("postcommit session handoff failed"), errors.New("postcommit release handoff failed")
				submits, rotationSubmits, releaseSubmits := 0, 0, 0
				committedID := ""
				registration, err := store.RegisterCompletionCandidateSink(ctx, runlifecycle.CandidateScope{BundleHash: bundleHash}, &completionHandoffEvidenceProbeSink{submit: func(candidate runlifecycle.Candidate) error {
					submits++
					if candidate.RunID != identity.RunID {
						t.Errorf("foreign candidate: %+v", candidate)
					}
					var activeID string
					var holder sql.NullString
					if err := db.QueryRowContext(base, `SELECT session_id::text, lease_holder FROM agent_sessions WHERE run_id=$1::uuid AND status='active'`, identity.RunID).Scan(&activeID, &holder); err != nil {
						return err
					}
					if !holder.Valid {
						releaseSubmits++
						if fail {
							return cleanup
						}
						return nil
					}
					if activeID != oldID {
						rotationSubmits++
						committedID = activeID
						var status string
						if err := db.QueryRowContext(base, `SELECT status FROM agent_sessions WHERE session_id=$1::uuid`, oldID).Scan(&status); err != nil {
							return err
						}
						if status != "terminated" {
							t.Errorf("handoff before predecessor termination: %s", status)
						}
					}
					if fail && (activeID != oldID || operation == "prepare_acquire") {
						return failure
					}
					return nil
				}})
				if err != nil {
					t.Fatal(err)
				}
				defer registration.Release()
				var result *sessions.Lease
				switch operation {
				case "turn":
					result, err = runtimellm.MaybeRotateAfterTurn(ctx, session, store, "worker-1", 1, nil)
				case "parse":
					result, err = runtimellm.MaybeRotateAfterParseFailures(ctx, session, store, "worker-1", 1, nil)
				default:
					cfg := &config.Config{}
					cfg.LLM.Session.RotateAfterTurns = 1
					err = runtimellm.NewMockRuntime(cfg, store, "worker-1", nil, nil, nil).PrepareManagedSession(ctx, session)
				}
				if fail && !errors.Is(err, failure) || !fail && err != nil {
					t.Fatalf("handoff error not preserved: %v", err)
				}
				prepared := operation == "prepare" || operation == "prepare_acquire"
				if fail && prepared && !errors.Is(err, cleanup) {
					t.Fatalf("release handoff error lost: %v", err)
				}
				wantRows, wantRotations, wantSubmits := 2, 1, 1
				if prepared {
					wantSubmits = 3
				}
				if operation == "prepare_acquire" && fail {
					wantRows, wantRotations, wantSubmits = 1, 0, 2
				}
				if rotationSubmits != wantRotations || submits != wantSubmits {
					t.Fatalf("replayed or missing operations: rotations=%d submits=%d", rotationSubmits, submits)
				}
				if prepared && releaseSubmits != 1 {
					t.Fatalf("release submissions=%d", releaseSubmits)
				}
				var count int
				if err := db.QueryRowContext(base, `SELECT COUNT(*) FROM agent_sessions WHERE run_id=$1::uuid`, identity.RunID).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != wantRows {
					t.Fatalf("session rows=%d want=%d", count, wantRows)
				}
				if wantRotations == 1 {
					if committedID == "" || session.ID != committedID || session.ProviderSessionID != "" || session.TurnCount != 0 || session.ParseFailures != 0 || len(session.Messages) != 1 || session.Messages[0].Role != "system" {
						t.Fatalf("committed successor not adopted: %+v durable=%s", session, committedID)
					}
					if !prepared && (result == nil || result.SessionID != committedID || result.RetriesFromSessionID != oldID) {
						t.Fatalf("committed lease not returned: %+v", result)
					}
				} else if session.ID != oldID || session.TurnCount != 1 {
					t.Fatalf("acquire failure rotated session: %+v", session)
				}
				registration.Release()
				if !prepared {
					if err := store.Release(ctx, result); err != nil {
						t.Fatal(err)
					}
				}
				current, err := store.Acquire(ctx, identity, "worker-2")
				if err != nil {
					t.Fatalf("committed session still leased: %v", err)
				}
				if current.SessionID != session.ID {
					t.Fatalf("successor changed on reacquire: %+v session=%s", current, session.ID)
				}
				if err := store.Release(context.WithoutCancel(ctx), current); err != nil {
					t.Fatal(err)
				}
				t.Logf("real commits: sessions=%d rotation_handoffs=%d release_handoffs=%d total_handoffs=%d", count, rotationSubmits, releaseSubmits, submits)
			})
		}
	}
}
