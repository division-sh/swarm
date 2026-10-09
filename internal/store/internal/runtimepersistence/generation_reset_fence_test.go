package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	"github.com/google/uuid"
)

func TestGenerationMutationFenceRetainedResetBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, completion := range []string{"commit", "rollback"} {
			t.Run(backend+"/"+completion, func(t *testing.T) {
				selected, db, sqlite := selectedForkDiscardTestStore(t, backend)
				ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 15*time.Second)
				defer cancel()
				process := selectedPreparationProcessForTest(t, selected)
				runA, runB := uuid.NewString(), uuid.NewString()
				now := time.Now().UTC()
				requirePausedRunForTest(t, ctx, selected, runA, now)
				requirePausedRunForTest(t, ctx, selected, runB, now)
				request := admitRetainedResetCleanupProof(t, process, selected.(destructivereset.QuiescenceStore), runA, false, runB)
				var sequence int64
				if err := db.QueryRowContext(ctx, `SELECT last_sequence FROM author_activity_order WHERE singleton_id=1`).Scan(&sequence); err != nil {
					t.Fatal(err)
				}
				mutationStore := selected
				if sqlite {
					mutationStore = NewSQLiteRuntimeStoreForTest(db)
				}
				rolledBack := errors.New("release reset mutation fence by rollback")
				reset := make(chan error, 1)
				err := runUnrevisionedEventFixtureTransactionForTest(ctx, mutationStore, func(txctx context.Context, tx *sql.Tx) error {
					if err := generationauthority.FenceMutation(txctx, tx, sqlite); err != nil {
						return err
					}
					// Cleanup uses the retained production operation; the independent
					// native mutation keeps its database fence until settlement.
					waiting, stop := context.WithTimeout(ctx, time.Second)
					defer stop()
					go func() {
						_, err := process.ApplyDestructiveResetCleanup(waiting, request, nil)
						reset <- err
					}()
					<-waiting.Done()
					if completion == "rollback" {
						return rolledBack
					}
					return nil
				})
				if (completion == "commit" && err != nil) || (completion == "rollback" && !errors.Is(err, rolledBack)) {
					t.Fatalf("reset mutation fence %s: %v", completion, err)
				}
				if err := <-reset; !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("reset crossed held generation mutation fence: %v", err)
				}
				var runs int
				if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE run_id IN ($1,$2)`, runA, runB).Scan(&runs); err != nil || runs != 2 {
					t.Fatalf("blocked/refused cleanup mutated runs: count=%d err=%v", runs, err)
				}
				var afterSequence int64
				if err := db.QueryRowContext(ctx, `SELECT last_sequence FROM author_activity_order WHERE singleton_id=1`).Scan(&afterSequence); err != nil || afterSequence != sequence {
					t.Fatalf("fencing allocated author activity: before=%d after=%d err=%v", sequence, afterSequence, err)
				}
				if err := process.ProveCurrent(ctx); err != nil {
					t.Fatalf("cancelled/refused cleanup lost process possession: %v", err)
				}
				result, err := process.ApplyDestructiveResetCleanup(ctx, request, nil)
				if err != nil || result.DryRun || len(result.RunIDs) != 2 {
					t.Fatalf("reset after mutation completion: result=%+v err=%v", result, err)
				}
				if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE run_id IN ($1,$2)`, runA, runB).Scan(&runs); err != nil || runs != 0 {
					t.Fatalf("successful reset retained planned runs: count=%d err=%v", runs, err)
				}
			})
		}
	}
}
