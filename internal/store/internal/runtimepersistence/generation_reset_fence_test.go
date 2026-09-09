package runtimepersistence

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

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
				request := destructiveResetCleanupRequest(runA, runB, now)
				var sequence int64
				if err := db.QueryRowContext(ctx, `SELECT last_sequence FROM author_activity_order WHERE singleton_id=1`).Scan(&sequence); err != nil {
					t.Fatal(err)
				}
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				if err := generationauthority.FenceMutation(ctx, tx, sqlite); err != nil {
					t.Fatal(err)
				}
				// Only the transaction lifetime is held. Cleanup uses the same
				// retained process operation as the production serve composition.
				waiting, stop := context.WithTimeout(ctx, time.Second)
				_, err = process.ApplyDestructiveResetCleanup(waiting, request, nil)
				stop()
				if sqlite {
					if err == nil || !strings.Contains(err.Error(), "destructive reset is unsupported by the SQLite selected-store composition") {
						t.Fatalf("SQLite reset capability changed: %v", err)
					}
				} else if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("reset crossed held generation mutation fence: %v", err)
				}
				if completion == "commit" {
					err = tx.Commit()
				} else {
					err = tx.Rollback()
				}
				if err != nil {
					t.Fatal(err)
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
				if sqlite {
					if err == nil || !strings.Contains(err.Error(), "destructive reset is unsupported by the SQLite selected-store composition") {
						t.Fatalf("SQLite reset became executable after fence release: %v", err)
					}
					return
				}
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
