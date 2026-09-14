package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/google/uuid"
)

func TestMailboxRequesterFixtureTransactionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			owner, runID := decisionCardTestStore(t, backend)
			db, postgres := decisionCardStoreDB(t, owner)
			ctx := testAuthorActivityContext()
			now := time.Now().UTC()
			card, continuation := newHumanTaskDecisionCardTestFixture(t, runID, uuid.NewString(), now, 0, now.Add(time.Hour))
			if err := owner.(decisioncard.HumanTaskStore).CreateHumanTaskCard(ctx, card, continuation); err != nil {
				t.Fatal(err)
			}
			for _, coordinate := range []string{"requester_flow_id", "requester_flow_instance", "requester_entity_id"} {
				t.Run(coordinate, func(t *testing.T) {
					before := snapshotForkHistoricalExecutionTables(t, db, postgres)
					read := func() sql.NullString {
						t.Helper()
						var v sql.NullString
						if err := db.QueryRow(`SELECT CAST(`+coordinate+` AS TEXT) FROM human_task_continuations WHERE card_id=$1 AND run_id=$2`, card.CardID, runID).Scan(&v); err != nil {
							t.Fatal(err)
						}
						return v
					}
					original := read()
					hostile := uuid.NewString()
					restore, err := CorruptHumanTaskRequesterForTest(ctx, owner, runID, card.CardID, coordinate, hostile)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if restore != nil {
							if err := restore(context.Background()); err != nil {
								t.Error(err)
							}
						}
					})
					if got := read(); got != (sql.NullString{String: hostile, Valid: true}) {
						t.Fatalf("installation = %+v", got)
					}
					if err := restore(ctx); err != nil {
						t.Fatal(err)
					}
					restore = nil
					if got := read(); got != original {
						t.Fatalf("restoration = %+v, want %+v", got, original)
					}
					if after := snapshotForkHistoricalExecutionTables(t, db, postgres); !reflect.DeepEqual(before, after) {
						t.Fatal("fixture installation/restoration drafted durable business evidence")
					}
				})
			}
			for _, tc := range []struct{ run, card, coordinate string }{
				{uuid.NewString(), card.CardID, "requester_flow_id"},
				{runID, uuid.NewString(), "requester_flow_id"},
				{runID, card.CardID, "state"},
			} {
				before := snapshotForkHistoricalExecutionTables(t, db, postgres)
				if restore, err := CorruptHumanTaskRequesterForTest(ctx, owner, tc.run, tc.card, tc.coordinate, "hostile"); err == nil || restore != nil {
					t.Fatalf("invalid fixture admitted: restore=%v err=%v", restore != nil, err)
				}
				if after := snapshotForkHistoricalExecutionTables(t, db, postgres); !reflect.DeepEqual(before, after) {
					t.Fatal("rejected fixture mutated state")
				}
			}
			t.Run("nullable_restoration", func(t *testing.T) {
				for _, coordinate := range []string{"requester_flow_id", "requester_flow_instance", "requester_entity_id"} {
					var canonical sql.NullString
					query := `SELECT CAST(` + coordinate + ` AS TEXT) FROM human_task_continuations WHERE card_id=$1`
					if err := db.QueryRow(query, card.CardID).Scan(&canonical); err != nil {
						t.Fatal(err)
					}
					values := []sql.NullString{{}}
					if coordinate != "requester_entity_id" {
						values = append(values, sql.NullString{Valid: true})
					}
					for _, original := range values {
						if err := runMailboxFixtureTransaction(ctx, owner, func(ctx context.Context, tx *sql.Tx) error {
							_, err := tx.ExecContext(ctx, `UPDATE human_task_continuations SET `+coordinate+`=$1 WHERE card_id=$2`, original, card.CardID)
							return err
						}); err != nil {
							t.Fatal(err)
						}
						restore, err := CorruptHumanTaskRequesterForTest(ctx, owner, runID, card.CardID, coordinate, uuid.NewString())
						if err != nil {
							t.Fatal(err)
						}
						if err := restore(ctx); err != nil {
							t.Fatal(err)
						}
						var got sql.NullString
						if err := db.QueryRow(query, card.CardID).Scan(&got); err != nil || got != original {
							t.Fatalf("nullable %s: got=%+v want=%+v err=%v", coordinate, got, original, err)
						}
					}
					if err := runMailboxFixtureTransaction(ctx, owner, func(ctx context.Context, tx *sql.Tx) error {
						_, err := tx.ExecContext(ctx, `UPDATE human_task_continuations SET `+coordinate+`=$1 WHERE card_id=$2`, canonical, card.CardID)
						return err
					}); err != nil {
						t.Fatal(err)
					}
				}
			})
			beforeContention := snapshotForkHistoricalExecutionTables(t, db, postgres)
			t.Run("controlled_writer_contention", func(t *testing.T) {
				locked, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
				go func() {
					done <- runMailboxFixtureTransaction(ctx, owner, func(ctx context.Context, tx *sql.Tx) error {
						if _, err := tx.ExecContext(ctx, `UPDATE human_task_continuations SET requester_flow_id=requester_flow_id WHERE card_id=$1`, card.CardID); err != nil {
							return err
						}
						close(locked)
						<-release
						return nil
					})
				}()
				var releaseOnce sync.Once
				releaseWriter := func() { releaseOnce.Do(func() { close(release) }) }
				defer func() {
					releaseWriter()
					if err := <-done; err != nil {
						t.Error(err)
					}
				}()
				select {
				case <-locked:
				case <-time.After(5 * time.Second):
					t.Fatal("contending selected writer did not acquire row")
				}
				blocked, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				defer cancel()
				// Admitted PostgreSQL SQL drains before cancellation rejects commit.
				// Release the real lock after cancellation, then join both owners.
				released := make(chan struct{})
				go func() { <-blocked.Done(); releaseWriter(); close(released) }()
				defer func() { <-released }()
				if restore, err := CorruptHumanTaskRequesterForTest(blocked, owner, runID, card.CardID, "requester_flow_id", "hostile"); err == nil || restore != nil || blocked.Err() == nil {
					t.Fatalf("blocked fixture must fail without installation: %v", err)
				}
			})
			if after := snapshotForkHistoricalExecutionTables(t, db, postgres); !reflect.DeepEqual(beforeContention, after) {
				t.Fatal("cancelled contended installation mutated state")
			}
			// The cancelled contended writer must not poison the selected owner.
			restore, err := CorruptHumanTaskRequesterForTest(ctx, owner, runID, card.CardID, "requester_flow_id", "after-contention")
			if err != nil {
				t.Fatal(err)
			}
			if err := restore(ctx); err != nil {
				t.Fatal(err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if _, err := CorruptHumanTaskRequesterForTest(cancelled, owner, runID, card.CardID, "requester_flow_id", "never"); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled fixture = %v", err)
			}
		})
	}
}
