package mutationprotocol

import (
	"context"
	"database/sql"
	"testing"

	privateactivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

func TestEmptyDeclaredEffectsSkipRevisionFinalizerWithoutSkippingEvidence(t *testing.T) {
	faultMatrixStores(t, func(t *testing.T, db *sql.DB, dialect privateactivity.Dialect) {
		faultMatrixActivitySchema(t, db)
		for _, tc := range []struct {
			name         string
			evidence     Evidence
			wantSequence sql.NullInt64
			wantStories  int
		}{
			{name: "revision_only", evidence: RevisionOnly},
			{name: "authority_fence", evidence: AuthorityFence, wantSequence: sql.NullInt64{Int64: 0, Valid: true}},
			{name: "story", evidence: Story, wantSequence: sql.NullInt64{Int64: 1, Valid: true}, wantStories: 1},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var slot transactiontest.Slot
				collector, restore, err := slot.Install(transactiontest.Options{})
				if err != nil {
					t.Fatal(err)
				}
				defer restore()
				receipt := slot.Begin(false, false)
				ctx := transactiontest.WithAttempt(context.Background(), receipt)
				value := string(dialect) + "-empty-" + tc.name
				result := run(ctx, dialect, tc.evidence, Ordinary, nil, nil, faultMatrixNative(db, nil), func(ctx context.Context, attempt *Attempt) (string, error) {
					if err := faultMatrixInsert(ctx, attempt, value); err != nil {
						return "", err
					}
					if tc.evidence == Story {
						if err := attempt.Record(ctx, faultMatrixDraft(value)); err != nil {
							return "", err
						}
					}
					return value, nil
				})
				receipt.Finish(result.Err())
				if got, ok := result.Value(); !ok || got != value || result.Err() != nil || result.Phase() != PostCommit {
					t.Fatalf("empty revision mutation = value:%q ack:%v phase:%v err:%v", got, ok, result.Phase(), result.Err())
				}
				if revision := collector.Snapshot().Total.Revision; revision != (transactiontest.RevisionCounts{}) {
					t.Fatalf("empty declaration invoked revision finalizer: %+v", revision)
				}
				if count := faultMatrixCount(t, db, value); count != 1 {
					t.Fatalf("domain write count=%d, want 1", count)
				}
				var sequence sql.NullInt64
				err = db.QueryRow(`SELECT last_sequence FROM author_activity_order WHERE singleton_id=1`).Scan(&sequence)
				if !tc.wantSequence.Valid {
					if err != sql.ErrNoRows {
						t.Fatalf("revision-only mutation acquired activity order: %v", err)
					}
				} else if err != nil || sequence != tc.wantSequence {
					t.Fatalf("activity order=%+v err=%v, want %+v", sequence, err, tc.wantSequence)
				}
				var stories int
				if err := db.QueryRow(`SELECT COUNT(*) FROM author_activity_occurrences WHERE dedup_key=$1`, value).Scan(&stories); err != nil || stories != tc.wantStories {
					t.Fatalf("activity stories=%d err=%v, want %d", stories, err, tc.wantStories)
				}
			})
		}
	})
}
