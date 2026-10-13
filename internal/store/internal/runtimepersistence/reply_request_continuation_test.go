package runtimepersistence

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

func TestIssue642ReplyRequestContinuationRechecksNativeRowBothStores(t *testing.T) {
	for _, backend := range []struct {
		name  string
		setup func(*testing.T) (replyContextStoreTestSurface, func(context.Context, string, ...string) error)
	}{{"sqlite", setupSQLiteReplyContextStoreTest}, {"postgres", setupPostgresReplyContextStoreTest}} {
		t.Run(backend.name, func(t *testing.T) {
			store, seed := backend.setup(t)
			ctx, run, request, answer, rollbackRequest := context.Background(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
			if err := seed(ctx, run, request, answer, rollbackRequest); err != nil {
				t.Fatal(err)
			}
			record := replyContextFreezeTestRecord(request, run, "continued", time.Now().UTC().Truncate(time.Microsecond))
			if err := store.CreateReplyContext(ctx, record); err != nil {
				t.Fatal(err)
			}
			for _, fault := range []string{"exact", "missing", "foreign", "enclosing", "original", "birth", "canceled", "settled_after_plan"} {
				t.Run(fault, func(t *testing.T) {
					expected := record.Normalized()
					switch fault {
					case "missing":
						expected.ID = uuid.NewString()
					case "foreign":
						expected.RunID = uuid.NewString()
					case "enclosing":
						expected.EnclosingReplyContextID = uuid.NewString()
					case "original":
						expected.SourceRequestEventID = uuid.NewString()
					case "birth":
						expected.CreatedAt = expected.CreatedAt.Add(time.Second)
					case "settled_after_plan":
						if _, outcome, err := store.ClaimReplyContext(ctx, record.ID, answer); err != nil || outcome != replycontext.ClaimAccepted {
							t.Fatalf("settle after planning: outcome=%s error=%v", outcome, err)
						}
					}
					before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, store)
					if err != nil {
						t.Fatal(err)
					}
					err = runSelectedFixtureMutation(ctx, store, "reply request continuation proof", func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
						if fault != "exact" {
							written := replyContextFreezeTestRecord(rollbackRequest, run, "rollback-"+fault, record.CreatedAt)
							var err error
							if native, ok := store.(*PostgresStore); ok {
								err = native.replyPostgresOwner.CreateWithinTransaction(ctx, attempt, written)
							} else {
								err = store.(*SQLiteRuntimeStore).replySQLiteOwner.CreateWithinTransaction(ctx, attempt, written)
							}
							if err != nil {
								t.Fatalf("actual pre-refusal write failed: %v", err)
							}
							got, readErr := storeReplyWithinAttempt(ctx, store, attempt, written.ID)
							if readErr != nil || !got.SameIdentity(written) {
								t.Fatalf("pre-refusal SQL row not observable in attempt: %v", readErr)
							}
						}
						if fault == "canceled" {
							canceled, cancel := context.WithCancel(ctx)
							cancel()
							ctx = canceled
						}
						switch native := store.(type) {
						case *PostgresStore:
							return native.replyPostgresOwner.RequireOpenWithinTransaction(ctx, attempt, expected)
						case *SQLiteRuntimeStore:
							return native.replySQLiteOwner.RequireOpenWithinTransaction(ctx, attempt, expected)
						default:
							t.Fatalf("unsupported selected owner %T", store)
							return nil
						}
					})
					if (err == nil) != (fault == "exact") {
						t.Fatalf("native continuation %s: %v", fault, err)
					}
					after, readErr := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, store)
					if readErr != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("continuation admission rewrote business/history evidence: %v", readErr)
					}
				})
			}
		})
	}
}

func storeReplyWithinAttempt(ctx context.Context, selected any, attempt *mutationprotocol.Attempt, id string) (replycontext.Record, error) {
	var record replycontext.Record
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		if native, ok := selected.(*PostgresStore); ok {
			record, err = native.replyPostgresOwner.LoadWithinTransaction(ctx, tx, id)
		} else {
			record, err = selected.(*SQLiteRuntimeStore).replySQLiteOwner.LoadWithinTransaction(ctx, tx, id)
		}
		return err
	})
	return record, err
}
