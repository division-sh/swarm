package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/replycontext"
	replycontextstore "github.com/division-sh/swarm/internal/store/internal/backend/replycontext"
	"github.com/google/uuid"
)

func TestIssue642OriginalReplyEvidenceAtomicFenceBothStores(t *testing.T) {
	for _, backend := range []struct {
		name  string
		setup func(*testing.T) (replyContextStoreTestSurface, func(context.Context, string, ...string) error)
	}{{"sqlite", setupSQLiteReplyContextStoreTest}, {"postgres", setupPostgresReplyContextStoreTest}} {
		t.Run(backend.name, func(t *testing.T) {
			store, seed := backend.setup(t)
			ctx, run, sibling := context.Background(), uuid.NewString(), uuid.NewString()
			request, otherRequest, runlessRequest := uuid.NewString(), uuid.NewString(), uuid.NewString()
			if err := seed(ctx, run, request); err != nil {
				t.Fatal(err)
			}
			if err := seed(ctx, sibling, otherRequest); err != nil {
				t.Fatal(err)
			}
			seedReplyRunlessPhysicalEvent(t, ctx, store, runlessRequest)
			for _, test := range []struct {
				name, sourceRun, sourceEvent string
				accepted                     bool
			}{
				{"matching_original_run", run, request, true},
				{"lawful_runless_reference", "", runlessRequest, true},
				{"false_null_run_owned_event", "", request, false},
				{"run_on_runless_event", run, runlessRequest, false},
				{"wrong_original_run", sibling, request, false},
				{"missing_original_request", run, uuid.NewString(), false},
			} {
				t.Run(test.name, func(t *testing.T) {
					// Logical coordinates deliberately have no physical child event.
					// This is constraint proof, not runtime projection admission.
					record := replyContextFreezeTestRecord(uuid.NewString(), run, test.name, time.Now().UTC().Truncate(time.Microsecond))
					record.SourceRunID, record.SourceRequestEventID = test.sourceRun, test.sourceEvent
					before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, store)
					if err != nil {
						t.Fatal(err)
					}
					var inserted bool
					err = runUnrevisionedEventFixtureTransactionForTest(ctx, store, func(ctx context.Context, tx *sql.Tx) (err error) {
						inserted, err = replycontextstore.InsertConstraintFixtureForTest(ctx, tx, backend.name == "postgres", record)
						return err
					})
					if test.accepted {
						got, readErr := store.LoadReplyContext(ctx, record.ID)
						if err != nil || !inserted || readErr != nil || !reflect.DeepEqual(got, record.Normalized()) {
							t.Fatalf("original evidence control: inserted=%v err=%v read=%v got=%+v", inserted, err, readErr, got)
						}
						return
					}
					if err == nil || inserted {
						t.Fatalf("mismatched original evidence passed physical atomic fence: inserted=%v err=%v", inserted, err)
					}
					if _, err := store.LoadReplyContext(ctx, record.ID); !errors.Is(err, replycontext.ErrNotFound) {
						t.Fatalf("rejected evidence left a row: %v", err)
					}
					after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, store)
					if err != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("constraint refusal mutated selected store: %v", err)
					}
				})
			}
		})
	}
}

func TestIssue642EnclosingReplyAdmissionIsAtomicBothStores(t *testing.T) {
	for _, backend := range []struct {
		name  string
		setup func(*testing.T) (replyContextStoreTestSurface, func(context.Context, string, ...string) error)
	}{{"sqlite", setupSQLiteReplyContextStoreTest}, {"postgres", setupPostgresReplyContextStoreTest}} {
		t.Run(backend.name, func(t *testing.T) {
			store, seed := backend.setup(t)
			ctx, run := context.Background(), uuid.NewString()
			q1, q2, answer := uuid.NewString(), uuid.NewString(), uuid.NewString()
			if err := seed(ctx, run, q1, q2, answer); err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC().Truncate(time.Microsecond)
			parent := replyContextFreezeTestRecord(q1, run, "enclosing", now)
			if err := store.CreateReplyContext(ctx, parent); err != nil {
				t.Fatal(err)
			}
			nested := replyContextFreezeTestRecord(q2, run, "nested", now)
			nested.RequesterFlowID = parent.ProviderFlowID
			nested.ProviderFlowID = "inner-provider"
			nested.ID = replycontext.DeterministicID(q2, nested.RequesterFlowID, nested.RequestOutputPin, nested.ReplyInputPin, nested.ProviderFlowID, nested.Origin)
			if err := nested.BindEnclosing(parent); err != nil {
				t.Fatal(err)
			}
			// A parent that closes after planning cannot authorize a new request.
			if _, outcome, err := store.ClaimReplyContext(ctx, parent.ID, answer); err != nil || outcome != replycontext.ClaimAccepted {
				t.Fatalf("close original enclosing obligation: %s %v", outcome, err)
			}
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, store)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.CreateReplyContext(ctx, nested); err == nil {
				t.Fatal("closed enclosing obligation admitted a newly published request")
			}
			after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, store)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("late parent refusal leaked row/history: %v", err)
			}
		})
	}
}
