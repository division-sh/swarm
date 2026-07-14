package authoractivity

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testutil"
)

func TestPostgresSingletonOrdersByCommitAndRollbackReusesSequence(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)

	txA, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	storyA, err := Begin(ctx, txA, DialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	draftA := testDraft(KindInboundReceived, "received", now)
	draftA.DedupKey = "postgres-a"
	if err := Record(storyA, draftA); err != nil {
		t.Fatal(err)
	}

	type blockedResult struct {
		tx    *sql.Tx
		story context.Context
		err   error
	}
	blocked := make(chan blockedResult, 1)
	go func() {
		txB, err := db.BeginTx(ctx, nil)
		if err != nil {
			blocked <- blockedResult{err: err}
			return
		}
		storyB, err := Begin(ctx, txB, DialectPostgres)
		blocked <- blockedResult{tx: txB, story: storyB, err: err}
	}()

	select {
	case result := <-blocked:
		if result.tx != nil {
			_ = result.tx.Rollback()
		}
		t.Fatalf("second story acquired singleton before first committed: %v", result.err)
	case <-time.After(150 * time.Millisecond):
	}
	before, err := List(ctx, db, DialectPostgres, ListOptions{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Occurrences) != 0 {
		t.Fatalf("reader observed uncommitted occurrence: %#v", before.Occurrences)
	}
	if err := Finalize(storyA); err != nil {
		t.Fatal(err)
	}
	if err := txA.Commit(); err != nil {
		t.Fatal(err)
	}

	resultB := <-blocked
	if resultB.err != nil {
		t.Fatal(resultB.err)
	}
	draftB := testDraft(KindEventEmitted, "emitted", now.Add(time.Second))
	draftB.DedupKey = "postgres-b"
	if err := Record(resultB.story, draftB); err != nil {
		t.Fatal(err)
	}
	if err := Finalize(resultB.story); err != nil {
		t.Fatal(err)
	}
	if err := resultB.tx.Commit(); err != nil {
		t.Fatal(err)
	}

	txRollback, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	rollbackStory, err := Begin(ctx, txRollback, DialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	rolledBack := testDraft(KindCardLifecycle, "created", now.Add(2*time.Second))
	rolledBack.DedupKey = "postgres-rollback"
	if err := Record(rollbackStory, rolledBack); err != nil {
		t.Fatal(err)
	}
	if err := Finalize(rollbackStory); err != nil {
		t.Fatal(err)
	}
	if err := txRollback.Rollback(); err != nil {
		t.Fatal(err)
	}
	third := testDraft(KindRunLifecycle, "started", now.Add(3*time.Second))
	third.DedupKey = "postgres-third"
	commitDrafts(t, db, DialectPostgres, third)

	listed, err := List(ctx, db, DialectPostgres, ListOptions{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Occurrences) != 3 || listed.Occurrences[0].Sequence != 1 || listed.Occurrences[1].Sequence != 2 || listed.Occurrences[2].Sequence != 3 {
		t.Fatalf("ordered occurrences = %#v", listed.Occurrences)
	}
}

func TestSQLitePostgresRegistryPersistenceParity(t *testing.T) {
	_, postgres, _ := testutil.StartPostgres(t)
	sqlite := openAuthorActivitySQLite(t)
	now := time.Date(2026, 7, 14, 13, 0, 0, 0, time.UTC)
	drafts := make([]Draft, 0, 32)
	for _, kind := range Kinds() {
		for transition := range kindContracts[kind].Transitions {
			draft := testDraft(kind, transition, now.Add(time.Duration(len(drafts))*time.Millisecond))
			if failureRequired(kind, transition) {
				draft.Failure = testFailure(t)
			}
			drafts = append(drafts, draft)
		}
	}
	commitDrafts(t, sqlite, DialectSQLite, drafts...)
	commitDrafts(t, postgres, DialectPostgres, drafts...)
	sqliteRows, err := List(context.Background(), sqlite, DialectSQLite, ListOptions{Limit: 500})
	if err != nil {
		t.Fatal(err)
	}
	postgresRows, err := List(context.Background(), postgres, DialectPostgres, ListOptions{Limit: 500})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sqliteRows, postgresRows) {
		t.Fatalf("backend rows differ\nsqlite: %#v\npostgres: %#v", sqliteRows, postgresRows)
	}
}
