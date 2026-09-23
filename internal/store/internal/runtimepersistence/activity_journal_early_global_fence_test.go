package runtimepersistence

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestActivityJournalEarlyGlobalFenceNoOpAndRollbackBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			postgres := backend.name == "postgres"
			started := seedActivityStoryAttempt(t, fixture, executionmode.Live, "started", false)
			request := activityStoryTerminal(t, started, "succeeded")
			before := activityStoryOccurrences(t, fixture, postgres, started.RunID)
			beforeHead := activityJournalSequenceHead(t, fixture.db)

			var completed runtimepipeline.ActivityAttemptRecord
			if postgres {
				completed = completeActivityBehindPostgresCompetingLocks(t, fixture, request)
			} else {
				completed = completeActivityBehindSQLiteWriterFence(t, fixture, request)
			}
			if completed.Status != "succeeded" {
				t.Fatalf("completed source status = %q", completed.Status)
			}
			after := activityStoryOccurrences(t, fixture, postgres, started.RunID)
			if len(after) != len(before)+1 || !reflect.DeepEqual(after[:len(before)], before) {
				t.Fatalf("completion story = %#v, want exactly one append after %#v", after, before)
			}
			last := after[len(after)-1]
			if last.Kind != runtimeauthoractivity.KindActivityLifecycle || last.SourceIdentity != started.RequestEventID || last.Transition != "succeeded" || last.Sequence != beforeHead+1 || activityJournalSequenceHead(t, fixture.db) != last.Sequence {
				t.Fatalf("completion story = %#v, previous head = %d", last, beforeHead)
			}

			duplicate, _, err := activityStoryOuter(fixture, "complete", request)
			if err != nil || !reflect.DeepEqual(duplicate, completed) {
				t.Fatalf("duplicate completion = %#v, err=%v, want %#v", duplicate, err, completed)
			}
			if got := activityStoryOccurrences(t, fixture, postgres, started.RunID); !reflect.DeepEqual(got, after) {
				t.Fatalf("duplicate source transition emitted story: %#v", got)
			}
			if head := activityJournalSequenceHead(t, fixture.db); head != last.Sequence {
				t.Fatalf("duplicate source transition advanced sequence: %d, want %d", head, last.Sequence)
			}

			rollbackSource := seedActivityStoryAttempt(t, fixture, executionmode.Live, "started", false)
			rollbackRequest := activityStoryTerminal(t, rollbackSource, "succeeded")
			rollbackBefore := activityStoryOccurrences(t, fixture, postgres, rollbackSource.RunID)
			rollbackHead := activityJournalSequenceHead(t, fixture.db)
			removeFailure := installActivityStoryFailure(t, fixture.db, postgres)
			_, _, err = activityStoryOuter(fixture, "complete", rollbackRequest)
			removeFailure()
			if err == nil || !strings.Contains(err.Error(), "injected activity story persistence") {
				t.Fatalf("story persistence failure = %v", err)
			}
			loaded, found, err := fixture.store.(activityStoryJournal).LoadActivityAttempt(testAuthorActivityContext(), rollbackSource.RequestEventID)
			if err != nil || !found || !reflect.DeepEqual(loaded, rollbackSource) {
				t.Fatalf("rollback source = %#v, found=%v err=%v, want %#v", loaded, found, err, rollbackSource)
			}
			if got := activityStoryOccurrences(t, fixture, postgres, rollbackSource.RunID); !reflect.DeepEqual(got, rollbackBefore) {
				t.Fatalf("rollback left story: %#v", got)
			}
			if head := activityJournalSequenceHead(t, fixture.db); head != rollbackHead {
				t.Fatalf("rollback advanced sequence: %d, want %d", head, rollbackHead)
			}
			if _, _, err := activityStoryOuter(fixture, "complete", rollbackRequest); err != nil {
				t.Fatalf("retry after rollback: %v", err)
			}
			retried := activityStoryOccurrences(t, fixture, postgres, rollbackSource.RunID)
			if len(retried) != len(rollbackBefore)+1 || retried[len(retried)-1].SourceIdentity != rollbackSource.RequestEventID || retried[len(retried)-1].Transition != "succeeded" || retried[len(retried)-1].Sequence != rollbackHead+1 || activityJournalSequenceHead(t, fixture.db) != rollbackHead+1 {
				t.Fatalf("retry story = %#v, previous head = %d", retried, rollbackHead)
			}
		})
	}
}

type activityJournalCompletion struct {
	record runtimepipeline.ActivityAttemptRecord
	err    error
}

func completeActivityBehindPostgresCompetingLocks(t *testing.T, fixture authorActivityReceiptFixture, request runtimepipeline.ActivityAttemptRecord) runtimepipeline.ActivityAttemptRecord {
	t.Helper()
	ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 15*time.Second)
	defer cancel()
	domain, err := fixture.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer domain.Rollback()
	var domainPID int
	if err := domain.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&domainPID); err != nil {
		t.Fatal(err)
	}
	var lockedID string
	if err := domain.QueryRowContext(ctx, `SELECT request_event_id::text FROM activity_attempts WHERE request_event_id = $1::uuid FOR UPDATE`, request.RequestEventID).Scan(&lockedID); err != nil || lockedID != request.RequestEventID {
		t.Fatalf("lock activity domain row = %q, err=%v", lockedID, err)
	}
	global, err := fixture.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer global.Rollback()
	var globalPID int
	if err := global.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&globalPID); err != nil {
		t.Fatal(err)
	}
	var head int64
	if err := global.QueryRowContext(ctx, `SELECT last_sequence FROM author_activity_order WHERE singleton_id = 1 FOR UPDATE`).Scan(&head); err != nil {
		t.Fatal(err)
	}
	done := make(chan activityJournalCompletion, 1)
	go func() {
		record, _, err := activityStoryOuter(fixture, "complete", request)
		done <- activityJournalCompletion{record, err}
	}()
	// The owner must wait on the global story row before it can wait on the
	// independently locked activity row. After releasing the first blocker,
	// observe the domain wait before releasing the second blocker.
	waitForActivityJournalPostgresBlocker(t, ctx, fixture.db, globalPID, "SELECT last_sequence FROM author_activity_order", done)
	assertActivityJournalStarted(t, fixture, request.RequestEventID)
	if err := global.Rollback(); err != nil {
		t.Fatal(err)
	}
	waitForActivityJournalPostgresBlocker(t, ctx, fixture.db, domainPID, "UPDATE activity_attempts", done)
	assertActivityJournalStarted(t, fixture, request.RequestEventID)
	if err := domain.Rollback(); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-done:
		if result.err != nil {
			t.Fatal(result.err)
		}
		return result.record
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		return runtimepipeline.ActivityAttemptRecord{}
	}
}

func waitForActivityJournalPostgresBlocker(t *testing.T, ctx context.Context, db *sql.DB, blockerPID int, queryPrefix string, done <-chan activityJournalCompletion) {
	t.Helper()
	for {
		var blocked bool
		err := db.QueryRowContext(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity a
			WHERE a.wait_event_type = 'Lock'
			  AND $1 = ANY(pg_blocking_pids(a.pid))
			  AND a.query LIKE $2
		)`, blockerPID, "%"+queryPrefix+"%").Scan(&blocked)
		if err != nil {
			t.Fatal(err)
		}
		if blocked {
			return
		}
		select {
		case result := <-done:
			t.Fatalf("activity owner escaped %q blocker %d: %+v", queryPrefix, blockerPID, result)
		case <-ctx.Done():
			t.Fatalf("waiting for activity owner to block on %q: %v", queryPrefix, ctx.Err())
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func completeActivityBehindSQLiteWriterFence(t *testing.T, fixture authorActivityReceiptFixture, request runtimepipeline.ActivityAttemptRecord) runtimepipeline.ActivityAttemptRecord {
	t.Helper()
	ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 10*time.Second)
	defer cancel()
	global, err := fixture.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer global.Rollback()
	if _, err := global.ExecContext(ctx, `UPDATE author_activity_order SET last_sequence = last_sequence WHERE singleton_id = 1`); err != nil {
		t.Fatal(err)
	}
	done := make(chan activityJournalCompletion, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		record, _, err := activityStoryOuter(fixture, "complete", request)
		done <- activityJournalCompletion{record, err}
	}()
	<-started
	// SQLite has a single writer lock rather than independently observable row
	// locks. Its held ordering write must keep the real owner from committing.
	select {
	case result := <-done:
		t.Fatalf("activity owner crossed held SQLite writer fence: %+v", result)
	case <-time.After(30 * time.Millisecond):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := global.Rollback(); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-done:
		if result.err != nil {
			t.Fatal(result.err)
		}
		return result.record
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		return runtimepipeline.ActivityAttemptRecord{}
	}
}

func activityJournalSequenceHead(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var head int64
	if err := db.QueryRow(`SELECT last_sequence FROM author_activity_order WHERE singleton_id = 1`).Scan(&head); err != nil {
		t.Fatal(err)
	}
	return head
}

func assertActivityJournalStarted(t *testing.T, fixture authorActivityReceiptFixture, requestEventID string) {
	t.Helper()
	record, found, err := fixture.store.(activityStoryJournal).LoadActivityAttempt(testAuthorActivityContext(), requestEventID)
	if err != nil || !found || record.Status != "started" {
		t.Fatalf("activity source before lock release = %#v, found=%v err=%v", record, found, err)
	}
}
