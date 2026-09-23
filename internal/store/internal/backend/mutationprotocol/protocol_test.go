package mutationprotocol

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	runtimelifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateactivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/runhandoff"
	"github.com/google/uuid"
)

func mutationTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE mutation_protocol_probe (value TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	return db
}

func mutationTestRunner(t *testing.T, db *sql.DB, commits ...bool) nativeRunner {
	t.Helper()
	return func(ctx context.Context, write func(context.Context, *sql.Tx) error) (bool, error) {
		for _, commit := range commits {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return false, err
			}
			if err := write(ctx, tx); err != nil {
				_ = tx.Rollback()
				return false, err
			}
			if !commit {
				if err := tx.Rollback(); err != nil {
					return false, err
				}
				continue
			}
			if err := tx.Commit(); err != nil {
				return false, err
			}
			return true, nil
		}
		return false, errors.New("commit acknowledgement lost")
	}
}

func TestAttemptResultUsesOnlyAcknowledgedRetry(t *testing.T) {
	ctx := context.Background()
	db := mutationTestDB(t)
	baseline := NewBaseline()
	var attempts []*Attempt
	result := run(ctx, privateactivity.DialectSQLite, RevisionOnly, Ordinary, baseline, nil,
		mutationTestRunner(t, db, false, true), func(ctx context.Context, attempt *Attempt) (string, error) {
			attempts = append(attempts, attempt)
			value := "rolled-back"
			if len(attempts) == 2 {
				value = "committed"
			}
			err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `INSERT INTO mutation_protocol_probe (value) VALUES (?)`, value)
				return err
			})
			return value, err
		})
	if result.Err() != nil || !result.Acknowledged() {
		t.Fatalf("expected acknowledged retry: %+v", result)
	}
	if value, ok := result.Value(); !ok || value != "committed" {
		t.Fatalf("published wrong attempt result: %q, %v", value, ok)
	}
	var got string
	if err := db.QueryRow(`SELECT value FROM mutation_protocol_probe`).Scan(&got); err != nil || got != "committed" {
		t.Fatalf("wrong durable attempt: %q, %v", got, err)
	}
	for _, attempt := range attempts {
		if err := attempt.WithSQL(ctx, func(context.Context, *sql.Tx) error { return nil }); err == nil {
			t.Fatal("retired attempt retained SQL access")
		}
	}
	if err := baseline.AddWholeFamily("00000000-0000-0000-0000-000000000001", "events"); err == nil {
		t.Fatal("used baseline remained mutable")
	}
}

func TestAcknowledgedResultSurvivesCleanupFailure(t *testing.T) {
	ctx := context.Background()
	db := mutationTestDB(t)
	cleanupErr := errors.New("cleanup failed after commit")
	native := mutationTestRunner(t, db, true)
	result := run(ctx, privateactivity.DialectSQLite, RevisionOnly, Ordinary, NewBaseline(), nil,
		func(ctx context.Context, write func(context.Context, *sql.Tx) error) (bool, error) {
			ack, err := native(ctx, write)
			return ack, errors.Join(err, cleanupErr)
		}, func(context.Context, *Attempt) (int, error) { return 17, nil })
	if !result.Acknowledged() || !errors.Is(result.Err(), cleanupErr) {
		t.Fatalf("lost acknowledged cleanup outcome: %+v", result)
	}
	if value, ok := result.Value(); !ok || value != 17 {
		t.Fatalf("lost acknowledged value: %d, %v", value, ok)
	}
}

func TestMissingAcknowledgementHidesAttemptValue(t *testing.T) {
	ctx := context.Background()
	db := mutationTestDB(t)
	native := mutationTestRunner(t, db, true)
	lost := errors.New("commit acknowledgement lost")
	result := run(ctx, privateactivity.DialectSQLite, RevisionOnly, Ordinary, NewBaseline(), nil,
		func(ctx context.Context, write func(context.Context, *sql.Tx) error) (bool, error) {
			_, err := native(ctx, write)
			return false, errors.Join(err, lost)
		}, func(context.Context, *Attempt) (int, error) { return 19, nil })
	if result.Acknowledged() || !errors.Is(result.Err(), lost) {
		t.Fatalf("invented commit truth: %+v", result)
	}
	if value, ok := result.Value(); ok || value != 0 {
		t.Fatalf("unacknowledged value escaped: %d, %v", value, ok)
	}
}

func TestMissingAcknowledgementWithoutNativeErrorFailsClosed(t *testing.T) {
	result := run(context.Background(), privateactivity.DialectSQLite, RevisionOnly, Ordinary, NewBaseline(), nil,
		func(context.Context, func(context.Context, *sql.Tx) error) (bool, error) {
			return false, nil
		}, func(context.Context, *Attempt) (int, error) { return 21, nil })
	if result.Acknowledged() || result.Err() == nil {
		t.Fatalf("missing commit acknowledgement was accepted: %+v", result)
	}
	if value, ok := result.Value(); ok || value != 0 {
		t.Fatalf("unacknowledged value escaped: %d, %v", value, ok)
	}
}

func TestDestructiveKindRequiresStoryAndExplicitCleanup(t *testing.T) {
	ctx := context.Background()
	called := false
	result := run(ctx, privateactivity.DialectSQLite, RevisionOnly, WholeParentDeletion, NewBaseline(), nil,
		func(context.Context, func(context.Context, *sql.Tx) error) (bool, error) {
			called = true
			return false, nil
		}, func(context.Context, *Attempt) (struct{}, error) { return struct{}{}, nil })
	if result.Err() == nil || called {
		t.Fatal("whole-parent deletion entered an un-fenced transaction")
	}
}

func TestWholeParentDeletionCannotBeSelectedByCaller(t *testing.T) {
	called := false
	result := run(context.Background(), privateactivity.DialectSQLite, Story, WholeParentDeletion, NewBaseline(), nil,
		func(context.Context, func(context.Context, *sql.Tx) error) (bool, error) {
			called = true
			return false, nil
		}, func(context.Context, *Attempt) (struct{}, error) { return struct{}{}, nil })
	if result.Err() == nil || called {
		t.Fatal("caller-selected whole-parent deletion entered transaction")
	}
}

type mismatchedCandidateWriter struct{}

func (mismatchedCandidateWriter) WriteCompletionCandidateTx(ctx context.Context, tx *sql.Tx, _ string, _ *time.Time) (runtimelifecycle.CandidateRequestResult, error) {
	if _, err := tx.ExecContext(ctx, `INSERT INTO mutation_protocol_probe (value) VALUES ('wrong-run')`); err != nil {
		return runtimelifecycle.CandidateRequestResult{}, err
	}
	return runtimelifecycle.CandidateRequestResult{
		Disposition: runtimelifecycle.CandidateRequested,
		Candidate: runtimelifecycle.Candidate{
			RunID: "00000000-0000-0000-0000-000000000002", BundleHash: "bundle",
			Revision: 1, DueAt: time.Now().UTC().Truncate(time.Microsecond),
		},
	}, nil
}

func TestCandidateReservationRejectsDifferentDurableRun(t *testing.T) {
	ctx := context.Background()
	db := mutationTestDB(t)
	result := run(ctx, privateactivity.DialectSQLite, RevisionOnly, Ordinary, NewBaseline(), runhandoff.NewCandidateCoordinator(),
		mutationTestRunner(t, db, true), func(ctx context.Context, attempt *Attempt) (string, error) {
			_, err := attempt.RequestCompletion(ctx, mismatchedCandidateWriter{}, "00000000-0000-0000-0000-000000000001", nil)
			return "must-not-escape", err
		})
	if result.Err() == nil || result.Acknowledged() {
		t.Fatalf("mismatched candidate was admitted: %+v", result)
	}
	if value, ok := result.Value(); ok || value != "" {
		t.Fatalf("failed candidate attempt escaped: %q, %v", value, ok)
	}
	var rows int
	if err := db.QueryRow(`SELECT COUNT(*) FROM mutation_protocol_probe`).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("candidate mismatch persisted request: rows=%d err=%v", rows, err)
	}
}

func TestWholeParentDeletionHasNamedEarlyStoryCut(t *testing.T) {
	ctx := context.Background()
	db := mutationTestDB(t)
	for _, ddl := range []string{
		`CREATE TABLE author_activity_order (singleton_id INTEGER PRIMARY KEY, last_sequence BIGINT NOT NULL)`,
		`CREATE TABLE run_fork_selected_contract_runtime_executions (fork_run_id TEXT NOT NULL)`,
		`CREATE TABLE parents (id TEXT PRIMARY KEY)`,
		`CREATE TABLE children (parent_id TEXT NOT NULL REFERENCES parents(id))`,
		`INSERT INTO parents (id) VALUES ('parent')`,
		`INSERT INTO children (parent_id) VALUES ('parent')`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	runID := uuid.NewString()
	result := run(ctx, privateactivity.DialectSQLite, Story, RetainedForkCleanup, NewBaseline(), nil,
		mutationTestRunner(t, db, true), func(ctx context.Context, attempt *Attempt) (string, error) {
			retained, err := attempt.SelectForkDiscardRetention(ctx, runID)
			if err != nil || retained {
				return "", fmt.Errorf("whole-parent retention=%v err=%v", retained, err)
			}
			if err := attempt.BeginDestructiveCleanup(ctx); err != nil {
				return "", err
			}
			err = attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
				if _, err := tx.ExecContext(ctx, `DELETE FROM children WHERE parent_id='parent'`); err != nil {
					return err
				}
				_, err := tx.ExecContext(ctx, `DELETE FROM parents WHERE id='parent'`)
				return err
			})
			return "deleted", err
		})
	if !result.Acknowledged() || result.Err() != nil {
		t.Fatalf("named whole-parent deletion failed: %+v", result)
	}
	if value, ok := result.Value(); !ok || value != "deleted" {
		t.Fatalf("whole-parent result = %q, %v", value, ok)
	}
	var parents, children, head int
	if err := db.QueryRow(`SELECT COUNT(*) FROM parents`).Scan(&parents); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM children`).Scan(&children); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT last_sequence FROM author_activity_order WHERE singleton_id=1`).Scan(&head); err != nil {
		t.Fatal(err)
	}
	if parents != 0 || children != 0 || head != 0 {
		t.Fatalf("wrong destructive cut: parents=%d children=%d head=%d", parents, children, head)
	}
}

func TestForkDiscardKindFollowsDurableRetentionEvidence(t *testing.T) {
	ctx := context.Background()
	db := mutationTestDB(t)
	for _, ddl := range []string{
		`CREATE TABLE author_activity_order (singleton_id INTEGER PRIMARY KEY, last_sequence BIGINT NOT NULL)`,
		`CREATE TABLE run_fork_selected_contract_runtime_executions (fork_run_id TEXT NOT NULL)`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	deletedRunID, retainedRunID := uuid.NewString(), uuid.NewString()
	if _, err := db.Exec(`INSERT INTO run_fork_selected_contract_runtime_executions (fork_run_id) VALUES (?)`, retainedRunID); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		runID    string
		retained bool
		kind     Kind
	}{
		{deletedRunID, false, WholeParentDeletion},
		{retainedRunID, true, RetainedForkCleanup},
	} {
		result := run(ctx, privateactivity.DialectSQLite, Story, RetainedForkCleanup, NewBaseline(), nil,
			mutationTestRunner(t, db, true), func(ctx context.Context, attempt *Attempt) (Kind, error) {
				if !tc.retained {
					if err := attempt.AddFact(tc.runID, "events", uuid.NewString()); err != nil {
						return 0, err
					}
				}
				retained, err := attempt.SelectForkDiscardRetention(ctx, tc.runID)
				if err != nil || retained != tc.retained {
					return 0, fmt.Errorf("retention=%v err=%v, want %v", retained, err, tc.retained)
				}
				if err := attempt.BeginDestructiveCleanup(ctx); err != nil {
					return 0, err
				}
				if attempt.kind != tc.kind {
					return 0, fmt.Errorf("kind=%d, want %d", attempt.kind, tc.kind)
				}
				if tc.kind == WholeParentDeletion {
					if err := attempt.AddFact(tc.runID, "events", uuid.NewString()); err == nil {
						return 0, errors.New("whole-parent deletion accepted a new revision fact")
					}
				}
				return attempt.kind, nil
			})
		if result.Err() != nil || !result.Acknowledged() {
			t.Fatalf("selected fork discard kind failed: %+v", result)
		}
	}
}
