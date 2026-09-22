package mutationprotocol

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	runtimelifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateactivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/runhandoff"
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
