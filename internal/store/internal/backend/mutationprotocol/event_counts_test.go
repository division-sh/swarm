package mutationprotocol

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	privateactivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/google/uuid"
)

func counterProbeSchema(t *testing.T, db *sql.DB, runID string) {
	t.Helper()
	for _, ddl := range []string{
		`CREATE TABLE runs (run_id UUID PRIMARY KEY, event_count INTEGER NOT NULL DEFAULT 0 CHECK (event_count >= 0))`,
		`CREATE TABLE counter_event_probe (event_id TEXT PRIMARY KEY, run_id TEXT)`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT INTO runs (run_id) VALUES ($1)`, runID); err != nil {
		t.Fatal(err)
	}
}

func counterProbeInsert(ctx context.Context, attempt *Attempt, runID, eventID string) error {
	var rows int64
	if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `INSERT INTO counter_event_probe (event_id, run_id) VALUES ($1, $2) ON CONFLICT(event_id) DO NOTHING`, eventID, runID)
		if err != nil {
			return err
		}
		rows, err = result.RowsAffected()
		return err
	}); err != nil {
		return err
	}
	return attempt.AddEventCountDelta(runID, rows)
}

func counterProbeRead(t *testing.T, db *sql.DB, runID string, want int) {
	t.Helper()
	var counter, physical int
	if err := db.QueryRow(`SELECT event_count, (SELECT COUNT(*) FROM counter_event_probe WHERE run_id = $1) FROM runs WHERE CAST(run_id AS TEXT) = $1`, runID).Scan(&counter, &physical); err != nil {
		t.Fatal(err)
	}
	if counter != want || physical != want {
		t.Fatalf("event projection counter=%d physical=%d, want %d", counter, physical, want)
	}
}

func TestEventCountDeltasRollbackAndAcknowledgmentBothStores(t *testing.T) {
	faultMatrixStores(t, func(t *testing.T, db *sql.DB, dialect privateactivity.Dialect) {
		runID, eventID := uuid.NewString(), uuid.NewString()
		counterProbeSchema(t, db, runID)
		native := faultMatrixNative(db, nil, dialect)
		rollback := errors.New("rollback after counter visibility boundary")
		result := run(context.Background(), dialect, RevisionOnly, Ordinary, nil, nil, native, func(ctx context.Context, attempt *Attempt) (string, error) {
			if err := counterProbeInsert(ctx, attempt, runID, eventID); err != nil {
				return "", err
			}
			if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
				if err := FlushEventCountBeforeRead(ctx, tx, runID); err != nil {
					return err
				}
				var count int
				if err := tx.QueryRowContext(ctx, `SELECT event_count FROM runs WHERE run_id=$1`, runID).Scan(&count); err != nil {
					return err
				}
				if count != 1 {
					t.Fatalf("in-transaction snapshot saw %d, want 1", count)
				}
				return nil
			}); err != nil {
				return "", err
			}
			return "", rollback
		})
		if result.Acknowledged() || !errors.Is(result.Err(), rollback) {
			t.Fatalf("rollback result = ack:%v err:%v", result.Acknowledged(), result.Err())
		}
		counterProbeRead(t, db, runID, 0)

		lost := errors.New("native commit acknowledgment lost")
		result = run(context.Background(), dialect, RevisionOnly, Ordinary, nil, nil, func(ctx context.Context, write func(context.Context, *sql.Tx) error) (bool, error) {
			ack, err := native(ctx, write)
			if !ack || err != nil {
				return ack, err
			}
			return false, lost
		}, func(ctx context.Context, attempt *Attempt) (string, error) {
			return eventID, counterProbeInsert(ctx, attempt, runID, eventID)
		})
		if result.Acknowledged() || !errors.Is(result.Err(), lost) {
			t.Fatalf("uncertain commit exposed success: ack:%v err:%v", result.Acknowledged(), result.Err())
		}
		counterProbeRead(t, db, runID, 1)
		result = run(context.Background(), dialect, RevisionOnly, Ordinary, nil, nil, native, func(ctx context.Context, attempt *Attempt) (string, error) {
			return eventID, counterProbeInsert(ctx, attempt, runID, eventID)
		})
		if !result.Acknowledged() || result.Err() != nil {
			t.Fatalf("exact duplicate replay: ack:%v err:%v", result.Acknowledged(), result.Err())
		}
		counterProbeRead(t, db, runID, 1)
	})
}

func TestEventCountDeltasIndependentPublishersBothStores(t *testing.T) {
	faultMatrixStores(t, func(t *testing.T, db *sql.DB, dialect privateactivity.Dialect) {
		runID := uuid.NewString()
		counterProbeSchema(t, db, runID)
		native := faultMatrixNative(db, nil, dialect)
		results := make(chan error, 2)
		for range 2 {
			go func() {
				result := run(context.Background(), dialect, RevisionOnly, Ordinary, nil, nil, native, func(ctx context.Context, attempt *Attempt) (struct{}, error) {
					for range 20 {
						if err := counterProbeInsert(ctx, attempt, runID, uuid.NewString()); err != nil {
							return struct{}{}, err
						}
					}
					return struct{}{}, nil
				})
				results <- result.Err()
			}()
		}
		for range 2 {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		counterProbeRead(t, db, runID, 40)
	})
}

func TestEventCountDeltasBatchOrderAndForeignReadRefusal(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	first, second := uuid.NewString(), uuid.NewString()
	attempt := &Attempt{tx: tx, dialect: privateactivity.DialectPostgres, active: true, kind: Ordinary}
	for range 32 {
		if err := attempt.AddEventCountDelta(first, 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := attempt.AddEventCountDelta(second, 2); err != nil {
		t.Fatal(err)
	}
	if err := attempt.AddEventCountDelta("", 1); err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(context.Background(), sqlAttemptKey{}, attempt)
	if err := FlushEventCountBeforeRead(ctx, new(sql.Tx), first); err == nil {
		t.Fatal("foreign transaction borrowed pending projection")
	}
	mock.ExpectExec(`UPDATE runs SET event_count = event_count \+`).WithArgs(int64(32), first).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE runs SET event_count = event_count \+`).WithArgs(int64(2), second).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := attempt.FlushEventCounts(ctx); err != nil {
		t.Fatal(err)
	}
	if err := attempt.FlushEventCounts(ctx); err != nil {
		t.Fatal(err)
	}
	if err := attempt.AddEventCountDelta(first, 1); err != nil {
		t.Fatal(err)
	}
	mock.ExpectExec(`UPDATE runs SET event_count = event_count \+`).WithArgs(int64(1), first).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := FlushEventCountBeforeRead(ctx, tx, first); err != nil {
		t.Fatal(err)
	}
	attempt.active = false
	if err := FlushEventCountBeforeRead(ctx, tx, first); err == nil {
		t.Fatal("ended attempt retained projection authority")
	}
	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
