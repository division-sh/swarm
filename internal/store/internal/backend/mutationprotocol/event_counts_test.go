package mutationprotocol

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	privateactivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/google/uuid"
)

func counterProbeSchema(t *testing.T, db *sql.DB, runID string) {
	t.Helper()
	for _, ddl := range []string{
		`CREATE TABLE runs (run_id UUID PRIMARY KEY)`,
		`CREATE TABLE run_event_counts (run_id UUID PRIMARY KEY REFERENCES runs(run_id) ON DELETE CASCADE, event_count BIGINT NOT NULL DEFAULT 0 CHECK (event_count >= 0))`,
		`CREATE TABLE counter_event_probe (event_id TEXT PRIMARY KEY, run_id TEXT)`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT INTO runs (run_id) VALUES ($1)`, runID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO run_event_counts (run_id) VALUES ($1)`, runID); err != nil {
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
	if err := db.QueryRow(`SELECT event_count, (SELECT COUNT(*) FROM counter_event_probe WHERE run_id = $1) FROM run_event_counts WHERE CAST(run_id AS TEXT) = $1`, runID).Scan(&counter, &physical); err != nil {
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
		readID := runID
		if dialect == privateactivity.DialectPostgres {
			readID = strings.ToUpper(runID)
		}
		native := faultMatrixNative(db, nil, dialect)
		rollback := errors.New("rollback after counter visibility boundary")
		result := run(context.Background(), dialect, RevisionOnly, Ordinary, nil, nil, native, func(ctx context.Context, attempt *Attempt) (string, error) {
			if err := counterProbeInsert(ctx, attempt, runID, eventID); err != nil {
				return "", err
			}
			if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
				var count int
				if err := tx.QueryRowContext(ctx, `SELECT event_count FROM run_event_counts WHERE run_id=$1`, readID).Scan(&count); err != nil {
					return err
				}
				count, err := EventCountForRead(ctx, tx, readID, count)
				if err != nil {
					return err
				}
				if count != 1 {
					t.Fatalf("in-transaction snapshot saw %d, want 1", count)
				}
				var stored int
				if err := tx.QueryRowContext(ctx, `SELECT event_count FROM run_event_counts WHERE run_id=$1`, readID).Scan(&stored); err != nil {
					return err
				}
				if stored != 0 {
					t.Errorf("same-attempt read acquired the counter before finalization: stored=%d, want 0", stored)
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
			return eventID, counterProbeInsert(ctx, attempt, readID, eventID)
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
	if _, err := EventCountForRead(ctx, new(sql.Tx), first, 0); err == nil {
		t.Fatal("foreign transaction borrowed pending projection")
	}
	ordered := []string{first, second}
	slices.Sort(ordered)
	for _, runID := range ordered {
		amount := int64(32)
		if runID == second {
			amount = 2
		}
		mock.ExpectExec(`UPDATE run_event_counts SET event_count = event_count \+`).WithArgs(amount, runID).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	if err := attempt.FlushEventCounts(ctx); err != nil {
		t.Fatal(err)
	}
	if err := attempt.FlushEventCounts(ctx); err != nil {
		t.Fatal(err)
	}
	if err := attempt.AddEventCountDelta(first, 1); err != nil {
		t.Fatal(err)
	}
	if count, err := EventCountForRead(ctx, tx, first, 32); err != nil || count != 33 {
		t.Fatalf("pending count=%d err=%v, want 33", count, err)
	}
	attempt.active = false
	if _, err := EventCountForRead(ctx, tx, first, 32); err == nil {
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

func TestEventCountDeltasPhysicalIdentityAndOrder(t *testing.T) {
	runAdmissionDialects(t, func(t *testing.T, dialect privateactivity.Dialect) {
		const first = "aabbccdd-eeff-0011-2233-445566778899"
		const second = "00000000-0000-0000-0000-000000000001"
		aliases := runIdentityAliases(first)
		db, mock := runAdmissionMockDB(t)
		mock.ExpectBegin()
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			mock.ExpectRollback()
			if err := tx.Rollback(); err != nil {
				t.Error(err)
			}
		}()
		attempt := &Attempt{tx: tx, dialect: dialect, active: true, kind: Ordinary}
		ctx := context.WithValue(context.Background(), sqlAttemptKey{}, attempt)
		// Both additions and retained deletions use the same physical identity.
		for i, alias := range aliases {
			if err := attempt.AddEventCountDelta(" "+alias+" ", 2); err != nil {
				t.Fatal(err)
			}
			if i == 0 {
				if err := attempt.AddEventCountDelta(second, 3); err != nil {
					t.Fatal(err)
				}
			}
		}
		for _, alias := range aliases {
			if err := attempt.AddEventCountDelta(alias, -1); err != nil {
				t.Fatal(err)
			}
		}
		order := append([]string{aliases[0], second}, aliases[1:]...)
		if dialect == privateactivity.DialectPostgres {
			order = order[:2]
		}
		if !slices.Equal(attempt.eventCountOrder, order) || len(attempt.eventCounts) != len(order) {
			t.Fatalf("projection order=%v keys=%v, want order=%v", attempt.eventCountOrder, attempt.eventCounts, order)
		}
		ordered := slices.Clone(order)
		slices.SortFunc(ordered, func(a, b string) int {
			left, _ := physicalRunKey(dialect, a)
			right, _ := physicalRunKey(dialect, b)
			return strings.Compare(left, right)
		})
		for _, runID := range ordered {
			delta := int64(1)
			if runID == second {
				delta = 3
			} else if dialect == privateactivity.DialectPostgres {
				delta = int64(len(aliases))
			}
			if dialect == privateactivity.DialectPostgres {
				mock.ExpectExec(`UPDATE run_event_counts SET event_count = event_count \+`).WithArgs(delta, runID).WillReturnResult(sqlmock.NewResult(0, 1))
			} else {
				mock.ExpectExec(`UPDATE run_event_counts SET event_count = event_count \+`).WithArgs(delta, runID, delta).WillReturnResult(sqlmock.NewResult(0, 1))
			}
		}
		if err := attempt.FlushEventCounts(ctx); err != nil {
			t.Fatal(err)
		}
		if err := attempt.FlushEventCounts(ctx); err != nil {
			t.Fatal(err)
		}
		if err := attempt.AddEventCountDelta(first, 2); err != nil {
			t.Fatal(err)
		}
		if err := attempt.AddEventCountDelta(aliases[2], -1); err != nil {
			t.Fatal(err)
		}
		want := 5
		if dialect == privateactivity.DialectPostgres {
			want++
		}
		if count, err := EventCountForRead(ctx, tx, " "+aliases[3]+" ", 5); err != nil || count != want {
			t.Fatalf("alias overlay count=%d err=%v, want %d", count, err, want)
		}
		for _, runID := range ordered {
			key, _ := physicalRunKey(dialect, runID)
			delta := attempt.eventCounts[key]
			if delta == 0 {
				continue
			}
			update := mock.ExpectExec(`UPDATE run_event_counts SET event_count = event_count \+`)
			if dialect == privateactivity.DialectPostgres {
				update.WithArgs(delta, runID)
			} else {
				update.WithArgs(delta, runID, delta)
			}
			update.WillReturnResult(sqlmock.NewResult(0, 1))
		}
		if err := attempt.FlushEventCounts(ctx); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(attempt.eventCountOrder, order) {
			t.Fatalf("later contribution changed order: %v", attempt.eventCountOrder)
		}
	})
}

func TestEventCountDeltasUnrecognizedIdentityPreservesSQLValidation(t *testing.T) {
	runAdmissionDialects(t, func(t *testing.T, dialect privateactivity.Dialect) {
		db, mock := runAdmissionMockDB(t)
		mock.ExpectBegin()
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			mock.ExpectRollback()
			if err := tx.Rollback(); err != nil {
				t.Error(err)
			}
		}()
		attempt := &Attempt{tx: tx, dialect: dialect, active: true, kind: Ordinary}
		if err := attempt.AddEventCountDelta(" run ", 1); err != nil {
			t.Fatal(err)
		}
		ctx := context.WithValue(context.Background(), sqlAttemptKey{}, attempt)
		var want error
		if dialect == privateactivity.DialectPostgres {
			want = errors.New("native invalid UUID rejection")
			mock.ExpectExec(`UPDATE run_event_counts SET event_count = event_count \+`).WithArgs(int64(1), "run").WillReturnError(want)
		} else {
			mock.ExpectExec(`UPDATE run_event_counts SET event_count = event_count \+`).WithArgs(int64(1), "run", int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
		}
		if err := attempt.FlushEventCounts(ctx); !errors.Is(err, want) {
			t.Fatalf("SQL validation err=%v, want %v", err, want)
		}
		if dialect == privateactivity.DialectPostgres && attempt.eventCounts["run"] != 1 {
			t.Fatal("failed SQL discarded the pending delta")
		}
	})
}
