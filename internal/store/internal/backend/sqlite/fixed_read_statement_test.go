package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	driversqlite "modernc.org/sqlite"
)

func TestFixedReadStatementConcurrentPreparationAndClose(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	const query = "SELECT ?"
	closeFault := errors.New("database close fault")
	mock.ExpectPrepare(query).WillDelayFor(20 * time.Millisecond).WillBeClosed()
	var slot FixedReadStatement
	var wg sync.WaitGroup
	statements := make([]*sql.Stmt, 16)
	for i := range statements {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stmt, err := slot.PrepareContext(context.Background(), b, query)
			if err != nil {
				t.Errorf("prepare: %v", err)
				return
			}
			statements[i] = stmt
		}()
	}
	wg.Wait()
	for _, stmt := range statements {
		if stmt == nil || stmt != statements[0] {
			t.Fatal("concurrent first calls did not share one handle")
		}
	}
	if len(b.readStatements.owned) != 1 {
		t.Fatalf("owned handles=%d", len(b.readStatements.owned))
	}
	for _, n := range []int{1, 2} {
		mock.ExpectQuery(query).WithArgs(n).WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(n)).RowsWillBeClosed()
		rows, err := slot.QueryContext(context.Background(), b, query, n)
		if err != nil {
			t.Fatal(err)
		}
		if err := expectDirectQueryRow(rows, n); err != nil {
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := slot.PrepareContext(context.Background(), b, "SELECT 2"); err == nil {
		t.Fatal("fixed SQL could be replaced")
	}
	mock.ExpectClose().WillReturnError(closeFault)
	if err := b.Close(); !errors.Is(err, closeFault) {
		t.Fatalf("lost close error: %v", err)
	}
	if _, err := slot.QueryContext(context.Background(), b, query, 3); err == nil {
		t.Fatal("closed handle reopened")
	}
	var unused FixedReadStatement
	if _, err := unused.QueryContext(context.Background(), b, query, 4); err == nil {
		t.Fatal("closed pool prepared an unused slot")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func waitFixedReadPreparation(t *testing.T, slot *FixedReadStatement) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		slot.mu.Lock()
		pending := slot.preparing != nil
		slot.mu.Unlock()
		if pending {
			return
		}
		select {
		case <-deadline:
			t.Fatal("preparation did not start")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestFixedReadStatementCanceledPreparationDoesNotPoison(t *testing.T) {
	b := newTransactionTestBackend(t, filepath.Join(t.TempDir(), "fixed-cancel.db"))
	b.db.SetMaxOpenConns(1)
	ctx := context.Background()
	held, err := b.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	var slot FixedReadStatement
	first, cancelFirst := context.WithCancel(ctx)
	defer cancelFirst()
	done := make(chan error, 1)
	go func() { _, err := slot.PrepareContext(first, b, "SELECT ?"); done <- err }()
	waitFixedReadPreparation(t, &slot)
	// A waiting caller must be able to cancel independently of the first one.
	waiter, cancelWaiter := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancelWaiter()
	if _, err := slot.PrepareContext(waiter, b, "SELECT ?"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting cancellation: %v", err)
	}
	cancelFirst()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("first cancellation: %v", err)
	}
	if slot.stmt != nil || len(b.readStatements.owned) != 0 {
		t.Fatal("failed preparation retained a handle")
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	rows, err := slot.QueryContext(ctx, b, "SELECT ?", 9)
	if err != nil {
		t.Fatal(err)
	}
	if err := expectDirectQueryRow(rows, 9); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFixedReadStatementFreshRowsSchemaAndConcurrentBindings(t *testing.T) {
	b := newTransactionTestBackend(t, filepath.Join(t.TempDir(), "fixed-fresh.db"))
	b.db.SetMaxOpenConns(4)
	ctx := context.Background()
	var slot FixedReadStatement
	const query = "SELECT value FROM samples WHERE id = ?"
	if _, err := slot.QueryContext(ctx, b, query, 1); err == nil {
		t.Fatal("missing table admitted")
	}
	for _, ddl := range []string{"CREATE TABLE samples (id INTEGER PRIMARY KEY, value INTEGER)", "INSERT INTO samples VALUES (1,11),(2,22)"} {
		if _, err := b.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	read := func(id, want int) error {
		rows, err := slot.QueryContext(ctx, b, query, id)
		if err != nil {
			return err
		}
		defer rows.Close()
		return expectDirectQueryRow(rows, want)
	}
	if err := read(1, 11); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Exec("UPDATE samples SET value=33 WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	if err := read(1, 33); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, want := 1, 33
			if i%2 != 0 {
				id, want = 2, 22
			}
			if err := read(id, want); err != nil {
				t.Errorf("concurrent read: %v", err)
			}
		}()
	}
	wg.Wait()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := slot.QueryContext(canceled, b, query, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled execution: %v", err)
	}
	if err := read(2, 22); err != nil {
		t.Fatal(err)
	}
	for _, ddl := range []string{"ALTER TABLE samples RENAME COLUMN value TO changed", "DROP TABLE samples"} {
		if _, err := b.Exec(ddl); err != nil {
			t.Fatal(err)
		}
		rows, err := slot.QueryContext(ctx, b, query, 1)
		if rows != nil {
			rows.Close()
		}
		if err == nil {
			t.Fatalf("stale schema accepted after %s", ddl)
		}
	}
	for _, ddl := range []string{"CREATE TABLE samples (id INTEGER PRIMARY KEY, value INTEGER)", "INSERT INTO samples VALUES (1,44)"} {
		if _, err := b.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	if err := read(1, 44); err != nil {
		t.Fatal(err)
	}
	if err := b.db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := read(1, 44); err == nil {
		t.Fatal("direct DB.Close reopened statement")
	}
}

func TestFixedReadStatementCloseDuringPreparation(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	mock.ExpectPrepare("SELECT 1").WillDelayFor(30 * time.Millisecond).WillBeClosed()
	mock.ExpectClose()
	var slot FixedReadStatement
	done := make(chan error, 1)
	go func() { _, err := slot.PrepareContext(context.Background(), b, "SELECT 1"); done <- err }()
	// Wait for the mock to consume Prepare, rather than just enter the slot.
	deadline := time.Now().Add(time.Second)
	for db.Stats().InUse == 0 {
		if time.Now().After(deadline) {
			t.Fatal("prepare did not acquire connection")
		}
		time.Sleep(time.Millisecond)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil {
		t.Fatal("preparation published after close")
	}
	if len(b.readStatements.owned) != 0 || slot.stmt != nil {
		t.Fatal("closed pool retained new statement")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(fmt.Errorf("close/prepare lifecycle: %w", err))
	}
}

func TestFixedReadStatementCancellationDuringExecutionThenReuse(t *testing.T) {
	var failures []error
	for _, prepared := range []bool{false, true} {
		t.Run(fmt.Sprintf("prepared=%t", prepared), func(t *testing.T) {
			started := make(chan struct{})
			resume := make(chan struct{})
			var release sync.Once
			unblock := func() { release.Do(func() { close(resume) }) }
			defer unblock()
			name := "fixed_read_execution_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			if err := driversqlite.RegisterScalarFunction(name, 1, func(_ *driversqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
				if args[0] == int64(1) {
					close(started)
					<-resume
				}
				return int64(7), nil
			}); err != nil {
				t.Fatal(err)
			}
			b := newTransactionTestBackend(t, filepath.Join(t.TempDir(), "executing.db"))
			b.db.SetMaxOpenConns(1)
			// The long canceled branch cannot finish before the driver's
			// interrupt goroutine observes cancellation after the callback.
			query := "WITH RECURSIVE work(n) AS (VALUES(" + name + "(?)) UNION ALL SELECT n+1 FROM work WHERE n < ?) SELECT sum(n) FROM work"
			var slot FixedReadStatement
			read := func(ctx context.Context, block int) (int, error) {
				var rows *sql.Rows
				var err error
				limit := 7
				if block != 0 {
					limit = 1000000000
				}
				if prepared {
					rows, err = slot.QueryContext(ctx, b, query, block, limit)
				} else {
					rows, err = b.QueryContext(ctx, query, block, limit)
				}
				if err != nil {
					return 0, err
				}
				defer rows.Close()
				if !rows.Next() {
					return 0, rows.Err()
				}
				var got int
				if err := rows.Scan(&got); err != nil {
					return 0, err
				}
				return got, rows.Close()
			}
			if got, err := read(context.Background(), 0); err != nil || got != 7 {
				t.Fatalf("warm read=%d: %v", got, err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := read(ctx, 1); done <- err }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("native execution did not start")
			}
			cancel()
			unblock()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("during-execution cancellation: %v", err)
				}
				failures = append(failures, err)
			case <-time.After(time.Second):
				t.Fatal("native execution did not stop")
			}
			for range 2 {
				if got, err := read(context.Background(), 0); err != nil || got != 7 {
					t.Fatalf("reused read=%d: %v", got, err)
				}
			}
		})
	}
	if len(failures) != 2 || fmt.Sprint(failures[0]) != fmt.Sprint(failures[1]) || reflect.TypeOf(failures[0]) != reflect.TypeOf(failures[1]) {
		t.Fatalf("raw/prepared cancellation errors differ: %v", failures)
	}
}

func TestFixedReadStatementConnectionRetirementAndReplacement(t *testing.T) {
	b := newTransactionTestBackend(t, filepath.Join(t.TempDir(), "replacement.db"))
	b.db.SetMaxOpenConns(1)
	ctx := context.Background()
	for _, sql := range []string{"CREATE TABLE replacement (value INTEGER)", "INSERT INTO replacement VALUES (11)"} {
		if _, err := b.ExecContext(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	var slot FixedReadStatement
	const query = "SELECT value FROM replacement"
	read := func(want int) {
		t.Helper()
		rows, err := slot.QueryContext(ctx, b, query)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		if err := expectDirectQueryRow(rows, want); err != nil {
			t.Fatal(err)
		}
	}
	read(11)
	retained := slot.stmt
	conn, err := b.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var retired any
	if err := conn.Raw(func(native any) error { retired = native; return driver.ErrBadConn }); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("retire connection: %v", err)
	}
	_ = conn.Close()
	replacement, err := b.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if err := replacement.Raw(func(native any) error {
		if native == retired {
			return errors.New("retired native connection was reused")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := replacement.ExecContext(ctx, "UPDATE replacement SET value=22"); err != nil {
		t.Fatal(err)
	}
	if err := replacement.Close(); err != nil {
		t.Fatal(err)
	}
	read(22)
	read(22)
	if slot.stmt != retained || len(b.readStatements.owned) != 1 {
		t.Fatal("connection replacement changed the fixed slot or registered another DB statement")
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := slot.QueryContext(ctx, b, query); err == nil {
		t.Fatal("closed pool reopened after connection replacement")
	}
}
