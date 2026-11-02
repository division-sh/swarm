package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"

	modernc "modernc.org/sqlite"
)

// Invariant reach: real interrupted DML retires only its exact transaction;
// physical rollback, independent errors and subsequent transactions stay owned.
func proveNativeRollbackWitness(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		for _, callbackError := range []bool{false, true} {
			t.Run(fmt.Sprintf("interrupted_dml/prepared=%t/callback_error=%t", prepared, callbackError), func(t *testing.T) {
				proveInterruptedNativeRollback(t, prepared, callbackError)
			})
		}
	}
	t.Run("epoch_hostiles", proveRollbackEpochHostiles)
	t.Run("later_native_work", proveRollbackLaterNativeWork)
	t.Run("native_interfaces", proveRollbackNativeInterfaces)
	t.Run("foreign_and_retired_connections", proveRollbackConnectionIsolation)
	t.Run("independent_exit_errors", proveRollbackExitErrors)
}

type rollbackWitnessSpy struct {
	nativeRollbackConnection
	path               string
	rollbacks, commits atomic.Int32
	rollbackErr        error
	commitErr          error
	closeErr           error
	afterCommit        context.CancelFunc
}

func (s *rollbackWitnessSpy) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := s.nativeRollbackConnection.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &rollbackWitnessSpyTx{Tx: tx, spy: s}, nil
}

func (s *rollbackWitnessSpy) Close() error {
	return errors.Join(s.nativeRollbackConnection.Close(), s.closeErr)
}

type rollbackWitnessSpyTx struct {
	driver.Tx
	spy *rollbackWitnessSpy
}

func (t *rollbackWitnessSpyTx) Rollback() error {
	t.spy.rollbacks.Add(1)
	return errors.Join(t.Tx.Rollback(), t.spy.rollbackErr)
}

func (t *rollbackWitnessSpyTx) Commit() error {
	t.spy.commits.Add(1)
	err := t.Tx.Commit()
	if t.spy.afterCommit != nil {
		t.spy.afterCommit()
	}
	return errors.Join(err, t.spy.commitErr)
}

func openRollbackWitnessTest(t *testing.T) (*sql.DB, *rollbackConnection, *rollbackWitnessSpy) {
	t.Helper()
	path := "file:" + filepath.Join(t.TempDir(), "native.db")
	db, err := OpenDatabase(path + "?_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var owned *rollbackConnection
	var spy *rollbackWitnessSpy
	if err := conn.Raw(func(raw any) error {
		var ok bool
		owned, ok = raw.(*rollbackConnection)
		if !ok {
			return fmt.Errorf("constructor bypassed rollback ownership: %T", raw)
		}
		spy = &rollbackWitnessSpy{nativeRollbackConnection: owned.native, path: path}
		owned.native = spy
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	return db, owned, spy
}

func proveInterruptedNativeRollback(t *testing.T, prepared, callbackError bool) {
	db, owned, spy := openRollbackWitnessTest(t)
	if _, err := db.Exec(`CREATE TABLE saved(value INTEGER); INSERT INTO saved VALUES(0);
		CREATE TABLE counter(value INTEGER);
		WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000) INSERT INTO counter SELECT 0 FROM n`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var arrived atomic.Bool
	var captured atomic.Pointer[rollbackEpoch]
	spy.RegisterPreUpdateHook(func(data modernc.SQLitePreUpdateData) {
		if data.TableName == "counter" && arrived.CompareAndSwap(false, true) {
			captured.Store(owned.active.Load())
			cancel()
			runtime.Gosched()
		}
	})
	b := originalTransactionTestBackend(t, db, spy.path)
	primary := errors.New("independent interrupted callback error")
	err := b.RunTransaction(ctx, "interrupted native write", func(ctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, "UPDATE saved SET value=17"); err != nil {
			return err
		}
		var statementErr error
		if prepared {
			stmt, err := tx.PrepareContext(ctx, "UPDATE counter SET value=value+1")
			if err != nil {
				return err
			}
			_, statementErr = stmt.ExecContext(ctx)
			statementErr = errors.Join(statementErr, stmt.Close())
		} else {
			_, statementErr = tx.ExecContext(ctx, "UPDATE counter SET value=value+1")
		}
		if callbackError {
			return errors.Join(statementErr, primary)
		}
		return statementErr
	})
	epoch := captured.Load()
	if !arrived.Load() || epoch == nil || !epoch.aborted.Load() || !epoch.retired.Load() || owned.active.Load() != nil {
		t.Fatalf("missing exact interrupted transaction retirement: arrived=%t epoch=%+v active=%p", arrived.Load(), epoch, owned.active.Load())
	}
	if spy.rollbacks.Load() != 0 || spy.commits.Load() != 0 {
		t.Fatalf("automatic abort followed by redundant native work: rollbacks=%d commits=%d", spy.rollbacks.Load(), spy.commits.Load())
	}
	if !errors.Is(err, context.Canceled) || (callbackError && !errors.Is(err, primary)) {
		t.Fatalf("lost cancellation/callback cause: %v", err)
	}
	if before, clean := CleanCancellationBeforeCommit(err); before || clean == callbackError {
		t.Fatalf("wrong cancellation evidence: before=%t clean=%t err=%v", before, clean, err)
	}
	var saved, total int
	if err := db.QueryRow("SELECT value, (SELECT SUM(value) FROM counter) FROM saved").Scan(&saved, &total); err != nil {
		t.Fatal(err)
	}
	if saved != 0 || total != 0 {
		t.Fatalf("interrupted DML leaked prior/applied writes: saved=%d total=%d", saved, total)
	}
	// The pinned driver may discard an interrupted file connection. Inspect the
	// actual successor instead of bypassing its validation/disposal decision.
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var successor *rollbackConnection
	if err := conn.Raw(func(raw any) error { successor = raw.(*rollbackConnection); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.RunTransaction(context.Background(), "successor native write", func(ctx context.Context, tx *sql.Tx) error {
		next := successor.active.Load()
		if next == nil || next == epoch || next.aborted.Load() || next.retired.Load() {
			return errors.New("successor borrowed the interrupted epoch")
		}
		_, err := tx.ExecContext(ctx, "UPDATE saved SET value=29")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT value FROM saved").Scan(&saved); err != nil || saved != 29 || successor.active.Load() != nil {
		t.Fatalf("successor did not commit/retire: saved=%d active=%p err=%v", saved, successor.active.Load(), err)
	}
}

type rollbackEpochTestTx struct {
	rollbacks, commits int
	err                error
}

func (t *rollbackEpochTestTx) Rollback() error { t.rollbacks++; return t.err }
func (t *rollbackEpochTestTx) Commit() error   { t.commits++; return t.err }

func proveRollbackEpochHostiles(t *testing.T) {
	for _, name := range []string{"missing", "foreign", "retired", "prior_epoch", "commit_attempted", "ordinary", "later_native_work"} {
		t.Run(name, func(t *testing.T) {
			owner := &rollbackConnection{}
			epoch := &rollbackEpoch{owner: owner}
			epoch.aborted.Store(true)
			owner.active.Store(epoch)
			nativeErr := errors.New("independent native rollback error")
			native := &rollbackEpochTestTx{err: nativeErr}
			tx := &rollbackTransaction{native: native, owner: owner, epoch: epoch}
			switch name {
			case "missing":
				tx.epoch = nil
			case "foreign":
				epoch.owner = &rollbackConnection{}
			case "retired":
				epoch.retired.Store(true)
			case "prior_epoch":
				owner.active.Store(&rollbackEpoch{owner: owner})
			case "commit_attempted":
				epoch.commitAttempted.Store(true)
			case "ordinary":
				epoch.aborted.Store(false)
			case "later_native_work":
				owner.beforeNativeOperation()
			}
			if err := tx.Rollback(); !errors.Is(err, nativeErr) || native.rollbacks != 1 {
				t.Fatalf("%s evidence skipped the native error path: calls=%d err=%v", name, native.rollbacks, err)
			}
			if err := tx.Rollback(); err != sql.ErrTxDone || native.rollbacks != 1 {
				t.Fatalf("retired transaction performed more native work: calls=%d err=%v", native.rollbacks, err)
			}
			if name == "prior_epoch" && owner.active.Load() == nil {
				t.Fatal("old finalizer cleared successor evidence")
			}
		})
	}
}

func proveRollbackLaterNativeWork(t *testing.T) {
	for _, mode := range []string{"exec", "prepared_legacy", "prepared_context", "prepared_query", "prepare_after_abort"} {
		t.Run(mode, func(t *testing.T) {
			db, owned, spy := openRollbackWitnessTest(t)
			if _, err := db.Exec(`CREATE TABLE saved(value INTEGER); INSERT INTO saved VALUES(0);
				CREATE TABLE counter(value INTEGER);
				WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000) INSERT INTO counter SELECT 0 FROM n`); err != nil {
				t.Fatal(err)
			}
			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if err := conn.Raw(func(any) error {
				return interruptThenUseNativeTransaction(owned, spy, mode)
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func interruptThenUseNativeTransaction(owned *rollbackConnection, spy *rollbackWitnessSpy, mode string) error {
	var later driver.Stmt
	var err error
	if mode != "prepare_after_abort" {
		later, err = owned.PrepareContext(context.Background(), "BEGIN; UPDATE saved SET value=37; SELECT value FROM saved")
		if err != nil {
			return err
		}
		defer later.Close()
	}
	tx, err := owned.BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	epoch := owned.active.Load()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var arrived atomic.Bool
	spy.RegisterPreUpdateHook(func(data modernc.SQLitePreUpdateData) {
		if data.TableName == "counter" && arrived.CompareAndSwap(false, true) {
			cancel()
			runtime.Gosched()
		}
	})
	_, err = owned.ExecContext(ctx, "UPDATE counter SET value=value+1", nil)
	if !errors.Is(err, context.Canceled) || !arrived.Load() || epoch == nil || !epoch.aborted.Load() {
		return fmt.Errorf("later-work control never reached native abort: %v", err)
	}
	switch mode {
	case "exec":
		_, err = owned.ExecContext(context.Background(), "BEGIN; UPDATE saved SET value=37", nil)
	case "prepared_legacy":
		_, err = later.Exec(nil)
	case "prepared_context":
		_, err = later.(driver.StmtExecContext).ExecContext(context.Background(), nil)
	case "prepared_query":
		var rows driver.Rows
		rows, err = later.(driver.StmtQueryContext).QueryContext(context.Background(), nil)
		if err == nil {
			err = rows.Close()
		}
	case "prepare_after_abort":
		var stmt driver.Stmt
		stmt, err = owned.Prepare("SELECT value FROM saved")
		if err == nil {
			err = stmt.Close()
		}
	}
	if err != nil || !epoch.retired.Load() {
		return fmt.Errorf("later native work borrowed abort witness: retired=%t err=%v", epoch.retired.Load(), err)
	}
	err = tx.Rollback()
	if spy.rollbacks.Load() != 1 || (mode == "prepare_after_abort" && err == nil) || (mode != "prepare_after_abort" && err != nil) {
		return fmt.Errorf("later-work native rollback not preserved: calls=%d err=%v", spy.rollbacks.Load(), err)
	}
	return nil
}

func proveRollbackNativeInterfaces(t *testing.T) {
	db, owned, _ := openRollbackWitnessTest(t)
	baseline, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "driver.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer baseline.Close()
	if db.Driver() != baseline.Driver() || !owned.IsValid() {
		t.Fatal("owned connector changed the registered driver or connection validation")
	}
	var enabled int
	if err := db.QueryRow("PRAGMA foreign_keys").Scan(&enabled); err != nil || enabled != 1 {
		t.Fatalf("DSN pragma lost: foreign_keys=%d err=%v", enabled, err)
	}
	if _, err := db.Exec("CREATE TABLE native(value INTEGER); BEGIN; INSERT INTO native VALUES(1); ROLLBACK"); err != nil || owned.active.Load() != nil {
		t.Fatalf("bootstrap rollback fabricated transaction evidence: active=%p err=%v", owned.active.Load(), err)
	}
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	epoch := owned.active.Load()
	stmt, err := tx.PrepareContext(context.Background(), "SELECT COUNT(*) FROM native")
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := stmt.QueryRowContext(context.Background()).Scan(&count); err != nil || count != 0 {
		t.Fatalf("prepared read changed bootstrap facts: count=%d err=%v", count, err)
	}
	if err := stmt.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil || epoch == nil || !epoch.retired.Load() || owned.active.Load() != nil {
		t.Fatalf("ordinary read rollback not retired: epoch=%+v err=%v", epoch, err)
	}
	next, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	fresh := owned.active.Load()
	if fresh == nil || fresh == epoch || fresh.aborted.Load() || fresh.retired.Load() {
		t.Fatalf("healthy same-connection next transaction borrowed prior evidence: %+v", fresh)
	}
	if err := next.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := owned.ExecContext(context.Background(), "BEGIN", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := owned.BeginTx(context.Background(), driver.TxOptions{}); err == nil || owned.active.Load() != nil {
		t.Fatalf("failed native BEGIN fabricated evidence: active=%p err=%v", owned.active.Load(), err)
	}
	if _, err := owned.ExecContext(context.Background(), "ROLLBACK", nil); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "readonly.db")
	writable, err := OpenDatabase("file:" + path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writable.Exec("CREATE TABLE retained(value INTEGER); INSERT INTO retained VALUES(7)"); err != nil {
		t.Fatal(err)
	}
	if err := writable.Close(); err != nil {
		t.Fatal(err)
	}
	readonly, err := OpenDatabase("file:" + path + "?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer readonly.Close()
	if err := readonly.QueryRow("SELECT value FROM retained").Scan(&count); err != nil || count != 7 {
		t.Fatalf("read-only constructor lost data: count=%d err=%v", count, err)
	}
	if _, err := readonly.Exec("UPDATE retained SET value=8"); err == nil {
		t.Fatal("owned connector removed read-only enforcement")
	}
}

func proveRollbackConnectionIsolation(t *testing.T) {
	_, first, firstSpy := openRollbackWitnessTest(t)
	_, second, _ := openRollbackWitnessTest(t)
	firstTx, err := first.BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer firstTx.Rollback()
	firstEpoch := first.active.Load()
	secondTx, err := second.BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer secondTx.Rollback()
	secondEpoch := second.active.Load()
	// Complete the real second native transaction without retiring its wrapper,
	// then test that neither its hook nor its pool epoch authorizes the first.
	if err := secondTx.(*rollbackTransaction).native.Rollback(); err != nil {
		t.Fatal(err)
	}
	if firstEpoch == nil || firstEpoch.aborted.Load() || secondEpoch == nil || !secondEpoch.aborted.Load() {
		t.Fatal("foreign connection rollback crossed transaction evidence")
	}
	if err := second.ResetSession(context.Background()); err != nil || !secondEpoch.retired.Load() || second.active.Load() != nil {
		t.Fatalf("pool reset kept an old witness: active=%p err=%v", second.active.Load(), err)
	}
	if err := secondTx.Rollback(); err == nil {
		t.Fatal("reset witness suppressed the ordinary native rollback error")
	}
	if err := firstTx.Rollback(); err != nil || firstSpy.rollbacks.Load() != 1 || !firstEpoch.retired.Load() {
		t.Fatalf("foreign abort skipped first connection rollback: calls=%d err=%v", firstSpy.rollbacks.Load(), err)
	}
	_, err = second.BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	thirdEpoch := second.active.Load()
	if thirdEpoch == nil || thirdEpoch == secondEpoch || thirdEpoch.aborted.Load() {
		t.Fatal("same connection reused a retired witness")
	}
	if err := second.Close(); err != nil || second.active.Load() != nil || !thirdEpoch.retired.Load() {
		t.Fatalf("connection disposal retained witness: active=%p err=%v", second.active.Load(), err)
	}
	if second.IsValid() {
		t.Fatal("disposed physical connection remained valid")
	}
}

func proveRollbackExitErrors(t *testing.T) {
	for _, name := range []string{"rollback", "close", "commit_response", "failed_commit", "acknowledged_commit"} {
		t.Run(name, func(t *testing.T) {
			db, owned, spy := openRollbackWitnessTest(t)
			if _, err := db.Exec("CREATE TABLE retained(value INTEGER); INSERT INTO retained VALUES(0)"); err != nil {
				t.Fatal(err)
			}
			primary := errors.New("independent " + name + " failure")
			if name == "close" {
				spy.closeErr = primary
				if err := db.Close(); !errors.Is(err, primary) || owned.active.Load() != nil || owned.IsValid() {
					t.Fatalf("physical disposal lost failure or evidence: %v", err)
				}
				return
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			b := originalTransactionTestBackend(t, db, spy.path)
			if name == "rollback" {
				spy.rollbackErr = primary
			} else {
				if name != "acknowledged_commit" {
					spy.commitErr = primary
				}
				spy.afterCommit = cancel
			}
			if name == "failed_commit" {
				if _, err := db.Exec("PRAGMA foreign_keys=ON; CREATE TABLE parent(id INTEGER PRIMARY KEY); CREATE TABLE child(id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)"); err != nil {
					t.Fatal(err)
				}
			}
			attempts := 0
			var epoch *rollbackEpoch
			committed, err := b.RunTransactionOutcome(ctx, "owned exit error", func(ctx context.Context, tx *sql.Tx) error {
				attempts++
				epoch = owned.active.Load()
				if name == "failed_commit" {
					_, err := tx.ExecContext(ctx, "INSERT INTO child VALUES(9)")
					return err
				}
				if _, err := tx.ExecContext(ctx, "UPDATE retained SET value=17"); err != nil {
					return err
				}
				if name == "rollback" {
					cancel()
					return context.Canceled
				}
				return nil
			})
			if name == "acknowledged_commit" {
				if err != nil || !committed {
					t.Fatalf("acknowledged COMMIT lost its original outcome: committed=%t err=%v", committed, err)
				}
			} else if !errors.Is(err, primary) || attempts != 1 || committed {
				t.Fatalf("independent failure lost or replayed: attempts=%d err=%v", attempts, err)
			}
			if _, clean := CleanCancellationBeforeCommit(err); clean {
				t.Fatalf("independent/attempted commit failure certified cancellation: %v", err)
			}
			if epoch == nil || !epoch.retired.Load() || (name != "rollback" && !epoch.commitAttempted.Load()) {
				t.Fatalf("missing commit/retirement fencing: %+v", epoch)
			}
			if name == "failed_commit" && !epoch.aborted.Load() {
				t.Fatal("failed COMMIT control did not reach real native rollback")
			}
			var value int
			want := 0
			if name == "commit_response" || name == "acknowledged_commit" {
				want = 17
			}
			if err := db.QueryRow("SELECT value FROM retained").Scan(&value); err != nil || value != want {
				t.Fatalf("native exit changed durable outcome: got=%d want=%d err=%v", value, want, err)
			}
			if name == "failed_commit" {
				if err := db.QueryRow("SELECT COUNT(*) FROM child").Scan(&value); err != nil || value != 0 {
					t.Fatalf("failed COMMIT leaked deferred foreign-key write: count=%d err=%v", value, err)
				}
			}
		})
	}
}
