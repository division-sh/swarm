package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

type agentSnapshotScopeProbe struct {
	actual       agentSnapshotConstraintScope
	partial      error
	afterPrepare func(*sql.Conn) error
	afterCleanup func(*sql.Conn) error
}

func (s *agentSnapshotScopeProbe) prepare(ctx context.Context, conn *sql.Conn) error {
	if s.partial != nil {
		s.actual.armed = true
		_, err := conn.ExecContext(ctx, `PRAGMA foreign_keys=OFF`)
		return errors.Join(s.partial, err)
	}
	if err := s.actual.prepare(ctx, conn); err != nil {
		return err
	}
	if s.afterPrepare != nil {
		return s.afterPrepare(conn)
	}
	return nil
}

func (s *agentSnapshotScopeProbe) cleanup(conn *sql.Conn) error {
	err := s.actual.cleanup(conn)
	if s.afterCleanup != nil {
		err = errors.Join(err, s.afterCleanup(conn))
	}
	return err
}

func TestAgentSnapshotConnectionScopeFailureCuts(t *testing.T) {
	for _, cut := range []string{"partial_prepare", "before_begin", "cleanup"} {
		t.Run(cut, func(t *testing.T) {
			b := newAgentSnapshotConnectionProbe(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cause := errors.New("injected connection-scope cut")
			scope := &agentSnapshotScopeProbe{actual: agentSnapshotConstraintScope{foreign: true}}
			switch cut {
			case "partial_prepare":
				scope.partial = cause
			case "before_begin":
				scope.afterPrepare = func(*sql.Conn) error { cancel(); return nil }
			case "cleanup":
				scope.afterCleanup = func(conn *sql.Conn) error {
					// Make the exact pooled connection unsafe after restoration;
					// the coordinator must discard it, not merely report the error.
					_, err := conn.ExecContext(context.Background(), `PRAGMA foreign_keys=OFF`)
					return errors.Join(cause, err)
				}
			}
			entered := false
			committed, err := b.runTransactionWithConnectionScope(ctx, nil, 0, scope, func(context.Context, *sql.Tx) error { entered = true; return nil })
			if cut == "before_begin" {
				// The single-attempt transport can expose SQLite's interrupted
				// Begin error; the public writer retains the caller's cause.
				var code interface{ Code() int }
				interrupted := errors.As(err, &code) && code.Code()&255 == 9
				if ctx.Err() != context.Canceled || (!errors.Is(err, context.Canceled) && !interrupted) {
					t.Fatalf("cancelled Begin lost its cause: %v", err)
				}
			} else if !errors.Is(err, cause) {
				t.Fatalf("lost scope failure %s: %v", cut, err)
			}
			if entered != (cut == "cleanup") || committed != (cut == "cleanup") {
				t.Fatalf("wrong callback/commit disposition: entered=%v committed=%v", entered, committed)
			}
			requireAgentSnapshotProbeEnforced(t, b, 1)
		})
	}
}

func newAgentSnapshotConnectionProbe(t *testing.T) *Backend {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "constraint-probe.db")+"?_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	backend, err := New(db)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Error(err)
		}
	})
	// Deliberately isolated transport probes, not handwritten runtime tables.
	if _, err := db.Exec(`CREATE TABLE constraint_parent(id INTEGER PRIMARY KEY CHECK(id>0));
CREATE TABLE constraint_child(id INTEGER PRIMARY KEY,parent_id INTEGER REFERENCES constraint_parent(id));
INSERT INTO constraint_parent VALUES(1); INSERT INTO constraint_child VALUES(1,1)`); err != nil {
		t.Fatal(err)
	}
	return backend
}

func requireAgentSnapshotProbeEnforced(t *testing.T, b *Backend, parent int) {
	t.Helper()
	conn, err := b.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := requireAgentSnapshotConstraints(context.Background(), conn); err != nil {
		t.Fatal(err)
	}
	var got, child int
	if err := conn.QueryRowContext(context.Background(), `SELECT id FROM constraint_parent`).Scan(&got); err != nil || got != parent {
		t.Fatalf("parent=%d err=%v want%d", got, err, parent)
	}
	if err := conn.QueryRowContext(context.Background(), `SELECT parent_id FROM constraint_child`).Scan(&child); err != nil || child != 1 {
		t.Fatalf("child=%d err=%v want1", child, err)
	}
	if _, err := conn.ExecContext(context.Background(), `INSERT INTO constraint_parent VALUES(-1)`); err == nil {
		t.Fatal("CHECK enforcement was not restored")
	}
	if _, err := conn.ExecContext(context.Background(), `INSERT INTO constraint_child VALUES(2,42)`); err == nil {
		t.Fatal("foreign-key enforcement was not restored")
	}
}

func TestAgentSnapshotCorruptionConnectionScopeRestoresEnforcement(t *testing.T) {
	for _, route := range []bool{false, true} {
		name := "status"
		if route {
			name = "route"
		}
		t.Run(name, func(t *testing.T) {
			proveAgentSnapshotConnectionRestoration(t, route)
		})
	}
}

func proveAgentSnapshotConnectionRestoration(t *testing.T, route bool) {
	t.Helper()

	b := newAgentSnapshotConnectionProbe(t)
	run := b.RunAgentSnapshotStatusCorruptionForTest
	if route {
		run = b.RunAgentSnapshotRouteCorruptionForTest
	}
	committed, err := run(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		var foreign, ignored int
		if err := tx.QueryRowContext(ctx, `PRAGMA foreign_keys`).Scan(&foreign); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `PRAGMA ignore_check_constraints`).Scan(&ignored); err != nil {
			return err
		}
		wantForeign := 1
		if route {
			wantForeign = 0
		}
		if foreign != wantForeign || ignored != 1 {
			t.Errorf("fault settings foreign=%d ignored=%d", foreign, ignored)
		}
		if !route {
			return nil
		}
		_, err := tx.ExecContext(ctx, `UPDATE constraint_parent SET id=0 WHERE id=1`)
		return err
	})
	if err != nil || !committed {
		t.Fatalf("fault commit=%v err=%v", committed, err)
	}
	want := 1
	if route {
		want = 0
	}
	requireAgentSnapshotProbeEnforced(t, b, want)
	if err := b.RunTransaction(context.Background(), "restore probe row", func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE constraint_parent SET id=1 WHERE id=0`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	requireAgentSnapshotProbeEnforced(t, b, 1)

}

func TestAgentSnapshotCorruptionConnectionScopeConservesFailedWork(t *testing.T) {
	for _, kind := range []string{"error", "cancel", "panic"} {
		t.Run(kind, func(t *testing.T) {
			b := newAgentSnapshotConnectionProbe(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cause := errors.New("fixed probe failure")
			var committed bool
			var runErr error
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				committed, runErr = b.RunAgentSnapshotRouteCorruptionForTest(ctx, func(ctx context.Context, tx *sql.Tx) error {
					if _, err := tx.ExecContext(ctx, `UPDATE constraint_parent SET id=0 WHERE id=1`); err != nil {
						return err
					}
					switch kind {
					case "panic":
						panic(cause)
					case "cancel":
						cancel()
						return ctx.Err()
					default:
						return cause
					}
				})
			}()
			if committed {
				t.Fatal("failed work reported committed")
			}
			if kind == "panic" {
				if recovered != cause {
					t.Fatalf("lost original panic: %v", recovered)
				}
			} else {
				want := cause
				if kind == "cancel" {
					want = context.Canceled
				}
				if !errors.Is(runErr, want) || recovered != nil {
					t.Fatalf("lost original failure: err=%v panic=%v", runErr, recovered)
				}
			}
			requireAgentSnapshotProbeEnforced(t, b, 1)
		})
	}
}

func TestAgentSnapshotCorruptionUsesOriginalWriterAdmission(t *testing.T) {
	b := newAgentSnapshotConnectionProbe(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	var once sync.Once
	defer func() {
		once.Do(func() { close(release) })
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	go func() {
		done <- b.RunTransaction(context.Background(), "admitted holder", func(context.Context, *sql.Tx) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	refused := make(chan error, 1)
	go func() {
		_, err := b.RunAgentSnapshotRouteCorruptionForTest(ctx, func(context.Context, *sql.Tx) error {
			return errors.New("fault bypassed writer admission")
		})
		refused <- err
	}()
	waitForQueuedMutations(t, b, 1)
	cancel()
	if err := <-refused; !errors.Is(err, context.Canceled) {
		t.Fatalf("queued fault=%v want cancellation", err)
	}
	once.Do(func() { close(release) })
}
