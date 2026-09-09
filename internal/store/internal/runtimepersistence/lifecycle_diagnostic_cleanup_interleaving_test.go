package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/lib/pq"
	"modernc.org/sqlite"
)

// The driver adapter delegates every SQL operation. Its sole intervention is
// holding a successful real write before the transaction can commit, or
// observing a competing transaction's admission. No production hook is needed.
const diagnosticEventInsertSQL = "insert into events"

type diagnosticSQLConnector struct {
	driver.Connector
	afterExec func(context.Context, string) error
	begin     func()
}

func (c diagnosticSQLConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return diagnosticSQLConn{Conn: conn, afterExec: c.afterExec, begin: c.begin}, nil
}

type diagnosticSQLiteConnector struct{ path string }

func (c diagnosticSQLiteConnector) Connect(context.Context) (driver.Conn, error) {
	return c.Driver().Open(c.path)
}
func (diagnosticSQLiteConnector) Driver() driver.Driver { return &sqlite.Driver{} }

type diagnosticSQLConn struct {
	driver.Conn
	afterExec func(context.Context, string) error
	begin     func()
}

func (c diagnosticSQLConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	if c.begin != nil {
		c.begin()
	}
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, options)
}
func (c diagnosticSQLConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	result, err := c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
	if err == nil && c.afterExec != nil {
		err = c.afterExec(ctx, query)
	}
	return result, err
}
func (c diagnosticSQLConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}
func (c diagnosticSQLConn) CheckNamedValue(value *driver.NamedValue) error {
	if checker, ok := c.Conn.(driver.NamedValueChecker); ok {
		return checker.CheckNamedValue(value)
	}
	return driver.ErrSkip
}
func (c diagnosticSQLConn) Ping(ctx context.Context) error {
	if pinger, ok := c.Conn.(driver.Pinger); ok {
		return pinger.Ping(ctx)
	}
	return nil
}

func TestLifecycleDiagnosticCleanupTransactionInterleavingsBothStores(t *testing.T) {
	for _, useSQLite := range []bool{true, false} {
		for _, diagnosticFirst := range []bool{true, false} {
			t.Run(fmt.Sprintf("sqlite=%t/diagnostic_first=%t", useSQLite, diagnosticFirst), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 15*time.Second)
				defer cancel()
				var connector driver.Connector
				if useSQLite {
					connector = diagnosticSQLiteConnector{filepath.Join(t.TempDir(), "overlap.db") + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"}
				} else {
					dsn, _, _ := testutil.StartPostgres(t)
					var err error
					connector, err = pq.NewConnector(dsn)
					if err != nil {
						t.Fatal(err)
					}
				}
				entered, release, competing := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var armed, held atomic.Bool
				var releaseOnce, competingOnce sync.Once
				unblock := func() { releaseOnce.Do(func() { close(release) }) }
				defer unblock()
				needle := diagnosticEventInsertSQL
				if !diagnosticFirst {
					needle = `delete from "runs"`
				}
				firstDB := sql.OpenDB(diagnosticSQLConnector{Connector: connector, afterExec: func(ctx context.Context, query string) error {
					if !armed.Load() || !strings.Contains(strings.ToLower(query), needle) || !held.CompareAndSwap(false, true) {
						return nil
					}
					close(entered)
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}})
				secondDB := sql.OpenDB(diagnosticSQLConnector{Connector: connector, begin: func() {
					if armed.Load() {
						competingOnce.Do(func() { close(competing) })
					}
				}})
				t.Cleanup(func() { _ = firstDB.Close(); _ = secondDB.Close() })
				open := func(db *sql.DB) lifecycleDiagnosticTestStore {
					if !useSQLite {
						return admitTestPostgresStore(t, db)
					}
					store := NewSQLiteRuntimeStoreForTest(db)
					if err := store.BootstrapSchema(ctx, canonicalSchemaBootstrapTestRequest(t)); err != nil {
						t.Fatal(err)
					}
					store.SetEventPayloadAdmitter(storeTestPayloadAdmitter)
					return store
				}
				firstStore, secondStore := open(firstDB), open(secondDB)
				cleanupStore, logStore := firstStore, secondStore
				if diagnosticFirst {
					cleanupStore, logStore = secondStore, firstStore
				}
				item := createLifecycleDiagnostic(t, ctx, cleanupStore)
				capability, err := agentfixture.ProcessCapability(t, ctx, cleanupStore)
				if err != nil {
					t.Fatal(err)
				}
				request := admitRetainedResetCleanupProof(t, capability, cleanupStore, item.Identity.RunID, false)
				project := func() error {
					return runtimepkg.NewRuntimeLogger(logStore, executionposture.Live, nil).ProjectLifecycleDiagnostic(ctx, item)
				}
				cleanup := func() error { _, err := capability.ApplyDestructiveResetCleanup(ctx, request, nil); return err }
				firstOperation, secondOperation := cleanup, project
				if diagnosticFirst {
					firstOperation, secondOperation = project, cleanup
				}
				firstDone, secondDone := make(chan error, 1), make(chan error, 1)
				armed.Store(true)
				go func() { firstDone <- firstOperation() }()
				select {
				case <-entered:
				case err := <-firstDone:
					t.Fatalf("first transaction missed SQL barrier: %v", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				go func() { secondDone <- secondOperation() }()
				select {
				case <-competing:
				case err := <-secondDone:
					t.Fatalf("second operation missed transaction admission: %v", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				select {
				case err := <-secondDone:
					t.Fatalf("second transaction escaped uncommitted predecessor: %v", err)
				default:
				}
				unblock()
				for _, done := range []chan error{firstDone, secondDone} {
					select {
					case err := <-done:
						if err != nil {
							t.Fatal(err)
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				if err := project(); err != nil {
					t.Fatalf("exact post-cleanup replay: %v", err)
				}
				want := 1
				if diagnosticFirst {
					want = 0
				}
				if count := diagnosticLogCount(t, secondDB, item.OutboxID); count != want {
					t.Fatalf("historical logs=%d want=%d", count, want)
				}
				var runs, pending int
				if err := secondDB.QueryRow("SELECT COUNT(*) FROM runs").Scan(&runs); err != nil || runs != 0 {
					t.Fatalf("resurrected runs=%d err=%v", runs, err)
				}
				if err := secondDB.QueryRow("SELECT COUNT(*) FROM agent_lifecycle_diagnostic_outbox WHERE projected_at IS NULL").Scan(&pending); err != nil || pending != 0 {
					t.Fatalf("pending=%d err=%v", pending, err)
				}
			})
		}
	}
}
