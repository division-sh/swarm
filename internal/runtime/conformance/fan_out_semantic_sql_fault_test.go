package conformance

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"net/url"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/lib/pq"
	"modernc.org/sqlite"
)

type semanticProofSQLFaultKey struct{}

type semanticProofSQLFault struct {
	cause         error
	outcomeInsert bool
	fired         atomic.Bool
}

func (f *semanticProofSQLFault) afterInsert(query string) error {
	if f == nil {
		return nil
	}
	prefix := "insert into events "
	if f.outcomeInsert {
		prefix = "insert into fan_out_outcomes "
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(query)), prefix) && f.fired.CompareAndSwap(false, true) {
		return f.cause
	}
	return nil
}

// This fixture wraps real driver results, never successful synthetic SQL. Only
// the exact chunk context carries a fault; canonical rollback grants resealing.
type semanticProofConnector struct {
	driver driver.Driver
	dsn    string
}

func (c *semanticProofConnector) Driver() driver.Driver { return c.driver }
func (c *semanticProofConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := c.driver.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &semanticProofConn{Conn: conn}, nil
}

type semanticProofConn struct{ driver.Conn }

func (c *semanticProofConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}

func (c *semanticProofConn) Ping(ctx context.Context) error {
	if ping, ok := c.Conn.(driver.Pinger); ok {
		return ping.Ping(ctx)
	}
	return nil
}

func (c *semanticProofConn) CheckNamedValue(value *driver.NamedValue) error {
	if check, ok := c.Conn.(driver.NamedValueChecker); ok {
		return check.CheckNamedValue(value)
	}
	return driver.ErrSkip
}

func (c *semanticProofConn) ResetSession(ctx context.Context) error {
	if reset, ok := c.Conn.(driver.SessionResetter); ok {
		return reset.ResetSession(ctx)
	}
	return nil
}

func (c *semanticProofConn) IsValid() bool {
	if valid, ok := c.Conn.(driver.Validator); ok {
		return valid.IsValid()
	}
	return true
}

func (c *semanticProofConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	result, err := c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
	if err != nil {
		return result, err
	}
	fault, _ := ctx.Value(semanticProofSQLFaultKey{}).(*semanticProofSQLFault)
	return result, fault.afterInsert(query)
}

func (c *semanticProofConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
	if err != nil {
		return nil, err
	}
	fault, _ := ctx.Value(semanticProofSQLFaultKey{}).(*semanticProofSQLFault)
	if fault == nil {
		return rows, nil
	}
	return &semanticProofRowsFault{Rows: rows, fault: fault, query: query}, nil
}

type semanticProofRowsFault struct {
	driver.Rows
	fault *semanticProofSQLFault
	query string
}

func (r *semanticProofRowsFault) Next(dest []driver.Value) error {
	if err := r.Rows.Next(dest); err != nil {
		return err
	}
	// PostgreSQL RETURNING has mutated the row before the first successful Next.
	return r.fault.afterInsert(r.query)
}

func newSemanticProofSQLFaultStore(t *testing.T, backend string) (notifyAllChildrenStore, *sql.DB) {
	t.Helper()
	connector := &semanticProofConnector{}
	maxOpen := 4
	if backend == "postgres" {
		dsn, original, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		connector.driver, connector.dsn = &pq.Driver{}, dsn
		maxOpen = original.Stats().MaxOpenConnections
	} else {
		connector.driver = &sqlite.Driver{}
		// Match schemastore.sqliteFileDSN and openSQLite, including fresh-connection
		// pragmas. Bootstrap below owns WAL and its connection's bootstrap timeout.
		u := url.URL{Scheme: "file", Opaque: filepath.Join(t.TempDir(), "semantic-proof.sqlite")}
		q := u.Query()
		q.Add("_pragma", "foreign_keys(ON)")
		q.Add("_pragma", "busy_timeout(50)")
		u.RawQuery = q.Encode()
		connector.dsn = u.String()
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(maxOpen)
	if backend == "sqlite" {
		db.SetMaxIdleConns(4)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	if backend == "postgres" {
		return storetest.AdmitPostgresRuntimeStore(t, db), db
	}
	return storetest.AdmitSQLiteRuntimeStore(t, db), db
}

func TestSemanticProofSQLFaultStorePreservesSQLiteConstructor(t *testing.T) {
	ctx := context.Background()
	normal := storetest.DatabaseForTest(storetest.StartSQLiteRuntimeStore(t))
	_, wrapped := newSemanticProofSQLFaultStore(t, "sqlite")
	type settings struct {
		foreignKeys, busyTimeout int
		journal                  string
	}
	readPool := func(db *sql.DB) []settings {
		t.Helper()
		if got := db.Stats().MaxOpenConnections; got != 4 {
			t.Fatalf("max open connections = %d, want 4", got)
		}
		var held []*sql.Conn
		defer func() {
			for _, conn := range held {
				if err := conn.Close(); err != nil {
					t.Error(err)
				}
			}
			if got := db.Stats().Idle; got != 4 {
				t.Errorf("idle connections after release = %d, want 4", got)
			}
		}()
		var result []settings
		for i := 0; i < 4; i++ {
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			held = append(held, conn)
			var s settings
			for query, dest := range map[string]any{"PRAGMA foreign_keys": &s.foreignKeys, "PRAGMA busy_timeout": &s.busyTimeout, "PRAGMA journal_mode": &s.journal} {
				if err := conn.QueryRowContext(ctx, query).Scan(dest); err != nil {
					t.Fatal(err)
				}
			}
			result = append(result, s)
		}
		sort.Slice(result, func(i, j int) bool { return result[i].busyTimeout < result[j].busyTimeout })
		return result
	}
	want, got := readPool(normal), readPool(wrapped)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("wrapped pool settings = %+v, normal constructor = %+v", got, want)
	}
	t.Logf("four held connections match normal constructor: %+v", got)

	conn, err := wrapped.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "CREATE TEMP TABLE semantic_fault_forwarding (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	for _, commit := range []bool{false, true} {
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		result, err := tx.ExecContext(ctx, "INSERT INTO semantic_fault_forwarding (id) VALUES (?)", 7)
		if err != nil {
			t.Fatal(err)
		}
		if count, err := result.RowsAffected(); err != nil || count != 1 {
			t.Fatalf("native affected rows = %d, err=%v", count, err)
		}
		if commit {
			err = tx.Commit()
		} else {
			err = tx.Rollback()
		}
		if err != nil {
			t.Fatal(err)
		}
		var count int
		if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM semantic_fault_forwarding WHERE id=?", 7).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if (count == 1) != commit || count < 0 || count > 1 {
			t.Fatalf("commit=%t, persisted rows=%d", commit, count)
		}
	}
	if _, err := conn.ExecContext(ctx, "INSERT INTO semantic_fault_forwarding (id) VALUES (?)", 7); err == nil {
		t.Fatal("native duplicate-key error lost by no-fault forwarding")
	}
}
