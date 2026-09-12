package schemastore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/lib/pq"
)

type postgresBootstrapExitConnector struct {
	driver.Connector
	phase  string
	cancel context.CancelFunc
	fired  atomic.Bool
	begins atomic.Int32
}

func (c *postgresBootstrapExitConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &postgresBootstrapExitConn{Conn: conn, owner: c}, nil
}

type postgresBootstrapExitConn struct {
	driver.Conn
	owner *postgresBootstrapExitConnector
}

func (c *postgresBootstrapExitConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if ctx.Done() != nil {
		return nil, errors.New("bootstrap BEGIN used caller-cancellable SQL context")
	}
	c.owner.begins.Add(1)
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &postgresBootstrapExitTx{Tx: tx, owner: c.owner}, nil
}

func (c *postgresBootstrapExitConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if ctx.Done() != nil {
		return nil, errors.New("bootstrap Exec used caller-cancellable SQL context")
	}
	result, err := c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
	if err == nil && strings.Contains(query, "pg_advisory_xact_lock") && !c.owner.fired.Swap(true) {
		switch c.owner.phase {
		case "during_sql":
			c.owner.cancel()
		case "independent_error":
			c.owner.cancel()
			return nil, errBootstrapPrimary
		}
	}
	return result, err
}

func (c *postgresBootstrapExitConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if ctx.Done() != nil {
		return nil, errors.New("bootstrap Query used caller-cancellable SQL context")
	}
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

type postgresBootstrapExitTx struct {
	driver.Tx
	owner *postgresBootstrapExitConnector
}

func (tx *postgresBootstrapExitTx) Commit() error {
	if err := tx.Tx.Commit(); err != nil {
		return err
	}
	if tx.owner.phase == "lost_commit_response" {
		// Real SQL commits; only the caller's acknowledgement is faulted.
		return errBootstrapPrimary
	}
	return nil
}

func TestPostgresBootstrapOwnsSQLAndCommitOutcome(t *testing.T) {
	spec := loadPlatformSpecForSQLiteSchemaTest(t)
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatal(err)
	}
	for _, plan := range plans {
		if plan.TableName == RuntimeStoreMetadataTable {
			plans = []SchemaTableDDL{plan}
			break
		}
	}
	if len(plans) != 1 {
		t.Fatal("origin-table plan missing")
	}
	request := SchemaBootstrapRequest{PlatformPlans: plans, Origin: RuntimeStoreOrigin{
		SwarmVersion: "bounded-writer-proof", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC(),
	}}
	for _, phase := range []string{"success", "before_admission", "during_sql", "independent_error", "lost_commit_response"} {
		t.Run(phase, func(t *testing.T) {
			dsn, observer, cleanup := testutil.StartEmptyPostgres(t)
			t.Cleanup(cleanup)
			connector, err := pq.NewConnector(dsn)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			control := &postgresBootstrapExitConnector{Connector: connector, phase: phase, cancel: cancel}
			db := sql.OpenDB(control)
			defer db.Close()
			backend, err := postgresbackend.New(db)
			if err != nil {
				t.Fatal(err)
			}
			store, err := NewPostgres(backend)
			if err != nil {
				t.Fatal(err)
			}
			if phase == "before_admission" {
				cancel()
			}
			err = store.BootstrapSchema(ctx, request)
			switch phase {
			case "success":
				if err != nil {
					t.Fatal(err)
				}
			case "before_admission", "during_sql", "independent_error":
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("logical cancellation lost: %v", err)
				}
			case "lost_commit_response":
				if !errors.Is(err, errBootstrapPrimary) {
					t.Fatalf("commit uncertainty lost: %v", err)
				}
			}
			if phase == "independent_error" && !errors.Is(err, errBootstrapPrimary) {
				t.Fatalf("independent error lost: %v", err)
			}
			wantBegins := int32(1)
			if phase == "before_admission" {
				wantBegins = 0
			}
			if control.begins.Load() != wantBegins {
				t.Fatalf("BEGIN count = %d, want %d; no automatic callback replay", control.begins.Load(), wantBegins)
			}
			var exists bool
			if err := observer.QueryRow(`SELECT to_regclass('public.runtime_store_metadata') IS NOT NULL`).Scan(&exists); err != nil {
				t.Fatal(err)
			}
			wantExists := phase == "success" || phase == "lost_commit_response"
			if exists != wantExists {
				t.Fatalf("durable schema exists=%t, want %t", exists, wantExists)
			}
			if admitted := store.RequireCurrent() == nil; admitted != (phase == "success") {
				t.Fatalf("schema admitted=%t after %s", admitted, phase)
			}
		})
	}
}
