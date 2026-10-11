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
	phase        string
	cancel       context.CancelFunc
	fired        atomic.Bool
	begins       atomic.Int32
	leasePID     atomic.Int32
	bootstrapPID atomic.Int32
}

func (c *postgresBootstrapExitConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := conn.(driver.QueryerContext).QueryContext(ctx, `SELECT pg_backend_pid()`, nil)
	if err != nil {
		return nil, errors.Join(err, conn.Close())
	}
	values := make([]driver.Value, 1)
	readErr := rows.Next(values)
	if err := errors.Join(readErr, rows.Close()); err != nil {
		return nil, errors.Join(err, conn.Close())
	}
	return &postgresBootstrapExitConn{Conn: conn, owner: c, pid: int32(values[0].(int64))}, nil
}

type postgresBootstrapExitConn struct {
	driver.Conn
	owner *postgresBootstrapExitConnector
	pid   int32
}

func (c *postgresBootstrapExitConn) IsValid() bool {
	validator, ok := c.Conn.(driver.Validator)
	return ok && validator.IsValid()
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
	if err == nil && strings.Contains(query, "pg_advisory_lock(hashtext") {
		c.owner.leasePID.Store(c.pid)
	}
	if err == nil && strings.Contains(query, "pg_advisory_xact_lock") && !c.owner.fired.Swap(true) {
		c.owner.bootstrapPID.Store(c.pid)
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
		for _, retained := range []bool{false, true} {
			t.Run(phase+"/"+map[bool]string{false: "pooled", true: "process_lease"}[retained], func(t *testing.T) {
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
				var lease *postgresbackend.AdvisoryLockLease
				if retained {
					lease, err = postgresbackend.AcquireBlockingAdvisoryLockLease(context.Background(), backend, "swarm:runtime:shared-store-owner")
					if err != nil {
						t.Fatal(err)
					}
					defer func() {
						if err := lease.Release(context.Background()); err != nil {
							t.Error(err)
						}
					}()
					if err := store.BindProcessLease(lease); err != nil {
						t.Fatal(err)
					}
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
				if retained {
					wantPID := control.leasePID.Load()
					if phase == "before_admission" {
						wantPID = 0
					}
					if control.leasePID.Load() == 0 || control.bootstrapPID.Load() != wantPID {
						t.Fatalf("bootstrap changed session: lease=%d bootstrap=%d want=%d", control.leasePID.Load(), control.bootstrapPID.Load(), wantPID)
					}
					if err := lease.ProveCurrent(context.Background()); (err == nil) == (phase == "lost_commit_response") {
						t.Fatalf("retained possession after %s: %v", phase, err)
					}
					if phase == "lost_commit_response" {
						peer, err := postgresbackend.New(observer)
						if err != nil {
							t.Fatal(err)
						}
						available, acquired, err := postgresbackend.AcquireAdvisoryLockLease(context.Background(), peer, "swarm:runtime:shared-store-owner")
						if err != nil || !acquired {
							t.Fatalf("ambiguous bootstrap leaked retained possession: acquired=%t err=%v", acquired, err)
						}
						if err := available.Release(context.Background()); err != nil {
							t.Fatal(err)
						}
					}
				}
			})
		}
	}
}
