package schemastore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	modernc "modernc.org/sqlite"
)

var errBootstrapPrimary = errors.New("injected bootstrap failure")
var errBootstrapRollback = errors.New("injected bootstrap rollback failure")

// Fault only the raw bootstrap exit; SQL, schema validation and readback remain real.
type bootstrapExitConnector struct {
	path, fault               string
	cancel                    context.CancelFunc
	fired                     atomic.Bool
	closes, begins, rollbacks atomic.Int32
}

func (c *bootstrapExitConnector) Driver() driver.Driver { return &modernc.Driver{} }
func (c *bootstrapExitConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.Driver().Open(c.path)
	if err != nil {
		return nil, err
	}
	return &bootstrapExitConn{Conn: conn, owner: c}, nil
}

type bootstrapExitConn struct {
	driver.Conn
	owner *bootstrapExitConnector
}

func (c *bootstrapExitConn) Close() error { c.owner.closes.Add(1); return c.Conn.Close() }
func (c *bootstrapExitConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	q := strings.TrimSpace(query)
	if q == "BEGIN IMMEDIATE" {
		c.owner.begins.Add(1)
	}
	if q == "ROLLBACK" {
		c.owner.rollbacks.Add(1)
		if c.owner.fault == "rollback" {
			return nil, errBootstrapRollback
		}
	}
	if q == "COMMIT" && !c.owner.fired.Swap(true) {
		switch c.owner.fault {
		case "commit", "rollback":
			return nil, errBootstrapPrimary
		case "cancel":
			c.owner.cancel()
			return nil, ctx.Err()
		case "committed_error":
			if _, err := c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args); err != nil {
				return nil, err
			}
			return nil, errBootstrapPrimary
		}
	}
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}
func (c *bootstrapExitConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.owner.fault == "inspect" {
		if !c.owner.fired.Swap(true) {
			return nil, errBootstrapPrimary
		}
	}
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func TestSQLiteBootstrapOwnsEveryExit(t *testing.T) {
	spec := loadPlatformSpecForSQLiteSchemaTest(t)
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatal(err)
	}
	// Full platform/schema shape coverage lives in sqlite_test.go. Exercise
	// termination against the real origin-table DDL without repeating 95-table
	// rendering for every fault and every race repetition.
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
		SwarmVersion: "bootstrap-exit-proof", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC(),
	}}
	for _, fault := range []string{"success", "inspect", "commit", "rollback", "cancel", "committed_error", "cancel_before_acquire"} {
		t.Run(fault, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "bootstrap.db")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			control := &bootstrapExitConnector{path: sqliteFileDSN(path), fault: fault, cancel: cancel}
			db := sql.OpenDB(control)
			db.SetMaxOpenConns(1)
			defer db.Close()
			backend, err := sqlitebackend.New(db)
			if err != nil {
				t.Fatal(err)
			}
			store, err := NewSQLiteWithBackend(backend, path)
			if err != nil {
				t.Fatal(err)
			}
			if fault == "cancel_before_acquire" {
				cancel()
			}
			err = store.BootstrapSchema(ctx, request)
			switch fault {
			case "success":
				if err != nil {
					t.Fatal(err)
				}
			case "cancel", "cancel_before_acquire":
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("lost cancellation: %v", err)
				}
			default:
				if !errors.Is(err, errBootstrapPrimary) {
					t.Fatalf("lost primary error: %v", err)
				}
			}
			if fault == "rollback" && !errors.Is(err, errBootstrapRollback) {
				t.Fatalf("lost rollback error: %v", err)
			}
			if fault == "commit" || fault == "rollback" || fault == "cancel" || fault == "committed_error" {
				if control.closes.Load() != 1 {
					t.Fatalf("uncertain connection not disposed: %d", control.closes.Load())
				}
			}
			wantBegins, wantRollbacks := int32(1), int32(1)
			if fault == "success" {
				wantRollbacks = 0
			}
			if fault == "cancel_before_acquire" {
				wantBegins, wantRollbacks = 0, 0
			}
			if control.begins.Load() != wantBegins || control.rollbacks.Load() != wantRollbacks {
				t.Fatalf("raw begin/rollback = %d/%d want %d/%d", control.begins.Load(), control.rollbacks.Load(), wantBegins, wantRollbacks)
			}
			other, err := sql.Open("sqlite", sqliteFileDSN(path))
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			wantTables := 0
			if fault == "success" || fault == "committed_error" {
				wantTables = len(plans)
			}
			for _, reader := range []*sql.DB{db, other} {
				var tables int
				if err := reader.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").Scan(&tables); err != nil || tables != wantTables {
					t.Fatalf("schema durable tables=%d want=%d error=%v", tables, wantTables, err)
				}
			}
			// Explicit operator retry, not owner automatic replay. Also exercises
			// current-schema acceptance and WAL setup with a one-connection pool.
			control.fault = "success"
			if err := store.BootstrapSchema(context.Background(), request); err != nil {
				t.Fatalf("next bootstrap: %v", err)
			}
			if err := store.BootstrapSchema(context.Background(), request); err != nil {
				t.Fatalf("current bootstrap: %v", err)
			}
		})
	}
}
