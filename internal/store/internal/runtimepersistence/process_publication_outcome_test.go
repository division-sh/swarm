package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/schemastore"
	storeownership "github.com/division-sh/swarm/internal/store/internal/startupownership"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	"github.com/lib/pq"
	modernc "modernc.org/sqlite"
)

var errProcessCommitAcknowledgement = errors.New("injected process commit acknowledgement loss")

// Inject only at the native statement/COMMIT boundary, after real construction
// and bootstrap. Capability creation, possession and cleanup remain production.
type processPublicationConnector struct {
	driver.Connector
	phase    string
	cancel   context.CancelFunc
	armed    atomic.Bool
	begins   atomic.Int32
	commits  atomic.Int32
	postgres bool
}

func (c *processPublicationConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &processPublicationConn{Conn: conn, owner: c}, nil
}

type processPublicationConn struct {
	driver.Conn
	owner *processPublicationConnector
}

func (c *processPublicationConn) IsValid() bool {
	if validator, ok := c.Conn.(driver.Validator); ok {
		return validator.IsValid()
	}
	return !c.owner.postgres
}

func (c *processPublicationConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	if c.owner.armed.Load() {
		c.owner.begins.Add(1)
	}
	return &processPublicationTx{Tx: tx, owner: c.owner}, nil
}

func (c *processPublicationConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	result, err := c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
	if err == nil && c.owner.armed.Load() && c.owner.phase == "cancel_after_row" &&
		strings.Contains(query, "UPDATE runtime_process_generation") {
		c.owner.cancel()
	}
	return result, err
}

func (c *processPublicationConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

type processPublicationTx struct {
	driver.Tx
	owner *processPublicationConnector
}

func (tx *processPublicationTx) Commit() error {
	err := tx.Tx.Commit()
	if err == nil && tx.owner.armed.Load() {
		tx.owner.commits.Add(1)
		switch tx.owner.phase {
		case "lost_ack":
			return errProcessCommitAcknowledgement
		case "cancel_after_commit":
			tx.owner.cancel()
		}
	}
	return err
}

type processSQLiteConnector struct{ path string }

func (c processSQLiteConnector) Driver() driver.Driver { return &modernc.Driver{} }
func (c processSQLiteConnector) Connect(context.Context) (driver.Conn, error) {
	return c.Driver().Open(c.path)
}

func TestProcessCapabilityPublicationRequiresAcknowledgedNativeCommitBothStores(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, phase := range []string{"success", "cancel_before", "cancel_after_row", "cancel_after_commit", "lost_ack"} {
			t.Run(map[bool]string{true: "postgres", false: "sqlite"}[postgres]+"/"+phase, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				control := &processPublicationConnector{phase: phase, cancel: cancel, postgres: postgres}
				var observer *sql.DB
				var selected interface {
					runtimeownership.Store
					BootstrapSchema(context.Context, schemastore.SchemaBootstrapRequest) error
					Close() error
				}
				var checkAvailable func()
				if postgres {
					dsn, handle, _ := testutil.StartEmptyPostgres(t)
					observer = handle
					connector, err := pq.NewConnector(dsn)
					if err != nil {
						t.Fatal(err)
					}
					control.Connector = connector
					backend, err := postgresbackend.New(sql.OpenDB(control))
					if err != nil {
						t.Fatal(err)
					}
					store, err := newPostgresStoreComposition(backend)
					if err != nil {
						t.Fatal(err)
					}
					selected = store
					if err := store.RetainConstructionPossession(ctx); err != nil {
						t.Fatal(err)
					}
					checkAvailable = func() {
						peer, err := postgresbackend.New(observer)
						if err != nil {
							t.Fatal(err)
						}
						lease, acquired, err := postgresbackend.AcquireAdvisoryLockLease(context.Background(), peer, "swarm:runtime:shared-store-owner")
						if err != nil || !acquired {
							t.Fatalf("failed publication leaked advisory lock: acquired=%t err=%v", acquired, err)
						}
						if err := lease.Release(context.Background()); err != nil {
							t.Fatal(err)
						}
					}
				} else {
					path := filepath.Join(t.TempDir(), "publication.db")
					guard, err := storeownership.WaitSQLiteConstructionGuard(ctx, path)
					if err != nil {
						t.Fatal(err)
					}
					defer guard.Release()
					control.Connector = processSQLiteConnector{path: "file:" + path}
					backend, err := sqlitebackend.New(sql.OpenDB(control))
					if err != nil {
						t.Fatal(err)
					}
					schema, err := schemastore.NewSQLiteWithBackend(backend, path)
					if err != nil {
						t.Fatal(err)
					}
					identity, err := guard.BackendIdentity(ctx)
					if err != nil {
						t.Fatal(err)
					}
					store, err := newSQLiteStoreComposition(schema, backend, identity)
					if err != nil {
						t.Fatal(err)
					}
					selected = store
					observer, err = sql.Open("sqlite", "file:"+path)
					if err != nil {
						t.Fatal(err)
					}
					defer observer.Close()
					checkAvailable = func() {
						peer, err := storeownership.AcquireSQLiteConstructionGuard(path)
						if err != nil {
							t.Fatalf("failed publication leaked flock: %v", err)
						}
						if err := peer.Release(); err != nil {
							t.Fatal(err)
						}
					}
				}
				t.Cleanup(func() {
					if err := selected.Close(); err != nil {
						t.Error(err)
					}
				})
				if err := selected.BootstrapSchema(ctx, canonicalSchemaBootstrapTestRequest(t)); err != nil {
					t.Fatal(err)
				}
				control.armed.Store(true)
				if phase == "cancel_before" {
					cancel()
				}
				capability, err := selected.AcquireProcessCapability(ctx, runtimeownership.AcquireRequest{
					OwnerID: "native-publication", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
				})
				control.armed.Store(false)
				if capability != nil {
					t.Cleanup(func() {
						if err := capability.Release(context.Background()); err != nil {
							t.Error(err)
						}
					})
				}
				if phase == "success" {
					if err != nil || capability == nil {
						t.Fatalf("acknowledged acquisition refused: cap=%v err=%v", capability, err)
					}
					if err := capability.ProveCurrent(context.Background()); err != nil {
						t.Fatal(err)
					}
				} else {
					if capability != nil || err == nil {
						t.Fatalf("unacknowledged/canceled acquisition published capability: cap=%v err=%v", capability, err)
					}
					cause := error(context.Canceled)
					if phase == "lost_ack" {
						cause = errProcessCommitAcknowledgement
					}
					if !errors.Is(err, cause) {
						t.Fatalf("publication lost primary cause: %v", err)
					}
				}
				wantBegins, wantCommits := int32(1), int32(0)
				if phase == "cancel_before" {
					wantBegins = 0
				}
				if phase == "success" || phase == "lost_ack" || phase == "cancel_after_commit" {
					wantCommits = 1
				}
				if control.begins.Load() != wantBegins || control.commits.Load() != wantCommits {
					t.Fatalf("native acquisition replayed: begins=%d commits=%d want=%d/%d", control.begins.Load(), control.commits.Load(), wantBegins, wantCommits)
				}
				var generation int
				if err := observer.QueryRow(`SELECT generation FROM runtime_process_generation WHERE singleton_id=1`).Scan(&generation); err != nil || generation != int(wantCommits) {
					t.Fatalf("physical COMMIT observation: generation=%d want=%d err=%v", generation, wantCommits, err)
				}
				if capability != nil {
					if err := capability.Release(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
				checkAvailable()
			})
		}
	}
}
