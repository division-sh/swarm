package startupownership

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/store/internal/schemastore"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

var errGenerationMonitorRead = errors.New("independent generation read failure")

type generationMonitorConnector struct {
	driver.Connector
	phase           string
	armed           atomic.Bool
	entered, resume chan struct{}
	enterOnce       sync.Once
	resumeOnce      sync.Once
	monitored       atomic.Pointer[generationMonitorConn]
	holdClose       bool
	closeEntered    chan struct{}
	closeResume     chan struct{}
	closeResumeOnce sync.Once
}

func (c *generationMonitorConnector) stall(phase string) {
	if c.armed.Load() && c.phase == phase {
		c.enterOnce.Do(func() { close(c.entered) })
		<-c.resume
	}
}

func (c *generationMonitorConnector) unblock() { c.resumeOnce.Do(func() { close(c.resume) }) }

func (c *generationMonitorConnector) unblockClose() {
	c.closeResumeOnce.Do(func() { close(c.closeResume) })
}

func (c *generationMonitorConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &generationMonitorConn{Conn: conn, owner: c, closed: make(chan struct{})}, nil
}

type generationMonitorConn struct {
	driver.Conn
	owner  *generationMonitorConnector
	closed chan struct{}
}

func (c *generationMonitorConn) IsValid() bool { return c.Conn.(driver.Validator).IsValid() }

func (c *generationMonitorConn) Close() error {
	if c.owner.monitored.Load() == c && c.owner.holdClose {
		close(c.owner.closeEntered)
		<-c.owner.closeResume
	}
	err := c.Conn.Close()
	close(c.closed)
	return err
}

func (c *generationMonitorConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func (c *generationMonitorConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.owner.armed.Load() && strings.Contains(query, "FROM runtime_process_generation") {
		c.owner.stall("query")
		if c.owner.phase == "rollback" {
			return nil, errGenerationMonitorRead
		}
	}
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func (c *generationMonitorConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.owner.armed.Load() {
		c.owner.monitored.Store(c)
	}
	c.owner.stall("begin")
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &generationMonitorTx{Tx: tx, owner: c.owner}, nil
}

type generationMonitorTx struct {
	driver.Tx
	owner *generationMonitorConnector
}

func (t *generationMonitorTx) Commit() error {
	t.owner.stall("commit")
	return t.Tx.Commit()
}

func (t *generationMonitorTx) Rollback() error {
	t.owner.stall("rollback")
	return t.Tx.Rollback()
}

func newPostgresGenerationMonitor(t *testing.T, phase string) (*postgresSession, *sql.Conn, *generationMonitorConnector) {
	t.Helper()
	dsn, observerDB, _ := testutil.StartEmptyPostgres(t)
	observer, err := observerDB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := observer.Close(); err != nil {
			t.Error(err)
		}
	})
	native, err := pq.NewConnector(dsn)
	if err != nil {
		t.Fatal(err)
	}
	control := &generationMonitorConnector{Connector: native, phase: phase, entered: make(chan struct{}), resume: make(chan struct{}),
		closeEntered: make(chan struct{}), closeResume: make(chan struct{})}
	db := sql.OpenDB(control)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	backend, err := postgresbackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := postgresbackend.AcquireBlockingAdvisoryLockLease(context.Background(), backend, runtimeSharedStoreOwnershipLock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := lease.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	t.Cleanup(control.unblock)
	t.Cleanup(control.unblockClose)
	spec, err := contracts.LoadPlatformSpecDocument(filepath.Join("..", "..", "..", "..", "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	plans, err := schemastore.GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := schemastore.NewPostgres(backend)
	if err != nil {
		t.Fatal(err)
	}
	if err := schema.BindProcessLease(lease); err != nil {
		t.Fatal(err)
	}
	if err := schema.BootstrapSchema(context.Background(), schemastore.SchemaBootstrapRequest{PlatformPlans: plans, Origin: schemastore.RuntimeStoreOrigin{
		SwarmVersion: "generation-monitor", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC(),
	}}); err != nil {
		t.Fatal(err)
	}
	identity := processbinding.LeaseIdentity{LeaseID: uuid.NewString(), OwnerID: "generation-monitor", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(), Backend: processbinding.PostgresLeaseBackend}
	var stamp processbinding.LeaseStamp
	if err := lease.RunTransaction(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		var err error
		stamp, err = generationauthority.AdvanceProcessGeneration(ctx, tx, identity, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return &postgresSession{owner: &StartupPostgresOwner{backend: backend}, lease: lease, authority: runtimeownership.AuthorityFromLease(stamp)}, observer, control
}

func TestPostgresGenerationMonitorFencesStalledNativePhase(t *testing.T) {
	for _, phase := range []string{"begin", "query", "commit", "rollback"} {
		for _, disposition := range []string{"deadline", "cancel"} {
			if phase == "rollback" && disposition == "cancel" {
				continue // Independent read failure is not neutral cancellation.
			}
			t.Run(disposition+"_"+phase, func(t *testing.T) {
				session, observer, control := newPostgresGenerationMonitor(t, phase)
				control.holdClose = disposition == "deadline"
				fenced := make(chan struct{})
				var fenceOnce sync.Once
				if err := session.lease.InstallLocalFenceOwner(func() { fenceOnce.Do(func() { close(fenced) }) }); err != nil {
					t.Fatal(err)
				}
				control.armed.Store(true)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- session.MonitorProveCurrent(ctx, 40*time.Millisecond) }()
				select {
				case <-control.entered:
				case err := <-done:
					t.Fatalf("proof returned before native %s: %v", phase, err)
				case <-time.After(5 * time.Second):
					t.Fatalf("proof did not enter native %s", phase)
				}
				if disposition == "deadline" {
					select {
					case <-fenced:
					case <-time.After(time.Second):
						t.Fatal("deadline waited for native SQL before local fencing")
					}
					if phase == "rollback" {
						time.Sleep(80 * time.Millisecond) // The independent failure fenced before cleanup stalled.
					}
					cancel() // A later stop cannot erase the fencing decision.
				} else {
					cancel()
					time.Sleep(80 * time.Millisecond)
					select {
					case <-fenced:
						t.Fatal("ordinary cancellation fenced healthy native possession")
					default:
					}
				}
				select {
				case err := <-done:
					t.Fatalf("monitor returned before native settlement joined: %v", err)
				default:
				}
				var acquired bool
				if err := observer.QueryRowContext(context.Background(), "SELECT pg_try_advisory_lock(hashtext($1))", runtimeSharedStoreOwnershipLock).Scan(&acquired); err != nil || acquired {
					t.Fatalf("outstanding proof lost native successor exclusion: acquired=%t err=%v", acquired, err)
				}
				joinCtx, cancelJoin := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancelJoin()
				control.unblock()
				if disposition == "deadline" {
					select {
					case <-control.closeEntered:
					case err := <-done:
						t.Fatalf("monitor returned before native Close admission: %v", err)
					case <-joinCtx.Done():
						t.Fatal("monitor did not reach exact native disposal")
					}
					select {
					case err := <-done:
						t.Fatalf("monitor returned before native Close joined: %v", err)
					default:
					}
					control.unblockClose()
				}
				var err error
				select {
				case err = <-done:
				case <-joinCtx.Done():
					t.Fatal("monitor did not join released native work")
				}
				if disposition == "deadline" {
					if !errors.Is(err, context.DeadlineExceeded) || session.lease.Current() {
						t.Fatalf("late native success erased deadline/fence: %v", err)
					}
					if phase == "rollback" && !errors.Is(err, errGenerationMonitorRead) {
						t.Fatalf("cleanup masked independent read cause: %v", err)
					}
				} else if !errors.Is(err, context.Canceled) || !session.lease.Current() {
					t.Fatalf("neutral cancellation lost cause or healthy lease: %v", err)
				}
				control.armed.Store(false)
				if err := session.lease.Release(context.Background()); err != nil {
					t.Fatal(err)
				}
				query := "SELECT pg_try_advisory_lock(hashtext($1))"
				if disposition == "deadline" {
					if session.lease.ReleasedCleanly() {
						t.Fatal("terminal discard fabricated acknowledged normal unlock")
					}
					select {
					case <-control.monitored.Load().closed:
					default:
						t.Fatal("joined disposal did not complete actual native Close")
					}
					// Terminate/Close has no server acknowledgement. Native blocking
					// acquisition observes remote handoff without polling or retries.
					query = "SELECT true FROM pg_advisory_lock(hashtext($1))"
				} else if !session.lease.ReleasedCleanly() {
					t.Fatal("neutral cancellation lost acknowledged normal unlock")
				}
				if err := observer.QueryRowContext(joinCtx, query, runtimeSharedStoreOwnershipLock).Scan(&acquired); err != nil || !acquired {
					t.Fatalf("joined disposal retained native possession: acquired=%t err=%v", acquired, err)
				}
				if err := observer.QueryRowContext(joinCtx, "SELECT pg_advisory_unlock(hashtext($1))", runtimeSharedStoreOwnershipLock).Scan(&acquired); err != nil || !acquired {
					t.Fatalf("successor unlock was not acknowledged: unlocked=%t err=%v", acquired, err)
				}
			})
		}
	}
}

func TestPostgresGenerationMonitorHealthyQueueAndCancellation(t *testing.T) {
	for _, cancelQueued := range []bool{false, true} {
		t.Run(map[bool]string{false: "healthy_queue", true: "cancelled_queue"}[cancelQueued], func(t *testing.T) {
			session, _, _ := newPostgresGenerationMonitor(t, "none")
			if err := session.MonitorProveCurrent(context.Background(), time.Second); err != nil {
				t.Fatalf("healthy control: %v", err)
			}
			entered, resume := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(resume) }) }
			defer unblock()
			workDone := make(chan error, 1)
			go func() {
				workDone <- session.lease.RunTransaction(context.Background(), func(context.Context, *sql.Tx) error {
					close(entered)
					<-resume
					return nil
				})
			}()
			<-entered
			fenced := make(chan struct{}, 1)
			if err := session.lease.InstallLocalFenceOwner(func() {
				select {
				case fenced <- struct{}{}:
				default:
				}
			}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- session.MonitorProveCurrent(ctx, 50*time.Millisecond) }()
			if cancelQueued {
				cancel()
			}
			time.Sleep(150 * time.Millisecond)
			select {
			case <-fenced:
				t.Fatal("queued monitor fenced admitted healthy work")
			default:
			}
			unblock()
			if err := <-workDone; err != nil {
				t.Fatal(err)
			}
			err := <-done
			if cancelQueued && !errors.Is(err, context.Canceled) || !cancelQueued && err != nil || !session.lease.Current() {
				t.Fatalf("queued proof changed healthy authority or result: %v", err)
			}
			if err := session.ProveCurrent(context.Background()); err != nil {
				t.Fatalf("subsequent exact generation proof: %v", err)
			}
		})
	}
}

func TestPostgresGenerationMonitorWithdrawsCapabilityBeforeNativeJoin(t *testing.T) {
	session, observer, control := newPostgresGenerationMonitor(t, "query")
	capability, err := runtimeownership.NewProcessCapability(session)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := capability.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	t.Cleanup(control.unblock)
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64)}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := capability.InstallCompleteSourceSet(context.Background(), agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
		t.Fatal(err)
	}
	grant, err := capability.IssueGenerationGrant(context.Background(), runtimeownership.GrantRequest{
		BundleHash: plan.Sources[0].BundleHash, RuntimeInstanceID: session.authority.RuntimeInstanceID,
		RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grant.MarkProbesSettled(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := grant.AdmitExecution(context.Background()); err != nil {
		t.Fatal(err)
	}
	control.armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- session.MonitorProveCurrent(context.Background(), 40*time.Millisecond) }()
	select {
	case <-control.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("native generation read never entered")
	}
	select {
	case <-capability.Done():
	case <-time.After(time.Second):
		t.Fatal("local budget did not withdraw the real process capability")
	}
	select {
	case <-grant.Done():
	case <-time.After(time.Second):
		t.Fatal("local fencing left the real execution grant admitted")
	}
	if _, err := capability.Evidence(); err == nil {
		t.Fatal("fenced capability still exposes executable evidence")
	}
	if _, err := grant.Evidence(); err == nil {
		t.Fatal("fenced grant still exposes executable evidence")
	}
	select {
	case err := <-done:
		t.Fatalf("local withdrawal faked native join: %v", err)
	default:
	}
	var acquired bool
	if err := observer.QueryRowContext(context.Background(), "SELECT pg_try_advisory_lock(hashtext($1))", runtimeSharedStoreOwnershipLock).Scan(&acquired); err != nil || acquired {
		t.Fatalf("withdrawal released outstanding native possession: acquired=%t err=%v", acquired, err)
	}
	control.unblock()
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late healthy generation read resurrected capability: %v", err)
	}
	control.armed.Store(false)
}
