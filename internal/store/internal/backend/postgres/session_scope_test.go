package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testutil"
	"github.com/lib/pq"
)

func TestPostgresSessionScopeMonitorReleasesHealthyBinding(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	db.SetMaxOpenConns(2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const key = "monitor-scope-release"
	lease, acquired, err := AcquireAdvisoryLockLease(ctx, db, key)
	if err != nil || !acquired {
		t.Fatalf("acquire=%t err=%v", acquired, err)
	}
	t.Cleanup(func() {
		if err := lease.Release(context.Background()); err != nil {
			t.Errorf("cleanup lease: %v", err)
		}
	})
	var ownerPID int
	if err := lease.Session().QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&ownerPID); err != nil {
		t.Fatal(err)
	}
	competitor, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer competitor.Close()
	var competitorPID int
	if err := competitor.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&competitorPID); err != nil {
		t.Fatal(err)
	}
	if competitorPID == ownerPID {
		t.Fatal("competitor reused the owning physical session")
	}
	var held bool
	if err := competitor.QueryRowContext(ctx, "SELECT pg_try_advisory_lock(hashtext($1))", key).Scan(&held); err != nil || held {
		t.Fatalf("competing acquire before release=%t err=%v", held, err)
	}
	if err := lease.MonitorProveCurrent(ctx, time.Second); err != nil {
		t.Fatalf("healthy monitor: %v", err)
	}
	// No transaction or new scoped query may overwrite the completed probe's binding.
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release after successful monitor: %v", err)
	}
	var returnedPID int
	if err := db.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&returnedPID); err != nil {
		t.Fatal(err)
	}
	if returnedPID != ownerPID {
		t.Fatalf("healthy session was discarded: owner=%d returned=%d", ownerPID, returnedPID)
	}
	if err := competitor.QueryRowContext(ctx, "SELECT pg_try_advisory_lock(hashtext($1))", key).Scan(&held); err != nil || !held {
		t.Fatalf("competing acquire after release=%t err=%v", held, err)
	}
	if err := competitor.QueryRowContext(ctx, "SELECT pg_advisory_unlock(hashtext($1))", key).Scan(&held); err != nil || !held {
		t.Fatalf("competing unlock=%t err=%v", held, err)
	}
	t.Logf("owner PID %d preserved; competitor PID %d blocked before release and acquired afterward", ownerPID, competitorPID)
}

func TestPostgresSessionScopeFailedBeginFencesWithOpaqueCloseFailure(t *testing.T) {
	for _, failClose := range []bool{false, true} {
		name := "clean_close"
		if failClose {
			name = "native_close_failure"
		}
		t.Run(name, func(t *testing.T) {
			dsn, _, _ := testutil.StartPostgres(t)
			beginFailure := errors.New("independent begin failure")
			closeFailure := errors.New("independent native transport close failure")
			dialer := &sessionScopeDialer{}
			if failClose {
				dialer.closeFailure = closeFailure
			}
			native, err := pq.NewConnector(dsn)
			if err != nil {
				t.Fatal(err)
			}
			native.Dialer(dialer)
			probe := &exitProbeConnector{
				beginFailure: beginFailure,
				closedSignal: make(chan struct{}),
			}
			db := sql.OpenDB(&sessionScopeConnector{Connector: native, probe: probe})
			defer db.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			lease, acquired, err := AcquireAdvisoryLockLease(ctx, db, "failed-begin-native-close")
			if err != nil || !acquired {
				t.Fatalf("acquire=%t err=%v", acquired, err)
			}
			t.Cleanup(func() { _ = lease.Release(context.Background()) })
			err = lease.RunTransaction(ctx, func(context.Context, *sql.Tx) error {
				t.Error("callback executed despite failed BEGIN")
				return nil
			})
			if !errors.Is(err, beginFailure) {
				t.Errorf("lost independent BEGIN error: %v", err)
			}
			// database/sql discards the driver Close error during bad-connection
			// disposal. The application must preserve BEGIN failure, not invent it.
			if errors.Is(err, closeFailure) {
				t.Errorf("manufactured driver-internal disposal evidence: %v", err)
			}
			if errors.Is(err, sql.ErrConnDone) {
				t.Errorf("manufactured connection cleanup error: %v", err)
			}
			if lease.Current() {
				t.Error("failed BEGIN retained possession")
			}
			if got := probe.closed.Load(); got != 1 {
				t.Errorf("driver closes=%d, want 1", got)
			}
			if got := dialer.closed.Load(); got != 1 {
				t.Errorf("native transport closes=%d, want 1", got)
			}
			t.Logf("failed BEGIN outcome: %v", err)
		})
	}
}

func TestPostgresSessionScopeOwnedProofCancellationRetainsConnection(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE FUNCTION session_scope_owned_stop() RETURNS boolean LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'proof-active'; PERFORM pg_sleep(0.06); RETURN true; END $$`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lease, acquired, err := AcquireAdvisoryLockLease(ctx, db, "owned-proof-cancellation")
	if err != nil || !acquired {
		t.Fatalf("acquire=%t err=%v", acquired, err)
	}
	t.Cleanup(func() { _ = lease.Release(context.Background()) })
	conn, err := lease.Session().connection()
	if err != nil {
		t.Fatal(err)
	}
	var beforePID int
	if err := conn.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&beforePID); err != nil {
		t.Fatal(err)
	}
	if err := conn.Raw(func(raw any) error {
		pq.SetNoticeHandler(raw.(driver.Conn), func(*pq.Error) { cancel() })
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	lease.SetProveForTest(func(queryCtx context.Context, session *SessionAuthority, _ string) (bool, error) {
		var held bool
		err := session.QueryRowContext(queryCtx, "SELECT session_scope_owned_stop()").Scan(&held)
		return held, err
	})
	started := time.Now()
	err = lease.ProveCurrent(ctx)
	lease.SetProveForTest(nil)
	if time.Since(started) < 50*time.Millisecond {
		t.Fatal("ordinary cancellation interrupted admitted healthy SQL instead of draining")
	}
	if !onlyCancellationLeaves(err, context.Canceled) {
		t.Fatalf("native owning cancellation gained independent error: %v", err)
	}
	if !lease.Current() {
		t.Fatal("settled owning cancellation discarded healthy possession")
	}
	next, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	var afterPID int
	if err := conn.QueryRowContext(next, "SELECT pg_backend_pid()").Scan(&afterPID); err != nil {
		t.Fatalf("retained connection unusable after owning cancellation: %v", err)
	}
	if afterPID != beforePID {
		t.Fatalf("retained PID changed: %d -> %d", beforePID, afterPID)
	}
	if err := lease.MonitorProveCurrent(next, time.Second); err != nil {
		t.Fatalf("possession lost after owning cancellation: %v", err)
	}
	if err := lease.Release(next); err != nil {
		t.Fatalf("release after owning cancellation: %v", err)
	}
	t.Logf("owning proof cancellation retained healthy PID %d", beforePID)
}

func TestPostgresSessionScopeProofDisposesClosedConnection(t *testing.T) {
	for _, monitor := range []bool{false, true} {
		for _, cancelCaller := range []bool{false, true} {
			name := "prove"
			if monitor {
				name = "monitor"
			}
			if cancelCaller {
				name += "/caller_canceled"
			} else {
				name += "/caller_live"
			}
			t.Run(name, func(t *testing.T) {
				dsn, _, _ := testutil.StartPostgres(t)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				writeFailure := errors.New("independent native proof write failure")
				closeFailure := errors.New("independent native proof close failure")
				dialer := &sessionScopeDialer{writeFailure: writeFailure, closeFailure: closeFailure}
				if cancelCaller {
					dialer.onWriteFailure = cancel
				}
				native, err := pq.NewConnector(dsn)
				if err != nil {
					t.Fatal(err)
				}
				native.Dialer(dialer)
				db := sql.OpenDB(native)
				defer db.Close()
				lease, acquired, err := AcquireAdvisoryLockLease(ctx, db, "native-proof-closed-session")
				if err != nil || !acquired {
					t.Fatalf("acquire=%t err=%v", acquired, err)
				}
				t.Cleanup(func() { _ = lease.Release(context.Background()) })
				session := lease.Session()
				var retired atomic.Int32
				lease.InstallTerminalOwner(nil, func() { retired.Add(1) }, nil)
				conn, err := session.connection()
				if err != nil {
					t.Fatal(err)
				}
				dialer.failWrite.Store(true)
				if monitor {
					err = lease.MonitorProveCurrent(ctx, time.Second)
				} else {
					err = lease.ProveCurrent(ctx)
				}
				// Stock pq collapses this write failure to ErrBadConn and hides
				// secondary Close failure. Counters below prove actual disposal.
				if !errors.Is(err, driver.ErrBadConn) {
					t.Errorf("lost observable bad-connection failure: %v", err)
				}
				if errors.Is(err, context.Canceled) != cancelCaller {
					t.Errorf("caller cancellation preserved=%t, want %t: %v", errors.Is(err, context.Canceled), cancelCaller, err)
				}
				if rawErr := conn.Raw(func(any) error { return nil }); rawErr != sql.ErrConnDone {
					t.Fatalf("expected native failure to close exact sql.Conn, got %v", rawErr)
				}
				if errors.Is(err, sql.ErrConnDone) {
					t.Errorf("unbinding disposed connection manufactured cleanup failure: %v", err)
				}
				if lease.Current() {
					t.Error("known-closed native connection still advertises current possession")
				}
				if session.AttachedForTest(lease) || retired.Load() != 1 {
					t.Errorf("closed possession not terminally drained: attached=%t retirements=%d", session.AttachedForTest(lease), retired.Load())
				}
				if err := lease.Release(context.Background()); err != nil || retired.Load() != 1 {
					t.Errorf("repeated release changed retirement: err=%v retirements=%d", err, retired.Load())
				}
				if got := dialer.closed.Load(); got != 1 {
					t.Errorf("native transport closes=%d, want 1", got)
				}
				t.Logf("native proof outcome: %v", err)
			})
		}
	}
}

func TestPostgresSessionScopeProofDisposesInvalidOpenConnection(t *testing.T) {
	for _, monitor := range []bool{false, true} {
		name := "prove"
		if monitor {
			name = "monitor"
		}
		t.Run(name, func(t *testing.T) {
			dsn, _, _ := testutil.StartPostgres(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			readFailure := errors.New("independent native proof read failure")
			closeFailure := errors.New("independent invalid-session close failure")
			dialer := &sessionScopeDialer{readFailure: readFailure, closeFailure: closeFailure}
			native, err := pq.NewConnector(dsn)
			if err != nil {
				t.Fatal(err)
			}
			native.Dialer(dialer)
			probe := &exitProbeConnector{closedSignal: make(chan struct{})}
			var observedInvalidOpen bool
			db := sql.OpenDB(&sessionScopeConnector{
				Connector: native, probe: probe,
				afterQuery: func(c driver.Conn, queryErr error) {
					if errors.Is(queryErr, readFailure) {
						// The native method has completed before owner cancellation.
						// net.OpError poisons pq without asking database/sql to close.
						observedInvalidOpen = !c.(driver.Validator).IsValid() && !errors.Is(queryErr, driver.ErrBadConn)
						cancel()
					}
				},
			})
			db.SetMaxOpenConns(1)
			defer db.Close()
			lease, acquired, err := AcquireAdvisoryLockLease(ctx, db, "invalid-open-proof")
			if err != nil || !acquired {
				t.Fatalf("acquire=%t err=%v", acquired, err)
			}
			t.Cleanup(func() { _ = lease.Release(context.Background()) })
			session := lease.Session()
			conn, err := session.connection()
			if err != nil {
				t.Fatal(err)
			}
			dialer.onFirstClose = func() {
				if session.operationMu.TryLock() {
					session.operationMu.Unlock()
					t.Error("invalid session disposal ran after operation ownership was released")
				}
			}
			var retired atomic.Int32
			lease.InstallTerminalOwner(nil, func() {
				retired.Add(1)
				if !session.operationMu.TryLock() {
					t.Error("lease retirement ran under the session operation lock")
					return
				}
				session.operationMu.Unlock()
			}, nil)
			dialer.failRead.Store(true)
			if monitor {
				err = lease.MonitorProveCurrent(ctx, time.Second)
			} else {
				err = lease.ProveCurrent(ctx)
			}
			if !observedInvalidOpen {
				t.Fatal("did not exercise native-invalid, sql.Conn-open outcome")
			}
			if !errors.Is(err, readFailure) || !errors.Is(err, context.Canceled) {
				t.Errorf("lost observable read or caller cancellation cause: %v", err)
			}
			if lease.Current() || session.AttachedForTest(lease) || retired.Load() != 1 {
				t.Errorf("invalid possession survived: current=%t attached=%t retired=%d", lease.Current(), session.AttachedForTest(lease), retired.Load())
			}
			if got := conn.Raw(func(any) error { return nil }); got != sql.ErrConnDone {
				t.Errorf("poisoned sql.Conn not physically disposed: %v", got)
			}
			if dialer.closed.Load() != 1 {
				t.Errorf("physical closes=%d, want 1", dialer.closed.Load())
			}
			if !lease.Current() {
				next, stop := context.WithTimeout(context.Background(), 3*time.Second)
				defer stop()
				var value int
				if err := db.QueryRowContext(next, "SELECT 1").Scan(&value); err != nil || value != 1 {
					t.Fatalf("pool1 successor failed: value=%d err=%v", value, err)
				}
			}
		})
	}
}

func TestPostgresSessionScopeProofDispositionPreservesObservableFailure(t *testing.T) {
	for _, monitor := range []bool{false, true} {
		for _, missing := range []bool{false, true} {
			name := "prove"
			if monitor {
				name = "monitor"
			}
			query := "SELECT 1 / 0"
			if missing {
				name += "/missing_possession"
				query = "SELECT false"
			} else {
				name += "/server_error"
			}
			t.Run(name, func(t *testing.T) {
				dsn, _, _ := testutil.StartPostgres(t)
				closeFailure := errors.New("independent proof-disposition close failure")
				dialer := &sessionScopeDialer{closeFailure: closeFailure}
				native, err := pq.NewConnector(dsn)
				if err != nil {
					t.Fatal(err)
				}
				native.Dialer(dialer)
				probe := &exitProbeConnector{closedSignal: make(chan struct{})}
				var drainedHealthy bool
				db := sql.OpenDB(&sessionScopeConnector{
					Connector: native, probe: probe, proofQuery: query,
					afterQuery: func(c driver.Conn, _ error) {
						drainedHealthy = c.(driver.Validator).IsValid()
					},
				})
				defer db.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				lease, acquired, err := AcquireAdvisoryLockLease(ctx, db, "proof-disposition-close")
				if err != nil || !acquired {
					t.Fatalf("acquire=%t err=%v", acquired, err)
				}
				t.Cleanup(func() { _ = lease.Release(context.Background()) })
				session := lease.Session()
				dialer.onFirstClose = func() {
					if session.operationMu.TryLock() {
						session.operationMu.Unlock()
						t.Error("proof disposition released operation ownership before physical close")
					}
				}
				var retired atomic.Int32
				lease.InstallTerminalOwner(nil, func() {
					retired.Add(1)
					if !session.operationMu.TryLock() {
						t.Error("disposition retired leases under operation lock")
						return
					}
					session.operationMu.Unlock()
				}, nil)
				if monitor {
					err = lease.MonitorProveCurrent(ctx, time.Second)
				} else {
					err = lease.ProveCurrent(ctx)
				}
				if !drainedHealthy {
					t.Fatal("did not exercise proof disposition after a healthy native drain")
				}
				if err == nil {
					t.Error("failed possession proof reported success")
				}
				if !missing {
					var server *pq.Error
					if !errors.As(err, &server) || server.Code != "22012" {
						t.Errorf("independent server failure lost: %v", err)
					}
				}
				if errors.Is(err, context.Canceled) || errors.Is(err, sql.ErrConnDone) {
					t.Errorf("manufactured cancellation/connection cleanup error: %v", err)
				}
				if lease.Current() || session.AttachedForTest(lease) || retired.Load() != 1 || dialer.closed.Load() != 1 {
					t.Errorf("proof disposal incomplete: current=%t retired=%d closes=%d", lease.Current(), retired.Load(), dialer.closed.Load())
				}
			})
		}
	}
}

type sessionScopeConnector struct {
	driver.Connector
	probe      *exitProbeConnector
	proofQuery string
	afterQuery func(driver.Conn, error)
}

func (c *sessionScopeConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	wrapped := &exitProbeConn{Conn: conn, probe: c.probe}
	if c.proofQuery != "" || c.afterQuery != nil {
		return &sessionScopeProofConn{exitProbeConn: wrapped, proofQuery: c.proofQuery, afterQuery: c.afterQuery}, nil
	}
	return wrapped, nil
}

type sessionScopeProofConn struct {
	*exitProbeConn
	proofQuery string
	afterQuery func(driver.Conn, error)
}

func (c *sessionScopeProofConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if query == retainedAdvisoryLockProofSQL && c.proofQuery != "" {
		query, args = c.proofQuery, nil
	}
	rows, err := c.exitProbeConn.QueryContext(ctx, query, args)
	if c.afterQuery != nil {
		c.afterQuery(c.Conn, err)
	}
	return rows, err
}

type sessionScopeDialer struct {
	closeFailure   error
	writeFailure   error
	readFailure    error
	onWriteFailure func()
	onFirstClose   func()
	failWrite      atomic.Bool
	failRead       atomic.Bool
	closed         atomic.Int32
}

func (d *sessionScopeDialer) Dial(network, address string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, address)
}

func (d *sessionScopeDialer) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return d.DialContext(ctx, network, address)
}

func (d *sessionScopeDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return &sessionScopeTransport{Conn: conn, dialer: d}, nil
}

type sessionScopeTransport struct {
	net.Conn
	dialer *sessionScopeDialer
}

func (c *sessionScopeTransport) Read(p []byte) (int, error) {
	if c.dialer.failRead.CompareAndSwap(true, false) {
		return 0, &net.OpError{Op: "read", Net: "tcp", Err: c.dialer.readFailure}
	}
	return c.Conn.Read(p)
}

func (c *sessionScopeTransport) Write(p []byte) (int, error) {
	if c.dialer.failWrite.CompareAndSwap(true, false) {
		if c.dialer.onWriteFailure != nil {
			c.dialer.onWriteFailure()
		}
		return 0, c.dialer.writeFailure
	}
	return c.Conn.Write(p)
}

func (c *sessionScopeTransport) Close() error {
	if c.dialer.closed.Add(1) == 1 && c.dialer.onFirstClose != nil {
		c.dialer.onFirstClose()
	}
	// Fail below pq.Close so the native scope, not a wrapper-side recorder, owns it.
	return errors.Join(c.Conn.Close(), c.dialer.closeFailure)
}
