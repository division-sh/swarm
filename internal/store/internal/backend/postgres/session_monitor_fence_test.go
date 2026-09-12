package postgres

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testutil"
	"github.com/lib/pq"
)

func TestSessionMonitorFenceBeforeSilentProofDrain(t *testing.T) {
	for _, phase := range []string{"deadline", "cancel", "complete"} {
		t.Run(phase, func(t *testing.T) {
			backend, _ := newExitProbe(t)
			backend.db.SetMaxOpenConns(2)
			lease, held, err := AcquireAdvisoryLockLease(context.Background(), backend.db, "monitor-silence")
			if err != nil || !held {
				t.Fatalf("acquire held=%t: %v", held, err)
			}
			defer lease.Release(context.Background())
			session := lease.Session()
			entered, resume, fenced := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var resumeOnce, fenceOnce sync.Once
			unblock := func() { resumeOnce.Do(func() { close(resume) }) }
			defer unblock()
			if err := lease.InstallLocalFenceOwner(func() { fenceOnce.Do(func() { close(fenced) }) }); err != nil {
				t.Fatal(err)
			}
			lease.SetProveForTest(func(ctx context.Context, session *SessionAuthority, key string) (bool, error) {
				if ctx.Done() != nil {
					t.Error("monitor detached neither logical cancellation nor its local proof budget")
				}
				close(entered)
				<-resume
				var held bool
				err := session.queryRowContext(ctx, retainedAdvisoryLockProofSQL, key).Scan(&held)
				return held, err
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- lease.MonitorProveCurrent(ctx, 40*time.Millisecond) }()
			<-entered
			if phase == "deadline" {
				select {
				case <-fenced:
				case <-time.After(time.Second):
					t.Fatal("local fencing waited for stalled SQL")
				}
				cancel() // A later monitor stop cannot erase the deadline decision.
				if lease.Current() {
					t.Fatal("late SQL retained current authority")
				}
				if release, ok := session.Retain(); ok {
					_ = release()
					t.Fatal("local fence admitted another retained owner")
				}
				if _, err := session.beginTx(context.Background()); err == nil {
					t.Fatal("fenced session admitted a successor transaction")
				}
				var independentlyHeld bool
				observer, err := backend.db.Conn(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				defer observer.Close()
				if err := observer.QueryRowContext(context.Background(), "SELECT pg_try_advisory_lock(hashtext($1))", "monitor-silence").Scan(&independentlyHeld); err != nil || independentlyHeld {
					t.Fatalf("local fence falsely implied remote release: held=%t, %v", independentlyHeld, err)
				}
			} else if phase == "cancel" {
				cancel()
				time.Sleep(80 * time.Millisecond)
				select {
				case <-fenced:
					t.Fatal("ordinary monitor cancellation fenced healthy authority")
				default:
				}
			}
			select {
			case err := <-done:
				t.Fatalf("proof returned before its SQL/result cleanup joined: %v", err)
			default:
			}
			unblock()
			err = <-done
			if phase == "deadline" {
				if !errors.Is(err, context.DeadlineExceeded) || lease.Current() {
					t.Fatalf("late healthy proof erased terminal deadline: %v", err)
				}
			} else {
				if phase == "cancel" && !errors.Is(err, context.Canceled) {
					t.Fatalf("ordinary cancellation missing: %v", err)
				}
				if phase == "complete" && err != nil {
					t.Fatal(err)
				}
				if !lease.Current() {
					t.Fatal("healthy joined proof lost authority")
				}
			}
		})
	}
}

type monitorSilentDialer struct {
	net.Dialer
	armed           atomic.Bool
	entered, resume chan struct{}
}

func (d *monitorSilentDialer) Dial(network, address string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, address)
}

func (d *monitorSilentDialer) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return d.DialContext(ctx, network, address)
}

func (d *monitorSilentDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	conn, err := d.Dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return &monitorSilentConn{Conn: conn, dialer: d}, nil
}

type monitorSilentConn struct {
	net.Conn
	dialer *monitorSilentDialer
}

func (c *monitorSilentConn) Read(p []byte) (int, error) {
	if c.dialer.armed.CompareAndSwap(true, false) {
		close(c.dialer.entered)
		<-c.dialer.resume
	}
	return c.Conn.Read(p)
}

func TestSessionMonitorSilentSocketFencesBeforeJoinedRelease(t *testing.T) {
	dsn, observer, _ := testutil.StartPostgres(t)
	dialer := &monitorSilentDialer{entered: make(chan struct{}), resume: make(chan struct{})}
	connector, err := pq.NewConnector(dsn)
	if err != nil {
		t.Fatal(err)
	}
	connector.Dialer(dialer)
	db := sql.OpenDB(connector)
	defer db.Close()
	lease, held, err := AcquireAdvisoryLockLease(context.Background(), db, "silent-monitor-io")
	if err != nil || !held {
		t.Fatalf("acquire held=%t: %v", held, err)
	}
	defer lease.Release(context.Background())
	var unblockOnce sync.Once
	unblock := func() { unblockOnce.Do(func() { close(dialer.resume) }) }
	defer unblock()
	fenced := make(chan struct{})
	var once sync.Once
	if err := lease.InstallLocalFenceOwner(func() { once.Do(func() { close(fenced) }) }); err != nil {
		t.Fatal(err)
	}
	var retired atomic.Bool
	lease.InstallTerminalOwner(nil, func() { retired.Store(true) }, nil)
	dialer.armed.Store(true)
	proofDone := make(chan error, 1)
	go func() { proofDone <- lease.MonitorProveCurrent(context.Background(), 40*time.Millisecond) }()
	<-dialer.entered
	select {
	case <-fenced:
	case <-time.After(time.Second):
		t.Fatal("local fencing waited for silent native socket read")
	}
	if retired.Load() {
		t.Fatal("local fencing fabricated physical retirement")
	}
	releaseDone := make(chan error, 1)
	go func() { releaseDone <- lease.Release(context.Background()) }()
	select {
	case err := <-releaseDone:
		t.Fatalf("release failed to join active proof: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
	var remoteHeld bool
	if err := observer.QueryRow("SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objid=(hashtext('silent-monitor-io')::bigint & 4294967295)::oid AND granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))").Scan(&remoteHeld); err != nil || !remoteHeld {
		t.Fatalf("local fence incorrectly implied remote release: held=%t err=%v", remoteHeld, err)
	}
	unblock()
	if err := <-proofDone; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late socket result erased deadline: %v", err)
	}
	<-releaseDone
	if !retired.Load() {
		t.Fatal("physical cleanup did not join terminal retirement")
	}
	if lease.Current() {
		t.Fatal("late socket result revived authority")
	}
}

func TestSessionMonitorBudgetStartsAfterAdmittedTransaction(t *testing.T) {
	backend, _ := newExitProbe(t)
	lease, held, err := AcquireAdvisoryLockLease(context.Background(), backend.db, "monitor-admitted-work")
	if err != nil || !held {
		t.Fatalf("acquire held=%t: %v", held, err)
	}
	defer lease.Release(context.Background())
	session := lease.Session()
	tx, err := session.beginTx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if tx != nil {
			_ = rollbackSessionTransaction(tx, session)
		}
	}()
	done := make(chan error, 1)
	go func() { done <- lease.MonitorProveCurrent(context.Background(), 40*time.Millisecond) }()
	time.Sleep(100 * time.Millisecond)
	if session.fenced.Load() {
		t.Fatal("time behind admitted work consumed independent proof budget")
	}
	if _, err := tx.ExecContext(context.Background(), "SELECT pg_sleep(0.06)"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := session.endTx(tx); err != nil {
		t.Fatal(err)
	}
	tx = nil
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := lease.RunTransaction(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "SELECT 1")
		return err
	}); err != nil {
		t.Fatalf("healthy successor after serialized proof: %v", err)
	}
}
