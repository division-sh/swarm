package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

type AdvisoryLockLease struct {
	mu                       sync.Mutex
	session                  *SessionAuthority
	lockKey                  string
	releaseSession           func() error
	releaseCapacity          func()
	onTerminal               func()
	unlocked                 bool
	released                 bool
	testUnlock               func(context.Context, *SessionAuthority, string) (bool, error)
	testProve                func(context.Context, *SessionAuthority, string) (bool, error)
	testBeforeBeginOperation func()
}

type SessionAuthority struct {
	operationMu             sync.Mutex
	mu                      sync.Mutex
	conn                    *sql.Conn
	activeTx                *sql.Tx
	activeTxCancel          context.CancelFunc
	refs                    int
	closed                  bool
	discardPending          bool
	leases                  map[*AdvisoryLockLease]struct{}
	pendingLeaseRetirements []*AdvisoryLockLease
	cancelCurrentOperation  func() error
	testBeginTx             func(context.Context, *sql.Conn) (*sql.Tx, error)
	testEndTxError          func() error
}

type sessionDiscard struct {
	leases []*AdvisoryLockLease
	err    error
}

func (d sessionDiscard) drain() error {
	for _, lease := range d.leases {
		lease.retireFromSession()
	}
	return d.err
}

func newSessionAuthority(conn *sql.Conn) *SessionAuthority {
	return &SessionAuthority{
		conn:   conn,
		refs:   1,
		leases: map[*AdvisoryLockLease]struct{}{},
	}
}

func NewSessionAuthority(conn *sql.Conn) (*SessionAuthority, error) {
	if conn == nil {
		return nil, errors.New("PostgreSQL connection is required")
	}
	return newSessionAuthority(conn), nil
}

func (a *SessionAuthority) connection() (*sql.Conn, error) {
	if a == nil {
		return nil, errors.New("PostgreSQL session authority is missing")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.discardPending || a.conn == nil {
		return nil, errors.New("PostgreSQL session authority is closed")
	}
	return a.conn, nil
}

func (a *SessionAuthority) attach(lease *AdvisoryLockLease) error {
	if a == nil || lease == nil {
		return errors.New("PostgreSQL session lease is required")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.discardPending || a.conn == nil {
		return errors.New("PostgreSQL session authority is closed")
	}
	a.leases[lease] = struct{}{}
	return nil
}

func (a *SessionAuthority) detach(lease *AdvisoryLockLease) {
	if a == nil || lease == nil {
		return
	}
	a.mu.Lock()
	delete(a.leases, lease)
	a.mu.Unlock()
}

func (a *SessionAuthority) retain() (func() error, bool) {
	if a == nil {
		return nil, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.discardPending || a.conn == nil {
		return nil, false
	}
	a.refs++
	var (
		mu   sync.Mutex
		done bool
	)
	return func() error {
		mu.Lock()
		defer mu.Unlock()
		if done {
			return nil
		}
		if err := a.release(); err != nil {
			return err
		}
		done = true
		return nil
	}, true
}

func (a *SessionAuthority) release() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	if a.closed || a.refs <= 0 {
		a.mu.Unlock()
		return nil
	}
	if a.refs > 1 {
		a.refs--
		a.mu.Unlock()
		return nil
	}
	if a.activeTx != nil {
		a.mu.Unlock()
		return errors.New("close PostgreSQL session authority while its transaction is active")
	}
	if a.discardPending {
		err := a.forceDiscardConnectionLocked()
		a.mu.Unlock()
		return err
	}
	if a.conn == nil {
		a.closed = true
		a.refs = 0
		a.mu.Unlock()
		return nil
	}
	if err := a.conn.Close(); err != nil {
		a.mu.Unlock()
		return err
	}
	a.conn = nil
	a.refs = 0
	a.closed = true
	a.mu.Unlock()
	return nil
}

func (a *SessionAuthority) forceDiscard() error {
	return a.prepareDiscardExcept(nil).drain()
}

func (a *SessionAuthority) prepareDiscardExcept(except *AdvisoryLockLease) sessionDiscard {
	if a == nil {
		return sessionDiscard{}
	}
	a.mu.Lock()
	a.discardPending = true
	leases := make([]*AdvisoryLockLease, 0, len(a.leases))
	for lease := range a.leases {
		delete(a.leases, lease)
		if lease != except {
			leases = append(leases, lease)
		}
	}
	if a.activeTx != nil {
		a.pendingLeaseRetirements = append(a.pendingLeaseRetirements, leases...)
		a.mu.Unlock()
		return sessionDiscard{}
	}
	leases = append(leases, a.takePendingLeaseRetirementsLocked()...)
	var err error
	if !a.closed && a.conn != nil && a.activeTx == nil {
		err = a.forceDiscardConnectionLocked()
	}
	a.mu.Unlock()
	return sessionDiscard{leases: leases, err: err}
}

func (a *SessionAuthority) takePendingLeaseRetirementsLocked() []*AdvisoryLockLease {
	retirements := a.pendingLeaseRetirements
	a.pendingLeaseRetirements = nil
	return retirements
}

func (a *SessionAuthority) forceDiscardConnectionLocked() error {
	rawErr := a.conn.Raw(func(any) error { return driver.ErrBadConn })
	if errors.Is(rawErr, driver.ErrBadConn) {
		rawErr = nil
	}
	closeErr := a.conn.Close()
	a.conn = nil
	a.refs = 0
	a.closed = true
	if rawErr != nil || (closeErr != nil && !errors.Is(closeErr, sql.ErrConnDone)) {
		return errors.Join(rawErr, closeErr)
	}
	return nil
}

func (a *SessionAuthority) beginTx(ctx context.Context) (*sql.Tx, error) {
	if a == nil {
		return nil, errors.New("PostgreSQL session authority is missing")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	a.operationMu.Lock()
	a.mu.Lock()
	if a.closed || a.conn == nil {
		a.mu.Unlock()
		a.operationMu.Unlock()
		return nil, errors.New("PostgreSQL session authority is closed")
	}
	if a.activeTx != nil {
		a.mu.Unlock()
		a.operationMu.Unlock()
		return nil, errors.New("PostgreSQL session authority already has an active transaction")
	}
	conn := a.conn
	cancelCurrent := a.cancelCurrentOperation
	begin := a.testBeginTx
	if begin == nil {
		begin = func(beginCtx context.Context, conn *sql.Conn) (*sql.Tx, error) {
			return conn.BeginTx(beginCtx, nil)
		}
	}
	// Startup is caller-cancellable, but a successfully retained transaction is
	// detached and remains owned by this authority until explicit settlement.
	beginCtx := ctx
	var (
		cancelBegin context.CancelFunc
		completed   chan struct{}
		cancelDone  chan error
	)
	if cancelCurrent != nil && ctx.Done() != nil {
		beginCtx, cancelBegin = context.WithCancel(context.Background())
		completed = make(chan struct{})
		cancelDone = make(chan error, 1)
		go func() {
			select {
			case <-ctx.Done():
				cancelBegin()
				cancelDone <- cancelCurrent()
			case <-completed:
				cancelDone <- nil
			}
		}()
	}
	tx, err := begin(beginCtx, conn)
	if completed != nil {
		close(completed)
		err = errors.Join(err, <-cancelDone)
	}
	if callerErr := contextError(ctx); callerErr != nil {
		err = errors.Join(callerErr, err)
	}
	if err == nil && tx == nil {
		err = errors.New("PostgreSQL session transaction start returned no transaction")
	}
	if err != nil {
		if tx != nil {
			a.activeTx = tx
			a.activeTxCancel = cancelBegin
		} else if cancelBegin != nil {
			cancelBegin()
		}
		a.mu.Unlock()
		if tx != nil {
			return nil, errors.Join(err, rollbackSessionTransaction(tx, a))
		}
		a.operationMu.Unlock()
		if callerErr := contextError(ctx); callerErr != nil {
			return nil, err
		}
		discard := a.prepareDiscardExcept(nil)
		return nil, errors.Join(err, wrapAdvisoryDiscardError(discard.drain()))
	}
	a.activeTx = tx
	a.activeTxCancel = cancelBegin
	a.mu.Unlock()
	return tx, nil
}

func (a *SessionAuthority) endTx(tx *sql.Tx) error {
	if a == nil || tx == nil {
		return errors.New("PostgreSQL session transaction is missing")
	}
	a.mu.Lock()
	if a.activeTx != tx {
		a.mu.Unlock()
		return errors.New("PostgreSQL transaction does not match private session authority")
	}
	a.activeTx = nil
	cancelTx := a.activeTxCancel
	a.activeTxCancel = nil
	var err error
	if a.discardPending && !a.closed && a.conn != nil {
		err = a.forceDiscardConnectionLocked()
	}
	retirements := a.takePendingLeaseRetirementsLocked()
	testEndTxError := a.testEndTxError
	a.mu.Unlock()
	if cancelTx != nil {
		cancelTx()
	}
	a.operationMu.Unlock()
	sessionDiscard{leases: retirements}.drain()
	if testEndTxError != nil {
		err = errors.Join(err, testEndTxError())
	}
	return err
}

func (a *SessionAuthority) beginOperation() (func(), error) {
	if a == nil {
		return nil, errors.New("PostgreSQL session authority is missing")
	}
	a.operationMu.Lock()
	a.mu.Lock()
	if a.closed || a.discardPending || a.conn == nil {
		a.mu.Unlock()
		a.operationMu.Unlock()
		return nil, errors.New("PostgreSQL session authority is closed")
	}
	a.mu.Unlock()
	return a.operationMu.Unlock, nil
}

func (a *SessionAuthority) queryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	a.mu.Lock()
	tx := a.activeTx
	conn := a.conn
	a.mu.Unlock()
	if tx != nil {
		return tx.QueryRowContext(ctx, query, args...)
	}
	return conn.QueryRowContext(ctx, query, args...)
}

func (a *SessionAuthority) runWithCallerCancellation(ctx context.Context, run func(context.Context) error) error {
	a.mu.Lock()
	cancelCurrent := a.cancelCurrentOperation
	a.mu.Unlock()
	return runWithIndependentCallerCancellation(ctx, cancelCurrent, run)
}

func runWithIndependentCallerCancellation(ctx context.Context, cancelCurrent func() error, run func(context.Context) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if cancelCurrent == nil || ctx.Done() == nil {
		return run(ctx)
	}

	completed := make(chan struct{})
	cancelDone := make(chan error, 1)
	go func() {
		select {
		case <-ctx.Done():
			cancelDone <- cancelCurrent()
		case <-completed:
			cancelDone <- nil
		}
	}()
	runErr := run(context.WithoutCancel(ctx))
	close(completed)
	cancelErr := <-cancelDone
	if callerErr := ctx.Err(); callerErr != nil {
		return errors.Join(callerErr, cancelErr)
	}
	return errors.Join(runErr, cancelErr)
}

func (a *SessionAuthority) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	a.mu.Lock()
	tx := a.activeTx
	conn := a.conn
	a.mu.Unlock()
	if tx != nil {
		return tx.QueryContext(ctx, query, args...)
	}
	return conn.QueryContext(ctx, query, args...)
}

func (a *SessionAuthority) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	a.mu.Lock()
	tx := a.activeTx
	conn := a.conn
	a.mu.Unlock()
	if tx != nil {
		return tx.ExecContext(ctx, query, args...)
	}
	if conn == nil {
		return nil, errors.New("PostgreSQL session authority is closed")
	}
	return conn.ExecContext(ctx, query, args...)
}

func (a *SessionAuthority) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return a.queryRowContext(ctx, query, args...)
}

func (l *AdvisoryLockLease) Release(ctx context.Context) error {
	return l.releaseWithRetirement(ctx)
}

func (l *AdvisoryLockLease) releaseWithRetirement(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return nil
	}
	if l.session == nil {
		actions := l.retireLocked()
		l.mu.Unlock()
		actions.run()
		return errors.New("advisory lock lease has no private session authority")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = context.WithoutCancel(ctx)
	session := l.session
	if l.testBeforeBeginOperation != nil {
		l.testBeforeBeginOperation()
	}
	endOperation, operationErr := session.beginOperation()
	if operationErr != nil {
		discard := session.prepareDiscardExcept(l)
		actions := l.retireLocked()
		l.mu.Unlock()
		actions.run()
		return errors.Join(
			fmt.Errorf("release advisory lock: %w", operationErr),
			wrapAdvisoryDiscardError(discard.drain()),
		)
	}
	terminalDiscard := func(releaseErr error) error {
		discard := session.prepareDiscardExcept(l)
		actions := l.retireLocked()
		l.mu.Unlock()
		endOperation()
		actions.run()
		return errors.Join(releaseErr, wrapAdvisoryDiscardError(discard.drain()))
	}
	if !l.unlocked {
		var (
			unlocked bool
			err      error
		)
		if l.testUnlock != nil {
			unlocked, err = l.testUnlock(ctx, session, l.lockKey)
		} else {
			err = session.queryRowContext(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, l.lockKey).Scan(&unlocked)
		}
		if err != nil {
			releaseErr := fmt.Errorf("release advisory lock: %w", err)
			return terminalDiscard(releaseErr)
		}
		if !unlocked {
			releaseErr := errors.New("release advisory lock: PostgreSQL session did not own the lock")
			return terminalDiscard(releaseErr)
		}
		l.unlocked = true
	}
	if l.releaseSession != nil {
		if err := l.releaseSession(); err != nil {
			closeErr := fmt.Errorf("close advisory lock session: %w", err)
			return terminalDiscard(closeErr)
		}
	}
	session.detach(l)
	actions := l.retireLocked()
	l.mu.Unlock()
	endOperation()
	actions.run()
	return nil
}

func wrapAdvisoryDiscardError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("discard advisory lock session: %w", err)
}

type leaseRetirement struct {
	releaseCapacity func()
	onTerminal      func()
}

func (r leaseRetirement) run() {
	if r.releaseCapacity != nil {
		r.releaseCapacity()
	}
	if r.onTerminal != nil {
		r.onTerminal()
	}
}

func (l *AdvisoryLockLease) retireLocked() leaseRetirement {
	actions := leaseRetirement{
		releaseCapacity: l.releaseCapacity,
		onTerminal:      l.onTerminal,
	}
	l.releaseCapacity = nil
	l.onTerminal = nil
	l.releaseSession = nil
	l.released = true
	return actions
}

func (l *AdvisoryLockLease) retireFromSession() {
	if l == nil {
		return
	}
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return
	}
	actions := l.retireLocked()
	l.mu.Unlock()
	actions.run()
}

func (l *AdvisoryLockLease) installTerminalOwner(
	releaseCapacity func(),
	onTerminal func(),
	register func(),
) bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released || l.session == nil {
		return false
	}
	l.releaseCapacity = releaseCapacity
	l.onTerminal = onTerminal
	if register != nil {
		register()
	}
	return true
}

func (l *AdvisoryLockLease) current() bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	session := l.session
	current := !l.released && session != nil
	l.mu.Unlock()
	if !current {
		return false
	}
	_, err := session.connection()
	return err == nil
}

func AcquireAdvisoryLockLease(ctx context.Context, db ConnectionOwner, lockKey string) (*AdvisoryLockLease, bool, error) {
	return AcquireAdvisoryLockLeaseWith(ctx, db, lockKey, nil)
}

type AdvisoryLockAcquire func(context.Context, *SessionAuthority, string) (bool, error)

type ConnectionOwner interface {
	Conn(context.Context) (*sql.Conn, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func AcquireAdvisoryLockLeaseWith(
	ctx context.Context,
	db ConnectionOwner,
	lockKey string,
	acquire AdvisoryLockAcquire,
) (*AdvisoryLockLease, bool, error) {
	if db == nil {
		return nil, false, nil
	}
	lockKey = strings.TrimSpace(lockKey)
	if lockKey == "" {
		return nil, false, fmt.Errorf("advisory lock key is required")
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("acquire advisory lock connection: %w", err)
	}
	authority := newSessionAuthority(conn)
	if err := authority.installCancellation(ctx, db); err != nil {
		return nil, false, errors.Join(err, authority.release())
	}
	return AcquireAdvisoryLockLeaseOnSession(ctx, authority, lockKey, acquire, authority.release)
}

func (a *SessionAuthority) installCancellation(ctx context.Context, db ConnectionOwner) error {
	if a == nil || db == nil {
		return errors.New("PostgreSQL session cancellation authority is required")
	}
	var backendPID int
	if err := a.queryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&backendPID); err != nil {
		return fmt.Errorf("load retained PostgreSQL backend identity: %w", err)
	}
	a.mu.Lock()
	a.cancelCurrentOperation = func() error {
		cancelCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var cancelled bool
		if err := db.QueryRowContext(cancelCtx, `SELECT pg_cancel_backend($1)`, backendPID).Scan(&cancelled); err != nil {
			return fmt.Errorf("cancel retained PostgreSQL operation: %w", err)
		}
		return nil
	}
	a.mu.Unlock()
	return nil
}

func AcquireAdvisoryLockLeaseOnSession(
	ctx context.Context,
	authority *SessionAuthority,
	lockKey string,
	acquire AdvisoryLockAcquire,
	releaseSession func() error,
) (*AdvisoryLockLease, bool, error) {
	if authority == nil || releaseSession == nil {
		return nil, false, errors.New("acquire advisory lock requires private session authority")
	}
	var acquired bool
	var err error
	if acquire != nil {
		acquired, err = acquire(ctx, authority, lockKey)
	} else {
		err = authority.queryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, lockKey).Scan(&acquired)
	}
	if err != nil {
		return nil, false, errors.Join(
			fmt.Errorf("acquire advisory lock: %w", err),
			wrapAdvisoryDiscardError(authority.forceDiscard()),
			releaseSession(),
		)
	}
	if !acquired {
		if err := releaseSession(); err != nil {
			return nil, false, fmt.Errorf("release unacquired advisory lock session: %w", err)
		}
		return nil, false, nil
	}
	lease := &AdvisoryLockLease{
		session:        authority,
		lockKey:        lockKey,
		releaseSession: releaseSession,
	}
	if err := authority.attach(lease); err != nil {
		return nil, false, errors.Join(
			fmt.Errorf("attach advisory lock lease: %w", err),
			wrapAdvisoryDiscardError(authority.forceDiscard()),
			releaseSession(),
		)
	}
	return lease, true, nil
}

func RunSessionTransaction(
	ctx context.Context,
	db ConnectionOwner,
	fn func(context.Context, *sql.Tx) error,
) (err error) {
	if db == nil {
		return errors.New("PostgreSQL store is required")
	}
	if fn == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	session := newSessionAuthority(conn)
	defer func() {
		err = errors.Join(err, session.release())
	}()
	return RunAuthorityTransaction(ctx, session, fn)
}

func RunAuthorityTransaction(
	ctx context.Context,
	session *SessionAuthority,
	fn func(context.Context, *sql.Tx) error,
) error {
	if session == nil {
		return errors.New("PostgreSQL session authority is required")
	}
	tx, err := session.beginTx(ctx)
	if err != nil {
		return err
	}
	if runErr := session.runWithCallerCancellation(ctx, func(operationCtx context.Context) error {
		return fn(operationCtx, tx)
	}); runErr != nil {
		rollbackErr := rollbackSessionTransaction(tx, session)
		return errors.Join(runErr, rollbackErr)
	}
	if callerErr := contextError(ctx); callerErr != nil {
		return errors.Join(callerErr, rollbackSessionTransaction(tx, session))
	}
	if commitErr := tx.Commit(); commitErr != nil {
		if callerErr := contextError(ctx); callerErr != nil {
			return errors.Join(callerErr, rollbackSessionTransaction(tx, session))
		}
		endErr := session.endTx(tx)
		discardErr := session.forceDiscard()
		return errors.Join(commitErr, endErr, wrapAdvisoryDiscardError(discardErr))
	}
	if endErr := session.endTx(tx); endErr != nil {
		return errors.Join(endErr, wrapAdvisoryDiscardError(session.forceDiscard()))
	}
	return nil
}

func (l *AdvisoryLockLease) RunTransaction(
	ctx context.Context,
	fn func(context.Context, *sql.Tx) error,
) error {
	if l == nil {
		return errors.New("advisory lock lease has no current PostgreSQL session")
	}
	l.mu.Lock()
	session := l.session
	current := !l.released && session != nil
	l.mu.Unlock()
	if !current {
		return errors.New("advisory lock lease has no current PostgreSQL session")
	}
	return RunAuthorityTransaction(ctx, session, fn)
}

func rollbackSessionTransaction(tx *sql.Tx, session *SessionAuthority) error {
	if tx == nil || session == nil {
		return errors.New("PostgreSQL session transaction is missing")
	}
	rollbackErr := tx.Rollback()
	if errors.Is(rollbackErr, sql.ErrTxDone) {
		rollbackErr = nil
	}
	endErr := session.endTx(tx)
	if rollbackErr == nil && endErr == nil {
		return nil
	}
	return errors.Join(rollbackErr, endErr, wrapAdvisoryDiscardError(session.forceDiscard()))
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

const retainedAdvisoryLockProofSQL = `
	SELECT EXISTS (
		SELECT 1
		FROM pg_locks
		WHERE locktype = 'advisory'
		  AND pid = pg_backend_pid()
		  AND granted
		  AND classid::bigint = CASE WHEN hashtext($1) < 0 THEN 4294967295::bigint ELSE 0::bigint END
		  AND objid::bigint = (hashtext($1)::bigint & 4294967295::bigint)
		  AND objsubid = 1
	)
`

// ProveCurrent verifies the exact advisory lock on the retained session.
// Session liveness alone is not possession evidence.
func (l *AdvisoryLockLease) ProveCurrent(ctx context.Context) error {
	if l == nil {
		return errors.New("advisory lock lease has no current PostgreSQL session")
	}
	l.mu.Lock()
	session := l.session
	current := !l.released && session != nil
	l.mu.Unlock()
	if !current {
		return errors.New("advisory lock lease has no current PostgreSQL session")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	endOperation, err := session.beginOperation()
	if err != nil {
		discard := session.prepareDiscardExcept(nil)
		return errors.Join(err, wrapAdvisoryDiscardError(discard.drain()))
	}
	var held bool
	err = session.runWithCallerCancellation(ctx, func(operationCtx context.Context) error {
		if l.testProve != nil {
			var proveErr error
			held, proveErr = l.testProve(operationCtx, session, l.lockKey)
			return proveErr
		}
		return session.queryRowContext(operationCtx, retainedAdvisoryLockProofSQL, l.lockKey).Scan(&held)
	})
	endOperation()
	if callerErr := contextError(ctx); callerErr != nil {
		return callerErr
	}
	if err == nil && held {
		return nil
	}
	discard := session.prepareDiscardExcept(nil)
	if err == nil {
		err = errors.New("retained PostgreSQL session no longer owns its advisory lock")
	}
	return errors.Join(err, wrapAdvisoryDiscardError(discard.drain()))
}

// MonitorProveCurrent starts its deadline only after the exact retained
// session-operation boundary is held. Waiting behind canonical local work is
// therefore neutral and cannot be misclassified as possession loss.
func (l *AdvisoryLockLease) MonitorProveCurrent(ctx context.Context, deadline time.Duration) error {
	if l == nil {
		return errors.New("advisory lock lease has no current PostgreSQL session")
	}
	if deadline <= 0 {
		return errors.New("PostgreSQL possession monitor deadline must be positive")
	}
	l.mu.Lock()
	session := l.session
	current := !l.released && session != nil
	l.mu.Unlock()
	if !current {
		return errors.New("advisory lock lease has no current PostgreSQL session")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	endOperation, err := session.beginOperation()
	if err != nil {
		discard := session.prepareDiscardExcept(nil)
		return errors.Join(err, wrapAdvisoryDiscardError(discard.drain()))
	}
	if err := ctx.Err(); err != nil {
		endOperation()
		return err
	}
	probeCtx, cancel := context.WithTimeout(ctx, deadline)
	var held bool
	err = session.runWithCallerCancellation(probeCtx, func(operationCtx context.Context) error {
		return session.queryRowContext(operationCtx, retainedAdvisoryLockProofSQL, l.lockKey).Scan(&held)
	})
	cancel()
	endOperation()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err == nil && held {
		return nil
	}
	discard := session.prepareDiscardExcept(nil)
	if err == nil {
		err = errors.New("retained PostgreSQL session no longer owns its advisory lock")
	}
	return errors.Join(err, wrapAdvisoryDiscardError(discard.drain()))
}

func RollbackAuthorityTransaction(tx *sql.Tx, session *SessionAuthority) error {
	return rollbackSessionTransaction(tx, session)
}

func (l *AdvisoryLockLease) Session() *SessionAuthority {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return nil
	}
	return l.session
}

func (l *AdvisoryLockLease) Current() bool { return l.current() }

func (l *AdvisoryLockLease) ReleaseTerminal(ctx context.Context) error {
	return l.releaseWithRetirement(ctx)
}

func (l *AdvisoryLockLease) InstallTerminalOwner(releaseCapacity, onTerminal, register func()) bool {
	return l.installTerminalOwner(releaseCapacity, onTerminal, register)
}

func (a *SessionAuthority) BeginTx(ctx context.Context) (*sql.Tx, error) { return a.beginTx(ctx) }
func (a *SessionAuthority) EndTx(tx *sql.Tx) error                       { return a.endTx(tx) }
func (a *SessionAuthority) Retain() (func() error, bool)                 { return a.retain() }
func (a *SessionAuthority) ForceDiscard() error                          { return a.forceDiscard() }
func (a *SessionAuthority) Release() error                               { return a.release() }

func (a *SessionAuthority) SetEndTxErrorForTest(hook func() error) {
	if a == nil {
		return
	}
	a.mu.Lock()
	a.testEndTxError = hook
	a.mu.Unlock()
}

func (l *AdvisoryLockLease) SetUnlockForTest(hook func(context.Context, *SessionAuthority, string) (bool, error)) {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.testUnlock = hook
	l.mu.Unlock()
}

func (l *AdvisoryLockLease) SetProveForTest(hook func(context.Context, *SessionAuthority, string) (bool, error)) {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.testProve = hook
	l.mu.Unlock()
}

func (l *AdvisoryLockLease) SetBeforeBeginOperationForTest(hook func()) {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.testBeforeBeginOperation = hook
	l.mu.Unlock()
}

func (l *AdvisoryLockLease) SetReleaseSessionForTest(hook func() error) {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.releaseSession = hook
	l.mu.Unlock()
}

func (a *SessionAuthority) AttachedForTest(lease *AdvisoryLockLease) bool {
	if a == nil || lease == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := a.leases[lease]
	return ok
}
