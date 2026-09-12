package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
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
	fenceMu                 sync.Mutex
	fenced                  atomic.Bool
	fenceOwners             map[*AdvisoryLockLease]func()
	mu                      sync.Mutex
	conn                    *sql.Conn
	activeTx                *sql.Tx
	refs                    int
	closed                  bool
	discardPending          bool
	leases                  map[*AdvisoryLockLease]struct{}
	pendingLeaseRetirements []*AdvisoryLockLease
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
	if a.fenced.Load() {
		return nil, errors.New("PostgreSQL session authority is fenced")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.fenced.Load() || a.closed || a.discardPending || a.conn == nil {
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
	if a.fenced.Load() || a.closed || a.discardPending || a.conn == nil {
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
	a.fenceMu.Lock()
	delete(a.fenceOwners, lease)
	a.fenceMu.Unlock()
}

// fenceLocal withdraws authority without waiting for SQL or physical disposal.
// Registered callbacks belong to the existing capability and must not do I/O.
func (a *SessionAuthority) fenceLocal() {
	if a == nil {
		return
	}
	a.fenced.Store(true)
	a.fenceMu.Lock()
	owners := make([]func(), 0, len(a.fenceOwners))
	for _, owner := range a.fenceOwners {
		owners = append(owners, owner)
	}
	a.fenceMu.Unlock()
	for _, owner := range owners {
		owner()
	}
}

// InstallLocalFenceOwner registers the same capability's nonblocking safety
// notification separately from its joined physical-retirement/readback callback.
func (l *AdvisoryLockLease) InstallLocalFenceOwner(owner func()) error {
	if l == nil || owner == nil {
		return errors.New("advisory lock local fence owner is required")
	}
	l.mu.Lock()
	a := l.session
	current := !l.released && a != nil
	l.mu.Unlock()
	if !current {
		return errors.New("advisory lock lease has no current PostgreSQL session")
	}
	a.fenceMu.Lock()
	if a.fenceOwners == nil {
		a.fenceOwners = make(map[*AdvisoryLockLease]func())
	}
	a.fenceOwners[l] = owner
	fenced := a.fenced.Load()
	a.fenceMu.Unlock()
	if fenced {
		owner()
	}
	return nil
}

func (a *SessionAuthority) retain() (func() error, bool) {
	if a == nil || a.fenced.Load() {
		return nil, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.fenced.Load() || a.closed || a.discardPending || a.conn == nil {
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
	a.fenceLocal()
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
	if rawErr == driver.ErrBadConn || rawErr == sql.ErrConnDone {
		rawErr = nil
	}
	closeErr := a.conn.Close()
	if closeErr == sql.ErrConnDone {
		closeErr = nil
	}
	a.conn = nil
	a.refs = 0
	a.closed = true
	return errors.Join(rawErr, closeErr)
}

func (a *SessionAuthority) beginTx(ctx context.Context) (*sql.Tx, error) {
	return a.beginTxOptions(ctx, nil)
}

func (a *SessionAuthority) beginTxOptions(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	if a == nil || a.fenced.Load() {
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
	if a.fenced.Load() || a.closed || a.discardPending || a.conn == nil {
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
	if err := ctx.Err(); err != nil {
		a.mu.Unlock()
		a.operationMu.Unlock()
		return nil, err
	}
	begin := a.testBeginTx
	if begin == nil {
		begin = func(beginCtx context.Context, conn *sql.Conn) (*sql.Tx, error) {
			return conn.BeginTx(beginCtx, opts)
		}
	}
	// Investigation only: the existing owner settles admitted SQL synchronously.
	beginCtx := context.WithoutCancel(ctx)
	tx, err := begin(beginCtx, conn)
	if callerErr := contextError(ctx); callerErr != nil {
		err = errors.Join(callerErr, err)
	}
	if err == nil && tx == nil {
		err = errors.New("PostgreSQL session transaction start returned no transaction")
	}
	if err != nil {
		if tx != nil {
			a.activeTx = tx
		}
		a.mu.Unlock()
		if tx != nil {
			return nil, errors.Join(err, rollbackSessionTransaction(tx, a))
		}
		discard := a.prepareDiscardExcept(nil)
		a.operationMu.Unlock()
		return nil, errors.Join(err, wrapAdvisoryDiscardError(discard.drain()))
	}
	a.activeTx = tx
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
	var err error
	if a.discardPending && !a.closed && a.conn != nil {
		err = a.forceDiscardConnectionLocked()
	}
	retirements := a.takePendingLeaseRetirementsLocked()
	testEndTxError := a.testEndTxError
	a.mu.Unlock()
	if testEndTxError != nil {
		err = errors.Join(err, testEndTxError())
	}
	var discard sessionDiscard
	if err != nil {
		// A successor cannot borrow the session between failed settlement and
		// its exact disposition. Terminal callbacks run after operation unlock.
		discard = a.prepareDiscardExcept(nil)
	}
	a.operationMu.Unlock()
	sessionDiscard{leases: retirements}.drain()
	return errors.Join(err, discard.drain())
}

func (a *SessionAuthority) beginOperation() (func(), error) {
	if a == nil {
		return nil, errors.New("PostgreSQL session authority is missing")
	}
	// Cleanup callers must still join the outstanding operation after a local
	// fence; a fenced fast path here could dispose a still-owned result stream.
	a.operationMu.Lock()
	a.mu.Lock()
	if a.fenced.Load() || a.closed || a.discardPending || a.conn == nil {
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

func (a *SessionAuthority) runWithCallerCancellation(ctx context.Context, run func(context.Context) error) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.Lock()
	tx := a.activeTx
	a.mu.Unlock()
	if tx == nil {
		return errors.New("PostgreSQL transaction is missing")
	}
	defer func() {
		err = errors.Join(err, ctx.Err())
	}()
	return run(context.WithoutCancel(ctx))
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
	return AcquireAdvisoryLockLeaseOnSession(ctx, authority, lockKey, acquire, authority.release)
}

func AcquireAdvisoryLockLeaseOnSession(
	ctx context.Context,
	authority *SessionAuthority,
	lockKey string,
	acquire AdvisoryLockAcquire,
	releaseSession func() error,
) (lease *AdvisoryLockLease, acquired bool, err error) {
	if authority == nil || releaseSession == nil {
		return nil, false, errors.New("acquire advisory lock requires private session authority")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, false, errors.Join(err, releaseSession())
	}
	endOperation, err := authority.beginOperation()
	if err != nil {
		return nil, false, errors.Join(err, releaseSession())
	}
	discard, handedOff := false, false
	defer func() {
		var disposition sessionDiscard
		if discard {
			disposition = authority.prepareDiscardExcept(nil)
		}
		endOperation()
		cleanupErr := wrapAdvisoryDiscardError(disposition.drain())
		if !handedOff {
			cleanupErr = errors.Join(cleanupErr, releaseSession())
		}
		if cleanupErr != nil {
			slog.Error("postgres advisory acquisition cleanup failed", "error", cleanupErr)
		}
		err = errors.Join(err, cleanupErr)
	}()
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	// An interrupted acquisition cannot return a possibly held lock to the pool.
	discard = true
	if acquire != nil {
		// Custom callbacks run inside this operation boundary, with their
		// original context. They must not reenter another session owner.
		acquired, err = acquire(ctx, authority, lockKey)
	} else {
		err = authority.queryRowContext(context.WithoutCancel(ctx), `SELECT pg_try_advisory_lock(hashtext($1))`, lockKey).Scan(&acquired)
	}
	if err != nil {
		return nil, false, errors.Join(ctx.Err(), fmt.Errorf("acquire advisory lock: %w", err))
	}
	if !acquired {
		discard = false
		return nil, false, ctx.Err()
	}
	if callerErr := ctx.Err(); callerErr != nil {
		var unlocked bool
		unlockErr := authority.queryRowContext(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock(hashtext($1))`, lockKey).Scan(&unlocked)
		if unlockErr == nil && !unlocked {
			unlockErr = errors.New("canceled PostgreSQL advisory acquisition did not hold its lock")
		}
		discard = unlockErr != nil
		return nil, false, errors.Join(callerErr, unlockErr)
	}
	lease = &AdvisoryLockLease{
		session:        authority,
		lockKey:        lockKey,
		releaseSession: releaseSession,
	}
	if err := authority.attach(lease); err != nil {
		return nil, false, fmt.Errorf("attach advisory lock lease: %w", err)
	}
	discard, handedOff = false, true
	return lease, true, nil
}

func RunAuthorityTransaction(
	ctx context.Context,
	session *SessionAuthority,
	fn func(context.Context, *sql.Tx) error,
) error {
	_, err := RunAuthorityTransactionOutcome(ctx, session, fn)
	return err
}

// RunAuthorityTransactionOutcome reports only an acknowledged successful COMMIT.
// Later cleanup errors do not erase committed evidence; false does not prove rollback.
func RunAuthorityTransactionOutcome(
	ctx context.Context,
	session *SessionAuthority,
	fn func(context.Context, *sql.Tx) error,
) (committed bool, err error) {
	return runAuthorityTransaction(ctx, session, nil, fn)
}

// RunAuthorityTransactionOutcomeWithOptions uses the same settlement owner with
// caller-selected isolation. Committed evidence survives later cleanup errors.
func RunAuthorityTransactionOutcomeWithOptions(
	ctx context.Context,
	session *SessionAuthority,
	opts *sql.TxOptions,
	fn func(context.Context, *sql.Tx) error,
) (committed bool, err error) {
	return runAuthorityTransaction(ctx, session, opts, fn)
}

// RunAuthorityReadTransaction owns a complete retained read snapshot. The callback
// must consume and close its results before returning; it cannot export SQL work.
func RunAuthorityReadTransaction(
	ctx context.Context,
	session *SessionAuthority,
	fn func(context.Context, *sql.Tx) error,
) error {
	_, err := runAuthorityTransaction(ctx, session, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true}, fn)
	return err
}

func runAuthorityTransaction(
	ctx context.Context,
	session *SessionAuthority,
	opts *sql.TxOptions,
	fn func(context.Context, *sql.Tx) error,
) (committed bool, err error) {
	if session == nil {
		return false, errors.New("PostgreSQL session authority is required")
	}
	if fn == nil {
		return false, nil
	}
	tx, err := session.beginTxOptions(ctx, opts)
	if err != nil {
		return false, err
	}
	defer func() {
		if tx != nil {
			cleanupErr := rollbackSessionTransaction(tx, session)
			if cleanupErr != nil {
				slog.Error("postgres retained transaction cleanup failed", "error", cleanupErr)
				err = errors.Join(err, cleanupErr)
			}
		}
	}()
	if runErr := session.runWithCallerCancellation(ctx, func(operationCtx context.Context) error {
		return fn(operationCtx, tx)
	}); runErr != nil {
		return false, runErr
	}
	if callerErr := contextError(ctx); callerErr != nil {
		return false, callerErr
	}
	if session.fenced.Load() {
		return false, errors.New("PostgreSQL session authority fenced before COMMIT admission")
	}
	if commitErr := tx.Commit(); commitErr != nil {
		// Fence possession before endTx releases operationMu. A successor must
		// never borrow the session between ambiguous settlement and disposal.
		discardErr := session.prepareDiscardExcept(nil).drain()
		endErr := session.endTx(tx)
		tx = nil
		return false, errors.Join(commitErr, contextError(ctx), endErr, wrapAdvisoryDiscardError(discardErr))
	}
	committed = true
	endErr := session.endTx(tx)
	tx = nil
	return committed, endErr
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
	settledElsewhere := rollbackErr == sql.ErrTxDone
	if settledElsewhere {
		rollbackErr = nil
	}
	var discardErr error
	if rollbackErr != nil || settledElsewhere {
		discardErr = session.prepareDiscardExcept(nil).drain()
	}
	endErr := session.endTx(tx)
	if endErr != nil {
		discardErr = errors.Join(discardErr, session.forceDiscard())
	}
	return errors.Join(rollbackErr, endErr, wrapAdvisoryDiscardError(discardErr))
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
	return l.proveCurrent(ctx, 0)
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
	return l.proveCurrent(ctx, deadline)
}

func (l *AdvisoryLockLease) proveCurrent(ctx context.Context, deadline time.Duration) (err error) {
	if l == nil {
		return errors.New("advisory lock lease has no current PostgreSQL session")
	}
	l.mu.Lock()
	session := l.session
	current := !l.released && session != nil
	prove := l.testProve
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
	// This timer makes only the local fencing decision. SQL remains on this
	// owner goroutine and is joined before the session operation is released.
	var decisionMu sync.Mutex
	finished, expired := false, false
	expiresAt := time.Now().Add(deadline)
	if deadline > 0 {
		timerDone := make(chan struct{})
		timer := time.AfterFunc(time.Until(expiresAt), func() {
			defer close(timerDone)
			decisionMu.Lock()
			if !finished && ctx.Err() == nil {
				expired = true
				session.fenceLocal()
			}
			decisionMu.Unlock()
		})
		defer func() {
			decisionMu.Lock()
			finished = true
			decisionMu.Unlock()
			if !timer.Stop() {
				<-timerDone
			}
		}()
	}
	var conn *sql.Conn
	keep := false
	defer func() {
		var discard sessionDiscard
		if !keep {
			// Fence and close while serialized.
			// Lease callbacks run only after releasing the operation boundary.
			discard = session.prepareDiscardExcept(nil)
		}
		endOperation()
		err = errors.Join(err, wrapAdvisoryDiscardError(discard.drain()))
	}()
	conn, err = session.connection()
	if err != nil {
		return err
	}
	queryCtx := context.WithoutCancel(ctx)
	var held bool
	var proofErr error
	if prove != nil {
		held, proofErr = prove(queryCtx, session, l.lockKey)
	} else {
		proofErr = session.queryRowContext(queryCtx, retainedAdvisoryLockProofSQL, l.lockKey).Scan(&held)
	}
	err = errors.Join(proofErr, ctx.Err())
	if proofErr == nil && !held {
		err = errors.Join(err, errors.New("retained PostgreSQL session no longer owns its advisory lock"))
	}
	if proofErr != nil || !held {
		session.fenceLocal()
	}
	// Validity fences physical reuse only. It neither establishes cancellation
	// provenance nor substitutes for the exact advisory-lock proof.
	validityErr := conn.Raw(func(raw any) error {
		validator, ok := raw.(driver.Validator)
		if !ok || !validator.IsValid() {
			return errors.New("retained PostgreSQL native session is not reusable")
		}
		return nil
	})
	if validityErr != nil {
		session.fenceLocal()
		if validityErr != sql.ErrConnDone {
			err = errors.Join(err, validityErr)
		} else if err == nil {
			err = errors.New("retained PostgreSQL session closed before proof settlement")
		}
	}
	decisionMu.Lock()
	if deadline > 0 && !finished && !time.Now().Before(expiresAt) && ctx.Err() == nil {
		expired = true
		session.fenceLocal()
	}
	finished = true
	if expired {
		err = errors.Join(err, context.DeadlineExceeded)
	}
	decisionMu.Unlock()
	if session.fenced.Load() {
		return errors.Join(err, errors.New("retained PostgreSQL session authority is fenced"))
	}
	if ctx.Err() != nil && proofErr == nil && held {
		keep = true
		return errors.Join(ctx.Err(), err)
	}
	if err == nil && held {
		keep = true
		return nil
	}
	return err
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

func (a *SessionAuthority) Retain() (func() error, bool) { return a.retain() }
func (a *SessionAuthority) ForceDiscard() error          { return a.forceDiscard() }
func (a *SessionAuthority) Release() error               { return a.release() }

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
