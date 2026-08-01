package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

const (
	pipelineReplayClaimNamespace = "swarm:pipeline-replay:"
	scheduleClaimNamespace       = "swarm:schedule:"
)

type sqlAdvisoryLockLease struct {
	mu                       sync.Mutex
	session                  *postgresSessionAuthority
	lockKey                  string
	releaseSession           func() error
	releaseCapacity          func()
	onTerminal               func()
	unlocked                 bool
	released                 bool
	testUnlock               func(context.Context, *postgresSessionAuthority, string) (bool, error)
	testBeforeBeginOperation func()
}

type postgresSessionAuthorityContextKey struct{}

type postgresSessionAuthority struct {
	operationMu             sync.Mutex
	mu                      sync.Mutex
	conn                    *sql.Conn
	activeTx                *sql.Tx
	refs                    int
	closed                  bool
	discardPending          bool
	leases                  map[*sqlAdvisoryLockLease]struct{}
	pendingLeaseRetirements []*sqlAdvisoryLockLease
	testEndTxError          func() error
}

type postgresSessionDiscard struct {
	leases []*sqlAdvisoryLockLease
	err    error
}

func (d postgresSessionDiscard) drain() error {
	for _, lease := range d.leases {
		lease.retireFromSession()
	}
	return d.err
}

func newPostgresSessionAuthority(conn *sql.Conn) *postgresSessionAuthority {
	return &postgresSessionAuthority{
		conn:   conn,
		refs:   1,
		leases: map[*sqlAdvisoryLockLease]struct{}{},
	}
}

func (a *postgresSessionAuthority) bindContext(ctx context.Context) (context.Context, error) {
	if a == nil {
		return ctx, errors.New("PostgreSQL session authority is missing")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	conn, err := a.connection()
	if err != nil {
		return ctx, err
	}
	ctx = runtimepipeline.WithPipelineSQLConnContext(ctx, conn)
	return context.WithValue(ctx, postgresSessionAuthorityContextKey{}, a), nil
}

func postgresSessionAuthorityFromContext(ctx context.Context) (*postgresSessionAuthority, bool, error) {
	if ctx == nil {
		return nil, false, nil
	}
	authority, ok := ctx.Value(postgresSessionAuthorityContextKey{}).(*postgresSessionAuthority)
	if !ok || authority == nil {
		if _, borrowed := runtimepipeline.PipelineSQLConnFromContext(ctx); borrowed {
			return nil, false, errors.New("borrowed PostgreSQL connection lacks private session authority")
		}
		if _, borrowed := runtimepipeline.PipelineSQLTxFromContext(ctx); borrowed {
			return nil, false, errors.New("borrowed PostgreSQL transaction lacks private session authority")
		}
		return nil, false, nil
	}
	borrowedConn, hasConn := runtimepipeline.PipelineSQLConnFromContext(ctx)
	borrowedTx, hasTx := runtimepipeline.PipelineSQLTxFromContext(ctx)
	if !hasConn && !hasTx {
		// Detached work contexts deliberately strip public SQL capabilities.
		// A private token left in the value chain cannot revive that authority.
		return nil, false, nil
	}
	if !hasConn {
		return nil, false, errors.New("borrowed PostgreSQL transaction lacks its exact private session connection")
	}
	conn, err := authority.connection()
	if err != nil {
		return nil, false, err
	}
	if borrowedConn != conn {
		return nil, false, errors.New("borrowed PostgreSQL connection does not match private session authority")
	}
	if hasTx {
		authority.mu.Lock()
		exact := authority.activeTx == borrowedTx
		authority.mu.Unlock()
		if !exact {
			return nil, false, errors.New("borrowed PostgreSQL transaction does not match private session authority")
		}
	}
	return authority, true, nil
}

func (a *postgresSessionAuthority) connection() (*sql.Conn, error) {
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

func (a *postgresSessionAuthority) attach(lease *sqlAdvisoryLockLease) error {
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

func (a *postgresSessionAuthority) detach(lease *sqlAdvisoryLockLease) {
	if a == nil || lease == nil {
		return
	}
	a.mu.Lock()
	delete(a.leases, lease)
	a.mu.Unlock()
}

func (a *postgresSessionAuthority) retain() (func() error, bool) {
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

func (a *postgresSessionAuthority) release() error {
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

func (a *postgresSessionAuthority) forceDiscard() error {
	return a.prepareDiscardExcept(nil).drain()
}

func (a *postgresSessionAuthority) prepareDiscardExcept(except *sqlAdvisoryLockLease) postgresSessionDiscard {
	if a == nil {
		return postgresSessionDiscard{}
	}
	a.mu.Lock()
	a.discardPending = true
	leases := make([]*sqlAdvisoryLockLease, 0, len(a.leases))
	for lease := range a.leases {
		delete(a.leases, lease)
		if lease != except {
			leases = append(leases, lease)
		}
	}
	if a.activeTx != nil {
		a.pendingLeaseRetirements = append(a.pendingLeaseRetirements, leases...)
		a.mu.Unlock()
		return postgresSessionDiscard{}
	}
	leases = append(leases, a.takePendingLeaseRetirementsLocked()...)
	var err error
	if !a.closed && a.conn != nil && a.activeTx == nil {
		err = a.forceDiscardConnectionLocked()
	}
	a.mu.Unlock()
	return postgresSessionDiscard{leases: leases, err: err}
}

func (a *postgresSessionAuthority) takePendingLeaseRetirementsLocked() []*sqlAdvisoryLockLease {
	retirements := a.pendingLeaseRetirements
	a.pendingLeaseRetirements = nil
	return retirements
}

func (a *postgresSessionAuthority) forceDiscardConnectionLocked() error {
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

func (a *postgresSessionAuthority) beginTx(ctx context.Context) (*sql.Tx, error) {
	if a == nil {
		return nil, errors.New("PostgreSQL session authority is missing")
	}
	a.operationMu.Lock()
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.conn == nil {
		a.operationMu.Unlock()
		return nil, errors.New("PostgreSQL session authority is closed")
	}
	if a.activeTx != nil {
		a.operationMu.Unlock()
		return nil, errors.New("PostgreSQL session authority already has an active transaction")
	}
	tx, err := a.conn.BeginTx(ctx, nil)
	if err != nil {
		a.operationMu.Unlock()
		return nil, err
	}
	a.activeTx = tx
	return tx, nil
}

func (a *postgresSessionAuthority) endTx(tx *sql.Tx) error {
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
	a.operationMu.Unlock()
	postgresSessionDiscard{leases: retirements}.drain()
	if testEndTxError != nil {
		err = errors.Join(err, testEndTxError())
	}
	return err
}

func (a *postgresSessionAuthority) beginOperation(ctx context.Context) (func(), error) {
	if a == nil {
		return nil, errors.New("PostgreSQL session authority is missing")
	}
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		a.mu.Lock()
		exact := !a.closed && !a.discardPending && a.conn != nil && a.activeTx == tx
		a.mu.Unlock()
		if exact {
			return func() {}, nil
		}
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

func (a *postgresSessionAuthority) queryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	a.mu.Lock()
	tx := a.activeTx
	conn := a.conn
	a.mu.Unlock()
	if tx != nil {
		return tx.QueryRowContext(ctx, query, args...)
	}
	return conn.QueryRowContext(ctx, query, args...)
}

func (a *postgresSessionAuthority) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	a.mu.Lock()
	tx := a.activeTx
	conn := a.conn
	a.mu.Unlock()
	if tx != nil {
		return tx.QueryContext(ctx, query, args...)
	}
	return conn.QueryContext(ctx, query, args...)
}

func (a *postgresSessionAuthority) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return a.queryRowContext(ctx, query, args...)
}

func (l *sqlAdvisoryLockLease) BindContext(ctx context.Context) (context.Context, error) {
	if l == nil {
		return ctx, errors.New("advisory lock lease has no current PostgreSQL session")
	}
	l.mu.Lock()
	session := l.session
	current := !l.released && session != nil
	l.mu.Unlock()
	if !current {
		return ctx, errors.New("advisory lock lease has no current PostgreSQL session")
	}
	return session.bindContext(ctx)
}

func (l *sqlAdvisoryLockLease) Release(ctx context.Context) error {
	return l.releaseWithRetirement(ctx)
}

func (l *sqlAdvisoryLockLease) releaseWithRetirement(ctx context.Context) error {
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
	endOperation, operationErr := session.beginOperation(ctx)
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

type advisoryLeaseRetirement struct {
	releaseCapacity func()
	onTerminal      func()
}

func (r advisoryLeaseRetirement) run() {
	if r.releaseCapacity != nil {
		r.releaseCapacity()
	}
	if r.onTerminal != nil {
		r.onTerminal()
	}
}

func (l *sqlAdvisoryLockLease) retireLocked() advisoryLeaseRetirement {
	actions := advisoryLeaseRetirement{
		releaseCapacity: l.releaseCapacity,
		onTerminal:      l.onTerminal,
	}
	l.releaseCapacity = nil
	l.onTerminal = nil
	l.releaseSession = nil
	l.released = true
	return actions
}

func (l *sqlAdvisoryLockLease) retireFromSession() {
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

func (l *sqlAdvisoryLockLease) installTerminalOwner(
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

func (l *sqlAdvisoryLockLease) current() bool {
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

func acquireAdvisoryLockLease(ctx context.Context, db *sql.DB, lockKey string) (*sqlAdvisoryLockLease, bool, error) {
	return acquireAdvisoryLockLeaseWith(ctx, db, lockKey, nil)
}

type advisoryLockAcquire func(context.Context, *postgresSessionAuthority, string) (bool, error)

func acquireAdvisoryLockLeaseWith(
	ctx context.Context,
	db *sql.DB,
	lockKey string,
	acquire advisoryLockAcquire,
) (*sqlAdvisoryLockLease, bool, error) {
	if db == nil {
		return nil, false, nil
	}
	lockKey = strings.TrimSpace(lockKey)
	if lockKey == "" {
		return nil, false, fmt.Errorf("advisory lock key is required")
	}
	authority, borrowed, err := postgresSessionAuthorityFromContext(ctx)
	var releaseSession func() error
	if err != nil {
		return nil, false, err
	}
	if !borrowed {
		conn, err := db.Conn(ctx)
		if err != nil {
			return nil, false, fmt.Errorf("acquire advisory lock connection: %w", err)
		}
		authority = newPostgresSessionAuthority(conn)
		releaseSession = authority.release
	} else {
		var retained bool
		releaseSession, retained = authority.retain()
		if !retained {
			return nil, false, fmt.Errorf("acquire advisory lock connection: private session authority is closed")
		}
	}
	var acquired bool
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
	lease := &sqlAdvisoryLockLease{
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

func runPostgresSessionTransaction(
	ctx context.Context,
	db *sql.DB,
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
	session, borrowed, err := postgresSessionAuthorityFromContext(ctx)
	if err != nil {
		return err
	}
	if !borrowed {
		conn, connErr := db.Conn(ctx)
		if connErr != nil {
			return connErr
		}
		session = newPostgresSessionAuthority(conn)
		defer func() {
			err = errors.Join(err, session.release())
		}()
	}
	ctx, err = session.bindContext(ctx)
	if err != nil {
		return err
	}
	tx, err := session.beginTx(ctx)
	if err != nil {
		return err
	}
	txctx := runtimepipeline.WithPipelineSQLTxContext(ctx, tx)
	if runErr := fn(txctx, tx); runErr != nil {
		return errors.Join(runErr, rollbackPostgresSessionTransaction(tx, session))
	}
	if commitErr := tx.Commit(); commitErr != nil {
		return errors.Join(commitErr, rollbackPostgresSessionTransaction(tx, session))
	}
	return session.endTx(tx)
}

func replayClaimLockKey(eventID string) string {
	return pipelineReplayClaimNamespace + strings.TrimSpace(eventID)
}

func scheduleClaimLockKey(sc runtimepipeline.Schedule) string {
	identityFingerprint := ""
	if !sc.AgentIdentity.IsZero() {
		identityFingerprint, _ = sc.AgentIdentity.Fingerprint()
	}
	return scheduleClaimNamespace + strings.Join([]string{
		strings.TrimSpace(sc.EffectiveRunID()),
		strings.TrimSpace(sc.AgentID),
		strings.TrimSpace(string(sc.OwnerKind)),
		identityFingerprint,
		strings.TrimSpace(sc.EventType),
		strings.TrimSpace(sc.EffectiveEntityID()),
		strings.TrimSpace(sc.EffectiveFlowInstance()),
		strings.TrimSpace(sc.TaskID),
	}, "|")
}
