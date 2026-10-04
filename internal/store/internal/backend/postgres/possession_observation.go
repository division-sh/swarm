package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/lib/pq"
)

const possessionObservationCleanupLimit = 5 * time.Second

// ObserveAdvisoryLock owns an independent, joined native session. No lease or
// authority escapes, and ambiguous acquisition/unlock always physically discards.
func (b *Backend) ObserveAdvisoryLock(ctx context.Context, key string) (available bool, resultErr error) {
	return b.observeAdvisoryLock(ctx, key, &observationDialer{})
}

func (b *Backend) observeAdvisoryLock(ctx context.Context, key string, dialer *observationDialer) (available bool, resultErr error) {
	if !b.Valid() || b.inspectionConfig == nil || key == "" {
		return false, errors.New("selected PostgreSQL observation configuration and lock key are required")
	}
	if err := b.refuseInspectionMutation(ctx); err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if _, ok := ctx.Deadline(); !ok {
		return false, errors.New("possession observation requires a bounded context")
	}
	connector, err := pq.NewConnectorConfig(b.inspectionConfig.Clone())
	if err != nil {
		return false, err
	}
	stop, err := dialer.bind(ctx)
	if err != nil {
		return false, err
	}
	connector.Dialer(dialer)
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	var session *SessionAuthority
	ambiguous := true
	defer func() {
		resultErr = errors.Join(resultErr, stop(), observationContextError(ctx))
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), possessionObservationCleanupLimit)
		defer cancel()
		finish, bindErr := dialer.bind(cleanup)
		resultErr = errors.Join(resultErr, bindErr)
		if session != nil {
			if available && !ambiguous && resultErr == nil {
				var unlocked bool
				unlockErr := session.QueryRowContext(context.WithoutCancel(cleanup), unlockAdvisoryLockSQL, key).Scan(&unlocked)
				if unlockErr == nil && !unlocked {
					unlockErr = errors.New("PostgreSQL possession observation did not release its exact lock")
				}
				resultErr = errors.Join(resultErr, unlockErr)
			}
			if ambiguous || resultErr != nil {
				resultErr = errors.Join(resultErr, session.ForceDiscard())
			} else {
				resultErr = errors.Join(resultErr, session.Release())
			}
		}
		resultErr = errors.Join(resultErr, db.Close())
		resultErr = errors.Join(resultErr, dialer.close())
		if finish != nil {
			resultErr = errors.Join(resultErr, finish())
		}
		resultErr = errors.Join(resultErr, observationContextError(cleanup))
		if resultErr != nil {
			available = false
		}
	}()
	// lib/pq cancellation itself performs native I/O. The observation's socket
	// deadline/cancellation owner bounds that I/O instead of starting a detached
	// driver cancellation request whose join cannot be observed.
	nativeCtx := context.WithoutCancel(ctx)
	conn, err := db.Conn(nativeCtx)
	if err != nil {
		return false, err
	}
	session, err = b.NewSessionAuthority(conn)
	if err != nil {
		return false, errors.Join(err, conn.Close())
	}
	if err := session.QueryRowContext(nativeCtx, tryAdvisoryLockSQL, key).Scan(&available); err != nil {
		return false, fmt.Errorf("observe PostgreSQL possession: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !available {
		ambiguous = false
		return false, nil
	}
	ambiguous = false
	return true, ctx.Err()
}

func observationContextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Native deadlines can fire before Go schedules the context timer callback.
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

// observationDialer only scopes sockets for this one native observation; it
// does not wrap the SQL driver, change SQL semantics, or publish connection access.
type observationDialer struct {
	mu       sync.Mutex
	ctx      context.Context
	sockets  []*observationSocket
	phaseErr error
	dial     func(context.Context, string, string) (net.Conn, error)
}

func (d *observationDialer) bind(ctx context.Context) (func() error, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, errors.New("native observation phase requires a deadline")
	}
	d.mu.Lock()
	d.ctx = ctx
	d.phaseErr = nil
	for _, socket := range d.sockets {
		// The preceding phase joined and reported its I/O failures. Do not
		// misreport those same failures as a later close/rebind failure.
		socket.mu.Lock()
		socket.ioErr = nil
		socket.mu.Unlock()
		d.phaseErr = errors.Join(d.phaseErr, socket.bound(deadline))
	}
	err := d.phaseErr
	d.mu.Unlock()
	finished := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		d.mu.Lock()
		for _, socket := range d.sockets {
			d.phaseErr = errors.Join(d.phaseErr, socket.bound(time.Now()))
		}
		d.mu.Unlock()
		close(finished)
	})
	return func() error {
		if !stop() {
			<-finished
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		result := d.phaseErr
		for _, socket := range d.sockets {
			socket.mu.Lock()
			result = errors.Join(result, socket.ioErr, socket.closeErr)
			socket.mu.Unlock()
		}
		return result
	}, err
}

func (d *observationDialer) close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	var result error
	for _, socket := range d.sockets {
		result = errors.Join(result, socket.Close())
	}
	return result
}

func (d *observationDialer) Dial(network, address string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, address)
}

func (d *observationDialer) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return d.DialContext(ctx, network, address)
}

func (d *observationDialer) DialContext(request context.Context, network, address string) (net.Conn, error) {
	d.mu.Lock()
	ctx := d.ctx
	d.mu.Unlock()
	if deadline, ok := request.Deadline(); ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	dial := d.dial
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	conn, err := dial(ctx, network, address)
	if err != nil {
		return nil, err
	}
	socket := &observationSocket{Conn: conn}
	d.mu.Lock()
	defer d.mu.Unlock()
	deadline, _ := d.ctx.Deadline()
	if err := d.ctx.Err(); err != nil {
		return nil, errors.Join(err, conn.Close())
	}
	if err := socket.bound(deadline); err != nil {
		return nil, errors.Join(err, conn.Close())
	}
	d.sockets = append(d.sockets, socket)
	return socket, nil
}

type observationSocket struct {
	net.Conn
	mu       sync.Mutex
	deadline time.Time
	closed   bool
	closeErr error
	ioErr    error
}

func (s *observationSocket) Read(data []byte) (int, error) {
	n, err := s.Conn.Read(data)
	if err != nil {
		s.mu.Lock()
		s.ioErr = errors.Join(s.ioErr, err)
		s.mu.Unlock()
	}
	return n, err
}

func (s *observationSocket) Write(data []byte) (int, error) {
	n, err := s.Conn.Write(data)
	if err != nil {
		s.mu.Lock()
		s.ioErr = errors.Join(s.ioErr, err)
		s.mu.Unlock()
	}
	return n, err
}

func (s *observationSocket) bound(deadline time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.deadline = deadline
	return s.Conn.SetDeadline(deadline)
}

func (s *observationSocket) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	s.closeErr = s.Conn.Close()
	return s.closeErr
}

func (s *observationSocket) SetDeadline(deadline time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if deadline.IsZero() || s.deadline.Before(deadline) {
		deadline = s.deadline
	}
	return s.Conn.SetDeadline(deadline)
}
