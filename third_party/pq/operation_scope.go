package pq

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lib/pq/internal/proto"
)

// OperationScope is the outcome record for one privately owned SQL operation,
// including its transaction and physical cleanup. It is not session authority.
// The owner must settle the transaction and close its rows before calling Wait,
// and must not admit new work concurrently with Wait or reuse a completed scope.
type OperationScope struct {
	owner    context.Context
	implicit bool
	work     sync.WaitGroup
	mu       sync.Mutex
	errs     []error
}

// NewOperationScope binds cancellation to its actual owner, independently of
// the context database/sql uses for transport and automatic transaction cleanup.
func NewOperationScope(owner context.Context) *OperationScope {
	if owner == nil {
		owner = context.Background()
	}
	return &OperationScope{owner: owner}
}

type operationScopeKey struct{}

// Context attaches s without changing ctx's cancellation or values. Use
// context.WithoutCancel(s.Context(ctx)) for detached transaction/retained work.
// The driver still observes s's owner when this transport context is detached.
func (s *OperationScope) Context(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, operationScopeKey{}, s)
}

// Err returns independent and uncertain native failures observed so far. Owned
// stops are returned directly by native methods, not added to this error set.
func (s *OperationScope) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return errors.Join(s.errs...)
}

// Wait joins native operations, cancellation workers and transaction settlement,
// then returns Err. Call it after requesting synchronous transaction cleanup.
func (s *OperationScope) Wait() error {
	s.work.Wait()
	return s.Err()
}

// BindOperationScope binds the acquired physical connection under sql.Conn.Raw.
// Private driver wrappers may forward this method explicitly to their underlying
// connection. No wrapper discovery or unwrapping is performed.
func BindOperationScope(c driver.Conn, scope *OperationScope) error {
	binder, ok := c.(interface{ BindOperationScope(*OperationScope) error })
	if !ok {
		return fmt.Errorf("pq: connection %T does not support operation scope binding", c)
	}
	return binder.BindOperationScope(scope)
}

// BindOperationScope retains scope through transaction and physical cleanup.
// After settlement and Wait, nil releases a healthy retained binding.
func (cn *conn) BindOperationScope(scope *OperationScope) error {
	if cn.native != nil || cn.txnScope != nil {
		return errors.New("pq: cannot replace an active operation scope")
	}
	cn.operationScope = scope
	return nil
}

func (s *OperationScope) record(err error) {
	if err == nil {
		return
	}
	if _, ok := err.(*OwnedStopError); ok {
		return
	}
	s.mu.Lock()
	s.errs = append(s.errs, err)
	s.mu.Unlock()
}

// OwnedStopError records local cancellation arbitration, not wire-level proof
// that this client's CancelRequest caused a particular server ErrorResponse.
// Native retains that response for diagnostics without making it an independent
// error leaf. Only the producing operation can create this result.
type OwnedStopError struct {
	cause  error
	native error
}

func (e *OwnedStopError) Error() string { return e.cause.Error() }
func (e *OwnedStopError) Unwrap() error { return e.cause }
func (e *OwnedStopError) Native() error { return e.native }

type nativeOperation struct {
	cn            *conn
	scope         *OperationScope
	kind          string
	mu            sync.Mutex
	finished      bool
	errorSeen     bool
	responseOwned bool
	requested     bool
	issued        bool
	sent          bool
	ready         bool
	sending       chan struct{}
	writeErr      error
	cause         error
	cancelErr     error
	errs          []error
	candidates    map[*Error]bool
	stop          chan struct{}
	workerDone    chan struct{}
	finishOnce    sync.Once
	result        error
}

func (cn *conn) scopeFor(ctx context.Context) *OperationScope {
	if s, _ := ctx.Value(operationScopeKey{}).(*OperationScope); s != nil {
		return s
	}
	if cn.txnScope != nil {
		return cn.txnScope
	}
	return cn.operationScope
}

func (cn *conn) startNative(ctx context.Context, kind string) (*nativeOperation, error) {
	s := cn.scopeFor(ctx)
	if s == nil {
		s = NewOperationScope(ctx)
		s.implicit = true
	}
	if cn.native != nil {
		return nil, errors.New("pq: native operation started before prior rows settled")
	}
	if cn.txnScope != nil && cn.txnScope != s {
		return nil, errors.New("pq: operation scope differs from active transaction")
	}
	// Implicit authority is retained only by txnScope after successful Begin.
	// The connection binding belongs to explicit owners through physical cleanup.
	if !s.implicit {
		cn.operationScope = s
	}
	cleanup := kind == "rollback" || kind == "statement close"
	if !cleanup {
		if err := s.owner.Err(); err != nil {
			return nil, &OwnedStopError{cause: context.Cause(s.owner)}
		}
		if err := ctx.Err(); err != nil {
			return nil, &OwnedStopError{cause: context.Cause(ctx)}
		}
	}
	op := &nativeOperation{
		cn: cn, scope: s, kind: kind, candidates: make(map[*Error]bool),
		stop: make(chan struct{}), workerDone: make(chan struct{}),
	}
	cn.native = op
	s.work.Add(1)
	if cleanup || kind == "commit" {
		// COMMIT admission is the cancellation cut. Once admitted, its actual
		// outcome must settle; rollback and statement cleanup are never canceled.
		close(op.workerDone)
	} else {
		go func() {
			defer close(op.workerDone)
			select {
			case <-s.owner.Done():
				op.cancel(context.Cause(s.owner))
			case <-ctx.Done():
				op.cancel(context.Cause(ctx))
			case <-op.stop:
			}
		}()
	}
	return op, nil
}

func (op *nativeOperation) cancel(cause error) {
	op.mu.Lock()
	if op.finished || op.errorSeen {
		op.mu.Unlock()
		return
	}
	op.requested, op.cause = true, cause
	sending := op.sending
	op.mu.Unlock()
	if sending != nil {
		// Do not wait indefinitely behind a blocked native write. An interrupted
		// write remains an independent transport failure, never an owned 57014.
		if err := op.cn.c.SetWriteDeadline(time.Now()); err != nil {
			op.cancelFailed(err)
			return
		}
		<-sending
		if err := op.cn.c.SetWriteDeadline(time.Time{}); err != nil {
			op.cancelFailed(err)
			return
		}
	}
	op.mu.Lock()
	if op.finished || op.errorSeen || !op.sent || op.ready || op.writeErr != nil {
		op.mu.Unlock()
		return
	}
	// The local issuance gate precedes ErrorResponse observation. Successful
	// completion of this exact cancel action is also required for normalization.
	op.issued = true
	op.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), watchCancelDialContextTimeout)
	err := op.cn.cancel(ctx)
	cancel()
	if err != nil {
		op.cancelFailed(err)
	}
}

func (op *nativeOperation) cancelFailed(err error) {
	// A failed cancel cannot leave a native read and its owner stranded.
	// This is unsafe disposal, not a successful owning cancellation.
	closeErr := op.cn.c.Close()
	err = errors.Join(fmt.Errorf("pq: native cancellation failed: %w", err), closeErr)
	op.cn.err.set(err)
	op.mu.Lock()
	op.cancelErr = err
	op.mu.Unlock()
}

func (op *nativeOperation) beforeSend() error {
	op.mu.Lock()
	defer op.mu.Unlock()
	if op.requested {
		return &OwnedStopError{cause: op.cause}
	}
	op.sending = make(chan struct{})
	op.sent, op.ready = true, false
	return nil
}

func (op *nativeOperation) afterSend(err error) {
	op.mu.Lock()
	op.writeErr = err
	close(op.sending)
	op.sending = nil
	op.mu.Unlock()
}

func (op *nativeOperation) observeError(err error) {
	if err == nil {
		return
	}
	if _, ok := err.(*OwnedStopError); ok {
		return
	}
	op.mu.Lock()
	defer op.mu.Unlock()
	if e, ok := err.(*Error); ok {
		if _, seen := op.candidates[e]; seen {
			return
		}
		op.candidates[e] = op.responseOwned && op.kind != "commit" && op.kind != "rollback" && op.kind != "statement close" && e.Code == "57014" && !e.Fatal()
	}
	op.errorSeen = true
	op.errs = append(op.errs, err)
}

func (op *nativeOperation) observeResponse(response proto.ResponseCode) {
	op.mu.Lock()
	defer op.mu.Unlock()
	switch response {
	case proto.ErrorResponse:
		op.responseOwned = op.issued && !op.errorSeen
		op.errorSeen = true
	case proto.ReadyForQuery:
		op.ready = true
	}
}

func (op *nativeOperation) finish(err error) error {
	if op == nil {
		return err
	}
	op.finishOnce.Do(func() { op.result = op.complete(err) })
	return op.result
}

func (op *nativeOperation) complete(err error) error {
	op.observeError(err)
	op.mu.Lock()
	op.finished = true
	close(op.stop)
	op.mu.Unlock()
	<-op.workerDone
	op.mu.Lock()
	defer op.mu.Unlock()
	var failures []error
	var native error
	for _, observed := range op.errs {
		if e, ok := observed.(*Error); ok && op.candidates[e] && op.cancelErr == nil {
			native = e
			continue
		}
		failures = append(failures, observed)
		op.scope.record(observed)
	}
	if op.cancelErr != nil {
		failures = append(failures, op.cancelErr)
		op.scope.record(op.cancelErr)
	}
	if op.sent && !op.ready {
		// An undrained protocol cannot be returned to either pool or possession.
		op.cn.err.set(driver.ErrBadConn)
		if len(failures) == 0 && native == nil {
			unsettled := errors.New("pq: native operation ended before protocol settlement")
			failures = append(failures, unsettled)
			op.scope.record(unsettled)
		}
	}
	if native != nil || (op.issued && op.cancelErr == nil && err == nil) {
		failures = append(failures, &OwnedStopError{cause: op.cause, native: native})
	} else if _, ok := err.(*OwnedStopError); ok {
		failures = append(failures, err)
	}
	result := err
	switch len(failures) {
	case 0:
	case 1:
		result = failures[0]
	default:
		result = errors.Join(failures...)
	}
	op.cn.native = nil
	op.scope.work.Done()
	return result
}

func (cn *conn) nativeError(err error) {
	if cn.native != nil {
		cn.native.observeError(err)
	}
}

func (cn *conn) parseError(r *readBuf, query string) *Error {
	err := parseError(r, query)
	cn.nativeError(err)
	return err
}

func (cn *conn) finishNativeTransaction() {
	if cn.txnScope != nil {
		cn.txnScope.work.Done()
		cn.txnScope = nil
	}
}
