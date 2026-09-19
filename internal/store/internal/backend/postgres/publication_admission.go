package postgres

import (
	"context"
	"database/sql"
	"errors"
	"sync"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

// PublicationAdmission borrows only the exact callback-owned read transaction
// of a bounded claim batch. It expires before route planning or dispatch. The
// pipeline owner defines parent keys; this owner retains physical possession.
type PublicationAdmission struct {
	mu      sync.Mutex
	session *SessionAuthority
	tx      *sql.Tx
}

func RunPublicationAdmission(ctx context.Context, session *SessionAuthority, fn func(context.Context, *PublicationAdmission) error) error {
	return RunAuthorityReadTransaction(ctx, session, func(ctx context.Context, tx *sql.Tx) error {
		transactiontest.Mark(ctx, transactiontest.PipelinePublicationAdmission)
		admission := &PublicationAdmission{session: session, tx: tx}
		defer func() { admission.mu.Lock(); admission.tx = nil; admission.mu.Unlock() }()
		return fn(ctx, admission)
	})
}

func (a *PublicationAdmission) borrowOperation(session *SessionAuthority) (func(), error) {
	if a == nil || a.tx == nil || a.session != session {
		return nil, errors.New("publication admission does not own this session operation")
	}
	session.mu.Lock()
	valid := session.activeTx == a.tx && !session.closed && !session.discardPending && !session.fenced.Load()
	session.mu.Unlock()
	if !valid {
		return nil, errors.New("publication admission session is no longer current")
	}
	return func() {}, nil
}

func (a *PublicationAdmission) Run(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := a.borrowOperation(a.session); err != nil {
		return err
	}
	return fn(ctx, a.tx)
}

func (a *PublicationAdmission) AcquireLease(ctx context.Context, key string, releaseSession func() error) (*AdvisoryLockLease, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return acquireAdvisoryLockLeaseForPublication(ctx, a.session, key, nil, releaseSession, a)
}

func (a *PublicationAdmission) ReleaseLease(ctx context.Context, lease *AdvisoryLockLease) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return lease.releaseWithPublicationAdmission(ctx, a)
}
