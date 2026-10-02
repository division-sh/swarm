package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
)

// OwnershipProof retains fresh read access to the existing backend without
// competing with workload connections. It never holds a read transaction.
type OwnershipProof struct {
	backend *Backend
	conn    *sql.Conn
	cancel  context.CancelFunc
	ready   chan struct{}
	once    sync.Once
	err     error
}

func (b *Backend) RetainOwnershipProof(ctx context.Context) (*OwnershipProof, error) {
	if !b.Valid() {
		return nil, errors.New("SQLite ownership proof requires a backend")
	}
	acquireCtx, cancel := context.WithCancel(ctx)
	proof := &OwnershipProof{backend: b, cancel: cancel, ready: make(chan struct{})}
	b.ownershipProofs.Lock()
	if b.ownershipProofs.closed {
		b.ownershipProofs.Unlock()
		cancel()
		return nil, errors.New("SQLite ownership proof backend is closed")
	}
	if len(b.ownershipProofs.retained) == 0 {
		b.ownershipProofs.capacity = b.db.Stats().MaxOpenConnections
	}
	if b.ownershipProofs.retained == nil {
		b.ownershipProofs.retained = make(map[*OwnershipProof]struct{})
	}
	b.ownershipProofs.retained[proof] = struct{}{}
	b.resizeOwnershipCapacityLocked()
	b.ownershipProofs.Unlock()

	conn, err := b.db.Conn(acquireCtx)
	proof.conn = conn
	close(proof.ready)
	cancel()
	b.ownershipProofs.Lock()
	closed := b.ownershipProofs.closed
	b.ownershipProofs.Unlock()
	if err != nil || closed {
		if err == nil {
			err = errors.New("SQLite ownership proof backend closed during acquisition")
		}
		return nil, errors.Join(fmt.Errorf("retain SQLite ownership proof connection: %w", err), proof.Close())
	}
	return proof, nil
}

func (p *OwnershipProof) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return p.conn.QueryRowContext(ctx, query, args...)
}

func (p *OwnershipProof) Close() error {
	if p == nil {
		return nil
	}
	p.once.Do(func() {
		p.cancel()
		<-p.ready
		if p.conn != nil {
			p.err = p.conn.Close()
		}
		p.backend.ownershipProofs.Lock()
		delete(p.backend.ownershipProofs.retained, p)
		p.backend.resizeOwnershipCapacityLocked()
		p.backend.ownershipProofs.Unlock()
	})
	return p.err
}

func (b *Backend) resizeOwnershipCapacityLocked() {
	if b.ownershipProofs.capacity > 0 {
		b.db.SetMaxOpenConns(b.ownershipProofs.capacity + len(b.ownershipProofs.retained))
	}
}

func (b *Backend) closeOwnershipProofs() error {
	b.ownershipProofs.Lock()
	b.ownershipProofs.closed = true
	retained := make([]*OwnershipProof, 0, len(b.ownershipProofs.retained))
	for proof := range b.ownershipProofs.retained {
		proof.cancel()
		retained = append(retained, proof)
	}
	b.ownershipProofs.Unlock()
	var err error
	for _, proof := range retained {
		err = errors.Join(err, proof.Close())
	}
	return err
}
