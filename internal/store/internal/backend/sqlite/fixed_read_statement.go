package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
)

// FixedReadStatement is one named adapter slot, bound to one pool and SQL
// string. It retains only a DB-backed handle, never a connection or query data.
// The pool owner closes successful handles; failed preparation is retryable.
type FixedReadStatement struct {
	mu        sync.Mutex
	backend   *Backend
	query     string
	preparing chan struct{}
	stmt      *sql.Stmt
}

func (s *FixedReadStatement) PrepareContext(ctx context.Context, b *Backend, query string) (*sql.Stmt, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.mu.Lock()
		if s.backend == nil {
			s.backend, s.query = b, query
		} else if s.backend != b || s.query != query {
			s.mu.Unlock()
			return nil, fmt.Errorf("fixed sqlite read statement cannot change pool or SQL")
		}
		if s.stmt != nil {
			stmt := s.stmt
			s.mu.Unlock()
			return stmt, nil
		}
		if pending := s.preparing; pending != nil {
			s.mu.Unlock()
			select {
			case <-pending:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		s.preparing = make(chan struct{})
		s.mu.Unlock()
		stmt, err := b.prepareFixedRead(ctx, query)
		s.mu.Lock()
		if err == nil {
			s.stmt = stmt
		}
		close(s.preparing)
		s.preparing = nil
		s.mu.Unlock()
		return stmt, err
	}
}

func (s *FixedReadStatement) QueryContext(ctx context.Context, b *Backend, query string, args ...any) (*sql.Rows, error) {
	stmt, err := s.PrepareContext(ctx, b, query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryContext(ctx, args...)
}

func (b *Backend) prepareFixedRead(ctx context.Context, query string) (*sql.Stmt, error) {
	if !b.Valid() {
		return nil, fmt.Errorf("sqlite backend is required")
	}
	b.readStatements.Lock()
	closed := b.readStatements.closed
	b.readStatements.Unlock()
	if closed {
		return nil, errors.New("sql: database is closed")
	}
	stmt, err := b.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	b.readStatements.Lock()
	if b.readStatements.closed {
		b.readStatements.Unlock()
		return nil, errors.Join(errors.New("sql: database is closed"), stmt.Close())
	}
	b.readStatements.owned = append(b.readStatements.owned, stmt)
	b.readStatements.Unlock()
	return stmt, nil
}
