package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
)

// Backend is the private owner of a SQLite pool. Only store-private
// persistence adapters may retain it; public selected-store facades expose
// closed semantic operations instead.
type Backend struct {
	db *sql.DB

	mutationToken chan struct{}
	mutationState struct {
		sync.Mutex
		active        mutationOperation
		waiting       int
		nextSequence  uint64
		ring          [mutationDiagnosticRingCapacity]mutationAttemptTrace
		ringNext      int
		ringCount     int
		labelActivity map[string]mutationLabelAggregate
	}
	firstBusyObservation                  sync.Once
	firstCancellationObservation          sync.Once
	firstAdmissionCancellationObservation sync.Once
}

func New(db *sql.DB) (*Backend, error) {
	if db == nil {
		return nil, fmt.Errorf("sqlite database is required")
	}
	backend := &Backend{db: db, mutationToken: make(chan struct{}, 1)}
	backend.mutationToken <- struct{}{}
	return backend, nil
}

func (b *Backend) Valid() bool {
	return b != nil && b.db != nil && b.mutationToken != nil
}

func (b *Backend) Ping(ctx context.Context) error {
	if !b.Valid() {
		return fmt.Errorf("sqlite backend is required")
	}
	return b.db.PingContext(ctx)
}

func (b *Backend) Close() error {
	if !b.Valid() {
		return nil
	}
	return b.db.Close()
}

// ConstructionHandle returns the separately owned process-construction
// handle. Runtime adapters must never retain the returned capability.
func (b *Backend) ConstructionHandle() *sql.DB {
	if !b.Valid() {
		return nil
	}
	return b.db
}

func (b *Backend) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	if !b.Valid() {
		return nil, fmt.Errorf("sqlite backend is required")
	}
	return b.db.BeginTx(ctx, opts)
}

func (b *Backend) Conn(ctx context.Context) (*sql.Conn, error) {
	if !b.Valid() {
		return nil, fmt.Errorf("sqlite backend is required")
	}
	return b.db.Conn(ctx)
}

func (b *Backend) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if !b.Valid() {
		return nil, fmt.Errorf("sqlite backend is required")
	}
	return b.db.ExecContext(ctx, query, args...)
}

func (b *Backend) Exec(query string, args ...any) (sql.Result, error) {
	if !b.Valid() {
		return nil, fmt.Errorf("sqlite backend is required")
	}
	return b.db.Exec(query, args...)
}

func (b *Backend) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if !b.Valid() {
		return nil, fmt.Errorf("sqlite backend is required")
	}
	return b.db.QueryContext(ctx, query, args...)
}

func (b *Backend) Query(query string, args ...any) (*sql.Rows, error) {
	if !b.Valid() {
		return nil, fmt.Errorf("sqlite backend is required")
	}
	return b.db.Query(query, args...)
}

func (b *Backend) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if !b.Valid() {
		return invalidRow("sqlite backend is required")
	}
	return b.db.QueryRowContext(ctx, query, args...)
}

func (b *Backend) QueryRow(query string, args ...any) *sql.Row {
	if !b.Valid() {
		return invalidRow("sqlite backend is required")
	}
	return b.db.QueryRow(query, args...)
}

func invalidRow(message string) *sql.Row {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		panic(message)
	}
	_ = db.Close()
	return db.QueryRow("SELECT 1")
}
