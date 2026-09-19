package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

// Backend is the private owner of a PostgreSQL pool. Only store-private
// persistence adapters may retain it; public selected-store facades expose
// closed semantic operations instead.
type Backend struct {
	db               *sql.DB
	testTransactions transactiontest.Slot

	capacityMu           sync.Mutex
	baseOpenConnections  int
	capacityReservations int
}

func (b *Backend) InstallTransactionProbeForTest(options transactiontest.Options) (*transactiontest.Collector, func(), error) {
	return b.testTransactions.Install(options)
}

func (b *Backend) NewSessionAuthority(conn *sql.Conn) (*SessionAuthority, error) {
	session, err := NewSessionAuthority(conn)
	if err == nil {
		session.testTransactions = &b.testTransactions
	}
	return session, err
}

// RetainConnectionCapacity reserves one pool slot for a dedicated private
// backend session and returns an idempotent release.
func (b *Backend) RetainConnectionCapacity() func() {
	if !b.Valid() {
		return func() {}
	}
	b.capacityMu.Lock()
	if b.capacityReservations == 0 {
		b.baseOpenConnections = b.db.Stats().MaxOpenConnections
	}
	b.capacityReservations++
	if b.baseOpenConnections > 0 {
		b.db.SetMaxOpenConns(b.baseOpenConnections + b.capacityReservations)
	}
	b.capacityMu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			b.capacityMu.Lock()
			if b.capacityReservations > 0 {
				b.capacityReservations--
			}
			if b.baseOpenConnections > 0 {
				b.db.SetMaxOpenConns(b.baseOpenConnections + b.capacityReservations)
			}
			b.capacityMu.Unlock()
		})
	}
}

func (b *Backend) CapacityReservationsForTest() int {
	if b == nil {
		return 0
	}
	b.capacityMu.Lock()
	defer b.capacityMu.Unlock()
	return b.capacityReservations
}

// ConnectionCapacity observes the pool owner without resizing it. Callers must
// subtract dedicated reservations before allocating shared execution capacity.
func (b *Backend) ConnectionCapacity() (maximum, dedicated int, err error) {
	if !b.Valid() {
		return 0, 0, fmt.Errorf("postgres backend is required")
	}
	b.capacityMu.Lock()
	defer b.capacityMu.Unlock()
	return b.db.Stats().MaxOpenConnections, b.capacityReservations, nil
}

func New(db *sql.DB) (*Backend, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres database is required")
	}
	return &Backend{db: db}, nil
}

func (b *Backend) Valid() bool {
	return b != nil && b.db != nil
}

func (b *Backend) Ping(ctx context.Context) error {
	if !b.Valid() {
		return fmt.Errorf("postgres backend is required")
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
		return nil, fmt.Errorf("postgres backend is required")
	}
	return b.db.BeginTx(ctx, opts)
}

func (b *Backend) Conn(ctx context.Context) (*sql.Conn, error) {
	if !b.Valid() {
		return nil, fmt.Errorf("postgres backend is required")
	}
	return b.db.Conn(ctx)
}

func (b *Backend) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if !b.Valid() {
		return nil, fmt.Errorf("postgres backend is required")
	}
	return b.db.ExecContext(ctx, query, args...)
}

func (b *Backend) Exec(query string, args ...any) (sql.Result, error) {
	if !b.Valid() {
		return nil, fmt.Errorf("postgres backend is required")
	}
	return b.db.Exec(query, args...)
}

func (b *Backend) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if !b.Valid() {
		return nil, fmt.Errorf("postgres backend is required")
	}
	return b.db.QueryContext(ctx, query, args...)
}

func (b *Backend) Query(query string, args ...any) (*sql.Rows, error) {
	if !b.Valid() {
		return nil, fmt.Errorf("postgres backend is required")
	}
	return b.db.Query(query, args...)
}

func (b *Backend) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if !b.Valid() {
		return invalidRow("postgres backend is required")
	}
	return b.db.QueryRowContext(ctx, query, args...)
}

func (b *Backend) QueryRow(query string, args ...any) *sql.Row {
	if !b.Valid() {
		return invalidRow("postgres backend is required")
	}
	return b.db.QueryRow(query, args...)
}

func invalidRow(message string) *sql.Row {
	db, err := sql.Open("postgres", "")
	if err != nil {
		panic(message)
	}
	_ = db.Close()
	return db.QueryRow("SELECT 1")
}
