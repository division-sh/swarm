package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/lib/pq"
)

// Backend is the private owner of a PostgreSQL pool. Only store-private
// persistence adapters may retain it; public selected-store facades expose
// closed semantic operations instead.
type Backend struct {
	db                 *sql.DB
	inspectionConfig   *pq.Config
	inspectionDialer   *observationDialer
	inspectionIOActive atomic.Bool
	testTransactions   transactiontest.Slot

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

// NewWithInspectionConfig freezes the same native configuration used by the
// selected pool. A possession observation never borrows its runtime session.
func NewWithInspectionConfig(db *sql.DB, cfg pq.Config) (*Backend, error) {
	b, err := New(db)
	if err != nil {
		return nil, err
	}
	frozen := cfg.Clone()
	b.inspectionConfig = &frozen
	return b, nil
}

func (b *Backend) Valid() bool {
	return b != nil && b.db != nil
}

func (b *Backend) Ping(ctx context.Context) error {
	if !b.Valid() {
		return fmt.Errorf("postgres backend is required")
	}
	if b.inspectionDialer != nil {
		return b.withInspectionIO(ctx, func(native context.Context, _ func() error) error {
			return b.db.PingContext(native)
		})
	}
	return b.db.PingContext(ctx)
}

func (b *Backend) Close() error {
	if !b.Valid() {
		return nil
	}
	if b.inspectionDialer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return b.withInspectionIO(ctx, func(context.Context, func() error) error {
			return b.db.Close()
		})
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
	if err := b.refuseInspectionMutation(ctx); err != nil {
		return nil, err
	}
	if !b.Valid() {
		return nil, fmt.Errorf("postgres backend is required")
	}
	return b.db.BeginTx(ctx, opts)
}

func (b *Backend) Conn(ctx context.Context) (*sql.Conn, error) {
	if err := b.refuseInspectionMutation(ctx); err != nil {
		return nil, err
	}
	if !b.Valid() {
		return nil, fmt.Errorf("postgres backend is required")
	}
	return b.db.Conn(ctx)
}

func (b *Backend) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if err := b.refuseInspectionMutation(ctx); err != nil {
		return nil, err
	}
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
	if tx, err := b.inspectionTransaction(ctx); err != nil {
		return nil, err
	} else if tx != nil {
		return tx.QueryContext(b.inspectionSQLContext(ctx), query, args...)
	}
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
	if tx, err := b.inspectionTransaction(ctx); err != nil {
		return invalidRow(err.Error())
	} else if tx != nil {
		return tx.QueryRowContext(b.inspectionSQLContext(ctx), query, args...)
	}
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
