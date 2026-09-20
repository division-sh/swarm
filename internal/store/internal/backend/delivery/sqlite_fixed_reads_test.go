package delivery

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/google/uuid"
)

func TestSQLiteDeliveryFixedReadQueryOrderAndReuse(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	b, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	a := &Adapter{dialect: DialectSQLite, sqliteReads: &sqliteDeliveryReads{backend: b}}
	ctx := context.Background()
	id := uuid.NewString()
	byID := a.selectRecord() + ` WHERE d.delivery_id = ?`
	membership := `SELECT CAST(delivery_id AS TEXT), CAST(event_id AS TEXT) FROM event_deliveries WHERE event_id IN (?)`
	records := a.selectRecord() + ` WHERE d.event_id IN (?) ORDER BY d.created_at, d.delivery_id`
	// Strict expectations prove two prepares total, unchanged SQL/arguments,
	// and the original execution count/order on every observation.
	for round := 0; round < 3; round++ {
		mock.ExpectQuery(byID).WithArgs(id).WillReturnRows(sqlmock.NewRows([]string{"delivery_id"})).RowsWillBeClosed()
		if _, err := a.Snapshot(ctx, b, id); !errors.Is(err, runtimedelivery.ErrNotFound) {
			t.Fatal(err)
		}
		mock.ExpectQuery(`SELECT ` + sqliteDatabaseNowExpression).WillReturnRows(sqlmock.NewRows([]string{"now"}).AddRow("2026-09-20T01:02:03.456Z")).RowsWillBeClosed()
		if round == 0 {
			mock.ExpectPrepare(membership).WillBeClosed()
		}
		mock.ExpectQuery(membership).WithArgs(id).WillReturnRows(sqlmock.NewRows([]string{"delivery_id", "event_id"})).RowsWillBeClosed()
		if round == 0 {
			mock.ExpectPrepare(records).WillBeClosed()
		}
		mock.ExpectQuery(records).WithArgs(id).WillReturnRows(sqlmock.NewRows([]string{"delivery_id"})).RowsWillBeClosed()
		if got, err := a.SnapshotsForEvent(ctx, b, id); err != nil || got == nil || len(got) != 0 {
			t.Fatalf("empty: %v %v", got, err)
		}
	}
	// Even an adapter with warm slots must leave a raw transaction untouched.
	mock.ExpectBegin()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery(byID).WithArgs(id).WillReturnRows(sqlmock.NewRows([]string{"delivery_id"})).RowsWillBeClosed()
	if _, err := a.Snapshot(ctx, tx, id); !errors.Is(err, runtimedelivery.ErrNotFound) {
		t.Fatal(err)
	}
	mock.ExpectQuery(`SELECT ` + sqliteDatabaseNowExpression).WillReturnRows(sqlmock.NewRows([]string{"now"}).AddRow("2026-09-20T01:02:04.456Z")).RowsWillBeClosed()
	mock.ExpectQuery(membership).WithArgs(id).WillReturnRows(sqlmock.NewRows([]string{"delivery_id", "event_id"})).RowsWillBeClosed()
	mock.ExpectQuery(records).WithArgs(id).WillReturnRows(sqlmock.NewRows([]string{"delivery_id"})).RowsWillBeClosed()
	if _, err := a.SnapshotsForEvent(ctx, tx, id); err != nil {
		t.Fatal(err)
	}
	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if a.poolReads(db) != nil || a.poolReads(tx) != nil || sqliteDeliveryAdapter.poolReads(b) != nil {
		t.Fatal("raw or package-global adapter acquired slots")
	}
	other, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if a.poolReads(other) != nil {
		t.Fatal("another backend acquired this owner's slots")
	}
	mock.ExpectClose()
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
