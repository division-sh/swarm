package pipelinepersistence

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func TestIndexDeclarationMemoIsSnapshotLocalAndPreservesCancellation(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	query := sqliteWorkflowInstanceSelect + ` WHERE fi.run_id=$1 AND fi.flow_template=$2 ORDER BY fi.instance_path`
	ctx := context.Background()
	scope := pipeline.FlowInstanceLookupScope{}
	snapshot := flowInstanceIndexSnapshot{tx: tx, run: runlifecycle.Snapshot{}}
	read := func(owner *flowInstanceIndexSnapshot) {
		t.Helper()
		observations, err := owner.observeDeclaration(ctx, scope, "worker")
		if err != nil || len(observations) != 0 {
			t.Fatalf("empty declaration observation=%+v err=%v", observations, err)
		}
	}
	mock.ExpectQuery(query).WithArgs("", "worker").WillReturnRows(sqlmock.NewRows([]string{"instance_path"})).RowsWillBeClosed()
	read(&snapshot)
	read(&snapshot)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := snapshot.observeDeclaration(cancelled, scope, "worker"); !errors.Is(err, context.Canceled) {
		t.Fatalf("memo hid cancellation: %v", err)
	}
	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	mock.ExpectBegin()
	next, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery(query).WithArgs("", "worker").WillReturnRows(sqlmock.NewRows([]string{"instance_path"})).RowsWillBeClosed()
	read(&flowInstanceIndexSnapshot{tx: next})
	mock.ExpectRollback()
	if err := next.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestIndexDeclarationMemoDoesNotCacheFailuresAcrossScopes(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	snapshot := flowInstanceIndexSnapshot{tx: tx}
	query := sqliteWorkflowInstanceSelect + ` WHERE fi.run_id=$1 AND fi.flow_template=$2 ORDER BY fi.instance_path`
	want := errors.New("independent declaration read failure")
	mock.ExpectQuery(query).WithArgs("", "corrupt").WillReturnError(want)
	if _, err := snapshot.observeDeclaration(ctx, pipeline.FlowInstanceLookupScope{}, "corrupt"); !errors.Is(err, want) {
		t.Fatalf("read failure was reclassified: %v", err)
	}
	for _, flow := range []string{"valid", "corrupt"} {
		mock.ExpectQuery(query).WithArgs("", flow).WillReturnRows(sqlmock.NewRows([]string{"instance_path"})).RowsWillBeClosed()
		if _, err := snapshot.observeDeclaration(ctx, pipeline.FlowInstanceLookupScope{}, flow); err != nil {
			t.Fatalf("scope %s inherited another read failure: %v", flow, err)
		}
	}
	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
