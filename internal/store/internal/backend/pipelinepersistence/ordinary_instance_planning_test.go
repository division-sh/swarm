package pipelinepersistence

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestOrdinaryPlanningMemoKeepsLaterReadFailuresAndCancellation(t *testing.T) {
	artifact := sourceartifactfixture.Artifact()
	source := semanticview.Wrap(&contracts.WorkflowContractBundle{
		SourceArtifact: artifact, Semantics: contracts.WorkflowSemanticView{Version: "original"},
		FlowSchemas: map[string]contracts.FlowSchemaDocument{"empty": {}, "failure": {}},
	})
	fact := sourceartifactfixture.FactFor(artifact)
	runID := "11111111-1111-1111-1111-111111111111"
	scope, err := pipeline.NewFlowInstanceLookupScope(source, fact, runID, []string{"empty"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := pipeline.NewFlowInstanceLookupScope(source, fact, runID, []string{"failure"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := pipeline.NewFlowInstanceLookupScope(source, fact, "22222222-2222-2222-2222-222222222222", []string{"empty"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := &flowInstanceIndexSnapshot{tx: tx, source: source, run: runlifecycle.Snapshot{RunID: runID}}
	reader := &flowInstanceIndexPlanningReader{snapshot: snapshot, scope: scope}
	query := sqliteWorkflowInstanceSelect + ` WHERE fi.run_id=$1 AND fi.flow_template=$2 ORDER BY fi.instance_path`
	mock.ExpectQuery(query).WithArgs(runID, "empty").WillReturnRows(sqlmock.NewRows([]string{"instance_path"})).RowsWillBeClosed()
	for i := 0; i < 3; i++ {
		if members, err := reader.ListFlowInstances(ctx, scope); err != nil || len(members) != 0 {
			t.Fatalf("same-cut empty inventory changed: %+v err=%v", members, err)
		}
	}
	if members, err := reader.ListFlowInstances(ctx, foreign); err == nil || len(members) != 0 {
		t.Fatal("same-cut reader admitted a foreign run")
	}
	want := errors.New("independent later declaration read failure")
	mock.ExpectQuery(query).WithArgs(runID, "failure").WillReturnError(want)
	if members, err := reader.ListFlowInstances(ctx, failed); !errors.Is(err, want) || len(members) != 0 {
		t.Fatalf("earlier successful memo hid a later read failure: %+v err=%v", members, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if members, err := reader.ListFlowInstances(cancelled, scope); !errors.Is(err, context.Canceled) || len(members) != 0 {
		t.Fatalf("memo hid cancellation: %+v err=%v", members, err)
	}
	reader.snapshot = nil
	if members, err := reader.ListFlowInstances(ctx, scope); err == nil || len(members) != 0 {
		t.Fatal("memo outlived its read transaction")
	}
	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
