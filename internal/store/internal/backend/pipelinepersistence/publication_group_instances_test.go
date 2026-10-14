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

func TestGroupInstancePlanningKeepsScopesIsolatedAndReobservesOnce(t *testing.T) {
	ctx := context.Background()
	artifact := sourceartifactfixture.Artifact()
	source := semanticview.Wrap(&contracts.WorkflowContractBundle{
		SourceArtifact: artifact, Semantics: contracts.WorkflowSemanticView{Version: "original"},
		FlowSchemas: map[string]contracts.FlowSchemaDocument{"worker": {}, "corrupt": {}},
	})
	fact := sourceartifactfixture.FactFor(artifact)
	runID := "11111111-1111-1111-1111-111111111111"
	scope, err := pipeline.NewFlowInstanceLookupScope(source, fact, runID, []string{"worker"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	corrupt, err := pipeline.NewFlowInstanceLookupScope(source, fact, runID, []string{"corrupt"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	query := sqliteWorkflowInstanceSelect + ` WHERE fi.run_id=$1 AND fi.flow_template=$2 ORDER BY fi.instance_path`
	reads := &flowInstancePlanningReadSet{source: source, fact: fact, runID: runID}
	mock.ExpectBegin()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	reader := &groupInstancePlanningReader{snapshot: &flowInstanceIndexSnapshot{tx: tx, source: source, run: runlifecycle.Snapshot{RunID: runID}}, readSet: reads}
	want := &pipeline.FlowInstanceConstructionCorruption{RunID: runID, FlowID: "corrupt", Cause: errors.New("malformed sibling")}
	mock.ExpectQuery(query).WithArgs(runID, "corrupt").WillReturnError(want)
	if _, err := reader.ListFlowInstances(ctx, corrupt); err != want {
		t.Fatalf("corruption was hidden or reclassified: %v", err)
	}
	mock.ExpectQuery(query).WithArgs(runID, "worker").WillReturnRows(sqlmock.NewRows([]string{"instance_path"})).RowsWillBeClosed()
	for i := 0; i < 32; i++ {
		if observations, err := reader.ListFlowInstances(ctx, scope); err != nil || len(observations) != 0 {
			t.Fatalf("member %d inherited corruption or new audience: %+v %v", i, observations, err)
		}
	}
	if len(reads.reads) != 1 || reads.reads[0].scope.FlowIDs()[0] != "worker" {
		t.Fatal("failed or duplicate member scope entered the successful read set")
	}
	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	reader.snapshot, reader.readSet = nil, nil
	if _, err := reader.ListFlowInstances(ctx, scope); err == nil {
		t.Fatal("ended preparation reader admitted another operation")
	}
	mock.ExpectBegin()
	currentTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	current := flowInstanceIndexSnapshot{tx: currentTx, source: source, run: runlifecycle.Snapshot{RunID: runID}}
	mock.ExpectQuery(query).WithArgs(runID, "worker").WillReturnRows(sqlmock.NewRows([]string{"instance_path"})).RowsWillBeClosed()
	if err := reads.reobserve(ctx, &current); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := reads.reobserve(cancelled, &current); !errors.Is(err, context.Canceled) {
		t.Fatalf("memo hid cancellation: %v", err)
	}
	// A fresh commit snapshot never borrows the previous snapshot's memo.
	mock.ExpectQuery(query).WithArgs(runID, "worker").WillReturnError(want)
	failed := flowInstanceIndexSnapshot{tx: currentTx, source: source, run: runlifecycle.Snapshot{RunID: runID}}
	if err := reads.reobserve(ctx, &failed); err != want {
		t.Fatalf("malformed current evidence became retryable staleness: %v", err)
	}
	mock.ExpectRollback()
	if err := currentTx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
