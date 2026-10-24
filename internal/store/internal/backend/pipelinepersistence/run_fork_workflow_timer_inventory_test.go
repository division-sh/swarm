package pipelinepersistence

import (
	"context"
	"errors"

	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"

	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

type forkTimerInventoryReader interface {
	ReadRunForkWorkflowTimerInventoryTx(context.Context, *mutationprotocol.Attempt, string) ([]pipeline.WorkflowTimerActivation, error)
}

func forkTimerInventoryContext(runID string) context.Context {
	ctx := correlation.WithRunID(context.Background(), runID)
	ctx = correlation.WithSourceArtifactFact(ctx, sourceartifactfixture.Fact())
	return authoractivity.WithScope(ctx, authoractivity.BundleScope("timer-inventory-test", sourceartifactfixture.BundleHash))
}

// SQLmock supplies rows, but the real mutation constructor and lifecycle source
// loader own the frame and admission. No test installs native authority.
func withForkTimerInventoryAttempt(t *testing.T, postgres bool, caller context.Context, expectations func(sqlmock.Sqlmock), check func(context.Context, *mutationprotocol.Attempt, forkTimerInventoryReader)) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	expectations(mock)
	mock.ExpectRollback()
	rollback := errors.New("fork timer inventory unit rollback")
	var reader forkTimerInventoryReader
	write := func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		check(ctx, attempt, reader)
		return struct{}{}, rollback
	}
	var result mutationprotocol.Result[struct{}]
	if postgres {
		backend, err := postgresbackend.New(db)
		if err != nil {
			t.Fatal(err)
		}
		reader = &PipelinePostgresOwner{RunLifecyclePostgresOwner: &runlifecycle.RunLifecyclePostgresOwner{}}
		result = mutationprotocol.RunPostgres(caller, backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, write)
	} else {
		backend, err := sqlitebackend.New(db)
		if err != nil {
			t.Fatal(err)
		}
		reader = &PipelineSQLiteOwner{RunLifecycleSQLiteOwner: &runlifecycle.RunLifecycleSQLiteOwner{}}
		result = mutationprotocol.RunSQLite(caller, backend, "fork timer inventory unit", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, write)
	}
	if result.Acknowledged() || !errors.Is(result.Err(), rollback) {
		t.Fatalf("inventory unit outcome: acknowledged=%t err=%v", result.Acknowledged(), result.Err())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func forkTimerInventoryDialects(t *testing.T, check func(*testing.T, bool)) {
	t.Helper()
	for _, postgres := range []bool{false, true} {
		t.Run(map[bool]string{false: "sqlite", true: "postgres"}[postgres], func(t *testing.T) { check(t, postgres) })
	}
}

func expectForkTimerInventorySource(mock sqlmock.Sqlmock, postgres bool, runID, status, bundle string, artifact bool) {
	query := `SELECT status, bundle_hash FROM runs WHERE run_id = ?`
	if postgres {
		query = `SELECT status, bundle_hash FROM runs WHERE run_id = $1::uuid FOR UPDATE`
	}
	rows := sqlmock.NewRows([]string{"status", "bundle_hash"})
	if status != "" {
		rows.AddRow(status, bundle)
	}
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(runID).WillReturnRows(rows)
	if status == "running" || status == "paused" {
		expectForkTimerInventoryArtifact(mock, postgres, bundle, artifact)
	}
}

func expectForkTimerInventoryArtifact(mock sqlmock.Sqlmock, postgres bool, bundle string, present bool) {
	query := `SELECT EXISTS (SELECT 1 FROM source_artifacts WHERE bundle_hash = ?)`
	if postgres {
		query = `SELECT EXISTS (SELECT 1 FROM source_artifacts WHERE bundle_hash = $1)`
	}
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(bundle).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(present))
}

func forkTimerInventoryQuery(postgres bool) string {
	query := `SELECT CAST(timer_id AS TEXT), timer_name FROM timers
		WHERE run_id = ? AND task_type = 'workflow_timer'
		AND (source_timer_id IS NOT NULL OR forked_from_run_id IS NOT NULL
		     OR reconstruction_owner IS NOT NULL OR forked_from_event_id IS NOT NULL
		     OR forked_from_point_kind IS NOT NULL OR forked_from_point_revision IS NOT NULL
		     OR source_armed_at IS NOT NULL)
		ORDER BY timer_id`
	if postgres {
		query = strings.Replace(query, "run_id = ?", "run_id = $1::uuid", 1)
	}
	return regexp.QuoteMeta(query)
}

func forkTimerInventoryRows(timers ...pipeline.WorkflowTimerActivation) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"timer_id", "timer_name"})
	for _, timer := range timers {
		rows.AddRow(timer.Ref.ActivationID, timer.Ref.TaskID())
	}
	return rows
}

func expectForkTimerInventoryActivation(t *testing.T, mock sqlmock.Sqlmock, postgres bool, activation pipeline.WorkflowTimerActivation) {
	t.Helper()
	query := `(?s)^SELECT timer_name, .*FROM timers WHERE timer_id = \? AND task_type = 'workflow_timer'$`
	if postgres {
		query = `(?s)^SELECT timer_name, .*FROM timers WHERE timer_id = \$1::uuid AND task_type = 'workflow_timer' FOR UPDATE$`
	}
	mock.ExpectQuery(query).WithArgs(activation.Ref.ActivationID).WillReturnRows(nativeTimerRows(t, activation, true))
}
