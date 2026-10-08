package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/google/uuid"
)

// Statement/order control only; native correctness is covered separately.
func TestIssue2589NestedStateGuardReusesRunAdmissionAndPreservesState(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		name := "sqlite"
		if postgres {
			name = "postgres"
		}
		for _, cut := range []string{"acknowledged", "missing_artifact", "stale_header", "stale_state"} {
			t.Run(name+"/"+cut, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				runID := uuid.NewString()
				const bundle = "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
				runQuery := `^SELECT\s+status, bundle_hash FROM runs WHERE run_id = \?$`
				artifactQuery := `^SELECT\s+EXISTS \(SELECT 1 FROM source_artifacts WHERE bundle_hash = \?\)$`
				if postgres {
					runQuery = `^SELECT\s+status, bundle_hash FROM runs WHERE run_id = \$1::uuid FOR UPDATE$`
					artifactQuery = `^SELECT\s+EXISTS \(SELECT 1 FROM source_artifacts WHERE bundle_hash = \$1\)$`
				}
				mock.ExpectBegin()
				mock.ExpectQuery(runQuery).WithArgs(runID).WillReturnRows(sqlmock.NewRows([]string{"status", "bundle_hash"}).AddRow("running", bundle))
				mock.ExpectQuery(artifactQuery).WithArgs(bundle).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
				mock.ExpectQuery(artifactQuery).WithArgs(bundle).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(cut != "missing_artifact"))
				at := time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)
				record := runtimepipeline.WorkflowEngineStateRecord{
					Identity: flowidentity.RunScopedFlowInstance{RunID: runID, Route: flowidentity.StoredRoute("root", "root", "root")},
					EntityID: uuid.NewString(), WorkflowName: "root", Mode: "static", Status: "active", StageDefined: true, CurrentState: "ready",
					Fields: []byte(`{}`), Bookkeeping: []byte(`{}`), Gates: []byte(`{}`), Accumulator: []byte(`{}`), Config: []byte(`{"instance_id":"root","storage_ref":"root","flow_path":"root"}`), InitialFields: []byte(`{}`),
					EnteredStageAt: at, CreatedAt: at, UpdatedAt: at, ExpectedState: "ready", ExpectedRevision: 1,
					Transition: runtimepipeline.WorkflowEngineStateTransitionPreserveStateAndCompanion,
				}
				if cut == "stale_state" {
					record.ExpectedState = "waiting"
				}
				if cut != "missing_artifact" {
					if postgres {
						mock.ExpectExec(`^SELECT\s+pg_advisory_xact_lock\(hashtextextended\(\$1, 0\)\)$`).
							WithArgs("36:" + runID + record.Identity.Route.InstancePath).WillReturnResult(sqlmock.NewResult(0, 1))
					}
					query := sqliteWorkflowTargetPersistenceSelect
					if postgres {
						query = postgresWorkflowTargetPersistenceSelect
					}
					revision := int64(1)
					if cut == "stale_header" {
						revision = 2
					}
					mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(runID, record.EntityID, record.Identity.Route.InstancePath).
						WillReturnRows(issue2589FieldlessTargetRows(record, revision))
				}
				if cut == "acknowledged" {
					mock.ExpectCommit()
				} else {
					mock.ExpectRollback()
				}
				write := func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
					err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
						if postgres {
							_, err = (&runlifecycle.RunLifecyclePostgresOwner{}).RequireActiveSourceTx(ctx, tx, runID)
						} else {
							_, err = (&runlifecycle.RunLifecycleSQLiteOwner{}).RequireActiveSourceTx(ctx, tx, runID)
						}
						if err != nil {
							return err
						}
						return commitWorkflowEngineState(ctx, attempt, postgres, record)
					})
					return struct{}{}, err
				}
				var result mutationprotocol.Result[struct{}]
				if postgres {
					backend, err := postgresbackend.New(db)
					if err != nil {
						t.Fatal(err)
					}
					result = mutationprotocol.RunPostgres(context.Background(), backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, write)
				} else {
					backend, err := sqlitebackend.New(db)
					if err != nil {
						t.Fatal(err)
					}
					result = mutationprotocol.RunSQLite(context.Background(), backend, "nested run-admission probe", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, write)
				}
				if cut == "acknowledged" {
					if !result.Acknowledged() || result.Err() != nil {
						t.Fatalf("preserve-state commit: acknowledged=%t err=%v", result.Acknowledged(), result.Err())
					}
				} else if result.Acknowledged() || result.Err() == nil {
					t.Fatalf("negative control acknowledged: %t err=%v", result.Acknowledged(), result.Err())
				} else if cut == "missing_artifact" {
					var unavailable *runtimerunlifecycle.SourceArtifactUnavailableError
					if !errors.As(result.Err(), &unavailable) {
						t.Fatalf("fresh artifact refusal = %v", result.Err())
					}
				} else if failure, ok := runtimefailures.As(result.Err()); !ok || failure.Failure.Detail.Code != "workflow_engine_state_revision_conflict" {
					t.Fatalf("header revision refusal = %v", result.Err())
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func issue2589FieldlessTargetRows(record runtimepipeline.WorkflowEngineStateRecord, revision int64) *sqlmock.Rows {
	columns := make([]string, 34)
	for i := range columns {
		columns[i] = "column" + string(rune('A'+i))
	}
	return sqlmock.NewRows(columns).AddRow(
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		record.Identity.Route.InstancePath, record.WorkflowName, nil, record.Mode, record.Status,
		string(record.Config), nil, record.CreatedAt, record.EntityID, nil, nil, nil,
		record.CurrentState, revision, record.EnteredStageAt, record.UpdatedAt, record.StageDefined,
		string(record.Gates), string(record.Bookkeeping), string(record.Accumulator),
	)
}
