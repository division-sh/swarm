package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/google/uuid"
)

// Diagnostic only: this confirms the surviving duplicate; it is not closure proof.
func TestIssue2589NestedStateGuardStillReadsRunAfterCanonicalAdmission(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		name := "sqlite"
		if postgres {
			name = "postgres"
		}
		t.Run(name, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			runID := uuid.NewString()
			const bundle = "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			runQuery := "SELECT status, bundle_hash FROM runs WHERE run_id = ?"
			artifactQuery := "SELECT EXISTS (SELECT 1 FROM source_artifacts WHERE bundle_hash = ?)"
			if postgres {
				runQuery = "SELECT status, bundle_hash FROM runs WHERE run_id = $1::uuid FOR UPDATE"
				artifactQuery = "SELECT EXISTS (SELECT 1 FROM source_artifacts WHERE bundle_hash = $1)"
			}
			duplicate := errors.New("observed redundant run admission inside nested state writer")
			mock.ExpectBegin()
			mock.ExpectQuery(runQuery).WithArgs(runID).WillReturnRows(sqlmock.NewRows([]string{"status", "bundle_hash"}).AddRow("running", bundle))
			mock.ExpectQuery(artifactQuery).WithArgs(bundle).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
			mock.ExpectQuery(runQuery).WithArgs(runID).WillReturnError(duplicate)
			mock.ExpectRollback()
			at := time.Now().UTC()
			record := runtimepipeline.WorkflowEngineStateRecord{
				Identity: flowidentity.RunScopedFlowInstance{RunID: runID, Route: flowidentity.StoredRoute("root", "root", "root")},
				EntityID: uuid.NewString(), WorkflowName: "root", Mode: "static", Status: "active", CurrentState: "ready",
				Fields: []byte(`{}`), Bookkeeping: []byte(`{}`), Gates: []byte(`{}`), Accumulator: []byte(`{}`), Config: []byte(`{}`), InitialFields: []byte(`{}`),
				EnteredStageAt: at, CreatedAt: at, UpdatedAt: at, ExpectedState: "ready", ExpectedRevision: 1,
				Transition: runtimepipeline.WorkflowEngineStateTransitionPreserveStateAndCompanion,
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
			if result.Acknowledged() || !errors.Is(result.Err(), duplicate) {
				t.Fatalf("nested duplicate not observed: acknowledged=%t err=%v", result.Acknowledged(), result.Err())
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
