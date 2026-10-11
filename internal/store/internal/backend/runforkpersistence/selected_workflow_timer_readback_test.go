package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/google/uuid"
)

func TestSelectedSuccessorTimerReadbackUsesPermanentResolvedPoint(t *testing.T) {
	for _, kind := range []runfork.RunForkPointKind{runfork.RunForkPointEvent, runfork.RunForkPointRunStart, runfork.RunForkPointDeploymentRevision} {
		for _, postgres := range []bool{false, true} {
			t.Run(string(kind)+map[bool]string{false: "/sqlite", true: "/postgres"}[postgres], func(t *testing.T) {
				f := newSelectedAttachmentFixture(t, kind)
				binding, point := f.evidence.binding, f.evidence.binding.ForkPoint
				if kind == runfork.RunForkPointEvent {
					point.Input, point.EventName, point.ProducedBy, point.ProducedByType = point.EventID, "timer.armed", "root-node", "node"
					point.Timestamp = time.Unix(99, 0).UTC()
				}
				operation, hash, err := (runfork.ForkOperationRequest{
					OperationID: uuid.NewString(), Actor: "operator", IdempotencyKey: "timer-recovery", TransportHash: "transport",
					SourceRunID: binding.SourceRunID, ForkEventID: point.EventID, AtStart: kind == runfork.RunForkPointRunStart,
					ResolvedPoint: &point, TargetBundleHash: f.snapshot.BundleHash, ContractSelection: binding.ContractSelection,
				}).Canonical()
				if err != nil {
					t.Fatal(err)
				}
				raw, err := json.Marshal(operation)
				if err != nil {
					t.Fatal(err)
				}
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				backend, err := postgresbackend.New(db)
				if err != nil {
					t.Fatal(err)
				}
				mock.ExpectBegin()
				expectSelectedAttachmentBinding(mock, binding)
				mock.ExpectQuery(`SELECT CAST\(operation_id AS TEXT\) FROM run_fork_operations`).WithArgs(binding.ForkRunID).
					WillReturnRows(sqlmock.NewRows([]string{"operation"}).AddRow(operation.OperationID))
				mock.ExpectQuery(`FROM run_fork_operations WHERE operation_id`).WithArgs(operation.OperationID).
					WillReturnRows(sqlmock.NewRows([]string{"operation", "actor", "key", "transport", "hash", "request", "kind", "revision", "event", "child", "binding", "state", "result", "failure"}).
						AddRow(operation.OperationID, operation.Actor, operation.IdempotencyKey, operation.TransportHash, hash, raw,
							point.Kind, point.Revision, point.EventID, binding.ForkRunID, binding.BindingID, runfork.ForkOperationMaterialized, nil, nil))
				mock.ExpectRollback()
				stop := errors.New("stop after exact timer replan")
				planned := false
				port := runForkWorkflowTimerReadbackPort{
					postgres: postgres, timers: &workflowTimerMaterializerOwner{}, arrivalSchedules: &arrivalJoinInventoryOwner{},
					snapshot: func(context.Context, *sql.Tx, string) (runlifecycle.Snapshot, error) { return f.snapshot, nil },
					plan: func(_ context.Context, _ *sql.Tx, req runfork.RunForkPlanRequest) (runfork.RunForkPlan, error) {
						planned = true
						if req.SourceRunID != operation.SourceRunID || req.ResolvedPoint == nil || *req.ResolvedPoint != point || req.AtStart != operation.AtStart {
							t.Fatalf("successor replan lost permanent selector/evidence: request=%+v point=%+v", req, point)
						}
						return runfork.RunForkPlan{}, stop
					},
				}
				ctx := correlation.WithRunID(context.Background(), binding.ForkRunID)
				result := mutationprotocol.RunPostgres(ctx, backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil,
					func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
						return struct{}{}, requireSelectedSuccessorWorkflowTimers(ctx, attempt, runfork.SelectedContractRuntimeExecutionIssueRequest{
							Admission:       runfork.RunForkSelectedContractExecutionAdmission{ForkRunID: binding.ForkRunID, SourceRunID: binding.SourceRunID, ForkPoint: binding.ForkPoint},
							DeclarationPlan: f.declarations,
						}, port)
					})
				if result.Acknowledged() || !errors.Is(result.Err(), stop) || !planned {
					t.Fatalf("timer replan was not exact/require-only: acknowledged=%t planned=%t err=%v", result.Acknowledged(), planned, result.Err())
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
