package pipelinepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

// This is a SQL count/order regression, not mock timing or native correctness
// evidence. The full command includes state, timer, history, revision and story.
func TestIssue2589FullEngineCommandStatementCount(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		name := "sqlite"
		if postgres {
			name = "postgres"
		}
		for _, casLost := range []bool{false, true} {
			cut := "acknowledged"
			if casLost {
				cut = "cas_lost"
			}
			t.Run(name+"/"+cut, func(t *testing.T) {
				var statements []string
				matcher := sqlmock.QueryMatcherFunc(func(expected, actual string) error {
					if err := sqlmock.QueryMatcherRegexp.Match(expected, actual); err != nil {
						return err
					}
					statements = append(statements, strings.Join(strings.Fields(actual), " "))
					return nil
				})
				db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(matcher))
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				at := time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)
				runID, entityID := uuid.NewString(), uuid.NewString()
				record := pipeline.WorkflowEngineStateRecord{
					Identity: flowidentity.RunScopedFlowInstance{RunID: runID, Route: flowidentity.StoredRoute("root", "root", "root")},
					EntityID: entityID, EntityType: "task", WorkflowName: "root", Mode: "static", Status: "active",
					StageDefined: true, CurrentState: "done", ExpectedState: "ready", ExpectedRevision: 1,
					Fields: []byte(`{}`), InitialFields: []byte(`{}`), Bookkeeping: []byte(`{}`), Gates: []byte(`{}`), Accumulator: []byte(`{}`),
					Config:         []byte(`{"instance_id":"root","storage_ref":"root","flow_path":"root"}`),
					EnteredStageAt: at.Add(time.Second), CreatedAt: at, UpdatedAt: at.Add(time.Second),
					Transition: pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion,
				}
				source, err := events.NewRootRoutingSource(entityID)
				if err != nil {
					t.Fatal(err)
				}
				timer := pipeline.WorkflowTimerActivation{
					Ref:   timeridentity.WorkflowTimerActivationRef{ActivationID: uuid.NewString(), DeclarationKey: "ready.timeout", DeclarationRevision: "revision", Cause: timeridentity.WorkflowTimerActivationCauseInitial},
					RunID: runID, EntityID: entityID, Route: record.Identity.Route, RoutingSource: source,
					OwnerAgent: "timer-owner", EventType: "timer.elapsed", Payload: []byte(`{}`), ExecutionMode: executionmode.Live,
					CreatedAt: at, FireAt: at.Add(time.Hour), Status: "active",
				}
				command := pipeline.WorkflowEngineMutationCommand{State: record, Lifecycle: pipeline.WorkflowLifecycleMutationPlan{
					Timers: []pipeline.WorkflowTimerMutation{{Kind: pipeline.WorkflowTimerMutationCancel, Activation: timer}},
				}}
				mock.ExpectBegin()
				issue2589ExpectStoryLock(mock, postgres)
				runQuery := `^SELECT\s+status, bundle_hash FROM runs WHERE run_id = \?$`
				if postgres {
					runQuery = `^SELECT\s+status, bundle_hash FROM runs WHERE run_id = \$1::uuid FOR UPDATE$`
				}
				mock.ExpectQuery(runQuery).WithArgs(runID).WillReturnRows(issue2589Rows("status bundle_hash", "running", sourceartifactfixture.BundleHash))
				issue2589ExpectArtifact(mock, postgres)
				projection := `(?s)^SELECT\s+fi.current_state, es.fields, fi.bookkeeping, fi.gates, fi.accumulator .*WHERE fi.run_id = \? AND fi.entity_id = \? AND fi.instance_path = \?$`
				if postgres {
					projection = `(?s)^SELECT\s+fi.current_state, es.fields, fi.bookkeeping, fi.gates, fi.accumulator .*WHERE fi.run_id = \$1::uuid AND fi.entity_id = \$2::uuid AND fi.instance_path = \$3 FOR UPDATE OF fi$`
				}
				mock.ExpectQuery(projection).WithArgs(runID, entityID, "root").WillReturnRows(issue2589Rows("state fields bookkeeping gates accumulator", "ready", "{}", "{}", "{}", "{}"))
				issue2589ExpectArtifact(mock, postgres)
				if postgres {
					mock.ExpectExec(`^SELECT\s+pg_advisory_xact_lock\(hashtextextended\(\$1, 0\)\)$`).WithArgs("36:" + runID + "root").WillReturnResult(sqlmock.NewResult(0, 1))
				}
				cas := `(?s)^UPDATE\s+flow_instances SET .*WHERE run_id = \? AND instance_path = \? AND entity_id = \? AND revision = \? AND current_state = \? AND flow_template = \? AND mode = \? AND entity_type IS NULLIF\(\?, ''\) RETURNING CAST\(run_id AS TEXT\), CAST\(entity_id AS TEXT\)$`
				if postgres {
					cas = `(?s)^UPDATE\s+flow_instances SET .*WHERE run_id = \$12::uuid AND instance_path = \$13 AND entity_id = \$14::uuid AND revision = \$15 AND current_state = \$16 AND flow_template = \$17 AND mode = \$18 AND entity_type IS NOT DISTINCT FROM NULLIF\(\$19, ''\) RETURNING CAST\(run_id AS TEXT\), CAST\(entity_id AS TEXT\)$`
				}
				casRows := issue2589Rows("run_id entity_id")
				if !casLost {
					casRows.AddRow(runID, entityID)
				}
				mock.ExpectQuery(cas).WithArgs("", "", string(record.Config), "active", "done", "{}", "{}", "{}", record.EnteredStageAt, record.UpdatedAt, nil, runID, "root", entityID, int64(1), "ready", "root", "static", "task").WillReturnRows(casRows)
				if casLost {
					mock.ExpectRollback()
				} else {
					mock.ExpectExec(`(?s)^UPDATE\s+entity_state SET .*revision = revision \+ 1, .*WHERE run_id = .* AND entity_id = .* AND flow_instance = .* AND entity_type = .*$`).
						WithArgs("", "", "done", "{}", "{}", "{}", "{}", record.EnteredStageAt, record.UpdatedAt, runID, entityID, "root", "task").WillReturnResult(sqlmock.NewResult(0, 1))
					issue2589ExpectTimerCancellation(mock, postgres, timer)
					issue2589ExpectArtifact(mock, postgres)
					mutationRows := issue2589Rows("mutation_id entity_id domain path new_value caused_by_event created_at")
					var mutationID string
					capture := issue2589Argument(func(value driver.Value) bool {
						id, ok := value.(string)
						if !ok || uuid.Validate(id) != nil {
							return false
						}
						mutationID = id
						return true
					})
					captureTime := issue2589Argument(func(value driver.Value) bool {
						createdAt, ok := value.(time.Time)
						if !ok || createdAt.IsZero() {
							return false
						}
						mutationRows.AddRow(mutationID, entityID, "lifecycle_state", "", `"done"`, nil, createdAt)
						return true
					})
					mock.ExpectExec(`(?s)^INSERT\s+INTO entity_mutations \(.*\) VALUES \(.*\)$`).
						WithArgs(capture, runID, entityID, "lifecycle_state", "", `"ready"`, `"done"`, "", "platform", "workflow_engine", "mutate", captureTime).WillReturnResult(sqlmock.NewResult(0, 1))
					issue2589ExpectEngineRevision(mock, postgres, record, timer, mutationRows)
					mock.ExpectQuery(`(?s)^SELECT\s+.* FROM author_activity_occurrences WHERE dedup_key = .*$`).WillReturnRows(issue2589Rows("occurrence_id"))
					mock.ExpectExec(`^UPDATE\s+author_activity_order SET last_sequence = .* WHERE singleton_id = 1 AND last_sequence = .*$`).WithArgs(int64(1), int64(0)).WillReturnResult(sqlmock.NewResult(0, 1))
					mock.ExpectExec(`(?s)^INSERT\s+INTO author_activity_occurrences \(.*\) VALUES \(.*\)$`).WillReturnResult(sqlmock.NewResult(0, 1))
					mock.ExpectCommit()
				}
				ctx := correlation.WithSourceArtifactFact(correlation.WithRunID(context.Background(), runID), sourceartifactfixture.Fact())
				ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(uuid.NewString(), sourceartifactfixture.BundleHash))
				result, err := issue2589CommitEngine(t, ctx, db, postgres, command)
				wantCount, wantArtifacts := 26, 3
				if postgres {
					wantCount = 25
				}
				if casLost {
					wantCount, wantArtifacts = 8, 2
					if postgres {
						wantCount = 7
					}
					failure, ok := failures.As(err)
					if result.Committed || !ok || failure.Failure.Detail.Code != "workflow_engine_state_revision_conflict" {
						t.Fatalf("CAS refusal: result=%+v err=%v", result, err)
					}
				} else if err != nil || !result.Committed || !result.Lifecycle.Committed || len(result.Lifecycle.Cancellations) != 1 || result.Lifecycle.Cancellations[0] != timer.Ref {
					t.Fatalf("full engine commit: result=%+v err=%v", result, err)
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
				runReads, artifactReads := 0, 0
				for _, statement := range statements {
					if sqlmock.QueryMatcherRegexp.Match(runQuery, statement) == nil {
						runReads++
					}
					if strings.Contains(statement, "FROM source_artifacts") {
						artifactReads++
					}
				}
				if len(statements) != wantCount || runReads != 1 || artifactReads != wantArtifacts {
					t.Fatalf("SQL calls=%d want=%d run/source reads=%d want=1 artifact reads=%d want=%d\n%s", len(statements), wantCount, runReads, artifactReads, wantArtifacts, strings.Join(statements, "\n"))
				}
				t.Logf("full operation: %d SQL calls (excluding BEGIN/COMMIT), 1 run/source admission, %d fresh artifact checks", wantCount, wantArtifacts)
			})
		}
	}
}

type issue2589Argument func(driver.Value) bool

func (match issue2589Argument) Match(value driver.Value) bool { return match(value) }

func issue2589Rows(columns string, values ...driver.Value) *sqlmock.Rows {
	rows := sqlmock.NewRows(strings.Fields(columns))
	if len(values) != 0 {
		rows.AddRow(values...)
	}
	return rows
}

func issue2589ExpectArtifact(mock sqlmock.Sqlmock, postgres bool) {
	query := `^SELECT\s+EXISTS \(SELECT 1 FROM source_artifacts WHERE bundle_hash = \?\)$`
	if postgres {
		query = `^SELECT\s+EXISTS \(SELECT 1 FROM source_artifacts WHERE bundle_hash = \$1\)$`
	}
	mock.ExpectQuery(query).WithArgs(sourceartifactfixture.BundleHash).WillReturnRows(issue2589Rows("exists", true))
}

func issue2589ExpectStoryLock(mock sqlmock.Sqlmock, postgres bool) {
	query := `^SELECT\s+last_sequence FROM author_activity_order WHERE singleton_id = 1$`
	if postgres {
		query = `^SELECT\s+last_sequence FROM author_activity_order WHERE singleton_id = 1 FOR UPDATE$`
	} else {
		mock.ExpectExec(`^INSERT\s+OR IGNORE INTO author_activity_order .*`).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`^UPDATE\s+author_activity_order SET last_sequence = last_sequence WHERE singleton_id = 1$`).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectQuery(query).WillReturnRows(issue2589Rows("last_sequence", int64(0)))
}

func issue2589ExpectTimerCancellation(mock sqlmock.Sqlmock, postgres bool, timer pipeline.WorkflowTimerActivation) {
	routing, _ := json.Marshal(timer.RoutingSource)
	query := `(?s)^SELECT\s+timer_name, .* FROM timers WHERE timer_id = \? AND task_type = 'workflow_timer'$`
	if postgres {
		query = `(?s)^SELECT\s+timer_name, .* FROM timers WHERE timer_id = \$1::uuid AND task_type = 'workflow_timer' FOR UPDATE$`
	}
	mock.ExpectQuery(query).WithArgs(timer.Ref.ActivationID).WillReturnRows(issue2589Rows(
		"timer_name run_id entity_id flow_scope_key flow_instance_id flow_instance fire_event fire_payload routing_source execution_mode fire_at recurring recurrence_interval owner_agent status fired_at created_at source_timer_id forked_from_run_id forked_from_event_id reconstruction_owner forked_from_point_kind forked_from_point_revision source_armed_at cancel_cause cancelled_at",
		timer.Ref.TaskID(), timer.RunID, timer.EntityID, timer.Route.ScopeKey, timer.Route.InstanceID, timer.Route.InstancePath,
		timer.EventType, string(timer.Payload), string(routing), string(timer.ExecutionMode), timer.FireAt, false, "", timer.OwnerAgent, "active", nil, timer.CreatedAt, "", "", "", "", "", int64(0), nil, "", nil,
	))
	if postgres {
		mock.ExpectQuery(`^UPDATE\s+timers SET status = 'cancelled', cancel_cause = NULLIF\(\$1, ''\), cancelled_at = \$2 WHERE timer_id = \$3::uuid AND task_type = 'workflow_timer' AND status = 'active' RETURNING CAST\(run_id AS TEXT\), CAST\(timer_id AS TEXT\)$`).WithArgs("", nil, timer.Ref.ActivationID).WillReturnRows(issue2589Rows("run_id timer_id", timer.RunID, timer.Ref.ActivationID))
	} else {
		mock.ExpectExec(`^UPDATE\s+timers SET status = 'cancelled', cancel_cause = NULLIF\(\?, ''\), cancelled_at = \? WHERE timer_id = \? AND task_type = 'workflow_timer' AND status = 'active'$`).WithArgs("", nil, timer.Ref.ActivationID).WillReturnResult(sqlmock.NewResult(0, 1))
	}
}

func issue2589ExpectEngineRevision(mock sqlmock.Sqlmock, postgres bool, state pipeline.WorkflowEngineStateRecord, timer pipeline.WorkflowTimerActivation, mutations *sqlmock.Rows) {
	parent := `^SELECT\s+CAST\(run_id AS TEXT\) FROM runs WHERE run_id=\$1$`
	if postgres {
		parent = `^SELECT\s+CAST\(run_id AS TEXT\) FROM runs WHERE run_id=\$1 FOR KEY SHARE$`
	}
	mock.ExpectQuery(parent).WithArgs(state.Identity.RunID).WillReturnRows(issue2589Rows("run_id", state.Identity.RunID))
	mock.ExpectExec(`^INSERT\s+INTO run_fork_revision_heads .*`).WillReturnResult(sqlmock.NewResult(0, 0))
	if postgres {
		mock.ExpectQuery(`^SELECT\s+last_revision FROM run_fork_revision_heads WHERE run_id=\$1 FOR UPDATE$`).WithArgs(state.Identity.RunID).WillReturnRows(issue2589Rows("last_revision", int64(1)))
	}
	mock.ExpectQuery(`(?s)^WITH\s+requested\(family,fact_key\).*run_fork_fact_revisions.*`).WillReturnRows(issue2589Rows("family fact_key fact present"))
	mock.ExpectQuery(`(?s)^SELECT\s+CAST\(e.entity_id AS TEXT\).*FROM .*flow_instances.*UNION ALL.*WHERE e.run_id = .*`).WillReturnRows(issue2589Rows(
		"entity_id flow_instance entity_type slug name created_at flow_config construction_kind stage_defined flow_template mode status current_state entered_state_at updated_at terminated_at",
		state.EntityID, state.Identity.Route.InstancePath, state.EntityType, nil, nil, state.CreatedAt, string(state.Config), "constructed", true, state.WorkflowName, state.Mode, state.Status, state.CurrentState, state.EnteredStageAt, state.UpdatedAt, nil,
	))
	mock.ExpectQuery(`(?s)^SELECT\s+CAST\(m.mutation_id AS TEXT\).*FROM entity_mutations m WHERE m.run_id = .*`).WillReturnRows(mutations)
	routing, _ := json.Marshal(timer.RoutingSource)
	columns := strings.Fields("timer_id timer_name schedule_scope schedule_key immutable_hash run_id source_timer_id forked_from_run_id forked_from_event_id reconstruction_owner entity_id flow_scope_key flow_instance_id flow_instance fire_event fire_payload routing_source execution_mode fire_at initial_fire_at recurring recurrence_interval owner_node owner_agent owner_kind agent_name_owner agent_name_source agent_route_presence agent_flow_scope_key agent_flow_instance_id reply_context_id task_id due_basis_kind due_basis_absolute due_basis_duration due_basis_cron occurrence_event_id occurrence_admitted_at accepted_at cancel_cause cancelled_at failure_code failure_message failed_at clock_suspension task_type status fired_at created_at forked_from_point_kind forked_from_point_revision source_armed_at")
	values := map[string]driver.Value{"timer_id": timer.Ref.ActivationID, "timer_name": timer.Ref.TaskID(), "run_id": timer.RunID, "entity_id": timer.EntityID, "flow_scope_key": timer.Route.ScopeKey, "flow_instance_id": timer.Route.InstanceID, "flow_instance": timer.Route.InstancePath, "fire_event": timer.EventType, "fire_payload": string(timer.Payload), "routing_source": string(routing), "execution_mode": string(timer.ExecutionMode), "fire_at": timer.FireAt, "recurring": false, "owner_agent": timer.OwnerAgent, "owner_kind": "system", "task_type": "workflow_timer", "status": "cancelled", "created_at": timer.CreatedAt}
	row := make([]driver.Value, len(columns))
	for i, column := range columns {
		row[i] = values[column]
	}
	mock.ExpectQuery(`(?s)^SELECT\s+CAST\(t.timer_id AS TEXT\).*FROM timers t WHERE t.run_id = .*`).WillReturnRows(sqlmock.NewRows(columns).AddRow(row...))
	if postgres {
		mock.ExpectQuery(`^UPDATE\s+run_fork_revision_heads SET last_revision=last_revision\+1, updated_at=NOW\(\) WHERE run_id=\$1 RETURNING last_revision$`).WithArgs(state.Identity.RunID).WillReturnRows(issue2589Rows("last_revision", int64(2)))
	} else {
		mock.ExpectExec(`^UPDATE\s+run_fork_revision_heads SET last_revision=last_revision\+1, updated_at=\$2 WHERE run_id=\$1$`).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery(`^SELECT\s+last_revision FROM run_fork_revision_heads WHERE run_id=\$1$`).WithArgs(state.Identity.RunID).WillReturnRows(issue2589Rows("last_revision", int64(2)))
	}
	mock.ExpectExec(`^INSERT\s+INTO run_fork_revisions .*`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`^INSERT\s+INTO run_fork_fact_revisions .*`).WillReturnResult(sqlmock.NewResult(0, 3))
}

func issue2589CommitEngine(t *testing.T, ctx context.Context, db *sql.DB, postgres bool, command pipeline.WorkflowEngineMutationCommand) (pipeline.CommittedWorkflowEngineMutation, error) {
	t.Helper()
	if postgres {
		backend, err := postgresbackend.New(db)
		if err != nil {
			t.Fatal(err)
		}
		owner := &PipelinePostgresOwner{backend: backend, requireCurrent: func() error { return nil }, RunLifecyclePostgresOwner: &runlifecycle.RunLifecyclePostgresOwner{}}
		return owner.CommitWorkflowEngineMutation(ctx, command)
	}
	backend, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	owner := &PipelineSQLiteOwner{backend: backend, requireCurrent: func() error { return nil }, RunLifecycleSQLiteOwner: &runlifecycle.RunLifecycleSQLiteOwner{}}
	return owner.CommitWorkflowEngineMutation(ctx, command)
}
