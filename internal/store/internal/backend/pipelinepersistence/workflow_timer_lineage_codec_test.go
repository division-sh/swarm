package pipelinepersistence

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func nativeInheritedWorkflowTimer(t *testing.T) pipeline.WorkflowTimerActivation {
	t.Helper()
	const runID = "11111111-1111-4111-8111-111111111111"
	const entityID = "22222222-2222-4222-8222-222222222222"
	source, err := events.NewRootRoutingSource(entityID)
	if err != nil {
		t.Fatal(err)
	}
	armedAt := time.Date(2026, 10, 9, 0, 0, 0, 123456000, time.UTC)
	return pipeline.WorkflowTimerActivation{
		Ref: timeridentity.WorkflowTimerActivationRef{
			ActivationID: "33333333-3333-4333-8333-333333333333", DeclarationKey: "waiting.timeout",
			DeclarationRevision: "revision", Cause: timeridentity.WorkflowTimerActivationCauseInitial,
		},
		RunID: runID, EntityID: entityID, Route: flowidentity.StoredRoute(".", runID, runID), RoutingSource: source,
		OwnerAgent: "timer-owner", EventType: "timer.elapsed", ExecutionMode: executionmode.Live, Payload: []byte(`{}`),
		CreatedAt: armedAt.Add(3 * time.Hour), FireAt: armedAt.Add(time.Hour), Status: "active",
		SourceTimerID: "44444444-4444-4444-8444-444444444444", ForkedFromRunID: "55555555-5555-4555-8555-555555555555",
		ForkedFromPointKind: forkpoint.RunStart, ForkedFromPointRevision: 7, SourceArmedAt: armedAt, ReconstructionOwner: "selected-cut",
	}
}

func nativeTimerRows(t *testing.T, a pipeline.WorkflowTimerActivation, locking bool) *sqlmock.Rows {
	t.Helper()
	routing, err := json.Marshal(a.RoutingSource)
	if err != nil {
		t.Fatal(err)
	}
	interval := ""
	if a.Recurring {
		interval = a.RecurrenceInterval.String()
	}
	var firedAt driver.Value
	if !a.FiredAt.IsZero() {
		firedAt = a.FiredAt
	}
	values := map[string]driver.Value{
		"timer_id": a.Ref.ActivationID, "timer_name": a.Ref.TaskID(), "run_id": a.RunID, "entity_id": a.EntityID,
		"flow_scope_key": a.Route.ScopeKey, "flow_instance_id": a.Route.InstanceID, "flow_instance": a.Route.InstancePath,
		"fire_event": a.EventType, "fire_payload": string(a.Payload), "routing_source": string(routing), "execution_mode": string(a.ExecutionMode),
		"fire_at": a.FireAt, "recurring": a.Recurring, "recurrence_interval": interval, "owner_node": "", "owner_agent": a.OwnerAgent,
		"task_type": "workflow_timer", "status": a.Status, "fired_at": firedAt, "created_at": a.CreatedAt,
		"source_timer_id": a.SourceTimerID, "forked_from_run_id": a.ForkedFromRunID, "forked_from_event_id": a.ForkedFromEventID,
		"reconstruction_owner": a.ReconstructionOwner, "forked_from_point_kind": string(a.ForkedFromPointKind),
		"forked_from_point_revision": a.ForkedFromPointRevision, "source_armed_at": nullableWorkflowTimerSourceArmedAt(a.SourceArmedAt),
		"cancel_cause": string(a.CancelCause), "cancelled_at": nullableWorkflowTimerSourceArmedAt(a.CancelledAt),
	}
	columns := "timer_id timer_name run_id entity_id flow_scope_key flow_instance_id flow_instance fire_event fire_payload routing_source execution_mode fire_at recurring recurrence_interval owner_node owner_agent task_type status fired_at created_at source_timer_id forked_from_run_id forked_from_event_id reconstruction_owner forked_from_point_kind forked_from_point_revision source_armed_at cancel_cause cancelled_at"
	if locking {
		columns = "timer_name run_id entity_id flow_scope_key flow_instance_id flow_instance fire_event fire_payload routing_source execution_mode fire_at recurring recurrence_interval owner_agent status fired_at created_at source_timer_id forked_from_run_id forked_from_event_id reconstruction_owner forked_from_point_kind forked_from_point_revision source_armed_at cancel_cause cancelled_at"
	}
	names := strings.Fields(columns)
	row := make([]driver.Value, len(names))
	for i, name := range names {
		row[i] = values[name]
	}
	return sqlmock.NewRows(names).AddRow(row...)
}

func TestWorkflowTimerTypedLineageNativeReadBothDialects(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, kind := range []forkpoint.Kind{forkpoint.RunStart, forkpoint.Event, forkpoint.DeploymentRevision} {
			t.Run(string(kind)+map[bool]string{false: "/sqlite", true: "/postgres"}[postgres], func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				activation := nativeInheritedWorkflowTimer(t)
				activation.ForkedFromPointKind = kind
				if kind == forkpoint.Event {
					activation.ForkedFromEventID = "66666666-6666-4666-8666-666666666666"
				}
				mock.ExpectQuery(`(?s)^SELECT.*FROM timers t.*`).WithArgs(activation.Ref.ActivationID).WillReturnRows(nativeTimerRows(t, activation, false))
				actual, found, err := loadWorkflowTimerActivation(context.Background(), db, !postgres, activation.Ref.ActivationID)
				if err != nil || !found || !reflect.DeepEqual(actual.Canonical(), activation.Canonical()) {
					t.Fatalf("typed inherited read: found=%v actual=%+v: %v", found, actual, err)
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestWorkflowTimerTypedLineageNativeReadRefusesPartialBothDialects(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		t.Run(map[bool]string{false: "sqlite", true: "postgres"}[postgres], func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for _, test := range []struct {
				name   string
				change func(*pipeline.WorkflowTimerActivation)
			}{
				{"missing_kind", func(a *pipeline.WorkflowTimerActivation) { a.ForkedFromPointKind = "" }},
				{"missing_revision", func(a *pipeline.WorkflowTimerActivation) { a.ForkedFromPointRevision = 0 }},
				{"missing_arm", func(a *pipeline.WorkflowTimerActivation) { a.SourceArmedAt = time.Time{} }},
				{"fabricated_event", func(a *pipeline.WorkflowTimerActivation) {
					a.ForkedFromEventID = "66666666-6666-4666-8666-666666666666"
				}},
				{"self_run", func(a *pipeline.WorkflowTimerActivation) { a.ForkedFromRunID = a.RunID }},
			} {
				t.Run(test.name, func(t *testing.T) {
					activation := nativeInheritedWorkflowTimer(t)
					test.change(&activation)
					mock.ExpectQuery(`(?s)^SELECT.*FROM timers t.*`).WithArgs(activation.Ref.ActivationID).WillReturnRows(nativeTimerRows(t, activation, false))
					if _, _, err := loadWorkflowTimerActivation(context.Background(), db, !postgres, activation.Ref.ActivationID); err == nil {
						t.Fatal("native read accepted incomplete or contradictory inherited lineage")
					}
				})
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWorkflowTimerTypedLineageNativeWriteAndReplayBothDialects(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		t.Run(map[bool]string{false: "sqlite", true: "postgres"}[postgres], func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectBegin()
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			activation := nativeInheritedWorkflowTimer(t)
			routing, _ := json.Marshal(activation.RoutingSource)
			args := []driver.Value{
				activation.Ref.ActivationID, activation.RunID, activation.Ref.TaskID(), activation.EntityID,
				activation.Route.ScopeKey, activation.Route.InstanceID, activation.Route.InstancePath,
				activation.EventType, string(activation.Payload), string(routing), string(activation.ExecutionMode), activation.FireAt,
				false, "", activation.OwnerAgent, activation.CreatedAt, activation.SourceTimerID,
				activation.ForkedFromRunID, "", activation.ReconstructionOwner,
				string(activation.ForkedFromPointKind), activation.ForkedFromPointRevision, activation.SourceArmedAt,
			}
			query := `(?s)^INSERT INTO timers .*forked_from_point_kind, forked_from_point_revision, source_armed_at.*`
			if postgres {
				mock.ExpectQuery(query).WithArgs(args...).WillReturnRows(sqlmock.NewRows([]string{"run_id", "timer_id"}).AddRow(activation.RunID, activation.Ref.ActivationID))
			} else {
				mock.ExpectExec(query).WithArgs(args...).WillReturnResult(sqlmock.NewResult(0, 1))
			}
			mock.ExpectQuery(`(?s)^SELECT timer_name, .*`).WithArgs(activation.Ref.ActivationID).WillReturnRows(nativeTimerRows(t, activation, true))
			effects := runforkrevision.NewEffects()
			changed, err := insertWorkflowEngineTimerActivation(context.Background(), tx, postgres, effects, activation)
			if err != nil || !changed {
				t.Fatalf("inherited insert changed=%v: %v", changed, err)
			}
			want := runforkrevision.NewEffects()
			if err := want.AddFact(activation.RunID, runforkrevision.FamilyTimers, activation.Ref.ActivationID); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(effects, want) {
				t.Fatal("typed timer insertion lost exact physical fact identity")
			}
			for _, change := range []func(*pipeline.WorkflowTimerActivation){
				func(a *pipeline.WorkflowTimerActivation) { a.ForkedFromPointRevision++ },
				func(a *pipeline.WorkflowTimerActivation) { a.ForkedFromPointKind = forkpoint.DeploymentRevision },
				func(a *pipeline.WorkflowTimerActivation) { a.SourceArmedAt = a.SourceArmedAt.Add(-time.Hour) },
			} {
				changed := activation
				change(&changed)
				if err := changed.Validate(); err != nil {
					t.Fatal(err)
				}
				if err := activation.ValidateCauseReplay(changed); err == nil {
					t.Fatal("valid foreign coordinates were accepted as exact native replay")
				}
			}
			mock.ExpectRollback()
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
