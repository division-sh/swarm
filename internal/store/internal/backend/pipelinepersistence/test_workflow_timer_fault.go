package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
)

// These exact faults operate only on an existing active workflow timer. They
// neither create an activation nor interpret its lifecycle or revision facts.
func RemoveWorkflowTimerForTest(ctx context.Context, tx *sql.Tx, postgres bool, run, activation string) error {
	query := `DELETE FROM timers WHERE run_id = ? AND timer_id = ? AND task_type = 'workflow_timer' AND status = 'active'`
	if postgres {
		query = `DELETE FROM timers WHERE run_id = $1::uuid AND timer_id = $2::uuid AND task_type = 'workflow_timer' AND status = 'active'`
	}
	result, err := tx.ExecContext(ctx, query, run, activation)
	return requireWorkflowTimerFaultRow(result, err)
}

func SetWorkflowTimerForeignDeclarationForTest(ctx context.Context, tx *sql.Tx, postgres bool, run, activation string) error {
	current, found, err := loadWorkflowTimerActivation(ctx, tx, !postgres, activation)
	if err != nil {
		return err
	}
	if !found || current.RunID != run || current.Status != "active" {
		return fmt.Errorf("foreign declaration fault requires its exact active timer")
	}
	foreign, err := events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{
		FlowID: "foreign", FlowInstance: current.Route.InstancePath, EntityID: current.EntityID,
	})
	if err != nil {
		return err
	}
	wire, err := json.Marshal(foreign)
	if err != nil {
		return err
	}
	query := `UPDATE timers SET routing_source = ?, fire_event = ? WHERE run_id = ? AND timer_id = ? AND task_type = 'workflow_timer' AND status = 'active'`
	if postgres {
		query = `UPDATE timers SET routing_source = $1::jsonb, fire_event = $2 WHERE run_id = $3::uuid AND timer_id = $4::uuid AND task_type = 'workflow_timer' AND status = 'active'`
	}
	result, err := tx.ExecContext(ctx, query, string(wire), "foreign/timer.timeout", run, activation)
	return requireWorkflowTimerFaultRow(result, err)
}

func requireWorkflowTimerFaultRow(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return fmt.Errorf("workflow timer fault changed %d rows, want exactly one", changed)
	}
	return nil
}

func SetWorkflowTimerMalformedNameForTest(ctx context.Context, tx *sql.Tx, postgres bool, run, activation string) error {
	query := `UPDATE timers SET timer_name = 'malformed' WHERE run_id = ? AND timer_id = ? AND task_type = 'workflow_timer' AND status = 'active'`
	if postgres {
		query = `UPDATE timers SET timer_name = 'malformed' WHERE run_id = $1::uuid AND timer_id = $2::uuid AND task_type = 'workflow_timer' AND status = 'active'`
	}
	result, err := tx.ExecContext(ctx, query, run, activation)
	return requireWorkflowTimerFaultRow(result, err)
}

func SetWorkflowTimerForeignRouteAndMalformedNameForTest(ctx context.Context, tx *sql.Tx, postgres bool, run, activation string) error {
	query := `UPDATE timers SET flow_instance = 'other-route', timer_name = 'malformed' WHERE run_id = ? AND timer_id = ? AND task_type = 'workflow_timer' AND status = 'active'`
	if postgres {
		query = `UPDATE timers SET flow_instance = 'other-route', timer_name = 'malformed' WHERE run_id = $1::uuid AND timer_id = $2::uuid AND task_type = 'workflow_timer' AND status = 'active'`
	}
	result, err := tx.ExecContext(ctx, query, run, activation)
	return requireWorkflowTimerFaultRow(result, err)
}
