package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

// This closed fault port relocates the historical-header fixture's raw edits.
// It deliberately changes evidence outside semantic mutation/revision owners.
type FlowConstructorHeaderFaultField string

func (f FlowConstructorHeaderFaultField) valid() bool {
	switch f {
	case "flow_template", "mode", "status", "current_state", "stage_defined", "config",
		"entered_state_at", "created_at", "updated_at", "revision", "gates", "bookkeeping",
		"accumulator", "slug", "name", "parent_instance", "instance_key":
		return true
	default:
		return false
	}
}

type constructorHeaderFaultBackend interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func (s *PipelinePostgresOwner) FaultFlowConstructorHeaderForTest(ctx context.Context, owner flowidentity.RunScopedFlowInstance, field FlowConstructorHeaderFaultField, value any) (func(context.Context) error, error) {
	return faultFlowConstructorHeaderForTest(ctx, s.backend, owner, field, value)
}

func (s *PipelineSQLiteOwner) FaultFlowConstructorHeaderForTest(ctx context.Context, owner flowidentity.RunScopedFlowInstance, field FlowConstructorHeaderFaultField, value any) (func(context.Context) error, error) {
	return faultFlowConstructorHeaderForTest(ctx, s.backend, owner, field, value)
}

func faultFlowConstructorHeaderForTest(ctx context.Context, backend constructorHeaderFaultBackend, owner flowidentity.RunScopedFlowInstance, field FlowConstructorHeaderFaultField, value any) (func(context.Context) error, error) {
	if backend == nil || owner != owner.Normalize() || owner.Validate() != nil || !field.valid() {
		return nil, fmt.Errorf("constructor header fault requires its exact closed coordinate")
	}
	var original any
	if err := backend.QueryRowContext(ctx, "SELECT "+string(field)+" FROM flow_instances WHERE run_id=$1 AND instance_path=$2", owner.RunID, owner.Route.InstancePath).Scan(&original); err != nil {
		return nil, err
	}
	query := "UPDATE flow_instances SET " + string(field) + "=$1 WHERE run_id=$2 AND instance_path=$3"
	if _, err := backend.ExecContext(ctx, query, value, owner.RunID, owner.Route.InstancePath); err != nil {
		return nil, err
	}
	return func(ctx context.Context) error {
		_, err := backend.ExecContext(ctx, query, original, owner.RunID, owner.Route.InstancePath)
		return err
	}, nil
}
