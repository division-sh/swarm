package runtimepersistence

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
)

// Prepared component aggregates use the canonical commit, not handler creation.
// This does not qualify public constructor eligibility or installed topology.
func commitPreparedWorkflowAggregateFixture(t *testing.T, ctx context.Context, selected interface {
	CommitFlowInstanceActivation(context.Context, bus.FlowInstanceActivationCommand) (bus.CommittedFlowInstanceActivation, error)
}, runID string, instance pipeline.WorkflowInstance, at time.Time) pipeline.WorkflowEngineStateRecord {
	t.Helper()
	ctx = effects.WithExecutionMode(correlation.WithRunID(ctx, runID), effects.ExecutionModeLive)
	command, err := flowactivationfixture.Command(ctx, instance, pipeline.WorkflowLifecycleMutationPlan{}, at)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := selected.CommitFlowInstanceActivation(ctx, command)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("construct prepared component aggregate: acknowledged=%t created=%t err=%v", committed.Acknowledged, committed.Created, err)
	}
	record, err := command.Plan.PersistenceRecord()
	if err != nil {
		t.Fatal(err)
	}
	return record.State
}

// Native projection controls seed an explicit header, not construction receipts.
// These fixtures are not public constructor or activation qualification.
func seedWorkflowHeaderProjectionFixture(t *testing.T, ctx context.Context, db flowRouteTestExecutor, runID, entityID, path, flow, entityType, stage, accumulator string, at time.Time) {
	t.Helper()
	payload, err := pipeline.WorkflowInstanceHeaderPayloadForRoute(flowidentity.StoredRoute(flow, flowidentity.LogicalInstanceID(path), path), "1")
	if err != nil {
		t.Fatal(err)
	}
	config, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var fieldType any
	if entityType != "" {
		fieldType = entityType
	}
	_, err = db.ExecContext(ctx, `INSERT INTO flow_instances
		(run_id, instance_path, entity_id, entity_type, flow_template, mode, stage_defined,
		 current_state, gates, bookkeeping, accumulator, config, revision, entered_state_at, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,'static',TRUE,$6,'{}','{}',$7,$8,1,$9,$9,$9)`,
		runID, path, entityID, fieldType, flow, stage, accumulator, string(config), at)
	if err != nil {
		t.Fatalf("seed native constructed-header projection: %v", err)
	}
}
