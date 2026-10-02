package flowactivationfixture

import (
	"context"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

// Command packages already-prepared component fixture data for the real
// selected-store owner. It neither prepares lifecycle nor persists anything.
// Empty route sets are not evidence of public construction or route readiness.
func Command(ctx context.Context, initialized pipeline.WorkflowInstance, lifecycle pipeline.WorkflowLifecycleMutationPlan, at time.Time) (bus.FlowInstanceActivationCommand, error) {
	source, found := correlation.SourceArtifactFactFromContext(ctx)
	if !found {
		return bus.FlowInstanceActivationCommand{}, fmt.Errorf("activation fixture requires an exact source artifact")
	}
	mode, found := effects.ExecutionModeFromContext(ctx)
	if !found || !mode.Valid() {
		return bus.FlowInstanceActivationCommand{}, fmt.Errorf("activation fixture requires typed execution mode")
	}
	identity := flowidentity.Stored(nil, initialized.WorkflowName, initialized.StorageRef, initialized.InstanceID, initialized.EntityID, initialized.ParentEntityID)
	readiness := pipeline.DynamicFlowRuntimeReadinessPlan{
		Identity: identity, RunID: correlation.RunIDFromContext(ctx), BundleHash: source.BundleHash(),
		WorkflowVersion: initialized.WorkflowVersion, ExecutionMode: mode,
	}
	if initialized.RuntimeReadiness != nil {
		readiness = *initialized.RuntimeReadiness
	}
	plan, err := (pipeline.FlowInstanceActivationPlan{
		Instance: initialized, Readiness: readiness, Lifecycle: lifecycle, OccurredAt: at,
	}).Normalized()
	if err != nil {
		return bus.FlowInstanceActivationCommand{}, err
	}
	command := bus.FlowInstanceActivationCommand{Plan: plan}
	for _, construction := range plan.ConstructionPlans() {
		command.RouteTopology = append(command.RouteTopology, bus.FlowInstanceRouteRecordSet{
			Identity: flowidentity.RunScopedFlowInstance{RunID: construction.Readiness.RunID, Route: construction.Identity.Route()},
		})
	}
	return command, command.Validate()
}
