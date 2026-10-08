package manager

// Plan derivation consumes the committed receiver and admitted source. Plan equality belongs to selected-store planning, not attachment progression.

import (
	"context"
	"fmt"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"strings"
	"time"
)

func (am *AgentManager) reconcileEnsuredDynamicFlowRuntimeReadinessPlan(
	ctx context.Context,
	req runtimepipeline.FlowInstanceActivationRequest,
	runID string,
) (runtimepipeline.DynamicFlowRuntimeReadiness, error) {
	if err := am.requireRunExecutionOwnership(ctx, runID); err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadiness{}, err
	}
	admittedSource, err := am.dynamicFlowRuntimeReadinessSource(ctx, req.ContractBundle)
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadiness{}, err
	}
	occurredAt := req.OccurredAt.UTC()
	if !req.TriggerEvent.CreatedAt().IsZero() {
		occurredAt = req.TriggerEvent.CreatedAt().UTC()
	}
	if occurredAt.IsZero() {
		return runtimepipeline.DynamicFlowRuntimeReadiness{}, fmt.Errorf("flow readiness reconciliation requires an exact occurrence time")
	}
	current, found, err := am.workflowInstances.LoadDynamicFlowRuntimeReadiness(ctx, runID, req.Instance.Route())
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadiness{}, err
	}
	if !found {
		return runtimepipeline.DynamicFlowRuntimeReadiness{}, fmt.Errorf("dynamic flow runtime readiness not found for %s", req.Instance.InstancePath)
	}
	// Ensure and source reconciliation derive from the same committed receiver;
	// creation-only caller inputs never replace its configuration or projection.
	expected, err := am.deriveCurrentDynamicFlowRuntimeReadinessPlan(ctx, current, admittedSource)
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadiness{}, err
	}
	if expected.Identity.Route() != req.Instance.Route() || expected.Identity.TemplateID != req.Instance.TemplateID || expected.Identity.EntityID != req.Instance.EntityID {
		return runtimepipeline.DynamicFlowRuntimeReadiness{}, fmt.Errorf("ensured flow persisted identity disagrees with request")
	}
	results, err := am.workflowInstances.ReconcileDynamicFlowRuntimeReadinessPlans(ctx, []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation{{Observed: current, Expected: expected}}, occurredAt)
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadiness{}, fmt.Errorf("reconcile dynamic flow runtime readiness plan %s: %w", req.Instance.InstancePath, err)
	}
	if len(results) != 1 || results[0].AttemptOrdinal == 0 || results[0].Readiness.AttemptOrdinal != results[0].AttemptOrdinal || results[0].Readiness.Plan.RunID != runID || results[0].Readiness.Plan.Identity != expected.Identity {
		return runtimepipeline.DynamicFlowRuntimeReadiness{}, fmt.Errorf("readiness reconciliation returned no exact row for %s", req.Instance.InstancePath)
	}
	return results[0].Readiness, nil
}

// ReconcileDynamicFlowRuntimeReadinessPlansForRun advances every active
// readiness owner in the selected run before revised routes are derived.
func (am *AgentManager) ReconcileDynamicFlowRuntimeReadinessPlansForRun(
	ctx context.Context,
	observedAt time.Time,
) error {
	if am == nil || am.workflowInstances == nil {
		return fmt.Errorf("dynamic flow runtime readiness reconciler requires manager and workflow store")
	}
	admittedSource, err := am.dynamicFlowRuntimeReadinessSource(ctx)
	if err != nil {
		return err
	}
	source := admittedSource.source
	runID := strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	if runID == "" {
		return fmt.Errorf("dynamic flow runtime readiness reconciliation requires exact run_id")
	}
	if err := am.requireRunExecutionOwnership(ctx, runID); err != nil {
		return err
	}
	observedAt = observedAt.UTC()
	if observedAt.IsZero() {
		return fmt.Errorf("dynamic flow runtime readiness reconciliation requires exact occurrence time")
	}
	items, err := am.workflowInstances.InspectDynamicFlowRuntimeReadinessForRun(ctx, runID, admittedSource.fact)
	if err != nil {
		return err
	}
	requests := make([]runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation, 0, len(items))
	plansByKey := make(map[runtimepipeline.DynamicFlowRuntimeReadinessKey]runtimepipeline.DynamicFlowRuntimeReadinessPlan, len(items))
	for _, item := range items {
		expected, err := am.deriveCurrentDynamicFlowRuntimeReadinessPlan(ctx, item, admittedSource)
		if err != nil {
			return err
		}
		requests = append(requests, runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation{Observed: item, Expected: expected})
		plansByKey[runtimepipeline.DynamicFlowRuntimeReadinessKey{RunID: expected.RunID, InstancePath: expected.Identity.InstancePath}] = expected
	}
	results, err := am.workflowInstances.ReconcileDynamicFlowRuntimeReadinessPlans(ctx, requests, observedAt)
	if err != nil {
		return fmt.Errorf("reconcile dynamic flow runtime readiness plan set: %w", err)
	}
	for _, result := range results {
		if !result.Changed {
			continue
		}
		expected := plansByKey[runtimepipeline.DynamicFlowRuntimeReadinessKey{RunID: result.RunID, InstancePath: result.InstancePath}]
		if err := am.reconcileDynamicFlowRuntimeReadinessPlan(ctx, expected, result.AttemptOrdinal, source); err != nil {
			am.signalDynamicFlowRuntimeReadiness()
			return err
		}
	}
	return nil
}

func (am *AgentManager) deriveCurrentDynamicFlowRuntimeReadinessPlan(
	ctx context.Context,
	item runtimepipeline.DynamicFlowRuntimeReadiness,
	admittedSource dynamicFlowRuntimeReadinessSource,
) (runtimepipeline.DynamicFlowRuntimeReadinessPlan, error) {
	if !item.OwningRunSource.Matches(admittedSource.fact) {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow readiness %s is not owned by the admitted source", item.InstancePath)
	}
	plan, err := item.Plan.Normalized()
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, err
	}
	ctx = runtimecorrelation.WithRunID(ctx, plan.RunID)
	flowIdentity, err := runtimeflowidentity.NewRunScopedFlowInstance(plan.RunID, plan.Identity.Route())
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, err
	}
	projection, err := am.workflowInstances.LoadRouteRecoveryProjection(ctx, flowIdentity)
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("load dynamic flow readiness projection %s: %w", item.InstancePath, err)
	}
	if projection.Identity.Route() != plan.Identity.Route() || projection.Identity.TemplateID != plan.Identity.TemplateID || projection.Identity.EntityID != plan.Identity.EntityID {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow readiness %s persisted identity changed", item.InstancePath)
	}
	source := admittedSource.source
	scope, ok := semanticview.FlowScopeByID(source, projection.Identity.TemplateID)
	if !ok {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("flow contract view not found: %s", projection.Identity.TemplateID)
	}
	schema, ok := source.FlowSchemaByID(projection.Identity.TemplateID)
	if !ok {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("flow schema not found: %s", projection.Identity.TemplateID)
	}
	records, err := am.flowInstanceAgentRecords(plan.RunID, runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: source,
		Instance:       projection.Identity,
	}, schema, scope)
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("derive dynamic flow readiness agents %s: %w", item.InstancePath, err)
	}
	expected := plan
	expected.Identity = projection.Identity
	expected.BundleHash = admittedSource.fact.BundleHash()
	expected.WorkflowVersion = strings.TrimSpace(source.WorkflowVersion())
	expected.Agents = make([]runtimepipeline.DynamicFlowRuntimeAgentExpectation, 0, len(records))
	for _, record := range records {
		identity, err := record.Config.ConcreteIdentity()
		if err != nil {
			return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("derive dynamic flow agent identity %s: %w", record.Config.ID, err)
		}
		revision, err := lifecycleConfigRevision(record)
		if err != nil {
			return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("derive dynamic flow agent revision %s: %w", record.Config.ID, err)
		}
		expected.Agents = append(expected.Agents, runtimepipeline.DynamicFlowRuntimeAgentExpectation{
			Identity: identity, ConfigRevision: revision, EntityID: record.Config.EntityID,
		})
	}
	if expected.BundleHash != plan.BundleHash || expected.WorkflowVersion != plan.WorkflowVersion {
		expected.CreationEvent, err = rebuildPendingDynamicFlowRuntimeCreationEventPlan(
			plan.CreationEvent,
			!item.CreationEventEmittedAt.IsZero(),
			source,
			schema,
			projection.Identity,
		)
		if err != nil {
			return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("rebuild dynamic flow creation plan %s: %w", item.InstancePath, err)
		}
	}
	if err := am.executionPosture.Admit(expected.ExecutionMode, "dynamic flow runtime readiness plan reconciliation"); err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, err
	}
	return expected.Normalized()
}

func rebuildPendingDynamicFlowRuntimeCreationEventPlan(
	current *runtimepipeline.DynamicFlowRuntimeCreationEventPlan,
	emitted bool,
	source semanticview.Source,
	schema runtimecontracts.FlowSchemaDocument,
	identity runtimeflowidentity.Instance,
) (*runtimepipeline.DynamicFlowRuntimeCreationEventPlan, error) {
	if emitted {
		return current, nil
	}
	autoEmit := strings.TrimSpace(schema.AutoEmitOnCreate.Event)
	if current == nil {
		if autoEmit != "" {
			return nil, fmt.Errorf(
				"cannot introduce auto-emit %s without persisted trigger lineage",
				autoEmit,
			)
		}
		return nil, nil
	}
	var payload map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes(current.Payload, &payload); err != nil {
		return nil, err
	}
	return buildDynamicFlowRuntimeCreationEventPlan(
		source,
		schema,
		identity,
		events.EventLineage{
			RunID:         current.RunID,
			ParentEventID: current.ParentEventID,
			ExecutionMode: current.ExecutionMode,
		},
		payload,
		current.DeliveryContext,
		current.CreatedAt,
	)
}

func dynamicFlowRuntimeCreationEvent(source semanticview.Source, plan runtimepipeline.DynamicFlowRuntimeReadinessPlan) (events.Event, error) {
	var empty events.Event
	creation := plan.CreationEvent
	if creation == nil {
		return empty, fmt.Errorf("dynamic flow creation event plan is required")
	}
	route := events.RouteIdentity{FlowID: plan.Identity.TemplateID, FlowInstance: plan.Identity.InstancePath, EntityID: plan.Identity.EntityID}
	routingSource, err := pinrouting.AdmitFlowExecutionRoutingSource(source, plan.RunID, plan.Identity, route)
	if err != nil {
		return empty, err
	}
	envelope := events.EnvelopeForSourceRoute(events.EventEnvelope{
		EntityID:     plan.Identity.EntityID,
		FlowInstance: plan.Identity.InstancePath,
	}, events.RouteIdentity{
		FlowID:       plan.Identity.TemplateID,
		FlowInstance: plan.Identity.InstancePath,
		EntityID:     plan.Identity.EntityID,
	})
	return events.NewChildEvent(events.ChildEventInput{
		Facts: events.EventFacts{
			ID: creation.EventID, Type: events.EventType(creation.EventType),
			Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "flow-instance-activator"},
			Payload:  creation.Payload, Envelope: envelope, RoutingSource: routingSource,
			CreatedAt: creation.CreatedAt, ExecutionMode: creation.ExecutionMode,
		},
		Lineage: events.EventLineage{
			RunID: creation.RunID, ParentEventID: creation.ParentEventID, ExecutionMode: creation.ExecutionMode,
		},
	})
}
