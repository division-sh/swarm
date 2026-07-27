package manager

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func (am *AgentManager) finalizeDynamicFlowRuntimeReadinessForStartup(ctx context.Context) error {
	if am == nil || am.workflowInstances == nil {
		return nil
	}
	items, err := am.workflowInstances.ListDynamicFlowRuntimeReadiness(ctx)
	if err != nil {
		return err
	}
	for _, item := range items {
		if err := am.finalizeDynamicFlowRuntimeReadiness(ctx, item, am.semanticSource); err != nil {
			return fmt.Errorf("finalize dynamic flow runtime readiness %s: %w", item.InstancePath, err)
		}
	}
	return nil
}

func (am *AgentManager) finalizeDynamicFlowRuntimeReadiness(
	ctx context.Context,
	readiness runtimepipeline.DynamicFlowRuntimeReadiness,
	source semanticview.Source,
) error {
	if am == nil || am.workflowInstances == nil || source == nil {
		return fmt.Errorf("dynamic flow runtime readiness finalizer requires manager, workflow store, and semantic source")
	}
	plan, err := readiness.Plan.Normalized()
	if err != nil {
		return err
	}
	if strings.TrimSpace(source.WorkflowVersion()) != plan.WorkflowVersion {
		return fmt.Errorf(
			"dynamic flow runtime readiness %s workflow version changed: persisted=%s active=%s",
			readiness.InstancePath,
			plan.WorkflowVersion,
			strings.TrimSpace(source.WorkflowVersion()),
		)
	}
	ctx = runtimecorrelation.WithRunID(ctx, plan.RunID)
	projection, err := am.workflowInstances.LoadRouteRecoveryProjection(ctx, plan.Identity.Route())
	if err != nil {
		return err
	}
	if projection.Identity.Route() != plan.Identity.Route() || projection.Identity.TemplateID != plan.Identity.TemplateID || projection.Identity.EntityID != plan.Identity.EntityID {
		return fmt.Errorf("dynamic flow runtime readiness %s persisted identity changed", readiness.InstancePath)
	}
	scope, ok := semanticview.FlowScopeByID(source, plan.Identity.TemplateID)
	if !ok {
		return fmt.Errorf("flow contract view not found: %s", plan.Identity.TemplateID)
	}
	schema, ok := source.FlowSchemaByID(plan.Identity.TemplateID)
	if !ok {
		return fmt.Errorf("flow schema not found: %s", plan.Identity.TemplateID)
	}
	req := runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: source,
		Instance:       projection.Identity,
		Config:         projection.Config,
	}
	records, err := am.flowInstanceAgentRecords(req, schema, scope)
	if err != nil {
		return err
	}
	if err := verifyDynamicFlowAgentExpectations(records, plan.Agents); err != nil {
		return fmt.Errorf("dynamic flow runtime readiness %s: %w", readiness.InstancePath, err)
	}
	persistedAgents, err := am.loadDynamicFlowPersistedAgents(ctx, readiness.InstancePath)
	if err != nil {
		return fmt.Errorf("load dynamic flow agents for %s: %w", readiness.InstancePath, err)
	}
	if err := am.rejectUnexpectedDynamicFlowAgents(readiness.InstancePath, records, persistedAgents); err != nil {
		return err
	}
	for _, rec := range records {
		if err := am.reconcileDynamicFlowAgent(ctx, rec, persistedAgents[strings.TrimSpace(rec.Config.ID)]); err != nil {
			return fmt.Errorf("reconcile dynamic flow agent %s: %w", rec.Config.ID, err)
		}
	}
	if err := am.verifyDynamicFlowAgents(ctx, readiness.InstancePath, records); err != nil {
		return fmt.Errorf("verify dynamic flow agents for %s: %w", readiness.InstancePath, err)
	}
	if err := am.installFlowInstanceRoute(ctx, req); err != nil {
		return fmt.Errorf("reconcile dynamic flow route %s: %w", readiness.InstancePath, err)
	}
	if err := am.verifyDynamicFlowRoute(ctx, plan.Identity.Route()); err != nil {
		return err
	}
	now := time.Now().UTC()
	if err := am.workflowInstances.MarkDynamicFlowRuntimeTopologyReady(ctx, plan.RunID, readiness.InstancePath, now); err != nil {
		return fmt.Errorf("record dynamic flow runtime readiness %s: %w", readiness.InstancePath, err)
	}
	if err := am.workflowInstances.ArmInitialEntryTimers(ctx, readiness.InstancePath); err != nil {
		return fmt.Errorf("arm initial workflow timers for %s: %w", readiness.InstancePath, err)
	}
	if plan.CreationEvent == nil || !readiness.CreationEventEmittedAt.IsZero() {
		return nil
	}
	evt, err := dynamicFlowRuntimeCreationEvent(plan)
	if err != nil {
		return err
	}
	publishCtx := runtimebus.WithoutCommitPublishTransaction(ctx)
	publishCtx = events.WithDeliveryContext(publishCtx, plan.CreationEvent.DeliveryContext)
	if err := am.bus.Publish(publishCtx, evt); err != nil {
		return fmt.Errorf("publish dynamic flow creation event %s: %w", plan.CreationEvent.EventType, err)
	}
	if err := am.workflowInstances.MarkDynamicFlowRuntimeCreationEventEmitted(ctx, plan.RunID, readiness.InstancePath, time.Now().UTC()); err != nil {
		return fmt.Errorf("record dynamic flow creation event completion %s: %w", readiness.InstancePath, err)
	}
	return nil
}

func (am *AgentManager) loadDynamicFlowPersistedAgents(ctx context.Context, instancePath string) (map[string]PersistedAgent, error) {
	instancePath = strings.Trim(strings.TrimSpace(instancePath), "/")
	persistedByID := map[string]PersistedAgent{}
	if am.store == nil {
		return persistedByID, nil
	}
	persisted, err := am.store.LoadAgents(ctx)
	if err != nil {
		return nil, err
	}
	for _, rec := range persisted {
		if strings.Trim(strings.TrimSpace(rec.Config.FlowPath), "/") == instancePath {
			persistedByID[strings.TrimSpace(rec.Config.ID)] = rec
		}
	}
	return persistedByID, nil
}

func (am *AgentManager) rejectUnexpectedDynamicFlowAgents(
	instancePath string,
	expected []PersistedAgent,
	persisted map[string]PersistedAgent,
) error {
	expectedIDs := make(map[string]struct{}, len(expected))
	for _, rec := range expected {
		expectedIDs[strings.TrimSpace(rec.Config.ID)] = struct{}{}
	}
	for id := range persisted {
		if _, ok := expectedIDs[id]; !ok {
			return fmt.Errorf("dynamic flow runtime readiness %s has unexpected persisted agent %s", instancePath, id)
		}
	}
	for _, cfg := range am.ListAgentConfigs() {
		if strings.Trim(strings.TrimSpace(cfg.FlowPath), "/") != strings.Trim(strings.TrimSpace(instancePath), "/") {
			continue
		}
		if _, ok := expectedIDs[strings.TrimSpace(cfg.ID)]; !ok {
			return fmt.Errorf("dynamic flow runtime readiness %s has unexpected process agent %s", instancePath, cfg.ID)
		}
	}
	return nil
}

func (am *AgentManager) reconcileDynamicFlowAgent(ctx context.Context, rec PersistedAgent, persisted PersistedAgent) error {
	expectedRevision, err := lifecycleConfigRevision(rec)
	if err != nil {
		return err
	}
	existing, live := am.GetAgentConfig(rec.Config.ID)
	if live {
		actualRevision, err := lifecycleConfigRevision(PersistedAgent{Config: existing})
		if err != nil {
			return err
		}
		if actualRevision != expectedRevision {
			return fmt.Errorf("agent %s config revision changed: expected=%s actual=%s", rec.Config.ID, expectedRevision, actualRevision)
		}
	}
	persistedID := strings.TrimSpace(persisted.Config.ID)
	if persistedID != "" {
		persistedRevision, err := lifecycleConfigRevision(persisted)
		if err != nil {
			return err
		}
		if persistedID != strings.TrimSpace(rec.Config.ID) || persistedRevision != expectedRevision {
			return fmt.Errorf("agent %s persisted config revision changed", rec.Config.ID)
		}
		if live {
			return nil
		}
		return am.spawnAgentInternal(ctx, persisted, false)
	}
	if live {
		return fmt.Errorf("agent %s is process-ready without durable registration", rec.Config.ID)
	}
	return am.spawnAgentInternal(ctx, rec, true)
}

func (am *AgentManager) verifyDynamicFlowAgents(ctx context.Context, instancePath string, expected []PersistedAgent) error {
	instancePath = strings.Trim(strings.TrimSpace(instancePath), "/")
	expectedByID := make(map[string]PersistedAgent, len(expected))
	for _, rec := range expected {
		expectedByID[strings.TrimSpace(rec.Config.ID)] = rec
	}
	persistedByID := make(map[string]PersistedAgent, len(expected))
	if am.store == nil {
		if len(expectedByID) != 0 {
			return fmt.Errorf("declared agents require durable manager persistence")
		}
	} else {
		persisted, err := am.store.LoadAgents(ctx)
		if err != nil {
			return err
		}
		for _, rec := range persisted {
			if strings.Trim(strings.TrimSpace(rec.Config.FlowPath), "/") == instancePath {
				persistedByID[strings.TrimSpace(rec.Config.ID)] = rec
			}
		}
	}
	liveByID := make(map[string]models.AgentConfig, len(expected))
	for _, cfg := range am.ListAgentConfigs() {
		if strings.Trim(strings.TrimSpace(cfg.FlowPath), "/") == instancePath {
			liveByID[strings.TrimSpace(cfg.ID)] = cfg
		}
	}
	if len(persistedByID) != len(expectedByID) || len(liveByID) != len(expectedByID) {
		return fmt.Errorf(
			"declared agent set mismatch: expected=%d persisted=%d process=%d",
			len(expectedByID),
			len(persistedByID),
			len(liveByID),
		)
	}
	for _, rec := range expected {
		expectedRevision, err := lifecycleConfigRevision(rec)
		if err != nil {
			return err
		}
		live, ok := liveByID[strings.TrimSpace(rec.Config.ID)]
		if !ok {
			return fmt.Errorf("declared agent %s is not process-ready", rec.Config.ID)
		}
		liveRevision, err := lifecycleConfigRevision(PersistedAgent{Config: live})
		if err != nil {
			return err
		}
		stored, ok := persistedByID[strings.TrimSpace(rec.Config.ID)]
		if !ok {
			return fmt.Errorf("declared agent %s is not durably registered", rec.Config.ID)
		}
		storedRevision, err := lifecycleConfigRevision(stored)
		if err != nil {
			return err
		}
		if liveRevision != expectedRevision || storedRevision != expectedRevision {
			return fmt.Errorf("declared agent %s readiness revision mismatch", rec.Config.ID)
		}
	}
	return nil
}

func verifyDynamicFlowAgentExpectations(actual []PersistedAgent, expected []runtimepipeline.DynamicFlowRuntimeAgentExpectation) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("declared agent count changed: expected=%d actual=%d", len(expected), len(actual))
	}
	for idx, rec := range actual {
		revision, err := lifecycleConfigRevision(rec)
		if err != nil {
			return err
		}
		if strings.TrimSpace(rec.Config.ID) != expected[idx].AgentID || revision != expected[idx].ConfigRevision {
			return fmt.Errorf("declared agent topology changed at %s", rec.Config.ID)
		}
	}
	return nil
}

func (am *AgentManager) verifyDynamicFlowRoute(ctx context.Context, route runtimeflowidentity.Route) error {
	verifier, ok := am.bus.(flowInstanceRouteContextVerifier)
	if !ok || verifier == nil || !verifier.HasFlowInstanceRoute(route) {
		return fmt.Errorf("dynamic flow route %s is not process-ready", route.InstancePath)
	}
	if err := verifier.VerifyFlowInstanceRoute(ctx, route); err != nil {
		return fmt.Errorf("verify dynamic flow route %s: %w", route.InstancePath, err)
	}
	return nil
}

func dynamicFlowRuntimeCreationEvent(plan runtimepipeline.DynamicFlowRuntimeReadinessPlan) (events.Event, error) {
	var empty events.Event
	creation := plan.CreationEvent
	if creation == nil {
		return empty, fmt.Errorf("dynamic flow creation event plan is required")
	}
	routingSource, err := events.NewRuntimeRoutingSource(plan.Identity.TemplateID, plan.Identity.InstancePath, plan.Identity.EntityID)
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
