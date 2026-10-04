package manager

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type RecoverableStateReader interface {
	InspectDynamicFlowRuntimeReadinessForSource(context.Context, correlation.SourceArtifactFact) (pipeline.DynamicFlowRuntimeReadinessProjection, error)
	LoadAgents(context.Context) ([]PersistedAgent, error)
	ListFlowInstanceRoutes(context.Context) ([]flowidentity.RunScopedFlowInstance, error)
	ListSelectedContractRouteRecoveryRecords(context.Context) ([]SelectedContractRouteRecoveryRecord, error)
	GlobalWorkPresence(context.Context) (pipelineobligation.GlobalWorkPresence, error)
}

func InspectRecoverableStateSnapshot(ctx context.Context, source correlation.SourceArtifactFact, reader RecoverableStateReader) (RecoverableStateSnapshot, error) {
	if reader == nil {
		return RecoverableStateSnapshot{}, errors.New("recoverable manager state reader is required")
	}
	if err := ctx.Err(); err != nil {
		return RecoverableStateSnapshot{}, err
	}
	if err := source.Validate(); err != nil {
		return RecoverableStateSnapshot{}, err
	}
	projection, err := reader.InspectDynamicFlowRuntimeReadinessForSource(ctx, source)
	if err != nil {
		return RecoverableStateSnapshot{}, fmt.Errorf("inspect source-scoped dynamic flow runtime readiness: %w", err)
	}
	snapshot := RecoverableStateSnapshot{PendingDynamicFlowRuntimeReadinessCount: len(projection.CurrentPending)}
	for _, item := range projection.SourceTransitionRequired {
		if item.Pending() {
			snapshot.PendingDynamicFlowRuntimeReadinessCount++
		}
	}
	agents, err := reader.LoadAgents(ctx)
	if err != nil {
		return RecoverableStateSnapshot{}, fmt.Errorf("load persisted agents: %w", err)
	}
	snapshot.PersistedAgentCount = len(agents)
	routes, err := reader.ListFlowInstanceRoutes(ctx)
	if err != nil {
		return RecoverableStateSnapshot{}, fmt.Errorf("list persisted flow instance routes: %w", err)
	}
	snapshot.PersistedFlowInstanceRouteCount = len(routes)
	recoveries, err := reader.ListSelectedContractRouteRecoveryRecords(ctx)
	if err != nil {
		return RecoverableStateSnapshot{}, fmt.Errorf("list selected-contract route recoveries: %w", err)
	}
	snapshot.PersistedSelectedContractRouteRecoveryCount = len(recoveries)
	presence, err := reader.GlobalWorkPresence(ctx)
	if err != nil {
		return RecoverableStateSnapshot{}, fmt.Errorf("load pipeline work presence: %w", err)
	}
	snapshot.ReplayEligibleEventPresent = presence.Any()
	if err := ctx.Err(); err != nil {
		return RecoverableStateSnapshot{}, err
	}
	return snapshot, nil
}

type managerRecoveryReads struct{ am *AgentManager }

func (r managerRecoveryReads) InspectDynamicFlowRuntimeReadinessForSource(ctx context.Context, source correlation.SourceArtifactFact) (pipeline.DynamicFlowRuntimeReadinessProjection, error) {
	if r.am.workflowInstances == nil {
		return pipeline.DynamicFlowRuntimeReadinessProjection{}, errors.New("dynamic flow readiness reader is required")
	}
	return r.am.InspectDynamicFlowRuntimeReadinessForSource(ctx, source)
}

func (r managerRecoveryReads) LoadAgents(ctx context.Context) ([]PersistedAgent, error) {
	if r.am.store == nil {
		return nil, errors.New("persisted agent reader is required")
	}
	return r.am.store.LoadAgents(ctx)
}

func (r managerRecoveryReads) ListFlowInstanceRoutes(ctx context.Context) ([]flowidentity.RunScopedFlowInstance, error) {
	if r.am.bus == nil {
		return nil, errors.New("flow instance route reader is required")
	}
	reader, ok := r.am.bus.Store().(interface {
		ListFlowInstanceRoutes(context.Context) ([]flowidentity.RunScopedFlowInstance, error)
	})
	if !ok || reader == nil {
		return nil, errors.New("flow instance route reader is required")
	}
	return reader.ListFlowInstanceRoutes(ctx)
}

func (r managerRecoveryReads) ListSelectedContractRouteRecoveryRecords(ctx context.Context) ([]SelectedContractRouteRecoveryRecord, error) {
	if r.am.bus == nil {
		return nil, errors.New("selected-contract route recovery reader is required")
	}
	reader, ok := r.am.bus.Store().(SelectedContractRouteRecoveryReader)
	if !ok || reader == nil {
		return nil, errors.New("selected-contract route recovery reader is required")
	}
	return reader.ListSelectedContractRouteRecoveryRecords(ctx)
}

func (r managerRecoveryReads) GlobalWorkPresence(ctx context.Context) (pipelineobligation.GlobalWorkPresence, error) {
	if r.am.bus == nil {
		return pipelineobligation.GlobalWorkPresence{}, errors.New("pipeline work presence reader is required")
	}
	return r.am.bus.PipelineWorkPresence(ctx)
}
