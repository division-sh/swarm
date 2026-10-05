package runforkadmission

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// FixedConstructionForRoute is the fixed header/config decoder shared by all
// selected consumers. Parent context validates construction only; it cannot
// grant routing or target permission.
func FixedConstructionForRoute(source semanticview.Source, plan runfork.RunForkPlan, route flowidentity.Route) (flowidentity.Instance, runfork.RunForkEntityState, bool, error) {
	if source == nil || plan.SourceRunID == "" || !route.Valid() {
		return flowidentity.Instance{}, runfork.RunForkEntityState{}, false, fmt.Errorf("fixed construction requires its exact source, run and route")
	}
	if err := validateFixedConstructionMetadata(plan.Entities); err != nil {
		return flowidentity.Instance{}, runfork.RunForkEntityState{}, false, err
	}
	var constructed flowidentity.Instance
	var matched runfork.RunForkEntityState
	found := false
	for _, entity := range plan.Entities {
		metadata := entity.MaterializationMetadata
		if metadata == nil || metadata.FlowInstance != route.InstancePath {
			continue
		}
		if found || metadata.Owner != runfork.RunForkMaterializedEntitySnapshotMetadataOwner ||
			metadata.Source != runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance {
			return flowidentity.Instance{}, matched, false, fmt.Errorf("selected readiness requires one exact fixed-revision construction header")
		}
		config, err := pipeline.DecodeWorkflowInstanceRecordedHeader(route, metadata.FlowConfig)
		if err != nil {
			return flowidentity.Instance{}, matched, false, err
		}
		constructed = flowidentity.Instance{
			TemplateID: metadata.FlowTemplate, ScopeKey: route.ScopeKey, InstanceID: route.InstanceID,
			InstancePath: route.InstancePath, EntityID: entity.EntityID, ParentRoute: config.ParentRoute(),
			ParentEntityID: config.ParentRoute().EntityID, HasStoredPath: true,
		}
		if err := constructed.ValidateConstruction(source, plan.SourceRunID); err != nil {
			return flowidentity.Instance{}, matched, false, fmt.Errorf("selected fixed-revision construction: %w", err)
		}
		scope, declared := source.FlowScopeByID(constructed.TemplateID)
		if !declared || metadata.Mode != scope.Mode {
			return flowidentity.Instance{}, matched, false, fmt.Errorf("selected header conflicts with declared flow mode")
		}
		contract, declared := entityruntime.ResolveForFlow(source, constructed.TemplateID)
		if (declared && contract.EntityType != metadata.EntityType) || (!declared && (metadata.EntityType != "" || len(entity.Fields) != 0)) {
			return flowidentity.Instance{}, matched, false, fmt.Errorf("selected entity %s type %q disagrees with selected flow %s", entity.EntityID, metadata.EntityType, constructed.TemplateID)
		}
		matched = entity
		found = true
	}
	return constructed, matched, found, nil
}

// ConstructedInstances inventories only already-recorded non-root construction.
// It neither reconstructs paths from recipients nor creates future instances.
func ConstructedInstances(source semanticview.Source, plan runfork.RunForkPlan) ([]flowidentity.Instance, error) {
	if source == nil {
		return nil, fmt.Errorf("constructed census requires selected semantic source")
	}
	if err := validateFixedConstructionMetadata(plan.Entities); err != nil {
		return nil, err
	}
	var instances []flowidentity.Instance
	for _, entity := range plan.Entities {
		meta := entity.MaterializationMetadata
		route := flowidentity.StoredRoute(flowidentity.ScopeKey(source, meta.FlowTemplate), flowidentity.LogicalInstanceID(meta.FlowInstance), meta.FlowInstance)
		instance, _, found, err := FixedConstructionForRoute(source, plan, route)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("constructed census lost fixed header %s", meta.FlowInstance)
		}
		if meta.FlowTemplate == semanticview.RootExecutionFlowID(source) {
			continue
		}
		instances = append(instances, instance)
	}
	sort.Slice(instances, func(i, j int) bool { return instances[i].InstancePath < instances[j].InstancePath })
	return instances, nil
}

func validateFixedConstructionMetadata(entities []runfork.RunForkEntityState) error {
	seen := make(map[string]struct{}, len(entities))
	routes := make(map[string]struct{}, len(entities))
	for _, entity := range entities {
		id := strings.TrimSpace(entity.EntityID)
		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("%s: duplicate entity ownership %s", runfork.RunForkMaterializedEntitySnapshotMetadataOwner, id)
		}
		metadata := entity.MaterializationMetadata
		if id == "" || id != entity.EntityID || metadata == nil || metadata.Owner != runfork.RunForkMaterializedEntitySnapshotMetadataOwner ||
			metadata.Source != runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance ||
			metadata.FlowTemplate == "" || (metadata.Mode != "static" && metadata.Mode != "template") || len(metadata.FlowConfig) == 0 ||
			strings.TrimSpace(metadata.FlowInstance) == "" || strings.TrimSpace(entity.CurrentState) == "" ||
			entity.EnteredStateAt == nil || entity.EnteredStateAt.IsZero() {
			return fmt.Errorf("%s: entity %s requires exact fixed-revision owner metadata", runfork.RunForkMaterializedEntitySnapshotMetadataOwner, id)
		}
		if _, duplicate := routes[metadata.FlowInstance]; duplicate {
			return fmt.Errorf("%s: duplicate construction route %s", runfork.RunForkMaterializedEntitySnapshotMetadataOwner, metadata.FlowInstance)
		}
		routes[metadata.FlowInstance] = struct{}{}
		seen[id] = struct{}{}
	}
	return nil
}
