package runforkadmission

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
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
	if source == nil {
		return flowidentity.Instance{}, runfork.RunForkEntityState{}, false, fmt.Errorf("fixed construction requires its exact source, run and route")
	}
	constructed, entity, found, err := FixedConstructionEvidenceForRoute(plan, route)
	if err != nil || !found {
		return flowidentity.Instance{}, entity, false, err
	}
	if err := validateSelectedConstruction(source, plan.SourceRunID, constructed, entity); err != nil {
		return flowidentity.Instance{}, entity, false, err
	}
	return constructed, entity, true, nil
}

func validateSelectedConstruction(source semanticview.Source, runID string, constructed flowidentity.Instance, entity runfork.RunForkEntityState) error {
	if err := constructed.ValidateConstruction(source, runID); err != nil {
		return fmt.Errorf("selected fixed-revision construction: %w", err)
	}
	metadata := entity.MaterializationMetadata
	scope, declared := source.FlowScopeByID(constructed.TemplateID)
	if !declared || metadata.Mode != scope.Mode {
		return fmt.Errorf("selected header conflicts with declared flow mode")
	}
	contract, declared := entityruntime.ResolveForFlow(source, constructed.TemplateID)
	if (declared && contract.EntityType != metadata.EntityType) || (!declared && (metadata.EntityType != "" || len(entity.Fields) != 0)) {
		return fmt.Errorf("selected entity %s type %q disagrees with selected flow %s", entity.EntityID, metadata.EntityType, constructed.TemplateID)
	}
	return nil
}

// FixedConstructionEvidenceForRoute reports captured facts without admitting
// today's executable declaration. Identity comes only from the immutable receipt.
func FixedConstructionEvidenceForRoute(plan runfork.RunForkPlan, route flowidentity.Route) (flowidentity.Instance, runfork.RunForkEntityState, bool, error) {
	if plan.SourceRunID == "" || !route.Valid() {
		return flowidentity.Instance{}, runfork.RunForkEntityState{}, false, fmt.Errorf("fixed construction evidence requires its exact run and recorded route")
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
		receipt, err := fixedConstructionEvidence(plan, entity)
		if err != nil {
			return flowidentity.Instance{}, matched, false, err
		}
		constructed = receipt.Identity
		if constructed.Route() != route || config.ParentRoute() != constructed.ParentRoute ||
			metadata.ParentInstance != constructed.ParentRoute.FlowInstance || metadata.InstanceKey != receipt.InstanceKey {
			return flowidentity.Instance{}, matched, false, fmt.Errorf("fixed header/config contradicts immutable construction identity")
		}
		if err := pipeline.ValidateCapturedConstructionModeKey(metadata.Mode, receipt.InstanceKey); err != nil {
			return flowidentity.Instance{}, matched, false, fmt.Errorf("fixed header construction mode: %w", err)
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
		receipt, err := fixedConstructionEvidence(plan, entity)
		if err != nil {
			return nil, err
		}
		route := receipt.Identity.Route()
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

type capturedConstructionEvidence struct {
	Identity    flowidentity.Instance
	InstanceKey string
	EntityType  string
	// Original is the detached, decoded native evidence at the terminal hop.
	// It is never rewritten into a receipt for a historical child.
	Original pipeline.FlowConstructionReceipt
	Lineage  []capturedConstructionHop
}

type capturedConstructionHop struct {
	SourceRunID, RunID string
	SourceRevision     int64
}

// FixedConstructionOccurrenceID names only the recorded constructor output in
// this captured run. Event membership separately decides whether it is owed.
func FixedConstructionOccurrenceID(plan runfork.RunForkPlan, entity runfork.RunForkEntityState) (string, error) {
	evidence, err := fixedConstructionEvidence(plan, entity)
	if err != nil || evidence.Original.CreationEvent == nil {
		return "", err
	}
	id := evidence.Original.CreationEvent.EventID
	for _, hop := range evidence.Lineage {
		id = runfork.ProjectReplayEventID(hop.RunID, id)
	}
	return id, nil
}

func (e capturedConstructionEvidence) creatingSelector(plan runfork.RunForkPlan) (events.Event, bool, error) {
	id := e.Original.CreatingInput.EventID
	if id == "" {
		return events.Event{}, false, nil
	}
	previousID := id
	for _, hop := range e.Lineage {
		previousID = id
		id = runfork.ProjectReplayEventID(hop.RunID, id)
	}
	selector, found, err := plan.HistoricalRouteSelector(id)
	if err != nil || !found {
		return events.Event{}, false, err
	}
	if len(e.Lineage) != 0 {
		hop := e.Lineage[len(e.Lineage)-1]
		lineage, recorded := selector.SelectedForkLineage()
		if !recorded || lineage.DestinationRunID() != hop.RunID || lineage.SourceRunID() != hop.SourceRunID || lineage.SourceEventID() != previousID {
			return events.Event{}, false, fmt.Errorf("historical constructor selector contradicts its recorded source lineage")
		}
	}
	return selector, true, nil
}

func fixedConstructionEvidence(plan runfork.RunForkPlan, entity runfork.RunForkEntityState) (capturedConstructionEvidence, error) {
	type hop struct {
		runID, sourceRunID string
		revision           int64
		entity             runfork.RunForkEntityState
	}
	var hops []hop
	seen := make(map[string]bool)
	runID := plan.SourceRunID
	var evidence capturedConstructionEvidence
	for {
		if runID == "" || seen[runID] {
			return evidence, fmt.Errorf("captured construction has missing or cyclic lineage")
		}
		seen[runID] = true
		meta := entity.MaterializationMetadata
		if meta == nil || meta.Source != runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance {
			return evidence, fmt.Errorf("fixed construction requires captured constructed evidence")
		}
		raw := bytes.TrimSpace(meta.InitialMaterialization)
		if len(raw) != 0 && !bytes.Equal(raw, []byte("null")) {
			receipt, err := pipeline.DecodeStoredFlowConstructionReceipt(raw, runID, entity.EntityID, meta.FlowInstance, meta.FlowTemplate)
			if err != nil {
				return evidence, err
			}
			if meta.HistoricalConstruction != nil {
				return evidence, fmt.Errorf("captured construction has conflicting native and historical evidence")
			}
			evidence = capturedConstructionEvidence{Identity: receipt.Identity, InstanceKey: receipt.InstanceKey, EntityType: receipt.Persisted.Control.EntityType, Original: receipt}
			if err := validateCapturedConstructionBinding(entity, evidence); err != nil {
				return evidence, err
			}
			break
		}
		history := meta.HistoricalConstruction
		if history == nil || history.SourceRevision <= 0 || history.SourceRunID == "" || history.SourceRunID == runID {
			return evidence, fmt.Errorf("fixed construction lacks positive historical provenance and its native receipt")
		}
		hops = append(hops, hop{runID: runID, sourceRunID: history.SourceRunID, revision: history.SourceRevision, entity: entity})
		runID, entity = history.SourceRunID, history.SourceEntity
	}
	for i := len(hops) - 1; i >= 0; i-- {
		projected, err := runfork.ProjectCapturedConstructionIdentity(runID, hops[i].runID, evidence.Identity)
		if err != nil {
			return evidence, err
		}
		evidence.Identity = projected
		evidence.Lineage = append(evidence.Lineage, capturedConstructionHop{SourceRunID: hops[i].sourceRunID, RunID: hops[i].runID, SourceRevision: hops[i].revision})
		if err := validateCapturedConstructionBinding(hops[i].entity, evidence); err != nil {
			return evidence, err
		}
		runID = hops[i].runID
	}
	return evidence, nil
}

func validateCapturedConstructionBinding(entity runfork.RunForkEntityState, evidence capturedConstructionEvidence) error {
	meta := entity.MaterializationMetadata
	identity := evidence.Identity
	if meta == nil || meta.Owner != runfork.RunForkMaterializedEntitySnapshotMetadataOwner ||
		entity.EntityID != identity.EntityID || meta.FlowInstance != identity.InstancePath || meta.FlowTemplate != identity.TemplateID ||
		meta.ParentInstance != identity.ParentRoute.FlowInstance || meta.InstanceKey != evidence.InstanceKey || meta.EntityType != evidence.EntityType {
		return fmt.Errorf("fixed header contradicts captured construction identity")
	}
	config, err := pipeline.DecodeWorkflowInstanceRecordedHeader(identity.Route(), meta.FlowConfig)
	if err != nil {
		return err
	}
	if config.ParentRoute() != identity.ParentRoute {
		return fmt.Errorf("fixed header/config contradicts immutable construction ancestry")
	}
	return pipeline.ValidateCapturedConstructionModeKey(meta.Mode, evidence.InstanceKey)
}

// ValidateFixedConstructionTree checks the original source's atomic keyless
// construction, not future keyed instances or the selected target's new graph.
func ValidateFixedConstructionTree(original semanticview.Source, plan runfork.RunForkPlan) error {
	if original == nil {
		return fmt.Errorf("fixed construction tree requires its original immutable source")
	}
	if err := validateFixedConstructionMetadata(plan.Entities); err != nil {
		return err
	}
	instances := make(map[string]flowidentity.Instance, len(plan.Entities))
	for _, entity := range plan.Entities {
		receipt, err := fixedConstructionEvidence(plan, entity)
		if err != nil {
			return err
		}
		if err := receipt.Identity.ValidateConstruction(original, plan.SourceRunID); err != nil {
			return err
		}
		instances[receipt.Identity.InstancePath] = receipt.Identity
	}
	for _, instance := range instances {
		if parent := instance.ParentRoute; !parent.Empty() {
			owner, present := instances[parent.FlowInstance]
			if !present || owner.TemplateID != parent.FlowID || owner.EntityID != parent.EntityID {
				return fmt.Errorf("captured constructor has missing or conflicting exact parent %s", parent.FlowInstance)
			}
		}
		children, err := flowidentity.KeylessChildFlowIDs(original, instance.TemplateID)
		if err != nil {
			return err
		}
		for _, flowID := range children {
			expected, err := flowidentity.KeylessChild(original, instance, flowID)
			if err != nil {
				return err
			}
			if actual, present := instances[expected.InstancePath]; !present || actual != expected {
				return fmt.Errorf("captured atomic constructor tree lacks exact keyless descendant %s", expected.InstancePath)
			}
		}
	}
	return nil
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
			metadata.FlowTemplate == "" || (metadata.Mode != "static" && metadata.Mode != "template") || len(metadata.FlowConfig) == 0 || (len(metadata.InitialMaterialization) == 0 && metadata.HistoricalConstruction == nil) ||
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
