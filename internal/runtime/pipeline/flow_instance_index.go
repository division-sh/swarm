package pipeline

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type FlowInstanceIndexReader interface {
	LookupFlowInstance(context.Context, FlowInstanceLookupRequest) (FlowInstanceObservation, bool, error)
	ListFlowInstances(context.Context, FlowInstanceLookupScope) ([]FlowInstanceObservation, error)
}

type FlowInstanceLookupRequest struct {
	source                           semanticview.Source
	fact                             correlation.SourceArtifactFact
	runID, flowID, parent, key, path string
	declared                         bool
	parentIdentity                   flowidentity.Instance
	exactRoute                       flowidentity.Route
}

func NewExactFlowInstanceLookup(source semanticview.Source, fact correlation.SourceArtifactFact, owner flowidentity.RunScopedFlowInstance) (FlowInstanceLookupRequest, error) {
	if err := validateFlowInstanceLookupSource(source, fact, owner.RunID); err != nil {
		return FlowInstanceLookupRequest{}, err
	}
	if owner != owner.Normalize() || owner.Validate() != nil {
		return FlowInstanceLookupRequest{}, fmt.Errorf("instance lookup requires an exact canonical coordinate")
	}
	flowID := owner.Route.ScopeKey
	if _, found := source.FlowSchemaByID(flowID); !found || flowidentity.ScopeKey(source, flowID) != owner.Route.ScopeKey {
		return FlowInstanceLookupRequest{}, fmt.Errorf("instance lookup has an unknown declaration")
	}
	if (flowID == semanticview.RootExecutionFlowID(source)) != (owner.Route.InstancePath == owner.RunID) {
		return FlowInstanceLookupRequest{}, fmt.Errorf("instance lookup has a foreign root coordinate")
	}
	return FlowInstanceLookupRequest{source: source, fact: fact, runID: owner.RunID, flowID: flowID, path: owner.Route.InstancePath, exactRoute: owner.Route}, nil
}

func NewDeclaredFlowInstanceLookup(source semanticview.Source, fact correlation.SourceArtifactFact, runID, flowID string, parent flowidentity.Instance, keys []contracts.TemplateInstanceKeyValue) (FlowInstanceLookupRequest, error) {
	if err := validateFlowInstanceLookupSource(source, fact, runID); err != nil {
		return FlowInstanceLookupRequest{}, err
	}
	flow, found := source.FlowSchemaByID(flowID)
	if !found || flowID != strings.TrimSpace(flowID) {
		return FlowInstanceLookupRequest{}, fmt.Errorf("instance lookup requires its exact declared flow")
	}
	key, err := admitFlowInstanceLookupKey(source, flowID, flow.Instance, keys)
	if err != nil {
		return FlowInstanceLookupRequest{}, err
	}
	request := FlowInstanceLookupRequest{source: source, fact: fact, runID: runID, flowID: flowID, key: key, declared: true}
	if flowID == semanticview.RootExecutionFlowID(source) {
		if parent != (flowidentity.Instance{}) {
			return FlowInstanceLookupRequest{}, fmt.Errorf("selected root cannot have a structural parent")
		}
		request.path = runID
		return request, nil
	}
	if err := parent.ValidateConstruction(source, runID); err != nil {
		return FlowInstanceLookupRequest{}, err
	}
	bundle, _ := semanticview.Bundle(source)
	view, found := bundle.FlowViewByID(flowID)
	if !found || view.Parent == nil || view.Parent.Paths.FlowPath != parent.TemplateID {
		return FlowInstanceLookupRequest{}, fmt.Errorf("instance lookup requires its exact compiled structural parent")
	}
	request.parent = parent.InstancePath
	request.parentIdentity = parent
	return request, nil
}

func admitFlowInstanceLookupKey(source semanticview.Source, flowID string, field contracts.TemplateInstanceField, keys []contracts.TemplateInstanceKeyValue) (string, error) {
	if field.Empty() {
		if len(keys) != 0 {
			return "", fmt.Errorf("keyless declaration cannot have lookup keys")
		}
		return "", nil
	}
	if len(keys) != 1 || keys[0].Field != field {
		return "", fmt.Errorf("keyed lookup requires exactly its declared key field")
	}
	value, err := keys[0].ResolvedValue()
	if err != nil {
		return "", err
	}
	material, err := AdmitFlowInstanceKey(source, flowID, value)
	if err != nil {
		return "", err
	}
	if keys[0].Value != material {
		return "", fmt.Errorf("instance key text contradicts its admitted scalar")
	}
	return material, nil
}

type FlowInstanceLookupScope struct {
	source semanticview.Source
	fact   correlation.SourceArtifactFact
	runID  string
	flows  []string
	exact  []flowidentity.RunScopedFlowInstance
}

func NewFlowInstanceLookupScope(source semanticview.Source, fact correlation.SourceArtifactFact, runID string, flowIDs []string, owners []flowidentity.RunScopedFlowInstance) (FlowInstanceLookupScope, error) {
	if err := validateFlowInstanceLookupSource(source, fact, runID); err != nil {
		return FlowInstanceLookupScope{}, err
	}
	scope := FlowInstanceLookupScope{source: source, fact: fact, runID: runID}
	for _, flowID := range flowIDs {
		if _, found := source.FlowSchemaByID(flowID); !found || flowID != strings.TrimSpace(flowID) {
			return FlowInstanceLookupScope{}, fmt.Errorf("instance inventory requires exact dependency declarations")
		}
		if !slices.Contains(scope.flows, flowID) {
			scope.flows = append(scope.flows, flowID)
		}
	}
	coordinates := make(map[string]flowidentity.RunScopedFlowInstance, len(owners))
	for _, owner := range owners {
		if err := validateFlowInstanceInventoryCoordinate(source, fact, runID, owner); err != nil {
			return FlowInstanceLookupScope{}, err
		}
		if previous, exists := coordinates[owner.Key()]; exists && previous != owner {
			return FlowInstanceLookupScope{}, fmt.Errorf("instance inventory contradicts one canonical coordinate")
		}
		coordinates[owner.Key()] = owner
	}
	for _, owner := range coordinates {
		scope.exact = append(scope.exact, owner)
	}
	slices.Sort(scope.flows)
	slices.SortFunc(scope.exact, func(a, b flowidentity.RunScopedFlowInstance) int { return strings.Compare(a.Key(), b.Key()) })
	return scope, nil
}

func validateFlowInstanceInventoryCoordinate(source semanticview.Source, fact correlation.SourceArtifactFact, runID string, owner flowidentity.RunScopedFlowInstance) error {
	if owner.RunID != runID {
		return fmt.Errorf("instance inventory has a foreign run coordinate")
	}
	if _, err := NewExactFlowInstanceLookup(source, fact, owner); err != nil {
		return fmt.Errorf("instance inventory has a foreign or invalid coordinate: %w", err)
	}
	return nil
}

func validateFlowInstanceLookupSource(source semanticview.Source, fact correlation.SourceArtifactFact, runID string) error {
	if err := fact.Validate(); err != nil {
		return err
	}
	id, err := uuid.Parse(runID)
	if err != nil || id == uuid.Nil || id.String() != runID {
		return fmt.Errorf("instance lookup requires an exact run UUID")
	}
	bundle, found := semanticview.Bundle(source)
	if !found || bundle.SourceArtifact == nil || bundle.SourceArtifact.BundleHash() != fact.BundleHash() || source.WorkflowVersion() == "" {
		return fmt.Errorf("instance lookup source contradicts its admitted artifact")
	}
	return nil
}

func (r FlowInstanceLookupRequest) Valid() bool {
	return r.source != nil && r.runID != "" && r.flowID != ""
}
func (r FlowInstanceLookupRequest) Source() semanticview.Source                { return r.source }
func (r FlowInstanceLookupRequest) SourceFact() correlation.SourceArtifactFact { return r.fact }
func (r FlowInstanceLookupRequest) RunID() string                              { return r.runID }
func (r FlowInstanceLookupRequest) FlowID() string                             { return r.flowID }
func (r FlowInstanceLookupRequest) ParentInstance() string                     { return r.parent }
func (r FlowInstanceLookupRequest) ParentIdentity() flowidentity.Instance      { return r.parentIdentity }
func (r FlowInstanceLookupRequest) InstanceKey() string                        { return r.key }
func (r FlowInstanceLookupRequest) ExactPath() string                          { return r.path }
func (r FlowInstanceLookupRequest) DeclaredSelection() bool                    { return r.declared }
func (s FlowInstanceLookupScope) Valid() bool                                  { return s.source != nil && s.runID != "" }
func (s FlowInstanceLookupScope) Source() semanticview.Source                  { return s.source }
func (s FlowInstanceLookupScope) SourceFact() correlation.SourceArtifactFact   { return s.fact }
func (s FlowInstanceLookupScope) RunID() string                                { return s.runID }
func (s FlowInstanceLookupScope) FlowIDs() []string                            { return slices.Clone(s.flows) }
func (s FlowInstanceLookupScope) Coordinates() []flowidentity.RunScopedFlowInstance {
	return slices.Clone(s.exact)
}
