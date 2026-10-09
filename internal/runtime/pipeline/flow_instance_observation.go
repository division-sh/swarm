package pipeline

import (
	"fmt"
	"slices"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

// FlowInstanceObservation is snapshot data, never attachment or write authority.
type FlowInstanceObservation struct {
	request     FlowInstanceLookupRequest
	identity    flowidentity.Instance
	instance    WorkflowInstance
	native      *FlowConstructionPublicationEvidence
	historical  *HistoricalFlowInstanceConstruction
	readiness   *DynamicFlowRuntimeReadiness
	runState    runlifecycle.State
	runRevision int64
}

// The private historical owner produces these facts from stored origin and the
// exact fixed cut. Requests cannot choose provenance with a flag or public plan.
type HistoricalFlowInstanceConstruction struct {
	Identity       flowidentity.Instance
	InstanceKey    string
	SourceOwner    flowidentity.RunScopedFlowInstance
	SourceRevision int64
}

func AdmitNativeFlowInstanceObservation(request FlowInstanceLookupRequest, instance WorkflowInstance, run runlifecycle.Snapshot, revision int64, receipt FlowConstructionPublicationEvidence, readiness DynamicFlowRuntimeReadiness) (FlowInstanceObservation, error) {
	observation, err := admitFlowInstanceObservation(request, instance, run, revision)
	if err != nil {
		return FlowInstanceObservation{}, err
	}
	if receipt.Identity != observation.identity || receipt.InstanceKey != instance.InstanceKey {
		return FlowInstanceObservation{}, fmt.Errorf("native instance contradicts its immutable construction receipt")
	}
	fields, err := cloneIndexFields(receipt.Fields)
	if err != nil {
		return FlowInstanceObservation{}, err
	}
	receipt.Fields = fields
	observation.native = &receipt
	if err := observation.admitReadiness(readiness); err != nil {
		return FlowInstanceObservation{}, err
	}
	return observation, nil
}

func AdmitHistoricalFlowInstanceObservation(request FlowInstanceLookupRequest, instance WorkflowInstance, run runlifecycle.Snapshot, revision int64, historical HistoricalFlowInstanceConstruction, readiness *DynamicFlowRuntimeReadiness) (FlowInstanceObservation, error) {
	observation, err := admitFlowInstanceObservation(request, instance, run, revision)
	if err != nil {
		return FlowInstanceObservation{}, err
	}
	if run.Origin.Kind() != runlifecycle.OriginForkMaterialization || historical.SourceOwner.RunID != run.Origin.SourceRunID() ||
		historical.SourceRevision != run.Origin.ForkRevision() || historical.SourceRevision <= 0 || historical.SourceOwner.Validate() != nil ||
		historical.Identity != observation.identity || historical.InstanceKey != instance.InstanceKey {
		return FlowInstanceObservation{}, fmt.Errorf("historical instance contradicts its admitted fixed construction")
	}
	projected, err := runfork.ProjectExecutionRoute(historical.SourceOwner.RunID, run.RunID, historical.SourceOwner.Route.ScopeKey, historical.SourceOwner.Route)
	if err != nil || projected != observation.identity.Route() {
		return FlowInstanceObservation{}, fmt.Errorf("historical instance has a crossed fixed receiver coordinate")
	}
	observation.historical = &historical
	if readiness != nil {
		if err := observation.admitReadiness(*readiness); err != nil {
			return FlowInstanceObservation{}, err
		}
	}
	return observation, nil
}

func admitFlowInstanceObservation(request FlowInstanceLookupRequest, instance WorkflowInstance, run runlifecycle.Snapshot, revision int64) (FlowInstanceObservation, error) {
	if !request.Valid() || run.Validate() != nil || run.RunID != request.runID || run.BundleHash != request.fact.BundleHash() || revision <= 0 {
		return FlowInstanceObservation{}, fmt.Errorf("instance observation requires its exact source, run and revision")
	}
	if instance.WorkflowName != request.flowID || instance.WorkflowVersion != request.source.WorkflowVersion() ||
		instance.Revision <= 0 || instance.CurrentState == "" || instance.CreatedAt.IsZero() || instance.UpdatedAt.IsZero() {
		return FlowInstanceObservation{}, fmt.Errorf("instance observation has incomplete or crossed header facts")
	}
	identity := flowidentity.Instance{
		TemplateID: instance.WorkflowName, ScopeKey: flowidentity.ScopeKey(request.source, instance.WorkflowName),
		InstanceID: instance.InstanceID, InstancePath: instance.StorageRef, EntityID: instance.EntityID, HasStoredPath: true,
		ParentEntityID: instance.ParentEntityID,
		ParentRoute:    flowidentity.ParentRoute{FlowID: instance.ParentFlowID, FlowInstance: instance.ParentFlowInstance, EntityID: instance.ParentEntityID},
	}
	if err := identity.ValidateConstruction(request.source, run.RunID); err != nil {
		return FlowInstanceObservation{}, err
	}
	if !request.declared && identity.Route() != request.exactRoute {
		return FlowInstanceObservation{}, fmt.Errorf("instance observation contradicts its requested route coordinate")
	}
	if err := validateIndexedHeaderDeclaration(request, instance); err != nil {
		return FlowInstanceObservation{}, err
	}
	if request.path != "" && instance.StorageRef != request.path ||
		request.declared && (instance.ParentFlowInstance != request.parent || instance.InstanceKey != request.key) {
		return FlowInstanceObservation{}, fmt.Errorf("instance observation contradicts its exact selection")
	}
	isolated, err := cloneIndexWorkflowInstance(instance)
	if err != nil {
		return FlowInstanceObservation{}, err
	}
	return FlowInstanceObservation{request: request, identity: identity, instance: isolated, runState: run.State, runRevision: revision}, nil
}

func validateIndexedHeaderDeclaration(request FlowInstanceLookupRequest, instance WorkflowInstance) error {
	flow, found := request.source.FlowSchemaByID(instance.WorkflowName)
	if !found || instance.Mode != flow.EffectiveMode() || flow.Instance.Empty() != (instance.InstanceKey == "") {
		return fmt.Errorf("instance observation contradicts declared key or mode")
	}
	entity, declared := entityruntime.ResolveForFlow(request.source, instance.WorkflowName)
	if declared && instance.EntityType != entity.EntityType || !declared && instance.EntityType != "" {
		return fmt.Errorf("instance observation contradicts its declared entity owner")
	}
	return nil
}

func (o *FlowInstanceObservation) admitReadiness(readiness DynamicFlowRuntimeReadiness) error {
	owner, err := readiness.Plan.FlowIdentity()
	if err != nil || owner.Key() != o.Owner().Key() || readiness.Plan.Identity != o.identity ||
		!readiness.OwningRunSource.Matches(o.request.fact) || readiness.Plan.BundleHash != o.request.fact.BundleHash() ||
		readiness.Plan.WorkflowVersion != o.instance.WorkflowVersion || readiness.RunStatus != string(o.runState) ||
		readiness.InstanceStatus != o.instance.Status || !readiness.InstanceTerminatedAt.Equal(o.instance.TerminatedAt) {
		return fmt.Errorf("instance observation contradicts its exact desired attachment")
	}
	readiness.Plan = cloneIndexReadinessPlan(readiness.Plan)
	o.readiness = &readiness
	return nil
}

func (o FlowInstanceObservation) Valid() bool {
	return o.request.Valid() && (o.native != nil || o.historical != nil)
}
func (o FlowInstanceObservation) Identity() flowidentity.Instance { return o.identity }
func (o FlowInstanceObservation) Owner() flowidentity.RunScopedFlowInstance {
	return flowidentity.RunScopedFlowInstance{RunID: o.request.runID, Route: o.identity.Route()}
}
func (o FlowInstanceObservation) SourceFact() correlation.SourceArtifactFact { return o.request.fact }
func (o FlowInstanceObservation) RunState() runlifecycle.State               { return o.runState }
func (o FlowInstanceObservation) RunRevision() int64                         { return o.runRevision }
func (o FlowInstanceObservation) HeaderRevision() int64                      { return o.instance.Revision }
func (o FlowInstanceObservation) InstanceKey() string                        { return o.instance.InstanceKey }
func (o FlowInstanceObservation) Selection() FlowInstanceLookupRequest       { return o.request }
func (o FlowInstanceObservation) WorkflowInstance() (WorkflowInstance, error) {
	return cloneIndexWorkflowInstance(o.instance)
}
func (o FlowInstanceObservation) HistoricalConstruction() (HistoricalFlowInstanceConstruction, bool) {
	if o.historical == nil {
		return HistoricalFlowInstanceConstruction{}, false
	}
	return *o.historical, true
}
func (o FlowInstanceObservation) NativeConstruction() (FlowConstructionPublicationEvidence, bool, error) {
	if o.native == nil {
		return FlowConstructionPublicationEvidence{}, false, nil
	}
	receipt := *o.native
	fields, err := cloneIndexFields(receipt.Fields)
	if err != nil {
		return FlowConstructionPublicationEvidence{}, false, err
	}
	receipt.Fields = fields
	return receipt, true, nil
}
func (o FlowInstanceObservation) Readiness() (DynamicFlowRuntimeReadiness, bool) {
	if o.readiness == nil {
		return DynamicFlowRuntimeReadiness{}, false
	}
	readiness := *o.readiness
	readiness.Plan = cloneIndexReadinessPlan(readiness.Plan)
	return readiness, true
}

func cloneIndexWorkflowInstance(instance WorkflowInstance) (WorkflowInstance, error) {
	for _, fields := range []*map[string]any{&instance.Fields, &instance.Bookkeeping, &instance.StateBuckets, &instance.InitialFieldValues} {
		isolated, err := cloneIndexFields(*fields)
		if err != nil {
			return WorkflowInstance{}, err
		}
		*fields = isolated
	}
	instance.Gates = cloneWorkflowGates(instance.Gates)
	instance.TransitionHistory = slices.Clone(instance.TransitionHistory)
	if instance.RuntimeReadiness != nil {
		readiness := cloneIndexReadinessPlan(*instance.RuntimeReadiness)
		instance.RuntimeReadiness = &readiness
	}
	return instance, nil
}

func cloneIndexFields(fields map[string]any) (map[string]any, error) {
	if fields == nil {
		return nil, nil
	}
	isolated, err := canonicaljson.CloneRuntimeValue(fields)
	if err != nil {
		return nil, err
	}
	return isolated.(map[string]any), nil
}

func cloneIndexReadinessPlan(plan DynamicFlowRuntimeReadinessPlan) DynamicFlowRuntimeReadinessPlan {
	plan.Agents = slices.Clone(plan.Agents)
	if plan.CreationEvent != nil {
		creation := *plan.CreationEvent
		creation.Payload = slices.Clone(creation.Payload)
		creation.DeliveryContext.Joins = slices.Clone(creation.DeliveryContext.Joins)
		if creation.DeliveryContext.Reply != nil {
			reply := *creation.DeliveryContext.Reply
			creation.DeliveryContext.Reply = &reply
		}
		plan.CreationEvent = &creation
	}
	return plan
}
