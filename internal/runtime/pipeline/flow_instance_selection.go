package pipeline

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

// Selection contains either an admitted stored receiver or a pure R5.1
// construction plan. Neither is authority to commit or attach that receiver.
type FlowInstanceSelection struct {
	Observation FlowInstanceObservation
	Activation  *FlowInstanceActivationPlan
	Proposed    *FlowInstanceActivationPlan
}

func (s FlowInstanceSelection) Identity() flowidentity.Instance {
	if s.Activation != nil {
		return s.Activation.Identity
	}
	if s.Proposed != nil {
		return s.Proposed.Identity
	}
	return s.Observation.Identity()
}

type FlowInstanceSelectionRequest struct {
	Lookup            FlowInstanceLookupRequest
	Mode              contracts.FlowInputResolutionMode
	MissingInstanceID string
	Constructor       FlowInstanceActivationRequest
	Prepared          []FlowInstanceActivationPlan
}

// PrepareFlowInstanceSelection is the shared R5.1 select/create boundary.
// A stored identity is never reconstructed from fields or a creation hash.
func PrepareFlowInstanceSelection(ctx context.Context, reader FlowInstanceIndexReader, planner FlowInstanceActivationPlanner, request FlowInstanceSelectionRequest) (FlowInstanceSelection, error) {
	if reader == nil || !request.Lookup.Valid() || !request.Lookup.DeclaredSelection() {
		return FlowInstanceSelection{}, fmt.Errorf("flow selection requires its admitted declared lookup and index owner")
	}
	switch request.Mode {
	case contracts.FlowInputResolutionModeCreate, contracts.FlowInputResolutionModeSelect, contracts.FlowInputResolutionModeSelectOrCreate:
	default:
		return FlowInstanceSelection{}, fmt.Errorf("flow selection requires an ordinary resolution mode")
	}
	proposed, parentPrepared, err := selectPreparedFlowInstance(request)
	if err != nil {
		return FlowInstanceSelection{}, err
	}
	if proposed != nil {
		if request.Mode == contracts.FlowInputResolutionModeCreate {
			owner, err := flowidentity.NewRunScopedFlowInstance(request.Lookup.RunID(), proposed.Identity.Route())
			if err != nil {
				return FlowInstanceSelection{}, err
			}
			return FlowInstanceSelection{}, flowInstanceOccupiedConflict(owner)
		}
		return FlowInstanceSelection{Proposed: proposed}, nil
	}
	observation, found, err := readFlowInstanceSelection(ctx, reader, request.Lookup, parentPrepared)
	if err != nil {
		return FlowInstanceSelection{}, err
	}
	if !found && observation.Valid() {
		return FlowInstanceSelection{}, fmt.Errorf("instance index returned an observation as absence")
	}
	if found {
		if err := observation.ValidateSelection(request.Lookup); err != nil {
			return FlowInstanceSelection{}, err
		}
		if request.Mode == contracts.FlowInputResolutionModeCreate {
			return FlowInstanceSelection{}, flowInstanceOccupiedConflict(observation.Owner())
		}
		instance, err := observation.WorkflowInstance()
		if err != nil {
			return FlowInstanceSelection{}, err
		}
		stage := ""
		if instance.StageDefined {
			stage = instance.CurrentState
		}
		if err := NewDeliveryTargetAvailability(stage, instance.Status, !instance.TerminatedAt.IsZero()).Validate(request.Lookup.Source(), request.Lookup.FlowID()); err != nil {
			return FlowInstanceSelection{}, err
		}
		return FlowInstanceSelection{Observation: observation}, nil
	}
	if request.Mode == contracts.FlowInputResolutionModeSelect {
		return FlowInstanceSelection{}, &WorkflowInstanceLookupMiss{RequestedKey: request.Lookup.FlowID()}
	}
	return prepareMissingFlowInstance(ctx, planner, request)
}

func selectPreparedFlowInstance(request FlowInstanceSelectionRequest) (*FlowInstanceActivationPlan, bool, error) {
	var selected *FlowInstanceActivationPlan
	parentPrepared := false
	for _, tree := range request.Prepared {
		for _, plan := range tree.ConstructionPlans() {
			if err := plan.Validate(); err != nil {
				return nil, false, err
			}
			if plan.Readiness.RunID != request.Lookup.RunID() || plan.Readiness.BundleHash != request.Lookup.SourceFact().BundleHash() {
				return nil, false, fmt.Errorf("prepared tree contradicts the selected source or run")
			}
			parentPrepared = parentPrepared || plan.Identity == request.Lookup.ParentIdentity()
			if plan.Identity.TemplateID != request.Lookup.FlowID() || plan.Instance.ParentFlowInstance != request.Lookup.ParentInstance() || plan.Instance.InstanceKey != request.Lookup.InstanceKey() {
				continue
			}
			if err := validateObservedInstanceSelection(request.Lookup, plan.Identity, plan.Instance); err != nil {
				return nil, false, err
			}
			if selected != nil {
				return nil, false, fmt.Errorf("prepared tree has ambiguous receiver occupancy")
			}
			copy := plan
			selected = &copy
		}
	}
	return selected, parentPrepared, nil
}

func readFlowInstanceSelection(ctx context.Context, reader FlowInstanceIndexReader, lookup FlowInstanceLookupRequest, parentPrepared bool) (FlowInstanceObservation, bool, error) {
	if !parentPrepared {
		return reader.LookupFlowInstance(ctx, lookup)
	}
	// A prospective parent is not persisted permission. Admit the bounded native
	// declaration inventory before declaring its dependent receiver absent.
	scope, err := NewFlowInstanceLookupScope(lookup.Source(), lookup.SourceFact(), lookup.RunID(), []string{lookup.FlowID()}, nil)
	if err != nil {
		return FlowInstanceObservation{}, false, err
	}
	observations, err := reader.ListFlowInstances(ctx, scope)
	if err != nil {
		return FlowInstanceObservation{}, false, err
	}
	var selected FlowInstanceObservation
	for _, observation := range observations {
		if observation.Identity().ParentRoute.FlowInstance != lookup.ParentInstance() || observation.InstanceKey() != lookup.InstanceKey() {
			continue
		}
		if err := observation.ValidateSelection(lookup); err != nil {
			return FlowInstanceObservation{}, false, err
		}
		if selected.Valid() {
			return FlowInstanceObservation{}, false, &FlowInstanceConstructionCorruption{RunID: lookup.RunID(), FlowID: lookup.FlowID(), Cause: fmt.Errorf("selector has ambiguous stored receivers")}
		}
		selected = observation
	}
	return selected, selected.Valid(), nil
}

func prepareMissingFlowInstance(ctx context.Context, planner FlowInstanceActivationPlanner, request FlowInstanceSelectionRequest) (FlowInstanceSelection, error) {
	if planner == nil || request.Constructor.Instance != (flowidentity.Instance{}) {
		return FlowInstanceSelection{}, fmt.Errorf("missing receiver requires R5.1 preparation, not a preselected creation identity")
	}
	lookup := request.Lookup
	constructor := request.Constructor
	if err := validateFlowInstanceLookupSource(constructor.ContractBundle, lookup.SourceFact(), lookup.RunID()); err != nil {
		return FlowInstanceSelection{}, err
	}
	if constructor.ContractBundle.WorkflowVersion() != lookup.Source().WorkflowVersion() {
		return FlowInstanceSelection{}, fmt.Errorf("constructor source contradicts the selected workflow version")
	}
	var err error
	if lookup.ParentIdentity() == (flowidentity.Instance{}) {
		constructor.Instance = flowidentity.Stored(lookup.Source(), lookup.FlowID(), lookup.RunID(), lookup.RunID(), lookup.RunID(), "")
	} else if flow, _ := lookup.Source().FlowSchemaByID(lookup.FlowID()); flow.Instance.Empty() {
		constructor.Instance, err = flowidentity.KeylessChild(lookup.Source(), lookup.ParentIdentity(), lookup.FlowID())
	} else {
		constructor.Instance, err = flowidentity.KeyedChild(lookup.Source(), lookup.ParentIdentity(), lookup.FlowID(), request.MissingInstanceID)
	}
	if err != nil {
		return FlowInstanceSelection{}, err
	}
	plan, err := planner.PrepareFlowInstanceActivation(ctx, constructor)
	if err != nil {
		return FlowInstanceSelection{}, err
	}
	if err := plan.Validate(); err != nil {
		return FlowInstanceSelection{}, err
	}
	if err := validateObservedInstanceSelection(lookup, plan.Identity, plan.Instance); err != nil {
		return FlowInstanceSelection{}, err
	}
	if plan.Readiness.RunID != lookup.RunID() || plan.Readiness.BundleHash != lookup.SourceFact().BundleHash() {
		return FlowInstanceSelection{}, fmt.Errorf("prepared construction contradicts its selected source or run")
	}
	return FlowInstanceSelection{Activation: &plan}, nil
}

func flowInstanceOccupiedConflict(owner flowidentity.RunScopedFlowInstance) error {
	return &FlowInstanceActivationConflict{Owner: owner, Cause: failures.New(
		failures.ClassConflictingDuplicate, "flow_instance_already_exists", "flow-instance-selection", "prepare",
		map[string]any{"flow_instance": owner.Route.InstancePath},
	)}
}
