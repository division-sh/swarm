package runforkexecution

import (
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const selectedContractDeferredWorkOwnerUnavailable = "selected_contract_deferred_work_owner_unavailable"

const (
	selectedContractDeferredWorkRevisionTimerHistory = "revision_timer_history"
	selectedContractDeferredWorkWorkflowTimer        = "workflow_timer"
	selectedContractDeferredWorkWorkflowJoinTimeout  = "workflow_join_timeout"
	selectedContractDeferredWorkDynamicFlowCreation  = "dynamic_flow_instance_creation"
)

type selectedContractDeferredWorkAdmission struct {
	owner           string
	sourceRunID     string
	forkEventID     string
	workflowName    string
	workflowVersion string
	fanOutPlanRefs  []runtimecontracts.FanOutPlanRef
}

func admitSelectedContractDeferredWork(plan runfork.RunForkPlan, source semanticview.Source) (selectedContractDeferredWorkAdmission, error) {
	sourceRunID := strings.TrimSpace(plan.SourceRunID)
	forkEventID := strings.TrimSpace(plan.ForkPoint.EventID)
	if !validSelectedContractDeferredWorkCoordinates(sourceRunID, forkEventID) {
		return selectedContractDeferredWorkAdmission{}, fmt.Errorf("selected-contract deferred-work admission requires exact source run and fork event coordinates")
	}
	if source == nil {
		return selectedContractDeferredWorkAdmission{}, fmt.Errorf("selected-contract deferred-work admission requires selected semantic source")
	}
	workflowName := strings.TrimSpace(source.WorkflowName())
	workflowVersion := strings.TrimSpace(source.WorkflowVersion())
	if workflowName == "" || workflowVersion == "" {
		return selectedContractDeferredWorkAdmission{}, fmt.Errorf("selected-contract deferred-work admission requires workflow name and version")
	}
	fanOutPlanRefs, err := admitSelectedContractFanOutPlans(plan, source)
	if err != nil {
		return selectedContractDeferredWorkAdmission{}, err
	}

	capabilities, revisionTimerHistory := selectedContractDeferredWorkCapabilities(plan, source)
	if len(capabilities) > 0 {
		detailCode := selectedContractDeferredWorkOwnerUnavailable
		if revisionTimerHistory {
			detailCode = runfork.RunForkBlockerTimerHistoryUnproven
		}
		return selectedContractDeferredWorkAdmission{}, runtimefailures.New(
			runtimefailures.ClassDependencyUnavailable,
			detailCode,
			"selected-contract-run-fork",
			"admit-deferred-work-ownership",
			map[string]any{"capabilities": capabilities},
		)
	}
	return selectedContractDeferredWorkAdmission{
		owner:           runfork.RunForkSelectedContractDeferredWorkAdmissionOwner,
		sourceRunID:     sourceRunID,
		forkEventID:     forkEventID,
		workflowName:    workflowName,
		workflowVersion: workflowVersion,
		fanOutPlanRefs:  fanOutPlanRefs,
	}, nil
}

func (a selectedContractDeferredWorkAdmission) validate(sourceRunID, forkEventID string, source semanticview.Source) error {
	if a.owner != runfork.RunForkSelectedContractDeferredWorkAdmissionOwner {
		return fmt.Errorf("selected-contract execution requires %s", runfork.RunForkSelectedContractDeferredWorkAdmissionOwner)
	}
	sourceRunID = strings.TrimSpace(sourceRunID)
	forkEventID = strings.TrimSpace(forkEventID)
	if a.sourceRunID != sourceRunID || a.forkEventID != forkEventID || !validSelectedContractDeferredWorkCoordinates(sourceRunID, forkEventID) {
		return fmt.Errorf("selected-contract deferred-work admission coordinates do not match selected source")
	}
	if source == nil {
		return fmt.Errorf("selected-contract deferred-work admission requires selected semantic source")
	}
	if a.workflowName != strings.TrimSpace(source.WorkflowName()) || a.workflowVersion != strings.TrimSpace(source.WorkflowVersion()) {
		return fmt.Errorf("selected-contract deferred-work admission workflow identity does not match selected source")
	}
	wantFanOutPlanRefs, err := selectedContractCompiledFanOutPlans(source)
	if err != nil {
		return err
	}
	if len(a.fanOutPlanRefs) > 0 {
		for _, ref := range a.fanOutPlanRefs {
			if want, ok := wantFanOutPlanRefs[ref.ElementRef]; !ok || want != ref {
				return fmt.Errorf("selected-contract deferred-work fan-out proof no longer matches selected source")
			}
		}
	}
	if capabilities, _ := selectedContractDeferredWorkCapabilities(runfork.RunForkPlan{}, source); len(capabilities) > 0 {
		return fmt.Errorf("selected-contract deferred-work admission source now declares unsupported capabilities: %s", strings.Join(capabilities, ","))
	}
	return nil
}

func admitSelectedContractFanOutPlans(plan runfork.RunForkPlan, source semanticview.Source) ([]runtimecontracts.FanOutPlanRef, error) {
	if err := runfork.ValidateFanOutPendingReplayAdmission(plan); err != nil {
		return nil, err
	}
	compiled, err := selectedContractCompiledFanOutPlans(source)
	if err != nil {
		return nil, err
	}
	if len(plan.FanOutObligations) == 0 {
		return nil, nil
	}
	refs := make([]runtimecontracts.FanOutPlanRef, 0, len(plan.FanOutObligations))
	seen := make(map[runtimecontracts.FanOutElementRef]struct{}, len(plan.FanOutObligations))
	for _, obligation := range plan.FanOutObligations {
		sourceRef := obligation.Intent.Request.PlanRef
		selectedRef, ok := compiled[sourceRef.ElementRef]
		if !ok {
			return nil, fmt.Errorf("selected contract is missing pending fan_out element %s/%s", sourceRef.ElementRef.PackageKey, sourceRef.ElementRef.ElementID)
		}
		if selectedRef.SemanticDigest != sourceRef.SemanticDigest {
			return nil, fmt.Errorf("selected contract fan_out element %s/%s changed semantic digest", sourceRef.ElementRef.PackageKey, sourceRef.ElementRef.ElementID)
		}
		if obligation.Barrier != nil {
			if err := obligation.Barrier.Validate(); err != nil {
				return nil, fmt.Errorf("selected contract fan_out barrier: %w", err)
			}
			if obligation.Barrier.Registration.IntentKey != obligation.Intent.Request.Key {
				return nil, fmt.Errorf("selected contract fan_out barrier disagrees with fixed intent")
			}
			sourceJoin, _ := obligation.Barrier.Registration.Handle.JoinRef()
			selectedJoin, err := timeridentity.NewFanOutDeliveryJoinRef(
				sourceJoin.Node(), sourceJoin.HandlerEvent(), sourceJoin.JoinID(),
				selectedRef.ElementRef.PackageKey, selectedRef.ElementRef.ElementID,
				selectedRef.BundleHash, selectedRef.SemanticDigest,
			)
			if err != nil {
				return nil, err
			}
			selectedJoin, err = selectedJoin.BindFanOutIntent(obligation.Intent.Request.Key.TriggeringDeliveryID, sourceJoin.Generation())
			if err != nil {
				return nil, err
			}
			if _, ok := semanticview.WorkflowJoinPlanForRef(source, selectedJoin.Declaration()); !ok {
				return nil, fmt.Errorf("selected contract changed fan_out delivery join declaration %s/%s", sourceJoin.NodeID(), sourceJoin.JoinID())
			}
		}
		if _, duplicate := seen[selectedRef.ElementRef]; duplicate {
			continue
		}
		seen[selectedRef.ElementRef] = struct{}{}
		refs = append(refs, selectedRef)
	}
	return refs, nil
}

func selectedContractCompiledFanOutPlans(source semanticview.Source) (map[runtimecontracts.FanOutElementRef]runtimecontracts.FanOutPlanRef, error) {
	if source == nil {
		return nil, fmt.Errorf("selected-contract fan-out admission requires selected semantic source")
	}
	out := make(map[runtimecontracts.FanOutElementRef]runtimecontracts.FanOutPlanRef)
	for _, plan := range source.FanOutPlans() {
		if prior, duplicate := out[plan.Ref.ElementRef]; duplicate && prior != plan.Ref {
			return nil, fmt.Errorf("selected contract has contradictory fan_out plan for %s/%s", plan.Ref.ElementRef.PackageKey, plan.Ref.ElementRef.ElementID)
		}
		out[plan.Ref.ElementRef] = plan.Ref
	}
	return out, nil
}

func selectedContractDeferredWorkCapabilities(plan runfork.RunForkPlan, source semanticview.Source) ([]string, bool) {
	capabilities := make([]string, 0, 4)
	revisionTimerHistory := false
	for _, blocker := range plan.UnsupportedBlockers {
		if strings.TrimSpace(blocker.Code) == runfork.RunForkBlockerTimerHistoryUnproven {
			revisionTimerHistory = true
			capabilities = append(capabilities, selectedContractDeferredWorkRevisionTimerHistory)
			break
		}
	}
	if source != nil && len(source.WorkflowTimers()) > 0 {
		capabilities = append(capabilities, selectedContractDeferredWorkWorkflowTimer)
	}
	if source != nil {
		for _, join := range source.WorkflowJoins() {
			if join.Spec.TimeoutFound || strings.TrimSpace(join.Spec.Timeout.After) != "" {
				capabilities = append(capabilities, selectedContractDeferredWorkWorkflowJoinTimeout)
				break
			}
		}
		if selectedContractSourceCanCreateDynamicFlow(source) {
			capabilities = append(capabilities, selectedContractDeferredWorkDynamicFlowCreation)
		}
	}
	sort.Strings(capabilities)
	return capabilities, revisionTimerHistory
}

func selectedContractSourceCanCreateDynamicFlow(source semanticview.Source) bool {
	if source == nil {
		return false
	}
	graph := runtimepinrouting.CompileConnectGraph(source)
	for flowID := range source.FlowSchemaEntries() {
		for _, pin := range source.FlowInputEventPins(flowID) {
			if !selectedContractFlowInputResolutionRequiresDynamicFlowOwner(pin.Resolution().Mode) {
				continue
			}
			for _, edge := range graph.Edges() {
				if edge.Consumer().MatchesInputPin(strings.TrimSpace(flowID), pin) {
					return true
				}
			}
		}
	}
	for _, record := range source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err != nil {
			continue
		}
		for _, handler := range source.ExecutableNodeEventHandlers(node) {
			if selectedContractHandlerCreatesDynamicFlow(handler) {
				return true
			}
		}
	}
	return false
}

func selectedContractFlowInputResolutionRequiresDynamicFlowOwner(mode runtimecontracts.FlowInputResolutionMode) bool {
	switch mode {
	case runtimecontracts.FlowInputResolutionModeCreate,
		runtimecontracts.FlowInputResolutionModeSelectOrCreate:
		return true
	case runtimecontracts.FlowInputResolutionModeSelect,
		runtimecontracts.FlowInputResolutionModeFanIn,
		runtimecontracts.FlowInputResolutionModeFanOut,
		runtimecontracts.FlowInputResolutionModeReply:
		return false
	case runtimecontracts.FlowInputResolutionModeNone:
		return false
	default:
		// Invalid or newly introduced modes cannot silently bypass the selected
		// container's missing long-lived dynamic-flow owner.
		return true
	}
}

func selectedContractHandlerCreatesDynamicFlow(handler runtimecontracts.SystemNodeEventHandler) bool {
	creates := func(action runtimecontracts.ActionSpec) bool {
		return runtimecontracts.NormalizeHandlerActionID(action.ID) == "create_flow_instance"
	}
	if creates(handler.Action) {
		return true
	}
	for _, rules := range [][]runtimecontracts.HandlerRuleEntry{handler.Rules, handler.OnComplete} {
		for _, rule := range rules {
			if creates(rule.Action) {
				return true
			}
		}
	}
	if handler.Join != nil {
		return creates(handler.Join.OnComplete.Action) || creates(handler.Join.Timeout.Outcome.Action)
	}
	return false
}

func validSelectedContractDeferredWorkCoordinates(sourceRunID, forkEventID string) bool {
	if _, err := uuid.Parse(strings.TrimSpace(sourceRunID)); err != nil {
		return false
	}
	if _, err := uuid.Parse(strings.TrimSpace(forkEventID)); err != nil {
		return false
	}
	return true
}
