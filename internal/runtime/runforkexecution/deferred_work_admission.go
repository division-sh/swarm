package runforkexecution

import (
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
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
}

func admitSelectedContractDeferredWork(plan store.RunForkPlan, source semanticview.Source) (selectedContractDeferredWorkAdmission, error) {
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

	capabilities, revisionTimerHistory := selectedContractDeferredWorkCapabilities(plan, source)
	if len(capabilities) > 0 {
		detailCode := selectedContractDeferredWorkOwnerUnavailable
		if revisionTimerHistory {
			detailCode = store.RunForkBlockerTimerHistoryUnproven
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
		owner:           store.RunForkSelectedContractDeferredWorkAdmissionOwner,
		sourceRunID:     sourceRunID,
		forkEventID:     forkEventID,
		workflowName:    workflowName,
		workflowVersion: workflowVersion,
	}, nil
}

func (a selectedContractDeferredWorkAdmission) validate(sourceRunID, forkEventID string, source semanticview.Source) error {
	if a.owner != store.RunForkSelectedContractDeferredWorkAdmissionOwner {
		return fmt.Errorf("selected-contract execution requires %s", store.RunForkSelectedContractDeferredWorkAdmissionOwner)
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
	if capabilities, _ := selectedContractDeferredWorkCapabilities(store.RunForkPlan{}, source); len(capabilities) > 0 {
		return fmt.Errorf("selected-contract deferred-work admission source now declares unsupported capabilities: %s", strings.Join(capabilities, ","))
	}
	return nil
}

func selectedContractDeferredWorkCapabilities(plan store.RunForkPlan, source semanticview.Source) ([]string, bool) {
	capabilities := make([]string, 0, 4)
	revisionTimerHistory := false
	for _, blocker := range plan.UnsupportedBlockers {
		if strings.TrimSpace(blocker.Code) == store.RunForkBlockerTimerHistoryUnproven {
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
	for flowID := range source.FlowSchemaEntries() {
		for _, pin := range source.FlowInputEventPins(flowID) {
			if strings.TrimSpace(pin.Resolution.Mode) != runtimecontracts.FlowInputResolutionModeCreate {
				continue
			}
			if len(semanticview.ResolvedCompositionConnectsTo(source, flowID, pin.PinName())) > 0 {
				return true
			}
		}
	}
	for nodeID := range source.NodeEntries() {
		for _, handler := range source.NodeEventHandlers(nodeID) {
			if selectedContractHandlerCreatesDynamicFlow(handler) {
				return true
			}
		}
	}
	return false
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
