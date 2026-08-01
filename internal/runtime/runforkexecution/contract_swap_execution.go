package runforkexecution

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

type HistoricalReplayContractSwapBootResumeRequest struct {
	SelectedExecutionAdmission runfork.RunForkSelectedContractExecutionAdmission
	ContractSwapAdmission      runfork.RunForkContractSwapBootResumeAdmission
	HistoricalReplayAdmission  runfork.RunForkHistoricalReplayExecutionAdmission
	HistoricalReplayExecution  runfork.RunForkHistoricalReplayExecution
	RouteRecovery              *runfork.RunForkSelectedContractRouteRecovery
}

func BuildHistoricalReplayContractSwapBootResumeExecution(req HistoricalReplayContractSwapBootResumeRequest) (runfork.RunForkHistoricalReplayContractSwapBootResume, error) {
	selectedAdmission := req.SelectedExecutionAdmission
	if err := validateContractSwapSelectedExecutionAdmission(selectedAdmission); err != nil {
		return runfork.RunForkHistoricalReplayContractSwapBootResume{}, fmt.Errorf("contract-swap historical replay execution selected prerequisite: %w", err)
	}
	contractSwapAdmission := req.ContractSwapAdmission
	if err := validateContractSwapExecutionAdmission(selectedAdmission, contractSwapAdmission); err != nil {
		return runfork.RunForkHistoricalReplayContractSwapBootResume{}, err
	}
	historicalAdmission := req.HistoricalReplayAdmission
	if err := validateContractSwapHistoricalReplayAdmission(selectedAdmission, contractSwapAdmission, historicalAdmission); err != nil {
		return runfork.RunForkHistoricalReplayContractSwapBootResume{}, err
	}
	if req.RouteRecovery == nil {
		return runfork.RunForkHistoricalReplayContractSwapBootResume{}, fmt.Errorf("contract-swap historical replay execution requires %s route recovery", runfork.RunForkSelectedContractRouteRecoveryOwner)
	}
	if err := validateContractSwapRouteRecovery(selectedAdmission, *req.RouteRecovery); err != nil {
		return runfork.RunForkHistoricalReplayContractSwapBootResume{}, fmt.Errorf("contract-swap historical replay execution route recovery prerequisite: %w", err)
	}
	historicalExecution := req.HistoricalReplayExecution
	if err := validateContractSwapHistoricalReplayExecution(historicalAdmission, historicalExecution); err != nil {
		return runfork.RunForkHistoricalReplayContractSwapBootResume{}, err
	}
	if err := validateContractSwapFactMatrix(historicalExecution.FactAdmissions); err != nil {
		return runfork.RunForkHistoricalReplayContractSwapBootResume{}, err
	}
	work, err := contractSwapExecutableWork(historicalExecution.DeliveryEventReplayWork, selectedAdmission.RecipientPlanning)
	if err != nil {
		return runfork.RunForkHistoricalReplayContractSwapBootResume{}, err
	}

	return runfork.RunForkHistoricalReplayContractSwapBootResume{
		Owner:                                   runfork.RunForkHistoricalReplayContractSwapBootResumeOwner,
		ParentHistoricalReplayExecutionOwner:    historicalExecution.Owner,
		HistoricalReplayExecutionAdmissionOwner: historicalAdmission.Owner,
		ContractSwapAdmissionOwner:              contractSwapAdmission.Owner,
		SelectedExecutionAdmissionOwner:         selectedAdmission.Owner,
		SelectedBindingOwner:                    selectedAdmission.ContractBindingOwner,
		RouteTopologyOwner:                      selectedAdmission.RouteTopology.Owner,
		RouteRecoveryOwner:                      req.RouteRecovery.Owner,
		RuntimeRouteRecoveryOwner:               req.RouteRecovery.RuntimeRecoveryOwner,
		RecipientPlanningOwner:                  selectedAdmission.RecipientPlanning.Owner,
		ForkRunID:                               selectedAdmission.ForkRunID,
		SourceRunID:                             selectedAdmission.SourceRunID,
		ForkEventID:                             selectedAdmission.ForkEventID,
		ContractSelection:                       selectedAdmission.ContractSelection,
		ClosureLevel:                            "contract_swap_boot_resume_delivery_event_replay_ready_first_slice",
		DeliveryEventReplayReady:                true,
		ExecutableWork:                          work,
		FactAdmissions:                          append([]runfork.RunForkHistoricalReplayFactAdmission(nil), historicalExecution.FactAdmissions...),
		RequiredConsumers:                       contractSwapExecutionRequiredConsumers(),
		BlockedSiblings:                         contractSwapExecutionBlockedSiblings(historicalExecution.BlockedSiblings),
		InvalidPaths:                            contractSwapExecutionInvalidPaths(historicalExecution.InvalidPaths),
	}, nil
}

func validateContractSwapExecutionAdmission(selectedAdmission runfork.RunForkSelectedContractExecutionAdmission, admission runfork.RunForkContractSwapBootResumeAdmission) error {
	if strings.TrimSpace(admission.Owner) != runfork.RunForkContractSwapBootResumeAdmissionOwner {
		return fmt.Errorf("contract-swap historical replay execution requires %s; got %q", runfork.RunForkContractSwapBootResumeAdmissionOwner, admission.Owner)
	}
	if !admission.NonMutating || admission.BootResumeSupported {
		return fmt.Errorf("contract-swap historical replay execution consumes non-mutating contract-swap admission only")
	}
	if strings.TrimSpace(admission.FutureExecutionOwner) != runfork.RunForkHistoricalReplayContractSwapBootResumeOwner {
		return fmt.Errorf("contract-swap historical replay execution requires future owner %s; got %q", runfork.RunForkHistoricalReplayContractSwapBootResumeOwner, admission.FutureExecutionOwner)
	}
	if strings.TrimSpace(admission.SelectedExecutionAdmissionOwner) != selectedAdmission.Owner ||
		strings.TrimSpace(admission.SelectedBindingOwner) != selectedAdmission.ContractBindingOwner ||
		strings.TrimSpace(admission.ForkRunID) != strings.TrimSpace(selectedAdmission.ForkRunID) ||
		strings.TrimSpace(admission.SourceRunID) != strings.TrimSpace(selectedAdmission.SourceRunID) ||
		strings.TrimSpace(admission.ForkEventID) != strings.TrimSpace(selectedAdmission.ForkEventID) {
		return fmt.Errorf("contract-swap historical replay execution admission identity does not match selected execution admission")
	}
	return nil
}

func validateContractSwapHistoricalReplayAdmission(
	selectedAdmission runfork.RunForkSelectedContractExecutionAdmission,
	contractSwapAdmission runfork.RunForkContractSwapBootResumeAdmission,
	admission runfork.RunForkHistoricalReplayExecutionAdmission,
) error {
	if strings.TrimSpace(admission.Owner) != runfork.RunForkHistoricalReplayExecutionAdmissionOwner {
		return fmt.Errorf("contract-swap historical replay execution requires %s; got %q", runfork.RunForkHistoricalReplayExecutionAdmissionOwner, admission.Owner)
	}
	if !admission.NonMutating || admission.ExecutionSupported {
		return fmt.Errorf("contract-swap historical replay execution consumes non-mutating historical replay admission only")
	}
	if strings.TrimSpace(admission.FutureExecutionOwner) != runfork.RunForkHistoricalReplayExecutionOwner {
		return fmt.Errorf("contract-swap historical replay execution requires parent future owner %s; got %q", runfork.RunForkHistoricalReplayExecutionOwner, admission.FutureExecutionOwner)
	}
	if strings.TrimSpace(admission.ContractSwapAdmissionOwner) != contractSwapAdmission.Owner ||
		strings.TrimSpace(admission.SelectedExecutionAdmissionOwner) != selectedAdmission.Owner ||
		strings.TrimSpace(admission.SelectedBindingOwner) != selectedAdmission.ContractBindingOwner ||
		strings.TrimSpace(admission.RouteTopologyOwner) != runfork.RunForkSelectedContractRouteTopologyOwner ||
		strings.TrimSpace(admission.RecipientPlanningOwner) != runfork.RunForkSelectedContractRecipientPlanningOwner ||
		strings.TrimSpace(admission.RouteRecoveryOwner) != runfork.RunForkSelectedContractRoutePersistenceOwner ||
		strings.TrimSpace(admission.RuntimeRouteRecoveryOwner) != runfork.RunForkSelectedContractRouteRecoveryOwner {
		return fmt.Errorf("contract-swap historical replay execution historical admission owner consumption is incomplete")
	}
	if strings.TrimSpace(admission.ForkRunID) != strings.TrimSpace(selectedAdmission.ForkRunID) ||
		strings.TrimSpace(admission.SourceRunID) != strings.TrimSpace(selectedAdmission.SourceRunID) ||
		strings.TrimSpace(admission.ForkEventID) != strings.TrimSpace(selectedAdmission.ForkEventID) {
		return fmt.Errorf("contract-swap historical replay execution historical admission identity mismatch")
	}
	return nil
}

func validateContractSwapHistoricalReplayExecution(admission runfork.RunForkHistoricalReplayExecutionAdmission, execution runfork.RunForkHistoricalReplayExecution) error {
	if strings.TrimSpace(execution.Owner) != runfork.RunForkHistoricalReplayExecutionOwner {
		return fmt.Errorf("contract-swap historical replay execution requires parent owner %s; got %q", runfork.RunForkHistoricalReplayExecutionOwner, execution.Owner)
	}
	if strings.TrimSpace(execution.AdmissionOwner) != admission.Owner ||
		strings.TrimSpace(execution.ReplayResumeAdmissionOwner) != admission.ReplayResumeAdmissionOwner ||
		strings.TrimSpace(execution.ForkRunID) != strings.TrimSpace(admission.ForkRunID) ||
		strings.TrimSpace(execution.SourceRunID) != strings.TrimSpace(admission.SourceRunID) ||
		strings.TrimSpace(execution.ForkEventID) != strings.TrimSpace(admission.ForkEventID) {
		return fmt.Errorf("contract-swap historical replay execution parent execution identity mismatch")
	}
	if !execution.FullReplayUnsupported {
		return fmt.Errorf("contract-swap historical replay execution first slice cannot consume broader source-run replay execution")
	}
	if !execution.DeliveryEventReplayReady ||
		execution.EventDeliveriesAdmission.Fact != runfork.RunForkHistoricalReplayFactEventDeliveries ||
		execution.EventDeliveriesAdmission.Admission != runfork.RunForkHistoricalReplayAdmissionExecutableForkWork ||
		len(execution.DeliveryEventReplayWork) == 0 {
		return fmt.Errorf("contract-swap historical replay execution requires owner-authorized delivery_event_replay_ready work")
	}
	return nil
}

func validateContractSwapFactMatrix(admissions []runfork.RunForkHistoricalReplayFactAdmission) error {
	for _, admission := range admissions {
		if strings.TrimSpace(admission.Fact) == runfork.RunForkHistoricalReplayFactEventDeliveries {
			if strings.TrimSpace(admission.Admission) != runfork.RunForkHistoricalReplayAdmissionExecutableForkWork {
				return fmt.Errorf("contract-swap historical replay execution requires event_deliveries executable fork work")
			}
			continue
		}
		if strings.TrimSpace(admission.Admission) == runfork.RunForkHistoricalReplayAdmissionExecutableForkWork {
			return fmt.Errorf("contract-swap historical replay execution cannot execute unsupported fact family %s", admission.Fact)
		}
		if strings.TrimSpace(admission.Admission) == runfork.RunForkHistoricalReplayAdmissionFailClosedBlocker {
			return fmt.Errorf("contract-swap historical replay execution blocked by fact family %s: %s", admission.Fact, admission.BlockerCode)
		}
	}
	return nil
}

func contractSwapExecutableWork(
	historicalWork []runfork.RunForkHistoricalReplayExecutableWork,
	planning *runfork.RunForkSelectedContractRecipientPlanning,
) ([]runfork.RunForkHistoricalReplayContractSwapWork, error) {
	if planning == nil || strings.TrimSpace(planning.Owner) != runfork.RunForkSelectedContractRecipientPlanningOwner {
		return nil, fmt.Errorf("contract-swap historical replay execution requires %s", runfork.RunForkSelectedContractRecipientPlanningOwner)
	}
	planBySourceEvent := map[string]runfork.RunForkSelectedContractRecipientPlanEvent{}
	for _, event := range planning.RecipientPlanEvents {
		sourceEventID := strings.TrimSpace(event.SourceEventID)
		if sourceEventID == "" {
			continue
		}
		if len(event.Recipients) == 0 {
			return nil, fmt.Errorf("contract-swap historical replay execution source event %s has no selected recipients", sourceEventID)
		}
		planBySourceEvent[sourceEventID] = event
	}

	byEvent := map[string]*runfork.RunForkHistoricalReplayContractSwapWork{}
	seenDeliveries := map[string]struct{}{}
	for _, item := range historicalWork {
		if item.Fact != runfork.RunForkHistoricalReplayFactEventDeliveries {
			return nil, fmt.Errorf("contract-swap historical replay execution cannot consume historical work fact %q", item.Fact)
		}
		sourceEventID := strings.TrimSpace(item.SourceEventID)
		sourceDeliveryID := strings.TrimSpace(item.SourceDeliveryID)
		if sourceEventID == "" || sourceDeliveryID == "" {
			return nil, fmt.Errorf("contract-swap historical replay execution requires source event and delivery lineage identity")
		}
		if _, exists := seenDeliveries[sourceDeliveryID]; exists {
			return nil, fmt.Errorf("contract-swap historical replay execution duplicate source delivery %s", sourceDeliveryID)
		}
		seenDeliveries[sourceDeliveryID] = struct{}{}
		plan, ok := planBySourceEvent[sourceEventID]
		if !ok {
			return nil, fmt.Errorf("contract-swap historical replay execution has no selected recipient plan for source event %s", sourceEventID)
		}
		work, ok := byEvent[sourceEventID]
		if !ok {
			work = &runfork.RunForkHistoricalReplayContractSwapWork{
				Fact:               runfork.RunForkHistoricalReplayFactEventDeliveries,
				SourceEventID:      sourceEventID,
				EventName:          strings.TrimSpace(plan.EventName),
				SelectedRecipients: append([]runfork.RunForkContractFrontierRecipient(nil), plan.Recipients...),
				Classification:     strings.TrimSpace(item.Classification),
				ReasonCode:         strings.TrimSpace(item.ReasonCode),
				SourceDeliveryIDs:  []string{},
			}
			byEvent[sourceEventID] = work
		}
		if strings.TrimSpace(work.EventName) != strings.TrimSpace(plan.EventName) {
			return nil, fmt.Errorf("contract-swap historical replay execution event name mismatch for source event %s", sourceEventID)
		}
		work.SourceDeliveryIDs = append(work.SourceDeliveryIDs, sourceDeliveryID)
	}
	out := make([]runfork.RunForkHistoricalReplayContractSwapWork, 0, len(byEvent))
	for _, work := range byEvent {
		sort.Strings(work.SourceDeliveryIDs)
		out = append(out, *work)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].SourceEventID < out[j].SourceEventID
	})
	if len(out) == 0 {
		return nil, fmt.Errorf("contract-swap historical replay execution requires executable selected work")
	}
	return out, nil
}

func contractSwapExecutionRequiredConsumers() []runfork.RunForkSelectedContractExecutionBoundary {
	return []runfork.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "historical_replay_execution",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkHistoricalReplayExecutionOwner,
			Reason:      "contract-swap boot/resume consumes owner-authorized historical replay work before mutation",
		},
		{
			Concept:     "selected_recipient_planning",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractRecipientPlanningOwner,
			Reason:      "selected recipient planning, not source delivery subscribers, owns selected-fork recipient truth",
		},
		{
			Concept:     "eventbus_publish",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       "internal/runtime/bus.EventBus.Publish",
			Reason:      "fork-local event, delivery, receipt, and follow-up writes must pass through normal selected-contract publish and pipeline execution",
		},
	}
}

func contractSwapExecutionBlockedSiblings(items []runfork.RunForkSelectedContractExecutionBoundary) []runfork.RunForkSelectedContractExecutionBoundary {
	out := append([]runfork.RunForkSelectedContractExecutionBoundary(nil), items...)
	out = append(out,
		runfork.RunForkSelectedContractExecutionBoundary{
			Concept:     "full_historical_replay_resume",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       runfork.RunForkHistoricalReplayExecutionOwner,
			Reason:      "this child closes only selected-contract execution of delivery_event_replay_ready work; full #564 replay/resume remains open",
		},
		runfork.RunForkSelectedContractExecutionBoundary{
			Concept:     "timers_sessions_turns_audits_non_agent_restart_api",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "unsupported source fact families remain fail-closed or split siblings and are not silently replayed",
		},
	)
	return out
}

func contractSwapExecutionInvalidPaths(items []runfork.RunForkSelectedContractExecutionBoundary) []runfork.RunForkSelectedContractExecutionBoundary {
	out := append([]runfork.RunForkSelectedContractExecutionBoundary(nil), items...)
	out = append(out,
		runfork.RunForkSelectedContractExecutionBoundary{
			Concept:     "source_subscriber_as_selected_recipient",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "source delivery subscriber identity is lineage only; selected recipient planning owns fork delivery recipients",
		},
		runfork.RunForkSelectedContractExecutionBoundary{
			Concept:     "store_delivery_event_replay_as_contract_swap_owner",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "the generic delivery replay writer preserves source subscribers and cannot own selected-contract boot/resume",
		},
	)
	return out
}

func contractSwapBootResumeSourceEvents(execution runfork.RunForkHistoricalReplayContractSwapBootResume) []string {
	out := make([]string, 0, len(execution.ExecutableWork))
	for _, item := range execution.ExecutableWork {
		if eventID := strings.TrimSpace(item.SourceEventID); eventID != "" {
			out = append(out, eventID)
		}
	}
	return out
}
