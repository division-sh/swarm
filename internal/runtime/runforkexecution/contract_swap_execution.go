package runforkexecution

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/store"
)

type HistoricalReplayContractSwapBootResumeRequest struct {
	SelectedExecutionAdmission store.RunForkSelectedContractExecutionAdmission
	ContractSwapAdmission      store.RunForkContractSwapBootResumeAdmission
	HistoricalReplayAdmission  store.RunForkHistoricalReplayExecutionAdmission
	HistoricalReplayExecution  store.RunForkHistoricalReplayExecution
	RouteRecovery              *store.RunForkSelectedContractRouteRecovery
}

func BuildHistoricalReplayContractSwapBootResumeExecution(req HistoricalReplayContractSwapBootResumeRequest) (store.RunForkHistoricalReplayContractSwapBootResume, error) {
	selectedAdmission := req.SelectedExecutionAdmission
	if err := validateContractSwapSelectedExecutionAdmission(selectedAdmission); err != nil {
		return store.RunForkHistoricalReplayContractSwapBootResume{}, fmt.Errorf("contract-swap historical replay execution selected prerequisite: %w", err)
	}
	contractSwapAdmission := req.ContractSwapAdmission
	if err := validateContractSwapExecutionAdmission(selectedAdmission, contractSwapAdmission); err != nil {
		return store.RunForkHistoricalReplayContractSwapBootResume{}, err
	}
	historicalAdmission := req.HistoricalReplayAdmission
	if err := validateContractSwapHistoricalReplayAdmission(selectedAdmission, contractSwapAdmission, historicalAdmission); err != nil {
		return store.RunForkHistoricalReplayContractSwapBootResume{}, err
	}
	if req.RouteRecovery == nil {
		return store.RunForkHistoricalReplayContractSwapBootResume{}, fmt.Errorf("contract-swap historical replay execution requires %s route recovery", store.RunForkSelectedContractRouteRecoveryOwner)
	}
	if err := validateContractSwapRouteRecovery(selectedAdmission, *req.RouteRecovery); err != nil {
		return store.RunForkHistoricalReplayContractSwapBootResume{}, fmt.Errorf("contract-swap historical replay execution route recovery prerequisite: %w", err)
	}
	historicalExecution := req.HistoricalReplayExecution
	if err := validateContractSwapHistoricalReplayExecution(historicalAdmission, historicalExecution); err != nil {
		return store.RunForkHistoricalReplayContractSwapBootResume{}, err
	}
	if err := validateContractSwapFactMatrix(historicalExecution.FactAdmissions); err != nil {
		return store.RunForkHistoricalReplayContractSwapBootResume{}, err
	}
	work, err := contractSwapExecutableWork(historicalExecution.DeliveryEventReplayWork, selectedAdmission.RecipientPlanning)
	if err != nil {
		return store.RunForkHistoricalReplayContractSwapBootResume{}, err
	}

	return store.RunForkHistoricalReplayContractSwapBootResume{
		Owner:                                   store.RunForkHistoricalReplayContractSwapBootResumeOwner,
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
		FactAdmissions:                          append([]store.RunForkHistoricalReplayFactAdmission(nil), historicalExecution.FactAdmissions...),
		RequiredConsumers:                       contractSwapExecutionRequiredConsumers(),
		BlockedSiblings:                         contractSwapExecutionBlockedSiblings(historicalExecution.BlockedSiblings),
		InvalidPaths:                            contractSwapExecutionInvalidPaths(historicalExecution.InvalidPaths),
	}, nil
}

func validateContractSwapExecutionAdmission(selectedAdmission store.RunForkSelectedContractExecutionAdmission, admission store.RunForkContractSwapBootResumeAdmission) error {
	if strings.TrimSpace(admission.Owner) != store.RunForkContractSwapBootResumeAdmissionOwner {
		return fmt.Errorf("contract-swap historical replay execution requires %s; got %q", store.RunForkContractSwapBootResumeAdmissionOwner, admission.Owner)
	}
	if !admission.NonMutating || admission.BootResumeSupported {
		return fmt.Errorf("contract-swap historical replay execution consumes non-mutating contract-swap admission only")
	}
	if strings.TrimSpace(admission.FutureExecutionOwner) != store.RunForkHistoricalReplayContractSwapBootResumeOwner {
		return fmt.Errorf("contract-swap historical replay execution requires future owner %s; got %q", store.RunForkHistoricalReplayContractSwapBootResumeOwner, admission.FutureExecutionOwner)
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
	selectedAdmission store.RunForkSelectedContractExecutionAdmission,
	contractSwapAdmission store.RunForkContractSwapBootResumeAdmission,
	admission store.RunForkHistoricalReplayExecutionAdmission,
) error {
	if strings.TrimSpace(admission.Owner) != store.RunForkHistoricalReplayExecutionAdmissionOwner {
		return fmt.Errorf("contract-swap historical replay execution requires %s; got %q", store.RunForkHistoricalReplayExecutionAdmissionOwner, admission.Owner)
	}
	if !admission.NonMutating || admission.ExecutionSupported {
		return fmt.Errorf("contract-swap historical replay execution consumes non-mutating historical replay admission only")
	}
	if strings.TrimSpace(admission.FutureExecutionOwner) != store.RunForkHistoricalReplayExecutionOwner {
		return fmt.Errorf("contract-swap historical replay execution requires parent future owner %s; got %q", store.RunForkHistoricalReplayExecutionOwner, admission.FutureExecutionOwner)
	}
	if strings.TrimSpace(admission.ContractSwapAdmissionOwner) != contractSwapAdmission.Owner ||
		strings.TrimSpace(admission.SelectedExecutionAdmissionOwner) != selectedAdmission.Owner ||
		strings.TrimSpace(admission.SelectedBindingOwner) != selectedAdmission.ContractBindingOwner ||
		strings.TrimSpace(admission.RouteTopologyOwner) != store.RunForkSelectedContractRouteTopologyOwner ||
		strings.TrimSpace(admission.RecipientPlanningOwner) != store.RunForkSelectedContractRecipientPlanningOwner ||
		strings.TrimSpace(admission.RouteRecoveryOwner) != store.RunForkSelectedContractRoutePersistenceOwner ||
		strings.TrimSpace(admission.RuntimeRouteRecoveryOwner) != store.RunForkSelectedContractRouteRecoveryOwner {
		return fmt.Errorf("contract-swap historical replay execution historical admission owner consumption is incomplete")
	}
	if strings.TrimSpace(admission.ForkRunID) != strings.TrimSpace(selectedAdmission.ForkRunID) ||
		strings.TrimSpace(admission.SourceRunID) != strings.TrimSpace(selectedAdmission.SourceRunID) ||
		strings.TrimSpace(admission.ForkEventID) != strings.TrimSpace(selectedAdmission.ForkEventID) {
		return fmt.Errorf("contract-swap historical replay execution historical admission identity mismatch")
	}
	return nil
}

func validateContractSwapHistoricalReplayExecution(admission store.RunForkHistoricalReplayExecutionAdmission, execution store.RunForkHistoricalReplayExecution) error {
	if strings.TrimSpace(execution.Owner) != store.RunForkHistoricalReplayExecutionOwner {
		return fmt.Errorf("contract-swap historical replay execution requires parent owner %s; got %q", store.RunForkHistoricalReplayExecutionOwner, execution.Owner)
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
		execution.EventDeliveriesAdmission.Fact != store.RunForkHistoricalReplayFactEventDeliveries ||
		execution.EventDeliveriesAdmission.Admission != store.RunForkHistoricalReplayAdmissionExecutableForkWork ||
		len(execution.DeliveryEventReplayWork) == 0 {
		return fmt.Errorf("contract-swap historical replay execution requires owner-authorized delivery_event_replay_ready work")
	}
	return nil
}

func validateContractSwapFactMatrix(admissions []store.RunForkHistoricalReplayFactAdmission) error {
	for _, admission := range admissions {
		if strings.TrimSpace(admission.Fact) == store.RunForkHistoricalReplayFactEventDeliveries {
			if strings.TrimSpace(admission.Admission) != store.RunForkHistoricalReplayAdmissionExecutableForkWork {
				return fmt.Errorf("contract-swap historical replay execution requires event_deliveries executable fork work")
			}
			continue
		}
		if strings.TrimSpace(admission.Admission) == store.RunForkHistoricalReplayAdmissionExecutableForkWork {
			return fmt.Errorf("contract-swap historical replay execution cannot execute unsupported fact family %s", admission.Fact)
		}
		if strings.TrimSpace(admission.Admission) == store.RunForkHistoricalReplayAdmissionFailClosedBlocker {
			return fmt.Errorf("contract-swap historical replay execution blocked by fact family %s: %s", admission.Fact, admission.BlockerCode)
		}
	}
	return nil
}

func contractSwapExecutableWork(
	historicalWork []store.RunForkHistoricalReplayExecutableWork,
	planning *store.RunForkSelectedContractRecipientPlanning,
) ([]store.RunForkHistoricalReplayContractSwapWork, error) {
	if planning == nil || strings.TrimSpace(planning.Owner) != store.RunForkSelectedContractRecipientPlanningOwner {
		return nil, fmt.Errorf("contract-swap historical replay execution requires %s", store.RunForkSelectedContractRecipientPlanningOwner)
	}
	planBySourceEvent := map[string]store.RunForkSelectedContractRecipientPlanEvent{}
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

	byEvent := map[string]*store.RunForkHistoricalReplayContractSwapWork{}
	seenDeliveries := map[string]struct{}{}
	for _, item := range historicalWork {
		if item.Fact != store.RunForkHistoricalReplayFactEventDeliveries {
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
			work = &store.RunForkHistoricalReplayContractSwapWork{
				Fact:               store.RunForkHistoricalReplayFactEventDeliveries,
				SourceEventID:      sourceEventID,
				EventName:          strings.TrimSpace(plan.EventName),
				SelectedRecipients: append([]store.RunForkContractFrontierRecipient(nil), plan.Recipients...),
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
	out := make([]store.RunForkHistoricalReplayContractSwapWork, 0, len(byEvent))
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

func contractSwapExecutionRequiredConsumers() []store.RunForkSelectedContractExecutionBoundary {
	return []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "historical_replay_execution",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       store.RunForkHistoricalReplayExecutionOwner,
			Reason:      "contract-swap boot/resume consumes owner-authorized historical replay work before mutation",
		},
		{
			Concept:     "selected_recipient_planning",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       store.RunForkSelectedContractRecipientPlanningOwner,
			Reason:      "selected recipient planning, not source delivery subscribers, owns selected-fork recipient truth",
		},
		{
			Concept:     "eventbus_publish",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       "internal/runtime/bus.EventBus.Publish",
			Reason:      "fork-local event, delivery, receipt, and follow-up writes must pass through normal selected-contract publish and pipeline execution",
		},
	}
}

func contractSwapExecutionBlockedSiblings(items []store.RunForkSelectedContractExecutionBoundary) []store.RunForkSelectedContractExecutionBoundary {
	out := append([]store.RunForkSelectedContractExecutionBoundary(nil), items...)
	out = append(out,
		store.RunForkSelectedContractExecutionBoundary{
			Concept:     "full_historical_replay_resume",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       store.RunForkHistoricalReplayExecutionOwner,
			Reason:      "this child closes only selected-contract execution of delivery_event_replay_ready work; full #564 replay/resume remains open",
		},
		store.RunForkSelectedContractExecutionBoundary{
			Concept:     "timers_sessions_turns_audits_non_agent_restart_api",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "unsupported source fact families remain fail-closed or split siblings and are not silently replayed",
		},
	)
	return out
}

func contractSwapExecutionInvalidPaths(items []store.RunForkSelectedContractExecutionBoundary) []store.RunForkSelectedContractExecutionBoundary {
	out := append([]store.RunForkSelectedContractExecutionBoundary(nil), items...)
	out = append(out,
		store.RunForkSelectedContractExecutionBoundary{
			Concept:     "source_subscriber_as_selected_recipient",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "source delivery subscriber identity is lineage only; selected recipient planning owns fork delivery recipients",
		},
		store.RunForkSelectedContractExecutionBoundary{
			Concept:     "store_delivery_event_replay_as_contract_swap_owner",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "the generic delivery replay writer preserves source subscribers and cannot own selected-contract boot/resume",
		},
	)
	return out
}

func contractSwapBootResumeSourceEvents(execution store.RunForkHistoricalReplayContractSwapBootResume) []string {
	out := make([]string, 0, len(execution.ExecutableWork))
	for _, item := range execution.ExecutableWork {
		if eventID := strings.TrimSpace(item.SourceEventID); eventID != "" {
			out = append(out, eventID)
		}
	}
	return out
}
