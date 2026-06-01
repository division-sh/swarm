package runforkexecution

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/store"
)

type ContractSwapBootResumeAdmissionRequest struct {
	SelectedExecutionAdmission store.RunForkSelectedContractExecutionAdmission
	ReplayResumeAdmission      store.RunForkReplayResumeAdmission
	RouteRecovery              *store.RunForkSelectedContractRouteRecovery
}

func BuildContractSwapBootResumeAdmission(req ContractSwapBootResumeAdmissionRequest) (store.RunForkContractSwapBootResumeAdmission, error) {
	selectedAdmission := req.SelectedExecutionAdmission
	if err := validateContractSwapSelectedExecutionAdmission(selectedAdmission); err != nil {
		return store.RunForkContractSwapBootResumeAdmission{}, err
	}
	replayAdmission := req.ReplayResumeAdmission
	if strings.TrimSpace(replayAdmission.Owner) != store.RunForkReplayResumeAdmissionOwner {
		return store.RunForkContractSwapBootResumeAdmission{}, fmt.Errorf("contract-swap boot/resume admission requires %s; got %q", store.RunForkReplayResumeAdmissionOwner, replayAdmission.Owner)
	}

	var routeRecoveryOwner, runtimeRouteRecoveryOwner string
	if req.RouteRecovery != nil {
		if err := validateContractSwapRouteRecovery(selectedAdmission, *req.RouteRecovery); err != nil {
			return store.RunForkContractSwapBootResumeAdmission{}, err
		}
		routeRecoveryOwner = req.RouteRecovery.Owner
		runtimeRouteRecoveryOwner = req.RouteRecovery.RuntimeRecoveryOwner
	}

	blockers := append([]store.RunForkUnsupportedBlocker(nil), selectedAdmission.UnsupportedBlockers...)
	for _, blocker := range replayAdmission.UnsupportedBlockers {
		blockers = appendRunForkUnsupportedBlocker(blockers, blocker)
	}
	blockers = appendRunForkUnsupportedBlocker(blockers, store.RunForkUnsupportedBlocker{
		Code:    store.RunForkBlockerContractSwapBootResumeAdmissionNonMutating,
		Message: "contract-swap boot/resume admission is non-mutating; full historical replay/resume remains separately gated",
	})
	if req.RouteRecovery == nil {
		blockers = appendRunForkUnsupportedBlocker(blockers, store.RunForkUnsupportedBlocker{
			Code:    store.RunForkBlockerContractSwapRouteRecoveryMissing,
			Message: "contract-swap boot/resume readiness requires fork-local selected route recovery evidence before future mutation",
		})
	}

	classifications := append([]store.RunForkReplayResumeDisposition(nil), replayAdmission.Dispositions...)
	classifications = append(classifications, store.RunForkReplayResumeDisposition{
		Fact:        store.RunForkReplayResumeFactContractSwap,
		Disposition: store.RunForkReplayResumeDispositionFailClosedBlocker,
		BlockerCode: store.RunForkBlockerContractSwapBootResumeAdmissionNonMutating,
		Message:     "contract-swap boot/resume readiness is classified by the canonical non-mutating admission owner; mutation is not authorized by this slice",
	})

	routeTopologyOwner := ""
	if selectedAdmission.RouteTopology != nil {
		routeTopologyOwner = selectedAdmission.RouteTopology.Owner
	}
	recipientPlanningOwner := ""
	if selectedAdmission.RecipientPlanning != nil {
		recipientPlanningOwner = selectedAdmission.RecipientPlanning.Owner
	}

	return store.RunForkContractSwapBootResumeAdmission{
		Owner:                           store.RunForkContractSwapBootResumeAdmissionOwner,
		NonMutating:                     true,
		BootResumeSupported:             false,
		FutureExecutionOwner:            store.RunForkHistoricalReplayContractSwapBootResumeOwner,
		ForkRunID:                       selectedAdmission.ForkRunID,
		SourceRunID:                     selectedAdmission.SourceRunID,
		ForkEventID:                     selectedAdmission.ForkEventID,
		ContractSelection:               selectedAdmission.ContractSelection,
		SelectedBindingOwner:            selectedAdmission.ContractBindingOwner,
		SelectedExecutionAdmissionOwner: selectedAdmission.Owner,
		ReplayResumeAdmissionOwner:      replayAdmission.Owner,
		RouteTopologyOwner:              routeTopologyOwner,
		RouteRecoveryOwner:              routeRecoveryOwner,
		RuntimeRouteRecoveryOwner:       runtimeRouteRecoveryOwner,
		RecipientPlanningOwner:          recipientPlanningOwner,
		SourceWorkflowName:              selectedAdmission.SourceWorkflowName,
		SourceWorkflowVersion:           selectedAdmission.SourceWorkflowVersion,
		FrontierEventCount:              selectedAdmission.FrontierEventCount,
		Prerequisites:                   contractSwapBootResumePrerequisites(),
		Classifications:                 classifications,
		BlockedSiblings:                 contractSwapBootResumeBlockedSiblings(),
		InvalidPaths:                    contractSwapBootResumeInvalidPaths(),
		UnsupportedBlockers:             blockers,
	}, nil
}

func validateContractSwapSelectedExecutionAdmission(admission store.RunForkSelectedContractExecutionAdmission) error {
	if strings.TrimSpace(admission.Owner) != store.RunForkSelectedContractExecutionAdmissionOwner {
		return fmt.Errorf("contract-swap boot/resume admission requires %s; got %q", store.RunForkSelectedContractExecutionAdmissionOwner, admission.Owner)
	}
	if !admission.NonMutating {
		return fmt.Errorf("contract-swap boot/resume admission requires non-mutating selected execution admission")
	}
	if admission.ExecutionSupported {
		return fmt.Errorf("contract-swap boot/resume admission cannot consume mutating selected execution admission")
	}
	if strings.TrimSpace(admission.ContractBindingOwner) != store.RunForkSelectedContractBindingOwner {
		return fmt.Errorf("contract-swap boot/resume admission requires %s; got %q", store.RunForkSelectedContractBindingOwner, admission.ContractBindingOwner)
	}
	if strings.TrimSpace(admission.ForkRunID) == "" || strings.TrimSpace(admission.SourceRunID) == "" || strings.TrimSpace(admission.ForkEventID) == "" {
		return fmt.Errorf("contract-swap boot/resume admission requires fork/source/event identity from selected binding")
	}
	if err := validateSelectedContractSelection("contract-swap boot/resume admission", admission.ContractSelection); err != nil {
		return err
	}
	if admission.RouteTopology == nil || strings.TrimSpace(admission.RouteTopology.Owner) != store.RunForkSelectedContractRouteTopologyOwner {
		return fmt.Errorf("contract-swap boot/resume admission requires %s evidence", store.RunForkSelectedContractRouteTopologyOwner)
	}
	if admission.RecipientPlanning == nil || strings.TrimSpace(admission.RecipientPlanning.Owner) != store.RunForkSelectedContractRecipientPlanningOwner {
		return fmt.Errorf("contract-swap boot/resume admission requires %s evidence", store.RunForkSelectedContractRecipientPlanningOwner)
	}
	return nil
}

func validateContractSwapRouteRecovery(admission store.RunForkSelectedContractExecutionAdmission, recovery store.RunForkSelectedContractRouteRecovery) error {
	if strings.TrimSpace(recovery.Owner) != store.RunForkSelectedContractRoutePersistenceOwner {
		return fmt.Errorf("contract-swap boot/resume admission requires %s route recovery; got %q", store.RunForkSelectedContractRoutePersistenceOwner, recovery.Owner)
	}
	if strings.TrimSpace(recovery.RuntimeRecoveryOwner) != store.RunForkSelectedContractRouteRecoveryOwner {
		return fmt.Errorf("contract-swap boot/resume admission requires %s runtime recovery; got %q", store.RunForkSelectedContractRouteRecoveryOwner, recovery.RuntimeRecoveryOwner)
	}
	if strings.TrimSpace(recovery.ForkRunID) != strings.TrimSpace(admission.ForkRunID) ||
		strings.TrimSpace(recovery.SourceRunID) != strings.TrimSpace(admission.SourceRunID) ||
		strings.TrimSpace(recovery.ForkEventID) != strings.TrimSpace(admission.ForkEventID) {
		return fmt.Errorf("contract-swap boot/resume admission route recovery identity does not match selected execution admission")
	}
	if err := validateSelectionMatches("contract-swap route recovery", admission.ContractSelection, recovery.ContractSelection); err != nil {
		return err
	}
	if strings.TrimSpace(recovery.RouteTopologyOwner) != store.RunForkSelectedContractRouteTopologyOwner {
		return fmt.Errorf("contract-swap boot/resume admission route recovery requires %s; got %q", store.RunForkSelectedContractRouteTopologyOwner, recovery.RouteTopologyOwner)
	}
	if strings.TrimSpace(recovery.RecipientPlanningOwner) != store.RunForkSelectedContractRecipientPlanningOwner {
		return fmt.Errorf("contract-swap boot/resume admission route recovery requires %s; got %q", store.RunForkSelectedContractRecipientPlanningOwner, recovery.RecipientPlanningOwner)
	}
	return nil
}

func contractSwapBootResumePrerequisites() []store.RunForkSelectedContractExecutionBoundary {
	return []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "selected_contract_binding",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       store.RunForkSelectedContractBindingOwner,
			Reason:      "contract-swap boot/resume readiness consumes the durable selected contract binding; CLI/API arguments are not owners",
		},
		{
			Concept:     "selected_contract_execution_admission",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       store.RunForkSelectedContractExecutionAdmissionOwner,
			Reason:      "selected semantic source loading and validation must come from the canonical selected execution admission owner",
		},
		{
			Concept:     "replay_resume_admission",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       store.RunForkReplayResumeAdmissionOwner,
			Reason:      "source historical facts must be classified by the canonical replay/resume taxonomy before contract-swap readiness is answered",
		},
		{
			Concept:     "selected_contract_route_recovery",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       store.RunForkSelectedContractRouteRecoveryOwner,
			Reason:      "future mutating boot/resume must recover selected route truth from fork-local route recovery evidence, not current routing rows",
		},
		{
			Concept:     "selected_contract_recipient_planning",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       store.RunForkSelectedContractRecipientPlanningOwner,
			Reason:      "future mutation must continue to route recipients through selected recipient planning and publish guards",
		},
	}
}

func contractSwapBootResumeBlockedSiblings() []store.RunForkSelectedContractExecutionBoundary {
	return []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "mutating_contract_swap_boot_resume",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       store.RunForkHistoricalReplayContractSwapBootResumeOwner,
			Reason:      "this admission owner classifies readiness only; handler execution and fork-local writes remain separately gated",
		},
		{
			Concept:     "timer_reconstruction",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "source timer facts remain fail-closed until a timer reconstruction owner is approved",
		},
		{
			Concept:     "sessions_turns_audits",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "source session, turn, and audit reconstruction remains under historical replay/resume siblings",
		},
		{
			Concept:     "node_system_non_agent_execution",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "node/system/non-agent replay remains separately gated and is not admitted through contract-swap readiness",
		},
		{
			Concept:     "builder_dashboard_api",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "operator surfaces may consume this admission but must not own contract-swap readiness semantics",
		},
	}
}

func contractSwapBootResumeInvalidPaths() []store.RunForkSelectedContractExecutionBoundary {
	return []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "cli_owned_contract_swap_readiness",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "CLI/API flags are request surfaces and cannot decide contract-swap boot/resume readiness",
		},
		{
			Concept:     "copy_source_event_deliveries",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "source event_deliveries are lineage/blocker evidence, not executable selected-fork work",
		},
		{
			Concept:     "source_outcome_suppression",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "source receipts, dead letters, retry state, and post-T outcomes cannot suppress future fork-local work",
		},
		{
			Concept:     "current_route_rows_as_fork_truth",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "current routing_rules and flow-instance route rows are not selected-fork route truth",
		},
		{
			Concept:     "same_run_outbox_replay_as_timestamp_fork_resume",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "same-run replay proof does not define timestamp-fork contract-swap boot/resume semantics",
		},
	}
}
