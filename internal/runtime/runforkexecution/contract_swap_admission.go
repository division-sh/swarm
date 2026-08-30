package runforkexecution

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

type ContractSwapBootResumeAdmissionRequest struct {
	SelectedExecutionAdmission runfork.RunForkSelectedContractExecutionAdmission
	ReplayResumeAdmission      runfork.RunForkReplayResumeAdmission
	RouteRecovery              *runfork.RunForkSelectedContractRouteRecovery
}

func BuildContractSwapBootResumeAdmission(req ContractSwapBootResumeAdmissionRequest) (runfork.RunForkContractSwapBootResumeAdmission, error) {
	selectedAdmission := req.SelectedExecutionAdmission
	if err := validateContractSwapSelectedExecutionAdmission(selectedAdmission); err != nil {
		return runfork.RunForkContractSwapBootResumeAdmission{}, err
	}
	replayAdmission := req.ReplayResumeAdmission
	if strings.TrimSpace(replayAdmission.Owner) != runfork.RunForkReplayResumeAdmissionOwner {
		return runfork.RunForkContractSwapBootResumeAdmission{}, fmt.Errorf("contract-swap boot/resume admission requires %s; got %q", runfork.RunForkReplayResumeAdmissionOwner, replayAdmission.Owner)
	}

	var routeRecoveryOwner, runtimeRouteRecoveryOwner string
	if req.RouteRecovery != nil {
		if err := validateContractSwapRouteRecovery(selectedAdmission, *req.RouteRecovery); err != nil {
			return runfork.RunForkContractSwapBootResumeAdmission{}, err
		}
		routeRecoveryOwner = req.RouteRecovery.Owner
		runtimeRouteRecoveryOwner = req.RouteRecovery.RuntimeRecoveryOwner
	}

	blockers := append([]runfork.RunForkUnsupportedBlocker(nil), selectedAdmission.UnsupportedBlockers...)
	for _, blocker := range replayAdmission.UnsupportedBlockers {
		blockers = appendRunForkUnsupportedBlocker(blockers, blocker)
	}
	blockers = appendRunForkUnsupportedBlocker(blockers, runfork.RunForkUnsupportedBlocker{
		Code:    runfork.RunForkBlockerContractSwapBootResumeAdmissionNonMutating,
		Message: "contract-swap boot/resume admission is non-mutating; bounded fork re-execution remains separately gated",
	})
	if req.RouteRecovery == nil {
		blockers = appendRunForkUnsupportedBlocker(blockers, runfork.RunForkUnsupportedBlocker{
			Code:    runfork.RunForkBlockerContractSwapRouteRecoveryMissing,
			Message: "contract-swap boot/resume readiness requires fork-local selected route recovery evidence before future mutation",
		})
	}

	classifications := append([]runfork.RunForkReplayResumeDisposition(nil), replayAdmission.Dispositions...)
	classifications = append(classifications, runfork.RunForkReplayResumeDisposition{
		Fact:        runfork.RunForkReplayResumeFactContractSwap,
		Disposition: runfork.RunForkReplayResumeDispositionFailClosedBlocker,
		BlockerCode: runfork.RunForkBlockerContractSwapBootResumeAdmissionNonMutating,
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

	return runfork.RunForkContractSwapBootResumeAdmission{
		Owner:                           runfork.RunForkContractSwapBootResumeAdmissionOwner,
		NonMutating:                     true,
		BootResumeSupported:             false,
		FutureExecutionOwner:            runfork.RunForkHistoricalReplayContractSwapBootResumeOwner,
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

func validateContractSwapSelectedExecutionAdmission(admission runfork.RunForkSelectedContractExecutionAdmission) error {
	if strings.TrimSpace(admission.Owner) != runfork.RunForkSelectedContractExecutionAdmissionOwner {
		return fmt.Errorf("contract-swap boot/resume admission requires %s; got %q", runfork.RunForkSelectedContractExecutionAdmissionOwner, admission.Owner)
	}
	if !admission.NonMutating {
		return fmt.Errorf("contract-swap boot/resume admission requires non-mutating selected execution admission")
	}
	if admission.ExecutionSupported {
		return fmt.Errorf("contract-swap boot/resume admission cannot consume mutating selected execution admission")
	}
	if strings.TrimSpace(admission.ContractBindingOwner) != runfork.RunForkSelectedContractBindingOwner {
		return fmt.Errorf("contract-swap boot/resume admission requires %s; got %q", runfork.RunForkSelectedContractBindingOwner, admission.ContractBindingOwner)
	}
	if strings.TrimSpace(admission.ForkRunID) == "" || strings.TrimSpace(admission.SourceRunID) == "" || strings.TrimSpace(admission.ForkEventID) == "" {
		return fmt.Errorf("contract-swap boot/resume admission requires fork/source/event identity from selected binding")
	}
	if err := validateSelectedContractSelection("contract-swap boot/resume admission", admission.ContractSelection); err != nil {
		return err
	}
	if admission.RouteTopology == nil || strings.TrimSpace(admission.RouteTopology.Owner) != runfork.RunForkSelectedContractRouteTopologyOwner {
		return fmt.Errorf("contract-swap boot/resume admission requires %s evidence", runfork.RunForkSelectedContractRouteTopologyOwner)
	}
	if admission.RecipientPlanning == nil || strings.TrimSpace(admission.RecipientPlanning.Owner) != runfork.RunForkSelectedContractRecipientPlanningOwner {
		return fmt.Errorf("contract-swap boot/resume admission requires %s evidence", runfork.RunForkSelectedContractRecipientPlanningOwner)
	}
	return nil
}

func validateContractSwapRouteRecovery(admission runfork.RunForkSelectedContractExecutionAdmission, recovery runfork.RunForkSelectedContractRouteRecovery) error {
	if strings.TrimSpace(recovery.Owner) != runfork.RunForkSelectedContractRoutePersistenceOwner {
		return fmt.Errorf("contract-swap boot/resume admission requires %s route recovery; got %q", runfork.RunForkSelectedContractRoutePersistenceOwner, recovery.Owner)
	}
	if strings.TrimSpace(recovery.RuntimeRecoveryOwner) != runfork.RunForkSelectedContractRouteRecoveryOwner {
		return fmt.Errorf("contract-swap boot/resume admission requires %s runtime recovery; got %q", runfork.RunForkSelectedContractRouteRecoveryOwner, recovery.RuntimeRecoveryOwner)
	}
	if strings.TrimSpace(recovery.ForkRunID) != strings.TrimSpace(admission.ForkRunID) ||
		strings.TrimSpace(recovery.SourceRunID) != strings.TrimSpace(admission.SourceRunID) ||
		strings.TrimSpace(recovery.ForkEventID) != strings.TrimSpace(admission.ForkEventID) {
		return fmt.Errorf("contract-swap boot/resume admission route recovery identity does not match selected execution admission")
	}
	if err := validateSelectionMatches("contract-swap route recovery", admission.ContractSelection, recovery.ContractSelection); err != nil {
		return err
	}
	if strings.TrimSpace(recovery.RouteTopologyOwner) != runfork.RunForkSelectedContractRouteTopologyOwner {
		return fmt.Errorf("contract-swap boot/resume admission route recovery requires %s; got %q", runfork.RunForkSelectedContractRouteTopologyOwner, recovery.RouteTopologyOwner)
	}
	if strings.TrimSpace(recovery.RecipientPlanningOwner) != runfork.RunForkSelectedContractRecipientPlanningOwner {
		return fmt.Errorf("contract-swap boot/resume admission route recovery requires %s; got %q", runfork.RunForkSelectedContractRecipientPlanningOwner, recovery.RecipientPlanningOwner)
	}
	return nil
}

func contractSwapBootResumePrerequisites() []runfork.RunForkSelectedContractExecutionBoundary {
	return []runfork.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "selected_contract_binding",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractBindingOwner,
			Reason:      "contract-swap boot/resume readiness consumes the durable selected contract binding; CLI/API arguments are not owners",
		},
		{
			Concept:     "selected_contract_execution_admission",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractExecutionAdmissionOwner,
			Reason:      "selected semantic source loading and validation must come from the canonical selected execution admission owner",
		},
		{
			Concept:     "replay_resume_admission",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkReplayResumeAdmissionOwner,
			Reason:      "source historical facts must be classified by the canonical replay/resume taxonomy before contract-swap readiness is answered",
		},
		{
			Concept:     "selected_contract_route_recovery",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractRouteRecoveryOwner,
			Reason:      "future mutating boot/resume must recover selected route truth from fork-local route recovery evidence, not current routing rows",
		},
		{
			Concept:     "selected_contract_recipient_planning",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractRecipientPlanningOwner,
			Reason:      "future mutation must continue to route recipients through selected recipient planning and publish guards",
		},
	}
}

func contractSwapBootResumeBlockedSiblings() []runfork.RunForkSelectedContractExecutionBoundary {
	return []runfork.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "mutating_contract_swap_boot_resume",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       runfork.RunForkHistoricalReplayContractSwapBootResumeOwner,
			Reason:      "this admission owner classifies readiness only; handler execution and fork-local writes remain separately gated",
		},
		{
			Concept:     "timer_reconstruction",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "source timer facts remain fail-closed under #642 until a long-lived selected-fork timer owner is approved",
		},
		{
			Concept:     "sessions_turns_audits",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "source session, turn, and audit reconstruction remains under bounded fork re-execution siblings",
		},
		{
			Concept:     "node_system_non_agent_execution",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "node/system/non-agent replay remains separately gated and is not admitted through contract-swap readiness",
		},
		{
			Concept:     "dashboard_api",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "operator surfaces may consume this admission but must not own contract-swap readiness semantics",
		},
	}
}

func contractSwapBootResumeInvalidPaths() []runfork.RunForkSelectedContractExecutionBoundary {
	return []runfork.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "cli_owned_contract_swap_readiness",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "CLI/API flags are request surfaces and cannot decide contract-swap boot/resume readiness",
		},
		{
			Concept:     "copy_source_event_deliveries",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "source event_deliveries are lineage/blocker evidence, not executable selected-fork work",
		},
		{
			Concept:     "source_outcome_suppression",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "source receipts, dead letters, retry state, and post-T outcomes cannot suppress future fork-local work",
		},
		{
			Concept:     "current_route_rows_as_fork_truth",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "current routing_rules and flow-instance route rows are not selected-fork route truth",
		},
		{
			Concept:     "same_run_outbox_replay_as_fixed_event_fork_resume",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "same-run replay proof does not define fixed-event fork contract-swap boot/resume semantics",
		},
	}
}
