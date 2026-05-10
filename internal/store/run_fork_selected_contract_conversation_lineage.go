package store

import (
	"fmt"
	"strings"
)

const RunForkSelectedContractSessionTurnAuditLineagePolicyOwner = "runtime.run_fork.historical_replay_execution.selected_contract_session_turn_audit_lineage_policy"

var runForkSelectedContractConversationLineageFacts = map[string]string{
	RunForkReplayResumeFactSessionHistory:           RunForkBlockerSessionHistoryUnproven,
	RunForkReplayResumeFactConversationAuditHistory: RunForkBlockerConversationAuditUnproven,
	RunForkReplayResumeFactActiveTurnHistory:        RunForkBlockerActiveTurnHistoryUnproven,
}

// RunForkSelectedContractSessionTurnAuditLineageAdmission applies the selected-contract
// conversation-history policy after the store replay taxonomy has identified source facts.
func RunForkSelectedContractSessionTurnAuditLineageAdmission(plan RunForkPlan) RunForkReplayResumeAdmission {
	admission := plan.ReplayResumeAdmission
	if strings.TrimSpace(admission.Owner) == "" {
		admission.Owner = RunForkReplayResumeAdmissionOwner
	}
	if runForkSelectedContractHasActiveDeliverySessionCoupling(plan.PendingWork) {
		return admission
	}

	changed := false
	for i := range admission.Dispositions {
		disposition := &admission.Dispositions[i]
		fact := strings.TrimSpace(disposition.Fact)
		blockerCode, ok := runForkSelectedContractConversationLineageFacts[fact]
		if !ok {
			continue
		}
		if strings.TrimSpace(disposition.Disposition) != RunForkReplayResumeDispositionFailClosedBlocker {
			continue
		}
		if code := strings.TrimSpace(disposition.BlockerCode); code != "" && code != blockerCode {
			continue
		}
		disposition.Disposition = RunForkReplayResumeDispositionLineageOnly
		disposition.Owner = RunForkSelectedContractSessionTurnAuditLineagePolicyOwner
		disposition.BlockerCode = ""
		disposition.Classification = ""
		disposition.Message = fmt.Sprintf("%s classifies at/before-T source %s as selected-contract lineage/no-action evidence only; fresh fork-local conversation rows must be created by normal runtime execution under the fork run_id", RunForkSelectedContractSessionTurnAuditLineagePolicyOwner, fact)
		changed = true
	}
	if !changed {
		return admission
	}

	filtered := make([]RunForkUnsupportedBlocker, 0, len(admission.UnsupportedBlockers))
	for _, blocker := range admission.UnsupportedBlockers {
		if runForkSelectedContractConversationLineageBlocker(strings.TrimSpace(blocker.Code)) {
			continue
		}
		filtered = append(filtered, blocker)
	}
	admission.UnsupportedBlockers = filtered
	return runForkReplayResumeAdmissionRecalculateReadiness(admission)
}

func runForkSelectedContractConversationLineageBlocker(code string) bool {
	for _, blockerCode := range runForkSelectedContractConversationLineageFacts {
		if code == blockerCode {
			return true
		}
	}
	return false
}

func runForkSelectedContractHasActiveDeliverySessionCoupling(pending []RunForkPendingWork) bool {
	for _, item := range pending {
		if strings.TrimSpace(item.Classification) == RunForkPendingClassificationDeliveredCompleted {
			continue
		}
		if strings.TrimSpace(item.ActiveSessionID) != "" ||
			item.StartedAt != nil ||
			item.DeliveredAt != nil ||
			item.ReceiptAt != nil ||
			strings.TrimSpace(item.ReceiptOutcome) != "" {
			return true
		}
		if strings.TrimSpace(item.Classification) == RunForkPendingClassificationInProgress {
			return true
		}
	}
	return false
}

func runForkReplayResumeAdmissionRecalculateReadiness(admission RunForkReplayResumeAdmission) RunForkReplayResumeAdmission {
	hasHistoricalReplayRequirement := false
	hasReplayableDeliveryEvent := false
	for _, disposition := range admission.Dispositions {
		switch strings.TrimSpace(disposition.Fact) {
		case RunForkReplayResumeFactEntityStateSnapshot,
			RunForkReplayResumeFactHistoricalReplayExecution,
			RunForkReplayResumeFactContractSwap:
			continue
		}
		switch strings.TrimSpace(disposition.Disposition) {
		case RunForkReplayResumeDispositionForkReplay:
			hasHistoricalReplayRequirement = true
			hasReplayableDeliveryEvent = true
		case RunForkReplayResumeDispositionFailClosedBlocker,
			RunForkReplayResumeDispositionReconstruct:
			hasHistoricalReplayRequirement = true
		}
	}
	admission.DeliveryEventReplayReady = hasReplayableDeliveryEvent && len(admission.UnsupportedBlockers) == 0
	admission.StateOnlyExecutionReady = len(admission.UnsupportedBlockers) == 0 && !hasHistoricalReplayRequirement
	admission.HistoricalReplayRequired = hasHistoricalReplayRequirement
	admission.HistoricalReplaySupported = admission.DeliveryEventReplayReady
	return admission
}
