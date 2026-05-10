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
	return runForkSelectedContractSessionTurnAuditLineageAdmission(plan, plan.ReplayResumeAdmission)
}

func runForkSelectedContractSessionTurnAuditLineageAdmission(plan RunForkPlan, admission RunForkReplayResumeAdmission) RunForkReplayResumeAdmission {
	if strings.TrimSpace(admission.Owner) == "" {
		admission.Owner = RunForkReplayResumeAdmissionOwner
	}
	if runForkSelectedContractHasUnsafeActiveDeliverySessionCoupling(plan) {
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

// RunForkSelectedContractActiveSourceDeliveryConversationCouplingAdmission admits
// only the narrow #678 case: the in-progress source delivery is the same agent
// delivery that emitted the selected fork-point event, and it carries no live
// session/receipt/outcome state that would have to be resumed in the fork.
func RunForkSelectedContractActiveSourceDeliveryConversationCouplingAdmission(plan RunForkPlan, admission RunForkReplayResumeAdmission) RunForkReplayResumeAdmission {
	policy := runForkSelectedContractActiveSourceDeliveryConversationCouplingPolicy(plan)
	if len(policy.admitted) == 0 {
		return admission
	}
	if strings.TrimSpace(admission.Owner) == "" {
		admission.Owner = RunForkReplayResumeAdmissionOwner
	}

	changed := false
	for i := range admission.Dispositions {
		disposition := &admission.Dispositions[i]
		if strings.TrimSpace(disposition.Disposition) != RunForkReplayResumeDispositionFailClosedBlocker {
			continue
		}
		if strings.TrimSpace(disposition.BlockerCode) != RunForkBlockerDeliveryHistoryUnproven {
			continue
		}
		if strings.TrimSpace(disposition.Fact) != RunForkReplayResumeFactDeliveryInProgressHistory {
			continue
		}
		if !policy.admitted[runForkSelectedContractDispositionKey(*disposition)] {
			continue
		}
		disposition.Disposition = RunForkReplayResumeDispositionLineageOnly
		disposition.Owner = RunForkSelectedContractActiveSourceDeliveryConversationCouplingPolicyOwner
		disposition.BlockerCode = ""
		disposition.Classification = RunForkSelectedContractActiveSourceDeliveryConversationCouplingClassification
		disposition.Message = fmt.Sprintf("%s classifies the same in-progress source delivery that emitted the fork-point event as selected-contract branch-divergence lineage only; source sessions, receipts, outcomes, and delivery rows are not copied or used as fork-local truth", RunForkSelectedContractActiveSourceDeliveryConversationCouplingPolicyOwner)
		changed = true
	}
	if !changed {
		return admission
	}
	return runForkReplayResumeAdmissionPruneResolvedBlockers(admission)
}

func runForkSelectedContractConversationLineageBlocker(code string) bool {
	for _, blockerCode := range runForkSelectedContractConversationLineageFacts {
		if code == blockerCode {
			return true
		}
	}
	return false
}

func runForkSelectedContractHasUnsafeActiveDeliverySessionCoupling(plan RunForkPlan) bool {
	return runForkSelectedContractActiveSourceDeliveryConversationCouplingPolicy(plan).unsafe
}

func runForkSelectedContractActiveSourceDeliveryConversationCouplingAdmitted(plan RunForkPlan, item RunForkPendingWork) bool {
	return runForkSelectedContractActiveSourceDeliveryConversationCouplingPolicy(plan).admitted[runForkSelectedContractPendingWorkKey(item)]
}

func runForkSelectedContractActiveSourceDeliveryConversationCouplingFacts(admission RunForkReplayResumeAdmission) []string {
	facts := []string{}
	for _, disposition := range admission.Dispositions {
		if strings.TrimSpace(disposition.Fact) != RunForkReplayResumeFactDeliveryInProgressHistory {
			continue
		}
		if strings.TrimSpace(disposition.Disposition) != RunForkReplayResumeDispositionLineageOnly {
			continue
		}
		if strings.TrimSpace(disposition.Owner) != RunForkSelectedContractActiveSourceDeliveryConversationCouplingPolicyOwner {
			continue
		}
		classification := strings.TrimSpace(disposition.Classification)
		if classification == "" {
			classification = RunForkSelectedContractActiveSourceDeliveryConversationCouplingClassification
		}
		facts = append(facts, classification)
	}
	return uniqueNonEmptyStrings(facts)
}

type runForkSelectedContractActiveSourceDeliveryConversationCouplingPolicyResult struct {
	admitted map[string]bool
	unsafe   bool
}

func runForkSelectedContractActiveSourceDeliveryConversationCouplingPolicy(plan RunForkPlan) runForkSelectedContractActiveSourceDeliveryConversationCouplingPolicyResult {
	result := runForkSelectedContractActiveSourceDeliveryConversationCouplingPolicyResult{
		admitted: map[string]bool{},
	}
	for _, item := range plan.PendingWork {
		if !runForkSelectedContractPendingWorkHasActiveDeliverySessionCoupling(item) {
			continue
		}
		if runForkSelectedContractSameSourceDeliveryForkPointEmission(plan, item) {
			result.admitted[runForkSelectedContractPendingWorkKey(item)] = true
			continue
		}
		result.unsafe = true
	}
	return result
}

func runForkSelectedContractPendingWorkHasActiveDeliverySessionCoupling(item RunForkPendingWork) bool {
	classification := strings.TrimSpace(item.Classification)
	switch classification {
	case RunForkPendingClassificationInProgress:
		return true
	case RunForkPendingClassificationPending:
	default:
		return false
	}
	return strings.TrimSpace(item.ActiveSessionID) != "" ||
		item.StartedAt != nil ||
		item.DeliveredAt != nil ||
		item.ReceiptAt != nil ||
		strings.TrimSpace(item.ReceiptOutcome) != ""
}

func runForkSelectedContractSameSourceDeliveryForkPointEmission(plan RunForkPlan, item RunForkPendingWork) bool {
	if strings.TrimSpace(item.Classification) != RunForkPendingClassificationInProgress {
		return false
	}
	if strings.TrimSpace(item.EventID) == "" ||
		strings.TrimSpace(item.EventID) != strings.TrimSpace(plan.ForkPoint.SourceEventID) {
		return false
	}
	if strings.TrimSpace(item.DeliveryID) == "" || strings.TrimSpace(item.SubscriberType) != "agent" {
		return false
	}
	if strings.TrimSpace(plan.ForkPoint.ProducedByType) != "agent" ||
		strings.TrimSpace(item.SubscriberID) == "" ||
		strings.TrimSpace(item.SubscriberID) != strings.TrimSpace(plan.ForkPoint.ProducedBy) {
		return false
	}
	if strings.TrimSpace(item.ActiveSessionID) != "" ||
		item.StartedAt == nil ||
		item.StartedAt.After(plan.ForkPoint.Timestamp) ||
		item.DeliveredAt != nil ||
		item.ReceiptAt != nil ||
		strings.TrimSpace(item.ReceiptOutcome) != "" {
		return false
	}
	return true
}

func runForkSelectedContractPendingWorkKey(item RunForkPendingWork) string {
	return strings.Join([]string{
		strings.TrimSpace(item.EventID),
		strings.TrimSpace(item.DeliveryID),
		strings.TrimSpace(item.SubscriberType),
		strings.TrimSpace(item.SubscriberID),
	}, "\x00")
}

func runForkSelectedContractDispositionKey(item RunForkReplayResumeDisposition) string {
	return strings.Join([]string{
		strings.TrimSpace(item.EventID),
		strings.TrimSpace(item.DeliveryID),
		strings.TrimSpace(item.SubscriberType),
		strings.TrimSpace(item.SubscriberID),
	}, "\x00")
}

func runForkReplayResumeAdmissionPruneResolvedBlockers(admission RunForkReplayResumeAdmission) RunForkReplayResumeAdmission {
	unresolved := map[string]bool{}
	for _, disposition := range admission.Dispositions {
		if strings.TrimSpace(disposition.Disposition) != RunForkReplayResumeDispositionFailClosedBlocker {
			continue
		}
		if code := strings.TrimSpace(disposition.BlockerCode); code != "" {
			unresolved[code] = true
		}
	}
	filtered := make([]RunForkUnsupportedBlocker, 0, len(admission.UnsupportedBlockers))
	for _, blocker := range admission.UnsupportedBlockers {
		if unresolved[strings.TrimSpace(blocker.Code)] {
			filtered = append(filtered, blocker)
		}
	}
	admission.UnsupportedBlockers = filtered
	return runForkReplayResumeAdmissionRecalculateReadiness(admission)
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
