package runfork

import (
	"fmt"
	"slices"
	"strings"
)

const RunForkSelectedFanOutReplayAdmissionOwner = RunForkSelectedContractExecutionOwner + ".fan_out_replay_admission"

// RunForkSelectedFanOutReplayAdmission classifies exact continuing handler and
// deployment partitions. It grants no execution authority or source attempts.
func RunForkSelectedFanOutReplayAdmission(plan RunForkPlan, admission RunForkReplayResumeAdmission) (RunForkReplayResumeAdmission, error) {
	if err := ValidateFanOutPendingReplayAdmission(plan); err != nil {
		return RunForkReplayResumeAdmission{}, fmt.Errorf("selected fan-out replay admission: %w", err)
	}
	if admission.Owner != RunForkReplayResumeAdmissionOwner {
		return RunForkReplayResumeAdmission{}, fmt.Errorf("selected fan-out replay admission requires %s", RunForkReplayResumeAdmissionOwner)
	}
	owedByEvent, err := selectedFanOutReplayPendingPartitions(plan)
	if err != nil {
		return RunForkReplayResumeAdmission{}, err
	}
	if len(owedByEvent) == 0 {
		return admission, nil
	}
	byDelivery, err := selectedFanOutReplayFixedDeliveries(plan.PendingWork, owedByEvent)
	if err != nil {
		return RunForkReplayResumeAdmission{}, err
	}
	indices, err := selectedFanOutReplayDispositionIndices(admission.Dispositions, owedByEvent, byDelivery)
	if err != nil {
		return RunForkReplayResumeAdmission{}, err
	}
	return selectedFanOutReplayApplyAdmission(admission, owedByEvent, indices), nil
}

func selectedFanOutReplayPendingPartitions(plan RunForkPlan) (map[string]map[string]bool, error) {
	var sourceEvents []string
	for _, obligation := range plan.FanOutObligations {
		for _, replay := range obligation.PendingReplays {
			sourceEvents = append(sourceEvents, replay.SourceEventID)
		}
		for _, pending := range obligation.PendingDeployment {
			sourceEvents = append(sourceEvents, pending.SourceEventID)
		}
	}
	owedByEvent := make(map[string]map[string]bool, len(sourceEvents))
	for _, eventID := range sourceEvents {
		if owedByEvent[eventID] != nil {
			return nil, fmt.Errorf("selected fan-out event %s has ambiguous pending origins", eventID)
		}
		partition, present, err := plan.FanOutPendingRecipientPartition(eventID)
		if err != nil {
			return nil, fmt.Errorf("selected fan-out recipient partition: %w", err)
		}
		if !present {
			return nil, fmt.Errorf("selected fan-out event %s lacks its pending partition", eventID)
		}
		owed := make(map[string]bool, len(partition.SourceDeliveryIDs))
		for _, deliveryID := range partition.SourceDeliveryIDs {
			owed[deliveryID] = true
		}
		owedByEvent[eventID] = owed
	}
	return owedByEvent, nil
}

func selectedFanOutReplayFixedDeliveries(pending []RunForkPendingWork, owedByEvent map[string]map[string]bool) (map[string]RunForkPendingWork, error) {
	byDelivery := make(map[string]RunForkPendingWork)
	for _, item := range pending {
		if owedByEvent[item.EventID] == nil || item.DeliveryID == "" {
			continue
		}
		if _, duplicate := byDelivery[item.DeliveryID]; duplicate {
			return nil, fmt.Errorf("selected fan-out repeats fixed delivery %s", item.DeliveryID)
		}
		byDelivery[item.DeliveryID] = item
	}
	return byDelivery, nil
}

func selectedFanOutReplayDispositionIndices(dispositions []RunForkReplayResumeDisposition, owedByEvent map[string]map[string]bool, byDelivery map[string]RunForkPendingWork) (map[string]int, error) {
	indices := make(map[string]int, len(byDelivery))
	for index, disposition := range dispositions {
		if !selectedFanOutReplayDeliveryFact(disposition.Fact) {
			continue
		}
		item, known := byDelivery[strings.TrimSpace(disposition.DeliveryID)]
		if owedByEvent[strings.TrimSpace(disposition.EventID)] == nil && !known {
			continue
		}
		// Receipt-only platform facts are not members of the delivery partition.
		if disposition.DeliveryID == "" && disposition.SubscriberType == "platform" {
			continue
		}
		if !selectedFanOutReplayDispositionMatches(disposition, item, known) {
			return nil, fmt.Errorf("selected fan-out disposition does not exactly match fixed delivery %s", disposition.DeliveryID)
		}
		if _, duplicate := indices[item.DeliveryID]; duplicate {
			return nil, fmt.Errorf("selected fan-out repeats disposition for delivery %s", item.DeliveryID)
		}
		owed := owedByEvent[item.EventID][item.DeliveryID]
		if err := selectedFanOutReplayValidateDispositionState(disposition, item, owed); err != nil {
			return nil, err
		}
		indices[item.DeliveryID] = index
	}
	for deliveryID := range byDelivery {
		if _, present := indices[deliveryID]; !present {
			return nil, fmt.Errorf("selected fan-out requires the exact disposition for delivery %s", deliveryID)
		}
	}
	return indices, nil
}

func selectedFanOutReplayDispositionMatches(disposition RunForkReplayResumeDisposition, item RunForkPendingWork, known bool) bool {
	return known && disposition.EventID == item.EventID && disposition.DeliveryID == item.DeliveryID &&
		disposition.SubscriberType == item.SubscriberType && disposition.SubscriberID == item.SubscriberID &&
		disposition.Classification == item.Classification && disposition.Fact == selectedFanOutReplayFact(item.Classification)
}

// SelectedFanOutReplayDeliveryAdmitted consumes the canonical partition admission,
// not a source attempt or permission to resume its provider/session execution.
func SelectedFanOutReplayDeliveryAdmitted(admission RunForkReplayResumeAdmission, item RunForkPendingWork) bool {
	if admission.Owner != RunForkReplayResumeAdmissionOwner || item.DeliveryID == "" {
		return false
	}
	matches := 0
	for _, disposition := range admission.Dispositions {
		if ReplayResumeDispositionKey(disposition) != PendingWorkKey(item) {
			continue
		}
		if !selectedFanOutReplayDispositionMatches(disposition, item, true) ||
			disposition.Owner != RunForkSelectedFanOutReplayAdmissionOwner ||
			disposition.Disposition != RunForkReplayResumeDispositionForkReplay || disposition.BlockerCode != "" {
			return false
		}
		matches++
	}
	return matches == 1
}

func selectedFanOutReplayValidateDispositionState(disposition RunForkReplayResumeDisposition, item RunForkPendingWork, owed bool) error {
	if owed && item.SubscriberType != "node" && item.SubscriberType != "agent" {
		return fmt.Errorf("selected fan-out delivery %s has an unsupported recipient", item.DeliveryID)
	}
	switch disposition.Disposition {
	case RunForkReplayResumeDispositionFailClosedBlocker:
		if disposition.BlockerCode != RunForkBlockerDeliveryHistoryUnproven && disposition.BlockerCode != RunForkBlockerNonAgentDeliveryReplayUnsupported {
			return fmt.Errorf("selected fan-out delivery %s has an unrelated blocker", item.DeliveryID)
		}
	case RunForkReplayResumeDispositionForkReplay:
		if !owed || disposition.BlockerCode != "" {
			return fmt.Errorf("selected fan-out delivery %s has contradictory replay admission", item.DeliveryID)
		}
	case RunForkReplayResumeDispositionLineageOnly:
		if owed || disposition.BlockerCode != "" {
			return fmt.Errorf("selected fan-out delivery %s has contradictory terminal admission", item.DeliveryID)
		}
	default:
		return fmt.Errorf("selected fan-out delivery %s has an unsupported disposition", item.DeliveryID)
	}
	return nil
}

func selectedFanOutReplayApplyAdmission(admission RunForkReplayResumeAdmission, owedByEvent map[string]map[string]bool, indices map[string]int) RunForkReplayResumeAdmission {
	admission.Dispositions = slices.Clone(admission.Dispositions)
	admission.UnsupportedBlockers = slices.Clone(admission.UnsupportedBlockers)
	discharged := make(map[string]bool)
	for deliveryID, index := range indices {
		disposition := &admission.Dispositions[index]
		if disposition.BlockerCode != "" {
			discharged[disposition.BlockerCode] = true
		}
		disposition.Owner = RunForkSelectedFanOutReplayAdmissionOwner
		disposition.BlockerCode = ""
		if owedByEvent[disposition.EventID][deliveryID] {
			disposition.Disposition = RunForkReplayResumeDispositionForkReplay
			disposition.Message = "exact unfinished fan-out recipient requires fresh selected-child execution; source attempts remain lineage only"
		} else {
			disposition.Disposition = RunForkReplayResumeDispositionLineageOnly
			disposition.Message = "exact terminal fan-out recipient is retained as lineage and is never reissued"
		}
	}
	for _, disposition := range admission.Dispositions {
		delete(discharged, disposition.BlockerCode)
	}
	blockers := admission.UnsupportedBlockers[:0]
	for _, blocker := range admission.UnsupportedBlockers {
		if !discharged[blocker.Code] {
			blockers = append(blockers, blocker)
		}
	}
	admission.UnsupportedBlockers = blockers
	return RecalculateReplayResumeAdmission(admission)
}

func selectedFanOutReplayFact(classification string) string {
	switch classification {
	case RunForkPendingClassificationPending:
		return RunForkReplayResumeFactDeliveryPendingHistory
	case RunForkPendingClassificationInProgress:
		return RunForkReplayResumeFactDeliveryInProgressHistory
	case RunForkPendingClassificationFailedRetryable:
		return RunForkReplayResumeFactDeliveryFailedHistory
	case RunForkPendingClassificationDeliveredCompleted:
		return RunForkReplayResumeFactDeliveryCompletedHistory
	case RunForkPendingClassificationDeadLetter:
		return RunForkReplayResumeFactDeliveryDeadLetterHistory
	case RunForkPendingClassificationCanceled:
		return RunForkReplayResumeFactDeliveryCanceledHistory
	default:
		return ""
	}
}

func selectedFanOutReplayDeliveryFact(fact string) bool {
	switch fact {
	case RunForkReplayResumeFactDeliveryPendingHistory, RunForkReplayResumeFactDeliveryInProgressHistory,
		RunForkReplayResumeFactDeliveryFailedHistory, RunForkReplayResumeFactDeliveryCompletedHistory,
		RunForkReplayResumeFactDeliveryDeadLetterHistory, RunForkReplayResumeFactDeliveryCanceledHistory:
		return true
	default:
		return false
	}
}
