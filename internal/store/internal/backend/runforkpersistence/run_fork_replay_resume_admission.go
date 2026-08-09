package runforkpersistence

import (
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func runForkReplayResumeAdmission(evidence runForkAdmissionEvidence) runfork.RunForkReplayResumeAdmission {
	dispositions := []runfork.RunForkReplayResumeDisposition{
		{
			Fact:        runfork.RunForkReplayResumeFactEntityStateSnapshot,
			Disposition: runfork.RunForkReplayResumeDispositionReconstruct,
			Message:     "fork current-state snapshots are reconstructed from entity_mutations and may be materialized by the state-only fork owner",
		},
		{
			Fact:        runfork.RunForkReplayResumeFactHistoricalReplayExecution,
			Disposition: runfork.RunForkReplayResumeDispositionSplitSibling,
			Message:     "bounded fork re-execution remains a separate gated child; this admission taxonomy is non-mutating",
		},
		{
			Fact:        runfork.RunForkReplayResumeFactContractSwap,
			Disposition: runfork.RunForkReplayResumeDispositionSplitSibling,
			Message:     "contract-swap execution belongs to bounded selected-frontier fork re-execution and is not implemented by this admission taxonomy",
		},
	}
	blockers := []runfork.RunForkUnsupportedBlocker{}
	hasHistoricalReplayRequirement := false
	hasReplayableDeliveryEvent := false

	for _, item := range evidence.Pending {
		disposition := runForkReplayResumeDispositionForPendingWork(item)
		dispositions = append(dispositions, disposition)
		if item.Classification != runfork.RunForkPendingClassificationDeliveredCompleted {
			hasHistoricalReplayRequirement = true
		}
		if disposition.Disposition == runfork.RunForkReplayResumeDispositionForkReplay {
			hasReplayableDeliveryEvent = true
		}
		if strings.TrimSpace(disposition.BlockerCode) != "" {
			blockers = appendRunForkBlocker(blockers, runForkReplayResumeBlocker(disposition.BlockerCode))
		}
	}
	if len(evidence.Pending) == 0 {
		dispositions = append(dispositions, runfork.RunForkReplayResumeDisposition{
			Fact:        runfork.RunForkReplayResumeFactDeliveryCompletedHistory,
			Disposition: runfork.RunForkReplayResumeDispositionNoHistoricalAction,
			Message:     "no delivery or receipt facts at the fork point require historical replay",
		})
	}
	if evidence.RelevantTimer {
		blocker := runForkReplayResumeBlocker(runfork.RunForkBlockerTimerHistoryUnproven)
		blockers = appendRunForkBlocker(blockers, blocker)
		dispositions = append(dispositions, runfork.RunForkReplayResumeDisposition{
			Fact:        runfork.RunForkReplayResumeFactTimerHistory,
			Disposition: runfork.RunForkReplayResumeDispositionFailClosedBlocker,
			BlockerCode: blocker.Code,
			Message:     blocker.Message,
		})
		hasHistoricalReplayRequirement = true
	}
	if strings.TrimSpace(evidence.RouteHistory.State) == runfork.RunForkRouteHistoryUnknownUnversioned {
		blocker := runForkReplayResumeBlocker(runfork.RunForkBlockerFlowRouteHistoryUnproven)
		blockers = appendRunForkBlocker(blockers, blocker)
		dispositions = append(dispositions, runfork.RunForkReplayResumeDisposition{
			Fact:        runfork.RunForkReplayResumeFactRouteHistory,
			Disposition: runfork.RunForkReplayResumeDispositionFailClosedBlocker,
			BlockerCode: blocker.Code,
			Message:     blocker.Message,
		})
		hasHistoricalReplayRequirement = true
	}
	if evidence.ActiveSession {
		blocker := runForkReplayResumeBlocker(runfork.RunForkBlockerSessionHistoryUnproven)
		blockers = appendRunForkBlocker(blockers, blocker)
		dispositions = append(dispositions, runfork.RunForkReplayResumeDisposition{
			Fact:        runfork.RunForkReplayResumeFactSessionHistory,
			Disposition: runfork.RunForkReplayResumeDispositionFailClosedBlocker,
			BlockerCode: blocker.Code,
			Message:     blocker.Message,
		})
		hasHistoricalReplayRequirement = true
	}
	if evidence.ActiveConversationAudit {
		blocker := runForkReplayResumeBlocker(runfork.RunForkBlockerConversationAuditUnproven)
		blockers = appendRunForkBlocker(blockers, blocker)
		dispositions = append(dispositions, runfork.RunForkReplayResumeDisposition{
			Fact:        runfork.RunForkReplayResumeFactConversationAuditHistory,
			Disposition: runfork.RunForkReplayResumeDispositionFailClosedBlocker,
			BlockerCode: blocker.Code,
			Message:     blocker.Message,
		})
		hasHistoricalReplayRequirement = true
	}
	if evidence.ActiveTurn {
		blocker := runForkReplayResumeBlocker(runfork.RunForkBlockerActiveTurnHistoryUnproven)
		blockers = appendRunForkBlocker(blockers, blocker)
		dispositions = append(dispositions, runfork.RunForkReplayResumeDisposition{
			Fact:        runfork.RunForkReplayResumeFactActiveTurnHistory,
			Disposition: runfork.RunForkReplayResumeDispositionFailClosedBlocker,
			BlockerCode: blocker.Code,
			Message:     blocker.Message,
		})
		hasHistoricalReplayRequirement = true
	}
	if evidence.OpenReplyContext {
		blocker := runForkReplayResumeBlocker(runfork.RunForkBlockerOpenReplyContextUnsupported)
		blockers = appendRunForkBlocker(blockers, blocker)
		dispositions = append(dispositions, runfork.RunForkReplayResumeDisposition{
			Fact:        runfork.RunForkReplayResumeFactOpenReplyContext,
			Disposition: runfork.RunForkReplayResumeDispositionFailClosedBlocker,
			BlockerCode: blocker.Code,
			Message:     blocker.Message,
		})
		hasHistoricalReplayRequirement = true
	}

	deliveryEventReplayReady := hasReplayableDeliveryEvent && len(blockers) == 0
	stateOnlyExecutionReady := len(blockers) == 0 && !hasHistoricalReplayRequirement
	return runfork.RunForkReplayResumeAdmission{
		Owner:                    runfork.RunForkReplayResumeAdmissionOwner,
		StateOnlyExecutionReady:  stateOnlyExecutionReady,
		DeliveryEventReplayReady: deliveryEventReplayReady,
		BoundedReplaySupported:   deliveryEventReplayReady,
		ReplayResumeFactsPresent: hasHistoricalReplayRequirement,
		Dispositions:             dispositions,
		UnsupportedBlockers:      blockers,
	}
}

// runfork.RunForkReplayResumeAdmissionWithSelectedRouteResolution discharges only the
// unversioned route-history blocker after the selected route topology and its
// persisted fork-local recovery have been validated by the caller.

func runForkReplayResumeDispositionForPendingWork(item runfork.RunForkPendingWork) runfork.RunForkReplayResumeDisposition {
	disposition := runfork.RunForkReplayResumeDisposition{
		EventID:        strings.TrimSpace(item.EventID),
		DeliveryID:     strings.TrimSpace(item.DeliveryID),
		SubscriberType: strings.TrimSpace(item.SubscriberType),
		SubscriberID:   strings.TrimSpace(item.SubscriberID),
	}
	switch item.Classification {
	case runfork.RunForkPendingClassificationDeliveredCompleted:
		disposition.Fact = runfork.RunForkReplayResumeFactDeliveryCompletedHistory
		disposition.Disposition = runfork.RunForkReplayResumeDispositionLineageOnly
		disposition.Classification = item.Classification
		disposition.Message = "completed delivery and receipt facts are preserved as source-run lineage/proof only; they are not redelivered into the fork"
		return disposition
	case runfork.RunForkPendingClassificationPending:
		if runForkPendingWorkReplayable(item) {
			disposition.Fact = runfork.RunForkReplayResumeFactDeliveryPendingHistory
			disposition.Disposition = runfork.RunForkReplayResumeDispositionForkReplay
			disposition.Classification = item.Classification
			disposition.Message = "pending unstarted source delivery can be replayed by creating fork-local event and delivery rows with explicit source lineage"
			return disposition
		}
		if runForkPendingWorkIsNonAgent(item) {
			return runForkReplayResumeNonAgentBlocker(item, runfork.RunForkReplayResumeFactDeliveryPendingHistory)
		}
		return runForkReplayResumePendingBlocker(item, runfork.RunForkReplayResumeFactDeliveryPendingHistory)
	case runfork.RunForkPendingClassificationInProgress:
		if runForkPendingWorkIsNonAgent(item) {
			return runForkReplayResumeNonAgentBlocker(item, runfork.RunForkReplayResumeFactDeliveryInProgressHistory)
		}
		return runForkReplayResumePendingBlocker(item, runfork.RunForkReplayResumeFactDeliveryInProgressHistory)
	case runfork.RunForkPendingClassificationFailedRetryable, runfork.RunForkPendingClassificationFailedTerminal:
		if runForkPendingWorkIsNonAgent(item) {
			return runForkReplayResumeNonAgentBlocker(item, runfork.RunForkReplayResumeFactDeliveryFailedHistory)
		}
		return runForkReplayResumePendingBlocker(item, runfork.RunForkReplayResumeFactDeliveryFailedHistory)
	case runfork.RunForkPendingClassificationDeadLetter:
		if runForkPendingWorkIsNonAgent(item) {
			return runForkReplayResumeNonAgentBlocker(item, runfork.RunForkReplayResumeFactDeliveryDeadLetterHistory)
		}
		return runForkReplayResumePendingBlocker(item, runfork.RunForkReplayResumeFactDeliveryDeadLetterHistory)
	case runfork.RunForkPendingClassificationCommittedReplay:
		return runForkReplayResumeCommittedReplayScopeBlocker(item)
	default:
		if runForkPendingWorkIsNonAgent(item) {
			return runForkReplayResumeNonAgentBlocker(item, runfork.RunForkReplayResumeFactDeliveryPendingHistory)
		}
		return runForkReplayResumePendingBlocker(item, runfork.RunForkReplayResumeFactDeliveryPendingHistory)
	}
}

// runfork.RunForkPendingWorkReplayableForHistoricalReplay is the shared taxonomy predicate
// consumed by the runtime historical replay owner before any fork-local mutation.

func runForkPendingWorkReplayable(item runfork.RunForkPendingWork) bool {
	return runfork.RunForkPendingWorkReplayableForHistoricalReplay(item)
}

func runForkPendingWorkIsNonAgent(item runfork.RunForkPendingWork) bool {
	return strings.TrimSpace(item.SubscriberType) != "agent"
}

func runForkReplayResumePendingBlocker(item runfork.RunForkPendingWork, fact string) runfork.RunForkReplayResumeDisposition {
	blocker := runForkReplayResumeBlocker(runfork.RunForkBlockerDeliveryHistoryUnproven)
	return runfork.RunForkReplayResumeDisposition{
		Fact:           fact,
		Disposition:    runfork.RunForkReplayResumeDispositionFailClosedBlocker,
		BlockerCode:    blocker.Code,
		Classification: item.Classification,
		EventID:        strings.TrimSpace(item.EventID),
		DeliveryID:     strings.TrimSpace(item.DeliveryID),
		SubscriberType: strings.TrimSpace(item.SubscriberType),
		SubscriberID:   strings.TrimSpace(item.SubscriberID),
		Message:        blocker.Message,
	}
}

func runForkReplayResumeNonAgentBlocker(item runfork.RunForkPendingWork, fact string) runfork.RunForkReplayResumeDisposition {
	blocker := runForkReplayResumeBlocker(runfork.RunForkBlockerNonAgentDeliveryReplayUnsupported)
	return runfork.RunForkReplayResumeDisposition{
		Fact:           fact,
		Disposition:    runfork.RunForkReplayResumeDispositionFailClosedBlocker,
		BlockerCode:    blocker.Code,
		Classification: item.Classification,
		EventID:        strings.TrimSpace(item.EventID),
		DeliveryID:     strings.TrimSpace(item.DeliveryID),
		SubscriberType: strings.TrimSpace(item.SubscriberType),
		SubscriberID:   strings.TrimSpace(item.SubscriberID),
		Message:        blocker.Message,
	}
}

func runForkReplayResumeCommittedReplayScopeBlocker(item runfork.RunForkPendingWork) runfork.RunForkReplayResumeDisposition {
	blocker := runForkReplayResumeBlocker(runfork.RunForkBlockerCommittedReplayScopeReplayUnsupported)
	return runfork.RunForkReplayResumeDisposition{
		Fact:           runfork.RunForkReplayResumeFactCommittedReplayScope,
		Disposition:    runfork.RunForkReplayResumeDispositionFailClosedBlocker,
		BlockerCode:    blocker.Code,
		Classification: item.Classification,
		EventID:        strings.TrimSpace(item.EventID),
		DeliveryID:     strings.TrimSpace(item.DeliveryID),
		SubscriberType: strings.TrimSpace(item.SubscriberType),
		SubscriberID:   strings.TrimSpace(item.SubscriberID),
		Message:        blocker.Message,
	}
}

func runForkReplayResumeAdmissionWithBlocker(admission runfork.RunForkReplayResumeAdmission, fact string, blocker runfork.RunForkUnsupportedBlocker) runfork.RunForkReplayResumeAdmission {
	if strings.TrimSpace(admission.Owner) == "" {
		admission.Owner = runfork.RunForkReplayResumeAdmissionOwner
	}
	admission.StateOnlyExecutionReady = false
	admission.DeliveryEventReplayReady = false
	admission.BoundedReplaySupported = false
	admission.ReplayResumeFactsPresent = true
	admission.UnsupportedBlockers = appendRunForkBlocker(admission.UnsupportedBlockers, blocker)
	admission.Dispositions = append(admission.Dispositions, runfork.RunForkReplayResumeDisposition{
		Fact:        fact,
		Disposition: runfork.RunForkReplayResumeDispositionFailClosedBlocker,
		BlockerCode: blocker.Code,
		Message:     blocker.Message,
	})
	return admission
}

type runForkReplayResumeBlockerError struct {
	blocker runfork.RunForkUnsupportedBlocker
	fact    string
	message string
}

func (e runForkReplayResumeBlockerError) Error() string {
	return e.message
}

func runForkReplayResumeError(code, fact, message string) error {
	return runForkReplayResumeBlockerError{
		blocker: runfork.RunForkUnsupportedBlocker{
			Code:    strings.TrimSpace(code),
			Message: strings.TrimSpace(message),
		},
		fact:    strings.TrimSpace(fact),
		message: strings.TrimSpace(message),
	}
}

func runForkReplayResumeBlockerFromError(err error) (runfork.RunForkUnsupportedBlocker, string, bool) {
	var blockerErr runForkReplayResumeBlockerError
	if !errors.As(err, &blockerErr) {
		return runfork.RunForkUnsupportedBlocker{}, "", false
	}
	return blockerErr.blocker, blockerErr.fact, true
}

func RunForkReplayResumeBlockerFromError(err error) (runfork.RunForkUnsupportedBlocker, string, bool) {
	return runForkReplayResumeBlockerFromError(err)
}

func runForkReplayResumeBlocker(code string) runfork.RunForkUnsupportedBlocker {
	switch strings.TrimSpace(code) {
	case runfork.RunForkBlockerDeliveryHistoryUnproven:
		return runfork.RunForkUnsupportedBlocker{
			Code:    runfork.RunForkBlockerDeliveryHistoryUnproven,
			Message: "event_deliveries stores current delivery state; arbitrary historical delivery transitions at the fork point are not append-only proven",
		}
	case runfork.RunForkBlockerNonAgentDeliveryReplayUnsupported:
		return runfork.RunForkUnsupportedBlocker{
			Code:    runfork.RunForkBlockerNonAgentDeliveryReplayUnsupported,
			Message: "non-agent delivery replay requires runtime handler, idempotency, receipt, route, and side-effect semantics and is not supported by the fixed-revision delivery/event replay primitive",
		}
	case runfork.RunForkBlockerCommittedReplayScopeReplayUnsupported:
		return runfork.RunForkUnsupportedBlocker{
			Code:    runfork.RunForkBlockerCommittedReplayScopeReplayUnsupported,
			Message: "committed replay-scope marker rows are same-run replay proof and are not node work to replay into a fixed-revision fork",
		}
	case runfork.RunForkBlockerTimerHistoryUnproven:
		return runfork.RunForkUnsupportedBlocker{
			Code:    runfork.RunForkBlockerTimerHistoryUnproven,
			Message: "timers are current-state rows and timer creation/cancellation is not represented in the mutation log",
		}
	case runfork.RunForkBlockerFlowRouteHistoryUnproven:
		return runfork.RunForkUnsupportedBlocker{
			Code:    runfork.RunForkBlockerFlowRouteHistoryUnproven,
			Message: "routing_rules are current-state rows and cannot prove historical flow-route membership at the fork point",
		}
	case runfork.RunForkBlockerSessionHistoryUnproven:
		return runfork.RunForkUnsupportedBlocker{
			Code:    runfork.RunForkBlockerSessionHistoryUnproven,
			Message: "source-run session facts reference current session rows without append-only session-state proof at the fork point",
		}
	case runfork.RunForkBlockerConversationAuditUnproven:
		return runfork.RunForkUnsupportedBlocker{
			Code:    runfork.RunForkBlockerConversationAuditUnproven,
			Message: "source-run task conversation audit facts do not carry append-only termination proof at the fork point and are not a fork-local session reconstruction source",
		}
	case runfork.RunForkBlockerActiveTurnHistoryUnproven:
		return runfork.RunForkUnsupportedBlocker{
			Code:    runfork.RunForkBlockerActiveTurnHistoryUnproven,
			Message: "active turn ownership at the fork point cannot be proven from current session/turn rows alone",
		}
	case runfork.RunForkBlockerEntitySnapshotMetadataUnproven:
		return runfork.RunForkUnsupportedBlocker{
			Code:    runfork.RunForkBlockerEntitySnapshotMetadataUnproven,
			Message: "fork materialization requires owner-authorized entity snapshot metadata at the selected revision for every reconstructed entity",
		}
	case runfork.RunForkBlockerOpenReplyContextUnsupported:
		return runfork.RunForkUnsupportedBlocker{
			Code:    runfork.RunForkBlockerOpenReplyContextUnsupported,
			Message: "fixed-revision fork is blocked because the source run has an open reply context at the selected revision; complete or cancel the request before forking",
		}
	default:
		code = strings.TrimSpace(code)
		if code == "" {
			code = "fork_reexecution_unproven"
		}
		return runfork.RunForkUnsupportedBlocker{
			Code:    code,
			Message: fmt.Sprintf("fixed-revision bounded re-execution is not proven for %s by the canonical admission taxonomy", code),
		}
	}
}

func appendRunForkBlocker(blockers []runfork.RunForkUnsupportedBlocker, blocker runfork.RunForkUnsupportedBlocker) []runfork.RunForkUnsupportedBlocker {
	if strings.TrimSpace(blocker.Code) == "" {
		return blockers
	}
	for _, existing := range blockers {
		if existing.Code == blocker.Code {
			return blockers
		}
	}
	return append(blockers, blocker)
}
