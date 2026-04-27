package store

import (
	"errors"
	"fmt"
	"strings"
)

const (
	RunForkReplayResumeAdmissionOwner = "store.run_fork.replay_resume_admission"

	RunForkReplayResumeDispositionReconstruct        = "reconstruct"
	RunForkReplayResumeDispositionForkReplay         = "fork_replay"
	RunForkReplayResumeDispositionLineageOnly        = "lineage_only"
	RunForkReplayResumeDispositionFailClosedBlocker  = "fail_closed_blocker"
	RunForkReplayResumeDispositionSplitSibling       = "split_sibling"
	RunForkReplayResumeDispositionNoHistoricalAction = "no_historical_action"
)

const (
	RunForkReplayResumeFactEntityStateSnapshot       = "entity_state_snapshot"
	RunForkReplayResumeFactDeliveryCompletedHistory  = "delivery_completed_history"
	RunForkReplayResumeFactDeliveryPendingHistory    = "delivery_pending_history"
	RunForkReplayResumeFactDeliveryInProgressHistory = "delivery_in_progress_history"
	RunForkReplayResumeFactDeliveryFailedHistory     = "delivery_failed_history"
	RunForkReplayResumeFactDeliveryDeadLetterHistory = "delivery_dead_letter_history"
	RunForkReplayResumeFactCommittedReplayScope      = "committed_replay_scope"
	RunForkReplayResumeFactTimerHistory              = "timer_history"
	RunForkReplayResumeFactRouteHistory              = "flow_route_history"
	RunForkReplayResumeFactSessionHistory            = "session_history"
	RunForkReplayResumeFactActiveTurnHistory         = "active_turn_history"
	RunForkReplayResumeFactSourceAdvanced            = "source_advanced_after_fork_point"
	RunForkReplayResumeFactForkReplayState           = "fork_replay_state"
	RunForkReplayResumeFactContractSwap              = "contract_swap"
	RunForkReplayResumeFactHistoricalReplayExecution = "historical_replay_execution"
)

const (
	RunForkBlockerDeliveryHistoryUnproven   = "delivery_history_unproven"
	RunForkBlockerTimerHistoryUnproven      = "timer_history_unproven"
	RunForkBlockerFlowRouteHistoryUnproven  = "flow_route_history_unproven"
	RunForkBlockerSessionHistoryUnproven    = "session_history_unproven"
	RunForkBlockerActiveTurnHistoryUnproven = "active_turn_history_unproven"
)

type RunForkReplayResumeAdmission struct {
	Owner                     string                           `json:"owner"`
	StateOnlyExecutionReady   bool                             `json:"state_only_execution_ready"`
	DeliveryEventReplayReady  bool                             `json:"delivery_event_replay_ready"`
	HistoricalReplaySupported bool                             `json:"historical_replay_supported"`
	HistoricalReplayRequired  bool                             `json:"historical_replay_required"`
	Dispositions              []RunForkReplayResumeDisposition `json:"dispositions,omitempty"`
	UnsupportedBlockers       []RunForkUnsupportedBlocker      `json:"unsupported_blockers,omitempty"`
}

type RunForkReplayResumeDisposition struct {
	Fact           string `json:"fact"`
	Disposition    string `json:"disposition"`
	BlockerCode    string `json:"blocker_code,omitempty"`
	Classification string `json:"classification,omitempty"`
	Message        string `json:"message"`
}

func runForkReplayResumeAdmission(evidence runForkAdmissionEvidence) RunForkReplayResumeAdmission {
	dispositions := []RunForkReplayResumeDisposition{
		{
			Fact:        RunForkReplayResumeFactEntityStateSnapshot,
			Disposition: RunForkReplayResumeDispositionReconstruct,
			Message:     "fork current-state snapshots are reconstructed from entity_mutations and may be materialized by the state-only fork owner",
		},
		{
			Fact:        RunForkReplayResumeFactHistoricalReplayExecution,
			Disposition: RunForkReplayResumeDispositionSplitSibling,
			Message:     "historical replay execution remains a separate gated child; this admission taxonomy is non-mutating",
		},
		{
			Fact:        RunForkReplayResumeFactContractSwap,
			Disposition: RunForkReplayResumeDispositionSplitSibling,
			Message:     "contract-swap execution belongs to full historical resume and is not implemented by this admission taxonomy",
		},
	}
	blockers := []RunForkUnsupportedBlocker{}
	hasHistoricalReplayRequirement := false
	hasReplayableDeliveryEvent := false

	for _, item := range evidence.Pending {
		disposition := runForkReplayResumeDispositionForPendingWork(item)
		dispositions = append(dispositions, disposition)
		if item.Classification != RunForkPendingClassificationDeliveredCompleted {
			hasHistoricalReplayRequirement = true
		}
		if disposition.Disposition == RunForkReplayResumeDispositionForkReplay {
			hasReplayableDeliveryEvent = true
		}
		if strings.TrimSpace(disposition.BlockerCode) != "" {
			blockers = appendRunForkBlocker(blockers, runForkReplayResumeBlocker(disposition.BlockerCode))
		}
	}
	if len(evidence.Pending) == 0 {
		dispositions = append(dispositions, RunForkReplayResumeDisposition{
			Fact:        RunForkReplayResumeFactDeliveryCompletedHistory,
			Disposition: RunForkReplayResumeDispositionNoHistoricalAction,
			Message:     "no delivery or receipt facts at the fork point require historical replay",
		})
	}
	if evidence.RelevantTimer {
		blocker := runForkReplayResumeBlocker(RunForkBlockerTimerHistoryUnproven)
		blockers = appendRunForkBlocker(blockers, blocker)
		dispositions = append(dispositions, RunForkReplayResumeDisposition{
			Fact:        RunForkReplayResumeFactTimerHistory,
			Disposition: RunForkReplayResumeDispositionFailClosedBlocker,
			BlockerCode: blocker.Code,
			Message:     blocker.Message,
		})
		hasHistoricalReplayRequirement = true
	}
	if evidence.RelevantRoute {
		blocker := runForkReplayResumeBlocker(RunForkBlockerFlowRouteHistoryUnproven)
		blockers = appendRunForkBlocker(blockers, blocker)
		dispositions = append(dispositions, RunForkReplayResumeDisposition{
			Fact:        RunForkReplayResumeFactRouteHistory,
			Disposition: RunForkReplayResumeDispositionFailClosedBlocker,
			BlockerCode: blocker.Code,
			Message:     blocker.Message,
		})
		hasHistoricalReplayRequirement = true
	}
	if evidence.ActiveSession {
		blocker := runForkReplayResumeBlocker(RunForkBlockerSessionHistoryUnproven)
		blockers = appendRunForkBlocker(blockers, blocker)
		dispositions = append(dispositions, RunForkReplayResumeDisposition{
			Fact:        RunForkReplayResumeFactSessionHistory,
			Disposition: RunForkReplayResumeDispositionFailClosedBlocker,
			BlockerCode: blocker.Code,
			Message:     blocker.Message,
		})
		hasHistoricalReplayRequirement = true
	}
	if evidence.ActiveTurn {
		blocker := runForkReplayResumeBlocker(RunForkBlockerActiveTurnHistoryUnproven)
		blockers = appendRunForkBlocker(blockers, blocker)
		dispositions = append(dispositions, RunForkReplayResumeDisposition{
			Fact:        RunForkReplayResumeFactActiveTurnHistory,
			Disposition: RunForkReplayResumeDispositionFailClosedBlocker,
			BlockerCode: blocker.Code,
			Message:     blocker.Message,
		})
		hasHistoricalReplayRequirement = true
	}

	deliveryEventReplayReady := hasReplayableDeliveryEvent && len(blockers) == 0
	stateOnlyExecutionReady := len(blockers) == 0 && !hasHistoricalReplayRequirement
	return RunForkReplayResumeAdmission{
		Owner:                     RunForkReplayResumeAdmissionOwner,
		StateOnlyExecutionReady:   stateOnlyExecutionReady,
		DeliveryEventReplayReady:  deliveryEventReplayReady,
		HistoricalReplaySupported: deliveryEventReplayReady,
		HistoricalReplayRequired:  hasHistoricalReplayRequirement,
		Dispositions:              dispositions,
		UnsupportedBlockers:       blockers,
	}
}

func runForkReplayResumeDispositionForPendingWork(item RunForkPendingWork) RunForkReplayResumeDisposition {
	switch item.Classification {
	case RunForkPendingClassificationDeliveredCompleted:
		return RunForkReplayResumeDisposition{
			Fact:           RunForkReplayResumeFactDeliveryCompletedHistory,
			Disposition:    RunForkReplayResumeDispositionLineageOnly,
			Classification: item.Classification,
			Message:        "completed delivery and receipt facts are preserved as source-run lineage/proof only; they are not redelivered into the fork",
		}
	case RunForkPendingClassificationPending:
		if runForkPendingWorkReplayable(item) {
			return RunForkReplayResumeDisposition{
				Fact:           RunForkReplayResumeFactDeliveryPendingHistory,
				Disposition:    RunForkReplayResumeDispositionForkReplay,
				Classification: item.Classification,
				Message:        "pending unstarted source delivery can be replayed by creating fork-local event and delivery rows with explicit source lineage",
			}
		}
		return runForkReplayResumePendingBlocker(item, RunForkReplayResumeFactDeliveryPendingHistory)
	case RunForkPendingClassificationInProgress:
		return runForkReplayResumePendingBlocker(item, RunForkReplayResumeFactDeliveryInProgressHistory)
	case RunForkPendingClassificationFailedRetryable, RunForkPendingClassificationFailedTerminal:
		return runForkReplayResumePendingBlocker(item, RunForkReplayResumeFactDeliveryFailedHistory)
	case RunForkPendingClassificationDeadLetter:
		return runForkReplayResumePendingBlocker(item, RunForkReplayResumeFactDeliveryDeadLetterHistory)
	case RunForkPendingClassificationCommittedReplay:
		return runForkReplayResumePendingBlocker(item, RunForkReplayResumeFactCommittedReplayScope)
	default:
		return runForkReplayResumePendingBlocker(item, RunForkReplayResumeFactDeliveryPendingHistory)
	}
}

func runForkPendingWorkReplayable(item RunForkPendingWork) bool {
	if item.Classification != RunForkPendingClassificationPending {
		return false
	}
	if strings.TrimSpace(item.DeliveryID) == "" || strings.TrimSpace(item.SubscriberID) == "" {
		return false
	}
	if strings.TrimSpace(item.SubscriberType) != "agent" {
		return false
	}
	if strings.TrimSpace(item.Status) != "pending" || item.RetryCount != 0 {
		return false
	}
	return strings.TrimSpace(item.ActiveSessionID) == "" &&
		item.StartedAt == nil &&
		item.DeliveredAt == nil &&
		item.ReceiptAt == nil
}

func runForkReplayResumePendingBlocker(item RunForkPendingWork, fact string) RunForkReplayResumeDisposition {
	blocker := runForkReplayResumeBlocker(RunForkBlockerDeliveryHistoryUnproven)
	return RunForkReplayResumeDisposition{
		Fact:           fact,
		Disposition:    RunForkReplayResumeDispositionFailClosedBlocker,
		BlockerCode:    blocker.Code,
		Classification: item.Classification,
		Message:        blocker.Message,
	}
}

func runForkReplayResumeAdmissionWithBlocker(admission RunForkReplayResumeAdmission, fact string, blocker RunForkUnsupportedBlocker) RunForkReplayResumeAdmission {
	if strings.TrimSpace(admission.Owner) == "" {
		admission.Owner = RunForkReplayResumeAdmissionOwner
	}
	admission.StateOnlyExecutionReady = false
	admission.DeliveryEventReplayReady = false
	admission.HistoricalReplaySupported = false
	admission.HistoricalReplayRequired = true
	admission.UnsupportedBlockers = appendRunForkBlocker(admission.UnsupportedBlockers, blocker)
	admission.Dispositions = append(admission.Dispositions, RunForkReplayResumeDisposition{
		Fact:        fact,
		Disposition: RunForkReplayResumeDispositionFailClosedBlocker,
		BlockerCode: blocker.Code,
		Message:     blocker.Message,
	})
	return admission
}

type runForkReplayResumeBlockerError struct {
	blocker RunForkUnsupportedBlocker
	fact    string
	message string
}

func (e runForkReplayResumeBlockerError) Error() string {
	return e.message
}

func runForkReplayResumeError(code, fact, message string) error {
	return runForkReplayResumeBlockerError{
		blocker: RunForkUnsupportedBlocker{
			Code:    strings.TrimSpace(code),
			Message: strings.TrimSpace(message),
		},
		fact:    strings.TrimSpace(fact),
		message: strings.TrimSpace(message),
	}
}

func runForkReplayResumeBlockerFromError(err error) (RunForkUnsupportedBlocker, string, bool) {
	var blockerErr runForkReplayResumeBlockerError
	if !errors.As(err, &blockerErr) {
		return RunForkUnsupportedBlocker{}, "", false
	}
	return blockerErr.blocker, blockerErr.fact, true
}

func runForkReplayResumeBlocker(code string) RunForkUnsupportedBlocker {
	switch strings.TrimSpace(code) {
	case RunForkBlockerDeliveryHistoryUnproven:
		return RunForkUnsupportedBlocker{
			Code:    RunForkBlockerDeliveryHistoryUnproven,
			Message: "event_deliveries stores current delivery state; arbitrary historical delivery transitions at the fork point are not append-only proven",
		}
	case RunForkBlockerTimerHistoryUnproven:
		return RunForkUnsupportedBlocker{
			Code:    RunForkBlockerTimerHistoryUnproven,
			Message: "timers are current-state rows and timer creation/cancellation is not represented in the mutation log",
		}
	case RunForkBlockerFlowRouteHistoryUnproven:
		return RunForkUnsupportedBlocker{
			Code:    RunForkBlockerFlowRouteHistoryUnproven,
			Message: "routing_rules are current-state rows and cannot prove historical flow-route membership at the fork point",
		}
	case RunForkBlockerSessionHistoryUnproven:
		return RunForkUnsupportedBlocker{
			Code:    RunForkBlockerSessionHistoryUnproven,
			Message: "source-run session facts reference current session rows without append-only session-state proof at the fork point",
		}
	case RunForkBlockerActiveTurnHistoryUnproven:
		return RunForkUnsupportedBlocker{
			Code:    RunForkBlockerActiveTurnHistoryUnproven,
			Message: "active turn ownership at the fork point cannot be proven from current session/turn rows alone",
		}
	default:
		code = strings.TrimSpace(code)
		if code == "" {
			code = "historical_replay_unproven"
		}
		return RunForkUnsupportedBlocker{
			Code:    code,
			Message: fmt.Sprintf("timestamp-fork historical replay/resume is not proven for %s by the canonical admission taxonomy", code),
		}
	}
}

func appendRunForkBlocker(blockers []RunForkUnsupportedBlocker, blocker RunForkUnsupportedBlocker) []RunForkUnsupportedBlocker {
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
