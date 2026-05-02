package runforkexecution

import (
	"fmt"
	"strings"

	"swarm/internal/store"
)

type HistoricalReplayExecutionAdmissionRequest struct {
	ReplayResumeAdmission      store.RunForkReplayResumeAdmission
	SelectedExecutionAdmission store.RunForkSelectedContractExecutionAdmission
	ContractSwapAdmission      store.RunForkContractSwapBootResumeAdmission
	RouteRecovery              *store.RunForkSelectedContractRouteRecovery
}

func BuildHistoricalReplayExecutionAdmission(req HistoricalReplayExecutionAdmissionRequest) (store.RunForkHistoricalReplayExecutionAdmission, error) {
	replayAdmission := req.ReplayResumeAdmission
	if strings.TrimSpace(replayAdmission.Owner) != store.RunForkReplayResumeAdmissionOwner {
		return store.RunForkHistoricalReplayExecutionAdmission{}, fmt.Errorf("historical replay execution admission requires %s; got %q", store.RunForkReplayResumeAdmissionOwner, replayAdmission.Owner)
	}
	selectedAdmission := req.SelectedExecutionAdmission
	if err := validateContractSwapSelectedExecutionAdmission(selectedAdmission); err != nil {
		return store.RunForkHistoricalReplayExecutionAdmission{}, fmt.Errorf("historical replay execution admission selected prerequisite: %w", err)
	}
	contractSwapAdmission := req.ContractSwapAdmission
	if err := validateHistoricalReplayContractSwapAdmission(selectedAdmission, replayAdmission, contractSwapAdmission); err != nil {
		return store.RunForkHistoricalReplayExecutionAdmission{}, err
	}
	if req.RouteRecovery != nil {
		if err := validateContractSwapRouteRecovery(selectedAdmission, *req.RouteRecovery); err != nil {
			return store.RunForkHistoricalReplayExecutionAdmission{}, fmt.Errorf("historical replay execution admission route recovery prerequisite: %w", err)
		}
	}

	blockers := []store.RunForkUnsupportedBlocker{}
	for _, blocker := range replayAdmission.UnsupportedBlockers {
		blockers = appendRunForkUnsupportedBlocker(blockers, blocker)
	}
	for _, blocker := range selectedAdmission.UnsupportedBlockers {
		blockers = appendRunForkUnsupportedBlocker(blockers, blocker)
	}
	for _, blocker := range contractSwapAdmission.UnsupportedBlockers {
		blockers = appendRunForkUnsupportedBlocker(blockers, blocker)
	}
	blockers = appendRunForkUnsupportedBlocker(blockers, store.RunForkUnsupportedBlocker{
		Code:    store.RunForkBlockerHistoricalReplayExecutionAdmissionNonMutating,
		Message: "historical replay execution admission is non-mutating; full replay/resume mutation remains separately gated",
	})

	var routeTopologyOwner, recipientPlanningOwner string
	if selectedAdmission.RouteTopology != nil {
		routeTopologyOwner = selectedAdmission.RouteTopology.Owner
	}
	if selectedAdmission.RecipientPlanning != nil {
		recipientPlanningOwner = selectedAdmission.RecipientPlanning.Owner
	}
	var routeRecoveryOwner, runtimeRouteRecoveryOwner string
	if req.RouteRecovery != nil {
		routeRecoveryOwner = req.RouteRecovery.Owner
		runtimeRouteRecoveryOwner = req.RouteRecovery.RuntimeRecoveryOwner
	}
	selection := selectedAdmission.ContractSelection

	return store.RunForkHistoricalReplayExecutionAdmission{
		Owner:                           store.RunForkHistoricalReplayExecutionAdmissionOwner,
		NonMutating:                     true,
		ExecutionSupported:              false,
		FutureExecutionOwner:            store.RunForkHistoricalReplayExecutionOwner,
		ForkRunID:                       selectedAdmission.ForkRunID,
		SourceRunID:                     selectedAdmission.SourceRunID,
		ForkEventID:                     selectedAdmission.ForkEventID,
		ContractSelection:               &selection,
		ReplayResumeAdmissionOwner:      replayAdmission.Owner,
		SelectedExecutionAdmissionOwner: selectedAdmission.Owner,
		SelectedBindingOwner:            selectedAdmission.ContractBindingOwner,
		RouteTopologyOwner:              routeTopologyOwner,
		RouteRecoveryOwner:              routeRecoveryOwner,
		RuntimeRouteRecoveryOwner:       runtimeRouteRecoveryOwner,
		RecipientPlanningOwner:          recipientPlanningOwner,
		ContractSwapAdmissionOwner:      contractSwapAdmission.Owner,
		FactAdmissions:                  historicalReplayFactAdmissions(replayAdmission),
		Prerequisites:                   historicalReplayPrerequisites(req.RouteRecovery != nil),
		RequiredConsumers:               historicalReplayRequiredConsumers(),
		BlockedSiblings:                 historicalReplayBlockedSiblings(),
		InvalidPaths:                    historicalReplayInvalidPaths(),
		UnsupportedBlockers:             blockers,
	}, nil
}

func validateHistoricalReplayContractSwapAdmission(
	selectedAdmission store.RunForkSelectedContractExecutionAdmission,
	replayAdmission store.RunForkReplayResumeAdmission,
	admission store.RunForkContractSwapBootResumeAdmission,
) error {
	if strings.TrimSpace(admission.Owner) != store.RunForkContractSwapBootResumeAdmissionOwner {
		return fmt.Errorf("historical replay execution admission requires %s; got %q", store.RunForkContractSwapBootResumeAdmissionOwner, admission.Owner)
	}
	if !admission.NonMutating {
		return fmt.Errorf("historical replay execution admission requires non-mutating contract-swap admission")
	}
	if admission.BootResumeSupported {
		return fmt.Errorf("historical replay execution admission cannot consume mutating contract-swap admission")
	}
	if strings.TrimSpace(admission.SelectedExecutionAdmissionOwner) != selectedAdmission.Owner ||
		strings.TrimSpace(admission.ReplayResumeAdmissionOwner) != replayAdmission.Owner {
		return fmt.Errorf("historical replay execution admission contract-swap prerequisite owner mismatch")
	}
	if strings.TrimSpace(admission.ForkRunID) != strings.TrimSpace(selectedAdmission.ForkRunID) ||
		strings.TrimSpace(admission.SourceRunID) != strings.TrimSpace(selectedAdmission.SourceRunID) ||
		strings.TrimSpace(admission.ForkEventID) != strings.TrimSpace(selectedAdmission.ForkEventID) {
		return fmt.Errorf("historical replay execution admission contract-swap identity does not match selected execution admission")
	}
	return nil
}

func historicalReplayFactAdmissions(replay store.RunForkReplayResumeAdmission) []store.RunForkHistoricalReplayFactAdmission {
	return []store.RunForkHistoricalReplayFactAdmission{
		{
			Fact:        store.RunForkHistoricalReplayFactSourceEvents,
			Admission:   store.RunForkHistoricalReplayAdmissionLineageOnlyEvidence,
			SourceOwner: "events",
			Message:     "source events are payload and lineage evidence only; future replay must mint fresh fork-local events under the fork run_id",
		},
		historicalReplayEventDeliveriesAdmission(replay),
		historicalReplayLineageFact(store.RunForkHistoricalReplayFactReceipts, "event_receipts", "source receipts are outcome lineage only and cannot suppress future fork-local work"),
		historicalReplayDeadLettersAdmission(replay),
		historicalReplaySplitFact(store.RunForkHistoricalReplayFactRetryIdempotency, "runtime idempotency and retry state must be owned by a later mutating replay child; source state cannot suppress fork work", "#564"),
		historicalReplaySplitFact(store.RunForkHistoricalReplayFactEmittedFollowUps, "emitted follow-up regeneration belongs to the future mutating replay owner; source follow-up rows are not copied", "#564"),
		historicalReplayFactFromReplay(replay, store.RunForkHistoricalReplayFactTimers, []string{store.RunForkReplayResumeFactTimerHistory}, store.RunForkHistoricalReplayAdmissionSplitSibling, "timer reconstruction remains a split sibling unless timer_history is present and fail-closed", "#564"),
		historicalReplayFactFromReplay(replay, store.RunForkHistoricalReplayFactRoutes, []string{store.RunForkReplayResumeFactRouteHistory}, store.RunForkHistoricalReplayAdmissionSplitSibling, "route and route-recovery truth remains split under fork-local route persistence/runtime recovery", "#618"),
		historicalReplayFactFromReplay(replay, store.RunForkHistoricalReplayFactSessions, []string{store.RunForkReplayResumeFactSessionHistory}, store.RunForkHistoricalReplayAdmissionSplitSibling, "session reconstruction remains a split sibling unless active session facts are present and fail-closed", "#564"),
		historicalReplayFactFromReplay(replay, store.RunForkHistoricalReplayFactTurns, []string{store.RunForkReplayResumeFactActiveTurnHistory}, store.RunForkHistoricalReplayAdmissionSplitSibling, "turn reconstruction remains a split sibling unless active turn facts are present and fail-closed", "#564"),
		historicalReplayFactFromReplay(replay, store.RunForkHistoricalReplayFactAudits, []string{store.RunForkReplayResumeFactConversationAuditHistory}, store.RunForkHistoricalReplayAdmissionSplitSibling, "task conversation audit reconstruction remains a split sibling unless audit facts are present and fail-closed", "#564"),
		historicalReplayNonAgentAdmission(replay),
		historicalReplayFactFromReplay(replay, store.RunForkHistoricalReplayFactSourceAdvancedPostTFacts, []string{store.RunForkReplayResumeFactSourceAdvanced}, store.RunForkHistoricalReplayAdmissionSplitSibling, "source-advanced and post-T source outcomes remain source-run evidence and cannot suppress fork-local replay", "#564"),
		historicalReplaySplitFact(store.RunForkHistoricalReplayFactRuntimeRestartRecovery, "runtime restart recovery remains a consumer/sibling and cannot reconstruct historical replay state from current rows", "#564"),
		historicalReplaySplitFact(store.RunForkHistoricalReplayFactCLIApiDashboardOperator, "CLI, API, dashboard, and Builder surfaces are consumers only and must not compute historical replay admission independently", "#549"),
	}
}

func historicalReplayEventDeliveriesAdmission(replay store.RunForkReplayResumeAdmission) store.RunForkHistoricalReplayFactAdmission {
	facts := []string{
		store.RunForkReplayResumeFactDeliveryPendingHistory,
		store.RunForkReplayResumeFactDeliveryInProgressHistory,
		store.RunForkReplayResumeFactDeliveryFailedHistory,
		store.RunForkReplayResumeFactDeliveryDeadLetterHistory,
		store.RunForkReplayResumeFactCommittedReplayScope,
	}
	if blocker, ok := replayBlockerForFacts(replay, facts...); ok {
		return store.RunForkHistoricalReplayFactAdmission{
			Fact:        store.RunForkHistoricalReplayFactEventDeliveries,
			Admission:   store.RunForkHistoricalReplayAdmissionFailClosedBlocker,
			SourceOwner: store.RunForkReplayResumeAdmissionOwner,
			BlockerCode: blocker.Code,
			Message:     blocker.Message,
		}
	}
	if replay.DeliveryEventReplayReady || replayDispositionHas(replay, store.RunForkReplayResumeFactDeliveryPendingHistory, store.RunForkReplayResumeDispositionForkReplay) {
		return store.RunForkHistoricalReplayFactAdmission{
			Fact:        store.RunForkHistoricalReplayFactEventDeliveries,
			Admission:   store.RunForkHistoricalReplayAdmissionExecutableForkWork,
			SourceOwner: store.RunForkReplayResumeAdmissionOwner,
			Message:     "only the existing delivery_event_replay_ready primitive is admitted as future executable fork work; this admission does not create event or delivery rows",
		}
	}
	return historicalReplayLineageFact(store.RunForkHistoricalReplayFactEventDeliveries, store.RunForkReplayResumeAdmissionOwner, "source delivery history is lineage/no-op evidence unless the canonical replay taxonomy admits the pending unstarted agent-delivery primitive")
}

func historicalReplayDeadLettersAdmission(replay store.RunForkReplayResumeAdmission) store.RunForkHistoricalReplayFactAdmission {
	if blocker, ok := replayBlockerForFacts(replay, store.RunForkReplayResumeFactDeliveryDeadLetterHistory); ok {
		return store.RunForkHistoricalReplayFactAdmission{
			Fact:        store.RunForkHistoricalReplayFactDeadLetters,
			Admission:   store.RunForkHistoricalReplayAdmissionFailClosedBlocker,
			SourceOwner: store.RunForkReplayResumeAdmissionOwner,
			BlockerCode: blocker.Code,
			Message:     blocker.Message,
		}
	}
	return historicalReplayLineageFact(store.RunForkHistoricalReplayFactDeadLetters, "event_deliveries", "source dead letters are terminal source-run outcome evidence only and cannot suppress future fork-local work")
}

func historicalReplayNonAgentAdmission(replay store.RunForkReplayResumeAdmission) store.RunForkHistoricalReplayFactAdmission {
	for _, blocker := range replay.UnsupportedBlockers {
		if strings.TrimSpace(blocker.Code) == store.RunForkBlockerNonAgentDeliveryReplayUnsupported {
			return store.RunForkHistoricalReplayFactAdmission{
				Fact:        store.RunForkHistoricalReplayFactNonAgentNodeSystemWork,
				Admission:   store.RunForkHistoricalReplayAdmissionFailClosedBlocker,
				SourceOwner: store.RunForkReplayResumeAdmissionOwner,
				BlockerCode: blocker.Code,
				Message:     blocker.Message,
			}
		}
	}
	return historicalReplaySplitFact(store.RunForkHistoricalReplayFactNonAgentNodeSystemWork, "node, system, platform, and non-agent delivery replay requires a separate handler/idempotency/receipt owner", "#564")
}

func historicalReplayFactFromReplay(replay store.RunForkReplayResumeAdmission, fact string, replayFacts []string, fallbackAdmission, fallbackMessage, tracker string) store.RunForkHistoricalReplayFactAdmission {
	if blocker, ok := replayBlockerForFacts(replay, replayFacts...); ok {
		return store.RunForkHistoricalReplayFactAdmission{
			Fact:        fact,
			Admission:   store.RunForkHistoricalReplayAdmissionFailClosedBlocker,
			SourceOwner: store.RunForkReplayResumeAdmissionOwner,
			BlockerCode: blocker.Code,
			Message:     blocker.Message,
		}
	}
	return store.RunForkHistoricalReplayFactAdmission{
		Fact:        fact,
		Admission:   fallbackAdmission,
		SourceOwner: store.RunForkReplayResumeAdmissionOwner,
		Tracker:     tracker,
		Message:     fallbackMessage,
	}
}

func historicalReplayLineageFact(fact, sourceOwner, message string) store.RunForkHistoricalReplayFactAdmission {
	return store.RunForkHistoricalReplayFactAdmission{
		Fact:        fact,
		Admission:   store.RunForkHistoricalReplayAdmissionLineageOnlyEvidence,
		SourceOwner: sourceOwner,
		Message:     message,
	}
}

func historicalReplaySplitFact(fact, message, tracker string) store.RunForkHistoricalReplayFactAdmission {
	return store.RunForkHistoricalReplayFactAdmission{
		Fact:      fact,
		Admission: store.RunForkHistoricalReplayAdmissionSplitSibling,
		Tracker:   tracker,
		Message:   message,
	}
}

func replayBlockerForFacts(replay store.RunForkReplayResumeAdmission, facts ...string) (store.RunForkUnsupportedBlocker, bool) {
	for _, disposition := range replay.Dispositions {
		if !stringInSet(disposition.Fact, facts) {
			continue
		}
		if strings.TrimSpace(disposition.Disposition) != store.RunForkReplayResumeDispositionFailClosedBlocker {
			continue
		}
		code := strings.TrimSpace(disposition.BlockerCode)
		if code == "" {
			code = "historical_replay_fact_unproven"
		}
		for _, blocker := range replay.UnsupportedBlockers {
			if strings.TrimSpace(blocker.Code) == code {
				return blocker, true
			}
		}
		return store.RunForkUnsupportedBlocker{
			Code:    code,
			Message: strings.TrimSpace(disposition.Message),
		}, true
	}
	return store.RunForkUnsupportedBlocker{}, false
}

func replayDispositionHas(replay store.RunForkReplayResumeAdmission, fact, disposition string) bool {
	for _, item := range replay.Dispositions {
		if strings.TrimSpace(item.Fact) == fact && strings.TrimSpace(item.Disposition) == disposition {
			return true
		}
	}
	return false
}

func stringInSet(value string, items []string) bool {
	value = strings.TrimSpace(value)
	for _, item := range items {
		if value == strings.TrimSpace(item) {
			return true
		}
	}
	return false
}

func historicalReplayPrerequisites(routeRecoveryPresent bool) []store.RunForkSelectedContractExecutionBoundary {
	disposition := store.RunForkSelectedContractDispositionPrerequisite
	routeRecoveryReason := "selected route recovery is consumed when present; missing recovery remains a named blocker from contract-swap admission"
	if !routeRecoveryPresent {
		disposition = store.RunForkSelectedContractDispositionFailClosed
	}
	return []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "replay_resume_admission",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       store.RunForkReplayResumeAdmissionOwner,
			Reason:      "historical replay execution admission consumes the canonical store taxonomy and does not recompute source fact classifications",
		},
		{
			Concept:     "selected_contract_execution_admission",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       store.RunForkSelectedContractExecutionAdmissionOwner,
			Reason:      "selected binding/source/frontier evidence must come from the selected execution admission owner",
		},
		{
			Concept:     "selected_contract_route_topology",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       store.RunForkSelectedContractRouteTopologyOwner,
			Reason:      "route topology is prerequisite evidence; source routing rows remain invalid as fork truth",
		},
		{
			Concept:     "selected_contract_recipient_planning",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       store.RunForkSelectedContractRecipientPlanningOwner,
			Reason:      "future delivery work must consume recipient planning rather than deriving recipients from source deliveries or current routes",
		},
		{
			Concept:     "selected_contract_route_recovery",
			Disposition: disposition,
			Owner:       store.RunForkSelectedContractRouteRecoveryOwner,
			Reason:      routeRecoveryReason,
		},
		{
			Concept:     "contract_swap_boot_resume_admission",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       store.RunForkContractSwapBootResumeAdmissionOwner,
			Reason:      "contract-swap readiness is an adjacent non-mutating prerequisite and must not be recomputed by historical replay admission",
		},
	}
}

func historicalReplayRequiredConsumers() []store.RunForkSelectedContractExecutionBoundary {
	return []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "future_historical_replay_execution",
			Disposition: store.RunForkSelectedContractDispositionFutureOwnerRequired,
			Owner:       store.RunForkHistoricalReplayExecutionOwner,
			Reason:      "mutating replay/resume must consume this admission before any handler execution or fork-local write",
		},
		{
			Concept:     "selected_contract_execution",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       store.RunForkSelectedContractExecutionOwner,
			Reason:      "supported selected-contract execution remains a prerequisite proof, not a replacement for full replay/resume",
		},
		{
			Concept:     "event_bus_publish",
			Disposition: store.RunForkSelectedContractDispositionFutureOwnerRequired,
			Owner:       "internal/runtime/bus.EventBus.Publish",
			Reason:      "future fork-local event delivery must route through publish guards rather than direct source row copying",
		},
		{
			Concept:     "operator_surfaces",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Reason:      "CLI, API, dashboard, and Builder may display this admission but must not own replay/resume semantics",
		},
	}
}

func historicalReplayBlockedSiblings() []store.RunForkSelectedContractExecutionBoundary {
	return []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "mutating_historical_replay_execution",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       store.RunForkHistoricalReplayExecutionOwner,
			Reason:      "this slice establishes admission only and does not authorize mutation",
		},
		{
			Concept:     "timer_reconstruction",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "timer reconstruction remains under #564 until a timer owner is approved",
		},
		{
			Concept:     "route_persistence_runtime_recovery",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       store.RunForkSelectedContractRouteRecoveryOwner,
			Reason:      "route persistence/runtime recovery remains tracked separately under #618",
		},
		{
			Concept:     "sessions_turns_audits",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "session, turn, and audit reconstruction remains under #564",
		},
		{
			Concept:     "node_system_non_agent_replay",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "non-agent replay requires its own handler/idempotency/receipt owner",
		},
		{
			Concept:     "api_dashboard_builder_mutation",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "operator surfaces remain consumers only",
		},
	}
}

func historicalReplayInvalidPaths() []store.RunForkSelectedContractExecutionBoundary {
	return []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "source_event_copy",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "source event IDs are lineage inputs; future replay must mint fork-local events under the fork run_id",
		},
		{
			Concept:     "source_delivery_copy",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "source event_deliveries are lineage or blockers, not executable fork delivery rows",
		},
		{
			Concept:     "source_outcome_suppression",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "source receipts, dead letters, retry/idempotency state, and post-T outcomes cannot suppress fork-local work",
		},
		{
			Concept:     "current_route_rows_as_fork_truth",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "current routing_rules, flow-instance routes, and runtime recovery rows are not historical replay admission owners",
		},
		{
			Concept:     "selected_frontier_execution_as_full_replay",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "supported selected frontier execution does not prove full historical replay/resume",
		},
		{
			Concept:     "cli_api_dashboard_owned_replay",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "operator surfaces may request or display replay admission but cannot own it",
		},
	}
}
