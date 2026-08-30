package runforkexecution

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

type HistoricalReplayExecutionAdmissionRequest struct {
	ReplayResumeAdmission      runfork.RunForkReplayResumeAdmission
	SelectedExecutionAdmission runfork.RunForkSelectedContractExecutionAdmission
	ContractSwapAdmission      runfork.RunForkContractSwapBootResumeAdmission
	RouteRecovery              *runfork.RunForkSelectedContractRouteRecovery
}

type HistoricalReplayDeliveryEventReplayAdmissionRequest struct {
	ForkRunID             string
	SourceRunID           string
	ForkEventID           string
	ReplayResumeAdmission runfork.RunForkReplayResumeAdmission
}

type HistoricalReplayExecutionRequest struct {
	Admission             runfork.RunForkHistoricalReplayExecutionAdmission
	ReplayResumeAdmission runfork.RunForkReplayResumeAdmission
	PendingWork           []runfork.RunForkPendingWork
}

type HistoricalReplayExecutionAdmitter struct{}

func (HistoricalReplayExecutionAdmitter) AdmitRunForkHistoricalReplayExecution(_ context.Context, req runfork.RunForkHistoricalReplayExecutionRequest) (runfork.RunForkHistoricalReplayExecution, error) {
	admission, err := BuildHistoricalReplayDeliveryEventReplayAdmission(HistoricalReplayDeliveryEventReplayAdmissionRequest{
		ForkRunID:             req.ForkRunID,
		SourceRunID:           req.SourceRunID,
		ForkEventID:           req.ForkEventID,
		ReplayResumeAdmission: req.ReplayResumeAdmission,
	})
	if err != nil {
		return runfork.RunForkHistoricalReplayExecution{}, err
	}
	return BuildHistoricalReplayExecution(HistoricalReplayExecutionRequest{
		Admission:             admission,
		ReplayResumeAdmission: req.ReplayResumeAdmission,
		PendingWork:           req.PendingWork,
	})
}

func BuildHistoricalReplayDeliveryEventReplayAdmission(req HistoricalReplayDeliveryEventReplayAdmissionRequest) (runfork.RunForkHistoricalReplayExecutionAdmission, error) {
	replayAdmission := req.ReplayResumeAdmission
	if strings.TrimSpace(replayAdmission.Owner) != runfork.RunForkReplayResumeAdmissionOwner {
		return runfork.RunForkHistoricalReplayExecutionAdmission{}, fmt.Errorf("historical replay delivery/event replay admission requires %s; got %q", runfork.RunForkReplayResumeAdmissionOwner, replayAdmission.Owner)
	}
	forkRunID := strings.TrimSpace(req.ForkRunID)
	sourceRunID := strings.TrimSpace(req.SourceRunID)
	forkEventID := strings.TrimSpace(req.ForkEventID)
	if forkRunID == "" || sourceRunID == "" || forkEventID == "" {
		return runfork.RunForkHistoricalReplayExecutionAdmission{}, fmt.Errorf("historical replay delivery/event replay admission requires fork/source/event identity")
	}
	blockers := []runfork.RunForkUnsupportedBlocker{}
	for _, blocker := range replayAdmission.UnsupportedBlockers {
		blockers = appendRunForkUnsupportedBlocker(blockers, blocker)
	}
	blockers = appendRunForkUnsupportedBlocker(blockers, runfork.RunForkUnsupportedBlocker{
		Code:    runfork.RunForkBlockerHistoricalReplayExecutionAdmissionNonMutating,
		Message: "historical replay execution admission is non-mutating; delivery/event replay mutation requires runtime.run_fork.historical_replay_execution",
	})
	return runfork.RunForkHistoricalReplayExecutionAdmission{
		Owner:                      runfork.RunForkHistoricalReplayExecutionAdmissionOwner,
		NonMutating:                true,
		ExecutionSupported:         false,
		FutureExecutionOwner:       runfork.RunForkHistoricalReplayExecutionOwner,
		ForkRunID:                  forkRunID,
		SourceRunID:                sourceRunID,
		ForkEventID:                forkEventID,
		ReplayResumeAdmissionOwner: replayAdmission.Owner,
		FactAdmissions:             historicalReplayFactAdmissions(replayAdmission),
		Prerequisites: []runfork.RunForkSelectedContractExecutionBoundary{
			{
				Concept:     "replay_resume_admission",
				Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
				Owner:       runfork.RunForkReplayResumeAdmissionOwner,
				Reason:      "delivery/event replay mutation consumes the canonical replay taxonomy and does not recompute source fact classifications",
			},
		},
		RequiredConsumers:   historicalReplayRequiredConsumers(),
		BlockedSiblings:     historicalReplayBlockedSiblings(),
		InvalidPaths:        historicalReplayInvalidPaths(),
		UnsupportedBlockers: blockers,
	}, nil
}

func BuildHistoricalReplayExecution(req HistoricalReplayExecutionRequest) (runfork.RunForkHistoricalReplayExecution, error) {
	admission := req.Admission
	if strings.TrimSpace(admission.Owner) != runfork.RunForkHistoricalReplayExecutionAdmissionOwner {
		return runfork.RunForkHistoricalReplayExecution{}, fmt.Errorf("historical replay execution requires %s; got %q", runfork.RunForkHistoricalReplayExecutionAdmissionOwner, admission.Owner)
	}
	if !admission.NonMutating {
		return runfork.RunForkHistoricalReplayExecution{}, fmt.Errorf("historical replay execution requires non-mutating admission proof")
	}
	if strings.TrimSpace(admission.FutureExecutionOwner) != runfork.RunForkHistoricalReplayExecutionOwner {
		return runfork.RunForkHistoricalReplayExecution{}, fmt.Errorf("historical replay execution requires future owner %s; got %q", runfork.RunForkHistoricalReplayExecutionOwner, admission.FutureExecutionOwner)
	}
	if strings.TrimSpace(admission.ReplayResumeAdmissionOwner) != runfork.RunForkReplayResumeAdmissionOwner {
		return runfork.RunForkHistoricalReplayExecution{}, fmt.Errorf("historical replay execution requires replay taxonomy owner %s; got %q", runfork.RunForkReplayResumeAdmissionOwner, admission.ReplayResumeAdmissionOwner)
	}
	replayAdmission := req.ReplayResumeAdmission
	if strings.TrimSpace(replayAdmission.Owner) != runfork.RunForkReplayResumeAdmissionOwner {
		return runfork.RunForkHistoricalReplayExecution{}, fmt.Errorf("historical replay execution requires %s; got %q", runfork.RunForkReplayResumeAdmissionOwner, replayAdmission.Owner)
	}
	eventDeliveries, ok := historicalReplayFactAdmission(admission.FactAdmissions, runfork.RunForkHistoricalReplayFactEventDeliveries)
	if !ok || eventDeliveries.Admission != runfork.RunForkHistoricalReplayAdmissionExecutableForkWork {
		return runfork.RunForkHistoricalReplayExecution{}, fmt.Errorf("historical replay execution requires event_deliveries executable fork work admission")
	}
	if !replayAdmission.DeliveryEventReplayReady &&
		!replayDispositionHas(replayAdmission, runfork.RunForkReplayResumeFactDeliveryPendingHistory, runfork.RunForkReplayResumeDispositionForkReplay) {
		return runfork.RunForkHistoricalReplayExecution{}, fmt.Errorf("historical replay execution requires delivery_event_replay_ready replay taxonomy")
	}
	if err := validateHistoricalReplayFactMatrix(admission.FactAdmissions); err != nil {
		return runfork.RunForkHistoricalReplayExecution{}, err
	}
	deliveryEventReplayWork := historicalReplayDeliveryEventReplayWork(req.PendingWork)
	if len(deliveryEventReplayWork) == 0 {
		return runfork.RunForkHistoricalReplayExecution{}, fmt.Errorf("historical replay execution requires owner-authorized delivery_event_replay_ready work")
	}
	return runfork.RunForkHistoricalReplayExecution{
		Owner:                      runfork.RunForkHistoricalReplayExecutionOwner,
		AdmissionOwner:             admission.Owner,
		ReplayResumeAdmissionOwner: replayAdmission.Owner,
		ForkRunID:                  strings.TrimSpace(admission.ForkRunID),
		SourceRunID:                strings.TrimSpace(admission.SourceRunID),
		ForkEventID:                strings.TrimSpace(admission.ForkEventID),
		ClosureLevel:               "canonical_owner_promotion_with_delivery_event_replay_ready_only",
		FullReplayUnsupported:      true,
		DeliveryEventReplayReady:   true,
		EventDeliveriesAdmission:   eventDeliveries,
		FactAdmissions:             append([]runfork.RunForkHistoricalReplayFactAdmission(nil), admission.FactAdmissions...),
		DeliveryEventReplayWork:    deliveryEventReplayWork,
		RequiredConsumers:          append([]runfork.RunForkSelectedContractExecutionBoundary(nil), admission.RequiredConsumers...),
		BlockedSiblings:            historicalReplayExecutionBlockedSiblings(admission.BlockedSiblings),
		InvalidPaths:               admission.InvalidPaths,
	}, nil
}

func BuildHistoricalReplayExecutionAdmission(req HistoricalReplayExecutionAdmissionRequest) (runfork.RunForkHistoricalReplayExecutionAdmission, error) {
	replayAdmission := req.ReplayResumeAdmission
	if strings.TrimSpace(replayAdmission.Owner) != runfork.RunForkReplayResumeAdmissionOwner {
		return runfork.RunForkHistoricalReplayExecutionAdmission{}, fmt.Errorf("historical replay execution admission requires %s; got %q", runfork.RunForkReplayResumeAdmissionOwner, replayAdmission.Owner)
	}
	selectedAdmission := req.SelectedExecutionAdmission
	if err := validateContractSwapSelectedExecutionAdmission(selectedAdmission); err != nil {
		return runfork.RunForkHistoricalReplayExecutionAdmission{}, fmt.Errorf("historical replay execution admission selected prerequisite: %w", err)
	}
	contractSwapAdmission := req.ContractSwapAdmission
	if err := validateHistoricalReplayContractSwapAdmission(selectedAdmission, replayAdmission, contractSwapAdmission); err != nil {
		return runfork.RunForkHistoricalReplayExecutionAdmission{}, err
	}
	if req.RouteRecovery != nil {
		if err := validateContractSwapRouteRecovery(selectedAdmission, *req.RouteRecovery); err != nil {
			return runfork.RunForkHistoricalReplayExecutionAdmission{}, fmt.Errorf("historical replay execution admission route recovery prerequisite: %w", err)
		}
	}

	blockers := []runfork.RunForkUnsupportedBlocker{}
	for _, blocker := range replayAdmission.UnsupportedBlockers {
		blockers = appendRunForkUnsupportedBlocker(blockers, blocker)
	}
	for _, blocker := range selectedAdmission.UnsupportedBlockers {
		blockers = appendRunForkUnsupportedBlocker(blockers, blocker)
	}
	for _, blocker := range contractSwapAdmission.UnsupportedBlockers {
		blockers = appendRunForkUnsupportedBlocker(blockers, blocker)
	}
	blockers = appendRunForkUnsupportedBlocker(blockers, runfork.RunForkUnsupportedBlocker{
		Code:    runfork.RunForkBlockerHistoricalReplayExecutionAdmissionNonMutating,
		Message: "historical replay execution admission is non-mutating; bounded fork re-execution mutation remains separately gated",
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

	return runfork.RunForkHistoricalReplayExecutionAdmission{
		Owner:                           runfork.RunForkHistoricalReplayExecutionAdmissionOwner,
		NonMutating:                     true,
		ExecutionSupported:              false,
		FutureExecutionOwner:            runfork.RunForkHistoricalReplayExecutionOwner,
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
	selectedAdmission runfork.RunForkSelectedContractExecutionAdmission,
	replayAdmission runfork.RunForkReplayResumeAdmission,
	admission runfork.RunForkContractSwapBootResumeAdmission,
) error {
	if strings.TrimSpace(admission.Owner) != runfork.RunForkContractSwapBootResumeAdmissionOwner {
		return fmt.Errorf("historical replay execution admission requires %s; got %q", runfork.RunForkContractSwapBootResumeAdmissionOwner, admission.Owner)
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

func historicalReplayFactAdmissions(replay runfork.RunForkReplayResumeAdmission) []runfork.RunForkHistoricalReplayFactAdmission {
	return []runfork.RunForkHistoricalReplayFactAdmission{
		{
			Fact:        runfork.RunForkHistoricalReplayFactSourceEvents,
			Admission:   runfork.RunForkHistoricalReplayAdmissionLineageOnlyEvidence,
			SourceOwner: "events",
			Message:     "source events are payload and lineage evidence only; future replay must mint fresh fork-local events under the fork run_id",
		},
		historicalReplayEventDeliveriesAdmission(replay),
		historicalReplayLineageFact(runfork.RunForkHistoricalReplayFactReceipts, "event_receipts", "source receipts are outcome lineage only and cannot suppress future fork-local work"),
		historicalReplayDeadLettersAdmission(replay),
		historicalReplaySplitFact(runfork.RunForkHistoricalReplayFactRetryIdempotency, "runtime idempotency and retry state must be owned by a later mutating replay child; source state cannot suppress fork work", "#564"),
		historicalReplaySplitFact(runfork.RunForkHistoricalReplayFactEmittedFollowUps, "emitted follow-up regeneration belongs to the future mutating replay owner; source follow-up rows are not copied", "#564"),
		historicalReplayTimersAdmission(replay),
		historicalReplayFactFromReplay(replay, runfork.RunForkHistoricalReplayFactRoutes, []string{runfork.RunForkReplayResumeFactRouteHistory}, runfork.RunForkHistoricalReplayAdmissionSplitSibling, "route and route-recovery truth remains split under fork-local route persistence/runtime recovery", "#618"),
		historicalReplayConversationFactAdmission(replay, runfork.RunForkHistoricalReplayFactSessions, runfork.RunForkReplayResumeFactSessionHistory, "source session rows admitted by the selected-contract lineage policy are lineage/no-action evidence only; fresh fork-local sessions must be created by normal runtime execution"),
		historicalReplayConversationFactAdmission(replay, runfork.RunForkHistoricalReplayFactTurns, runfork.RunForkReplayResumeFactActiveTurnHistory, "source turn rows admitted by the selected-contract lineage policy are lineage/no-action evidence only; fresh fork-local turns must be created by normal runtime execution"),
		historicalReplayConversationFactAdmission(replay, runfork.RunForkHistoricalReplayFactAudits, runfork.RunForkReplayResumeFactConversationAuditHistory, "source task conversation audit rows admitted by the selected-contract lineage policy are lineage/no-action evidence only; fresh fork-local audits must be created by normal runtime execution"),
		historicalReplayNonAgentAdmission(replay),
		historicalReplaySourceAdvancedAdmission(replay),
		historicalReplaySplitFact(runfork.RunForkHistoricalReplayFactRuntimeRestartRecovery, "runtime restart recovery remains a consumer/sibling and cannot reconstruct historical replay state from current rows", "#564"),
		historicalReplaySplitFact(runfork.RunForkHistoricalReplayFactCLIApiDashboardOperator, "CLI, API, and dashboard surfaces are consumers only and must not compute historical replay admission independently", "#549"),
	}
}

func historicalReplayTimersAdmission(replay runfork.RunForkReplayResumeAdmission) runfork.RunForkHistoricalReplayFactAdmission {
	if blocker, ok := replayBlockerForFacts(replay, runfork.RunForkReplayResumeFactTimerHistory); ok {
		return runfork.RunForkHistoricalReplayFactAdmission{
			Fact:        runfork.RunForkHistoricalReplayFactTimers,
			Admission:   runfork.RunForkHistoricalReplayAdmissionFailClosedBlocker,
			SourceOwner: runfork.RunForkReplayResumeAdmissionOwner,
			BlockerCode: blocker.Code,
			Message:     blocker.Message,
		}
	}
	if replayDispositionHas(replay, runfork.RunForkReplayResumeFactTimerHistory, runfork.RunForkReplayResumeDispositionReconstruct) {
		return historicalReplaySplitFact(
			runfork.RunForkHistoricalReplayFactTimers,
			"timer reconstruction is unsupported until #642 provides a long-lived selected-fork timer owner",
			"#642",
		)
	}
	return historicalReplaySplitFact(runfork.RunForkHistoricalReplayFactTimers, "timer reconstruction remains fail-closed under the long-lived selected-fork timer owner gate", "#642")
}

func historicalReplayEventDeliveriesAdmission(replay runfork.RunForkReplayResumeAdmission) runfork.RunForkHistoricalReplayFactAdmission {
	facts := []string{
		runfork.RunForkReplayResumeFactDeliveryPendingHistory,
		runfork.RunForkReplayResumeFactDeliveryInProgressHistory,
		runfork.RunForkReplayResumeFactDeliveryFailedHistory,
		runfork.RunForkReplayResumeFactDeliveryDeadLetterHistory,
		runfork.RunForkReplayResumeFactCommittedReplayScope,
	}
	if blocker, ok := replayBlockerForFacts(replay, facts...); ok {
		return runfork.RunForkHistoricalReplayFactAdmission{
			Fact:        runfork.RunForkHistoricalReplayFactEventDeliveries,
			Admission:   runfork.RunForkHistoricalReplayAdmissionFailClosedBlocker,
			SourceOwner: runfork.RunForkReplayResumeAdmissionOwner,
			BlockerCode: blocker.Code,
			Message:     blocker.Message,
		}
	}
	if replay.DeliveryEventReplayReady || replayDispositionHas(replay, runfork.RunForkReplayResumeFactDeliveryPendingHistory, runfork.RunForkReplayResumeDispositionForkReplay) {
		return runfork.RunForkHistoricalReplayFactAdmission{
			Fact:        runfork.RunForkHistoricalReplayFactEventDeliveries,
			Admission:   runfork.RunForkHistoricalReplayAdmissionExecutableForkWork,
			SourceOwner: runfork.RunForkReplayResumeAdmissionOwner,
			Message:     "only the existing delivery_event_replay_ready primitive is admitted as future executable fork work; this admission does not create event or delivery rows",
		}
	}
	if disposition, ok := replayDispositionForFact(replay, runfork.RunForkReplayResumeFactCommittedReplayScope, runfork.RunForkReplayResumeDispositionLineageOnly); ok &&
		strings.TrimSpace(disposition.Owner) == runfork.RunForkSelectedContractCommittedReplayScopeMarkerPolicyOwner {
		return runfork.RunForkHistoricalReplayFactAdmission{
			Fact:        runfork.RunForkHistoricalReplayFactEventDeliveries,
			Admission:   runfork.RunForkHistoricalReplayAdmissionLineageOnlyEvidence,
			SourceOwner: runfork.RunForkSelectedContractCommittedReplayScopeMarkerPolicyOwner,
			Tracker:     "#663",
			Message:     "source committed replay-scope marker rows present at the selected revision are lineage/no-action evidence only for selected-contract forks; fork-local recovery proof must be written under the fork run_id",
		}
	}
	if disposition, ok := replayDispositionForFact(replay, runfork.RunForkReplayResumeFactDeliveryInProgressHistory, runfork.RunForkReplayResumeDispositionLineageOnly); ok &&
		strings.TrimSpace(disposition.Owner) == runfork.RunForkSelectedContractActiveSourceDeliveryConversationCouplingPolicyOwner {
		return runfork.RunForkHistoricalReplayFactAdmission{
			Fact:        runfork.RunForkHistoricalReplayFactEventDeliveries,
			Admission:   runfork.RunForkHistoricalReplayAdmissionLineageOnlyEvidence,
			SourceOwner: runfork.RunForkSelectedContractActiveSourceDeliveryConversationCouplingPolicyOwner,
			Tracker:     "#678",
			Message:     "same-source in-progress delivery that emitted the fork-point event is selected-contract branch-divergence lineage only; source delivery/session/receipt/outcome rows are not copied or resumed",
		}
	}
	return historicalReplayLineageFact(runfork.RunForkHistoricalReplayFactEventDeliveries, runfork.RunForkReplayResumeAdmissionOwner, "source delivery history is lineage/no-op evidence unless the canonical replay taxonomy admits the pending unstarted agent-delivery primitive")
}

func historicalReplayDeadLettersAdmission(replay runfork.RunForkReplayResumeAdmission) runfork.RunForkHistoricalReplayFactAdmission {
	if blocker, ok := replayBlockerForFacts(replay, runfork.RunForkReplayResumeFactDeliveryDeadLetterHistory); ok {
		return runfork.RunForkHistoricalReplayFactAdmission{
			Fact:        runfork.RunForkHistoricalReplayFactDeadLetters,
			Admission:   runfork.RunForkHistoricalReplayAdmissionFailClosedBlocker,
			SourceOwner: runfork.RunForkReplayResumeAdmissionOwner,
			BlockerCode: blocker.Code,
			Message:     blocker.Message,
		}
	}
	return historicalReplayLineageFact(runfork.RunForkHistoricalReplayFactDeadLetters, "event_deliveries", "source dead letters are terminal source-run outcome evidence only and cannot suppress future fork-local work")
}

func historicalReplayNonAgentAdmission(replay runfork.RunForkReplayResumeAdmission) runfork.RunForkHistoricalReplayFactAdmission {
	for _, blocker := range replay.UnsupportedBlockers {
		if strings.TrimSpace(blocker.Code) == runfork.RunForkBlockerNonAgentDeliveryReplayUnsupported {
			return runfork.RunForkHistoricalReplayFactAdmission{
				Fact:        runfork.RunForkHistoricalReplayFactNonAgentNodeSystemWork,
				Admission:   runfork.RunForkHistoricalReplayAdmissionFailClosedBlocker,
				SourceOwner: runfork.RunForkReplayResumeAdmissionOwner,
				BlockerCode: blocker.Code,
				Message:     blocker.Message,
			}
		}
	}
	return historicalReplaySplitFact(runfork.RunForkHistoricalReplayFactNonAgentNodeSystemWork, "node, system, platform, and non-agent delivery replay requires a separate handler/idempotency/receipt owner", "#564")
}

func historicalReplaySourceAdvancedAdmission(replay runfork.RunForkReplayResumeAdmission) runfork.RunForkHistoricalReplayFactAdmission {
	if blocker, ok := replayBlockerForFacts(replay, runfork.RunForkReplayResumeFactSourceAdvanced); ok {
		return runfork.RunForkHistoricalReplayFactAdmission{
			Fact:        runfork.RunForkHistoricalReplayFactSourceAdvancedPostTFacts,
			Admission:   runfork.RunForkHistoricalReplayAdmissionFailClosedBlocker,
			SourceOwner: runfork.RunForkReplayResumeAdmissionOwner,
			BlockerCode: blocker.Code,
			Message:     blocker.Message,
		}
	}
	if disposition, ok := replayDispositionForFact(replay, runfork.RunForkReplayResumeFactSourceAdvanced, runfork.RunForkReplayResumeDispositionLineageOnly); ok &&
		strings.TrimSpace(disposition.Owner) == runfork.RunForkSelectedContractSourceAdvancedConversationHistoryPolicyOwner {
		return runfork.RunForkHistoricalReplayFactAdmission{
			Fact:        runfork.RunForkHistoricalReplayFactSourceAdvancedPostTFacts,
			Admission:   runfork.RunForkHistoricalReplayAdmissionLineageOnlyEvidence,
			SourceOwner: runfork.RunForkSelectedContractSourceAdvancedConversationHistoryPolicyOwner,
			Tracker:     "#671",
			Message:     "post-T source conversation-history facts are selected-contract branch-divergence lineage evidence only; source sessions, turns, and audits are not copied or reused as fork-local runtime truth",
		}
	}
	return historicalReplaySplitFact(runfork.RunForkHistoricalReplayFactSourceAdvancedPostTFacts, "source-advanced and post-T source outcomes remain source-run evidence and cannot suppress fork-local replay", "#564")
}

func historicalReplayConversationFactAdmission(replay runfork.RunForkReplayResumeAdmission, fact, replayFact, lineageMessage string) runfork.RunForkHistoricalReplayFactAdmission {
	if blocker, ok := replayBlockerForFacts(replay, replayFact); ok {
		return runfork.RunForkHistoricalReplayFactAdmission{
			Fact:        fact,
			Admission:   runfork.RunForkHistoricalReplayAdmissionFailClosedBlocker,
			SourceOwner: runfork.RunForkReplayResumeAdmissionOwner,
			BlockerCode: blocker.Code,
			Message:     blocker.Message,
		}
	}
	if disposition, ok := replayDispositionForFact(replay, replayFact, runfork.RunForkReplayResumeDispositionLineageOnly); ok {
		sourceOwner := strings.TrimSpace(disposition.Owner)
		if sourceOwner == "" {
			sourceOwner = runfork.RunForkReplayResumeAdmissionOwner
		}
		return runfork.RunForkHistoricalReplayFactAdmission{
			Fact:        fact,
			Admission:   runfork.RunForkHistoricalReplayAdmissionLineageOnlyEvidence,
			SourceOwner: sourceOwner,
			Tracker:     "#661",
			Message:     lineageMessage,
		}
	}
	return historicalReplaySplitFact(fact, "session/turn/audit reconstruction remains a split sibling unless the selected-contract lineage policy admits source conversation history as lineage/no-action evidence", "#564")
}

func historicalReplayFactFromReplay(replay runfork.RunForkReplayResumeAdmission, fact string, replayFacts []string, fallbackAdmission, fallbackMessage, tracker string) runfork.RunForkHistoricalReplayFactAdmission {
	if blocker, ok := replayBlockerForFacts(replay, replayFacts...); ok {
		return runfork.RunForkHistoricalReplayFactAdmission{
			Fact:        fact,
			Admission:   runfork.RunForkHistoricalReplayAdmissionFailClosedBlocker,
			SourceOwner: runfork.RunForkReplayResumeAdmissionOwner,
			BlockerCode: blocker.Code,
			Message:     blocker.Message,
		}
	}
	return runfork.RunForkHistoricalReplayFactAdmission{
		Fact:        fact,
		Admission:   fallbackAdmission,
		SourceOwner: runfork.RunForkReplayResumeAdmissionOwner,
		Tracker:     tracker,
		Message:     fallbackMessage,
	}
}

func historicalReplayLineageFact(fact, sourceOwner, message string) runfork.RunForkHistoricalReplayFactAdmission {
	return runfork.RunForkHistoricalReplayFactAdmission{
		Fact:        fact,
		Admission:   runfork.RunForkHistoricalReplayAdmissionLineageOnlyEvidence,
		SourceOwner: sourceOwner,
		Message:     message,
	}
}

func historicalReplaySplitFact(fact, message, tracker string) runfork.RunForkHistoricalReplayFactAdmission {
	return runfork.RunForkHistoricalReplayFactAdmission{
		Fact:      fact,
		Admission: runfork.RunForkHistoricalReplayAdmissionSplitSibling,
		Tracker:   tracker,
		Message:   message,
	}
}

func replayBlockerForFacts(replay runfork.RunForkReplayResumeAdmission, facts ...string) (runfork.RunForkUnsupportedBlocker, bool) {
	for _, disposition := range replay.Dispositions {
		if !stringInSet(disposition.Fact, facts) {
			continue
		}
		if strings.TrimSpace(disposition.Disposition) != runfork.RunForkReplayResumeDispositionFailClosedBlocker {
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
		return runfork.RunForkUnsupportedBlocker{
			Code:    code,
			Message: strings.TrimSpace(disposition.Message),
		}, true
	}
	return runfork.RunForkUnsupportedBlocker{}, false
}

func replayDispositionHas(replay runfork.RunForkReplayResumeAdmission, fact, disposition string) bool {
	_, ok := replayDispositionForFact(replay, fact, disposition)
	return ok
}

func replayDispositionForFact(replay runfork.RunForkReplayResumeAdmission, fact, disposition string) (runfork.RunForkReplayResumeDisposition, bool) {
	for _, item := range replay.Dispositions {
		if strings.TrimSpace(item.Fact) == fact && strings.TrimSpace(item.Disposition) == disposition {
			return item, true
		}
	}
	return runfork.RunForkReplayResumeDisposition{}, false
}

func historicalReplayFactAdmission(admissions []runfork.RunForkHistoricalReplayFactAdmission, fact string) (runfork.RunForkHistoricalReplayFactAdmission, bool) {
	for _, item := range admissions {
		if strings.TrimSpace(item.Fact) == fact {
			return item, true
		}
	}
	return runfork.RunForkHistoricalReplayFactAdmission{}, false
}

func validateHistoricalReplayFactMatrix(admissions []runfork.RunForkHistoricalReplayFactAdmission) error {
	required := map[string]struct{}{
		runfork.RunForkHistoricalReplayFactSourceEvents:             {},
		runfork.RunForkHistoricalReplayFactEventDeliveries:          {},
		runfork.RunForkHistoricalReplayFactReceipts:                 {},
		runfork.RunForkHistoricalReplayFactDeadLetters:              {},
		runfork.RunForkHistoricalReplayFactRetryIdempotency:         {},
		runfork.RunForkHistoricalReplayFactEmittedFollowUps:         {},
		runfork.RunForkHistoricalReplayFactTimers:                   {},
		runfork.RunForkHistoricalReplayFactRoutes:                   {},
		runfork.RunForkHistoricalReplayFactSessions:                 {},
		runfork.RunForkHistoricalReplayFactTurns:                    {},
		runfork.RunForkHistoricalReplayFactAudits:                   {},
		runfork.RunForkHistoricalReplayFactNonAgentNodeSystemWork:   {},
		runfork.RunForkHistoricalReplayFactSourceAdvancedPostTFacts: {},
		runfork.RunForkHistoricalReplayFactRuntimeRestartRecovery:   {},
		runfork.RunForkHistoricalReplayFactCLIApiDashboardOperator:  {},
	}
	allowed := map[string]struct{}{
		runfork.RunForkHistoricalReplayAdmissionExecutableForkWork:     {},
		runfork.RunForkHistoricalReplayAdmissionReconstructedForkState: {},
		runfork.RunForkHistoricalReplayAdmissionLineageOnlyEvidence:    {},
		runfork.RunForkHistoricalReplayAdmissionFailClosedBlocker:      {},
		runfork.RunForkHistoricalReplayAdmissionSplitSibling:           {},
	}
	seen := map[string]struct{}{}
	for _, admission := range admissions {
		fact := strings.TrimSpace(admission.Fact)
		if _, ok := required[fact]; !ok {
			return fmt.Errorf("historical replay execution encountered unowned fact family %q", admission.Fact)
		}
		if _, ok := seen[fact]; ok {
			return fmt.Errorf("historical replay execution fact family %q admitted more than once", fact)
		}
		seen[fact] = struct{}{}
		disposition := strings.TrimSpace(admission.Admission)
		if _, ok := allowed[disposition]; !ok {
			return fmt.Errorf("historical replay execution fact %s has unsupported admission %q", fact, admission.Admission)
		}
		if disposition == runfork.RunForkHistoricalReplayAdmissionExecutableForkWork && fact != runfork.RunForkHistoricalReplayFactEventDeliveries {
			return fmt.Errorf("historical replay execution cannot execute unsupported fact family %s", fact)
		}
	}
	for fact := range required {
		if _, ok := seen[fact]; !ok {
			return fmt.Errorf("historical replay execution missing fact family %s", fact)
		}
	}
	return nil
}

func historicalReplayDeliveryEventReplayWork(pending []runfork.RunForkPendingWork) []runfork.RunForkHistoricalReplayExecutableWork {
	work := make([]runfork.RunForkHistoricalReplayExecutableWork, 0, len(pending))
	for _, item := range pending {
		if !runfork.RunForkPendingWorkReplayableForHistoricalReplay(item) {
			continue
		}
		work = append(work, runfork.RunForkHistoricalReplayExecutableWork{
			Fact:             runfork.RunForkHistoricalReplayFactEventDeliveries,
			SourceEventID:    strings.TrimSpace(item.EventID),
			SourceDeliveryID: strings.TrimSpace(item.DeliveryID),
			SubscriberType:   strings.TrimSpace(item.SubscriberType),
			SubscriberID:     strings.TrimSpace(item.SubscriberID),
			ReasonCode:       strings.TrimSpace(item.ReasonCode),
			Classification:   strings.TrimSpace(item.Classification),
		})
	}
	return work
}

func historicalReplayExecutionBlockedSiblings(items []runfork.RunForkSelectedContractExecutionBoundary) []runfork.RunForkSelectedContractExecutionBoundary {
	out := make([]runfork.RunForkSelectedContractExecutionBoundary, 0, len(items)+1)
	for _, item := range items {
		if strings.TrimSpace(item.Concept) == "mutating_historical_replay_execution" {
			continue
		}
		out = append(out, item)
	}
	out = append(out, runfork.RunForkSelectedContractExecutionBoundary{
		Concept:     "full_historical_replay_execution",
		Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
		Owner:       runfork.RunForkHistoricalReplayExecutionOwner,
		Reason:      "this child mutates only delivery_event_replay_ready; broader source-run replay remains under successor ownership",
	})
	return out
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

func historicalReplayPrerequisites(routeRecoveryPresent bool) []runfork.RunForkSelectedContractExecutionBoundary {
	disposition := runfork.RunForkSelectedContractDispositionPrerequisite
	routeRecoveryReason := "selected route recovery is consumed when present; missing recovery remains a named blocker from contract-swap admission"
	if !routeRecoveryPresent {
		disposition = runfork.RunForkSelectedContractDispositionFailClosed
	}
	return []runfork.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "replay_resume_admission",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkReplayResumeAdmissionOwner,
			Reason:      "historical replay execution admission consumes the canonical store taxonomy and does not recompute source fact classifications",
		},
		{
			Concept:     "selected_contract_execution_admission",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractExecutionAdmissionOwner,
			Reason:      "selected binding/source/frontier evidence must come from the selected execution admission owner",
		},
		{
			Concept:     "selected_contract_route_topology",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractRouteTopologyOwner,
			Reason:      "route topology is prerequisite evidence; source routing rows remain invalid as fork truth",
		},
		{
			Concept:     "selected_contract_recipient_planning",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractRecipientPlanningOwner,
			Reason:      "future delivery work must consume recipient planning rather than deriving recipients from source deliveries or current routes",
		},
		{
			Concept:     "selected_contract_route_recovery",
			Disposition: disposition,
			Owner:       runfork.RunForkSelectedContractRouteRecoveryOwner,
			Reason:      routeRecoveryReason,
		},
		{
			Concept:     "contract_swap_boot_resume_admission",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkContractSwapBootResumeAdmissionOwner,
			Reason:      "contract-swap readiness is an adjacent non-mutating prerequisite and must not be recomputed by historical replay admission",
		},
	}
}

func historicalReplayRequiredConsumers() []runfork.RunForkSelectedContractExecutionBoundary {
	return []runfork.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "future_historical_replay_execution",
			Disposition: runfork.RunForkSelectedContractDispositionFutureOwnerRequired,
			Owner:       runfork.RunForkHistoricalReplayExecutionOwner,
			Reason:      "mutating replay/resume must consume this admission before any handler execution or fork-local write",
		},
		{
			Concept:     "selected_contract_execution",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractExecutionOwner,
			Reason:      "supported selected-contract execution remains a prerequisite proof, not a replacement for broader source-run replay",
		},
		{
			Concept:     "event_bus_publish",
			Disposition: runfork.RunForkSelectedContractDispositionFutureOwnerRequired,
			Owner:       "internal/runtime/bus.EventBus.Publish",
			Reason:      "future fork-local event delivery must route through publish guards rather than direct source row copying",
		},
		{
			Concept:     "operator_surfaces",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Reason:      "CLI, API, and dashboard may display this admission but must not own replay/resume semantics",
		},
	}
}

func historicalReplayBlockedSiblings() []runfork.RunForkSelectedContractExecutionBoundary {
	return []runfork.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "mutating_historical_replay_execution",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       runfork.RunForkHistoricalReplayExecutionOwner,
			Reason:      "this slice establishes admission only and does not authorize mutation",
		},
		{
			Concept:     "timer_reconstruction",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "timer reconstruction remains under #642 until a long-lived selected-fork timer owner is approved",
		},
		{
			Concept:     "route_persistence_runtime_recovery",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       runfork.RunForkSelectedContractRouteRecoveryOwner,
			Reason:      "route persistence/runtime recovery remains tracked separately under #618",
		},
		{
			Concept:     "sessions_turns_audits",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "session, turn, and audit reconstruction remains under #564",
		},
		{
			Concept:     "node_system_non_agent_replay",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "non-agent replay requires its own handler/idempotency/receipt owner",
		},
		{
			Concept:     "api_dashboard_mutation",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "operator surfaces remain consumers only",
		},
	}
}

func historicalReplayInvalidPaths() []runfork.RunForkSelectedContractExecutionBoundary {
	return []runfork.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "source_event_copy",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "source event IDs are lineage inputs; future replay must mint fork-local events under the fork run_id",
		},
		{
			Concept:     "source_delivery_copy",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "source event_deliveries are lineage or blockers, not executable fork delivery rows",
		},
		{
			Concept:     "source_outcome_suppression",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "source receipts, dead letters, retry/idempotency state, and post-T outcomes cannot suppress fork-local work",
		},
		{
			Concept:     "current_route_rows_as_fork_truth",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "current routing_rules, flow-instance routes, and runtime recovery rows are not historical replay admission owners",
		},
		{
			Concept:     "selected_frontier_execution_as_full_replay",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "supported selected frontier execution does not prove broader source-run resumption",
		},
		{
			Concept:     "cli_api_dashboard_owned_replay",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "operator surfaces may request or display replay admission but cannot own it",
		},
	}
}
