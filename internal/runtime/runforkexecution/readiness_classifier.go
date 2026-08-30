package runforkexecution

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

type SelectedContractReadinessClassifierRequest struct {
	Plan                      runfork.RunForkPlan
	ContractFrontierAdmission runfork.RunForkContractFrontierAdmission
	SelectedContractExecution runfork.RunForkSelectedContractExecution
}

func BuildSelectedContractReadinessClassifier(req SelectedContractReadinessClassifierRequest) (runfork.RunForkSelectedContractReadiness, error) {
	plan := req.Plan
	replayAdmission := plan.ReplayResumeAdmission
	if strings.TrimSpace(replayAdmission.Owner) != runfork.RunForkReplayResumeAdmissionOwner {
		return runfork.RunForkSelectedContractReadiness{}, fmt.Errorf("selected-contract readiness classifier requires %s; got %q", runfork.RunForkReplayResumeAdmissionOwner, replayAdmission.Owner)
	}
	frontier := req.ContractFrontierAdmission
	if strings.TrimSpace(frontier.Owner) != runfork.RunForkContractFrontierAdmissionOwner {
		return runfork.RunForkSelectedContractReadiness{}, fmt.Errorf("selected-contract readiness classifier requires %s; got %q", runfork.RunForkContractFrontierAdmissionOwner, frontier.Owner)
	}
	if !frontier.NonMutating {
		return runfork.RunForkSelectedContractReadiness{}, fmt.Errorf("selected-contract readiness classifier requires non-mutating frontier admission")
	}
	if frontier.HistoricalExecutionSupported {
		return runfork.RunForkSelectedContractReadiness{}, fmt.Errorf("selected-contract readiness classifier cannot consume mutating frontier admission")
	}
	model := req.SelectedContractExecution
	if strings.TrimSpace(model.Owner) != runfork.RunForkSelectedContractExecutionModelOwner {
		return runfork.RunForkSelectedContractReadiness{}, fmt.Errorf("selected-contract readiness classifier requires %s; got %q", runfork.RunForkSelectedContractExecutionModelOwner, model.Owner)
	}
	if !model.NonMutating {
		return runfork.RunForkSelectedContractReadiness{}, fmt.Errorf("selected-contract readiness classifier requires non-mutating selected execution model")
	}
	if model.ExecutionSupported {
		return runfork.RunForkSelectedContractReadiness{}, fmt.Errorf("selected-contract readiness classifier cannot consume mutating selected execution model")
	}
	if strings.TrimSpace(model.AdmissionOwner) != frontier.Owner {
		return runfork.RunForkSelectedContractReadiness{}, fmt.Errorf("selected-contract readiness classifier frontier owner mismatch")
	}
	if err := validateSelectionMatches("readiness classifier", frontier.ContractSelection, model.ContractSelection); err != nil {
		return runfork.RunForkSelectedContractReadiness{}, err
	}
	if model.RouteTopology == nil {
		return runfork.RunForkSelectedContractReadiness{}, fmt.Errorf("selected-contract readiness classifier requires route topology owner")
	}
	if model.RecipientPlanning == nil {
		return runfork.RunForkSelectedContractReadiness{}, fmt.Errorf("selected-contract readiness classifier requires recipient planning owner")
	}
	if strings.TrimSpace(model.RouteTopology.Owner) != runfork.RunForkSelectedContractRouteTopologyOwner {
		return runfork.RunForkSelectedContractReadiness{}, fmt.Errorf("selected-contract readiness classifier requires %s; got %q", runfork.RunForkSelectedContractRouteTopologyOwner, model.RouteTopology.Owner)
	}
	if strings.TrimSpace(model.RecipientPlanning.Owner) != runfork.RunForkSelectedContractRecipientPlanningOwner {
		return runfork.RunForkSelectedContractReadiness{}, fmt.Errorf("selected-contract readiness classifier requires %s; got %q", runfork.RunForkSelectedContractRecipientPlanningOwner, model.RecipientPlanning.Owner)
	}

	historicalFacts := historicalReplayFactAdmissions(replayAdmission)
	blockers := []runfork.RunForkUnsupportedBlocker{}
	for _, blocker := range replayAdmission.UnsupportedBlockers {
		blockers = appendRunForkUnsupportedBlocker(blockers, blocker)
	}
	for _, blocker := range frontier.UnsupportedBlockers {
		blockers = appendRunForkUnsupportedBlocker(blockers, blocker)
	}
	for _, blocker := range model.UnsupportedBlockers {
		blockers = appendRunForkUnsupportedBlocker(blockers, blocker)
	}

	readiness := runfork.RunForkSelectedContractReadiness{
		Owner:                          runfork.RunForkSelectedContractReadinessClassifierOwner,
		NonMutating:                    true,
		ContractSelection:              frontier.ContractSelection,
		PlannerOwner:                   runfork.RunForkPlanningOwner,
		ReplayResumeAdmissionOwner:     replayAdmission.Owner,
		ContractFrontierAdmissionOwner: frontier.Owner,
		RouteAdmissionOwner:            model.RouteTopology.RouteAdmissionOwner,
		RouteTopologyOwner:             model.RouteTopology.Owner,
		DynamicRouteTopologyOwner:      model.RouteTopology.DynamicTopologyOwner,
		RecipientPlanningOwner:         model.RecipientPlanning.Owner,
		SelectedExecutionModelOwner:    model.Owner,
		FutureExecutionOwner:           model.FutureExecutionOwner,
		FactMatrix:                     selectedContractReadinessFacts(plan, frontier, model, historicalFacts),
		RequiredConsumers:              selectedContractReadinessRequiredConsumers(),
		BlockedSiblings:                selectedContractReadinessBlockedSiblings(model),
		InvalidPaths:                   selectedContractReadinessInvalidPaths(),
		UnsupportedBlockers:            blockers,
	}
	if err := validateSelectedContractReadinessMatrix(readiness.FactMatrix); err != nil {
		return runfork.RunForkSelectedContractReadiness{}, err
	}
	return readiness, nil
}

func selectedContractReadinessFacts(
	plan runfork.RunForkPlan,
	frontier runfork.RunForkContractFrontierAdmission,
	model runfork.RunForkSelectedContractExecution,
	historicalFacts []runfork.RunForkHistoricalReplayFactAdmission,
) []runfork.RunForkSelectedContractReadinessFact {
	return []runfork.RunForkSelectedContractReadinessFact{
		readinessHistoricalFact(runfork.RunForkSelectedContractReadinessFactSourceEvents, runfork.RunForkHistoricalReplayFactSourceEvents, historicalFacts),
		readinessForkEvents(frontier),
		readinessHistoricalFact(runfork.RunForkSelectedContractReadinessFactSourceDeliveries, runfork.RunForkHistoricalReplayFactEventDeliveries, historicalFacts),
		readinessForkDeliveries(model),
		readinessSelectedRecipientsRoutes(model),
		readinessHistoricalFact(runfork.RunForkSelectedContractReadinessFactTimers, runfork.RunForkHistoricalReplayFactTimers, historicalFacts),
		readinessHistoricalFact(runfork.RunForkSelectedContractReadinessFactSessions, runfork.RunForkHistoricalReplayFactSessions, historicalFacts),
		readinessHistoricalFact(runfork.RunForkSelectedContractReadinessFactTurns, runfork.RunForkHistoricalReplayFactTurns, historicalFacts),
		readinessHistoricalFact(runfork.RunForkSelectedContractReadinessFactAudits, runfork.RunForkHistoricalReplayFactAudits, historicalFacts),
		readinessReplayDispositionFact(plan.ReplayResumeAdmission, runfork.RunForkSelectedContractReadinessFactCommittedReplayScopeMarkers, runfork.RunForkReplayResumeFactCommittedReplayScope, "source committed replay-scope marker facts are classified by the canonical replay taxonomy and selected-contract marker policy; fork-local recovery proof must be freshly written by fork owners"),
		readinessPlatformRuntimeFacts(frontier),
		readinessHistoricalFact(runfork.RunForkSelectedContractReadinessFactReceipts, runfork.RunForkHistoricalReplayFactReceipts, historicalFacts),
		readinessHistoricalFact(runfork.RunForkSelectedContractReadinessFactDeadLetters, runfork.RunForkHistoricalReplayFactDeadLetters, historicalFacts),
		readinessHistoricalFact(runfork.RunForkSelectedContractReadinessFactRetryIdempotency, runfork.RunForkHistoricalReplayFactRetryIdempotency, historicalFacts),
		readinessHistoricalFact(runfork.RunForkSelectedContractReadinessFactEmittedFollowUps, runfork.RunForkHistoricalReplayFactEmittedFollowUps, historicalFacts),
		readinessHistoricalFact(runfork.RunForkSelectedContractReadinessFactSourcePostTFacts, runfork.RunForkHistoricalReplayFactSourceAdvancedPostTFacts, historicalFacts),
		readinessCurrentStateSnapshots(plan),
		readinessHistoricalFact(runfork.RunForkSelectedContractReadinessFactNonAgentNodeSystemWork, runfork.RunForkHistoricalReplayFactNonAgentNodeSystemWork, historicalFacts),
		readinessHistoricalFact(runfork.RunForkSelectedContractReadinessFactRestartRecovery, runfork.RunForkHistoricalReplayFactRuntimeRestartRecovery, historicalFacts),
		readinessHistoricalFact(runfork.RunForkSelectedContractReadinessFactOperatorConsumers, runfork.RunForkHistoricalReplayFactCLIApiDashboardOperator, historicalFacts),
	}
}

func readinessHistoricalFact(readinessFact, historicalFact string, admissions []runfork.RunForkHistoricalReplayFactAdmission) runfork.RunForkSelectedContractReadinessFact {
	for _, admission := range admissions {
		if strings.TrimSpace(admission.Fact) != historicalFact {
			continue
		}
		return runfork.RunForkSelectedContractReadinessFact{
			Fact:        readinessFact,
			Disposition: readinessDispositionFromHistoricalFact(readinessFact, admission),
			Owner:       readinessOwnerFromHistorical(admission),
			SourceOwner: admission.SourceOwner,
			BlockerCode: admission.BlockerCode,
			Tracker:     admission.Tracker,
			Message:     admission.Message,
		}
	}
	return runfork.RunForkSelectedContractReadinessFact{
		Fact:        readinessFact,
		Disposition: runfork.RunForkSelectedContractReadinessDispositionFailClosedBlocker,
		Owner:       runfork.RunForkHistoricalReplayExecutionAdmissionOwner,
		Message:     "fact is absent from the canonical historical replay admission matrix and must fail closed",
	}
}

func readinessForkEvents(frontier runfork.RunForkContractFrontierAdmission) runfork.RunForkSelectedContractReadinessFact {
	if frontier.FrontierEventCount == 0 {
		return runfork.RunForkSelectedContractReadinessFact{
			Fact:        runfork.RunForkSelectedContractReadinessFactForkEvents,
			Disposition: runfork.RunForkSelectedContractReadinessDispositionLineageNoAction,
			Owner:       runfork.RunForkSelectedContractExecutionOwner,
			SourceOwner: frontier.Owner,
			Message:     "no selected-contract frontier events require fork-local event minting at this fork point",
		}
	}
	return runfork.RunForkSelectedContractReadinessFact{
		Fact:        runfork.RunForkSelectedContractReadinessFactForkEvents,
		Disposition: runfork.RunForkSelectedContractReadinessDispositionExecutableForkWork,
		Owner:       runfork.RunForkSelectedContractExecutionOwner,
		SourceOwner: frontier.Owner,
		Evidence:    []string{runfork.RunForkSelectedContractExecutionModelOwner},
		Message:     "selected execution may mint fresh fork-local events only through runtime.run_fork.selected_contract_execution; dry-run creates none",
	}
}

func readinessForkDeliveries(model runfork.RunForkSelectedContractExecution) runfork.RunForkSelectedContractReadinessFact {
	if model.RecipientPlanning == nil || len(model.RecipientPlanning.RecipientPlanEvents) == 0 {
		return runfork.RunForkSelectedContractReadinessFact{
			Fact:        runfork.RunForkSelectedContractReadinessFactForkDeliveries,
			Disposition: runfork.RunForkSelectedContractReadinessDispositionLineageNoAction,
			Owner:       runfork.RunForkSelectedContractExecutionOwner,
			SourceOwner: runfork.RunForkSelectedContractRecipientPlanningOwner,
			Message:     "no selected recipient plan requires fork-local delivery rows at this fork point",
		}
	}
	return runfork.RunForkSelectedContractReadinessFact{
		Fact:        runfork.RunForkSelectedContractReadinessFactForkDeliveries,
		Disposition: runfork.RunForkSelectedContractReadinessDispositionExecutableForkWork,
		Owner:       runfork.RunForkSelectedContractExecutionOwner,
		SourceOwner: runfork.RunForkSelectedContractRecipientPlanningOwner,
		Evidence:    []string{runfork.RunForkSelectedContractAuthoritativeAgentDeliveryMaterializationOwner},
		Message:     "selected execution may write fork-local event_deliveries only from canonical recipient planning; source deliveries are not executable truth",
	}
}

func readinessSelectedRecipientsRoutes(model runfork.RunForkSelectedContractExecution) runfork.RunForkSelectedContractReadinessFact {
	if model.RouteTopology != nil {
		for _, blocker := range model.RouteTopology.UnsupportedBlockers {
			if strings.TrimSpace(blocker.Code) == runfork.RunForkBlockerSelectedContractDynamicRouteTopologyUnproven {
				return runfork.RunForkSelectedContractReadinessFact{
					Fact:        runfork.RunForkSelectedContractReadinessFactSelectedRecipientsRoutes,
					Disposition: runfork.RunForkSelectedContractReadinessDispositionFailClosedBlocker,
					Owner:       runfork.RunForkSelectedContractRouteTopologyOwner,
					SourceOwner: runfork.RunForkSelectedContractRouteAdmissionOwner,
					BlockerCode: blocker.Code,
					Tracker:     "#615",
					Message:     blocker.Message,
				}
			}
		}
	}
	return runfork.RunForkSelectedContractReadinessFact{
		Fact:        runfork.RunForkSelectedContractReadinessFactSelectedRecipientsRoutes,
		Disposition: runfork.RunForkSelectedContractReadinessDispositionReconstructedForkState,
		Owner:       runfork.RunForkSelectedContractRecipientPlanningOwner,
		SourceOwner: runfork.RunForkSelectedContractRouteTopologyOwner,
		Evidence:    []string{runfork.RunForkSelectedContractRouteAdmissionOwner, runfork.RunForkSelectedContractRouteTopologyOwner},
		Message:     "selected route topology and recipient planning are non-mutating fork-local evidence; route rows and source deliveries are not copied",
	}
}

func readinessReplayDispositionFact(replay runfork.RunForkReplayResumeAdmission, fact, replayFact, fallbackMessage string) runfork.RunForkSelectedContractReadinessFact {
	for _, disposition := range replay.Dispositions {
		if strings.TrimSpace(disposition.Fact) != replayFact {
			continue
		}
		return runfork.RunForkSelectedContractReadinessFact{
			Fact:        fact,
			Disposition: readinessDispositionFromReplay(disposition.Disposition),
			Owner:       readinessReplayOwner(disposition),
			SourceOwner: runfork.RunForkReplayResumeAdmissionOwner,
			BlockerCode: disposition.BlockerCode,
			Message:     disposition.Message,
		}
	}
	return runfork.RunForkSelectedContractReadinessFact{
		Fact:        fact,
		Disposition: runfork.RunForkSelectedContractReadinessDispositionLineageNoAction,
		Owner:       runfork.RunForkReplayResumeAdmissionOwner,
		SourceOwner: runfork.RunForkReplayResumeAdmissionOwner,
		Message:     fallbackMessage,
	}
}

func readinessPlatformRuntimeFacts(frontier runfork.RunForkContractFrontierAdmission) runfork.RunForkSelectedContractReadinessFact {
	if len(frontier.LineageOnlyEvents) > 0 {
		return runfork.RunForkSelectedContractReadinessFact{
			Fact:        runfork.RunForkSelectedContractReadinessFactPlatformRuntimeDiagnostics,
			Disposition: runfork.RunForkSelectedContractReadinessDispositionLineageNoAction,
			Owner:       runfork.RunForkSelectedContractDiagnosticPlatformOutcomePolicyOwner,
			SourceOwner: frontier.Owner,
			Evidence:    []string{runfork.RunForkSelectedContractForkLocalRuntimePlatformEventLineagePolicyOwner},
			Message:     "source diagnostic platform outcome facts are lineage/no-action only; fresh fork-local platform/runtime rows require selected-fork causal lineage",
		}
	}
	return runfork.RunForkSelectedContractReadinessFact{
		Fact:        runfork.RunForkSelectedContractReadinessFactPlatformRuntimeDiagnostics,
		Disposition: runfork.RunForkSelectedContractReadinessDispositionUnsupportedSplitSibling,
		Owner:       runfork.RunForkSelectedContractForkLocalRuntimePlatformEventLineagePolicyOwner,
		SourceOwner: frontier.Owner,
		Tracker:     "#702",
		Message:     "fork-local runtime/platform diagnostic and control rows remain owned by selected-fork runtime platform-event lineage; unrelated platform rows fail closed",
	}
}

func readinessCurrentStateSnapshots(plan runfork.RunForkPlan) runfork.RunForkSelectedContractReadinessFact {
	disposition := runfork.RunForkSelectedContractReadinessDispositionReconstructedForkState
	blockerCode := ""
	for _, blocker := range plan.UnsupportedBlockers {
		if strings.TrimSpace(blocker.Code) == runfork.RunForkBlockerEntitySnapshotMetadataUnproven {
			disposition = runfork.RunForkSelectedContractReadinessDispositionFailClosedBlocker
			blockerCode = blocker.Code
			break
		}
	}
	return runfork.RunForkSelectedContractReadinessFact{
		Fact:        runfork.RunForkSelectedContractReadinessFactCurrentStateSnapshots,
		Disposition: disposition,
		Owner:       runfork.RunForkMaterializedEntitySnapshotMetadataOwner,
		SourceOwner: "entity_mutations",
		BlockerCode: blockerCode,
		Tracker:     "#681",
		Message:     "fork-local current-state snapshots are reconstructed only from planner/entity-mutation evidence and materialized through the snapshot metadata owner; source current rows are not copied",
	}
}

func readinessDispositionFromHistorical(admission string) string {
	switch strings.TrimSpace(admission) {
	case runfork.RunForkHistoricalReplayAdmissionExecutableForkWork:
		return runfork.RunForkSelectedContractReadinessDispositionExecutableForkWork
	case runfork.RunForkHistoricalReplayAdmissionReconstructedForkState:
		return runfork.RunForkSelectedContractReadinessDispositionReconstructedForkState
	case runfork.RunForkHistoricalReplayAdmissionLineageOnlyEvidence:
		return runfork.RunForkSelectedContractReadinessDispositionLineageNoAction
	case runfork.RunForkHistoricalReplayAdmissionFailClosedBlocker:
		return runfork.RunForkSelectedContractReadinessDispositionFailClosedBlocker
	case runfork.RunForkHistoricalReplayAdmissionSplitSibling:
		return runfork.RunForkSelectedContractReadinessDispositionUnsupportedSplitSibling
	default:
		return runfork.RunForkSelectedContractReadinessDispositionFailClosedBlocker
	}
}

func readinessDispositionFromHistoricalFact(readinessFact string, admission runfork.RunForkHistoricalReplayFactAdmission) string {
	if strings.TrimSpace(admission.Admission) == runfork.RunForkHistoricalReplayAdmissionLineageOnlyEvidence {
		switch readinessFact {
		case runfork.RunForkSelectedContractReadinessFactSourcePostTFacts:
			if strings.TrimSpace(admission.SourceOwner) == runfork.RunForkSelectedContractSourceAdvancedConversationHistoryPolicyOwner {
				return runfork.RunForkSelectedContractReadinessDispositionBranchDivergenceEvidence
			}
		case runfork.RunForkSelectedContractReadinessFactSourceDeliveries:
			if strings.TrimSpace(admission.SourceOwner) == runfork.RunForkSelectedContractActiveSourceDeliveryConversationCouplingPolicyOwner {
				return runfork.RunForkSelectedContractReadinessDispositionBranchDivergenceEvidence
			}
		}
	}
	return readinessDispositionFromHistorical(admission.Admission)
}

func readinessDispositionFromReplay(disposition string) string {
	switch strings.TrimSpace(disposition) {
	case runfork.RunForkReplayResumeDispositionForkReplay:
		return runfork.RunForkSelectedContractReadinessDispositionExecutableForkWork
	case runfork.RunForkReplayResumeDispositionReconstruct:
		return runfork.RunForkSelectedContractReadinessDispositionReconstructedForkState
	case runfork.RunForkReplayResumeDispositionLineageOnly, runfork.RunForkReplayResumeDispositionNoHistoricalAction:
		return runfork.RunForkSelectedContractReadinessDispositionLineageNoAction
	case runfork.RunForkReplayResumeDispositionFailClosedBlocker:
		return runfork.RunForkSelectedContractReadinessDispositionFailClosedBlocker
	case runfork.RunForkReplayResumeDispositionSplitSibling:
		return runfork.RunForkSelectedContractReadinessDispositionUnsupportedSplitSibling
	default:
		return runfork.RunForkSelectedContractReadinessDispositionFailClosedBlocker
	}
}

func readinessOwnerFromHistorical(admission runfork.RunForkHistoricalReplayFactAdmission) string {
	switch strings.TrimSpace(admission.Admission) {
	case runfork.RunForkHistoricalReplayAdmissionExecutableForkWork:
		return runfork.RunForkHistoricalReplayExecutionOwner
	case runfork.RunForkHistoricalReplayAdmissionReconstructedForkState:
		if strings.TrimSpace(admission.SourceOwner) != "" {
			return strings.TrimSpace(admission.SourceOwner)
		}
		return runfork.RunForkHistoricalReplayExecutionOwner
	case runfork.RunForkHistoricalReplayAdmissionLineageOnlyEvidence, runfork.RunForkHistoricalReplayAdmissionFailClosedBlocker:
		if strings.TrimSpace(admission.SourceOwner) != "" {
			return strings.TrimSpace(admission.SourceOwner)
		}
		return runfork.RunForkHistoricalReplayExecutionAdmissionOwner
	case runfork.RunForkHistoricalReplayAdmissionSplitSibling:
		return runfork.RunForkHistoricalReplayExecutionAdmissionOwner
	default:
		return runfork.RunForkHistoricalReplayExecutionAdmissionOwner
	}
}

func readinessReplayOwner(disposition runfork.RunForkReplayResumeDisposition) string {
	if strings.TrimSpace(disposition.Owner) != "" {
		return strings.TrimSpace(disposition.Owner)
	}
	switch strings.TrimSpace(disposition.Disposition) {
	case runfork.RunForkReplayResumeDispositionForkReplay:
		return runfork.RunForkHistoricalReplayExecutionOwner
	case runfork.RunForkReplayResumeDispositionReconstruct:
		return runfork.RunForkHistoricalReplayExecutionOwner
	default:
		return runfork.RunForkReplayResumeAdmissionOwner
	}
}

func selectedContractReadinessRequiredConsumers() []runfork.RunForkSelectedContractExecutionBoundary {
	return []runfork.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "fork_local_runtime_container",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner,
			Reason:      "mutating selected execution must construct the canonical fork-local runtime container before EventBus, AgentManager, RuntimeLogger, activation, cleanup, or quiescence can be authoritative",
		},
		{
			Concept:     "fork_local_runtime_typed_lineage",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractForkLocalRuntimeTypedLineageOwner,
			Reason:      "runtime diagnostics and platform-control outputs emitted by selected execution must consume typed lineage before persistence writes canonical source_event_id lineage",
		},
		{
			Concept:     "cli_dry_run_explain_json",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractReadinessClassifierOwner,
			Reason:      "supported explain output must consume the canonical readiness classifier and must not synthesize the matrix in CLI code",
		},
		{
			Concept:     "future_api_dashboard_consumers",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       runfork.RunForkSelectedContractReadinessClassifierOwner,
			Reason:      "operator surfaces may display the classifier matrix later, but they cannot own readiness semantics",
		},
	}
}

func selectedContractReadinessBlockedSiblings(model runfork.RunForkSelectedContractExecution) []runfork.RunForkSelectedContractExecutionBoundary {
	out := []runfork.RunForkSelectedContractExecutionBoundary{}
	out = append(out, model.BlockedSiblings...)
	return out
}

func selectedContractReadinessInvalidPaths() []runfork.RunForkSelectedContractExecutionBoundary {
	return []runfork.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "cli_owned_readiness",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "CLI/API/dashboard/Builder are consumers only and must not compute selected-contract fork readiness semantics",
		},
		{
			Concept:     "source_row_copy_as_executable_truth",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "source events, deliveries, receipts, dead letters, sessions, timers, and routes are lineage or blocker evidence; fork execution must mint fresh fork-local truth",
		},
		{
			Concept:     "source_outcome_suppression",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "source outcomes cannot suppress fork-local selected execution or follow-up generation",
		},
		{
			Concept:     "explain_output_authorizes_mutation",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "readiness explanation is non-mutating evidence and does not weaken materialization, execution, or activation fail-closed gates",
		},
	}
}

func validateSelectedContractReadinessMatrix(facts []runfork.RunForkSelectedContractReadinessFact) error {
	required := []string{
		runfork.RunForkSelectedContractReadinessFactSourceEvents,
		runfork.RunForkSelectedContractReadinessFactForkEvents,
		runfork.RunForkSelectedContractReadinessFactSourceDeliveries,
		runfork.RunForkSelectedContractReadinessFactForkDeliveries,
		runfork.RunForkSelectedContractReadinessFactSelectedRecipientsRoutes,
		runfork.RunForkSelectedContractReadinessFactTimers,
		runfork.RunForkSelectedContractReadinessFactSessions,
		runfork.RunForkSelectedContractReadinessFactTurns,
		runfork.RunForkSelectedContractReadinessFactAudits,
		runfork.RunForkSelectedContractReadinessFactCommittedReplayScopeMarkers,
		runfork.RunForkSelectedContractReadinessFactPlatformRuntimeDiagnostics,
		runfork.RunForkSelectedContractReadinessFactReceipts,
		runfork.RunForkSelectedContractReadinessFactDeadLetters,
		runfork.RunForkSelectedContractReadinessFactRetryIdempotency,
		runfork.RunForkSelectedContractReadinessFactEmittedFollowUps,
		runfork.RunForkSelectedContractReadinessFactSourcePostTFacts,
		runfork.RunForkSelectedContractReadinessFactCurrentStateSnapshots,
		runfork.RunForkSelectedContractReadinessFactNonAgentNodeSystemWork,
		runfork.RunForkSelectedContractReadinessFactRestartRecovery,
		runfork.RunForkSelectedContractReadinessFactOperatorConsumers,
	}
	seen := map[string]struct{}{}
	for _, fact := range facts {
		name := strings.TrimSpace(fact.Fact)
		if name == "" {
			return fmt.Errorf("selected-contract readiness matrix contains unnamed fact")
		}
		if strings.TrimSpace(fact.Disposition) == "" {
			return fmt.Errorf("selected-contract readiness fact %s has no disposition", name)
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("selected-contract readiness matrix repeats fact %s", name)
		}
		seen[name] = struct{}{}
	}
	for _, requiredFact := range required {
		if _, ok := seen[requiredFact]; !ok {
			return fmt.Errorf("selected-contract readiness matrix missing fact %s", requiredFact)
		}
	}
	return nil
}
