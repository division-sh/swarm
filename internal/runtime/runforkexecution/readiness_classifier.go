package runforkexecution

import (
	"fmt"
	"strings"

	"swarm/internal/store"
)

type SelectedContractReadinessClassifierRequest struct {
	Plan                      store.RunForkPlan
	ContractFrontierAdmission store.RunForkContractFrontierAdmission
	SelectedContractExecution store.RunForkSelectedContractExecution
}

func BuildSelectedContractReadinessClassifier(req SelectedContractReadinessClassifierRequest) (store.RunForkSelectedContractReadiness, error) {
	plan := req.Plan
	replayAdmission := plan.ReplayResumeAdmission
	if strings.TrimSpace(replayAdmission.Owner) != store.RunForkReplayResumeAdmissionOwner {
		return store.RunForkSelectedContractReadiness{}, fmt.Errorf("selected-contract readiness classifier requires %s; got %q", store.RunForkReplayResumeAdmissionOwner, replayAdmission.Owner)
	}
	frontier := req.ContractFrontierAdmission
	if strings.TrimSpace(frontier.Owner) != store.RunForkContractFrontierAdmissionOwner {
		return store.RunForkSelectedContractReadiness{}, fmt.Errorf("selected-contract readiness classifier requires %s; got %q", store.RunForkContractFrontierAdmissionOwner, frontier.Owner)
	}
	if !frontier.NonMutating {
		return store.RunForkSelectedContractReadiness{}, fmt.Errorf("selected-contract readiness classifier requires non-mutating frontier admission")
	}
	if frontier.HistoricalExecutionSupported {
		return store.RunForkSelectedContractReadiness{}, fmt.Errorf("selected-contract readiness classifier cannot consume mutating frontier admission")
	}
	model := req.SelectedContractExecution
	if strings.TrimSpace(model.Owner) != store.RunForkSelectedContractExecutionModelOwner {
		return store.RunForkSelectedContractReadiness{}, fmt.Errorf("selected-contract readiness classifier requires %s; got %q", store.RunForkSelectedContractExecutionModelOwner, model.Owner)
	}
	if !model.NonMutating {
		return store.RunForkSelectedContractReadiness{}, fmt.Errorf("selected-contract readiness classifier requires non-mutating selected execution model")
	}
	if model.ExecutionSupported {
		return store.RunForkSelectedContractReadiness{}, fmt.Errorf("selected-contract readiness classifier cannot consume mutating selected execution model")
	}
	if strings.TrimSpace(model.AdmissionOwner) != frontier.Owner {
		return store.RunForkSelectedContractReadiness{}, fmt.Errorf("selected-contract readiness classifier frontier owner mismatch")
	}
	if err := validateSelectionMatches("readiness classifier", frontier.ContractSelection, model.ContractSelection); err != nil {
		return store.RunForkSelectedContractReadiness{}, err
	}
	if model.RouteTopology == nil {
		return store.RunForkSelectedContractReadiness{}, fmt.Errorf("selected-contract readiness classifier requires route topology owner")
	}
	if model.RecipientPlanning == nil {
		return store.RunForkSelectedContractReadiness{}, fmt.Errorf("selected-contract readiness classifier requires recipient planning owner")
	}
	if strings.TrimSpace(model.RouteTopology.Owner) != store.RunForkSelectedContractRouteTopologyOwner {
		return store.RunForkSelectedContractReadiness{}, fmt.Errorf("selected-contract readiness classifier requires %s; got %q", store.RunForkSelectedContractRouteTopologyOwner, model.RouteTopology.Owner)
	}
	if strings.TrimSpace(model.RecipientPlanning.Owner) != store.RunForkSelectedContractRecipientPlanningOwner {
		return store.RunForkSelectedContractReadiness{}, fmt.Errorf("selected-contract readiness classifier requires %s; got %q", store.RunForkSelectedContractRecipientPlanningOwner, model.RecipientPlanning.Owner)
	}

	historicalFacts := historicalReplayFactAdmissions(replayAdmission)
	blockers := []store.RunForkUnsupportedBlocker{}
	for _, blocker := range replayAdmission.UnsupportedBlockers {
		blockers = appendRunForkUnsupportedBlocker(blockers, blocker)
	}
	for _, blocker := range frontier.UnsupportedBlockers {
		blockers = appendRunForkUnsupportedBlocker(blockers, blocker)
	}
	for _, blocker := range model.UnsupportedBlockers {
		blockers = appendRunForkUnsupportedBlocker(blockers, blocker)
	}

	readiness := store.RunForkSelectedContractReadiness{
		Owner:                          store.RunForkSelectedContractReadinessClassifierOwner,
		NonMutating:                    true,
		ContractSelection:              frontier.ContractSelection,
		PlannerOwner:                   store.RunForkPlanningOwner,
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
		return store.RunForkSelectedContractReadiness{}, err
	}
	return readiness, nil
}

func selectedContractReadinessFacts(
	plan store.RunForkPlan,
	frontier store.RunForkContractFrontierAdmission,
	model store.RunForkSelectedContractExecution,
	historicalFacts []store.RunForkHistoricalReplayFactAdmission,
) []store.RunForkSelectedContractReadinessFact {
	return []store.RunForkSelectedContractReadinessFact{
		readinessHistoricalFact(store.RunForkSelectedContractReadinessFactSourceEvents, store.RunForkHistoricalReplayFactSourceEvents, historicalFacts),
		readinessForkEvents(frontier),
		readinessHistoricalFact(store.RunForkSelectedContractReadinessFactSourceDeliveries, store.RunForkHistoricalReplayFactEventDeliveries, historicalFacts),
		readinessForkDeliveries(model),
		readinessSelectedRecipientsRoutes(model),
		readinessHistoricalFact(store.RunForkSelectedContractReadinessFactTimers, store.RunForkHistoricalReplayFactTimers, historicalFacts),
		readinessHistoricalFact(store.RunForkSelectedContractReadinessFactSessions, store.RunForkHistoricalReplayFactSessions, historicalFacts),
		readinessHistoricalFact(store.RunForkSelectedContractReadinessFactTurns, store.RunForkHistoricalReplayFactTurns, historicalFacts),
		readinessHistoricalFact(store.RunForkSelectedContractReadinessFactAudits, store.RunForkHistoricalReplayFactAudits, historicalFacts),
		readinessReplayDispositionFact(plan.ReplayResumeAdmission, store.RunForkSelectedContractReadinessFactCommittedReplayScopeMarkers, store.RunForkReplayResumeFactCommittedReplayScope, "source committed replay-scope marker facts are classified by the canonical replay taxonomy and selected-contract marker policy; fork-local recovery proof must be freshly written by fork owners"),
		readinessPlatformRuntimeFacts(frontier),
		readinessHistoricalFact(store.RunForkSelectedContractReadinessFactReceipts, store.RunForkHistoricalReplayFactReceipts, historicalFacts),
		readinessHistoricalFact(store.RunForkSelectedContractReadinessFactDeadLetters, store.RunForkHistoricalReplayFactDeadLetters, historicalFacts),
		readinessHistoricalFact(store.RunForkSelectedContractReadinessFactRetryIdempotency, store.RunForkHistoricalReplayFactRetryIdempotency, historicalFacts),
		readinessHistoricalFact(store.RunForkSelectedContractReadinessFactEmittedFollowUps, store.RunForkHistoricalReplayFactEmittedFollowUps, historicalFacts),
		readinessHistoricalFact(store.RunForkSelectedContractReadinessFactSourcePostTFacts, store.RunForkHistoricalReplayFactSourceAdvancedPostTFacts, historicalFacts),
		readinessCurrentStateSnapshots(plan),
		readinessHistoricalFact(store.RunForkSelectedContractReadinessFactNonAgentNodeSystemWork, store.RunForkHistoricalReplayFactNonAgentNodeSystemWork, historicalFacts),
		readinessHistoricalFact(store.RunForkSelectedContractReadinessFactRestartRecovery, store.RunForkHistoricalReplayFactRuntimeRestartRecovery, historicalFacts),
		readinessHistoricalFact(store.RunForkSelectedContractReadinessFactOperatorConsumers, store.RunForkHistoricalReplayFactCLIApiDashboardOperator, historicalFacts),
	}
}

func readinessHistoricalFact(readinessFact, historicalFact string, admissions []store.RunForkHistoricalReplayFactAdmission) store.RunForkSelectedContractReadinessFact {
	for _, admission := range admissions {
		if strings.TrimSpace(admission.Fact) != historicalFact {
			continue
		}
		return store.RunForkSelectedContractReadinessFact{
			Fact:        readinessFact,
			Disposition: readinessDispositionFromHistoricalFact(readinessFact, admission),
			Owner:       readinessOwnerFromHistorical(admission),
			SourceOwner: admission.SourceOwner,
			BlockerCode: admission.BlockerCode,
			Tracker:     admission.Tracker,
			Message:     admission.Message,
		}
	}
	return store.RunForkSelectedContractReadinessFact{
		Fact:        readinessFact,
		Disposition: store.RunForkSelectedContractReadinessDispositionFailClosedBlocker,
		Owner:       store.RunForkHistoricalReplayExecutionAdmissionOwner,
		Message:     "fact is absent from the canonical historical replay admission matrix and must fail closed",
	}
}

func readinessForkEvents(frontier store.RunForkContractFrontierAdmission) store.RunForkSelectedContractReadinessFact {
	if frontier.FrontierEventCount == 0 {
		return store.RunForkSelectedContractReadinessFact{
			Fact:        store.RunForkSelectedContractReadinessFactForkEvents,
			Disposition: store.RunForkSelectedContractReadinessDispositionLineageNoAction,
			Owner:       store.RunForkSelectedContractExecutionOwner,
			SourceOwner: frontier.Owner,
			Message:     "no selected-contract frontier events require fork-local event minting at this fork point",
		}
	}
	return store.RunForkSelectedContractReadinessFact{
		Fact:        store.RunForkSelectedContractReadinessFactForkEvents,
		Disposition: store.RunForkSelectedContractReadinessDispositionExecutableForkWork,
		Owner:       store.RunForkSelectedContractExecutionOwner,
		SourceOwner: frontier.Owner,
		Evidence:    []string{store.RunForkSelectedContractExecutionModelOwner},
		Message:     "selected execution may mint fresh fork-local events only through runtime.run_fork.selected_contract_execution; dry-run creates none",
	}
}

func readinessForkDeliveries(model store.RunForkSelectedContractExecution) store.RunForkSelectedContractReadinessFact {
	if model.RecipientPlanning == nil || len(model.RecipientPlanning.RecipientPlanEvents) == 0 {
		return store.RunForkSelectedContractReadinessFact{
			Fact:        store.RunForkSelectedContractReadinessFactForkDeliveries,
			Disposition: store.RunForkSelectedContractReadinessDispositionLineageNoAction,
			Owner:       store.RunForkSelectedContractExecutionOwner,
			SourceOwner: store.RunForkSelectedContractRecipientPlanningOwner,
			Message:     "no selected recipient plan requires fork-local delivery rows at this fork point",
		}
	}
	return store.RunForkSelectedContractReadinessFact{
		Fact:        store.RunForkSelectedContractReadinessFactForkDeliveries,
		Disposition: store.RunForkSelectedContractReadinessDispositionExecutableForkWork,
		Owner:       store.RunForkSelectedContractExecutionOwner,
		SourceOwner: store.RunForkSelectedContractRecipientPlanningOwner,
		Evidence:    []string{store.RunForkSelectedContractAuthoritativeAgentDeliveryMaterializationOwner},
		Message:     "selected execution may write fork-local event_deliveries only from canonical recipient planning; source deliveries are not executable truth",
	}
}

func readinessSelectedRecipientsRoutes(model store.RunForkSelectedContractExecution) store.RunForkSelectedContractReadinessFact {
	if model.RouteTopology != nil {
		for _, blocker := range model.RouteTopology.UnsupportedBlockers {
			if strings.TrimSpace(blocker.Code) == store.RunForkBlockerSelectedContractDynamicRouteTopologyUnproven {
				return store.RunForkSelectedContractReadinessFact{
					Fact:        store.RunForkSelectedContractReadinessFactSelectedRecipientsRoutes,
					Disposition: store.RunForkSelectedContractReadinessDispositionFailClosedBlocker,
					Owner:       store.RunForkSelectedContractRouteTopologyOwner,
					SourceOwner: store.RunForkSelectedContractRouteAdmissionOwner,
					BlockerCode: blocker.Code,
					Tracker:     "#615",
					Message:     blocker.Message,
				}
			}
		}
	}
	return store.RunForkSelectedContractReadinessFact{
		Fact:        store.RunForkSelectedContractReadinessFactSelectedRecipientsRoutes,
		Disposition: store.RunForkSelectedContractReadinessDispositionReconstructedForkState,
		Owner:       store.RunForkSelectedContractRecipientPlanningOwner,
		SourceOwner: store.RunForkSelectedContractRouteTopologyOwner,
		Evidence:    []string{store.RunForkSelectedContractRouteAdmissionOwner, store.RunForkSelectedContractRouteTopologyOwner},
		Message:     "selected route topology and recipient planning are non-mutating fork-local evidence; route rows and source deliveries are not copied",
	}
}

func readinessReplayDispositionFact(replay store.RunForkReplayResumeAdmission, fact, replayFact, fallbackMessage string) store.RunForkSelectedContractReadinessFact {
	for _, disposition := range replay.Dispositions {
		if strings.TrimSpace(disposition.Fact) != replayFact {
			continue
		}
		return store.RunForkSelectedContractReadinessFact{
			Fact:        fact,
			Disposition: readinessDispositionFromReplay(disposition.Disposition),
			Owner:       readinessReplayOwner(disposition),
			SourceOwner: store.RunForkReplayResumeAdmissionOwner,
			BlockerCode: disposition.BlockerCode,
			Message:     disposition.Message,
		}
	}
	return store.RunForkSelectedContractReadinessFact{
		Fact:        fact,
		Disposition: store.RunForkSelectedContractReadinessDispositionLineageNoAction,
		Owner:       store.RunForkReplayResumeAdmissionOwner,
		SourceOwner: store.RunForkReplayResumeAdmissionOwner,
		Message:     fallbackMessage,
	}
}

func readinessPlatformRuntimeFacts(frontier store.RunForkContractFrontierAdmission) store.RunForkSelectedContractReadinessFact {
	if len(frontier.LineageOnlyEvents) > 0 {
		return store.RunForkSelectedContractReadinessFact{
			Fact:        store.RunForkSelectedContractReadinessFactPlatformRuntimeDiagnostics,
			Disposition: store.RunForkSelectedContractReadinessDispositionLineageNoAction,
			Owner:       store.RunForkSelectedContractDiagnosticPlatformOutcomePolicyOwner,
			SourceOwner: frontier.Owner,
			Evidence:    []string{store.RunForkSelectedContractForkLocalRuntimePlatformEventLineagePolicyOwner},
			Message:     "source diagnostic platform outcome facts are lineage/no-action only; fresh fork-local platform/runtime rows require selected-fork causal lineage",
		}
	}
	return store.RunForkSelectedContractReadinessFact{
		Fact:        store.RunForkSelectedContractReadinessFactPlatformRuntimeDiagnostics,
		Disposition: store.RunForkSelectedContractReadinessDispositionUnsupportedSplitSibling,
		Owner:       store.RunForkSelectedContractForkLocalRuntimePlatformEventLineagePolicyOwner,
		SourceOwner: frontier.Owner,
		Tracker:     "#702",
		Message:     "fork-local runtime/platform diagnostic and control rows remain owned by selected-fork runtime platform-event lineage; unrelated platform rows fail closed",
	}
}

func readinessCurrentStateSnapshots(plan store.RunForkPlan) store.RunForkSelectedContractReadinessFact {
	disposition := store.RunForkSelectedContractReadinessDispositionReconstructedForkState
	blockerCode := ""
	for _, blocker := range plan.UnsupportedBlockers {
		if strings.TrimSpace(blocker.Code) == store.RunForkBlockerEntitySnapshotMetadataUnproven {
			disposition = store.RunForkSelectedContractReadinessDispositionFailClosedBlocker
			blockerCode = blocker.Code
			break
		}
	}
	return store.RunForkSelectedContractReadinessFact{
		Fact:        store.RunForkSelectedContractReadinessFactCurrentStateSnapshots,
		Disposition: disposition,
		Owner:       store.RunForkMaterializedEntitySnapshotMetadataOwner,
		SourceOwner: "entity_mutations",
		BlockerCode: blockerCode,
		Tracker:     "#681",
		Message:     "fork-local current-state snapshots are reconstructed only from planner/entity-mutation evidence and materialized through the snapshot metadata owner; source current rows are not copied",
	}
}

func readinessDispositionFromHistorical(admission string) string {
	switch strings.TrimSpace(admission) {
	case store.RunForkHistoricalReplayAdmissionExecutableForkWork:
		return store.RunForkSelectedContractReadinessDispositionExecutableForkWork
	case store.RunForkHistoricalReplayAdmissionReconstructedForkState:
		return store.RunForkSelectedContractReadinessDispositionReconstructedForkState
	case store.RunForkHistoricalReplayAdmissionLineageOnlyEvidence:
		return store.RunForkSelectedContractReadinessDispositionLineageNoAction
	case store.RunForkHistoricalReplayAdmissionFailClosedBlocker:
		return store.RunForkSelectedContractReadinessDispositionFailClosedBlocker
	case store.RunForkHistoricalReplayAdmissionSplitSibling:
		return store.RunForkSelectedContractReadinessDispositionUnsupportedSplitSibling
	default:
		return store.RunForkSelectedContractReadinessDispositionFailClosedBlocker
	}
}

func readinessDispositionFromHistoricalFact(readinessFact string, admission store.RunForkHistoricalReplayFactAdmission) string {
	if strings.TrimSpace(admission.Admission) == store.RunForkHistoricalReplayAdmissionLineageOnlyEvidence {
		switch readinessFact {
		case store.RunForkSelectedContractReadinessFactSourcePostTFacts:
			if strings.TrimSpace(admission.SourceOwner) == store.RunForkSelectedContractSourceAdvancedConversationHistoryPolicyOwner {
				return store.RunForkSelectedContractReadinessDispositionBranchDivergenceEvidence
			}
		case store.RunForkSelectedContractReadinessFactSourceDeliveries:
			if strings.TrimSpace(admission.SourceOwner) == store.RunForkSelectedContractActiveSourceDeliveryConversationCouplingPolicyOwner {
				return store.RunForkSelectedContractReadinessDispositionBranchDivergenceEvidence
			}
		}
	}
	return readinessDispositionFromHistorical(admission.Admission)
}

func readinessDispositionFromReplay(disposition string) string {
	switch strings.TrimSpace(disposition) {
	case store.RunForkReplayResumeDispositionForkReplay:
		return store.RunForkSelectedContractReadinessDispositionExecutableForkWork
	case store.RunForkReplayResumeDispositionReconstruct:
		return store.RunForkSelectedContractReadinessDispositionReconstructedForkState
	case store.RunForkReplayResumeDispositionLineageOnly, store.RunForkReplayResumeDispositionNoHistoricalAction:
		return store.RunForkSelectedContractReadinessDispositionLineageNoAction
	case store.RunForkReplayResumeDispositionFailClosedBlocker:
		return store.RunForkSelectedContractReadinessDispositionFailClosedBlocker
	case store.RunForkReplayResumeDispositionSplitSibling:
		return store.RunForkSelectedContractReadinessDispositionUnsupportedSplitSibling
	default:
		return store.RunForkSelectedContractReadinessDispositionFailClosedBlocker
	}
}

func readinessOwnerFromHistorical(admission store.RunForkHistoricalReplayFactAdmission) string {
	switch strings.TrimSpace(admission.Admission) {
	case store.RunForkHistoricalReplayAdmissionExecutableForkWork:
		return store.RunForkHistoricalReplayExecutionOwner
	case store.RunForkHistoricalReplayAdmissionReconstructedForkState:
		if strings.TrimSpace(admission.SourceOwner) != "" {
			return strings.TrimSpace(admission.SourceOwner)
		}
		return store.RunForkHistoricalReplayExecutionOwner
	case store.RunForkHistoricalReplayAdmissionLineageOnlyEvidence, store.RunForkHistoricalReplayAdmissionFailClosedBlocker:
		if strings.TrimSpace(admission.SourceOwner) != "" {
			return strings.TrimSpace(admission.SourceOwner)
		}
		return store.RunForkHistoricalReplayExecutionAdmissionOwner
	case store.RunForkHistoricalReplayAdmissionSplitSibling:
		return store.RunForkHistoricalReplayExecutionAdmissionOwner
	default:
		return store.RunForkHistoricalReplayExecutionAdmissionOwner
	}
}

func readinessReplayOwner(disposition store.RunForkReplayResumeDisposition) string {
	if strings.TrimSpace(disposition.Owner) != "" {
		return strings.TrimSpace(disposition.Owner)
	}
	switch strings.TrimSpace(disposition.Disposition) {
	case store.RunForkReplayResumeDispositionForkReplay:
		return store.RunForkHistoricalReplayExecutionOwner
	case store.RunForkReplayResumeDispositionReconstruct:
		return store.RunForkHistoricalReplayExecutionOwner
	default:
		return store.RunForkReplayResumeAdmissionOwner
	}
}

func selectedContractReadinessRequiredConsumers() []store.RunForkSelectedContractExecutionBoundary {
	return []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "cli_dry_run_explain_json",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       store.RunForkSelectedContractReadinessClassifierOwner,
			Reason:      "supported explain output must consume the canonical readiness classifier and must not synthesize the matrix in CLI code",
		},
		{
			Concept:     "future_api_dashboard_builder_consumers",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       store.RunForkSelectedContractReadinessClassifierOwner,
			Reason:      "operator surfaces may display the classifier matrix later, but they cannot own readiness semantics",
		},
	}
}

func selectedContractReadinessBlockedSiblings(model store.RunForkSelectedContractExecution) []store.RunForkSelectedContractExecutionBoundary {
	out := []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "typed_runtime_lineage",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       store.RunForkSelectedContractForkLocalRuntimePlatformEventLineagePolicyOwner,
			Reason:      "#708 remains the typed-lineage architecture sibling; readiness explains current owner output without requiring that refactor",
		},
		{
			Concept:     "fork_local_runtime_container",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       store.RunForkSelectedContractExecutionOwner + ".fork_local_runtime_container",
			Reason:      "#709 remains the runtime-container architecture sibling; readiness is non-mutating explanation only",
		},
	}
	out = append(out, model.BlockedSiblings...)
	return out
}

func selectedContractReadinessInvalidPaths() []store.RunForkSelectedContractExecutionBoundary {
	return []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "cli_owned_readiness",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "CLI/API/dashboard/Builder are consumers only and must not compute selected-contract fork readiness semantics",
		},
		{
			Concept:     "source_row_copy_as_executable_truth",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "source events, deliveries, receipts, dead letters, sessions, timers, and routes are lineage or blocker evidence; fork execution must mint fresh fork-local truth",
		},
		{
			Concept:     "source_outcome_suppression",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "source outcomes cannot suppress fork-local selected execution or follow-up generation",
		},
		{
			Concept:     "explain_output_authorizes_mutation",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "readiness explanation is non-mutating evidence and does not weaken materialization, execution, or activation fail-closed gates",
		},
	}
}

func validateSelectedContractReadinessMatrix(facts []store.RunForkSelectedContractReadinessFact) error {
	required := []string{
		store.RunForkSelectedContractReadinessFactSourceEvents,
		store.RunForkSelectedContractReadinessFactForkEvents,
		store.RunForkSelectedContractReadinessFactSourceDeliveries,
		store.RunForkSelectedContractReadinessFactForkDeliveries,
		store.RunForkSelectedContractReadinessFactSelectedRecipientsRoutes,
		store.RunForkSelectedContractReadinessFactTimers,
		store.RunForkSelectedContractReadinessFactSessions,
		store.RunForkSelectedContractReadinessFactTurns,
		store.RunForkSelectedContractReadinessFactAudits,
		store.RunForkSelectedContractReadinessFactCommittedReplayScopeMarkers,
		store.RunForkSelectedContractReadinessFactPlatformRuntimeDiagnostics,
		store.RunForkSelectedContractReadinessFactReceipts,
		store.RunForkSelectedContractReadinessFactDeadLetters,
		store.RunForkSelectedContractReadinessFactRetryIdempotency,
		store.RunForkSelectedContractReadinessFactEmittedFollowUps,
		store.RunForkSelectedContractReadinessFactSourcePostTFacts,
		store.RunForkSelectedContractReadinessFactCurrentStateSnapshots,
		store.RunForkSelectedContractReadinessFactNonAgentNodeSystemWork,
		store.RunForkSelectedContractReadinessFactRestartRecovery,
		store.RunForkSelectedContractReadinessFactOperatorConsumers,
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
