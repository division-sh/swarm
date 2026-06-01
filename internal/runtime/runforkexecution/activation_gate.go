package runforkexecution

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/store"
)

type SelectedContractActivationStore interface {
	LoadRunForkSelectedContractBinding(context.Context, string) (store.RunForkSelectedContractBinding, bool, error)
	RequireRunForkSelectedContractBinding(context.Context, string) (store.RunForkSelectedContractBinding, error)
	LoadRunForkSelectedContractRouteRecovery(context.Context, string) (store.RunForkSelectedContractRouteRecovery, bool, error)
	PlanRunFork(context.Context, store.RunForkPlanRequest) (store.RunForkPlan, error)
	ActivateRunFork(context.Context, store.RunForkActivateRequest) (store.RunForkActivation, error)
}

type SelectedContractActivationGateRequest struct {
	ForkRunID    string
	Store        SelectedContractActivationStore
	SourceLoader SelectedContractSourceLoader
}

type SelectedContractActivationGateResult struct {
	store.RunForkActivation
	Owner                              string                                               `json:"selected_contract_activation_gate_owner,omitempty"`
	SelectedContractExecutionAdmission *store.RunForkSelectedContractExecutionAdmission     `json:"selected_contract_execution_admission,omitempty"`
	ContractSwapBootResumeAdmission    *store.RunForkContractSwapBootResumeAdmission        `json:"contract_swap_boot_resume_admission,omitempty"`
	HistoricalReplayExecutionAdmission *store.RunForkHistoricalReplayExecutionAdmission     `json:"historical_replay_execution_admission,omitempty"`
	ContractSwapBootResumeExecution    *store.RunForkHistoricalReplayContractSwapBootResume `json:"contract_swap_boot_resume_execution,omitempty"`
	ForkLocalRuntimeContainer          *SelectedContractForkLocalRuntimeContainer           `json:"fork_local_runtime_container,omitempty"`
	ExecutedEventCount                 int                                                  `json:"executed_event_count,omitempty"`
	ForkEvents                         []SelectedContractExecutionForkEvent                 `json:"fork_events,omitempty"`
}

func ActivateSelectedContractRunFork(ctx context.Context, req SelectedContractActivationGateRequest) (SelectedContractActivationGateResult, error) {
	forkRunID := strings.TrimSpace(req.ForkRunID)
	if forkRunID == "" {
		return SelectedContractActivationGateResult{}, fmt.Errorf("selected-contract activation gate requires fork run_id")
	}
	if _, err := uuid.Parse(forkRunID); err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("selected-contract activation gate fork run_id must be a UUID: %w", err)
	}
	if req.Store == nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("selected-contract activation gate requires store")
	}

	binding, ok, err := req.Store.LoadRunForkSelectedContractBinding(ctx, forkRunID)
	if err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("load selected-contract binding for activation gate: %w", err)
	}
	if !ok {
		activation, err := req.Store.ActivateRunFork(ctx, store.RunForkActivateRequest{
			ForkRunID:                         forkRunID,
			HistoricalReplayExecutionAdmitter: HistoricalReplayExecutionAdmitter{},
		})
		return SelectedContractActivationGateResult{RunForkActivation: activation}, err
	}
	if req.SourceLoader == nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("selected-contract activation gate requires selected source loader")
	}

	loadedSource, err := loadRunForkSelectedContractSource(ctx, req.SourceLoader, SelectedContractSourceLoadRequest{
		SourceRunID: binding.SourceRunID,
		BundleHash:  binding.ContractSelection.BundleHash,
		Selection:   binding.ContractSelection,
	})
	if err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("load selected semantic source for activation gate: %w", err)
	}
	defer cleanupLoadedSelectedContractSource(loadedSource)
	plan, err := req.Store.PlanRunFork(ctx, store.RunForkPlanRequest{SourceRunID: binding.SourceRunID, At: binding.ForkEventID})
	if err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("plan selected-contract activation gate: %w", err)
	}
	replayAdmission := store.RunForkSelectedContractReplayResumeAdmission(plan)
	if len(plan.ReplayResumeAdmission.Dispositions) > 0 || len(plan.ReplayResumeAdmission.UnsupportedBlockers) > 0 {
		plan.UnsupportedBlockers = replayAdmission.UnsupportedBlockers
		plan.UnsupportedBlockerCount = len(replayAdmission.UnsupportedBlockers)
	}
	plan.ReplayResumeAdmission = replayAdmission
	plan.ExecutionReady = replayAdmission.StateOnlyExecutionReady || replayAdmission.DeliveryEventReplayReady
	frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{
		Plan:              plan,
		Source:            loadedSource.Source,
		ContractSelection: binding.ContractSelection,
	})
	if err != nil {
		return SelectedContractActivationGateResult{}, err
	}
	routeAdmission, err := runforkadmission.AdmitSelectedContractRouteHistory(runforkadmission.SelectedContractRouteHistoryRequest{
		Plan:              plan,
		Source:            loadedSource.Source,
		ContractSelection: binding.ContractSelection,
		FrontierAdmission: frontier,
	})
	if err != nil {
		return SelectedContractActivationGateResult{}, err
	}
	routeTopology, err := BuildSelectedContractRouteTopology(SelectedContractRouteTopologyRequest{
		Admission:      frontier,
		RouteAdmission: routeAdmission,
	})
	if err != nil {
		return SelectedContractActivationGateResult{}, err
	}
	model, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{
		Admission:      frontier,
		RouteAdmission: routeAdmission,
		RouteTopology:  routeTopology,
	})
	if err != nil {
		return SelectedContractActivationGateResult{}, err
	}
	admission, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:         forkRunID,
		SourceRunID:       binding.SourceRunID,
		BundleHash:        binding.ContractSelection.BundleHash,
		BindingReader:     req.Store,
		SourceLoader:      req.SourceLoader,
		FrontierAdmission: frontier,
		RouteAdmission:    routeAdmission,
		RouteTopology:     routeTopology,
		ExecutionModel:    model,
	})
	if err != nil {
		return SelectedContractActivationGateResult{}, err
	}
	var routeRecovery *store.RunForkSelectedContractRouteRecovery
	recoveredRoute, ok, err := req.Store.LoadRunForkSelectedContractRouteRecovery(ctx, forkRunID)
	if err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("load selected-contract route recovery for contract-swap admission: %w", err)
	}
	if ok {
		routeRecovery = &recoveredRoute
	}
	pgStore, _ := req.Store.(*store.PostgresStore)
	contractSwapAdmission, err := BuildContractSwapBootResumeAdmission(ContractSwapBootResumeAdmissionRequest{
		SelectedExecutionAdmission: admission,
		ReplayResumeAdmission:      plan.ReplayResumeAdmission,
		RouteRecovery:              routeRecovery,
	})
	if err != nil {
		return SelectedContractActivationGateResult{}, err
	}
	historicalReplayAdmission, err := BuildHistoricalReplayExecutionAdmission(HistoricalReplayExecutionAdmissionRequest{
		ReplayResumeAdmission:      plan.ReplayResumeAdmission,
		SelectedExecutionAdmission: admission,
		ContractSwapAdmission:      contractSwapAdmission,
		RouteRecovery:              routeRecovery,
	})
	if err != nil {
		return SelectedContractActivationGateResult{}, err
	}
	result := SelectedContractActivationGateResult{
		Owner:                              store.RunForkSelectedContractExecutionActivationGateOwner,
		SelectedContractExecutionAdmission: &admission,
		ContractSwapBootResumeAdmission:    &contractSwapAdmission,
		HistoricalReplayExecutionAdmission: &historicalReplayAdmission,
	}
	if !plan.ExecutionReady {
		return result, fmt.Errorf("selected-contract activation gate requires execution-ready plan before mutation; blockers: %s", selectedContractBlockerCodes(plan.UnsupportedBlockers))
	}
	if plan.ReplayResumeAdmission.DeliveryEventReplayReady {
		if pgStore == nil {
			return result, fmt.Errorf("%s requires postgres store", store.RunForkHistoricalReplayContractSwapBootResumeOwner)
		}
		if routeRecovery == nil {
			record, err := pgStore.RecordRunForkSelectedContractRouteRecovery(ctx, store.RunForkSelectedContractRouteRecoveryRequest{
				ForkRunID:         forkRunID,
				SourceRunID:       binding.SourceRunID,
				ForkEventID:       binding.ForkEventID,
				ContractSelection: binding.ContractSelection,
				RouteTopology:     routeTopology,
				RecipientPlanning: *model.RecipientPlanning,
			})
			if err != nil {
				return result, fmt.Errorf("record selected-contract route recovery for contract-swap execution: %w", err)
			}
			routeRecovery = &record
			contractSwapAdmission, err = BuildContractSwapBootResumeAdmission(ContractSwapBootResumeAdmissionRequest{
				SelectedExecutionAdmission: admission,
				ReplayResumeAdmission:      plan.ReplayResumeAdmission,
				RouteRecovery:              routeRecovery,
			})
			if err != nil {
				return result, err
			}
			historicalReplayAdmission, err = BuildHistoricalReplayExecutionAdmission(HistoricalReplayExecutionAdmissionRequest{
				ReplayResumeAdmission:      plan.ReplayResumeAdmission,
				SelectedExecutionAdmission: admission,
				ContractSwapAdmission:      contractSwapAdmission,
				RouteRecovery:              routeRecovery,
			})
			if err != nil {
				return result, err
			}
			result.ContractSwapBootResumeAdmission = &contractSwapAdmission
			result.HistoricalReplayExecutionAdmission = &historicalReplayAdmission
		}
		historicalReplayExecution, err := BuildHistoricalReplayExecution(HistoricalReplayExecutionRequest{
			Admission:             historicalReplayAdmission,
			ReplayResumeAdmission: plan.ReplayResumeAdmission,
			PendingWork:           plan.PendingWork,
		})
		if err != nil {
			return result, err
		}
		contractSwapExecution, err := BuildHistoricalReplayContractSwapBootResumeExecution(HistoricalReplayContractSwapBootResumeRequest{
			SelectedExecutionAdmission: admission,
			ContractSwapAdmission:      contractSwapAdmission,
			HistoricalReplayAdmission:  historicalReplayAdmission,
			HistoricalReplayExecution:  historicalReplayExecution,
			RouteRecovery:              routeRecovery,
		})
		if err != nil {
			return result, err
		}
		result.ContractSwapBootResumeExecution = &contractSwapExecution
		sourceEventIDs := contractSwapBootResumeSourceEvents(contractSwapExecution)
		container, err := buildSelectedContractForkLocalRuntimeContainer(ctx, publishSelectedContractForkEventsRequest{
			Store:             pgStore,
			LoadedSource:      loadedSource,
			RecipientPlanning: *model.RecipientPlanning,
			SourceRunID:       binding.SourceRunID,
			ForkRunID:         forkRunID,
			ForkEventID:       plan.ForkPoint.EventID,
			ForkTime:          plan.ForkPoint.Timestamp,
			SourceEvents:      sourceEventIDs,
			ExecutionOwner:    store.RunForkHistoricalReplayContractSwapBootResumeOwner,
		})
		if err != nil {
			return result, cleanupSelectedContractExecutionFailure(ctx, pgStore, forkRunID, err)
		}
		containerProof := container.Proof()
		result.ForkLocalRuntimeContainer = &containerProof
		published, err := container.Publish(ctx)
		result.ExecutedEventCount = len(published)
		result.ForkEvents = published
		if err != nil {
			return result, cleanupSelectedContractExecutionFailure(ctx, pgStore, forkRunID, err)
		}
		activation, err := pgStore.ActivateRunForkForSelectedContractExecution(ctx, store.RunForkSelectedContractExecutionActivateRequest{
			ForkRunID:             forkRunID,
			AllowedSourceEventIDs: sourceEventIDs,
		})
		result.RunForkActivation = activation
		if err != nil {
			return result, cleanupSelectedContractExecutionFailure(ctx, pgStore, forkRunID, err)
		}
		return result, nil
	}
	if plan.ReplayResumeAdmission.HistoricalReplayRequired {
		return result, fmt.Errorf("selected-contract activation gate blocks historical replay before mutation; blockers: %s", selectedContractBlockerCodes(plan.UnsupportedBlockers))
	}
	if frontier.FrontierEventCount > 0 {
		return result, fmt.Errorf("%s: selected-contract frontier execution remains non-mutating", store.RunForkBlockerContractFrontierExecutionUnsupported)
	}

	activation, err := req.Store.ActivateRunFork(ctx, store.RunForkActivateRequest{
		ForkRunID:                         forkRunID,
		HistoricalReplayExecutionAdmitter: HistoricalReplayExecutionAdmitter{},
	})
	result.RunForkActivation = activation
	return result, err
}

func selectedContractBlockerCodes(blockers []store.RunForkUnsupportedBlocker) string {
	if len(blockers) == 0 {
		return "none"
	}
	codes := make([]string, 0, len(blockers))
	for _, blocker := range blockers {
		if code := strings.TrimSpace(blocker.Code); code != "" {
			codes = append(codes, code)
		}
	}
	if len(codes) == 0 {
		return "none"
	}
	return strings.Join(codes, ",")
}
