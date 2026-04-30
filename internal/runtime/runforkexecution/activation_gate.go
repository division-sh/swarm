package runforkexecution

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"swarm/internal/runtime/runforkadmission"
	"swarm/internal/store"
)

type SelectedContractActivationStore interface {
	LoadRunForkSelectedContractBinding(context.Context, string) (store.RunForkSelectedContractBinding, bool, error)
	RequireRunForkSelectedContractBinding(context.Context, string) (store.RunForkSelectedContractBinding, error)
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
	Owner                              string                                           `json:"selected_contract_activation_gate_owner,omitempty"`
	SelectedContractExecutionAdmission *store.RunForkSelectedContractExecutionAdmission `json:"selected_contract_execution_admission,omitempty"`
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
		activation, err := req.Store.ActivateRunFork(ctx, store.RunForkActivateRequest{ForkRunID: forkRunID})
		return SelectedContractActivationGateResult{RunForkActivation: activation}, err
	}
	if req.SourceLoader == nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("selected-contract activation gate requires selected source loader")
	}

	loadedSource, err := req.SourceLoader.LoadRunForkSelectedContractSource(ctx, binding.ContractSelection)
	if err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("load selected semantic source for activation gate: %w", err)
	}
	plan, err := req.Store.PlanRunFork(ctx, store.RunForkPlanRequest{SourceRunID: binding.SourceRunID, At: binding.ForkEventID})
	if err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("plan selected-contract activation gate: %w", err)
	}
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
		Admission:     frontier,
		RouteTopology: routeTopology,
	})
	if err != nil {
		return SelectedContractActivationGateResult{}, err
	}
	admission, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:         forkRunID,
		BindingReader:     req.Store,
		SourceLoader:      req.SourceLoader,
		FrontierAdmission: frontier,
		RouteTopology:     routeTopology,
		ExecutionModel:    model,
	})
	if err != nil {
		return SelectedContractActivationGateResult{}, err
	}
	result := SelectedContractActivationGateResult{
		Owner:                              store.RunForkSelectedContractExecutionActivationGateOwner,
		SelectedContractExecutionAdmission: &admission,
	}
	if !plan.ExecutionReady {
		return result, fmt.Errorf("selected-contract activation gate requires execution-ready plan before mutation; blockers: %s", selectedContractBlockerCodes(plan.UnsupportedBlockers))
	}
	if plan.ReplayResumeAdmission.DeliveryEventReplayReady {
		return result, fmt.Errorf("%s: selected-bound activation cannot consume source delivery/event replay as selected-contract executable work", store.RunForkBlockerSelectedContractSourceReplayUnsupported)
	}
	if plan.ReplayResumeAdmission.HistoricalReplayRequired {
		return result, fmt.Errorf("selected-contract activation gate blocks historical replay before mutation; blockers: %s", selectedContractBlockerCodes(plan.UnsupportedBlockers))
	}
	if frontier.FrontierEventCount > 0 {
		return result, fmt.Errorf("%s: selected-contract frontier execution remains non-mutating", store.RunForkBlockerContractFrontierExecutionUnsupported)
	}

	activation, err := req.Store.ActivateRunFork(ctx, store.RunForkActivateRequest{ForkRunID: forkRunID})
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
