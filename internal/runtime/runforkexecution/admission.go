package runforkexecution

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/google/uuid"

	"swarm/internal/runtime/semanticview"
	"swarm/internal/store"
)

type SelectedContractBindingReader interface {
	RequireRunForkSelectedContractBinding(context.Context, string) (store.RunForkSelectedContractBinding, error)
}

type SelectedContractExecutionAdmissionRequest struct {
	ForkRunID         string
	BindingReader     SelectedContractBindingReader
	SelectedSource    semanticview.Source
	FrontierAdmission store.RunForkContractFrontierAdmission
	ExecutionModel    store.RunForkSelectedContractExecution
}

func BuildSelectedContractExecutionAdmission(ctx context.Context, req SelectedContractExecutionAdmissionRequest) (store.RunForkSelectedContractExecutionAdmission, error) {
	forkRunID := strings.TrimSpace(req.ForkRunID)
	if forkRunID == "" {
		return store.RunForkSelectedContractExecutionAdmission{}, fmt.Errorf("selected-contract execution admission requires fork run_id")
	}
	if _, err := uuid.Parse(forkRunID); err != nil {
		return store.RunForkSelectedContractExecutionAdmission{}, fmt.Errorf("selected-contract execution admission fork run_id must be a UUID: %w", err)
	}
	if req.BindingReader == nil {
		return store.RunForkSelectedContractExecutionAdmission{}, fmt.Errorf("selected-contract execution admission requires %s reader", store.RunForkSelectedContractBindingOwner)
	}
	binding, err := req.BindingReader.RequireRunForkSelectedContractBinding(ctx, forkRunID)
	if err != nil {
		return store.RunForkSelectedContractExecutionAdmission{}, fmt.Errorf("load selected-contract binding for execution admission: %w", err)
	}
	if err := validateSelectedContractExecutionBinding(forkRunID, binding); err != nil {
		return store.RunForkSelectedContractExecutionAdmission{}, err
	}
	if err := validateSelectedContractExecutionSource(binding, req.SelectedSource); err != nil {
		return store.RunForkSelectedContractExecutionAdmission{}, err
	}
	if err := validateSelectedContractExecutionFrontier(binding, req.FrontierAdmission); err != nil {
		return store.RunForkSelectedContractExecutionAdmission{}, err
	}
	if err := validateSelectedContractExecutionModel(binding, req.FrontierAdmission, req.ExecutionModel); err != nil {
		return store.RunForkSelectedContractExecutionAdmission{}, err
	}

	return store.RunForkSelectedContractExecutionAdmission{
		Owner:                 store.RunForkSelectedContractExecutionAdmissionOwner,
		FutureExecutionOwner:  store.RunForkSelectedContractExecutionOwner,
		NonMutating:           true,
		ExecutionSupported:    false,
		ForkRunID:             binding.ForkRunID,
		SourceRunID:           binding.SourceRunID,
		ForkEventID:           binding.ForkEventID,
		ContractSelection:     binding.ContractSelection,
		ContractBindingOwner:  binding.Owner,
		AdmissionOwner:        req.FrontierAdmission.Owner,
		AdmissionUse:          store.RunForkSelectedContractExecutionAdmissionUseDurableBinding,
		ExecutionModelOwner:   req.ExecutionModel.Owner,
		SourceWorkflowName:    strings.TrimSpace(req.SelectedSource.WorkflowName()),
		SourceWorkflowVersion: strings.TrimSpace(req.SelectedSource.WorkflowVersion()),
		FrontierEventCount:    req.ExecutionModel.FrontierEventCount,
		FrontierEvents:        append([]store.RunForkSelectedContractFrontierEvent(nil), req.ExecutionModel.FrontierEvents...),
		ContractBinding: store.RunForkSelectedContractExecutionBoundary{
			Concept:     "selected_contract_binding",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       store.RunForkSelectedContractBindingOwner,
			Reason:      "execution admission consumes the durable selected contract source bound to the fork run before any mutation",
		},
		RequiredConsumers: append([]store.RunForkSelectedContractExecutionBoundary(nil), req.ExecutionModel.RequiredConsumers...),
		BlockedSiblings:   append([]store.RunForkSelectedContractExecutionBoundary(nil), req.ExecutionModel.BlockedSiblings...),
		InvalidPaths:      append([]store.RunForkSelectedContractExecutionBoundary(nil), req.ExecutionModel.InvalidPaths...),
		UnsupportedBlockers: []store.RunForkUnsupportedBlocker{{
			Code:    store.RunForkBlockerSelectedContractExecutionAdmissionNonMutating,
			Message: "selected-contract execution admission is non-mutating; handler execution and fork-local writes remain separately gated",
		}},
	}, nil
}

func validateSelectedContractExecutionBinding(forkRunID string, binding store.RunForkSelectedContractBinding) error {
	if strings.TrimSpace(binding.Owner) != store.RunForkSelectedContractBindingOwner {
		return fmt.Errorf("selected-contract execution admission requires %s binding; got %q", store.RunForkSelectedContractBindingOwner, binding.Owner)
	}
	if strings.TrimSpace(binding.ForkRunID) != forkRunID {
		return fmt.Errorf("selected-contract execution admission binding fork run_id mismatch: got %q want %q", binding.ForkRunID, forkRunID)
	}
	for label, value := range map[string]string{
		"source run_id": binding.SourceRunID,
		"fork event_id": binding.ForkEventID,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("selected-contract execution admission binding missing %s", label)
		}
		if _, err := uuid.Parse(strings.TrimSpace(value)); err != nil {
			return fmt.Errorf("selected-contract execution admission binding %s must be a UUID: %w", label, err)
		}
	}
	return validateSelectedContractSelection("binding", binding.ContractSelection)
}

func validateSelectedContractExecutionSource(binding store.RunForkSelectedContractBinding, source semanticview.Source) error {
	if source == nil {
		return fmt.Errorf("selected-contract execution admission requires selected semantic source from durable binding")
	}
	selection := binding.ContractSelection
	sourceName := strings.TrimSpace(source.WorkflowName())
	sourceVersion := strings.TrimSpace(source.WorkflowVersion())
	if sourceName == "" || sourceVersion == "" {
		return fmt.Errorf("selected-contract execution admission selected source must expose workflow name and version")
	}
	if strings.TrimSpace(selection.WorkflowName) != sourceName {
		return fmt.Errorf("selected-contract execution admission workflow name mismatch: binding %q source %q", selection.WorkflowName, sourceName)
	}
	if strings.TrimSpace(selection.WorkflowVersion) != sourceVersion {
		return fmt.Errorf("selected-contract execution admission workflow version mismatch: binding %q source %q", selection.WorkflowVersion, sourceVersion)
	}
	return nil
}

func validateSelectedContractExecutionFrontier(binding store.RunForkSelectedContractBinding, admission store.RunForkContractFrontierAdmission) error {
	if strings.TrimSpace(admission.Owner) != store.RunForkContractFrontierAdmissionOwner {
		return fmt.Errorf("selected-contract execution admission requires %s frontier admission; got %q", store.RunForkContractFrontierAdmissionOwner, admission.Owner)
	}
	if !admission.NonMutating {
		return fmt.Errorf("selected-contract execution admission requires non-mutating frontier admission")
	}
	if admission.HistoricalExecutionSupported {
		return fmt.Errorf("selected-contract execution admission frontier unexpectedly supports historical execution")
	}
	return validateSelectionMatches("frontier admission", binding.ContractSelection, admission.ContractSelection)
}

func validateSelectedContractExecutionModel(binding store.RunForkSelectedContractBinding, frontier store.RunForkContractFrontierAdmission, model store.RunForkSelectedContractExecution) error {
	if strings.TrimSpace(model.Owner) != store.RunForkSelectedContractExecutionModelOwner {
		return fmt.Errorf("selected-contract execution admission requires %s model; got %q", store.RunForkSelectedContractExecutionModelOwner, model.Owner)
	}
	if strings.TrimSpace(model.FutureExecutionOwner) != store.RunForkSelectedContractExecutionOwner {
		return fmt.Errorf("selected-contract execution admission model must point to %s; got %q", store.RunForkSelectedContractExecutionOwner, model.FutureExecutionOwner)
	}
	if !model.NonMutating || model.ExecutionSupported {
		return fmt.Errorf("selected-contract execution admission requires non-mutating unsupported execution model")
	}
	if strings.TrimSpace(model.AdmissionOwner) != frontier.Owner {
		return fmt.Errorf("selected-contract execution admission model admission owner mismatch: got %q want %q", model.AdmissionOwner, frontier.Owner)
	}
	if model.FrontierEventCount != frontier.FrontierEventCount {
		return fmt.Errorf("selected-contract execution admission model frontier count mismatch: got %d want %d", model.FrontierEventCount, frontier.FrontierEventCount)
	}
	if !reflect.DeepEqual(model.FrontierEvents, selectedContractFrontierEvents(frontier.FrontierEvents)) {
		return fmt.Errorf("selected-contract execution admission model frontier events do not match durable frontier evidence")
	}
	if model.ContractBinding.Owner != store.RunForkSelectedContractBindingOwner ||
		model.ContractBinding.Disposition != store.RunForkSelectedContractDispositionPrerequisite {
		return fmt.Errorf("selected-contract execution admission model must consume %s as prerequisite", store.RunForkSelectedContractBindingOwner)
	}
	return validateSelectionMatches("execution model", binding.ContractSelection, model.ContractSelection)
}

func validateSelectionMatches(label string, want, got store.RunForkContractSelection) error {
	if err := validateSelectedContractSelection("binding", want); err != nil {
		return err
	}
	if err := validateSelectedContractSelection(label, got); err != nil {
		return err
	}
	if strings.TrimSpace(want.Mode) != strings.TrimSpace(got.Mode) ||
		strings.TrimSpace(want.ContractsRoot) != strings.TrimSpace(got.ContractsRoot) ||
		strings.TrimSpace(want.WorkflowName) != strings.TrimSpace(got.WorkflowName) ||
		strings.TrimSpace(want.WorkflowVersion) != strings.TrimSpace(got.WorkflowVersion) {
		return fmt.Errorf("selected-contract execution admission %s selection does not match durable binding", label)
	}
	return nil
}

func validateSelectedContractSelection(label string, selection store.RunForkContractSelection) error {
	if strings.TrimSpace(selection.Mode) != "selected_contracts" {
		return fmt.Errorf("selected-contract execution admission %s requires mode selected_contracts; got %q", label, selection.Mode)
	}
	if strings.TrimSpace(selection.ContractsRoot) == "" {
		return fmt.Errorf("selected-contract execution admission %s requires contracts_root", label)
	}
	if strings.TrimSpace(selection.WorkflowName) == "" {
		return fmt.Errorf("selected-contract execution admission %s requires workflow_name", label)
	}
	if strings.TrimSpace(selection.WorkflowVersion) == "" {
		return fmt.Errorf("selected-contract execution admission %s requires workflow_version", label)
	}
	return nil
}
