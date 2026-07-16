package runforkexecution

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/google/uuid"

	"github.com/division-sh/swarm/internal/providerconnectors"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/runbundle"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
)

type SelectedContractBindingReader interface {
	RequireRunForkSelectedContractBinding(context.Context, string) (store.RunForkSelectedContractBinding, error)
}

type SelectedContractSourceLoader interface {
	LoadRunForkSelectedContractSource(context.Context, store.RunForkContractSelection) (LoadedSelectedContractSource, error)
}

type SelectedContractSourceRequestLoader interface {
	LoadRunForkSelectedContractSourceForRequest(context.Context, SelectedContractSourceLoadRequest) (LoadedSelectedContractSource, error)
}

type SelectedContractSourceLoadRequest struct {
	SourceRunID          string
	BundleHash           string
	ExpectedBundleHash   string
	ExpectedBundleSource string
	Selection            store.RunForkContractSelection
}

type LoadedSelectedContractSource struct {
	Selection              store.RunForkContractSelection
	Source                 semanticview.Source
	Module                 runtimepipeline.WorkflowModule
	BundleHash             string
	BundleSource           string
	MockConnectorResponses *providerconnectors.MockResponsePlan
	Cleanup                func() error
}

type selectedContractWorkflowModule struct {
	source         semanticview.Source
	workflow       *runtimepipeline.WorkflowDefinition
	nodes          []runtimepipeline.WorkflowNode
	guardRegistry  runtimepipeline.GuardRegistry
	actionRegistry runtimepipeline.ActionRegistry
}

func (m selectedContractWorkflowModule) SemanticSource() semanticview.Source {
	return m.source
}

func (m selectedContractWorkflowModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return m.workflow
}

func (m selectedContractWorkflowModule) WorkflowNodes() []runtimepipeline.WorkflowNode {
	return append([]runtimepipeline.WorkflowNode(nil), m.nodes...)
}

func (m selectedContractWorkflowModule) GuardRegistry() runtimepipeline.GuardRegistry {
	return m.guardRegistry
}

func (m selectedContractWorkflowModule) ActionRegistry() runtimepipeline.ActionRegistry {
	return m.actionRegistry
}

type ContractBundleSourceLoader struct {
	RepoRoot         string
	PlatformSpecPath string
}

type BundleCatalogSelectedContractSourceStore interface {
	LoadRunBundleAvailability(context.Context, string) (runbundle.Availability, error)
	LoadBundleCatalogRuntimeRecord(context.Context, string) (store.BundleCatalogRuntimeRecord, error)
}

type BundleCatalogSelectedContractSourceLoader struct {
	RepoRoot         string
	PlatformSpecPath string
	Store            BundleCatalogSelectedContractSourceStore
}

func (l ContractBundleSourceLoader) LoadRunForkSelectedContractSource(ctx context.Context, selection store.RunForkContractSelection) (LoadedSelectedContractSource, error) {
	if err := ctx.Err(); err != nil {
		return LoadedSelectedContractSource{}, err
	}
	if err := validateSelectedSourceLoaderSelection(selection); err != nil {
		return LoadedSelectedContractSource{}, err
	}
	if strings.TrimSpace(selection.Mode) == store.RunForkContractSelectionModeBundleHash {
		return LoadedSelectedContractSource{}, fmt.Errorf("%s: disk selected-contract source loader cannot load bundle_hash mode %s", runbundle.CodeBundleUnavailable, strings.TrimSpace(selection.BundleHash))
	}
	repoRoot := strings.TrimSpace(l.RepoRoot)
	if repoRoot == "" {
		return LoadedSelectedContractSource{}, fmt.Errorf("selected-contract execution admission source loader requires repo root")
	}
	platformSpecPath := strings.TrimSpace(l.PlatformSpecPath)
	if platformSpecPath == "" {
		platformSpecPath = runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, strings.TrimSpace(selection.ContractsRoot), platformSpecPath)
	if err != nil {
		return LoadedSelectedContractSource{}, err
	}
	if err := runtimecontracts.ValidateBundlePlatformVersionCompatibility(bundle); err != nil {
		return LoadedSelectedContractSource{}, fmt.Errorf("selected-contract source admission failed: %w", err)
	}
	bundleHash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		return LoadedSelectedContractSource{}, fmt.Errorf("hash selected-contract source: %w", err)
	}
	source, mockConnectorResponses, err := compileSelectedContractSource(semanticview.Wrap(bundle))
	if err != nil {
		return LoadedSelectedContractSource{}, err
	}
	if strings.TrimSpace(selection.WorkflowName) == "" {
		selection.WorkflowName = strings.TrimSpace(source.WorkflowName())
	}
	if strings.TrimSpace(selection.WorkflowVersion) == "" {
		selection.WorkflowVersion = strings.TrimSpace(source.WorkflowVersion())
	}
	if err := validateSelectedContractSelection("selected source loader", selection); err != nil {
		return LoadedSelectedContractSource{}, err
	}
	workflow, err := runtimepipeline.LoadWorkflowDefinition(source)
	if err != nil {
		return LoadedSelectedContractSource{}, err
	}
	nodes, err := runtimepipeline.LoadWorkflowNodes(source)
	if err != nil {
		return LoadedSelectedContractSource{}, err
	}
	return LoadedSelectedContractSource{
		Selection:              selection,
		Source:                 source,
		BundleHash:             bundleHash,
		BundleSource:           storerunlifecycle.BundleSourceEphemeral,
		MockConnectorResponses: mockConnectorResponses,
		Module: selectedContractWorkflowModule{
			source:         source,
			workflow:       workflow,
			nodes:          nodes,
			guardRegistry:  runtimepipeline.NewContractGuardRegistry(source),
			actionRegistry: runtimepipeline.NewContractActionRegistry(source),
		},
	}, nil
}

func (l BundleCatalogSelectedContractSourceLoader) LoadRunForkSelectedContractSource(ctx context.Context, selection store.RunForkContractSelection) (LoadedSelectedContractSource, error) {
	return l.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{Selection: selection})
}

func (l BundleCatalogSelectedContractSourceLoader) LoadRunForkSelectedContractSourceForRequest(ctx context.Context, req SelectedContractSourceLoadRequest) (LoadedSelectedContractSource, error) {
	if err := ctx.Err(); err != nil {
		return LoadedSelectedContractSource{}, err
	}
	selection := req.Selection
	if err := validateSelectedSourceLoaderSelection(selection); err != nil {
		return LoadedSelectedContractSource{}, err
	}
	if l.Store == nil {
		return LoadedSelectedContractSource{}, fmt.Errorf("DB-loaded selected-contract source loader requires bundle catalog store")
	}
	sourceRunID := strings.TrimSpace(req.SourceRunID)
	requestedHash := strings.TrimSpace(req.BundleHash)
	bundleHash := strings.TrimSpace(selection.BundleHash)
	if selection.Mode == store.RunForkContractSelectionModeSelectedContracts {
		if sourceRunID == "" {
			return LoadedSelectedContractSource{}, fmt.Errorf("DB-loaded selected-contract source loader requires source run_id")
		}
		availability, err := l.Store.LoadRunBundleAvailability(ctx, sourceRunID)
		if err != nil {
			return LoadedSelectedContractSource{}, err
		}
		if availability.DataIntegrityError() {
			return LoadedSelectedContractSource{}, fmt.Errorf("%s: %s", runbundle.CodeBundleDataIntegrityError, availability.DetailString())
		}
		if !availability.Available() {
			return LoadedSelectedContractSource{}, fmt.Errorf("%s: %s", runbundle.CodeBundleUnavailable, availability.DetailString())
		}
		bundleHash = requestedHash
		if bundleHash == "" {
			bundleHash = strings.TrimSpace(availability.BundleHash)
		}
		if bundleHash == "" {
			return LoadedSelectedContractSource{}, fmt.Errorf("%s: source run %s has no canonical bundle_hash", runbundle.CodeBundleDataIntegrityError, sourceRunID)
		}
		if bundleHash != strings.TrimSpace(availability.BundleHash) {
			return LoadedSelectedContractSource{}, fmt.Errorf("%s: selected_contracts source hash mismatch: request %s source %s", runbundle.CodeBundleDataIntegrityError, bundleHash, availability.BundleHash)
		}
	} else {
		if requestedHash != "" && requestedHash != bundleHash {
			return LoadedSelectedContractSource{}, fmt.Errorf("%s: target bundle_hash selection %s does not match request %s", runbundle.CodeBundleDataIntegrityError, bundleHash, requestedHash)
		}
	}
	record, err := l.Store.LoadBundleCatalogRuntimeRecord(ctx, bundleHash)
	if errors.Is(err, store.ErrBundleNotFound) {
		if selection.Mode == store.RunForkContractSelectionModeBundleHash {
			return LoadedSelectedContractSource{}, fmt.Errorf("%s: target bundle %s is not available", runbundle.CodeBundleUnavailable, bundleHash)
		}
		return LoadedSelectedContractSource{}, fmt.Errorf("%s: source run %s bundle row missing for %s", runbundle.CodeBundleDataIntegrityError, sourceRunID, bundleHash)
	}
	if err != nil {
		return LoadedSelectedContractSource{}, err
	}
	runtimeSource, err := runtimecontracts.LoadBundleCatalogRuntimeSource(strings.TrimSpace(l.RepoRoot), runtimecontracts.BundleCatalogRuntimeLoadRequest{
		BundleHash:              bundleHash,
		ContentYAML:             record.ContentYAML,
		DataBlob:                record.DataBlob,
		RunningPlatformSpecPath: strings.TrimSpace(l.PlatformSpecPath),
	})
	if err != nil {
		return LoadedSelectedContractSource{}, fmt.Errorf("%s: load DB-backed selected-contract source %s: %w", runbundle.CodeBundleDataIntegrityError, bundleHash, err)
	}
	source, mockConnectorResponses, err := compileSelectedContractSource(semanticview.Wrap(runtimeSource.Bundle))
	if err != nil {
		_ = runtimeSource.Cleanup()
		return LoadedSelectedContractSource{}, err
	}
	if strings.TrimSpace(selection.WorkflowName) == "" {
		selection.WorkflowName = strings.TrimSpace(source.WorkflowName())
	}
	if strings.TrimSpace(selection.WorkflowVersion) == "" {
		selection.WorkflowVersion = strings.TrimSpace(source.WorkflowVersion())
	}
	if err := validateSelectedContractSelection("DB-loaded selected source loader", selection); err != nil {
		_ = runtimeSource.Cleanup()
		return LoadedSelectedContractSource{}, err
	}
	workflow, err := runtimepipeline.LoadWorkflowDefinition(source)
	if err != nil {
		_ = runtimeSource.Cleanup()
		return LoadedSelectedContractSource{}, err
	}
	nodes, err := runtimepipeline.LoadWorkflowNodes(source)
	if err != nil {
		_ = runtimeSource.Cleanup()
		return LoadedSelectedContractSource{}, err
	}
	return LoadedSelectedContractSource{
		Selection:              selection,
		Source:                 source,
		BundleHash:             bundleHash,
		BundleSource:           storerunlifecycle.BundleSourcePersisted,
		MockConnectorResponses: mockConnectorResponses,
		Module: selectedContractWorkflowModule{
			source:         source,
			workflow:       workflow,
			nodes:          nodes,
			guardRegistry:  runtimepipeline.NewContractGuardRegistry(source),
			actionRegistry: runtimepipeline.NewContractActionRegistry(source),
		},
		Cleanup: runtimeSource.Cleanup,
	}, nil
}

func compileSelectedContractSource(source semanticview.Source) (semanticview.Source, *providerconnectors.MockResponsePlan, error) {
	effective, err := providerconnectors.SourceWithConnectorPackImports(source)
	if err != nil {
		return nil, nil, fmt.Errorf("selected-contract provider connector pack import failed: %w", err)
	}
	plan, err := providerconnectors.CompileMockResponsePlan(effective)
	if err != nil {
		return nil, nil, fmt.Errorf("selected-contract mock response compilation failed: %w", err)
	}
	return effective, plan, nil
}

func loadRunForkSelectedContractSource(ctx context.Context, loader SelectedContractSourceLoader, req SelectedContractSourceLoadRequest) (LoadedSelectedContractSource, error) {
	var (
		loaded LoadedSelectedContractSource
		err    error
	)
	if requestLoader, ok := loader.(SelectedContractSourceRequestLoader); ok {
		loaded, err = requestLoader.LoadRunForkSelectedContractSourceForRequest(ctx, req)
	} else {
		loaded, err = loader.LoadRunForkSelectedContractSource(ctx, req.Selection)
	}
	if err != nil {
		return LoadedSelectedContractSource{}, err
	}
	expectedHash := strings.TrimSpace(req.ExpectedBundleHash)
	loadedHash := strings.TrimSpace(loaded.BundleHash)
	if expectedHash != "" && expectedHash != loadedHash {
		cleanupLoadedSelectedContractSource(loaded)
		return LoadedSelectedContractSource{}, fmt.Errorf("%s: selected-contract bundle_hash mismatch: expected %s loaded %s", runbundle.CodeBundleDataIntegrityError, expectedHash, loadedHash)
	}
	expectedSource := strings.TrimSpace(req.ExpectedBundleSource)
	loadedBundleSource := strings.TrimSpace(loaded.BundleSource)
	if expectedSource != "" && expectedSource != loadedBundleSource {
		cleanupLoadedSelectedContractSource(loaded)
		return LoadedSelectedContractSource{}, fmt.Errorf("%s: selected-contract bundle_source mismatch: expected %s loaded %s", runbundle.CodeBundleDataIntegrityError, expectedSource, loadedBundleSource)
	}
	return loaded, nil
}

func cleanupLoadedSelectedContractSource(source LoadedSelectedContractSource) {
	if source.Cleanup != nil {
		_ = source.Cleanup()
	}
}

type SelectedContractExecutionAdmissionRequest struct {
	ForkRunID         string
	SourceRunID       string
	BundleHash        string
	BundleSource      string
	BindingReader     SelectedContractBindingReader
	SourceLoader      SelectedContractSourceLoader
	FrontierAdmission store.RunForkContractFrontierAdmission
	RouteAdmission    store.RunForkSelectedContractRouteAdmission
	RouteTopology     store.RunForkSelectedContractRouteTopology
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
	if req.SourceLoader == nil {
		return store.RunForkSelectedContractExecutionAdmission{}, fmt.Errorf("selected-contract execution admission requires selected source loader bound to %s", store.RunForkSelectedContractBindingOwner)
	}
	loadedSource, err := loadRunForkSelectedContractSource(ctx, req.SourceLoader, SelectedContractSourceLoadRequest{
		SourceRunID:          firstNonEmpty(req.SourceRunID, binding.SourceRunID),
		BundleHash:           req.BundleHash,
		ExpectedBundleHash:   req.BundleHash,
		ExpectedBundleSource: req.BundleSource,
		Selection:            binding.ContractSelection,
	})
	if err != nil {
		return store.RunForkSelectedContractExecutionAdmission{}, fmt.Errorf("load selected semantic source for execution admission: %w", err)
	}
	defer cleanupLoadedSelectedContractSource(loadedSource)
	if err := validateSelectedContractExecutionSource(binding, loadedSource); err != nil {
		return store.RunForkSelectedContractExecutionAdmission{}, err
	}
	if err := validateSelectedContractExecutionFrontier(binding, req.FrontierAdmission); err != nil {
		return store.RunForkSelectedContractExecutionAdmission{}, err
	}
	if err := validateSelectedContractExecutionRouteTopology(binding, req.FrontierAdmission, req.RouteAdmission, req.RouteTopology); err != nil {
		return store.RunForkSelectedContractExecutionAdmission{}, err
	}
	if err := validateSelectedContractExecutionModel(binding, req.FrontierAdmission, req.RouteAdmission, req.RouteTopology, req.ExecutionModel); err != nil {
		return store.RunForkSelectedContractExecutionAdmission{}, err
	}
	recipientPlanning := req.ExecutionModel.RecipientPlanning
	if recipientPlanning == nil {
		return store.RunForkSelectedContractExecutionAdmission{}, fmt.Errorf("selected-contract execution admission model must carry %s", store.RunForkSelectedContractRecipientPlanningOwner)
	}
	if err := validateSelectedContractRecipientPlanning(req.FrontierAdmission, req.RouteAdmission, req.RouteTopology, *recipientPlanning); err != nil {
		return store.RunForkSelectedContractExecutionAdmission{}, err
	}

	unsupportedBlockers := append([]store.RunForkUnsupportedBlocker(nil), req.ExecutionModel.UnsupportedBlockers...)
	unsupportedBlockers = appendRunForkUnsupportedBlocker(unsupportedBlockers, store.RunForkUnsupportedBlocker{
		Code:    store.RunForkBlockerSelectedContractExecutionAdmissionNonMutating,
		Message: "selected-contract execution admission is non-mutating; handler execution and fork-local writes remain separately gated",
	})

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
		SourceWorkflowName:    strings.TrimSpace(loadedSource.Source.WorkflowName()),
		SourceWorkflowVersion: strings.TrimSpace(loadedSource.Source.WorkflowVersion()),
		FrontierEventCount:    req.ExecutionModel.FrontierEventCount,
		FrontierEvents:        append([]store.RunForkSelectedContractFrontierEvent(nil), req.ExecutionModel.FrontierEvents...),
		RouteTopology:         &req.RouteTopology,
		RecipientPlanning:     recipientPlanning,
		ContractBinding: store.RunForkSelectedContractExecutionBoundary{
			Concept:     "selected_contract_binding",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       store.RunForkSelectedContractBindingOwner,
			Reason:      "execution admission consumes the durable selected contract source bound to the fork run before any mutation",
		},
		RequiredConsumers:   append([]store.RunForkSelectedContractExecutionBoundary(nil), req.ExecutionModel.RequiredConsumers...),
		BlockedSiblings:     append([]store.RunForkSelectedContractExecutionBoundary(nil), req.ExecutionModel.BlockedSiblings...),
		InvalidPaths:        append([]store.RunForkSelectedContractExecutionBoundary(nil), req.ExecutionModel.InvalidPaths...),
		UnsupportedBlockers: unsupportedBlockers,
	}, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
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

func validateSelectedContractExecutionSource(binding store.RunForkSelectedContractBinding, loaded LoadedSelectedContractSource) error {
	if err := validateSelectionMatches("selected source", binding.ContractSelection, loaded.Selection); err != nil {
		return err
	}
	source := loaded.Source
	if source == nil {
		return fmt.Errorf("selected-contract execution admission requires selected semantic source from durable binding")
	}
	selection := loaded.Selection
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

func validateSelectedContractExecutionRouteTopology(binding store.RunForkSelectedContractBinding, frontier store.RunForkContractFrontierAdmission, routeAdmission store.RunForkSelectedContractRouteAdmission, routeTopology store.RunForkSelectedContractRouteTopology) error {
	if err := validateSelectedContractRouteAdmission(frontier, routeAdmission); err != nil {
		return err
	}
	if err := validateSelectedContractRouteTopology(frontier, routeAdmission, routeTopology); err != nil {
		return err
	}
	return validateSelectionMatches("route topology", binding.ContractSelection, routeTopology.ContractSelection)
}

func validateSelectedContractExecutionModel(binding store.RunForkSelectedContractBinding, frontier store.RunForkContractFrontierAdmission, routeAdmission store.RunForkSelectedContractRouteAdmission, routeTopology store.RunForkSelectedContractRouteTopology, model store.RunForkSelectedContractExecution) error {
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
	if model.RouteTopology == nil {
		return fmt.Errorf("selected-contract execution admission model must carry %s", store.RunForkSelectedContractRouteTopologyOwner)
	}
	if err := validateSelectedContractRouteTopology(frontier, routeAdmission, *model.RouteTopology); err != nil {
		return err
	}
	if !reflect.DeepEqual(*model.RouteTopology, routeTopology) {
		return fmt.Errorf("selected-contract execution admission model route topology does not match canonical route topology truth")
	}
	if model.RecipientPlanning == nil {
		return fmt.Errorf("selected-contract execution admission model must carry %s", store.RunForkSelectedContractRecipientPlanningOwner)
	}
	if err := validateSelectedContractRecipientPlanning(frontier, routeAdmission, routeTopology, *model.RecipientPlanning); err != nil {
		return err
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
		strings.TrimSpace(want.BundleHash) != strings.TrimSpace(got.BundleHash) ||
		strings.TrimSpace(want.WorkflowName) != strings.TrimSpace(got.WorkflowName) ||
		strings.TrimSpace(want.WorkflowVersion) != strings.TrimSpace(got.WorkflowVersion) {
		return fmt.Errorf("selected-contract execution admission %s selection does not match durable binding", label)
	}
	return nil
}

func validateSelectedContractSelection(label string, selection store.RunForkContractSelection) error {
	switch strings.TrimSpace(selection.Mode) {
	case store.RunForkContractSelectionModeSelectedContracts:
		if strings.TrimSpace(selection.ContractsRoot) == "" {
			return fmt.Errorf("selected-contract execution admission %s requires contracts_root", label)
		}
		if strings.TrimSpace(selection.BundleHash) != "" {
			return fmt.Errorf("selected-contract execution admission %s selected_contracts mode cannot carry bundle_hash", label)
		}
	case store.RunForkContractSelectionModeBundleHash:
		if strings.TrimSpace(selection.BundleHash) == "" {
			return fmt.Errorf("selected-contract execution admission %s requires bundle_hash", label)
		}
		if err := runtimecontracts.ValidateBundleHash(selection.BundleHash); err != nil {
			return fmt.Errorf("selected-contract execution admission %s bundle_hash invalid: %w", label, err)
		}
		if strings.TrimSpace(selection.ContractsRoot) != "" {
			return fmt.Errorf("selected-contract execution admission %s bundle_hash mode cannot carry contracts_root", label)
		}
	default:
		return fmt.Errorf("selected-contract execution admission %s requires mode selected_contracts or bundle_hash; got %q", label, selection.Mode)
	}
	if strings.TrimSpace(selection.WorkflowName) == "" {
		return fmt.Errorf("selected-contract execution admission %s requires workflow_name", label)
	}
	if strings.TrimSpace(selection.WorkflowVersion) == "" {
		return fmt.Errorf("selected-contract execution admission %s requires workflow_version", label)
	}
	return nil
}

func validateSelectedSourceLoaderSelection(selection store.RunForkContractSelection) error {
	switch strings.TrimSpace(selection.Mode) {
	case store.RunForkContractSelectionModeSelectedContracts:
		if strings.TrimSpace(selection.ContractsRoot) == "" {
			return fmt.Errorf("selected-contract execution admission selected source loader requires contracts_root")
		}
	case store.RunForkContractSelectionModeBundleHash:
		if strings.TrimSpace(selection.BundleHash) == "" {
			return fmt.Errorf("selected-contract execution admission selected source loader requires bundle_hash")
		}
		if err := runtimecontracts.ValidateBundleHash(selection.BundleHash); err != nil {
			return fmt.Errorf("selected-contract execution admission selected source loader bundle_hash invalid: %w", err)
		}
	default:
		return fmt.Errorf("selected-contract execution admission selected source loader requires mode selected_contracts or bundle_hash; got %q", selection.Mode)
	}
	return nil
}
