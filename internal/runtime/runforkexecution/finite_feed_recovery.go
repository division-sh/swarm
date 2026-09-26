package runforkexecution

import (
	"context"
	"errors"
	"fmt"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
)

type selectedFiniteFeedRecoveryAdmission struct {
	request SelectedContractExecutionRequest
	binding runfork.RunForkSelectedContractBinding
	action  selectedRecoveryAction
}

func (o SelectedContractExecutionOwner) admitSelectedFiniteFeedRecovery(ctx context.Context, recovered runfork.SelectedForkRecoveryResult, request SelectedContractExecutionRequest) (selectedFiniteFeedRecoveryAdmission, error) {
	if recovered.Resume == nil {
		return selectedFiniteFeedRecoveryAdmission{}, errors.New("selected finite-feed recovery lacks durable operation")
	}
	if recovered.Resume.ForkRunStatus != runfork.RunForkMaterializedStatus {
		return selectedFiniteFeedRecoveryAdmission{}, errors.New("selected finite-feed recovery child is not paused")
	}
	ports, err := o.require()
	if err != nil {
		return selectedFiniteFeedRecoveryAdmission{}, err
	}
	operation, _, err := recovered.Resume.Operation.Canonical()
	if err != nil {
		return selectedFiniteFeedRecoveryAdmission{}, fmt.Errorf("selected finite-feed recovery operation: %w", err)
	}
	if operation.ResolvedPoint == nil {
		return selectedFiniteFeedRecoveryAdmission{}, errors.New("selected finite-feed recovery operation lacks fixed revision point")
	}
	if request.SourceRunID != "" || request.At != "" || request.ExpectedBundleHash != "" || request.ForkOperation != nil ||
		request.DataPinOverrides != nil || request.ContractSelection != (runfork.RunForkContractSelection{}) ||
		request.AllowSourceFreeze || request.SourceArtifactFact.BundleHash() != "" ||
		request.EffectiveSourceIdentity.Digest() != "" || request.EffectiveSourceIdentity.SourceArtifactFact().BundleHash() != "" ||
		request.Recovery != nil {
		return selectedFiniteFeedRecoveryAdmission{}, errors.New("selected finite-feed recovery accepts source loader and runtime options, not caller-selected fork semantics")
	}
	binding, err := ports.fork.RequireRunForkSelectedContractBinding(ctx, recovered.RunID)
	if err != nil {
		return selectedFiniteFeedRecoveryAdmission{}, err
	}
	action, err := selectedRecoveryActionFor(recovered, runfork.SelectedForkRecoveryEntry{Binding: binding, BundleHash: operation.TargetBundleHash})
	if err != nil {
		return selectedFiniteFeedRecoveryAdmission{}, err
	}
	if action != selectedRecoveryResumeFiniteFeed && action != selectedRecoveryActivateFiniteFeed {
		return selectedFiniteFeedRecoveryAdmission{}, errors.New("selected finite-feed recovery has no executable action")
	}
	request.Owner = o
	request.SourceRunID = operation.SourceRunID
	request.ExpectedBundleHash = operation.TargetBundleHash
	request.ContractSelection = operation.ContractSelection
	request.AllowSourceFreeze = operation.AllowSourceFreeze
	request.DataPinOverrides = operation.DataPinOverrides
	request.ForkOperation = &operation
	return selectedFiniteFeedRecoveryAdmission{request: request, binding: binding, action: action}, nil
}

// ActivateRecoveredSelectedFiniteFeed completes an already quiesced finite
// feed. It neither issues another execution nor republishes source events.
func (o SelectedContractExecutionOwner) ActivateRecoveredSelectedFiniteFeed(ctx context.Context, recovered runfork.SelectedForkRecoveryResult, request SelectedContractExecutionRequest) (activation runfork.RunForkActivation, finalErr error) {
	if recovered.Disposition != runfork.SelectedForkRecoveryActivateFiniteFeed {
		return runfork.RunForkActivation{}, errors.New("selected finite-feed activation requires quiesced recovery evidence")
	}
	admitted, err := o.admitSelectedFiniteFeedRecovery(ctx, recovered, request)
	if err != nil {
		return runfork.RunForkActivation{}, err
	}
	if admitted.action != selectedRecoveryActivateFiniteFeed {
		return runfork.RunForkActivation{}, errors.New("selected finite-feed activation received resume action")
	}
	ports := o.ports
	operation := *admitted.request.ForkOperation
	if admitted.request.SourceLoader == nil {
		return runfork.RunForkActivation{}, errors.New("selected finite-feed activation requires exact source loader")
	}
	selection, err := normalizeSelectedContractExecutionSelection(operation.ContractSelection)
	if err != nil {
		return runfork.RunForkActivation{}, err
	}
	var owned LoadedSelectedContractSource
	loaded, err := loadRunForkSelectedContractSource(ctx, admitted.request.SourceLoader, SelectedContractSourceLoadRequest{
		SourceRunID: operation.SourceRunID, BundleHash: operation.TargetBundleHash,
		SourceArtifactFact: admitted.request.SourceArtifactFact, Selection: selection,
	}, &owned)
	defer func() { finalErr = errors.Join(finalErr, cleanupLoadedSelectedContractSource(owned)) }()
	if err != nil {
		return runfork.RunForkActivation{}, err
	}
	if loaded.Module == nil || loaded.SourceArtifactFact.BundleHash() != operation.TargetBundleHash {
		return runfork.RunForkActivation{}, errors.New("selected finite-feed activation source is not the fixed executable bundle")
	}
	if err := loaded.EffectiveSourceIdentity.Validate(); err != nil {
		return runfork.RunForkActivation{}, fmt.Errorf("selected finite-feed activation source identity: %w", err)
	}
	plan, err := ports.fork.PlanRunFork(ctx, runfork.RunForkPlanRequest{
		SourceRunID: operation.SourceRunID, ResolvedPoint: operation.ResolvedPoint,
	})
	if err != nil {
		return runfork.RunForkActivation{}, err
	}
	if !sameSelectedForkPointIdentity(plan.ForkPoint, *operation.ResolvedPoint) ||
		plan.SourceRunID != admitted.binding.SourceRunID {
		return runfork.RunForkActivation{}, errors.New("selected finite-feed activation plan differs from durable eventless binding")
	}
	deferredWork, err := admitSelectedContractDeferredWork(plan, loaded.Source)
	if err != nil {
		return runfork.RunForkActivation{}, err
	}
	frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{
		Plan: plan, Source: loaded.Source, ContractSelection: loaded.Selection,
	})
	if err != nil {
		return runfork.RunForkActivation{}, err
	}
	routes, err := runforkadmission.AdmitSelectedContractRouteHistory(runforkadmission.SelectedContractRouteHistoryRequest{
		Plan: plan, Source: loaded.Source, ContractSelection: loaded.Selection, FrontierAdmission: frontier,
	})
	if err != nil {
		return runfork.RunForkActivation{}, err
	}
	if err := validateSelectedContractExecutionFrontierForMutation(frontier); err != nil {
		return runfork.RunForkActivation{}, err
	}
	if len(selectedContractExecutionFrontierEventIDs(frontier.FrontierEvents)) != 0 {
		return runfork.RunForkActivation{}, errors.New("selected finite-feed activation has executable source frontier")
	}
	topology, err := BuildSelectedContractRouteTopology(SelectedContractRouteTopologyRequest{Admission: frontier, RouteAdmission: routes})
	if err != nil {
		return runfork.RunForkActivation{}, err
	}
	model, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{
		Admission: frontier, RouteAdmission: routes, RouteTopology: topology,
	})
	if err != nil {
		return runfork.RunForkActivation{}, err
	}
	admission, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID: recovered.RunID, SourceRunID: admitted.binding.SourceRunID,
		SourceArtifactFact: loaded.SourceArtifactFact,
		BindingReader:      ports.fork, LoadedSource: loaded,
		FrontierAdmission: frontier, RouteAdmission: routes,
		RouteTopology: topology, ExecutionModel: model,
		DeferredWorkAdmission: deferredWork,
	})
	if err != nil {
		return runfork.RunForkActivation{}, err
	}
	if admission.ForkRunID != recovered.RunID {
		return runfork.RunForkActivation{}, errors.New("selected finite-feed activation admission changed child identity")
	}
	activationCtx := runtimecorrelation.WithSourceArtifactFact(ctx, loaded.SourceArtifactFact)
	return ports.fork.ActivateRunForkForSelectedContractExecution(activationCtx, runfork.RunForkSelectedContractExecutionActivateRequest{
		ForkOperation: &operation, DataPins: recovered.Resume.Pins,
		ExecutionSource: loaded.Source, ForkRunID: recovered.RunID,
		AllowSourceFreeze:     operation.AllowSourceFreeze,
		AllowedSourceEventIDs: nil, FrontierAdmission: frontier,
		RouteTopology: topology, RecipientPlanning: *model.RecipientPlanning,
	})
}
