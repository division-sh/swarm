package runforkexecution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
)

type SelectedContractExecutionRequest struct {
	SourceRunID             string
	At                      string
	ExpectedBundleHash      string
	SourceArtifactFact      runtimecorrelation.SourceArtifactFact
	EffectiveSourceIdentity scenarioexecution.EffectiveSourceIdentity
	ConfirmSourceFreeze     bool
	DataPinOverrides        []durabledata.ExplicitPin

	Owner             SelectedContractExecutionOwner
	SourceLoader      SelectedContractSourceLoader
	ContractSelection runfork.RunForkContractSelection
	AgentRuntime      SelectedContractAgentRuntimeOptions
}

type SelectedContractExecutionForkEvent struct {
	SourceEventID string `json:"source_event_id"`
	ForkEventID   string `json:"fork_event_id"`
	EventName     string `json:"event_name"`
}

type SelectedContractExecutionResult struct {
	Owner                              string                                             `json:"owner"`
	Materialization                    runfork.RunForkMaterialization                     `json:"materialization"`
	Activation                         runfork.RunForkActivation                          `json:"activation"`
	SelectedContractExecutionAdmission *runfork.RunForkSelectedContractExecutionAdmission `json:"selected_contract_execution_admission,omitempty"`
	AgentRuntimeMaterialization        *SelectedContractAgentRuntimeMaterialization       `json:"selected_agent_runtime_materialization,omitempty"`
	ForkLocalRuntimeContainer          *SelectedContractForkLocalRuntimeContainer         `json:"fork_local_runtime_container,omitempty"`
	ExecutedEventCount                 int                                                `json:"executed_event_count"`
	ForkEvents                         []SelectedContractExecutionForkEvent               `json:"fork_events,omitempty"`
}

func ExecuteSelectedContractRunFork(ctx context.Context, req SelectedContractExecutionRequest) (SelectedContractExecutionResult, error) {
	ports, err := req.Owner.require()
	if err != nil {
		return SelectedContractExecutionResult{}, err
	}
	if req.SourceLoader == nil {
		return SelectedContractExecutionResult{}, fmt.Errorf("selected-contract execution requires selected source loader")
	}
	selection, err := normalizeSelectedContractExecutionSelection(req.ContractSelection)
	if err != nil {
		return SelectedContractExecutionResult{}, err
	}
	expectedBundleHash := strings.TrimSpace(req.ExpectedBundleHash)
	if req.SourceArtifactFact.BundleHash() != "" {
		if err := req.SourceArtifactFact.Validate(); err != nil {
			return SelectedContractExecutionResult{}, fmt.Errorf("selected-contract execution expected bundle source fact is invalid: %w", err)
		}
		if expectedBundleHash != "" && expectedBundleHash != req.SourceArtifactFact.BundleHash() {
			return SelectedContractExecutionResult{}, fmt.Errorf(
				"selected-contract execution expected bundle_hash %s does not match source fact %s",
				expectedBundleHash,
				req.SourceArtifactFact.BundleHash(),
			)
		}
		expectedBundleHash = req.SourceArtifactFact.BundleHash()
	}
	loadedSource, err := loadRunForkSelectedContractSource(ctx, req.SourceLoader, SelectedContractSourceLoadRequest{
		SourceRunID:        req.SourceRunID,
		BundleHash:         expectedBundleHash,
		SourceArtifactFact: req.SourceArtifactFact,
		Selection:          selection,
	})
	if err != nil {
		return SelectedContractExecutionResult{}, fmt.Errorf("load selected semantic source for execution: %w", err)
	}
	defer cleanupLoadedSelectedContractSource(loadedSource)
	selection = loadedSource.Selection
	if loadedSource.Module == nil {
		return SelectedContractExecutionResult{}, fmt.Errorf("selected-contract execution requires executable selected workflow module")
	}
	if err := loadedSource.SourceArtifactFact.Validate(); err != nil {
		return SelectedContractExecutionResult{}, fmt.Errorf("selected-contract source loader returned incomplete bundle identity: %w", err)
	}
	if err := loadedSource.EffectiveSourceIdentity.Validate(); err != nil {
		return SelectedContractExecutionResult{}, fmt.Errorf("selected-contract source loader returned incomplete effective source identity: %w", err)
	}
	if err := req.EffectiveSourceIdentity.Validate(); err == nil && !req.EffectiveSourceIdentity.Equal(loadedSource.EffectiveSourceIdentity) {
		return SelectedContractExecutionResult{}, fmt.Errorf("selected-contract effective source identity does not match loaded effective source")
	}
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, loadedSource.SourceArtifactFact)
	materializationBundleHash := loadedSource.SourceArtifactFact.BundleHash()
	selectedScope, err := runtimeauthoractivity.BundleScopeForTarget(ctx, materializationBundleHash)
	if err != nil {
		return SelectedContractExecutionResult{}, fmt.Errorf("resolve selected-contract author activity scope: %w", err)
	}
	ctx = runtimeauthoractivity.WithScope(ctx, selectedScope)
	descriptors, err := runtimepkg.AuthorActivityEventDescriptors(loadedSource.Source)
	if err != nil {
		return SelectedContractExecutionResult{}, fmt.Errorf("project selected-contract author activity descriptors: %w", err)
	}
	descriptorLease, err := ports.fork.RegisterAuthorActivityEventCatalog(selectedScope, descriptors)
	if err != nil {
		return SelectedContractExecutionResult{}, fmt.Errorf("register selected-contract author activity descriptors: %w", err)
	}
	defer descriptorLease.Release()
	plan, err := ports.fork.PlanRunFork(ctx, runfork.RunForkPlanRequest{
		SourceRunID: strings.TrimSpace(req.SourceRunID),
		At:          strings.TrimSpace(req.At),
	})
	if err != nil {
		return SelectedContractExecutionResult{}, fmt.Errorf("plan selected-contract execution: %w", err)
	}
	deferredWorkAdmission, err := admitSelectedContractDeferredWork(plan, loadedSource.Source)
	if err != nil {
		return SelectedContractExecutionResult{Owner: runfork.RunForkSelectedContractExecutionOwner}, err
	}
	frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{
		Plan:              plan,
		Source:            loadedSource.Source,
		ContractSelection: selection,
	})
	if err != nil {
		return SelectedContractExecutionResult{}, err
	}
	if frontier.FrontierEventCount == 0 {
		return SelectedContractExecutionResult{}, fmt.Errorf("selected-contract execution requires selected frontier events")
	}
	routeAdmission, err := runforkadmission.AdmitSelectedContractRouteHistory(runforkadmission.SelectedContractRouteHistoryRequest{
		Plan:              plan,
		Source:            loadedSource.Source,
		ContractSelection: selection,
		FrontierAdmission: frontier,
	})
	if err != nil {
		return SelectedContractExecutionResult{}, err
	}
	if err := validateSelectedContractExecutionFrontierForMutation(frontier); err != nil {
		return SelectedContractExecutionResult{Owner: runfork.RunForkSelectedContractExecutionOwner}, err
	}
	routeTopology, err := BuildSelectedContractRouteTopology(SelectedContractRouteTopologyRequest{
		Admission:      frontier,
		RouteAdmission: routeAdmission,
	})
	if err != nil {
		return SelectedContractExecutionResult{}, err
	}
	model, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{
		Admission:      frontier,
		RouteAdmission: routeAdmission,
		RouteTopology:  routeTopology,
	})
	if err != nil {
		return SelectedContractExecutionResult{}, err
	}
	agentRuntime, err := prepareSelectedContractAgentRuntimeMaterialization(ctx, loadedSource, *model.RecipientPlanning, req.AgentRuntime)
	if err != nil {
		return SelectedContractExecutionResult{
			Owner:                       runfork.RunForkSelectedContractExecutionOwner,
			AgentRuntimeMaterialization: &agentRuntime.Proof,
		}, err
	}
	if _, err := RequireSelectedContractAgentDeliveryMaterialization(ctx, SelectedContractAgentDeliveryMaterializationRequest{
		RecipientPlanning: *model.RecipientPlanning,
		AgentRuntime:      agentRuntime.Proof,
	}); err != nil {
		return SelectedContractExecutionResult{
			Owner:                       runfork.RunForkSelectedContractExecutionOwner,
			AgentRuntimeMaterialization: &agentRuntime.Proof,
		}, err
	}
	workflowStates, err := selectedContractWorkflowStateProjection(plan, loadedSource.Source, *model.RecipientPlanning)
	if err != nil {
		return SelectedContractExecutionResult{}, err
	}
	sourceEventIDs := selectedContractExecutionFrontierEventIDs(frontier.FrontierEvents)
	if !req.AgentRuntime.ExecutionPosture.Valid() {
		return SelectedContractExecutionResult{}, fmt.Errorf("selected-contract execution posture is invalid")
	}
	sourceModes, err := ports.replay.LoadRunForkSelectedContractSourceEventModes(ctx, plan.SourceRunID, sourceEventIDs)
	if err != nil {
		return SelectedContractExecutionResult{}, err
	}
	for _, mode := range sourceModes {
		if err := req.AgentRuntime.ExecutionPosture.Admit(mode, "selected-contract source event admission"); err != nil {
			return SelectedContractExecutionResult{}, err
		}
	}
	materialization, err := ports.fork.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID:             plan.SourceRunID,
		At:                      plan.ForkPoint.EventID,
		ContractSelection:       selection,
		SourceArtifactFact:      loadedSource.SourceArtifactFact,
		EffectiveSourceIdentity: loadedSource.EffectiveSourceIdentity,
		FrontierAdmission:       frontier,
		RouteTopology:           routeTopology,
		RecipientPlanning:       *model.RecipientPlanning,
		WorkflowStates:          workflowStates,
		DataPinOverrides:        req.DataPinOverrides,
		FanOutPlanRefs:          deferredWorkAdmission.fanOutPlanRefs,
	})
	if err != nil {
		return SelectedContractExecutionResult{Owner: runfork.RunForkSelectedContractExecutionOwner, Materialization: materialization}, err
	}
	admission, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:             materialization.ForkRunID,
		SourceRunID:           plan.SourceRunID,
		SourceArtifactFact:    loadedSource.SourceArtifactFact,
		BindingReader:         ports.fork,
		SourceLoader:          req.SourceLoader,
		FrontierAdmission:     frontier,
		RouteAdmission:        routeAdmission,
		RouteTopology:         routeTopology,
		ExecutionModel:        model,
		DeferredWorkAdmission: deferredWorkAdmission,
	})
	if err != nil {
		return SelectedContractExecutionResult{Owner: runfork.RunForkSelectedContractExecutionOwner, Materialization: materialization}, err
	}
	container, err := buildSelectedContractForkLocalRuntimeContainer(ctx, publishSelectedContractForkEventsRequest{
		Owner:                 req.Owner,
		Admission:             admission,
		LoadedSource:          loadedSource,
		RecipientPlanning:     *model.RecipientPlanning,
		AgentRuntime:          agentRuntime,
		SourceRunID:           plan.SourceRunID,
		ForkRunID:             materialization.ForkRunID,
		ForkEventID:           plan.ForkPoint.EventID,
		SourceEvents:          sourceEventIDs,
		WorkflowStates:        workflowStates,
		ExecutionOwner:        runfork.RunForkSelectedContractExecutionOwner,
		DeferredWorkAdmission: deferredWorkAdmission,
	})
	if err != nil {
		return SelectedContractExecutionResult{
			Owner:                              runfork.RunForkSelectedContractExecutionOwner,
			Materialization:                    materialization,
			SelectedContractExecutionAdmission: &admission,
			AgentRuntimeMaterialization:        &agentRuntime.Proof,
		}, cleanupSelectedContractExecutionFailure(ctx, ports.fork, materialization.ForkRunID, err)
	}
	containerProof := container.Proof()
	published, err := container.Publish(ctx)
	if err != nil {
		if authorityErr := container.Fail(ctx, err); authorityErr != nil {
			err = errors.Join(err, authorityErr)
		} else {
			err = cleanupSelectedContractExecutionFailure(ctx, ports.fork, materialization.ForkRunID, err)
		}
		return SelectedContractExecutionResult{
			Owner:                              runfork.RunForkSelectedContractExecutionOwner,
			Materialization:                    materialization,
			SelectedContractExecutionAdmission: &admission,
			AgentRuntimeMaterialization:        &agentRuntime.Proof,
			ForkLocalRuntimeContainer:          &containerProof,
			ExecutedEventCount:                 len(published),
			ForkEvents:                         published,
		}, err
	}
	if err := container.Quiesce(ctx); err != nil {
		if authorityErr := container.Fail(ctx, err); authorityErr != nil {
			return SelectedContractExecutionResult{}, errors.Join(err, authorityErr)
		}
		return SelectedContractExecutionResult{}, cleanupSelectedContractExecutionFailure(ctx, ports.fork, materialization.ForkRunID, err)
	}
	activation, err := ports.fork.ActivateRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionActivateRequest{
		ForkRunID:             materialization.ForkRunID,
		ConfirmSourceFreeze:   req.ConfirmSourceFreeze,
		AllowedSourceEventIDs: sourceEventIDs,
		FrontierAdmission:     frontier,
		RouteTopology:         routeTopology,
		RecipientPlanning:     *model.RecipientPlanning,
	})
	if err != nil {
		if closeErr := container.Close(ctx); closeErr != nil {
			err = errors.Join(err, closeErr)
		} else {
			err = cleanupSelectedContractExecutionFailure(ctx, ports.fork, materialization.ForkRunID, err)
		}
		return SelectedContractExecutionResult{
			Owner:                              runfork.RunForkSelectedContractExecutionOwner,
			Materialization:                    materialization,
			Activation:                         activation,
			SelectedContractExecutionAdmission: &admission,
			AgentRuntimeMaterialization:        &agentRuntime.Proof,
			ForkLocalRuntimeContainer:          &containerProof,
			ExecutedEventCount:                 len(published),
			ForkEvents:                         published,
		}, err
	}
	if err := container.Close(ctx); err != nil {
		return SelectedContractExecutionResult{}, err
	}
	result := SelectedContractExecutionResult{
		Owner:                              runfork.RunForkSelectedContractExecutionOwner,
		Materialization:                    materialization,
		Activation:                         activation,
		SelectedContractExecutionAdmission: &admission,
		AgentRuntimeMaterialization:        &agentRuntime.Proof,
		ForkLocalRuntimeContainer:          &containerProof,
		ExecutedEventCount:                 len(published),
		ForkEvents:                         published,
	}
	return result, err
}

func validateSelectedContractExecutionFrontierForMutation(frontier runfork.RunForkContractFrontierAdmission) error {
	for _, blocker := range frontier.UnsupportedBlockers {
		code := strings.TrimSpace(blocker.Code)
		switch code {
		case "", runfork.RunForkBlockerContractFrontierExecutionUnsupported:
			continue
		default:
			if msg := strings.TrimSpace(blocker.Message); msg != "" {
				return fmt.Errorf("%s: %s", code, msg)
			}
			return fmt.Errorf("%s", code)
		}
	}
	return nil
}

func cleanupSelectedContractExecutionFailure(ctx context.Context, store SelectedContractForkLifecycle, forkRunID string, cause error) error {
	if cause == nil {
		return nil
	}
	if store == nil || strings.TrimSpace(forkRunID) == "" {
		return cause
	}
	if err := store.DiscardMaterializedSelectedContractExecutionFork(ctx, forkRunID); err != nil {
		return fmt.Errorf("%w; cleanup selected-contract fork %s: %v", cause, forkRunID, err)
	}
	return cause
}

type publishSelectedContractForkEventsRequest struct {
	Owner                 SelectedContractExecutionOwner
	Admission             runfork.RunForkSelectedContractExecutionAdmission
	LoadedSource          LoadedSelectedContractSource
	RecipientPlanning     runfork.RunForkSelectedContractRecipientPlanning
	AgentRuntime          selectedContractAgentRuntimePlan
	SourceRunID           string
	ForkRunID             string
	ForkEventID           string
	SourceEvents          []string
	WorkflowStates        []runfork.RunForkSelectedContractWorkflowState
	ExecutionOwner        string
	RuntimeInstanceID     string
	DeferredWorkAdmission selectedContractDeferredWorkAdmission
}

func selectedContractForkEvent(sourceRunID, forkRunID, forkEventID string, sourceEvent runfork.RunForkSelectedContractSourceEvent, producerID string) (events.Event, error) {
	payload := json.RawMessage("{}")
	if len(sourceEvent.Payload) > 0 && json.Valid(sourceEvent.Payload) {
		payload = append(json.RawMessage(nil), sourceEvent.Payload...)
	}
	envelope := events.EventEnvelope{
		EntityID:     strings.TrimSpace(sourceEvent.EntityID),
		FlowInstance: strings.Trim(strings.TrimSpace(sourceEvent.FlowInstance), "/"),
		Scope:        events.EventScope(strings.TrimSpace(sourceEvent.Scope)),
	}
	routingSource := sourceEvent.RoutingSource
	if sourceRoute := routingSource.Route(); !sourceRoute.Empty() && routingSource.Kind() != events.RoutingSourceExternalIngress {
		envelope = events.EnvelopeForSourceRoute(envelope, sourceRoute)
	}
	lineage, err := events.NewSelectedForkLineage(forkRunID, sourceRunID, sourceEvent.SourceEventID, producerID, "", sourceEvent.ExecutionMode)
	if err != nil {
		var event events.Event
		return event, err
	}
	return events.NewSelectedForkReplayEvent(events.SelectedForkReplayEventInput{
		Facts: events.EventFacts{
			ID: strings.TrimSpace(forkEventID), Type: events.EventType(strings.TrimSpace(sourceEvent.EventName)),
			Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: producerID},
			Payload:  payload, Envelope: envelope, RoutingSource: routingSource,
			CreatedAt: time.Now().UTC(), ExecutionMode: sourceEvent.ExecutionMode,
		},
		Lineage: lineage,
	})
}

func newSelectedContractPipeline(
	bus *runtimebus.EventBus,
	ports *selectedContractExecutionPorts,
	loaded LoadedSelectedContractSource,
	agentRuntime SelectedContractAgentRuntimeOptions,
	instanceActivator runtimepipeline.FlowInstanceActivator,
) *runtimepipeline.PipelineCoordinator {
	return runtimepipeline.NewPipelineCoordinatorWithOptions(bus, selectedContractPipelineCoordinatorOptions(bus, ports, loaded, agentRuntime, instanceActivator))
}

func selectedContractPipelineCoordinatorOptions(
	bus *runtimebus.EventBus,
	ports *selectedContractExecutionPorts,
	loaded LoadedSelectedContractSource,
	agentRuntime SelectedContractAgentRuntimeOptions,
	instanceActivator runtimepipeline.FlowInstanceActivator,
) runtimepipeline.PipelineCoordinatorOptions {
	var scenarioProfiles runtimepipeline.ScenarioExecutionProfileReader
	if reader, ok := ports.fork.(runtimepipeline.ScenarioExecutionProfileReader); ok {
		scenarioProfiles = reader
	}
	return runtimepipeline.PipelineCoordinatorOptions{
		ExecutionPosture:          agentRuntime.ExecutionPosture,
		WorkOwner:                 agentRuntime.AgentManagerOptions.WorkOwner,
		ReceiverExecution:         agentRuntime.AgentManagerOptions.ReceiverExecution,
		Module:                    loaded.Module,
		Persistence:               ports.workflow,
		DeliveryStore:             ports.busDurable.DeliveryLifecycle,
		DeadLetters:               ports.busDurable.TargetFailureRecorder,
		PipelineObligations:       ports.pipelineObligations,
		InstanceActivator:         instanceActivator,
		MailboxMaterializer:       ports.mailbox,
		DecisionCards:             ports.decisionCards,
		ProposedEffects:           ports.proposedEffects,
		HumanTasks:                ports.humanTasks,
		DecisionCardDraftExpiry:   ports.decisionCardDraftExpiry,
		HumanTaskExpiry:           ports.humanTaskExpiry,
		DeliveryRuntime:           bus,
		FlowRoutes:                bus,
		RunLifecycle:              ports.busDurable.RunLifecycle,
		Credentials:               agentRuntime.Credentials,
		ManagedCredentials:        agentRuntime.ManagedCredentials,
		MockConnectorResponses:    loaded.MockConnectorResponses,
		SourceArtifactFact:        loaded.SourceArtifactFact,
		ScenarioExecutionProfiles: scenarioProfiles,
		EffectiveSourceIdentity:   loaded.EffectiveSourceIdentity,
	}
}

func selectedContractExecutionFrontierEventIDs(events []runfork.RunForkContractFrontierEvent) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(events))
	for _, event := range events {
		eventID := strings.TrimSpace(event.SourceEventID)
		if eventID == "" {
			continue
		}
		if _, ok := seen[eventID]; ok {
			continue
		}
		seen[eventID] = struct{}{}
		out = append(out, eventID)
	}
	return out
}

func normalizeSelectedContractExecutionSelection(selection runfork.RunForkContractSelection) (runfork.RunForkContractSelection, error) {
	selection.Mode = strings.TrimSpace(selection.Mode)
	if selection.Mode == "" {
		selection.Mode = runfork.RunForkContractSelectionModeSelectedContracts
	}
	selection.BundleHash = strings.TrimSpace(selection.BundleHash)
	switch selection.Mode {
	case runfork.RunForkContractSelectionModeSelectedContracts:
		if selection.BundleHash != "" {
			return runfork.RunForkContractSelection{}, fmt.Errorf("selected-contract execution selected_contracts mode cannot carry bundle_hash")
		}
	case runfork.RunForkContractSelectionModeBundleHash:
		if selection.BundleHash == "" {
			return runfork.RunForkContractSelection{}, fmt.Errorf("selected-contract execution requires bundle_hash")
		}
		if err := runtimecontracts.ValidateBundleHash(selection.BundleHash); err != nil {
			return runfork.RunForkContractSelection{}, fmt.Errorf("selected-contract execution bundle_hash invalid: %w", err)
		}
	default:
		return runfork.RunForkContractSelection{}, fmt.Errorf("selected-contract execution requires mode selected_contracts or bundle_hash; got %q", selection.Mode)
	}
	return selection, nil
}
