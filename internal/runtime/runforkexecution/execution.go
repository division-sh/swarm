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
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
)

type SelectedContractExecutionRequest struct {
	SourceRunID             string
	At                      string
	ExpectedBundleHash      string
	SourceArtifactFact      runtimecorrelation.SourceArtifactFact
	EffectiveSourceIdentity scenarioexecution.EffectiveSourceIdentity
	AllowSourceFreeze       bool
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

func ExecuteSelectedContractRunFork(ctx context.Context, req SelectedContractExecutionRequest) (out SelectedContractExecutionResult, finalErr error) {
	prepared, err := req.Owner.Prepare(ctx, req)
	if err != nil {
		return SelectedContractExecutionResult{Owner: runfork.RunForkSelectedContractExecutionOwner}, err
	}
	defer func() { finalErr = errors.Join(finalErr, req.Owner.completePreparation(prepared)) }()
	ports, operation, loadedSource := req.Owner.ports, prepared.operation, prepared.loadedSource
	plan, frontier, routeAdmission := prepared.plan, prepared.frontier, prepared.routeAdmission
	routeTopology, model := prepared.routeTopology, prepared.model
	agentRuntime := prepared.agentRuntime
	deferredWorkAdmission := prepared.deferredWorkAdmission
	sourceEventIDs := selectedContractExecutionFrontierEventIDs(frontier.FrontierEvents)
	ctx = operation.PreparationContext()
	materialization, err := req.Owner.materializePrepared(ctx, prepared)
	if err != nil {
		return SelectedContractExecutionResult{Owner: runfork.RunForkSelectedContractExecutionOwner, Materialization: materialization}, err
	}
	ctx = operation.Context()
	agentRuntime, err = agentRuntime.bindRun(materialization.ForkRunID, materialization.AgentTopologies)
	if err != nil {
		return SelectedContractExecutionResult{
			Owner: runfork.RunForkSelectedContractExecutionOwner, Materialization: materialization,
			AgentRuntimeMaterialization: &agentRuntime.Proof,
		}, cleanupSelectedContractExecutionFailure(ctx, ports.fork, materialization.ForkRunID, err)
	}
	if _, err := RequireSelectedContractAgentDeliveryMaterialization(ctx, SelectedContractAgentDeliveryMaterializationRequest{
		RunID:             materialization.ForkRunID,
		RecipientPlanning: *model.RecipientPlanning,
		AgentRuntime:      agentRuntime.Proof,
	}); err != nil {
		return SelectedContractExecutionResult{
			Owner: runfork.RunForkSelectedContractExecutionOwner, Materialization: materialization,
			AgentRuntimeMaterialization: &agentRuntime.Proof,
		}, cleanupSelectedContractExecutionFailure(ctx, ports.fork, materialization.ForkRunID, err)
	}
	admission, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:             materialization.ForkRunID,
		SourceRunID:           plan.SourceRunID,
		SourceArtifactFact:    loadedSource.SourceArtifactFact,
		BindingReader:         ports.fork,
		LoadedSource:          loadedSource,
		FrontierAdmission:     frontier,
		RouteAdmission:        routeAdmission,
		RouteTopology:         routeTopology,
		ExecutionModel:        model,
		DeferredWorkAdmission: deferredWorkAdmission,
	})
	if err != nil {
		return SelectedContractExecutionResult{Owner: runfork.RunForkSelectedContractExecutionOwner, Materialization: materialization}, cleanupSelectedContractExecutionFailure(ctx, ports.fork, materialization.ForkRunID, err)
	}
	container, err := buildSelectedContractForkLocalRuntimeContainer(ctx, publishSelectedContractForkEventsRequest{
		Prepared:              prepared,
		Operation:             operation,
		Owner:                 req.Owner,
		Admission:             admission,
		LoadedSource:          loadedSource,
		RecipientPlanning:     *model.RecipientPlanning,
		AgentRuntime:          agentRuntime,
		SourceRunID:           plan.SourceRunID,
		ForkRunID:             materialization.ForkRunID,
		ForkEventID:           plan.ForkPoint.EventID,
		SourceEvents:          sourceEventIDs,
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
	ctx = operation.Context()
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
		ExecutionSource:       loadedSource.Source,
		ForkRunID:             materialization.ForkRunID,
		AllowSourceFreeze:     req.AllowSourceFreeze,
		AllowedSourceEventIDs: sourceEventIDs,
		FrontierAdmission:     frontier,
		RouteTopology:         routeTopology,
		RecipientPlanning:     *model.RecipientPlanning,
	})
	if err != nil {
		if closeErr := container.Close(ctx); closeErr != nil {
			err = errors.Join(err, closeErr)
		} else if !activation.Activated {
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
	if activation.ForkRunStatus == runfork.RunForkActivatedStatus {
		if err := req.Owner.retainPrepared(prepared); err != nil {
			return SelectedContractExecutionResult{}, err
		}
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
	if selectedStopOwnsDisposition(ctx) {
		// The admitted stop use remains process-owned until this execution has
		// joined and the named terminal operation has committed or failed closed.
		return cause
	}
	if err := store.DiscardMaterializedSelectedContractExecutionFork(context.WithoutCancel(ctx), forkRunID); err != nil {
		return errors.Join(cause, fmt.Errorf("cleanup selected-contract fork %s: %w", forkRunID, err))
	}
	return cause
}

type publishSelectedContractForkEventsRequest struct {
	Prepared              *PreparedSelectedFork
	Operation             *selectedContractOperation
	Owner                 SelectedContractExecutionOwner
	Admission             runfork.RunForkSelectedContractExecutionAdmission
	LoadedSource          LoadedSelectedContractSource
	RecipientPlanning     runfork.RunForkSelectedContractRecipientPlanning
	AgentRuntime          selectedContractAgentRuntimePlan
	SourceRunID           string
	ForkRunID             string
	ForkEventID           string
	SourceEvents          []string
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
		Scope: events.EventScope(strings.TrimSpace(sourceEvent.Scope)),
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
	instanceDeactivationPreparer runtimepipeline.FlowInstanceDeactivationPreparer,
) *runtimepipeline.PipelineCoordinator {
	return runtimepipeline.NewPipelineCoordinatorWithOptions(bus, selectedContractPipelineCoordinatorOptions(bus, ports, loaded, agentRuntime, instanceActivator, instanceDeactivationPreparer))
}

func selectedContractPipelineCoordinatorOptions(
	bus *runtimebus.EventBus,
	ports *selectedContractExecutionPorts,
	loaded LoadedSelectedContractSource,
	agentRuntime SelectedContractAgentRuntimeOptions,
	instanceActivator runtimepipeline.FlowInstanceActivator,
	instanceDeactivationPreparer runtimepipeline.FlowInstanceDeactivationPreparer,
) runtimepipeline.PipelineCoordinatorOptions {
	var scenarioProfiles runtimepipeline.ScenarioExecutionProfileReader
	if reader, ok := ports.fork.(runtimepipeline.ScenarioExecutionProfileReader); ok {
		scenarioProfiles = reader
	}
	return runtimepipeline.PipelineCoordinatorOptions{
		ExecutionPosture:             agentRuntime.ExecutionPosture,
		WorkOwner:                    agentRuntime.AgentManagerOptions.WorkOwner,
		TestLifecycleProbe:           agentRuntime.AgentManagerOptions.TestLifecycleProbe,
		ReceiverExecution:            agentRuntime.AgentManagerOptions.ReceiverExecution,
		Module:                       loaded.Module,
		Persistence:                  ports.workflow,
		DeliveryStore:                ports.busDurable.DeliveryLifecycle,
		DeadLetters:                  ports.busDurable.TargetFailureRecorder,
		PipelineObligations:          ports.pipelineObligations,
		InstanceActivator:            instanceActivator,
		InstanceDeactivationPreparer: instanceDeactivationPreparer,
		MailboxMaterializer:          ports.mailbox,
		DecisionCards:                ports.decisionCards,
		ProposedEffects:              ports.proposedEffects,
		HumanTasks:                   ports.humanTasks,
		DecisionCardDraftExpiry:      ports.decisionCardDraftExpiry,
		HumanTaskExpiry:              ports.humanTaskExpiry,
		DeliveryRuntime:              bus,
		FlowRoutes:                   bus,
		RunLifecycle:                 ports.busDurable.RunLifecycle,
		Credentials:                  agentRuntime.Credentials,
		ManagedCredentials:           agentRuntime.ManagedCredentials,
		MockConnectorResponses:       loaded.MockConnectorResponses,
		SourceArtifactFact:           loaded.SourceArtifactFact,
		ScenarioExecutionProfiles:    scenarioProfiles,
		EffectiveSourceIdentity:      loaded.EffectiveSourceIdentity,
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
