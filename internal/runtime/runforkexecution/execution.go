package runforkexecution

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/runforkadmission"
	"swarm/internal/store"
	storerunlifecycle "swarm/internal/store/runlifecycle"
)

type SelectedContractExecutionRequest struct {
	SourceRunID       string
	At                string
	BundleHash        string
	BundleSource      string
	Store             *store.PostgresStore
	SourceLoader      SelectedContractSourceLoader
	ContractSelection store.RunForkContractSelection
	AgentRuntime      SelectedContractAgentRuntimeOptions
}

type SelectedContractExecutionForkEvent struct {
	SourceEventID string `json:"source_event_id"`
	ForkEventID   string `json:"fork_event_id"`
	EventName     string `json:"event_name"`
}

type SelectedContractExecutionResult struct {
	Owner                              string                                           `json:"owner"`
	Materialization                    store.RunForkMaterialization                     `json:"materialization"`
	Activation                         store.RunForkActivation                          `json:"activation"`
	SelectedContractExecutionAdmission *store.RunForkSelectedContractExecutionAdmission `json:"selected_contract_execution_admission,omitempty"`
	AgentRuntimeMaterialization        *SelectedContractAgentRuntimeMaterialization     `json:"selected_agent_runtime_materialization,omitempty"`
	ForkLocalRuntimeContainer          *SelectedContractForkLocalRuntimeContainer       `json:"fork_local_runtime_container,omitempty"`
	ExecutedEventCount                 int                                              `json:"executed_event_count"`
	ForkEvents                         []SelectedContractExecutionForkEvent             `json:"fork_events,omitempty"`
}

func ExecuteSelectedContractRunFork(ctx context.Context, req SelectedContractExecutionRequest) (SelectedContractExecutionResult, error) {
	if req.Store == nil {
		return SelectedContractExecutionResult{}, fmt.Errorf("selected-contract execution requires store")
	}
	if req.SourceLoader == nil {
		return SelectedContractExecutionResult{}, fmt.Errorf("selected-contract execution requires selected source loader")
	}
	selection, err := normalizeSelectedContractExecutionSelection(req.ContractSelection)
	if err != nil {
		return SelectedContractExecutionResult{}, err
	}
	loadedSource, err := loadRunForkSelectedContractSource(ctx, req.SourceLoader, SelectedContractSourceLoadRequest{
		SourceRunID: req.SourceRunID,
		BundleHash:  req.BundleHash,
		Selection:   selection,
	})
	if err != nil {
		return SelectedContractExecutionResult{}, fmt.Errorf("load selected semantic source for execution: %w", err)
	}
	defer cleanupLoadedSelectedContractSource(loadedSource)
	selection = loadedSource.Selection
	if loadedSource.Module == nil {
		return SelectedContractExecutionResult{}, fmt.Errorf("selected-contract execution requires executable selected workflow module")
	}
	materializationBundleHash := firstNonEmpty(req.BundleHash, loadedSource.BundleHash)
	materializationBundleSource := strings.TrimSpace(req.BundleSource)
	if materializationBundleSource == "" && materializationBundleHash != "" {
		materializationBundleSource = storerunlifecycle.BundleSourcePersisted
	}
	plan, err := req.Store.PlanRunFork(ctx, store.RunForkPlanRequest{
		SourceRunID: strings.TrimSpace(req.SourceRunID),
		At:          strings.TrimSpace(req.At),
	})
	if err != nil {
		return SelectedContractExecutionResult{}, fmt.Errorf("plan selected-contract execution: %w", err)
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
		return SelectedContractExecutionResult{Owner: store.RunForkSelectedContractExecutionOwner}, err
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
			Owner:                       store.RunForkSelectedContractExecutionOwner,
			AgentRuntimeMaterialization: &agentRuntime.Proof,
		}, err
	}
	if _, err := RequireSelectedContractAgentDeliveryMaterialization(ctx, SelectedContractAgentDeliveryMaterializationRequest{
		RecipientPlanning: *model.RecipientPlanning,
		AgentRuntime:      agentRuntime.Proof,
	}); err != nil {
		return SelectedContractExecutionResult{
			Owner:                       store.RunForkSelectedContractExecutionOwner,
			AgentRuntimeMaterialization: &agentRuntime.Proof,
		}, err
	}
	sourceEventIDs := selectedContractExecutionFrontierEventIDs(frontier.FrontierEvents)
	materialization, err := req.Store.MaterializeRunForkForSelectedContractExecution(ctx, store.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID:       plan.SourceRunID,
		At:                plan.ForkPoint.EventID,
		ContractSelection: selection,
		BundleHash:        materializationBundleHash,
		BundleSource:      materializationBundleSource,
	})
	if err != nil {
		return SelectedContractExecutionResult{Owner: store.RunForkSelectedContractExecutionOwner, Materialization: materialization}, err
	}
	admission, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:         materialization.ForkRunID,
		SourceRunID:       plan.SourceRunID,
		BundleHash:        materializationBundleHash,
		BindingReader:     req.Store,
		SourceLoader:      req.SourceLoader,
		FrontierAdmission: frontier,
		RouteAdmission:    routeAdmission,
		RouteTopology:     routeTopology,
		ExecutionModel:    model,
	})
	if err != nil {
		return SelectedContractExecutionResult{Owner: store.RunForkSelectedContractExecutionOwner, Materialization: materialization}, err
	}
	if _, err := req.Store.RecordRunForkSelectedContractRouteRecovery(ctx, store.RunForkSelectedContractRouteRecoveryRequest{
		ForkRunID:         materialization.ForkRunID,
		SourceRunID:       plan.SourceRunID,
		ForkEventID:       plan.ForkPoint.EventID,
		ContractSelection: selection,
		RouteTopology:     routeTopology,
		RecipientPlanning: *model.RecipientPlanning,
	}); err != nil {
		return SelectedContractExecutionResult{
			Owner:                              store.RunForkSelectedContractExecutionOwner,
			Materialization:                    materialization,
			SelectedContractExecutionAdmission: &admission,
			AgentRuntimeMaterialization:        &agentRuntime.Proof,
		}, cleanupSelectedContractExecutionFailure(ctx, req.Store, materialization.ForkRunID, err)
	}
	container, err := buildSelectedContractForkLocalRuntimeContainer(ctx, publishSelectedContractForkEventsRequest{
		Store:             req.Store,
		LoadedSource:      loadedSource,
		RecipientPlanning: *model.RecipientPlanning,
		AgentRuntime:      agentRuntime,
		SourceRunID:       plan.SourceRunID,
		ForkRunID:         materialization.ForkRunID,
		ForkEventID:       plan.ForkPoint.EventID,
		ForkTime:          plan.ForkPoint.Timestamp,
		SourceEvents:      sourceEventIDs,
		ExecutionOwner:    store.RunForkSelectedContractExecutionOwner,
	})
	if err != nil {
		return SelectedContractExecutionResult{
			Owner:                              store.RunForkSelectedContractExecutionOwner,
			Materialization:                    materialization,
			SelectedContractExecutionAdmission: &admission,
			AgentRuntimeMaterialization:        &agentRuntime.Proof,
		}, cleanupSelectedContractExecutionFailure(ctx, req.Store, materialization.ForkRunID, err)
	}
	containerProof := container.Proof()
	published, err := container.Publish(ctx)
	if err != nil {
		return SelectedContractExecutionResult{
			Owner:                              store.RunForkSelectedContractExecutionOwner,
			Materialization:                    materialization,
			SelectedContractExecutionAdmission: &admission,
			AgentRuntimeMaterialization:        &agentRuntime.Proof,
			ForkLocalRuntimeContainer:          &containerProof,
			ExecutedEventCount:                 len(published),
			ForkEvents:                         published,
		}, cleanupSelectedContractExecutionFailure(ctx, req.Store, materialization.ForkRunID, err)
	}
	activation, err := req.Store.ActivateRunForkForSelectedContractExecution(ctx, store.RunForkSelectedContractExecutionActivateRequest{
		ForkRunID:             materialization.ForkRunID,
		AllowedSourceEventIDs: sourceEventIDs,
	})
	if err != nil {
		return SelectedContractExecutionResult{
			Owner:                              store.RunForkSelectedContractExecutionOwner,
			Materialization:                    materialization,
			Activation:                         activation,
			SelectedContractExecutionAdmission: &admission,
			AgentRuntimeMaterialization:        &agentRuntime.Proof,
			ForkLocalRuntimeContainer:          &containerProof,
			ExecutedEventCount:                 len(published),
			ForkEvents:                         published,
		}, cleanupSelectedContractExecutionFailure(ctx, req.Store, materialization.ForkRunID, err)
	}
	result := SelectedContractExecutionResult{
		Owner:                              store.RunForkSelectedContractExecutionOwner,
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

func validateSelectedContractExecutionFrontierForMutation(frontier store.RunForkContractFrontierAdmission) error {
	for _, blocker := range frontier.UnsupportedBlockers {
		code := strings.TrimSpace(blocker.Code)
		switch code {
		case "", store.RunForkBlockerContractFrontierExecutionUnsupported:
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

func cleanupSelectedContractExecutionFailure(ctx context.Context, store *store.PostgresStore, forkRunID string, cause error) error {
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
	Store             *store.PostgresStore
	LoadedSource      LoadedSelectedContractSource
	RecipientPlanning store.RunForkSelectedContractRecipientPlanning
	AgentRuntime      selectedContractAgentRuntimePlan
	SourceRunID       string
	ForkRunID         string
	ForkEventID       string
	ForkTime          time.Time
	SourceEvents      []string
	ExecutionOwner    string
}

func selectedContractForkEvent(forkRunID, forkEventID string, sourceEvent store.RunForkSelectedContractSourceEvent, sourceAgent string) events.Event {
	payload := json.RawMessage("{}")
	if len(sourceEvent.Payload) > 0 && json.Valid(sourceEvent.Payload) {
		payload = append(json.RawMessage(nil), sourceEvent.Payload...)
	}
	evt := events.Event{
		ID:          strings.TrimSpace(forkEventID),
		Type:        events.EventType(strings.TrimSpace(sourceEvent.EventName)),
		SourceAgent: strings.TrimSpace(sourceAgent),
		Payload:     payload,
		RunID:       strings.TrimSpace(forkRunID),
		CreatedAt:   time.Now().UTC(),
	}
	envelope := events.EventEnvelope{
		EntityID:     strings.TrimSpace(sourceEvent.EntityID),
		FlowInstance: strings.Trim(strings.TrimSpace(sourceEvent.FlowInstance), "/"),
		Scope:        events.EventScope(strings.TrimSpace(sourceEvent.Scope)),
	}
	return evt.WithEnvelope(envelope)
}

func newSelectedContractPipeline(bus *runtimebus.EventBus, store *store.PostgresStore, loaded LoadedSelectedContractSource) *runtimepipeline.PipelineCoordinator {
	return runtimepipeline.NewPipelineCoordinatorWithOptions(bus, store.DB, runtimepipeline.PipelineCoordinatorOptions{
		Module:                  loaded.Module,
		EventReceiptsCapability: store.CanonicalEventReceiptsCapability,
	})
}

func selectedContractExecutionFrontierEventIDs(events []store.RunForkContractFrontierEvent) []string {
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

func normalizeSelectedContractExecutionSelection(selection store.RunForkContractSelection) (store.RunForkContractSelection, error) {
	selection.Mode = strings.TrimSpace(selection.Mode)
	if selection.Mode == "" {
		selection.Mode = "selected_contracts"
	}
	if selection.Mode != "selected_contracts" {
		return store.RunForkContractSelection{}, fmt.Errorf("selected-contract execution requires mode selected_contracts; got %q", selection.Mode)
	}
	selection.ContractsRoot = strings.TrimSpace(selection.ContractsRoot)
	selection.WorkflowName = strings.TrimSpace(selection.WorkflowName)
	selection.WorkflowVersion = strings.TrimSpace(selection.WorkflowVersion)
	if selection.ContractsRoot == "" {
		return store.RunForkContractSelection{}, fmt.Errorf("selected-contract execution requires contracts_root")
	}
	return selection, nil
}
