package runforkexecution

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimecorrelation "swarm/internal/runtime/correlation"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/runforkadmission"
	"swarm/internal/store"
)

type SelectedContractExecutionRequest struct {
	SourceRunID       string
	At                string
	Store             *store.PostgresStore
	SourceLoader      SelectedContractSourceLoader
	ContractSelection store.RunForkContractSelection
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
	loadedSource, err := req.SourceLoader.LoadRunForkSelectedContractSource(ctx, selection)
	if err != nil {
		return SelectedContractExecutionResult{}, fmt.Errorf("load selected semantic source for execution: %w", err)
	}
	selection = loadedSource.Selection
	if loadedSource.Module == nil {
		return SelectedContractExecutionResult{}, fmt.Errorf("selected-contract execution requires executable selected workflow module")
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
	model, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{Admission: frontier})
	if err != nil {
		return SelectedContractExecutionResult{}, err
	}
	sourceEventIDs := selectedContractExecutionFrontierEventIDs(frontier.FrontierEvents)
	materialization, err := req.Store.MaterializeRunForkForSelectedContractExecution(ctx, store.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID:       plan.SourceRunID,
		At:                plan.ForkPoint.EventID,
		ContractSelection: selection,
	})
	if err != nil {
		return SelectedContractExecutionResult{Owner: store.RunForkSelectedContractExecutionOwner, Materialization: materialization}, err
	}
	admission, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:         materialization.ForkRunID,
		BindingReader:     req.Store,
		SourceLoader:      req.SourceLoader,
		FrontierAdmission: frontier,
		ExecutionModel:    model,
	})
	if err != nil {
		return SelectedContractExecutionResult{Owner: store.RunForkSelectedContractExecutionOwner, Materialization: materialization}, err
	}
	published, err := publishSelectedContractForkEvents(ctx, publishSelectedContractForkEventsRequest{
		Store:        req.Store,
		LoadedSource: loadedSource,
		SourceRunID:  plan.SourceRunID,
		ForkRunID:    materialization.ForkRunID,
		SourceEvents: sourceEventIDs,
	})
	if err != nil {
		return SelectedContractExecutionResult{
			Owner:                              store.RunForkSelectedContractExecutionOwner,
			Materialization:                    materialization,
			SelectedContractExecutionAdmission: &admission,
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
			ExecutedEventCount:                 len(published),
			ForkEvents:                         published,
		}, cleanupSelectedContractExecutionFailure(ctx, req.Store, materialization.ForkRunID, err)
	}
	result := SelectedContractExecutionResult{
		Owner:                              store.RunForkSelectedContractExecutionOwner,
		Materialization:                    materialization,
		Activation:                         activation,
		SelectedContractExecutionAdmission: &admission,
		ExecutedEventCount:                 len(published),
		ForkEvents:                         published,
	}
	return result, err
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
	Store        *store.PostgresStore
	LoadedSource LoadedSelectedContractSource
	SourceRunID  string
	ForkRunID    string
	SourceEvents []string
}

func publishSelectedContractForkEvents(ctx context.Context, req publishSelectedContractForkEventsRequest) ([]SelectedContractExecutionForkEvent, error) {
	sourceEvents, err := req.Store.LoadRunForkSelectedContractSourceEvents(ctx, req.SourceRunID, req.SourceEvents)
	if err != nil {
		return nil, err
	}
	bus, err := runtimebus.NewEventBusWithOptions(req.Store, runtimebus.EventBusOptions{
		ContractBundle: req.LoadedSource.Source,
	})
	if err != nil {
		return nil, fmt.Errorf("create selected-contract execution bus: %w", err)
	}
	pipeline := newSelectedContractPipeline(bus, req.Store, req.LoadedSource)
	bus.SetInterceptors(pipeline)

	runCtx := runtimecorrelation.WithRunID(ctx, req.ForkRunID)
	out := make([]SelectedContractExecutionForkEvent, 0, len(sourceEvents))
	for _, sourceEvent := range sourceEvents {
		forkEventID := uuid.NewString()
		evt := selectedContractForkEvent(req.ForkRunID, forkEventID, sourceEvent)
		if err := bus.Publish(runCtx, evt); err != nil {
			return out, fmt.Errorf("execute selected-contract fork event %s as %s: %w", sourceEvent.SourceEventID, forkEventID, err)
		}
		lineage := store.RunForkSelectedContractExecutionLineage{
			Owner:         store.RunForkSelectedContractExecutionLineageOwner,
			ForkRunID:     req.ForkRunID,
			SourceRunID:   req.SourceRunID,
			SourceEventID: sourceEvent.SourceEventID,
			ForkEventID:   forkEventID,
			EventName:     sourceEvent.EventName,
			CreatedAt:     time.Now().UTC(),
		}
		if err := req.Store.RecordRunForkSelectedContractExecutionLineage(ctx, lineage); err != nil {
			return out, err
		}
		out = append(out, SelectedContractExecutionForkEvent{
			SourceEventID: sourceEvent.SourceEventID,
			ForkEventID:   forkEventID,
			EventName:     sourceEvent.EventName,
		})
	}
	return out, nil
}

func selectedContractForkEvent(forkRunID, forkEventID string, sourceEvent store.RunForkSelectedContractSourceEvent) events.Event {
	payload := json.RawMessage("{}")
	if len(sourceEvent.Payload) > 0 && json.Valid(sourceEvent.Payload) {
		payload = append(json.RawMessage(nil), sourceEvent.Payload...)
	}
	evt := events.Event{
		ID:          strings.TrimSpace(forkEventID),
		Type:        events.EventType(strings.TrimSpace(sourceEvent.EventName)),
		SourceAgent: store.RunForkSelectedContractExecutionOwner,
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
