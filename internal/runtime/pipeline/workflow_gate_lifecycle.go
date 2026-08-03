package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/google/uuid"
)

func (pc *PipelineCoordinator) applyWorkflowGateIntents(ctx context.Context, route runtimeflowidentity.Route, entityID, currentStage, nextStage, sourceEvent string, occurredAt time.Time) error {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.enabled() || pc.SemanticSource() == nil {
		return nil
	}
	if pc.workflowStore.engineMutations == nil {
		return fmt.Errorf("workflow gate lifecycle requires the selected workflow engine mutation owner")
	}
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	entityID = strings.TrimSpace(entityID)
	currentStage = strings.TrimSpace(currentStage)
	nextStage = strings.TrimSpace(nextStage)
	if !route.Valid() || entityID == "" || nextStage == "" || currentStage == nextStage {
		return nil
	}
	now := occurredAt.UTC()
	if now.IsZero() {
		return fmt.Errorf("workflow gate lifecycle requires an exact occurrence time")
	}
	instance, found, err := pc.workflowStore.Load(ctx, route)
	if err != nil {
		return err
	}
	if !found {
		return &WorkflowInstanceLookupMiss{RequestedKey: route.InstancePath}
	}
	expectedState := strings.TrimSpace(instance.CurrentState)
	expectedRevision := instance.Revision
	var prepared PreparedWorkflowLifecycleMutation
	if err := pc.planWorkflowGateEffect(ctx, &instance, entityID, currentStage, nextStage, sourceEvent, now, &prepared); err != nil {
		return err
	}
	runID := strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	if runID == "" {
		return fmt.Errorf("workflow gate lifecycle requires run identity")
	}
	updatedAt := time.Now().UTC()
	if updatedAt.Before(instance.CreatedAt) {
		updatedAt = instance.CreatedAt
	}
	state, err := workflowEngineStateRecord(runID, route, instance, expectedState, expectedRevision, false, updatedAt)
	if err != nil {
		return err
	}
	var publications []runtimeengine.DurablePublicationPlan
	if len(prepared.Emissions) > 0 {
		planner, ok := pc.bus.(EnginePublicationPlanner)
		if !ok {
			return fmt.Errorf("workflow gate lifecycle requires the publication planner")
		}
		publications, err = planner.PrepareEnginePublications(ctx, prepared.Emissions)
		if err != nil {
			return err
		}
		if len(publications) != len(prepared.Emissions) {
			releaseErr := planner.ReleaseEnginePublications(context.WithoutCancel(ctx), publications)
			return errors.Join(fmt.Errorf("workflow gate lifecycle planner returned %d plans for %d emissions", len(publications), len(prepared.Emissions)), releaseErr)
		}
	}
	committed, err := pc.workflowStore.engineMutations.CommitWorkflowEngineMutation(ctx, WorkflowEngineMutationCommand{
		State: state, Lifecycle: prepared.Commit, Publications: publications,
	})
	if err != nil {
		if planner, ok := pc.bus.(EnginePublicationPlanner); ok {
			err = errors.Join(err, planner.ReleaseEnginePublications(context.WithoutCancel(ctx), publications))
		}
		return err
	}
	if planner, ok := pc.bus.(EnginePublicationPlanner); ok {
		if err := planner.FinalizeEnginePublications(ctx, committed.Publications); err != nil {
			return err
		}
	}
	if err := pc.finalizeWorkflowLifecycleMutation(ctx, committed.Lifecycle); err != nil {
		return err
	}
	if len(prepared.Emissions) > 0 {
		dispatcher := pc.bus.EngineDispatcher()
		if dispatcher == nil {
			return fmt.Errorf("workflow gate lifecycle requires the post-commit dispatcher")
		}
		if err := dispatcher.DispatchPostCommit(context.WithoutCancel(ctx), prepared.Emissions); err != nil {
			return err
		}
	}
	return nil
}

func workflowGateSupersededEvent(card decisioncard.Card, activation gateruntime.Activation, instance WorkflowInstance, now time.Time) (events.Event, error) {
	payload, err := canonicaljson.Bytes(map[string]any{
		"card_id": activation.CardID, "anchor_kind": decisioncard.AnchorKindStageGate, "stage_activation_id": activation.ActivationID, "reason": activation.SupersededReason,
	})
	if err != nil {
		return events.Event{}, err
	}
	anchor, err := card.Anchor.StageGate()
	if err != nil {
		return events.Event{}, err
	}
	if err := validateStageGateInstanceOwner(anchor, instance, activation); err != nil {
		return events.Event{}, err
	}
	routingSource, err := card.Anchor.ControlRoutingSource()
	if err != nil {
		return events.Event{}, err
	}
	evt, err := events.NewRunScopedRuntimeControlEvent(events.RunScopedRuntimeEventInput{
		Facts: events.EventFacts{
			ID: uuid.NewString(), Type: events.EventType("mailbox.card_superseded"),
			Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "platform"},
			Payload:  payload, Envelope: events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, anchor.EntityID), anchor.Route.InstancePath),
			RoutingSource: routingSource, CreatedAt: now.UTC(), ExecutionMode: executionmode.Live,
		},
		RunID: card.RunID,
	})
	if err != nil {
		return events.Event{}, err
	}
	return evt, nil
}

func workflowGatePlanForInstance(pc *PipelineCoordinator, instance WorkflowInstance, stage string) (string, runtimecontracts.WorkflowGatePlan, bool) {
	if pc == nil || pc.SemanticSource() == nil {
		return "", runtimecontracts.WorkflowGatePlan{}, false
	}
	flowID := strings.TrimSpace(instance.WorkflowName)
	if plan, ok := pc.SemanticSource().WorkflowGateForStage(flowID, stage); ok {
		return flowID, plan, true
	}
	if flowID == strings.TrimSpace(pc.SemanticSource().WorkflowName()) {
		if plan, ok := pc.SemanticSource().WorkflowGateForStage("", stage); ok {
			return "", plan, true
		}
	}
	return "", runtimecontracts.WorkflowGatePlan{}, false
}

func workflowGateBundleHash(ctx context.Context, pc *PipelineCoordinator) string {
	if pc != nil && pc.bundleSourceFact.Validate() == nil {
		return pc.bundleSourceFact.BundleHash()
	}
	if fact, ok := runtimecorrelation.BundleSourceFactFromContext(ctx); ok && fact.Validate() == nil {
		return fact.BundleHash()
	}
	return ""
}

func (pc *PipelineCoordinator) buildWorkflowDecisionCard(ctx context.Context, entityID string, instance WorkflowInstance, flowInstance string, activation gateruntime.Activation, plan runtimecontracts.WorkflowGatePlan, frozenOutcomes map[string]runtimecontracts.WorkflowGateOutcomePlan) (decisioncard.Card, error) {
	contextSnapshot := make(map[string]any, len(plan.Context))
	for name, expression := range plan.Context {
		value, err := evalWorkflowGateContext(expression, instance, pc.SemanticSource(), plan.FlowID)
		if err != nil {
			return decisioncard.Card{}, fmt.Errorf("evaluate gate %s context %s: %w", plan.Decision, name, err)
		}
		contextSnapshot[name] = value
	}
	runID := runtimecorrelation.RunIDFromContext(ctx)
	if runID == "" {
		runID = asString(instance.Metadata["run_id"])
	}
	snapshot, err := decisioncard.FreezeSnapshot(plan.Decision, plan.Title, contextSnapshot, frozenOutcomes)
	if err != nil {
		return decisioncard.Card{}, err
	}
	executionMode, err := decisioncard.CausalExecutionMode(ctx)
	if err != nil {
		return decisioncard.Card{}, err
	}
	provenance, err := canonicaljson.FromGo(map[string]any{"source_event": activation.StartedByEvent, "flow_id": plan.FlowID, "stage": plan.Stage, "execution_mode": executionMode})
	if err != nil {
		return decisioncard.Card{}, fmt.Errorf("admit decision card provenance: %w", err)
	}
	instanceIdentity := StoredFlowInstance(pc.SemanticSource(), instance)
	anchorRoute := instanceIdentity.Route()
	if anchorRoute.InstancePath != strings.Trim(firstNonEmptyString(flowInstance, instance.WorkflowName, "root"), "/") {
		return decisioncard.Card{}, fmt.Errorf("gate anchor route disagrees with the workflow instance")
	}
	anchorFlowID := strings.TrimSpace(plan.FlowID)
	sourceRoute := events.RouteIdentity{EntityID: strings.TrimSpace(entityID)}
	if anchorFlowID != "" {
		sourceRoute.FlowID = anchorFlowID
		sourceRoute.FlowInstance = anchorRoute.InstancePath
	}
	anchorSource, err := runtimepinrouting.AdmitFlowExecutionRoutingSource(pc.SemanticSource(), anchorFlowID, sourceRoute)
	if err != nil {
		return decisioncard.Card{}, fmt.Errorf("admit stage-gate owner source: %w", err)
	}
	anchor, err := decisioncard.NewStageGateAnchor(decisioncard.StageGateAnchor{
		Route:  anchorRoute,
		FlowID: anchorFlowID, EntityID: strings.TrimSpace(entityID), Source: anchorSource, Stage: plan.Stage,
		StageActivationID: activation.ActivationID,
	})
	if err != nil {
		return decisioncard.Card{}, err
	}
	card := decisioncard.Card{
		CardID: activation.CardID, RunID: runID, Anchor: anchor,
		ExecutionMode: executionMode,
		Snapshot:      snapshot,
		BundleHash:    activation.BundleHash, WorkflowVersion: instance.WorkflowVersion,
		EffectiveCadence: pc.decisionCardCadence.Stamp(activation.OpenedAt),
		Provenance:       provenance,
		CreatedAt:        activation.OpenedAt,
	}
	return decisioncard.New(card)
}

func (pc *PipelineCoordinator) resolvedWorkflowGateOutcomes(plan runtimecontracts.WorkflowGatePlan) (map[string]runtimecontracts.WorkflowGateOutcomePlan, error) {
	frozenOutcomes := make(map[string]runtimecontracts.WorkflowGateOutcomePlan, len(plan.Outcomes))
	for verdict, outcome := range plan.Outcomes {
		if eventName := strings.TrimSpace(outcome.Emit.Event); eventName != "" {
			source := pc.SemanticSource()
			if source == nil {
				return nil, fmt.Errorf("freeze gate %s outcome %s event schema: semantic source is unavailable", plan.Decision, verdict)
			}
			resolution := semanticview.ResolveEventSchema(source, plan.FlowID, eventName)
			if !resolution.HasSchema {
				return nil, fmt.Errorf("freeze gate %s outcome %s event schema: event %s has no resolvable payload schema", plan.Decision, verdict, eventName)
			}
			if err := resolution.UnresolvedTypeError(); err != nil {
				return nil, fmt.Errorf("freeze gate %s outcome %s event schema: %w", plan.Decision, verdict, err)
			}
			outcome.Emit.Event = source.ResolveFlowEventReference(plan.FlowID, eventName)
			outcome.EmitSchema = cloneWorkflowGateSchema(resolution.Schema.Schema)
		}
		frozenOutcomes[verdict] = outcome
	}
	return frozenOutcomes, nil
}

func cloneWorkflowGateSchema(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		switch typed := value.(type) {
		case map[string]any:
			out[key] = cloneWorkflowGateSchema(typed)
		case []any:
			items := make([]any, len(typed))
			for i, item := range typed {
				if nested, ok := item.(map[string]any); ok {
					items[i] = cloneWorkflowGateSchema(nested)
				} else {
					items[i] = item
				}
			}
			out[key] = items
		default:
			out[key] = typed
		}
	}
	return out
}

func evalWorkflowGateContext(expression runtimecontracts.ExpressionValue, instance WorkflowInstance, source semanticview.Source, flowID string) (any, error) {
	if expression.HasLiteralValue() {
		return expression.Literal, nil
	}
	raw := strings.TrimSpace(expression.CEL)
	if raw == "" {
		raw = strings.TrimSpace(expression.Ref)
	}
	policy := map[string]any{}
	if source != nil {
		policy = workflowTimerPolicy(source, flowID)
	}
	return workflowexpr.EvalValueExpression(raw, workflowexpr.ValueContext{
		Entity: instance.Metadata,
		PlatformEntity: map[string]any{
			"entity_id": instance.StorageRef, "flow_instance": asString(instance.Metadata["flow_path"]), "current_state": instance.CurrentState,
		},
		Policy:   policy,
		Computed: payloadMap(instance.Metadata["computed"]),
	})
}
