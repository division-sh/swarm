package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/google/uuid"
)

type workflowGateMutationPublisher interface {
	PublishInMutation(context.Context, events.Event) error
}

type decisionCardDirectMutationPublisher interface {
	PublishDirectInMutation(context.Context, events.Event, []string) error
}

type workflowGateIntent struct {
	activation gateruntime.Activation
	card       decisioncard.Card
}

func (pc *PipelineCoordinator) applyWorkflowGateIntents(ctx context.Context, entityID, currentStage, nextStage, sourceEvent string) error {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() || pc.SemanticSource() == nil {
		return nil
	}
	entityID = strings.TrimSpace(entityID)
	currentStage = strings.TrimSpace(currentStage)
	nextStage = strings.TrimSpace(nextStage)
	if entityID == "" || nextStage == "" || currentStage == nextStage {
		return nil
	}
	if _, ok := PipelineSQLTxFromContext(ctx); !ok {
		return pc.workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
			return pc.applyWorkflowGateIntents(txctx, entityID, currentStage, nextStage, sourceEvent)
		})
	}

	var create *workflowGateIntent
	superseded := []gateruntime.Activation{}
	now := time.Now().UTC()
	err := pc.workflowStore.MutateE(ctx, entityID, func(instance *WorkflowInstance) error {
		carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
		if err != nil {
			return fmt.Errorf("decode gate state: %w", err)
		}
		activations, err := gateruntime.List(carrier.StateBuckets)
		if err != nil {
			return fmt.Errorf("list gate activations: %w", err)
		}
		for _, activation := range activations {
			if activation.Stage != currentStage || activation.Stage == nextStage {
				continue
			}
			if activation.Status == gateruntime.StatusDecisionCommitted {
				return fmt.Errorf("stage %s cannot exit while decision card %s has a committed verdict awaiting its frozen route", currentStage, activation.CardID)
			}
			if activation.Supersede(firstNonEmptyString(sourceEvent, "stage_exited"), now) {
				if err := gateruntime.Store(carrier.StateBuckets, activation); err != nil {
					return err
				}
				superseded = append(superseded, activation)
			}
		}

		flowID, plan, ok := workflowGatePlanForInstance(pc, *instance, nextStage)
		if !ok {
			instance.StateBuckets = carrier.PersistedStateBuckets()
			return nil
		}
		if pc.decisionCards == nil {
			return fmt.Errorf("decision card store is required before entering gated stage %s", nextStage)
		}
		if existing, found, err := gateruntime.Load(carrier.StateBuckets, flowID, plan.Decision); err != nil {
			return err
		} else if found && existing.Stage == nextStage && (existing.Status == gateruntime.StatusOpen || existing.Status == gateruntime.StatusDecisionCommitted) {
			instance.StateBuckets = carrier.PersistedStateBuckets()
			return nil
		}

		runID := strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
		if runID == "" {
			runID = strings.TrimSpace(asString(instance.Metadata["run_id"]))
		}
		if runID == "" {
			return fmt.Errorf("run identity is required before entering gated stage %s", nextStage)
		}
		bundleHash := workflowGateBundleHash(ctx, pc)
		if bundleHash == "" {
			return fmt.Errorf("bundle identity is required before entering gated stage %s", nextStage)
		}
		enteredAt := instance.EnteredStageAt.UTC()
		if enteredAt.IsZero() {
			enteredAt = now
		}
		identity := StoredFlowInstance(pc.SemanticSource(), *instance)
		frozenOutcomes, err := pc.resolvedWorkflowGateOutcomes(plan)
		if err != nil {
			return err
		}
		routesJSON, err := gateruntime.FreezeRoutes(frozenOutcomes)
		if err != nil {
			return fmt.Errorf("freeze gate %s continuation routes: %w", plan.Decision, err)
		}
		activation, err := gateruntime.New(runID, identity.InstancePath, entityID, flowID, nextStage, plan.Decision, bundleHash, routesJSON, sourceEvent, enteredAt)
		if err != nil {
			return err
		}
		if err := gateruntime.Store(carrier.StateBuckets, activation); err != nil {
			return err
		}
		card, err := pc.buildWorkflowDecisionCard(ctx, entityID, *instance, identity.InstancePath, activation, plan, frozenOutcomes)
		if err != nil {
			return err
		}
		instance.StateBuckets = carrier.PersistedStateBuckets()
		create = &workflowGateIntent{activation: activation, card: card}
		return nil
	})
	if err != nil {
		return err
	}
	for _, activation := range superseded {
		if pc.decisionCards == nil {
			return fmt.Errorf("decision card store is required to supersede gate activation %s", activation.ActivationID)
		}
		card, err := pc.decisionCards.GetDecisionCard(ctx, activation.CardID)
		if err != nil {
			return fmt.Errorf("load decision card %s for supersession: %w", activation.CardID, err)
		}
		if err := pc.decisionCards.SupersedeDecisionCardsForStage(ctx, runtimecorrelation.RunIDFromContext(ctx), entityID, activation.ActivationID, activation.SupersededReason, now); err != nil {
			return err
		}
		if err := pc.publishWorkflowGateSuperseded(ctx, card, activation, now); err != nil {
			return err
		}
	}
	if create != nil {
		if err := pc.decisionCards.CreateDecisionCard(ctx, create.card); err != nil {
			return err
		}
	}
	return nil
}

func (pc *PipelineCoordinator) publishWorkflowGateSuperseded(ctx context.Context, card decisioncard.Card, activation gateruntime.Activation, now time.Time) error {
	publisher, ok := pc.bus.(workflowGateMutationPublisher)
	if !ok || publisher == nil {
		return fmt.Errorf("transactional event publisher is required to supersede decision card %s", activation.CardID)
	}
	payload, err := canonicaljson.Bytes(map[string]any{
		"card_id": activation.CardID, "anchor_kind": decisioncard.AnchorKindStageGate, "stage_activation_id": activation.ActivationID, "reason": activation.SupersededReason,
	})
	if err != nil {
		return err
	}
	anchor, err := card.Anchor.StageGate()
	if err != nil {
		return err
	}
	evt, err := events.NewRuntimeControlEvent(events.RuntimeEventInput{
		Facts: events.EventFacts{
			ID: uuid.NewString(), Type: events.EventType("mailbox.card_superseded"),
			Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "platform"},
			Payload:  payload, Envelope: events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, anchor.EntityID), anchor.FlowInstance),
			CreatedAt: now.UTC(), ExecutionMode: executionmode.Live,
		},
		RunID: card.RunID,
	})
	if err != nil {
		return err
	}
	if err := publisher.PublishInMutation(ctx, evt); err != nil {
		return fmt.Errorf("publish decision card superseded event: %w", err)
	}
	return nil
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
	if fact, ok := runtimecorrelation.BundleSourceFactFromContext(ctx); ok {
		if value := strings.TrimSpace(firstNonEmptyString(fact.BundleHash, fact.BundleFingerprint)); value != "" {
			return value
		}
	}
	if pc != nil {
		return strings.TrimSpace(pc.bundleHash)
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
	anchor, err := decisioncard.NewStageGateAnchor(decisioncard.StageGateAnchor{
		FlowInstance: strings.Trim(firstNonEmptyString(flowInstance, instance.WorkflowName, "root"), "/"),
		FlowID:       plan.FlowID, EntityID: strings.TrimSpace(entityID), Stage: plan.Stage,
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
