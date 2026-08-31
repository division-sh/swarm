package engine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/values"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

// EvaluateFanOutOrdinal rehydrates one deferred ordinal through the same
// compiled emit, payload-shaping, lineage, and route owners as eager execution.
func (e *Executor) EvaluateFanOutOrdinal(ctx context.Context, intent fanoutobligation.Intent, trigger events.Event, item any, ordinal int) (EmitIntent, error) {
	if e == nil || e.deps.Source == nil {
		return EmitIntent{}, ErrMissingSemanticSource
	}
	if err := intent.Validate(); err != nil {
		return EmitIntent{}, err
	}
	if trigger.ID() != intent.Request.Capsule.Lineage.ParentEventID || trigger.RunID() != intent.Request.Capsule.Lineage.RunID {
		return EmitIntent{}, fmt.Errorf("fan-out trigger disagrees with immutable intent")
	}
	if ordinal < intent.Cursor || ordinal >= intent.Request.Cardinality {
		return EmitIntent{}, fmt.Errorf("fan-out ordinal %d is outside the claimed suffix [%d,%d)", ordinal, intent.Cursor, intent.Request.Cardinality)
	}
	capsule := intent.Request.Capsule
	node, err := identity.ParseExecutableNodeKey(capsule.NodeKey)
	if err != nil {
		return EmitIntent{}, fmt.Errorf("fan-out capsule node: %w", err)
	}
	handler, ok := e.deps.Source.ExecutableNodeEventHandler(node, capsule.HandlerEventKey)
	if !ok {
		return EmitIntent{}, fmt.Errorf("fan-out pinned handler %s on %s is unavailable", capsule.HandlerEventKey, node.Key())
	}
	plan, err := e.resolveFanOutPlan(intent.Request.PlanRef)
	if err != nil {
		return EmitIntent{}, err
	}

	payload, err := decodeFanOutPayload(trigger.Payload())
	if err != nil {
		return EmitIntent{}, err
	}
	carrier, err := StateCarrierFromPersisted(capsule.StateFields, capsule.StateBookkeeping, capsule.StateGates, nil)
	if err != nil {
		return EmitIntent{}, err
	}
	snapshot := StateSnapshot{
		EntityID: identity.NormalizeEntityID(capsule.EntityID), WorkflowName: capsule.ExecutionFlowID,
		CurrentState: capsule.CurrentState, StateCarrier: carrier,
	}
	state := ExecutionState{
		State: snapshot, Computed: cloneStringAnyMap(capsule.Computed), Accumulated: cloneStringAnyMap(capsule.Accumulated),
		FanOut: map[string]any{"item": item, "index": ordinal, "count": intent.Request.Cardinality},
		Join:   cloneStringAnyMap(capsule.Join), Loop: cloneStringAnyMap(capsule.Loop),
	}
	base := values.NewContext()
	base.Entity = values.Wrap(cloneStringAnyMap(capsule.Entity))
	base.PlatformEntity = values.Wrap(cloneStringAnyMap(capsule.PlatformEntity))
	base.FlowID = capsule.ExecutionFlowID
	base.Event = values.Wrap(trigger.ContextMap(capsule.CurrentState))
	base.Payload = values.Wrap(cloneStringAnyMap(payload))
	base.Policy = values.Wrap(policyDocumentToMap(e.deps.Source.ResolvedPolicyForFlow(capsule.ExecutionFlowID)))
	base.Computed = values.Wrap(cloneStringAnyMap(capsule.Computed))
	base.Accumulated = values.Wrap(cloneStringAnyMap(capsule.Accumulated))
	base.FanOut = values.Wrap(cloneStringAnyMap(state.FanOut))
	base.Join = values.Wrap(cloneStringAnyMap(capsule.Join))
	base.Loop = values.Wrap(cloneStringAnyMap(capsule.Loop))
	base.Metadata = values.Wrap(cloneStringAnyMap(capsule.StateFields))
	base.Gates = values.Wrap(boolMapToAnyMap(capsule.StateGates))

	if capsule.DeliveryRoute != nil {
		ctx = runtimedelivery.WithRoute(ctx, *capsule.DeliveryRoute)
	}
	frame := &executionFrame{
		ctx: ctx,
		req: ExecutionRequest{
			ExecutionID: intent.Request.Key.String(), EntityID: identity.NormalizeEntityID(capsule.EntityID), Node: node,
			ExecutionFlowID: identity.NormalizeFlowID(capsule.ExecutionFlowID), Route: capsule.Route,
			Event: trigger, ProducerSource: capsule.ProducerSource, HandlerEventKey: capsule.HandlerEventKey,
			Handler: handler, State: snapshot, ChainDepth: capsule.ChainDepth,
			FanOutPlans: e.deps.Source.FanOutPlansForHandler(node, capsule.HandlerEventKey),
		},
		base: base, state: state, payload: payload,
		emitLineage: &events.EventLineage{
			RunID: intent.Request.Key.RunID, ParentEventID: trigger.ID(), TaskID: trigger.TaskID(), ExecutionMode: trigger.ExecutionMode(),
		},
	}
	emitSpec := plan.Emit
	eventType := e.resolveDeclarativeEmitEventType(frame, emitSpec.EventType())
	if eventType == "" {
		return EmitIntent{}, fmt.Errorf("fan-out compiled plan has no emitted event")
	}
	emitSpec.Event = eventType
	transformed, err := emitFieldsPayload(base, state, emitSpec, workflowexpr.ValueExpressionOptions{ItemAlias: plan.ItemAlias})
	if err != nil {
		return EmitIntent{}, err
	}
	shaped, err := e.shapeEmitPayloadWithContext(ctx, frame, eventType, transformed)
	if err != nil {
		return EmitIntent{}, err
	}
	nextDepth, err := nextChainDepth(capsule.ChainDepth, e.MaxChainDepth())
	if err != nil {
		return EmitIntent{}, err
	}
	return e.newEmitIntent(frame, emitSpec, eventType, shaped, nextDepth)
}

func (e *Executor) resolveFanOutPlan(want runtimecontracts.FanOutPlanRef) (runtimecontracts.FanOutCompiledPlan, error) {
	plan, ok := e.deps.Source.FanOutPlanForElement(want.ElementRef)
	if !ok {
		return runtimecontracts.FanOutCompiledPlan{}, fmt.Errorf("fan-out pinned element %s/%s is unavailable", want.ElementRef.PackageKey, want.ElementRef.ElementID)
	}
	if plan.Ref != want {
		return runtimecontracts.FanOutCompiledPlan{}, fmt.Errorf("fan-out pinned plan disagrees with loaded bundle: persisted=%s/%s loaded=%s/%s", want.BundleHash, want.SemanticDigest, plan.Ref.BundleHash, plan.Ref.SemanticDigest)
	}
	return plan, nil
}

func decodeFanOutPayload(raw json.RawMessage) (map[string]any, error) {
	var payload map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes(raw, &payload); err != nil {
		return nil, fmt.Errorf("decode fan-out triggering payload: %w", err)
	}
	if payload == nil {
		payload = map[string]any{}
	}
	return payload, nil
}
