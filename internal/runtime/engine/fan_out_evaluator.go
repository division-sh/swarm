package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/core/values"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

// EvaluateFanOutOrdinal evaluates a single ordinal using the chunk preparation
// owner. Serving callers prepare once and evaluate the bounded chunk instead.
func (e *Executor) EvaluateFanOutOrdinal(ctx context.Context, intent fanoutobligation.Intent, trigger events.Event, item any, ordinal int) (EmitIntent, error) {
	prepared, err := e.PrepareFanOutEvaluation(ctx, intent, trigger)
	if err != nil {
		return EmitIntent{}, err
	}
	return prepared.EvaluateOrdinal(ctx, item, ordinal)
}

// FanOutEvaluation is private preparation for one pinned, bounded chunk, not
// claim or publication authority. It retains no live entity or evaluation result.
type FanOutEvaluation struct {
	executor *Executor
	intent   fanoutobligation.Intent
	frame    executionFrame
	emit     runtimecontracts.EmitSpec
	fields   map[fanOutFieldKey]preparedFanOutField
	envelope events.EventEnvelope
	routeErr error
}

type preparedFanOutField struct {
	program *workflowexpr.PreparedValueExpression
	err     error
	options workflowexpr.ValueExpressionOptions
}

type fanOutFieldKey struct {
	target, expression string
	kind               runtimecontracts.ExpressionKind
}

// PrepareFanOutEvaluation resolves immutable frame/schema/route and expression
// semantics once. Item failures still surface from EvaluateOrdinal, through the
// same shaping and failure classification path as single-ordinal execution.
func (e *Executor) PrepareFanOutEvaluation(ctx context.Context, intent fanoutobligation.Intent, trigger events.Event) (*FanOutEvaluation, error) {
	if e == nil || e.deps.Source == nil {
		return nil, ErrMissingSemanticSource
	}
	_, err := fanoutobligation.PrepareOrdinalEmission(intent, trigger, intent.Cursor)
	if err != nil {
		return nil, err
	}
	capsule := cloneFanOutCapsule(intent.Request.Capsule)
	intent.Request.Capsule = capsule
	node, err := identity.ParseExecutableNodeKey(capsule.NodeKey)
	if err != nil {
		return nil, fmt.Errorf("fan-out capsule node: %w", err)
	}
	handler, ok := e.deps.Source.ExecutableNodeEventHandler(node, capsule.HandlerEventKey)
	if !ok {
		return nil, fmt.Errorf("fan-out pinned handler %s on %s is unavailable", capsule.HandlerEventKey, node.Key())
	}
	plan, err := e.resolveFanOutPlan(intent.Request.PlanRef)
	if err != nil {
		return nil, err
	}

	payload, err := decodeFanOutPayload(trigger.Payload())
	if err != nil {
		return nil, err
	}
	carrier, err := StateCarrierFromPersisted(capsule.StateFields, capsule.StateBookkeeping, capsule.StateGates, nil)
	if err != nil {
		return nil, err
	}
	snapshot := StateSnapshot{
		EntityID: identity.NormalizeEntityID(capsule.EntityID), WorkflowName: capsule.ExecutionFlowID,
		CurrentState: capsule.CurrentState, StateCarrier: carrier,
	}
	state := ExecutionState{
		State: snapshot, Computed: cloneStringAnyMap(capsule.Computed), Accumulated: cloneStringAnyMap(capsule.Accumulated),
		FanOut: map[string]any{"count": intent.Request.Cardinality},
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

	var target events.DeliveryTargetOwnership
	if capsule.Receiver != nil {
		target = capsule.Receiver.Target
	}
	frame := &executionFrame{
		ctx:            ctx,
		deliveryTarget: runtimepinrouting.ClassifyExecutionReceiverTarget(target, capsule.Receiver != nil),
		req: ExecutionRequest{
			ExecutionID: intent.Request.Key.String(), EntityID: identity.NormalizeEntityID(capsule.EntityID), Node: node,
			ExecutionFlowID: identity.NormalizeFlowID(capsule.ExecutionFlowID), Route: capsule.Route,
			Event: trigger, ProducerSource: capsule.ProducerSource, HandlerEventKey: capsule.HandlerEventKey,
			Handler: handler, State: snapshot, ChainDepth: capsule.ChainDepth,
			FanOutPlans: e.deps.Source.FanOutPlansForHandler(node, capsule.HandlerEventKey),
		},
		base: base, state: state, payload: payload,
	}
	e.bindFrameExpressionSchemas(frame)
	emitSpec := plan.Emit
	eventType, err := admittedDeclarativeEmitEventType(frame, emitSpec.EventType())
	if err != nil {
		return nil, err
	}
	if eventType == "" {
		return nil, fmt.Errorf("fan-out compiled plan has no emitted event")
	}
	emitSpec.Event = eventType
	options := frameExpressionOptions(frame)
	itemType := plan.ItemType.Clone()
	options.ItemAlias = plan.ItemAlias
	options.ItemType = &itemType
	prepared := &FanOutEvaluation{executor: e, intent: intent, frame: *frame, emit: emitSpec, fields: make(map[fanOutFieldKey]preparedFanOutField)}
	resolution := semanticview.ResolveEventSchema(e.deps.Source, frame.req.ExecutionFlowID.String(), strings.TrimSpace(eventType))
	for target, expression := range emitSpec.Fields {
		target = strings.TrimSpace(target)
		if target == "" {
			continue
		}
		fieldOptions := emitFieldExpressionOptions(resolution, target, options)
		field := preparedFanOutField{options: fieldOptions}
		if expression.Kind == runtimecontracts.ExpressionKindCEL && !expression.IsZero() {
			field.program, field.err = workflowexpr.PrepareValueExpression(expression.CEL, fieldOptions)
		}
		prepared.fields[fanOutFieldKey{target, expression.CEL, expression.Kind}] = field
	}
	sourceRoute := emitSourceRoute(frame)
	route, routeErr := e.resolveEmitRoute(frame, eventType, events.EventEnvelope{EntityID: sourceRoute.EntityID, FlowInstance: sourceRoute.FlowInstance})
	prepared.envelope, prepared.routeErr = route.Envelope, routeErr
	return prepared, nil
}

func (p *FanOutEvaluation) EvaluateOrdinal(ctx context.Context, item any, ordinal int) (EmitIntent, error) {
	if p == nil || p.executor == nil {
		return EmitIntent{}, fmt.Errorf("fan-out evaluation preparation is required")
	}
	if ordinal >= p.intent.ChunkEndOrdinal() {
		return EmitIntent{}, fmt.Errorf("fan-out ordinal %d is outside the prepared chunk", ordinal)
	}
	emission, err := fanoutobligation.PrepareOrdinalEmission(p.intent, p.frame.req.Event, ordinal)
	if err != nil {
		return EmitIntent{}, err
	}
	// Downstream shapers receive independent maps, never the retained capsule.
	frame := p.frame
	frame.ctx, frame.fanOutEmission = ctx, &emission
	frame.base = p.frame.base.Clone()
	frame.payload = cloneStringAnyMap(p.frame.payload)
	state := p.frame.state
	carrier := state.State.StateCarrier
	state.State.StateCarrier = NewStateCarrierWithOwners(carrier.Fields, carrier.Bookkeeping, carrier.Control, carrier.Gates, carrier.StateBuckets)
	state.Computed, state.Accumulated = cloneStringAnyMap(state.Computed), cloneStringAnyMap(state.Accumulated)
	state.Join, state.Loop = cloneStringAnyMap(state.Join), cloneStringAnyMap(state.Loop)
	state.FanOut = map[string]any{"item": item, "index": ordinal, "count": p.intent.Request.Cardinality}
	frame.state, frame.req.State = state, state.State
	base := p.executor.currentContext(&frame)
	transformed, err := evaluateEmitFields(p.emit, func(target string, expression runtimecontracts.ExpressionValue) (any, bool, error) {
		field := p.fields[fanOutFieldKey{target, expression.CEL, expression.Kind}]
		if expression.Kind != runtimecontracts.ExpressionKindCEL || expression.IsZero() {
			return evalExpressionValue(base, state, expression, field.options)
		}
		if field.err != nil {
			return nil, false, field.err
		}
		result, err := field.program.Eval(workflowValueContext(base, state))
		return result.Value(), result.Present(), err
	})
	if err != nil {
		return EmitIntent{}, err
	}
	shaped, err := p.executor.shapeEmitPayloadWithContext(ctx, &frame, p.emit.Event, transformed)
	if err != nil {
		return EmitIntent{}, err
	}
	nextDepth, err := nextChainDepth(p.intent.Request.Capsule.ChainDepth, p.executor.MaxChainDepth())
	if err != nil {
		return EmitIntent{}, err
	}
	if p.routeErr != nil {
		return EmitIntent{}, p.routeErr
	}
	return p.executor.newEmitIntentWithEnvelope(&frame, p.emit.Event, shaped, nextDepth, p.envelope)
}

func cloneFanOutCapsule(c fanoutobligation.Capsule) fanoutobligation.Capsule {
	c.Entity, c.PlatformEntity = cloneStringAnyMap(c.Entity), cloneStringAnyMap(c.PlatformEntity)
	c.Computed, c.Accumulated = cloneStringAnyMap(c.Computed), cloneStringAnyMap(c.Accumulated)
	c.Join, c.Loop = cloneStringAnyMap(c.Join), cloneStringAnyMap(c.Loop)
	c.StateFields, c.StateBookkeeping = cloneStringAnyMap(c.StateFields), cloneStringAnyMap(c.StateBookkeeping)
	c.StateGates = mapsClone(c.StateGates)
	if c.Receiver != nil {
		receiver := *c.Receiver
		c.Receiver = &receiver
	}
	return c
}

func (e *Executor) resolveFanOutPlan(want runtimecontracts.FanOutPlanRef) (runtimecontracts.FanOutCompiledPlan, error) {
	plan, ok := e.deps.Source.FanOutPlanForElement(want.ElementRef)
	if !ok {
		identity, _ := want.ElementRef.DeclarationIdentity()
		return runtimecontracts.FanOutCompiledPlan{}, fmt.Errorf("fan-out pinned declaration %s is unavailable", identity.Key())
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
