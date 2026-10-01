package pipeline

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
)

// Committed join work retains its refusal semantics after a close winner makes
// the receiver terminal. This never authorizes a fresh publication or execution.
func validateAdmittedReceiverAvailability(ctx context.Context, source semanticview.Source, flowID string, evt events.Event, instance WorkflowInstance) error {
	err := NewDeliveryTargetAvailability(instance.CurrentState, instance.Status, !instance.TerminatedAt.IsZero()).Validate(source, flowID)
	var terminal *TerminalReceiverError
	if !errors.As(err, &terminal) {
		return err
	}
	route, admitted := workflowNodeDeliveryRoute(ctx)
	if !admitted {
		return err
	}
	join := false
	if len(route.Context.Joins) > 0 {
		if _, _, check := PrepareWorkflowJoinAdmission(source, evt.RunID(), string(evt.Type()), route, nil); check != nil {
			return check
		}
		for _, receipt := range route.Context.Joins {
			if receipt.Disposition == events.JoinAdmissionEarly {
				return failures.New(failures.ClassEarlyArrival, "join_not_armed", "runtime.pipeline", "receiver_preparation", map[string]any{"flow_id": flowID})
			}
		}
		join = true
	} else if isJoinLifecycleEvent(evt.Type()) {
		_, _, _, join, err = ResolveWorkflowJoinOccurrenceDeliveryTarget(source, evt)
		if err != nil {
			return err
		}
	}
	if join {
		return failures.New(failures.ClassStaleArrival, "join_stage_closed", "runtime.pipeline", "receiver_preparation", map[string]any{"flow_id": flowID, "stage": terminal.Stage})
	}
	return terminal
}

// WorkflowJoinAdmissionFence protects first publication against concurrent
// entry, close and supersession. Retained obligations do not acquire a new fence.
type WorkflowJoinAdmissionFence struct {
	Owner    flowidentity.RunScopedFlowInstance
	EntityID string
	Entry    timeridentity.StageEntryRef
	Arms     []WorkflowJoinAdmissionArm
}

type WorkflowJoinAdmissionArm struct {
	Receipt events.JoinAdmissionReceipt
	Status  joinruntime.Status
}

func (f WorkflowJoinAdmissionFence) Validate() error {
	if err := f.Owner.Validate(); err != nil {
		return err
	}
	if f.EntityID == "" || len(f.Arms) == 0 {
		return fmt.Errorf("join admission requires exact entity and observed declarations")
	}
	if !f.Entry.Empty() {
		if err := f.Entry.RequireOwner(f.Owner.RunID, f.Owner.Route.ScopeKey, f.Owner.Route.InstanceID, f.Owner.Route.InstancePath, f.EntityID, f.Entry.Stage); err != nil {
			return err
		}
	}
	receipts := make([]events.JoinAdmissionReceipt, len(f.Arms))
	for index, arm := range f.Arms {
		receipt := arm.Receipt
		receipts[index] = receipt
		if err := receipt.Validate(); err != nil {
			return err
		}
		if receipt.Disposition == events.JoinAdmissionBound && receipt.Ref.StageEntry() != f.Entry {
			return fmt.Errorf("join admission fence contradicts its observed entry")
		}
		if receipt.Disposition == events.JoinAdmissionBound {
			if arm.Status != joinruntime.StatusOpen && arm.Status != joinruntime.StatusClosed {
				return fmt.Errorf("join admission fence requires its observed arm status")
			}
		} else if arm.Status != "" || !f.Entry.Empty() && f.Entry.Stage == receipt.Ref.Stage() {
			return fmt.Errorf("early join admission contradicts its observed entry")
		}
	}
	return (events.DeliveryContext{Joins: receipts}).Validate()
}

// Unrelated writes and sibling arrivals do not change an immutable arm. Compare
// the actual entry and bindings under the publication lock, never a row revision.
func (f WorkflowJoinAdmissionFence) MatchesCurrent(stage string, bookkeeping, stateBuckets map[string]any) (bool, error) {
	if err := f.Validate(); err != nil {
		return false, err
	}
	entry, found, err := workflowlifecycle.LoadStageEntry(bookkeeping)
	if err != nil {
		return false, err
	}
	if !found {
		return false, fmt.Errorf("existing join receiver is missing lifecycle entry evidence")
	}
	if err := entry.RequireOwner(f.Owner.RunID, f.Owner.Route.ScopeKey, f.Owner.Route.InstanceID, f.Owner.Route.InstancePath, f.EntityID, stage); err != nil {
		return false, err
	}
	if entry != f.Entry {
		return false, nil
	}
	carrier, err := engine.StateCarrierFromPersisted(nil, bookkeeping, nil, stateBuckets)
	if err != nil {
		return false, err
	}
	activations, err := joinruntime.List(carrier.StateBuckets)
	if err != nil {
		return false, err
	}
	for _, arm := range f.Arms {
		receipt := arm.Receipt
		var actual *joinruntime.Activation
		for index := range activations {
			activation := &activations[index]
			if activation.JoinRef().StageEntry() != entry || !activation.JoinRef().Declaration().Equal(receipt.Ref.Declaration()) {
				continue
			}
			if actual != nil {
				return false, fmt.Errorf("join entry has competing immutable arms")
			}
			actual = activation
		}
		if receipt.Disposition == events.JoinAdmissionBound {
			if actual == nil || !actual.JoinRef().Equal(receipt.Ref) {
				return false, fmt.Errorf("join admission lost its immutable arm")
			}
			if actual.Status != arm.Status {
				return false, nil
			}
		} else if actual != nil {
			return false, nil
		}
	}
	return true, nil
}

func WorkflowJoinAdmissionPlans(source semanticview.Source, node identity.ExecutableNode, event string) []runtimecontracts.WorkflowJoinPlan {
	if source == nil {
		return nil
	}
	var plans []runtimecontracts.WorkflowJoinPlan
	resolved := semanticview.ResolveExecutableNodeSubscriptionHandler(source, node, event)
	canonical := event
	if bundle, ok := semanticview.Bundle(source); ok {
		canonical = bundle.ResolveExecutableNodeEventReference(node, event)
	}
	for _, plan := range source.WorkflowJoins() {
		if plan.Mode == runtimecontracts.WorkflowJoinModeArrival && plan.Node.Equal(node) && (plan.HandlerEvent == event || resolved.Matched && plan.HandlerEvent == resolved.HandlerEventKey || plan.UntilEvent == canonical) {
			plans = append(plans, plan)
		}
	}
	return plans
}

func WorkflowJoinAdmissionEvent(source semanticview.Source, route events.DeliveryRoute, event string) (string, error) {
	if route.ConnectClaim.Empty() {
		if node, isNode := route.Recipient.Node(); isNode {
			if resolved := workflowNodeDirectDeliveryHandlerResolution(source, node, event, route.Target.Route()); resolved.Matched {
				return resolved.HandlerEventKey, nil
			}
		}
		return event, nil
	}
	node, isNode := route.Recipient.Node()
	owner, handlerEvent, admitted := route.ConnectClaim.NodeHandlerOwner()
	if !isNode || !admitted || !owner.Equal(node) {
		return "", fmt.Errorf("join recipient has contradictory compiled handler authority")
	}
	return string(handlerEvent), nil
}

func WorkflowJoinAdmissionOwner(source semanticview.Source, runID string, target events.RouteIdentity) (flowidentity.RunScopedFlowInstance, error) {
	route, err := workflowInstanceRouteForExecution(source, target.FlowID, target.FlowInstance)
	if err != nil {
		return flowidentity.RunScopedFlowInstance{}, err
	}
	return flowidentity.NewRunScopedFlowInstance(runID, route)
}

// PrepareWorkflowJoinAdmission runs only after actual recipients are resolved.
// An existing receipt is immutable; absent receipts bind the observed entry.
func PrepareWorkflowJoinAdmission(source semanticview.Source, runID, event string, route events.DeliveryRoute, instance *WorkflowInstance) ([]events.JoinAdmissionReceipt, *WorkflowJoinAdmissionFence, error) {
	if err := route.Context.Validate(); err != nil {
		return nil, nil, err
	}
	node, isNode := route.Recipient.Node()
	if !isNode {
		return nil, nil, nil
	}
	var err error
	event, err = WorkflowJoinAdmissionEvent(source, route, event)
	if err != nil {
		return nil, nil, err
	}
	plans := WorkflowJoinAdmissionPlans(source, node, event)
	if len(plans) == 0 {
		if len(route.Context.Joins) > 0 {
			return nil, nil, fmt.Errorf("retained join receipt has no declaration for this message")
		}
		return nil, nil, nil
	}
	target := route.Target.Route()
	owner, err := WorkflowJoinAdmissionOwner(source, runID, target)
	if err != nil {
		return nil, nil, err
	}
	declarations := make([]timeridentity.JoinRef, len(plans))
	for index, plan := range plans {
		ref, err := timeridentity.NewJoinRef(plan.Node, plan.HandlerEvent, plan.Spec.Stage, plan.Spec.EffectiveID())
		if err != nil {
			return nil, nil, err
		}
		declarations[index] = ref
	}
	if len(route.Context.Joins) > 0 {
		if err := validateRetainedJoinAdmission(route.Context.Joins, declarations, owner, runID, target.EntityID); err != nil {
			return nil, nil, err
		}
		return append([]events.JoinAdmissionReceipt(nil), route.Context.Joins...), nil, nil
	}
	return prepareNewJoinAdmission(owner, runID, target, declarations, instance)
}

func validateRetainedJoinAdmission(receipts []events.JoinAdmissionReceipt, declarations []timeridentity.JoinRef, owner flowidentity.RunScopedFlowInstance, runID, entityID string) error {
	if len(receipts) != len(declarations) {
		return fmt.Errorf("retained message is missing required join admission evidence")
	}
	for _, receipt := range receipts {
		known := false
		for _, ref := range declarations {
			known = known || receipt.Ref.Declaration().Equal(ref)
		}
		if !known || receipt.Disposition == events.JoinAdmissionBound && receipt.Ref.StageEntry().RunID != runID {
			return fmt.Errorf("retained join receipt contradicts its exact message or run")
		}
		if receipt.Disposition == events.JoinAdmissionBound {
			route := owner.Route
			if err := receipt.Ref.StageEntry().RequireOwner(runID, route.ScopeKey, route.InstanceID, route.InstancePath, entityID, receipt.Ref.Stage()); err != nil {
				return err
			}
		}
	}
	return nil
}

func prepareNewJoinAdmission(owner flowidentity.RunScopedFlowInstance, runID string, target events.RouteIdentity, declarations []timeridentity.JoinRef, instance *WorkflowInstance) ([]events.JoinAdmissionReceipt, *WorkflowJoinAdmissionFence, error) {
	fence := &WorkflowJoinAdmissionFence{Owner: owner, EntityID: target.EntityID}
	entry, activations, err := joinAdmissionEntry(owner, runID, target.EntityID, instance)
	if err != nil {
		return nil, nil, err
	}
	receipts := make([]events.JoinAdmissionReceipt, len(declarations))
	fence.Arms = make([]WorkflowJoinAdmissionArm, len(declarations))
	for index, declaration := range declarations {
		receipt := events.JoinAdmissionReceipt{Ref: declaration, Disposition: events.JoinAdmissionEarly}
		var status joinruntime.Status
		for _, activation := range activations {
			if activation.JoinRef().Declaration().Equal(declaration) && activation.JoinRef().StageEntry() == entry {
				if receipt.Disposition == events.JoinAdmissionBound {
					return nil, nil, fmt.Errorf("join entry has competing immutable arms")
				}
				receipt = events.JoinAdmissionReceipt{Ref: activation.JoinRef(), Disposition: events.JoinAdmissionBound}
				status = activation.Status
			}
		}
		if !entry.Empty() && entry.Stage == declaration.Stage() && receipt.Disposition != events.JoinAdmissionBound {
			return nil, nil, fmt.Errorf("committed join stage entry is missing its arm")
		}
		receipts[index] = receipt
		fence.Arms[index] = WorkflowJoinAdmissionArm{Receipt: receipt, Status: status}
	}
	fence.Entry = entry
	if err := fence.Validate(); err != nil {
		return nil, nil, err
	}
	return receipts, fence, nil
}

func joinAdmissionEntry(owner flowidentity.RunScopedFlowInstance, runID, entityID string, instance *WorkflowInstance) (timeridentity.StageEntryRef, []joinruntime.Activation, error) {
	if instance == nil {
		return timeridentity.StageEntryRef{}, nil, nil
	}
	ownerRoute := owner.Route
	if _, err := requireWorkflowInstanceIdentity(ownerRoute, identity.EntityID(entityID), *instance); err != nil {
		return timeridentity.StageEntryRef{}, nil, err
	}
	entry, found, err := workflowlifecycle.LoadStageEntry(instance.Bookkeeping)
	if err != nil {
		return timeridentity.StageEntryRef{}, nil, err
	}
	if !found {
		return timeridentity.StageEntryRef{}, nil, fmt.Errorf("existing join receiver is missing lifecycle entry evidence")
	}
	if err := entry.RequireOwner(runID, ownerRoute.ScopeKey, ownerRoute.InstanceID, ownerRoute.InstancePath, entityID, instance.CurrentState); err != nil {
		return timeridentity.StageEntryRef{}, nil, err
	}
	carrier, err := workflowInstanceStateCarrier(*instance)
	if err != nil {
		return timeridentity.StageEntryRef{}, nil, err
	}
	activations, err := joinruntime.List(carrier.StateBuckets)
	return entry, activations, err
}
