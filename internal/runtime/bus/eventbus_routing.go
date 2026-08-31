package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedeliverycontinuation "github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

var errAuthoritativeDeliveryIncomplete = errors.New("authoritative delivery incomplete")

type internalSubscriptionHandle struct {
	mu           sync.RWMutex
	retireMu     sync.Mutex
	bus          *EventBus
	lifecycleCtx context.Context
	subscriberID string
	eventTypes   []events.EventType
	ch           chan *LocalDelivery
	active       bool
	retiring     chan struct{}
	receiverDone chan struct{}
	ready        chan struct{}
	retireOnce   sync.Once
	completeOnce sync.Once
	readyOnce    sync.Once
	restart      bool
	retained     []*LocalDelivery
}

func newInternalSubscriptionHandle(ctx context.Context, bus *EventBus, subscriberID string, eventTypes []events.EventType) *internalSubscriptionHandle {
	return &internalSubscriptionHandle{
		bus:          bus,
		lifecycleCtx: ctx,
		subscriberID: strings.TrimSpace(subscriberID),
		eventTypes:   append([]events.EventType(nil), eventTypes...),
		ch:           make(chan *LocalDelivery, 128),
		active:       true,
		retiring:     make(chan struct{}),
		receiverDone: make(chan struct{}),
		ready:        make(chan struct{}),
	}
}

func (h *internalSubscriptionHandle) Deliveries() <-chan *LocalDelivery {
	if h == nil {
		return nil
	}
	return h.ch
}

func (h *internalSubscriptionHandle) Retiring() <-chan struct{} {
	if h == nil {
		return nil
	}
	return h.retiring
}

func (h *internalSubscriptionHandle) MarkReady() {
	if h == nil {
		return
	}
	h.readyOnce.Do(func() { close(h.ready) })
	if h.bus != nil {
		h.bus.notifyInternalSubscriptionChanged()
	}
}

func (h *internalSubscriptionHandle) Complete(restart bool) error {
	if h == nil {
		return nil
	}
	completed := false
	h.completeOnce.Do(func() {
		completed = true
		h.mu.Lock()
		h.restart = restart
		h.mu.Unlock()
		close(h.receiverDone)
	})
	if !completed {
		return errors.New("internal subscription generation is already complete")
	}
	if h.bus == nil {
		return nil
	}
	return h.bus.completeInternalSubscription(h)
}

func (h *internalSubscriptionHandle) deactivate() {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.active = false
	h.retireOnce.Do(func() { close(h.retiring) })
	h.mu.Unlock()
}

func (h *internalSubscriptionHandle) wantsRestart() bool {
	if h == nil {
		return false
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.restart
}

func (h *internalSubscriptionHandle) restartContext() context.Context {
	if h == nil {
		return nil
	}
	return h.lifecycleCtx
}

func (h *internalSubscriptionHandle) send(ctx context.Context, evt events.Event, handoff events.DeliveryRoute, continuation worklifetime.DeliveryContinuation) (agentRouteSendResult, error) {
	if h == nil || h.bus == nil {
		return agentRouteSendInactive, returnDeliveryContinuation(ctx, continuation)
	}
	h.mu.RLock()
	active := h.active && h.ch != nil
	h.mu.RUnlock()
	if !active {
		return agentRouteSendInactive, returnDeliveryContinuation(ctx, continuation)
	}
	owner := h.bus.workOwnerForContext(ctx)
	if owner == nil {
		return agentRouteSendInactive, returnDeliveryContinuation(ctx, continuation)
	}
	deliveryCtx, closeDeliveryCtx, err := h.bus.receiverRouteContext(ctx, evt, handoff)
	if err != nil {
		return agentRouteSendInactive, errors.Join(err, returnDeliveryContinuation(ctx, continuation))
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if !h.active || h.ch == nil {
		closeDeliveryCtx()
		return agentRouteSendInactive, returnDeliveryContinuation(ctx, continuation)
	}
	delivery, err := owner.NewRoutedEventDelivery(deliveryCtx, evt, handoff)
	if err != nil {
		closeDeliveryCtx()
		return agentRouteSendInactive, errors.Join(err, returnDeliveryContinuation(ctx, continuation))
	}
	if err := delivery.OnComplete(closeDeliveryCtx); err != nil {
		closeDeliveryCtx()
		return agentRouteSendInactive, errors.Join(err, delivery.Complete())
	}
	if continuation != nil {
		if err := delivery.AttachContinuation(continuation); err != nil {
			return agentRouteSendInactive, errors.Join(err, delivery.Complete())
		}
	}
	if err := trackLocalDeliveryCompletion(deliveryCtx, delivery); err != nil {
		return agentRouteSendInactive, errors.Join(err, delivery.Complete())
	}
	timer := time.NewTimer(deliverySendTimeout)
	defer timer.Stop()
	select {
	case h.ch <- delivery:
		return agentRouteSendDelivered, nil
	case <-ctx.Done():
		return agentRouteSendContextDone, delivery.Complete()
	case <-timer.C:
		return agentRouteSendTimedOut, delivery.Complete()
	}
}

func (h *internalSubscriptionHandle) retireAndWait(ctx context.Context, store EventStore) error {
	if h == nil {
		return nil
	}
	h.retireMu.Lock()
	defer h.retireMu.Unlock()
	h.deactivate()
	select {
	case <-h.receiverDone:
	case <-ctx.Done():
		return fmt.Errorf("wait for internal subscriber %s receiver: %w", h.subscriberID, ctx.Err())
	}
	for {
		if len(h.retained) == 0 {
			select {
			case delivery := <-h.ch:
				if delivery != nil {
					h.retained = append(h.retained, delivery)
				}
			default:
				return nil
			}
		}
		if len(h.retained) == 0 {
			continue
		}
		if err := settleBufferedLocalDelivery(ctx, store, h.retained[0]); err != nil {
			return err
		}
		h.retained = h.retained[1:]
	}
}

type agentRouteHandle struct {
	mu       sync.RWMutex
	retireMu sync.Mutex
	token    runtimeeffects.LifecycleToken
	ch       chan *LocalDelivery
	owner    *worklifetime.RouteOccurrence
	active   bool
	retained []*LocalDelivery
	bus      *EventBus
}

func newAgentRouteHandle(bus *EventBus, token runtimeeffects.LifecycleToken, ch chan *LocalDelivery, owner *worklifetime.RouteOccurrence) *agentRouteHandle {
	return &agentRouteHandle{bus: bus, token: token, ch: ch, owner: owner, active: true}
}

func (r *agentRouteHandle) deactivate() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.active = false
	r.mu.Unlock()
}

type agentRouteSendResult uint8

const (
	agentRouteSendDelivered agentRouteSendResult = iota
	agentRouteSendInactive
	agentRouteSendTimedOut
	agentRouteSendContextDone
)

func (r *agentRouteHandle) send(ctx context.Context, evt events.Event, handoff events.DeliveryRoute, continuation worklifetime.DeliveryContinuation) (agentRouteSendResult, error) {
	if r == nil || r.bus == nil {
		return agentRouteSendInactive, returnDeliveryContinuation(ctx, continuation)
	}
	r.mu.RLock()
	active := r.active && r.ch != nil
	r.mu.RUnlock()
	if !active {
		return agentRouteSendInactive, returnDeliveryContinuation(ctx, continuation)
	}
	deliveryCtx, closeDeliveryCtx, err := r.bus.receiverRouteContext(ctx, evt, handoff)
	if err != nil {
		return agentRouteSendInactive, errors.Join(err, returnDeliveryContinuation(ctx, continuation))
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.active || r.ch == nil {
		closeDeliveryCtx()
		return agentRouteSendInactive, returnDeliveryContinuation(ctx, continuation)
	}
	delivery, err := r.owner.NewRoutedEventDelivery(deliveryCtx, evt, handoff)
	if err != nil {
		closeDeliveryCtx()
		return agentRouteSendInactive, errors.Join(err, returnDeliveryContinuation(ctx, continuation))
	}
	if err := delivery.OnComplete(closeDeliveryCtx); err != nil {
		closeDeliveryCtx()
		return agentRouteSendInactive, errors.Join(err, delivery.Complete())
	}
	if continuation != nil {
		if err := delivery.AttachContinuation(continuation); err != nil {
			return agentRouteSendInactive, errors.Join(err, delivery.Complete())
		}
	}
	if err := trackLocalDeliveryCompletion(deliveryCtx, delivery); err != nil {
		return agentRouteSendInactive, errors.Join(err, delivery.Complete())
	}
	timer := time.NewTimer(deliverySendTimeout)
	defer timer.Stop()
	select {
	case r.ch <- delivery:
		return agentRouteSendDelivered, nil
	case <-ctx.Done():
		return agentRouteSendContextDone, delivery.Complete()
	case <-timer.C:
		return agentRouteSendTimedOut, delivery.Complete()
	}
}

func (r *agentRouteHandle) retireAndWait(ctx context.Context, store EventStore) error {
	if r == nil || r.owner == nil {
		return nil
	}
	r.retireMu.Lock()
	defer r.retireMu.Unlock()
	r.deactivate()
	for {
		if len(r.retained) == 0 {
			select {
			case delivery := <-r.ch:
				if delivery != nil {
					r.retained = append(r.retained, delivery)
				}
			default:
				return r.owner.RetireAndWait(ctx)
			}
		}
		if len(r.retained) == 0 {
			continue
		}
		if err := settleBufferedLocalDelivery(ctx, store, r.retained[0]); err != nil {
			return err
		}
		r.retained = r.retained[1:]
	}
}

func settleBufferedLocalDelivery(ctx context.Context, store EventStore, delivery *LocalDelivery) error {
	if delivery == nil {
		return nil
	}
	return delivery.Complete()
}

func (eb *EventBus) activeAgentDescriptors(ctx context.Context) (map[agentidentity.Identity]ActiveAgentDescriptor, bool, error) {
	inbound, ok := runtimecorrelation.InboundEventFromContext(ctx)
	if !ok || strings.TrimSpace(inbound.RunID()) == "" {
		return nil, false, errors.New("active agent descriptors require exact inbound run identity")
	}
	runID := inbound.RunID()
	ephemeral := eb.runtimeActiveAgentDescriptors()
	lister := eb.durable.ActiveAgents
	if lister == nil {
		if len(ephemeral) > 0 {
			return ephemeral, true, nil
		}
		return nil, false, nil
	}
	descriptors, err := lister.ListActiveAgentDescriptors(ctx, runID)
	if err != nil {
		return nil, true, err
	}
	set := make(map[agentidentity.Identity]ActiveAgentDescriptor, len(descriptors)+len(ephemeral))
	for _, descriptor := range descriptors {
		descriptor = descriptor.Normalized()
		if err := descriptor.Identity.Validate(); err != nil {
			continue
		}
		if descriptor.Identity.RunID != runID {
			return nil, true, fmt.Errorf("active agent descriptor %s escaped selected run %s", descriptor.Identity.AgentID(), runID)
		}
		set[descriptor.Identity] = descriptor
	}
	for identity, descriptor := range ephemeral {
		if identity.RunID == runID {
			set[identity] = descriptor
		}
	}
	return set, true, nil
}

func (eb *EventBus) PinRoutingDescriptors(ctx context.Context) ([]runtimepinrouting.Descriptor, error) {
	descriptors, _, err := eb.activeTargetDescriptors(ctx)
	if err != nil {
		return nil, err
	}
	agents, _, err := eb.activeAgentDescriptors(ctx)
	if err != nil {
		return nil, err
	}
	for _, descriptor := range activeTargetDescriptorsFromAgents(agents) {
		descriptors = appendActiveTargetDescriptor(descriptors, descriptor)
	}
	out := make([]runtimepinrouting.Descriptor, 0, len(descriptors))
	for _, descriptor := range descriptors {
		descriptor = descriptor.Normalized()
		if descriptor.FlowInstance == "" && descriptor.EntityID == "" {
			continue
		}
		out = append(out, runtimepinrouting.Descriptor{
			ID:            descriptor.ID,
			EntityID:      descriptor.EntityID,
			FlowInstance:  descriptor.FlowInstance,
			AddressFields: normalizeDescriptorAddressFields(descriptor.AddressFields),
		})
	}
	return out, nil
}

func (eb *EventBus) activeTargetDescriptors(ctx context.Context) ([]ActiveTargetDescriptor, bool, error) {
	inbound, ok := runtimecorrelation.InboundEventFromContext(ctx)
	if !ok || strings.TrimSpace(inbound.RunID()) == "" {
		return nil, false, errors.New("active target descriptors require exact inbound run identity")
	}
	runID := inbound.RunID()
	out := []ActiveTargetDescriptor{}
	available := false
	targetOwners := eb.durable.TargetOwners
	if targetOwners != nil {
		available = true
		owners, err := targetOwners.ListSelectedRunTargetOwners(ctx, runID)
		if err != nil {
			return nil, true, err
		}
		for _, owner := range owners {
			out = appendActiveTargetDescriptor(out, owner)
		}
	}
	lister := eb.durable.ActiveFlows
	if lister == nil {
		return out, available, nil
	}
	available = true
	flowDescriptors, err := eb.activeFlowInstanceDescriptorsForSemanticSource(ctx, lister, runID)
	if err != nil {
		return nil, true, err
	}
	for _, descriptor := range flowDescriptors {
		if descriptor.RunID == runID {
			out = appendActiveFlowInstanceTargetDescriptors(out, []ActiveFlowInstanceDescriptor{descriptor})
		}
	}
	return out, available, nil
}

func activeTargetDescriptorsFromAgents(descriptors map[agentidentity.Identity]ActiveAgentDescriptor) []ActiveTargetDescriptor {
	if len(descriptors) == 0 {
		return nil
	}
	out := make([]ActiveTargetDescriptor, 0, len(descriptors))
	for _, descriptor := range descriptors {
		target := descriptor.TargetDescriptor()
		if target.EntityID == "" {
			continue
		}
		out = appendActiveTargetDescriptor(out, target)
	}
	return out
}

func appendActiveFlowInstanceTargetDescriptors(out []ActiveTargetDescriptor, descriptors []ActiveFlowInstanceDescriptor) []ActiveTargetDescriptor {
	if len(descriptors) == 0 {
		return out
	}
	for _, descriptor := range descriptors {
		out = appendActiveTargetDescriptor(out, descriptor.TargetDescriptor())
	}
	return out
}

func appendActiveTargetDescriptor(out []ActiveTargetDescriptor, descriptor ActiveTargetDescriptor) []ActiveTargetDescriptor {
	descriptor = descriptor.Normalized()
	if descriptor.FlowInstance == "" && descriptor.EntityID == "" {
		return out
	}
	for _, existing := range out {
		existing = existing.Normalized()
		if existing.ID == descriptor.ID && existing.EntityID == descriptor.EntityID && existing.FlowInstance == descriptor.FlowInstance {
			return out
		}
	}
	return append(out, descriptor)
}

// RegisterRuntimeActiveAgentDescriptor adds in-memory active-agent metadata for
// handlers that are intentionally not persisted as ordinary current-runtime
// agents. Delivery planning still uses the normal authoritative recipient
// policy; this only supplies runtime-local descriptor evidence to that policy.
func (eb *EventBus) RegisterRuntimeActiveAgentDescriptor(descriptor ActiveAgentDescriptor) {
	if eb == nil {
		return
	}
	descriptor = descriptor.Normalized()
	if err := descriptor.Identity.Validate(); err != nil {
		return
	}
	eb.mu.Lock()
	defer eb.mu.Unlock()
	if eb.runtimeAgentDescriptors == nil {
		eb.runtimeAgentDescriptors = make(map[agentidentity.Identity]ActiveAgentDescriptor)
	}
	eb.runtimeAgentDescriptors[descriptor.Identity] = descriptor
}

func (eb *EventBus) runtimeActiveAgentDescriptors() map[agentidentity.Identity]ActiveAgentDescriptor {
	if eb == nil {
		return nil
	}
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	if len(eb.runtimeAgentDescriptors) == 0 {
		return nil
	}
	out := make(map[agentidentity.Identity]ActiveAgentDescriptor, len(eb.runtimeAgentDescriptors))
	for identity, descriptor := range eb.runtimeAgentDescriptors {
		out[identity] = descriptor
	}
	return out
}

func (eb *EventBus) resolveRoutedSubscribersForEvent(evt events.Event) []Subscriber {
	if eb == nil {
		return nil
	}
	eventKeys := routedEventKeysForPlan(evt)
	if len(eventKeys) == 0 {
		return nil
	}
	eb.mu.RLock()
	table := eb.routeTable
	eb.mu.RUnlock()
	out := make([]Subscriber, 0, 8)
	if table != nil {
		for _, eventType := range eventKeys {
			out = append(out, table.ResolveForRun(evt.RunID(), eventType)...)
		}
	}
	return dedupeSubscribers(out)
}

func (eb *EventBus) deliverToRecipientsWithRoutes(ctx context.Context, evt events.Event, recipientIDs []string, deliveryRoutes []events.DeliveryRoute) error {
	deliveryRoutes = events.NormalizeDeliveryRoutes(deliveryRoutes)
	if err := events.ValidateDeliveryRoutes(deliveryRoutes); err != nil {
		return err
	}
	liveRecipients := deliveryRouteLiveRecipients(deliveryRoutes)
	routed := make(map[string]struct{}, len(liveRecipients))
	for _, recipient := range liveRecipients {
		routed[recipient.subscriberID()] = struct{}{}
	}
	for _, recipientID := range uniqueStrings(recipientIDs) {
		if _, ok := routed[recipientID]; ok {
			continue
		}
		eb.mu.RLock()
		internal := eb.internalHandles[recipientID]
		eb.mu.RUnlock()
		if internal == nil {
			return fmt.Errorf("event %s (%s) recipient %q has no exact delivery route", evt.ID(), evt.Type(), recipientID)
		}
		liveRecipients = append(liveRecipients, RoutePlanLiveRecipient{
			InternalID:        recipientID,
			PersistAsDelivery: false,
			liveAuthority:     liveRecipientAuthorityIdentity,
		})
	}
	return eb.deliverLiveRecipientsWithRoutes(ctx, evt, liveRecipients, deliveryRoutes)
}

func (eb *EventBus) deliverRoutePlanWithRoutes(ctx context.Context, evt events.Event, routePlan RoutePlan) error {
	if err := routePlan.ValidatePersistentDeliveries(); err != nil {
		return err
	}
	return eb.deliverLiveRecipientsWithRoutes(ctx, evt, routePlan.LiveRecipients, routePlan.liveDispatchDeliveryRoutes())
}

type liveDeliveryDispatch struct {
	expected  []deliveryRouteTargetKey
	delivered []deliveryRouteTargetKey
	missing   []deliveryRouteTargetKey
	timedOut  []deliveryRouteTargetKey
	cause     error
}

func (d liveDeliveryDispatch) complete() bool {
	return d.cause == nil && len(d.missing) == 0 && len(d.timedOut) == 0
}

func (eb *EventBus) deliverLiveRecipientsWithRoutes(ctx context.Context, evt events.Event, liveRecipients []RoutePlanLiveRecipient, deliveryRoutes []events.DeliveryRoute) error {
	dispatch, err := eb.dispatchLiveRecipientsWithRoutes(ctx, evt, liveRecipients, deliveryRoutes)
	if err != nil || dispatch.complete() {
		return err
	}
	return eb.logAuthoritativeDeliveryIncomplete(
		ctx,
		evt,
		dispatch.expected,
		dispatch.delivered,
		dispatch.missing,
		dispatch.timedOut,
		dispatch.cause,
	)
}

func (eb *EventBus) dispatchLiveRecipientsWithRoutes(ctx context.Context, evt events.Event, liveRecipients []RoutePlanLiveRecipient, deliveryRoutes []events.DeliveryRoute) (liveDeliveryDispatch, error) {
	if err := events.ValidateDeliveryRoutes(deliveryRoutes); err != nil {
		return liveDeliveryDispatch{}, err
	}
	liveRecipients = normalizeRoutePlanLiveRecipients(liveRecipients)
	recipientIDs := make([]string, 0, len(liveRecipients))
	for _, recipient := range liveRecipients {
		recipientIDs = append(recipientIDs, recipient.subscriberID())
	}
	expected := authoritativeDeliveryTargetKeys(liveRecipients, deliveryRoutes)
	for _, recipient := range liveRecipients {
		if !recipient.isInternal() {
			continue
		}
		if node, err := runtimeidentity.ParseExecutableNodeKey(recipient.subscriberID()); err == nil {
			expected = append(expected, deliveryRouteTargetKey{recipient: events.MustNodeDeliveryRecipient(node)})
		}
	}
	expected = uniqueDeliveryTargetKeys(expected)
	dispatchRecipients := uniqueStrings(append(append([]string(nil), recipientIDs...), deliveryTargetKeySubscriberIDs(expected)...))
	if len(dispatchRecipients) == 0 {
		return liveDeliveryDispatch{}, nil
	}
	expectedSet := make(map[deliveryRouteTargetKey]struct{}, len(expected))
	for _, recipient := range expected {
		expectedSet[recipient] = struct{}{}
	}
	routesByRecipient := deliveryRoutesBySubscriber(deliveryRoutes)
	recipients := eb.snapshotRoutePlanRecipientChans(dispatchRecipients, liveRecipients)
	delivered := make([]deliveryRouteTargetKey, 0, len(recipients))
	seen := make(map[deliveryRouteTargetKey]struct{}, len(recipients))
	for _, recipient := range recipients {
		seen[recipient.deliveryRouteTargetKey()] = struct{}{}
	}
	missing := make([]deliveryRouteTargetKey, 0, len(expected))
	for _, recipient := range expected {
		if _, ok := seen[recipient]; !ok {
			missing = append(missing, recipient)
		}
	}
	timedOut := make([]deliveryRouteTargetKey, 0, len(recipients))
	for _, recipient := range recipients {
		targetKey := recipient.deliveryRouteTargetKey()
		routes := routesByRecipient[targetKey]
		if recipient.isWorkflowRuntimeInternalCarrier() {
			// The workflow-runtime subscription is an in-memory carrier for the
			// concrete node delivery routes. Its placeholder route must never
			// hide the target or route-scoped context owned by those routes.
			if nodeRoutes := workflowRuntimeInternalCarrierRoutes(deliveryRoutes); len(nodeRoutes) > 0 {
				routes = nodeRoutes
			}
		}
		if len(routes) == 0 {
			route := events.DeliveryRoute{}
			if eb.ephemeral && recipient.kind == inMemorySubscriberAgent && eb.DeliveryContinuationOwner() != nil {
				route = events.DeliveryRoute{
					Recipient:     events.MustAgentDeliveryRecipient(recipient.subscriberID()),
					AgentIdentity: recipient.identity,
				}
			}
			routes = []events.DeliveryRoute{route}
		}
		for _, route := range routes {
			deliverEvent, err := projectEventForDeliveryRoute(evt, route)
			if err != nil {
				return liveDeliveryDispatch{}, err
			}
			var continuation worklifetime.DeliveryContinuation
			if !route.Recipient.Empty() {
				deliveryID, err := runtimedelivery.DeliveryID(evt.ID(), route)
				if err != nil {
					return liveDeliveryDispatch{}, err
				}
				owner := eb.DeliveryContinuationOwner()
				if owner == nil {
					if !eb.ephemeral {
						return liveDeliveryDispatch{}, errors.New("exact delivery continuation owner is required")
					}
				} else {
					continuation, err = owner.Acquire(deliveryID)
					if err != nil {
						return liveDeliveryDispatch{}, err
					}
				}
			}
			sendResult, sendErr := recipient.send(ctx, deliverEvent.Event(), route, continuation)
			if sendErr != nil {
				return liveDeliveryDispatch{}, fmt.Errorf("settle delivery carrier for %s: %w", recipient.subscriberID(), sendErr)
			}
			switch sendResult {
			case agentRouteSendDelivered:
				delivered = append(delivered, targetKey)
			case agentRouteSendInactive:
				if _, required := expectedSet[targetKey]; required {
					missing = append(missing, targetKey)
				}
			case agentRouteSendContextDone:
				remaining := make([]deliveryRouteTargetKey, 0, len(expected))
				deliveredSet := deliveryTargetKeySet(delivered)
				for _, recipient := range expected {
					if _, found := deliveredSet[recipient]; !found {
						remaining = append(remaining, recipient)
					}
				}
				return liveDeliveryDispatch{
					expected: expected, delivered: delivered,
					missing: uniqueDeliveryTargetKeys(missing), timedOut: remaining, cause: ctx.Err(),
				}, nil
			case agentRouteSendTimedOut:
				if _, required := expectedSet[targetKey]; required {
					timedOut = append(timedOut, targetKey)
				}
				eb.logRuntime(ctx, "warn", "Event delivery to a recipient timed out", "eventbus", "delivery_timeout", evt.ID(), string(evt.Type()), recipient.subscriberID(), evt.EntityID(), "", nil, map[string]any{
					"timeout_ms": int(deliverySendTimeout / time.Millisecond),
					"target":     targetKey.description(),
				}, targetDeliveryTimeoutFailure(evt, targetKey, deliverySendTimeout), 0)
			}
		}
	}
	missing = uniqueDeliveryTargetKeys(missing)
	timedOut = uniqueDeliveryTargetKeys(timedOut)
	return liveDeliveryDispatch{
		expected: expected, delivered: delivered, missing: missing, timedOut: timedOut,
	}, nil
}

// DispatchDeliveryContinuation re-enters one exact persisted route. It is used
// only by the normal generation coordinator after a selected-store scan.
func (eb *EventBus) DispatchDeliveryContinuation(ctx context.Context, evt events.Event, route events.DeliveryRoute) (result runtimedeliverycontinuation.DispatchResult) {
	if eb == nil {
		return runtimedeliverycontinuation.Fatal(errors.New("event bus is required"))
	}
	if _, err := route.Identity(); err != nil {
		return runtimedeliverycontinuation.Fatal(err)
	}
	var err error
	ctx, err = eb.admitBundleSourceFact(ctx)
	if err != nil {
		return runtimedeliverycontinuation.Fatal(err)
	}
	var standingLease *worklifetime.Lease
	ctx, standingLease, err = eb.bindClaimedRunWork(ctx, evt)
	if err != nil {
		return runtimedeliverycontinuation.Fatal(err)
	}
	if standingLease != nil {
		defer func() {
			if closeErr := standingLease.Done(); closeErr != nil {
				result = runtimedeliverycontinuation.Fatal(errors.Join(result.Failure(), closeErr))
			}
		}()
	}
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return runtimedeliverycontinuation.Fatal(err)
	}
	ctx = events.WithDeliveryContext(ctx, evt.DeliveryContext())
	if runID := strings.TrimSpace(evt.RunID()); runID != "" {
		ctx = runtimecorrelation.WithRunID(ctx, runID)
	}
	ctx, err = eb.withAuthorActivityEventDescriptor(ctx, evt)
	if err != nil {
		return runtimedeliverycontinuation.Fatal(err)
	}
	if route.Recipient.IsAgent() {
		if err := eb.finalizeCommittedAgentReadiness(ctx, evt, []events.DeliveryRoute{route}); err != nil {
			return runtimedeliverycontinuation.Fatal(err)
		}
	}
	projection, err := eb.receiverProjection(ctx, route.Context)
	if err != nil {
		return runtimedeliverycontinuation.Fatal(err)
	}
	receiverCtx, closeReceiver, err := eb.beginReceiverDispatch(ctx, projection, evt)
	if err != nil {
		return runtimedeliverycontinuation.Fatal(err)
	}
	defer func() {
		if closeErr := closeReceiver(); closeErr != nil {
			result = runtimedeliverycontinuation.Fatal(errors.Join(result.Failure(), closeErr))
		}
	}()
	ctx = receiverCtx.Context
	if route.Recipient.IsNode() {
		interception, err := eb.runInterceptorsForDeliveryRoutes(ctx, evt, []events.DeliveryRoute{route})
		if err != nil {
			return runtimedeliverycontinuation.Fatal(err)
		}
		if len(interception.Deferred) > 0 {
			if err := (engineDispatcher{bus: eb}).dispatchCommittedInterceptorPublications(ctx, interception.Deferred); err != nil {
				return runtimedeliverycontinuation.Fatal(err)
			}
		}
		if _, retry := interception.Outcome.RetryRelease(); retry {
			return runtimedeliverycontinuation.Fatal(errors.New("delivery continuation route requested event-level retry release"))
		}
		if _, settled := interception.Outcome.Disposition(); settled {
			return runtimedeliverycontinuation.TerminallySettled()
		}
		if !interception.EventPassthrough || !interception.NodePassthrough {
			return runtimedeliverycontinuation.Transferred()
		}
	}
	recipient := RoutePlanLiveRecipient{
		Recipient:         route.Recipient,
		AgentIdentity:     route.AgentIdentity,
		PersistAsDelivery: true,
		liveAuthority:     liveRecipientAuthorityIdentity,
	}
	if route.Recipient.IsNode() {
		recipient = RoutePlanLiveRecipient{InternalID: route.Recipient.ID()}
	}
	dispatch, err := eb.dispatchLiveRecipientsWithRoutes(ctx, evt, []RoutePlanLiveRecipient{recipient}, []events.DeliveryRoute{route})
	if err != nil {
		return runtimedeliverycontinuation.Fatal(err)
	}
	if dispatch.complete() {
		return runtimedeliverycontinuation.Transferred()
	}
	failure := eb.logAuthoritativeDeliveryIncomplete(
		ctx, evt, dispatch.expected, dispatch.delivered, dispatch.missing, dispatch.timedOut, dispatch.cause,
	)
	if dispatch.cause != nil {
		return runtimedeliverycontinuation.Fatal(failure)
	}
	if len(dispatch.timedOut) > 0 {
		return runtimedeliverycontinuation.Deferred(runtimedeliverycontinuation.DispatchWakeCarrierReturn)
	}
	if route.Recipient.IsAgent() {
		return runtimedeliverycontinuation.Deferred(runtimedeliverycontinuation.DispatchWakeAgentRouteLifecycle)
	}
	return runtimedeliverycontinuation.Deferred(runtimedeliverycontinuation.DispatchWakeInternalSubscriptionLifecycle)
}

func deliveryRoutesBySubscriber(deliveryRoutes []events.DeliveryRoute) map[deliveryRouteTargetKey][]events.DeliveryRoute {
	deliveryRoutes = events.NormalizeDeliveryRoutes(deliveryRoutes)
	if len(deliveryRoutes) == 0 {
		return nil
	}
	out := make(map[deliveryRouteTargetKey][]events.DeliveryRoute, len(deliveryRoutes))
	for _, route := range deliveryRoutes {
		route = route.Normalized()
		if route.Recipient.Empty() {
			continue
		}
		key := deliveryRouteTargetKey{
			recipient:     route.Recipient,
			agentIdentity: route.AgentIdentity,
		}
		out[key] = append(out[key], route)
	}
	return out
}

func authoritativeDeliveryTargetKeys(liveRecipients []RoutePlanLiveRecipient, deliveryRoutes []events.DeliveryRoute) []deliveryRouteTargetKey {
	deliveryRoutes = events.NormalizeDeliveryRoutes(deliveryRoutes)
	if len(deliveryRoutes) > 0 {
		out := make([]deliveryRouteTargetKey, 0, len(deliveryRoutes))
		for _, route := range deliveryRoutes {
			if !route.Recipient.IsAgent() {
				continue
			}
			out = append(out, deliveryRouteTargetKey{
				recipient:     route.Recipient,
				agentIdentity: route.AgentIdentity,
			})
		}
		return uniqueDeliveryTargetKeys(out)
	}
	out := make([]deliveryRouteTargetKey, 0, len(liveRecipients))
	for _, recipient := range normalizeRoutePlanLiveRecipients(liveRecipients) {
		out = append(out, deliveryRouteTargetKey{
			recipient:     recipient.Recipient,
			internalID:    recipient.InternalID,
			agentIdentity: recipient.AgentIdentity,
		})
	}
	return uniqueDeliveryTargetKeys(out)
}

func deliveryRoutesCoverAgentRecipients(routes []events.DeliveryRoute, recipients []string) bool {
	// Delivery routes own the exact target set. The persisted recipient list is
	// only its slug projection and must agree without becoming identity authority.
	exactTargets := authoritativeDeliveryTargetKeys(nil, routes)
	projectedRecipients := make(map[string]struct{}, len(exactTargets))
	agentRouteCount := 0
	for _, route := range events.NormalizeDeliveryRoutes(routes) {
		if !route.Recipient.IsAgent() {
			continue
		}
		agentRouteCount++
		if err := route.AgentIdentity.Validate(); err != nil || route.AgentIdentity.AgentID() != route.Recipient.ID() {
			return false
		}
		projectedRecipients[route.Recipient.ID()] = struct{}{}
	}
	if len(exactTargets) != agentRouteCount {
		return false
	}
	recipients = uniqueStrings(recipients)
	if len(projectedRecipients) != len(recipients) {
		return false
	}
	for _, recipient := range recipients {
		if _, ok := projectedRecipients[recipient]; !ok {
			return false
		}
	}
	return true
}

type deliveryRouteTargetKey struct {
	recipient     events.DeliveryRecipient
	internalID    string
	agentIdentity agentidentity.Identity
}

func (k deliveryRouteTargetKey) description() string {
	if k.recipient.IsAgent() {
		return "agent:" + k.agentIdentity.Description()
	}
	if k.internalID != "" {
		return "internal:" + k.internalID
	}
	return k.recipient.Code() + ":" + k.recipient.ID()
}

func deliveryTargetKeySet(in []deliveryRouteTargetKey) map[deliveryRouteTargetKey]struct{} {
	out := make(map[deliveryRouteTargetKey]struct{}, len(in))
	for _, key := range in {
		out[key] = struct{}{}
	}
	return out
}

func uniqueDeliveryTargetKeys(in []deliveryRouteTargetKey) []deliveryRouteTargetKey {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[deliveryRouteTargetKey]struct{}, len(in))
	out := make([]deliveryRouteTargetKey, 0, len(in))
	for _, key := range in {
		key.internalID = strings.TrimSpace(key.internalID)
		key.agentIdentity = key.agentIdentity.Normalize()
		if key.recipient.Empty() == (key.internalID == "") {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	return out
}

func deliveryTargetKeySubscriberIDs(in []deliveryRouteTargetKey) []string {
	out := make([]string, 0, len(in))
	for _, key := range in {
		if key.internalID != "" {
			out = append(out, key.internalID)
		} else {
			out = append(out, key.recipient.ID())
		}
	}
	return uniqueStrings(out)
}

func deliveryTargetKeyDescriptions(in []deliveryRouteTargetKey) []string {
	out := make([]string, 0, len(in))
	for _, key := range uniqueDeliveryTargetKeys(in) {
		out = append(out, key.description())
	}
	return out
}

type agentRecipient struct {
	identity   agentidentity.Identity
	internalID string
	kind       inMemorySubscriberKind
	route      *agentRouteHandle
	internal   *internalSubscriptionHandle
}

func (r agentRecipient) send(ctx context.Context, evt events.Event, handoff events.DeliveryRoute, continuation worklifetime.DeliveryContinuation) (agentRouteSendResult, error) {
	if r.route != nil {
		return r.route.send(ctx, evt, handoff, continuation)
	}
	if r.internal != nil {
		return r.internal.send(ctx, evt, handoff, continuation)
	}
	if continuation != nil {
		return agentRouteSendInactive, returnDeliveryContinuation(ctx, continuation)
	}
	return agentRouteSendInactive, nil
}

func returnDeliveryContinuation(ctx context.Context, continuation worklifetime.DeliveryContinuation) error {
	if continuation == nil {
		return nil
	}
	resolution, err := continuation.Resolve(context.WithoutCancel(ctx), worklifetime.DeliveryContinuationReturn)
	if err != nil {
		return err
	}
	if resolution != worklifetime.DeliveryContinuationReturned && resolution != worklifetime.DeliveryContinuationTerminal {
		return fmt.Errorf("delivery continuation return resolved as %d", resolution)
	}
	return nil
}

const workflowRuntimeInternalCarrierID = "workflow-runtime"

func (r agentRecipient) deliveryRouteTargetKey() deliveryRouteTargetKey {
	if r.kind == inMemorySubscriberInternal {
		if node, err := runtimeidentity.ParseExecutableNodeKey(r.subscriberID()); err == nil {
			return deliveryRouteTargetKey{recipient: events.MustNodeDeliveryRecipient(node)}
		}
		return deliveryRouteTargetKey{internalID: r.subscriberID()}
	}
	return deliveryRouteTargetKey{
		recipient:     events.MustAgentDeliveryRecipient(r.subscriberID()),
		agentIdentity: r.identity,
	}
}

func (r agentRecipient) subscriberID() string {
	if r.kind == inMemorySubscriberAgent {
		return r.identity.AgentID()
	}
	return strings.TrimSpace(r.internalID)
}

func (r agentRecipient) isWorkflowRuntimeInternalCarrier() bool {
	return r.kind == inMemorySubscriberInternal && strings.TrimSpace(r.internalID) == workflowRuntimeInternalCarrierID
}

func workflowRuntimeInternalCarrierRoutes(deliveryRoutes []events.DeliveryRoute) []events.DeliveryRoute {
	deliveryRoutes = events.NormalizeDeliveryRoutes(deliveryRoutes)
	if len(deliveryRoutes) == 0 {
		return nil
	}
	out := make([]events.DeliveryRoute, 0, len(deliveryRoutes))
	for _, route := range deliveryRoutes {
		if !route.Recipient.IsNode() {
			continue
		}
		if route.Recipient.LocalID() == workflowRuntimeInternalCarrierID {
			continue
		}
		out = append(out, route)
	}
	return events.NormalizeDeliveryRoutes(out)
}

func (eb *EventBus) snapshotRoutePlanRecipientChans(agentIDs []string, planned []RoutePlanLiveRecipient) []agentRecipient {
	if eb == nil || len(agentIDs) == 0 {
		return nil
	}
	plannedByID := make(map[string][]RoutePlanLiveRecipient, len(planned))
	for _, recipient := range normalizeRoutePlanLiveRecipients(planned) {
		id := recipient.subscriberID()
		plannedByID[id] = append(plannedByID[id], recipient)
	}
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	out := make([]agentRecipient, 0, len(agentIDs))
	for _, id := range agentIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if plannedRecipients := plannedByID[id]; len(plannedRecipients) > 0 {
			plannedInternal := false
			for _, plannedRecipient := range plannedRecipients {
				if plannedRecipient.isInternal() {
					plannedInternal = true
					continue
				}
				route := plannedRecipient.agentRoute
				if plannedRecipient.liveAuthority != liveRecipientAuthorityAgentRoute {
					route = eb.agentRouteHandles[plannedRecipient.AgentIdentity]
				}
				if route == nil {
					continue
				}
				out = append(out, agentRecipient{
					identity: plannedRecipient.AgentIdentity,
					kind:     inMemorySubscriberAgent,
					route:    route,
				})
			}
			if plannedInternal {
				if internal := eb.internalHandles[id]; internal != nil {
					out = append(out, agentRecipient{
						internalID: id,
						kind:       inMemorySubscriberInternal,
						internal:   internal,
					})
				}
			}
			continue
		}
		if internal := eb.internalHandles[id]; internal != nil {
			out = append(out, agentRecipient{
				internalID: id,
				kind:       inMemorySubscriberInternal,
				internal:   internal,
			})
		}
	}
	return out
}

func (eb *EventBus) resolveSubscribedRecipients(eventType string) []string {
	return deliveryRecipientIDs(eb.resolveSubscribedRecipientsForPlanning(eventType))
}

func (eb *EventBus) resolveSubscribedRecipientsForPlanning(eventType string) []deliveryRecipientCandidate {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	recipients := make([]deliveryRecipientCandidate, 0, len(eb.subscriptions))
	for key, pats := range eb.subscriptions {
		for _, pat := range pats {
			if routeMatches(string(pat), eventType) {
				authority := liveRecipientAuthorityIdentity
				var route *agentRouteHandle
				if key.kind == inMemorySubscriberAgent {
					route = eb.agentRouteHandles[key.agent]
					if route != nil {
						authority = liveRecipientAuthorityAgentRoute
					}
				}
				recipients = append(recipients, deliveryRecipientCandidate{
					ID:                key.subscriberID(),
					AgentIdentity:     key.agent,
					PersistAsDelivery: key.kind == inMemorySubscriberAgent,
					LiveAuthority:     authority,
					AgentRoute:        route,
				})
				break
			}
		}
	}
	return normalizeDeliveryRecipientCandidates(recipients)
}

func (eb *EventBus) resolveInternalRecipientsForRoutedNodePlanning(evt events.Event, routed []Subscriber) []deliveryRecipientCandidate {
	if eb == nil {
		return nil
	}
	aliases := routedNodeInternalSubscriptionAliases(evt, routed)
	if len(aliases) == 0 {
		return nil
	}
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	recipients := make([]deliveryRecipientCandidate, 0, len(eb.subscriptions))
	for key, pats := range eb.subscriptions {
		if key.kind != inMemorySubscriberInternal {
			continue
		}
		for _, pat := range pats {
			matched := false
			for _, alias := range aliases {
				if routeMatches(string(pat), alias) {
					matched = true
					break
				}
			}
			if matched {
				recipients = append(recipients, deliveryRecipientCandidate{
					ID:                key.internalID,
					PersistAsDelivery: false,
				})
				break
			}
		}
	}
	return normalizeDeliveryRecipientCandidates(recipients)
}

func (eb *EventBus) ResolveSubscribedRecipients(eventType string) []string {
	return eb.resolveSubscribedRecipients(eventType)
}

func routeMatches(pattern, eventType string) bool {
	return RouteMatches(pattern, eventType)
}

func isValidEventTypeName(raw string) bool {
	return IsValidEventTypeName(raw)
}

func uniqueStrings(in []string) []string {
	return UniqueStrings(in)
}

func dedupeSubscribers(in []Subscriber) []Subscriber {
	if len(in) == 0 {
		return nil
	}
	out := make([]Subscriber, 0, len(in))
	indexByRole := make(map[resolvedSubscriberRoleIdentity]int, len(in))
	for _, subscriber := range in {
		if subscriber.Recipient.Empty() {
			continue
		}
		key := resolvedSubscriberRoleKey(subscriber)
		if index, ok := indexByRole[key]; ok {
			out[index].MatchPattern = strongestSubscriberMatchEvidence(out[index].MatchPattern, subscriber.MatchPattern)
			continue
		}
		indexByRole[key] = len(out)
		out = append(out, subscriber)
	}
	return out
}

func ensurePublishEpoch(ctx context.Context) error {
	epoch, ok := RuntimeEpochFromContext(ctx)
	if !ok || epoch <= 0 {
		return nil
	}
	if !IsCurrentRuntimeEpoch(epoch) {
		return ErrStaleRuntimeEpoch
	}
	return nil
}

func (eb *EventBus) emitContradiction(ctx context.Context, source events.Event, reason string) error {
	eb.logRuntime(ctx, "warn", "Event routing contradiction was detected", "eventbus", "contradiction", strings.TrimSpace(source.ID()), strings.TrimSpace(string(source.Type())), "", strings.TrimSpace(source.EntityID()), "", nil, map[string]any{
		"reason": strings.TrimSpace(reason),
	}, nil, 0)
	return nil
}

func (eb *EventBus) logAuthoritativeDeliveryIncomplete(ctx context.Context, evt events.Event, expected, delivered, missing, timedOut []deliveryRouteTargetKey, cause error) error {
	expectedDescriptions := deliveryTargetKeyDescriptions(expected)
	deliveredDescriptions := deliveryTargetKeyDescriptions(delivered)
	missingDescriptions := deliveryTargetKeyDescriptions(missing)
	timedOutDescriptions := deliveryTargetKeyDescriptions(timedOut)
	detail := map[string]any{
		"expected_recipients":  expectedDescriptions,
		"delivered_recipients": deliveredDescriptions,
	}
	if len(missing) > 0 {
		detail["missing_recipients"] = missingDescriptions
	}
	if len(timedOut) > 0 {
		detail["timed_out_recipients"] = timedOutDescriptions
	}
	parts := make([]string, 0, 3)
	if len(missing) > 0 {
		parts = append(parts, "missing="+strings.Join(missingDescriptions, ","))
	}
	if len(timedOut) > 0 {
		parts = append(parts, "timed_out="+strings.Join(timedOutDescriptions, ","))
	}
	if cause != nil {
		parts = append(parts, cause.Error())
	}
	if len(parts) == 0 {
		parts = append(parts, "incomplete")
	}
	baseErr := fmt.Errorf("%w: %s", errAuthoritativeDeliveryIncomplete, strings.Join(parts, "; "))
	failureErr := runtimefailures.Wrap(runtimefailures.ClassTargetUnreachable, "authoritative_delivery_incomplete", "eventbus", "deliver_authoritative_recipients", detail, baseErr)
	failure := runtimefailures.Normalize(failureErr, "eventbus", "deliver_authoritative_recipients")
	eb.logRuntime(ctx, "warn", "Authoritative delivery fan-out was incomplete", "eventbus", "delivery_incomplete", evt.ID(), string(evt.Type()), "", evt.EntityID(), "", nil, detail, &failure, 0)
	return failureErr
}

func targetDeliveryTimeoutFailure(evt events.Event, recipient deliveryRouteTargetKey, timeout time.Duration) *runtimefailures.Envelope {
	failure := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassTimeout, "delivery_timeout", "eventbus", "deliver_recipient", map[string]any{
		"event_id": evt.ID(), "event_type": string(evt.Type()), "recipient": recipient.description(), "timeout_ms": int(timeout / time.Millisecond),
	}), "eventbus", "deliver_recipient")
	return &failure
}

func (eb *EventBus) logRuntime(ctx context.Context, level diaglog.Level, message, component, action, eventID, eventType, agentID, entityID, sessionID string, correlation map[string]string, detail any, failure *runtimefailures.Envelope, durationUS int) error {
	if eb == nil {
		return nil
	}
	eb.mu.RLock()
	logger := eb.logger
	eb.mu.RUnlock()
	if logger == nil {
		return nil
	}
	if failure != nil {
		if err := runtimefailures.ValidateEnvelope(*failure); err != nil {
			return fmt.Errorf("validate runtime log failure: %w", err)
		}
	}
	ctx = runtimecorrelation.WithRuntimeDiagnosticLineage(ctx, eventID, eventType)
	var err error
	ctx, err = eb.admitBundleSourceFact(ctx)
	if err != nil {
		return err
	}
	if err := logger.Log(ctx, level, message, component, action, eventID, eventType, agentID, entityID, sessionID, correlation, detail, runtimefailures.CloneEnvelope(failure), durationUS); err != nil {
		diaglog.ProcessLog("error", "diagnostics", "runtime log persistence failed",
			"component", strings.TrimSpace(component),
			"action", strings.TrimSpace(action),
			"error", err.Error(),
		)
		return err
	}
	return nil
}

func (eb *EventBus) LogRuntime(ctx context.Context, entry runtimepipeline.RuntimeLogEntry) error {
	if eb == nil {
		return nil
	}
	eb.mu.RLock()
	logger := eb.logger
	eb.mu.RUnlock()
	if logger == nil {
		return nil
	}
	if entry.Failure != nil {
		if err := runtimefailures.ValidateEnvelope(*entry.Failure); err != nil {
			return fmt.Errorf("validate runtime log failure: %w", err)
		}
	}
	ctx = runtimecorrelation.WithRuntimeDiagnosticLineage(ctx, entry.EventID, entry.EventType)
	var err error
	ctx, err = eb.admitBundleSourceFact(ctx)
	if err != nil {
		return err
	}
	return logger.Log(ctx, entry.Level, entry.Message, entry.Component, entry.Action, entry.EventID, entry.EventType, entry.AgentID, entry.EffectiveEntityID(), entry.SessionID, entry.Correlation, entry.Detail, runtimefailures.CloneEnvelope(entry.Failure), entry.DurationUS)
}
