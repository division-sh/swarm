package events

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// AdmittedEvent is the only event value accepted by persistence operations.
// Its zero value is invalid and no field can be populated outside this package.
type AdmittedEvent struct {
	event Event
}

func (a AdmittedEvent) Event() Event               { return a.event.Clone() }
func (a AdmittedEvent) ID() string                 { return a.event.ID() }
func (a AdmittedEvent) Class() EventAdmissionClass { return a.event.AdmissionClass() }

func newAdmittedEvent(event Event) AdmittedEvent { return AdmittedEvent{event: event.Clone()} }

type RestoredEventInput struct {
	Class         EventAdmissionClass
	Facts         EventFacts
	RunID         string
	ParentEventID string
	OperatorRef   *OperatorReferenceProvenance
	SelectedFork  *SelectedForkLineage
}

// RestoreAdmittedEvent is the canonical durable readback boundary. It does not
// allocate, infer, repair, or reclassify any fact.
func RestoreAdmittedEvent(input RestoredEventInput) (AdmittedEvent, error) {
	if strings.TrimSpace(input.Facts.ID) == "" || input.Facts.CreatedAt.IsZero() {
		return AdmittedEvent{}, fmt.Errorf("durable event requires event_id and created_at")
	}
	var (
		event Event
		err   error
	)
	switch input.Class {
	case EventAdmissionRootIngress:
		event, err = NewRootIngressEvent(RootIngressEventInput{Facts: input.Facts, RunID: input.RunID})
	case EventAdmissionOperatorInjected:
		event, err = NewOperatorInjectedEvent(OperatorInjectedEventInput{Facts: input.Facts, RunID: input.RunID, Provenance: input.OperatorRef})
	case EventAdmissionChild:
		event, err = NewChildEvent(ChildEventInput{Facts: input.Facts, Lineage: EventLineage{RunID: input.RunID, ParentEventID: input.ParentEventID, TaskID: input.Facts.TaskID, ExecutionMode: input.Facts.ExecutionMode}})
	case EventAdmissionReplay:
		event, err = NewReplayEvent(ReplayEventInput{Facts: input.Facts, Lineage: EventLineage{RunID: input.RunID, ParentEventID: input.ParentEventID, TaskID: input.Facts.TaskID, ExecutionMode: input.Facts.ExecutionMode}})
	case EventAdmissionSelectedForkReplay:
		if input.SelectedFork == nil {
			return AdmittedEvent{}, fmt.Errorf("selected-fork replay durable event requires lineage")
		}
		event, err = NewSelectedForkReplayEvent(SelectedForkReplayEventInput{Facts: input.Facts, Lineage: *input.SelectedFork})
	case EventAdmissionRuntimeControl:
		event, err = NewRuntimeControlEvent(RuntimeEventInput{Facts: input.Facts, RunID: input.RunID, ParentEventID: input.ParentEventID})
	case EventAdmissionRuntimeDiagnostic:
		event, err = NewRuntimeDiagnosticEvent(RuntimeEventInput{Facts: input.Facts, RunID: input.RunID, ParentEventID: input.ParentEventID})
	case EventAdmissionDiagnosticDirect:
		event, err = NewDiagnosticDirectEvent(DiagnosticDirectEventInput{Facts: input.Facts, RunID: input.RunID, ParentEventID: input.ParentEventID})
	default:
		return AdmittedEvent{}, fmt.Errorf("durable event class %q is invalid", input.Class)
	}
	if err != nil {
		return AdmittedEvent{}, err
	}
	if event.ID() != strings.TrimSpace(input.Facts.ID) || !event.CreatedAt().Equal(input.Facts.CreatedAt.UTC().Truncate(time.Microsecond)) {
		return AdmittedEvent{}, fmt.Errorf("durable event identity changed during readback")
	}
	if err := validateAdmittedIdentity(event.ID(), event.RunID(), event.ParentEventID(), true); err != nil {
		return AdmittedEvent{}, fmt.Errorf("durable event identity: %w", err)
	}
	return newAdmittedEvent(event), nil
}

type RouteProbe struct {
	eventType EventType
}

func NewRouteProbe(eventType EventType) (RouteProbe, error) {
	eventType = EventType(strings.TrimSpace(string(eventType)))
	if eventType == "" {
		return RouteProbe{}, fmt.Errorf("route probe event type is required")
	}
	return RouteProbe{eventType: eventType}, nil
}

func (p RouteProbe) Type() EventType { return p.eventType }

// DeliveryEvent is a receiver-local view. It deliberately cannot satisfy any
// persistence operation, which accepts only AdmittedEvent.
type DeliveryEvent struct {
	journal Event
	view    Event
}

func NewDeliveryEvent(event Event, route DeliveryRoute) (DeliveryEvent, error) {
	projection, err := route.PayloadProjection.Canonical()
	if err != nil {
		return DeliveryEvent{}, err
	}
	payload := event.Payload()
	if !projection.Empty() {
		var fields map[string]any
		if err := json.Unmarshal(payload, &fields); err != nil || fields == nil {
			return DeliveryEvent{}, fmt.Errorf("delivery payload projection requires an object payload")
		}
		for field, value := range projection.Fields() {
			if _, exists := fields[field]; exists {
				return DeliveryEvent{}, fmt.Errorf("delivery payload projection conflicts with producer field %q", field)
			}
			fields[field] = value
		}
		payload, err = json.Marshal(fields)
		if err != nil {
			return DeliveryEvent{}, fmt.Errorf("encode delivery payload projection: %w", err)
		}
	}
	view := event.Clone()
	view.payload = clonePayload(payload)
	view.deliveryContext = route.Context.Normalized()
	target := route.Target.Normalized()
	if target.Empty() && strings.TrimSpace(route.SubscriberType) == "node" {
		envelope := view.NormalizedEnvelope()
		envelope.Target = RouteIdentity{}
		envelope.TargetSet = nil
		view.setEnvelopeClaim(envelope)
	} else if !target.Empty() {
		view.setEnvelopeClaim(EnvelopeForTargetRoute(view.NormalizedEnvelope(), target))
	}
	return DeliveryEvent{journal: event.Clone(), view: view}, nil
}

func NewContextDeliveryEvent(event Event, deliveryContext DeliveryContext) DeliveryEvent {
	view := event.Clone()
	view.deliveryContext = deliveryContext.Normalized()
	return DeliveryEvent{journal: event.Clone(), view: view}
}

func (d DeliveryEvent) Event() Event        { return d.view.Clone() }
func (d DeliveryEvent) JournalEvent() Event { return d.journal.Clone() }

// ResolveEnvelope applies a routing-owner result without changing constructor
// class, producer origin, causal lineage, payload, mode, or routing source.
func ResolveEnvelope(event Event, envelope EventEnvelope) (Event, error) {
	if envelope.Source.Normalized() != event.NormalizedEnvelope().Source {
		return Event{}, fmt.Errorf("resolved envelope cannot rewrite event source projection")
	}
	if err := validateEnvelopeClaim(envelope, false); err != nil {
		return Event{}, fmt.Errorf("resolved event envelope: %w", err)
	}
	resolved := event.Clone()
	resolved.setEnvelopeClaim(envelope)
	return resolved, nil
}

// OptionalEvent represents absence without manufacturing an event-shaped
// sentinel. It is intended for lookup/decision APIs, not persistence.
type OptionalEvent struct {
	event *Event
}

func SomeEvent(event Event) OptionalEvent {
	cloned := event.Clone()
	return OptionalEvent{event: &cloned}
}

func NoEvent() OptionalEvent { return OptionalEvent{} }

func (o OptionalEvent) Get() (Event, bool) {
	if o.event == nil {
		return Event{}, false
	}
	return o.event.Clone(), true
}
