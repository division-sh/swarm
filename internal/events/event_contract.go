package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// ValidateEventContract is the canonical class, catalog, producer, and lineage
// contract for every durable event value.
func ValidateEventContract(event Event) error {
	producer := event.Producer()
	class := event.AdmissionClass()
	eventType := event.Type()
	if err := ValidateEventStructuralContract(class, eventType, producer, event.RunID(), event.Scope()); err != nil {
		return err
	}

	if class == EventAdmissionOperatorInjected {
		if event.RunID() == "" {
			return fmt.Errorf("operator-injected event requires run_id")
		}
	} else if _, ok := event.OperatorReference(); ok {
		return fmt.Errorf("operator provenance is only valid for operator-injected events")
	}
	if class == EventAdmissionChild || class == EventAdmissionReplay {
		if event.RunID() == "" || event.ParentEventID() == "" {
			return fmt.Errorf("%s event requires run_id and parent_event_id", class)
		}
	}
	if class == EventAdmissionSelectedForkReplay {
		if event.RunID() == "" {
			return fmt.Errorf("selected-fork replay requires destination run_id")
		}
		if _, ok := event.SelectedForkLineage(); !ok {
			return fmt.Errorf("selected-fork replay requires typed lineage")
		}
	} else if _, ok := event.SelectedForkLineage(); ok {
		return fmt.Errorf("selected-fork lineage is only valid for selected-fork replay events")
	}
	if event.ParentEventID() != "" && event.RunID() == "" {
		return fmt.Errorf("event class %q with causal parent requires run_id", class)
	}
	return nil
}

// ValidateEventStructuralContract is the canonical durable class, subtype,
// producer, run, and scope policy. Constructors, admission, readback, and
// named persistence operations all consume this owner.
func ValidateEventStructuralContract(class EventAdmissionClass, eventType EventType, producer ProducerIdentity, runID string, scope EventScope) error {
	if err := validateEventIdentityContract(class, eventType, producer); err != nil {
		return err
	}
	if class != EventAdmissionDiagnosticDirect {
		return nil
	}

	runID = strings.TrimSpace(runID)
	switch eventType {
	case EventTypePlatformRuntimeLog:
		if producer.ID() != "runtime" {
			return fmt.Errorf("%s requires exact platform producer runtime; got %q", eventType, producer.ID())
		}
		if scope != EventScopeGlobal {
			return fmt.Errorf("%s requires global scope; got %q", eventType, scope)
		}
	case EventTypePlatformInboundRecord:
		if runID == "" {
			return fmt.Errorf("%s requires run_id", eventType)
		}
		if scope != EventScopeEntity && scope != EventScopeGlobal {
			return fmt.Errorf("%s requires entity or global scope; got %q", eventType, scope)
		}
	case EventTypePlatformAgentDirective:
		if runID == "" {
			return fmt.Errorf("%s requires run_id", eventType)
		}
		if scope != EventScopeGlobal {
			return fmt.Errorf("%s requires global scope; got %q", eventType, scope)
		}
	default:
		return fmt.Errorf("diagnostic_direct event type %q is not in the closed catalog", eventType)
	}
	return nil
}

func validateEventIdentityContract(class EventAdmissionClass, eventType EventType, producer ProducerIdentity) error {
	if err := producer.Validate(); err != nil {
		return fmt.Errorf("event producer identity: %w", err)
	}
	closedType := IsDiagnosticDirectEventType(eventType)
	if closedType && class != EventAdmissionDiagnosticDirect {
		return fmt.Errorf("closed event type %q requires diagnostic_direct class", eventType)
	}
	if class == EventAdmissionDiagnosticDirect && !closedType {
		return fmt.Errorf("diagnostic_direct event type %q is not in the closed catalog", eventType)
	}

	switch class {
	case EventAdmissionRootIngress, EventAdmissionOperatorInjected:
		if producer.Type() != EventProducerExternal {
			return fmt.Errorf("event class %q requires external producer; got %q", class, producer.Type())
		}
	case EventAdmissionChild:
		switch producer.Type() {
		case EventProducerNode, EventProducerAgent, EventProducerPlatform:
		default:
			return fmt.Errorf("event class %q cannot use producer type %q", class, producer.Type())
		}
	case EventAdmissionReplay, EventAdmissionSelectedForkReplay:
		// Replay preserves the authoritative source producer identity.
	case EventAdmissionRuntimeControl, EventAdmissionRuntimeDiagnostic, EventAdmissionDiagnosticDirect:
		if producer.Type() != EventProducerPlatform {
			return fmt.Errorf("event class %q requires platform producer; got %q", class, producer.Type())
		}
	default:
		return fmt.Errorf("event class %q is not persistable", class)
	}
	return nil
}

// ValidateGenericPublishEvent rejects values owned by closed named operations.
func ValidateGenericPublishEvent(event Event) error {
	if err := ValidateEventContract(event); err != nil {
		return err
	}
	if IsDiagnosticDirectEventType(event.Type()) || event.AdmissionClass() == EventAdmissionDiagnosticDirect {
		return fmt.Errorf("closed event type %q requires its named persistence operation", event.Type())
	}
	if event.AdmissionClass() == EventAdmissionSelectedForkReplay {
		return fmt.Errorf("selected-fork replay requires its named persistence operation")
	}
	return nil
}

// ValidatePersistentEvent validates every UUID-bearing durable fact after
// admission has allocated the server-owned identity and timestamp.
func ValidatePersistentEvent(event Event) error {
	if err := ValidateEventContract(event); err != nil {
		return err
	}
	if err := validateRequiredUUID("event_id", event.ID()); err != nil {
		return err
	}
	if err := validateOptionalUUID("run_id", event.RunID()); err != nil {
		return err
	}
	if err := validateOptionalUUID("parent_event_id", event.ParentEventID()); err != nil {
		return err
	}
	if err := validateEnvelopeClaim(event.envelopeClaimForAdmission(), true); err != nil {
		return fmt.Errorf("event envelope: %w", err)
	}
	if source := event.RoutingSource(); !source.Empty() {
		if err := validateRequiredUUID("routing_source.entity_id", source.Route().EntityID); err != nil {
			return err
		}
	}
	if reference, ok := event.OperatorReference(); ok {
		if err := validateRequiredUUID("operator_reference_event_id", reference.ReferencedEventID()); err != nil {
			return err
		}
	}
	if lineage, ok := event.SelectedForkLineage(); ok {
		for field, value := range map[string]string{
			"selected_fork.destination_run_id": lineage.DestinationRunID(),
			"selected_fork.source_run_id":      lineage.SourceRunID(),
			"selected_fork.source_event_id":    lineage.SourceEventID(),
		} {
			if err := validateRequiredUUID(field, value); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateRequiredUUID(field, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("event %s is required", field)
	}
	return validateOptionalUUID(field, value)
}

// ValidateNamedEvent is the closed-operation admission check. Named operations
// must state both the exact semantic class and exact catalog subtype.
func ValidateNamedEvent(admitted AdmittedEvent, class EventAdmissionClass, eventType EventType) error {
	event := admitted.Event()
	if err := ValidatePersistentEvent(event); err != nil {
		return err
	}
	if event.AdmissionClass() != class || event.Type() != eventType {
		return fmt.Errorf("named event operation requires %s %s; got %s %s", class, eventType, event.AdmissionClass(), event.Type())
	}
	return nil
}

// BindManagerOutputIdentity assigns the deterministic identity owned by the
// agent-manager retry boundary while reconstructing through the original
// semantic class constructor. It cannot change any other event-owned fact.
func BindManagerOutputIdentity(event Event, eventID string) (Event, error) {
	if err := validateRequiredUUID("manager_output.event_id", eventID); err != nil {
		return Event{}, err
	}
	if event.ID() != "" {
		if event.ID() != eventID {
			return Event{}, fmt.Errorf("manager output already has a different event_id")
		}
		return event.Clone(), nil
	}
	facts := EventFacts{
		ID: eventID, Type: event.Type(), Producer: ProducerClaim{Type: event.ProducerType(), ID: event.SourceAgent()},
		TaskID: event.TaskID(), Payload: event.Payload(), ChainDepth: event.ChainDepth(),
		Envelope: event.envelopeClaimForAdmission(), RoutingSource: event.RoutingSource(),
		CreatedAt: event.CreatedAt(), ExecutionMode: event.ExecutionMode(),
	}
	switch event.AdmissionClass() {
	case EventAdmissionRootIngress:
		return NewRootIngressEvent(RootIngressEventInput{Facts: facts, RunID: event.RunID()})
	case EventAdmissionOperatorInjected:
		var provenance *OperatorReferenceProvenance
		if value, ok := event.OperatorReference(); ok {
			provenance = &value
		}
		return NewOperatorInjectedEvent(OperatorInjectedEventInput{Facts: facts, RunID: event.RunID(), Provenance: provenance})
	case EventAdmissionChild:
		return NewChildEvent(ChildEventInput{Facts: facts, Lineage: EventLineage{RunID: event.RunID(), ParentEventID: event.ParentEventID(), TaskID: event.TaskID(), ExecutionMode: event.ExecutionMode()}})
	case EventAdmissionReplay:
		return NewReplayEvent(ReplayEventInput{Facts: facts, Lineage: EventLineage{RunID: event.RunID(), ParentEventID: event.ParentEventID(), TaskID: event.TaskID(), ExecutionMode: event.ExecutionMode()}})
	case EventAdmissionSelectedForkReplay:
		lineage, ok := event.SelectedForkLineage()
		if !ok {
			return Event{}, fmt.Errorf("manager selected-fork output is missing typed lineage")
		}
		return NewSelectedForkReplayEvent(SelectedForkReplayEventInput{Facts: facts, Lineage: lineage})
	case EventAdmissionRuntimeControl:
		return restoreRuntimeEvent(EventAdmissionRuntimeControl, facts, event.RunID(), event.ParentEventID())
	case EventAdmissionRuntimeDiagnostic:
		return restoreRuntimeEvent(EventAdmissionRuntimeDiagnostic, facts, event.RunID(), event.ParentEventID())
	case EventAdmissionDiagnosticDirect:
		return Event{}, fmt.Errorf("diagnostic-direct events cannot be agent-manager outputs")
	default:
		return Event{}, fmt.Errorf("manager output class %q is invalid", event.AdmissionClass())
	}
}

func ProducerIs(event Event, producerType EventProducerType, producerID string) bool {
	want, err := NewProducerIdentity(producerType, producerID)
	return err == nil && event.Producer().Equal(want)
}

func IsRuntimePlatformEvent(event Event) bool {
	if !ProducerIs(event, EventProducerPlatform, "runtime") || !strings.HasPrefix(string(event.Type()), "platform.") {
		return false
	}
	return event.AdmissionClass() == EventAdmissionRuntimeControl || event.AdmissionClass() == EventAdmissionRuntimeDiagnostic
}

func mustProducerIdentity(producerType EventProducerType, producerID string) ProducerIdentity {
	producer, err := NewProducerIdentity(producerType, producerID)
	if err != nil {
		panic(err)
	}
	return producer
}

// IntegrityProjection is the canonical exact semantic input for integrity
// fingerprints. It includes every event-owned authority fact.
func IntegrityProjection(event Event) (any, error) {
	if err := ValidateEventContract(event); err != nil {
		return nil, err
	}
	var payload any
	decoder := json.NewDecoder(bytes.NewReader(event.Payload()))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode event payload for integrity: %w", err)
	}
	type routeProjection struct {
		FlowID       string `json:"flow_id,omitempty"`
		FlowInstance string `json:"flow_instance,omitempty"`
		EntityID     string `json:"entity_id,omitempty"`
	}
	projectRoute := func(route RouteIdentity) routeProjection {
		route = route.Normalized()
		return routeProjection{FlowID: route.FlowID, FlowInstance: route.FlowInstance, EntityID: route.EntityID}
	}
	// Integrity covers the exact durable view. Construction claims are validated
	// above, then projected through the same normalization used by Record.
	envelope := event.NormalizedEnvelope()
	targetSet := make([]routeProjection, 0, len(envelope.TargetSet))
	for _, route := range envelope.TargetSet {
		targetSet = append(targetSet, projectRoute(route))
	}
	projection := struct {
		Class         EventAdmissionClass `json:"class"`
		ID            string              `json:"id"`
		Type          EventType           `json:"type"`
		ProducerType  EventProducerType   `json:"producer_type"`
		ProducerID    string              `json:"producer_id"`
		TaskID        string              `json:"task_id"`
		Payload       any                 `json:"payload"`
		ChainDepth    int                 `json:"chain_depth"`
		RunID         string              `json:"run_id"`
		ParentEventID string              `json:"parent_event_id"`
		CreatedAt     string              `json:"created_at"`
		ExecutionMode string              `json:"execution_mode"`
		Envelope      struct {
			EntityID     string            `json:"entity_id"`
			FlowInstance string            `json:"flow_instance"`
			Scope        EventScope        `json:"scope"`
			Source       routeProjection   `json:"source"`
			Target       routeProjection   `json:"target"`
			TargetSet    []routeProjection `json:"target_set"`
		} `json:"envelope"`
		RoutingSource struct {
			Kind      RoutingSourceKind `json:"kind"`
			Route     routeProjection   `json:"route"`
			Authority string            `json:"authority"`
		} `json:"routing_source"`
		OperatorReferenceEventID string `json:"operator_reference_event_id"`
		SelectedFork             *struct {
			DestinationRunID string `json:"destination_run_id"`
			SourceRunID      string `json:"source_run_id"`
			SourceEventID    string `json:"source_event_id"`
			AuthorityStamp   string `json:"authority_stamp"`
		} `json:"selected_fork,omitempty"`
	}{
		Class: event.AdmissionClass(), ID: event.ID(), Type: event.Type(), ProducerType: event.ProducerType(),
		ProducerID: event.SourceAgent(), TaskID: event.TaskID(), Payload: payload, ChainDepth: event.ChainDepth(),
		RunID: event.RunID(), ParentEventID: event.ParentEventID(), ExecutionMode: string(event.ExecutionMode()),
	}
	if !event.CreatedAt().IsZero() {
		projection.CreatedAt = event.CreatedAt().Format("2006-01-02T15:04:05.999999Z07:00")
	}
	projection.Envelope.EntityID = strings.TrimSpace(envelope.EntityID)
	projection.Envelope.FlowInstance = strings.Trim(strings.TrimSpace(envelope.FlowInstance), "/")
	projection.Envelope.Scope = normalizeEventScope(envelope.Scope)
	projection.Envelope.Source = projectRoute(envelope.Source)
	projection.Envelope.Target = projectRoute(envelope.Target)
	projection.Envelope.TargetSet = targetSet
	source := event.RoutingSource()
	projection.RoutingSource.Kind = source.Kind()
	projection.RoutingSource.Route = projectRoute(source.Route())
	projection.RoutingSource.Authority = source.Authority()
	if reference, ok := event.OperatorReference(); ok {
		projection.OperatorReferenceEventID = reference.ReferencedEventID()
	}
	if lineage, ok := event.SelectedForkLineage(); ok {
		projection.SelectedFork = &struct {
			DestinationRunID string `json:"destination_run_id"`
			SourceRunID      string `json:"source_run_id"`
			SourceEventID    string `json:"source_event_id"`
			AuthorityStamp   string `json:"authority_stamp"`
		}{lineage.DestinationRunID(), lineage.SourceRunID(), lineage.SourceEventID(), lineage.AuthorityStamp()}
	}
	return projection, nil
}
