package eventpersistence

import (
	"context"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func eventFacts(
	eventID string,
	eventType events.EventType,
	producer events.ProducerIdentity,
	payload []byte,
	envelope events.EventEnvelope,
	createdAt time.Time,
) (events.EventFacts, error) {
	routingSource := events.NoRoutingSource()
	source := envelope.Source.Normalized()
	if !source.Empty() {
		var err error
		if source.FlowInstance != "" {
			routingSource, err = events.NewConcreteTemplateInstanceRoutingSource(source)
		} else {
			routingSource, err = events.NewExternalIngressRoutingSource(source.FlowID, events.RoutingSourceAuthorityProviderAdmissionPlan)
		}
		if err != nil {
			return events.EventFacts{}, err
		}
	}
	return events.EventFacts{
		ID: eventID, Type: eventType,
		Producer: events.ProducerClaim{Type: producer.Type(), ID: producer.ID()},
		Payload:  payload, Envelope: envelope, RoutingSource: routingSource,
		CreatedAt: createdAt, ExecutionMode: executionmode.Live,
	}, nil
}

func fixtureSettlement(event events.Event) (events.RouteSettlement, error) {
	switch event.Type() {
	case events.EventTypePlatformRuntimeLog:
		return events.NewNoDeliverySettlement(events.EventWriteRuntimeLogDirect, events.NoDeliveryNoSubscriberByDesign, events.ConnectEvaluationLedger{})
	case events.EventTypePlatformInboundRecord:
		return events.NewNoDeliverySettlement(events.EventWriteInboundEvidenceDirect, events.NoDeliveryNoSubscriberByDesign, events.ConnectEvaluationLedger{})
	case events.EventTypePlatformAgentDirective:
		return events.NewNoDeliverySettlement(events.EventWriteDirectiveDirect, events.NoDeliveryNoSubscriberByDesign, events.ConnectEvaluationLedger{})
	default:
		ledger, err := events.NewConnectEvaluationLedger(nil)
		if err != nil {
			return events.RouteSettlement{}, err
		}
		return events.NewNoDeliverySettlement(events.EventWriteNormalPublication, events.NoDeliveryDeclaredConsumerNoPlan, ledger)
	}
}

func (s *EventPostgresOwner) InsertRevisionedExistingRunRootForTest(ctx context.Context, eventID, runID string, eventType events.EventType, producer events.ProducerIdentity, payload []byte, envelope events.EventEnvelope, createdAt time.Time) (events.Event, error) {
	var event events.Event
	err := runNativeEventFixtureMutation(ctx, s, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		var err error
		event, err = InsertExistingRunRootEventTxForTest(ctx, attempt, true, eventID, runID, eventType, producer, payload, envelope, createdAt)
		return err
	})
	if err != nil {
		return events.Event{}, err
	}
	return event, nil
}

func (s *EventSQLiteOwner) InsertRevisionedExistingRunRootForTest(ctx context.Context, eventID, runID string, eventType events.EventType, producer events.ProducerIdentity, payload []byte, envelope events.EventEnvelope, createdAt time.Time) (events.Event, error) {
	var event events.Event
	err := runNativeEventFixtureMutation(ctx, s, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		var err error
		event, err = InsertExistingRunRootEventTxForTest(ctx, attempt, false, eventID, runID, eventType, producer, payload, envelope, createdAt)
		return err
	})
	if err != nil {
		return events.Event{}, err
	}
	return event, nil
}

func InsertExistingRunRootEventTxForTest(ctx context.Context, attempt *mutationprotocol.Attempt, postgres bool, eventID, runID string, eventType events.EventType, producer events.ProducerIdentity, payload []byte, envelope events.EventEnvelope, createdAt time.Time) (events.Event, error) {
	facts, err := eventFacts(eventID, eventType, producer, payload, envelope, createdAt)
	if err != nil {
		return events.Event{}, err
	}
	event, err := events.NewExistingRunRootIngressEvent(events.ExistingRunRootIngressEventInput{Facts: facts, RunID: runID})
	if err != nil {
		return event, err
	}
	if attempt == nil {
		return event, fmt.Errorf("canonical event fixture requires a mutation attempt")
	}
	bound, err := eventfixture.BindPayload(event)
	if err != nil {
		return event, err
	}
	admitted, err := events.AdmitForPersistence(bound, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return event, err
	}
	if admitted.Class() == events.EventAdmissionSelectedForkReplay {
		return event, fmt.Errorf("selected-fork replay fixture requires exact lineage persistence")
	}
	settlement, err := fixtureSettlement(admitted.Event())
	if err != nil {
		return event, err
	}
	record, err := eventrecord.FromAdmitted(admitted, settlement)
	if err != nil {
		return event, err
	}
	_, err = insertCanonicalFixtureRecord(ctx, attempt, postgres, record)
	return event, err
}
