// Package eventfixture seeds canonical event records for integration tests
// that cannot import the store package without creating an import cycle.
// Runtime code must use the closed named store operations instead.
package eventfixture

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
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
			routingSource, err = events.NewExternalIngressRoutingSource(source.FlowID, source.EntityID, events.RoutingSourceAuthorityProviderAdmissionPlan)
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

type Executor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func Load(ctx context.Context, q Executor, dialect authoractivityfixture.Dialect, eventID string) (event events.Event, err error) {
	if q == nil {
		return event, fmt.Errorf("canonical event fixture requires a database")
	}
	var (
		record eventrecord.Record
		found  bool
	)
	switch dialect {
	case authoractivityfixture.DialectPostgres:
		record, found, err = eventrecordpostgres.Load(ctx, q, eventID)
	case authoractivityfixture.DialectSQLite:
		record, found, err = eventrecordsqlite.Load(ctx, q, eventID)
	default:
		return event, fmt.Errorf("canonical event fixture dialect %q is unsupported", dialect)
	}
	if err != nil {
		return event, err
	}
	if !found {
		return event, fmt.Errorf("canonical event fixture %s is missing", eventID)
	}
	admitted, err := record.Decode()
	if err != nil {
		return event, err
	}
	return admitted.Event(), nil
}

func Insert(ctx context.Context, exec Executor, dialect authoractivityfixture.Dialect, event events.Event) error {
	if exec == nil {
		return fmt.Errorf("canonical event fixture requires a database")
	}
	event, err := BindPayload(event)
	if err != nil {
		return err
	}
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return err
	}
	if admitted.Class() == events.EventAdmissionSelectedForkReplay {
		return fmt.Errorf("selected-fork replay fixture requires exact lineage persistence")
	}
	settlement, err := fixtureSettlement(admitted.Event())
	if err != nil {
		return err
	}
	record, err := eventrecord.FromAdmitted(admitted, settlement)
	if err != nil {
		return err
	}
	var inserted bool
	switch dialect {
	case authoractivityfixture.DialectPostgres:
		inserted, err = eventrecordpostgres.Insert(ctx, exec, record)
	case authoractivityfixture.DialectSQLite:
		inserted, err = eventrecordsqlite.Insert(ctx, exec, record)
	default:
		return fmt.Errorf("canonical event fixture dialect %q is unsupported", dialect)
	}
	if err != nil {
		return err
	}
	if inserted {
		return nil
	}
	var (
		existing eventrecord.Record
		found    bool
	)
	switch dialect {
	case authoractivityfixture.DialectPostgres:
		existing, found, err = eventrecordpostgres.Load(ctx, exec, record.EventID)
	case authoractivityfixture.DialectSQLite:
		existing, found, err = eventrecordsqlite.Load(ctx, exec, record.EventID)
	}
	if err != nil {
		return err
	}
	if !found || !record.Equal(existing) {
		return fmt.Errorf("canonical event fixture %s conflicts with its persisted record", record.EventID)
	}
	return nil
}

// BindPayload attaches explicit schema-less fixture admission evidence. It is
// shared by store fixtures that intentionally bypass runtime publication.
func BindPayload(event events.Event) (events.Event, error) {
	binding, err := events.NewPayloadSchemaBinding(events.PayloadSchemaBindingInput{
		BundleHash:   "bundle-v1:sha256:0000000000000000000000000000000000000000000000000000000000000000",
		BundleSource: "ephemeral", FlowID: event.RoutingSource().Route().FlowID, EventKey: string(event.Type()),
		SchemaDigest: "sha256:0000000000000000000000000000000000000000000000000000000000000000",
		SchemaClass:  events.PayloadSchemaSchemaLess,
	})
	if err != nil {
		return event.Clone(), err
	}
	admission, err := events.NewPayloadAdmission(event.Payload(), binding)
	if err != nil {
		return event.Clone(), err
	}
	return events.ApplyPayloadAdmission(event, admission)
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

func ExistingRunRoot(
	ctx context.Context,
	db *sql.DB,
	dialect authoractivityfixture.Dialect,
	eventID string,
	runID string,
	eventType events.EventType,
	producer events.ProducerIdentity,
	payload []byte,
	envelope events.EventEnvelope,
	createdAt time.Time,
) (event events.Event, err error) {
	facts, err := eventFacts(eventID, eventType, producer, payload, envelope, createdAt)
	if err != nil {
		return event, err
	}
	event, err = events.NewExistingRunRootIngressEvent(events.ExistingRunRootIngressEventInput{Facts: facts, RunID: runID})
	if err != nil {
		return event, err
	}
	return event, Insert(ctx, db, dialect, event)
}

func Child(
	ctx context.Context,
	db *sql.DB,
	dialect authoractivityfixture.Dialect,
	eventID string,
	runID string,
	parentEventID string,
	eventType events.EventType,
	producer events.ProducerIdentity,
	payload []byte,
	envelope events.EventEnvelope,
	createdAt time.Time,
) (event events.Event, err error) {
	facts, err := eventFacts(eventID, eventType, producer, payload, envelope, createdAt)
	if err != nil {
		return event, err
	}
	event, err = events.NewChildEvent(events.ChildEventInput{
		Facts:   facts,
		Lineage: events.EventLineage{RunID: runID, ParentEventID: parentEventID, ExecutionMode: executionmode.Live},
	})
	if err != nil {
		return event, err
	}
	return event, Insert(ctx, db, dialect, event)
}

func DiagnosticDirect(
	ctx context.Context,
	db *sql.DB,
	dialect authoractivityfixture.Dialect,
	eventID string,
	producerID string,
	payload []byte,
	createdAt time.Time,
) (event events.Event, err error) {
	return DiagnosticDirectForRun(ctx, db, dialect, eventID, "", "", producerID, payload, createdAt)
}

func DiagnosticDirectForRun(
	ctx context.Context,
	db *sql.DB,
	dialect authoractivityfixture.Dialect,
	eventID string,
	runID string,
	parentEventID string,
	producerID string,
	payload []byte,
	createdAt time.Time,
) (event events.Event, err error) {
	producer, err := events.NewProducerIdentity(events.EventProducerPlatform, producerID)
	if err != nil {
		return event, err
	}
	facts, err := eventFacts(eventID, events.EventTypePlatformRuntimeLog, producer, payload, events.EventEnvelope{Scope: events.EventScopeGlobal}, createdAt)
	if err != nil {
		return event, err
	}
	inputFacts := events.EventFacts{
		ID: facts.ID, Type: facts.Type, Producer: facts.Producer,
		Payload: facts.Payload, Envelope: facts.Envelope, RoutingSource: facts.RoutingSource,
		CreatedAt: facts.CreatedAt, ExecutionMode: facts.ExecutionMode,
	}
	switch {
	case strings.TrimSpace(parentEventID) != "":
		event, err = events.NewCausalDiagnosticDirectEvent(events.CausalRuntimeEventInput{Facts: inputFacts, Lineage: events.EventLineage{
			RunID: runID, ParentEventID: parentEventID, ExecutionMode: facts.ExecutionMode,
		}})
	case strings.TrimSpace(runID) != "":
		event, err = events.NewRunScopedDiagnosticDirectEvent(events.RunScopedRuntimeEventInput{Facts: inputFacts, RunID: runID})
	default:
		event, err = events.NewStandaloneDiagnosticDirectEvent(events.StandaloneRuntimeEventInput{Facts: inputFacts})
	}
	if err != nil {
		return event, err
	}
	return event, Insert(ctx, db, dialect, event)
}
