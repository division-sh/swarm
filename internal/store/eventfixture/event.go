// Package eventfixture seeds canonical event records for integration tests
// that cannot import the store package without creating an import cycle.
// Runtime code must use the closed named store operations instead.
package eventfixture

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/store/internal/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/eventrecord/sqlite"
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
			routingSource, err = events.NewRuntimeRoutingSource(source.FlowID, source.FlowInstance, source.EntityID)
		} else {
			routingSource, err = events.NewDeclaredIngressRoutingSource(source.FlowID, source.FlowInstance, source.EntityID, "eventfixture")
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

func Load(ctx context.Context, q Executor, dialect runtimeauthoractivity.Dialect, eventID string) (event events.Event, err error) {
	if q == nil {
		return event, fmt.Errorf("canonical event fixture requires a database")
	}
	var (
		record eventrecord.Record
		found  bool
	)
	switch dialect {
	case runtimeauthoractivity.DialectPostgres:
		record, found, err = eventrecordpostgres.Load(ctx, q, eventID)
	case runtimeauthoractivity.DialectSQLite:
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

func Insert(ctx context.Context, exec Executor, dialect runtimeauthoractivity.Dialect, event events.Event) error {
	if exec == nil {
		return fmt.Errorf("canonical event fixture requires a database")
	}
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return err
	}
	if admitted.Class() == events.EventAdmissionSelectedForkReplay {
		return fmt.Errorf("selected-fork replay fixture requires exact lineage persistence")
	}
	record, err := eventrecord.FromAdmitted(admitted)
	if err != nil {
		return err
	}
	var inserted bool
	switch dialect {
	case runtimeauthoractivity.DialectPostgres:
		inserted, err = eventrecordpostgres.Insert(ctx, exec, record)
	case runtimeauthoractivity.DialectSQLite:
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
	case runtimeauthoractivity.DialectPostgres:
		existing, found, err = eventrecordpostgres.Load(ctx, exec, record.EventID)
	case runtimeauthoractivity.DialectSQLite:
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

func Root(
	ctx context.Context,
	db *sql.DB,
	dialect runtimeauthoractivity.Dialect,
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
	event, err = events.NewRootIngressEvent(events.RootIngressEventInput{Facts: facts, RunID: runID})
	if err != nil {
		return event, err
	}
	return event, Insert(ctx, db, dialect, event)
}

func Child(
	ctx context.Context,
	db *sql.DB,
	dialect runtimeauthoractivity.Dialect,
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
	dialect runtimeauthoractivity.Dialect,
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
	dialect runtimeauthoractivity.Dialect,
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
	event, err = events.NewDiagnosticDirectEvent(events.DiagnosticDirectEventInput{
		Facts: events.EventFacts{
			ID: facts.ID, Type: facts.Type, Producer: facts.Producer,
			Payload: facts.Payload, Envelope: facts.Envelope, RoutingSource: facts.RoutingSource,
			CreatedAt: facts.CreatedAt, ExecutionMode: facts.ExecutionMode,
		},
		RunID: runID, ParentEventID: parentEventID,
	})
	if err != nil {
		return event, err
	}
	return event, Insert(ctx, db, dialect, event)
}
