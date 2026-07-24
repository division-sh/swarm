package store

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/internal/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/eventrecord/sqlite"
)

type diagnosticRuntimeLogFixtureStore interface {
	commitRuntimeLogEvent(context.Context, events.AdmittedEvent) (runtimebus.EventAppendOutcome, error)
}

type semanticEventFixtureStore interface {
	eventCommitTxStore
}

type selectedForkEventFixtureStore interface {
	CommitSelectedForkEvent(context.Context, CommitSelectedForkEventRequest) (runtimebus.EventAppendOutcome, error)
}

func commitSemanticEventFixture(ctx context.Context, store any, event events.Event) error {
	_, err := commitSemanticEventFixtureOutcome(ctx, store, event, nil, runtimepipelineobligation.ScopeDirect)
	return err
}

func commitSemanticEventFixtureWithAgents(ctx context.Context, store any, event events.Event, agentIDs []string) error {
	routes := make([]events.DeliveryRoute, 0, len(agentIDs))
	for _, agentID := range agentIDs {
		routes = append(routes, events.DeliveryRoute{SubscriberType: "agent", SubscriberID: agentID})
	}
	scope := runtimepipelineobligation.ScopeDirect
	if len(routes) > 0 {
		scope = runtimepipelineobligation.ScopeSubscribed
	}
	_, err := commitSemanticEventFixtureOutcome(ctx, store, event, routes, scope)
	return err
}

func commitSemanticEventFixtureWithRoutes(ctx context.Context, store any, event events.Event, routes []events.DeliveryRoute) error {
	_, err := commitSemanticEventFixtureOutcome(ctx, store, event, routes, runtimepipelineobligation.ScopeSubscribed)
	return err
}

func commitSemanticParentFixture(ctx context.Context, store any, runID, parentEventID string, createdAt time.Time) error {
	parent := eventtest.RunCreatingRootIngress(
		parentEventID, "test.fixture_parent", "fixture", "", []byte(`{}`), 0,
		runID, "", events.EventEnvelope{}, createdAt,
	)
	if err := commitSemanticEventFixture(ctx, store, parent); err != nil {
		return err
	}
	owner, err := pipelineObligationFixtureOwner(store)
	if err != nil {
		return err
	}
	work, err := owner.ClaimEvent(ctx, parentEventID, runtimepipelineobligation.PurposeRecovery)
	if err != nil {
		return err
	}
	return owner.Settle(ctx, work.Claim, runtimepipelineobligation.Acknowledged("pipeline_persisted"))
}

func commitSemanticParentFixtureTx(ctx context.Context, store eventCommitTxStore, tx *sql.Tx, runID, parentEventID string, createdAt time.Time) error {
	parent := eventtest.RunCreatingRootIngress(
		parentEventID, "test.fixture_parent", "fixture", "", []byte(`{}`), 0,
		runID, "", events.EventEnvelope{}, createdAt,
	)
	if err := commitSemanticEventFixtureTx(ctx, store, tx, parent); err != nil {
		return err
	}
	postgres := false
	switch store.(type) {
	case *PostgresStore:
		postgres = true
	case *SQLiteRuntimeStore:
	default:
		return fmt.Errorf("semantic parent fixture store %T is unsupported", store)
	}
	return writePipelineDispositionTx(
		ctx,
		tx,
		parentEventID,
		runtimepipelineobligation.PurposeRecovery,
		runtimepipelineobligation.Acknowledged("pipeline_persisted"),
		postgres,
		createdAt,
	)
}

func pipelineObligationFixtureOwner(selectedStore any) (runtimepipelineobligation.Store, error) {
	switch selected := selectedStore.(type) {
	case *PostgresStore:
		return selected.PipelineObligations(), nil
	case *SQLiteRuntimeStore:
		return selected.PipelineObligations(), nil
	default:
		return nil, fmt.Errorf("semantic fixture store %T has no pipeline obligation owner", selectedStore)
	}
}

func acknowledgePipelineEventFixture(ctx context.Context, selectedStore any, eventID string) error {
	owner, err := pipelineObligationFixtureOwner(selectedStore)
	if err != nil {
		return err
	}
	work, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery)
	if err != nil {
		return err
	}
	return owner.Settle(ctx, work.Claim, runtimepipelineobligation.Acknowledged("pipeline_persisted"))
}

func commitDiagnosticRuntimeLogFixture(ctx context.Context, store diagnosticRuntimeLogFixtureStore, event events.Event) error {
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return err
	}
	_, err = store.commitRuntimeLogEvent(ctx, admitted)
	return err
}

func commitDiagnosticRuntimeLogFixtureTx(ctx context.Context, store eventCommitTxStore, tx *sql.Tx, event events.Event) error {
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return err
	}
	if admitted.Class() != events.EventAdmissionDiagnosticDirect || admitted.Event().Type() != events.EventTypePlatformRuntimeLog {
		return fmt.Errorf("runtime-log fixture requires a diagnostic_direct platform.runtime_log event")
	}
	outcome, err := store.appendAdmittedEventTxOutcome(ctx, tx, admitted)
	if err != nil {
		return err
	}
	if outcome != runtimebus.EventAppendInserted {
		return fmt.Errorf("runtime-log fixture append outcome = %d, want inserted", outcome)
	}
	return nil
}

func commitSelectedForkEventFixture(
	ctx context.Context,
	store selectedForkEventFixtureStore,
	event events.Event,
	lineage RunForkSelectedContractExecutionLineage,
) error {
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return err
	}
	ctx, release, err := semanticEventFixtureContext(ctx, store, admitted.Event())
	if err != nil {
		return err
	}
	defer release()
	outcome, err := store.CommitSelectedForkEvent(ctx, CommitSelectedForkEventRequest{
		Commit: runtimebus.CommitPublishRequest{
			Event:       admitted,
			ReplayScope: runtimepipelineobligation.ScopeDirect,
		},
		Lineage: lineage,
	})
	if err != nil {
		return err
	}
	if outcome != runtimebus.EventAppendInserted {
		return fmt.Errorf("selected-fork fixture append outcome = %d, want inserted", outcome)
	}
	return nil
}

func commitDeliveryReplayEventFixture(
	ctx context.Context,
	store *PostgresStore,
	source events.Event,
	forkRunID string,
	sourceDeliveryID string,
	forkDeliveryID string,
	subscriberType string,
	subscriberID string,
	now time.Time,
) error {
	forkEventID := deterministicRunForkReplayEventID(forkRunID, source.ID())
	replayed, err := projectRunForkReplayEvent(source, runForkActivationLineage{
		SourceRunID: source.RunID(),
		ForkRunID:   forkRunID,
	}, forkEventID, now)
	if err != nil {
		return err
	}
	ctx, release, err := semanticEventFixtureContext(ctx, store, replayed.Event())
	if err != nil {
		return err
	}
	defer release()
	return store.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		outcome, err := store.appendAdmittedEventTxOutcome(txctx, tx, replayed)
		if err != nil {
			return err
		}
		if outcome != runtimebus.EventAppendInserted {
			return fmt.Errorf("delivery-replay fixture append outcome = %d, want inserted", outcome)
		}
		if err := insertCommittedPipelineScopeTx(txctx, tx, forkEventID, runtimepipelineobligation.ScopeDirect, true, time.Now().UTC()); err != nil {
			return err
		}
		route := events.DeliveryRoute{SubscriberType: subscriberType, SubscriberID: subscriberID}
		obligation, err := runtimedelivery.NewObligation(forkEventID, forkRunID, route)
		if err != nil {
			return err
		}
		if forkDeliveryID != "" && forkDeliveryID != obligation.DeliveryID() {
			return fmt.Errorf("delivery-replay fixture delivery id %s does not match canonical id %s", forkDeliveryID, obligation.DeliveryID())
		}
		inserted, err := insertRunForkReplayDelivery(txctx, tx, runForkActivationLineage{
			SourceRunID: source.RunID(),
			ForkRunID:   forkRunID,
		}, RunForkHistoricalReplayExecutableWork{
			Fact:             RunForkHistoricalReplayFactEventDeliveries,
			SourceEventID:    source.ID(),
			SourceDeliveryID: sourceDeliveryID,
			SubscriberType:   subscriberType,
			SubscriberID:     subscriberID,
			ReasonCode:       "semantic_fixture",
		}, source.ID(), forkEventID, obligation, now)
		if err != nil {
			return err
		}
		if !inserted {
			return fmt.Errorf("delivery-replay fixture delivery %s was not inserted", forkDeliveryID)
		}
		return nil
	})
}

func commitSemanticEventFixtureOutcome(
	ctx context.Context,
	store any,
	event events.Event,
	routes []events.DeliveryRoute,
	scope runtimepipelineobligation.CommittedScope,
) (runtimebus.EventAppendOutcome, error) {
	admitted, err := events.AdmitForPublish(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return commitAdmittedSemanticEventFixtureOutcome(ctx, store, admitted, routes, scope)
}

func commitAdmittedSemanticEventFixtureOutcome(
	ctx context.Context,
	store any,
	admitted events.AdmittedEvent,
	routes []events.DeliveryRoute,
	scope runtimepipelineobligation.CommittedScope,
) (runtimebus.EventAppendOutcome, error) {
	if admitted.Class() == events.EventAdmissionSelectedForkReplay {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("selected-fork replay events require their closed named persistence operation")
	}
	req := runtimebus.CommitPublishRequest{Event: admitted, DeliveryRoutes: events.NormalizeDeliveryRoutes(routes), ReplayScope: scope}
	ctx, release, err := semanticEventFixtureContext(ctx, store, admitted.Event())
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	defer release()
	var outcome runtimebus.EventAppendOutcome
	commit := func(txctx context.Context, tx *sql.Tx, selected eventCommitTxStore) error {
		var err error
		outcome, err = selected.appendAdmittedEventTxOutcome(txctx, tx, admitted)
		if err != nil || outcome == runtimebus.EventAppendExactDuplicate {
			return err
		}
		return (sqlPublishCommitter{tx: tx, store: selected}).commitInitialSideEffects(txctx, req)
	}
	switch selected := store.(type) {
	case *PostgresStore:
		err = selected.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error { return commit(txctx, tx, selected) })
	case *SQLiteRuntimeStore:
		err = selected.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error { return commit(txctx, tx, selected) })
	default:
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("semantic event fixture store %T is unsupported", store)
	}
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return outcome, nil
}

func commitSemanticEventFixtureTx(ctx context.Context, store eventCommitTxStore, tx *sql.Tx, event events.Event) error {
	return commitSemanticEventFixtureWithRoutesTx(ctx, store, tx, event, nil)
}

func commitSemanticEventFixtureWithRoutesTx(ctx context.Context, store eventCommitTxStore, tx *sql.Tx, event events.Event, routes []events.DeliveryRoute) error {
	admitted, err := events.AdmitForPublish(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return err
	}
	if admitted.Class() == events.EventAdmissionSelectedForkReplay {
		return fmt.Errorf("selected-fork replay events require their closed named persistence operation")
	}
	ctx, release, err := semanticEventFixtureContext(ctx, store, admitted.Event())
	if err != nil {
		return err
	}
	defer release()
	outcome, err := store.appendAdmittedEventTxOutcome(ctx, tx, admitted)
	if err != nil || outcome == runtimebus.EventAppendExactDuplicate {
		return err
	}
	scope := runtimepipelineobligation.ScopeDirect
	if len(routes) > 0 {
		scope = runtimepipelineobligation.ScopeSubscribed
	}
	return (sqlPublishCommitter{tx: tx, store: store}).commitInitialSideEffects(ctx, runtimebus.CommitPublishRequest{
		Event: admitted, DeliveryRoutes: events.NormalizeDeliveryRoutes(routes), ReplayScope: scope,
	})
}

// insertCanonicalEventRecordFixture seeds an already-persisted event precondition
// without invoking active-run or initial-side-effect owners. It still uses the
// constructed/admitted/record boundary and is not a runtime writer.
func insertCanonicalEventRecordFixture(ctx context.Context, selectedStore any, event events.Event) error {
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
	switch selected := selectedStore.(type) {
	case *PostgresStore:
		inserted, err = eventrecordpostgres.Insert(ctx, selected.DB, record)
	case *SQLiteRuntimeStore:
		inserted, err = eventrecordsqlite.Insert(ctx, selected.DB, record)
	default:
		return fmt.Errorf("canonical event record fixture store %T is unsupported", selectedStore)
	}
	if err != nil {
		return err
	}
	if !inserted {
		return fmt.Errorf("canonical event record fixture %s was not inserted", record.EventID)
	}
	return nil
}

func insertPostgresCanonicalEventRecordFixture(ctx context.Context, db *sql.DB, event events.Event) error {
	if db == nil {
		return fmt.Errorf("postgres canonical event record fixture requires a database")
	}
	return insertCanonicalEventRecordFixture(ctx, &PostgresStore{DB: db}, event)
}

func insertPostgresCanonicalEventRecordFixtureTx(ctx context.Context, tx *sql.Tx, event events.Event) error {
	if tx == nil {
		return fmt.Errorf("postgres canonical event record fixture requires a transaction")
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
	inserted, err := eventrecordpostgres.Insert(ctx, tx, record)
	if err != nil {
		return err
	}
	if !inserted {
		return fmt.Errorf("canonical event record fixture %s was not inserted", record.EventID)
	}
	return nil
}

func seedPostgresSemanticEventRecordFixtureTx(
	t testing.TB,
	ctx context.Context,
	tx *sql.Tx,
	eventID string,
	runID string,
	eventType events.EventType,
	producerType events.EventProducerType,
	producerID string,
	entityID string,
	flowInstance string,
	createdAt time.Time,
) events.Event {
	t.Helper()
	event := semanticEventRecordFixture(
		eventID, runID, eventType, eventtest.Producer(producerType, producerID), []byte(`{}`),
		semanticEventRecordFixtureEnvelope(entityID, flowInstance), createdAt,
	)
	if err := insertPostgresCanonicalEventRecordFixtureTx(ctx, tx, event); err != nil {
		t.Fatalf("seed canonical event record %s in transaction: %v", eventID, err)
	}
	return event
}

func insertPostgresSemanticEventRecordFixture(
	ctx context.Context,
	db *sql.DB,
	eventID string,
	runID string,
	eventType events.EventType,
	producer events.ProducerIdentity,
	payload []byte,
	envelope events.EventEnvelope,
	createdAt time.Time,
) (events.Event, error) {
	event := semanticEventRecordFixture(eventID, runID, eventType, producer, payload, envelope, createdAt)
	return event, insertPostgresCanonicalEventRecordFixture(ctx, db, event)
}

func seedPostgresSemanticEventRecordFixture(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	eventID string,
	runID string,
	eventType events.EventType,
	producerType events.EventProducerType,
	producerID string,
	entityID string,
	flowInstance string,
	createdAt time.Time,
) events.Event {
	t.Helper()
	envelope := semanticEventRecordFixtureEnvelope(entityID, flowInstance)
	event, err := insertPostgresSemanticEventRecordFixture(
		ctx, db, eventID, runID, eventType, eventtest.Producer(producerType, producerID), []byte(`{}`), envelope, createdAt,
	)
	if err != nil {
		t.Fatalf("seed canonical event record %s: %v", eventID, err)
	}
	return event
}

func semanticEventRecordFixture(
	eventID, runID string,
	eventType events.EventType,
	producer events.ProducerIdentity,
	payload []byte,
	envelope events.EventEnvelope,
	createdAt time.Time,
) events.Event {
	if events.IsDiagnosticDirectEventType(eventType) {
		return eventtest.DiagnosticDirect(eventID, eventType, producer.ID(), "", payload, 0, runID, "", envelope, createdAt)
	}
	switch producer.Type() {
	case events.EventProducerExternal:
		return eventtest.RunCreatingRootIngress(eventID, eventType, producer.ID(), "", payload, 0, runID, "", envelope, createdAt)
	case events.EventProducerPlatform:
		return eventtest.PersistedRuntimeControlForProducer(eventID, eventType, producer, "", payload, 0, runID, "", envelope, createdAt)
	case events.EventProducerAgent, events.EventProducerNode:
		return eventtest.PersistedChildForProducer(eventID, eventType, producer, "", payload, 0, runID, eventtest.UUID("semantic-parent:"+eventID), envelope, createdAt)
	default:
		panic("unsupported semantic event fixture producer")
	}
}

func seedPostgresChildEventRecordFixture(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	eventID string,
	runID string,
	parentEventID string,
	eventType events.EventType,
	producerType events.EventProducerType,
	producerID string,
	entityID string,
	flowInstance string,
	payload []byte,
	createdAt time.Time,
) events.Event {
	t.Helper()
	event := eventtest.PersistedChildForProducer(
		eventID, eventType, eventtest.Producer(producerType, producerID), "", payload, 0,
		runID, parentEventID, semanticEventRecordFixtureEnvelope(entityID, flowInstance), createdAt,
	)
	if err := insertPostgresCanonicalEventRecordFixture(ctx, db, event); err != nil {
		t.Fatalf("seed canonical child event record %s: %v", eventID, err)
	}
	return event
}

func seedPostgresRuntimeLogEventRecordFixture(
	t testing.TB,
	ctx context.Context,
	store *PostgresStore,
	eventID string,
	runID string,
	parentEventID string,
	payload []byte,
	createdAt time.Time,
) events.Event {
	t.Helper()
	event := eventtest.DiagnosticDirect(
		eventID, events.EventTypePlatformRuntimeLog, "runtime", "", payload, 0,
		runID, parentEventID, events.EventEnvelope{Scope: events.EventScopeGlobal}, createdAt,
	)
	if err := commitDiagnosticRuntimeLogFixture(ctx, store, event); err != nil {
		t.Fatalf("seed canonical runtime-log event record %s: %v", eventID, err)
	}
	return event
}

func semanticEventRecordFixtureEnvelope(entityID, flowInstance string) events.EventEnvelope {
	envelope := events.EventEnvelope{EntityID: entityID, FlowInstance: flowInstance}
	switch {
	case entityID != "":
		envelope.Scope = events.EventScopeEntity
	case flowInstance != "":
		envelope.Scope = events.EventScopeFlow
	default:
		envelope.Scope = events.EventScopeGlobal
	}
	return envelope
}

type semanticFixtureCatalogStore interface {
	RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
	authorActivityEventDescriptor(runtimeauthoractivity.Scope, string) (runtimeauthoractivity.EventDescriptor, bool)
	authorActivityEventCatalogRegistered(runtimeauthoractivity.Scope) bool
}

func semanticEventFixtureContext(ctx context.Context, selectedStore any, event events.Event) (context.Context, func(), error) {
	store, ok := selectedStore.(semanticFixtureCatalogStore)
	if !ok {
		return ctx, func() {}, fmt.Errorf("semantic event fixture store %T has no author activity catalog", selectedStore)
	}
	scope, scoped := runtimeauthoractivity.ScopeFromContext(ctx)
	if !scoped || scope.Kind != runtimeauthoractivity.ScopeBundle {
		scope = runtimeauthoractivity.BundleScope(event.ID(), "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
		ctx = runtimeauthoractivity.WithScope(ctx, scope)
	}
	if store.authorActivityEventCatalogRegistered(scope) {
		descriptor, ok := store.authorActivityEventDescriptor(scope, string(event.Type()))
		if !ok {
			descriptor = runtimeauthoractivity.EventDescriptor{
				EventType: string(event.Type()), Disposition: runtimeauthoractivity.StoryDifferent,
			}
		}
		ctx, err := runtimeauthoractivity.WithResolvedEventDescriptor(ctx, scope, descriptor)
		return ctx, func() {}, err
	}
	lease, err := store.RegisterAuthorActivityEventCatalog(scope, []runtimeauthoractivity.EventDescriptor{{
		EventType: string(event.Type()), Disposition: runtimeauthoractivity.StoryDifferent,
	}})
	if err != nil {
		return ctx, func() {}, err
	}
	return ctx, lease.Release, nil
}
