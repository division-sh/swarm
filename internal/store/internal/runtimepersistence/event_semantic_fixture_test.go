package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	deliveryadapter "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

type diagnosticRuntimeLogFixtureStore interface {
	CommitRuntimeLogEvent(context.Context, events.AdmittedEvent) (runtimebus.EventAppendOutcome, error)
}

type semanticEventFixtureStore interface {
	eventCommitTxStore
}

type selectedForkEventFixtureStore interface {
	CommitSelectedForkEvent(context.Context, runtimebus.CommitSelectedForkEventRequest) (runtimebus.CommittedSelectedForkEvent, error)
}

func commitSelectedForkEventOutcome(ctx context.Context, store selectedForkEventFixtureStore, req runtimebus.CommitSelectedForkEventRequest) (runtimebus.EventAppendOutcome, error) {
	committed, err := store.CommitSelectedForkEvent(ctx, req)
	return committed.AppendOutcome, err
}

func commitSemanticEventFixture(ctx context.Context, store any, event events.Event) error {
	_, err := commitSemanticEventFixtureOutcome(ctx, store, event, nil, runtimepipelineobligation.ScopeDirect)
	return err
}

func commitSemanticPipelineProcessedEventFixture(ctx context.Context, store any, event events.Event) error {
	disposition := runtimepipelineobligation.Acknowledged("pipeline_persisted")
	_, err := commitSemanticEventFixtureOutcomeWithDisposition(
		ctx, store, event, nil, runtimepipelineobligation.ScopeDirect, &disposition,
	)
	return err
}

func commitSemanticEventFixtureWithAgents(ctx context.Context, store any, event events.Event, agentIDs []string) error {
	routes := make([]events.DeliveryRoute, 0, len(agentIDs))
	for _, agentID := range agentIDs {
		routes = append(routes, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(agentID), AgentIdentity: mustTestAgentIdentity(agentID, "fixture/"+agentID)})
	}
	scope := runtimepipelineobligation.ScopeDirect
	if len(routes) > 0 {
		scope = runtimepipelineobligation.ScopeSubscribed
	}
	_, err := commitSemanticEventFixtureOutcome(ctx, store, event, routes, scope)
	return err
}

func commitSemanticEventFixtureWithRoutes(ctx context.Context, store any, event events.Event, routes []events.DeliveryRoute) error {
	routes = canonicalDeliveryFixtureRoutes(routes)
	_, err := commitSemanticEventFixtureOutcome(ctx, store, event, routes, runtimepipelineobligation.ScopeSubscribed)
	return err
}

func canonicalDeliveryFixtureRoutes(routes []events.DeliveryRoute) []events.DeliveryRoute {
	canonical := make([]events.DeliveryRoute, len(routes))
	for i, route := range routes {
		canonical[i] = canonicalDeliveryFixtureRouteValue(route)
	}
	return canonical
}

func commitSemanticParentFixture(ctx context.Context, store any, runID, parentEventID string, createdAt time.Time) error {
	parent := eventtest.ExistingRunRootIngress(
		parentEventID, "test.fixture_parent", "fixture", "", []byte(`{}`), 0,
		runID, events.EventEnvelope{}, createdAt,
	)
	return commitSemanticPipelineProcessedEventFixture(ctx, store, parent)
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
	_, err = owner.Settle(ctx, work.Claim, runtimepipelineobligation.Acknowledged("pipeline_persisted"))
	return err
}

func commitDiagnosticRuntimeLogFixture(ctx context.Context, store diagnosticRuntimeLogFixtureStore, event events.Event) error {
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return err
	}
	_, err = store.CommitRuntimeLogEvent(ctx, admitted)
	return err
}

func commitDiagnosticRuntimeLogFixtureTx(ctx context.Context, store eventCommitTxStore, tx *sql.Tx, story *privateauthoractivity.Mutation, event events.Event) error {
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return err
	}
	if admitted.Class() != events.EventAdmissionDiagnosticDirect || admitted.Event().Type() != events.EventTypePlatformRuntimeLog {
		return fmt.Errorf("runtime-log fixture requires a diagnostic_direct platform.runtime_log event")
	}
	outcome, err := store.AppendAdmittedEventTxOutcome(ctx, tx, runtimeAuthorActivityMutation(story), admitted, testRouteSettlement(admitted.Event(), nil))
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
	lineage runfork.RunForkSelectedContractExecutionLineage,
) (err error) {
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return err
	}
	ctx, release, err := semanticEventFixtureContext(ctx, store, admitted.Event())
	if err != nil {
		return err
	}
	defer release()
	owner := pipelineObligationOwnerForFixture(store)
	claim, err := owner.ClaimPublication(ctx, admitted.Event().ID())
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, owner.Release(context.WithoutCancel(ctx), claim))
	}()
	outcome, err := commitSelectedForkEventOutcome(ctx, store, runtimebus.CommitSelectedForkEventRequest{
		Commit: runtimebus.CommitPublishRequest{
			Event:           admitted,
			RouteSettlement: testRouteSettlement(admitted.Event(), nil),
			ReplayScope:     runtimepipelineobligation.ScopeDirect,
			PipelineClaim:   claim,
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
	var recipient events.DeliveryRecipient
	switch subscriberType {
	case "node":
		recipient, err = events.NewNodeDeliveryRecipient(mustPersistenceRootNode(subscriberID))
	case "agent":
		recipient, err = events.NewAgentDeliveryRecipient(subscriberID)
	default:
		return fmt.Errorf("delivery-replay fixture subscriber type %q is unsupported", subscriberType)
	}
	if err != nil {
		return err
	}
	route := canonicalDeliveryFixtureRouteValue(events.DeliveryRoute{Recipient: recipient})
	ctx, release, err := semanticEventFixtureContext(ctx, store, replayed.Event())
	if err != nil {
		return err
	}
	defer release()
	return store.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		story, err := eventFixtureStory(txctx)
		if err != nil {
			return err
		}
		outcome, err := store.AppendAdmittedEventTxOutcome(txctx, tx, story, replayed, testHistoricalReplaySettlement([]events.DeliveryRoute{route}))
		if err != nil {
			return err
		}
		if outcome != runtimebus.EventAppendInserted {
			return fmt.Errorf("delivery-replay fixture append outcome = %d, want inserted", outcome)
		}
		if err := insertCommittedPipelineScopeTx(txctx, tx, forkEventID, runtimepipelineobligation.ScopeDirect, true, time.Now().UTC()); err != nil {
			return err
		}
		authority, err := deliveryFixtureAuthorityForRun(txctx, tx, deliveryadapter.DialectPostgres, forkRunID)
		if err != nil {
			return err
		}
		obligation, err := runtimedelivery.NewObligation(forkEventID, forkRunID, route, authority)
		if err != nil {
			return err
		}
		if forkDeliveryID != "" && forkDeliveryID != obligation.DeliveryID() {
			return fmt.Errorf("delivery-replay fixture delivery id %s does not match canonical id %s", forkDeliveryID, obligation.DeliveryID())
		}
		inserted, err := insertRunForkReplayDelivery(txctx, tx, runForkActivationLineage{
			SourceRunID: source.RunID(),
			ForkRunID:   forkRunID,
		}, runfork.RunForkHistoricalReplayExecutableWork{
			Fact:             runfork.RunForkHistoricalReplayFactEventDeliveries,
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
		_, err = finalizePostgresRunForkTestRevision(txctx, tx, forkRunID,
			runforkrevision.FamilyEvents,
			runforkrevision.FamilyEventDeliveries,
			runforkrevision.FamilyCommittedReplayScopes,
		)
		return err
	})
}

func commitSemanticEventFixtureOutcome(
	ctx context.Context,
	store any,
	event events.Event,
	routes []events.DeliveryRoute,
	scope runtimepipelineobligation.CommittedScope,
) (runtimebus.EventAppendOutcome, error) {
	return commitSemanticEventFixtureOutcomeWithDisposition(ctx, store, event, routes, scope, nil)
}

func commitSemanticEventFixtureOutcomeWithDisposition(
	ctx context.Context,
	store any,
	event events.Event,
	routes []events.DeliveryRoute,
	scope runtimepipelineobligation.CommittedScope,
	disposition *runtimepipelineobligation.Disposition,
) (runtimebus.EventAppendOutcome, error) {
	admitted, err := events.AdmitForPublish(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return commitAdmittedSemanticEventFixtureOutcomeWithDisposition(ctx, store, admitted, routes, scope, disposition)
}

func commitAdmittedSemanticEventFixtureOutcome(
	ctx context.Context,
	store any,
	admitted events.AdmittedEvent,
	routes []events.DeliveryRoute,
	scope runtimepipelineobligation.CommittedScope,
) (outcome runtimebus.EventAppendOutcome, err error) {
	return commitAdmittedSemanticEventFixtureOutcomeWithDisposition(ctx, store, admitted, routes, scope, nil)
}

func commitAdmittedSemanticEventFixtureOutcomeWithDisposition(
	ctx context.Context,
	store any,
	admitted events.AdmittedEvent,
	routes []events.DeliveryRoute,
	scope runtimepipelineobligation.CommittedScope,
	disposition *runtimepipelineobligation.Disposition,
) (outcome runtimebus.EventAppendOutcome, err error) {
	if admitted.Class() == events.EventAdmissionSelectedForkReplay {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("selected-fork replay events require their closed named persistence operation")
	}
	owner := pipelineObligationOwnerForFixture(store)
	claim, err := owner.ClaimPublication(ctx, admitted.ID())
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	defer func() {
		err = errors.Join(err, owner.Release(context.WithoutCancel(ctx), claim))
	}()
	req := runtimebus.CommitPublishRequest{
		Event: admitted, DeliveryRoutes: events.NormalizeDeliveryRoutes(routes), ReplayScope: scope, PipelineClaim: claim,
		Disposition: disposition,
	}
	ctx, release, err := semanticEventFixtureContext(ctx, store, admitted.Event())
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	defer release()
	commit := func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, selected eventCommitTxStore) error {
		runtimeStory := runtimeAuthorActivityMutation(story)
		var appendErr error
		outcome, appendErr = selected.AppendAdmittedEventTxOutcome(txctx, tx, runtimeStory, admitted, testRouteSettlement(admitted.Event(), req.DeliveryRoutes))
		if appendErr != nil || outcome == runtimebus.EventAppendExactDuplicate {
			return appendErr
		}
		if len(req.DeliveryRoutes) > 0 {
			req.DeliveryAuthority, appendErr = semanticEventFixtureDeliveryAuthority(
				txctx,
				tx,
				store,
				admitted.Event().RunID(),
			)
			if appendErr != nil {
				return appendErr
			}
		}
		_, appendErr = (sqlPublishCommitter{tx: tx, store: selected, story: runtimeStory}).commitInitialSideEffectEvidence(txctx, req, true)
		return appendErr
	}
	switch selected := store.(type) {
	case *PostgresStore:
		err = selected.runPrivateAuthorActivityMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
			if err := commit(txctx, tx, story, selected); err != nil {
				return err
			}
			_, err := finalizePostgresRunForkTestRevision(txctx, tx, admitted.Event().RunID(),
				runforkrevision.FamilyEvents,
				runforkrevision.FamilyEventDeliveries,
				runforkrevision.FamilyCommittedReplayScopes,
				runforkrevision.FamilyEventReceipts,
				runforkrevision.FamilyReplyContexts,
			)
			return err
		})
	case *SQLiteRuntimeStore:
		err = selected.runPrivateAuthorActivityMutation(ctx, "sqlite semantic event fixture", func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
			if err := commit(txctx, tx, story, selected); err != nil {
				return err
			}
			_, err := finalizeSQLiteRunForkTestRevision(txctx, tx, admitted.Event().RunID(),
				runforkrevision.FamilyEvents,
				runforkrevision.FamilyEventDeliveries,
				runforkrevision.FamilyCommittedReplayScopes,
				runforkrevision.FamilyEventReceipts,
				runforkrevision.FamilyReplyContexts,
			)
			return err
		})
	default:
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("semantic event fixture store %T is unsupported", store)
	}
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return outcome, nil
}

func commitSemanticEventFixtureWithRoutesStoryTx(ctx context.Context, store eventCommitTxStore, tx *sql.Tx, story runtimeauthoractivity.Mutation, event events.Event, routes []events.DeliveryRoute) (err error) {
	routes = canonicalDeliveryFixtureRoutes(routes)
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
	owner := pipelineObligationOwnerForFixture(store)
	claim, err := owner.ClaimPublication(ctx, admitted.ID())
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, owner.Release(context.WithoutCancel(ctx), claim))
	}()
	outcome, err := store.AppendAdmittedEventTxOutcome(ctx, tx, story, admitted, testRouteSettlement(admitted.Event(), routes))
	if err != nil || outcome == runtimebus.EventAppendExactDuplicate {
		return err
	}
	scope := runtimepipelineobligation.ScopeDirect
	if len(routes) > 0 {
		scope = runtimepipelineobligation.ScopeSubscribed
	}
	var authority runtimedelivery.ExecutionAuthority
	if len(routes) > 0 {
		authority, err = semanticEventFixtureDeliveryAuthority(ctx, tx, store, admitted.Event().RunID())
		if err != nil {
			return err
		}
	}
	_, err = (sqlPublishCommitter{tx: tx, store: store, story: story}).commitInitialSideEffectEvidence(ctx, runtimebus.CommitPublishRequest{
		Event: admitted, DeliveryRoutes: events.NormalizeDeliveryRoutes(routes), DeliveryAuthority: authority,
		ReplayScope: scope, PipelineClaim: claim,
	}, true)
	return err
}

func semanticEventFixtureDeliveryAuthority(
	ctx context.Context,
	tx *sql.Tx,
	selected any,
	runID string,
) (runtimedelivery.ExecutionAuthority, error) {
	switch selected.(type) {
	case *PostgresStore:
		return deliveryFixtureAuthorityForRun(ctx, tx, deliveryadapter.DialectPostgres, runID)
	case *SQLiteRuntimeStore:
		return deliveryFixtureAuthorityForRun(ctx, tx, deliveryadapter.DialectSQLite, runID)
	default:
		return runtimedelivery.ExecutionAuthority{}, fmt.Errorf(
			"semantic event fixture store %T has no delivery authority",
			selected,
		)
	}
}

func pipelineObligationOwnerForFixture(store any) runtimepipelineobligation.Store {
	switch selected := store.(type) {
	case *PostgresStore:
		return selected.PipelineObligations()
	case *SQLiteRuntimeStore:
		return selected.PipelineObligations()
	default:
		panic(fmt.Sprintf("semantic event fixture store %T has no pipeline obligation owner", store))
	}
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
	record, err := eventrecord.FromAdmitted(admitted, testRouteSettlement(admitted.Event(), nil))
	if err != nil {
		return err
	}
	var inserted bool
	switch selected := selectedStore.(type) {
	case *PostgresStore:
		inserted, err = eventrecordpostgres.Insert(ctx, selected.backend.ConstructionHandle(), record)
	case *SQLiteRuntimeStore:
		inserted, err = eventrecordsqlite.Insert(ctx, selected.backend.ConstructionHandle(), record)
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
	return insertCanonicalEventRecordFixture(ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), event)
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
	record, err := eventrecord.FromAdmitted(admitted, testRouteSettlement(admitted.Event(), nil))
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
	ResolveAuthorActivityEventDescriptor(runtimeauthoractivity.Scope, string) (runtimeauthoractivity.EventDescriptor, bool)
	AuthorActivityEventCatalogRegistered(runtimeauthoractivity.Scope) bool
}

func semanticEventFixtureContext(ctx context.Context, selectedStore any, event events.Event) (context.Context, func(), error) {
	store, ok := selectedStore.(semanticFixtureCatalogStore)
	if !ok {
		return ctx, func() {}, fmt.Errorf("semantic event fixture store %T has no author activity catalog", selectedStore)
	}
	source, hasSource := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !hasSource || source.BundleHash() == sourceartifactfixture.BundleHash {
		writer, ok := selectedStore.(sourceartifactfixture.Writer)
		if !ok {
			return ctx, func() {}, fmt.Errorf("semantic event fixture store %T has no source artifact writer", selectedStore)
		}
		if err := sourceartifactfixture.Ensure(ctx, writer); err != nil {
			return ctx, func() {}, fmt.Errorf("persist semantic event fixture source: %w", err)
		}
		if !hasSource {
			source = sourceartifactfixture.Fact()
			ctx = runtimecorrelation.WithSourceArtifactFact(ctx, source)
		}
	}
	scope, scoped := runtimeauthoractivity.ScopeFromContext(ctx)
	if !scoped || scope.Kind != runtimeauthoractivity.ScopeBundle {
		scope = runtimeauthoractivity.BundleScope(event.ID(), source.BundleHash())
		ctx = runtimeauthoractivity.WithScope(ctx, scope)
	}
	if store.AuthorActivityEventCatalogRegistered(scope) {
		descriptor, ok := store.ResolveAuthorActivityEventDescriptor(scope, string(event.Type()))
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
