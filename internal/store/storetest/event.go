package storetest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

// AdmitNativeDeliveryEvent keeps fixture payload admission separate from the
// publication writer's admitted-only boundary.
func AdmitNativeDeliveryEvent(t *testing.T, event events.Event) events.AdmittedEvent {
	t.Helper()
	var err error
	if _, bound := event.PayloadAdmission(); !bound {
		event, err = eventfixture.BindPayload(event)
		if err != nil {
			t.Fatal(err)
		}
	}
	admitted, err := events.AdmitForPublish(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	return admitted
}

func AcknowledgedPipelineDisposition() *runtimepipelineobligation.Disposition {
	disposition := runtimepipelineobligation.Acknowledged("pipeline_persisted")
	return &disposition
}

func InsertNativeExistingRunRootEvent(t testing.TB, ctx context.Context, selected any, eventID, runID string, eventType events.EventType, producer events.ProducerIdentity, payload []byte, envelope events.EventEnvelope, at time.Time) events.Event {
	t.Helper()
	event, err := private.InsertNativeExistingRunRootEventForTest(ctx, selected, eventID, runID, eventType, producer, payload, envelope, at)
	if err != nil {
		t.Fatalf("insert native existing run root event: %v", err)
	}
	return event
}

// InsertNativeCanonicalEventRecord preserves event-only history without
// manufacturing a publication scope, delivery plan or flow construction.
func InsertNativeCanonicalEventRecord(t testing.TB, ctx context.Context, selected any, event events.Event) runtimebus.EventAppendOutcome {
	t.Helper()
	bound, err := eventfixture.BindPayload(event)
	if err != nil {
		t.Fatalf("bind canonical event payload fixture: %v", err)
	}
	admitted, err := events.AdmitForPersistence(bound, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatalf("admit canonical event record fixture: %v", err)
	}
	inserted, err := private.InsertNativeCanonicalEventRecordForTest(ctx, selected, admitted, canonicalFixtureSettlement(t, admitted.Event(), nil))
	if err != nil {
		t.Fatalf("insert canonical event record fixture: %v", err)
	}
	if inserted {
		return runtimebus.EventAppendInserted
	}
	return runtimebus.EventAppendExactDuplicate
}

// CommitDeliveryObligationsForPersistedEvent seeds exact executable routes for
// an event that was already inserted as a fixture precondition. Delivery SQL
// stays behind the canonical adapter even in tests.
func CommitDeliveryObligationsForPersistedEvent(
	t testing.TB,
	ctx context.Context,
	selectedStore any,
	event events.Event,
	routes []events.DeliveryRoute,
) {
	t.Helper()
	if err := commitPersistedEventDeliveryFixture(ctx, selectedStore, event.ID(), event.RunID(), routes); err != nil {
		t.Fatalf("commit persisted event delivery fixture: %v", err)
	}
}

type DeliveryLifecycleStore interface {
	ClaimDelivery(context.Context, runtimedelivery.ExecutionAuthority, events.Event, events.DeliveryRoute) (runtimedelivery.ClaimResult, error)
	Snapshot(context.Context, string) (runtimedelivery.Snapshot, error)
}

// ClaimDelivery acquires an exact fixture delivery through the same typed
// authority boundary as production. It is intentionally unsuitable for tests
// that need to assert non-acquired dispositions; those should call the store
// method directly and inspect ClaimResult.
func ClaimDelivery(ctx context.Context, selected DeliveryLifecycleStore, event events.Event, route events.DeliveryRoute) (runtimedelivery.ClaimedObligation, error) {
	deliveryID, err := runtimedelivery.DeliveryID(event.ID(), route)
	if err != nil {
		return runtimedelivery.ClaimedObligation{}, err
	}
	snapshot, err := selected.Snapshot(ctx, deliveryID)
	if err != nil {
		return runtimedelivery.ClaimedObligation{}, err
	}
	result, err := selected.ClaimDelivery(ctx, snapshot.Authority, event, route)
	if err != nil {
		return runtimedelivery.ClaimedObligation{}, err
	}
	claimed, ok := result.Acquired()
	if !ok {
		return runtimedelivery.ClaimedObligation{}, fmt.Errorf("delivery %s was not acquired: %s", deliveryID, result.Disposition)
	}
	return claimed, nil
}

// LoadCanonicalEventRecord exercises the same complete-record decoder used by
// runtime recovery and replay readers.
func LoadCanonicalEventRecord(t testing.TB, ctx context.Context, selectedStore any, eventID string) events.Event {
	t.Helper()
	event, found, err := private.ReadCanonicalEventRecordForTest(ctx, selectedStore, eventID)
	if err != nil || !found {
		t.Fatalf("load canonical event record %s: found=%v err=%v", eventID, found, err)
	}
	return event
}

// ReadCanonicalEventRecord preserves the same complete-record ownership with
// an explicit missing result for optional observation consumers.
func ReadCanonicalEventRecord(ctx context.Context, selectedStore any, eventID string) (events.Event, bool, error) {
	return private.ReadCanonicalEventRecordForTest(ctx, selectedStore, eventID)
}

// InsertRevisionedExistingRunRootEventRecord advances the selected run's event
// history through its native mutation owner, without creating delivery work.
func InsertRevisionedExistingRunRootEventRecord(
	t testing.TB,
	ctx context.Context,
	selected any,
	eventID, runID string,
	eventType events.EventType,
	producer events.ProducerIdentity,
	payload []byte,
	envelope events.EventEnvelope,
	createdAt time.Time,
) events.Event {
	t.Helper()
	event, err := private.InsertNativeExistingRunRootEventForTest(ctx, selected, eventID, runID, eventType, producer, payload, envelope, createdAt)
	if err != nil {
		t.Fatalf("insert revisioned existing-run root event fixture: %v", err)
	}
	return event
}

// InsertUnrevisionedChildEventRecord leaves the child in the same first
// revision as other test-only facts captured later by the caller.
func InsertUnrevisionedChildEventRecord(
	t testing.TB,
	ctx context.Context,
	selected any,
	eventID string,
	runID string,
	parentEventID string,
	eventType events.EventType,
	producer events.ProducerIdentity,
	payload []byte,
	envelope events.EventEnvelope,
	createdAt time.Time,
) events.Event {
	t.Helper()
	event, err := private.InsertStagedChildEventForTest(ctx, selected, eventID, runID, parentEventID, eventType, producer, payload, envelope, createdAt)
	if err != nil {
		t.Fatalf("insert unrevisioned child event fixture %s: %v", eventID, err)
	}
	return event
}

func InsertDiagnosticDirectEventRecord(
	t testing.TB,
	ctx context.Context,
	selected any,
	eventID string,
	payload []byte,
	createdAt time.Time,
) events.Event {
	t.Helper()
	event := eventtest.DiagnosticDirect(eventID, events.EventTypePlatformRuntimeLog, "runtime", "", payload, 0, "", "", events.EventEnvelope{Scope: events.EventScopeGlobal}, createdAt)
	bound, err := eventfixture.BindPayload(event)
	if err != nil {
		t.Fatalf("bind diagnostic-direct payload: %v", err)
	}
	admission, ok := bound.PayloadAdmission()
	if !ok {
		t.Fatal("diagnostic-direct fixture requires payload admission")
	}
	var writer runtimepkg.RuntimeLogPersistence
	switch owner := selected.(type) {
	case *private.PostgresStore:
		if owner != nil {
			writer = owner
		}
	case *private.SQLiteRuntimeStore:
		if owner != nil {
			writer = owner
		}
	}
	if writer == nil {
		t.Fatalf("diagnostic-direct fixture requires the original selected owner, got %T", selected)
	}
	if err := writer.PersistRuntimeLog(ctx, runtimepkg.RuntimeLogPersistenceRecord{
		EventID: eventID, Payload: payload, PayloadAdmission: admission, CreatedAt: createdAt, ExecutionMode: bound.ExecutionMode(),
	}); err != nil {
		t.Fatalf("persist diagnostic-direct fixture through named owner: %v", err)
	}
	return event
}

func SeedPostForkSourceEvent(t testing.TB, ctx context.Context, selected any, runID, eventID, entityID string, createdAt time.Time) events.Event {
	t.Helper()
	event, err := private.SeedPostForkSourceEventForTest(ctx, selected, runID, eventID, entityID, createdAt)
	if err != nil {
		t.Fatalf("seed post-fork source event: %v", err)
	}
	return event
}

func SeedRunDiagnosticDeadLetterHistory(t testing.TB, ctx context.Context, selected any, runID, eventID string, payload []byte, createdAt time.Time) events.Event {
	t.Helper()
	event, err := private.SeedRunDiagnosticDeadLetterHistoryForTest(ctx, selected, runID, eventID, payload, createdAt)
	if err != nil {
		t.Fatalf("persist diagnostic history pair %s: %v", eventID, err)
	}
	return event
}

func CommitSemanticEvent(t testing.TB, ctx context.Context, selectedStore any, event events.Event) runtimebus.EventAppendOutcome {
	t.Helper()
	return CommitSemanticEventWithInitialFacts(t, ctx, selectedStore, event, nil, runtimepipelineobligation.ScopeDirect, nil)
}

func CommitSemanticEventWithRoutes(
	t testing.TB,
	ctx context.Context,
	selectedStore any,
	event events.Event,
	routes []events.DeliveryRoute,
	scope runtimepipelineobligation.CommittedScope,
) runtimebus.EventAppendOutcome {
	t.Helper()
	return CommitSemanticEventWithInitialFacts(t, ctx, selectedStore, event, routes, scope, nil)
}

func CommitSemanticEventWithInitialFacts(
	t testing.TB,
	ctx context.Context,
	selectedStore any,
	event events.Event,
	routes []events.DeliveryRoute,
	scope runtimepipelineobligation.CommittedScope,
	pipelineDisposition *runtimepipelineobligation.Disposition,
) runtimebus.EventAppendOutcome {
	t.Helper()
	return commitSemanticEventWithInitialFacts(t, ctx, selectedStore, event, routes, scope, pipelineDisposition, false, false)
}

// CommitSemanticForkFrontier seeds the exact PostgreSQL fact families that a
// selected-fork snapshot reads and captures their revision in the same
// transaction.
func CommitSemanticForkFrontier(
	t testing.TB,
	ctx context.Context,
	selectedStore any,
	event events.Event,
	routes []events.DeliveryRoute,
	pipelineDisposition *runtimepipelineobligation.Disposition,
) runtimebus.EventAppendOutcome {
	t.Helper()
	if selected, ok := selectedStore.(*private.PostgresStore); !ok || selected == nil {
		t.Fatalf("semantic fork frontier fixture requires the original PostgreSQL owner, got %T", selectedStore)
	}
	return commitSemanticEventWithInitialFacts(
		t, ctx, selectedStore, event, routes,
		runtimepipelineobligation.ScopeSubscribed,
		pipelineDisposition,
		true, true,
	)
}

func commitSemanticEventWithInitialFacts(
	t testing.TB,
	ctx context.Context,
	selectedStore any,
	event events.Event,
	routes []events.DeliveryRoute,
	scope runtimepipelineobligation.CommittedScope,
	pipelineDisposition *runtimepipelineobligation.Disposition,
	revisioned, captureForkFrontier bool,
) runtimebus.EventAppendOutcome {
	t.Helper()
	var err error
	if _, bound := event.PayloadAdmission(); !bound {
		event, err = eventfixture.BindPayload(event)
		if err != nil {
			t.Fatalf("bind event payload fixture: %v", err)
		}
	}
	admitted, err := events.AdmitForPublish(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatalf("admit event fixture: %v", err)
	}
	if admitted.Class() == events.EventAdmissionSelectedForkReplay {
		t.Fatal(fmt.Errorf("selected-fork replay events require their closed named persistence operation"))
	}
	settlement := canonicalFixtureSettlement(t, admitted.Event(), routes)
	record, err := eventrecord.FromAdmitted(admitted, settlement)
	if err != nil {
		t.Fatalf("project admitted event fixture: %v", err)
	}

	runner, ok := selectedStore.(RunFixtureStore)
	if !ok {
		t.Fatalf("semantic event fixture store %T has no run lifecycle mutation owner", selectedStore)
	}
	if admitted.RunDisposition() != events.AdmittedRunless {
		startedAt := record.CreatedAt
		if startedAt.IsZero() {
			startedAt = time.Now().UTC()
		}
		if err := EnsureRunForAdmittedEvent(ctx, runner, admitted, startedAt); err != nil {
			t.Fatalf("ensure semantic event fixture run: %v", err)
		}
	}
	obligationProvider, ok := selectedStore.(interface {
		PipelineObligations() runtimepipelineobligation.Store
	})
	if !ok {
		t.Fatalf("semantic event fixture store %T has no pipeline obligation owner", selectedStore)
	}
	obligationOwner := obligationProvider.PipelineObligations()
	publicationClaim, err := obligationOwner.ClaimPublication(ctx, record.EventID)
	if err != nil {
		t.Fatalf("claim semantic event fixture publication: %v", err)
	}
	defer func() {
		if err := obligationOwner.Release(context.WithoutCancel(ctx), publicationClaim); err != nil {
			t.Fatalf("release semantic event fixture publication: %v", err)
		}
	}()
	var inserted bool
	if captureForkFrontier {
		inserted, err = private.CommitSemanticForkFrontierForTest(ctx, selectedStore, admitted, settlement, routes, scope, pipelineDisposition)
	} else if revisioned {
		inserted, err = private.CommitRevisionedSemanticEventFixtureForTest(ctx, selectedStore, admitted, settlement, routes, scope, pipelineDisposition)
	} else {
		inserted, err = private.CommitSemanticEventFixtureForTest(ctx, selectedStore, admitted, settlement, routes, scope, pipelineDisposition)
	}
	if err != nil {
		t.Fatalf("commit semantic event fixture: %v", err)
	}
	if !inserted {
		return runtimebus.EventAppendExactDuplicate
	}
	return runtimebus.EventAppendInserted
}

func canonicalFixtureSettlement(t testing.TB, event events.Event, routes []events.DeliveryRoute) events.RouteSettlement {
	t.Helper()
	settlement, err := fixtureSettlement(event, routes)
	if err != nil {
		t.Fatalf("construct canonical event fixture settlement: %v", err)
	}
	return settlement
}

func fixtureSettlement(event events.Event, routes []events.DeliveryRoute) (events.RouteSettlement, error) {
	var (
		settlement events.RouteSettlement
		err        error
	)
	switch event.Type() {
	case events.EventTypePlatformRuntimeLog:
		settlement, err = events.NewNoDeliverySettlement(events.EventWriteRuntimeLogDirect, events.NoDeliveryNoSubscriberByDesign, events.ConnectEvaluationLedger{})
	case events.EventTypePlatformInboundRecord:
		settlement, err = events.NewNoDeliverySettlement(events.EventWriteInboundEvidenceDirect, events.NoDeliveryNoSubscriberByDesign, events.ConnectEvaluationLedger{})
	case events.EventTypePlatformAgentDirective:
		settlement, err = events.NewNoDeliverySettlement(events.EventWriteDirectiveDirect, events.NoDeliveryNoSubscriberByDesign, events.ConnectEvaluationLedger{})
	default:
		var ledger events.ConnectEvaluationLedger
		ledger, err = events.NewConnectEvaluationLedger(nil)
		if err == nil && len(routes) > 0 {
			settlement, err = events.NewDeliverySettlement(events.EventWriteNormalPublication, ledger)
		} else if err == nil {
			settlement, err = events.NewNoDeliverySettlement(events.EventWriteNormalPublication, events.NoDeliveryDeclaredConsumerNoPlan, ledger)
		}
	}
	if err != nil {
		return events.RouteSettlement{}, err
	}
	if err := settlement.Validate(routes); err != nil {
		return events.RouteSettlement{}, err
	}
	return settlement, nil
}
