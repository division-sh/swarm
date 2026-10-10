package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestAPIEventStoragePreservesWholeStoreCountsLineageAndNativeSnapshotBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			at := time.Now().UTC().Truncate(time.Microsecond)
			original := eventtest.RunCreatingRootIngress(uuid.NewString(), "scan.requested", "gateway", "", []byte(`{"value":1}`), 0, uuid.NewString(), "", events.EventEnvelope{}, at)
			replay := eventtest.Child(uuid.NewString(), "scan.requested", "gateway", "", []byte(`{"value":1}`), 1, original, events.EventEnvelope{}, at.Add(time.Second))
			audit := eventtest.Child(uuid.NewString(), "event.replayed", "gateway", "", []byte(fmt.Sprintf(`{"original":%q,"replay":%q,"agent":"agent-a"}`, original.ID(), replay.ID())), 1, original, events.EventEnvelope{}, at.Add(2*time.Second))
			other := eventtest.RunCreatingRootIngress(uuid.NewString(), "event.replayed", "gateway", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, at)
			for _, item := range []struct {
				event  events.Event
				agents []string
			}{{original, []string{"agent-a", "agent-b"}}, {replay, []string{"agent-a"}}, {audit, nil}, {other, nil}} {
				if item.event.ID() == original.ID() {
					node, err := runtimeidentity.ParseExecutableNode("fixture", "node")
					if err != nil {
						t.Fatal(err)
					}
					routes := []events.DeliveryRoute{{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "fixture", FlowInstance: "fixture"})}}
					for _, agent := range item.agents {
						routes = append(routes, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(agent), AgentIdentity: mustTestAgentIdentityForRun(item.event.RunID(), agent, "fixture/"+agent)})
					}
					if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, item.event, routes); err != nil {
						t.Fatal(err)
					}
					continue
				}
				if err := commitSemanticEventFixtureWithAgents(ctx, fixture.store, item.event, item.agents); err != nil {
					t.Fatal(err)
				}
			}
			// This is an inert physical receipt, including expired history, not
			// execution authority or a substitute for the API consumer proofs.
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `INSERT INTO api_idempotency(method,actor_kind,actor_id,idempotency_key,request_hash,resource_id,response,created_at,expires_at) VALUES('event.publish','bearer_token','fixture','expired','digest','resource','{}',$1,$2)`, at.Add(-2*time.Hour), at.Add(-time.Hour))
				return err
			}); err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for key, want := range map[string]int{"scan.requested": 2, "event.replayed": 2, "": 0, "SCAN.REQUESTED": 0, "scan.requested ": 0} {
				if count, err := CountEventNameStorageForTest(ctx, fixture.store, key); err != nil || count != want {
					t.Fatalf("exact name %q=%d/%v, want%d", key, count, err, want)
				}
			}
			counts, err := ReadAPIEventPublicationRefusalStorageForTest(ctx, fixture.store, "scan.requested")
			if err != nil || counts != (APIEventPublicationRefusalStorage{Runs: 2, MatchingEvents: 2, APICompletions: 1}) {
				t.Fatalf("whole-store physical counts=%+v/%v", counts, err)
			}
			got, err := ReadAPIEventReplayStorageForTest(ctx, fixture.store, original.ID(), replay.ID(), audit.ID())
			if err != nil || got.OriginalAgentDeliveries != 2 || got.ReplayAgentDeliveries != 1 || got.AuditEventCount != 2 || got.ReplaySourceEventID != original.ID() || got.AuditSourceEventID != original.ID() {
				t.Fatalf("replay physical scope/lineage=%+v/%v", got, err)
			}
			var wantPayload string
			if err := fixture.db.QueryRowContext(ctx, `SELECT CAST(payload AS TEXT) FROM events WHERE event_id=$1`, audit.ID()).Scan(&wantPayload); err != nil {
				t.Fatal(err)
			}
			if got.AuditPayload != wantPayload {
				t.Fatalf("native payload rendering changed: got=%q want=%q", got.AuditPayload, wantPayload)
			}
			if runs, err := CountPhysicalRunsForTest(ctx, fixture.store); err != nil || runs != 2 {
				t.Fatalf("whole-store run count=%d/%v", runs, err)
			}
			if events, err := CountPhysicalEventsForTest(ctx, fixture.store); err != nil || events != 4 {
				t.Fatalf("whole-store event count=%d/%v", events, err)
			}
			if receipts, err := CountAPICommandReceiptsForTest(ctx, fixture.store); err != nil || receipts != 1 {
				t.Fatalf("whole-store command history=%d/%v", receipts, err)
			}
			if whole, err := ReadAPIFlowPublicationRefusalStorageForTest(ctx, fixture.store); err != nil || whole != (APIPublicationCardinalityStorage{Runs: 2, Events: 4, APICompletions: 1}) {
				t.Fatalf("whole-store cardinality=%+v/%v", whole, err)
			}
			for run, want := range map[string]APIPublicationCardinalityStorage{
				original.RunID(): {Runs: 1, Events: 3, APICompletions: 1},
				uuid.NewString(): {APICompletions: 1},
			} {
				if scoped, err := ReadAPIRunStartRefusalStorageForTest(ctx, fixture.store, run); err != nil || scoped != want {
					t.Fatalf("exact-run counts=%+v/%v, want%+v", scoped, err, want)
				}
			}
			for run, wantEvents := range map[string]int{original.RunID(): 3, other.RunID(): 1, uuid.NewString(): 0} {
				wantRuns := 0
				if wantEvents != 0 {
					wantRuns = 1
				}
				if count, err := CountPhysicalRunIdentityForTest(ctx, fixture.store, run); err != nil || count != wantRuns {
					t.Fatalf("exact run cardinality=%d/%v, want%d", count, err, wantRuns)
				}
				if count, err := CountPhysicalRunEventsForTest(ctx, fixture.store, run); err != nil || count != wantEvents {
					t.Fatalf("exact run event cardinality=%d/%v, want%d", count, err, wantEvents)
				}
			}
			if count, err := CountAgentEventDeliveryStorageForTest(ctx, fixture.store, original.ID()); err != nil || count != 2 {
				t.Fatalf("native scalar agent cardinality=%d/%v, want2 excluding node", count, err)
			}
			if count, err := CountAgentEventDeliveryStorageForTest(ctx, fixture.store, uuid.NewString()); err != nil || count != 0 {
				t.Fatalf("absent scalar agent cardinality=%d/%v", count, err)
			}
			if count, err := CountPhysicalEventDeliveriesForTest(ctx, fixture.store); err != nil || count != 4 {
				t.Fatalf("whole-store agent/node/cross-event delivery count=%d/%v, want4", count, err)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 22 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("observation escaped original selected read snapshots: %+v", counts)
			}
			if nulls, err := ReadAPIEventReplayStorageForTest(ctx, fixture.store, original.ID(), original.ID(), original.ID()); err != nil || nulls.ReplaySourceEventID != "" || nulls.AuditSourceEventID != "" {
				t.Fatalf("SQL NULL lineage defaults changed: %+v/%v", nulls, err)
			}
			// Failure at the final exact-event read discards earlier counts and
			// lineage rather than exposing a plausible partial replay witness.
			if late, err := ReadAPIEventReplayStorageForTest(ctx, fixture.store, original.ID(), replay.ID(), uuid.NewString()); err == nil || !reflect.DeepEqual(late, APIEventReplayStorage{}) {
				t.Fatalf("late read retained partial evidence: %+v/%v", late, err)
			}
		})
	}
}

func TestAPILatestNamedEventIdentityPreservesPhysicalOrderAndGlobalScopeBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			at := time.Now().UTC().Truncate(time.Microsecond)
			first := eventtest.RunCreatingRootIngress(uuid.NewString(), "scan.requested", "gateway", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, at)
			second := eventtest.RunCreatingRootIngress(uuid.NewString(), "scan.requested", "gateway", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, at.Add(time.Second))
			other := eventtest.RunCreatingRootIngress(uuid.NewString(), "other.requested", "gateway", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, at.Add(2*time.Second))
			for _, event := range []events.Event{second, other, first} {
				if err := commitSemanticEventFixture(ctx, fixture.store, event); err != nil {
					t.Fatal(err)
				}
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for excluded, want := range map[string]string{
				first.ID(): second.ID(), second.ID(): first.ID(),
				"": second.ID(), "not-a-uuid": second.ID(),
			} {
				if got, err := ReadLatestNamedEventIdentityStorageForTest(ctx, fixture.store, "scan.requested", excluded); err != nil || got != want {
					t.Fatalf("latest exact name/global exclusion%q=%q/%v, want%q", excluded, got, err, want)
				}
			}
			for _, name := range []string{"", "SCAN.REQUESTED", "scan.requested "} {
				if got, err := ReadLatestNamedEventIdentityStorageForTest(ctx, fixture.store, name, ""); !errors.Is(err, sql.ErrNoRows) || got != "" {
					t.Fatalf("native exact-name absence%q=%q/%v", name, got, err)
				}
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 4 || counts.Total.Failed != 3 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("latest identity escaped original coordinator: %+v", counts)
			}
		})
	}
}

func TestAPIEventAcknowledgmentCountsPreserveNativeScopesBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "ack.observation", "gateway", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
			if err := commitSemanticPipelineProcessedEventFixture(ctx, fixture.store, event); err != nil {
				t.Fatal(err)
			}
			// Fixed physical receipt history is not executable delivery authority.
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				if _, err := tx.ExecContext(ctx, `UPDATE event_receipts SET outcome='waiting' WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, event.ID()); err != nil {
					return err
				}
				_, err := tx.ExecContext(ctx, `INSERT INTO event_receipts(receipt_id,event_id,subscriber_type,subscriber_id,outcome) VALUES($1,$2,'platform','unrelated','reject')`, uuid.NewString(), event.ID())
				return err
			}); err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if count, err := CountPipelineEventReceiptStorageForTest(ctx, fixture.store, event.ID()); err != nil || count != 1 {
				t.Fatalf("exact pipeline subscriber/outcome-neutral count=%d/%v", count, err)
			}
			if count, err := CountPipelineEventReceiptStorageForTest(ctx, fixture.store, uuid.NewString()); err != nil || count != 0 {
				t.Fatalf("absent pipeline receipt=%d/%v", count, err)
			}
			if count, err := CountAgentEventDeliveryStorageForTest(ctx, fixture.store, event.ID()); err != nil || count != 0 {
				t.Fatalf("pipeline acknowledgment became agent execution=%d/%v", count, err)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 3 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("ack/delivery counts escaped original read owner: %+v", counts)
			}
			if outcome, failure, err := ReadPipelineReceiptOutcomeStorageForTest(ctx, fixture.store, event.ID()); err != nil || outcome != "waiting" || failure != nil {
				t.Fatalf("waiting exact receipt with SQL NULL failure=%q/%+v/%v", outcome, failure, err)
			}
			want, ok := runtimefailures.EnvelopeFromError(runtimefailures.New(runtimefailures.ClassInternalFailure, "fixture_receipt_failure", "fixture", "readback", nil))
			if !ok {
				t.Fatal("typed fixture failure has no envelope")
			}
			raw, err := runtimefailures.MarshalEnvelope(want)
			if err != nil {
				t.Fatal(err)
			}
			writeFailure := func(raw []byte) {
				t.Helper()
				if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, `UPDATE event_receipts SET failure=$1 WHERE event_id=$2 AND subscriber_type='platform' AND subscriber_id='pipeline'`, raw, event.ID())
					return err
				}); err != nil {
					t.Fatal(err)
				}
			}
			writeFailure(raw)
			defer writeFailure(nil)
			if outcome, failure, err := ReadPipelineReceiptOutcomeStorageForTest(ctx, fixture.store, event.ID()); err != nil || outcome != "waiting" || !reflect.DeepEqual(failure, &want) {
				t.Fatalf("exact receipt failure changed: %q/%+v/%v, want%+v", outcome, failure, err, want)
			}
			if outcome, failure, err := ReadPipelineReceiptOutcomeStorageForTest(ctx, fixture.store, uuid.NewString()); !errors.Is(err, sql.ErrNoRows) || outcome != "" || failure != nil {
				t.Fatalf("missing receipt retained partial evidence: %q/%+v/%v", outcome, failure, err)
			}
			writeFailure([]byte(`{"unknown":true}`))
			if outcome, failure, err := ReadPipelineReceiptOutcomeStorageForTest(ctx, fixture.store, event.ID()); err == nil || outcome != "" || failure != nil {
				t.Fatalf("malformed receipt retained partial outcome: %q/%+v/%v", outcome, failure, err)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 5 || counts.Total.WriteCommits != 2 || counts.Total.Failed != 2 || counts.Active != 0 {
				t.Fatalf("receipt observations or faults escaped original coordinator: %+v", counts)
			}
		})
	}
}

func TestAPIEventStorageRefusesRawCancelledClosedAndUnavailableOwnershipBothStores(t *testing.T) {
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		assertAPIEventStorageRefusal(t, context.Background(), owner)
		assertAPIIndependentScalarCountRefusal(t, context.Background(), owner)
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := CountEventNameStorageForTest(ctx, fixture.store, "scan.requested"); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation identity=%v", err)
			}
			assertAPIEventStorageRefusal(t, ctx, fixture.store)
			assertAPIIndependentScalarCountRefusal(t, ctx, fixture.store)
			ctx = context.Background()
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `ALTER TABLE events RENAME TO unavailable_api_event_observation`)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			restored := false
			defer func() {
				if !restored {
					if err := runUnrevisionedEventFixtureTransactionForTest(context.Background(), fixture.store, func(ctx context.Context, tx *sql.Tx) error {
						_, err := tx.ExecContext(ctx, `ALTER TABLE unavailable_api_event_observation RENAME TO events`)
						return err
					}); err != nil {
						t.Error(err)
					}
				}
			}()
			assertAPIEventStorageRefusal(t, ctx, fixture.store)
			// A count does not gain dependencies on unrelated row families.
			if count, err := CountPhysicalRunsForTest(ctx, fixture.store); err != nil || count != 0 {
				t.Fatalf("run count acquired event-table dependency: %d/%v", count, err)
			}
			if count, err := CountAPICommandReceiptsForTest(ctx, fixture.store); err != nil || count != 0 {
				t.Fatalf("receipt count acquired event-table dependency: %d/%v", count, err)
			}
			if count, err := CountPhysicalRunIdentityForTest(ctx, fixture.store, uuid.NewString()); err != nil || count != 0 {
				t.Fatalf("exact run count acquired event-table dependency: %d/%v", count, err)
			}
			if count, err := CountPhysicalEventDeliveriesForTest(ctx, fixture.store); err != nil || count != 0 {
				t.Fatalf("delivery count acquired unrelated event-table dependency: %d/%v", count, err)
			}
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `ALTER TABLE unavailable_api_event_observation RENAME TO events`)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			restored = true
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			assertAPIEventStorageRefusal(t, ctx, fixture.store)
			assertAPIIndependentScalarCountRefusal(t, ctx, fixture.store)
		})
	}
}

func assertAPIIndependentScalarCountRefusal(t *testing.T, ctx context.Context, owner any) {
	t.Helper()
	if outcome, failure, err := ReadPipelineReceiptOutcomeStorageForTest(ctx, owner, uuid.NewString()); err == nil || outcome != "" || failure != nil {
		t.Fatalf("invalid receipt outcome owner %T=%q/%+v/%v", owner, outcome, failure, err)
	}
	if count, err := CountPhysicalEventDeliveriesForTest(ctx, owner); err == nil || count != 0 {
		t.Fatalf("invalid whole-store delivery count owner %T=%d/%v", owner, count, err)
	}
	if count, err := CountPhysicalRunsForTest(ctx, owner); err == nil || count != 0 {
		t.Fatalf("invalid physical run count owner %T=%d/%v", owner, count, err)
	}
	if count, err := CountAPICommandReceiptsForTest(ctx, owner); err == nil || count != 0 {
		t.Fatalf("invalid command receipt count owner %T=%d/%v", owner, count, err)
	}
	if count, err := CountPhysicalRunIdentityForTest(ctx, owner, uuid.NewString()); err == nil || count != 0 {
		t.Fatalf("invalid exact run owner %T=%d/%v", owner, count, err)
	}
	if count, err := CountAgentEventDeliveryStorageForTest(ctx, owner, uuid.NewString()); err == nil || count != 0 {
		t.Fatalf("invalid event delivery owner %T=%d/%v", owner, count, err)
	}
	if count, err := CountPipelineEventReceiptStorageForTest(ctx, owner, uuid.NewString()); err == nil || count != 0 {
		t.Fatalf("invalid pipeline receipt owner %T=%d/%v", owner, count, err)
	}
}

func assertAPIEventStorageRefusal(t *testing.T, ctx context.Context, owner any) {
	t.Helper()
	if got, err := ReadLatestNamedEventIdentityStorageForTest(ctx, owner, "scan.requested", ""); err == nil || got != "" {
		t.Fatalf("invalid latest-event owner %T=%q/%v", owner, got, err)
	}
	if got, err := CountEventNameStorageForTest(ctx, owner, "scan.requested"); err == nil || got != 0 {
		t.Fatalf("invalid count owner %T=%d/%v", owner, got, err)
	}
	if got, err := ReadAPIEventPublicationRefusalStorageForTest(ctx, owner, "scan.requested"); err == nil || !reflect.DeepEqual(got, APIEventPublicationRefusalStorage{}) {
		t.Fatalf("invalid publication owner %T=%+v/%v", owner, got, err)
	}
	if got, err := ReadAPIEventReplayStorageForTest(ctx, owner, uuid.NewString(), uuid.NewString(), uuid.NewString()); err == nil || !reflect.DeepEqual(got, APIEventReplayStorage{}) {
		t.Fatalf("invalid replay owner %T=%+v/%v", owner, got, err)
	}
	if got, err := CountPhysicalEventsForTest(ctx, owner); err == nil || got != 0 {
		t.Fatalf("invalid physical event owner %T=%d/%v", owner, got, err)
	}
	if got, err := CountPhysicalRunEventsForTest(ctx, owner, uuid.NewString()); err == nil || got != 0 {
		t.Fatalf("invalid exact-run event owner %T=%d/%v", owner, got, err)
	}
	if got, err := ReadAPIFlowPublicationRefusalStorageForTest(ctx, owner); err == nil || got != (APIPublicationCardinalityStorage{}) {
		t.Fatalf("invalid all-row cardinality owner %T=%+v/%v", owner, got, err)
	}
	if got, err := ReadAPIRunStartRefusalStorageForTest(ctx, owner, uuid.NewString()); err == nil || got != (APIPublicationCardinalityStorage{}) {
		t.Fatalf("invalid scoped cardinality owner %T=%+v/%v", owner, got, err)
	}
}
