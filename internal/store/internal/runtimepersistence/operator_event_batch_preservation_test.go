package runtimepersistence

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

func TestOperatorEventBatchPaginationBoundaryBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			store := fixture.store.(routeSettlementOperatorStore)
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			base := time.Date(2026, 8, 16, 12, 0, 0, 123456000, time.UTC)
			ids := make([]string, 129)
			for i := len(ids) - 1; i >= 0; i-- {
				ids[i] = fmt.Sprintf("23940000-0000-4000-8000-%012d", i)
				// Reverse insertion and tied timestamps exercise both cursor coordinates.
				at := base.Add(time.Duration(i/43) * time.Second)
				event := eventtest.ExistingRunRootIngress(ids[i], "batch.boundary", "gateway", "", []byte(fmt.Sprintf(`{"index":%d,"decimal":1.0}`, i)), 0, runID, events.EventEnvelope{}, at)
				if err := commitSemanticEventFixture(ctx, fixture.store, event); err != nil {
					t.Fatal(err)
				}
			}
			scalar := make(map[string]operatorread.OperatorEventFull, len(ids))
			for _, id := range ids {
				got, err := store.LoadOperatorEvent(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				scalar[id] = got
			}
			for _, order := range []string{"asc", "desc"} {
				t.Run(order, func(t *testing.T) {
					opts := operatorread.OperatorEventListOptions{Filter: operatorread.OperatorEventListFilter{RunID: runID}, Order: order, Limit: 128}
					page, err := store.ListOperatorEvents(ctx, opts)
					if err != nil || len(page.Events) != 128 || page.NextCursor == "" {
						t.Fatalf("boundary first page: count=%d cursor=%q err=%v", len(page.Events), page.NextCursor, err)
					}
					for i, got := range page.Events {
						index := i
						if order == "desc" {
							index = len(ids) - 1 - i
						}
						assertOperatorBatchScalarEqual(t, got, scalar[ids[index]])
					}
					opts.Cursor = page.NextCursor
					tail, err := store.ListOperatorEvents(ctx, opts)
					if err != nil || len(tail.Events) != 1 || tail.NextCursor != "" {
						t.Fatalf("boundary tail: count=%d cursor=%q err=%v", len(tail.Events), tail.NextCursor, err)
					}
					last := ids[len(ids)-1]
					if order == "desc" {
						last = ids[0]
					}
					assertOperatorBatchScalarEqual(t, tail.Events[0], scalar[last])
					opts.Order = "asc"
					if order == "asc" {
						opts.Order = "desc"
					}
					if _, err := store.ListOperatorEvents(ctx, opts); !errors.Is(err, operatorread.ErrInvalidObservabilityCursor) {
						t.Fatalf("opposite-order cursor: %v", err)
					}
					opts.Cursor, opts.Order, opts.Limit = "", order, 129
					whole, err := store.ListOperatorEvents(ctx, opts)
					if err != nil || len(whole.Events) != 129 || whole.NextCursor != "" {
						t.Fatalf("129 returned across raw batches: count=%d cursor=%q err=%v", len(whole.Events), whole.NextCursor, err)
					}
					want := append(append([]operatorread.OperatorEventFull{}, page.Events...), tail.Events...)
					if !reflect.DeepEqual(whole.Events, want) {
						t.Fatal("129-event batch differs from cursor walk/scalar records")
					}
				})
			}
			since, until := base, base.Add(time.Second)
			window, err := store.ListOperatorEvents(ctx, operatorread.OperatorEventListOptions{
				Filter: operatorread.OperatorEventListFilter{RunID: runID, EventName: "batch.boundary"}, Source: "gateway",
				Since: &since, Until: &until, Limit: 129, Order: "asc",
			})
			if err != nil || len(window.Events) != 43 || window.NextCursor != "" {
				t.Fatalf("exclusive since/inclusive until: count=%d cursor=%q err=%v", len(window.Events), window.NextCursor, err)
			}
			for i, got := range window.Events {
				assertOperatorBatchScalarEqual(t, got, scalar[ids[43+i]])
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			page, err := store.ListOperatorEvents(cancelled, operatorread.OperatorEventListOptions{Filter: operatorread.OperatorEventListFilter{RunID: runID}, Limit: 129})
			if !errors.Is(err, context.Canceled) || len(page.Events) != 0 || page.NextCursor != "" {
				t.Fatalf("cancelled read returned partial success: count=%d cursor=%q err=%v", len(page.Events), page.NextCursor, err)
			}
		})
	}
}

func TestOperatorEventBatchConsumedPrefixBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			store := fixture.store.(routeSettlementOperatorStore)
			runID, foreignRun := uuid.NewString(), uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			seedAuthorActivityReceiptRun(t, fixture, ctx, foreignRun)
			base := time.Date(2026, 8, 16, 13, 0, 0, 0, time.UTC)
			var ids [5]string
			var originals [5]operatorread.OperatorEventFull
			failure := testFailureEnvelope(runtimefailures.ClassRetryExhausted, "retry_exhausted", nil)
			for i := range ids {
				ids[i] = uuid.NewString()
				owner := runID
				if i == 4 {
					owner = foreignRun
				}
				route := testEntitylessNodeDeliveryRoute("keep")
				if i == 0 {
					route = testEntitylessNodeDeliveryRoute("drop")
				}
				event := eventtest.ExistingRunRootIngress(ids[i], "batch.prefix", "gateway", "", []byte(`{"prefix":true}`), 0, owner, events.EventEnvelope{}, base.Add(time.Duration(i)*time.Second))
				if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, []events.DeliveryRoute{route}); err != nil {
					t.Fatal(err)
				}
				seedDeliveryStateFixture(t, ctx, fixture.store, event, route, runtimedelivery.StateExhausted, &failure)
				var err error
				originals[i], err = store.LoadOperatorEvent(ctx, ids[i])
				if err != nil || len(originals[i].Deliveries) != 1 || len(originals[i].DeadLetters) != 1 {
					t.Fatalf("prefix seed %d: event=%+v err=%v", i, originals[i], err)
				}
			}
			opts := operatorread.OperatorEventListOptions{Filter: operatorread.OperatorEventListFilter{RunID: runID, SubscriberID: testEntitylessNodeDeliveryRoute("keep").Recipient.ID(), SubscriberType: "node"}, Limit: 1, Order: "asc"}
			clean, err := store.ListOperatorEvents(ctx, opts)
			if err != nil || len(clean.Events) != 1 || clean.NextCursor == "" {
				t.Fatalf("clean prefix: %+v err=%v", clean, err)
			}
			assertOperatorBatchScalarEqual(t, clean.Events[0], originals[1])
			for _, fault := range []string{"event", "delivery", "dead_letter"} {
				// A is filtered out only after admission; C is the matching lookahead;
				// D is beyond that lookahead and must not be admitted on this page.
				for _, position := range []int{0, 2, 3, 4} {
					t.Run(fmt.Sprintf("%s/%c", fault, 'A'+position), func(t *testing.T) {
						switch fault {
						case "event":
							replaceOperatorBatchFixtureCell(t, ctx, fixture, "events", "event_id", ids[position], "route_settlement", `{}`, true)
						case "delivery":
							replaceOperatorBatchFixtureCell(t, ctx, fixture, "event_deliveries", "delivery_id", originals[position].Deliveries[0].DeliveryID, "failure", `{"class":[]}`, true)
						case "dead_letter":
							replaceOperatorBatchFixtureCell(t, ctx, fixture, "dead_letters", "dead_letter_id", originals[position].DeadLetters[0].DeadLetterID, "failure", `{}`, true)
						}
						if _, err := store.LoadOperatorEvent(ctx, ids[position]); err == nil {
							t.Fatal("hostile fixture does not fail scalar admission")
						}
						page, err := store.ListOperatorEvents(ctx, opts)
						if position == 0 || position == 2 {
							assertOperatorBatchRefused(t, page, err)
							return
						}
						if err != nil || !reflect.DeepEqual(page, clean) {
							t.Fatalf("unconsumed/SQL-excluded corruption changed page: %+v err=%v", page, err)
						}
						if position == 3 {
							next := opts
							next.Cursor = page.NextCursor
							page, err = store.ListOperatorEvents(ctx, next)
							assertOperatorBatchRefused(t, page, err)
						}
					})
				}
			}
			// The cursor is B, not raw A nor unreturned C; D corruption is now restored.
			opts.Cursor = clean.NextCursor
			page, err := store.ListOperatorEvents(ctx, opts)
			if err != nil || len(page.Events) != 1 || page.NextCursor == "" {
				t.Fatalf("second matching page: %+v err=%v", page, err)
			}
			assertOperatorBatchScalarEqual(t, page.Events[0], originals[2])
			opts.Cursor = page.NextCursor
			page, err = store.ListOperatorEvents(ctx, opts)
			if err != nil || len(page.Events) != 1 || page.NextCursor != "" {
				t.Fatalf("last matching page: %+v err=%v", page, err)
			}
			assertOperatorBatchScalarEqual(t, page.Events[0], originals[3])
		})
	}
}

func TestOperatorEventBatchEvidenceMembershipAndFreshnessBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			store := fixture.store.(routeSettlementOperatorStore)
			runID, foreignRun := uuid.NewString(), uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			seedAuthorActivityReceiptRun(t, fixture, ctx, foreignRun)
			queued, failed := testEntitylessNodeDeliveryRoute("queued"), testEntitylessNodeDeliveryRoute("failed")
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "batch.evidence", "gateway", "", []byte(`{"evidence":true}`), 0, runID, events.EventEnvelope{}, time.Date(2026, 8, 16, 14, 0, 0, 0, time.UTC))
			if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, []events.DeliveryRoute{failed, queued}); err != nil {
				t.Fatal(err)
			}
			failure := testFailureEnvelope(runtimefailures.ClassRetryExhausted, "retry_exhausted", nil)
			seedDeliveryStateFixture(t, ctx, fixture.store, event, failed, runtimedelivery.StateExhausted, &failure)
			scalar, err := store.LoadOperatorEvent(ctx, event.ID())
			if err != nil || len(scalar.Deliveries) != 2 || len(scalar.DeadLetters) != 1 {
				t.Fatalf("evidence fixture: %+v err=%v", scalar, err)
			}
			currentDeadLetter := scalar.DeadLetters[0]
			// Same failure/time but distinct handler evidence must not be deduplicated
			// or attached to a delivery without an exact delivery/claim association.
			for _, handler := range []string{"aux-b", "aux-a"} {
				if err := fixture.store.(exactDeadLetterStore).RecordDeadLetter(ctx, runtimedeadletters.Record{
					OriginalEventID: event.ID(), OriginalEvent: string(event.Type()), OriginalPayload: event.Payload(),
					FlowInstance: "runtime", HandlerNode: handler, Timestamp: currentDeadLetter.CreatedAt.UTC().Format(time.RFC3339Nano),
					Failure: testFailureEnvelope(runtimefailures.ClassConnectorFailure, "unassociated_failure", nil),
				}); err != nil {
					t.Fatal(err)
				}
			}
			scalar, err = store.LoadOperatorEvent(ctx, event.ID())
			if err != nil || len(scalar.DeadLetters) != 3 {
				t.Fatalf("full event-only evidence: %+v err=%v", scalar, err)
			}
			for i, record := range scalar.DeadLetters {
				if !record.CreatedAt.Equal(currentDeadLetter.CreatedAt) || (i > 0 && scalar.DeadLetters[i-1].DeadLetterID >= record.DeadLetterID) {
					t.Fatalf("tied dead-letter order: %+v", scalar.DeadLetters)
				}
			}
			hasDeadLetter := true
			opts := operatorread.OperatorEventListOptions{Filter: operatorread.OperatorEventListFilter{
				RunID: runID, SubscriberID: queued.Recipient.ID(), SubscriberType: "node", DeliveryStatus: "pending",
				ReasonCode: "retry_exhausted", HasDeadLetter: &hasDeadLetter,
			}, Limit: 2, Order: "asc"}
			page, err := store.ListOperatorEvents(ctx, opts)
			if err != nil || len(page.Events) != 1 || page.NextCursor != "" {
				t.Fatalf("separate delivery/reason predicates: %+v err=%v", page, err)
			}
			assertOperatorBatchScalarEqual(t, page.Events[0], scalar)
			eventOnlyReason := opts
			eventOnlyReason.Filter.ReasonCode = "unassociated_failure"
			eventOnlyPage, err := store.ListOperatorEvents(ctx, eventOnlyReason)
			if err != nil || len(eventOnlyPage.Events) != 1 {
				t.Fatalf("event-only dead-letter reason predicate: %+v err=%v", eventOnlyPage, err)
			}
			assertOperatorBatchScalarEqual(t, eventOnlyPage.Events[0], scalar)
			var queuedID, failedID string
			for _, delivery := range page.Events[0].Deliveries {
				if delivery.SubscriberID == failed.Recipient.ID() {
					failedID = delivery.DeliveryID
					if len(delivery.DeadLetters) != 1 || delivery.DeadLetters[0].ClaimVersion != delivery.ClaimVersion {
						t.Fatalf("current-claim enrichment: %+v", delivery)
					}
				} else {
					queuedID = delivery.DeliveryID
					if len(delivery.DeadLetters) != 0 {
						t.Fatalf("sibling received unrelated dead letter: %+v", delivery)
					}
				}
			}
			if queuedID == "" || failedID == "" {
				t.Fatal("filtered event lost delivery siblings")
			}
			badConjunction := opts
			badConjunction.Filter.DeliveryStatus = "dead_letter"
			assertOperatorBatchEmpty(t, ctx, store, badConjunction)
			withoutDeadLetters := opts
			noDeadLetter := false
			withoutDeadLetters.Filter.HasDeadLetter = &noDeadLetter
			assertOperatorBatchEmpty(t, ctx, store, withoutDeadLetters)
			unfiltered := operatorread.OperatorEventListOptions{Filter: operatorread.OperatorEventListFilter{RunID: runID}, Limit: 2}
			t.Run("claim_version_is_not_event_wide_enrichment", func(t *testing.T) {
				// Deliberately mismatched read projection, not a fabricated historical
				// outcome: retain the original FK-bound dead-letter evidence unchanged.
				replaceOperatorBatchFixtureCell(t, ctx, fixture, "event_deliveries", "delivery_id", failedID, "claim_version", fmt.Sprint(currentDeadLetter.ClaimVersion+1), false)
				got, err := store.ListOperatorEvents(ctx, unfiltered)
				if err != nil || len(got.Events) != 1 || len(got.Events[0].DeadLetters) != 3 {
					t.Fatalf("different-claim evidence lost: %+v err=%v", got, err)
				}
				for _, delivery := range got.Events[0].Deliveries {
					if len(delivery.DeadLetters) != 0 {
						t.Fatalf("different claim attached to current delivery: %+v", delivery)
					}
				}
				want, err := store.LoadOperatorEvent(ctx, event.ID())
				if err != nil {
					t.Fatal(err)
				}
				assertOperatorBatchScalarEqual(t, got.Events[0], want)
			})
			t.Run("cross_run_delivery_must_not_disappear_in_join", func(t *testing.T) {
				corruptOperatorBatchDeliveryRun(t, ctx, fixture, queuedID, runID, foreignRun)
				if _, err := store.LoadOperatorEvent(ctx, event.ID()); err == nil {
					t.Fatal("scalar accepted cross-run delivery")
				}
				page, err := store.ListOperatorEvents(ctx, unfiltered)
				assertOperatorBatchRefused(t, page, err)
			})
			t.Run("missing_routes_are_not_no_delivery", func(t *testing.T) {
				// Moving both rows preserves their real evidence but removes event membership.
				other := eventtest.ExistingRunRootIngress(uuid.NewString(), "batch.other", "gateway", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, event.CreatedAt().Add(time.Second))
				if err := commitSemanticEventFixture(ctx, fixture.store, other); err != nil {
					t.Fatal(err)
				}
				replaceOperatorBatchFixtureCell(t, ctx, fixture, "event_deliveries", "delivery_id", queuedID, "event_id", other.ID(), false)
				replaceOperatorBatchFixtureCell(t, ctx, fixture, "event_deliveries", "delivery_id", failedID, "event_id", other.ID(), false)
				if _, err := store.LoadOperatorEvent(ctx, event.ID()); err == nil {
					t.Fatal("scalar accepted delivery settlement without routes")
				}
				onlyOriginal := unfiltered
				onlyOriginal.Filter.EventName = string(event.Type())
				page, err := store.ListOperatorEvents(ctx, onlyOriginal)
				assertOperatorBatchRefused(t, page, err)
			})
			// A real owner transition must be visible immediately; prior returned values stay sealed.
			before := page.Events[0]
			seedDeliveryStateFixture(t, ctx, fixture.store, event, queued, runtimedelivery.StateDelivered, nil)
			assertOperatorBatchEmpty(t, ctx, store, opts)
			opts.Filter.DeliveryStatus = "delivered"
			after, err := store.ListOperatorEvents(ctx, opts)
			if err != nil || len(after.Events) != 1 {
				t.Fatalf("fresh delivered projection: %+v err=%v", after, err)
			}
			freshScalar, err := store.LoadOperatorEvent(ctx, event.ID())
			if err != nil {
				t.Fatal(err)
			}
			assertOperatorBatchScalarEqual(t, after.Events[0], freshScalar)
			assertOperatorBatchScalarEqual(t, before, scalar)
			if reflect.DeepEqual(before, after.Events[0]) {
				t.Fatal("real delivery transition did not change projection")
			}
		})
	}
}

func assertOperatorBatchScalarEqual(t *testing.T, got, want operatorread.OperatorEventFull) {
	t.Helper()
	// Include the sealed event and non-JSON route/claim fields, not only public JSON.
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("batch event differs from scalar:\n got=%#v\nwant=%#v", got, want)
	}
}

func assertOperatorBatchRefused(t *testing.T, page operatorread.OperatorEventListResult, err error) {
	t.Helper()
	if err == nil || len(page.Events) != 0 || page.NextCursor != "" {
		t.Fatalf("hostile prefix returned partial success: count=%d cursor=%q err=%v", len(page.Events), page.NextCursor, err)
	}
}

func assertOperatorBatchEmpty(t *testing.T, ctx context.Context, store routeSettlementOperatorStore, opts operatorread.OperatorEventListOptions) {
	t.Helper()
	page, err := store.ListOperatorEvents(ctx, opts)
	if err != nil || page.Events == nil || len(page.Events) != 0 || page.NextCursor != "" {
		t.Fatalf("expected non-nil empty page: %+v err=%v", page, err)
	}
}

func replaceOperatorBatchFixtureCell(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, table, key, id, column, value string, jsonColumn bool) {
	t.Helper()
	var original string
	if err := fixture.db.QueryRowContext(ctx, fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1", column, table, key), id).Scan(&original); err != nil {
		t.Fatal(err)
	}
	placeholder := "$1"
	if fixture.dialect == "postgres" && jsonColumn {
		placeholder += "::jsonb"
	}
	query := fmt.Sprintf("UPDATE %s SET %s = %s WHERE %s = $2", table, column, placeholder, key)
	write := func(value string) {
		t.Helper()
		result, err := fixture.db.ExecContext(context.WithoutCancel(ctx), query, value, id)
		if err != nil {
			t.Fatal(err)
		}
		if count, err := result.RowsAffected(); err != nil || count != 1 {
			t.Fatalf("replace %s.%s: rows=%d err=%v", table, column, count, err)
		}
	}
	write(value)
	t.Cleanup(func() { write(original) })
}

func corruptOperatorBatchDeliveryRun(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, deliveryID, originalRun, foreignRun string) {
	t.Helper()
	// Normal writes cannot create this state. Exercise read admission against a
	// preexisting broken event/run join, restoring enforcement before leaving.
	if fixture.dialect == "postgres" {
		const constraint = "event_deliveries_event_id_run_id_fkey"
		var definition string
		if err := fixture.db.QueryRowContext(ctx, `SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='event_deliveries'::regclass AND conname=$1`, constraint).Scan(&definition); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.ExecContext(ctx, `ALTER TABLE event_deliveries DROP CONSTRAINT `+constraint); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if _, err := fixture.db.ExecContext(context.WithoutCancel(ctx), `ALTER TABLE event_deliveries ADD CONSTRAINT `+constraint+` `+definition); err != nil {
				t.Fatal(err)
			}
		})
		replaceOperatorBatchFixtureCell(t, ctx, fixture, "event_deliveries", "delivery_id", deliveryID, "run_id", foreignRun, false)
		return
	}
	conn, err := fixture.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `PRAGMA foreign_keys=OFF`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := conn.ExecContext(context.WithoutCancel(ctx), `PRAGMA foreign_keys=ON`); err != nil {
			t.Fatal(err)
		}
	}()
	if _, err := conn.ExecContext(ctx, `UPDATE event_deliveries SET run_id=$1 WHERE delivery_id=$2`, foreignRun, deliveryID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := fixture.db.ExecContext(context.WithoutCancel(ctx), `UPDATE event_deliveries SET run_id=$1 WHERE delivery_id=$2`, originalRun, deliveryID); err != nil {
			t.Fatal(err)
		}
	})
}
