package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	privatedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/google/uuid"
)

type m29BatchQueryCounter struct {
	eventReadQueryer
	rows, lists, maxArgs int
}

func (q *m29BatchQueryCounter) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	q.rows++
	q.maxArgs = max(q.maxArgs, len(args))
	return q.eventReadQueryer.QueryRowContext(ctx, query, args...)
}

func (q *m29BatchQueryCounter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	q.lists++
	q.maxArgs = max(q.maxArgs, len(args))
	return q.eventReadQueryer.QueryContext(ctx, query, args...)
}

func m29LoadBatch(ctx context.Context, q eventReadQueryer, postgres bool, ids []string) ([]eventrecord.AdmittedRecord, error) {
	if postgres {
		return eventrecordpostgres.LoadAdmittedMany(ctx, q, ids)
	}
	return eventrecordsqlite.LoadAdmittedMany(ctx, q, ids)
}

func m29DeliveryAdapter(t *testing.T, postgres bool) *privatedelivery.Adapter {
	t.Helper()
	dialect := privatedelivery.DialectSQLite
	if postgres {
		dialect = privatedelivery.DialectPostgres
	}
	adapter, err := privatedelivery.NewAdapter(dialect)
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func TestFanOutBatchPhysicalReadCountAndScalarParityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			ctx := testAuthorActivityContext()
			base := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
			fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 129, base)
			ids := make([]string, 129)
			for i := range ids {
				event := fanOutBarrierChildEvent(t, fixture, i, base.Add(time.Duration(i)*time.Microsecond))
				var routes []events.DeliveryRoute
				if i > 0 {
					routes = []events.DeliveryRoute{fanOutBarrierRoute("first")}
				}
				if i == 1 {
					routes = append(routes, fanOutBarrierRoute("second"))
				}
				if err := commitSemanticEventFixtureWithRoutes(ctx, owner, event, routes); err != nil {
					t.Fatal(err)
				}
				ids[i] = event.ID()
			}
			seedFanOutBarrierOutcomes(t, ctx, db, fixture, ids, false, base.Add(time.Second))
			adapter := m29DeliveryAdapter(t, postgres)
			for _, size := range []int{0, 1, 18, 128, 129} {
				t.Run(fmt.Sprintf("events_%d", size), func(t *testing.T) {
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					requested := append([]string(nil), ids[:size]...)
					for i, j := 0, size-1; i < j; i, j = i+1, j-1 {
						requested[i], requested[j] = requested[j], requested[i]
					}
					q := &m29BatchQueryCounter{eventReadQueryer: tx}
					admitted, err := m29LoadBatch(ctx, q, postgres, requested)
					chunks := (size + 127) / 128
					if err != nil || len(admitted) != size || q.rows != 0 || q.lists != chunks || q.maxArgs > 128 {
						t.Fatalf("event batch: len=%d rows=%d lists=%d maxArgs=%d chunks=%d err=%v", len(admitted), q.rows, q.lists, q.maxArgs, chunks, err)
					}
					q = &m29BatchQueryCounter{eventReadQueryer: tx}
					deliveries, err := adapter.SnapshotsForEvents(ctx, q, requested)
					wantClock := 0
					if size > 0 {
						wantClock = 1
					}
					if err != nil || len(deliveries) != size || q.rows != wantClock || q.lists != 2*chunks || q.maxArgs > 128 {
						t.Fatalf("delivery batch: len=%d rows=%d lists=%d maxArgs=%d chunks=%d err=%v", len(deliveries), q.rows, q.lists, q.maxArgs, chunks, err)
					}
					for i, id := range requested {
						scalar, settlement, found, err := loadJointSourceReadAdmitted(ctx, tx, postgres, id)
						if err != nil || !found || admitted[i].Event.Event().ID() != id {
							t.Fatalf("scalar membership/order %s: found=%v err=%v", id, found, err)
						}
						want, err := eventrecord.FromAdmitted(scalar, settlement)
						if err != nil {
							t.Fatal(err)
						}
						got, err := eventrecord.FromAdmitted(admitted[i].Event, admitted[i].Settlement)
						if err != nil || !got.Equal(want) {
							t.Fatalf("full admitted event/settlement differs for %s: %v", id, err)
						}
						wantDeliveries, err := adapter.SnapshotsForEvent(ctx, tx, id)
						if err != nil || !reflect.DeepEqual(deliveries[id], wantDeliveries) {
							t.Fatalf("full delivery snapshots/order differ for %s: %v", id, err)
						}
					}
					t.Logf("physical reads: events=%d event_queries=%d delivery_queries=%d clock_queries=%d", size, chunks, 2*chunks, wantClock)
				})
			}
			// Execute the actual fold consumer, not just the canonical adapters.
			summary, err := owner.FanOutRunSummary(ctx, fixture.runID, time.Now().UTC())
			if err != nil || summary.Committed != 129 || summary.Settled != 1 || summary.Unsettled != 128 {
				t.Fatalf("actual mixed no-route/pending fold: %+v err=%v", summary, err)
			}
			for _, duplicate := range [][]string{{ids[0], ids[0]}, append(append([]string(nil), ids...), ids[0])} {
				q := &m29BatchQueryCounter{eventReadQueryer: db}
				if got, err := m29LoadBatch(ctx, q, postgres, duplicate); err == nil || got != nil || q.rows+q.lists != 0 {
					t.Fatalf("duplicate event IDs admitted or queried: %v", err)
				}
				if got, err := adapter.SnapshotsForEvents(ctx, q, duplicate); err == nil || got != nil || q.rows+q.lists != 0 {
					t.Fatalf("duplicate delivery event IDs admitted or queried: %v", err)
				}
			}
			missing := uuid.NewString()
			got, err := m29LoadBatch(ctx, db, postgres, []string{ids[0], missing})
			var absent *eventrecord.MissingError
			if got != nil || !errors.Is(err, eventrecord.ErrMissing) || !errors.As(err, &absent) || absent.EventID != missing {
				t.Fatalf("batch must report exact missing ID without partial result: %v", err)
			}
			noDeliveries, err := adapter.SnapshotsForEvents(ctx, db, []string{missing})
			scalarEmpty, scalarErr := adapter.SnapshotsForEvent(ctx, db, missing)
			if err != nil || scalarErr != nil || !reflect.DeepEqual(noDeliveries[missing], scalarEmpty) {
				t.Fatalf("zero delivery membership scalar parity: batch=%v scalar=%v", err, scalarErr)
			}
		})
	}
}

func TestFanOutBatchEventAdmissionHostileBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			fixture, ctx, group, _ := prepareP16CommittedGroup(t, owner.(selectedFanOutLifecycleOwner), db, backend)
			if err := group.Close(ctx); err != nil {
				t.Fatal(err)
			}
			var ids []string
			rows, err := db.QueryContext(ctx, `SELECT event_id FROM fan_out_outcomes WHERE run_id=$1 ORDER BY ordinal`, fixture.runID)
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err != nil {
					t.Fatal(err)
				}
				ids = append(ids, id)
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			if len(ids) != 2 {
				t.Fatalf("need complete two-member group, got %d", len(ids))
			}
			originalBatch, err := m29LoadBatch(ctx, db, postgres, ids)
			if err != nil {
				t.Fatal(err)
			}
			original, err := eventrecord.FromAdmitted(originalBatch[1].Event, originalBatch[1].Settlement)
			if err != nil {
				t.Fatal(err)
			}
			for _, corruption := range []string{"unknown_settlement_field", "invalid_settlement_arm", "wrong_event_class", "payload_identity", "inherited_owner_mismatch"} {
				t.Run(corruption, func(t *testing.T) {
					hostile := original.Clone()
					switch corruption {
					case "unknown_settlement_field", "invalid_settlement_arm":
						var wire map[string]json.RawMessage
						if err := json.Unmarshal(hostile.RouteSettlement, &wire); err != nil {
							t.Fatal(err)
						}
						if corruption == "unknown_settlement_field" {
							wire["unowned"] = json.RawMessage(`true`)
						} else {
							wire["arm"] = json.RawMessage(`"invented"`)
						}
						hostile.RouteSettlement, err = json.Marshal(wire)
					case "wrong_event_class":
						settlement, createErr := events.NewNoDeliverySettlement(events.EventWriteDirectiveDirect, events.NoDeliveryNoSubscriberByDesign, events.ConnectEvaluationLedger{})
						if createErr != nil {
							t.Fatal(createErr)
						}
						hostile.RouteSettlement, err = json.Marshal(settlement)
					case "payload_identity":
						hostile.Payload = []byte(`{"unowned":null}`)
					case "inherited_owner_mismatch":
						declaration, admissionErr := identity.AdmitDeclarationIdentity(fixture.flowPath, "fan_out", fixture.semanticPath)
						if admissionErr != nil {
							t.Fatal(admissionErr)
						}
						origin, originErr := events.NewInheritedFanOutOrigin(fixture.runID, uuid.NewString(), fixture.eventID, uuid.NewString(), declaration, fixture.bundleHash, "sha256:"+strings.Repeat("3", 64), 1)
						if originErr != nil {
							t.Fatal(originErr)
						}
						hostile.Class, hostile.SourceEventID = events.EventAdmissionInheritedFanOut, ""
						hostile.InheritedFanOutOrigin, err = json.Marshal(origin)
						if _, _, err := hostile.DecodeWithSettlement(); err != nil {
							t.Fatalf("owner mismatch must pass full codec before SQL owner rejection: %v", err)
						}
					}
					if err != nil {
						t.Fatal(err)
					}
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					writeJointSourceReadRecord(t, ctx, tx, postgres, hostile)
					got, batchErr := m29LoadBatch(ctx, tx, postgres, ids)
					_, _, _, scalarErr := loadJointSourceReadAdmitted(ctx, tx, postgres, ids[1])
					assertJointSourceReadCorrupt(t, ids[1], scalarErr)
					assertJointSourceReadCorrupt(t, ids[1], batchErr)
					if got != nil {
						t.Fatal("batch leaked admitted prefix before hostile suffix")
					}
					if err := tx.Rollback(); err != nil {
						t.Fatal(err)
					}
					if _, err := m29LoadBatch(ctx, db, postgres, ids); err != nil {
						t.Fatalf("rollback must restore fresh admission: %v", err)
					}
				})
			}
		})
	}
}

func TestFanOutBatchDeliveryMembershipHostileBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			ctx := testAuthorActivityContext()
			base := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
			fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 1, base)
			other := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 1, base)
			event := fanOutBarrierChildEvent(t, fixture, 0, base)
			if err := commitSemanticEventFixtureWithRoutes(ctx, owner, event, []events.DeliveryRoute{fanOutBarrierRoute("first"), fanOutBarrierRoute("second")}); err != nil {
				t.Fatal(err)
			}
			seedFanOutBarrierOutcomes(t, ctx, db, fixture, []string{event.ID()}, false, base.Add(time.Second))
			adapter := m29DeliveryAdapter(t, postgres)
			baseline, err := adapter.SnapshotsForEvent(ctx, db, event.ID())
			if err != nil || len(baseline) != 2 {
				t.Fatalf("fixture deliveries=%d err=%v", len(baseline), err)
			}
			var summaryOwner selectedFanOutTxSummaryOwner
			switch store := owner.(type) {
			case *PostgresStore:
				summaryOwner = store.pipelinePostgresOwner
			case *SQLiteRuntimeStore:
				summaryOwner = store.pipelineSQLiteOwner
			}
			for _, corruption := range []string{"route_identity", "pending_shape", "cross_run_hidden_member"} {
				t.Run(corruption, func(t *testing.T) {
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					// Shadow only this transaction's delivery table so corrupt
					// physical rows reach canonical admission instead of being
					// stopped by the production CHECK/FK constraints first.
					if _, err := tx.ExecContext(ctx, `CREATE TEMP TABLE event_deliveries AS SELECT * FROM event_deliveries`); err != nil {
						t.Fatal(err)
					}
					query := `UPDATE event_deliveries SET route_identity=$1 WHERE delivery_id=$2`
					args := []any{"delivery-route-v2:sha256:" + strings.Repeat("f", 64), baseline[1].DeliveryID}
					sentinel := runtimedelivery.ErrConflict
					if corruption == "pending_shape" {
						query = `UPDATE event_deliveries SET reason_code='impossible-pending' WHERE delivery_id=$1`
						args = []any{baseline[1].DeliveryID}
					}
					if corruption == "cross_run_hidden_member" {
						query, args = `UPDATE event_deliveries SET run_id=$1 WHERE delivery_id=$2`, []any{other.runID, baseline[1].DeliveryID}
						sentinel = runtimedelivery.ErrNotFound
					}
					if _, err := tx.ExecContext(ctx, query, args...); err != nil {
						t.Fatalf("install hostile transaction-local row: %v", err)
					}
					got, batchErr := adapter.SnapshotsForEvents(ctx, tx, []string{event.ID()})
					scalar, scalarErr := adapter.SnapshotsForEvent(ctx, tx, event.ID())
					if got != nil || scalar != nil || !errors.Is(batchErr, sentinel) || !errors.Is(scalarErr, sentinel) {
						t.Fatalf("scalar/batch negative semantics: scalar=%v batch=%v want=%v", scalarErr, batchErr, sentinel)
					}
					if _, err := summaryOwner.SummarizeFanOutRunTx(ctx, tx, fixture.runID, base.Add(time.Second)); !errors.Is(err, sentinel) {
						t.Fatalf("actual fold hid hostile direct member: %v", err)
					}
				})
			}
			t.Run("cross_run_outcome_event", func(t *testing.T) {
				foreign := fanOutBarrierChildEvent(t, other, 0, base)
				if err := commitSemanticEventFixtureWithRoutes(ctx, owner, foreign, nil); err != nil {
					t.Fatal(err)
				}
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				if _, err := tx.ExecContext(ctx, `UPDATE fan_out_outcomes SET event_id=$1 WHERE run_id=$2`, foreign.ID(), fixture.runID); err != nil {
					t.Fatal(err)
				}
				if _, err := summaryOwner.SummarizeFanOutRunTx(ctx, tx, fixture.runID, base.Add(time.Second)); err == nil || !strings.Contains(err.Error(), "belongs to another run") {
					t.Fatalf("actual fold accepted foreign canonical event: %v", err)
				}
			})
			fresh, err := adapter.SnapshotsForEvents(ctx, db, []string{event.ID()})
			if err != nil || !reflect.DeepEqual(fresh[event.ID()], baseline) {
				t.Fatalf("rollback must restore exact membership without cache: %v", err)
			}
		})
	}
}
