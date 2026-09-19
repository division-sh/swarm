package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/google/uuid"
)

func TestFanOutJointSourceReadAdmissionAndFreshnessBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, sibling, db, postgres := newFanOutOwnerPairForTest(t, backend)
			fixture, ctx, group, _ := prepareP16CommittedGroup(t, selected.(selectedFanOutLifecycleOwner), db, backend)
			if err := group.Close(ctx); err != nil {
				t.Fatal(err)
			}
			var summaryOwner selectedFanOutTxSummaryOwner
			switch store := selected.(type) {
			case *PostgresStore:
				summaryOwner = store.pipelinePostgresOwner
			case *SQLiteRuntimeStore:
				summaryOwner = store.pipelineSQLiteOwner
			default:
				t.Fatalf("unexpected selected owner %T", selected)
			}
			var eventID string
			if err := db.QueryRowContext(ctx, `SELECT event_id FROM fan_out_outcomes WHERE run_id=$1 AND ordinal=0`, fixture.runID).Scan(&eventID); err != nil {
				t.Fatal(err)
			}
			var original eventrecord.Record
			var found bool
			var err error
			if postgres {
				original, found, err = eventrecordpostgres.Load(ctx, db, eventID)
			} else {
				original, found, err = eventrecordsqlite.Load(ctx, db, eventID)
			}
			if err != nil || !found {
				t.Fatalf("load committed fixture: found=%v err=%v", found, err)
			}
			observedAt := time.Now().UTC()
			baseline, err := selected.FanOutRunSummary(ctx, fixture.runID, observedAt)
			if err != nil || baseline.Committed != 2 || baseline.Settled != 2 || baseline.Unsettled != 0 || baseline.Owed != 0 {
				t.Fatalf("fixture must traverse two committed no-route events: %+v err=%v", baseline, err)
			}
			assertJointSourceReadRecord(t, ctx, db, postgres, original)
			assertRestored := func(t *testing.T) {
				t.Helper()
				for _, owner := range []selectedFanOutOwner{selected, sibling} {
					got, err := owner.FanOutRunSummary(ctx, fixture.runID, observedAt)
					if err != nil || !reflect.DeepEqual(got, baseline) {
						t.Fatalf("fresh owner summary after rollback/restore: got=%+v want=%+v err=%v", got, baseline, err)
					}
				}
				assertJointSourceReadRecord(t, ctx, db, postgres, original)
			}

			t.Run("missing_record", func(t *testing.T) {
				admitted, settlement, found, err := loadJointSourceReadAdmitted(ctx, db, postgres, uuid.NewString())
				if err != nil || found || admitted.ID() != "" || settlement.WriteClass().Code() != "" {
					t.Fatalf("missing event leaked evidence: id=%s class=%s found=%v err=%v", admitted.ID(), settlement.WriteClass().Code(), found, err)
				}
				assertRestored(t)
			})

			for _, corruption := range []string{"unknown_settlement_field", "invalid_settlement_arm", "valid_settlement_wrong_event_class", "invalid_payload_with_valid_settlement", "inherited_owner_mismatch"} {
				t.Run(corruption, func(t *testing.T) {
					assertRestored(t)
					hostile := original.Clone()
					switch corruption {
					case "unknown_settlement_field", "invalid_settlement_arm":
						var wire map[string]json.RawMessage
						if err := json.Unmarshal(hostile.RouteSettlement, &wire); err != nil {
							t.Fatal(err)
						}
						if corruption == "unknown_settlement_field" {
							wire["unowned_evidence"] = json.RawMessage(`true`)
						} else {
							wire["arm"] = json.RawMessage(`"invented"`)
						}
						hostile.RouteSettlement, err = json.Marshal(wire)
						if err != nil {
							t.Fatal(err)
						}
					case "valid_settlement_wrong_event_class":
						wrongClass, err := events.NewNoDeliverySettlement(events.EventWriteDirectiveDirect, events.NoDeliveryNoSubscriberByDesign, events.ConnectEvaluationLedger{})
						if err != nil {
							t.Fatal(err)
						}
						hostile.RouteSettlement, err = json.Marshal(wrongClass)
						if err != nil {
							t.Fatal(err)
						}
					case "invalid_payload_with_valid_settlement":
						hostile.Payload = []byte(`{"unowned":null}`)
					case "inherited_owner_mismatch":
						declaration, err := identity.AdmitDeclarationIdentity(fixture.flowPath, "fan_out", fixture.semanticPath)
						if err != nil {
							t.Fatal(err)
						}
						origin, err := events.NewInheritedFanOutOrigin(fixture.runID, uuid.NewString(), fixture.eventID, uuid.NewString(), declaration, fixture.bundleHash, "sha256:"+strings.Repeat("3", 64), 0)
						if err != nil {
							t.Fatal(err)
						}
						hostile.Class, hostile.SourceEventID = events.EventAdmissionInheritedFanOut, ""
						hostile.InheritedFanOutOrigin, err = json.Marshal(origin)
						if err != nil {
							t.Fatal(err)
						}
						// This must be codec-valid: rejection must come from the
						// SQL committed-owner proof, not malformed origin JSON.
						if _, _, err := hostile.DecodeWithSettlement(); err != nil {
							t.Fatalf("owner-mismatch fixture failed codec admission: %v", err)
						}
					}
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					assertJointSourceReadSummary(t, ctx, tx, summaryOwner, fixture.runID, observedAt, baseline)
					writeJointSourceReadRecord(t, ctx, tx, postgres, hostile)
					admitted, settlement, found, readErr := loadJointSourceReadAdmitted(ctx, tx, postgres, eventID)
					assertJointSourceReadCorrupt(t, eventID, readErr)
					if found || admitted.ID() != "" || settlement.WriteClass().Code() != "" {
						t.Fatal("joint adapter returned partial admission for corrupt record")
					}
					_, summaryErr := summaryOwner.SummarizeFanOutRunTx(ctx, tx, fixture.runID, observedAt)
					assertJointSourceReadCorrupt(t, eventID, summaryErr)
					if !strings.Contains(summaryErr.Error(), "load fan-out ordinal 0 settlement") {
						t.Fatalf("summary did not reach exact fan-out source read: %v", summaryErr)
					}
					if err := tx.Rollback(); err != nil {
						t.Fatal(err)
					}
					assertRestored(t)
				})
			}

			t.Run("committed_corruption_rejected_then_repaired", func(t *testing.T) {
				assertRestored(t)
				hostile := original.Clone()
				var wire map[string]json.RawMessage
				if err := json.Unmarshal(hostile.RouteSettlement, &wire); err != nil {
					t.Fatal(err)
				}
				wire["unowned_evidence"] = json.RawMessage(`true`)
				hostile.RouteSettlement, err = json.Marshal(wire)
				if err != nil {
					t.Fatal(err)
				}
				writeJointSourceReadRecord(t, ctx, db, postgres, hostile)
				defer writeJointSourceReadRecord(t, ctx, db, postgres, original)
				for _, owner := range []selectedFanOutOwner{selected, sibling} {
					_, err := owner.FanOutRunSummary(ctx, fixture.runID, observedAt)
					assertJointSourceReadCorrupt(t, eventID, err)
					if !strings.Contains(err.Error(), "load fan-out ordinal 0 settlement") {
						t.Fatalf("public summary did not reach exact source read: %v", err)
					}
				}
				writeJointSourceReadRecord(t, ctx, db, postgres, original)
				assertRestored(t)
			})

			t.Run("fresh_bytes_same_transaction_rollback_and_commit", func(t *testing.T) {
				changed := original.Clone()
				changed.Payload = []byte(`{"fresh_input":"changed"}`)
				_, originalSettlement, err := original.DecodeWithSettlement()
				if err != nil {
					t.Fatal(err)
				}
				reason := events.NoDeliveryResolutionBlocked
				if originalSettlement.Reason() == reason {
					reason = events.NoDeliveryDeclaredConsumerNoPlan
				}
				changedSettlement, err := events.NewNoDeliverySettlement(originalSettlement.WriteClass(), reason, originalSettlement.Ledger())
				if err != nil {
					t.Fatal(err)
				}
				changed.RouteSettlement, err = json.Marshal(changedSettlement)
				if err != nil {
					t.Fatal(err)
				}
				for _, commit := range []bool{false, true} {
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					assertJointSourceReadRecord(t, ctx, tx, postgres, original)
					assertJointSourceReadSummary(t, ctx, tx, summaryOwner, fixture.runID, observedAt, baseline)
					writeJointSourceReadRecord(t, ctx, tx, postgres, changed)
					assertJointSourceReadRecord(t, ctx, tx, postgres, changed)
					assertJointSourceReadSummary(t, ctx, tx, summaryOwner, fixture.runID, observedAt, baseline)
					if commit {
						if err := tx.Commit(); err != nil {
							t.Fatal(err)
						}
						assertJointSourceReadRecord(t, ctx, db, postgres, changed)
						if got, err := selected.FanOutRunSummary(ctx, fixture.runID, observedAt); err != nil || !reflect.DeepEqual(got, baseline) {
							t.Fatalf("committed fresh summary: %+v err=%v", got, err)
						}
						writeJointSourceReadRecord(t, ctx, db, postgres, original)
					} else if err := tx.Rollback(); err != nil {
						t.Fatal(err)
					}
					assertRestored(t)
				}
			})
		})
	}
}

func loadJointSourceReadAdmitted(ctx context.Context, q eventrecordsqlite.RowQueryer, postgres bool, eventID string) (events.AdmittedEvent, events.RouteSettlement, bool, error) {
	if postgres {
		return eventrecordpostgres.LoadAdmitted(ctx, q, eventID)
	}
	return eventrecordsqlite.LoadAdmitted(ctx, q, eventID)
}

func assertJointSourceReadRecord(t *testing.T, ctx context.Context, q eventrecordsqlite.RowQueryer, postgres bool, want eventrecord.Record) {
	t.Helper()
	admitted, settlement, found, err := loadJointSourceReadAdmitted(ctx, q, postgres, want.EventID)
	if err != nil || !found {
		t.Fatalf("joint adapter read: found=%v err=%v", found, err)
	}
	got, err := eventrecord.FromAdmitted(admitted, settlement)
	if err != nil || !want.Equal(got) {
		t.Fatalf("joint adapter lost exact event/settlement input: err=%v payload=%s settlement=%s", err, got.Payload, got.RouteSettlement)
	}
}

func assertJointSourceReadCorrupt(t *testing.T, eventID string, err error) {
	t.Helper()
	var corrupt *eventrecord.CorruptError
	if !errors.Is(err, eventrecord.ErrCorrupt) || !errors.As(err, &corrupt) || corrupt.EventID != eventID {
		t.Fatalf("expected exact event corruption for %s, got %v", eventID, err)
	}
}

func assertJointSourceReadSummary(t *testing.T, ctx context.Context, tx *sql.Tx, owner selectedFanOutTxSummaryOwner, runID string, now time.Time, want fanoutobligation.RunSummary) {
	t.Helper()
	got, err := owner.SummarizeFanOutRunTx(ctx, tx, runID, now)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("transaction summary changed: got=%+v want=%+v err=%v", got, want, err)
	}
}

func writeJointSourceReadRecord(t *testing.T, ctx context.Context, db interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, postgres bool, record eventrecord.Record) {
	t.Helper()
	// Corruption fixtures preserve SQL schema constraints and update both
	// payload representations; adapter admission, not invalid SQL, must fail.
	query := `UPDATE events SET payload=$1, payload_bytes=$2, route_settlement=$3, event_class=$4, source_event_id=NULLIF($5,''), inherited_fan_out_origin=NULLIF($6,'') WHERE event_id=$7`
	if postgres {
		query = `UPDATE events SET payload=$1::jsonb, payload_bytes=$2::bytea, route_settlement=$3::jsonb, event_class=$4, source_event_id=NULLIF($5,'')::uuid, inherited_fan_out_origin=NULLIF($6,'')::jsonb WHERE event_id=$7::uuid`
	}
	result, err := db.ExecContext(ctx, query, string(record.Payload), record.Payload, string(record.RouteSettlement), record.Class, record.SourceEventID, string(record.InheritedFanOutOrigin), record.EventID)
	if err != nil {
		t.Fatalf("install exact joint-read fixture: %v", err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		t.Fatalf("fixture affected %d rows: %v", count, err)
	}
}
