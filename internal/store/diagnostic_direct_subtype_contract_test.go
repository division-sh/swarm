package store

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/google/uuid"
)

func TestDiagnosticDirectNamedCommitSubtypeIsolationParity(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)

			for _, positive := range []struct {
				name      string
				eventType events.EventType
				runID     string
				envelope  events.EventEnvelope
			}{
				{"runtime_log_without_run", events.EventTypePlatformRuntimeLog, "", events.EventEnvelope{}},
				{"runtime_log_with_run", events.EventTypePlatformRuntimeLog, runID, events.EventEnvelope{}},
				{"inbound_recorded_global", events.EventTypePlatformInboundRecord, runID, events.EventEnvelope{}},
				{"inbound_recorded_entity", events.EventTypePlatformInboundRecord, runID, events.EnvelopeForEntityID(events.EventEnvelope{}, uuid.NewString())},
				{"agent_directive_global", events.EventTypePlatformAgentDirective, runID, events.EventEnvelope{}},
			} {
				t.Run("positive/"+positive.name, func(t *testing.T) {
					event := namedDiagnosticContractEventForTuple(positive.eventType, positive.runID, positive.envelope)
					if err := commitNamedDiagnosticContractEvent(ctx, fixture, positive.eventType, event); err != nil {
						t.Fatalf("matching named operation rejected subtype: %v", err)
					}
				})
			}

			for _, expectedType := range events.DiagnosticDirectEventTypes() {
				for _, actualType := range events.DiagnosticDirectEventTypes() {
					if expectedType == actualType {
						continue
					}
					t.Run(fmt.Sprintf("hostile/%s_rejects_%s", expectedType, actualType), func(t *testing.T) {
						event := namedDiagnosticContractEvent(actualType, runID)
						if err := commitNamedDiagnosticContractEvent(ctx, fixture, expectedType, event); err == nil {
							t.Fatal("named operation accepted another closed subtype")
						}
						assertEventRowAbsent(t, ctx, fixture, event.ID())
					})
				}
			}
		})
	}
}

func namedDiagnosticContractEvent(eventType events.EventType, runID string) events.Event {
	eventRunID := runID
	envelope := events.EventEnvelope{}
	if eventType == events.EventTypePlatformRuntimeLog {
		eventRunID = ""
	}
	if eventType == events.EventTypePlatformInboundRecord {
		envelope = events.EnvelopeForEntityID(envelope, uuid.NewString())
	}
	return namedDiagnosticContractEventForTuple(eventType, eventRunID, envelope)
}

func namedDiagnosticContractEventForTuple(eventType events.EventType, runID string, envelope events.EventEnvelope) events.Event {
	return eventtest.DiagnosticDirect(
		uuid.NewString(), eventType, "runtime", "", []byte(`{}`), 0, runID, "", envelope,
		time.Date(2026, 7, 19, 6, 0, 0, 0, time.UTC),
	)
}

func commitNamedDiagnosticContractEvent(ctx context.Context, fixture authorActivityReceiptFixture, expectedType events.EventType, event events.Event) error {
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return err
	}
	commit := func(store eventCommitTxStore, run func(context.Context, func(context.Context, *sql.Tx) error) error) error {
		return run(ctx, func(txctx context.Context, tx *sql.Tx) error {
			_, err := (sqlPublishCommitter{tx: tx, store: store}).commitNamedEvent(
				txctx, "diagnostic subtype contract proof", events.EventAdmissionDiagnosticDirect, expectedType,
				runtimebus.CommitPublishRequest{Event: admitted, ReplayScope: runtimereplayclaim.CommittedReplayScopeDirect},
			)
			return err
		})
	}
	switch store := fixture.store.(type) {
	case *PostgresStore:
		return commit(store, store.runEventTransaction)
	case *SQLiteRuntimeStore:
		return commit(store, store.runEventTransaction)
	default:
		return fmt.Errorf("unsupported diagnostic subtype fixture store %T", fixture.store)
	}
}

func assertEventRowAbsent(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, eventID string) {
	t.Helper()
	query := `SELECT COUNT(*) FROM events WHERE event_id = ?`
	if fixture.dialect == "postgres" {
		query = `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`
	}
	var count int
	if err := fixture.db.QueryRowContext(ctx, query, eventID).Scan(&count); err != nil {
		t.Fatalf("count rejected named diagnostic event: %v", err)
	}
	if count != 0 {
		t.Fatalf("rejected named diagnostic event rows = %d, want 0", count)
	}
}
