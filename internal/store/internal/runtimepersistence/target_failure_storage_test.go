package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestTargetFailureStoragePreservesExactEventClassAndHandlerBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(interface {
				semanticEventFixtureStore
				exactDeadLetterStore
			})
			ctx := testAuthorActivityContext()
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "storage.target", "runtime", "", []byte(`{}`), 0,
				uuid.NewString(), "", events.EventEnvelope{}, time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC))
			if err := commitSemanticEventFixture(ctx, selected, event); err != nil {
				t.Fatal(err)
			}
			targetFailure := testFailureEnvelope(runtimefailures.ClassTargetUnreachable, "target_unreachable_terminated", map[string]any{
				"target": map[string]any{"flow_instance": "missing-flow"},
			})
			foreignEvent := eventtest.RunCreatingRootIngress(uuid.NewString(), "storage.foreign", "runtime", "", []byte(`{}`), 0,
				uuid.NewString(), "", events.EventEnvelope{}, event.CreatedAt())
			if err := commitSemanticEventFixture(ctx, selected, foreignEvent); err != nil {
				t.Fatal(err)
			}
			record := runtimedeadletters.Record{
				OriginalEventID: event.ID(), OriginalEvent: string(event.Type()), OriginalPayload: event.Payload(),
				FlowInstance: "runtime", Failure: targetFailure, HandlerNode: "other_handler", Timestamp: event.CreatedAt().Add(time.Second).Format(time.RFC3339Nano),
			}
			if err := selected.RecordDeadLetter(ctx, record); err != nil {
				t.Fatal(err)
			}
			record.HandlerNode = "pin_routing"
			record.Failure = testFailureEnvelope(runtimefailures.ClassConnectorFailure, "connector_failed", nil)
			if err := selected.RecordDeadLetter(ctx, record); err != nil {
				t.Fatal(err)
			}
			foreignRecord := record
			foreignRecord.OriginalEventID, foreignRecord.OriginalEvent = foreignEvent.ID(), string(foreignEvent.Type())
			foreignRecord.Failure = targetFailure
			if err := selected.RecordDeadLetter(ctx, foreignRecord); err != nil {
				t.Fatal(err)
			}
			if out, err := ReadTargetFailureDeadLetterStorageForTest(ctx, selected, event.ID()); !errors.Is(err, sql.ErrNoRows) || out != (TargetFailureDeadLetterStorage{}) {
				t.Fatalf("irrelevant event/class/handler borrowed evidence: out=%+v err=%v", out, err)
			}
			record.Failure = targetFailure
			if err := selected.RecordDeadLetter(ctx, record); err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			out, err := ReadTargetFailureDeadLetterStorageForTest(ctx, selected, event.ID())
			if err != nil || out.Reason != "target_unreachable_terminated" || !strings.Contains(out.TargetContext, "missing-flow") {
				t.Fatalf("target failure storage: out=%+v err=%v", out, err)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("target observation escaped original read transaction: %+v", counts)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if out, err := ReadTargetFailureDeadLetterStorageForTest(cancelled, selected, event.ID()); !errors.Is(err, context.Canceled) || out != (TargetFailureDeadLetterStorage{}) {
				t.Fatalf("cancelled target read returned evidence: out=%+v err=%v", out, err)
			}
			if out, err := ReadTargetFailureDeadLetterStorageForTest(ctx, selected, uuid.NewString()); !errors.Is(err, sql.ErrNoRows) || out != (TargetFailureDeadLetterStorage{}) {
				t.Fatalf("missing target read returned evidence: out=%+v err=%v", out, err)
			}
			if probe.Snapshot().Active != 0 {
				t.Fatal("target observation retained work after refusal")
			}
		})
	}
}

func TestTargetFailureStorageRefusesInvalidAndClosedOwnersBothStores(t *testing.T) {
	eventID := uuid.NewString()
	for name, selected := range map[string]any{
		"nil": nil, "typed-nil-postgres": (*PostgresStore)(nil), "typed-nil-sqlite": (*SQLiteRuntimeStore)(nil),
		"uninitialized-postgres": &PostgresStore{}, "uninitialized-sqlite": &SQLiteRuntimeStore{}, "foreign": struct{}{},
	} {
		t.Run(name, func(t *testing.T) {
			if out, err := ReadTargetFailureDeadLetterStorageForTest(context.Background(), selected, eventID); err == nil || out != (TargetFailureDeadLetterStorage{}) {
				t.Fatalf("invalid owner returned evidence: out=%+v err=%v", out, err)
			}
		})
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			if out, err := ReadTargetFailureDeadLetterStorageForTest(context.Background(), fixture.store, "not-a-uuid"); err == nil || out != (TargetFailureDeadLetterStorage{}) {
				t.Fatalf("invalid event returned evidence: out=%+v err=%v", out, err)
			}
			switch owner := fixture.store.(type) {
			case *PostgresStore:
				if err := owner.backend.Close(); err != nil {
					t.Fatal(err)
				}
			case *SQLiteRuntimeStore:
				if err := owner.backend.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if out, err := ReadTargetFailureDeadLetterStorageForTest(context.Background(), fixture.store, eventID); err == nil || out != (TargetFailureDeadLetterStorage{}) {
				t.Fatalf("closed owner returned evidence: out=%+v err=%v", out, err)
			}
		})
	}
}
