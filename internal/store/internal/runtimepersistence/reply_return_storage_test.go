package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestReplyReturnStoragePreservesHistoryDuplicateAndEventRunScopesBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx, runID, otherRun := testAuthorActivityContext(), uuid.NewString(), uuid.NewString()
			eventID, otherEvent := uuid.NewString(), uuid.NewString()
			now := time.Now().UTC().Truncate(time.Microsecond)
			for _, scope := range [][2]string{{runID, eventID}, {otherRun, otherEvent}} {
				seedAuthorActivityReceiptRun(t, fixture, ctx, scope[0])
				if err := commitSemanticParentFixture(ctx, fixture.store, scope[0], scope[1], now); err != nil {
					t.Fatal(err)
				}
			}
			// Explicit physical cardinality controls, not simulated reply admission
			// or terminal claims. The public journey separately proves those owners.
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				for _, row := range []struct {
					run, accepted, terminal any
					event, state            string
				}{
					{runID, nil, nil, eventID, "open"},
					{runID, eventID, now, eventID, "terminal"},
					{runID, eventID, now, eventID, "terminal"},
					{otherRun, nil, nil, otherEvent, "open"},
					{nil, nil, nil, eventID, "open"},
				} {
					id := uuid.NewString()
					if _, err := tx.ExecContext(ctx, `INSERT INTO reply_contexts
						(reply_context_id,run_id,request_event_id,requester_flow_id,request_output_pin,reply_input_pin,provider_flow_id,
						provider_input_pin,provider_output_pin,origin_route,request_correlation_id,state,accepted_reply_event_id,created_at,updated_at,terminal_at)
						VALUES ($1,$2,$3,'requester','out','reply','provider','in','out','{}',$1,$4,$5,$6,$6,$7)`,
						id, row.run, row.event, row.state, row.accepted, now, row.terminal); err != nil {
						return err
					}
				}
				for _, original := range []any{eventID, eventID, eventID, otherEvent, nil} {
					if _, err := tx.ExecContext(ctx, `INSERT INTO dead_letters
						(dead_letter_id,original_event_id,original_event,original_payload,flow_instance,failure,created_at)
						VALUES ($1,$2,'physical.return','{}','physical/return','{"class":"internal_failure"}',$3)`,
						uuid.NewString(), original, now); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			want := ReplyReturnStorageEvidence{Contexts: 3, EventLinkedDeadLetters: 3}
			if got, err := ReadReplyReturnStorageForTest(ctx, fixture.store, runID); err != nil || got != want {
				t.Fatalf("historical/duplicate exact-run evidence changed: %+v %v want %+v", got, err, want)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("return witness bypassed original snapshot: %+v", counts)
			}
			if got, err := ReadReplyReturnStorageForTest(ctx, fixture.store, otherRun); err != nil || got != (ReplyReturnStorageEvidence{Contexts: 1, EventLinkedDeadLetters: 1}) {
				t.Fatalf("sibling run/NULL event association leaked: %+v %v", got, err)
			}
			if got, err := ReadReplyReturnStorageForTest(ctx, fixture.store, uuid.NewString()); err != nil || got != (ReplyReturnStorageEvidence{}) {
				t.Fatalf("absent run fabricated return history: %+v %v", got, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := ReadReplyReturnStorageForTest(cancelled, fixture.store, runID); !errors.Is(err, context.Canceled) || got != (ReplyReturnStorageEvidence{}) {
				t.Fatalf("cancelled snapshot returned counts: %+v %v", got, err)
			}
			if got, err := ReadReplyReturnStorageForTest(ctx, fixture.store, runID); err != nil || got != want || probe.Snapshot().Total.WriteCommits != 0 {
				t.Fatalf("observation changed return history: %+v %v", got, err)
			}
		})
	}
}

func TestReplyReturnStorageRefusesInvalidOwnersAndUnavailableEvidenceBothStores(t *testing.T) {
	ctx, runID := context.Background(), uuid.NewString()
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadReplyReturnStorageForTest(ctx, owner, runID); err == nil || got != (ReplyReturnStorageEvidence{}) {
			t.Errorf("invalid owner %T returned counts: %+v %v", owner, got, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, invalid := range []string{"", "bad", uuid.Nil.String(), "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"} {
				if got, err := ReadReplyReturnStorageForTest(ctx, fixture.store, invalid); err == nil || got != (ReplyReturnStorageEvidence{}) {
					t.Errorf("invalid run returned counts: %+v %v", got, err)
				}
			}
			seedAuthorActivityReceiptRun(t, fixture, testAuthorActivityContext(), runID)
			eventID, now := uuid.NewString(), time.Now().UTC().Truncate(time.Microsecond)
			if err := commitSemanticParentFixture(testAuthorActivityContext(), fixture.store, runID, eventID, now); err != nil {
				t.Fatal(err)
			}
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `INSERT INTO reply_contexts
					(reply_context_id,run_id,request_event_id,requester_flow_id,request_output_pin,reply_input_pin,provider_flow_id,
					provider_input_pin,provider_output_pin,origin_route,request_correlation_id,state,created_at,updated_at)
					VALUES ($1,$2,$3,'requester','out','reply','provider','in','out','{}',$1,'open',$4,$4)`,
					uuid.NewString(), runID, eventID, now)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadReplyReturnStorageForTest(ctx, fixture.store, runID); err != nil || got != (ReplyReturnStorageEvidence{Contexts: 1}) {
				t.Fatalf("partial-read checkpoint lacks a persisted context: %+v %v", got, err)
			}
			rename := func(from, to string) {
				t.Helper()
				if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, "ALTER TABLE "+from+" RENAME TO "+to)
					return err
				}); err != nil {
					t.Fatal(err)
				}
			}
			rename("dead_letters", "unavailable_return_storage_dead_letters")
			restored := false
			defer func() {
				if !restored {
					rename("unavailable_return_storage_dead_letters", "dead_letters")
				}
			}()
			if got, err := ReadReplyReturnStorageForTest(ctx, fixture.store, runID); err == nil || got != (ReplyReturnStorageEvidence{}) {
				t.Fatalf("unavailable association returned partial counts: %+v %v", got, err)
			}
			rename("unavailable_return_storage_dead_letters", "dead_letters")
			restored = true
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadReplyReturnStorageForTest(ctx, fixture.store, runID); err == nil || got != (ReplyReturnStorageEvidence{}) {
				t.Fatalf("closed owner returned counts: %+v %v", got, err)
			}
		})
	}
}
