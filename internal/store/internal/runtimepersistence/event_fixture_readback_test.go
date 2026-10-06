package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestCanonicalEventFixtureReadbackUsesOriginalReadOwnerParity(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "fixture.readback", "gateway", "read-task", []byte("{\n  \"value\": 1.0\n}"), 2, uuid.NewString(), "", events.EventEnvelope{}, time.Date(2026, 10, 2, 12, 0, 0, 123456000, time.UTC))
			if err := commitSemanticEventFixture(ctx, fixture.store.(semanticEventFixtureStore), event); err != nil {
				t.Fatal(err)
			}
			var probe *transactiontest.Collector
			var restore func()
			var err error
			switch owner := fixture.store.(type) {
			case *PostgresStore:
				probe, restore, err = owner.backend.InstallTransactionProbeForTest(transactiontest.Options{})
			case *SQLiteRuntimeStore:
				probe, restore, err = owner.backend.InstallTransactionProbeForTest(transactiontest.Options{})
			}
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			got, err := LoadCanonicalEventRecordForTest(ctx, fixture.store, event.ID())
			if err != nil || got.ID() != event.ID() || string(got.Payload()) != string(event.Payload()) || !got.CreatedAt().Equal(event.CreatedAt()) {
				t.Fatalf("complete readback: event=%#v err=%v", got, err)
			}
			counts := probe.Snapshot()
			if counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("readback did not use exactly one original-owner snapshot: %+v", counts)
			}
			if absent, err := LoadCanonicalEventRecordForTest(ctx, fixture.store, uuid.NewString()); err == nil || absent.ID() != "" {
				t.Fatalf("missing row returned evidence: event=%#v err=%v", absent, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if absent, err := LoadCanonicalEventRecordForTest(cancelled, fixture.store, event.ID()); !errors.Is(err, context.Canceled) || absent.ID() != "" {
				t.Fatalf("cancelled read returned evidence: event=%#v err=%v", absent, err)
			}
			if got, err := LoadCanonicalEventRecordForTest(ctx, fixture.store, event.ID()); err != nil || got.ID() != event.ID() {
				t.Fatalf("read after refusal: event=%#v err=%v", got, err)
			}
			if probe.Snapshot().Active != 0 {
				t.Fatal("readback left an active snapshot")
			}
		})
	}
}

func TestCanonicalEventFixtureReadbackRejectsInvalidOwners(t *testing.T) {
	for name, owner := range map[string]any{
		"nil": nil, "typed-nil-postgres": (*PostgresStore)(nil), "typed-nil-sqlite": (*SQLiteRuntimeStore)(nil),
		"uninitialized-postgres": &PostgresStore{}, "uninitialized-sqlite": &SQLiteRuntimeStore{}, "raw-database": &sql.DB{},
	} {
		t.Run(name, func(t *testing.T) {
			if event, err := LoadCanonicalEventRecordForTest(context.Background(), owner, uuid.NewString()); err == nil || event.ID() != "" {
				t.Fatalf("invalid owner returned evidence: event=%#v err=%v", event, err)
			}
			if evidence, err := ReadSemanticEventFixtureEvidenceForTest(context.Background(), owner, uuid.NewString(), uuid.NewString()); err == nil || !reflect.DeepEqual(evidence, SemanticEventFixtureEvidence{}) {
				t.Fatalf("invalid owner returned storage evidence: evidence=%+v err=%v", evidence, err)
			}
		})
	}
}

func TestCanonicalEventFixtureReadbackRejectsClosedOwnersParity(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
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
			if event, err := LoadCanonicalEventRecordForTest(context.Background(), fixture.store, uuid.NewString()); err == nil || event.ID() != "" {
				t.Fatalf("closed owner returned evidence: event=%#v err=%v", event, err)
			}
			if evidence, err := ReadSemanticEventFixtureEvidenceForTest(context.Background(), fixture.store, uuid.NewString(), uuid.NewString()); err == nil || !reflect.DeepEqual(evidence, SemanticEventFixtureEvidence{}) {
				t.Fatalf("closed owner returned storage evidence: evidence=%+v err=%v", evidence, err)
			}
		})
	}
}

func TestSemanticEventFixtureEvidenceUsesOriginalReadOwnerBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, runID := decisionCardTestStore(t, backend)
			ctx := testAuthorActivityContext()
			eventID := uuid.NewString()
			before, err := ReadSemanticEventFixtureEvidenceForTest(ctx, selected, runID, eventID)
			if err != nil || before.RecordFound || len(before.DeliveryProjections) != 0 || len(before.DeliveryStatuses) != 0 || before.NonPlatformReceiptCount != 0 {
				t.Fatalf("empty fixture evidence: evidence=%+v err=%v", before, err)
			}
			admitted, settlement, routes := compoundFixtureEvent(t, runID, eventID, `{"value":1}`, time.Now().UTC().Truncate(time.Microsecond))
			if inserted, err := CommitSemanticEventFixtureForTest(ctx, selected, admitted, settlement, routes, runtimepipelineobligation.ScopeSubscribed, nil); err != nil || !inserted {
				t.Fatalf("commit semantic fixture: inserted=%v err=%v", inserted, err)
			}
			var probe *transactiontest.Collector
			var restore func()
			switch owner := selected.(type) {
			case *PostgresStore:
				probe, restore, err = owner.backend.InstallTransactionProbeForTest(transactiontest.Options{})
			case *SQLiteRuntimeStore:
				probe, restore, err = owner.backend.InstallTransactionProbeForTest(transactiontest.Options{})
			}
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			evidence, err := ReadSemanticEventFixtureEvidenceForTest(ctx, selected, runID, eventID)
			deliveryID, deliveryErr := runtimedelivery.DeliveryID(eventID, routes[0])
			projection, found := evidence.DeliveryProjections[deliveryID]
			if err != nil || deliveryErr != nil || !evidence.RecordFound || evidence.Record.EventID != eventID || evidence.RevisionCount != before.RevisionCount || len(evidence.DeliveryProjections) != 1 || !found || projection[1] != "agent" || projection[2] != "fixture-agent" || evidence.DeliveryStatuses[deliveryID] != "pending" || len(evidence.DeliveryStatuses) != 1 || evidence.NonPlatformReceiptCount != 0 {
				t.Fatalf("exact fixture evidence: evidence=%+v err=%v deliveryErr=%v", evidence, err, deliveryErr)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("evidence did not use one original-owner read snapshot: %+v", counts)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			for _, refusal := range []struct {
				name  string
				ctx   context.Context
				runID string
				err   error
			}{
				{"cancelled", cancelled, runID, context.Canceled},
				{"blank-run", ctx, "", nil},
				{"wrong-run", ctx, uuid.NewString(), nil},
			} {
				t.Run(refusal.name, func(t *testing.T) {
					got, err := ReadSemanticEventFixtureEvidenceForTest(refusal.ctx, selected, refusal.runID, eventID)
					if err == nil || (refusal.err != nil && !errors.Is(err, refusal.err)) || !reflect.DeepEqual(got, SemanticEventFixtureEvidence{}) {
						t.Fatalf("refused read returned partial evidence: got=%+v err=%v", got, err)
					}
				})
			}
			if got, err := ReadSemanticEventFixtureEvidenceForTest(ctx, selected, runID, eventID); err != nil || !reflect.DeepEqual(got, evidence) {
				t.Fatalf("refusal changed storage: got=%+v want=%+v err=%v", got, evidence, err)
			}
			if counts := probe.Snapshot(); counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("evidence reader wrote or retained a snapshot: %+v", counts)
			}
		})
	}
}

func TestCanonicalEventFixtureReadbackBypassesSQLiteWriterAdmission(t *testing.T) {
	fixture := openSQLiteAuthorActivityReceiptFixture(t)
	owner := fixture.store.(*SQLiteRuntimeStore)
	ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 10*time.Second)
	defer cancel()
	event := eventtest.RunCreatingRootIngress(uuid.NewString(), "fixture.readback", "gateway", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
	if err := commitSemanticEventFixture(ctx, owner, event); err != nil {
		t.Fatal(err)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- owner.backend.RunTransaction(ctx, "hold readback sibling writer", func(context.Context, *sql.Tx) error {
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	var once sync.Once
	defer func() {
		once.Do(func() { close(release) })
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	got, err := LoadCanonicalEventRecordForTest(ctx, owner, event.ID())
	if err != nil || got.ID() != event.ID() {
		t.Fatalf("pure read was blocked by selected writer admission: event=%#v err=%v", got, err)
	}
	evidence, err := ReadSemanticEventFixtureEvidenceForTest(ctx, owner, event.RunID(), event.ID())
	if err != nil || !evidence.RecordFound || evidence.Record.EventID != event.ID() {
		t.Fatalf("physical evidence read was blocked by selected writer admission: evidence=%+v err=%v", evidence, err)
	}
	select {
	case err := <-done:
		done <- err
		t.Fatalf("writer was no longer held during readback: %v", err)
	default:
	}
}

func TestSemanticEventPublicationEvidencePreservesExactCutsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, phase := range []string{"direct", "acknowledged", "dead_letter"} {
			t.Run(backend+"/"+phase, func(t *testing.T) {
				selected, runID := decisionCardTestStore(t, backend)
				ctx := testAuthorActivityContext()
				eventID := uuid.NewString()
				before, err := ReadSemanticEventFixtureEvidenceForTest(ctx, selected, runID, eventID)
				if err != nil || before.RecordFound || before.RunStatus != "running" || before.CommittedScopeFound || before.PipelineReceiptCount != 0 || before.SettledDeliveryAttemptCount != 0 {
					t.Fatalf("pre-publication evidence = %+v, %v", before, err)
				}
				admitted, settlement, routes := compoundFixtureEvent(t, runID, eventID, `{"proof":"exact-cut"}`, time.Now().UTC())
				scope := runtimepipelineobligation.ScopeDirect
				var disposition *runtimepipelineobligation.Disposition
				var failure *runtimefailures.Envelope
				if phase != "direct" {
					scope = runtimepipelineobligation.ScopeSubscribed
					value := runtimepipelineobligation.Acknowledged("evidence_ack")
					if phase == "dead_letter" {
						envelope := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassInternalFailure, "event_interceptor_failed", "evidence", "publish", nil), "evidence", "publish")
						failure = &envelope
						value = runtimepipelineobligation.DeadLetter("event_interceptor_failed", failure)
					}
					disposition = &value
				}
				if inserted, err := CommitSemanticEventFixtureForTest(ctx, selected, admitted, settlement, routes, scope, disposition); err != nil || !inserted {
					t.Fatalf("commit exact publication: inserted=%v err=%v", inserted, err)
				}
				probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
				if err != nil {
					t.Fatal(err)
				}
				defer restore()
				evidence, err := ReadSemanticEventFixtureEvidenceForTest(ctx, selected, runID, eventID)
				if err != nil || !evidence.RecordFound || !evidence.CommittedScopeFound || evidence.CommittedScope != string(scope) || evidence.RunStatus != "running" || evidence.SettledDeliveryAttemptCount != 0 || len(evidence.DeliveryProjections) != 1 {
					t.Fatalf("post-publication evidence = %+v, %v", evidence, err)
				}
				if phase == "direct" {
					if evidence.PipelineReceiptCount != 0 || evidence.PipelineReceiptOutcome != "" || evidence.PipelineReceiptReason != "" || evidence.PipelineReceiptFailure != nil {
						t.Fatalf("fabricated pipeline receipt: %+v", evidence)
					}
				} else {
					wantOutcome, wantReason := "success", "evidence_ack"
					if phase == "dead_letter" {
						wantOutcome, wantReason = "dead_letter", "event_interceptor_failed"
					}
					if evidence.PipelineReceiptCount != 1 || evidence.PipelineReceiptOutcome != wantOutcome || evidence.PipelineReceiptReason != wantReason || !reflect.DeepEqual(evidence.PipelineReceiptFailure, failure) {
						t.Fatalf("pipeline receipt differs from exact committed outcome: %+v", evidence)
					}
				}
				if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
					t.Fatalf("publication witness replaced or split its original read snapshot: %+v", counts)
				}
				deliveryID, err := runtimedelivery.DeliveryID(eventID, routes[0])
				if err != nil || evidence.DeliveryStatuses[deliveryID] != "pending" || evidence.NonPlatformReceiptCount != 0 {
					t.Fatalf("pending delivery fabricated status or agent receipt: %+v, %v", evidence, err)
				}
				deliveryStore := selected.(deliveryFixtureStore)
				claim, err := claimDeliveryFixture(ctx, deliveryStore, admitted.Event(), routes[0])
				if err != nil {
					t.Fatal(err)
				}
				if _, err := deliveryStore.SettleSuccess(ctx, claim.Claim, nil, time.Millisecond, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
					t.Fatal(err)
				}
				settled, err := ReadSemanticEventFixtureEvidenceForTest(ctx, selected, runID, eventID)
				if err != nil || settled.SettledDeliveryAttemptCount != 1 || settled.RunEventCount != evidence.RunEventCount || settled.CommittedScope != evidence.CommittedScope || settled.PipelineReceiptOutcome != evidence.PipelineReceiptOutcome || settled.PipelineReceiptReason != evidence.PipelineReceiptReason || settled.DeliveryStatuses[deliveryID] != "delivered" || settled.NonPlatformReceiptCount != 0 {
					t.Fatalf("delivery settlement evidence = %+v, %v", settled, err)
				}
				if phase == "dead_letter" {
					savedFailure, err := json.Marshal(failure)
					if err != nil {
						t.Fatal(err)
					}
					writeFailure := func(value string) error {
						write := func(ctx context.Context, tx *sql.Tx) error {
							_, err := tx.ExecContext(ctx, `UPDATE event_receipts SET failure = $1
								WHERE event_id = $2 AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`, value, eventID)
							return err
						}
						switch owner := selected.(type) {
						case *PostgresStore:
							return owner.backend.RunTransaction(ctx, write)
						case *SQLiteRuntimeStore:
							return owner.backend.RunTransaction(ctx, "corrupt exact receipt readback proof", write)
						default:
							return errors.New("receipt readback proof requires original selected owner")
						}
					}
					restored := false
					defer func() {
						if !restored {
							if err := writeFailure(string(savedFailure)); err != nil {
								t.Error(err)
							}
						}
					}()
					if err := writeFailure(`"not-a-failure-envelope"`); err != nil {
						t.Fatal(err)
					}
					if got, err := ReadSemanticEventFixtureEvidenceForTest(ctx, selected, runID, eventID); err == nil || !reflect.DeepEqual(got, SemanticEventFixtureEvidence{}) {
						t.Fatalf("corrupt receipt returned partial publication evidence: %+v, %v", got, err)
					}
					if err := writeFailure(string(savedFailure)); err != nil {
						t.Fatal(err)
					}
					restored = true
					if got, err := ReadSemanticEventFixtureEvidenceForTest(ctx, selected, runID, eventID); err != nil || !reflect.DeepEqual(got, settled) {
						t.Fatalf("receipt restoration changed exact publication evidence: %+v, %v", got, err)
					}
				}
				terminal := selected.(interface {
					MarkTerminalRun(context.Context, runtimerunlifecycle.TerminalRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error)
				})
				if _, _, err := terminal.MarkTerminalRun(ctx, runtimerunlifecycle.TerminalRequest{RunID: runID, State: runtimerunlifecycle.StateCancelled, EndedAt: time.Now().UTC()}); err != nil {
					t.Fatal(err)
				}
				terminalEvidence, err := ReadSemanticEventFixtureEvidenceForTest(ctx, selected, runID, eventID)
				// Terminalization synchronizes the stored counter with the one
				// durable event; it need not equal the pre-terminal fixture counter.
				if err != nil || terminalEvidence.RunStatus != "cancelled" || terminalEvidence.SettledDeliveryAttemptCount != 1 || terminalEvidence.RunEventCount != 1 {
					t.Fatalf("terminal publication evidence = %+v, %v", terminalEvidence, err)
				}
			})
		}
	}
}

func TestSemanticEventEvidenceRejectsNonPlatformReceiptsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, runID := decisionCardTestStore(t, backend)
			ctx := testAuthorActivityContext()
			eventID := uuid.NewString()
			admitted, settlement, routes := compoundFixtureEvent(t, runID, eventID, `{}`, time.Now().UTC())
			disposition := runtimepipelineobligation.Acknowledged("evidence_ack")
			if _, err := CommitSemanticEventFixtureForTest(ctx, selected, admitted, settlement, routes, runtimepipelineobligation.ScopeSubscribed, &disposition); err != nil {
				t.Fatal(err)
			}
			for _, kind := range []string{"agent", "node"} {
				err := runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, `INSERT INTO event_receipts
						(receipt_id,event_id,subscriber_type,subscriber_id,outcome,side_effects,processed_at)
						VALUES ($1,$2,$3,'fixture-agent','success','{}',$4)`, uuid.NewString(), eventID, kind, time.Now().UTC())
					return err
				})
				var postgresError interface{ SQLState() string }
				var sqliteError interface{ Code() int }
				if err == nil || !((errors.As(err, &postgresError) && postgresError.SQLState() == "23514") || (errors.As(err, &sqliteError) && sqliteError.Code() == 275)) {
					t.Fatalf("schema did not reject %s receipt with its CHECK constraint: %v", kind, err)
				}
			}
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			evidence, err := ReadSemanticEventFixtureEvidenceForTest(ctx, selected, runID, eventID)
			if err != nil || evidence.NonPlatformReceiptCount != 0 || evidence.PipelineReceiptCount != 1 || evidence.PipelineReceiptReason != "evidence_ack" {
				t.Fatalf("refused non-platform receipts changed the exact platform receipt: %+v, %v", evidence, err)
			}
			for _, status := range evidence.DeliveryStatuses {
				if status != "pending" {
					t.Fatalf("stored receipt rewrote observed delivery status: %+v", evidence)
				}
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("receipt evidence did not consume one original-owner read snapshot: %+v", counts)
			}
		})
	}
}
