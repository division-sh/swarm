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

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

var mailboxCompletionOracleQueries = []string{
	`SELECT * FROM decision_cards WHERE run_id=$1 ORDER BY card_id`,
	`SELECT d.* FROM decision_card_input_drafts d JOIN decision_cards c ON c.card_id=d.card_id WHERE c.run_id=$1 ORDER BY d.input_draft_id`,
	`SELECT d.* FROM decision_card_changes d JOIN decision_cards c ON c.card_id=d.card_id WHERE c.run_id=$1 ORDER BY d.change_id`,
	`SELECT * FROM human_task_continuations WHERE run_id=$1 ORDER BY card_id`,
	`SELECT * FROM proposed_effect_continuations WHERE run_id=$1 ORDER BY card_id`,
	`SELECT * FROM events WHERE run_id=$1 AND event_class NOT IN ('runtime_diagnostic','diagnostic_direct') ORDER BY event_id`,
	`SELECT d.* FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE e.run_id=$1 ORDER BY d.delivery_id`,
	`SELECT * FROM entity_state WHERE run_id=$1 ORDER BY entity_id,flow_instance`,
	`SELECT * FROM flow_instances WHERE run_id=$1 ORDER BY instance_path`,
}

func TestMailboxCompletionEffectsKeepExactNineTableOracleBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			runID, _ := seedWorkflowProjectionFaultCut(t, fixture, ctx)
			seedMailboxCompletionCardRows(t, fixture, ctx, runID)
			claimed := seedDeliveryRecoveryClaim(t, fixture, ctx)
			seedMailboxCompletionCardRows(t, fixture, ctx, claimed.Claim.RunID())
			for _, id := range []string{runID, claimed.Claim.RunID(), uuid.NewString()} {
				before := readMailboxCompletionOracle(t, fixture.db, id)
				probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
				if err != nil {
					t.Fatal(err)
				}
				defer restore()
				got, err := ReadMailboxCompletionRunEffectsForTest(ctx, fixture.store, id)
				if err != nil || !reflect.DeepEqual(before, got) {
					t.Fatalf("nine-table exact row/bytes/NULL/time/order/run oracle: want=%v got=%v err=%v", before, got, err)
				}
				if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
					t.Fatalf("mailbox witness escaped the exact selected read transaction: %+v", counts)
				}
				restore()
				if after := readMailboxCompletionOracle(t, fixture.db, id); !reflect.DeepEqual(before, after) {
					t.Fatal("physical mailbox observation mutated its complete footprint")
				}
			}
		})
	}
}

func seedMailboxCompletionCardRows(t *testing.T, fixture authorActivityReceiptFixture, ctx context.Context, runID string) {
	t.Helper()
	store := fixture.store.(forkedDecisionConsumerSurface)
	now := time.Now().UTC().Truncate(time.Microsecond)
	card := newDecisionCardTestCard(t, runID, now)
	if err := store.CreateDecisionCard(ctx, card); err != nil {
		t.Fatal(err)
	}
	if _, err := DecisionCardDomainForTest(store).BeginInputForTest(ctx, decisioncard.BeginInputRequest{
		CardID: card.CardID, Verdict: "revise", PrincipalID: "operator", Now: now, TTL: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	human, continuation := newHumanTaskDecisionCardTestFixture(t, runID, "mailbox-effects", now, 1, now.Add(time.Hour))
	if err := store.CreateHumanTaskCard(ctx, human, continuation); err != nil {
		t.Fatal(err)
	}
	effect, proposed := newProposedEffectTestCard(t, runID, now, attemptgeneration.Generation{})
	if err := store.CreateProposedEffectCard(ctx, effect, proposed); err != nil {
		t.Fatal(err)
	}
}

func readMailboxCompletionOracle(t *testing.T, db *sql.DB, runID string) []string {
	t.Helper()
	var out []string
	for _, query := range mailboxCompletionOracleQueries {
		rows, err := db.Query(query, runID)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, readMailboxCompletionOracleRows(t, rows, query)...)
	}
	return out
}

func readMailboxCompletionOracleRows(t *testing.T, rows *sql.Rows, query string) []string {
	t.Helper()
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for rows.Next() {
		values, pointers := make([]any, len(columns)), make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(values)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, query+":"+string(raw))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestMailboxCompletionEffectsRefuseCancelledClosedAndLateMissingRowsBothStores(t *testing.T) {
	ctx := context.Background()
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadMailboxCompletionRunEffectsForTest(ctx, owner, uuid.NewString()); err == nil || got != nil {
			t.Fatalf("unowned mailbox snapshot returned evidence: %v %v", got, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			runID, _ := seedWorkflowProjectionFaultCut(t, fixture, testAuthorActivityContext())
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := ReadMailboxCompletionRunEffectsForTest(cancelled, fixture.store, runID); !errors.Is(err, context.Canceled) || got != nil {
				t.Fatalf("cancelled mailbox snapshot returned partial evidence: %v %v", got, err)
			}
			if got, err := ReadMailboxCompletionRunEffectsForTest(ctx, fixture.store, "not-a-run"); err == nil || got != nil {
				t.Fatalf("invalid mailbox run returned evidence: %v %v", got, err)
			}
			requireMailboxLateTableFailure(t, fixture, runID)
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadMailboxCompletionRunEffectsForTest(ctx, fixture.store, runID); err == nil || got != nil {
				t.Fatalf("closed mailbox snapshot returned evidence: %v %v", got, err)
			}
		})
	}
}

func requireMailboxLateTableFailure(t *testing.T, fixture authorActivityReceiptFixture, runID string) {
	t.Helper()
	ctx := testAuthorActivityContext()
	var installed bool
	restore := func() {
		if !installed {
			return
		}
		if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `ALTER TABLE mailbox_effect_unavailable_flows RENAME TO flow_instances`)
			return err
		}); err != nil {
			t.Error(err)
		} else {
			installed = false
		}
	}
	t.Cleanup(restore)
	if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `ALTER TABLE flow_instances RENAME TO mailbox_effect_unavailable_flows`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	installed = true
	if got, err := ReadMailboxCompletionRunEffectsForTest(ctx, fixture.store, runID); err == nil || got != nil {
		t.Fatalf("late missing relation published partial first-eight-table evidence: %v %v", got, err)
	}
	restore()
	if got, err := ReadMailboxCompletionRunEffectsForTest(ctx, fixture.store, runID); err != nil || !reflect.DeepEqual(got, readMailboxCompletionOracle(t, fixture.db, runID)) {
		t.Fatalf("restored exact selected-owner mailbox snapshot: %v %v", got, err)
	}
}

func TestServedRunDiagnosticsRetainAllElevenSectionsAndContinueAfterQueryErrorBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			runID, _ := seedWorkflowProjectionFaultCut(t, fixture, ctx)
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			summary, err := ReadServedRunDebugSummaryForTest(ctx, fixture.store, runID)
			if err != nil || !strings.Contains(summary, "flow_instances: [storage-ref projection-flow active]") {
				t.Fatalf("diagnostic dropped a physical flow header: %s %v", summary, err)
			}
			requireServedDebugSectionOrder(t, summary)
			if counts := probe.Snapshot(); counts.Total.Begun != 11 || counts.Total.ReadCommits != 11 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("diagnostics did not independently join eleven exact selected read transactions: %+v", counts)
			}
			restore()
			remove := installMailboxDiagnosticFlowAbsence(t, fixture, ctx)
			probe, restore, err = InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			summary, err = ReadServedRunDebugSummaryForTest(ctx, fixture.store, runID)
			if err != nil || !strings.Contains(summary, "runtime_logs: []") || (!strings.Contains(summary, "does not exist") && !strings.Contains(summary, "no such table")) {
				t.Fatalf("query error lost later independent diagnostic sections: %s %v", summary, err)
			}
			requireServedDebugSectionOrder(t, summary)
			if counts := probe.Snapshot(); counts.Total.Begun != 11 || counts.Total.ReadCommits != 10 || counts.Total.RollbackAttempts != 1 || counts.Total.CommitFailures != 0 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("failed section lost rollback/cleanup provenance or poisoned later transactions: %+v", counts)
			}
			restore()
			remove()
		})
	}
}

func installMailboxDiagnosticFlowAbsence(t *testing.T, fixture authorActivityReceiptFixture, ctx context.Context) func() {
	t.Helper()
	installed := false
	remove := func() {
		if !installed {
			return
		}
		if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `ALTER TABLE debug_unavailable_flows RENAME TO flow_instances`)
			return err
		}); err != nil {
			t.Error(err)
		} else {
			installed = false
		}
	}
	t.Cleanup(remove)
	if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `ALTER TABLE flow_instances RENAME TO debug_unavailable_flows`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	installed = true
	return remove
}

func requireServedDebugSectionOrder(t *testing.T, summary string) {
	t.Helper()
	sections := strings.Split(summary, "\n")
	want := []string{"runs", "entity_state", "flow_instances", "events", "event_deliveries", "settled_delivery_attempts", "event_receipts", "pipeline_obligations", "dead_letters", "delivery_agents", "runtime_logs"}
	if len(sections) != len(want) {
		t.Fatalf("diagnostic inventory=%d/%d: %s", len(sections), len(want), summary)
	}
	for i, name := range want {
		if !strings.HasPrefix(sections[i], name+": ") {
			t.Fatalf("diagnostic section %d: %q != %s", i, sections[i], name)
		}
	}
}

func TestRunDeadLetterDeliveryObservationRetainsExactRunAndFailureStatusBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			claim := seedDeliveryRecoveryClaim(t, fixture, ctx)
			sibling := seedDeliveryRecoveryClaim(t, fixture, ctx)
			if _, err := fixture.store.SettleFailure(ctx, claim.Claim, runtimedelivery.Settlement{
				Disposition: runtimedelivery.FailureDeadLetter, ReasonCode: "mailbox-observation",
				Failure: testRetryableFailure(), RuleSelection: runtimedelivery.NotApplicableHandlerRuleObservation(),
			}); err != nil {
				t.Fatal(err)
			}
			for id, want := range map[string]int{claim.Claim.RunID(): 1, sibling.Claim.RunID(): 0, uuid.NewString(): 0} {
				if got, err := CountRunDeadLetterDeliveriesForTest(ctx, fixture.store, id); err != nil || got != want {
					t.Fatalf("dead-letter exact run/status count=%d/%d err=%v", got, want, err)
				}
				if accounting, err := ObserveH2DeliveryAccountingForTest(ctx, fixture.store, id); err != nil || accounting.DeadLetters != want {
					t.Fatalf("existing H2 physical accounting diverged from the same closed count owner: %+v/%d %v", accounting, want, err)
				}
			}
		})
	}
}
