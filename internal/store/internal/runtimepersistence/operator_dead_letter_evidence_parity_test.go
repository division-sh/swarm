package runtimepersistence

import (
	"context"
	"encoding/json"
	agentfixture "github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

type operatorDeadLetterEvidenceStore interface {
	authorActivityReceiptStore
	agentfixture.Store
	LoadOperatorAgentDeliveryDiagnostics(context.Context, agentidentity.Identity, operatorread.OperatorAgentDeliveryDiagnosticsOptions) (operatorread.OperatorAgentDeliveryDiagnostics, error)
	LoadOperatorEvent(context.Context, string) (operatorread.OperatorEventFull, error)
	LoadRunDebugReport(context.Context, string, operatorread.RunDebugQueryOptions) (operatorread.RunDebugReport, error)
	runtimerunlifecycle.OperationOwner
}

func TestOperatorDeadLetterEvidenceIsScopedToExactDeliveryParity(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(operatorDeadLetterEvidenceStore)
			ctx := testAuthorActivityContext()
			now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
			runID := uuid.NewString()
			eventID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			identity := testAgentIdentity(t, "agent-a", "global")
			if err := agentfixture.Upsert(t, ctx, selected, runtimemanager.PersistedAgent{
				Config: withRuntimePersistenceTestIntent(t, runtimeactors.AgentConfig{
					Identity: identity, ID: "agent-a", Role: "worker", Type: "managed", Model: "regular", ExecutionMode: "live",
					Memory: agentmemory.PlatformDefault(), FlowPath: "global", Config: json.RawMessage(`{}`),
				}),
				Status: "active", StartedAt: now,
			}); err != nil {
				t.Fatalf("upsert agent-a: %v", err)
			}
			event := eventtest.PersistedProjection(
				eventID, events.EventType("delivery.evidence"), "gateway", "", json.RawMessage(`{"message":"proof"}`), 0,
				runID, "", events.EventEnvelope{}, now,
			)
			routes := []events.DeliveryRoute{
				{Recipient: events.MustAgentDeliveryRecipient("agent-a"), AgentIdentity: identity,
					Context: events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "dead-letter-route-a"}},
					Target:  events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "flow-a", FlowInstance: "flow-a/one", EntityID: uuid.NewString()}),
				},
				{Recipient: events.MustAgentDeliveryRecipient("agent-a"), AgentIdentity: identity,
					Context: events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "dead-letter-route-b"}},
					Target:  events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "flow-a", FlowInstance: "flow-a/two", EntityID: uuid.NewString()}),
				},
			}
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, routes); err != nil {
				t.Fatalf("commit event with sibling deliveries: %v", err)
			}

			snapshots := make(map[string]runtimedelivery.Snapshot, len(routes))
			for index, route := range routes {
				claimed, err := claimDeliveryFixture(ctx, selected, event, route)
				if err != nil {
					t.Fatalf("claim %s: %v", route.Recipient.ID(), err)
				}
				failure := testFailureEnvelope(runtimefailures.ClassRetryExhausted, "route_"+string(rune('a'+index))+"_failed", nil)
				snapshot, err := selected.SettleFailure(ctx, claimed.Claim, runtimedelivery.Settlement{
					Disposition: runtimedelivery.FailureDeadLetter,
					ReasonCode:  failure.Detail.Code,
					Failure:     &failure,
				})
				if err != nil {
					t.Fatalf("dead-letter %s: %v", route.Recipient.ID(), err)
				}
				snapshots[snapshot.DeliveryID] = snapshot
			}

			diagnostics, err := selected.LoadOperatorAgentDeliveryDiagnostics(ctx, identity, operatorread.OperatorAgentDeliveryDiagnosticsOptions{})
			if err != nil {
				t.Fatalf("load agent diagnostics: %v", err)
			}
			if len(diagnostics.DeadLetters) != 2 {
				t.Fatalf("agent dead letters = %#v, want both route-level deliveries", diagnostics.DeadLetters)
			}
			for _, delivery := range diagnostics.DeadLetters {
				assertExactOperatorDeadLetterEvidence(t, delivery.DeadLetterRecords, snapshots[delivery.DeliveryID])
			}

			full, err := selected.LoadOperatorEvent(ctx, eventID)
			if err != nil {
				t.Fatalf("load operator event: %v", err)
			}
			if len(full.DeadLetters) != 2 || len(full.Deliveries) != 2 {
				t.Fatalf("operator event evidence = dead_letters:%#v deliveries:%#v", full.DeadLetters, full.Deliveries)
			}
			for _, delivery := range full.Deliveries {
				assertExactOperatorDeadLetterEvidence(t, delivery.DeadLetters, snapshots[delivery.DeliveryID])
			}

			report, err := selected.LoadRunDebugReport(ctx, runID, operatorread.RunDebugQueryOptions{})
			if err != nil {
				t.Fatalf("load run debug report: %v", err)
			}
			if len(report.FailedDeliveries) != 2 {
				t.Fatalf("run failed deliveries = %#v, want two", report.FailedDeliveries)
			}
			for _, delivery := range report.FailedDeliveries {
				assertExactOperatorDeadLetterEvidence(t, delivery.DeadLetters, snapshots[delivery.DeliveryID])
			}
		})
	}
}

func TestOperatorRunTerminalizationPreservesExactDeadLetterEvidenceParity(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(operatorDeadLetterEvidenceStore)
			ctx := testAuthorActivityContext()
			now := time.Date(2026, 7, 22, 13, 0, 0, 0, time.UTC)
			runID := uuid.NewString()
			eventID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			identity := testAgentIdentity(t, "terminal-agent", "global")
			if err := agentfixture.Upsert(t, ctx, selected, runtimemanager.PersistedAgent{
				Config: withRuntimePersistenceTestIntent(t, runtimeactors.AgentConfig{
					Identity: identity, ID: "terminal-agent", Role: "worker", Type: "managed", Model: "regular", ExecutionMode: "live",
					Memory: agentmemory.PlatformDefault(), FlowPath: "global", Config: json.RawMessage(`{}`),
				}),
				Status: "active", StartedAt: now,
			}); err != nil {
				t.Fatalf("upsert terminal-agent: %v", err)
			}
			event := eventtest.PersistedProjection(
				eventID, events.EventType("delivery.terminalized"), "gateway", "", json.RawMessage(`{"message":"stop"}`), 0,
				runID, "", events.EventEnvelope{}, now,
			)
			route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("terminal-agent"), AgentIdentity: identity}
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
				t.Fatalf("commit terminalized event: %v", err)
			}
			claimed, err := claimDeliveryFixture(ctx, selected, event, route)
			if err != nil {
				t.Fatalf("claim terminalized delivery: %v", err)
			}
			removeFault := installRunTerminalizationDeadLetterFault(t, ctx, fixture, backend.name == "postgres")
			if _, err := markRunTerminalStatusForTest(ctx, selected, runID, "cancelled", nil, now.Add(time.Second)); err == nil {
				t.Fatal("run cancellation succeeded while required terminalization evidence writer was faulted")
			}
			rolledBack, err := selected.Snapshot(ctx, claimed.Snapshot.DeliveryID)
			if err != nil {
				t.Fatalf("load delivery after faulted run cancellation: %v", err)
			}
			if rolledBack.Status != runtimedelivery.StatusInProgress || rolledBack.ClaimVersion != claimed.Claim.Version() {
				t.Fatalf("delivery after faulted run cancellation = %#v, want original claim", rolledBack)
			}
			query := `SELECT status FROM runs WHERE run_id = ?`
			if backend.name == "postgres" {
				query = `SELECT status FROM runs WHERE run_id = $1::uuid`
			}
			var runStatus string
			if err := fixture.db.QueryRowContext(ctx, query, runID).Scan(&runStatus); err != nil {
				t.Fatalf("load run after faulted cancellation: %v", err)
			}
			if runStatus != "running" {
				t.Fatalf("run after faulted cancellation = %q, want running", runStatus)
			}
			removeFault()
			terminal, err := markRunTerminalStatusForTest(ctx, selected, runID, "cancelled", nil, now.Add(time.Second))
			if err != nil {
				t.Fatalf("cancel run: %v", err)
			}
			if terminal.Status != "cancelled" {
				t.Fatalf("terminal run status = %q", terminal.Status)
			}
			snapshot, err := selected.Snapshot(ctx, claimed.Snapshot.DeliveryID)
			if err != nil {
				t.Fatalf("load terminalized delivery: %v", err)
			}
			if snapshot.Status != runtimedelivery.StatusDeadLetter || snapshot.Failure == nil || snapshot.ReasonCode != "run_cancelled" {
				t.Fatalf("terminalized delivery = %#v", snapshot)
			}

			diagnostics, err := selected.LoadOperatorAgentDeliveryDiagnostics(ctx, identity, operatorread.OperatorAgentDeliveryDiagnosticsOptions{})
			if err != nil {
				t.Fatalf("load terminalized agent diagnostics: %v", err)
			}
			if len(diagnostics.DeadLetters) != 1 || diagnostics.DeadLetters[0].DeliveryID != snapshot.DeliveryID {
				t.Fatalf("terminalized agent dead letters = %#v", diagnostics.DeadLetters)
			}
			assertExactOperatorDeadLetterEvidence(t, diagnostics.DeadLetters[0].DeadLetterRecords, snapshot)

			full, err := selected.LoadOperatorEvent(ctx, eventID)
			if err != nil {
				t.Fatalf("load terminalized operator event: %v", err)
			}
			if len(full.Deliveries) != 1 {
				t.Fatalf("terminalized operator event deliveries = %#v", full.Deliveries)
			}
			assertExactOperatorDeadLetterEvidence(t, full.Deliveries[0].DeadLetters, snapshot)

			report, err := selected.LoadRunDebugReport(ctx, runID, operatorread.RunDebugQueryOptions{})
			if err != nil {
				t.Fatalf("load terminalized run debug report: %v", err)
			}
			if len(report.FailedDeliveries) != 1 {
				t.Fatalf("terminalized run failed deliveries = %#v", report.FailedDeliveries)
			}
			assertExactOperatorDeadLetterEvidence(t, report.FailedDeliveries[0].DeadLetters, snapshot)
		})
	}
}

func installRunTerminalizationDeadLetterFault(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, postgres bool) func() {
	t.Helper()
	if postgres {
		if _, err := fixture.db.ExecContext(ctx, `CREATE OR REPLACE FUNCTION fail_run_terminalization_dead_letter_insert() RETURNS trigger AS $$ BEGIN IF NEW.delivery_id IS NOT NULL THEN RAISE EXCEPTION 'forced run terminalization diagnostic failure'; END IF; RETURN NEW; END; $$ LANGUAGE plpgsql`); err != nil {
			t.Fatalf("create run terminalization diagnostic fault function: %v", err)
		}
		if _, err := fixture.db.ExecContext(ctx, `CREATE TRIGGER fail_run_terminalization_dead_letter_insert BEFORE INSERT ON dead_letters FOR EACH ROW EXECUTE FUNCTION fail_run_terminalization_dead_letter_insert()`); err != nil {
			t.Fatalf("create run terminalization diagnostic fault trigger: %v", err)
		}
		cleanup := func() {
			if _, err := fixture.db.ExecContext(ctx, `DROP TRIGGER IF EXISTS fail_run_terminalization_dead_letter_insert ON dead_letters`); err != nil {
				t.Fatalf("drop run terminalization diagnostic fault trigger: %v", err)
			}
			if _, err := fixture.db.ExecContext(ctx, `DROP FUNCTION IF EXISTS fail_run_terminalization_dead_letter_insert()`); err != nil {
				t.Fatalf("drop run terminalization diagnostic fault function: %v", err)
			}
		}
		t.Cleanup(cleanup)
		return cleanup
	}
	if _, err := fixture.db.ExecContext(ctx, `CREATE TRIGGER fail_run_terminalization_dead_letter_insert BEFORE INSERT ON dead_letters WHEN NEW.delivery_id IS NOT NULL BEGIN SELECT RAISE(ABORT, 'forced run terminalization diagnostic failure'); END`); err != nil {
		t.Fatalf("create sqlite run terminalization diagnostic fault trigger: %v", err)
	}
	cleanup := func() {
		if _, err := fixture.db.ExecContext(ctx, `DROP TRIGGER IF EXISTS fail_run_terminalization_dead_letter_insert`); err != nil {
			t.Fatalf("drop sqlite run terminalization diagnostic fault trigger: %v", err)
		}
	}
	t.Cleanup(cleanup)
	return cleanup
}

func assertExactOperatorDeadLetterEvidence(t *testing.T, records []operatorread.OperatorDeadLetterRecord, snapshot runtimedelivery.Snapshot) {
	t.Helper()
	if len(records) != 1 {
		t.Fatalf("dead-letter records for %s = %#v, want one", snapshot.DeliveryID, records)
	}
	if snapshot.Failure == nil {
		t.Fatalf("dead-letter snapshot %s has no failure", snapshot.DeliveryID)
	}
	if got := records[0]; got.DeliveryID != snapshot.DeliveryID || got.ClaimVersion != snapshot.ClaimVersion || got.Failure.Detail.Code != snapshot.Failure.Detail.Code {
		t.Fatalf("dead-letter record = %#v, want delivery=%s claim=%d failure=%s", got, snapshot.DeliveryID, snapshot.ClaimVersion, snapshot.Failure.Detail.Code)
	}
}
