package storetest

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestPostgresRunStatusStorageKeepsPhysicalCountersScopesAndOriginalReads(t *testing.T) {
	selected := StartPostgresRuntimeStore(t)
	ctx := semanticFixtureContext(context.Background(), sourceartifactfixture.Fact())
	run, sibling, paused := uuid.NewString(), uuid.NewString(), uuid.NewString()
	for _, id := range []string{run, sibling} {
		RequireRun(t, ctx, selected, RunFixture{RunID: id, Origin: ScenarioSetupOrigin()})
	}
	RequireRun(t, ctx, selected, RunFixture{RunID: paused, Origin: ScenarioSetupOrigin(), State: runtimerunlifecycle.StatePaused})
	at := time.Date(2026, 10, 12, 2, 0, 0, 123456000, time.UTC)
	eventID := uuid.NewString()
	for _, row := range []struct{ run, id string }{{run, eventID}, {run, uuid.NewString()}, {sibling, uuid.NewString()}} {
		CommitSemanticEvent(t, ctx, selected, eventtest.ExistingRunRootIngressWithRoutingSource(row.id, events.EventType("fixture.ready"), "fixture", "", []byte(`{}`), 0, row.run,
			events.EventEnvelope{}, eventtest.RootRoutingSource(row.run), at))
	}
	SeedRunDiagnosticDeadLetterHistory(t, ctx, selected, run, uuid.NewString(), []byte(`{"log_level":"error","message":"physical-counter control"}`), at.Add(time.Second))
	facts, err := ReadRunEventCounterStorageConsistency(ctx, selected, run)
	if err != nil || facts.Stored != 3 || facts.Actual != 3 {
		t.Fatalf("native event setup did not persist three actual counted records: %+v err=%v", facts, err)
	}
	probe := CollectTransactions(t, selected, TransactionProbeOptions{})
	assertPostgresRunStatusHeaderReads(t, ctx, selected, run, eventID, facts.Stored)
	assertPostgresRunStatusSettlementReads(t, ctx, selected, run)
	if receipt := probe.Snapshot(); receipt.Total.ReadCommits != 8 || receipt.Total.WriteCommits != 0 || receipt.Active != 0 {
		t.Fatalf("whole observation chain lost eight separate original reads: %+v", receipt)
	}
}

func assertPostgresRunStatusHeaderReads(t *testing.T, ctx context.Context, selected any, run, eventID string, stored int) {
	t.Helper()
	identity, err := ReadPostgresRunCompletionIdentityStorage(ctx, selected, eventID)
	if err != nil || identity.RunID != run || identity.BundleHash != sourceartifactfixture.Fact().BundleHash() {
		t.Fatalf("exact event/run completion identity changed: %+v err=%v", identity, err)
	}
	clock, err := ReadPostgresRunStatusClockStorage(ctx, selected, run)
	if err != nil || clock.Status != "running" || clock.EndedAt.Valid {
		t.Fatalf("native running clock changed: %+v err=%v", clock, err)
	}
	counts, err := ReadPostgresRunStatusCountsStorage(ctx, selected, run)
	if err != nil || counts.Status != "running" || counts.EventCount != stored || counts.EntityCount != 0 {
		t.Fatalf("physical run counters changed: %+v err=%v", counts, err)
	}
	other, err := CountPostgresExtraRunningStatusRows(ctx, selected, run)
	if err != nil || other != 1 {
		t.Fatalf("global exact-running complement counted target/paused or lost sibling: %d err=%v", other, err)
	}
}

func assertPostgresRunStatusSettlementReads(t *testing.T, ctx context.Context, selected any, run string) {
	t.Helper()
	semantic, err := CountPostgresRunStatusSemanticEvents(ctx, selected, run)
	if err != nil || semantic != 2 {
		t.Fatalf("exact-run non-log count included diagnostic/sibling: %d err=%v", semantic, err)
	}
	settlement, err := ReadPostgresRunStatusSettlementStorage(ctx, selected, run)
	if err != nil || settlement.EventCount != 2 || settlement.ActiveDeliveries != 0 {
		t.Fatalf("atomic event/delivery read changed scope: %+v err=%v", settlement, err)
	}
	active, err := CountPostgresRunStatusActiveDeliveries(ctx, selected, run)
	if err != nil || active != 0 {
		t.Fatalf("empty active-delivery read changed: %d err=%v", active, err)
	}
	diagnostics, err := ReadPostgresRunStatusFailureDiagnosticsStorage(ctx, selected, run)
	if err != nil || diagnostics != nil {
		t.Fatalf("empty physical delivery diagnostic changed: %+v err=%v", diagnostics, err)
	}
}

func TestPostgresRunStatusStorageRefusesUnavailableOwnersAndDiscardsEvidence(t *testing.T) {
	selected := StartPostgresRuntimeStore(t)
	sqlite := StartSQLiteRuntimeStore(t)
	reads := postgresRunStatusRefusalReaders(uuid.NewString())
	var nilPostgres *store.PostgresStore
	var nilSQLite *store.SQLiteRuntimeStore
	for _, read := range reads {
		t.Run(read.name, func(t *testing.T) {
			if zero, err := read.read(context.Background(), selected); !zero || (read.absent && !errors.Is(err, sql.ErrNoRows)) || (!read.absent && err != nil) {
				t.Fatalf("absent/empty physical evidence changed: zero=%t err=%v", zero, err)
			}
			for _, owner := range []any{nil, nilPostgres, nilSQLite, sqlite, &sql.DB{}, &sql.Tx{}, struct{}{}} {
				if zero, err := read.read(context.Background(), owner); !zero || err == nil {
					t.Fatalf("foreign %T supplied evidence: zero=%t err=%v", owner, zero, err)
				}
			}
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			if zero, err := read.read(cancelled, selected); !zero || !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled read supplied evidence: zero=%t err=%v", zero, err)
			}
		})
	}
	if err := selected.Close(); err != nil {
		t.Fatal(err)
	}
	for _, read := range reads {
		if zero, err := read.read(context.Background(), selected); !zero || err == nil {
			t.Fatalf("closed %s supplied evidence: zero=%t err=%v", read.name, zero, err)
		}
	}
}

type postgresRunStatusRefusalReader struct {
	name   string
	read   func(context.Context, any) (bool, error)
	absent bool
}

func postgresRunStatusRefusalReaders(id string) []postgresRunStatusRefusalReader {
	return []postgresRunStatusRefusalReader{
		{"completion_identity", func(ctx context.Context, selected any) (bool, error) {
			v, err := ReadPostgresRunCompletionIdentityStorage(ctx, selected, id)
			return v == (PostgresRunCompletionIdentityStorage{}), err
		}, true},
		{"status_clock", func(ctx context.Context, selected any) (bool, error) {
			v, err := ReadPostgresRunStatusClockStorage(ctx, selected, id)
			return v == (PostgresRunStatusClockStorage{}), err
		}, true},
		{"status_counts", func(ctx context.Context, selected any) (bool, error) {
			v, err := ReadPostgresRunStatusCountsStorage(ctx, selected, id)
			return v == (PostgresRunStatusCountsStorage{}), err
		}, true},
		{"other_running", func(ctx context.Context, selected any) (bool, error) {
			v, err := CountPostgresExtraRunningStatusRows(ctx, selected, id)
			return v == 0, err
		}, false},
		{"atomic_settlement", func(ctx context.Context, selected any) (bool, error) {
			v, err := ReadPostgresRunStatusSettlementStorage(ctx, selected, id)
			return v == (PostgresRunStatusSettlementStorage{}), err
		}, false},
		{"active_delivery", func(ctx context.Context, selected any) (bool, error) {
			v, err := CountPostgresRunStatusActiveDeliveries(ctx, selected, id)
			return v == 0, err
		}, false},
		{"failure_diagnostic", func(ctx context.Context, selected any) (bool, error) {
			v, err := ReadPostgresRunStatusFailureDiagnosticsStorage(ctx, selected, id)
			return v == nil, err
		}, false},
		{"semantic_event", func(ctx context.Context, selected any) (bool, error) {
			v, err := CountPostgresRunStatusSemanticEvents(ctx, selected, id)
			return v == 0, err
		}, false},
	}
}
