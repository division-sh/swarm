package storetest

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestPostgresSystemNodeCompletionReadsPreserveNativeClockAndNullableTrigger(t *testing.T) {
	selected := StartPostgresRuntimeStore(t)
	ctx := semanticFixtureContext(context.Background(), sourceartifactfixture.Fact())
	runID, eventID, unrelated := uuid.NewString(), uuid.NewString(), uuid.NewString()
	at := time.Date(2026, 10, 12, 2, 0, 0, 123456000, time.UTC)
	RequireRun(t, ctx, selected, RunFixture{RunID: runID, Origin: EventOrigin(t, eventID, "fixture.started"), StartedAt: at})
	RequireRun(t, ctx, selected, RunFixture{RunID: unrelated, Origin: ScenarioSetupOrigin(), StartedAt: at})
	before, err := ReadPostgresCompletionCandidateClockStorage(ctx, selected, runID)
	if err != nil || before.Revision != 0 || before.DueAt.Valid {
		t.Fatalf("initial native candidate clock=%+v err=%v, want0/NULL", before, err)
	}
	due := time.Date(2027, 10, 12, 2, 0, 0, 654321000, time.UTC)
	if disposition, err := selected.RequestCompletionCandidate(ctx, runtimerunlifecycle.CandidateAtTime(runID, due)); err != nil || disposition != runtimerunlifecycle.CandidateRequested {
		t.Fatalf("request actual scheduled candidate=%s err=%v", disposition, err)
	}
	probe := CollectTransactions(t, selected, TransactionProbeOptions{})
	clock, err := ReadPostgresCompletionCandidateClockStorage(ctx, selected, runID)
	if err != nil || clock.Revision != before.Revision+1 || !clock.DueAt.Valid || !clock.DueAt.Time.Equal(due) {
		t.Fatalf("original native clock/NULL/revision changed: clock=%+v due=%v err=%v", clock, due, err)
	}
	trigger, err := ReadPostgresRunTriggerEventIdentityStorage(ctx, selected, runID)
	if err != nil || trigger != eventID {
		t.Fatalf("original run trigger changed: got=%q want=%q err=%v", trigger, eventID, err)
	}
	if receipt := probe.Snapshot(); receipt.Active != 0 || receipt.Total.ReadCommits != 2 || receipt.Total.WriteCommits != 0 {
		t.Fatalf("physical observations lost separate original read transactions: %+v", receipt)
	}
	if trigger, err := ReadPostgresRunTriggerEventIdentityStorage(ctx, selected, unrelated); trigger != "" || err == nil || errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("NULL trigger was coalesced, fabricated or confused with a missing row: trigger=%q err=%v", trigger, err)
	}
}

func TestPostgresSystemNodeCompletionPortsRefuseMissingForeignCancelledAndClosedOwners(t *testing.T) {
	selected := StartPostgresRuntimeStore(t)
	sqlite := StartSQLiteRuntimeStore(t)
	id := uuid.NewString()
	reads := postgresSystemNodeCompletionStorageRefusalReaders(id)
	var nilPostgres *store.PostgresStore
	var nilSQLite *store.SQLiteRuntimeStore
	for _, read := range reads {
		t.Run(read.name, func(t *testing.T) {
			if zero, err := read.read(context.Background(), selected); !zero || !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("absent physical row changed: zero=%t err=%v", zero, err)
			}
			for _, owner := range []any{nil, nilPostgres, nilSQLite, sqlite, &sql.DB{}, &sql.Tx{}, struct{}{}} {
				if zero, err := read.read(context.Background(), owner); !zero || err == nil {
					t.Fatalf("foreign %T supplied successful/provisional evidence: zero=%t err=%v", owner, zero, err)
				}
			}
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			if zero, err := read.read(cancelled, selected); !zero || !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled read supplied physical evidence: zero=%t err=%v", zero, err)
			}
		})
	}
	if err := selected.Close(); err != nil {
		t.Fatal(err)
	}
	for _, read := range reads {
		if zero, err := read.read(context.Background(), selected); !zero || err == nil {
			t.Fatalf("closed %s returned evidence: zero=%t err=%v", read.name, zero, err)
		}
	}
}

type postgresSystemNodeRefusalReader struct {
	name string
	read func(context.Context, any) (bool, error)
}

func postgresSystemNodeCompletionStorageRefusalReaders(id string) []postgresSystemNodeRefusalReader {
	return []postgresSystemNodeRefusalReader{
		{"candidate_clock", func(ctx context.Context, selected any) (bool, error) {
			value, err := ReadPostgresCompletionCandidateClockStorage(ctx, selected, id)
			return value == (PostgresCompletionCandidateClockStorage{}), err
		}},
		{"trigger", func(ctx context.Context, selected any) (bool, error) {
			value, err := ReadPostgresRunTriggerEventIdentityStorage(ctx, selected, id)
			return value == "", err
		}},
		{"node_delivery", func(ctx context.Context, selected any) (bool, error) {
			value, err := ReadPostgresSystemNodeDeliveryStorage(ctx, selected, id, "flow/discovery/node/pipeline")
			return value == (PostgresSystemNodeDeliveryStorage{}), err
		}},
		{"node_outcome", func(ctx context.Context, selected any) (bool, error) {
			value, err := ReadPostgresSystemNodeOutcomeStorage(ctx, selected, id, "flow/discovery/node/pipeline")
			return value == (PostgresSystemNodeOutcomeStorage{}), err
		}},
		{"exact_pipeline_receipt", func(ctx context.Context, selected any) (bool, error) {
			value, err := ReadPostgresExactPipelineReceiptOutcomeStorage(ctx, selected, id)
			return value == "", err
		}},
		{"flow_entity", func(ctx context.Context, selected any) (bool, error) {
			value, err := ReadPostgresFlowEntityTerminalStorage(ctx, selected, id, id, "discovery")
			return value == (PostgresFlowEntityTerminalStorage{}), err
		}},
	}
}
