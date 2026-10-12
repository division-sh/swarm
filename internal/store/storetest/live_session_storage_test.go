package storetest

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/store"
	"github.com/google/uuid"
)

func TestPostgresLiveSessionStorageRefusesForeignCancelledAndClosedOwners(t *testing.T) {
	selected := StartPostgresRuntimeStore(t)
	sqlite := StartSQLiteRuntimeStore(t)
	id := uuid.NewString()
	reads := postgresLiveSessionStorageReaders(id)
	var nilPostgres *store.PostgresStore
	var nilSQLite *store.SQLiteRuntimeStore
	probe := CollectTransactions(t, selected, TransactionProbeOptions{})
	for _, read := range reads {
		t.Run(read.name, func(t *testing.T) {
			zero, err := read.read(context.Background(), selected)
			if !zero || (read.absent && !errors.Is(err, sql.ErrNoRows)) || (!read.absent && err != nil) {
				t.Fatalf("missing/empty original session evidence changed: zero=%t err=%v", zero, err)
			}
			for _, owner := range []any{nil, nilPostgres, nilSQLite, sqlite, &sql.DB{}, &sql.Tx{}, struct{}{}} {
				if zero, err := read.read(context.Background(), owner); !zero || err == nil {
					t.Fatalf("foreign %T supplied session evidence: zero=%t err=%v", owner, zero, err)
				}
			}
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			if zero, err := read.read(cancelled, selected); !zero || !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled read supplied session evidence: zero=%t err=%v", zero, err)
			}
		})
	}
	if receipt := probe.Snapshot(); receipt.Active != 0 || receipt.Total.ReadCommits != 2 || receipt.Total.WriteCommits != 0 {
		t.Fatalf("two independent empty-count reads lost the exact native coordinator: %+v", receipt)
	}
	assertPostgresLiveSessionInvalidIdentities(t, selected)
	if err := selected.Close(); err != nil {
		t.Fatal(err)
	}
	for _, read := range reads {
		if zero, err := read.read(context.Background(), selected); !zero || err == nil {
			t.Fatalf("closed %s supplied session evidence: zero=%t err=%v", read.name, zero, err)
		}
	}
}

func assertPostgresLiveSessionInvalidIdentities(t *testing.T, selected any) {
	t.Helper()
	if row, err := ReadPostgresAgentOneDeliverySessionStorage(context.Background(), selected, "invalid-event"); row != (PostgresAgentOneDeliverySessionStorage{}) || err == nil {
		t.Fatalf("invalid event supplied frontier evidence: %+v err=%v", row, err)
	}
	if row, err := ReadPostgresSessionFlowStorage(context.Background(), selected, "invalid-session"); row != (PostgresSessionFlowStorage{}) || err == nil {
		t.Fatalf("invalid session supplied flow evidence: %+v err=%v", row, err)
	}
	if count, err := CountPostgresAgentOneSupportLiveSessions(context.Background(), selected, "invalid-run"); count != 0 || err == nil {
		t.Fatalf("invalid run supplied lineage evidence: %d err=%v", count, err)
	}
}

type postgresLiveSessionStorageReader struct {
	name   string
	read   func(context.Context, any) (bool, error)
	absent bool
}

func postgresLiveSessionStorageReaders(id string) []postgresLiveSessionStorageReader {
	return []postgresLiveSessionStorageReader{
		{"delivery_session", func(ctx context.Context, selected any) (bool, error) {
			v, err := ReadPostgresAgentOneDeliverySessionStorage(ctx, selected, id)
			return v == (PostgresAgentOneDeliverySessionStorage{}), err
		}, true},
		{"session_flow", func(ctx context.Context, selected any) (bool, error) {
			v, err := ReadPostgresSessionFlowStorage(ctx, selected, id)
			return v == (PostgresSessionFlowStorage{}), err
		}, true},
		{"support_live_lineage", func(ctx context.Context, selected any) (bool, error) {
			v, err := CountPostgresAgentOneSupportLiveSessions(ctx, selected, id)
			return v == 0, err
		}, false},
		{"global_agent_sessions", func(ctx context.Context, selected any) (bool, error) {
			v, err := CountPostgresAgentOneSessions(ctx, selected)
			return v == 0, err
		}, false},
	}
}
