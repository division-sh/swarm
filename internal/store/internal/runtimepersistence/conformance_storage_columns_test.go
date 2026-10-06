package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

func conformanceColumnChecks() []struct {
	name  string
	check func(context.Context, any) error
} {
	return []struct {
		name  string
		check func(context.Context, any) error
	}{
		{"conversation", CheckConversationStorageColumnsForTest},
		{"runtime-log", CheckRuntimeLogStorageColumnsForTest},
		{"mutation", CheckMutationStorageColumnsForTest},
		{"delivery", CheckDeliveryLifecycleStorageColumnsForTest},
	}
}

func TestConformanceStorageColumnsUseOriginalReadOwnerBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), context.Background()
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for _, check := range conformanceColumnChecks() {
				if err := check.check(ctx, fixture.store); err != nil {
					t.Fatalf("%s: %v", check.name, err)
				}
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 4 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("column witnesses escaped native read ownership: %+v", counts)
			}
		})
	}
}

func TestConformanceStorageColumnsDetectMissingCanonicalColumnsBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, item := range []struct {
			table, column string
			check         func(context.Context, any) error
		}{
			{"agent_turns", "turn_blocks", CheckConversationStorageColumnsForTest},
			{"agent_conversation_audits", "session_id", CheckConversationStorageColumnsForTest},
			{"events", "payload", CheckRuntimeLogStorageColumnsForTest},
			{"entity_state", "bookkeeping", CheckMutationStorageColumnsForTest},
			{"entity_mutations", "handler_step", CheckMutationStorageColumnsForTest},
			{"event_deliveries", "settled_at", CheckDeliveryLifecycleStorageColumnsForTest},
			{"event_delivery_attempts", "current_delivery_id", CheckDeliveryLifecycleStorageColumnsForTest},
		} {
			t.Run(backend.name+"/"+item.table, func(t *testing.T) {
				fixture, ctx := backend.open(t), context.Background()
				if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, `ALTER TABLE `+item.table+` RENAME COLUMN `+item.column+` TO unavailable_conformance_column`)
					return err
				}); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
						_, err := tx.ExecContext(ctx, `ALTER TABLE `+item.table+` RENAME COLUMN unavailable_conformance_column TO `+item.column)
						return err
					}); err != nil {
						t.Error(err)
					}
				}()
				if err := item.check(ctx, fixture.store); err == nil || !strings.Contains(err.Error(), "missing required canonical column "+item.table+"."+item.column) {
					t.Fatalf("missing canonical column accepted: %v", err)
				}
			})
		}
	}
}

func TestConformanceStorageColumnsIgnoreSiblingSchemaBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), context.Background()
			if backend.name == "sqlite" {
				// Keep the connection-local hostile TEMP shadow on the same
				// original pool connection used by the native read owner.
				fixture.db.SetMaxOpenConns(1)
			}
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				if _, err := tx.ExecContext(ctx, `ALTER TABLE agent_turns RENAME COLUMN turn_blocks TO unavailable_conformance_blocks`); err != nil {
					return err
				}
				if backend.name == "sqlite" {
					_, err := tx.ExecContext(ctx, `CREATE TEMP TABLE agent_turns(turn_id TEXT, turn_blocks TEXT)`)
					return err
				}
				if _, err := tx.ExecContext(ctx, `CREATE SCHEMA sibling_conformance`); err != nil {
					return err
				}
				_, err := tx.ExecContext(ctx, `CREATE TABLE sibling_conformance.agent_turns(turn_id TEXT, turn_blocks TEXT)`)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
					if backend.name == "sqlite" {
						if _, err := tx.ExecContext(ctx, `DROP TABLE temp.agent_turns`); err != nil {
							return err
						}
					} else {
						if _, err := tx.ExecContext(ctx, `DROP TABLE sibling_conformance.agent_turns`); err != nil {
							return err
						}
						if _, err := tx.ExecContext(ctx, `DROP SCHEMA sibling_conformance`); err != nil {
							return err
						}
					}
					_, err := tx.ExecContext(ctx, `ALTER TABLE agent_turns RENAME COLUMN unavailable_conformance_blocks TO turn_blocks`)
					return err
				}); err != nil {
					t.Error(err)
				}
			}()
			if err := CheckConversationStorageColumnsForTest(ctx, fixture.store); err == nil || !strings.Contains(err.Error(), "missing required canonical column agent_turns.turn_blocks") {
				t.Fatalf("sibling schema promoted into canonical storage presence: %v", err)
			}
		})
	}
}

func TestConformanceStorageColumnsRefuseRawCancelledAndClosedBothStores(t *testing.T) {
	for _, check := range conformanceColumnChecks() {
		for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
			if err := check.check(context.Background(), owner); err == nil {
				t.Fatalf("%s accepted %T", check.name, owner)
			}
		}
	}
	for _, backend := range eventRecordContractBackends() {
		for _, cut := range []string{"cancelled", "closed"} {
			t.Run(backend.name+"/"+cut, func(t *testing.T) {
				fixture, ctx := backend.open(t), context.Background()
				if cut == "cancelled" {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				} else if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
					t.Fatal(err)
				}
				for _, check := range conformanceColumnChecks() {
					if err := check.check(ctx, fixture.store); err == nil || cut == "cancelled" && !errors.Is(err, context.Canceled) {
						t.Fatalf("%s %s accepted: %v", check.name, cut, err)
					}
				}
			})
		}
	}
}
