package storetest

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/store"
	"github.com/google/uuid"
)

func TestPostgresResetRecoveryAftermathStorageRefusesForeignCancelledAndClosedOwners(t *testing.T) {
	selected := StartPostgresRuntimeStore(t)
	sqlite := StartSQLiteRuntimeStore(t)
	var nilPostgres *store.PostgresStore
	var nilSQLite *store.SQLiteRuntimeStore
	reads := postgresResetRecoveryAftermathReaders(uuid.NewString())
	for _, read := range reads {
		t.Run(read.name, func(t *testing.T) {
			if zero, err := read.read(context.Background(), selected); !zero || !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("missing physical aftermath did not preserve exact absence: zero=%t err=%v", zero, err)
			}
			for _, owner := range []any{nil, nilPostgres, nilSQLite, sqlite, &sql.DB{}, &sql.Tx{}, struct{}{}} {
				if zero, err := read.read(context.Background(), owner); !zero || err == nil {
					t.Fatalf("foreign %T supplied aftermath evidence: zero=%t err=%v", owner, zero, err)
				}
			}
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			if zero, err := read.read(cancelled, selected); !zero || !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled read supplied aftermath evidence: zero=%t err=%v", zero, err)
			}
		})
	}
	if row, err := ReadPostgresSessionTerminationStorage(context.Background(), selected, "not-a-session-id"); row != (PostgresSessionTerminationStorage{}) || err == nil {
		t.Fatalf("invalid session identity supplied evidence: %+v err=%v", row, err)
	}
	if err := selected.Close(); err != nil {
		t.Fatal(err)
	}
	for _, read := range reads {
		if zero, err := read.read(context.Background(), selected); !zero || err == nil {
			t.Fatalf("closed %s supplied aftermath evidence: zero=%t err=%v", read.name, zero, err)
		}
	}
}

type postgresResetRecoveryAftermathReader struct {
	name string
	read func(context.Context, any) (bool, error)
}

func postgresResetRecoveryAftermathReaders(sessionID string) []postgresResetRecoveryAftermathReader {
	return []postgresResetRecoveryAftermathReader{
		{"recovery_failed_payload", func(ctx context.Context, selected any) (bool, error) {
			v, err := ReadPostgresRecoveryFailedPayloadStorage(ctx, selected)
			return v == nil, err
		}},
		{"reset_payload", func(ctx context.Context, selected any) (bool, error) {
			v, err := ReadPostgresResetPayloadStorage(ctx, selected)
			return v == nil, err
		}},
		{"session_termination", func(ctx context.Context, selected any) (bool, error) {
			v, err := ReadPostgresSessionTerminationStorage(ctx, selected, sessionID)
			return v == (PostgresSessionTerminationStorage{}), err
		}},
	}
}
