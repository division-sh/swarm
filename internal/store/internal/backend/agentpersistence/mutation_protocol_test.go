package agentpersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

func TestDirectiveMutationUsesProtocolStoryBoundary(t *testing.T) {
	for _, dialect := range []string{"postgres", "sqlite"} {
		for _, fail := range []bool{false, true} {
			name := dialect + "/commit"
			if fail {
				name = dialect + "/rollback"
			}
			t.Run(name, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				mock.ExpectBegin()
				if dialect == "postgres" {
					mock.ExpectQuery("SELECT last_sequence FROM author_activity_order").WillReturnRows(sqlmock.NewRows([]string{"last_sequence"}).AddRow(0))
				} else {
					mock.ExpectExec("INSERT OR IGNORE INTO author_activity_order").WillReturnResult(sqlmock.NewResult(0, 0))
					mock.ExpectExec("UPDATE author_activity_order SET last_sequence").WillReturnResult(sqlmock.NewResult(0, 1))
					mock.ExpectQuery("SELECT last_sequence FROM author_activity_order").WillReturnRows(sqlmock.NewRows([]string{"last_sequence"}).AddRow(0))
				}
				writeErr := errors.New("directive write failed")
				if fail {
					mock.ExpectRollback()
				} else {
					mock.ExpectCommit()
				}
				var calls int
				var acknowledged bool
				write := func(_ context.Context, _ *sql.Tx, attempt *mutationprotocol.Attempt) error {
					calls++
					if attempt == nil {
						t.Fatal("missing protocol attempt")
					}
					if fail {
						return writeErr
					}
					return nil
				}
				if dialect == "postgres" {
					backend, backendErr := postgresbackend.New(db)
					if backendErr != nil {
						t.Fatal(backendErr)
					}
					owner := &AgentPostgresOwner{backend: backend, schemaGuard: func() error { return nil }}
					acknowledged, err = owner.runDirectiveMutation(context.Background(), mutationprotocol.Story, write)
				} else {
					backend, backendErr := sqlitebackend.New(db)
					if backendErr != nil {
						t.Fatal(backendErr)
					}
					owner := &AgentSQLiteOwner{backend: backend, schemaGuard: func() error { return nil }}
					acknowledged, err = owner.runDirectiveMutation(context.Background(), "test directive", mutationprotocol.Story, write)
				}
				if acknowledged == fail {
					t.Fatalf("mutation acknowledged = %t, want %t", acknowledged, !fail)
				}
				if fail && !errors.Is(err, writeErr) || !fail && err != nil {
					t.Fatalf("mutation error = %v", err)
				}
				if calls != 1 {
					t.Fatalf("writer calls = %d, want 1", calls)
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestComposedDirectiveOriginRefusesInactiveAttempt(t *testing.T) {
	ctx := context.Background()
	attempt := &mutationprotocol.Attempt{}
	origin := runtimeagentcontrol.DirectiveExecutionOrigin{}
	for _, test := range []struct {
		name string
		call func() error
	}{
		{"postgres/renew", func() error {
			return (&AgentPostgresOwner{}).RenewProviderDirectiveOriginTx(ctx, attempt, origin, time.Now(), time.Minute)
		}},
		{"sqlite/renew", func() error {
			return (&AgentSQLiteOwner{}).RenewProviderDirectiveOriginTx(ctx, attempt, origin, time.Now(), time.Minute)
		}},
		{"postgres/settle", func() error {
			return (&AgentPostgresOwner{}).SettleProviderDirectiveOriginTx(ctx, attempt, origin, runtimeagentcontrol.DirectiveOperationFailed, runtimefailures.Envelope{}, time.Now())
		}},
		{"sqlite/settle", func() error {
			return (&AgentSQLiteOwner{}).SettleProviderDirectiveOriginTx(ctx, attempt, origin, runtimeagentcontrol.DirectiveOperationFailed, runtimefailures.Envelope{}, time.Now())
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); err == nil {
				t.Fatal("inactive mutation attempt was accepted")
			}
		})
	}
}
