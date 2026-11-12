package channelonboarding

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/runtime/effects"
	nativeinput "github.com/division-sh/swarm/internal/sessionprovider/input"
)

// Other owners retain these existing checks, never the provider-facts lookup.
func (s *PostgresOwner) RegisteredConfirmationCurrent(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, authority effects.Authority, postgres, lock bool) (bool, error) {
	return registeredConfirmationCurrent(ctx, postgresRunner{s}, q, authority, postgres, lock)
}

func (s *SQLiteOwner) RegisteredConfirmationCurrent(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, authority effects.Authority, postgres, lock bool) (bool, error) {
	return registeredConfirmationCurrent(ctx, sqliteRunner{s}, q, authority, postgres, lock)
}

func (s *PostgresOwner) LogoutEffectAuthorityCurrent(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, authority effects.Authority, postgres, lock bool) (bool, error) {
	return logoutEffectAuthorityCurrent(ctx, postgresRunner{s}, q, authority, postgres, lock)
}

func (s *SQLiteOwner) LogoutEffectAuthorityCurrent(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, authority effects.Authority, postgres, lock bool) (bool, error) {
	return logoutEffectAuthorityCurrent(ctx, sqliteRunner{s}, q, authority, postgres, lock)
}

func (s *PostgresOwner) SettleLogoutEffectTx(ctx context.Context, tx *sql.Tx, settlement effects.Settlement, postgres bool) (bool, error) {
	return settleLogoutEffectTx(ctx, postgresRunner{s}, tx, settlement, postgres)
}

func (s *SQLiteOwner) SettleLogoutEffectTx(ctx context.Context, tx *sql.Tx, settlement effects.Settlement, postgres bool) (bool, error) {
	return settleLogoutEffectTx(ctx, sqliteRunner{s}, tx, settlement, postgres)
}

func (s *PostgresOwner) RequireSessionBusinessInputTx(ctx context.Context, tx *sql.Tx, postgres bool, input nativeinput.Admission) error {
	return requireSessionBusinessInputTx(ctx, postgresRunner{s}, tx, postgres, input)
}

func (s *SQLiteOwner) RequireSessionBusinessInputTx(ctx context.Context, tx *sql.Tx, postgres bool, input nativeinput.Admission) error {
	return requireSessionBusinessInputTx(ctx, sqliteRunner{s}, tx, postgres, input)
}

func (s *PostgresOwner) ReadSessionConnectionForTest(ctx context.Context, tx *sql.Tx, postgres bool, operationID string) (string, error) {
	return readSessionConnectionForTest(ctx, postgresRunner{s}, tx, postgres, operationID)
}

func (s *SQLiteOwner) ReadSessionConnectionForTest(ctx context.Context, tx *sql.Tx, postgres bool, operationID string) (string, error) {
	return readSessionConnectionForTest(ctx, sqliteRunner{s}, tx, postgres, operationID)
}
