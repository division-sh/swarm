package testsql

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
)

type EventCorruptionClaim struct {
	Invariant string
	Reason    string
}

// CorruptEventStore is the sole test-only escape hatch for proving behavior
// against durable states that semantic constructors and named operations forbid.
func CorruptEventStore(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	dialect authoractivityfixture.Dialect,
	claim EventCorruptionClaim,
	sqliteStatement string,
	postgresStatement string,
	args ...any,
) {
	t.Helper()
	statement := eventCorruptionStatement(t, dialect, claim, sqliteStatement, postgresStatement)
	if _, err := db.ExecContext(ctx, statement, args...); err != nil {
		t.Fatalf("corrupt event store for %s (%s): %v", claim.Invariant, claim.Reason, err)
	}
}

func RejectEventStoreCorruption(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	dialect authoractivityfixture.Dialect,
	claim EventCorruptionClaim,
	sqliteStatement string,
	postgresStatement string,
	args ...any,
) {
	t.Helper()
	statement := eventCorruptionStatement(t, dialect, claim, sqliteStatement, postgresStatement)
	if _, err := db.ExecContext(ctx, statement, args...); err == nil {
		t.Fatalf("event store accepted corruption for %s (%s)", claim.Invariant, claim.Reason)
	}
}

func RejectEventStoreCorruptionCategory(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	dialect authoractivityfixture.Dialect,
	claim EventCorruptionClaim,
	category string,
	sqliteStatement string,
	postgresStatement string,
	args ...any,
) {
	t.Helper()
	statement := eventCorruptionStatement(t, dialect, claim, sqliteStatement, postgresStatement)
	_, err := db.ExecContext(ctx, statement, args...)
	if err == nil {
		t.Fatalf("event store accepted corruption for %s (%s)", claim.Invariant, claim.Reason)
	}
	message := strings.ToLower(err.Error())
	switch category {
	case "not_null":
		if !strings.Contains(message, "not null") && !strings.Contains(message, "not-null") {
			t.Fatalf("event store rejected %s for unrelated category: %v", claim.Reason, err)
		}
	case "check":
		if !strings.Contains(message, "check constraint") && !strings.Contains(message, "constraint failed") {
			t.Fatalf("event store rejected %s for unrelated category: %v", claim.Reason, err)
		}
	default:
		t.Fatalf("unknown event corruption error category %q", category)
	}
}

func RequireEventRowCount(t testing.TB, ctx context.Context, db *sql.DB, dialect authoractivityfixture.Dialect, eventID string, want int) {
	t.Helper()
	query := `SELECT COUNT(*) FROM events WHERE event_id = ?`
	if dialect == authoractivityfixture.DialectPostgres {
		query = `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`
	}
	var got int
	if err := db.QueryRowContext(ctx, query, eventID).Scan(&got); err != nil {
		t.Fatalf("count event fixture %s: %v", eventID, err)
	}
	if got != want {
		t.Fatalf("event fixture rows for %s = %d, want %d", eventID, got, want)
	}
}

func eventCorruptionStatement(t testing.TB, dialect authoractivityfixture.Dialect, claim EventCorruptionClaim, sqliteStatement, postgresStatement string) string {
	t.Helper()
	if strings.TrimSpace(claim.Invariant) == "" || strings.TrimSpace(claim.Reason) == "" {
		t.Fatal("unsafe event corruption requires an invariant and reason")
	}
	statement := sqliteStatement
	if dialect == authoractivityfixture.DialectPostgres {
		statement = postgresStatement
	}
	if strings.TrimSpace(statement) == "" {
		t.Fatal("unsafe event corruption statement is required")
	}
	return statement
}
