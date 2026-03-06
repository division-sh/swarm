package sessions

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresSessionRegistry_AcquireNewAndExistingAndRelease(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	sr := NewPostgresRegistry(db, 30*time.Second)
	fixedNow := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	sr.SetNowFnForTest(func() time.Time { return fixedNow })

	// Acquire new session (no rows).
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id::text, session_id, scope_key, lock_owner, lock_expires_at\\s+FROM agent_sessions").
		WithArgs("a1", "api", "").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("INSERT INTO agent_sessions").
		WithArgs("a1", "api", "", "anthropic", sqlmock.AnyArg(), "owner-1", 30).
		WillReturnRows(sqlmock.NewRows([]string{"id", "session_id", "scope_key"}).AddRow("row-1", "sess-1", nil))
	mock.ExpectCommit()

	lease, err := sr.Acquire("a1", "api", "owner-1", "")
	if err != nil {
		t.Fatalf("Acquire new: %v", err)
	}
	if lease.SessionID != "sess-1" || lease.AgentID != "a1" || lease.LockOwner != "owner-1" {
		t.Fatalf("unexpected lease: %+v", lease)
	}

	// Acquire existing session, same owner allowed.
	existingExpiry := fixedNow.Add(30 * time.Second)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id::text, session_id, scope_key, lock_owner, lock_expires_at\\s+FROM agent_sessions").
		WithArgs("a1", "api", "").
		WillReturnRows(sqlmock.NewRows([]string{"id", "session_id", "scope_key", "lock_owner", "lock_expires_at"}).
			AddRow("row-1", "sess-1", nil, "owner-1", existingExpiry))
	mock.ExpectQuery("UPDATE agent_sessions\\s+SET lock_owner = \\$1,").
		WithArgs("owner-1", 30, "row-1").
		WillReturnRows(sqlmock.NewRows([]string{"session_id", "lock_expires_at"}).AddRow("sess-1", existingExpiry))
	mock.ExpectCommit()

	lease2, err := sr.Acquire("a1", "api", "owner-1", "")
	if err != nil {
		t.Fatalf("Acquire existing: %v", err)
	}
	if lease2.SessionID != "sess-1" {
		t.Fatalf("unexpected session id: %+v", lease2)
	}

	// Release lease.
	mock.ExpectExec("UPDATE agent_sessions\\s+SET lock_owner = NULL").
		WithArgs("a1", "api", "sess-1", "", "owner-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := sr.Release(&Lease{AgentID: "a1", RuntimeMode: "api", SessionID: "sess-1", LockOwner: "owner-1"}); err != nil {
		t.Fatalf("Release: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestPostgresSessionRegistry_AcquireLeasedByOtherReturnsErrLeased(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	sr := NewPostgresRegistry(db, 30*time.Second)
	fixedNow := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	sr.SetNowFnForTest(func() time.Time { return fixedNow })

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id::text, session_id, scope_key, lock_owner, lock_expires_at\\s+FROM agent_sessions").
		WithArgs("a1", "api", "").
		WillReturnRows(sqlmock.NewRows([]string{"id", "session_id", "scope_key", "lock_owner", "lock_expires_at"}).
			AddRow("row-1", "sess-1", nil, "someone-else", fixedNow.Add(10*time.Second)))
	// Transaction should be rolled back by defer.

	_, err = sr.Acquire("a1", "api", "owner-1", "")
	if err != ErrSessionLeased {
		t.Fatalf("expected ErrSessionLeased, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestPostgresSessionRegistry_Rotate_And_IncrementTurn(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	sr := NewPostgresRegistry(db, 30*time.Second)
	fixedNow := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	sr.SetNowFnForTest(func() time.Time { return fixedNow })

	// Rotate happy path.
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id::text, session_id, lock_owner, lock_expires_at\\s+FROM agent_sessions").
		WithArgs("a1", "api", "").
		WillReturnRows(sqlmock.NewRows([]string{"id", "session_id", "lock_owner", "lock_expires_at"}).
			AddRow("row-1", "sess-1", "owner-1", fixedNow.Add(10*time.Second)))
	mock.ExpectExec("UPDATE agent_sessions\\s+SET status = 'rotated'").
		WithArgs("sum", "row-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("INSERT INTO agent_sessions").
		WithArgs("a1", "api", "", "anthropic", sqlmock.AnyArg(), "owner-1", 30).
		WillReturnRows(sqlmock.NewRows([]string{"lock_expires_at"}).AddRow(fixedNow.Add(30 * time.Second)))
	mock.ExpectCommit()

	lease, err := sr.Rotate("a1", "api", "owner-1", "sum", "")
	if err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if lease.SessionID == "" || lease.AgentID != "a1" {
		t.Fatalf("unexpected lease: %+v", lease)
	}

	// IncrementTurn success.
	mock.ExpectExec("UPDATE agent_sessions\\s+SET turn_count = turn_count \\+ 1").
		WithArgs("a1", "api", lease.SessionID, "").
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := sr.IncrementTurn("a1", "api", lease.SessionID, ""); err != nil {
		t.Fatalf("IncrementTurn: %v", err)
	}

	// IncrementTurn not found -> error.
	mock.ExpectExec("UPDATE agent_sessions\\s+SET turn_count = turn_count \\+ 1").
		WithArgs("a1", "api", "missing", "").
		WillReturnResult(sqlmock.NewResult(0, 0))
	if err := sr.IncrementTurn("a1", "api", "missing", ""); err == nil {
		t.Fatal("expected IncrementTurn to error on missing session")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestPostgresSessionRegistry_AdoptSessionID(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	sr := NewPostgresRegistry(db, 30*time.Second)
	fixedNow := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	sr.SetNowFnForTest(func() time.Time { return fixedNow })

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id::text, lock_owner, lock_expires_at\\s+FROM agent_sessions").
		WithArgs("a1", "cli_test", "").
		WillReturnRows(sqlmock.NewRows([]string{"id", "lock_owner", "lock_expires_at"}).
			AddRow("row-1", "owner-1", fixedNow.Add(10*time.Second)))
	mock.ExpectExec("UPDATE agent_sessions\\s+SET session_id = \\$1,").
		WithArgs("claude-session-1", "owner-1", 30, "row-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := sr.AdoptSessionID("a1", "cli_test", "owner-1", "claude-session-1", ""); err != nil {
		t.Fatalf("AdoptSessionID: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestPostgresSessionRegistry_Rotate_FallbackWithoutScopeKeyColumn(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	sr := NewPostgresRegistry(db, 30*time.Second)
	fixedNow := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	sr.SetNowFnForTest(func() time.Time { return fixedNow })

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id::text, session_id, lock_owner, lock_expires_at\\s+FROM agent_sessions").
		WithArgs("a1", "api", "scope-a").
		WillReturnError(errors.New(`pq: column "scope_key" does not exist`))
	mock.ExpectQuery("SELECT id::text, session_id, lock_owner, lock_expires_at\\s+FROM agent_sessions").
		WithArgs("a1", "api").
		WillReturnRows(sqlmock.NewRows([]string{"id", "session_id", "lock_owner", "lock_expires_at"}).
			AddRow("row-1", "sess-1", "owner-1", fixedNow.Add(10*time.Second)))
	mock.ExpectExec("UPDATE agent_sessions\\s+SET status = 'rotated'").
		WithArgs("sum", "row-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("INSERT INTO agent_sessions").
		WithArgs("a1", "api", "anthropic", sqlmock.AnyArg(), "owner-1", 30).
		WillReturnRows(sqlmock.NewRows([]string{"lock_expires_at"}).AddRow(fixedNow.Add(30 * time.Second)))
	mock.ExpectCommit()

	lease, err := sr.Rotate("a1", "api", "owner-1", "sum", "scope-a")
	if err != nil {
		t.Fatalf("Rotate fallback: %v", err)
	}
	if lease.ScopeKey != "" {
		t.Fatalf("expected empty scope key on fallback, got %q", lease.ScopeKey)
	}
	if sr.ScopeKeyEnabledForTest() {
		t.Fatal("expected scope-key mode disabled after fallback")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestPostgresSessionRegistry_ResetAll_AllRuntimesUsesRotatedStatus(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	sr := NewPostgresRegistry(db, 30*time.Second)
	mock.ExpectExec("UPDATE agent_sessions\\s+SET status = 'rotated'").
		WillReturnResult(sqlmock.NewResult(0, 2))

	if err := sr.ResetAll(""); err != nil {
		t.Fatalf("ResetAll(all runtimes): %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestPostgresSessionRegistry_ResetAll_RuntimeScopedUsesRotatedStatus(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	sr := NewPostgresRegistry(db, 30*time.Second)
	mock.ExpectExec("UPDATE agent_sessions\\s+SET status = 'rotated'").
		WithArgs("cli_test").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := sr.ResetAll("cli_test"); err != nil {
		t.Fatalf("ResetAll(cli_test): %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}
