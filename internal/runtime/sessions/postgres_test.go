package sessions

import (
	"context"
	"database/sql"
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

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT\\s+session_id::text,\\s+scope_key,\\s+status,\\s+NULLIF\\(runtime_state->>'provider_session_id'").
		WithArgs("a1", "global", RuntimeModeSession).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("INSERT INTO agent_sessions").
		WithArgs(sqlmock.AnyArg(), "a1", "", "", "global", "global", "session", "owner-1", fixedNow.Add(30*time.Second)).
		WillReturnRows(sqlmock.NewRows([]string{"session_id", "scope_key", "lease_expires_at"}).AddRow("sess-1", "global", fixedNow.Add(30*time.Second)))
	mock.ExpectCommit()

	lease, err := sr.Acquire(context.Background(), "a1", RuntimeModeSession, "owner-1", "global")
	if err != nil {
		t.Fatalf("Acquire new: %v", err)
	}
	if lease.SessionID != "sess-1" || lease.AgentID != "a1" || lease.ScopeKey != "global" {
		t.Fatalf("unexpected lease: %+v", lease)
	}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT\\s+session_id::text,\\s+scope_key,\\s+status,\\s+NULLIF\\(runtime_state->>'provider_session_id'").
		WithArgs("a1", "global", RuntimeModeSession).
		WillReturnRows(sqlmock.NewRows([]string{"session_id", "scope_key", "status", "provider_session_id", "lease_holder", "lease_expires_at"}).
			AddRow("sess-1", "global", "active", nil, "owner-1", fixedNow.Add(30*time.Second)))
	mock.ExpectQuery("UPDATE agent_sessions\\s+SET lease_holder = \\$1,").
		WithArgs("owner-1", fixedNow.Add(30*time.Second), "sess-1").
		WillReturnRows(sqlmock.NewRows([]string{"lease_expires_at"}).AddRow(fixedNow.Add(30 * time.Second)))
	mock.ExpectCommit()

	lease2, err := sr.Acquire(context.Background(), "a1", RuntimeModeSession, "owner-1", "global")
	if err != nil {
		t.Fatalf("Acquire existing: %v", err)
	}
	if lease2.SessionID != "sess-1" || lease2.ScopeKey != "global" {
		t.Fatalf("unexpected session lease: %+v", lease2)
	}

	mock.ExpectExec("UPDATE agent_sessions\\s+SET lease_holder = NULL").
		WithArgs("a1", RuntimeModeSession, "sess-1", "global", "owner-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := sr.Release(context.Background(), &Lease{
		AgentID:     "a1",
		RuntimeMode: RuntimeModeSession,
		SessionID:   "sess-1",
		ScopeKey:    "global",
		LockOwner:   "owner-1",
	}); err != nil {
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
	mock.ExpectQuery("SELECT\\s+session_id::text,\\s+scope_key,\\s+status,\\s+NULLIF\\(runtime_state->>'provider_session_id'").
		WithArgs("a1", "global", RuntimeModeSession).
		WillReturnRows(sqlmock.NewRows([]string{"session_id", "scope_key", "status", "provider_session_id", "lease_holder", "lease_expires_at"}).
			AddRow("sess-1", "global", "active", nil, "someone-else", fixedNow.Add(10*time.Second)))

	_, err = sr.Acquire(context.Background(), "a1", RuntimeModeSession, "owner-1", "global")
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

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT session_id::text, lease_holder, lease_expires_at\\s+FROM agent_sessions").
		WithArgs("a1", "global", RuntimeModeSession).
		WillReturnRows(sqlmock.NewRows([]string{"session_id", "lease_holder", "lease_expires_at"}).
			AddRow("sess-1", "owner-1", fixedNow.Add(10*time.Second)))
	mock.ExpectQuery("UPDATE agent_sessions\\s+SET session_id = \\$1::uuid,").
		WithArgs(sqlmock.AnyArg(), "", "", "global", RuntimeModeSession, "sum", "owner-1", fixedNow.Add(30*time.Second), "sess-1").
		WillReturnRows(sqlmock.NewRows([]string{"lease_expires_at"}).AddRow(fixedNow.Add(30 * time.Second)))
	mock.ExpectCommit()

	lease, err := sr.Rotate(context.Background(), "a1", RuntimeModeSession, "owner-1", "sum", "global")
	if err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if lease.SessionID == "" || lease.AgentID != "a1" || lease.ScopeKey != "global" {
		t.Fatalf("unexpected lease: %+v", lease)
	}

	mock.ExpectExec("UPDATE agent_sessions\\s+SET turn_count = turn_count \\+ 1").
		WithArgs("a1", RuntimeModeSession, lease.SessionID, "global").
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := sr.IncrementTurn(context.Background(), "a1", RuntimeModeSession, lease.SessionID, "global"); err != nil {
		t.Fatalf("IncrementTurn: %v", err)
	}

	mock.ExpectExec("UPDATE agent_sessions\\s+SET turn_count = turn_count \\+ 1").
		WithArgs("a1", RuntimeModeSession, "missing", "global").
		WillReturnResult(sqlmock.NewResult(0, 0))
	if err := sr.IncrementTurn(context.Background(), "a1", RuntimeModeSession, "missing", "global"); err == nil {
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
	mock.ExpectQuery("SELECT session_id::text, lease_holder, lease_expires_at\\s+FROM agent_sessions").
		WithArgs("a1", "global", RuntimeModeSession).
		WillReturnRows(sqlmock.NewRows([]string{"session_id", "lease_holder", "lease_expires_at"}).
			AddRow("sess-1", "owner-1", fixedNow.Add(10*time.Second)))
	mock.ExpectExec("UPDATE agent_sessions\\s+SET runtime_state = COALESCE").
		WithArgs("claude-session-1", "owner-1", fixedNow.Add(30*time.Second), "sess-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := sr.AdoptSessionID(context.Background(), "a1", RuntimeModeSession, "owner-1", "claude-session-1", "global"); err != nil {
		t.Fatalf("AdoptSessionID: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestPostgresSessionRegistry_TaskModeIsStateless(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	sr := NewPostgresRegistry(db, 30*time.Second)
	if _, err := sr.Acquire(context.Background(), "a1", RuntimeModeTask, "owner-1", "task-1"); err == nil {
		t.Fatal("expected task mode acquire to reject stateless sessions")
	}
}

func TestPostgresSessionRegistry_SessionScopeRequiresExplicitDeclaration(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	sr := NewPostgresRegistry(db, 30*time.Second)
	if _, err := sr.Acquire(context.Background(), "a1", RuntimeModeSession, "owner-1", ""); err == nil {
		t.Fatal("expected session acquire without explicit scope to fail closed")
	}
}

func TestPostgresSessionRegistry_ResetAll_TerminatesActiveSessions(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	sr := NewPostgresRegistry(db, 30*time.Second)
	mock.ExpectExec("UPDATE agent_sessions\\s+SET status = 'terminated'.*runtime_mode IN \\('session', 'session_per_entity'\\)").
		WillReturnResult(sqlmock.NewResult(0, 2))
	if err := sr.ResetAll(""); err != nil {
		t.Fatalf("ResetAll(all runtimes): %v", err)
	}

	mock.ExpectExec("UPDATE agent_sessions\\s+SET status = 'terminated'.*runtime_mode = \\$1").
		WithArgs(RuntimeModeSession).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := sr.ResetAll(RuntimeModeSession); err != nil {
		t.Fatalf("ResetAll(session): %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}
