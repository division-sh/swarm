package delivery

import (
	"context"
	"database/sql"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	_ "modernc.org/sqlite"
)

func TestDatabaseNowPreservesSQLiteSubsecondPrecision(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	want := time.Date(2026, 8, 9, 12, 34, 56, int(789*time.Millisecond), time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT ` + sqliteDatabaseNowExpression)).
		WillReturnRows(sqlmock.NewRows([]string{"now"}).AddRow(want.Format(time.RFC3339Nano)))

	adapter, err := NewAdapter(DialectSQLite)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	got, err := adapter.databaseNow(context.Background(), db)
	if err != nil {
		t.Fatalf("databaseNow: %v", err)
	}
	if !got.Equal(want) {
		t.Fatalf("databaseNow = %s, want %s", got, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestAgentPendingRetryEligibilityUsesCanonicalDatabaseClock(t *testing.T) {
	for _, tc := range []struct {
		name       string
		predicate  string
		expression string
	}{
		{name: "sqlite", predicate: sqliteAgentPendingEligibility, expression: sqliteDatabaseNowExpression},
		{name: "postgres", predicate: postgresAgentPendingEligibility, expression: postgresDatabaseNowExpression},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.Contains(tc.predicate, tc.expression) {
				t.Fatalf("pending retry predicate does not use canonical database clock %q", tc.expression)
			}
		})
	}
}

func TestSQLiteCaptureSnapshotTimeEstablishesSnapshotBeforeReturningTime(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "snapshot.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(4)

	var journalMode string
	if err := db.QueryRowContext(ctx, `PRAGMA journal_mode=WAL`).Scan(&journalMode); err != nil {
		t.Fatalf("enable WAL: %v", err)
	}
	if !strings.EqualFold(journalMode, "wal") {
		t.Fatalf("journal mode = %q, want wal", journalMode)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE runtime_store_metadata (
		id INTEGER PRIMARY KEY CHECK (id = 1),
		swarm_version TEXT NOT NULL,
		platform_version TEXT NOT NULL,
		created_at TEXT NOT NULL
	)`); err != nil {
		t.Fatalf("create runtime store metadata: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO runtime_store_metadata (id, swarm_version, platform_version, created_at) VALUES (1, 'test', 'test', '2026-08-25T00:00:00Z')`); err != nil {
		t.Fatalf("insert runtime store metadata: %v", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE snapshot_fact (id INTEGER PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		t.Fatalf("create snapshot fact: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO snapshot_fact (id, value) VALUES (1, 'before')`); err != nil {
		t.Fatalf("insert snapshot fact: %v", err)
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("begin read transaction: %v", err)
	}
	defer tx.Rollback()
	adapter, err := NewAdapter(DialectSQLite)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	if _, err := adapter.CaptureSnapshotTime(ctx, tx); err != nil {
		t.Fatalf("CaptureSnapshotTime: %v", err)
	}

	if _, err := db.ExecContext(ctx, `UPDATE snapshot_fact SET value = 'after' WHERE id = 1`); err != nil {
		t.Fatalf("commit concurrent fact mutation: %v", err)
	}
	var got string
	if err := tx.QueryRowContext(ctx, `SELECT value FROM snapshot_fact WHERE id = 1`).Scan(&got); err != nil {
		t.Fatalf("read fact through captured snapshot: %v", err)
	}
	if got != "before" {
		t.Fatalf("captured snapshot observed fact %q, want before", got)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit read transaction: %v", err)
	}
}
