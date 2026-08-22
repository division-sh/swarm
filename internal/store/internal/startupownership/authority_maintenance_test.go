//go:build darwin || linux

package startupownership

import (
	"context"
	"database/sql"
	"runtime"
	"strings"
	"testing"
	"time"

	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

func TestSQLiteUnsupportedFilesystemInspectionOnly(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/inspection.db")
	if err != nil {
		t.Fatalf("open SQLite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	backend, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatalf("sqlite backend: %v", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE runtime_startup_authority_facts (
			fact_id TEXT PRIMARY KEY,
			authority_id TEXT NOT NULL,
			authority_generation INTEGER NOT NULL,
			transition_ordinal INTEGER NOT NULL,
			state_version INTEGER NOT NULL,
			state TEXT NOT NULL,
			owner_id TEXT NOT NULL,
			boot_id TEXT NOT NULL,
			runtime_instance_id TEXT NOT NULL,
			backend TEXT NOT NULL,
			acquisition_id TEXT NOT NULL,
			acquisition_request_hash TEXT NOT NULL,
			acquisition_kind TEXT NOT NULL,
			predecessor_authority_id TEXT,
			successor_authority_id TEXT,
			snapshot TEXT NOT NULL,
			created_at TEXT NOT NULL
		);
		CREATE TABLE runtime_startup_authority_repairs (operation_id TEXT PRIMARY KEY);
	`); err != nil {
		t.Fatalf("create authority proof tables: %v", err)
	}
	authorityID := uuid.NewString()
	if _, err := db.Exec(`
		INSERT INTO runtime_startup_authority_facts (
			fact_id, authority_id, authority_generation, transition_ordinal, state_version,
			state, owner_id, boot_id, runtime_instance_id, backend, acquisition_id,
			acquisition_request_hash, acquisition_kind, snapshot, created_at
		) VALUES (?, ?, 1, 1, 1, 'active', 'corrupt-owner', ?, ?,
			'sqlite_retained_owner', ?, ?, 'cold', '{}', ?)
	`, uuid.NewString(), authorityID, uuid.NewString(), uuid.NewString(), uuid.NewString(), strings.Repeat("0", 64), time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("seed corrupt authority: %v", err)
	}
	owner := &StartupSQLiteOwner{
		backend:     backend,
		path:        unsupportedSQLiteFilesystemPath(),
		schemaGuard: func() error { return nil },
	}
	inspection, err := owner.InspectAuthority(context.Background())
	if err != nil {
		t.Fatalf("InspectAuthority: %v", err)
	}
	if inspection.Status != runtimestartupownership.AuthorityInspectionCorrupt {
		t.Fatalf("inspection status = %s, want corrupt", inspection.Status)
	}
	beforeAuthority := sqliteAuthorityProofCount(t, db, "runtime_startup_authority_facts")
	beforeRepairs := sqliteAuthorityProofCount(t, db, "runtime_startup_authority_repairs")
	_, err = owner.RepairAuthority(context.Background(), runtimestartupownership.AuthorityRepairRequest{
		OperationID: uuid.NewString(), FindingsDigest: inspection.FindingsDigest, Confirmed: true,
	})
	if err == nil || !strings.Contains(err.Error(), "filesystem cannot prove process ownership") {
		t.Fatalf("RepairAuthority error = %v, want unsupported-filesystem refusal", err)
	}
	if got := sqliteAuthorityProofCount(t, db, "runtime_startup_authority_facts"); got != beforeAuthority {
		t.Fatalf("authority rows after refused repair = %d, want %d", got, beforeAuthority)
	}
	if got := sqliteAuthorityProofCount(t, db, "runtime_startup_authority_repairs"); got != beforeRepairs {
		t.Fatalf("repair rows after refused repair = %d, want %d", got, beforeRepairs)
	}
}

func unsupportedSQLiteFilesystemPath() string {
	if runtime.GOOS == "darwin" {
		return "/dev/null"
	}
	return "/proc/version"
}

func sqliteAuthorityProofCount(t testing.TB, db *sql.DB, table string) int {
	t.Helper()
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return count
}
