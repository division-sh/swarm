package runbundle

import (
	"database/sql"
	"testing"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestDeletedAvailabilityCannotBecomeExecutableFact(t *testing.T) {
	source, err := DecodeAvailabilitySource("deleted")
	if err != nil {
		t.Fatalf("DecodeAvailabilitySource: %v", err)
	}
	if !source.IsDeleted() {
		t.Fatalf("source = %q, want deleted", source.String())
	}
	if _, err := runtimecorrelation.DecodeBundleSourceFact(testBundleHash, source.String()); err == nil {
		t.Fatal("deleted availability decoded as executable source fact")
	}
}

const (
	testBundleHash = "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111"
)

func TestLoadAvailabilityClassifiesBundleSourceStates(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext()

	persistedPresent := seedRunBundleAvailability(t, db, "running", testBundleHash, "persisted")
	persistedMissing := seedRunBundleAvailability(t, db, "running", "bundle-v1:sha256:2222222222222222222222222222222222222222222222222222222222222222", "persisted")
	ephemeral := seedRunBundleAvailability(t, db, "running", "bundle-v1:sha256:3333333333333333333333333333333333333333333333333333333333333333", "ephemeral")
	deleted := seedRunBundleAvailability(t, db, "running", "bundle-v1:sha256:4444444444444444444444444444444444444444444444444444444444444444", "deleted")

	if _, err := db.ExecContext(ctx, `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json)
		VALUES ($1, 'name: test', '{}'::jsonb)
	`, testBundleHash); err != nil {
		t.Fatalf("seed bundle row: %v", err)
	}

	for _, tc := range []struct {
		name       string
		runID      string
		available  bool
		code       string
		cause      string
		rowPresent bool
	}{
		{name: "persisted present", runID: persistedPresent, available: true, rowPresent: true},
		{name: "persisted missing", runID: persistedMissing, code: CodeBundleDataIntegrityError, cause: "persisted_missing_bundle_row"},
		{name: "ephemeral", runID: ephemeral, code: CodeBundleUnavailable, cause: "ephemeral"},
		{name: "deleted", runID: deleted, code: CodeBundleUnavailable, cause: "deleted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			availability, err := LoadAvailability(ctx, db, tc.runID)
			if err != nil {
				t.Fatalf("LoadAvailability: %v", err)
			}
			if availability.Available() != tc.available {
				t.Fatalf("Available() = %t, want %t: %#v", availability.Available(), tc.available, availability)
			}
			if availability.ErrorCode != tc.code {
				t.Fatalf("ErrorCode = %q, want %q: %#v", availability.ErrorCode, tc.code, availability)
			}
			if availability.Cause != tc.cause {
				t.Fatalf("Cause = %q, want %q: %#v", availability.Cause, tc.cause, availability)
			}
			if availability.BundleRowPresent != tc.rowPresent {
				t.Fatalf("BundleRowPresent = %t, want %t: %#v", availability.BundleRowPresent, tc.rowPresent, availability)
			}
		})
	}
}

func TestLoadAvailabilityReadsSourceBeforeBundleRows(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext()
	runID := seedRunBundleAvailability(t, db, "running", testBundleHash, "deleted")
	if _, err := db.ExecContext(ctx, `DROP TABLE bundles`); err != nil {
		t.Fatalf("drop bundles table: %v", err)
	}

	availability, err := LoadAvailability(ctx, db, runID)
	if err != nil {
		t.Fatalf("LoadAvailability deleted without bundles table: %v", err)
	}
	if availability.ErrorCode != CodeBundleUnavailable || availability.Cause != "deleted" {
		t.Fatalf("availability = %#v, want deleted unavailable without bundle lookup", availability)
	}
}

func TestListActiveConflictsUsesAvailabilityOwner(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext()
	persistedPresent := seedRunBundleAvailability(t, db, "running", testBundleHash, "persisted")
	ephemeral := seedRunBundleAvailability(t, db, "paused", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "ephemeral")
	completedDeleted := seedRunBundleAvailability(t, db, "completed", "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "deleted")

	if _, err := db.ExecContext(ctx, `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json)
		VALUES ($1, 'name: test', '{}'::jsonb)
	`, testBundleHash); err != nil {
		t.Fatalf("seed bundle row: %v", err)
	}

	conflicts, err := ListActiveConflicts(ctx, db)
	if err != nil {
		t.Fatalf("ListActiveConflicts: %v", err)
	}
	if len(conflicts) != 1 {
		t.Fatalf("conflicts = %#v, want only active ephemeral conflict; persisted=%s completed=%s", conflicts, persistedPresent, completedDeleted)
	}
	if conflicts[0].RunID != ephemeral || conflicts[0].ErrorCode != CodeBundleUnavailable || conflicts[0].Cause != "ephemeral" {
		t.Fatalf("conflict = %#v, want active ephemeral run", conflicts[0])
	}
}

func seedRunBundleAvailability(t *testing.T, db *sql.DB, status, bundleHash, bundleSource string) string {
	t.Helper()
	runID := uuid.NewString()
	var hash sql.NullString
	if bundleHash != "" {
		hash = sql.NullString{String: bundleHash, Valid: true}
	}
	if _, err := db.Exec(`
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source)
		VALUES ($1::uuid, $2, $3, $4)
	`, runID, status, hash, bundleSource); err != nil {
		t.Fatalf("seed run availability %s: %v", status, err)
	}
	return runID
}
