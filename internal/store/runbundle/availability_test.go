package runbundle

import (
	"database/sql"
	"testing"

	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const (
	testBundleHash = "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111"
)

func TestLoadAvailabilityClassifiesBundleSourceStates(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext()

	persistedPresent := seedRunBundleAvailability(t, db, "running", testBundleHash, storerunlifecycle.BundleSourcePersisted, "")
	persistedMissing := seedRunBundleAvailability(t, db, "running", "bundle-v1:sha256:2222222222222222222222222222222222222222222222222222222222222222", storerunlifecycle.BundleSourcePersisted, "")
	ephemeral := seedRunBundleAvailability(t, db, "running", "bundle-v1:sha256:3333333333333333333333333333333333333333333333333333333333333333", storerunlifecycle.BundleSourceEphemeral, "")
	deleted := seedRunBundleAvailability(t, db, "running", "bundle-v1:sha256:4444444444444444444444444444444444444444444444444444444444444444", storerunlifecycle.BundleSourceDeleted, "")
	legacy := seedRunBundleAvailability(t, db, "running", "", storerunlifecycle.BundleSourceLegacy, "sha256:5555555555555555555555555555555555555555555555555555555555555555")

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
		{name: "ephemeral", runID: ephemeral, code: CodeBundleUnavailable, cause: storerunlifecycle.BundleSourceEphemeral},
		{name: "deleted", runID: deleted, code: CodeBundleUnavailable, cause: storerunlifecycle.BundleSourceDeleted},
		{name: "legacy", runID: legacy, code: CodeBundleUnavailable, cause: storerunlifecycle.BundleSourceLegacy},
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

func TestLoadAvailabilityClassifiesPersistedMissingHashAsDataIntegrity(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext()
	runID := seedRunBundleAvailability(t, db, "running", "", storerunlifecycle.BundleSourcePersisted, "")

	availability, err := LoadAvailability(ctx, db, runID)
	if err != nil {
		t.Fatalf("LoadAvailability: %v", err)
	}
	if availability.ErrorCode != CodeBundleDataIntegrityError || availability.Cause != "persisted_missing_hash" {
		t.Fatalf("availability = %#v, want persisted missing hash data-integrity error", availability)
	}
}

func TestLoadAvailabilityReadsSourceBeforeBundleRows(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext()
	runID := seedRunBundleAvailability(t, db, "running", "", storerunlifecycle.BundleSourceLegacy, testBundleHash)
	if _, err := db.ExecContext(ctx, `DROP TABLE bundles`); err != nil {
		t.Fatalf("drop bundles table: %v", err)
	}

	availability, err := LoadAvailability(ctx, db, runID)
	if err != nil {
		t.Fatalf("LoadAvailability legacy without bundles table: %v", err)
	}
	if availability.ErrorCode != CodeBundleUnavailable || availability.Cause != storerunlifecycle.BundleSourceLegacy {
		t.Fatalf("availability = %#v, want legacy unavailable without bundle lookup", availability)
	}
}

func TestListActiveConflictsUsesAvailabilityOwner(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext()
	persistedPresent := seedRunBundleAvailability(t, db, "running", testBundleHash, storerunlifecycle.BundleSourcePersisted, "")
	legacy := seedRunBundleAvailability(t, db, "paused", "", storerunlifecycle.BundleSourceLegacy, "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	completedLegacy := seedRunBundleAvailability(t, db, "completed", "", storerunlifecycle.BundleSourceLegacy, "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

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
		t.Fatalf("conflicts = %#v, want only active legacy conflict; persisted=%s completed=%s", conflicts, persistedPresent, completedLegacy)
	}
	if conflicts[0].RunID != legacy || conflicts[0].ErrorCode != CodeBundleUnavailable || conflicts[0].Cause != storerunlifecycle.BundleSourceLegacy {
		t.Fatalf("conflict = %#v, want active legacy run", conflicts[0])
	}
}

func seedRunBundleAvailability(t *testing.T, db *sql.DB, status, bundleHash, bundleSource, fingerprint string) string {
	t.Helper()
	runID := uuid.NewString()
	var hash, legacy sql.NullString
	if bundleHash != "" {
		hash = sql.NullString{String: bundleHash, Valid: true}
	}
	if fingerprint != "" {
		legacy = sql.NullString{String: fingerprint, Valid: true}
	}
	if _, err := db.Exec(`
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source, bundle_fingerprint)
		VALUES ($1::uuid, $2, $3, $4, $5)
	`, runID, status, hash, bundleSource, legacy); err != nil {
		t.Fatalf("seed run availability %s: %v", status, err)
	}
	return runID
}
