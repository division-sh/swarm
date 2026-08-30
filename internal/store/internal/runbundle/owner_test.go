package runbundle

import (
	"database/sql"
	"testing"

	runtimerunbundle "github.com/division-sh/swarm/internal/runtime/runbundle"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/testutil"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

const testBundleHash = "bundle-v2:sha256:1111111111111111111111111111111111111111111111111111111111111111"

func TestLoadAvailabilityClassifiesSourceArtifactPresence(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext()
	owner := newPostgresOwnerForTest(t, db)
	present := seedRunBundleAvailability(t, db, "running", testBundleHash)
	missingHash := "bundle-v2:sha256:2222222222222222222222222222222222222222222222222222222222222222"
	missing := seedRunBundleAvailability(t, db, "running", missingHash)

	if _, err := db.ExecContext(ctx, `
		INSERT INTO source_artifacts (bundle_hash, source_blob, member_count, total_bytes)
		VALUES ($1, $2::bytea, 1, 0)
	`, testBundleHash, []byte{0}); err != nil {
		t.Fatalf("seed source artifact row: %v", err)
	}

	for _, tc := range []struct {
		name       string
		runID      string
		available  bool
		code       string
		cause      string
		rowPresent bool
	}{
		{name: "present", runID: present, available: true, rowPresent: true},
		{name: "missing", runID: missing, code: runtimerunbundle.CodeBundleDataIntegrityError, cause: "missing_source_artifact"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			availability, err := owner.Load(ctx, tc.runID)
			if err != nil {
				t.Fatalf("LoadAvailability: %v", err)
			}
			if availability.Available() != tc.available || availability.ErrorCode != tc.code || availability.Cause != tc.cause || availability.SourceArtifactPresent != tc.rowPresent {
				t.Fatalf("availability = %#v, want available=%t code=%q cause=%q row_present=%t", availability, tc.available, tc.code, tc.cause, tc.rowPresent)
			}
		})
	}
}

func TestListActiveNonStandingUsesArtifactPresenceOwner(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext()
	owner := newPostgresOwnerForTest(t, db)
	present := seedRunBundleAvailability(t, db, "running", testBundleHash)
	missing := seedRunBundleAvailability(t, db, "paused", "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	completed := seedRunBundleAvailability(t, db, "completed", "bundle-v2:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

	if _, err := db.ExecContext(ctx, `
		INSERT INTO source_artifacts (bundle_hash, source_blob, member_count, total_bytes)
		VALUES ($1, $2::bytea, 1, 0)
	`, testBundleHash, []byte{0}); err != nil {
		t.Fatalf("seed source artifact row: %v", err)
	}

	availabilities, err := owner.ListActiveNonStanding(ctx)
	if err != nil {
		t.Fatalf("ListActiveNonStanding: %v", err)
	}
	if len(availabilities) != 2 {
		t.Fatalf("availabilities = %#v, want active present and missing runs; present=%s completed=%s", availabilities, present, completed)
	}
	var conflict *runtimerunbundle.Availability
	for i := range availabilities {
		if !availabilities[i].Available() {
			conflict = &availabilities[i]
		}
	}
	if conflict == nil || conflict.RunID != missing || conflict.ErrorCode != runtimerunbundle.CodeBundleDataIntegrityError || conflict.Cause != "missing_source_artifact" {
		t.Fatalf("conflict = %#v, want missing source artifact for run %s", conflict, missing)
	}
}

func newPostgresOwnerForTest(t *testing.T, db *sql.DB) *Postgres {
	t.Helper()
	backend, err := postgresbackend.New(db)
	if err != nil {
		t.Fatalf("postgres backend: %v", err)
	}
	owner, err := NewPostgres(backend)
	if err != nil {
		t.Fatalf("postgres run-bundle owner: %v", err)
	}
	return owner
}

func seedRunBundleAvailability(t *testing.T, db *sql.DB, status, bundleHash string) string {
	t.Helper()
	runID := uuid.NewString()
	runlifecyclefixture.RequireCorruptPostgresSnapshot(
		t,
		testAuthorActivityContext(),
		db,
		runlifecyclefixture.CorruptSnapshot{
			OriginKind: runlifecyclefixture.ScenarioSetupOriginKind(),
			RunID:      runID, State: status, BundleHash: bundleHash,
		},
	)
	return runID
}
