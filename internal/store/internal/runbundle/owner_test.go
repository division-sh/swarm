package runbundle

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	runtimerunbundle "github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	sourceartifactstore "github.com/division-sh/swarm/internal/store/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

var testBundleHash = sourceartifactfixture.Artifact().BundleHash()

func TestLoadAvailabilityClassifiesSourceArtifactPresence(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	owner, ctx := newPostgresOwnerForTest(t, db)
	present := seedRunBundleAvailability(t, ctx, db, "running", testBundleHash)
	missingHash := "bundle-v2:sha256:2222222222222222222222222222222222222222222222222222222222222222"
	missing := seedRunBundleAvailability(t, ctx, db, "running", missingHash)

	seedRetainedRunBundleSource(t, ctx, owner)

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
	owner, ctx := newPostgresOwnerForTest(t, db)
	present := seedRunBundleAvailability(t, ctx, db, "running", testBundleHash)
	missing := seedRunBundleAvailability(t, ctx, db, "paused", "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	completed := seedRunBundleAvailability(t, ctx, db, "completed", "bundle-v2:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

	seedRetainedRunBundleSource(t, ctx, owner)

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

func newPostgresOwnerForTest(t *testing.T, db *sql.DB) (*Postgres, context.Context) {
	t.Helper()
	backend, err := postgresbackend.New(db)
	if err != nil {
		t.Fatalf("postgres backend: %v", err)
	}
	owner, err := NewPostgres(backend)
	if err != nil {
		t.Fatalf("postgres run-bundle owner: %v", err)
	}
	ctx := testAuthorActivityContext()
	lease, err := postgresbackend.AcquireBlockingAdvisoryLockLease(ctx, backend, "native-run-bundle-fixture")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := lease.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	var original processbinding.LeaseStamp
	committed, err := lease.RunProcessAcquisitionOutcome(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		original, err = generationauthority.AdvanceProcessGeneration(txctx, tx, processbinding.LeaseIdentity{
			LeaseID: uuid.NewString(), OwnerID: "run-bundle-fixture", BootID: uuid.NewString(),
			RuntimeInstanceID: "11111111-1111-1111-1111-111111111111", Backend: processbinding.PostgresLeaseBackend,
		}, false)
		return err
	})
	if err != nil || !committed {
		t.Fatalf("acquire original run-bundle fixture: committed=%t err=%v", committed, err)
	}
	ctx, err = generationauthority.MutationContext(ctx, original, false)
	if err != nil {
		t.Fatal(err)
	}
	return owner, ctx
}

func seedRetainedRunBundleSource(t *testing.T, ctx context.Context, owner *Postgres) {
	t.Helper()
	rooted, _ := sourceartifactfixture.Rooted(t, "fixture", sourceartifactfixture.Artifact())
	if err := owner.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		_, err := sourceartifactstore.EnsurePostgresRootedSourceTx(ctx, tx, rooted, time.Now().UTC())
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func seedRunBundleAvailability(t *testing.T, ctx context.Context, db *sql.DB, status, bundleHash string) string {
	t.Helper()
	runID := uuid.NewString()
	if err := storerunlifecycle.AttemptCorruptPostgresSnapshotForTest(
		ctx,
		db,
		runlifecyclefixture.CorruptSnapshot{
			OriginKind:    runlifecyclefixture.ScenarioSetupOriginKind(),
			RootDirectory: "fixture",
			RunID:         runID, State: status, BundleHash: bundleHash,
		},
	); err != nil {
		t.Fatalf("materialize corrupt PostgreSQL run snapshot %s: %v", runID, err)
	}
	return runID
}
