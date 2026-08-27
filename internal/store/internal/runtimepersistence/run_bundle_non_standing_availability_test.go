package runtimepersistence

import (
	"context"
	"database/sql"
	"testing"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunbundle "github.com/division-sh/swarm/internal/runtime/runbundle"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimestartuprecovery "github.com/division-sh/swarm/internal/runtime/startuprecovery"
	"github.com/division-sh/swarm/internal/testutil"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

func TestActiveNonStandingRunBundleAvailabilitySelectorParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newRunBundleAvailabilityParityFixture(t, backend)
			ctx := testAuthorActivityRuntimeContext()

			persistedHash := "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111"
			missingHash := "bundle-v1:sha256:2222222222222222222222222222222222222222222222222222222222222222"
			ephemeralHash := "bundle-v1:sha256:3333333333333333333333333333333333333333333333333333333333333333"
			deletedHash := "bundle-v1:sha256:4444444444444444444444444444444444444444444444444444444444444444"
			retiredHash := "bundle-v1:sha256:5555555555555555555555555555555555555555555555555555555555555555"
			seedStoreTestPersistedBundle(t, fixture.db, persistedHash)

			ordinaryRunningPersisted := fixture.seedRun(t, ctx, string(runtimerunlifecycle.StateRunning), persistedHash, runtimerunlifecycle.BundleSourcePersisted)
			ordinaryPausedPersistedMissing := fixture.seedRun(t, ctx, string(runtimerunlifecycle.StatePaused), missingHash, runtimerunlifecycle.BundleSourcePersisted)
			ordinaryRunningEphemeral := fixture.seedRun(t, ctx, string(runtimerunlifecycle.StateRunning), ephemeralHash, runtimerunlifecycle.BundleSourceEphemeral)
			ordinaryPausedDeleted := fixture.seedRun(t, ctx, string(runtimerunlifecycle.StatePaused), deletedHash, runtimerunlifecycle.BundleSourceDeleted)
			completed := fixture.seedRun(t, ctx, string(runtimerunlifecycle.StateCompleted), deletedHash, runtimerunlifecycle.BundleSourceDeleted)

			currentActive := fixture.createStanding(t, ctx, "current-active", persistedHash)
			currentSuspended := fixture.createStanding(t, ctx, "current-suspended", persistedHash)
			fixture.rewriteCurrentStanding(t, ctx, currentSuspended, string(runtimerunlifecycle.StatePaused), ephemeralHash, runtimerunlifecycle.BundleSourceEphemeral, "suspended", true)
			currentOrphaned := fixture.createStanding(t, ctx, "current-orphaned", persistedHash)
			fixture.rewriteCurrentStanding(t, ctx, currentOrphaned, string(runtimerunlifecycle.StatePaused), deletedHash, runtimerunlifecycle.BundleSourceDeleted, "orphaned", false)

			retired := fixture.createStanding(t, ctx, "retired-generation", persistedHash)
			reset, err := fixture.workflow.ResetStandingService(ctx, runtimepipeline.StandingServiceOperation{
				ServiceID: retired.serviceID,
				Actor:     "availability-selector-proof",
			})
			if err != nil {
				t.Fatalf("reset standing service: %v", err)
			}
			fixture.rewriteRun(t, ctx, retired.runID, string(runtimerunlifecycle.StateRunning), retiredHash, runtimerunlifecycle.BundleSourceEphemeral)

			availabilities, err := fixture.availability.ActiveNonStandingRunBundleAvailabilities(ctx)
			if err != nil {
				t.Fatalf("ActiveNonStandingRunBundleAvailabilities: %v", err)
			}
			got := make(map[string]runtimerunbundle.Availability, len(availabilities))
			for _, availability := range availabilities {
				got[availability.RunID] = availability
			}
			for _, runID := range []string{ordinaryRunningPersisted, ordinaryPausedPersistedMissing, ordinaryRunningEphemeral, ordinaryPausedDeleted, retired.runID} {
				if _, ok := got[runID]; !ok {
					t.Errorf("non-standing active availability omitted run %s: %#v", runID, availabilities)
				}
			}
			for _, runID := range []string{completed, currentActive.runID, currentSuspended.runID, currentOrphaned.runID, reset.RunID} {
				if _, ok := got[runID]; ok {
					t.Errorf("non-standing active availability included excluded run %s: %#v", runID, availabilities)
				}
			}
			if len(got) != 5 {
				t.Fatalf("non-standing active availabilities = %#v, want exactly four ordinary running/paused rows plus active retired generation", availabilities)
			}
			if got[ordinaryRunningPersisted].ErrorCode != "" || !got[ordinaryRunningPersisted].Available() {
				t.Errorf("ordinary persisted run = %#v, want available", got[ordinaryRunningPersisted])
			}
			if got[ordinaryPausedPersistedMissing].Cause != "persisted_missing_bundle_row" || got[ordinaryPausedPersistedMissing].ErrorCode != runtimerunbundle.CodeBundleDataIntegrityError {
				t.Errorf("ordinary persisted-missing run = %#v, want data-integrity conflict", got[ordinaryPausedPersistedMissing])
			}
			if got[ordinaryRunningEphemeral].Cause != runtimerunlifecycle.BundleSourceEphemeral || got[ordinaryRunningEphemeral].ErrorCode != runtimerunbundle.CodeBundleUnavailable {
				t.Errorf("ordinary ephemeral run = %#v, want unavailable", got[ordinaryRunningEphemeral])
			}
			if got[ordinaryPausedDeleted].Cause != runtimerunlifecycle.BundleSourceDeleted || got[ordinaryPausedDeleted].ErrorCode != runtimerunbundle.CodeBundleUnavailable {
				t.Errorf("ordinary deleted run = %#v, want unavailable", got[ordinaryPausedDeleted])
			}
			if got[retired.runID].Cause != runtimerunlifecycle.BundleSourceEphemeral || got[retired.runID].ErrorCode != runtimerunbundle.CodeBundleUnavailable {
				t.Errorf("retired standing generation = %#v, want ordinary unavailable classification", got[retired.runID])
			}
		})
	}
}

func TestLoadRunBundleAvailabilityIncludesCurrentStandingRunParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newRunBundleAvailabilityParityFixture(t, backend)
			ctx := testAuthorActivityRuntimeContext()
			persistedHash := "bundle-v1:sha256:5555555555555555555555555555555555555555555555555555555555555555"
			deletedHash := "bundle-v1:sha256:6666666666666666666666666666666666666666666666666666666666666666"
			standing := fixture.createStanding(t, ctx, "exact-load", persistedHash)
			fixture.rewriteCurrentStanding(t, ctx, standing, string(runtimerunlifecycle.StatePaused), deletedHash, runtimerunlifecycle.BundleSourceDeleted, "orphaned", false)

			availability, err := fixture.availability.LoadRunBundleAvailability(ctx, standing.runID)
			if err != nil {
				t.Fatalf("LoadRunBundleAvailability: %v", err)
			}
			if availability.RunID != standing.runID || availability.Status != string(runtimerunlifecycle.StatePaused) || availability.BundleHash != deletedHash || availability.ErrorCode != runtimerunbundle.CodeBundleUnavailable || availability.Cause != runtimerunlifecycle.BundleSourceDeleted {
				t.Fatalf("exact current standing availability = %#v, want deleted paused standing run", availability)
			}
		})
	}
}

func TestStartupRecoveryCleansOrdinaryUnavailableRunWithoutClaimingCurrentStanding(t *testing.T) {
	fixture := newRunBundleAvailabilityParityFixture(t, "postgres")
	ctx := testAuthorActivityRuntimeContext()
	persistedHash := "bundle-v1:sha256:7777777777777777777777777777777777777777777777777777777777777777"
	ephemeralHash := "bundle-v1:sha256:8888888888888888888888888888888888888888888888888888888888888888"
	ordinary := fixture.seedRun(t, ctx, string(runtimerunlifecycle.StateRunning), ephemeralHash, runtimerunlifecycle.BundleSourceEphemeral)
	standing := fixture.createStanding(t, ctx, "startup-recovery", persistedHash)
	fixture.rewriteCurrentStanding(t, ctx, standing, string(runtimerunlifecycle.StatePaused), ephemeralHash, runtimerunlifecycle.BundleSourceEphemeral, "suspended", true)

	cleanup, ok := fixture.availability.(runtimestartuprecovery.PreservationCleanupStore)
	if !ok {
		t.Fatal("postgres availability store does not expose startup preservation cleanup")
	}
	result, err := runtimestartuprecovery.Recover(ctx, runtimestartuprecovery.Request{
		AvailabilityReader: fixture.availability,
		CleanupStore:       cleanup,
		Containers:         emptyManagedContainerOwner{},
	})
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(result.OrphanTargets) != 1 || result.OrphanTargets[0].RunID != ordinary {
		t.Fatalf("startup orphan targets = %#v, want only ordinary non-standing run %s", result.OrphanTargets, ordinary)
	}
	if len(result.Cleanup.Runs) != 1 || result.Cleanup.Runs[0].RunID != ordinary {
		t.Fatalf("startup cleanup runs = %#v, want only ordinary non-standing run %s", result.Cleanup.Runs, ordinary)
	}
	var ordinaryStatus, standingStatus string
	if err := fixture.db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, ordinary).Scan(&ordinaryStatus); err != nil {
		t.Fatalf("load ordinary startup-cleaned run: %v", err)
	}
	if err := fixture.db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, standing.runID).Scan(&standingStatus); err != nil {
		t.Fatalf("load current standing run after startup cleanup: %v", err)
	}
	if ordinaryStatus != string(runtimerunlifecycle.StateCancelled) || standingStatus != string(runtimerunlifecycle.StatePaused) {
		t.Fatalf("post-recovery statuses ordinary=%s standing=%s, want cancelled/paused", ordinaryStatus, standingStatus)
	}
}

type runBundleAvailabilityParityFixture struct {
	backend      string
	db           *sql.DB
	availability runtimerunbundle.AvailabilityStore
	workflow     *runtimepipeline.PipelineCoordinator
}

type standingAvailabilityFixture struct {
	serviceID string
	runID     string
}

type emptyManagedContainerOwner struct{}

func (emptyManagedContainerOwner) ManagedContainers(context.Context) ([]runtimestartuprecovery.ManagedContainer, error) {
	return nil, nil
}

func (emptyManagedContainerOwner) StopManagedContainer(context.Context, string) error { return nil }

func newRunBundleAvailabilityParityFixture(t *testing.T, backend string) runBundleAvailabilityParityFixture {
	t.Helper()
	if backend == "sqlite" {
		selected := newBootstrappedSQLiteRuntimeStoreForTest(t)
		db := selected.backend.ConstructionHandle()
		return runBundleAvailabilityParityFixture{
			backend: backend, db: db, availability: selected,
			workflow: newSQLiteWorkflowTestCoordinator(t, db, selected),
		}
	}
	_, db, _ := testutil.StartPostgres(t)
	selected := admitTestPostgresStore(t, db)
	return runBundleAvailabilityParityFixture{
		backend: backend, db: db, availability: selected,
		workflow: newPostgresWorkflowTestCoordinator(t, db, selected),
	}
}

func (f runBundleAvailabilityParityFixture) seedRun(t *testing.T, ctx context.Context, state, bundleHash, bundleSource string) string {
	t.Helper()
	runID := uuid.NewString()
	snapshot := runlifecyclefixture.CorruptSnapshot{
		OriginKind: runlifecyclefixture.ScenarioSetupOriginKind(),
		RunID:      runID, State: state, BundleHash: bundleHash, BundleSource: bundleSource,
	}
	if f.backend == "sqlite" {
		runlifecyclefixture.RequireCorruptSQLiteSnapshot(t, ctx, f.db, snapshot)
	} else {
		runlifecyclefixture.RequireCorruptPostgresSnapshot(t, ctx, f.db, snapshot)
	}
	return runID
}

func (f runBundleAvailabilityParityFixture) createStanding(t *testing.T, ctx context.Context, name, bundleHash string) standingAvailabilityFixture {
	t.Helper()
	seedStoreTestPersistedBundle(t, f.db, bundleHash)
	serviceID := runtimeflowidentity.StandingServiceID("availability-selector", name)
	created, err := f.workflow.ReconcileStandingService(ctx, runtimepipeline.StandingServiceCandidate{
		ServiceID: serviceID, PackageKey: "availability-selector", FlowID: name,
		InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
		Source: mustStoreTestPersistedBundleSourceFact(bundleHash),
	})
	if err != nil {
		t.Fatalf("create standing service %s: %v", name, err)
	}
	return standingAvailabilityFixture{serviceID: serviceID, runID: created.RunID}
}

func (f runBundleAvailabilityParityFixture) rewriteCurrentStanding(t *testing.T, ctx context.Context, standing standingAvailabilityFixture, state, bundleHash, bundleSource, effectiveState string, declarationPresent bool) {
	t.Helper()
	f.rewriteRun(t, ctx, standing.runID, state, bundleHash, bundleSource)
	operatorOverride := "none"
	var overrideActor any
	var overrideAt any
	if effectiveState == "suspended" {
		operatorOverride = "suspended"
		overrideActor = "availability-selector-proof"
		overrideAt = time.Now().UTC()
	}
	var err error
	if f.backend == "sqlite" {
		_, err = f.db.ExecContext(ctx, `UPDATE standing_services SET effective_state = ?, declaration_present = ?, operator_override = ?, override_actor = ?, override_at = ? WHERE service_id = ?`, effectiveState, declarationPresent, operatorOverride, overrideActor, overrideAt, standing.serviceID)
	} else {
		_, err = f.db.ExecContext(ctx, `UPDATE standing_services SET effective_state = $2, declaration_present = $3, operator_override = $4, override_actor = $5, override_at = $6 WHERE service_id = $1::uuid`, standing.serviceID, effectiveState, declarationPresent, operatorOverride, overrideActor, overrideAt)
	}
	if err != nil {
		t.Fatalf("rewrite current standing service %s: %v", standing.serviceID, err)
	}
}

func (f runBundleAvailabilityParityFixture) rewriteRun(t *testing.T, ctx context.Context, runID, state, bundleHash, bundleSource string) {
	t.Helper()
	parsedState, err := runtimerunlifecycle.ParseState(state)
	if err != nil || !parsedState.Active() {
		t.Fatalf("rewrite active run %s state %q: %v", runID, state, err)
	}
	if f.backend == "sqlite" {
		runlifecyclefixture.CorruptSQLiteState(t, ctx, f.db, runID, state, time.Time{})
		runlifecyclefixture.CorruptSQLiteSource(t, ctx, f.db, runID, bundleHash, bundleSource)
	} else {
		runlifecyclefixture.CorruptPostgresState(t, ctx, f.db, runID, state, time.Time{})
		runlifecyclefixture.CorruptPostgresSource(t, ctx, f.db, runID, bundleHash, bundleSource)
	}
}
