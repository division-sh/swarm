package runtimepersistence

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

type rootedRunAvailabilityStore interface {
	rootedRunStore
	runbundle.AvailabilityStore
}

func TestS1RunAvailabilityCarriesExactRootBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected := selectedRootAdmissionStore(t, backend).(rootedRunAvailabilityStore)
			first, _ := admittedRootForStorage(t, "first")
			second, _ := admittedRootForStorage(t, "second")
			ctx := testAuthorActivityContextForBundle(first.Artifact().BundleHash())
			for _, source := range []sourceartifact.RootedSource{first, second} {
				if _, err := selected.EnsureRootedSource(ctx, source); err != nil {
					t.Fatal(err)
				}
			}
			request := rootedRunCreationRequest(first)
			if _, err := selected.CreateRun(ctx, request); err != nil {
				t.Fatal(err)
			}
			check := func(want sourceartifact.RootAdmissionReference, present bool) {
				t.Helper()
				availability, err := selected.LoadRunBundleAvailability(ctx, request.RunID)
				if err != nil || availability.RootReference != want || availability.Available() != present || availability.SourceArtifactPresent != present {
					t.Fatalf("exact run-root availability: %+v, %v", availability, err)
				}
				active, err := selected.ActiveNonStandingRunBundleAvailabilities(ctx)
				if err != nil || len(active) != 1 || active[0].RootReference != want || active[0].Available() != present {
					t.Fatalf("active root projection: %+v, %v", active, err)
				}
				if !present && (!availability.DataIntegrityError() || availability.Cause != "missing_source_artifact") {
					t.Fatalf("deleted source lost integrity classification: %+v", availability)
				}
			}
			check(first.Admission().Reference(), true)
			if _, err := selected.ReviseRunSource(ctx, runlifecycle.SourceRevisionRequest{
				RunID: request.RunID, Source: request.Source, RootAdmission: second.Admission(),
			}); err != nil {
				t.Fatal(err)
			}
			check(second.Admission().Reference(), true)
			if removed, err := RemoveRuntimeLogFixtureSourceArtifactForTest(ctx, selected, request.Source.BundleHash()); err != nil || removed != 1 {
				t.Fatalf("remove content: %d, %v", removed, err)
			}
			check(second.Admission().Reference(), false)
		})
	}
}

func TestS1RunAvailabilityRefusesUnretainedOrMalformedRootBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected := selectedRootAdmissionStore(t, backend).(rootedRunAvailabilityStore)
			source, _ := admittedRootForStorage(t, "first")
			ctx := testAuthorActivityContextForBundle(source.Artifact().BundleHash())
			if _, err := selected.EnsureRootedSource(ctx, source); err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct{ directory, cause string }{
				{"not-retained", "missing_source_root_admission"},
				{" first", "invalid_source_root"},
			} {
				runID := uuid.NewString()
				RequireCorruptRunSnapshotForTest(t, ctx, selected, runlifecyclefixture.CorruptSnapshot{
					RunID: runID, State: "running", BundleHash: source.Artifact().BundleHash(),
					RootDirectory: tc.directory, OriginKind: runlifecyclefixture.ScenarioSetupOriginKind(),
				})
				availability, err := selected.LoadRunBundleAvailability(ctx, runID)
				if err != nil || availability.Available() || !availability.DataIntegrityError() || availability.Cause != tc.cause ||
					!availability.SourceArtifactPresent || availability.RootAdmissionPresent || availability.RootReference.RootDirectory != tc.directory {
					t.Fatalf("invalid rooted availability: %+v, %v", availability, err)
				}
			}
			active, err := selected.ActiveNonStandingRunBundleAvailabilities(ctx)
			if err != nil || len(active) != 2 {
				t.Fatalf("active negative projection: %+v, %v", active, err)
			}
			for _, row := range active {
				if row.Available() || !row.DataIntegrityError() {
					t.Fatalf("active projection hid missing root evidence: %+v", row)
				}
			}
		})
	}
}
