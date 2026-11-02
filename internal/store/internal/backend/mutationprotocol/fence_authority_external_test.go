package mutationprotocol_test

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/construction"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

// The external fixture can assemble the canonical store without making the
// protocol's private unit package depend on its own downstream store assembly.
func prepareNativeFenceAuthority(t *testing.T, dsn string) context.Context {
	t.Helper()
	ctx := context.Background()
	selected, _, err := construction.OpenPostgresRuntime(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := selected.Close(); err != nil {
			t.Error(err)
		}
	})
	storetest.BootstrapPostgresRuntimeStore(t, selected)
	const runtimeID = "00000000-0000-0000-0000-000000000242"
	process, err := selected.AcquireProcessCapability(ctx, startupownership.AcquireRequest{
		OwnerID: "native-fence-fixture", BootID: uuid.NewString(), RuntimeInstanceID: runtimeID,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := process.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	ctx, err = startupownership.WithProcessMutationAuthority(ctx, process)
	if err != nil {
		t.Fatal(err)
	}
	rooted, _ := sourceartifactfixture.Rooted(t, "fence", sourceartifactfixture.Artifact())
	if _, err := selected.EnsureRootedSource(ctx, rooted); err != nil {
		t.Fatal(err)
	}
	coordinate, err := agenttopology.NewSourceCoordinate(rooted.Admission())
	if err != nil {
		t.Fatal(err)
	}
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{coordinate}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := process.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
		t.Fatal(err)
	}
	grant, err := process.IssueGenerationGrant(ctx, startupownership.GrantRequest{
		BundleHash: coordinate.BundleHash, RuntimeInstanceID: runtimeID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := grant.AdmitExecution(ctx); err != nil {
		t.Fatal(err)
	}
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runtimeID, coordinate.BundleHash))
	ctx, err = startupownership.WithRuntimeMutationAuthority(ctx, grant)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func TestNativeRetainedPostgresFenceConflictRetriesBeforeDomain(t *testing.T) {
	mutationprotocol.VerifyNativeRetainedPostgresFenceConflictRetriesBeforeDomainForTest(t, prepareNativeFenceAuthority)
}

func TestNativePostgresFenceConsumers(t *testing.T) {
	mutationprotocol.VerifyNativePostgresFenceConsumersForTest(t, prepareNativeFenceAuthority)
}

func TestNativeRetainedPostgresPersistentFenceConflictCancellation(t *testing.T) {
	mutationprotocol.VerifyNativeRetainedPostgresPersistentFenceConflictCancellationForTest(t, prepareNativeFenceAuthority)
}

func TestNativeRetainedPostgresFenceSettlementRefusesRetry(t *testing.T) {
	mutationprotocol.VerifyNativeRetainedPostgresFenceSettlementRefusesRetryForTest(t, prepareNativeFenceAuthority)
}

func TestNativeRetainedPostgresPostFenceConflictsNeverReplay(t *testing.T) {
	mutationprotocol.VerifyNativeRetainedPostgresPostFenceConflictsNeverReplayForTest(t, prepareNativeFenceAuthority)
}

func TestNativeRetainedPostgresFenceRetryRechecksAdmission(t *testing.T) {
	mutationprotocol.VerifyNativeRetainedPostgresFenceRetryRechecksAdmissionForTest(t, prepareNativeFenceAuthority)
}
