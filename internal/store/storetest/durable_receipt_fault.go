package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ResourceReceiptFaultCase = private.ResourceReceiptFaultCase
type ResourceReceiptFault = private.ResourceReceiptFault

func CaptureResourceReceiptFault(ctx context.Context, selected any, id, child string, kind ResourceReceiptFaultCase) (*ResourceReceiptFault, error) {
	return private.CaptureResourceReceiptFaultForTest(ctx, selected, id, child, kind)
}

func RequireResourceReceiptFault(t testing.TB, ctx context.Context, selected any, id, child string, kind ResourceReceiptFaultCase) *ResourceReceiptFault {
	t.Helper()
	fault, err := CaptureResourceReceiptFault(ctx, selected, id, child, kind)
	return requireCapturedResourceReceiptFault(t, ctx, fault, err)
}

func RequireImportProvenanceRemoval(t testing.TB, ctx context.Context, selected any, source string) *ResourceReceiptFault {
	t.Helper()
	fault, err := private.CaptureImportProvenanceRemovalForTest(ctx, selected, source)
	return requireCapturedResourceReceiptFault(t, ctx, fault, err)
}

func RequireResourcePayloadCorruption(t testing.TB, ctx context.Context, selected any, version, kind string) *ResourceReceiptFault {
	t.Helper()
	fault, err := private.CaptureResourcePayloadCorruptionForTest(ctx, selected, version, kind)
	return requireCapturedResourceReceiptFault(t, ctx, fault, err)
}

func RequireSourceCandidateCorruption(t testing.TB, ctx context.Context, selected any, id string, candidate durabledata.CompiledVersion) *ResourceReceiptFault {
	t.Helper()
	fault, err := private.CaptureSourceCandidateCorruptionForTest(ctx, selected, id, candidate)
	return requireCapturedResourceReceiptFault(t, ctx, fault, err)
}

func RequireSourceExactInputCorruption(t testing.TB, ctx context.Context, selected any, id string) durabledata.SourceCommand {
	t.Helper()
	fault, command, err := private.CaptureSourceExactInputCorruptionForTest(ctx, selected, id)
	requireCapturedResourceReceiptFault(t, ctx, fault, err)
	return command
}

func RequireSourceClockRewrite(t testing.TB, ctx context.Context, selected any, id string, completed time.Time) *ResourceReceiptFault {
	t.Helper()
	fault, err := private.CaptureSourceClockRewriteForTest(ctx, selected, id, completed)
	return requireCapturedResourceReceiptFault(t, ctx, fault, err)
}

func RequireProvenanceReadArm(t testing.TB, ctx context.Context, selected any, version, kind, producer string, completed time.Time) *ResourceReceiptFault {
	t.Helper()
	fault, err := private.CaptureProvenanceReadArmForTest(ctx, selected, version, kind, producer, completed)
	return requireCapturedResourceReceiptFault(t, ctx, fault, err)
}

func RequireNonMutatingProvenance(t testing.TB, ctx context.Context, selected any, version, source string) *ResourceReceiptFault {
	t.Helper()
	fault, err := private.CaptureNonMutatingProvenanceForTest(ctx, selected, version, source)
	return requireCapturedResourceReceiptFault(t, ctx, fault, err)
}

func RequireProvenanceCorruption(t testing.TB, ctx context.Context, selected any, version, kind string) *ResourceReceiptFault {
	t.Helper()
	fault, err := private.CaptureProvenanceCorruptionForTest(ctx, selected, version, kind)
	return requireCapturedResourceReceiptFault(t, ctx, fault, err)
}

func RequireSourceHeadCommitFault(t testing.TB, ctx context.Context, selected any, id, kind string) *ResourceReceiptFault {
	t.Helper()
	fault, err := private.CaptureSourceHeadCommitFaultForTest(ctx, selected, id, kind)
	return requireCapturedResourceReceiptFault(t, ctx, fault, err)
}

func RequirePrunePinEvidenceRemoval(t testing.TB, ctx context.Context, selected any, id string, ordinal int) *ResourceReceiptFault {
	t.Helper()
	fault, err := private.CapturePrunePinEvidenceRemovalForTest(ctx, selected, id, ordinal)
	return requireCapturedResourceReceiptFault(t, ctx, fault, err)
}

func RequireNonMutatingHeadHistory(t testing.TB, ctx context.Context, selected any, source string) *ResourceReceiptFault {
	t.Helper()
	fault, err := private.CaptureNonMutatingHeadHistoryForTest(ctx, selected, source)
	return requireCapturedResourceReceiptFault(t, ctx, fault, err)
}

func MaterializePruneWitnessPins(ctx context.Context, selected any, version durabledata.VersionID, ref durabledata.DeclarationRef, schema durabledata.SchemaDigest) error {
	return private.MaterializePruneWitnessPinsForTest(ctx, selected, version, ref, schema)
}

func CaptureSourceContextCorruption(t testing.TB, ctx context.Context, selected any, id, kind string) (*ResourceReceiptFault, durabledata.SourceOperationResult) {
	t.Helper()
	fault, result, err := private.CaptureSourceContextCorruptionForTest(ctx, selected, id, kind)
	registerResourceReceiptCleanup(t, fault, err)
	return fault, result
}

func CaptureFailedFusedContextCorruption(t testing.TB, ctx context.Context, selected any, run, source string) (*ResourceReceiptFault, durabledata.FusedChildEvaluation, []durabledata.ValidationDefect) {
	t.Helper()
	fault, projection, defects, err := private.CaptureFailedFusedContextCorruptionForTest(ctx, selected, run, source)
	registerResourceReceiptCleanup(t, fault, err)
	return fault, projection, defects
}

func requireCapturedResourceReceiptFault(t testing.TB, ctx context.Context, fault *ResourceReceiptFault, err error) *ResourceReceiptFault {
	t.Helper()
	registerResourceReceiptCleanup(t, fault, err)
	if err := fault.InstallForTest(ctx); err != nil {
		t.Fatal(err)
	}
	return fault
}

func registerResourceReceiptCleanup(t testing.TB, fault *ResourceReceiptFault, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := fault.RestoreForTest(cleanup); err != nil {
			t.Error(err)
		}
	})
}
