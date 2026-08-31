package startuprecovery

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runbundle"
)

func TestRecoverFailsClosedWhenAnyActiveRunLacksItsSourceArtifact(t *testing.T) {
	reader := fakeAvailabilityReader{items: []runbundle.Availability{
		{
			RunID:      "11111111-1111-1111-1111-111111111111",
			Status:     "running",
			BundleHash: "bundle-v2:sha256:2222222222222222222222222222222222222222222222222222222222222222",
			ErrorCode:  runbundle.CodeBundleDataIntegrityError,
			Cause:      "missing_source_artifact",
		},
		{
			RunID:                 "22222222-2222-2222-2222-222222222222",
			Status:                "running",
			BundleHash:            "bundle-v2:sha256:3333333333333333333333333333333333333333333333333333333333333333",
			SourceArtifactPresent: true,
		},
	}}

	result, err := Recover(context.Background(), Request{AvailabilityReader: reader})
	if err == nil || !IsDataIntegrityError(err) || !strings.Contains(err.Error(), runbundle.CodeBundleDataIntegrityError) {
		t.Fatalf("Recover err = %v, want data integrity error", err)
	}
	if len(result.CheckedAvailabilities) != 2 || len(result.DataIntegrityErrors) != 1 {
		t.Fatalf("result = %#v, want complete census and one data-integrity conflict", result)
	}
}

func TestRecoverAcceptsOnlyAvailableSourceArtifacts(t *testing.T) {
	reader := fakeAvailabilityReader{items: []runbundle.Availability{
		{
			RunID:                 "11111111-1111-1111-1111-111111111111",
			Status:                "running",
			BundleHash:            "bundle-v2:sha256:1111111111111111111111111111111111111111111111111111111111111111",
			SourceArtifactPresent: true,
		},
	}}

	result, err := Recover(context.Background(), Request{AvailabilityReader: reader})
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(result.CheckedAvailabilities) != 1 || len(result.DataIntegrityErrors) != 0 {
		t.Fatalf("result = %#v, want one verified source artifact", result)
	}
}

type fakeAvailabilityReader struct {
	items []runbundle.Availability
	err   error
}

func (r fakeAvailabilityReader) ActiveNonStandingRunBundleAvailabilities(context.Context) ([]runbundle.Availability, error) {
	if r.err != nil {
		return nil, r.err
	}
	return append([]runbundle.Availability(nil), r.items...), nil
}
