package selected

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

type unusedAdmissionReader struct{ admissionInspectionPort }

func TestS1RootedInspectionJoinsFinalCallerCancellation(t *testing.T) {
	rooted, _ := sourceartifactfixture.Rooted(t, "inspection", sourceartifactfixture.Artifact())
	for _, nativeError := range []error{nil, errors.New("native snapshot completion failed")} {
		caller, cancel := context.WithCancel(context.Background())
		snapshot := &AdmissionSnapshot{ctx: context.Background(), store: unusedAdmissionReader{}}
		got, err := snapshot.readRootedSource(caller, func(context.Context) (sourceartifact.RootedSource, error) {
			cancel()
			return rooted, nativeError
		})
		cancel()
		if !errors.Is(err, context.Canceled) || (nativeError != nil && !errors.Is(err, nativeError)) || got.Validate() == nil {
			t.Fatalf("final caller cancellation disclosed source or lost native cause: %+v err=%v", got.Admission().Reference(), err)
		}
	}
}

type inspectionRootRead struct {
	name    string
	load    func(context.Context) (sourceartifact.RootedSource, error)
	present bool
}

func inspectionRootReads(snapshot *AdmissionSnapshot, ref sourceartifact.RootAdmissionReference) []inspectionRootRead {
	const missingRun = "00000000-0000-4000-8000-000000000001"
	return []inspectionRootRead{
		{"exact", func(ctx context.Context) (sourceartifact.RootedSource, error) {
			return snapshot.GetRootedSource(ctx, ref)
		}, true},
		{"unbound", func(ctx context.Context) (sourceartifact.RootedSource, error) {
			return snapshot.ResolveUnboundRootedSource(ctx, ref.BundleHash)
		}, true},
		{"original_missing", func(ctx context.Context) (sourceartifact.RootedSource, error) {
			return snapshot.LoadRunRootedSource(ctx, missingRun)
		}, false},
		{"retained_missing", func(ctx context.Context) (sourceartifact.RootedSource, error) {
			return snapshot.LoadSelectedForkRootedSource(ctx, missingRun)
		}, false},
	}
}
