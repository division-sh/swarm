package serveapp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

type sourceArtifactWriteFuncForTest func(context.Context, *sourceartifact.AdmittedSourceArtifact, durabledata.Catalog) (sourceartifact.EnsureResult, error)

func (f sourceArtifactWriteFuncForTest) EnsureSourceArtifactWithData(ctx context.Context, artifact *sourceartifact.AdmittedSourceArtifact, catalog durabledata.Catalog) (sourceartifact.EnsureResult, error) {
	return f(ctx, artifact, catalog)
}

func Test2376ServeSourcePublicationRequiresAcknowledgedIngest(t *testing.T) {
	bundle := loadWorkflowValidationFixtureBundle(t, "examples/routing/root-ingress")
	for _, candidate := range []bool{false, true} {
		name := "no_candidate"
		if candidate {
			name = "unacknowledged_candidate"
		}
		t.Run(name, func(t *testing.T) {
			cause := errors.New("source commit acknowledgement unavailable")
			calls := 0
			writer := sourceArtifactWriteFuncForTest(func(_ context.Context, artifact *sourceartifact.AdmittedSourceArtifact, _ durabledata.Catalog) (sourceartifact.EnsureResult, error) {
				calls++
				var result sourceartifact.EnsureResult
				if candidate {
					persisted, err := sourceartifact.PersistedFromArtifact(artifact, time.Now().UTC())
					if err != nil {
						t.Fatal(err)
					}
					result = sourceartifact.EnsureResult{Artifact: persisted, Created: true}
				}
				return result, cause
			})
			// Unit caller contract; the both-store test injects the actual physical COMMIT.
			fact, err := prepareServeSourceArtifact(context.Background(), writer, bundle)
			if !errors.Is(err, cause) || fact.BundleHash() != "" || calls != 1 {
				t.Fatalf("unacknowledged ingest published/retried: fact=%+v err=%v calls=%d", fact, err, calls)
			}
		})
	}
}
