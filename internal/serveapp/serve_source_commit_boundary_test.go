package serveapp

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

type sourceArtifactWriteFuncForTest func(context.Context, sourceartifact.RootedSource, durabledata.Catalog) (sourceartifact.RootedEnsureResult, error)

func (f sourceArtifactWriteFuncForTest) EnsureRootedSourceWithData(ctx context.Context, source sourceartifact.RootedSource, catalog durabledata.Catalog) (sourceartifact.RootedEnsureResult, error) {
	return f(ctx, source, catalog)
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
			writer := sourceArtifactWriteFuncForTest(func(_ context.Context, source sourceartifact.RootedSource, _ durabledata.Catalog) (sourceartifact.RootedEnsureResult, error) {
				calls++
				var result sourceartifact.RootedEnsureResult
				if candidate {
					persisted, err := sourceartifact.PersistedFromArtifact(source.Artifact(), time.Now().UTC())
					if err != nil {
						t.Fatal(err)
					}
					result = sourceartifact.RootedEnsureResult{Source: source, Artifact: sourceartifact.EnsureResult{Artifact: persisted, Created: true}, Created: true}
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

func TestS1ServeIngestRequiresExactRootAcknowledgment(t *testing.T) {
	bundle := loadWorkflowValidationFixtureBundle(t, "examples/routing/root-ingress")
	rooted, err := bundle.RootedSource()
	if err != nil {
		t.Fatal(err)
	}
	other, _ := sourceartifactfixture.Rooted(t, "other", rooted.Artifact())
	for _, result := range []sourceartifact.RootedSource{{}, other, rooted} {
		calls := 0
		writer := sourceArtifactWriteFuncForTest(func(_ context.Context, source sourceartifact.RootedSource, catalog durabledata.Catalog) (sourceartifact.RootedEnsureResult, error) {
			calls++
			if !source.Admission().Equal(rooted.Admission()) || catalog.BundleHash != rooted.Artifact().BundleHash() {
				t.Fatal("serve ingress changed the exact planned source")
			}
			return sourceartifact.RootedEnsureResult{Source: result}, nil
		})
		fact, err := prepareServeSourceArtifact(context.Background(), writer, bundle)
		valid := result.Admission().Equal(rooted.Admission())
		if calls != 1 || (err == nil) != valid || (fact.BundleHash() != "") != valid {
			t.Fatalf("rooted acknowledgement: valid=%t calls=%d fact=%+v error=%v", valid, calls, fact, err)
		}
	}
}

func TestS1ServeIngestRetainsExactRootBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected := openStandingRuntimeContextStore(t, backend, "s1-rooted-ingest")
			bundle := loadWorkflowValidationFixtureBundle(t, "examples/routing/root-ingress")
			rooted, err := bundle.RootedSource()
			if err != nil {
				t.Fatal(err)
			}
			fact, err := prepareServeSourceArtifact(context.Background(), selected.SourceArtifactWriter(), bundle)
			if err != nil || fact.BundleHash() != rooted.Artifact().BundleHash() {
				t.Fatalf("publish exact rooted serve source: %+v, %v", fact, err)
			}
			retained, err := selected.SourceArtifactStore().GetRootedSource(context.Background(), rooted.Admission().Reference())
			if err != nil || !retained.Admission().Equal(rooted.Admission()) || !bytes.Equal(retained.Artifact().LogicalBlob(), rooted.Artifact().LogicalBlob()) {
				t.Fatalf("serve ingest lost retained root/content: %+v, %v", retained.Admission().Reference(), err)
			}
			if _, err := prepareServeSourceArtifact(context.Background(), selected.SourceArtifactWriter(), bundle); err != nil {
				t.Fatalf("exact repeated rooted ingest: %v", err)
			}
		})
	}
}
