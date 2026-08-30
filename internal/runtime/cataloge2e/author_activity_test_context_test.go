package cataloge2e

import (
	"context"
	"sort"
	"strings"
	"testing"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"

var authorActivityTestSourceArtifactFact = sourceartifactfixture.Fact()

func testAuthorActivityContext(ctx context.Context) context.Context {
	return testAuthorActivityContextForBundle(ctx, authorActivityTestSourceArtifactFact)
}

func testAuthorActivityContextForBundle(ctx context.Context, fact runtimecorrelation.SourceArtifactFact) context.Context {
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, fact)
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID,
		fact.BundleHash(),
	))
}

func catalogSourceArtifactFact(t testing.TB, bundle *runtimecontracts.WorkflowContractBundle) runtimecorrelation.SourceArtifactFact {
	t.Helper()
	hash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		t.Fatalf("compute catalog fixture bundle identity: %v", err)
	}
	fact, err := runtimecorrelation.NewSourceArtifactFact(hash)
	if err != nil {
		t.Fatalf("admit catalog fixture bundle source fact: %v", err)
	}
	return fact
}

type testAuthorActivityCatalogRegistrar interface {
	RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
}

func registerTestAuthorActivityCatalog(t *testing.T, target testAuthorActivityCatalogRegistrar, eventTypes ...string) {
	t.Helper()
	sort.Strings(eventTypes)
	descriptors := make([]runtimeauthoractivity.EventDescriptor, 0, len(eventTypes))
	for _, eventType := range eventTypes {
		descriptors = append(descriptors, runtimeauthoractivity.EventDescriptor{
			EventType: strings.TrimSpace(eventType), Disposition: runtimeauthoractivity.StoryDifferent,
		})
	}
	lease, err := target.RegisterAuthorActivityEventCatalog(
		runtimeauthoractivity.BundleScope(authorActivityTestRuntimeInstanceID, authorActivityTestSourceArtifactFact.BundleHash()),
		descriptors,
	)
	if err != nil {
		t.Fatalf("register test author activity catalog: %v", err)
	}
	t.Cleanup(lease.Release)
}
