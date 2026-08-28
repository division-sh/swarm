package cataloge2e

import (
	"context"
	"sort"
	"strings"
	"testing"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"

var authorActivityTestBundleSourceFact = mustAuthorActivityTestBundleSourceFact()

func mustAuthorActivityTestBundleSourceFact() runtimecorrelation.BundleSourceFact {
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("e", 64))
	if err != nil {
		panic(err)
	}
	return fact
}

func testAuthorActivityContext(ctx context.Context) context.Context {
	return testAuthorActivityContextForBundle(ctx, authorActivityTestBundleSourceFact)
}

func testAuthorActivityContextForBundle(ctx context.Context, fact runtimecorrelation.BundleSourceFact) context.Context {
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, fact)
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID,
		fact.BundleHash(),
	))
}

func catalogBundleSourceFact(t testing.TB, bundle *runtimecontracts.WorkflowContractBundle) runtimecorrelation.BundleSourceFact {
	t.Helper()
	hash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		t.Fatalf("compute catalog fixture bundle identity: %v", err)
	}
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact(hash)
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
		runtimeauthoractivity.BundleScope(authorActivityTestRuntimeInstanceID, authorActivityTestBundleSourceFact.BundleHash()),
		descriptors,
	)
	if err != nil {
		t.Fatalf("register test author activity catalog: %v", err)
	}
	t.Cleanup(lease.Release)
}
