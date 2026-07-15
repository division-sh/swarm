package cataloge2e

import (
	"context"
	"sort"
	"strings"
	"testing"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"

var authorActivityTestBundleSourceFact = runtimecorrelation.BundleSourceFact{
	BundleHash:        "bundle-v1:sha256:" + strings.Repeat("a", 64),
	BundleSource:      "ephemeral",
	BundleFingerprint: "sha256:" + strings.Repeat("a", 64),
}

func testAuthorActivityContext(ctx context.Context) context.Context {
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID,
		authorActivityTestBundleSourceFact.BundleHash,
	))
}

func testAuthorActivityRuntimeOptions(opts runtimepkg.RuntimeOptions) runtimepkg.RuntimeOptions {
	if strings.TrimSpace(opts.RuntimeInstanceID) == "" {
		opts.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
	}
	if strings.TrimSpace(opts.BundleSourceFact.BundleHash) == "" {
		opts.BundleSourceFact = authorActivityTestBundleSourceFact
	}
	return opts
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
		runtimeauthoractivity.BundleScope(authorActivityTestRuntimeInstanceID, authorActivityTestBundleSourceFact.BundleHash),
		descriptors,
	)
	if err != nil {
		t.Fatalf("register test author activity catalog: %v", err)
	}
	t.Cleanup(lease.Release)
}
