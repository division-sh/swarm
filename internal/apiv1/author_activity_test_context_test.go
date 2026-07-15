package apiv1

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"testing"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"

var authorActivityTestBundleSourceFact = runtimecorrelation.BundleSourceFact{
	BundleHash:        "bundle-v1:sha256:" + strings.Repeat("a", 64),
	BundleSource:      "ephemeral",
	BundleFingerprint: "sha256:" + strings.Repeat("a", 64),
}

var authorActivityTestDifferentEventTypes = strings.Fields(`
bootstrap.requested filler.event mailbox.card_decided mailbox.review_requested platform.activity_requested review.requested
scan.followup scan.requested thing.created trace.visible triage.requested
`)

type authorActivityTestCatalogRegistrar interface {
	RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
}

func testAuthorActivityRuntimeContext(ctx context.Context) context.Context {
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.RuntimeScope(authorActivityTestRuntimeInstanceID))
}

func testAuthorActivityContext(ctx context.Context) context.Context {
	return testAuthorActivityContextForSource(ctx, authorActivityTestBundleSourceFact)
}

func testAuthorActivityContextForSource(ctx context.Context, fact runtimecorrelation.BundleSourceFact) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	fact = fact.Normalized()
	ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, authorActivityTestRuntimeInstanceID)
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, fact)
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID, fact.BundleHash,
	))
}

func testAuthorActivityRequest(req *http.Request) *http.Request {
	if req == nil {
		return nil
	}
	return req.WithContext(testAuthorActivityRuntimeContext(req.Context()))
}

func newScopedAPITestEventBus(t *testing.T, eventStore runtimebus.EventStore, options ...runtimebus.EventBusOptions) (*runtimebus.EventBus, error) {
	t.Helper()
	opts := runtimebus.EventBusOptions{}
	if len(options) > 0 {
		opts = options[0]
	}
	if strings.TrimSpace(opts.RuntimeInstanceID) == "" {
		opts.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
	}
	if strings.TrimSpace(opts.BundleSourceFact.BundleHash) == "" {
		opts.BundleSourceFact = authorActivityTestBundleSourceFact
	}
	if registrar, ok := eventStore.(authorActivityTestCatalogRegistrar); ok {
		descriptors, err := authorActivityTestDescriptors(opts.ContractBundle)
		if err != nil {
			return nil, err
		}
		lease, err := registrar.RegisterAuthorActivityEventCatalog(
			runtimeauthoractivity.BundleScope(opts.RuntimeInstanceID, opts.BundleSourceFact.BundleHash), descriptors,
		)
		if err != nil {
			return nil, err
		}
		t.Cleanup(lease.Release)
	}
	return runtimebus.NewEventBusWithOptions(eventStore, opts)
}

func registerScopedAPITestCatalog(t *testing.T, target authorActivityTestCatalogRegistrar, source semanticview.Source) {
	t.Helper()
	descriptors, err := authorActivityTestDescriptors(source)
	if err != nil {
		t.Fatalf("project API test author activity catalog: %v", err)
	}
	lease, err := target.RegisterAuthorActivityEventCatalog(
		runtimeauthoractivity.BundleScope(authorActivityTestRuntimeInstanceID, authorActivityTestBundleSourceFact.BundleHash), descriptors,
	)
	if err != nil {
		t.Fatalf("register API test author activity catalog: %v", err)
	}
	t.Cleanup(lease.Release)
}

func authorActivityTestDescriptors(source semanticview.Source) ([]runtimeauthoractivity.EventDescriptor, error) {
	descriptors, err := runtimepkg.AuthorActivityEventDescriptors(source)
	if err != nil {
		return nil, err
	}
	byName := make(map[string]runtimeauthoractivity.EventDescriptor, len(authorActivityTestDifferentEventTypes)+len(descriptors))
	for _, name := range authorActivityTestDifferentEventTypes {
		byName[name] = runtimeauthoractivity.EventDescriptor{EventType: name, Disposition: runtimeauthoractivity.StoryDifferent}
	}
	for _, descriptor := range descriptors {
		byName[descriptor.EventType] = descriptor
	}
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	descriptors = descriptors[:0]
	for _, name := range names {
		descriptors = append(descriptors, byName[name])
	}
	return descriptors, nil
}
