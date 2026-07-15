package bus

import (
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestRouteFlowInputHarnessSourceSuppressesProducerFallbackWithoutAddingRoute(t *testing.T) {
	harness := loadHarnessRouteSource(t, canonicalrouting.ExampleRoot(t, canonicalrouting.HarnessInjection))
	withoutSource := loadHarnessRouteSource(t, canonicalrouting.CopyHarnessInjectionWithoutSource(t))
	if !routeFlowInputHasExternalProducer(harness, "worker", "work.requested") {
		t.Fatal("harness input did not suppress the local-producer fallback")
	}

	harnessRoutes, err := DeriveRouteTable(harness)
	if err != nil {
		t.Fatalf("DeriveRouteTable(harness): %v", err)
	}
	plainRoutes, err := DeriveRouteTable(withoutSource)
	if err != nil {
		t.Fatalf("DeriveRouteTable(without source): %v", err)
	}
	for _, eventType := range []string{"work.requested", "worker/work.requested"} {
		got := harnessRoutes.Resolve(eventType)
		want := plainRoutes.Resolve(eventType)
		if subscriberSignature(got) != subscriberSignature(want) {
			t.Fatalf("routes for %s changed by harness source: got %#v want %#v", eventType, got, want)
		}
		for _, subscriber := range got {
			if subscriber.RouteSource != "subscription" {
				t.Fatalf("harness-created route authority survived: %#v", subscriber)
			}
		}
	}
}

func TestRouteResolveSubscriberPatterns_HarnessAddsNoProducerPattern(t *testing.T) {
	source := loadHarnessRouteSource(t, canonicalrouting.ExampleRoot(t, canonicalrouting.HarnessInjection))
	scope, ok := source.FlowScopeByID("worker")
	if !ok {
		t.Fatal("worker flow scope missing")
	}
	patterns := routeResolveSubscriberPatterns(
		source,
		scope.PackageKey,
		scope.ID,
		scope.InputEvents,
		scope.Path,
		routeFlowLocalEventSet(source, scope),
		"work.requested",
	)
	if len(patterns) == 0 {
		t.Fatal("ordinary authored subscription did not resolve")
	}
	for _, pattern := range patterns {
		if pattern.RouteSource != "subscription" {
			t.Fatalf("resolved pattern = %#v, want ordinary subscription only", pattern)
		}
	}
}

func loadHarnessRouteSource(t *testing.T, root string) semanticview.Source {
	t.Helper()
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		root,
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load harness injection artifact: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func subscriberSignature(subscribers []Subscriber) string {
	parts := make([]string, 0, len(subscribers))
	for _, subscriber := range subscribers {
		parts = append(parts, strings.Join([]string{subscriber.ID, subscriber.Type, subscriber.Path, subscriber.MatchPattern, subscriber.RouteSource, subscriber.LocalizedEvent}, "|"))
	}
	return strings.Join(parts, "\n")
}
