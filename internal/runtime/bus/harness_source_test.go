package bus

import (
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestNamesOnlyPrivateInputDoesNotInventExternalProducer(t *testing.T) {
	source := loadNamesOnlyRouteSource(t, canonicalrouting.WritePublicTemplateInputRoute(t))
	if routeFlowInputHasExternalProducer(source, "operating", "opco.product_initialization_requested") {
		t.Fatal("unconnected private input acquired external producer authority")
	}

	routes, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	for _, eventType := range []string{"opco.product_initialization_requested", "operating/opco.product_initialization_requested"} {
		for _, subscriber := range routes.ResolveForRun(busInternalTestRunID, eventType) {
			if subscriber.RouteSourceCode() != "subscription" {
				t.Fatalf("unconnected input invented route authority: %#v", subscriber)
			}
		}
	}
}

func TestRouteResolveSubscriberPatterns_PrivatePinKeepsOrdinarySubscription(t *testing.T) {
	source := loadNamesOnlyRouteSource(t, canonicalrouting.WritePublicTemplateInputRoute(t))
	scope, ok := source.FlowScopeByID("operating")
	if !ok {
		t.Fatal("worker flow scope missing")
	}
	patterns, err := routeResolveSubscriberPatterns(
		source,
		subscriberNode,
		scope.ID,
		scope.InputEvents,
		scope.Path,
		scope.Path,
		routeFlowLocalEventSet(source, scope),
		"opco.product_initialization_requested",
	)
	if err != nil {
		t.Fatalf("resolve subscriber patterns: %v", err)
	}
	if len(patterns) == 0 {
		t.Fatal("ordinary authored subscription did not resolve")
	}
	for _, pattern := range patterns {
		if pattern.routeSource != subscriberRouteSourceSubscription {
			t.Fatalf("resolved pattern = %#v, want ordinary subscription only", pattern)
		}
	}
}

func loadNamesOnlyRouteSource(t *testing.T, root string) semanticview.Source {
	t.Helper()
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		root,
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load names-only interface artifact: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func subscriberSignature(subscribers []Subscriber) string {
	parts := make([]string, 0, len(subscribers))
	for _, subscriber := range subscribers {
		parts = append(parts, strings.Join([]string{subscriber.Recipient.LocalID(), subscriber.Recipient.Code(), subscriber.Path, subscriber.MatchPattern, subscriber.RouteSourceCode(), subscriber.LocalizedEvent}, "|"))
	}
	return strings.Join(parts, "\n")
}
