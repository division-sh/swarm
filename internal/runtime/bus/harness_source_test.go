package bus

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestNamesOnlyPrivateInputDoesNotInventExternalProducer(t *testing.T) {
	source := loadNamesOnlyRouteSource(t, canonicalrouting.WritePublicTemplateInputRoute(t))
	routes := derivedRouteTableFixture(t, source)
	if routeFlowInputProducerIsExternal(routes.inputProducers.Resolve("operating", "opco.product_initialization_requested")) {
		t.Fatal("unconnected private input acquired external producer authority")
	}

	for _, eventType := range []string{"opco.product_initialization_requested", "operating/opco.product_initialization_requested"} {
		for _, subscriber := range routes.PubsubDeclarationDefinitionsFixture(t, "operating", eventType) {
			if subscriber.RouteSourceCode() != "subscription" {
				t.Fatalf("unconnected input invented route authority: %#v", subscriber)
			}
		}
	}
}

func TestRouteResolveSubscriberPatterns_PrivatePinKeepsOrdinarySubscription(t *testing.T) {
	source := loadNamesOnlyRouteSource(t, canonicalrouting.WritePublicTemplateInputRoute(t))
	routes := derivedRouteTableFixture(t, source)
	scope, found := source.FlowScopeByID("operating")
	if !found {
		t.Fatal("worker flow scope missing")
	}
	admission := routeClassifyAuthoredSubscription(source, subscriberNode, scope.ID, scope.InputEvents, scope.Path,
		routeFlowLocalEventSetWithInputProducers(scope, routes.inputProducers), "opco.product_initialization_requested")
	if !admission.Admitted() {
		t.Fatal(admission.Message())
	}
	inputEvent := !admission.Pattern() && source.FlowHasInputEvent(scope.ID, admission.LocalEvent())
	patterns := routeProjectAdmittedSubscriberPatterns(admission, scope.ID, scope.Path, inputEvent, routes.inputProducers)
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
