package serveapp

import "testing"

// R5.1 activates the formerly parked #2438 root-provider proof using the one
// canonical keyless standing form. The retired singleton flag is not a variant.
func TestProviderSelectedRootStandingBootBothStores(t *testing.T) {
	runProviderAliasAuthorityScenario(t, providerAliasScenario{
		name: "root-provider", rootProvider: true, alphaReceiver: "alpha-receiver",
	})
}
