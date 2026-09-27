//go:build issue2438

package serveapp

import "testing"

// This executable acceptance proof is deferred to #2438 by #2304 ruling R3.
// It is not passing #2304 coverage: both forms currently fail before HTTP
// admission with INVALID-SINGLETON for the selected root. Run explicitly with
// go test -tags=issue2438 ./internal/serveapp -run '^TestIssue2438SelectedRootProviderBootBothStores$'.
func TestIssue2438SelectedRootProviderBootBothStores(t *testing.T) {
	for _, singleton := range []bool{false, true} {
		name := "static-standing"
		if singleton {
			name = "singleton-standing"
		}
		t.Run(name, func(t *testing.T) {
			runProviderAliasAuthorityScenario(t, providerAliasScenario{
				name: "root-provider", rootProvider: true, rootSingleton: singleton, alphaReceiver: "alpha-receiver",
			})
		})
	}
}
