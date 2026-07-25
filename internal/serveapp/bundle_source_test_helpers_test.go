package serveapp

import runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"

func mustServeTestEphemeralBundleSourceFact(bundleHash string) runtimecorrelation.BundleSourceFact {
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact(bundleHash)
	if err != nil {
		panic(err)
	}
	return fact
}

func mustServeTestPersistedBundleSourceFact(bundleHash string) runtimecorrelation.BundleSourceFact {
	fact, err := runtimecorrelation.NewPersistedBundleSourceFact(bundleHash)
	if err != nil {
		panic(err)
	}
	return fact
}
