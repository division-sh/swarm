package serveapp

import (
	"context"
	"testing"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func requireServeTestSourceArtifactFact(t testing.TB, selected storetest.DurableDataCatalogStore) runtimecorrelation.SourceArtifactFact {
	t.Helper()
	return sourceartifactfixture.Require(t, context.Background(), selected)
}

func mustServeTestEphemeralSourceArtifactFact(bundleHash string) runtimecorrelation.SourceArtifactFact {
	fact, err := runtimecorrelation.NewSourceArtifactFact(bundleHash)
	if err != nil {
		panic(err)
	}
	return fact
}

func mustServeTestPersistedSourceArtifactFact(bundleHash string) runtimecorrelation.SourceArtifactFact {
	fact, err := runtimecorrelation.NewSourceArtifactFact(bundleHash)
	if err != nil {
		panic(err)
	}
	return fact
}
