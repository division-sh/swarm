package runtime_test

import (
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"testing"
)

func TestRuntimeShutdownDeliveryFixtureClaimsThroughCanonicalAdapter(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimepkg.VerifyRuntimeShutdownDeliveryFixtureClaimsThroughCanonicalAdapterForTest(t, func(t *testing.T, artifact *sourceartifact.AdmittedSourceArtifact) runtimepkg.RuntimeLogNativeFixtureForTest {
				return nativeRuntimeLogFixture(t, backend, artifact)
			})
		})
	}
}

func TestRuntimeShutdown_ClosesAdmissionBeforeManagerDrainAndInboundIngress(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimepkg.VerifyRuntimeShutdown_ClosesAdmissionBeforeManagerDrainAndInboundIngressForTest(t, func(t *testing.T, artifact *sourceartifact.AdmittedSourceArtifact) runtimepkg.RuntimeLogNativeFixtureForTest {
				return nativeRuntimeLogFixture(t, backend, artifact)
			})
		})
	}
}

func TestRuntimeShutdownWithOptions_PropagatesConfiguredGraceToManagerDrain(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimepkg.VerifyRuntimeShutdownWithOptions_PropagatesConfiguredGraceToManagerDrainForTest(t, func(t *testing.T, artifact *sourceartifact.AdmittedSourceArtifact) runtimepkg.RuntimeLogNativeFixtureForTest {
				return nativeRuntimeLogFixture(t, backend, artifact)
			})
		})
	}
}

func TestRuntimeShutdownFanOutCommittedTurnKeepsDependenciesLive(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimepkg.VerifyRuntimeShutdownFanOutCommittedTurnKeepsDependenciesLiveForTest(t, func(t *testing.T, artifact *sourceartifact.AdmittedSourceArtifact) runtimepkg.RuntimeLogNativeFixtureForTest {
				return nativeRuntimeLogFixture(t, backend, artifact)
			})
		})
	}
}
