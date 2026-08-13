package apiv1

import (
	"context"
	"strings"
	"testing"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
)

type scenarioExecutionProfileReaderStub struct {
	profile scenarioexecution.Profile
}

func (r scenarioExecutionProfileReaderStub) LoadScenarioExecutionProfile(context.Context, string) (scenarioexecution.Profile, bool, error) {
	return r.profile, true, nil
}

func TestScenarioExecutionRejectsPersistedRuntimeMismatchBeforePublication(t *testing.T) {
	fact, err := runtimecorrelation.NewPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("1", 64))
	if err != nil {
		t.Fatal(err)
	}
	persistedIdentity, err := scenarioexecution.NewEffectiveSourceIdentity(fact, "sha256:"+strings.Repeat("2", 64))
	if err != nil {
		t.Fatal(err)
	}
	runtimeIdentity, err := scenarioexecution.NewEffectiveSourceIdentity(fact, "sha256:"+strings.Repeat("3", 64))
	if err != nil {
		t.Fatal(err)
	}
	profile, err := scenarioexecution.NewProfile(persistedIdentity, "derived:work/message", nil)
	if err != nil {
		t.Fatal(err)
	}
	opts := EventPublicationOptions{
		ExecutionPosture: executionposture.MockOnly, EffectiveSourceIdentity: runtimeIdentity,
		ScenarioExecutionProfiles: scenarioExecutionProfileReaderStub{profile: profile},
	}
	ctx, err := admitScenarioExecutionSelector(context.Background(), opts, "run-1", false, nil)
	if ctx == nil {
		t.Fatal("admission returned nil context")
	}
	applicationErr, ok := err.(*ApplicationError)
	if !ok || applicationErr.Code != BundleMismatchCode {
		t.Fatalf("persisted/runtime mismatch error = %#v", err)
	}
	details, _ := applicationErr.Details.(map[string]any)
	if details["cause"] != "scenario_execution_persisted_runtime_mismatch" {
		t.Fatalf("mismatch details = %#v", details)
	}

	opts.EffectiveSourceIdentity = persistedIdentity
	admitted, err := admitScenarioExecutionSelector(context.Background(), opts, "run-1", false, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, found := scenarioexecution.AdmittedProfileFromContext(admitted)
	if !found || got.Digest() != profile.Digest() {
		t.Fatalf("admitted profile found=%v digest=%q", found, got.Digest())
	}
}
