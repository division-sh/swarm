package apiv1

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/google/uuid"
)

type scenarioExecutionProfileReaderStub struct {
	profile scenarioexecution.Profile
}

func (r scenarioExecutionProfileReaderStub) LoadScenarioExecutionProfile(context.Context, string) (scenarioexecution.Profile, bool, error) {
	return r.profile, true, nil
}

func TestScenarioExecutionRejectsPersistedRuntimeMismatchBeforePublication(t *testing.T) {
	fact, err := runtimecorrelation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("1", 64))
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

	t.Run("private_host_scope", func(t *testing.T) {
		ctx, err := scenarioexecution.WithPrivateHostScope(admitted, uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		ctx = runtimecorrelation.WithSourceArtifactFact(ctx, fact)
		runID := uuid.NewString()
		ctx, err = scenarioexecution.WithPrivateRunScope(ctx, runID, ".")
		if err != nil {
			t.Fatal(err)
		}
		reader := &fakeRunReadStore{headers: map[string]operatorread.RunHeader{
			runID:     {RunID: runID},
			"foreign": {RunID: "foreign", FlowPath: ".", Status: "live"},
		}}
		for _, tc := range []struct{ status, reason string }{
			{"paused", "all_runs_paused"}, {"completed", "all_runs_ended"}, {"running", "live_run_changed"},
		} {
			reader.headers[runID] = operatorread.RunHeader{RunID: runID, Status: tc.status}
			err := eventPublicationNoLiveRun(ctx, reader, runlifecycle.LiveRunScope{FlowPath: "."}, "work.requested")
			app, ok := err.(*ApplicationError)
			if !ok || app.Code != EventPublishRejectedCode {
				t.Fatalf("private %s refusal = %#v", tc.status, err)
			}
			details, _ := app.Details.(map[string]any)
			if details["reason"] != tc.reason || (tc.status == "paused" && details["run_id"] != runID) {
				t.Fatalf("private %s refusal escaped its own run: %#v", tc.status, details)
			}
		}
		_, _, _, private, err := bindPrivateScenarioPublicationScope(ctx, EventPublicationOptions{Runs: reader}, eventPublicationParams{RunID: runID}, bundleIdentityParam{}, false)
		if err != nil || private || reader.headerReads != 0 {
			t.Fatalf("later explicit publication acquired live selection: private=%v reads=%d err=%v", private, reader.headerReads, err)
		}
	})
}

func TestRunCreationSemanticIdentityIncludesScenarioSelector(t *testing.T) {
	selectorA := scenarioexecution.Selector{
		ProfileID:             "derived:first",
		ProfileDigest:         "sha256:" + strings.Repeat("1", 64),
		EffectiveSourceDigest: "sha256:" + strings.Repeat("2", 64),
	}
	selectorB := selectorA
	selectorB.ProfileID = "derived:second"
	semantic := func(selector *scenarioexecution.Selector) []byte {
		t.Helper()
		raw, err := runCreationInitialEvent(eventPublicationParams{
			EventName: "work.requested", Payload: []byte(`{"topic":"proof"}`), Emitter: "operator",
			ScenarioExecution: selector,
		})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	hash := func(initial []byte) string {
		t.Helper()
		hash, _, _, err := (durabledata.RunCreationCommand{
			RunID: "11111111-1111-1111-1111-111111111111", Actor: "operator",
			BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64),
			EventID:    "22222222-2222-2222-2222-222222222222", InitialEvent: initial,
		}).RequestHash()
		if err != nil {
			t.Fatal(err)
		}
		return hash
	}

	without := hash(semantic(nil))
	withA := hash(semantic(&selectorA))
	withB := hash(semantic(&selectorB))
	if without == withA || withA == withB || without == withB {
		t.Fatalf("run-creation hashes do not distinguish scenario selectors: none=%s a=%s b=%s", without, withA, withB)
	}
}
