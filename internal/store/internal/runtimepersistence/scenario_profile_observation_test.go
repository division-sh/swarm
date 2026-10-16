package runtimepersistence

import (
	"bytes"
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/google/uuid"
)

func TestScenarioPhysicalProfileObservationPreservesCompleteImageBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			proveScenarioPhysicalProfileImage(t, backend.open(t).store)
		})
	}
}

func proveScenarioPhysicalProfileImage(t *testing.T, selected any) {
	t.Helper()
	ctx := testAuthorActivityContext()
	requireDefaultSourceArtifactForTest(t, ctx, selected)
	fact, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !ok {
		t.Fatal("source authority missing")
	}
	identity, err := scenarioexecution.NewEffectiveSourceIdentity(fact, "sha256:"+strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	profile, err := scenarioexecution.NewProfile(identity, "exact-physical-profile", nil)
	if err != nil {
		t.Fatal(err)
	}
	run := uuid.NewString()
	owner := selected.(interface {
		SetupScenarioEntities(context.Context, pipeline.ScenarioSetupRequest) (pipeline.ScenarioSetupResult, error)
		InspectSnapshot(context.Context, func(context.Context) error) error
	})
	if err := owner.InspectSnapshot(ctx, func(scoped context.Context) error {
		if got, err := ReadScenarioPhysicalProfileForTest(scoped, selected); err == nil || !reflect.DeepEqual(got, ScenarioPhysicalProfile{}) {
			t.Fatalf("missing profile returned evidence: %+v %v", got, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	request := pipeline.ScenarioSetupRequest{
		RunID: run, CreatedAt: time.Now().UTC(), ScenarioExecutionProfile: &profile,
		Entities: []pipeline.ScenarioSetupEntityRequest{{Alias: "subject", EntityID: uuid.NewString(), FlowInstance: "flow", EntityType: "default", CurrentState: "ready"}},
	}
	if _, err := owner.SetupScenarioEntities(ctx, request); err != nil {
		t.Fatal(err)
	}
	var retained context.Context
	if err := owner.InspectSnapshot(ctx, func(scoped context.Context) error {
		retained = scoped
		got, err := ReadScenarioPhysicalProfileForTest(scoped, selected)
		if err != nil {
			return err
		}
		requireScenarioPhysicalProfileImage(t, got, run, identity, profile)
		if count, err := CountScenarioPhysicalProfilesForTest(scoped, selected); err != nil || count != 1 {
			t.Fatalf("global profile cardinality: %d %v", count, err)
		}
		if got, err := ReadScenarioPhysicalProfileForTest(scoped, &sql.DB{}); err == nil || !reflect.DeepEqual(got, ScenarioPhysicalProfile{}) {
			t.Fatalf("foreign owner returned evidence: %+v %v", got, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, scoped := range []context.Context{retained, canceledScenarioReadContext(ctx)} {
		if got, err := ReadScenarioPhysicalProfileForTest(scoped, selected); err == nil || !reflect.DeepEqual(got, ScenarioPhysicalProfile{}) {
			t.Fatalf("expired/canceled scope returned evidence: %+v %v", got, err)
		}
	}
	request.RunID = uuid.NewString()
	request.Entities[0].EntityID = uuid.NewString()
	if _, err := owner.SetupScenarioEntities(ctx, request); err != nil {
		t.Fatal(err)
	}
	if err := owner.InspectSnapshot(ctx, func(scoped context.Context) error {
		if count, err := CountScenarioPhysicalProfilesForTest(scoped, selected); err != nil || count != 2 {
			t.Fatalf("global profile count excluded a foreign run: %d %v", count, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func requireScenarioPhysicalProfileImage(t testing.TB, got ScenarioPhysicalProfile, run string, identity scenarioexecution.EffectiveSourceIdentity, profile scenarioexecution.Profile) {
	t.Helper()
	if got.RunID != run || got.ProfileID != profile.ID() || got.ProfileDigest != profile.Digest() || got.EffectiveSourceDigest != identity.Digest() || !bytes.Equal(got.Bytes, profile.CanonicalBytes()) {
		t.Fatalf("physical profile lost identity/bytes: %+v", got)
	}
}

func canceledScenarioReadContext(ctx context.Context) context.Context {
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	return ctx
}
