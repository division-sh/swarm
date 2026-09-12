package runfork

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/google/uuid"
)

func preparedBindingFixture(t *testing.T) (SelectedForkPreparationBinding, managedcapabilities.Surface) {
	t.Helper()
	name, err := agentidentity.DeclaredName("worker", "target")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := agentidentity.NewPlan(name, agentidentity.RootRoute())
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := plan.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	binding := SelectedForkPreparationBinding{
		ForkRunID: uuid.NewString(),
		SelectedForkPreparation: SelectedForkPreparation{
			DeclarationPlanFingerprint: "sha256:" + strings.Repeat("e", 64),
			PreparationID:              uuid.NewString(), ProcessGeneration: 1,
			SourceRunID: uuid.NewString(), ForkEventID: uuid.NewString(),
			Coordinates: managedcapabilities.SelectedForkPreparationCoordinates{
				ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "process", ProcessBootID: uuid.NewString(),
				BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), SourceFingerprint: strings.Repeat("b", 64),
				AdmittedPlanFingerprint: strings.Repeat("c", 64), ConfigurationFingerprint: strings.Repeat("d", 64), CatalogFingerprint: strings.Repeat("e", 64),
			},
		},
	}
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorPlan: plan, RuntimeMode: "startup_probe", Provider: "claude_cli", Transport: "cli", ProviderContract: "test", CreatedAt: time.Now().UTC(),
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityStartupProbe, ID: uuid.NewString(),
			ExecutionKind: managedcapabilities.ExecutionSelectedForkPreparation, ExecutionAuthorityID: binding.PreparationID,
			Preparation: &managedcapabilities.PreparedSelectedForkProbeAuthority{SelectedForkPreparationCoordinates: binding.Coordinates, ActorPlanFingerprint: fingerprint},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	binding.Actors = []SelectedForkPreparedActor{{Plan: plan, ConfigurationRevision: strings.Repeat("f", 64), Backend: selection.BackendClaudeCLI, Mode: executionmode.Live, SurfaceID: surface.ID, SurfaceIntegrity: surface.IntegrityHash}}
	return binding, surface
}

func TestSelectedPreparationBindingClosedCensus(t *testing.T) {
	base, surface := preparedBindingFixture(t)
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := base.ValidateSurface(base.Actors[0], surface); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*SelectedForkPreparationBinding)
	}{
		{"zero_process_generation", func(b *SelectedForkPreparationBinding) { b.ProcessGeneration = 0 }},
		{"missing_preparation", func(b *SelectedForkPreparationBinding) { b.PreparationID = "" }},
		{"source_as_fork", func(b *SelectedForkPreparationBinding) { b.ForkRunID = b.SourceRunID }},
		{"missing_census", func(b *SelectedForkPreparationBinding) { b.Actors = nil }},
		{"duplicate_actor", func(b *SelectedForkPreparationBinding) { b.Actors = append(b.Actors, b.Actors[0]) }},
		{"missing_receipt", func(b *SelectedForkPreparationBinding) { b.Actors[0].SurfaceID = "" }},
		{"missing_integrity", func(b *SelectedForkPreparationBinding) { b.Actors[0].SurfaceIntegrity = "" }},
		{"unknown_backend", func(b *SelectedForkPreparationBinding) { b.Actors[0].Backend = "unknown" }},
		{"noncanonical_backend", func(b *SelectedForkPreparationBinding) { b.Actors[0].Backend += " " }},
		{"wrong_mode", func(b *SelectedForkPreparationBinding) { b.Actors[0].Mode = executionmode.Mock }},
		{"native_with_receipt", func(b *SelectedForkPreparationBinding) { b.Actors[0].Backend = selection.BackendAnthropic }},
		{"mock_with_receipt", func(b *SelectedForkPreparationBinding) {
			b.Actors[0].Backend = selection.BackendMock
			b.Actors[0].Mode = executionmode.Mock
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			binding := base
			binding.Actors = append([]SelectedForkPreparedActor(nil), base.Actors...)
			test.mutate(&binding)
			if err := binding.Validate(); err == nil {
				t.Fatal("accepted invalid binding")
			}
		})
	}
	for _, backend := range []string{selection.BackendMock, selection.BackendAnthropic, selection.BackendOpenAICompatible, selection.BackendOpenAIResponses} {
		t.Run(backend, func(t *testing.T) {
			binding := base
			actor := base.Actors[0]
			actor.Backend, actor.SurfaceID, actor.SurfaceIntegrity = backend, "", ""
			profile, err := selection.ResolveActiveBackend(backend)
			if err != nil {
				t.Fatal(err)
			}
			actor.Mode, err = selection.ExecutionModeForProfile(profile)
			if err != nil {
				t.Fatal(err)
			}
			binding.Actors = []SelectedForkPreparedActor{actor}
			if err := binding.Validate(); err != nil {
				t.Fatal(err)
			}
			if len(binding.SurfaceIDs()) != 0 {
				t.Fatal("invented receipt")
			}
		})
	}
	empty := base
	empty.Actors = []SelectedForkPreparedActor{}
	if err := empty.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestSelectedPreparationBindingRejectsCrossedReceipt(t *testing.T) {
	base, surface := preparedBindingFixture(t)
	for _, test := range []struct {
		name   string
		mutate func(*SelectedForkPreparationBinding)
	}{
		{"preparation", func(b *SelectedForkPreparationBinding) { b.PreparationID = uuid.NewString() }},
		{"process", func(b *SelectedForkPreparationBinding) { b.Coordinates.ProcessAuthorityID = uuid.NewString() }},
		{"owner", func(b *SelectedForkPreparationBinding) { b.Coordinates.ProcessOwnerID += "other" }},
		{"boot", func(b *SelectedForkPreparationBinding) { b.Coordinates.ProcessBootID = uuid.NewString() }},
		{"source", func(b *SelectedForkPreparationBinding) { b.Coordinates.SourceFingerprint = strings.Repeat("1", 64) }},
		{"plan", func(b *SelectedForkPreparationBinding) {
			b.Coordinates.AdmittedPlanFingerprint = strings.Repeat("1", 64)
		}},
		{"configuration", func(b *SelectedForkPreparationBinding) {
			b.Coordinates.ConfigurationFingerprint = strings.Repeat("1", 64)
		}},
		{"catalog", func(b *SelectedForkPreparationBinding) { b.Coordinates.CatalogFingerprint = strings.Repeat("1", 64) }},
		{"receipt", func(b *SelectedForkPreparationBinding) { b.Actors[0].SurfaceID = uuid.NewString() }},
		{"integrity", func(b *SelectedForkPreparationBinding) { b.Actors[0].SurfaceIntegrity = strings.Repeat("1", 64) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			binding := base
			binding.Actors = append([]SelectedForkPreparedActor(nil), base.Actors...)
			test.mutate(&binding)
			if err := binding.Validate(); err != nil {
				t.Fatal(err)
			}
			if err := binding.ValidateSurface(binding.Actors[0], surface); err == nil {
				t.Fatal("accepted crossed receipt")
			}
		})
	}
	if err := base.ValidateSurfaceIDs([]string{surface.ID}); err != nil {
		t.Fatal(err)
	}
	for _, ids := range [][]string{nil, {uuid.NewString()}, {surface.ID, surface.ID}} {
		if err := base.ValidateSurfaceIDs(ids); err == nil {
			t.Fatal("accepted wrong receipt set")
		}
	}
}
