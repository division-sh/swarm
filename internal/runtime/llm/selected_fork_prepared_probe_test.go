package llm

import (
	"context"
	"strings"
	"testing"

	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/google/uuid"
)

func TestPreparedSelectedForkStartupSurfaceNeverProjectsLiveActor(t *testing.T) {
	name, err := agentidentity.DeclaredName("worker", "target-owner")
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
	authority := managedcapabilities.Authority{
		Kind: managedcapabilities.AuthorityStartupProbe, ID: uuid.NewString(),
		ExecutionKind: managedcapabilities.ExecutionSelectedForkPreparation, ExecutionAuthorityID: uuid.NewString(),
		Preparation: &managedcapabilities.PreparedSelectedForkProbeAuthority{
			ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "process-owner", ProcessBootID: uuid.NewString(),
			BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), SourceFingerprint: strings.Repeat("b", 64),
			AdmittedPlanFingerprint: strings.Repeat("c", 64), ConfigurationFingerprint: strings.Repeat("d", 64),
			CatalogFingerprint: strings.Repeat("e", 64), ActorPlanFingerprint: fingerprint,
		},
	}
	actor := runtimeactors.AgentConfig{ID: "worker"}
	ctx := runtimeactors.WithActor(context.Background(), actor)
	runtime := NewNoopRuntime(ClaudeCLIProviderContract())
	surface, err := ManagedCapabilitySurfaceForStartup(ctx, plan, runtime, nil, toolcapabilities.Set{}, authority)
	if err != nil {
		t.Fatal(err)
	}
	if !surface.MatchesActorPlan(plan) || !surface.ActorIdentity.IsZero() || surface.Authority.RunID != "" {
		t.Fatal("startup surface fabricated executable actor authority")
	}
	actor.Identity, err = plan.Live(uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	ctx = runtimeactors.WithActor(context.Background(), actor)
	if _, err := ManagedCapabilitySurfaceForStartup(ctx, plan, runtime, nil, toolcapabilities.Set{}, authority); err == nil {
		t.Fatal("preparation silently stripped live actor authority")
	}
}
