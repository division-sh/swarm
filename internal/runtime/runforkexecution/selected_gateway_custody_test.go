package runforkexecution

import (
	"context"
	"errors"
	"fmt"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

// The existing parent-bound execution reaches this with its actual native
// child and actor. Only the provider-turn metadata is a component fixture.
func (g *selectedChildContextProofGrant) requireGatewayCustody(ctx context.Context, actor actors.AgentConfig) error {
	evidence, err := g.GenerationGrant.Evidence()
	if err != nil {
		return err
	}
	admission, ok := managedexecution.FromContext(ctx)
	if !ok {
		return errors.New("gateway control lacks actual selected admission")
	}
	authority, ok := effects.AuthorityFromContext(ctx)
	if !ok {
		return errors.New("gateway control lacks actual selected execution")
	}
	identity, err := actor.ConcreteIdentity()
	if err != nil {
		return err
	}
	plan, err := identity.Plan()
	if err != nil {
		return err
	}
	authority.Target = effects.UsageTarget{Kind: effects.UsageTargetAgentTurn, ID: uuid.NewString(),
		RunID: identity.RunID, AgentID: identity.AgentID(), AgentIdentity: identity, SessionID: uuid.NewString(), FlowInstance: identity.FlowInstance()}
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorIdentity: identity, RuntimeMode: "task", Provider: "test", Transport: "api", ProviderContract: "selected-gateway-custody",
		Authority: managedcapabilities.Authority{Kind: managedcapabilities.AuthorityProviderTurn, ID: authority.Target.ID,
			ExecutionKind: managedcapabilities.ExecutionSelectedContractFork, ExecutionAuthorityID: authority.ID,
			RunID: identity.RunID, SessionID: authority.Target.SessionID, TurnOrdinal: 1}, CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	registry := mcp.NewTurnContextRegistry(actors.ActorFromContext)
	turnContext := actors.WithActor(effects.WithAuthority(ctx, authority), actor)
	token := registry.RegisterTurnContextWithCapabilitySurface(turnContext, time.Minute, surface)
	if token == "" {
		return errors.New("gateway control could not register its exact turn")
	}
	defer registry.UnregisterTurnContext(token)
	custody := &selectedGatewayCustody{grant: g.GenerationGrant, admission: admission,
		preparation: runfork.SelectedForkPreparation{Actors: []runfork.SelectedForkPreparedActor{{Plan: plan, Mode: actor.ExecutionMode}}}}
	hooks := runtimepkg.RuntimeMCPGatewayHooks(nil, nil, nil, nil, registry)
	type epochKey struct{}
	epoch := hooks.WithCurrentRuntimeEpoch
	hooks.WithCurrentRuntimeEpoch = func(ctx context.Context) context.Context {
		return context.WithValue(epoch(ctx), epochKey{}, true)
	}
	hooks = custody.hooks(hooks)
	turn, ok := hooks.ResolveTurnContext(token)
	if !ok {
		return errors.New("gateway rejected exact registered native-child turn")
	}
	for _, field := range []string{"run", "actor", "source", "scope", "generation", "owner", "fence", "admission", "surface", "missing_surface"} {
		candidate := turn
		switch field {
		case "run":
			candidate.RunID = uuid.NewString()
		case "actor":
			candidate.Actor.ID = "foreign-agent"
		case "source":
			candidate.HasSourceArtifactFact = false
		case "scope":
			candidate.AuthorActivityScope = authoractivity.RuntimeScope(uuid.NewString())
		case "generation":
			candidate.EffectAuthority.SelectedFork.Generation++
		case "owner":
			candidate.EffectAuthority.ExecutionOwner = "foreign-owner"
		case "fence":
			candidate.EffectAuthority.FenceGeneration++
		case "admission":
			candidate.ExecutionAdmission.ID = uuid.NewString()
		case "surface":
			changed := surface.Clone()
			changed.IntegrityHash = "changed"
			candidate.CapabilitySurface = &changed
		case "missing_surface":
			candidate.CapabilitySurface = nil
		}
		if custody.matchesTurn(candidate) {
			return fmt.Errorf("gateway admitted crossed %s", field)
		}
	}
	if _, ok := hooks.ResolveTurnContext(uuid.NewString()); ok {
		return errors.New("gateway resolved an unregistered turn")
	}
	request := managedexecution.WithAdmission(effects.WithAuthority(context.Background(), authority), admission)
	request = authoractivity.WithScope(request, turn.AuthorActivityScope)
	request = correlation.WithSourceArtifactFact(request, turn.SourceArtifactFact)
	request = context.WithValue(request, selectedChildContextProofKey{}, g)
	request, cancel := context.WithDeadline(request, time.Now().Add(time.Minute))
	defer cancel()
	bound := hooks.WithCurrentRuntimeEpoch(request)
	original, err := startupownership.CaptureMutationAuthority(bound)
	deadline, bounded := bound.Deadline()
	wantDeadline, _ := request.Deadline()
	if err != nil || original.GrantEvidence().GrantID != evidence.GrantID ||
		!bounded || deadline != wantDeadline || bound.Done() != request.Done() || bound.Value(epochKey{}) != true || bound.Value(selectedChildContextProofKey{}) != g {
		return fmt.Errorf("gateway changed exact custody, epoch hook or HTTP lifetime: %v", err)
	}
	foreign := startupownership.CarryMutationAuthority(request, g.proof.foreign)
	parent := startupownership.CarryMutationAuthority(request, g.proof.parent)
	refused, _ := startupownership.WithRuntimeMutationAuthority(request, nil)
	stale, err := startupownership.FreezeMutationAuthority(bound)
	if err != nil {
		return err
	}
	frozen := startupownership.CarryMutationAuthority(request, stale)
	canceled, stop := context.WithCancel(request)
	stop()
	for name, candidate := range map[string]context.Context{"foreign": foreign, "parent": parent, "refused": refused, "canceled": canceled} {
		if _, err := startupownership.CaptureMutationAuthority(hooks.WithCurrentRuntimeEpoch(candidate)); err == nil {
			return fmt.Errorf("gateway recovered unusable %s custody", name)
		}
	}
	if _, err := startupownership.CaptureMutationAuthority(hooks.WithCurrentRuntimeEpoch(frozen)); err != nil {
		return fmt.Errorf("gateway rejected unchanged frozen original: %w", err)
	}
	if g.stale == nil {
		return errors.New("gateway control lacks its actual pre-admission frozen original")
	}
	if _, err := startupownership.CaptureMutationAuthority(hooks.WithCurrentRuntimeEpoch(startupownership.CarryMutationAuthority(request, g.stale))); err == nil {
		return errors.New("gateway refreshed a stale frozen original")
	}
	g.proof.t.Cleanup(func() {
		if _, err := startupownership.CaptureMutationAuthority(hooks.WithCurrentRuntimeEpoch(context.WithoutCancel(request))); err == nil {
			g.proof.t.Error("retired/released selected gateway regained usable custody")
		}
	})
	registry.UnregisterTurnContext(token)
	if _, ok := hooks.ResolveTurnContext(token); ok {
		return errors.New("gateway retained unregistered turn authority")
	}
	g.gatewayChecks.Add(1)
	return nil
}
