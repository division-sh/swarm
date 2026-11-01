package scenarioexecution

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/google/uuid"
)

type privateHostScope struct {
	runtimeID string
}

type privateHostScopeKey struct{}
type privateRunScopeKey struct{}

// PrivateRunScope selects one scenario inside the private host that created it.
// It is not a deployment live-scope claim or a different persisted run status.
type PrivateRunScope struct {
	host                  *privateHostScope
	RunID                 string
	FlowPath              string
	ProfileID             string
	ProfileDigest         string
	EffectiveSourceDigest string
	Source                correlation.SourceArtifactFact
}

func WithPrivateHostScope(ctx context.Context, runtimeID string) (context.Context, error) {
	if _, err := uuid.Parse(runtimeID); err != nil {
		return ctx, fmt.Errorf("private scenario host requires its runtime")
	}
	return context.WithValue(ctx, privateHostScopeKey{}, &privateHostScope{runtimeID: runtimeID}), nil
}

func HasPrivateHostScope(ctx context.Context) bool {
	host, ok := ctx.Value(privateHostScopeKey{}).(*privateHostScope)
	return ok && host != nil
}

func CarryPrivateHostScope(from, to context.Context) context.Context {
	if host, ok := from.Value(privateHostScopeKey{}).(*privateHostScope); ok && host != nil {
		return context.WithValue(to, privateHostScopeKey{}, host)
	}
	return to
}

func WithPrivateRunScope(ctx context.Context, runID, flowPath string) (context.Context, error) {
	host, ok := ctx.Value(privateHostScopeKey{}).(*privateHostScope)
	profile, admitted := AdmittedProfileFromContext(ctx)
	source, supplied := correlation.SourceArtifactFactFromContext(ctx)
	if !ok || host == nil || !supplied || (admitted && !source.Matches(profile.EffectiveSourceIdentity().SourceArtifactFact())) {
		return ctx, fmt.Errorf("private scenario selection requires its host and admitted source/profile")
	}
	if parsed, err := uuid.Parse(runID); err != nil || parsed == uuid.Nil || parsed.String() != runID {
		return ctx, fmt.Errorf("private scenario selection requires its exact run")
	}
	if _, err := identity.AdmitFlowIdentity(flowPath); err != nil {
		return ctx, err
	}
	scope := PrivateRunScope{host: host, RunID: runID, FlowPath: flowPath, Source: source}
	if admitted {
		scope.ProfileID, scope.ProfileDigest, scope.EffectiveSourceDigest = profile.ID(), profile.Digest(), profile.EffectiveSourceIdentity().Digest()
	}
	ctx = correlation.WithRunID(ctx, runID)
	return context.WithValue(ctx, privateRunScopeKey{}, scope), nil
}

func HasPrivateRunScope(ctx context.Context) bool {
	return ctx.Value(privateRunScopeKey{}) != nil
}

func PrivateRunScopeFromContext(ctx context.Context) (PrivateRunScope, bool) {
	scope, ok := ctx.Value(privateRunScopeKey{}).(PrivateRunScope)
	host, present := ctx.Value(privateHostScopeKey{}).(*privateHostScope)
	profile, admitted := AdmittedProfileFromContext(ctx)
	source, supplied := correlation.SourceArtifactFactFromContext(ctx)
	if !ok || !present || host == nil || scope.host != host || !supplied || !source.Matches(scope.Source) ||
		admitted != (scope.ProfileDigest != "") || (admitted && (profile.ID() != scope.ProfileID ||
		profile.Digest() != scope.ProfileDigest || profile.EffectiveSourceIdentity().Digest() != scope.EffectiveSourceDigest)) {
		return PrivateRunScope{}, false
	}
	if runtimeID, present := correlation.RuntimeInstanceIDFromContext(ctx); present && runtimeID != host.runtimeID {
		return PrivateRunScope{}, false
	}
	return scope, true
}
