package startupownership

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
)

var ErrMutationAuthorityMissing = errors.New("runtime mutation requires its original generation owner")

type mutationAuthorityContextKey struct{}

type refusedMutationAuthority struct{ err error }

// MutationAuthority is captured from the original grant before entering the
// native transaction. Its private owner and immutable facts cannot be replaced
// by a successor selected from mutable store state.
type MutationAuthority struct {
	owner    *processCapability
	grant    *generationGrant
	process  Authority
	evidence GrantEvidence
}

// WithProcessMutationAuthority is for process composition before runtime grant
// issuance. It cannot replace an operation's existing process or widen a grant.
func WithProcessMutationAuthority(ctx context.Context, process ProcessCapability) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	owner, ok := process.(*processCapability)
	if !ok || owner == nil {
		return context.WithValue(ctx, mutationAuthorityContextKey{}, refusedMutationAuthority{ErrMutationAuthorityMissing}), ErrMutationAuthorityMissing
	}
	if HasMutationAuthority(ctx) {
		original, err := CaptureMutationAuthority(ctx)
		if err != nil {
			return ctx, err
		}
		if original.owner != owner {
			err := errors.New("process mutation cannot replace its original owner")
			return context.WithValue(ctx, mutationAuthorityContextKey{}, refusedMutationAuthority{err}), err
		}
		return ctx, nil
	}
	ctx = owner.processMutationContext(ctx)
	if _, err := owner.Evidence(); err != nil {
		return ctx, err
	}
	return ctx, nil
}

// WithRuntimeMutationAuthority binds the actual issuing owner, not copied grant
// coordinates. An already-bound operation cannot change its original grant.
func WithRuntimeMutationAuthority(ctx context.Context, grant GenerationGrant) (context.Context, error) {
	if ctx == nil {
		return nil, ErrMutationAuthorityMissing
	}
	var original *generationGrant
	switch grant := grant.(type) {
	case *liveGenerationGrant:
		if grant != nil {
			original = grant.generationGrant
		}
	case *generationGrant:
		original = grant
	}
	if original == nil || original.owner == nil {
		return context.WithValue(ctx, mutationAuthorityContextKey{}, refusedMutationAuthority{ErrMutationAuthorityMissing}), ErrMutationAuthorityMissing
	}
	if previous := ctx.Value(mutationAuthorityContextKey{}); previous != nil {
		if frozen, ok := previous.(MutationAuthority); ok && frozen.grant == original {
			return ctx, frozen.Validate()
		}
		if previous == original {
			return ctx, nil
		}
		if previous != original.owner {
			err := errors.New("runtime mutation cannot replace its original generation owner")
			return context.WithValue(ctx, mutationAuthorityContextKey{}, refusedMutationAuthority{err}), err
		}
	}
	ctx = context.WithValue(ctx, mutationAuthorityContextKey{}, original)
	if _, err := original.Evidence(); err != nil {
		return ctx, err
	}
	return ctx, nil
}

// CaptureMutationAuthority freezes one command's facts. Every native retry
// consumes this same value rather than recapturing a later grant state.
func CaptureMutationAuthority(ctx context.Context) (MutationAuthority, error) {
	if ctx == nil {
		return MutationAuthority{}, ErrMutationAuthorityMissing
	}
	if err := ctx.Err(); err != nil {
		return MutationAuthority{}, err
	}
	if refused, ok := ctx.Value(mutationAuthorityContextKey{}).(refusedMutationAuthority); ok {
		return MutationAuthority{}, refused.err
	}
	if frozen, ok := ctx.Value(mutationAuthorityContextKey{}).(MutationAuthority); ok {
		if err := frozen.Validate(); err != nil {
			return MutationAuthority{}, err
		}
		if frozen.HasRuntimeGrant() {
			if err := validateMutationContext(ctx, frozen.evidence); err != nil {
				return MutationAuthority{}, err
			}
		}
		return frozen, nil
	}
	if process, ok := ctx.Value(mutationAuthorityContextKey{}).(*processCapability); ok {
		fact, err := process.Evidence()
		if err != nil {
			return MutationAuthority{}, err
		}
		original := MutationAuthority{owner: process, process: fact}
		return original, original.Validate()
	}
	grant, ok := ctx.Value(mutationAuthorityContextKey{}).(*generationGrant)
	if !ok || grant == nil || grant.owner == nil {
		return MutationAuthority{}, ErrMutationAuthorityMissing
	}
	evidence, err := grant.Evidence()
	if err != nil {
		return MutationAuthority{}, err
	}
	process, err := grant.owner.Evidence()
	if err != nil {
		return MutationAuthority{}, err
	}
	result := MutationAuthority{owner: grant.owner, grant: grant, process: process, evidence: evidence}
	if err := result.Validate(); err != nil {
		return MutationAuthority{}, err
	}
	if err := validateMutationContext(ctx, evidence); err != nil {
		return MutationAuthority{}, err
	}
	return result, nil
}

func validateMutationContext(ctx context.Context, evidence GrantEvidence) error {
	if runtimeID, present := correlation.RuntimeInstanceIDFromContext(ctx); present && runtimeID != evidence.RuntimeInstanceID {
		return errors.New("runtime mutation context differs from its original grant runtime")
	}
	if source, present := correlation.SourceArtifactFactFromContext(ctx); present && source.BundleHash() != evidence.BundleHash {
		return errors.New("runtime mutation context differs from its original grant source")
	}
	if scope, present := authoractivity.ScopeFromContext(ctx); present {
		if scope.RuntimeInstanceID != evidence.RuntimeInstanceID ||
			(scope.Kind == authoractivity.ScopeBundle && scope.BundleHash != evidence.BundleHash) {
			return errors.New("runtime mutation scope differs from its original grant")
		}
	}
	return nil
}

func (a MutationAuthority) Validate() error {
	if a.owner == nil || (a.grant != nil && a.grant.owner != a.owner) {
		return ErrMutationAuthorityMissing
	}
	if err := a.process.Validate(); err != nil {
		return err
	}
	if a.grant != nil {
		if err := a.evidence.Validate(); err != nil {
			return err
		}
		if a.evidence.State == GrantRetired || a.process.LeaseStamp().Coordinates() != a.evidence.ProcessCoordinates() {
			return errors.New("runtime mutation original process and grant differ")
		}
	}
	return a.owner.requireLive()
}

func (a MutationAuthority) ProcessAuthority() Authority  { return a.process }
func (a MutationAuthority) GrantEvidence() GrantEvidence { return a.evidence.clone() }
func (a MutationAuthority) HasRuntimeGrant() bool        { return a.grant != nil }

func HasMutationAuthority(ctx context.Context) bool {
	return ctx != nil && ctx.Value(mutationAuthorityContextKey{}) != nil
}

func FreezeMutationAuthority(ctx context.Context) (context.Context, error) {
	original, err := CaptureMutationAuthority(ctx)
	if err != nil {
		return nil, err
	}
	return context.WithValue(ctx, mutationAuthorityContextKey{}, original), nil
}

func (g *generationGrant) BindRuntimeMutationContext(ctx context.Context) (context.Context, error) {
	return WithRuntimeMutationAuthority(ctx, g)
}

// Only the complete process capability's closed operations use this scope.
// Runtime consumers receive a generation grant, never this release-capable owner.
func (p *processCapability) processMutationContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, mutationAuthorityContextKey{}, p)
}
