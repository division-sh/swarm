package serveapp

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/sessionprovider"
)

// Platform code receives the existing owner's held construction context, not
// the runtime manager's unrelated persistence-bearing implementation fields.
type serveSessionBootstrapRuntime func(context.Context, channelonboarding.Candidate) (context.Context, *sessionprovider.RuntimeIncomingOptions, func() error, error)

// One private platform constructor, initialized only by the compiled native
// implementation. There is no registration API or pack-selected Go callback.
var newServeSessionBootstrap = unavailableServeSessionBootstrap

func unavailableServeSessionBootstrap(serveSessionBootstrapRuntime, channelonboarding.Store,
	operatorchannel.CredentialCurrentness, string,
) (channelonboarding.SessionBootstrapOwner, error) {
	return nil, &operatorchannel.SessionProviderUnavailableError{Provider: "whatsapp"}
}

func serveSessionBootstrapRuntimeSelector(manager *runtime.RuntimeContextManager) serveSessionBootstrapRuntime {
	if manager == nil {
		return nil
	}
	return func(ctx context.Context, candidate channelonboarding.Candidate) (context.Context, *sessionprovider.RuntimeIncomingOptions, func() error, error) {
		if ctx == nil {
			return nil, nil, nil, channelonboarding.ErrInvalidRequest
		}
		if ctx.Err() != nil {
			return nil, nil, nil, context.Cause(ctx)
		}
		if err := candidate.ValidateDeclaration(); err != nil {
			return nil, nil, nil, err
		}
		use, lookup, err := manager.AcquireBundleHash(ctx, candidate.Coordinate.BundleHash)
		if err != nil || !lookup.Loaded() || use == nil {
			return nil, nil, nil, errors.Join(channelonboarding.ErrRevisionConflict, err)
		}
		if !candidate.Coordinate.MatchesContextOccurrence(use.Context.RuntimeInstanceID, use.Context.PublicationGeneration) {
			return nil, nil, nil, errors.Join(channelonboarding.ErrRevisionConflict, use.Done())
		}
		// Construction holds this use until the connection owns its real runtime
		// work. Settling the use cannot cancel the durable connection context.
		owned := correlation.WithSourceArtifactFact(context.WithoutCancel(use.WorkContext()), use.Context.SourceArtifactFact)
		declarations, err := runtime.ResolveStandingTargetDeclarations(use.Context.Source, use.Context.ProviderTriggerCatalog)
		if err != nil {
			return nil, nil, nil, errors.Join(err, use.Done())
		}
		incoming, err := serveSessionIncomingSelection(use.Context, declarations, candidate)
		if err != nil {
			return nil, nil, nil, errors.Join(err, use.Done())
		}
		if _, err := incoming.Bus.AdmitSourceArtifactFact(owned); err != nil {
			return nil, nil, nil, errors.Join(err, use.Done())
		}
		if ctx.Err() != nil {
			return nil, nil, nil, errors.Join(context.Cause(ctx), use.Done())
		}
		return owned, incoming, use.Done, nil
	}
}

func serveSessionIncomingSelection(contextDef runtime.BundleContext, declarations []runtime.StandingTargetDeclaration, candidate channelonboarding.Candidate) (*sessionprovider.RuntimeIncomingOptions, error) {
	candidates, err := serveChannelContextCandidates(contextDef, declarations)
	if err != nil {
		return nil, err
	}
	catalog, err := channelonboarding.NewCandidateCatalog(candidates)
	if err != nil {
		return nil, err
	}
	current, found := catalog.FindExactDeclaration(candidate.Provider, candidate.Interface, candidate.Coordinate, candidate.Target.Selector)
	if !found {
		return nil, channelonboarding.ErrRevisionConflict
	}
	if err := validateServeSessionCandidate(candidate, current); err != nil {
		return nil, err
	}
	if contextDef.Runtime == nil || contextDef.Runtime.Bus == nil || !contextDef.Runtime.ExecutionPosture.Valid() {
		return nil, fmt.Errorf("session incoming selection requires the owning runtime bus and posture")
	}
	var incoming *sessionprovider.RuntimeIncomingOptions
	for _, declaration := range declarations {
		if declaration.FlowPath != current.Target.FlowPath || declaration.Alias != current.Target.Alias {
			continue
		}
		for _, binding := range declaration.Ingress {
			if binding.Provider != current.Provider || !binding.AdmissionPlan.Generation().Equal(current.Target.AdmissionGeneration) {
				continue
			}
			if incoming != nil || binding.AdmissionPlan.Transport() != current.Plan.Transport() {
				return nil, channelonboarding.ErrConflict
			}
			incoming = &sessionprovider.RuntimeIncomingOptions{Alias: declaration.Alias, Trigger: binding.AdmissionPlan,
				Bus: contextDef.Runtime.Bus, Posture: contextDef.Runtime.ExecutionPosture}
		}
	}
	if incoming == nil {
		return nil, channelonboarding.ErrRevisionConflict
	}
	return incoming, nil
}

// Catalog identity is not permission to substitute a request's plan or
// behavior. Validate those fields before projecting the runtime's owners.
func validateServeSessionCandidate(candidate, current channelonboarding.Candidate) error {
	if current.Posture != channelonboarding.ActivationSessionConnection ||
		candidate.Target != current.Target || candidate.Posture != current.Posture || candidate.Ceremony != current.Ceremony ||
		candidate.ProviderCredentialRole != current.ProviderCredentialRole || candidate.SigningCredentialRole != current.SigningCredentialRole ||
		candidate.ConfirmationOperation != current.ConfirmationOperation || candidate.ConnectionHealth != current.ConnectionHealth {
		return channelonboarding.ErrRevisionConflict
	}
	generation, err := candidate.Plan.Generation()
	if err != nil {
		return err
	}
	identity, err := candidate.Plan.InterfaceIdentity()
	if err != nil {
		return err
	}
	if !generation.Equal(current.Coordinate.PlanGeneration) || identity.Normalized() != current.Interface.Normalized() ||
		candidate.Plan.Transport() != current.Plan.Transport() || candidate.Plan.Provider() != current.Provider {
		return channelonboarding.ErrRevisionConflict
	}
	return nil
}
