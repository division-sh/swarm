package operatorchannel

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// RetainedCurrentBinding selects identity and revision evidence, not a live
// provider permit. Recovery must not require the connection it will replace.
func (s *Service) RetainedCurrentBinding(ctx context.Context, identity InterfaceIdentity) (Binding, error) {
	binding, found, err := s.selectRetainedBinding(ctx, identity)
	if err != nil {
		return Binding{}, err
	}
	if !found || binding.Status != BindingCurrent {
		return Binding{}, fmt.Errorf("%w: current retained binding %q was not found", ErrNotFound, identity.Selector)
	}
	return binding, nil
}

// BindingFence contains retained mutation evidence only. An unbound fence has
// no claimant, provider permit, confirmed binding or readiness authority.
type BindingFence struct {
	Revision        int64
	CurrentIdentity Binding
}

func (f BindingFence) CurrentBinding() (Binding, bool) {
	if f.CurrentIdentity.Status != BindingCurrent {
		return Binding{}, false
	}
	return f.CurrentIdentity, true
}

func (s *Service) RetainedBindingFence(ctx context.Context, identity InterfaceIdentity) (BindingFence, error) {
	binding, found, err := s.selectRetainedBinding(ctx, identity)
	if err != nil {
		return BindingFence{}, err
	}
	if found && binding.Status != BindingUnbound && binding.Status != BindingCurrent {
		return BindingFence{}, fmt.Errorf("%w: retained fence has non-durable binding status", ErrConflict)
	}
	fence := BindingFence{Revision: binding.Revision}
	if found && binding.Status == BindingCurrent {
		fence.CurrentIdentity = binding
	}
	return fence, nil
}

func (s *Service) selectRetainedBinding(ctx context.Context, identity InterfaceIdentity) (Binding, bool, error) {
	if ctx == nil {
		return Binding{}, false, fmt.Errorf("%w: retained binding context is required", ErrInvalidRequest)
	}
	if err := ctx.Err(); err != nil {
		return Binding{}, false, err
	}
	principal, err := s.Principal()
	if err != nil {
		return Binding{}, false, err
	}
	identity = identity.Normalized()
	if err := identity.Validate(); err != nil {
		return Binding{}, false, err
	}
	if identity.InterfaceRef != InterfaceHITLChannelV2 {
		return Binding{}, false, fmt.Errorf("%w: retained operator binding requires its supported interface", ErrInvalidRequest)
	}
	bindings, err := s.store.ListOperatorChannelBindings(ctx, principal.ID)
	if err != nil || ctx.Err() != nil {
		return Binding{}, false, errors.Join(err, context.Cause(ctx))
	}
	var selected Binding
	seen := false
	for _, binding := range bindings {
		binding, err = normalizeRetainedBinding(principal.ID, binding)
		if err != nil {
			return Binding{}, false, err
		}
		if binding.Interface != identity {
			continue
		}
		if seen {
			return Binding{}, false, fmt.Errorf("%w: duplicate retained binding for exact interface", ErrConflict)
		}
		seen, selected = true, binding
	}
	if err := ctx.Err(); err != nil {
		return Binding{}, false, context.Cause(ctx)
	}
	return selected, seen, nil
}

func normalizeRetainedBinding(principalID string, binding Binding) (Binding, error) {
	binding.Interface = binding.Interface.Normalized()
	if binding.PrincipalID != principalID || binding.Interface.Validate() != nil || binding.Interface.InterfaceRef != InterfaceHITLChannelV2 {
		return Binding{}, fmt.Errorf("%w: retained binding has contradictory principal or interface", ErrConflict)
	}
	switch binding.Status {
	case BindingCurrent:
		if err := validateRetainedCurrentBinding(binding); err != nil {
			return Binding{}, err
		}
	case BindingUnbound:
		if binding.Revision < 1 || uuid.Validate(binding.OperationID) != nil || binding.UpdatedAt.IsZero() ||
			binding.ProviderAuthority != (ProviderAuthority{}) || binding.ExternalAccountRef != "" || binding.ConversationRef != "" ||
			binding.ConversationScope != "" || binding.AccountPresentation != "" || binding.Source != "" || binding.ProofID != "" || binding.ProofRevision != 0 {
			return Binding{}, fmt.Errorf("%w: retained unbound fence carries contradictory identity or authority", ErrConflict)
		}
	case BindingStale, BindingRevoked:
	default:
		return Binding{}, fmt.Errorf("%w: retained binding has unknown status", ErrConflict)
	}
	binding.ProviderAuthority = ProviderAuthority{Kind: binding.ProviderAuthority.Kind,
		Credential: binding.ProviderAuthority.Credential, Session: binding.ProviderAuthority.Session}
	return binding, nil
}

func validateRetainedCurrentBinding(binding Binding) error {
	if binding.Revision < 1 || uuid.Validate(binding.OperationID) != nil || binding.UpdatedAt.IsZero() ||
		strings.TrimSpace(binding.ExternalAccountRef) == "" || strings.TrimSpace(binding.ConversationRef) == "" ||
		!binding.ConversationScope.Valid() || binding.ProviderAuthority.Validate() != nil {
		return fmt.Errorf("%w: retained current binding has incomplete identity or revision evidence", ErrConflict)
	}
	if binding.ProofID == "" && binding.ProofRevision != 0 ||
		binding.ProofID != "" && (uuid.Validate(binding.ProofID) != nil || binding.ProofRevision < 1) {
		return fmt.Errorf("%w: retained current binding has contradictory proof evidence", ErrConflict)
	}
	if binding.Source != BindingSourceLiveVerification && binding.Source != BindingSourceLocalProof ||
		binding.Source == BindingSourceLocalProof && binding.ProofID == "" {
		return fmt.Errorf("%w: retained current binding has contradictory identity source", ErrConflict)
	}
	return nil
}

// Missing native execution is a stale read-model fact, not absent identity.
// Cancellation and unknown provider/store errors still refuse observation.
func (s *Service) bindingReadbackCurrent(ctx context.Context, binding Binding) (bool, error) {
	current, err := binding.ProviderAuthority.Current(ctx, s.credentials)
	if ctx.Err() != nil {
		return false, errors.Join(err, ctx.Err())
	}
	if knownUnavailableSession(binding.ProviderAuthority, err) {
		return false, nil
	}
	return current, err
}

func knownUnavailableSession(provider ProviderAuthority, err error) bool {
	unavailable, known := err.(*SessionProviderUnavailableError)
	return known && provider.Kind == ProviderAuthoritySession && unavailable.Provider == provider.Session.Provider
}
