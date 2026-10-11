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
	if ctx == nil {
		return Binding{}, fmt.Errorf("%w: retained binding context is required", ErrInvalidRequest)
	}
	if err := ctx.Err(); err != nil {
		return Binding{}, err
	}
	principal, err := s.Principal()
	if err != nil {
		return Binding{}, err
	}
	identity = identity.Normalized()
	if err := identity.Validate(); err != nil {
		return Binding{}, err
	}
	if identity.InterfaceRef != InterfaceHITLChannelV2 {
		return Binding{}, fmt.Errorf("%w: retained operator binding requires its supported interface", ErrInvalidRequest)
	}
	bindings, err := s.store.ListOperatorChannelBindings(ctx, principal.ID)
	if err != nil || ctx.Err() != nil {
		return Binding{}, errors.Join(err, ctx.Err())
	}
	var selected Binding
	seen := false
	for _, binding := range bindings {
		binding.Interface = binding.Interface.Normalized()
		if binding.PrincipalID != principal.ID || binding.Interface.Validate() != nil || binding.Interface.InterfaceRef != InterfaceHITLChannelV2 {
			return Binding{}, fmt.Errorf("%w: retained binding has contradictory principal or interface", ErrConflict)
		}
		if binding.Interface != identity {
			continue
		}
		if seen {
			return Binding{}, fmt.Errorf("%w: duplicate retained binding for exact interface", ErrConflict)
		}
		seen, selected = true, binding
	}
	if err := ctx.Err(); err != nil {
		return Binding{}, err
	}
	if seen {
		switch selected.Status {
		case BindingCurrent, BindingUnbound, BindingStale, BindingRevoked:
		default:
			return Binding{}, fmt.Errorf("%w: retained binding has unknown status", ErrConflict)
		}
	}
	if !seen || selected.Status != BindingCurrent {
		return Binding{}, fmt.Errorf("%w: current retained binding %q was not found", ErrNotFound, identity.Selector)
	}
	if err := validateRetainedCurrentBinding(selected); err != nil {
		return Binding{}, err
	}
	selected.ProviderAuthority = ProviderAuthority{Kind: selected.ProviderAuthority.Kind,
		Credential: selected.ProviderAuthority.Credential, Session: selected.ProviderAuthority.Session}
	return selected, nil
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
	unavailable, knownUnavailable := err.(*SessionProviderUnavailableError)
	if binding.ProviderAuthority.Kind == ProviderAuthoritySession && knownUnavailable &&
		unavailable.Provider == binding.ProviderAuthority.Session.Provider {
		return false, nil
	}
	return current, err
}
