package channelonboarding

import (
	"context"
	"database/sql"

	domain "github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	identityowner "github.com/division-sh/swarm/internal/store/internal/backend/operatorchannel"
)

func RegisteredConfirmationCurrent(ctx context.Context, q queryer, a effects.Authority, postgres, lock bool) (bool, error) {
	if !a.Valid() || a.Kind != effects.AuthorityChannelConfirmation || a.ChannelConfirmation.TransportParent == nil {
		return false, nil
	}
	c, parent := a.ChannelConfirmation, *a.ChannelConfirmation.TransportParent
	if lock {
		tx, ok := q.(*sql.Tx)
		if !ok {
			return false, domain.ErrInvalidRequest
		}
		if err := generationauthority.FenceMutation(ctx, tx, !postgres); err != nil {
			return false, err
		}
		if err := channeldelivery.LockPrincipalTx(ctx, tx, c.PrincipalID, postgres); err != nil {
			return false, err
		}
	}
	op, found, err := loadOperation(ctx, q, logoutDialect(postgres), parent.ID, lock, true)
	if err != nil || !found {
		return false, err
	}
	if op.Revision != parent.Revision || op.IdentityOperationID != parent.BindingOperationID ||
		op.Phase != domain.PhaseAwaitingOperatorConfirmation || !op.AccountConfirmed || op.LogoutCompleted || op.FailureCode != "" ||
		op.Provider != "whatsapp" || op.PrincipalID != c.PrincipalID || op.ValidateSessionAccount() != nil {
		return false, nil
	}
	binding, child, err := identityowner.ReadEffectBinding(ctx, q, postgres, lock, op.Interface, parent.BindingOperationID)
	if err != nil {
		return false, err
	}
	return registeredConfirmationBindingMatches(op, c, binding, child), nil
}

func registeredConfirmationBindingMatches(op domain.Operation, c effects.ChannelConfirmationAuthority, binding operatorchannel.Binding, child operatorchannel.Operation) bool {
	parent := c.TransportParent
	return binding.Status == operatorchannel.BindingCurrent && binding.Revision == c.BindingRevision &&
		binding.OperationID == parent.BindingOperationID && binding.PrincipalID == op.PrincipalID &&
		binding.ConversationRef == c.Destination && binding.ProviderAuthority.Kind == operatorchannel.ProviderAuthoritySession &&
		binding.ProviderAuthority.Scope == authority.DeploymentParent &&
		binding.ProviderAuthority.Session == op.SessionAccount && binding.ProofID == parent.ProofID && binding.ProofRevision == parent.ProofRevision &&
		child.State == operatorchannel.StateBound && child.Revision == parent.BindingOperationRevision &&
		child.OnboardingOperationID == parent.ID && child.PrincipalID == op.PrincipalID && child.Interface == op.Interface &&
		child.BindingRevision == binding.Revision && child.ProviderAuthority == binding.ProviderAuthority &&
		child.ExternalAccountRef == binding.ExternalAccountRef && child.ConversationRef == binding.ConversationRef &&
		child.ConversationScope == binding.ConversationScope && child.ProofID == binding.ProofID && child.ProofRevision == binding.ProofRevision
}
