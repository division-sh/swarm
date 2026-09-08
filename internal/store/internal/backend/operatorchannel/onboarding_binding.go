package operatorchannel

import (
	"context"
	"database/sql"
	"fmt"

	domain "github.com/division-sh/swarm/internal/operatorchannel"
)

type OnboardingBindingRequest struct {
	ParentID              string
	PrincipalID           string
	Interface             domain.InterfaceIdentity
	ChildID               string
	BindingRevision       int64
	ParentBindingRevision int64
}

// RequireOnboardingBinding consumes the identity owner's decoders inside the
// caller's parent-locked transaction. Bound child identity is immutable; reading
// it without a child lock preserves confirmation's child-before-parent ordering.
func RequireOnboardingBinding(ctx context.Context, tx *sql.Tx, postgres bool, req OnboardingBindingRequest) error {
	d := dialectSQLite
	if postgres {
		d = dialectPostgres
	}
	binding, found, err := loadBinding(ctx, tx, d, req.Interface.Key(), true)
	if err != nil {
		return err
	}
	if !found || binding.Status != domain.BindingCurrent || binding.Revision != req.BindingRevision ||
		binding.PrincipalID != req.PrincipalID || binding.Interface.Normalized() != req.Interface.Normalized() {
		return fmt.Errorf("%w: onboarding binding changed", domain.ErrRevisionConflict)
	}
	if req.ChildID == "" {
		if req.ParentBindingRevision != binding.Revision {
			return domain.ErrRevisionConflict
		}
		return nil // An exact retained binding may be reused by reconnect without a new child.
	}
	child, found, err := loadOperationByID(ctx, tx, d, req.ChildID, false)
	if err != nil {
		return err
	}
	if !found || child.State != domain.StateBound || child.OnboardingOperationID != req.ParentID ||
		child.PrincipalID != req.PrincipalID || child.Interface.Normalized() != req.Interface.Normalized() ||
		child.BindingRevision != binding.Revision || binding.OperationID != child.OperationID ||
		child.ProviderCredential != binding.ProviderCredential || child.ExternalAccountRef != binding.ExternalAccountRef ||
		child.ConversationRef != binding.ConversationRef || child.ConversationScope != binding.ConversationScope ||
		child.AccountPresentation != binding.AccountPresentation ||
		child.ProofID != binding.ProofID || child.ProofRevision != binding.ProofRevision ||
		!child.CompletedAt.Equal(binding.UpdatedAt) {
		return fmt.Errorf("%w: onboarding child does not own the exact committed binding", domain.ErrRevisionConflict)
	}
	if req.ParentBindingRevision != 0 && req.ParentBindingRevision != binding.Revision && req.ParentBindingRevision != child.ExpectedBindingRevision {
		return fmt.Errorf("%w: parent revision is neither the child's predecessor nor its committed binding", domain.ErrRevisionConflict)
	}
	return nil
}
