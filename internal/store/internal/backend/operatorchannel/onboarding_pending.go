package operatorchannel

import (
	"context"
	"database/sql"
	"fmt"
	"slices"

	domain "github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/credentials"
)

type StaleOnboardingChildRequest struct {
	ParentID                string
	PrincipalID             string
	Interface               domain.InterfaceIdentity
	ChildID                 string
	RetainedBindingRevision int64
	AdmittedCredentials     []credentials.ValueEvidence
}

// The caller holds the parent lock. Terminal stale children are immutable;
// avoid a child lock that would invert confirmation's child-before-parent order.
func RequireStaleOnboardingChild(ctx context.Context, tx *sql.Tx, postgres bool, req StaleOnboardingChildRequest) error {
	d := dialectSQLite
	if postgres {
		d = dialectPostgres
	}
	child, found, err := loadOperationByID(ctx, tx, d, req.ChildID, false)
	if err != nil {
		return err
	}
	if !found || child.State != domain.StateCredentialStale || child.OnboardingOperationID != req.ParentID ||
		child.PrincipalID != req.PrincipalID || child.Interface.Normalized() != req.Interface.Normalized() ||
		child.BindingRevision != 0 || child.ProofID != "" ||
		!slices.Contains(req.AdmittedCredentials, child.ProviderCredential) {
		return fmt.Errorf("%w: pending reset requires the exact settled stale child and admitted evidence", domain.ErrRevisionConflict)
	}
	if req.RetainedBindingRevision == 0 {
		return nil
	}
	binding, found, err := loadBinding(ctx, tx, d, req.Interface.Key(), true)
	if err != nil {
		return err
	}
	if child.Kind != domain.OperationReconnect || child.ExpectedBindingRevision != req.RetainedBindingRevision ||
		!found || binding.Status != domain.BindingCurrent || binding.Revision != req.RetainedBindingRevision ||
		binding.PrincipalID != req.PrincipalID || binding.Interface.Normalized() != req.Interface.Normalized() {
		return fmt.Errorf("%w: stale reconnect no longer owns the retained binding obligation", domain.ErrRevisionConflict)
	}
	return nil
}
