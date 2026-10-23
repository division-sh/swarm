//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

type DeploymentConnectionOptions struct {
	Directory   string
	OperationID string
	Principal   operatorchannel.Principal
	Process     *worklifetime.Process
	Operations  channelonboarding.OperationStore
	Store       channelonboarding.Store
	Credentials operatorchannel.CredentialCurrentness
	Plan        packs.SatisfactionPlan
	Trigger     providertriggers.InboundAdmissionPlan
}

// OpenDeploymentConnection reuses private SDK possession and pairing under the
// invocation's process lifetime. No runtime source or executable channel exists.
func OpenDeploymentConnection(ctx context.Context, opts DeploymentConnectionOptions) (*RuntimeConnection, error) {
	if ctx == nil || ctx.Err() != nil || opts.Process == nil || opts.Operations == nil || opts.Store == nil ||
		opts.Directory == "" || opts.Principal.Validate() != nil {
		return nil, errPairingScope
	}
	op, err := opts.Operations.GetChannelOnboarding(ctx, opts.OperationID)
	if err != nil {
		return nil, err
	}
	identity, err := opts.Plan.InterfaceIdentity()
	if err != nil {
		return nil, err
	}
	if !op.DeploymentRegistration || op.OperationID != opts.OperationID || op.PrincipalID != opts.Principal.ID ||
		op.Provider != "whatsapp" || op.Posture != channelonboarding.ActivationSessionConnection ||
		op.Phase != channelonboarding.PhaseActivatingProvider && op.Phase != channelonboarding.PhaseAwaitingExternalIdentity &&
			op.Phase != channelonboarding.PhaseAwaitingOperatorConfirmation && op.Phase != channelonboarding.PhaseSucceeded ||
		op.ValidateSessionAccount() != nil || identity.Normalized() != op.Interface.Normalized() {
		return nil, errPairingScope
	}
	if err := QualifyBootstrapPlan(opts.Plan); err != nil {
		return nil, err
	}
	if opts.Trigger.Valid() && (opts.Trigger.Provider() != op.Provider || opts.Trigger.Transport() != packs.ChannelTransportSession) {
		return nil, errPairingScope
	}
	work, err := opts.Process.BeginStanding(context.WithoutCancel(ctx))
	if err != nil {
		return nil, err
	}
	owned, cancel := context.WithCancel(work.Context())
	c := &RuntimeConnection{parent: opts.Process, work: work, ctx: owned, cancel: cancel,
		closed: make(chan struct{}), lifecycle: make(chan struct{}, 1), directory: opts.Directory,
		store: opts.Store, operations: opts.Operations, operation: op, plan: opts.Plan, credentials: opts.Credentials}
	if op.SessionAccount != (operatorchannel.SessionAccountAdmission{}) {
		account := op.SessionAccount
		c.account.Store(&account)
	}
	go c.joinRetirement()
	var incoming *RuntimeIncomingOptions
	if opts.Trigger.Valid() {
		incoming = &RuntimeIncomingOptions{Trigger: opts.Trigger}
	}
	if err := c.open(incoming); err != nil {
		return c, errors.Join(err, c.Close(context.WithoutCancel(ctx)))
	}
	return c, nil
}

func (c *RuntimeConnection) VerifyDeploymentRegistration(ctx context.Context, expected channelonboarding.Operation) (bool, error) {
	op, err := c.currentOperation(ctx)
	if err != nil || !op.DeploymentRegistration || op.SessionAccount.Validate() != nil ||
		op.Revision != expected.Revision || op.AccountReference != expected.AccountReference || op.SessionAccount != expected.SessionAccount {
		return false, errors.Join(channelonboarding.ErrRevisionConflict, err)
	}
	err = c.AwaitSessionAccount(ctx)
	if err != nil {
		occurrence := c.state.currentOccurrence()
		if occurrence == nil {
			return false, err
		}
		occurrence.mu.Lock()
		revoked := occurrence.revoked != nil
		occurrence.mu.Unlock()
		if revoked && ctx.Err() == nil {
			return false, nil
		}
		return false, err
	}
	account, _, err := c.AccountPresentation(ctx)
	if account != op.AccountReference {
		return false, errors.Join(channelonboarding.ErrConflict, err)
	}
	return err == nil, err
}

func (c *RuntimeConnection) LogoutDeploymentRegistration(ctx context.Context, original, retired channelonboarding.Operation) (err error) {
	work, err := c.begin(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, work.Done()) }()
	op, err := c.operations.GetChannelOnboarding(work.Context(), original.OperationID)
	if err != nil || !c.operation.DeploymentRegistration || !original.DeploymentRegistration || !op.DeploymentRegistration ||
		op.OperationID != c.operation.OperationID || op.OperationID != retired.OperationID || op.Revision != retired.Revision ||
		op.Revision != original.Revision+1 || op.PrincipalID != c.operation.PrincipalID || op.PrincipalID != original.PrincipalID ||
		op.Phase != channelonboarding.PhaseRetired || op.LogoutCompleted || op.FailureCode != "account_logout_pending" ||
		op.SessionAccount != original.SessionAccount || op.SessionAccount != c.sessionAccount() || op.AccountReference != original.AccountReference {
		return errors.Join(channelonboarding.ErrRevisionConflict, err)
	}
	occurrence := c.state.currentOccurrence()
	if occurrence == nil || !c.state.ownsConnectedOccurrence(work.Context(), occurrence) ||
		occurrence.client.Store.Deleted || occurrence.client.Store.ID == nil ||
		occurrence.client.Store.ID.ToNonAD().String() != op.SessionAccount.AccountRef {
		return errClientOccurrenceFenced
	}
	owned, release, err := occurrence.prepareLogout(work.Context())
	if err != nil {
		return err
	}
	defer release()
	return occurrence.unlink(owned)
}
