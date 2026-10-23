//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"
	"fmt"
	"time"

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
	var incoming *RuntimeIncomingOptions
	if opts.Trigger.Valid() {
		incoming = &RuntimeIncomingOptions{Trigger: opts.Trigger}
	}
	return openOwnedConnection(context.WithoutCancel(ctx), opts.Process, RuntimeConnectionOptions{
		Directory: opts.Directory, Store: opts.Store, Credentials: opts.Credentials, Plan: opts.Plan, Incoming: incoming}, opts.Operations, op)
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

func (c *RuntimeConnection) LogoutDeploymentRegistration(ctx context.Context, expected channelonboarding.Operation) (err error) {
	work, err := c.begin(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, work.Done()) }()
	op, err := c.operations.GetChannelOnboarding(work.Context(), expected.OperationID)
	if err != nil || !c.operation.DeploymentRegistration || !expected.DeploymentRegistration || !op.DeploymentRegistration ||
		op.OperationID != c.operation.OperationID || op.OperationID != expected.OperationID || op.Revision != expected.Revision ||
		op.PrincipalID != c.operation.PrincipalID || op.PrincipalID != expected.PrincipalID ||
		op.Phase.Terminal() && op.Phase != channelonboarding.PhaseSucceeded || op.LogoutCompleted ||
		op.SessionAccount != expected.SessionAccount || op.SessionAccount != c.sessionAccount() || op.AccountReference != expected.AccountReference {
		return errors.Join(channelonboarding.ErrRevisionConflict, err)
	}
	return c.unlinkDeploymentAccount(work.Context(), op)
}

func (c *RuntimeConnection) unlinkDeploymentAccount(ctx context.Context, op channelonboarding.Operation) error {
	occurrence := c.state.currentOccurrence()
	if op.FailureCode == "account_logout_launched" {
		if occurrence == nil || !occurrence.client.Store.Deleted {
			return fmt.Errorf("%w: logout outcome is unconfirmed; no unlink was repeated", channelonboarding.ErrConflict)
		}
	} else {
		if op.FailureCode != "account_logout_pending" {
			return channelonboarding.ErrConflict
		}
		if occurrence == nil || !c.state.ownsConnectedOccurrence(ctx, occurrence) ||
			occurrence.client.Store.Deleted || occurrence.client.Store.ID == nil ||
			occurrence.client.Store.ID.ToNonAD().String() != op.SessionAccount.AccountRef {
			return errClientOccurrenceFenced
		}
		err := func() error {
			owned, release, err := occurrence.prepareLogout(ctx)
			if err != nil {
				return err
			}
			defer release()
			op, err = c.operations.AdvanceChannelOnboarding(owned, channelonboarding.AdvanceRequest{OperationID: op.OperationID,
				ExpectedRevision: op.Revision, Phase: op.Phase, FailureCode: "account_logout_launched", Now: time.Now().UTC()})
			if err != nil {
				return err
			}
			return occurrence.unlink(owned)
		}()
		if err != nil {
			return err
		}
	}
	if err := occurrence.join(context.WithoutCancel(ctx)); err != nil {
		return err
	}
	if !occurrence.client.Store.Deleted {
		return channelonboarding.ErrConflict
	}
	_, err := c.operations.AdvanceChannelOnboarding(context.WithoutCancel(ctx), channelonboarding.AdvanceRequest{OperationID: op.OperationID,
		ExpectedRevision: op.Revision, Phase: op.Phase, FailureCode: "account_logout_unlinked", Now: time.Now().UTC()})
	return err
}
