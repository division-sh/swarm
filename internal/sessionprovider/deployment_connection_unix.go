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
	work, err := opts.Process.BeginStanding(ctx)
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
