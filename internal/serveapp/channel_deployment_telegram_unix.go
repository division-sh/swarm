//go:build linux || darwin

package serveapp

import (
	"context"
	"errors"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/sessionprovider"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
)

// The HTTP adapter owns Bot API identity/polling and local registration
// retirement. It cannot acquire SDK/session authority or revoke a bot token.
type telegramDeploymentSession struct {
	opts   cliapp.ChannelRegistrationOptions
	offset int64
}

func (c *telegramDeploymentSession) QualifySessionPlan(candidate channelonboarding.Candidate) error {
	return sessionprovider.QualifyBootstrapPlan(candidate.Plan)
}

func (c *telegramDeploymentSession) BootstrapSession(_ context.Context, op channelonboarding.Operation, _ channelonboarding.Candidate) error {
	if op.Provider != "telegram" {
		return channelonboarding.ErrInvalidRequest
	}
	return nil
}

func (c *telegramDeploymentSession) ResumeSession(ctx context.Context, op channelonboarding.Operation, candidate channelonboarding.Candidate) error {
	return c.BootstrapSession(ctx, op, candidate)
}

func (c *telegramDeploymentSession) AwaitSessionBootstrap(_ context.Context, op channelonboarding.Operation) error {
	if op.Provider != "telegram" {
		return channelonboarding.ErrInvalidRequest
	}
	return nil // The pairing checkpoint performs the authenticated getMe.
}

func (c *telegramDeploymentSession) CheckpointSessionPairing(ctx context.Context, op channelonboarding.Operation) (channelonboarding.Operation, bool, error) {
	identity, err := c.telegramRead(ctx, op)
	if err != nil {
		return op, false, err
	}
	ref, err := channelonboarding.CanonicalAccount(op.Provider, identity)
	if err != nil || op.ExpectedAccount != "" && op.ExpectedAccount != ref.Account {
		return op, false, errors.Join(channelonboarding.ErrConflict, err)
	}
	next, err := c.opts.Operations.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{OperationID: op.OperationID,
		ExpectedRevision: op.Revision, Phase: channelonboarding.PhaseAwaitingExternalIdentity, AccountReference: ref.Account, Now: time.Now().UTC()})
	return next, err == nil, err
}

func (c *telegramDeploymentSession) ReadSessionPairing(context.Context, channelonboarding.Operation, operatorchannel.Principal) (channelonboarding.PairingReadback, error) {
	return channelonboarding.PairingReadback{Status: "not_started"}, nil
}

func (c *telegramDeploymentSession) ReadSessionConnection(ctx context.Context, op channelonboarding.Operation) (channelonboarding.SessionConnectionReadback, error) {
	view := channelonboarding.SessionConnectionReadback{State: "disconnected", Linked: op.AccountReference != "", AccountPresentation: op.AccountReference}
	if op.AccountReference != "" && !op.Phase.Terminal() {
		_, name, err := c.telegramIdentity(ctx, op)
		if err != nil {
			return view, err
		}
		view.AccountName = name
		view.State = "connected"
	}
	return view, nil
}

func (c *telegramDeploymentSession) CurrentValueMatchesSeal(ctx context.Context, value credentials.ValueEvidence) (bool, error) {
	return c.opts.Credentials.CurrentValueMatchesSeal(ctx, value)
}

func (c *telegramDeploymentSession) AdmitSessionAccount(_ context.Context, account operatorchannel.SessionAccountAdmission) (authority.Admission, error) {
	return authority.Admission{}, &operatorchannel.SessionProviderUnavailableError{Provider: account.Provider}
}

func (c *telegramDeploymentSession) Close(context.Context) error { return nil }

func (c *telegramDeploymentSession) PollOperatorClaim(ctx context.Context, op channelonboarding.Operation) error {
	var err error
	c.offset, err = sessionprovider.PollDeploymentClaim(ctx, c.opts.HTTP, c.opts.Operations, c.opts.Identities, c.opts.Credentials,
		c.opts.Plan, c.opts.Trigger, op, c.offset)
	var active providertriggers.PollingWebhookActive
	if errors.As(err, &active) && c.opts.CurrentWebhook != nil && c.opts.CurrentWebhook(ctx, active.URL, op.AccountReference) {
		return nil
	}
	return err
}

func (c *telegramDeploymentSession) ConsumeWebhookClaim(ctx context.Context, account channelonboarding.AccountReference, credential func(channelonboarding.CredentialAdmission) bool, admit func(providertriggers.InboundAdmissionPlan) (providertriggers.Delivery, error), current func() bool) (bool, error) {
	return sessionprovider.ConsumeDeploymentWebhookClaim(ctx, sessionprovider.DeploymentConnectionOptions{Operations: c.opts.Operations, Principal: c.opts.Principal, Plan: c.opts.Plan, Trigger: c.opts.Webhook}, c.opts.Identities, account, credential, admit, current)
}

func (c *telegramDeploymentSession) VerifyRegistration(ctx context.Context, op channelonboarding.Operation) (bool, error) {
	identity, err := c.telegramRead(ctx, op)
	if err != nil {
		return false, err
	}
	if identity != op.AccountReference {
		return false, channelonboarding.ErrConflict
	}
	if op.IdentityOperationID != "" {
		return true, c.PollOperatorClaim(ctx, op)
	}
	return true, nil
}

func (c *telegramDeploymentSession) LogoutRegistration(ctx context.Context, op channelonboarding.Operation) error {
	for _, state := range []string{"account_logout_launched", "account_logout_unlinked"} {
		var err error
		op, err = c.opts.Operations.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{OperationID: op.OperationID,
			ExpectedRevision: op.Revision, Phase: op.Phase, FailureCode: state, Now: time.Now().UTC()})
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *telegramDeploymentSession) CloseRegistration(context.Context, channelonboarding.Operation) error {
	return channelonboarding.ErrRevisionConflict
}

func (c *telegramDeploymentSession) AcknowledgeRegisteredOperator(ctx context.Context, op channelonboarding.Operation, child operatorchannel.Operation, binding operatorchannel.Binding) error {
	if c == nil {
		return channelonboarding.ErrInvalidRequest
	}
	return c.acknowledgeRegisteredTelegram(ctx, op, child, binding)
}
