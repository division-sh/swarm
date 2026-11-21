//go:build linux || darwin

package serveapp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/registration"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

func (c *telegramDeploymentSession) acknowledgeRegisteredTelegram(ctx context.Context, op channelonboarding.Operation, child operatorchannel.Operation, binding operatorchannel.Binding) (err error) {
	work, err := c.opts.Process.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, work.Done()) }()
	ctx = work.Context()
	profile, present := c.opts.Plan.OnboardingProfile()
	if !present || op.Provider != "telegram" || !op.DeploymentRegistration || child.State != operatorchannel.StateBound || binding.OperationID != child.OperationID {
		return channelonboarding.ErrInvalidRequest
	}
	admission, err := channelonboarding.ProviderCredentialAdmission(op, profile.ProviderCredential())
	if err != nil {
		return err
	}
	evidence := credentials.ValueEvidence{Key: admission.StoreKey, Seal: admission.ValueSeal}
	if binding.ProviderAuthority.Kind != operatorchannel.ProviderAuthorityCredential || binding.ProviderAuthority.Credential != evidence {
		return channelonboarding.ErrRevisionConflict
	}
	snapshot, current, err := c.opts.Credentials.ObserveValueMatchingSeal(ctx, evidence)
	if err != nil || !current {
		return errors.Join(operatorchannel.ErrCredentialStale, err)
	}
	toolID, tool, err := c.opts.Plan.ConnectorOperation("deliver")
	if err != nil {
		return err
	}
	input, err := c.opts.Plan.PrepareOperationInput("deliver", map[string]any{
		"presentation": map[string]any{"text": effects.RegisteredOperatorAcknowledgementText}, "actions": []any{}},
		map[string]any{"destination": binding.ConversationRef})
	if err != nil {
		return err
	}
	request, err := registration.ChannelWriteFingerprint(toolID, tool, input)
	if err != nil {
		return err
	}
	mutation, err := startupownership.CaptureMutationAuthority(ctx)
	if err != nil {
		return err
	}
	process := mutation.ProcessAuthority()
	parent := channelnative.TransportParent{Kind: "deployment_registration", ID: op.OperationID, Revision: op.Revision,
		BindingOperationID: child.OperationID, BindingOperationRevision: child.Revision, ProofID: binding.ProofID, ProofRevision: binding.ProofRevision}
	id := effects.RegisteredOperatorAcknowledgementID(child.OperationID, binding.Revision)
	a := effects.Authority{Kind: effects.AuthorityChannelConfirmation, ID: id, ExecutionOwner: process.BootID,
		FenceGeneration: uint64(parent.Revision), LeaseExpiresAt: time.Now().UTC().Add(5 * time.Minute), ExecutionMode: effects.ExecutionModeLive,
		ChannelConfirmation: effects.ChannelConfirmationAuthority{TransportParent: &parent, Destination: binding.ConversationRef, RegisteredRequest: string(request),
			EffectOperationID: id, OnboardingOperationID: op.OperationID, OnboardingRevision: op.Revision, BindingRevision: binding.Revision, PrincipalID: op.PrincipalID}}
	reader, ok := c.opts.Effects.(effects.OutcomeStore)
	if !ok || !a.Valid() || process.RuntimeInstanceID == "" {
		return fmt.Errorf("registered Telegram acknowledgement requires its exact journal and process authority")
	}
	if settled, err := effects.RegisteredAcknowledgementSettled(ctx, reader, id); err != nil || settled {
		return err
	}
	ctx = authoractivity.WithScope(ctx, authoractivity.RuntimeScope(process.RuntimeInstanceID))
	ctx = effects.WithExecutionMode(ctx, a.ExecutionMode)
	ctx = effects.WithAuthority(ctx, a)
	ctx = effects.WithController(ctx, effects.NewController(c.opts.Effects).WithExecutionPosture(executionposture.Live))
	executor := registration.HTTPExecutor{Client: c.opts.HTTP, Preflight: func(ctx context.Context) error {
		return c.validateRegisteredTelegramAcknowledgement(ctx, op, child.OperationID, evidence)
	}}
	_, err = executor.DeliverChannelConfirmation(ctx, toolID, tool, input, map[string]any{admission.Role: snapshot.CredentialValue()}, nil)
	return err
}

func (c *telegramDeploymentSession) validateRegisteredTelegramAcknowledgement(ctx context.Context, op channelonboarding.Operation, childID string, evidence credentials.ValueEvidence) error {
	retained, err := c.opts.Operations.GetChannelOnboarding(ctx, op.OperationID)
	if err != nil || retained.Revision != op.Revision || retained.PrincipalID != op.PrincipalID || retained.AccountReference != op.AccountReference ||
		retained.IdentityOperationID != childID || retained.FailureCode != "" || retained.LogoutCompleted {
		return errors.Join(channelonboarding.ErrRevisionConflict, err)
	}
	matches, err := c.opts.Credentials.CurrentValueMatchesSeal(ctx, evidence)
	if err != nil || !matches {
		return errors.Join(operatorchannel.ErrCredentialStale, err)
	}
	return nil
}
