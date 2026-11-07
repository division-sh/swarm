package serveapp

import (
	"context"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/effects"
)

func (d *serveChannelDeliveryDispatcher) requireDeploymentReceiver(ctx context.Context, selected channelonboarding.Result) (string, error) {
	if selected.Operation.Posture != channelonboarding.ActivationWebhookRegistration {
		return "", nil
	}
	if d.ingress == nil {
		return "", fmt.Errorf("deployment delivery has no physical registration owner")
	}
	registration, found := d.ingress.DeploymentRegistrationCurrent(ctx, d.now().UTC(), deploymentParent(selected))
	if !found || !registration.Current || registration.Registration.SlotID == "" {
		return "", fmt.Errorf("deployment physical registration is not exact-current")
	}
	return registration.Registration.SlotID, nil
}

func (d *serveChannelDeliveryDispatcher) processDeploymentNotice(ctx context.Context, pending render.PendingAction, resolved render.ResolvedAction) error {
	if d.mailbox == nil {
		return fmt.Errorf("deployment notice requires its canonical mailbox owner")
	}
	item, err := d.mailbox.GetV1MailboxItem(ctx, resolved.SourceID)
	if err != nil {
		return err
	}
	if item.Item.MailboxID != resolved.SourceID || item.Item.SourceRunID == "" {
		return fmt.Errorf("deployment notice has no exact originating run")
	}
	use, _, availability, err := d.manager.AcquireRun(ctx, item.Item.SourceRunID)
	if err != nil {
		return err
	}
	if use == nil || !availability.Available() || use.Runtime() == nil || use.Runtime().Bus == nil {
		if use != nil {
			_ = use.Done()
		}
		return fmt.Errorf("notice originating runtime is unavailable")
	}
	defer func() { _ = use.Done() }()
	actionCtx, err := use.Runtime().Bus.AdmitSourceArtifactFact(use.WorkContext())
	if err != nil {
		return err
	}
	req := apiidempotency.Request{Method: "mailbox.acknowledge", Actor: apiidempotency.PrincipalActor(resolved.PrincipalID),
		IdempotencyKey: pending.PublicationID, ResourceID: resolved.SourceID,
		RequestHash: operatorchannel.Hash("channel-notice-ack-v1", pending.PublicationID, pending.Fact.ProviderAuthorization,
			pending.Fact.Interface.Key(), pending.Fact.Token, resolved.RenderHash, resolved.SourceID),
		TTL: 24 * time.Hour, Now: pending.ReceivedAt.UTC()}
	_, _, err = d.store.AcknowledgeChannelNotice(actionCtx, req, pending.Fact)
	return err
}

func (d *serveChannelDeliveryDispatcher) acknowledgeDeploymentAction(ctx context.Context, operationID string, pending render.PendingAction, resolved render.ResolvedAction) error {
	selected, plan, err := d.deployment.selectCurrent(ctx, resolved.Parent)
	if err != nil {
		return err
	}
	b := selected.Binding
	if b == nil || b.PrincipalID != resolved.PrincipalID || b.Revision != resolved.BindingRevision ||
		b.Interface.Key() != pending.Fact.Interface.Key() || b.ExternalAccountRef != pending.Fact.ExternalAccountRef ||
		b.ConversationRef != pending.Fact.ConversationRef || b.ConversationScope != pending.Fact.ConversationScope ||
		pending.Fact.Kind != operatorchannel.ActionSourceCallback {
		return channelonboarding.ErrRevisionConflict
	}
	if !plan.Capabilities().Vector().Acknowledgment {
		return nil
	}
	if _, err := d.requireDeploymentReceiver(ctx, selected); err != nil {
		return err
	}
	input, err := channelAcknowledgmentInput(plan, pending.Fact.InteractionRef)
	if err != nil {
		return err
	}
	toolID, tool, err := plan.ConnectorOperation("acknowledge_interaction")
	if err != nil {
		return err
	}
	credentials, err := resolveChannelDeliveryCredentials(ctx, d.credentials, plan, selected.Operation.CredentialAdmissions, tool)
	if err != nil {
		return err
	}
	ack := effects.ChannelActionAckAuthority{Parent: resolved.Parent, EffectOperationID: operationID, PublicationID: pending.PublicationID,
		Provider: pending.Fact.Provider, ProviderEventID: pending.Fact.ProviderEventID, ProviderAuthorization: pending.Fact.ProviderAuthorization,
		InterfaceKey: pending.Fact.Interface.Key(), ExternalAccountRef: pending.Fact.ExternalAccountRef, ConversationRef: pending.Fact.ConversationRef,
		ConversationScope: string(pending.Fact.ConversationScope), MessageReference: pending.Fact.MessageReference, InteractionRef: pending.Fact.InteractionRef,
		Token: pending.Fact.Token, ReceiptOperationID: resolved.ReceiptOperationID, PrincipalID: resolved.PrincipalID, BindingRevision: resolved.BindingRevision}
	ack.PlanGeneration, err = plan.PlanGeneration()
	if err != nil {
		return err
	}
	authority := effects.Authority{Kind: effects.AuthorityChannelActionAck, ID: operationID, ExecutionOwner: "channel-action:" + d.runtimeInstanceID,
		LeaseExpiresAt: d.now().UTC().Add(5 * time.Minute), FenceGeneration: uint64(resolved.Parent.Revision),
		ExecutionMode: effects.ExecutionMode(d.posture.RootMode()), ChannelActionAck: ack}
	if !authority.Valid() {
		return fmt.Errorf("deployment acknowledgment authority is incomplete")
	}
	ctx = effects.WithExecutionMode(ctx, authority.ExecutionMode)
	ctx = effects.WithController(ctx, effects.NewController(d.effects).WithExecutionPosture(d.posture))
	ctx = effects.WithAuthority(ctx, authority)
	ctx = authoractivity.WithScope(ctx, authoractivity.RuntimeScope(d.runtimeInstanceID))
	_, err = channelCredentialHTTPExecutor(d.httpClient, d.credentials, plan, selected.Operation.CredentialAdmissions, tool).
		AcknowledgeChannelAction(ctx, toolID, tool, input, credentials, map[string]string{"publication_id": pending.PublicationID})
	return err
}

func (d *serveChannelDeliveryDispatcher) dispatchDeploymentChannel(ctx context.Context, selected channelonboarding.Result, plan packs.OutboundBindingPlan,
	candidate render.Candidate, prepared render.PreparedRender, operation string, previous any) error {
	if _, err := d.requireDeploymentReceiver(ctx, selected); err != nil {
		return err
	}
	native, err := selectServeChannelWrite(ctx, d.sessions, selected.Operation, plan)
	if err != nil {
		return err
	}
	if err := plan.RequireOperation(operation); err != nil {
		return err
	}
	semantic, err := channelDeliverySemanticInput(plan, prepared, operation, previous)
	if err != nil {
		return err
	}
	_, input, err := plan.PrepareOperation(operation, semantic)
	if err != nil {
		return err
	}
	toolID, tool, err := plan.ConnectorOperation(operation)
	if err != nil {
		return err
	}
	if _, ok := tool.CompiledResultExecution(); !ok {
		return fmt.Errorf("deployment delivery requires its compiled receipt projection")
	}
	credentials, err := resolveChannelDeliveryCredentials(ctx, d.credentials, plan, selected.Operation.CredentialAdmissions, tool)
	if err != nil {
		return err
	}
	operationID, err := effects.ChannelDeliveryOperationID(candidate.DeliveryID, prepared.RenderID)
	if err != nil {
		return err
	}
	parent := deploymentParent(selected)
	generation, err := plan.PlanGeneration()
	if err != nil {
		return err
	}
	authority := effects.Authority{Kind: effects.AuthorityChannelDelivery, ID: operationID,
		ExecutionOwner: "channel-delivery:" + d.runtimeInstanceID, LeaseExpiresAt: d.now().UTC().Add(5 * time.Minute),
		FenceGeneration: uint64(parent.Revision), ExecutionMode: effects.ExecutionMode(d.posture.RootMode()),
		ChannelDelivery: effects.ChannelDeliveryAuthority{Parent: parent, EffectOperationID: operationID,
			PlanGeneration: generation,
			DeliveryID:     candidate.DeliveryID, RenderID: prepared.RenderID, RenderHash: prepared.Frozen.Hash,
			PreviousReceiptOperationID: candidate.CurrentReceiptID, PrincipalID: candidate.Audience.PrincipalID,
			InterfaceKey: candidate.Audience.InterfaceKey, DeliveryEpoch: candidate.Audience.DeliveryEpoch,
			BindingRevision: candidate.BindingRevision, ExternalAccountRef: candidate.Audience.ExternalAccountRef,
			ConversationRef: candidate.Audience.ConversationRef}}
	if !authority.Valid() {
		return fmt.Errorf("deployment delivery authority is incomplete")
	}
	ctx = effects.WithExecutionMode(ctx, authority.ExecutionMode)
	ctx = effects.WithController(ctx, effects.NewController(d.effects).WithExecutionPosture(d.posture))
	ctx = effects.WithAuthority(ctx, authority)
	ctx = authoractivity.WithScope(ctx, authoractivity.RuntimeScope(d.runtimeInstanceID))
	_, err = executeChannelWrite(ctx, selected.Operation.OperationID, operation, toolID, tool, input, credentials,
		map[string]string{"delivery_id": candidate.DeliveryID, "render_id": prepared.RenderID}, native,
		channelCredentialHTTPExecutor(d.httpClient, d.credentials, plan, selected.Operation.CredentialAdmissions, tool))
	return err
}
