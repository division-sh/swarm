package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimechanneldelivery "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	runtimechannelnative "github.com/division-sh/swarm/internal/runtime/channelnative"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepublicingress "github.com/division-sh/swarm/internal/runtime/publicingress"
	runtimeregistration "github.com/division-sh/swarm/internal/runtime/registration"
	"github.com/google/uuid"
)

type serveChannelDeliveryDispatcher struct {
	store             runtimechanneldelivery.Store
	native            runtimechannelnative.Store
	cards             decisioncard.Store
	mailbox           apiv1.MailboxAPIStore
	proposedEffects   decisioncard.ProposedEffectStore
	activations       channelonboarding.Store
	manager           *runtime.RuntimeContextManager
	ingress           *runtimepublicingress.ReadinessOwner
	effects           runtimeeffects.Store
	credentials       *runtimecredentials.SnapshotOwner
	posture           executionposture.Posture
	runtimeInstanceID string
	httpClient        *http.Client
	now               func() time.Time
}

type channelCardActionResult struct {
	Response json.RawMessage
	Replayed bool
}

func (d *serveChannelDeliveryDispatcher) processCardAction(ctx context.Context, pending runtimechanneldelivery.PendingAction) (channelCardActionResult, error) {
	if d == nil || d.store == nil || d.cards == nil || d.manager == nil {
		return channelCardActionResult{}, fmt.Errorf("channel card action owners are unavailable")
	}
	resolved, found, err := d.store.ResolveChannelActionFact(ctx, pending.Fact.ActionFact)
	if err != nil {
		return channelCardActionResult{}, err
	}
	if !found || resolved.SourceKind != "card" {
		return channelCardActionResult{}, fmt.Errorf("channel callback has no current card receipt")
	}
	card, err := d.cards.GetDecisionCard(ctx, resolved.SourceID)
	if err != nil {
		return channelCardActionResult{}, err
	}
	prepared, err := runtimechanneldelivery.PrepareCardAction(pending, resolved, card)
	if err != nil {
		return channelCardActionResult{}, err
	}
	use, lookup, err := d.manager.AcquireBundleHash(ctx, card.BundleHash)
	if err != nil {
		return channelCardActionResult{}, err
	}
	if !lookup.Loaded() || use == nil || use.Runtime() == nil || use.Runtime().Pipeline == nil || use.Runtime().Bus == nil {
		return channelCardActionResult{}, fmt.Errorf("channel card runtime source is unavailable")
	}
	defer use.Done()
	actionCtx, err := use.Runtime().Bus.AdmitSourceArtifactFact(use.WorkContext())
	if err != nil {
		return channelCardActionResult{}, err
	}
	response, replayed, err := use.Runtime().Pipeline.CommitDecisionCardMutation(actionCtx, prepared.Request, prepared.Mutation)
	if err != nil {
		return channelCardActionResult{}, err
	}
	return channelCardActionResult{Response: response, Replayed: replayed}, nil
}

func (d *serveChannelDeliveryDispatcher) processFinalCardText(ctx context.Context, pending runtimechanneldelivery.PendingText,
	candidate runtimechanneldelivery.InputDraftCandidate, progress decisioncard.InputFieldProgress, principalID string,
	choice *runtimechanneldelivery.PendingAction) error {
	if d == nil || d.cards == nil || d.manager == nil || !progress.Complete || candidate.DraftID == "" || principalID == "" {
		return fmt.Errorf("final channel card text requires a complete current draft")
	}
	card, err := d.cards.GetDecisionCard(ctx, candidate.CardID)
	if err != nil {
		return err
	}
	if card.Status != decisioncard.StatusPending {
		return decisioncard.ErrDraftNotAuthority
	}
	fields, err := canonicaljson.Encode(progress.Fields)
	if err != nil {
		return err
	}
	occurrenceID := pending.PublicationID
	now := pending.ReceivedAt.UTC()
	if choice != nil {
		occurrenceID = choice.PublicationID
		now = choice.ReceivedAt.UTC()
	}
	publicationID, err := uuid.Parse(occurrenceID)
	if err != nil {
		return err
	}
	mutation, err := pipeline.NewDecisionCardDecision(decisioncard.DecideRequest{
		CardID: card.CardID, Verdict: candidate.Verdict, Fields: progress.Fields,
		PrincipalID: principalID, ObservedContentHash: card.CardContentHash,
		InputDraftID: candidate.DraftID, DeliveryReceiptID: candidate.ReceiptOperationID,
		DecisionEventID: uuid.NewSHA1(publicationID, []byte("card-decision")).String(), Now: now,
	}).WithChannelText(pending.Fact)
	if err != nil {
		return err
	}
	if choice != nil {
		mutation, err = mutation.WithChannelChoice(choice.Fact)
		if err != nil {
			return err
		}
	}
	request := apiidempotency.Request{
		Method: "mailbox.decide", Actor: apiidempotency.PrincipalActor(principalID),
		IdempotencyKey: occurrenceID, ResourceID: card.CardID,
		RequestHash: operatorchannel.Hash("channel-card-input-v1", occurrenceID, pending.PublicationID,
			pending.Fact.ProviderAuthorization, pending.Fact.Interface.Key(), pending.Fact.ProviderEventID,
			candidate.DraftID, card.CardContentHash, string(fields)),
		TTL: 24 * time.Hour, Now: now,
	}
	if err := mutation.ValidateRequest(request); err != nil {
		return err
	}
	use, lookup, err := d.manager.AcquireBundleHash(ctx, card.BundleHash)
	if err != nil {
		return err
	}
	if !lookup.Loaded() || use == nil || use.Runtime() == nil || use.Runtime().Pipeline == nil || use.Runtime().Bus == nil {
		return fmt.Errorf("channel card runtime source is unavailable")
	}
	defer use.Done()
	mutationCtx, err := use.Runtime().Bus.AdmitSourceArtifactFact(use.WorkContext())
	if err != nil {
		return err
	}
	_, _, err = use.Runtime().Pipeline.CommitDecisionCardMutation(mutationCtx, request, mutation)
	return err
}

func (d *serveChannelDeliveryDispatcher) processFinalCardSkip(ctx context.Context, pending runtimechanneldelivery.PendingAction,
	resolved runtimechanneldelivery.ResolvedAction, draft decisioncard.InputDraft, progress decisioncard.InputFieldProgress) error {
	if d == nil || d.cards == nil || d.manager == nil || !progress.Complete ||
		resolved.Action.Kind != "skip_input" || draft.InputDraftID != resolved.Action.DraftID {
		return fmt.Errorf("final channel skip requires a complete current draft")
	}
	card, err := d.cards.GetDecisionCard(ctx, draft.CardID)
	if err != nil {
		return err
	}
	if card.Status != decisioncard.StatusPending {
		return decisioncard.ErrDraftNotAuthority
	}
	fields, err := canonicaljson.Encode(progress.Fields)
	if err != nil {
		return err
	}
	publicationID, err := uuid.Parse(pending.PublicationID)
	if err != nil {
		return err
	}
	now := pending.ReceivedAt.UTC()
	mutation, err := pipeline.NewDecisionCardDecision(decisioncard.DecideRequest{
		CardID: card.CardID, Verdict: draft.Verdict, Fields: progress.Fields,
		PrincipalID: resolved.PrincipalID, ObservedContentHash: card.CardContentHash,
		InputDraftID: draft.InputDraftID, DeliveryReceiptID: draft.DeliveryReceiptID,
		DecisionEventID: uuid.NewSHA1(publicationID, []byte("card-decision")).String(), Now: now,
	}).WithChannelSkip(pending.Fact)
	if err != nil {
		return err
	}
	request := apiidempotency.Request{
		Method: "mailbox.decide", Actor: apiidempotency.PrincipalActor(resolved.PrincipalID),
		IdempotencyKey: pending.PublicationID, ResourceID: card.CardID,
		RequestHash: operatorchannel.Hash("channel-card-skip-v1", pending.PublicationID,
			pending.Fact.ProviderAuthorization, pending.Fact.Interface.Key(), pending.Fact.Token,
			resolved.RenderHash, draft.InputDraftID, card.CardContentHash, string(fields)),
		TTL: 24 * time.Hour, Now: now,
	}
	if err := mutation.ValidateRequest(request); err != nil {
		return err
	}
	use, lookup, err := d.manager.AcquireBundleHash(ctx, card.BundleHash)
	if err != nil {
		return err
	}
	if !lookup.Loaded() || use == nil || use.Runtime() == nil || use.Runtime().Pipeline == nil || use.Runtime().Bus == nil {
		return fmt.Errorf("channel card runtime source is unavailable")
	}
	defer use.Done()
	mutationCtx, err := use.Runtime().Bus.AdmitSourceArtifactFact(use.WorkContext())
	if err != nil {
		return err
	}
	_, _, err = use.Runtime().Pipeline.CommitDecisionCardMutation(mutationCtx, request, mutation)
	return err
}

func (d *serveChannelDeliveryDispatcher) processNoticeAction(ctx context.Context, pending runtimechanneldelivery.PendingAction,
	resolved runtimechanneldelivery.ResolvedAction) error {
	if d == nil || d.store == nil || d.activations == nil || d.manager == nil ||
		resolved.SourceKind != "notice" || resolved.Action.Kind != "acknowledge_notice" {
		return fmt.Errorf("channel notice acknowledgment owners or action are invalid")
	}
	activations, err := d.activations.ListCurrentConnectedChannelActivations(ctx)
	if err != nil {
		return err
	}
	var selected channelonboarding.ConnectedChannelActivation
	for _, activation := range activations {
		if activation.ActivationID != resolved.ActivationID {
			continue
		}
		if selected.ActivationID != "" {
			return fmt.Errorf("channel notice activation identity is duplicated")
		}
		selected = activation
	}
	if selected.ActivationID == "" || selected.PrincipalID != resolved.PrincipalID ||
		selected.Revision != resolved.ActivationRevision || selected.BindingRevision != resolved.BindingRevision ||
		selected.Interface.Key() != pending.Fact.Interface.Key() ||
		selected.ConversationRef != pending.Fact.ConversationRef {
		return fmt.Errorf("channel notice activation contradicts verified callback")
	}
	use, lookup, err := d.manager.AcquireBundleHash(ctx, selected.Coordinate.BundleHash)
	if err != nil {
		return err
	}
	if !lookup.Loaded() || use == nil || use.Runtime() == nil || use.Runtime().Bus == nil {
		return fmt.Errorf("channel notice runtime source is unavailable")
	}
	defer use.Done()
	actionCtx, err := use.Runtime().Bus.AdmitSourceArtifactFact(use.WorkContext())
	if err != nil {
		return err
	}
	req := apiidempotency.Request{
		Method: "mailbox.acknowledge", Actor: apiidempotency.PrincipalActor(resolved.PrincipalID),
		IdempotencyKey: pending.PublicationID, ResourceID: resolved.SourceID,
		RequestHash: operatorchannel.Hash("channel-notice-ack-v1", pending.PublicationID,
			pending.Fact.ProviderAuthorization, pending.Fact.Interface.Key(), pending.Fact.Token,
			resolved.RenderHash, resolved.SourceID),
		TTL: 24 * time.Hour, Now: pending.ReceivedAt.UTC(),
	}
	_, _, err = d.store.AcknowledgeChannelNotice(actionCtx, req, pending.Fact)
	return err
}

func (d *serveChannelDeliveryDispatcher) currentChannelActionActivation(ctx context.Context, pending runtimechanneldelivery.PendingAction, resolved runtimechanneldelivery.ResolvedAction) (channelonboarding.ConnectedChannelActivation, error) {
	activations, err := d.activations.ListCurrentConnectedChannelActivations(ctx)
	if err != nil {
		return channelonboarding.ConnectedChannelActivation{}, err
	}
	var selected channelonboarding.ConnectedChannelActivation
	for _, activation := range activations {
		if activation.ActivationID == resolved.ActivationID {
			if selected.ActivationID != "" {
				return channelonboarding.ConnectedChannelActivation{}, fmt.Errorf("channel callback activation identity is duplicated")
			}
			selected = activation
		}
	}
	if selected.ActivationID == "" || selected.Revision != resolved.ActivationRevision ||
		selected.BindingRevision != resolved.BindingRevision || selected.PrincipalID != resolved.PrincipalID ||
		selected.Provider != pending.Fact.Provider || selected.Interface.Key() != pending.Fact.Interface.Key() ||
		selected.ConversationRef != pending.Fact.ConversationRef {
		return channelonboarding.ConnectedChannelActivation{}, fmt.Errorf("channel callback activation contradicts verified fact")
	}
	return selected, nil
}

func (d *serveChannelDeliveryDispatcher) acknowledgeChannelAction(ctx context.Context, pending runtimechanneldelivery.PendingAction, resolved runtimechanneldelivery.ResolvedAction) error {
	if d == nil || d.activations == nil || d.manager == nil || d.effects == nil || d.credentials == nil || d.now == nil || !d.posture.Valid() {
		return fmt.Errorf("channel callback acknowledgment owners are unavailable")
	}
	operationID, err := runtimeeffects.ChannelActionAckOperationID(pending.PublicationID)
	if err != nil {
		return err
	}
	outcomes, ok := d.effects.(runtimeeffects.OutcomeStore)
	if !ok {
		return fmt.Errorf("channel callback acknowledgment outcome owner is unavailable")
	}
	if outcome, found, err := outcomes.GetExternalEffectOutcome(ctx, operationID); err != nil {
		return err
	} else if found {
		if outcome.Kind != runtimeeffects.KindChannelActionAck || outcome.AuthorityKind != runtimeeffects.AuthorityChannelActionAck || outcome.AuthorityID != operationID {
			return fmt.Errorf("channel callback acknowledgment operation identity conflicts")
		}
		return nil
	}
	selected, err := d.currentChannelActionActivation(ctx, pending, resolved)
	if err != nil {
		return err
	}
	lease, current, err := d.manager.AcquireChannelActivationPublication(selected.Coordinate.BundleHash, selected.Coordinate.ContextPublicationGeneration)
	if err != nil {
		return err
	}
	if !current {
		return fmt.Errorf("channel callback runtime publication is unavailable")
	}
	defer lease.Release()
	var compiled channelonboarding.CompiledActivation
	for _, activation := range lease.Activations() {
		if activation.Source == channelonboarding.ActivationSourceLearned &&
			activation.OnboardingOperationID == selected.OperationID && activation.ActivationRevision == selected.Revision &&
			activation.Coordinate.Matches(selected.Coordinate) {
			if compiled.OnboardingOperationID != "" {
				return fmt.Errorf("channel callback compiled activation is duplicated")
			}
			compiled = activation
		}
	}
	if compiled.OnboardingOperationID == "" {
		return fmt.Errorf("channel callback compiled activation is absent")
	}
	interaction, err := compiled.Plan.RestoreOpaqueReference("interaction_reference", pending.Fact.InteractionRef)
	if err != nil {
		return err
	}
	_, input, err := compiled.Plan.PrepareOperation("acknowledge_interaction", map[string]any{
		"interaction_reference": interaction,
	})
	if err != nil {
		return err
	}
	toolID, tool, err := compiled.Plan.ConnectorOperation("acknowledge_interaction")
	if err != nil {
		return err
	}
	credentials, err := resolveChannelDeliveryCredentials(ctx, d.credentials, compiled.Plan, selected.CredentialAdmissions, tool)
	if err != nil {
		return err
	}
	ack := runtimeeffects.ChannelActionAckAuthority{
		EffectOperationID: operationID, PublicationID: pending.PublicationID,
		Provider: pending.Fact.Provider, ProviderEventID: pending.Fact.ProviderEventID,
		ProviderAuthorization: pending.Fact.ProviderAuthorization, InterfaceKey: pending.Fact.Interface.Key(),
		ExternalAccountRef: pending.Fact.ExternalAccountRef, ConversationRef: pending.Fact.ConversationRef,
		ConversationScope: string(pending.Fact.ConversationScope), MessageReference: pending.Fact.MessageReference,
		InteractionRef: pending.Fact.InteractionRef, Token: pending.Fact.Token,
		ReceiptOperationID: resolved.ReceiptOperationID, PrincipalID: resolved.PrincipalID,
		BindingRevision: resolved.BindingRevision, ActivationID: selected.ActivationID,
		ActivationRevision: selected.Revision, BundleHash: selected.Coordinate.BundleHash,
		BundleIdentity: selected.Coordinate.BundleIdentity, PackInventoryGeneration: selected.Coordinate.PackInventoryGeneration,
		RuntimeInstanceID:            selected.Coordinate.RuntimeInstanceID,
		ContextPublicationGeneration: selected.Coordinate.ContextPublicationGeneration,
		PlanGeneration:               selected.Coordinate.PlanGeneration, TargetGeneration: selected.Coordinate.TargetGeneration,
	}
	authority := runtimeeffects.Authority{
		Kind: runtimeeffects.AuthorityChannelActionAck, ID: operationID,
		ExecutionOwner:  "channel-action:" + d.runtimeInstanceID,
		LeaseExpiresAt:  d.now().UTC().Add(5 * time.Minute),
		FenceGeneration: selected.Coordinate.ContextPublicationGeneration,
		ExecutionMode:   runtimeeffects.ExecutionMode(d.posture.RootMode()), ChannelActionAck: ack,
	}
	if !authority.Valid() {
		return fmt.Errorf("channel callback acknowledgment authority is invalid")
	}
	effectCtx := runtimeeffects.WithExecutionMode(ctx, authority.ExecutionMode)
	effectCtx = runtimeeffects.WithController(effectCtx, runtimeeffects.NewController(d.effects).WithExecutionPosture(d.posture))
	effectCtx = runtimeeffects.WithAuthority(effectCtx, authority)
	effectCtx = runtimeauthoractivity.WithScope(effectCtx, runtimeauthoractivity.BundleScope(d.runtimeInstanceID, selected.Coordinate.BundleHash))
	_, err = (runtimeregistration.HTTPExecutor{Client: d.httpClient}).AcknowledgeChannelAction(
		effectCtx, toolID, tool, input, credentials, map[string]string{"publication_id": pending.PublicationID},
	)
	return err
}

func (d *serveChannelDeliveryDispatcher) dispatchInitial(ctx context.Context, candidate runtimechanneldelivery.Candidate, prepared runtimechanneldelivery.PreparedRender) error {
	if candidate.CurrentReceiptID != "" {
		return fmt.Errorf("initial channel delivery already has a receipt")
	}
	return d.dispatchChannel(ctx, candidate, prepared, "deliver", nil)
}

func (d *serveChannelDeliveryDispatcher) dispatchEdit(ctx context.Context, candidate runtimechanneldelivery.Candidate, prepared runtimechanneldelivery.PreparedRender, previous runtimechanneldelivery.SentReceipt) error {
	if candidate.CurrentReceiptID == "" || previous.OperationID != candidate.CurrentReceiptID ||
		previous.DeliveryID != candidate.DeliveryID || previous.RenderID == prepared.RenderID || previous.DeliveryReference == nil {
		return fmt.Errorf("channel edit lacks an exact predecessor receipt")
	}
	return d.dispatchChannel(ctx, candidate, prepared, "edit", previous.DeliveryReference)
}

func (d *serveChannelDeliveryDispatcher) currentCompiledDelivery(ctx context.Context, candidate runtimechanneldelivery.Candidate) (channelonboarding.ConnectedChannelActivation, packs.OutboundBindingPlan, func(), error) {
	if d == nil || d.store == nil || d.activations == nil || d.manager == nil {
		return channelonboarding.ConnectedChannelActivation{}, packs.OutboundBindingPlan{}, nil, fmt.Errorf("channel delivery binding owners are unavailable")
	}
	selectedID := candidate.RequestActivationID
	if candidate.SourceKind != "response" {
		if selectedID != "" {
			return channelonboarding.ConnectedChannelActivation{}, packs.OutboundBindingPlan{}, nil, fmt.Errorf("non-response delivery has entry activation")
		}
		var found bool
		var err error
		selectedID, found, err = d.store.CurrentChannelDeliveryActivationID(ctx)
		if err != nil {
			return channelonboarding.ConnectedChannelActivation{}, packs.OutboundBindingPlan{}, nil, err
		}
		if !found {
			return channelonboarding.ConnectedChannelActivation{}, packs.OutboundBindingPlan{}, nil, fmt.Errorf("channel delivery has no succeeded activation")
		}
	} else if selectedID == "" {
		return channelonboarding.ConnectedChannelActivation{}, packs.OutboundBindingPlan{}, nil, fmt.Errorf("channel response has no admitted entry activation")
	}
	activations, err := d.activations.ListCurrentConnectedChannelActivations(ctx)
	if err != nil {
		return channelonboarding.ConnectedChannelActivation{}, packs.OutboundBindingPlan{}, nil, err
	}
	var selected channelonboarding.ConnectedChannelActivation
	for _, activation := range activations {
		if activation.ActivationID == selectedID {
			if selected.ActivationID != "" {
				return channelonboarding.ConnectedChannelActivation{}, packs.OutboundBindingPlan{}, nil, fmt.Errorf("channel delivery activation identity is duplicated")
			}
			selected = activation
		}
	}
	if selected.ActivationID == "" || selected.PrincipalID != candidate.Audience.PrincipalID ||
		selected.Interface.Key() != candidate.Audience.InterfaceKey ||
		selected.BindingRevision != candidate.BindingRevision ||
		selected.ConversationRef != candidate.Audience.ConversationRef {
		return channelonboarding.ConnectedChannelActivation{}, packs.OutboundBindingPlan{}, nil, fmt.Errorf("channel delivery activation contradicts selected destination")
	}
	lease, current, err := d.manager.AcquireChannelActivationPublication(selected.Coordinate.BundleHash, selected.Coordinate.ContextPublicationGeneration)
	if err != nil {
		return channelonboarding.ConnectedChannelActivation{}, packs.OutboundBindingPlan{}, nil, err
	}
	if !current {
		return channelonboarding.ConnectedChannelActivation{}, packs.OutboundBindingPlan{}, nil, fmt.Errorf("channel delivery runtime publication is unavailable")
	}
	var compiled channelonboarding.CompiledActivation
	for _, activation := range lease.Activations() {
		if activation.Source == channelonboarding.ActivationSourceLearned &&
			activation.OnboardingOperationID == selected.OperationID &&
			activation.ActivationRevision == selected.Revision && activation.Coordinate.Matches(selected.Coordinate) {
			if compiled.OnboardingOperationID != "" {
				lease.Release()
				return channelonboarding.ConnectedChannelActivation{}, packs.OutboundBindingPlan{}, nil, fmt.Errorf("channel delivery compiled activation is duplicated")
			}
			compiled = activation
		}
	}
	if compiled.OnboardingOperationID == "" {
		lease.Release()
		return channelonboarding.ConnectedChannelActivation{}, packs.OutboundBindingPlan{}, nil, fmt.Errorf("channel delivery compiled activation is absent")
	}
	return selected, compiled.Plan, lease.Release, nil
}

func (d *serveChannelDeliveryDispatcher) selectedPresentationBounds(ctx context.Context, candidate runtimechanneldelivery.Candidate) (packs.PresentationBounds, error) {
	_, plan, release, err := d.currentCompiledDelivery(ctx, candidate)
	if err != nil {
		return packs.PresentationBounds{}, err
	}
	defer release()
	return plan.PresentationBounds()
}

func validateChannelDispatch(candidate runtimechanneldelivery.Candidate, prepared runtimechanneldelivery.PreparedRender, operation string, previousReference any) error {
	if (operation != "deliver" && operation != "edit") ||
		(operation == "deliver" && (candidate.CurrentReceiptID != "" || previousReference != nil)) ||
		(operation == "edit" && (candidate.CurrentReceiptID == "" || previousReference == nil)) ||
		candidate.State != "rendered" ||
		candidate.DeliveryID != prepared.DeliveryID || candidate.CurrentRenderID != prepared.RenderID ||
		candidate.SourceID != prepared.Frozen.SourceID || candidate.SourceKind != prepared.Frozen.SourceKind ||
		candidate.Audience != prepared.Frozen.Audience {
		return fmt.Errorf("channel delivery candidate and render are not exact-current")
	}
	return nil
}

func (d *serveChannelDeliveryDispatcher) dispatchChannel(ctx context.Context, candidate runtimechanneldelivery.Candidate, prepared runtimechanneldelivery.PreparedRender, operation string, previousReference any) error {
	if d == nil || d.store == nil || d.activations == nil || d.manager == nil || d.effects == nil || d.credentials == nil ||
		!d.posture.Valid() || strings.TrimSpace(d.runtimeInstanceID) == "" || d.now == nil {
		return fmt.Errorf("channel delivery dispatcher is incomplete")
	}
	if err := validateChannelDispatch(candidate, prepared, operation, previousReference); err != nil {
		return err
	}
	selected, plan, release, err := d.currentCompiledDelivery(ctx, candidate)
	if err != nil {
		return err
	}
	defer release()
	if err := d.reconcileNativeInboxActivation(ctx, selected); err != nil {
		return fmt.Errorf("channel delivery recovery entry is unavailable: %w", err)
	}
	presentation, _, err := runtimechanneldelivery.PresentationText(prepared.Frozen)
	if err != nil {
		return err
	}
	actions := make([]any, 0, len(prepared.Actions))
	for _, action := range prepared.Actions {
		if action.Token == "" || action.Label == "" {
			return fmt.Errorf("channel delivery action is incomplete")
		}
		actions = append(actions, map[string]any{"label": action.Label, "token": action.Token})
	}
	bounds, err := plan.PresentationBounds()
	if err != nil {
		return err
	}
	if len(actions) > bounds.Actions || prepared.Frozen.ActionPage == nil || prepared.Frozen.Bounds != bounds {
		return fmt.Errorf("channel delivery actions exceed selected compiled capacity")
	}
	semanticInput := map[string]any{
		"presentation": map[string]any{"text": presentation}, "actions": actions,
	}
	if operation == "edit" {
		semanticInput["delivery_reference"] = previousReference
	}
	_, input, err := plan.PrepareOperation(operation, semanticInput)
	if err != nil {
		return err
	}
	toolID, tool, err := plan.ConnectorOperation(operation)
	if err != nil {
		return err
	}
	projection, ok := tool.CompiledResultExecution()
	if !ok {
		return fmt.Errorf("channel delivery connector has no compiled receipt projection")
	}
	credentials, err := resolveChannelDeliveryCredentials(ctx, d.credentials, plan, selected.CredentialAdmissions, tool)
	if err != nil {
		return err
	}
	operationID, err := runtimeeffects.ChannelDeliveryOperationID(candidate.DeliveryID, prepared.RenderID)
	if err != nil {
		return err
	}
	now := d.now().UTC()
	authority := runtimeeffects.Authority{
		Kind: runtimeeffects.AuthorityChannelDelivery, ID: operationID,
		ExecutionOwner: "channel-delivery:" + d.runtimeInstanceID,
		LeaseExpiresAt: now.Add(5 * time.Minute), FenceGeneration: selected.Coordinate.ContextPublicationGeneration,
		ExecutionMode: runtimeeffects.ExecutionMode(d.posture.RootMode()),
		ChannelDelivery: runtimeeffects.ChannelDeliveryAuthority{
			EffectOperationID: operationID, DeliveryID: candidate.DeliveryID, RenderID: prepared.RenderID,
			RenderHash: prepared.Frozen.Hash, PreviousReceiptOperationID: candidate.CurrentReceiptID,
			PrincipalID:  candidate.Audience.PrincipalID,
			InterfaceKey: candidate.Audience.InterfaceKey, DeliveryEpoch: candidate.Audience.DeliveryEpoch,
			BindingRevision: candidate.BindingRevision, ExternalAccountRef: candidate.Audience.ExternalAccountRef,
			ConversationRef: candidate.Audience.ConversationRef,
			ActivationID:    selected.ActivationID, ActivationRevision: selected.Revision,
			BundleHash: selected.Coordinate.BundleHash, BundleIdentity: selected.Coordinate.BundleIdentity,
			PackInventoryGeneration:      selected.Coordinate.PackInventoryGeneration,
			RuntimeInstanceID:            selected.Coordinate.RuntimeInstanceID,
			ContextPublicationGeneration: selected.Coordinate.ContextPublicationGeneration,
			PlanGeneration:               selected.Coordinate.PlanGeneration, TargetGeneration: selected.Coordinate.TargetGeneration,
		},
	}
	if !authority.Valid() {
		return fmt.Errorf("channel delivery authority is invalid")
	}
	effectCtx := runtimeeffects.WithExecutionMode(ctx, authority.ExecutionMode)
	effectCtx = runtimeeffects.WithController(effectCtx, runtimeeffects.NewController(d.effects).WithExecutionPosture(d.posture))
	effectCtx = runtimeeffects.WithAuthority(effectCtx, authority)
	effectCtx = runtimeauthoractivity.WithScope(effectCtx, runtimeauthoractivity.BundleScope(d.runtimeInstanceID, selected.Coordinate.BundleHash))
	_, err = (runtimeregistration.HTTPExecutor{Client: d.httpClient}).DeliverChannelMessage(
		effectCtx, toolID, tool, input, credentials,
		map[string]string{"delivery_id": candidate.DeliveryID, "render_id": prepared.RenderID}, projection.Project,
	)
	return err
}

func resolveChannelDeliveryCredentials(ctx context.Context, owner *runtimecredentials.SnapshotOwner, plan packs.OutboundBindingPlan, admissions []channelonboarding.CredentialAdmission, tool runtimecontracts.ToolSchemaEntry) (map[string]any, error) {
	if owner == nil {
		return nil, fmt.Errorf("channel delivery credential owner is unavailable")
	}
	keys := plan.CredentialStoreKeys()
	byRole := make(map[string]channelonboarding.CredentialAdmission, len(admissions))
	for _, admission := range admissions {
		if err := admission.Validate(); err != nil {
			return nil, err
		}
		if _, duplicate := byRole[admission.Role]; duplicate {
			return nil, fmt.Errorf("duplicate channel delivery credential role %q", admission.Role)
		}
		byRole[admission.Role] = admission
	}
	credentials := make(map[string]any, len(tool.Credentials()))
	for _, logical := range tool.Credentials() {
		admission, admitted := byRole[logical]
		if !admitted || admission.StoreKey == "" || admission.StoreKey != keys[logical] {
			return nil, fmt.Errorf("channel delivery credential role %q is not admitted", logical)
		}
		observed, current, err := owner.ObserveValueMatchingSeal(ctx, runtimecredentials.ValueEvidence{Key: admission.StoreKey, Seal: admission.ValueSeal})
		if err != nil {
			return nil, err
		}
		if !current || !observed.Present || strings.TrimSpace(observed.CredentialValue()) == "" {
			return nil, fmt.Errorf("channel delivery credential role %q is unavailable", logical)
		}
		credentials[logical] = observed.CredentialValue()
	}
	return credentials, nil
}
