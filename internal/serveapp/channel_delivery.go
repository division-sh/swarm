package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimechanneldelivery "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimeregistration "github.com/division-sh/swarm/internal/runtime/registration"
)

type serveChannelDeliveryDispatcher struct {
	store             runtimechanneldelivery.Store
	cards             decisioncard.Store
	activations       channelonboarding.Store
	manager           *runtime.RuntimeContextManager
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

func (d *serveChannelDeliveryDispatcher) dispatchInitial(ctx context.Context, candidate runtimechanneldelivery.Candidate, prepared runtimechanneldelivery.PreparedRender) error {
	if d == nil || d.store == nil || d.activations == nil || d.manager == nil || d.effects == nil || d.credentials == nil ||
		!d.posture.Valid() || strings.TrimSpace(d.runtimeInstanceID) == "" || d.now == nil {
		return fmt.Errorf("channel delivery dispatcher is incomplete")
	}
	if candidate.State != "rendered" || candidate.CurrentReceiptID != "" ||
		candidate.DeliveryID != prepared.DeliveryID || candidate.CurrentRenderID != prepared.RenderID ||
		candidate.SourceID != prepared.Frozen.SourceID || candidate.SourceKind != prepared.Frozen.SourceKind ||
		candidate.Audience != prepared.Frozen.Audience {
		return fmt.Errorf("channel delivery candidate and render are not exact-current")
	}
	selectedID, found, err := d.store.CurrentChannelDeliveryActivationID(ctx)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("channel delivery has no succeeded activation")
	}
	activations, err := d.activations.ListCurrentConnectedChannelActivations(ctx)
	if err != nil {
		return err
	}
	var selected channelonboarding.ConnectedChannelActivation
	for _, activation := range activations {
		if activation.ActivationID == selectedID {
			if selected.ActivationID != "" {
				return fmt.Errorf("channel delivery activation identity is duplicated")
			}
			selected = activation
		}
	}
	if selected.ActivationID == "" || selected.PrincipalID != candidate.Audience.PrincipalID ||
		selected.Interface.Key() != candidate.Audience.InterfaceKey ||
		selected.BindingRevision != candidate.BindingRevision ||
		selected.ConversationRef != candidate.Audience.ConversationRef {
		return fmt.Errorf("channel delivery activation contradicts selected destination")
	}
	lease, current, err := d.manager.AcquireChannelActivationPublication(selected.Coordinate.BundleHash, selected.Coordinate.ContextPublicationGeneration)
	if err != nil {
		return err
	}
	if !current {
		return fmt.Errorf("channel delivery runtime publication is unavailable")
	}
	defer lease.Release()
	var compiled channelonboarding.CompiledActivation
	for _, activation := range lease.Activations() {
		if activation.Source == channelonboarding.ActivationSourceLearned &&
			activation.OnboardingOperationID == selected.OperationID &&
			activation.ActivationRevision == selected.Revision && activation.Coordinate.Matches(selected.Coordinate) {
			if compiled.OnboardingOperationID != "" {
				return fmt.Errorf("channel delivery compiled activation is duplicated")
			}
			compiled = activation
		}
	}
	if compiled.OnboardingOperationID == "" {
		return fmt.Errorf("channel delivery compiled activation is absent")
	}
	presentation, truncated, err := runtimechanneldelivery.PresentationText(prepared.Frozen)
	if err != nil {
		return err
	}
	actions := make([]any, 0, len(prepared.Actions))
	viewFull := false
	verdicts := 0
	for _, action := range prepared.Actions {
		if action.Token == "" || action.Label == "" {
			return fmt.Errorf("channel delivery action is incomplete")
		}
		if action.Kind == "view_full" {
			viewFull = true
		}
		if action.Kind == "verdict" {
			verdicts++
		}
		actions = append(actions, map[string]any{"label": action.Label, "token": action.Token})
	}
	if viewFull != truncated || (candidate.SourceKind == "card" && verdicts != len(prepared.Frozen.Choices)) {
		return fmt.Errorf("channel delivery actions contradict frozen presentation")
	}
	_, input, err := compiled.Plan.PrepareOperation("deliver", map[string]any{
		"presentation": map[string]any{"text": presentation}, "actions": actions,
	})
	if err != nil {
		return err
	}
	toolID, tool, err := compiled.Plan.ConnectorOperation("deliver")
	if err != nil {
		return err
	}
	projection, ok := tool.CompiledResultExecution()
	if !ok {
		return fmt.Errorf("channel delivery connector has no compiled receipt projection")
	}
	credentials, err := resolveChannelDeliveryCredentials(ctx, d.credentials, compiled.Plan, selected.CredentialAdmissions, tool)
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
			RenderHash: prepared.Frozen.Hash, PrincipalID: candidate.Audience.PrincipalID,
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
