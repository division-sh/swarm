package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimechanneldelivery "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeregistration "github.com/division-sh/swarm/internal/runtime/registration"
)

func (d *serveChannelDeliveryDispatcher) reconcileNativeInboxSettings(ctx context.Context) error {
	if d == nil || d.native == nil || d.activations == nil || d.manager == nil || d.ingress == nil ||
		d.effects == nil || d.credentials == nil || !d.posture.Valid() || d.runtimeInstanceID == "" || d.now == nil {
		return fmt.Errorf("native inbox setting owners are unavailable")
	}
	if err := d.native.RetireStaleNativeInboxConsumers(ctx); err != nil {
		return err
	}
	activations, err := d.activations.ListCurrentConnectedChannelActivations(ctx)
	if err != nil {
		return err
	}
	for _, activation := range activations {
		if err := d.reconcileNativeInboxActivation(ctx, activation); err != nil {
			return fmt.Errorf("native inbox activation %s: %w", activation.ActivationID, err)
		}
	}
	return nil
}

func (d *serveChannelDeliveryDispatcher) resolveNativeInboxEntry(ctx context.Context, text operatorchannel.InboundText) (runtimechanneldelivery.ResolvedNativeEntry, bool, error) {
	if d == nil || d.store == nil || d.activations == nil || d.manager == nil || d.ingress == nil || d.credentials == nil || d.now == nil {
		return runtimechanneldelivery.ResolvedNativeEntry{}, false, fmt.Errorf("native inbox entry owners are unavailable")
	}
	entry, found, err := d.store.ResolveCurrentNativeInboxEntry(ctx, text)
	if err != nil || !found {
		return runtimechanneldelivery.ResolvedNativeEntry{}, found, err
	}
	activations, err := d.activations.ListCurrentConnectedChannelActivations(ctx)
	if err != nil {
		return runtimechanneldelivery.ResolvedNativeEntry{}, false, err
	}
	var selected channelonboarding.ConnectedChannelActivation
	for _, activation := range activations {
		if activation.ActivationID != entry.ActivationID {
			continue
		}
		if selected.ActivationID != "" {
			return runtimechanneldelivery.ResolvedNativeEntry{}, false, fmt.Errorf("native inbox activation is duplicated")
		}
		selected = activation
	}
	if selected.ActivationID == "" || selected.PrincipalID != entry.PrincipalID ||
		selected.Interface.Key() != entry.InterfaceKey || selected.BindingRevision != entry.BindingRevision ||
		selected.Provider != text.Provider || selected.ConversationRef != text.ConversationRef {
		return runtimechanneldelivery.ResolvedNativeEntry{}, false, nil
	}
	registration, current := d.ingress.ChannelRegistrationCurrent(ctx, d.now().UTC(),
		channelonboarding.LearnedBindingID(selected.SlotKey), selected.TargetSelector, selected.Provider)
	if !current || !registration.Current || registration.Registration.SlotID != entry.ResourceSlotID {
		return runtimechanneldelivery.ResolvedNativeEntry{}, false, nil
	}
	lease, current, err := d.manager.AcquireChannelActivationPublication(selected.Coordinate.BundleHash, selected.Coordinate.ContextPublicationGeneration)
	if err != nil || !current {
		return runtimechanneldelivery.ResolvedNativeEntry{}, false, err
	}
	defer lease.Release()
	var plan packs.OutboundBindingPlan
	matched := false
	for _, compiled := range lease.Activations() {
		if compiled.Source != channelonboarding.ActivationSourceLearned ||
			compiled.OnboardingOperationID != selected.OperationID || compiled.ActivationRevision != selected.Revision ||
			!compiled.Coordinate.Matches(selected.Coordinate) {
			continue
		}
		if matched {
			return runtimechanneldelivery.ResolvedNativeEntry{}, false, fmt.Errorf("native inbox compiled activation is duplicated")
		}
		plan, matched = compiled.Plan, true
	}
	if !matched {
		return runtimechanneldelivery.ResolvedNativeEntry{}, false, nil
	}
	address, err := d.readNativeInboxAddress(ctx, plan, selected.CredentialAdmissions)
	if err != nil {
		return runtimechanneldelivery.ResolvedNativeEntry{}, false, err
	}
	if text.ConversationScope == operatorchannel.ConversationScopeShared && text.EntryAddress == "" {
		return runtimechanneldelivery.ResolvedNativeEntry{}, false, nil
	}
	if text.EntryAddress != "" && !strings.EqualFold(text.EntryAddress, address) {
		return runtimechanneldelivery.ResolvedNativeEntry{}, false, nil
	}
	return entry, true, nil
}

func (d *serveChannelDeliveryDispatcher) reconcileNativeInboxActivation(ctx context.Context, activation channelonboarding.ConnectedChannelActivation) error {
	if activation.Coordinate.ContextPublicationGeneration > math.MaxInt64 {
		return fmt.Errorf("native inbox publication generation exceeds selected-store range")
	}
	lease, current, err := d.manager.AcquireChannelActivationPublication(activation.Coordinate.BundleHash, activation.Coordinate.ContextPublicationGeneration)
	if err != nil {
		return err
	}
	if !current {
		return fmt.Errorf("native inbox publication is unavailable")
	}
	defer lease.Release()
	var compiled channelonboarding.CompiledActivation
	for _, candidate := range lease.Activations() {
		if candidate.Source != channelonboarding.ActivationSourceLearned ||
			candidate.OnboardingOperationID != activation.OperationID || candidate.ActivationRevision != activation.Revision ||
			!candidate.Coordinate.Matches(activation.Coordinate) {
			continue
		}
		if compiled.OnboardingOperationID != "" {
			return fmt.Errorf("native inbox compiled activation is duplicated")
		}
		compiled = candidate
	}
	if compiled.OnboardingOperationID == "" {
		return fmt.Errorf("native inbox compiled activation is not exact-current")
	}
	registration, found := d.ingress.ChannelRegistrationCurrent(ctx, d.now().UTC(),
		channelonboarding.LearnedBindingID(activation.SlotKey), activation.TargetSelector, activation.Provider)
	if !found || !registration.Current || registration.Registration.SlotID == "" ||
		!registration.ActivationGeneration.Equal(lease.Generation()) {
		return fmt.Errorf("native inbox physical registration is not exact-current")
	}
	contractHash, err := channelnative.EntryContractHash(activation.Coordinate.PlanGeneration)
	if err != nil {
		return err
	}
	setting, err := d.native.AttachNativeInboxSetting(ctx, channelnative.Admission{
		Provider: activation.Provider, ResourceSlotID: registration.Registration.SlotID,
		ConversationReference: activation.ConversationRef, PrincipalID: activation.PrincipalID,
		InterfaceKey: activation.Interface.Key(), BindingRevision: activation.BindingRevision,
		ActivationID: activation.ActivationID, ActivationRevision: activation.Revision,
		ContextPublicationGeneration: int64(activation.Coordinate.ContextPublicationGeneration),
		PackID:                       activation.Interface.ChannelPackID, PackVersion: activation.Interface.ChannelPackVersion,
		PackManifestHash: activation.Interface.ChannelManifestHash,
		PlanGeneration:   activation.Coordinate.PlanGeneration, EntryContractHash: contractHash,
	})
	if err != nil {
		return err
	}
	if setting.State == "uncertain" || setting.State == "unavailable" || setting.State == "retired" {
		return fmt.Errorf("native inbox setting is %s and needs administrative recovery", setting.State)
	}
	if _, err := d.readNativeInboxAddress(ctx, compiled.Plan, activation.CredentialAdmissions); err != nil {
		return fmt.Errorf("native inbox bot address is unavailable: %w", err)
	}
	observed, err := d.readNativeInboxCommands(ctx, compiled.Plan, activation.CredentialAdmissions, setting)
	if err != nil {
		return err
	}
	desired, err := channelnative.DesiredCommands(setting.SettingID, setting.Generation)
	if err != nil {
		return err
	}
	if setting.State == "installed" {
		if bytes.Equal(observed, desired) {
			return nil
		}
		if err := d.native.MarkNativeInboxSettingUnavailable(ctx, setting.SettingID, setting.Generation); err != nil {
			return err
		}
		return fmt.Errorf("native inbox readback contradicts installed setting")
	}
	if setting.State != "planned" {
		return fmt.Errorf("native inbox setting has unknown state %q", setting.State)
	}
	if !bytes.Equal(observed, []byte("[]")) {
		if err := d.native.MarkNativeInboxSettingUnavailable(ctx, setting.SettingID, setting.Generation); err != nil {
			return err
		}
		return fmt.Errorf("native inbox provider setting is already occupied")
	}
	return d.installNativeInboxCommands(ctx, activation, compiled.Plan, setting)
}

func (d *serveChannelDeliveryDispatcher) readNativeInboxAddress(ctx context.Context, plan packs.OutboundBindingPlan, admissions []channelonboarding.CredentialAdmission) (string, error) {
	const operation = "identify_inbox_address"
	_, input, err := plan.PrepareOperation(operation, map[string]any{})
	if err != nil {
		return "", err
	}
	toolID, tool, err := plan.ConnectorOperation(operation)
	if err != nil {
		return "", err
	}
	if tool.Effect() != runtimecontracts.ActivityEffectClassReadOnly {
		return "", fmt.Errorf("native inbox address requires a read-only operation")
	}
	credentials, err := resolveChannelDeliveryCredentials(ctx, d.credentials, plan, admissions, tool)
	if err != nil {
		return "", err
	}
	output, err := (runtimeregistration.HTTPExecutor{Client: d.httpClient}).Read(ctx, toolID, tool, input, credentials)
	if err != nil {
		return "", err
	}
	projected, err := plan.ProjectOperationOutput(operation, output)
	if err != nil {
		return "", err
	}
	address, ok := projected["address_reference"].(string)
	if !ok || address == "" {
		return "", fmt.Errorf("native inbox address readback is incomplete")
	}
	return address, nil
}

func nativeInboxOperation(scopeKind, action string) (string, error) {
	if scopeKind != "chat" && scopeKind != "chat_member" {
		return "", fmt.Errorf("native inbox scope is unsupported")
	}
	if action != "read" && action != "install" {
		return "", fmt.Errorf("native inbox operation is unsupported")
	}
	if scopeKind == "chat_member" {
		return action + "_shared_inbox_entry", nil
	}
	return action + "_inbox_entry", nil
}

func nativeInboxInput(setting channelnative.Setting, includeCommands bool) (map[string]any, error) {
	input := map[string]any{}
	if setting.ScopeKind == "chat_member" {
		if setting.MemberReference == "" {
			return nil, fmt.Errorf("shared native inbox scope lacks member reference")
		}
		input["member_reference"] = setting.MemberReference
	} else if setting.ScopeKind != "chat" || setting.MemberReference != "" {
		return nil, fmt.Errorf("native inbox setting scope contradicts member reference")
	}
	if includeCommands {
		desired, err := channelnative.DesiredCommands(setting.SettingID, setting.Generation)
		if err != nil {
			return nil, err
		}
		var commands []map[string]string
		if err := json.Unmarshal(desired, &commands); err != nil {
			return nil, err
		}
		input["commands"] = commands
	}
	return input, nil
}

func (d *serveChannelDeliveryDispatcher) readNativeInboxCommands(ctx context.Context, plan packs.OutboundBindingPlan, admissions []channelonboarding.CredentialAdmission, setting channelnative.Setting) ([]byte, error) {
	operation, err := nativeInboxOperation(setting.ScopeKind, "read")
	if err != nil {
		return nil, err
	}
	semanticInput, err := nativeInboxInput(setting, false)
	if err != nil {
		return nil, err
	}
	_, input, err := plan.PrepareOperation(operation, semanticInput)
	if err != nil {
		return nil, err
	}
	toolID, tool, err := plan.ConnectorOperation(operation)
	if err != nil {
		return nil, err
	}
	credentials, err := resolveChannelDeliveryCredentials(ctx, d.credentials, plan, admissions, tool)
	if err != nil {
		return nil, err
	}
	output, err := (runtimeregistration.HTTPExecutor{Client: d.httpClient}).Read(ctx, toolID, tool, input, credentials)
	if err != nil {
		return nil, err
	}
	projected, err := plan.ProjectOperationOutput(operation, output)
	if err != nil {
		return nil, err
	}
	commands, ok := projected["commands"]
	if !ok {
		return nil, fmt.Errorf("native inbox readback omits commands")
	}
	return canonicaljson.Bytes(commands)
}

func (d *serveChannelDeliveryDispatcher) installNativeInboxCommands(ctx context.Context, activation channelonboarding.ConnectedChannelActivation, plan packs.OutboundBindingPlan, setting channelnative.Setting) error {
	desired, err := channelnative.DesiredCommands(setting.SettingID, setting.Generation)
	if err != nil {
		return err
	}
	operation, err := nativeInboxOperation(setting.ScopeKind, "install")
	if err != nil {
		return err
	}
	semanticInput, err := nativeInboxInput(setting, true)
	if err != nil {
		return err
	}
	_, input, err := plan.PrepareOperation(operation, semanticInput)
	if err != nil {
		return err
	}
	toolID, tool, err := plan.ConnectorOperation(operation)
	if err != nil {
		return err
	}
	if tool.Effect() != runtimecontracts.ActivityEffectClassNonIdempotentWrite {
		return fmt.Errorf("native inbox install is not a managed write")
	}
	credentials, err := resolveChannelDeliveryCredentials(ctx, d.credentials, plan, activation.CredentialAdmissions, tool)
	if err != nil {
		return err
	}
	coordinate := activation.Coordinate
	bridge := runtimeeffects.ChannelNativeSettingAuthority{
		EffectOperationID: setting.InstallOperationID, SettingID: setting.SettingID,
		SettingGeneration: setting.Generation, Provider: setting.Provider, ResourceSlotID: setting.ResourceSlotID,
		ConversationRef: setting.ConversationRef, ScopeKind: setting.ScopeKind, MemberReference: setting.MemberReference,
		PrincipalID: setting.PrincipalID, EntryContractHash: setting.EntryContractHash, EntryCommand: setting.EntryCommand,
		PackID:      activation.Interface.ChannelPackID,
		PackVersion: activation.Interface.ChannelPackVersion, PackManifestHash: activation.Interface.ChannelManifestHash,
		ActivationID: activation.ActivationID, ActivationRevision: activation.Revision, BindingRevision: activation.BindingRevision,
		BundleHash: coordinate.BundleHash, BundleIdentity: coordinate.BundleIdentity,
		PackInventoryGeneration: coordinate.PackInventoryGeneration, RuntimeInstanceID: coordinate.RuntimeInstanceID,
		ContextPublicationGeneration: coordinate.ContextPublicationGeneration, PlanGeneration: coordinate.PlanGeneration,
		TargetGeneration: coordinate.TargetGeneration,
	}
	now := d.now().UTC()
	authority := runtimeeffects.Authority{
		Kind: runtimeeffects.AuthorityChannelNativeSetting, ID: setting.InstallOperationID,
		ExecutionOwner: "channel-native-setting:" + d.runtimeInstanceID,
		LeaseExpiresAt: now.Add(5 * time.Minute), FenceGeneration: uint64(setting.Generation),
		ExecutionMode: runtimeeffects.ExecutionMode(d.posture.RootMode()), ChannelNativeSetting: bridge,
	}
	if !authority.Valid() {
		return fmt.Errorf("native inbox effect authority is invalid")
	}
	effectCtx := runtimeeffects.WithExecutionMode(ctx, authority.ExecutionMode)
	effectCtx = runtimeeffects.WithController(effectCtx, runtimeeffects.NewController(d.effects).WithExecutionPosture(d.posture))
	effectCtx = runtimeeffects.WithAuthority(effectCtx, authority)
	effectCtx = runtimeauthoractivity.WithScope(effectCtx, runtimeauthoractivity.BundleScope(d.runtimeInstanceID, coordinate.BundleHash))
	result, applyErr := (runtimeregistration.HTTPExecutor{Client: d.httpClient}).ApplyChannelNativeSetting(
		effectCtx, toolID, tool, input, credentials, map[string]string{"setting_id": setting.SettingID, "setting_generation": fmt.Sprint(setting.Generation)},
	)
	if result.Pending == nil {
		return applyErr
	}
	if applyErr != nil {
		settleErr := result.Pending.SettleNativeSettingReadback(context.WithoutCancel(ctx), nil, desired, applyErr)
		return errors.Join(fmt.Errorf("native inbox write acknowledgment is uncertain: %w", applyErr), settleErr)
	}
	observed, readErr := d.readNativeInboxCommands(ctx, plan, activation.CredentialAdmissions, setting)
	if err := result.Pending.SettleNativeSettingReadback(context.WithoutCancel(ctx), observed, desired, readErr); err != nil {
		return errors.Join(readErr, err)
	}
	if readErr != nil {
		return fmt.Errorf("native inbox setting readback is unavailable: %w", readErr)
	}
	if !bytes.Equal(observed, desired) {
		return fmt.Errorf("native inbox setting readback contradicts desired commands")
	}
	return nil
}
