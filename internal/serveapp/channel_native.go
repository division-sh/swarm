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
)

func (d *serveChannelDeliveryDispatcher) qualifyNativeInboxActivation(ctx context.Context, activation channelonboarding.ConnectedChannelActivation, plan packs.OutboundBindingPlan, setting channelnative.Setting) error {
	recordFailure := func(state channelnative.QualificationState, cause error) error {
		return errors.Join(cause, d.recordNativeQualification(context.WithoutCancel(ctx), activation, setting, state, cause.Error(), packs.NativeInboxReadback{}))
	}
	profile, err := plan.NativeInboxProfile()
	if err != nil {
		return recordFailure(channelnative.QualificationInvalid, err)
	}
	if setting.ClientLanguage == "" {
		return recordFailure(channelnative.QualificationMissing, fmt.Errorf("native inbox requires an explicit client-language declaration; use swarm channel resume %s --client-language en or fr", activation.OperationID))
	}
	if err := profile.ValidateLanguage(setting.ClientLanguage); err != nil {
		return recordFailure(channelnative.QualificationInvalid, err)
	}
	if setting.State == "uncertain" || setting.State == "unavailable" {
		return recordFailure(channelnative.QualificationInvalid, fmt.Errorf("native inbox setting is %s and needs administrative recovery", setting.State))
	}
	if _, err := d.readNativeInboxAddress(ctx, plan, activation.CredentialAdmissions); err != nil {
		return recordFailure(channelnative.QualificationInvalid, fmt.Errorf("native inbox bot address is unavailable: %w", err))
	}
	readback, err := d.readNativeInboxQualification(ctx, plan, activation.CredentialAdmissions, setting, profile)
	if err != nil {
		return recordFailure(channelnative.QualificationInvalid, err)
	}
	desired, err := channelnative.DesiredCommands(setting.SettingID, setting.Generation)
	if err != nil {
		return err
	}
	if setting.State == "installed" || setting.State == "retired" {
		if err := profile.Qualify(setting.ClientLanguage, readback, desired); err != nil {
			return recordFailure(channelnative.QualificationInvalid, err)
		}
		return d.recordNativeQualification(ctx, activation, setting, channelnative.QualificationQualified, "", readback)
	}
	if setting.State != "planned" {
		return fmt.Errorf("native inbox setting has unknown state %q", setting.State)
	}
	if !bytes.Equal(readback.FallbackCommands, []byte("[]")) {
		return recordFailure(channelnative.QualificationInvalid, fmt.Errorf("native inbox provider fallback is already occupied; foreign settings are never overwritten"))
	}
	prospective := readback
	prospective.FallbackCommands = desired
	if err := profile.Qualify(setting.ClientLanguage, prospective, desired); err != nil {
		return recordFailure(channelnative.QualificationInvalid, err)
	}
	if err := d.installNativeInboxCommands(ctx, activation, plan, setting); err != nil {
		return recordFailure(channelnative.QualificationInvalid, err)
	}
	readback, err = d.readNativeInboxQualification(ctx, plan, activation.CredentialAdmissions, setting, profile)
	if err != nil {
		return recordFailure(channelnative.QualificationInvalid, err)
	}
	if err := profile.Qualify(setting.ClientLanguage, readback, desired); err != nil {
		return recordFailure(channelnative.QualificationInvalid, err)
	}
	return d.recordNativeQualification(ctx, activation, setting, channelnative.QualificationQualified, "", readback)
}

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

func (d *serveChannelDeliveryDispatcher) resolveInboxEntry(ctx context.Context, text operatorchannel.InboundText) (runtimechanneldelivery.ResolvedInboxEntry, runtimechanneldelivery.InboxEntryDisposition, error) {
	if d == nil || d.store == nil || d.activations == nil || d.manager == nil || d.ingress == nil || d.credentials == nil || d.now == nil {
		return runtimechanneldelivery.ResolvedInboxEntry{}, runtimechanneldelivery.InboxEntryUnavailable, fmt.Errorf("native inbox entry owners are unavailable")
	}
	entry, found, err := d.store.ResolveCurrentInboxEntry(ctx, text)
	if err != nil {
		return runtimechanneldelivery.ResolvedInboxEntry{}, runtimechanneldelivery.InboxEntryUnavailable, err
	}
	if !found {
		return runtimechanneldelivery.ResolvedInboxEntry{}, runtimechanneldelivery.InboxEntryRejected, nil
	}
	activations, err := d.activations.ListCurrentConnectedChannelActivations(ctx)
	if err != nil {
		return runtimechanneldelivery.ResolvedInboxEntry{}, runtimechanneldelivery.InboxEntryUnavailable, err
	}
	var selected channelonboarding.ConnectedChannelActivation
	for _, activation := range activations {
		if activation.ActivationID != entry.ActivationID {
			continue
		}
		if selected.ActivationID != "" {
			return runtimechanneldelivery.ResolvedInboxEntry{}, runtimechanneldelivery.InboxEntryUnavailable, fmt.Errorf("native inbox activation is duplicated")
		}
		selected = activation
	}
	if selected.ActivationID == "" {
		return runtimechanneldelivery.ResolvedInboxEntry{}, runtimechanneldelivery.InboxEntryUnavailable, nil
	}
	if selected.PrincipalID != entry.PrincipalID ||
		selected.Interface.Key() != entry.InterfaceKey || selected.BindingRevision != entry.BindingRevision ||
		selected.Provider != text.Provider || selected.ConversationRef != text.ConversationRef {
		return runtimechanneldelivery.ResolvedInboxEntry{}, runtimechanneldelivery.InboxEntryRejected, nil
	}
	return d.qualifyResolvedNativeEntry(ctx, text, entry, selected)
}

func (d *serveChannelDeliveryDispatcher) qualifyResolvedNativeEntry(ctx context.Context, text operatorchannel.InboundText, entry runtimechanneldelivery.ResolvedInboxEntry, selected channelonboarding.ConnectedChannelActivation) (runtimechanneldelivery.ResolvedInboxEntry, runtimechanneldelivery.InboxEntryDisposition, error) {
	registration, current := d.ingress.ChannelRegistrationCurrent(ctx, d.now().UTC(),
		channelonboarding.LearnedBindingID(selected.SlotKey), selected.TargetSelector, selected.Provider)
	if !current || !registration.Current {
		return runtimechanneldelivery.ResolvedInboxEntry{}, runtimechanneldelivery.InboxEntryUnavailable, nil
	}
	if entry.Kind == runtimechanneldelivery.InboxEntryNative && registration.Registration.SlotID != entry.ResourceSlotID {
		return runtimechanneldelivery.ResolvedInboxEntry{}, runtimechanneldelivery.InboxEntryRejected, nil
	}
	lease, current, err := d.manager.AcquireChannelActivationPublication(selected.Coordinate.BundleHash, selected.Coordinate.ContextPublicationGeneration)
	if err != nil || !current {
		return runtimechanneldelivery.ResolvedInboxEntry{}, runtimechanneldelivery.InboxEntryUnavailable, err
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
			return runtimechanneldelivery.ResolvedInboxEntry{}, runtimechanneldelivery.InboxEntryUnavailable, fmt.Errorf("native inbox compiled activation is duplicated")
		}
		plan, matched = compiled.Plan, true
	}
	if !matched {
		return runtimechanneldelivery.ResolvedInboxEntry{}, runtimechanneldelivery.InboxEntryUnavailable, fmt.Errorf("native inbox compiled publication is unavailable")
	}
	ctx, err = withChannelProviderAdmission(ctx, lease, plan)
	if err != nil {
		return runtimechanneldelivery.ResolvedInboxEntry{}, runtimechanneldelivery.InboxEntryUnavailable, err
	}
	if entry.Kind == runtimechanneldelivery.InboxEntryTextReply {
		if err := plan.Capabilities().Require(packs.ChannelCapabilityInboxListing); err != nil {
			return runtimechanneldelivery.ResolvedInboxEntry{}, runtimechanneldelivery.InboxEntryUnavailable, err
		}
		if text.EntryReference != runtimechanneldelivery.TextReplyInboxReference {
			return runtimechanneldelivery.ResolvedInboxEntry{}, runtimechanneldelivery.InboxEntryRejected, nil
		}
		if text.EntryAddress != "" {
			if !plan.HasNativeInbox() {
				return runtimechanneldelivery.ResolvedInboxEntry{}, runtimechanneldelivery.InboxEntryRejected, nil
			}
			address, err := d.readNativeInboxAddress(ctx, plan, selected.CredentialAdmissions)
			if err != nil {
				return runtimechanneldelivery.ResolvedInboxEntry{}, runtimechanneldelivery.InboxEntryUnavailable, err
			}
			if !strings.EqualFold(text.EntryAddress, address) {
				return runtimechanneldelivery.ResolvedInboxEntry{}, runtimechanneldelivery.InboxEntryRejected, nil
			}
		}
		return entry, runtimechanneldelivery.InboxEntryAccepted, nil
	}
	if entry.Kind != runtimechanneldelivery.InboxEntryNative || !plan.HasNativeInbox() {
		return runtimechanneldelivery.ResolvedInboxEntry{}, runtimechanneldelivery.InboxEntryRejected, nil
	}
	if err := d.reconcileNativeInboxActivation(ctx, selected); err != nil {
		return runtimechanneldelivery.ResolvedInboxEntry{}, runtimechanneldelivery.InboxEntryUnavailable, err
	}
	qualification, err := d.native.ReadNativeInboxQualification(ctx, selected.ActivationID)
	if err != nil || qualification.State != channelnative.QualificationQualified || qualification.SettingID != entry.SettingID || qualification.SettingGeneration != entry.SettingGeneration {
		return runtimechanneldelivery.ResolvedInboxEntry{}, runtimechanneldelivery.InboxEntryUnavailable, errors.Join(err, fmt.Errorf("native inbox entry has no exact current client qualification"))
	}
	address, err := d.readNativeInboxAddress(ctx, plan, selected.CredentialAdmissions)
	if err != nil {
		return runtimechanneldelivery.ResolvedInboxEntry{}, runtimechanneldelivery.InboxEntryUnavailable, err
	}
	if text.ConversationScope == operatorchannel.ConversationScopeShared && text.EntryAddress == "" {
		return runtimechanneldelivery.ResolvedInboxEntry{}, runtimechanneldelivery.InboxEntryRejected, nil
	}
	if text.EntryAddress != "" && !strings.EqualFold(text.EntryAddress, address) {
		return runtimechanneldelivery.ResolvedInboxEntry{}, runtimechanneldelivery.InboxEntryRejected, nil
	}
	return entry, runtimechanneldelivery.InboxEntryAccepted, nil
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
	if !compiled.Plan.HasNativeInbox() {
		return nil
	}
	ctx, err = withChannelProviderAdmission(ctx, lease, compiled.Plan)
	if err != nil {
		return err
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
	return d.qualifyNativeInboxActivation(ctx, activation, compiled.Plan, setting)
}

func (d *serveChannelDeliveryDispatcher) recordNativeQualification(ctx context.Context, activation channelonboarding.ConnectedChannelActivation,
	setting channelnative.Setting, state channelnative.QualificationState, reason string, readback packs.NativeInboxReadback) error {
	hash := ""
	if state == channelnative.QualificationQualified {
		body, err := canonicaljson.Bytes(readback)
		if err != nil {
			return err
		}
		hash = operatorchannel.Hash("native-inbox-qualification-v1", string(body))
	}
	return d.native.RecordNativeInboxQualification(ctx, channelnative.QualificationRequest{
		SettingID: setting.SettingID, SettingGeneration: setting.Generation, ActivationID: activation.ActivationID,
		ActivationRevision: activation.Revision, ContextGeneration: int64(activation.Coordinate.ContextPublicationGeneration),
		BindingRevision: activation.BindingRevision, EntryContractHash: setting.EntryContractHash,
		ClientLanguage: setting.ClientLanguage, LocaleRevision: setting.ClientLocaleRevision,
		State: state, Reason: reason, ReadbackHash: hash, ObservedAt: d.now().UTC(),
	})
}

func (d *serveChannelDeliveryDispatcher) readNativeInboxQualification(ctx context.Context, plan packs.OutboundBindingPlan,
	admissions []channelonboarding.CredentialAdmission, setting channelnative.Setting, profile packs.CompiledNativeInboxProfile) (packs.NativeInboxReadback, error) {
	readback := packs.NativeInboxReadback{Shared: setting.ScopeKind == "chat_member"}
	selected, err := d.readNativeInboxLanguageCommands(ctx, plan, admissions, setting, setting.ClientLanguage)
	if err != nil {
		return readback, err
	}
	readback.SelectedCommands = selected
	if setting.State == "planned" || bytes.Equal(selected, []byte("[]")) {
		fallback, err := d.readNativeInboxCommands(ctx, plan, admissions, setting)
		if err != nil {
			return readback, err
		}
		readback.FallbackCommands = fallback
	}
	if readback.Shared {
		return readback, nil
	}
	readback.ChatLauncher, err = d.readNativeInboxLauncher(ctx, plan, admissions, profile.DirectLauncherRead())
	if err != nil {
		return readback, err
	}
	if profile.InheritsLauncher(readback.ChatLauncher) {
		readback.DefaultLauncher, err = d.readNativeInboxLauncher(ctx, plan, admissions, profile.DefaultLauncherRead())
	}
	return readback, err
}

func (d *serveChannelDeliveryDispatcher) readNativeInboxLauncher(ctx context.Context, plan packs.OutboundBindingPlan, admissions []channelonboarding.CredentialAdmission, operation string) (string, error) {
	_, input, err := plan.PrepareOperation(operation, map[string]any{})
	if err != nil {
		return "", err
	}
	toolID, tool, err := plan.ConnectorOperation(operation)
	if err != nil {
		return "", err
	}
	if tool.Effect() != runtimecontracts.ActivityEffectClassReadOnly {
		return "", fmt.Errorf("native inbox launcher requires a compiled read-only operation")
	}
	credentials, err := resolveChannelDeliveryCredentials(ctx, d.credentials, plan, admissions, tool)
	if err != nil {
		return "", err
	}
	output, err := channelCredentialHTTPExecutor(d.httpClient, d.credentials, plan, admissions, tool).Read(ctx, toolID, tool, input, credentials)
	if err != nil {
		return "", err
	}
	projected, err := plan.ProjectOperationOutput(operation, output)
	if err != nil {
		return "", err
	}
	launcher, ok := projected["launcher"].(string)
	if !ok || launcher == "" {
		return "", fmt.Errorf("native inbox launcher readback is incomplete")
	}
	return launcher, nil
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
	output, err := channelCredentialHTTPExecutor(d.httpClient, d.credentials, plan, admissions, tool).Read(ctx, toolID, tool, input, credentials)
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

func nativeInboxInput(plan packs.OutboundBindingPlan, setting channelnative.Setting, includeCommands bool) (map[string]any, error) {
	input := map[string]any{}
	if !includeCommands {
		input["language_code"] = ""
	}
	if setting.ScopeKind == "chat_member" {
		if setting.MemberReference == "" {
			return nil, fmt.Errorf("shared native inbox scope lacks member reference")
		}
		member, err := plan.RestoreOpaqueReference("external_account_reference", setting.MemberReference)
		if err != nil {
			return nil, err
		}
		input["member_reference"] = member
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
	return d.readNativeInboxLanguageCommands(ctx, plan, admissions, setting, "")
}

func (d *serveChannelDeliveryDispatcher) readNativeInboxLanguageCommands(ctx context.Context, plan packs.OutboundBindingPlan, admissions []channelonboarding.CredentialAdmission, setting channelnative.Setting, language string) ([]byte, error) {
	operation, err := nativeInboxOperation(setting.ScopeKind, "read")
	if err != nil {
		return nil, err
	}
	semanticInput, err := nativeInboxInput(plan, setting, false)
	if err != nil {
		return nil, err
	}
	semanticInput["language_code"] = language
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
	output, err := channelCredentialHTTPExecutor(d.httpClient, d.credentials, plan, admissions, tool).Read(ctx, toolID, tool, input, credentials)
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
	semanticInput, err := nativeInboxInput(plan, setting, true)
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
	result, applyErr := channelCredentialHTTPExecutor(d.httpClient, d.credentials, plan, activation.CredentialAdmissions, tool).ApplyChannelNativeSetting(
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
