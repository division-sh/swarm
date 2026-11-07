package serveapp

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/division-sh/swarm/internal/runtime/effects"
)

func (d *serveChannelDeliveryDispatcher) reconcileDeploymentNativeInbox(ctx context.Context, selected channelonboarding.Result, plan packs.OutboundBindingPlan) error {
	if !plan.HasNativeInbox() {
		return nil
	}
	slot, err := d.requireDeploymentReceiver(ctx, selected)
	if err != nil {
		return err
	}
	parent, binding := deploymentParent(selected), selected.Binding
	generation, err := plan.PlanGeneration()
	if err != nil || parent.Validate() != nil || binding == nil {
		return errors.Join(err, channelonboarding.ErrRevisionConflict)
	}
	contract, err := channelnative.EntryContractHash(generation)
	if err != nil {
		return err
	}
	setting, err := d.native.AttachNativeInboxSetting(ctx, channelnative.Admission{Parent: parent,
		Provider: selected.Operation.Provider, ResourceSlotID: slot, ConversationReference: binding.ConversationRef,
		PrincipalID: binding.PrincipalID, InterfaceKey: binding.Interface.Key(), BindingRevision: binding.Revision,
		PackID: binding.Interface.ChannelPackID, PackVersion: binding.Interface.ChannelPackVersion,
		PackManifestHash: binding.Interface.ChannelManifestHash, PlanGeneration: generation, EntryContractHash: contract})
	if err != nil {
		return err
	}
	return d.qualifyNativeInboxTransport(ctx, selected.Operation.OperationID, selected.Operation.CredentialAdmissions, plan, setting,
		func(state channelnative.QualificationState, reason string, readback packs.NativeInboxReadback) error {
			hash := ""
			if state == channelnative.QualificationQualified {
				body, err := canonicaljson.Bytes(readback)
				if err != nil {
					return err
				}
				hash = operatorchannel.Hash("native-inbox-qualification-v1", string(body))
			}
			return d.native.RecordNativeInboxQualification(context.WithoutCancel(ctx), channelnative.QualificationRequest{Parent: parent,
				SettingID: setting.SettingID, SettingGeneration: setting.Generation, BindingRevision: binding.Revision,
				EntryContractHash: contract, ClientLanguage: setting.ClientLanguage, LocaleRevision: setting.ClientLocaleRevision,
				State: state, Reason: reason, ReadbackHash: hash, ObservedAt: d.now().UTC()})
		}, func() error {
			bridge := effects.ChannelNativeSettingAuthority{Parent: parent, BindingRevision: binding.Revision,
				PlanGeneration: generation,
				PackID:         binding.Interface.ChannelPackID, PackVersion: binding.Interface.ChannelPackVersion,
				PackManifestHash: binding.Interface.ChannelManifestHash}
			return d.installNativeInboxTransport(ctx, plan, setting, selected.Operation.CredentialAdmissions, bridge,
				authoractivity.RuntimeScope(d.runtimeInstanceID))
		})
}

func (d *serveChannelDeliveryDispatcher) qualifyDeploymentInboxEntry(ctx context.Context, text operatorchannel.InboundText, entry render.ResolvedInboxEntry) (render.ResolvedInboxEntry, render.InboxEntryDisposition, error) {
	selected, plan, err := d.deployment.selectCurrent(ctx, entry.Parent)
	if err != nil {
		return render.ResolvedInboxEntry{}, render.InboxEntryUnavailable, err
	}
	b := selected.Binding
	if b == nil || b.PrincipalID != entry.PrincipalID || b.Interface.Key() != entry.InterfaceKey || b.Revision != entry.BindingRevision ||
		b.ExternalAccountRef != text.ExternalAccountRef || b.ConversationRef != text.ConversationRef || b.ConversationScope != text.ConversationScope {
		return render.ResolvedInboxEntry{}, render.InboxEntryRejected, nil
	}
	if _, err := d.requireDeploymentReceiver(ctx, selected); err != nil {
		return render.ResolvedInboxEntry{}, render.InboxEntryUnavailable, err
	}
	if _, err := selectServeChannelWrite(ctx, d.sessions, selected.Operation, plan); err != nil {
		return render.ResolvedInboxEntry{}, render.InboxEntryUnavailable, err
	}
	if entry.Kind == render.InboxEntryTextReply {
		return d.qualifyTextReplyInbox(ctx, text, entry, selected.Operation.CredentialAdmissions, plan)
	}
	if entry.Kind != render.InboxEntryNative || !plan.HasNativeInbox() {
		return render.ResolvedInboxEntry{}, render.InboxEntryRejected, nil
	}
	if err := d.reconcileDeploymentNativeInbox(ctx, selected, plan); err != nil {
		return render.ResolvedInboxEntry{}, render.InboxEntryUnavailable, err
	}
	qualification, err := d.native.ReadNativeInboxQualificationParent(ctx, entry.Parent)
	if err != nil || qualification.State != channelnative.QualificationQualified || qualification.SettingID != entry.SettingID || qualification.SettingGeneration != entry.SettingGeneration {
		return render.ResolvedInboxEntry{}, render.InboxEntryUnavailable, errors.Join(err, fmt.Errorf("deployment inbox has no exact current qualification"))
	}
	address, err := d.readNativeInboxAddress(ctx, plan, selected.Operation.CredentialAdmissions)
	if err != nil {
		return render.ResolvedInboxEntry{}, render.InboxEntryUnavailable, err
	}
	if text.ConversationScope == operatorchannel.ConversationScopeShared && text.EntryAddress == "" ||
		text.EntryAddress != "" && !strings.EqualFold(text.EntryAddress, address) {
		return render.ResolvedInboxEntry{}, render.InboxEntryRejected, nil
	}
	return entry, render.InboxEntryAccepted, nil
}
