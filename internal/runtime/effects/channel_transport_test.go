package effects

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/google/uuid"
)

func TestChannelTransportEvidenceHasDisjointParents(t *testing.T) {
	plan, err := plangeneration.FromCanonicalValue(map[string]string{"channel": "transport"})
	if err != nil {
		t.Fatal(err)
	}
	parent := channelnative.TransportParent{Kind: channelnative.TransportDeploymentRegistration,
		ID: uuid.NewString(), Revision: 3, BindingOperationID: uuid.NewString(), BindingOperationRevision: 2,
		ProofID: uuid.NewString(), ProofRevision: 2}
	deliveryID, renderID, publicationID, settingID := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	deliveryOperation, err := ChannelDeliveryOperationID(deliveryID, renderID)
	if err != nil {
		t.Fatal(err)
	}
	ackOperation, err := ChannelActionAckOperationID(publicationID)
	if err != nil {
		t.Fatal(err)
	}
	settingOperation, err := channelnative.InstallOperationID(settingID, 1)
	if err != nil {
		t.Fatal(err)
	}
	settingCommand, err := channelnative.EntryCommand(settingID, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, authority := range []Authority{
		{Kind: AuthorityServeRegistration, ID: deliveryOperation,
			ServeRegistration: ServeRegistrationAuthority{Parent: parent, IntentID: deliveryOperation,
				StartupAuthorityID: uuid.NewString(), StartupStateVersion: 1, OnboardingOperationID: parent.ID, OnboardingRevision: parent.Revision}},
		{Kind: AuthorityChannelDelivery, ID: deliveryOperation,
			ChannelDelivery: ChannelDeliveryAuthority{Parent: parent, EffectOperationID: deliveryOperation,
				DeliveryID: deliveryID, RenderID: renderID, RenderHash: "sha256:render", PrincipalID: uuid.NewString(),
				InterfaceKey: "telegram:v2", DeliveryEpoch: 1, BindingRevision: 2, ExternalAccountRef: "operator",
				ConversationRef: "chat", PlanGeneration: plan}},
		{Kind: AuthorityChannelActionAck, ID: ackOperation,
			ChannelActionAck: ChannelActionAckAuthority{Parent: parent, EffectOperationID: ackOperation,
				PublicationID: publicationID, Token: uuid.NewString(), ReceiptOperationID: deliveryOperation,
				PrincipalID: uuid.NewString(), BindingRevision: 2, Provider: "telegram", ProviderEventID: "event",
				ProviderAuthorization: "authenticated", InterfaceKey: "telegram:v2", ExternalAccountRef: "operator",
				ConversationRef: "chat", ConversationScope: "direct", MessageReference: "message", InteractionRef: "callback",
				PlanGeneration: plan}},
		{Kind: AuthorityChannelNativeSetting, ID: settingOperation,
			ChannelNativeSetting: ChannelNativeSettingAuthority{Parent: parent, EffectOperationID: settingOperation,
				SettingID: settingID, SettingGeneration: 1, Provider: "telegram", ResourceSlotID: "telegram:bot",
				ConversationRef: "chat", ScopeKind: "chat", PrincipalID: uuid.NewString(), BindingRevision: 2,
				EntryContractHash: "sha256:contract", EntryCommand: settingCommand,
				PackID: "telegram", PackVersion: "1", PackManifestHash: "manifest", PlanGeneration: plan}},
	} {
		t.Run(string(authority.Kind), func(t *testing.T) {
			authority.ExecutionOwner, authority.LeaseExpiresAt = "serve", time.Now().Add(time.Minute)
			authority.ExecutionMode, authority.FenceGeneration = ExecutionModeLive, uint64(parent.Revision)
			if authority.Kind == AuthorityChannelNativeSetting {
				authority.FenceGeneration = 1
			}
			if !authority.Valid() {
				t.Fatal("explicit deployment transport provenance rejected")
			}
			evidence := authority.Evidence()
			if evidence["transport_parent"] != parent {
				t.Fatal("deployment parent or proof was dropped")
			}
			for _, key := range []string{"activation_id", "activation_revision", "bundle_hash", "bundle_identity",
				"pack_inventory_generation", "runtime_instance_id", "context_publication_generation"} {
				if _, found := evidence[key]; found {
					t.Fatalf("deployment evidence invented runtime coordinate %s", key)
				}
			}
			bad := authority
			switch authority.Kind {
			case AuthorityServeRegistration:
				bad.ServeRegistration.Parent.Kind = ""
			case AuthorityChannelDelivery:
				bad.ChannelDelivery.Parent.Kind = ""
			case AuthorityChannelActionAck:
				bad.ChannelActionAck.Parent.Kind = ""
			case AuthorityChannelNativeSetting:
				bad.ChannelNativeSetting.Parent.Kind = ""
			}
			if bad.Valid() {
				t.Fatal("missing deployment tag was accepted")
			}
			if authority.Kind != AuthorityChannelNativeSetting && authority.Kind != AuthorityServeRegistration {
				bad = authority
				bad.FenceGeneration++
				if bad.Valid() {
					t.Fatal("wrong registration revision fence accepted")
				}
			}
		})
	}
}

func TestChannelTransportRetainsRuntimeShapeAndRejectsMixedCoordinates(t *testing.T) {
	activation, runtimeID := uuid.NewString(), uuid.NewString()
	for _, parent := range []channelnative.TransportParent{{},
		{Kind: channelnative.TransportRuntimeActivation, ID: activation, Revision: 2}} {
		if !channelTransportValid(parent, activation, 2, "bundle", "identity", "inventory", runtimeID, 4) {
			t.Fatal("existing runtime coordinate was rejected")
		}
	}
	legacy := map[string]any{"activation_id": activation, "activation_revision": int64(2), "runtime_instance_id": runtimeID,
		"context_publication_generation": uint64(4), "bundle_hash": "bundle", "bundle_identity": "identity", "pack_inventory_generation": "inventory"}
	retainChannelTransportEvidence(legacy, channelnative.TransportParent{})
	if len(legacy) != 7 || legacy["activation_id"] != activation || legacy["context_publication_generation"] != uint64(4) {
		t.Fatal("legacy runtime evidence changed")
	}
	parent := channelnative.TransportParent{Kind: channelnative.TransportDeploymentRegistration,
		ID: uuid.NewString(), Revision: 3, BindingOperationID: uuid.NewString(), BindingOperationRevision: 2,
		ProofID: uuid.NewString(), ProofRevision: 2}
	for name, check := range map[string]bool{
		"activation":          channelTransportValid(parent, parent.ID, 0, "", "", "", "", 0),
		"activation_revision": channelTransportValid(parent, "", 1, "", "", "", "", 0),
		"source":              channelTransportValid(parent, "", 0, "bundle", "", "", "", 0),
		"identity":            channelTransportValid(parent, "", 0, "", "identity", "", "", 0),
		"inventory":           channelTransportValid(parent, "", 0, "", "", "inventory", "", 0),
		"runtime":             channelTransportValid(parent, "", 0, "", "", "", runtimeID, 0),
		"generation":          channelTransportValid(parent, "", 0, "", "", "", "", 1),
	} {
		if check {
			t.Fatalf("deployment parent borrowed runtime %s", name)
		}
	}
}
