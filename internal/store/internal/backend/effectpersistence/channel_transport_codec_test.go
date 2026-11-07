package effectpersistence

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/google/uuid"
)

func TestChannelSettlementCodecPreservesOriginalTransportParent(t *testing.T) {
	plan, err := plangeneration.FromCanonicalValue(map[string]string{"channel": "codec"})
	if err != nil {
		t.Fatal(err)
	}
	deliveryID, renderID := uuid.NewString(), uuid.NewString()
	opID, err := runtimeeffects.ChannelDeliveryOperationID(deliveryID, renderID)
	if err != nil {
		t.Fatal(err)
	}
	base := runtimeeffects.Authority{Kind: runtimeeffects.AuthorityChannelDelivery, ID: opID,
		ExecutionOwner: "delivery", LeaseExpiresAt: time.Now().Add(time.Minute), FenceGeneration: 4,
		ExecutionMode: runtimeeffects.ExecutionModeLive, ChannelDelivery: runtimeeffects.ChannelDeliveryAuthority{
			EffectOperationID: opID, DeliveryID: deliveryID, RenderID: renderID, RenderHash: "sha256:render",
			PrincipalID: uuid.NewString(), InterfaceKey: "telegram:v2", DeliveryEpoch: 1, BindingRevision: 2,
			ExternalAccountRef: "operator", ConversationRef: "chat", PlanGeneration: plan,
			ActivationID: uuid.NewString(), ActivationRevision: 3, ContextPublicationGeneration: 4,
			RuntimeInstanceID: uuid.NewString(), BundleHash: "bundle", BundleIdentity: "identity", PackInventoryGeneration: "inventory"}}
	for _, kind := range []string{"legacy_runtime", "explicit_runtime", "deployment"} {
		t.Run(kind, func(t *testing.T) {
			a := base
			if kind == "explicit_runtime" {
				a.ChannelDelivery.Parent = channelnative.TransportParent{Kind: channelnative.TransportRuntimeActivation,
					ID: a.ChannelDelivery.ActivationID, Revision: a.ChannelDelivery.ActivationRevision}
			} else if kind == "deployment" {
				d := &a.ChannelDelivery
				d.Parent = channelnative.TransportParent{Kind: channelnative.TransportDeploymentRegistration,
					ID: uuid.NewString(), Revision: 4, BindingOperationID: uuid.NewString(), BindingOperationRevision: 2,
					ProofID: uuid.NewString(), ProofRevision: 2}
				d.ActivationID, d.ActivationRevision, d.RuntimeInstanceID, d.ContextPublicationGeneration = "", 0, "", 0
				d.BundleHash, d.BundleIdentity, d.PackInventoryGeneration = "", "", ""
			}
			if !a.Valid() {
				t.Fatal("test requires valid original coordinates")
			}
			raw, err := canonicaljson.Bytes(a.Evidence())
			if err != nil {
				t.Fatal(err)
			}
			var stored channelSettlementEvidence
			if err := json.Unmarshal(raw, &stored); err != nil {
				t.Fatal(err)
			}
			restored, err := stored.authority(a.LeaseExpiresAt)
			if err != nil || restored.ChannelDelivery != a.ChannelDelivery {
				t.Fatalf("original parent was not restored: err=%v", err)
			}
			reencoded, err := canonicaljson.Bytes(restored.Evidence())
			if err != nil || !bytes.Equal(raw, reencoded) {
				t.Fatalf("original evidence changed: err=%v", err)
			}
			if kind == "deployment" {
				stored.ActivationID = stored.TransportParent.ID
				if _, err := stored.authority(a.LeaseExpiresAt); err == nil {
					t.Fatal("mixed deployment/runtime settlement accepted")
				}
				stored.ActivationID = ""
				stored.TransportParent.Kind = ""
				if _, err := stored.authority(a.LeaseExpiresAt); err == nil {
					t.Fatal("missing deployment tag reinterpreted by settlement")
				}
			}
		})
	}
}
