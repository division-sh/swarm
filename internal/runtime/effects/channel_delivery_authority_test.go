package effects

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/google/uuid"
)

func TestChannelDeliveryAuthorityRequiresExactFrozenIntent(t *testing.T) {
	plan, err := plangeneration.FromCanonicalValue(map[string]string{"channel": "delivery"})
	if err != nil {
		t.Fatal(err)
	}
	deliveryID, renderID := uuid.NewString(), uuid.NewString()
	operationID, err := ChannelDeliveryOperationID(deliveryID, renderID)
	if err != nil {
		t.Fatal(err)
	}
	authority := Authority{
		Kind: AuthorityChannelDelivery, ID: operationID, ExecutionOwner: "delivery-worker",
		LeaseExpiresAt: time.Now().Add(time.Minute), FenceGeneration: 2, ExecutionMode: ExecutionModeLive,
		ChannelDelivery: ChannelDeliveryAuthority{
			EffectOperationID: operationID, DeliveryID: deliveryID, RenderID: renderID,
			RenderHash: "sha256:render", PrincipalID: uuid.NewString(), InterfaceKey: "telegram:v2",
			DeliveryEpoch: 1, BindingRevision: 1, ExternalAccountRef: "account", ConversationRef: "chat",
			ActivationID: uuid.NewString(), ActivationRevision: 1,
			BundleHash: "bundle-v2:sha256:source", BundleIdentity: "bundle", PackInventoryGeneration: "inventory",
			RuntimeInstanceID: uuid.NewString(), ContextPublicationGeneration: 2,
			PlanGeneration: plan, TargetGeneration: 1,
		},
	}
	if !authority.Valid() {
		t.Fatal("exact channel delivery authority rejected")
	}
	if _, err := BeginChannelDelivery(WithAuthority(context.Background(), authority), []byte("request"), nil); err == nil {
		t.Fatal("channel delivery without a selected-store controller was admitted")
	}
	cases := map[string]func(*Authority){
		"foreign_operation": func(a *Authority) { a.ChannelDelivery.EffectOperationID = uuid.NewString() },
		"foreign_render":    func(a *Authority) { a.ChannelDelivery.RenderID = uuid.NewString() },
		"missing_hash":      func(a *Authority) { a.ChannelDelivery.RenderHash = "" },
		"retired_epoch":     func(a *Authority) { a.ChannelDelivery.DeliveryEpoch = 0 },
		"missing_binding":   func(a *Authority) { a.ChannelDelivery.BindingRevision = 0 },
		"wrong_generation":  func(a *Authority) { a.FenceGeneration++ },
		"missing_activation": func(a *Authority) {
			a.ChannelDelivery.ActivationID = ""
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			bad := authority
			mutate(&bad)
			if bad.Valid() {
				t.Fatalf("invalid authority %s admitted", name)
			}
		})
	}
}
