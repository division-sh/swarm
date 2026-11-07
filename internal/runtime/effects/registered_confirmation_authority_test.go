package effects

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/google/uuid"
)

func TestRegisteredAcknowledgementAuthorityIsOneClosedMessage(t *testing.T) {
	parent := channelnative.TransportParent{Kind: "deployment_registration", ID: uuid.NewString(), Revision: 7,
		BindingOperationID: uuid.NewString(), BindingOperationRevision: 3}
	id := RegisteredOperatorAcknowledgementID(parent.BindingOperationID, 2)
	a := Authority{Kind: AuthorityChannelConfirmation, ID: id, ExecutionOwner: uuid.NewString(), FenceGeneration: 7,
		LeaseExpiresAt: time.Now().Add(time.Minute), ExecutionMode: ExecutionModeLive,
		ChannelConfirmation: ChannelConfirmationAuthority{TransportParent: &parent, EffectOperationID: id,
			OnboardingOperationID: parent.ID, OnboardingRevision: parent.Revision, PrincipalID: uuid.NewString(),
			BindingRevision: 2, Destination: "15550000000@s.whatsapp.net"}}
	if !a.Valid() {
		t.Fatal("exact registered acknowledgement was refused")
	}
	for name, change := range map[string]func(*Authority){
		"arbitrary operation": func(a *Authority) { a.ID = uuid.NewString(); a.ChannelConfirmation.EffectOperationID = a.ID },
		"different parent":    func(a *Authority) { a.ChannelConfirmation.OnboardingOperationID = uuid.NewString() },
		"different revision":  func(a *Authority) { a.ChannelConfirmation.OnboardingRevision++ },
		"different fence":     func(a *Authority) { a.FenceGeneration++ },
		"different binding":   func(a *Authority) { a.ChannelConfirmation.BindingRevision++ },
		"missing destination": func(a *Authority) { a.ChannelConfirmation.Destination = "" },
		"borrowed activation": func(a *Authority) { a.ChannelConfirmation.ActivationID = uuid.NewString() },
		"borrowed bundle":     func(a *Authority) { a.ChannelConfirmation.BundleHash = "bundle" },
		"borrowed runtime":    func(a *Authority) { a.ChannelConfirmation.RuntimeInstanceID = uuid.NewString() },
		"missing parent":      func(a *Authority) { a.ChannelConfirmation.TransportParent = nil },
	} {
		t.Run(name, func(t *testing.T) {
			changed := a
			change(&changed)
			if changed.Valid() {
				t.Fatal("widened registered authority was admitted")
			}
		})
	}
	evidence := a.Evidence()
	for _, key := range []string{"activation_id", "activation_revision", "bundle_hash", "bundle_identity", "runtime_instance_id", "context_publication_generation", "pack_inventory_generation", "plan_generation"} {
		if _, present := evidence[key]; present {
			t.Fatal("registered authority fabricated an authored coordinate", key)
		}
	}
	if evidence["transport_parent"] != parent || evidence["destination"] != a.ChannelConfirmation.Destination {
		t.Fatal("registered authority lost its original parent or exact destination")
	}
}
