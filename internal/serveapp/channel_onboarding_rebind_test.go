package serveapp

import (
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
)

func TestActivationSupersededByExactRebindRequiresOneBoundOwner(t *testing.T) {
	identity := operatorchannel.InterfaceIdentity{
		InterfaceRef: "swarm.hitl-channel/v2", ChannelPackID: "provider.telegram.hitl_channel",
		ChannelPackVersion: "0.1.0", ChannelManifestHash: "sha256:manifest", SemanticGeneration: "sha256:plan",
	}.Normalized()
	activation := channelonboarding.ConnectedChannelActivation{
		PrincipalID: "principal-a", Interface: identity, BindingRevision: 3, Status: channelonboarding.ActivationCurrent,
	}
	binding := operatorchannel.Binding{
		PrincipalID: "principal-a", Interface: identity, Revision: 4, Status: operatorchannel.BindingCurrent,
		OperationID: "identity-rebind-a",
	}
	rebind := channelonboarding.Operation{
		OperationID: "channel-rebind-a", PrincipalID: "principal-a", Verb: channelonboarding.VerbRebind, Interface: identity,
		IdentityOperationID: binding.OperationID, Phase: channelonboarding.PhasePublishingActivation,
	}
	otherIdentity := identity
	otherIdentity.ChannelPackID = "provider.telegram.other_channel"
	otherIdentity.SemanticGeneration = "sha256:other-plan"
	otherIdentity = otherIdentity.Normalized()

	for _, test := range []struct {
		name       string
		activation channelonboarding.ConnectedChannelActivation
		binding    operatorchannel.Binding
		operations []channelonboarding.Operation
		want       bool
		wantErr    bool
	}{
		{name: "exact bound rebind supersedes stale activation", activation: activation, binding: binding, operations: []channelonboarding.Operation{rebind}, want: true},
		{name: "same revision remains current", activation: func() channelonboarding.ConnectedChannelActivation {
			value := activation
			value.BindingRevision = 4
			return value
		}(), binding: binding, operations: []channelonboarding.Operation{rebind}},
		{name: "different interface cannot supersede", activation: activation, binding: binding, operations: []channelonboarding.Operation{func() channelonboarding.Operation { value := rebind; value.Interface = otherIdentity; return value }()}},
		{name: "unrelated identity operation cannot supersede", activation: activation, binding: binding, operations: []channelonboarding.Operation{func() channelonboarding.Operation {
			value := rebind
			value.IdentityOperationID = "identity-rebind-b"
			return value
		}()}},
		{name: "terminal rebind cannot supersede", activation: activation, binding: binding, operations: []channelonboarding.Operation{func() channelonboarding.Operation {
			value := rebind
			value.Phase = channelonboarding.PhaseFailed
			return value
		}()}},
		{name: "contradictory principal fails closed", activation: activation, binding: func() operatorchannel.Binding { value := binding; value.PrincipalID = "principal-b"; return value }(), operations: []channelonboarding.Operation{rebind}, wantErr: true},
		{name: "duplicate exact owners fail closed", activation: activation, binding: binding, operations: []channelonboarding.Operation{rebind, func() channelonboarding.Operation {
			value := rebind
			value.OperationID = "channel-rebind-b"
			return value
		}()}, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := activationSupersededByExactRebind(test.activation, test.binding, test.operations)
			if (err != nil) != test.wantErr || got != test.want {
				t.Fatalf("superseded=%v err=%v, want %v err=%v", got, err, test.want, test.wantErr)
			}
		})
	}
}
