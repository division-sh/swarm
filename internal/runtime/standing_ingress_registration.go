package runtime

import (
	"fmt"

	"github.com/division-sh/swarm/internal/channelonboarding"
)

// AdmitPublicIngressTarget classifies only the context already acquired for
// this request and retains its existing credential-replacement fence.
func (u *RuntimeContextUse) AdmitPublicIngressTarget(target InboundTarget) (InboundTarget, bool, error) {
	if u == nil || u.runtime == nil || u.Context.BundleHash() != target.BundleHash {
		return InboundTarget{}, false, fmt.Errorf("ingress registration requires its exact runtime context")
	}
	found := false
	for _, owned := range u.Context.StandingTargets {
		if target.BundleHash == owned.BundleHash && target.ServiceID == owned.ServiceID && target.FlowPath == owned.FlowPath &&
			target.RunID == owned.RunID && target.Generation == owned.Generation && target.PublicationSequence == owned.PublicationSequence &&
			target.Alias == owned.Alias && target.Provider == owned.Provider && target.SigningSecret == owned.SigningSecret &&
			target.AdmissionPlan.Generation().Equal(owned.AdmissionPlan.Generation()) {
			found = true
			break
		}
	}
	if !found {
		return InboundTarget{}, false, fmt.Errorf("public ingress target is not owned by the acquired context")
	}
	publication := u.Context.DeclaredChannelPublication
	if err := publication.Validate(); err != nil {
		return InboundTarget{}, false, err
	}
	if publication.Mode() != channelonboarding.ChannelActivationPublicationDeclaredOnly ||
		!publication.Generation().Equal(u.runtime.Options.DeclaredChannelPublication.Generation()) {
		return InboundTarget{}, false, fmt.Errorf("ingress registration declaration publication is not the acquired runtime's")
	}
	credential, _, validate, err := u.runtime.standingIngressCredentialScope(target)
	if err != nil {
		return InboundTarget{}, false, err
	}
	if !credential.enabled {
		return InboundTarget{}, false, fmt.Errorf("ingress registration binding is not admitted")
	}
	managed := credential.operationID != ""
	selector := standingIngressSelector(target.FlowPath, target.Provider)
	for _, binding := range publication.Bindings() {
		if binding.RegistrationTarget() != selector {
			continue
		}
		if _, present := binding.Registration(); !present {
			return InboundTarget{}, false, fmt.Errorf("ingress registration declaration has no compiled recipe")
		}
		managed = true
	}
	target.registrationAdmission = validate
	return target, managed, nil
}
