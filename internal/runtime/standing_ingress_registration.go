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
		if sameDeclaredIngressTarget(target, owned) {
			found = true
			break
		}
	}
	if !found {
		return InboundTarget{}, false, fmt.Errorf("public ingress target is not owned by the acquired context")
	}
	return u.runtime.admitPublicIngressBinding(u.Context.DeclaredChannelPublication, target)
}

// Binding classification reads only the existing declaration and frozen
// credential owners. It grants no runtime use or permission to publish.
func (rt *Runtime) admitPublicIngressBinding(publication channelonboarding.ChannelActivationPublication, target InboundTarget) (InboundTarget, bool, error) {
	if err := publication.Validate(); err != nil {
		return InboundTarget{}, false, err
	}
	if publication.Mode() != channelonboarding.ChannelActivationPublicationDeclaredOnly ||
		!publication.Generation().Equal(rt.Options.DeclaredChannelPublication.Generation()) {
		return InboundTarget{}, false, fmt.Errorf("ingress registration declaration publication is not the acquired runtime's")
	}
	credential, _, validate, err := rt.standingIngressCredentialScope(target)
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

func declaredAuthorIngress(rt *Runtime, contextDef *BundleContext, target StandingTarget) bool {
	if contextDef == nil || rt == nil {
		return false
	}
	inbound := InboundTarget{
		BundleHash: target.BundleHash, ServiceID: target.ServiceID, FlowPath: target.FlowPath,
		RunID: target.RunID, Generation: target.Generation, PublicationSequence: target.PublicationSequence,
		Alias: target.Alias, Provider: target.Provider, SigningSecret: target.SigningSecret, AdmissionPlan: target.AdmissionPlan,
	}
	for _, owned := range contextDef.StandingTargets {
		if sameDeclaredIngressTarget(inbound, owned) {
			_, managed, err := rt.admitPublicIngressBinding(contextDef.DeclaredChannelPublication, inbound)
			return err == nil && !managed
		}
	}
	return false
}

func sameDeclaredIngressTarget(target InboundTarget, owned StandingTarget) bool {
	return target.BundleHash == owned.BundleHash && target.ServiceID == owned.ServiceID && target.FlowPath == owned.FlowPath &&
		target.RunID == owned.RunID && target.Generation == owned.Generation && target.PublicationSequence == owned.PublicationSequence &&
		target.Alias == owned.Alias && target.Provider == owned.Provider && target.SigningSecret == owned.SigningSecret &&
		target.AdmissionPlan.Generation().Equal(owned.AdmissionPlan.Generation())
}
