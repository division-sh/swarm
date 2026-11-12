package serveapp

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
)

// This is access to the existing deployment controls, not another connection
// registry. Each use rechecks the configured account and current parent.
type serveDeploymentTransport struct {
	account       channelonboarding.AccountReference
	read          func(context.Context, channelnative.TransportParent) (channelonboarding.Result, packs.OutboundBindingPlan, error)
	registrations func(context.Context) ([]serveDeploymentRegistration, error)
}

type serveDeploymentRegistration struct {
	result  channelonboarding.Result
	plan    packs.OutboundBindingPlan
	trigger providertriggers.InboundAdmissionPlan
}

func (o *serveDeploymentTransport) selectCurrent(ctx context.Context, parent channelnative.TransportParent) (channelonboarding.Result, packs.OutboundBindingPlan, error) {
	if o == nil || o.read == nil || parent.Validate() != nil || parent.Kind != channelnative.TransportDeploymentRegistration {
		return channelonboarding.Result{}, packs.OutboundBindingPlan{}, channelonboarding.ErrInvalidRequest
	}
	return o.read(ctx, parent)
}

func deploymentParent(selected channelonboarding.Result) channelnative.TransportParent {
	if selected.Binding == nil || selected.IdentityOperation == nil {
		return channelnative.TransportParent{}
	}
	return channelnative.TransportParent{Kind: channelnative.TransportDeploymentRegistration,
		ID: selected.Operation.OperationID, Revision: selected.Operation.Revision,
		BindingOperationID: selected.IdentityOperation.OperationID, BindingOperationRevision: selected.IdentityOperation.Revision,
		ProofID: selected.Binding.ProofID, ProofRevision: selected.Binding.ProofRevision}
}

func (d *serveChannelDeliveryDispatcher) deploymentDelivery(ctx context.Context, candidate render.Candidate) (channelonboarding.Result, packs.OutboundBindingPlan, bool, error) {
	parent := candidate.RequestParent
	if candidate.SourceKind != "response" {
		var found bool
		var err error
		parent, found, err = d.store.CurrentChannelDeliveryParent(ctx)
		if err != nil || !found {
			return channelonboarding.Result{}, packs.OutboundBindingPlan{}, false, err
		}
	}
	if parent.Kind != channelnative.TransportDeploymentRegistration {
		return channelonboarding.Result{}, packs.OutboundBindingPlan{}, false, nil
	}
	selected, plan, err := d.deployment.selectCurrent(ctx, parent)
	if err != nil {
		return selected, plan, true, err
	}
	b := selected.Binding
	if candidate.RequestActivationID != "" || b == nil || b.PrincipalID != candidate.Audience.PrincipalID || b.Interface.Key() != candidate.Audience.InterfaceKey ||
		b.Revision != candidate.BindingRevision || b.ConversationRef != candidate.Audience.ConversationRef ||
		b.ExternalAccountRef != candidate.Audience.ExternalAccountRef || b.ConversationScope != candidate.Audience.ConversationScope {
		return selected, plan, true, fmt.Errorf("deployment delivery parent contradicts selected destination")
	}
	latest, _, err := d.deployment.selectCurrent(ctx, deploymentParent(selected))
	if err != nil || latest.Binding == nil || *latest.Binding != *b {
		return selected, plan, true, errors.Join(channelonboarding.ErrRevisionConflict, err)
	}
	return selected, plan, true, nil
}
