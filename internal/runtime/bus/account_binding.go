package bus

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// selectedAccountInstance consumes the authenticated carrier and the same
// snapshot index used by ordinary route planning. Customer fields cannot select
// another account, and no account-to-flow registry is maintained.
func (p selectedRunTargetOwnerProjection) selectedAccountInstance(ctx context.Context, event events.Event) (flowidentity.Instance, bool, error) {
	publication, authenticated := authenticatedProviderPublicationForEvent(ctx, event)
	if !authenticated {
		return flowidentity.Instance{}, false, nil
	}
	bundle, ok := semanticview.Bundle(p.source)
	if !ok || bundle == nil {
		return flowidentity.Instance{}, false, nil
	}
	rooted, err := bundle.RootedSource()
	if err != nil {
		return flowidentity.Instance{}, false, err
	}
	providers, err := packadmission.AdmitProviderSource(rooted, bundle.Platform)
	if err != nil {
		return flowidentity.Instance{}, false, err
	}
	flow := event.RoutingSource().Route().FlowID
	scope, err := providers.Scope(flow)
	if err != nil {
		return flowidentity.Instance{}, false, err
	}
	provider, declaration, err := scope.ConnectionAccount(bundle)
	if err != nil {
		return flowidentity.Instance{}, false, err
	}
	plan, held := provider.ChannelPlan()
	if !held || declaration.FlowID == "" {
		return flowidentity.Instance{}, false, nil
	}
	account := channelonboarding.AccountReference{Provider: plan.Provider(), Account: publication.account}
	if account.Validate() != nil || p.instances == nil {
		return flowidentity.Instance{}, true, fmt.Errorf("account-selected ingress requires its authenticated receiving account and native instance index")
	}
	fact, present := correlation.SourceArtifactFactFromContext(ctx)
	if !present {
		return flowidentity.Instance{}, true, fmt.Errorf("account-selected ingress requires its admitted source")
	}
	lookup, err := pipeline.NewFlowInstanceLookupScope(p.source, fact, event.RunID(), []string{flow}, nil)
	if err != nil {
		return flowidentity.Instance{}, true, err
	}
	instances, err := p.instances.ListFlowInstances(ctx, lookup)
	if err != nil {
		return flowidentity.Instance{}, true, err
	}
	var selected flowidentity.Instance
	for _, observed := range instances {
		instance, err := observed.WorkflowInstance()
		if err != nil {
			return selected, true, err
		}
		value, assigned := instance.Fields["operator_account"].(string)
		if declaration.FromPolicy {
			value, assigned = declaration.Policy, true
		}
		if !assigned || value == "" || !instance.TerminatedAt.IsZero() {
			continue
		}
		candidate, err := channelonboarding.CanonicalAccount(plan.Provider(), value)
		if err != nil {
			return selected, true, fmt.Errorf("flow %s operator_account: %w", flow, err)
		}
		if candidate != account {
			continue
		}
		if selected.InstancePath != "" {
			return selected, true, fmt.Errorf("account is held by competing flow instances")
		}
		selected = observed.Identity()
	}
	return selected, true, nil
}
