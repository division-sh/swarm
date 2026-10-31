package pipeline

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Carry only the account key out of the acknowledged mutation. The receiver
// resolves current holders itself, after the entity lock has been released.
func (pc *PipelineCoordinator) committedAccountHolder(flow string, before, after map[string]any) (*channelonboarding.AccountReference, error) {
	if pc.accountHolderCreated == nil {
		return nil, nil
	}
	bundle, ok := semanticview.Bundle(pc.SemanticSource())
	if !ok {
		return nil, fmt.Errorf("account holder notification requires the admitted contract")
	}
	providers, err := packadmission.ProviderSourceForBundle(bundle)
	if err != nil {
		return nil, err
	}
	scope, err := providers.Scope(flow)
	if err != nil {
		return nil, err
	}
	provider, declaration, err := scope.ConnectionAccount(bundle)
	if err != nil {
		return nil, err
	}
	plan, held := provider.ChannelPlan()
	if !held || plan.Provider() != "telegram" {
		return nil, nil
	}
	previous, _ := declaration.Value(before)
	value, present := declaration.Value(after)
	if !present || value == "" || value == previous {
		return nil, nil
	}
	ref, err := channelonboarding.CanonicalAccount(plan.Provider(), value)
	if err != nil {
		return nil, err
	}
	return &ref, nil
}

func (pc *PipelineCoordinator) notifyAccountHolder(ctx context.Context, ref *channelonboarding.AccountReference) error {
	if ref == nil || pc.accountHolderCreated == nil {
		return nil
	}
	return pc.accountHolderCreated(ctx, *ref)
}
