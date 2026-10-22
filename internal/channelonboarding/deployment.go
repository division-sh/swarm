package channelonboarding

import "context"

// DeploymentAccounts never consults flow activations or another scope's table.
func (s *Service) DeploymentAccounts(ctx context.Context) ([]Result, error) {
	if s == nil || !s.deployment {
		return nil, ErrInvalidRequest
	}
	operations, err := s.store.ListChannelOnboardingOperations(ctx)
	if err != nil {
		return nil, err
	}
	accounts := make([]Result, 0, len(operations))
	for _, op := range operations {
		if !op.AccountConfirmed || op.Phase == PhaseRetired {
			continue
		}
		candidate, err := s.currentCandidate(op)
		if err != nil {
			return nil, err
		}
		view, err := s.result(ctx, op, &candidate)
		if err != nil {
			return nil, err
		}
		accounts = append(accounts, view)
	}
	return accounts, nil
}

// Only operation persistence changes scope. Runtime activation/teardown ports
// remain the original owner and are never traversed by deployment completion.
type deploymentStore struct {
	Store
	operations OperationStore
}

func (s deploymentStore) ReserveChannelOnboarding(ctx context.Context, req StartRequest) (Operation, error) {
	return s.operations.ReserveChannelOnboarding(ctx, req)
}
func (s deploymentStore) GetChannelOnboarding(ctx context.Context, id string) (Operation, error) {
	return s.operations.GetChannelOnboarding(ctx, id)
}
func (s deploymentStore) ListChannelOnboardingOperations(ctx context.Context) ([]Operation, error) {
	return s.operations.ListChannelOnboardingOperations(ctx)
}
func (s deploymentStore) AdvanceChannelOnboarding(ctx context.Context, req AdvanceRequest) (Operation, error) {
	return s.operations.AdvanceChannelOnboarding(ctx, req)
}
func (s deploymentStore) SetChannelClientLocale(ctx context.Context, req SetClientLocaleRequest) (Operation, error) {
	return s.operations.SetChannelClientLocale(ctx, req)
}
