package channelonboarding

import (
	"context"
	"strings"

	domain "github.com/division-sh/swarm/internal/channelonboarding"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
)

// This port selects persistence scope before any lookup. It shares the
// onboarding writer and phase checks; it never searches another scope by ID.
type deploymentOperations struct{ runner }

type deploymentRunner struct{ runner }

func (deploymentRunner) deployment() bool                                        { return true }
func (deploymentRunner) publishChannelChange(bool, render.ReconcileDemand) error { return nil }

func (s *PostgresOwner) DeploymentOperations() domain.OperationStore {
	return deploymentOperations{deploymentRunner{postgresRunner{s}}}
}

func (s *SQLiteOwner) DeploymentOperations() domain.OperationStore {
	return deploymentOperations{deploymentRunner{sqliteRunner{s}}}
}

func (s deploymentOperations) ReserveChannelOnboarding(ctx context.Context, request domain.StartRequest) (domain.Operation, error) {
	return reserve(ctx, s.runner, request)
}

func (s deploymentOperations) GetChannelOnboarding(ctx context.Context, id string) (domain.Operation, error) {
	return getOperation(ctx, s.runner, id)
}

func (s deploymentOperations) ListChannelOnboardingOperations(ctx context.Context) ([]domain.Operation, error) {
	return listOperations(ctx, s.runner)
}

func (s deploymentOperations) AdvanceChannelOnboarding(ctx context.Context, request domain.AdvanceRequest) (domain.Operation, error) {
	return advance(ctx, s.runner, request)
}

func (s deploymentOperations) SetChannelClientLocale(context.Context, domain.SetClientLocaleRequest) (domain.Operation, error) {
	return domain.Operation{}, domain.ErrInvalidRequest
}

func operationTable(deployment bool) string {
	if deployment {
		return "deployment_channel_operations"
	}
	return "channel_onboarding_operations"
}

func operationQuery(deployment bool) string {
	if !deployment {
		return operationSelect
	}
	query := strings.Replace(operationSelect, "FALSE,'','',FALSE,FALSE FROM channel_onboarding_operations", "TRUE,account_reference,expected_account,account_confirmed,logout_completed FROM deployment_channel_operations", 1)
	query = strings.Replace(query, "bundle_hash,bundle_identity,pack_inventory_generation,runtime_instance_id,context_publication_generation,plan_generation,",
		"COALESCE(bundle_hash,''),bundle_identity,pack_inventory_generation,COALESCE(CAST(runtime_instance_id AS TEXT),''),context_publication_generation,plan_generation,", 1)
	return query
}
