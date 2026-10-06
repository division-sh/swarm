package delivery

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func validateSelectedProviderOrigin(ctx context.Context, tx *sql.Tx, adapter *Adapter, claim deliverylifecycle.Claim, actor agentidentity.Identity, authority deliverylifecycle.ExecutionAuthority) error {
	if authority.Validate() != nil || authority.Kind() != deliverylifecycle.ExecutionAuthoritySelectedContractFork || actor.Validate() != nil || actor.RunID != authority.ForkRunID() {
		return fmt.Errorf("selected provider origin requires exact fork execution and actor evidence")
	}
	record, _, err := adapter.requireCurrentClaim(ctx, tx, claim)
	if err != nil {
		return err
	}
	if !record.Authority.Equal(authority) || record.Route.AgentIdentity != actor.Normalize() || claim.SubscriberClass() != deliverylifecycle.SubscriberAgent || claim.SubscriberID() != actor.AgentID() {
		return fmt.Errorf("selected provider claim contradicts its exact execution/actor ownership")
	}
	return nil
}

// This validates recorded possession, not permission to execute. Recovery never
// replaces the delivery's selected generation or source with a current grant.
func validateSelectedOriginExecution(ctx context.Context, tx *sql.Tx, adapter *Adapter, claim deliverylifecycle.Claim, executionID string) error {
	if _, err := adapter.readProviderOriginRecoveryDisposition(ctx, tx, claim, false); err != nil {
		return err
	}
	record, err := adapter.loadByID(ctx, tx, claim.DeliveryID(), false)
	if err != nil {
		return err
	}
	fact, found := correlation.SourceArtifactFactFromContext(ctx)
	if !found || fact.Validate() != nil || record.Authority.Kind() != deliverylifecycle.ExecutionAuthoritySelectedContractFork || record.Authority.ExecutionID() != executionID || record.Authority.SourceArtifact().BundleHash() != fact.BundleHash() {
		return fmt.Errorf("selected recovery origin differs from its exact execution/source")
	}
	var runID, bundleHash string
	var generation uint64
	err = tx.QueryRowContext(ctx, `SELECT CAST(e.fork_run_id AS TEXT),e.generation,r.bundle_hash
		FROM run_fork_selected_contract_runtime_executions e JOIN runs r ON r.run_id=e.fork_run_id
		WHERE e.execution_id=$1`, executionID).Scan(&runID, &generation, &bundleHash)
	if err != nil {
		return err
	}
	if record.RunID != runID || record.Authority.ForkRunID() != runID || record.Authority.Generation() != generation || fact.BundleHash() != bundleHash {
		return fmt.Errorf("selected recovery origin contradicts its stored generation")
	}
	return nil
}

func (s *DeliveryPostgresOwner) ValidateSelectedOriginExecutionTx(ctx context.Context, tx *sql.Tx, claim deliverylifecycle.Claim, executionID string) error {
	return validateSelectedOriginExecution(ctx, tx, postgresDeliveryAdapter, claim, executionID)
}

func (s *DeliverySQLiteOwner) ValidateSelectedOriginExecutionTx(ctx context.Context, tx *sql.Tx, claim deliverylifecycle.Claim, executionID string) error {
	return validateSelectedOriginExecution(ctx, tx, sqliteDeliveryAdapter, claim, executionID)
}

func (s *DeliveryPostgresOwner) ValidateSelectedProviderOriginTx(ctx context.Context, tx *sql.Tx, claim deliverylifecycle.Claim, actor agentidentity.Identity, authority deliverylifecycle.ExecutionAuthority) error {
	return validateSelectedProviderOrigin(ctx, tx, postgresDeliveryAdapter, claim, actor, authority)
}

func (s *DeliverySQLiteOwner) ValidateSelectedProviderOriginTx(ctx context.Context, tx *sql.Tx, claim deliverylifecycle.Claim, actor agentidentity.Identity, authority deliverylifecycle.ExecutionAuthority) error {
	return validateSelectedProviderOrigin(ctx, tx, sqliteDeliveryAdapter, claim, actor, authority)
}
