package agentpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

func (s *AgentPostgresOwner) ReceiverExecutionReadyTx(ctx context.Context, tx *sql.Tx, route events.DeliveryRoute, authority deliverylifecycle.ExecutionAuthority, claim bool) (bool, error) {
	return receiverExecutionReadyTx(ctx, tx, route, authority, claim, false)
}

func (s *AgentSQLiteOwner) ReceiverExecutionReadyTx(ctx context.Context, tx *sql.Tx, route events.DeliveryRoute, authority deliverylifecycle.ExecutionAuthority, claim bool) (bool, error) {
	return receiverExecutionReadyTx(ctx, tx, route, authority, claim, true)
}

func receiverExecutionReadyTx(ctx context.Context, tx *sql.Tx, route events.DeliveryRoute, authority deliverylifecycle.ExecutionAuthority, claim, sqlite bool) (bool, error) {
	state, ready, err := ReceiverExecutionReady(ctx, tx, route, authority, sqlite)
	if err != nil || !ready || !claim {
		return ready, err
	}
	if err := AuthorizeReceiverExecutionTx(ctx, tx, state, sqlite); err != nil {
		return false, err
	}
	// Grant authorization holds the shared mutation fence through claim commit.
	// Recheck and lock the cell afterwards so retirement/replacement cannot race
	// a claim based on the earlier read-only readiness observation.
	current, entityID, found, err := loadAgentLifecycleState(ctx, tx, route.AgentIdentity, sqlite, true)
	if err != nil {
		return false, err
	}
	if !found || current.Generation != state.Generation || current.Phase != state.Phase || current.RunMode != state.RunMode || current.ConfigRevision != state.ConfigRevision || current.ProcessBinding != state.ProcessBinding {
		return false, nil
	}
	if err := validateReceiverActorEntity(entityID, route); err != nil {
		return false, err
	}
	return true, nil
}

// ReceiverExecutionReady consumes the admitted lifecycle cell, not a speculative
// descriptor. Read scans can advertise readiness; the claim transaction must
// additionally fence the same retained grant through AuthorizeReceiverExecutionTx.
func ReceiverExecutionReady(ctx context.Context, q rowQueryer, route events.DeliveryRoute, authority deliverylifecycle.ExecutionAuthority, sqlite bool) (manager.AgentLifecycleState, bool, error) {
	state, entityID, found, err := loadAgentLifecycleState(ctx, q, route.AgentIdentity, sqlite, false)
	if err != nil || !found {
		return state, false, err
	}
	// Empty is an exact actor entity value. Construction dependency belongs to
	// the flow and cannot grant that entity to an independently entityless actor.
	if err := validateReceiverActorEntity(entityID, route); err != nil {
		return state, false, err
	}
	if state.Phase != manager.AgentLifecycleRunning {
		return state, false, nil
	}
	var raw []byte
	if err := q.QueryRowContext(ctx, `SELECT snapshot FROM runtime_generation_grants WHERE grant_id=$1 ORDER BY state_version DESC LIMIT 1`, state.ProcessBinding.GenerationGrantID).Scan(&raw); err != nil {
		return state, false, fmt.Errorf("load receiver lifecycle grant: %w", err)
	}
	var grant startupownership.GrantEvidence
	if err := canonicaljson.DecodeInto(raw, &grant); err != nil {
		return state, false, err
	}
	if err := grant.Validate(); err != nil {
		return state, false, err
	}
	if grant.State != startupownership.GrantAdmitted {
		return state, false, nil
	}
	if state.ProcessBinding != processBindingForGrant(grant) {
		return state, false, fmt.Errorf("receiver lifecycle grant differs from its admitted process binding")
	}
	if authority.SourceArtifact().BundleHash() != grant.BundleHash {
		return state, false, nil
	}
	switch authority.Kind() {
	case deliverylifecycle.ExecutionAuthorityNormalRuntime:
		return state, grant.SelectedFork == nil && state.RunMode == manager.AgentRunModeStandard &&
			grant.GrantID == authority.ExecutionID() && grant.RuntimeGeneration == authority.Generation(), nil
	case deliverylifecycle.ExecutionAuthoritySelectedContractFork:
		return state, grant.SelectedFork != nil && state.RunMode == manager.AgentRunModeAuthoritativeDeliveryOnly &&
			grant.SelectedFork.ExecutionID == authority.ExecutionID() && grant.SelectedFork.ExecutionGeneration == authority.Generation() &&
			grant.SelectedFork.ForkRunID == route.AgentIdentity.RunID, nil
	default:
		return state, false, fmt.Errorf("receiver has unsupported execution authority")
	}
}

func validateReceiverActorEntity(entityID string, route events.DeliveryRoute) error {
	if !route.Target.Empty() && entityID != route.Target.Route().EntityID {
		return fmt.Errorf("receiver lifecycle entity contradicts admitted actor ownership")
	}
	return nil
}

// AuthorizeReceiverExecutionTx uses the same durable execution fence as lifecycle
// mutations. A ready projection alone never grants selected or normal execution.
func AuthorizeReceiverExecutionTx(ctx context.Context, tx *sql.Tx, state manager.AgentLifecycleState, sqlite bool) error {
	return AuthorizeGenerationMutationTx(ctx, tx, manager.AgentLifecycleTransition{
		Identity: state.Identity, ProcessBinding: state.ProcessBinding,
	}, sqlite)
}
