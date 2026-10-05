package agentpersistence

import (
	"context"
	"database/sql"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

// The private individual-transition permission is reachable only after this
// complete actor census and the exact attempt's transactional grant stamp.
func (s *AgentPostgresOwner) CommitFlowReadinessSourceSetTransitionsTx(ctx context.Context, attempt *mutationprotocol.Attempt, req manager.FlowReadinessSourceSetRebindRequest, binding manager.ProcessExecutionBinding) ([]manager.AgentLifecycleTransitionResult, error) {
	return commitFlowReadinessSourceSetTransitionsTx(ctx, attempt, req, binding, s.commitAgentLifecycleTransitionTx)
}

func (s *AgentSQLiteOwner) CommitFlowReadinessSourceSetTransitionsTx(ctx context.Context, attempt *mutationprotocol.Attempt, req manager.FlowReadinessSourceSetRebindRequest, binding manager.ProcessExecutionBinding) ([]manager.AgentLifecycleTransitionResult, error) {
	return commitFlowReadinessSourceSetTransitionsTx(ctx, attempt, req, binding, s.commitAgentLifecycleTransitionTx)
}

func commitFlowReadinessSourceSetTransitionsTx(ctx context.Context, attempt *mutationprotocol.Attempt, req manager.FlowReadinessSourceSetRebindRequest, binding manager.ProcessExecutionBinding,
	commit func(context.Context, *mutationprotocol.Attempt, manager.AgentLifecycleTransition, bool) (manager.AgentLifecycleTransitionResult, error),
) ([]manager.AgentLifecycleTransitionResult, error) {
	if attempt == nil {
		return nil, errors.New("readiness actor rebind requires its instance transaction")
	}
	if err := req.Attempt.Validate(); err != nil {
		return nil, err
	}
	if err := binding.Validate(); err != nil {
		return nil, err
	}
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var id, stamp, state, hash string
		if err := tx.QueryRowContext(ctx, `SELECT activation_attempt_id, activation_attempt_grant_id, activation_attempt_state, plan_hash FROM flow_instance_runtime_readiness WHERE run_id=$1 AND instance_path=$2`, req.Attempt.RunID(), req.Attempt.InstancePath()).Scan(&id, &stamp, &state, &hash); err != nil {
			return err
		}
		if id != req.Attempt.ID() || stamp != binding.GenerationGrantID || state != "accepted" || hash != req.PlanHash {
			return errors.New("readiness actor rebind requires the exact atomically stamped instance attempt")
		}
		return verifyFlowReadinessRebindCensusTx(ctx, tx, req)
	})
	if err != nil {
		return nil, err
	}
	results := make([]manager.AgentLifecycleTransitionResult, 0, len(req.Transitions))
	for _, transition := range req.Transitions {
		transition.ProcessBinding = binding
		result, err := commit(ctx, attempt, transition, true)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

func rejectIndividualReadinessRestamp(req manager.AgentLifecycleTransition, instanceRebind bool) error {
	if !instanceRebind && req.OperationKind == "source_set_rebind" && req.Topology.Authority.Kind == agenttopology.AuthorityFlowReadinessPlan && req.TargetGeneration == req.ExpectedGeneration {
		return topologyConflict(req, "readiness_same_attempt_rebind_requires_atomic_instance_operation")
	}
	return nil
}

func validateFlowReadinessSuccessor(req manager.AgentLifecycleTransition, previous lifecycleCell, exists, instanceRebind bool) error {
	if req.OperationKind != "source_set_rebind" || req.Topology.Authority.Kind != agenttopology.AuthorityFlowReadinessPlan {
		return nil
	}
	if instanceRebind {
		return nil
	}
	if !exists {
		return topologyConflict(req, "readiness_successor_predecessor_missing")
	}
	var old agenttopology.Admission
	if err := canonicaljson.DecodeInto(previous.Topology, &old); err != nil {
		return err
	}
	if err := old.Validate(); err != nil {
		return err
	}
	if old.Authority.Kind != agenttopology.AuthorityFlowReadinessPlan {
		return topologyConflict(req, "readiness_successor_predecessor_authority_mismatch")
	}
	predecessor, successor := old.Authority.Readiness, req.Topology.Authority.Readiness
	if predecessor.RunID != successor.RunID || predecessor.InstancePath != successor.InstancePath {
		return topologyConflict(req, "readiness_successor_instance_mismatch")
	}
	oldID, err := flowidentity.ParseActivationAttemptID(predecessor.AttemptID)
	if err != nil {
		return err
	}
	newID, err := flowidentity.ParseActivationAttemptID(successor.AttemptID)
	if err != nil {
		return err
	}
	if newID <= oldID || req.TargetGeneration != req.ExpectedGeneration+1 {
		return topologyConflict(req, "readiness_same_attempt_rebind_requires_atomic_instance_operation")
	}
	return nil
}

func verifyFlowReadinessRebindCensusTx(ctx context.Context, tx *sql.Tx, req manager.FlowReadinessSourceSetRebindRequest) error {
	expected := make(map[string]agenttopology.Admission, len(req.Transitions))
	for _, transition := range req.Transitions {
		owner := transition.Topology.Authority.Readiness
		if transition.OperationKind != "source_set_rebind" || transition.Topology.Authority.Kind != agenttopology.AuthorityFlowReadinessPlan ||
			transition.Identity.RunID != req.Attempt.RunID() || transition.AgentID != transition.Identity.AgentID() ||
			owner.RunID != req.Attempt.RunID() || owner.InstancePath != req.Attempt.InstancePath() || owner.AttemptID != req.Attempt.ID() || owner.PlanFingerprint != req.PlanHash ||
			transition.ExpectedEpoch != transition.TargetEpoch || transition.ExpectedGeneration != transition.TargetGeneration || transition.ExpectedPhase != transition.TargetPhase || transition.Agent != nil {
			return errors.New("readiness rebind must preserve the exact instance actor and attachment")
		}
		if _, duplicate := expected[transition.AgentID]; duplicate {
			return errors.New("readiness rebind duplicates an actor")
		}
		expected[transition.AgentID] = transition.Topology
	}
	rows, err := tx.QueryContext(ctx, `SELECT agent_id, topology_admission FROM agents WHERE run_id=$1 AND lifecycle_phase <> 'terminated' AND topology_authority_kind='flow_readiness_plan'`, req.Attempt.RunID())
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var raw []byte
		if err := rows.Scan(&id, &raw); err != nil {
			return err
		}
		var topology agenttopology.Admission
		if err := canonicaljson.DecodeInto(raw, &topology); err != nil {
			return err
		}
		if err := topology.Validate(); err != nil {
			return err
		}
		if topology.Authority.Readiness.InstancePath != req.Attempt.InstancePath() {
			continue
		}
		wanted, found := expected[id]
		if !found {
			return errors.New("readiness rebind omitted a live instance actor")
		}
		if !topology.Equal(wanted) {
			return errors.New("readiness rebind changed a live actor's attachment topology")
		}
		delete(expected, id)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(expected) != 0 {
		return errors.New("readiness rebind contains an actor absent from the lifecycle census")
	}
	return nil
}
