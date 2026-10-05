package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
)

// The composition owner commits this stamp and the complete instance actor set
// in its retained lifecycle transaction. No attachment progress is fabricated.
func RebindFlowActivationAttemptTx(ctx context.Context, tx *sql.Tx, postgres bool, req manager.FlowReadinessSourceSetRebindRequest, binding processbinding.Binding) (pipeline.DynamicFlowRuntimeActivationAttempt, error) {
	if err := req.Attempt.Validate(); err != nil {
		return pipeline.DynamicFlowRuntimeActivationAttempt{}, err
	}
	old := req.Attempt.ProcessBinding()
	if !old.SameProcessExecution(binding) || (!old.Equal(binding) &&
		(old.RuntimeGeneration == ^uint64(0) || binding.RuntimeGeneration != old.RuntimeGeneration+1 || old.GenerationGrantID == binding.GenerationGrantID)) {
		return pipeline.DynamicFlowRuntimeActivationAttempt{}, errors.New("readiness rebind requires the exact same-process adjacent generation")
	}
	if err := agentpersistence.VerifyFlowActivationRetirementBindingTx(ctx, tx, old); err != nil {
		return pipeline.DynamicFlowRuntimeActivationAttempt{}, err
	}
	if err := agentpersistence.AuthorizeDynamicFlowActivationTx(ctx, tx, binding, req.Attempt.RunID(), !postgres); err != nil {
		return pipeline.DynamicFlowRuntimeActivationAttempt{}, err
	}
	current, found, err := loadDynamicFlowRuntimeReadiness(ctx, tx, postgres, req.Attempt.RunID(), flowidentity.RouteForInstancePath(req.Attempt.InstancePath()), true)
	if err != nil {
		return pipeline.DynamicFlowRuntimeActivationAttempt{}, err
	}
	if !found || !current.Eligible() || current.AttemptState != "accepted" || current.AttemptOrdinal != req.Attempt.Ordinal() || current.PlanHash != req.PlanHash {
		return pipeline.DynamicFlowRuntimeActivationAttempt{}, pipeline.ErrFlowAttachmentStale
	}
	query := `SELECT activation_attempt_grant_id::text FROM flow_instance_runtime_readiness WHERE run_id=$1::uuid AND instance_path=$2`
	if !postgres {
		query = `SELECT activation_attempt_grant_id FROM flow_instance_runtime_readiness WHERE run_id=? AND instance_path=?`
	}
	var stamp string
	if err := tx.QueryRowContext(ctx, query, req.Attempt.RunID(), req.Attempt.InstancePath()).Scan(&stamp); err != nil {
		return pipeline.DynamicFlowRuntimeActivationAttempt{}, err
	}
	if err := agentpersistence.VerifyFlowActivationAttemptProcessTx(ctx, tx, stamp, binding); err != nil {
		return pipeline.DynamicFlowRuntimeActivationAttempt{}, err
	}
	if stamp != binding.GenerationGrantID {
		query = `UPDATE flow_instance_runtime_readiness SET activation_attempt_grant_id=$1::uuid, updated_at=$2 WHERE run_id=$3::uuid AND instance_path=$4 AND activation_attempt_id=$5 AND plan_hash=$6 AND activation_attempt_state='accepted'`
		if !postgres {
			query = `UPDATE flow_instance_runtime_readiness SET activation_attempt_grant_id=?, updated_at=? WHERE run_id=? AND instance_path=? AND activation_attempt_id=? AND plan_hash=? AND activation_attempt_state='accepted'`
		}
		result, err := tx.ExecContext(ctx, query, binding.GenerationGrantID, time.Now().UTC(), req.Attempt.RunID(), req.Attempt.InstancePath(), req.Attempt.ID(), req.PlanHash)
		if err != nil {
			return pipeline.DynamicFlowRuntimeActivationAttempt{}, err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return pipeline.DynamicFlowRuntimeActivationAttempt{}, err
		}
		if count != 1 {
			return pipeline.DynamicFlowRuntimeActivationAttempt{}, fmt.Errorf("readiness rebind lost its exact attempt: count=%d", count)
		}
	}
	return pipeline.NewDynamicFlowRuntimeActivationAttempt(req.Attempt.ID(), req.Attempt.RunID(), req.Attempt.InstancePath(), binding)
}
