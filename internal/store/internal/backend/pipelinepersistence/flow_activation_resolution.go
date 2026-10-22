package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
)

func (s *PipelinePostgresOwner) ResolveDynamicFlowRuntimeActivation(ctx context.Context, request runtimepipeline.DynamicFlowRuntimeActivationRequest) (runtimepipeline.DynamicFlowRuntimeActivationResolution, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimepipeline.DynamicFlowRuntimeActivationResolution{}, err
	}
	return resolveDynamicFlowRuntimeActivation(ctx, true, request, s.backend.RunReadTransaction)
}

func (s *PipelineSQLiteOwner) ResolveDynamicFlowRuntimeActivation(ctx context.Context, request runtimepipeline.DynamicFlowRuntimeActivationRequest) (runtimepipeline.DynamicFlowRuntimeActivationResolution, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimepipeline.DynamicFlowRuntimeActivationResolution{}, err
	}
	return resolveDynamicFlowRuntimeActivation(ctx, false, request, s.backend.RunReadTransaction)
}

// The caller joins Begin before taking this read snapshot. Resolution validates
// the durable request's historical binding, not live execution authority.
// Retired grants may resolve, not execute.
func resolveDynamicFlowRuntimeActivation(ctx context.Context, postgres bool, request runtimepipeline.DynamicFlowRuntimeActivationRequest,
	read func(context.Context, func(context.Context, *sql.Tx) error) error,
) (runtimepipeline.DynamicFlowRuntimeActivationResolution, error) {
	if err := request.Validate(); err != nil {
		return runtimepipeline.DynamicFlowRuntimeActivationResolution{}, err
	}
	plan := request.Plan()
	hash, err := plan.Hash()
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeActivationResolution{}, err
	}
	var resolved runtimepipeline.DynamicFlowRuntimeActivationResolution
	err = read(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if err := agentpersistence.VerifyFlowActivationRetirementBindingTx(txctx, tx, request.ProcessBinding()); err != nil {
			return fmt.Errorf("verify resolving flow activation request: %w", err)
		}
		query := `SELECT activation_attempt_id::text, activation_attempt_grant_id::text, activation_request_id::text, activation_attempt_state, plan_hash FROM flow_instance_runtime_readiness WHERE run_id=$1::uuid AND instance_path=$2`
		if !postgres {
			query = `SELECT activation_attempt_id, activation_attempt_grant_id, activation_request_id, activation_attempt_state, plan_hash FROM flow_instance_runtime_readiness WHERE run_id=? AND instance_path=?`
		}
		var id, grant, requestID sql.NullString
		var state, actualHash string
		if err := tx.QueryRowContext(txctx, query, plan.RunID, plan.Identity.InstancePath).Scan(&id, &grant, &requestID, &state, &actualHash); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				resolved.Disposition = runtimepipeline.FlowActivationForeign
				return nil
			}
			return err
		}
		ordinal, err := runtimeflowidentity.ParseActivationAttemptID(id.String)
		if err != nil {
			return err
		}
		switch state {
		case "planned", "accepted", "aborted", "retired", "superseded":
		default:
			return fmt.Errorf("invalid activation attempt disposition %q", state)
		}
		if requestID.Valid && requestID.String == request.ID() && ordinal == request.AttemptOrdinal() && state != "planned" {
			if err := agentpersistence.VerifyFlowActivationAttemptProcessTx(txctx, tx, grant.String, request.ProcessBinding()); err != nil {
				return err
			}
			resolved.Attempt, err = runtimepipeline.NewDynamicFlowRuntimeActivationAttempt(id.String, plan.RunID, plan.Identity.InstancePath, request.ProcessBinding())
			resolved.Disposition = runtimepipeline.FlowActivationAdmitted
			return err
		}
		if ordinal == request.Predecessor() && state == request.PredecessorDisposition() && actualHash == hash && (state == "planned" || state == "aborted" || state == "retired") {
			resolved.Disposition = runtimepipeline.FlowActivationUnadmitted
		} else {
			resolved.Disposition = runtimepipeline.FlowActivationForeign
		}
		return nil
	})
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeActivationResolution{}, err
	}
	return resolved, nil
}
