package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func (s *PipelinePostgresOwner) ResolveDynamicFlowRuntimeActivation(ctx context.Context, request runtimepipeline.DynamicFlowRuntimeActivationRequest) (runtimepipeline.DynamicFlowRuntimeActivationResolution, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimepipeline.DynamicFlowRuntimeActivationResolution{}, err
	}
	return resolveDynamicFlowRuntimeActivation(ctx, true, request, func(ctx context.Context, fn func(context.Context, *mutationprotocol.Attempt) (runtimepipeline.DynamicFlowRuntimeActivationResolution, error)) mutationprotocol.Result[runtimepipeline.DynamicFlowRuntimeActivationResolution] {
		return mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.AuthorityFence, mutationprotocol.Ordinary, nil, nil, fn)
	})
}

func (s *PipelineSQLiteOwner) ResolveDynamicFlowRuntimeActivation(ctx context.Context, request runtimepipeline.DynamicFlowRuntimeActivationRequest) (runtimepipeline.DynamicFlowRuntimeActivationResolution, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimepipeline.DynamicFlowRuntimeActivationResolution{}, err
	}
	return resolveDynamicFlowRuntimeActivation(ctx, false, request, func(ctx context.Context, fn func(context.Context, *mutationprotocol.Attempt) (runtimepipeline.DynamicFlowRuntimeActivationResolution, error)) mutationprotocol.Result[runtimepipeline.DynamicFlowRuntimeActivationResolution] {
		return mutationprotocol.RunSQLite(ctx, s.backend, "sqlite flow activation resolution", mutationprotocol.AuthorityFence, mutationprotocol.Ordinary, nil, nil, fn)
	})
}

// The caller joins Begin before resolving. This transaction serializes on the
// same row as admission, so an unchanged predecessor is not a stale read while
// that admission is still committing. Retired grants may resolve, not execute.
func resolveDynamicFlowRuntimeActivation(ctx context.Context, postgres bool, request runtimepipeline.DynamicFlowRuntimeActivationRequest,
	run func(context.Context, func(context.Context, *mutationprotocol.Attempt) (runtimepipeline.DynamicFlowRuntimeActivationResolution, error)) mutationprotocol.Result[runtimepipeline.DynamicFlowRuntimeActivationResolution],
) (runtimepipeline.DynamicFlowRuntimeActivationResolution, error) {
	if err := request.Validate(); err != nil {
		return runtimepipeline.DynamicFlowRuntimeActivationResolution{}, err
	}
	plan := request.Plan()
	hash, err := plan.Hash()
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeActivationResolution{}, err
	}
	outcome := run(ctx, func(txctx context.Context, mutation *mutationprotocol.Attempt) (runtimepipeline.DynamicFlowRuntimeActivationResolution, error) {
		var resolved runtimepipeline.DynamicFlowRuntimeActivationResolution
		err := mutation.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			if err := agentpersistence.VerifyFlowActivationRetirementBindingTx(txctx, tx, request.ProcessBinding()); err != nil {
				return fmt.Errorf("verify resolving flow activation request: %w", err)
			}
			if !postgres {
				if _, err := tx.ExecContext(txctx, `UPDATE flow_instance_runtime_readiness SET activation_attempt_id=activation_attempt_id WHERE run_id=? AND instance_path=?`, plan.RunID, plan.Identity.InstancePath); err != nil {
					return err
				}
			}
			query := `SELECT activation_attempt_id::text, activation_attempt_grant_id::text, activation_request_id::text, activation_attempt_state, plan_hash FROM flow_instance_runtime_readiness WHERE run_id=$1::uuid AND instance_path=$2 FOR UPDATE`
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
			if requestID.Valid && requestID.String == request.ID() && grant.String == request.ProcessBinding().GenerationGrantID && state != "planned" {
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
		return resolved, err
	})
	resolved, acknowledged := outcome.Value()
	if !acknowledged {
		return runtimepipeline.DynamicFlowRuntimeActivationResolution{}, standalonePipelineMutationError(outcome)
	}
	return resolved, standalonePipelineMutationError(outcome)
}
