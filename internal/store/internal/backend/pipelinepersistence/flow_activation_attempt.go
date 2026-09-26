package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	runtimecanonicaljson "github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeprocessbinding "github.com/division-sh/swarm/internal/runtime/core/processbinding"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

type flowActivationAttemptMutation struct {
	attempt runtimepipeline.DynamicFlowRuntimeActivationAttempt
	reused  bool
}

func (s *PipelinePostgresOwner) BeginDynamicFlowRuntimeActivation(ctx context.Context, plan runtimepipeline.DynamicFlowRuntimeReadinessPlan, revision uint64, binding runtimeprocessbinding.Binding) (runtimepipeline.DynamicFlowRuntimeActivationAdmissionResult, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimepipeline.DynamicFlowRuntimeActivationAdmissionResult{}, err
	}
	return beginDynamicFlowRuntimeActivation(ctx, true, plan, revision, binding, func(ctx context.Context, fn func(context.Context, *mutationprotocol.Attempt) (flowActivationAttemptMutation, error)) mutationprotocol.Result[flowActivationAttemptMutation] {
		return mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, fn)
	})
}

func (s *PipelineSQLiteOwner) BeginDynamicFlowRuntimeActivation(ctx context.Context, plan runtimepipeline.DynamicFlowRuntimeReadinessPlan, revision uint64, binding runtimeprocessbinding.Binding) (runtimepipeline.DynamicFlowRuntimeActivationAdmissionResult, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimepipeline.DynamicFlowRuntimeActivationAdmissionResult{}, err
	}
	return beginDynamicFlowRuntimeActivation(ctx, false, plan, revision, binding, func(ctx context.Context, fn func(context.Context, *mutationprotocol.Attempt) (flowActivationAttemptMutation, error)) mutationprotocol.Result[flowActivationAttemptMutation] {
		return mutationprotocol.RunSQLite(ctx, s.backend, "sqlite flow activation admission", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, fn)
	})
}

func beginDynamicFlowRuntimeActivation(
	ctx context.Context,
	postgres bool,
	expected runtimepipeline.DynamicFlowRuntimeReadinessPlan,
	revision uint64,
	binding runtimeprocessbinding.Binding,
	run func(context.Context, func(context.Context, *mutationprotocol.Attempt) (flowActivationAttemptMutation, error)) mutationprotocol.Result[flowActivationAttemptMutation],
) (runtimepipeline.DynamicFlowRuntimeActivationAdmissionResult, error) {
	plan, err := expected.Normalized()
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeActivationAdmissionResult{}, err
	}
	if revision == 0 {
		return runtimepipeline.DynamicFlowRuntimeActivationAdmissionResult{}, errors.New("flow activation requires a positive plan revision")
	}
	if err := binding.Validate(); err != nil {
		return runtimepipeline.DynamicFlowRuntimeActivationAdmissionResult{}, err
	}
	if plan.BundleHash != binding.BundleHash {
		return runtimepipeline.DynamicFlowRuntimeActivationAdmissionResult{}, errors.New("flow activation plan differs from generation grant bundle")
	}
	expectedJSON, err := runtimecanonicaljson.MarshalPreservingNumberKinds(plan)
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeActivationAdmissionResult{}, err
	}
	outcome := run(ctx, func(txctx context.Context, mutation *mutationprotocol.Attempt) (flowActivationAttemptMutation, error) {
		var admitted flowActivationAttemptMutation
		err := mutation.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			if err := agentpersistence.AuthorizeDynamicFlowActivationTx(txctx, tx, binding, plan.RunID, !postgres); err != nil {
				return err
			}
			current, found, err := loadDynamicFlowRuntimeReadiness(txctx, tx, postgres, plan.RunID, plan.Identity.Route(), true)
			if err != nil {
				return err
			}
			if !found || !current.Eligible() || current.PlanRevision != revision {
				return &runtimepipeline.DynamicFlowRuntimeReadinessObservationConflict{RunID: plan.RunID, InstancePath: plan.Identity.InstancePath, Coordinate: "plan_revision_or_lifecycle"}
			}
			currentJSON, err := runtimecanonicaljson.MarshalPreservingNumberKinds(current.Plan)
			if err != nil {
				return err
			}
			if string(currentJSON) != string(expectedJSON) {
				return &runtimepipeline.DynamicFlowRuntimeReadinessObservationConflict{RunID: plan.RunID, InstancePath: plan.Identity.InstancePath, Coordinate: "plan"}
			}
			query := `SELECT activation_attempt_id::text, activation_attempt_grant_id::text, activation_attempt_revision, activation_attempt_state FROM flow_instance_runtime_readiness WHERE run_id=$1::uuid AND instance_path=$2`
			if !postgres {
				query = `SELECT activation_attempt_id, activation_attempt_grant_id, activation_attempt_revision, activation_attempt_state FROM flow_instance_runtime_readiness WHERE run_id=? AND instance_path=?`
			}
			var oldID, oldGrantID, oldState sql.NullString
			var oldRevision sql.NullInt64
			if err := tx.QueryRowContext(txctx, query, plan.RunID, plan.Identity.InstancePath).Scan(&oldID, &oldGrantID, &oldRevision, &oldState); err != nil {
				return fmt.Errorf("load flow activation attempt: %w", err)
			}
			if oldID.Valid {
				if !oldGrantID.Valid || !oldRevision.Valid || !oldState.Valid {
					return errors.New("flow activation attempt is incomplete")
				}
				if oldGrantID.String == binding.GenerationGrantID && oldRevision.Int64 == int64(revision) && oldState.String == "topology_committed" {
					if current.TopologyReadyAt.IsZero() {
						return errors.New("committed flow activation attempt has no topology completion")
					}
					admitted.attempt, err = runtimepipeline.NewDynamicFlowRuntimeActivationAttempt(oldID.String, plan.RunID, plan.Identity.InstancePath, revision, binding)
					admitted.reused = true
					return err
				}
				if oldState.String != "aborted" {
					otherProcess, err := agentpersistence.FlowActivationPredecessorIsFromAnotherProcessTx(txctx, tx, oldGrantID.String, binding)
					if err != nil {
						return err
					}
					if !otherProcess {
						return errors.New("flow activation predecessor retains unsettled process resources")
					}
				}
			}
			id := uuid.NewString()
			admitted.attempt, err = runtimepipeline.NewDynamicFlowRuntimeActivationAttempt(id, plan.RunID, plan.Identity.InstancePath, revision, binding)
			if err != nil {
				return err
			}
			query = `UPDATE flow_instance_runtime_readiness SET activation_attempt_id=$1::uuid, activation_attempt_grant_id=$2::uuid, activation_attempt_revision=$3, activation_attempt_state='accepted', topology_ready_at=NULL, updated_at=$4 WHERE run_id=$5::uuid AND instance_path=$6 AND plan_revision=$3`
			if !postgres {
				query = `UPDATE flow_instance_runtime_readiness SET activation_attempt_id=?, activation_attempt_grant_id=?, activation_attempt_revision=?, activation_attempt_state='accepted', topology_ready_at=NULL, updated_at=? WHERE run_id=? AND instance_path=? AND plan_revision=?`
			}
			args := []any{id, binding.GenerationGrantID, revision, time.Now().UTC(), plan.RunID, plan.Identity.InstancePath}
			if !postgres {
				args = append(args, revision)
			}
			result, err := tx.ExecContext(txctx, query, args...)
			if err != nil {
				return err
			}
			rows, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if rows != 1 {
				return errors.New("flow activation admission lost its plan revision")
			}
			return nil
		})
		return admitted, err
	})
	value, acknowledged := outcome.Value()
	return runtimepipeline.DynamicFlowRuntimeActivationAdmissionResult{
		Attempt: value.attempt, Acknowledged: acknowledged, Reused: value.reused,
	}, standalonePipelineMutationError(outcome)
}

func authorizeCurrentFlowActivationAttemptTx(ctx context.Context, tx *sql.Tx, postgres bool, attempt runtimepipeline.DynamicFlowRuntimeActivationAttempt) (string, error) {
	if err := attempt.Validate(); err != nil {
		return "", err
	}
	if err := agentpersistence.AuthorizeDynamicFlowActivationTx(ctx, tx, attempt.ProcessBinding(), attempt.RunID(), !postgres); err != nil {
		return "", err
	}
	query := `SELECT activation_attempt_id::text, activation_attempt_grant_id::text, activation_attempt_revision, activation_attempt_state, plan_revision FROM flow_instance_runtime_readiness WHERE run_id=$1::uuid AND instance_path=$2 FOR UPDATE`
	if !postgres {
		query = `SELECT activation_attempt_id, activation_attempt_grant_id, activation_attempt_revision, activation_attempt_state, plan_revision FROM flow_instance_runtime_readiness WHERE run_id=? AND instance_path=?`
	}
	var id, grantID, state sql.NullString
	var revision sql.NullInt64
	var currentRevision int64
	if err := tx.QueryRowContext(ctx, query, attempt.RunID(), attempt.InstancePath()).Scan(&id, &grantID, &revision, &state, &currentRevision); err != nil {
		return "", fmt.Errorf("load current flow activation attempt: %w", err)
	}
	if !id.Valid || id.String != attempt.ID() || !grantID.Valid || grantID.String != attempt.ProcessBinding().GenerationGrantID ||
		!revision.Valid || revision.Int64 != int64(attempt.PlanRevision()) || currentRevision != int64(attempt.PlanRevision()) || !state.Valid {
		return "", errors.New("flow activation attempt is no longer current")
	}
	return state.String, nil
}

// RetireDynamicFlowRuntimeActivationAttempt records settlement of one exact
// predecessor. The caller must first settle its process-local resources.
// Retired grants retain this CAS authority but cannot make forward progress.
func (s *PipelinePostgresOwner) RetireDynamicFlowRuntimeActivationAttempt(ctx context.Context, attempt runtimepipeline.DynamicFlowRuntimeActivationAttempt) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return retireDynamicFlowRuntimeActivationAttempt(ctx, true, attempt, func(ctx context.Context, fn func(context.Context, *mutationprotocol.Attempt) (struct{}, error)) mutationprotocol.Result[struct{}] {
		return mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, fn)
	})
}

func (s *PipelineSQLiteOwner) RetireDynamicFlowRuntimeActivationAttempt(ctx context.Context, attempt runtimepipeline.DynamicFlowRuntimeActivationAttempt) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return retireDynamicFlowRuntimeActivationAttempt(ctx, false, attempt, func(ctx context.Context, fn func(context.Context, *mutationprotocol.Attempt) (struct{}, error)) mutationprotocol.Result[struct{}] {
		return mutationprotocol.RunSQLite(ctx, s.backend, "sqlite flow activation retirement", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, fn)
	})
}

func retireDynamicFlowRuntimeActivationAttempt(
	ctx context.Context,
	postgres bool,
	attempt runtimepipeline.DynamicFlowRuntimeActivationAttempt,
	run func(context.Context, func(context.Context, *mutationprotocol.Attempt) (struct{}, error)) mutationprotocol.Result[struct{}],
) error {
	if err := attempt.Validate(); err != nil {
		return err
	}
	outcome := run(ctx, func(txctx context.Context, mutation *mutationprotocol.Attempt) (struct{}, error) {
		err := mutation.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			query := `UPDATE flow_instance_runtime_readiness SET activation_attempt_state='aborted', topology_ready_at=CASE WHEN plan_revision=$6 THEN NULL ELSE topology_ready_at END, updated_at=$1 WHERE run_id=$2::uuid AND instance_path=$3 AND activation_attempt_id=$4::uuid AND activation_attempt_grant_id=$5::uuid AND activation_attempt_revision=$6 AND activation_attempt_state IN ('accepted', 'topology_committed', 'superseded', 'aborted')`
			if !postgres {
				query = `UPDATE flow_instance_runtime_readiness SET activation_attempt_state='aborted', topology_ready_at=CASE WHEN plan_revision=? THEN NULL ELSE topology_ready_at END, updated_at=? WHERE run_id=? AND instance_path=? AND activation_attempt_id=? AND activation_attempt_grant_id=? AND activation_attempt_revision=? AND activation_attempt_state IN ('accepted', 'topology_committed', 'superseded', 'aborted')`
			}
			args := []any{time.Now().UTC(), attempt.RunID(), attempt.InstancePath(), attempt.ID(), attempt.ProcessBinding().GenerationGrantID, attempt.PlanRevision()}
			if !postgres {
				args = append([]any{attempt.PlanRevision()}, args...)
			}
			result, err := tx.ExecContext(txctx, query, args...)
			if err != nil {
				return err
			}
			rows, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if rows != 1 {
				return errors.New("flow activation retirement does not own the current attempt slot")
			}
			return nil
		})
		return struct{}{}, err
	})
	return standalonePipelineMutationError(outcome)
}

func (s *PipelinePostgresOwner) MarkDynamicFlowRuntimeTopologyReadyForAttempt(ctx context.Context, attempt runtimepipeline.DynamicFlowRuntimeActivationAttempt, expected runtimepipeline.DynamicFlowRuntimeReadinessPlan, readyAt time.Time) (runtimepipeline.DynamicFlowRuntimeTopologyReadyResult, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimepipeline.DynamicFlowRuntimeTopologyReadyResult{}, err
	}
	return markDynamicFlowRuntimeTopologyReadyForAttempt(ctx, true, attempt, expected, readyAt, func(ctx context.Context, fn func(context.Context, *mutationprotocol.Attempt) (struct{}, error)) mutationprotocol.Result[struct{}] {
		return mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, fn)
	})
}

func (s *PipelineSQLiteOwner) MarkDynamicFlowRuntimeTopologyReadyForAttempt(ctx context.Context, attempt runtimepipeline.DynamicFlowRuntimeActivationAttempt, expected runtimepipeline.DynamicFlowRuntimeReadinessPlan, readyAt time.Time) (runtimepipeline.DynamicFlowRuntimeTopologyReadyResult, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimepipeline.DynamicFlowRuntimeTopologyReadyResult{}, err
	}
	return markDynamicFlowRuntimeTopologyReadyForAttempt(ctx, false, attempt, expected, readyAt, func(ctx context.Context, fn func(context.Context, *mutationprotocol.Attempt) (struct{}, error)) mutationprotocol.Result[struct{}] {
		return mutationprotocol.RunSQLite(ctx, s.backend, "sqlite flow activation topology completion", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, fn)
	})
}

func markDynamicFlowRuntimeTopologyReadyForAttempt(
	ctx context.Context,
	postgres bool,
	attempt runtimepipeline.DynamicFlowRuntimeActivationAttempt,
	expected runtimepipeline.DynamicFlowRuntimeReadinessPlan,
	readyAt time.Time,
	run func(context.Context, func(context.Context, *mutationprotocol.Attempt) (struct{}, error)) mutationprotocol.Result[struct{}],
) (runtimepipeline.DynamicFlowRuntimeTopologyReadyResult, error) {
	if err := attempt.Validate(); err != nil {
		return runtimepipeline.DynamicFlowRuntimeTopologyReadyResult{}, err
	}
	plan, err := expected.Normalized()
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeTopologyReadyResult{}, err
	}
	if plan.RunID != attempt.RunID() || plan.Identity.InstancePath != attempt.InstancePath() {
		return runtimepipeline.DynamicFlowRuntimeTopologyReadyResult{}, errors.New("flow activation topology plan differs from attempt identity")
	}
	if readyAt.IsZero() {
		return runtimepipeline.DynamicFlowRuntimeTopologyReadyResult{}, errors.New("flow activation topology completion requires an occurrence time")
	}
	planJSON, err := runtimecanonicaljson.MarshalPreservingNumberKinds(plan)
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeTopologyReadyResult{}, err
	}
	outcome := run(ctx, func(txctx context.Context, mutation *mutationprotocol.Attempt) (struct{}, error) {
		err := mutation.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			state, err := authorizeCurrentFlowActivationAttemptTx(txctx, tx, postgres, attempt)
			if err != nil {
				return err
			}
			current, found, err := loadDynamicFlowRuntimeReadiness(txctx, tx, postgres, plan.RunID, plan.Identity.Route(), true)
			if err != nil {
				return err
			}
			if !found || !current.Eligible() {
				return errors.New("flow activation topology completion requires an eligible instance")
			}
			currentJSON, err := runtimecanonicaljson.MarshalPreservingNumberKinds(current.Plan)
			if err != nil {
				return err
			}
			if string(currentJSON) != string(planJSON) {
				return errors.New("flow activation topology completion plan changed")
			}
			if state == "topology_committed" && !current.TopologyReadyAt.IsZero() {
				return nil
			}
			if state != "accepted" || !current.TopologyReadyAt.IsZero() {
				return errors.New("flow activation topology completion requires one accepted unready attempt")
			}
			query := `UPDATE flow_instance_runtime_readiness SET topology_ready_at=$1, activation_attempt_state='topology_committed', updated_at=$1 WHERE run_id=$2::uuid AND instance_path=$3 AND plan_revision=$4 AND activation_attempt_id=$5::uuid AND activation_attempt_grant_id=$6::uuid AND activation_attempt_revision=$4 AND activation_attempt_state='accepted'`
			if !postgres {
				query = `UPDATE flow_instance_runtime_readiness SET topology_ready_at=?, activation_attempt_state='topology_committed', updated_at=? WHERE run_id=? AND instance_path=? AND plan_revision=? AND activation_attempt_id=? AND activation_attempt_grant_id=? AND activation_attempt_revision=? AND activation_attempt_state='accepted'`
			}
			args := []any{readyAt.UTC(), plan.RunID, plan.Identity.InstancePath, attempt.PlanRevision(), attempt.ID(), attempt.ProcessBinding().GenerationGrantID}
			if !postgres {
				args = []any{readyAt.UTC(), readyAt.UTC(), plan.RunID, plan.Identity.InstancePath, attempt.PlanRevision(), attempt.ID(), attempt.ProcessBinding().GenerationGrantID, attempt.PlanRevision()}
			}
			result, err := tx.ExecContext(txctx, query, args...)
			if err != nil {
				return err
			}
			rows, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if rows != 1 {
				return errors.New("flow activation topology completion lost its exact attempt")
			}
			return nil
		})
		return struct{}{}, err
	})
	_, acknowledged := outcome.Value()
	return runtimepipeline.DynamicFlowRuntimeTopologyReadyResult{Acknowledged: acknowledged}, standalonePipelineMutationError(outcome)
}
