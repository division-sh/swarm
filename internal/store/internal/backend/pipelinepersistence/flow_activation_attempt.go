package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeprocessbinding "github.com/division-sh/swarm/internal/runtime/core/processbinding"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
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
		return runtimepipeline.DynamicFlowRuntimeActivationAdmissionResult{}, errors.New("flow activation requires a positive attachment attempt")
	}
	if err := binding.Validate(); err != nil {
		return runtimepipeline.DynamicFlowRuntimeActivationAdmissionResult{}, err
	}
	if plan.BundleHash != binding.BundleHash {
		return runtimepipeline.DynamicFlowRuntimeActivationAdmissionResult{}, errors.New("flow activation plan differs from generation grant bundle")
	}
	expectedHash, err := plan.Hash()
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
			if !found || !current.Eligible() || current.AttemptOrdinal != revision {
				return &runtimepipeline.DynamicFlowRuntimeReadinessObservationConflict{RunID: plan.RunID, InstancePath: plan.Identity.InstancePath, Coordinate: "activation_attempt_id_or_lifecycle"}
			}
			if current.PlanHash != expectedHash {
				return &runtimepipeline.DynamicFlowRuntimeReadinessObservationConflict{RunID: plan.RunID, InstancePath: plan.Identity.InstancePath, Coordinate: "plan"}
			}
			query := `SELECT activation_attempt_id::text, activation_attempt_grant_id::text, activation_attempt_state FROM flow_instance_runtime_readiness WHERE run_id=$1::uuid AND instance_path=$2`
			if !postgres {
				query = `SELECT activation_attempt_id, activation_attempt_grant_id, activation_attempt_state FROM flow_instance_runtime_readiness WHERE run_id=? AND instance_path=?`
			}
			var oldID, oldGrantID, oldState sql.NullString
			if err := tx.QueryRowContext(txctx, query, plan.RunID, plan.Identity.InstancePath).Scan(&oldID, &oldGrantID, &oldState); err != nil {
				return fmt.Errorf("load flow activation attempt: %w", err)
			}
			if !oldID.Valid || !oldState.Valid || (oldState.String != "planned" && !oldGrantID.Valid) {
				return errors.New("flow activation attempt is incomplete")
			}
			if oldState.String != "planned" {
				if oldGrantID.String == binding.GenerationGrantID && oldState.String == "accepted" {
					admitted.attempt, err = runtimepipeline.NewDynamicFlowRuntimeActivationAttempt(oldID.String, plan.RunID, plan.Identity.InstancePath, binding)
					admitted.reused = true
					return err
				}
				if oldState.String != "aborted" && oldState.String != "retired" {
					otherProcess, err := agentpersistence.FlowActivationPredecessorIsFromAnotherProcessTx(txctx, tx, oldGrantID.String, binding)
					if err != nil {
						return err
					}
					if !otherProcess {
						return errors.New("flow activation predecessor retains unsettled process resources")
					}
				}
			}
			ordinal, err := runtimeflowidentity.ParseActivationAttemptID(oldID.String)
			if err != nil {
				return err
			}
			if oldState.String != "planned" {
				if ordinal == math.MaxInt64 {
					return errors.New("flow activation attempt cannot advance its ordinal")
				}
				ordinal++
			}
			id := strconv.FormatUint(ordinal, 10)
			admitted.attempt, err = runtimepipeline.NewDynamicFlowRuntimeActivationAttempt(id, plan.RunID, plan.Identity.InstancePath, binding)
			if err != nil {
				return err
			}
			query = `UPDATE flow_instance_runtime_readiness SET activation_attempt_id=$1, activation_attempt_grant_id=$2::uuid, activation_attempt_state='accepted', phase='planned', updated_at=$3 WHERE run_id=$4::uuid AND instance_path=$5 AND activation_attempt_id=$6 AND plan_hash=$7 AND activation_attempt_state=$8`
			if !postgres {
				query = `UPDATE flow_instance_runtime_readiness SET activation_attempt_id=?, activation_attempt_grant_id=?, activation_attempt_state='accepted', phase='planned', updated_at=? WHERE run_id=? AND instance_path=? AND activation_attempt_id=? AND plan_hash=? AND activation_attempt_state=?`
			}
			args := []any{id, binding.GenerationGrantID, time.Now().UTC(), plan.RunID, plan.Identity.InstancePath, oldID.String, expectedHash, oldState.String}
			result, err := tx.ExecContext(txctx, query, args...)
			if err != nil {
				return err
			}
			rows, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if rows != 1 {
				return errors.New("flow activation admission lost its observed attachment attempt")
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
	query := `SELECT activation_attempt_id::text, activation_attempt_grant_id::text, activation_attempt_state FROM flow_instance_runtime_readiness WHERE run_id=$1::uuid AND instance_path=$2 FOR UPDATE`
	if !postgres {
		query = `SELECT activation_attempt_id, activation_attempt_grant_id, activation_attempt_state FROM flow_instance_runtime_readiness WHERE run_id=? AND instance_path=?`
	}
	var id, grantID, state sql.NullString
	if err := tx.QueryRowContext(ctx, query, attempt.RunID(), attempt.InstancePath()).Scan(&id, &grantID, &state); err != nil {
		return "", fmt.Errorf("load current flow activation attempt: %w", err)
	}
	if !id.Valid || id.String != attempt.ID() || !grantID.Valid || grantID.String != attempt.ProcessBinding().GenerationGrantID ||
		!state.Valid {
		return "", runtimepipeline.ErrFlowAttachmentStale
	}
	return state.String, nil
}

func authorizeEligibleFlowActivationAttemptTx(ctx context.Context, tx *sql.Tx, postgres bool, attempt runtimepipeline.DynamicFlowRuntimeActivationAttempt) (string, error) {
	state, err := authorizeCurrentFlowActivationAttemptTx(ctx, tx, postgres, attempt)
	if err != nil {
		return "", err
	}
	current, found, err := loadDynamicFlowRuntimeReadiness(ctx, tx, postgres, attempt.RunID(), runtimeflowidentity.RouteForInstancePath(attempt.InstancePath()), true)
	if err != nil {
		return "", err
	}
	if !found || !current.Eligible() || current.AttemptOrdinal != attempt.Ordinal() || current.Plan.BundleHash != attempt.ProcessBinding().BundleHash {
		return "", errors.New("flow activation attempt lost current instance eligibility")
	}
	return state, nil
}

func (s *PipelinePostgresOwner) VerifyDynamicFlowRuntimeActivationAttempt(ctx context.Context, attempt runtimepipeline.DynamicFlowRuntimeActivationAttempt) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return verifyDynamicFlowRuntimeActivationAttempt(ctx, true, attempt, func(ctx context.Context, fn func(context.Context, *mutationprotocol.Attempt) (struct{}, error)) mutationprotocol.Result[struct{}] {
		return mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, fn)
	})
}

func (s *PipelineSQLiteOwner) VerifyDynamicFlowRuntimeActivationAttempt(ctx context.Context, attempt runtimepipeline.DynamicFlowRuntimeActivationAttempt) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return verifyDynamicFlowRuntimeActivationAttempt(ctx, false, attempt, func(ctx context.Context, fn func(context.Context, *mutationprotocol.Attempt) (struct{}, error)) mutationprotocol.Result[struct{}] {
		return mutationprotocol.RunSQLite(ctx, s.backend, "sqlite flow activation verification", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, fn)
	})
}

func verifyDynamicFlowRuntimeActivationAttempt(
	ctx context.Context,
	postgres bool,
	actor runtimepipeline.DynamicFlowRuntimeActivationAttempt,
	run func(context.Context, func(context.Context, *mutationprotocol.Attempt) (struct{}, error)) mutationprotocol.Result[struct{}],
) error {
	if err := actor.Validate(); err != nil {
		return err
	}
	outcome := run(ctx, func(txctx context.Context, mutation *mutationprotocol.Attempt) (struct{}, error) {
		err := mutation.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			state, err := authorizeEligibleFlowActivationAttemptTx(txctx, tx, postgres, actor)
			if err != nil {
				return err
			}
			if state != "accepted" {
				return runtimepipeline.ErrFlowAttachmentStale
			}
			return nil
		})
		return struct{}{}, err
	})
	return standalonePipelineMutationError(outcome)
}

// RetireDynamicFlowRuntimeActivationAttempt records settlement of one exact
// predecessor. The caller must first settle its process-local resources.
// Retired grants retain this CAS authority but cannot make forward progress.
func (s *PipelinePostgresOwner) RetireDynamicFlowRuntimeActivationAttempt(ctx context.Context, attempt runtimepipeline.DynamicFlowRuntimeActivationAttempt) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return retireDynamicFlowRuntimeActivationAttempts(ctx, true, []runtimepipeline.DynamicFlowRuntimeActivationAttempt{attempt}, false, func(ctx context.Context, fn func(context.Context, *mutationprotocol.Attempt) (struct{}, error)) mutationprotocol.Result[struct{}] {
		return mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, fn)
	})
}

func (s *PipelineSQLiteOwner) RetireDynamicFlowRuntimeActivationAttempt(ctx context.Context, attempt runtimepipeline.DynamicFlowRuntimeActivationAttempt) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return retireDynamicFlowRuntimeActivationAttempts(ctx, false, []runtimepipeline.DynamicFlowRuntimeActivationAttempt{attempt}, false, func(ctx context.Context, fn func(context.Context, *mutationprotocol.Attempt) (struct{}, error)) mutationprotocol.Result[struct{}] {
		return mutationprotocol.RunSQLite(ctx, s.backend, "sqlite flow activation retirement", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, fn)
	})
}

// RetireDynamicFlowRuntimeActivationAttempts atomically settles a bounded set
// whose process-local routes, agents, and timers have already joined.
func (s *PipelinePostgresOwner) RetireDynamicFlowRuntimeActivationAttempts(ctx context.Context, attempts []runtimepipeline.DynamicFlowRuntimeActivationAttempt) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return retireDynamicFlowRuntimeActivationAttempts(ctx, true, attempts, false, func(ctx context.Context, fn func(context.Context, *mutationprotocol.Attempt) (struct{}, error)) mutationprotocol.Result[struct{}] {
		return mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, fn)
	})
}

func (s *PipelineSQLiteOwner) RetireDynamicFlowRuntimeActivationAttempts(ctx context.Context, attempts []runtimepipeline.DynamicFlowRuntimeActivationAttempt) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return retireDynamicFlowRuntimeActivationAttempts(ctx, false, attempts, false, func(ctx context.Context, fn func(context.Context, *mutationprotocol.Attempt) (struct{}, error)) mutationprotocol.Result[struct{}] {
		return mutationprotocol.RunSQLite(ctx, s.backend, "sqlite flow activation retirement batch", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, fn)
	})
}

func (s *PipelinePostgresOwner) AbandonDynamicFlowRuntimeActivationAttempt(ctx context.Context, attempt runtimepipeline.DynamicFlowRuntimeActivationAttempt) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return retireDynamicFlowRuntimeActivationAttempts(ctx, true, []runtimepipeline.DynamicFlowRuntimeActivationAttempt{attempt}, true, func(ctx context.Context, fn func(context.Context, *mutationprotocol.Attempt) (struct{}, error)) mutationprotocol.Result[struct{}] {
		return mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, fn)
	})
}

func (s *PipelineSQLiteOwner) AbandonDynamicFlowRuntimeActivationAttempt(ctx context.Context, attempt runtimepipeline.DynamicFlowRuntimeActivationAttempt) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return retireDynamicFlowRuntimeActivationAttempts(ctx, false, []runtimepipeline.DynamicFlowRuntimeActivationAttempt{attempt}, true, func(ctx context.Context, fn func(context.Context, *mutationprotocol.Attempt) (struct{}, error)) mutationprotocol.Result[struct{}] {
		return mutationprotocol.RunSQLite(ctx, s.backend, "sqlite failed flow activation settlement", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, fn)
	})
}

func retireDynamicFlowRuntimeActivationAttempts(
	ctx context.Context,
	postgres bool,
	attempts []runtimepipeline.DynamicFlowRuntimeActivationAttempt,
	failed bool,
	run func(context.Context, func(context.Context, *mutationprotocol.Attempt) (struct{}, error)) mutationprotocol.Result[struct{}],
) error {
	if len(attempts) == 0 || len(attempts) > runtimepipeline.DynamicFlowRuntimeRetirementBatchLimit {
		return errors.New("flow activation retirement requires a bounded nonempty attempt set")
	}
	seen := make(map[string]struct{}, len(attempts))
	for _, attempt := range attempts {
		if err := attempt.Validate(); err != nil {
			return err
		}
		key := attempt.RunID() + "\x00" + attempt.InstancePath()
		if _, duplicate := seen[key]; duplicate {
			return errors.New("flow activation retirement batch contains duplicate instance")
		}
		seen[key] = struct{}{}
	}
	settledState := "retired"
	if failed {
		settledState = "aborted"
	}
	outcome := run(ctx, func(txctx context.Context, mutation *mutationprotocol.Attempt) (struct{}, error) {
		err := mutation.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			// Authority is shared only for identical bindings inside this transaction.
			verified := make(map[runtimeprocessbinding.Binding]struct{}, len(attempts))
			for _, attempt := range attempts {
				binding := attempt.ProcessBinding()
				if _, found := verified[binding]; !found {
					if err := agentpersistence.VerifyFlowActivationRetirementBindingTx(txctx, tx, binding); err != nil {
						return fmt.Errorf("verify retiring flow activation %s: %w", attempt.InstancePath(), err)
					}
					verified[binding] = struct{}{}
				}
				if err := retireDynamicFlowRuntimeActivationAttemptTx(txctx, tx, postgres, attempt, settledState, failed); err != nil {
					return fmt.Errorf("retire flow activation attempt %s: %w", attempt.InstancePath(), err)
				}
			}
			return nil
		})
		return struct{}{}, err
	})
	return standalonePipelineMutationError(outcome)
}

func retireDynamicFlowRuntimeActivationAttemptTx(ctx context.Context, tx *sql.Tx, postgres bool, attempt runtimepipeline.DynamicFlowRuntimeActivationAttempt, settledState string, failed bool) error {
	query := `UPDATE flow_instance_runtime_readiness SET activation_attempt_state=$1, updated_at=$2 WHERE run_id=$3::uuid AND instance_path=$4 AND activation_attempt_id=$5 AND activation_attempt_grant_id=$6::uuid AND activation_attempt_state IN ('accepted', 'superseded', $1)`
	if !postgres {
		query = `UPDATE flow_instance_runtime_readiness SET activation_attempt_state=?, updated_at=? WHERE run_id=? AND instance_path=? AND activation_attempt_id=? AND activation_attempt_grant_id=? AND activation_attempt_state IN ('accepted', 'superseded', ?)`
	}
	args := []any{settledState, time.Now().UTC(), attempt.RunID(), attempt.InstancePath(), attempt.ID(), attempt.ProcessBinding().GenerationGrantID}
	if !postgres {
		args = append(args, settledState)
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		query := `SELECT activation_attempt_id::text, activation_attempt_grant_id::text FROM flow_instance_runtime_readiness WHERE run_id=$1::uuid AND instance_path=$2 FOR UPDATE`
		if !postgres {
			query = `SELECT activation_attempt_id, activation_attempt_grant_id FROM flow_instance_runtime_readiness WHERE run_id=? AND instance_path=?`
		}
		var currentID, currentGrantID sql.NullString
		if err := tx.QueryRowContext(ctx, query, attempt.RunID(), attempt.InstancePath()).Scan(&currentID, &currentGrantID); err != nil {
			return fmt.Errorf("load successor flow activation attempt: %w", err)
		}
		if !currentID.Valid || !currentGrantID.Valid || currentID.String == attempt.ID() {
			return errors.New("flow activation retirement does not own the current attempt slot")
		}
		foreign, err := agentpersistence.FlowActivationRetirementHasForeignSuccessorTx(ctx, tx, currentGrantID.String, attempt.ProcessBinding())
		if err != nil {
			return err
		}
		if !foreign {
			return errors.New("flow activation retirement cannot ignore a same-process successor")
		}
	}
	return nil
}
