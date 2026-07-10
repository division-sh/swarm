package engine

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/computemodule"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

type PersistedEffects struct {
	StateMutation StateMutation
	TimerIntents  []TimerIntent
	EmitIntents   []EmitIntent
}

func NormalizeFailure(err error, component, operation string) *failures.Error {
	if err == nil {
		return nil
	}
	if existing, ok := failures.As(err); ok {
		return existing
	}
	if errors.Is(err, ErrChainDepthExceeded) {
		return failures.New(
			failures.ClassChainDepthExceeded,
			"chain_depth_limit",
			component,
			operation,
			nil,
		).(*failures.Error)
	}
	if errors.Is(err, ErrFanOutBoundExceeded) {
		return failures.Wrap(
			failures.ClassFanOutBoundExceeded,
			"fan_out_bound",
			component,
			operation,
			nil,
			err,
		).(*failures.Error)
	}
	var computeErr *computemodule.Error
	if errors.As(err, &computeErr) && computeErr != nil {
		attributes := map[string]any{
			"module_id": computeErr.ModuleID,
			"row_id":    computeErr.RowID,
		}
		if computeErr.Code == computemodule.CodeOutputSize {
			attributes["limit_kind"] = "compute_output_bytes"
			attributes["limit"] = computeErr.Limit
			attributes["actual"] = computeErr.Actual
			return failures.Wrap(
				failures.ClassDataLimitExceeded,
				string(computeErr.Code),
				component,
				operation,
				attributes,
				err,
			).(*failures.Error)
		}
		return failures.Wrap(
			failures.ClassComputeFailure,
			string(computeErr.Code),
			component,
			operation,
			attributes,
			err,
		).(*failures.Error)
	}

	switch {
	case errors.Is(err, ErrEmitPayloadContractViolation):
		return failures.Wrap(
			failures.ClassSchemaInvalid,
			"emit_payload_contract_violation",
			component,
			operation,
			nil,
			err,
		).(*failures.Error)
	case errors.Is(err, ErrMissingStateRepo),
		errors.Is(err, ErrMissingTransaction),
		errors.Is(err, ErrMissingEntityLocker),
		errors.Is(err, ErrMissingOutbox),
		errors.Is(err, ErrMissingDispatcher),
		errors.Is(err, ErrEmitPersistencePrerequisite):
		return failures.Wrap(
			failures.ClassDependencyUnavailable,
			"runtime_dependency_missing",
			component,
			operation,
			nil,
			err,
		).(*failures.Error)
	case errors.Is(err, context.DeadlineExceeded):
		return failures.Wrap(
			failures.ClassTimeout,
			"context_deadline_exceeded",
			component,
			operation,
			nil,
			err,
		).(*failures.Error)
	default:
		return failures.FromError(err, component, operation)
	}
}

func FailureDispositionFor(err error) FailureDisposition {
	if err == nil {
		return FailureDispositionNone
	}
	if errors.Is(err, ErrChainDepthExceeded) {
		return FailureDispositionDeadLetter
	}
	failure := NormalizeFailure(err, "engine", "failure_disposition")
	if failure.Failure.Retryable {
		return FailureDispositionRetry
	}
	return FailureDispositionTerminal
}

func SetExecutionFailure(result *ExecutionResult, err error, component, operation string) {
	if result == nil {
		return
	}
	failure := NormalizeFailure(err, component, operation)
	if failure == nil {
		result.Failure = nil
		result.FailureDisposition = FailureDispositionNone
		return
	}
	envelope := failure.Failure
	result.Failure = &envelope
	result.FailureDisposition = FailureDispositionFor(failure)
}

func PersistAndDispatch(ctx context.Context, runner TransactionRunner, outbox OutboxWriter, dispatcher PostCommitDispatcher, intents []EmitIntent) error {
	if runner == nil {
		return ErrMissingTransaction
	}
	if outbox == nil {
		return ErrMissingOutbox
	}
	if dispatcher == nil {
		return ErrMissingDispatcher
	}
	if err := runner.Run(ctx, func(tx Tx) error {
		return outbox.WriteOutbox(tx.Context(), intents)
	}); err != nil {
		return err
	}
	return dispatcher.DispatchPostCommit(ctx, intents)
}
