package pipelinepersistence

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/durabledata"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

type fanOutReadDefect struct{ cause error }

func (e *fanOutReadDefect) Error() string { return e.cause.Error() }
func (e *fanOutReadDefect) Unwrap() error { return e.cause }

func fanOutReadFailure(ctx context.Context, stage string, cause error, readDisposed bool) error {
	disposition := pipeline.FanOutReadUncertain
	var defect *fanOutReadDefect
	switch {
	case ctx.Err() != nil, errors.Is(cause, fanoutobligation.ErrStaleClaim):
		disposition = pipeline.FanOutReadYield
	case fanOutReadHasUncertainCause(cause):
		disposition = pipeline.FanOutReadUncertain
	case errors.As(cause, &defect):
		disposition = pipeline.FanOutReadDefect
	case readDisposed:
		failure := runtimefailures.Normalize(cause, "runtime.fan_out", stage)
		if failure.Class != runtimefailures.ClassOutcomeUncertain {
			disposition = pipeline.FanOutReadRetry
		}
	}
	return &pipeline.FanOutEvaluationLoadError{Cause: cause, Stage: stage, Disposition: disposition}
}

func fanOutReadHasUncertainCause(err error) bool {
	if failure, ok := err.(*runtimefailures.Error); ok && failure.Failure.Class == runtimefailures.ClassOutcomeUncertain {
		return true
	}
	switch cause := err.(type) {
	case interface{ Unwrap() []error }:
		for _, child := range cause.Unwrap() {
			if fanOutReadHasUncertainCause(child) {
				return true
			}
		}
	case interface{ Unwrap() error }:
		return fanOutReadHasUncertainCause(cause.Unwrap())
	}
	return false
}

func fanOutInvalidRead(cause error) error { return &fanOutReadDefect{cause: cause} }

func fanOutPinnedSourceError(err error) error {
	// Only the pinned-data owner proves this immutable contradiction. A joined
	// disposal or independent read failure is not covered by that statement.
	if runtimefailures.OnlyBranches(err, func(cause error) bool {
		failure, ok := cause.(*durabledata.DomainError)
		return ok && failure != nil && failure.Code == durabledata.CodeIntegrity
	}) {
		return fanOutInvalidRead(err)
	}
	return err
}

func fanOutDecodedReadFailure(decoded bool, err error) error {
	if decoded && err != nil {
		return fanOutInvalidRead(err)
	}
	return err
}
