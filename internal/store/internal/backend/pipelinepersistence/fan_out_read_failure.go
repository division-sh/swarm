package pipelinepersistence

import (
	"context"
	"errors"

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

func fanOutInvalidRead(cause error) error { return &fanOutReadDefect{cause: cause} }
