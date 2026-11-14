package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func fanOutReadWaitAge(ctx context.Context, db pipelineQueryer, postgres bool, rawSince any, now time.Time) (*int64, error) {
	since, known, err := sqliteTimeValue(rawSince)
	if err != nil {
		return nil, errors.New("fan-out read wait summary has invalid observed failure time")
	}
	if !known {
		return nil, nil
	}
	// A retry may commit between the caller's timestamp and this snapshot.
	// Observe after the row read rather than inventing a zero/negative age.
	observed, err := fanOutAdmissionTime(ctx, db, postgres, time.Now)
	if err != nil {
		return nil, err
	}
	if observed.Before(now) {
		observed = now
	}
	if since.After(observed) {
		return nil, errors.New("fan-out read failure is later than its native observation")
	}
	age := observed.Sub(since).Milliseconds()
	return &age, nil
}

func decodeFanOutReadRetry(rawAt any, delay sql.NullInt64, rawSince any, rawFailure []byte) (*fanoutobligation.ReadRetry, error) {
	if rawAt == nil && !delay.Valid && rawSince == nil && rawFailure == nil {
		return nil, nil
	}
	at, present, err := sqliteTimeValue(rawAt)
	if err != nil || !present || !delay.Valid || delay.Int64 < fanoutobligation.InitialReadRetryDelay.Milliseconds() || delay.Int64 > fanoutobligation.MaxReadRetryDelay.Milliseconds() {
		return nil, errors.New("fan-out read pacing requires exact due time and delay")
	}
	r := &fanoutobligation.ReadRetry{NotBefore: at, Delay: time.Duration(delay.Int64) * time.Millisecond}
	if rawSince != nil || rawFailure != nil {
		since, present, err := sqliteTimeValue(rawSince)
		if err != nil || !present {
			return nil, errors.New("fan-out read failure requires its observed time")
		}
		failure, err := runtimefailures.UnmarshalEnvelope(rawFailure)
		if err != nil {
			return nil, err
		}
		r.Since, r.Failure = &since, &failure
	}
	return r, r.Validate()
}

func encodeFanOutReadFailure(r fanoutobligation.ReadRetry) (any, any, error) {
	if err := r.Validate(); err != nil {
		return nil, nil, err
	}
	if r.Failure == nil {
		return nil, nil, nil
	}
	raw, err := runtimefailures.MarshalEnvelope(*r.Failure)
	return r.Since.UTC(), string(raw), err
}

func fanOutRetryReleaseEvidence(intent fanoutobligation.Intent, request pipeline.FanOutRetryableRelease, now time.Time) ([]any, error) {
	failure, err := runtimefailures.MarshalEnvelope(request.Failure)
	if err != nil {
		return nil, err
	}
	if !request.ReadEvaluation {
		return []any{now, now.Add(time.Second), string(failure), nil, nil, nil, nil}, nil
	}
	if intent.ReadRetry == nil {
		return nil, errors.New("read retry requires its admitted attempt pacing")
	}
	if err := intent.ReadRetry.ObserveFailure(intent.UpdatedAt, now, request.Failure); err != nil {
		return nil, err
	}
	since, cause, err := encodeFanOutReadFailure(*intent.ReadRetry)
	if err != nil {
		return nil, err
	}
	return []any{now, nil, nil, intent.ReadRetry.NotBefore, intent.ReadRetry.Delay.Milliseconds(), since, cause}, nil
}
