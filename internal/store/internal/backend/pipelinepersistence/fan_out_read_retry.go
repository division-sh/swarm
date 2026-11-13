package pipelinepersistence

import (
	"database/sql"
	"errors"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

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
