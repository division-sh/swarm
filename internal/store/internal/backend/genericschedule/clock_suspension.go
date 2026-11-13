package genericschedule

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
)

func decodeClockSuspension(raw any) (*runtimegenericschedule.ClockSuspension, error) {
	if raw == nil {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(jsonBytes(raw)))
	decoder.DisallowUnknownFields()
	var suspension runtimegenericschedule.ClockSuspension
	if err := decoder.Decode(&suspension); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, errors.New("clock suspension contains trailing data")
	}
	return suspension.Canonical(), nil
}
