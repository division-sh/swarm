package store

import (
	"encoding/json"
	"fmt"
	"strings"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func decodeStoredFailure(raw any) (*runtimefailures.Envelope, error) {
	var encoded []byte
	switch value := raw.(type) {
	case nil:
		return nil, nil
	case []byte:
		encoded = value
	case string:
		encoded = []byte(value)
	default:
		var err error
		encoded, err = json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("encode stored canonical failure: %w", err)
		}
	}
	if text := strings.TrimSpace(string(encoded)); text == "" || text == "null" {
		return nil, nil
	}
	failure, err := runtimefailures.UnmarshalEnvelope(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode stored canonical failure: %w", err)
	}
	return &failure, nil
}

func encodeStoredFailure(failure *runtimefailures.Envelope) (any, error) {
	if failure == nil {
		return nil, nil
	}
	raw, err := runtimefailures.MarshalEnvelope(*failure)
	if err != nil {
		return nil, fmt.Errorf("encode canonical failure: %w", err)
	}
	return string(raw), nil
}

func replayAdmissionFailure(reasonCode string) *runtimefailures.Envelope {
	failure := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassSchemaInvalid, "persisted_replay_run_identity_invalid", "event-store", "load_replay", map[string]any{"reason_code": strings.TrimSpace(reasonCode)}), "event-store", "load_replay")
	return &failure
}
