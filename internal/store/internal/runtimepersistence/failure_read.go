package runtimepersistence

import (
	"strings"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	storefailurecodec "github.com/division-sh/swarm/internal/store/internal/failurecodec"
)

func decodeStoredFailure(raw any) (*runtimefailures.Envelope, error) {
	return storefailurecodec.Decode(raw)
}

func encodeStoredFailure(failure *runtimefailures.Envelope) (any, error) {
	return storefailurecodec.Encode(failure)
}

func replayAdmissionFailure(reasonCode string) *runtimefailures.Envelope {
	failure := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassSchemaInvalid, "persisted_replay_run_identity_invalid", "event-store", "load_replay", map[string]any{"reason_code": strings.TrimSpace(reasonCode)}), "event-store", "load_replay")
	return &failure
}
