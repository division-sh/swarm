package fanoutobligation

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
)

// CaptureEntitySource freezes the source at evaluation, before subsequent
// handler phases mutate business state. Only the immutable revision is stored;
// this transient value is not part of the persisted capsule or request wire.
func (r IntentRequest) CaptureEntitySource(value any) (IntentRequest, error) {
	if r.Source.Kind != SourceEntityField || r.Source.MutationID != "" {
		return IntentRequest{}, fmt.Errorf("entity source capture requires an unbound entity-field intent")
	}
	if _, err := r.ProjectSource(value); err != nil {
		return IntentRequest{}, err
	}
	raw, err := canonicaljson.MarshalPreservingNumberKinds(value)
	if err != nil {
		return IntentRequest{}, fmt.Errorf("capture fan-out entity source: %w", err)
	}
	r.entitySource = raw
	return r, nil
}

// CapturedEntitySource admits the frozen value only at immutable-source binding.
// A retained intent instead reads its exact mutation revision, never live state.
func (r IntentRequest) CapturedEntitySource() (any, error) {
	if r.Source.Kind != SourceEntityField || len(r.entitySource) == 0 {
		return nil, fmt.Errorf("fan-out entity source has no evaluation-point capture")
	}
	var value any
	if err := canonicaljson.DecodePreservingNumberLexemes(r.entitySource, &value); err != nil {
		return nil, fmt.Errorf("decode captured fan-out entity source: %w", err)
	}
	if _, err := r.ProjectSource(value); err != nil {
		return nil, err
	}
	return value, nil
}
