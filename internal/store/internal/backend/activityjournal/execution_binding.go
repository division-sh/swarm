package activityjournal

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
)

func activityExecutionBindingValue(binding processbinding.ActivityExecutionBinding) (any, error) {
	if binding.IsZero() {
		return nil, nil
	}
	if err := binding.Validate(); err != nil {
		return nil, err
	}
	raw, err := canonicaljson.Bytes(binding)
	if err != nil {
		return nil, fmt.Errorf("encode activity execution binding: %w", err)
	}
	return string(raw), nil
}

func decodeActivityExecutionBinding(raw any) (processbinding.ActivityExecutionBinding, error) {
	var binding processbinding.ActivityExecutionBinding
	if raw == nil {
		return binding, nil
	}
	encoded := jsonRaw(raw)
	var fields map[string]any
	if err := canonicaljson.DecodeInto(encoded, &fields); err != nil {
		return binding, fmt.Errorf("decode activity execution binding: %w", err)
	}
	if len(fields) != 3 || fields["version"] == nil || fields["grant_id"] == nil || fields["state_version"] == nil {
		return binding, fmt.Errorf("activity execution binding requires exactly version, grant_id and state_version")
	}
	if err := canonicaljson.DecodeInto(encoded, &binding); err != nil {
		return processbinding.ActivityExecutionBinding{}, fmt.Errorf("decode activity execution binding fields: %w", err)
	}
	if err := binding.Validate(); err != nil {
		return processbinding.ActivityExecutionBinding{}, err
	}
	return binding, nil
}
