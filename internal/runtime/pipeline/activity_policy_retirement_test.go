package pipeline

import (
	"encoding/json"
	"errors"
	"testing"

	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
)

func TestActivityCarriersOmitRetiredPolicy(t *testing.T) {
	intent := runtimeengine.ActivityIntent{ActivityID: "once", Tool: "provider.http"}
	for name, value := range map[string]any{
		"request": activityRequestPayloadFromIntent(intent),
		"success": activitySuccessPayload(intent, map[string]any{"receipt": "exact"}),
		"failure": activityFailurePayload(intent, errors.New("provider unavailable")),
	} {
		t.Run(name, func(t *testing.T) {
			raw, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(raw, &fields); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"effect_class", "fork_policy", "retry_max_attempts", "retry_backoff", "attempt"} {
				if _, present := fields[key]; present {
					t.Errorf("%s still publishes retired authored policy %s: %s", name, key, raw)
				}
			}
			if string(fields["activity_id"]) != `"once"` || string(fields["tool"]) != `"provider.http"` {
				t.Fatalf("activity identity was lost: %s", raw)
			}
		})
	}
}
