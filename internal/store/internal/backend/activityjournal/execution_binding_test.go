package activityjournal

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/google/uuid"
)

func TestActivityExecutionBindingNativeCodec(t *testing.T) {
	binding := processbinding.ActivityExecutionBinding{
		Version: processbinding.ActivityExecutionBindingVersion, GrantID: uuid.NewString(), StateVersion: 3,
	}
	raw, err := activityExecutionBindingValue(binding)
	if err != nil || raw == nil {
		t.Fatalf("encode exact reference: %v %v", raw, err)
	}
	for _, encoded := range []any{raw, []byte(raw.(string))} {
		got, err := decodeActivityExecutionBinding(encoded)
		if err != nil || got != binding {
			t.Fatalf("native string/bytes round trip: %+v %v", got, err)
		}
	}
	if raw, err := activityExecutionBindingValue(processbinding.ActivityExecutionBinding{}); err != nil || raw != nil {
		t.Fatalf("legacy/nonexecuting reference must remain SQL NULL: %v %v", raw, err)
	}
	if got, err := decodeActivityExecutionBinding(nil); err != nil || !got.IsZero() {
		t.Fatalf("legacy SQL NULL: %+v %v", got, err)
	}
	for _, invalid := range []string{
		`null`, `{}`, `[]`, `"reference"`,
		`{"version":"activity-execution.v1","grant_id":"` + binding.GrantID + `","state_version":0}`,
		`{"version":"activity-execution.v1","grant_id":"` + binding.GrantID + `","state_version":1.5}`,
		`{"version":"activity-execution.v1","grant_id":"` + binding.GrantID + `","state_version":"3"}`,
		`{"version":"activity-execution.v1","grant_id":"` + binding.GrantID + `","state_version":9223372036854775808}`,
		`{"version":"activity-execution.v1","grant_id":"` + binding.GrantID + `","state_version":3,"current":true}`,
		`{"version":"activity-execution.v1","grant_id":"` + binding.GrantID + `","state_version":3,"state_version":4}`,
		`{"version":"activity-execution.v1","GrantID":"` + binding.GrantID + `","state_version":3}`,
		`{"version":"activity-execution.v1","grant_id":"` + binding.GrantID + `","StateVersion":3}`,
		`{"version":"activity-execution.v2","grant_id":"` + binding.GrantID + `","state_version":3}`,
		`{"version":"activity-execution.v1","grant_id":null,"state_version":3}`,
	} {
		t.Run(invalid, func(t *testing.T) {
			if got, err := decodeActivityExecutionBinding(invalid); err == nil || !got.IsZero() {
				t.Fatalf("invalid stored reference was admitted: %+v %v", got, err)
			}
		})
	}
	partial := binding
	partial.StateVersion = 0
	if _, err := activityExecutionBindingValue(partial); err == nil {
		t.Fatal("partial reference was encoded")
	}
}
