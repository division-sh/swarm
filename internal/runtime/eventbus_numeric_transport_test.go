package runtime

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

// Admission receives execution payloads after their semantic-to-CEL handoff.
// Schema validation and optional-null normalization must not erase their kinds.
func TestRuntimePayloadAdmissionPreservesExecutionNumberKinds(t *testing.T) {
	for _, tc := range []struct {
		name, schema, payload string
	}{
		{"authored", "numeric.completed:\n  integer: integer\n  explicit_double: numeric\n", `{"integer":8,"explicit_double":8.0}`},
		{"optional_null", "numeric.completed:\n  integer: integer\n  explicit_double: numeric\n  optional: text?\n", `{"integer":8,"explicit_double":8.0,"optional":null}`},
		{"schema_less", "", `{"integer":8,"explicit_double":8.0}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := loadRootPayloadBundle(t, tc.schema, "")
			event := eventtest.RuntimeControl("numeric-kind-proof", "numeric.completed", "numeric-proof", "", []byte(tc.payload), 0, "", "", events.EventEnvelope{}, time.Time{})
			admitted, err := testRuntimePayloadAdmitter(t, bundle)(context.Background(), event, "")
			if err != nil {
				t.Fatal(err)
			}
			var decoded map[string]any
			if err := canonicaljson.DecodePreservingNumberLexemes(admitted.Payload(), &decoded); err != nil {
				t.Fatal(err)
			}
			projected, err := workflowexpr.ProjectCELValue(decoded)
			if err != nil {
				t.Fatal(err)
			}
			values := projected.(map[string]any)
			if values["integer"] != int64(8) || values["explicit_double"] != float64(8) {
				t.Fatalf("admission erased execution kinds: integer=%T double=%T bytes=%s", values["integer"], values["explicit_double"], admitted.Payload())
			}
			if bytes.Contains(admitted.Payload(), []byte("null")) {
				t.Fatal("optional-null normalization was bypassed")
			}
		})
	}
}
