package runforkpersistence

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/google/uuid"
)

func TestOriginalActivityArtifactRequiresCapturedPlatformBinding(t *testing.T) {
	originalHash := "bundle-v2:sha256:" + strings.Repeat("a", 64)
	original, err := correlation.NewSourceArtifactFact(originalHash)
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"exact", "absent_admission", "foreign_artifact", "foreign_schema_class", "foreign_event_key", "changed_run_source", "absent_run_source"} {
		t.Run(scenario, func(t *testing.T) {
			request := eventtest.RuntimeControl(uuid.NewString(), runForkActivityRequestEvent, "activity-runtime", "", []byte(`{"input":{}}`), 0,
				uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC().Truncate(time.Microsecond))
			binding := events.PayloadSchemaBindingInput{BundleHash: originalHash, FlowID: ".", EventKey: runForkActivityRequestEvent,
				SchemaClass: events.PayloadSchemaPlatform, SchemaDigest: "sha256:" + strings.Repeat("c", 64)}
			fact := original
			switch scenario {
			case "foreign_artifact":
				binding.BundleHash = "bundle-v2:sha256:" + strings.Repeat("b", 64)
			case "foreign_schema_class":
				binding.SchemaClass = events.PayloadSchemaAuthored
			case "foreign_event_key":
				binding.EventKey = "platform.foreign"
			case "changed_run_source":
				fact, err = correlation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("b", 64))
				if err != nil {
					t.Fatal(err)
				}
			case "absent_run_source":
				fact = correlation.SourceArtifactFact{}
			}
			if scenario != "absent_admission" {
				schema, err := events.NewPayloadSchemaBinding(binding)
				if err != nil {
					t.Fatal(err)
				}
				payload, err := events.NewPayloadAdmission(request.Payload(), schema)
				if err != nil {
					t.Fatal(err)
				}
				request, err = events.ApplyPayloadAdmission(request, payload)
				if err != nil {
					t.Fatal(err)
				}
			}
			err := requireOriginalActivityRequestArtifact(request, fact)
			if scenario == "exact" && err != nil {
				t.Fatal(err)
			} else if scenario != "exact" && err == nil {
				t.Fatalf("%s selected an originating declaration", scenario)
			}
		})
	}
}
