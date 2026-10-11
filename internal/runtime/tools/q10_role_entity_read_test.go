package tools_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestQ10RoleScopedReadLifecycleBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			actor := models.AgentConfig{ID: "reader", Type: "internal", Role: "operator", ExecutionMode: "live"}
			bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
				"review": {
					SchemaYAML:   "name: review\ninstance: fixture_key\nstages:\n  unknown: {}\n  done: {final: true}\n",
					EntitiesYAML: "accounts:\n  current_state: text\n",
					AgentsYAML:   entityToolAgentYAML(actor),
				},
			})
			var selected runtimetools.EntityPersistence
			if backend == "sqlite" {
				selected = storetest.StartSQLiteRuntimeStore(t)
			} else {
				selected = storetest.StartPostgresRuntimeStore(t)
			}
			ctx := seedEntityToolSourceRun(t, selected, bundle)
			executor := runtimetools.NewExecutorWithOptions(nil, runtimetools.ExecutorOptions{
				EntityStore: selected, WorkflowSource: semanticview.Wrap(bundle),
			})
			if problems := runtimetools.ValidateGeneratedToolSchemaClosureForSource(semanticview.Wrap(bundle)); len(problems) != 0 {
				t.Fatal(problems)
			}
			for _, posture := range []string{"constructed", "import"} {
				t.Run(posture, func(t *testing.T) {
					path := "review/" + posture
					input := map[string]any{"flow_instance": path,
						"fields": map[string]any{"fixture_key": posture, "current_state": "business-" + posture}}
					var entityID string
					var wantStage any
					if posture == "constructed" {
						entityID = seedImportedEntityForToolTest(t, ctx, input)
						wantStage = "unknown"
					} else {
						entityID = seedReadOnlyImportedEntityForToolTest(t, ctx, input)
					}
					scopedActor := entityToolFixtureActor(t, semanticview.Wrap(bundle), actor, entityToolTestRunID, "review", path)
					current := runtimebus.WithInboundEvent(runtimetools.WithActor(ctx, scopedActor), eventtest.RunCreatingRootIngress(
						uuid.NewString(), events.EventType("review/read.requested"), "test", "", nil, 0, entityToolTestRunID, "",
						events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), path), time.Now().UTC(),
					))
					output, err := executor.Execute(current, "read_accounts", map[string]any{})
					if err != nil {
						t.Fatal(err)
					}
					row := output.(map[string]any)
					stage, present := row["current_state"]
					if !present || stage != wantStage || row["entity_id"] != entityID {
						t.Fatalf("role read lost lifecycle presence or identity: %#v", row)
					}
					if fields := row["fields"].(map[string]any); fields["current_state"] != "business-"+posture {
						t.Fatalf("business field became lifecycle: %#v", fields)
					}
					for _, private := range []string{"bookkeeping", "loops"} {
						if _, present := row[private]; present {
							t.Fatalf("role read exposed %s", private)
						}
					}
					encoded, err := json.Marshal(row)
					if err != nil {
						t.Fatal(err)
					}
					var wire map[string]json.RawMessage
					if err := json.Unmarshal(encoded, &wire); err != nil {
						t.Fatal(err)
					}
					if posture == "import" && string(wire["current_state"]) != "null" {
						t.Fatalf("role read omitted or coerced null: %s", encoded)
					}
				})
			}
		})
	}
}
