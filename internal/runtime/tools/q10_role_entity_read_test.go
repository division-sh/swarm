package tools_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestQ10RoleScopedReadLifecycleBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			actor := models.AgentConfig{ID: "reader", Type: "internal", Role: "operator", ExecutionMode: "live"}
			noStageActor := models.AgentConfig{ID: "nostage-reader", Type: "internal", Role: "operator", ExecutionMode: "live"}
			bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
				"review": {
					SchemaYAML:   "instance: fixture_key\nstages:\n  unknown: {}\n  done: {final: true}\n",
					EntitiesYAML: "fixture_key: text\ncurrent_state: text\n",
					AgentsYAML:   entityToolAgentYAML(actor),
				},
				"nostage": {
					SchemaYAML:   "instance: fixture_key\nstages: []\n",
					EntitiesYAML: "fixture_key: text\ncurrent_state: text\n",
					AgentsYAML:   entityToolAgentYAML(noStageActor),
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
			for _, tc := range []struct {
				posture, flowID, storedStage string
				actor                        models.AgentConfig
				constructed, namedStage      bool
			}{
				{"constructed", "review", "unknown", actor, true, true},
				{"import", "review", "", actor, false, false},
				{"constructed-no-stage", "nostage", "pending", noStageActor, true, false},
			} {
				t.Run(tc.posture, func(t *testing.T) {
					path := tc.flowID + "/" + tc.posture
					input := map[string]any{"flow_instance": path,
						"fields": map[string]any{"fixture_key": tc.posture, "current_state": "business-" + tc.posture}}
					var entityID string
					if tc.constructed {
						entityID = seedImportedEntityForToolTest(t, ctx, input)
					} else {
						entityID = seedReadOnlyImportedEntityForToolTest(t, ctx, input)
					}
					fixture := ctx.Value(entityToolImportFixtureKey{}).(entityToolImportFixture)
					owner := flowidentity.RunScopedFlowInstance{RunID: entityToolTestRunID,
						Route: flowidentity.StoredRoute(tc.flowID, flowidentity.LogicalInstanceID(path), path)}
					stored, found, err := fixture.pipeline.Load(ctx, owner)
					if err != nil || found != tc.constructed {
						t.Fatalf("independent header membership: found=%t want=%t err=%v", found, tc.constructed, err)
					}
					if found && (stored.EntityID != entityID || stored.StorageRef != path || stored.WorkflowName != tc.flowID ||
						stored.CurrentState != tc.storedStage || stored.StageDefined != tc.namedStage || stored.Revision < 1 ||
						stored.CreatedAt.IsZero() || stored.EnteredStageAt.IsZero()) {
						t.Fatalf("constructed header lost internal lifecycle evidence: %+v", stored)
					}
					scopedActor := entityToolFixtureActor(t, semanticview.Wrap(bundle), tc.actor, entityToolTestRunID, tc.flowID, path)
					current := runtimebus.WithInboundEvent(runtimetools.WithActor(ctx, scopedActor), eventtest.RunCreatingRootIngress(
						uuid.NewString(), events.EventType(tc.flowID+"/read.requested"), "test", "", nil, 0, entityToolTestRunID, "",
						events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), path), time.Now().UTC(),
					))
					output, err := executor.Execute(current, "read_state", map[string]any{})
					if err != nil {
						t.Fatal(err)
					}
					row := output.(map[string]any)
					if len(row) != 2 || row["current_state"] != "business-"+tc.posture || row["fixture_key"] != tc.posture {
						t.Fatalf("role read changed declared business fields: %#v", row)
					}
					for _, private := range []string{"entity_id", "flow_instance", "entity_type", "stage_defined", "revision", "entered_state_at", "fields", "bookkeeping", "loops"} {
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
					want, err := json.Marshal("business-" + tc.posture)
					if err != nil || string(wire["current_state"]) != string(want) {
						t.Fatalf("role read changed the declared current_state field: %s", encoded)
					}
				})
			}
		})
	}
}
