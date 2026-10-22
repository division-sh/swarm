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
					SchemaYAML:   "name: review\ninstance: fixture_key\nstages:\n  unknown: {}\n  done: {final: true}\n",
					EntitiesYAML: "accounts:\n  current_state: text\n",
					AgentsYAML:   entityToolAgentYAML(actor),
				},
				"nostage": {
					SchemaYAML:   "name: nostage\ninstance: fixture_key\nstages: []\n",
					EntitiesYAML: "accounts:\n  current_state: text\n",
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
				publicStage                  any
			}{
				{"constructed", "review", "unknown", actor, true, true, "unknown"},
				{"import", "review", "", actor, false, false, nil},
				{"constructed-no-stage", "nostage", "pending", noStageActor, true, false, nil},
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
					output, err := executor.Execute(current, "read_accounts", map[string]any{})
					if err != nil {
						t.Fatal(err)
					}
					row := output.(map[string]any)
					stage, present := row["current_state"]
					if !present || stage != tc.publicStage || row["entity_id"] != entityID {
						t.Fatalf("role read lost lifecycle presence or identity: %#v", row)
					}
					if tc.constructed {
						if row["stage_defined"] != tc.namedStage || testNumericValue(row["revision"]) < 1 {
							t.Fatalf("constructed role read acquired imported metadata: %#v", row)
						}
					} else if row["stage_defined"] != nil || row["revision"] != nil {
						t.Fatalf("imported role read acquired lifecycle metadata: %#v", row)
					}
					if tc.publicStage == nil && row["entered_state_at"] != nil {
						t.Fatalf("role read fabricated a named-stage entry clock: %#v", row)
					}
					if fields := row["fields"].(map[string]any); fields["current_state"] != "business-"+tc.posture {
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
					if tc.publicStage == nil && string(wire["current_state"]) != "null" {
						t.Fatalf("role read omitted or coerced null: %s", encoded)
					}
				})
			}
		})
	}
}
