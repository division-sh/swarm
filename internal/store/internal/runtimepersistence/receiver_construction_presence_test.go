package runtimepersistence

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

// Exercise the real transaction-bound claim reader, not a substitute presence
// predicate. Agent admission and public execution are proven separately.
func TestReceiverReadinessRequiresConstructedHeaderBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, test := range []struct {
			name, corruption, refusal string
			fielded, construct, ready bool
		}{
			{name: "fieldless", construct: true, ready: true},
			{name: "fielded", fielded: true, construct: true, ready: true},
			{name: "absent"},
			{name: "state_only", fielded: true, construct: true, corruption: "flow_instances", refusal: "incomplete"},
			{name: "required_fields_missing", fielded: true, construct: true, corruption: "entity_state", refusal: "incomplete"},
			{name: "terminated_fieldless", construct: true, refusal: "terminated"},
			{name: "terminated_fielded", fielded: true, construct: true, refusal: "terminated"},
		} {
			t.Run(backend+"/"+test.name, func(t *testing.T) {
				documents := map[string]string{
					"schema.yaml":        "name: receiver-construction\n",
					"review/schema.yaml": "name: review\nstages:\n  pending: {}\n",
				}
				if test.fielded {
					documents["review/entities.yaml"] = "review_item:\n  marker: {type: text, initial: original}\n"
				}
				f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, documents, nil)
				runID := correlation.RunIDFromContext(f.ctx)
				root := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
				root.Instance = flowidentity.Stored(root.ContractBundle, ".", runID, runID, runID, "")
				plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, root)
				if err != nil || len(plan.Children) != 1 {
					t.Fatalf("prepare exact receiver tree: %+v err=%v", plan, err)
				}
				req := sqliteFlowActivationRequest(f.bundle, "review", "review", "", "review")
				req.Instance = plan.Children[0].Identity
				if test.construct {
					if result, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil || !result.Acknowledged {
						t.Fatalf("canonical construction: %+v %v", result, err)
					}
				}
				if test.corruption != "" {
					if _, err := f.db.ExecContext(f.ctx, "DELETE FROM "+test.corruption+" WHERE run_id=$1", runID); err != nil {
						t.Fatal(err)
					}
				}
				if strings.HasPrefix(test.name, "terminated_") {
					owner, err := flowidentity.NewRunScopedFlowInstance(runID, req.Instance.Route())
					if err != nil {
						t.Fatal(err)
					}
					if err := f.workflows.MarkTerminated(f.ctx, owner, identity.NormalizeEntityID(req.Instance.EntityID), time.Now().UTC()); err != nil {
						t.Fatal(err)
					}
				}
				agent := mustTestAgentIdentityForRun(runID, "worker", "review")
				route := events.DeliveryRoute{
					Recipient: events.MustAgentDeliveryRecipient(agent.AgentID()), AgentIdentity: agent,
					Target: events.MustMaterializingEntityTarget(events.RouteIdentity{FlowID: "review", EntityID: req.Instance.EntityID, FlowInstance: "review"}),
				}
				ready, err := readReceiverConstructionReady(t, f, route)
				if test.refusal != "" {
					if ready || err == nil || !strings.Contains(err.Error(), test.refusal) {
						t.Fatalf("claim reader did not refuse %s: ready=%t err=%v", test.name, ready, err)
					}
				} else if err != nil || ready != test.ready {
					t.Fatalf("claim reader ready=%t err=%v want=%t", ready, err, test.ready)
				}
				var fields int
				if err := f.db.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM entity_state WHERE run_id=$1", runID).Scan(&fields); err != nil {
					t.Fatal(err)
				}
				wantFields := 0
				if test.fielded && test.construct && test.corruption != "entity_state" {
					wantFields = 1
				}
				if fields != wantFields {
					t.Fatalf("claim reader repaired persistence: fields=%d want=%d", fields, wantFields)
				}
			})
		}
	}
}

func readReceiverConstructionReady(t *testing.T, f receiverConfigActivationFixture, route events.DeliveryRoute) (bool, error) {
	t.Helper()
	tx, err := f.db.BeginTx(f.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	switch store := f.store.(type) {
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.ReceiverMaterializedTx(f.ctx, tx, route)
	case *PostgresStore:
		return store.pipelinePostgresOwner.ReceiverMaterializedTx(f.ctx, tx, route)
	default:
		t.Fatal("unsupported test store")
		return false, nil
	}
}

func TestReceiverConstructionReceiptBindsCreatingPublicationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, shape := range []string{"root", "keyed", "fieldless_descendant"} {
			for _, variant := range []string{"exact", "unrelated_event", "missing_receipt", "partial_receipt", "contradictory_control", "contradictory_origin", "crossed_agent_scope"} {
				t.Run(backend+"/"+shape+"/"+variant, func(t *testing.T) {
					var f receiverConfigActivationFixture
					if shape == "root" {
						f = newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
							"schema.yaml": "name: root-receiver-construction\npins:\n  inputs:\n    - start.seeded\n",
							"events.yaml": "start.seeded:\n",
						}, nil)
					} else {
						f = newEagerFlowConstructorFixture(t, backend)
					}
					var req pipeline.FlowInstanceActivationRequest
					if shape == "root" {
						runID := correlation.RunIDFromContext(f.ctx)
						req = sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
						req.Instance = flowidentity.Stored(req.ContractBundle, ".", runID, runID, runID, "")
						req.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "start.seeded", "constructor-fixture", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, req.OccurredAt)
					} else {
						req = f.request("business-key", "r1", "first")
					}
					plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
					if err != nil {
						t.Fatal(err)
					}
					if result, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil || !result.Acknowledged {
						t.Fatalf("canonical construction: %+v %v", result, err)
					}
					target := plan.Identity
					if shape == "fieldless_descendant" {
						target = plan.Children[0].Identity
					}
					runID := correlation.RunIDFromContext(f.ctx)
					var receipt []byte
					if err := f.db.QueryRowContext(f.ctx, `SELECT projection FROM workflow_instance_initial_materializations WHERE run_id=$1 AND instance_path=$2`, runID, target.InstancePath).Scan(&receipt); err != nil {
						t.Fatal(err)
					}
					event := req.TriggerEvent
					switch variant {
					case "unrelated_event":
						event = eventtest.ExistingRunRootIngress(uuid.NewString(), event.Type(), "constructor-fixture", "", event.Payload(), 0, runID, events.EventEnvelope{}, event.CreatedAt())
					case "missing_receipt":
						if _, err := f.db.ExecContext(f.ctx, `DELETE FROM workflow_instance_initial_materializations WHERE run_id=$1 AND instance_path=$2`, runID, target.InstancePath); err != nil {
							t.Fatal(err)
						}
					case "partial_receipt", "contradictory_control", "contradictory_origin":
						var document map[string]json.RawMessage
						if err := json.Unmarshal(receipt, &document); err != nil {
							t.Fatal(err)
						}
						switch variant {
						case "partial_receipt":
							delete(document, "creating_input")
						case "contradictory_origin":
							document["creating_input"] = json.RawMessage(`{"event_id":"` + uuid.NewString() + `","input":""}`)
						case "contradictory_control":
							var persisted map[string]json.RawMessage
							if err := json.Unmarshal(document["persisted"], &persisted); err != nil {
								t.Fatal(err)
							}
							var control map[string]json.RawMessage
							if err := json.Unmarshal(persisted["control"], &control); err != nil {
								t.Fatal(err)
							}
							control["entity_id"] = json.RawMessage(`"` + uuid.NewString() + `"`)
							persisted["control"], err = json.Marshal(control)
							if err != nil {
								t.Fatal(err)
							}
							document["persisted"], err = json.Marshal(persisted)
							if err != nil {
								t.Fatal(err)
							}
						}
						receipt, err = json.Marshal(document)
						if err != nil {
							t.Fatal(err)
						}
						if _, err := f.db.ExecContext(f.ctx, `UPDATE workflow_instance_initial_materializations SET projection=$1 WHERE run_id=$2 AND instance_path=$3`, string(receipt), runID, target.InstancePath); err != nil {
							t.Fatal(err)
						}
					}
					name, err := agentidentity.RuntimeName("worker", "constructor-fixture")
					if err != nil {
						t.Fatal(err)
					}
					agentRoute, err := target.Route().AgentIdentityRoute()
					if err != nil {
						t.Fatal(err)
					}
					if variant == "crossed_agent_scope" {
						agentRoute, err = agentidentity.PresentRoute("foreign", target.InstanceID, target.InstancePath)
						if err != nil {
							t.Fatal(err)
						}
					}
					agent, err := agentidentity.New(runID, name, agentRoute)
					if err != nil {
						t.Fatal(err)
					}
					route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(agent.AgentID()), AgentIdentity: agent,
						Target: events.MustMaterializingEntityTarget(events.RouteIdentity{FlowID: target.TemplateID, EntityID: target.EntityID, FlowInstance: target.InstancePath})}
					route.Initialization, err = events.AdmitFlowReceiverInitialization(event, route.Target)
					if err != nil {
						t.Fatal(err)
					}
					before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
					ready, err := readReceiverConstructionReady(t, f, route)
					if variant == "exact" {
						if !ready || err != nil {
							t.Fatalf("exact constructor receipt refused: ready=%t %v", ready, err)
						}
					} else if ready || err == nil {
						t.Fatalf("corrupt/unrelated receipt admitted: ready=%t %v", ready, err)
					}
					if after := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres"); !reflect.DeepEqual(before, after) {
						t.Fatal("construction receipt reader mutated execution state")
					}
				})
			}
		}
	}
}
