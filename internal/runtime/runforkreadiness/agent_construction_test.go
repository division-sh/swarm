package runforkreadiness

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

// This is fixed-plan/scope consumption proof, not fork execution or live admission.
func TestSelectedAgentConstructionUsesExactFixedHeader(t *testing.T) {
	root := canonicalrouting.CopySelectedInputAgentProbe(t)
	for _, flow := range []string{"templ/child", "templ/child/leaf", "templ/audit", "templ/audit/leaf"} {
		if err := os.MkdirAll(filepath.Join(root, flow), 0755); err != nil {
			t.Fatal(err)
		}
		for name, body := range map[string]string{
			"schema.yaml": "name: child\npins:\n  inputs:\n    - work.ready\n",
			"events.yaml": "work.ready:\n",
			"agents.yaml": "worker:\n  model: regular\n  intent:\n    inline: Consume the exact selected input.\n  subscriptions: [work.ready]\n",
		} {
			if err := os.WriteFile(filepath.Join(root, flow, name), []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source, runID := semanticview.Wrap(bundle), uuid.NewString()
	for _, key := range []string{"one", "two"} {
		for _, branch := range []string{"child", "audit"} {
			parent := flowidentity.Derive(source, "templ", key)
			for _, flow := range []string{"templ/" + branch, "templ/" + branch + "/leaf"} {
				constructed, err := flowidentity.KeylessChild(source, parent, flow)
				if err != nil {
					t.Fatal(err)
				}
				parent = constructed
				t.Run(constructed.InstancePath, func(t *testing.T) {
					owner, found := semanticview.AgentDeclarationOwner(source, flow, "worker")
					if !found {
						t.Fatal("missing exact agent declaration")
					}
					name, err := agentidentity.DeclaredName("worker", owner)
					if err != nil {
						t.Fatal(err)
					}
					route, err := constructed.Route().AgentIdentityRoute()
					if err != nil {
						t.Fatal(err)
					}
					agent, err := agentidentity.NewPlan(name, route)
					if err != nil {
						t.Fatal(err)
					}
					metadata := runfork.RunForkMaterializedEntitySnapshotMetadata{
						Owner:  runfork.RunForkMaterializedEntitySnapshotMetadataOwner,
						Source: runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance,
						Mode:   "static", FlowTemplate: flow, FlowInstance: constructed.InstancePath,
						FlowConfig: selectedAgentHeaderConfig(t, constructed),
					}
					lifecycle, found := semanticview.WorkflowStageTopology(source, flow)
					if !found {
						t.Fatal("fixed header requires its lifecycle declaration")
					}
					stage, err := lifecycle.InitialStoredStage()
					if err != nil {
						t.Fatal(err)
					}
					entered := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
					metadata.CreatedAt, metadata.UpdatedAt, metadata.EnteredStateAt = entered, entered, entered
					metadata.Status, metadata.StageDefined = "active", lifecycle.StageCount() != 0
					plan := runfork.RunForkPlan{SourceRunID: runID, Entities: []runfork.RunForkEntityState{{
						EntityID: constructed.EntityID, MaterializationMetadata: &metadata, CurrentState: stage.ID(), EnteredStateAt: &entered,
					}}}
					before, err := json.Marshal(plan)
					if err != nil {
						t.Fatal(err)
					}
					actual, err := AgentConstruction(source, plan, agent)
					if err != nil || actual != constructed {
						t.Fatalf("fixed construction consumption: actual=%#v err=%v", actual, err)
					}
					if _, err := semanticview.ResolveAgentPlanExecutionSemanticScope(source, runID, agent, actual); err != nil {
						t.Fatal(err)
					}
					after, err := json.Marshal(plan)
					if err != nil || !reflect.DeepEqual(before, after) {
						t.Fatal("preparation mutated fixed history")
					}
					input := eventtest.OperatorInjectedWithRoutingSource(uuid.NewString(), "work.ready", "operator", "", []byte(`{}`), 0, runID, nil, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), time.Now().UTC())
					input, err = eventtest.AdmitPayload(input, ".", "work.ready")
					if err != nil {
						t.Fatal(err)
					}
					validation, err := bus.RevalidateSelectedInput(source, input)
					if err != nil {
						t.Fatal(err)
					}
					evidence, err := forkrecipient.NewLocal(forkrecipient.Input{Recipient: events.MustAgentDeliveryRecipient(agent.AgentID()), Path: agent.FlowInstance(), AgentPlan: agent, HandlerEvent: "work.ready"})
					if err != nil {
						t.Fatal(err)
					}
					if _, err := validation.SelectRecipients([]forkrecipient.Evidence{evidence}, nil); err == nil {
						t.Fatal("selected input accepted nonroot agent without constructor facts")
					}
					selected, err := validation.SelectRecipients([]forkrecipient.Evidence{evidence}, map[agentidentity.Plan]flowidentity.Instance{agent: actual})
					if err != nil {
						t.Fatal(err)
					}
					if selected.AllowsSubscriber(bus.Subscriber{Recipient: evidence.Recipient, Path: evidence.Path, AgentPlan: agent}) {
						t.Fatal("construction facts invented a root-to-descendant routing permission")
					}
					for _, fault := range []string{"missing", "duplicate", "duplicate_route", "wrong_declaration", "wrong_mode", "wrong_entity", "missing_state", "missing_entered", "missing_parent", "foreign_parent", "malformed_config", "foreign_route"} {
						t.Run(fault, func(t *testing.T) {
							bad, row := plan, metadata
							bad.Entities = []runfork.RunForkEntityState{plan.Entities[0]}
							bad.Entities[0].MaterializationMetadata = &row
							switch fault {
							case "missing":
								bad.Entities = nil
							case "duplicate":
								bad.Entities = append(bad.Entities, bad.Entities[0])
							case "duplicate_route":
								duplicate := bad.Entities[0]
								duplicate.EntityID = uuid.NewString()
								bad.Entities = append(bad.Entities, duplicate)
							case "wrong_declaration":
								row.FlowTemplate = "templ"
							case "wrong_mode":
								row.Mode = "template"
							case "wrong_entity":
								bad.Entities[0].EntityID = uuid.NewString()
							case "missing_state":
								bad.Entities[0].CurrentState = ""
							case "missing_entered":
								bad.Entities[0].EnteredStateAt = nil
							case "missing_parent", "foreign_parent":
								copy := constructed
								copy.ParentRoute, copy.ParentEntityID = flowidentity.ParentRoute{}, ""
								if fault == "foreign_parent" {
									copy.ParentRoute = constructed.ParentRoute
									copy.ParentRoute.FlowInstance += "/foreign"
									copy.ParentRoute.EntityID = flowidentity.EntityID(copy.ParentRoute.FlowInstance)
									copy.ParentEntityID = copy.ParentRoute.EntityID
								}
								row.FlowConfig = selectedAgentHeaderConfig(t, copy)
							case "malformed_config":
								row.FlowConfig = []byte(`{}`)
							case "foreign_route":
								row.FlowInstance += "/foreign"
							}
							if _, err := AgentConstruction(source, bad, agent); err == nil {
								t.Fatal("invalid fixed construction admitted selected agent scope")
							}
						})
					}
				})
			}
		}
	}
}

func selectedAgentHeaderConfig(t *testing.T, instance flowidentity.Instance) []byte {
	t.Helper()
	config, err := json.Marshal(map[string]any{
		"instance_id": instance.InstanceID, "storage_ref": instance.InstancePath, "flow_path": instance.InstancePath,
		"parent_flow_id": instance.ParentRoute.FlowID, "parent_flow_instance": instance.ParentRoute.FlowInstance,
		"parent_entity_id": instance.ParentEntityID, "config": map[string]any{},
	})
	if err != nil {
		t.Fatal(err)
	}
	return config
}
