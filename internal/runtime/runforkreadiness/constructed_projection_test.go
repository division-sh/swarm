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
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

// Component projection proof. Source headers are explicit construction fixtures,
// not evidence of selected execution or public-launcher qualification.
func TestSelectedConstructedActorProjectionCompleteCensus(t *testing.T) {
	for _, fields := range []bool{false, true} {
		name := "fieldless"
		if fields {
			name = "field_bearing"
		}
		t.Run(name, func(t *testing.T) {
			root := canonicalrouting.CopySelectedInputAgentProbe(t)
			for _, flow := range []string{"templ/child", "templ/child/leaf", "templ/audit", "templ/audit/leaf"} {
				for name, body := range map[string]string{
					"schema.yaml": "name: child\nstages:\n  active: {}\n  done: {final: true}\npins:\n  inputs:\n    - work.ready\nrequired_agents:\n  - role: worker\n    subscribes_to: [work.ready]\n",
					"events.yaml": "work.ready:\nwork.done:\n",
					"agents.yaml": "worker:\n  model: regular\n  intent:\n    inline: Consume the exact selected input.\n  subscriptions: [work.ready]\n",
					"nodes.yaml":  "sink:\n  execution_type: system_node\n  subscribes_to: [work.ready]\n  produces: [work.done]\n  event_handlers:\n    work.ready:\n      advances_to: done\n      emit:\n        event: work.done\n",
				} {
					path := filepath.Join(root, flow, name)
					if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, []byte(body), 0600); err != nil {
						t.Fatal(err)
					}
				}
				if fields {
					if err := os.WriteFile(filepath.Join(root, flow, "entities.yaml"), []byte("child_state:\n  value: text\n"), 0600); err != nil {
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
			plan := runfork.RunForkPlan{SourceRunID: runID}
			planning := runfork.RunForkSelectedContractRecipientPlanning{}
			want := make(map[agentidentity.Plan]flowidentity.Instance)
			var first forkrecipient.Evidence
			for _, key := range []string{"one", "two"} {
				for _, branch := range []string{"child", "audit"} {
					parent := flowidentity.Derive(source, "templ", key)
					for _, flow := range []string{"templ/" + branch, "templ/" + branch + "/leaf"} {
						child, err := flowidentity.KeylessChild(source, parent, flow)
						if err != nil {
							t.Fatal(err)
						}
						parent = child
						if len(source.FlowRequiredAgents(flow)) != 1 {
							t.Fatal("fixture lost its declared/required duplicate")
						}
						owner, _ := semanticview.AgentDeclarationOwner(source, flow, "worker")
						declared, err := agentidentity.DeclaredName("worker", owner)
						if err != nil {
							t.Fatal(err)
						}
						route, _ := child.Route().AgentIdentityRoute()
						actor, err := agentidentity.NewPlan(declared, route)
						if err != nil {
							t.Fatal(err)
						}
						want[actor] = child
						entered := time.Now().UTC()
						row := runfork.RunForkEntityState{EntityID: child.EntityID, CurrentState: "active", EnteredStateAt: &entered,
							MaterializationMetadata: &runfork.RunForkMaterializedEntitySnapshotMetadata{
								Owner: runfork.RunForkMaterializedEntitySnapshotMetadataOwner, Source: runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance,
								Mode: "static", FlowTemplate: flow, FlowInstance: child.InstancePath, FlowConfig: selectedAgentHeaderConfig(t, child),
							}}
						if fields {
							row.MaterializationMetadata.EntityType = "child_state"
							row.Fields = map[string]any{"value": key}
						}
						plan.Entities = append(plan.Entities, row)
						if first.Path == "" {
							first, err = forkrecipient.NewLocal(forkrecipient.Input{Recipient: events.MustAgentDeliveryRecipient(actor.AgentID()), Path: child.InstancePath, AgentPlan: actor, HandlerEvent: "work.ready"})
							if err != nil {
								t.Fatal(err)
							}
						}
					}
				}
			}
			planning.RecipientPlanEvents = []runfork.RunForkSelectedContractRecipientPlanEvent{{SourceEventID: "input", EventName: "work.ready", Recipients: []forkrecipient.Evidence{first}}}
			before, _ := json.Marshal(plan)
			projection, err := Project(plan, source, planning, map[string]executionmode.Mode{"input": executionmode.Live}, templateAdmissionRequest(t).ModelOptions)
			if err != nil {
				t.Fatal(err)
			}
			if len(projection.Blueprints) != len(want)+1 || len(projection.Flows) != len(want) || len(projection.Attachments) != len(want) || len(projection.States) != 1 {
				t.Fatalf("complete census actors=%d flows=%d dispatch states=%d", len(projection.Blueprints), len(projection.Flows), len(projection.States))
			}
			for _, attachment := range projection.Attachments {
				if attachment.SourceEventID != "" || len(attachment.SourceEvents) != 0 || len(attachment.Agents) != 1 {
					t.Fatalf("construction attachment invented delivery evidence or lost its actor: %+v", attachment)
				}
			}
			materializations, err := projection.MaterializationStates()
			if err != nil || len(materializations) != len(want) {
				t.Fatalf("complete attachment materialization: count=%d err=%v", len(materializations), err)
			}
			dispatches := 0
			for _, state := range materializations {
				dispatches += len(state.SourceEvents)
			}
			if dispatches != 1 {
				t.Fatalf("attachment census changed initial dispatch: %d", dispatches)
			}
			for _, fault := range []string{"duplicate", "delivery_association", "config", "actor_revision", "route"} {
				t.Run("attachment_"+fault, func(t *testing.T) {
					bad := *projection
					bad.Attachments = append([]runfork.RunForkSelectedContractWorkflowState(nil), projection.Attachments...)
					index := 0
					for i, state := range bad.Attachments {
						if state.EntityID == projection.States[0].EntityID {
							index = i
						}
					}
					attachment := &bad.Attachments[index]
					switch fault {
					case "duplicate":
						bad.Attachments = append(bad.Attachments, *attachment)
					case "delivery_association":
						attachment.SourceEventID = "invented-input"
					case "config":
						attachment.WorkflowVersion = "invented"
					case "actor_revision":
						attachment.Agents = append([]runfork.RunForkSelectedContractAgentExpectation(nil), attachment.Agents...)
						attachment.Agents[0].ConfigRevision = "crossed"
					case "route":
						attachment.Route.InstancePath += "/foreign"
					}
					if _, err := bad.MaterializationStates(); err == nil {
						t.Fatalf("accepted contradictory attachment %s", fault)
					}
				})
			}
			seen := map[agentidentity.Plan]bool{}
			for _, actor := range projection.Blueprints {
				if seen[actor.Identity] {
					t.Fatalf("duplicate actor %s", actor.Identity.Description())
				}
				seen[actor.Identity] = true
				if actor.Identity.Route.Presence != agentidentity.RouteRoot {
					if _, exists := want[actor.Identity]; !exists {
						t.Fatalf("authored phantom/unowned actor %s", actor.Identity.Description())
					}
				}
			}
			for actor := range want {
				if !seen[actor] {
					t.Fatalf("missing exact actor %s", actor.Description())
				}
			}
			for _, flow := range projection.Flows {
				if len(flow.Agents) != 1 || want[flow.Agents[0].Identity] != flow.Instance {
					t.Fatalf("flow/actor lost exact constructor parent: %#v", flow)
				}
			}
			after, _ := json.Marshal(plan)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("projection mutated fixed source bytes")
			}
			selectedProjectionRefusals(t, source, plan, first)
		})
	}
}

func selectedProjectionRefusals(t *testing.T, source semanticview.Source, plan runfork.RunForkPlan, agent forkrecipient.Evidence) {
	t.Helper()
	node, err := identity.AdmitExecutableNodeDeclaration("templ/child", "sink")
	if err != nil {
		t.Fatal(err)
	}
	for _, fault := range []string{"valid", "missing", "duplicate", "wrong_declaration", "wrong_mode", "wrong_entity", "missing_parent", "foreign_parent", "malformed_config", "foreign_route", "wrong_type"} {
		t.Run(fault, func(t *testing.T) {
			bad, row := plan, plan.Entities[0]
			metadata := *row.MaterializationMetadata
			row.MaterializationMetadata = &metadata
			bad.Entities = []runfork.RunForkEntityState{row}
			switch fault {
			case "missing":
				bad.Entities = nil
			case "duplicate":
				bad.Entities = append(bad.Entities, row)
			case "wrong_declaration":
				metadata.FlowTemplate = "templ/audit"
			case "wrong_mode":
				metadata.Mode = "template"
			case "wrong_entity":
				bad.Entities[0].EntityID = uuid.NewString()
			case "foreign_route":
				metadata.FlowInstance += "/foreign"
			case "wrong_type":
				metadata.EntityType = "foreign"
			case "malformed_config":
				metadata.FlowConfig = []byte(`{}`)
			case "missing_parent", "foreign_parent":
				instance, err := AgentConstruction(source, plan, agent.AgentPlan)
				if err != nil {
					t.Fatal(err)
				}
				if fault == "missing_parent" {
					instance.ParentRoute, instance.ParentEntityID = flowidentity.ParentRoute{}, ""
				} else {
					instance.ParentRoute.FlowInstance += "/foreign"
					instance.ParentRoute.EntityID = flowidentity.EntityID(instance.ParentRoute.FlowInstance)
					instance.ParentEntityID = instance.ParentRoute.EntityID
				}
				metadata.FlowConfig = selectedAgentHeaderConfig(t, instance)
			}
			for _, consumer := range []string{"agent", "node", "activity"} {
				t.Run(consumer, func(t *testing.T) {
					var err error
					switch consumer {
					case "agent":
						_, _, err = selectedContractAgentWorkflowState(source, bad, "input", agent)
					case "node":
						_, _, err = selectedContractNodeWorkflowState(source, bad, "input", node, agent.Path, "work.ready")
					case "activity":
						_, err = selectedContractPlatformActivityWorkflowState(source, bad, runfork.RunForkPendingWork{EventID: "input", RoutingSource: eventtest.StaticFlowRoutingSource("templ/child", agent.Path, row.EntityID)})
					}
					if (err == nil) != (fault == "valid") {
						t.Fatalf("fixed construction %s: %v", fault, err)
					}
				})
			}
		})
	}
}
