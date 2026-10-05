package runforkpersistence

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func selectedWorkflowConstructionFixture(t *testing.T, state selectedContractWorkflowState) selectedContractWorkflowState {
	t.Helper()
	state.SourceRunID = "00000000-0000-0000-0000-000000000229"
	config, err := pipeline.WorkflowInstanceConfigPayloadForRoute(flowidentity.StoredRoute(state.WorkflowName, flowidentity.LogicalInstanceID(state.Route), state.Route), state.WorkflowVersion, state.Config)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	state.History = runfork.RunForkEntityState{EntityID: state.EntityID, MaterializationMetadata: &runfork.RunForkMaterializedEntitySnapshotMetadata{
		Owner: runfork.RunForkMaterializedEntitySnapshotMetadataOwner, Source: runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance,
		FlowInstance: state.Route, FlowTemplate: state.WorkflowName, FlowConfig: raw,
	}}
	return state
}

func TestSelectedContractWorkflowReadinessComposesExactForkAgentTopology(t *testing.T) {
	const runID = "00000000-0000-0000-0000-000000000227"
	identity := agentidentitytest.DeclaredForRun(t, runID, "worker", "flow/worker", "flow", "instance", "flow/instance")
	declaration, err := identity.Plan()
	if err != nil {
		t.Fatalf("agent plan: %v", err)
	}
	source, err := correlation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("a", 64))
	if err != nil {
		t.Fatalf("bundle source: %v", err)
	}
	plan, topologies, encoded, err := selectedContractWorkflowReadiness(source, selectedWorkflowConstructionFixture(t, selectedContractWorkflowState{
		RunID: runID, EntityID: "00000000-0000-0000-0000-000000000228", WorkflowName: "flow",
		WorkflowVersion: "v1", ExecutionMode: executionmode.Mock, Mode: "template", Route: "flow/instance",
		Agents: []runfork.RunForkSelectedContractAgentExpectation{{
			Plan: declaration, ConfigRevision: strings.Repeat("b", 64),
		}},
	}))
	if err != nil {
		t.Fatalf("selectedContractWorkflowReadiness: %v", err)
	}
	if plan == nil || len(encoded) == 0 || plan.RunID != runID || plan.Identity.InstancePath != "flow/instance" ||
		plan.CreationEvent != nil || len(plan.Agents) != 1 || plan.Agents[0].Identity != identity ||
		plan.Agents[0].EntityID != plan.Identity.EntityID {
		t.Fatalf("readiness plan = %#v", plan)
	}
	if len(topologies) != 1 || topologies[0].Identity != identity ||
		topologies[0].Admission.Authority.Kind != agenttopology.AuthorityFlowReadinessPlan ||
		topologies[0].Admission.Authority.Readiness == nil ||
		topologies[0].Admission.Authority.Readiness.RunID != runID ||
		topologies[0].Admission.Authority.Readiness.InstancePath != "flow/instance" {
		t.Fatalf("committed topology = %#v", topologies)
	}
}

func TestSelectedContractWorkflowReadinessRejectsAgentOutsideExactFlowOwner(t *testing.T) {
	const runID = "00000000-0000-0000-0000-000000000227"
	identity := agentidentitytest.DeclaredForRun(t, runID, "worker", "flow/worker", "flow", "other", "flow/other")
	declaration, err := identity.Plan()
	if err != nil {
		t.Fatalf("agent plan: %v", err)
	}
	source, err := correlation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("a", 64))
	if err != nil {
		t.Fatalf("bundle source: %v", err)
	}
	_, _, _, err = selectedContractWorkflowReadiness(source, selectedWorkflowConstructionFixture(t, selectedContractWorkflowState{
		RunID: runID, EntityID: "00000000-0000-0000-0000-000000000228", WorkflowName: "flow",
		WorkflowVersion: "v1", ExecutionMode: executionmode.Mock, Mode: "template", Route: "flow/instance",
		Agents: []runfork.RunForkSelectedContractAgentExpectation{{
			Plan: declaration, ConfigRevision: strings.Repeat("b", 64),
		}},
	}))
	if err == nil || !strings.Contains(err.Error(), "invalid agent declaration") {
		t.Fatalf("error = %v, want exact-flow rejection", err)
	}
}

func TestSelectedWorkflowReadinessAndConfigRetainRecordedParent(t *testing.T) {
	const runID = "00000000-0000-0000-0000-000000000227"
	artifact, err := correlation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	for _, variant := range []string{"nested_keyless", "root_parent", "missing_history", "partial_parent", "crossed_child"} {
		t.Run(variant, func(t *testing.T) {
			const flow = "outer/left/sink/final"
			const path = "outer/left/sink/revision/final"
			state := selectedWorkflowConstructionFixture(t, selectedContractWorkflowState{
				RunID: runID, EntityID: flowidentity.EntityID(path), WorkflowName: flow, Route: path,
				WorkflowVersion: "v1", Mode: "static", ExecutionMode: executionmode.Mock,
				Config: map[string]any{"business": "unchanged"},
			})
			parent := flowidentity.ParentRoute{FlowID: "outer/left/sink", FlowInstance: "outer/left/sink/revision", EntityID: flowidentity.EntityID("outer/left/sink/revision")}
			if variant == "root_parent" {
				parent = flowidentity.ParentRoute{FlowID: ".", FlowInstance: state.SourceRunID, EntityID: state.SourceRunID}
			}
			identity := flowidentity.Instance{
				TemplateID: flow, ScopeKey: flow, InstanceID: "final", InstancePath: path, EntityID: state.EntityID,
				ParentRoute: parent, ParentEntityID: parent.EntityID, HasStoredPath: true,
			}
			config, err := pipeline.WorkflowInstanceConfigPayloadForIdentity(identity, "v1", state.Config)
			if err != nil {
				t.Fatal(err)
			}
			if variant == "partial_parent" {
				delete(config, "parent_entity_id")
			}
			state.History.MaterializationMetadata.FlowConfig, err = json.Marshal(config)
			if err != nil {
				t.Fatal(err)
			}
			if variant == "missing_history" {
				state.History.MaterializationMetadata = nil
			}
			if variant == "crossed_child" {
				state.Route = "outer/right/sink/revision/final"
			}
			plan, _, _, planErr := selectedContractWorkflowReadiness(artifact, state)
			encoded, configErr := selectedContractWorkflowStateConfig(state)
			if variant == "missing_history" || variant == "partial_parent" || variant == "crossed_child" {
				if planErr == nil || configErr == nil {
					t.Fatalf("invalid recorded identity accepted: plan=%v config=%v", planErr, configErr)
				}
				return
			}
			if planErr != nil || configErr != nil {
				t.Fatalf("recorded identity refused: plan=%v config=%v", planErr, configErr)
			}
			if variant == "root_parent" {
				identity.ParentRoute.FlowInstance, identity.ParentRoute.EntityID, identity.ParentEntityID = runID, runID, runID
			}
			if plan.Identity != identity {
				t.Fatalf("readiness lost construction identity: got=%+v want=%+v", plan.Identity, identity)
			}
			recorded, err := pipeline.DecodeWorkflowInstanceRecordedConfig(identity.Route(), encoded)
			if err != nil || recorded.ParentRoute() != identity.ParentRoute {
				t.Fatalf("config lost exact parent: parent=%+v err=%v", recorded.ParentRoute(), err)
			}
			business, err := pipeline.WorkflowInstanceBusinessConfigForRoute(identity.Route(), encoded)
			if err != nil || business["business"] != "unchanged" {
				t.Fatalf("parent projection changed business config: %+v %v", business, err)
			}
		})
	}
}
