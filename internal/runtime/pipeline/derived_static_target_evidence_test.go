package pipeline

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type derivedStaticInstanceIndex struct {
	observation FlowInstanceObservation
	err         error
	force       bool
}

func TestComponentFixtureInstanceIndexFailsClosed(t *testing.T) {
	owner := unavailablePipelineTestInstanceIndex{}
	if observation, found, err := owner.LookupFlowInstance(context.Background(), FlowInstanceLookupRequest{}); err == nil || found || observation.Valid() {
		t.Fatalf("unsupported component fixture minted instance evidence: found=%v observation=%+v err=%v", found, observation, err)
	}
	if observations, err := owner.ListFlowInstances(context.Background(), FlowInstanceLookupScope{}); err == nil || len(observations) != 0 {
		t.Fatalf("unsupported component fixture minted an inventory: observations=%+v err=%v", observations, err)
	}
}

func (o derivedStaticInstanceIndex) LookupFlowInstance(_ context.Context, request FlowInstanceLookupRequest) (FlowInstanceObservation, bool, error) {
	if o.err != nil {
		return FlowInstanceObservation{}, false, o.err
	}
	if o.force || o.observation.Owner().RunID == request.RunID() && o.observation.Identity().InstancePath == request.ExactPath() {
		return o.observation, true, nil
	}
	return FlowInstanceObservation{}, false, nil
}

func (derivedStaticInstanceIndex) ListFlowInstances(context.Context, FlowInstanceLookupScope) ([]FlowInstanceObservation, error) {
	return nil, errors.New("target admission requires an exact lookup")
}

func derivedStaticTargetObservation(t *testing.T, source semanticview.Source, fact correlation.SourceArtifactFact, runID string, historical bool) FlowInstanceObservation {
	t.Helper()
	root, err := flowidentity.StandingForGeneration(source, ".", runID)
	if err != nil {
		t.Fatal(err)
	}
	parent, err := flowidentity.KeyedChild(source, root, "parent", "stored-coordinate")
	if err != nil {
		t.Fatal(err)
	}
	identity, err := flowidentity.KeylessChild(source, parent, "parent/receiver")
	if err != nil {
		t.Fatal(err)
	}
	request, err := NewExactFlowInstanceLookup(source, fact, flowidentity.RunScopedFlowInstance{RunID: runID, Route: identity.Route()})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	instance := WorkflowInstance{WorkflowName: identity.TemplateID, WorkflowVersion: source.WorkflowVersion(), Mode: "static", Status: "active",
		InstanceID: identity.InstanceID, StorageRef: identity.InstancePath, EntityID: identity.EntityID,
		ParentFlowID: identity.ParentRoute.FlowID, ParentFlowInstance: identity.ParentRoute.FlowInstance, ParentEntityID: identity.ParentEntityID,
		CurrentState: "active", Revision: 1, CreatedAt: at, UpdatedAt: at}
	run := runlifecycle.Snapshot{RunID: runID, State: runlifecycle.StateRunning, Origin: runlifecycle.DeploymentRunOrigin(), BundleHash: fact.BundleHash(), StartedAt: at}
	var observation FlowInstanceObservation
	if historical {
		sourceRun := eventtest.UUID("fixed-static-target-source")
		run.Origin, err = runlifecycle.ForkMaterializationRunOrigin(sourceRun, runlifecycle.ForkOriginPointDeploymentRevision, 2, "")
		if err == nil {
			observation, err = AdmitHistoricalFlowInstanceObservation(request, instance, run, 3, HistoricalFlowInstanceConstruction{
				Identity: identity, SourceOwner: flowidentity.RunScopedFlowInstance{RunID: sourceRun, Route: identity.Route()}, SourceRevision: 2,
			}, nil)
		}
	} else {
		readiness := DynamicFlowRuntimeReadiness{Plan: DynamicFlowRuntimeReadinessPlan{Identity: identity, RunID: runID, BundleHash: fact.BundleHash(), WorkflowVersion: source.WorkflowVersion(), ExecutionMode: executionmode.Live},
			OwningRunSource: fact, RunStatus: "running", InstanceStatus: "active"}
		observation, err = AdmitNativeFlowInstanceObservation(request, instance, run, 3, FlowConstructionPublicationEvidence{Identity: identity}, readiness)
	}
	if err != nil {
		t.Fatal(err)
	}
	return observation
}

func TestDerivedStaticTargetRequiresCanonicalInstanceEvidence(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{
		"schema.yaml":                 "name: derived-target\n",
		"parent/schema.yaml":          "name: parent\ninstance: parent_id\npins:\n  inputs: [parent.created]\n",
		"parent/events.yaml":          "parent.created:\n  parent_id: text\n",
		"parent/entities.yaml":        "parent:\n  parent_id: text\n",
		"parent/receiver/schema.yaml": "name: receiver\npins:\n  inputs: [work.started]\n",
		"parent/receiver/events.yaml": "work.started:\n",
		"parent/receiver/nodes.yaml":  "receiver-node:\n  execution_type: system_node\n  event_handlers:\n    work.started: {}\n",
	})
	bundle, _ := semanticview.Bundle(source)
	fact, err := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	run := eventtest.UUID("derived-static-target-run")
	for _, historical := range []bool{false, true} {
		observation := derivedStaticTargetObservation(t, source, fact, run, historical)
		pc := &PipelineCoordinator{module: &previewWorkflowModule{bundle: bundle}, sourceArtifactFact: fact,
			workflowStore: &workflowInstanceStore{instanceIndex: derivedStaticInstanceIndex{observation: observation}}}
		node := pipelineSourceNode(t, source, "parent/receiver", "receiver-node")
		for _, tc := range []struct {
			name, run, path, entity string
			want                    bool
		}{
			{"declaration", run, "parent/receiver", "", true},
			{"actual stored path", run, observation.Identity().InstancePath, observation.Identity().EntityID, true},
			{"absent", run, "parent/missing/receiver", "", false},
			{"lookalike", run, "parent/stored-coordinate/receiverish", "", false},
			{"foreign", run, "foreign/item", "", false},
			{"wrong run", eventtest.UUID("other-static-target-run"), observation.Identity().InstancePath, "", false},
			{"wrong entity", run, observation.Identity().InstancePath, eventtest.UUID("foreign-static-target-entity"), false},
		} {
			for _, qualified := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/historical=%t/qualified=%t", tc.name, historical, qualified), func(t *testing.T) {
					target := events.RouteIdentity{FlowInstance: tc.path, EntityID: tc.entity}
					if qualified {
						target.FlowID = "parent/receiver"
					}
					if got, err := pc.workflowNodeMatchesDeliveryTarget(context.Background(), node, tc.run, target); err != nil || got != tc.want {
						t.Fatalf("target admission=%t want=%t, %#v err=%v", got, tc.want, target, err)
					}
				})
			}
		}
		for _, corrupt := range []bool{false, true} {
			fault := errors.New("independent index failure")
			index := derivedStaticInstanceIndex{err: fault}
			if corrupt {
				index = derivedStaticInstanceIndex{force: true, observation: derivedStaticTargetObservation(t, source, fact, eventtest.UUID("crossed-index-run"), historical)}
			}
			pc.workflowStore.instanceIndex = index
			if matched, err := pc.workflowNodeMatchesDeliveryTarget(context.Background(), node, run, events.RouteIdentity{FlowInstance: observation.Identity().InstancePath}); matched || err == nil || (!corrupt && !errors.Is(err, fault)) {
				t.Fatalf("index failure became target absence: matched=%t err=%v", matched, err)
			}
		}
	}
}
