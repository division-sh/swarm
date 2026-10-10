package tools

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

// Explicit component evidence, not selected-store construction or attachment.
type emitInstanceObservationFixture struct {
	observations []pipeline.FlowInstanceObservation
}

func emitInstanceActor(t testing.TB, source semanticview.Source, instance flowidentity.Instance, emitted string) actors.AgentConfig {
	t.Helper()
	declarations := semanticview.AgentDeclarationsForOwner(source, instance.TemplateID)
	if len(declarations) != 1 {
		t.Fatalf("emit fixture requires one physical agent: %+v", declarations)
	}
	namePlan, err := semanticview.ScopedAgentNamePlan(source, declarations[0])
	if err != nil {
		t.Fatal(err)
	}
	name, err := namePlan.Materialize()
	if err != nil {
		t.Fatal(err)
	}
	route, err := instance.Route().AgentIdentityRoute()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := agentidentity.NewPlan(name, route)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := plan.Live(toolTestRunID)
	if err != nil {
		t.Fatal(err)
	}
	return actors.AgentConfig{
		ExecutionMode: "live", ID: identity.AgentID(), Identity: identity,
		Role: namePlan.EffectiveRole(declarations[0].Entry), FlowID: instance.TemplateID,
		FlowPath: identity.FlowInstance(), EntityID: instance.EntityID, EmitEvents: []string{emitted},
	}
}

func emitInstanceObservation(t testing.TB, source semanticview.Source, identity flowidentity.Instance, key string) pipeline.FlowInstanceObservation {
	t.Helper()
	fact := emitInstanceSourceFact(t, source)
	lookup, err := pipeline.NewExactFlowInstanceLookup(source, fact, flowidentity.RunScopedFlowInstance{RunID: toolTestRunID, Route: identity.Route()})
	if err != nil {
		t.Fatal(err)
	}
	schema, found := source.FlowSchemaByID(identity.TemplateID)
	if !found {
		t.Fatal("emit fixture requires its exact declaration")
	}
	at := time.Unix(1700000000, 0).UTC()
	header := pipeline.WorkflowInstance{
		WorkflowName: identity.TemplateID, WorkflowVersion: source.WorkflowVersion(), Mode: schema.EffectiveMode(), Status: "active",
		InstanceID: identity.InstanceID, StorageRef: identity.InstancePath, EntityID: identity.EntityID, InstanceKey: key,
		ParentFlowID: identity.ParentRoute.FlowID, ParentFlowInstance: identity.ParentRoute.FlowInstance, ParentEntityID: identity.ParentEntityID,
		CurrentState: "active", Revision: 1, CreatedAt: at, UpdatedAt: at,
	}
	if entity, declared := entityruntime.ResolveForFlow(source, identity.TemplateID); declared {
		header.EntityType = entity.EntityType
	}
	run := runlifecycle.Snapshot{RunID: toolTestRunID, BundleHash: fact.BundleHash(), State: runlifecycle.StateRunning, Origin: runlifecycle.DeploymentRunOrigin(), StartedAt: at}
	receipt := pipeline.FlowConstructionPublicationEvidence{Identity: identity, InstanceKey: key}
	readiness := pipeline.DynamicFlowRuntimeReadiness{
		Plan:            pipeline.DynamicFlowRuntimeReadinessPlan{Identity: identity, RunID: toolTestRunID, BundleHash: fact.BundleHash(), WorkflowVersion: source.WorkflowVersion(), ExecutionMode: executionmode.Live},
		OwningRunSource: fact, RunStatus: "running", InstanceStatus: "active",
	}
	observed, err := pipeline.AdmitNativeFlowInstanceObservation(lookup, header, run, 1, receipt, readiness)
	if err != nil {
		t.Fatal(err)
	}
	return observed
}

func emitInstanceSourceFact(t testing.TB, source semanticview.Source) correlation.SourceArtifactFact {
	t.Helper()
	bundle, found := semanticview.Bundle(source)
	if !found || bundle.SourceArtifact == nil {
		t.Fatal("emit route fixture requires its admitted artifact")
	}
	return sourceartifactfixture.FactFor(bundle.SourceArtifact)
}

func (f emitInstanceObservationFixture) LookupFlowInstance(ctx context.Context, request pipeline.FlowInstanceLookupRequest) (pipeline.FlowInstanceObservation, bool, error) {
	if err := ctx.Err(); err != nil {
		return pipeline.FlowInstanceObservation{}, false, err
	}
	var selected pipeline.FlowInstanceObservation
	for _, observed := range f.observations {
		identity := observed.Identity()
		if request.RunID() != observed.Owner().RunID || request.FlowID() != identity.TemplateID {
			continue
		}
		if request.ExactPath() != "" && request.ExactPath() != identity.InstancePath || request.DeclaredSelection() &&
			(request.ParentInstance() != identity.ParentRoute.FlowInstance || request.InstanceKey() != observed.InstanceKey()) {
			continue
		}
		if err := observed.ValidateSelection(request); err != nil {
			return pipeline.FlowInstanceObservation{}, false, err
		}
		if selected.Valid() {
			return pipeline.FlowInstanceObservation{}, false, fmt.Errorf("emit fixture repeats its exact instance selector")
		}
		selected = observed
	}
	return selected, selected.Valid(), nil
}

func (f emitInstanceObservationFixture) ListFlowInstances(ctx context.Context, scope pipeline.FlowInstanceLookupScope) ([]pipeline.FlowInstanceObservation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var out []pipeline.FlowInstanceObservation
	for _, observed := range f.observations {
		if observed.Owner().RunID != scope.RunID() {
			continue
		}
		included := slices.Contains(scope.FlowIDs(), observed.Identity().TemplateID)
		for _, owner := range scope.Coordinates() {
			if owner.Key() != observed.Owner().Key() {
				continue
			}
			lookup, err := pipeline.NewExactFlowInstanceLookup(scope.Source(), scope.SourceFact(), owner)
			if err != nil {
				return nil, err
			}
			if err := observed.ValidateSelection(lookup); err != nil {
				return nil, err
			}
			included = true
		}
		if included {
			out = append(out, observed)
		}
	}
	return out, nil
}
