package tools

import (
	"context"
	"testing"
	"time"

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
	for _, observed := range f.observations {
		if request.RunID() != observed.Owner().RunID || request.ExactPath() != observed.Identity().InstancePath {
			continue
		}
		if err := observed.ValidateSelection(request); err != nil {
			return pipeline.FlowInstanceObservation{}, false, err
		}
		return observed, true, nil
	}
	return pipeline.FlowInstanceObservation{}, false, nil
}

func (f emitInstanceObservationFixture) ListFlowInstances(ctx context.Context, scope pipeline.FlowInstanceLookupScope) ([]pipeline.FlowInstanceObservation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var out []pipeline.FlowInstanceObservation
	for _, owner := range scope.Coordinates() {
		lookup, err := pipeline.NewExactFlowInstanceLookup(scope.Source(), scope.SourceFact(), owner)
		if err != nil {
			return nil, err
		}
		observed, found, err := f.LookupFlowInstance(ctx, lookup)
		if err != nil {
			return nil, err
		}
		if found {
			out = append(out, observed)
		}
	}
	return out, nil
}
