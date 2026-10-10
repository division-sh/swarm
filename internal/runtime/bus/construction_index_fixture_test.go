package bus

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type constructionIndexTestReader struct {
	observations []pipeline.FlowInstanceObservation
	err          error
	selectionErr error
}

func constructionIndexContext(t testing.TB, source semanticview.Source) context.Context {
	t.Helper()
	bundle, found := semanticview.Bundle(source)
	if !found || bundle.SourceArtifact == nil {
		t.Fatal("index fixture requires its admitted source artifact")
	}
	fact, err := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	return correlation.WithSourceArtifactFact(context.Background(), fact)
}

func constructionIndexObservation(t testing.TB, source semanticview.Source, runID string, identity flowidentity.Instance, key string) pipeline.FlowInstanceObservation {
	t.Helper()
	observed, err := admitConstructionIndexTestObservation(source, runID, identity, key)
	if err != nil {
		t.Fatal(err)
	}
	return observed
}

func admitConstructionIndexTestObservation(source semanticview.Source, runID string, identity flowidentity.Instance, key string) (pipeline.FlowInstanceObservation, error) {
	bundle, found := semanticview.Bundle(source)
	if !found || bundle.SourceArtifact == nil {
		return pipeline.FlowInstanceObservation{}, fmt.Errorf("fixture requires admitted source artifact")
	}
	fact, err := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		return pipeline.FlowInstanceObservation{}, err
	}
	request, err := pipeline.NewExactFlowInstanceLookup(source, fact, flowidentity.RunScopedFlowInstance{RunID: runID, Route: identity.Route()})
	if err != nil {
		return pipeline.FlowInstanceObservation{}, err
	}
	schema, found := source.FlowSchemaByID(identity.TemplateID)
	if !found {
		return pipeline.FlowInstanceObservation{}, fmt.Errorf("index fixture requires its declared flow")
	}
	at := time.Unix(1700000000, 0).UTC()
	instance := pipeline.WorkflowInstance{
		WorkflowName: identity.TemplateID, WorkflowVersion: source.WorkflowVersion(), Mode: schema.EffectiveMode(), Status: "active",
		InstanceID: identity.InstanceID, StorageRef: identity.InstancePath, EntityID: identity.EntityID, InstanceKey: key,
		ParentFlowID: identity.ParentRoute.FlowID, ParentFlowInstance: identity.ParentRoute.FlowInstance, ParentEntityID: identity.ParentEntityID,
		CurrentState: "active", Revision: 1, CreatedAt: at, UpdatedAt: at,
	}
	if entity, declared := entityruntime.ResolveForFlow(source, identity.TemplateID); declared {
		instance.EntityType = entity.EntityType
	}
	run := runlifecycle.Snapshot{RunID: runID, State: runlifecycle.StateRunning, Origin: runlifecycle.DeploymentRunOrigin(), BundleHash: fact.BundleHash(), StartedAt: at}
	receipt := pipeline.FlowConstructionPublicationEvidence{Identity: identity, InstanceKey: key}
	readiness := pipeline.DynamicFlowRuntimeReadiness{
		Plan:            pipeline.DynamicFlowRuntimeReadinessPlan{Identity: identity, RunID: runID, BundleHash: fact.BundleHash(), WorkflowVersion: source.WorkflowVersion(), ExecutionMode: executionmode.Live},
		OwningRunSource: fact, RunStatus: "running", InstanceStatus: "active",
	}
	return pipeline.AdmitNativeFlowInstanceObservation(request, instance, run, 1, receipt, readiness)
}

func (r constructionIndexTestReader) LookupFlowInstance(ctx context.Context, request pipeline.FlowInstanceLookupRequest) (pipeline.FlowInstanceObservation, bool, error) {
	if err := ctx.Err(); err != nil {
		return pipeline.FlowInstanceObservation{}, false, err
	}
	if r.err != nil {
		return pipeline.FlowInstanceObservation{}, false, r.err
	}
	if request.DeclaredSelection() && r.selectionErr != nil {
		return pipeline.FlowInstanceObservation{}, false, r.selectionErr
	}
	var selected pipeline.FlowInstanceObservation
	for _, observed := range r.observations {
		identity := observed.Identity()
		if observed.Owner().RunID != request.RunID() || identity.TemplateID != request.FlowID() {
			continue
		}
		if request.ExactPath() != "" && request.ExactPath() != identity.InstancePath || request.DeclaredSelection() &&
			(identity.ParentRoute.FlowInstance != request.ParentInstance() || observed.InstanceKey() != request.InstanceKey()) {
			continue
		}
		if err := observed.ValidateSelection(request); err != nil {
			return pipeline.FlowInstanceObservation{}, false, err
		}
		if selected.Valid() {
			return pipeline.FlowInstanceObservation{}, false, &pipeline.FlowInstanceConstructionCorruption{RunID: request.RunID(), FlowID: request.FlowID(), Cause: fmt.Errorf("duplicate native selector")}
		}
		selected = observed
	}
	return selected, selected.Valid(), nil
}

func (r constructionIndexTestReader) ListFlowInstances(ctx context.Context, scope pipeline.FlowInstanceLookupScope) ([]pipeline.FlowInstanceObservation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.err != nil {
		return nil, r.err
	}
	var selected []pipeline.FlowInstanceObservation
	for _, observed := range r.observations {
		if observed.Owner().RunID != scope.RunID() {
			continue
		}
		included := slices.Contains(scope.FlowIDs(), observed.Identity().TemplateID)
		for _, owner := range scope.Coordinates() {
			if owner.Key() == observed.Owner().Key() {
				request, err := pipeline.NewExactFlowInstanceLookup(scope.Source(), scope.SourceFact(), owner)
				if err != nil {
					return nil, err
				}
				if err := observed.ValidateSelection(request); err != nil {
					return nil, err
				}
				included = true
			}
		}
		if included {
			selected = append(selected, observed)
		}
	}
	return selected, nil
}

func (s *targetRouteMemoryStore) installIndexObservation(observed pipeline.FlowInstanceObservation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, existing := range s.instanceObservations {
		if existing.Owner().Key() == observed.Owner().Key() {
			s.instanceObservations[i] = observed
			return
		}
	}
	s.instanceObservations = append(s.instanceObservations, observed)
}

func (s *targetRouteMemoryStore) setTestConstructionSource(source semanticview.Source) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.constructionIndexSource = source
}

func (s *targetRouteMemoryStore) LookupFlowInstance(ctx context.Context, request pipeline.FlowInstanceLookupRequest) (pipeline.FlowInstanceObservation, bool, error) {
	s.mu.Lock()
	reader := constructionIndexTestReader{observations: slices.Clone(s.instanceObservations)}
	s.mu.Unlock()
	return reader.LookupFlowInstance(ctx, request)
}

func (s *targetRouteMemoryStore) ListFlowInstances(ctx context.Context, scope pipeline.FlowInstanceLookupScope) ([]pipeline.FlowInstanceObservation, error) {
	s.mu.Lock()
	reader := constructionIndexTestReader{observations: slices.Clone(s.instanceObservations)}
	s.mu.Unlock()
	return reader.ListFlowInstances(ctx, scope)
}
