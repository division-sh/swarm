package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestR7ReadinessDiagnosticsDoNotExemptLaterNativeForkConstructionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
				"schema.yaml":               "name: later-native-diagnostic\n",
				"events.yaml":               "checkpoint:\n",
				"worker/schema.yaml":        "name: worker\ninstance: item_id\npins:\n  inputs: [item.created]\n",
				"worker/entities.yaml":      "item:\n  item_id: text\n",
				"worker/events.yaml":        "item.created:\n  item_id: text\n",
				"worker/detail/schema.yaml": "name: fieldless-detail\n",
			}, nil)
			f.constructKeylessRoot(t)
			runID := correlation.RunIDFromContext(f.ctx)
			marker := eventtest.ExistingRunRootIngress(uuid.NewString(), "checkpoint", "test", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
			if err := commitSemanticPipelineProcessedEventFixture(f.ctx, f.store, marker); err != nil {
				t.Fatal(err)
			}
			fork, err := f.store.(interface {
				MaterializeRunFork(context.Context, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
			}).MaterializeRunFork(f.ctx, runfork.RunForkMaterializeRequest{SourceRunID: runID, At: marker.ID()})
			if err != nil {
				t.Fatal(err)
			}
			fact := mustStoreTestSourceArtifactFact(f.bundle.SourceArtifact.BundleHash())
			lookup, err := pipeline.NewExactFlowInstanceLookup(semanticview.Wrap(f.bundle), fact, flowidentity.RunScopedFlowInstance{RunID: fork.ForkRunID, Route: flowidentity.StoredRoute(".", fork.ForkRunID, fork.ForkRunID)})
			if err != nil {
				t.Fatal(err)
			}
			root, found, err := f.store.(pipeline.FlowInstanceIndexReader).LookupFlowInstance(f.ctx, lookup)
			if err != nil || !found {
				t.Fatalf("exact projected parent: found=%t err=%v", found, err)
			}
			if _, projected := root.HistoricalConstruction(); !projected {
				t.Fatal("materialize-only parent lost fixed construction provenance")
			}
			proveR7HistoricalDiagnosticRefusesCrossedHeader(t, f, fact, fork.ForkRunID, root.Identity())
			forkCtx := correlation.WithRunID(f.ctx, fork.ForkRunID)
			request := sqliteFlowActivationRequest(f.bundle, "worker", "later", "", "")
			request.Instance, err = flowidentity.KeyedChild(request.ContractBundle, root.Identity(), "worker", "later")
			if err != nil {
				t.Fatal(err)
			}
			request.ConstructorInput, request.ResolvedKey = "item.created", "later"
			request.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "item.created", "test", "", []byte(`{"item_id":"later"}`), 0, fork.ForkRunID, events.EventEnvelope{}, request.OccurredAt)
			plan, err := f.manager.PrepareFlowInstanceActivation(forkCtx, request)
			if err != nil {
				t.Fatal(err)
			}
			committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(forkCtx, plan)
			if err != nil || !committed.Acknowledged || !committed.Created {
				t.Fatalf("later native construction: %+v %v", committed, err)
			}
			reader := f.store.(pipeline.DynamicFlowRuntimeReadinessPersistence)
			items, err := reader.InspectDynamicFlowRuntimeReadinessForRun(forkCtx, fork.ForkRunID, fact)
			if err != nil || len(items) != 2 || items[0].Plan.Identity != request.Instance || !items[0].Pending() {
				t.Fatalf("fork origin erased its later native attachment: %+v %v", items, err)
			}
			if len(plan.Children) != 1 || plan.Children[0].Identity.TemplateID != "worker/detail" || plan.Children[0].Instance.EntityType != "" || len(plan.Children[0].Instance.Fields) != 0 || items[1].Plan.Identity != plan.Children[0].Identity {
				t.Fatalf("later native fieldless descendant is not exact constructed evidence: %+v", plan.Children)
			}
			projection, err := reader.InspectDynamicFlowRuntimeReadinessForSource(forkCtx, fact)
			if err != nil || len(projection.CurrentPending) != 3 || len(projection.CurrentCompleted) != 0 || len(projection.SourceTransitionRequired) != 0 {
				t.Fatalf("mixed native/fixed diagnostic inventory: %+v %v", projection, err)
			}
			child := plan.Children[0].Identity
			owner := flowidentity.RunScopedFlowInstance{RunID: fork.ForkRunID, Route: child.Route()}
			// This closed fault leaves the already-NULL fieldless type unchanged;
			// only its native attachment is removed, not its construction receipt.
			if err := PrepareReceiverNullableConstructionObservationForTest(forkCtx, f.store, owner, child.EntityID); err != nil {
				t.Fatal(err)
			}
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(forkCtx, f.store)
			if err != nil {
				t.Fatal(err)
			}
			for _, surface := range []string{"source", "run"} {
				var corruption *pipeline.FlowInstanceConstructionCorruption
				err := inspectConstructedRouteReadiness(f, reader, fact, fork.ForkRunID, surface)
				if !errors.As(err, &corruption) || corruption.RunID != fork.ForkRunID || corruption.InstancePath != child.InstancePath || !strings.Contains(err.Error(), "native header lacks its desired attachment evidence") {
					t.Errorf("%s granted fixed-cut exemption to later native absence: %v", surface, err)
				}
			}
			if after, err := ReadSelectedForkApplicationStorageSnapshotForTest(forkCtx, f.store); err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("later-native refusal mutated fixed or native evidence: %v", err)
			}
		})
	}
}

func proveR7HistoricalDiagnosticRefusesCrossedHeader(t *testing.T, f receiverConfigActivationFixture, fact correlation.SourceArtifactFact, forkRunID string, instance flowidentity.Instance) {
	t.Helper()
	owner := flowidentity.RunScopedFlowInstance{RunID: forkRunID, Route: instance.Route()}
	restore, err := FaultFlowConstructorHeaderForTest(f.ctx, f.store, owner, "mode", "template")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := restore(f.ctx); err != nil {
			t.Error(err)
		}
	}()
	before, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
	if err != nil {
		t.Fatal(err)
	}
	reader := f.store.(pipeline.DynamicFlowRuntimeReadinessPersistence)
	for _, surface := range []string{"source", "run"} {
		var corruption *pipeline.FlowInstanceConstructionCorruption
		if err := inspectConstructedRouteReadiness(f, reader, fact, forkRunID, surface); !errors.As(err, &corruption) || corruption.RunID != forkRunID || corruption.InstancePath != instance.InstancePath {
			t.Errorf("%s ignored contradictory projected header: %v", surface, err)
		}
	}
	if after, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store); err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("historical diagnostic refusal changed fixed-cut evidence: %v", err)
	}
}
