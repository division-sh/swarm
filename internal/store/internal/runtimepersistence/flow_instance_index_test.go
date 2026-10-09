package runtimepersistence

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestR7DirectoryScalarSelectorsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, scalar := range []struct {
			name, kind, material string
			value, missing       any
		}{
			{"text", "text", "business-key", "business-key", "missing-key"},
			{"integer", "integer", "7", int64(7), int64(8)},
			{"numeric", "double", "7.5", json.Number("7.5"), json.Number("8.5")},
			{"integral_numeric", "double", "7", json.Number("7.0"), json.Number("8.0")},
			{"boolean", "boolean", "true", true, false},
		} {
			t.Run(backend+"/"+scalar.name, func(t *testing.T) {
				f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
					"schema.yaml":          "name: scalar-index\n",
					"events.yaml":          "checkpoint:\n",
					"worker/schema.yaml":   "name: worker\ninstance: item_id\npins:\n  inputs:\n    - item.created\n",
					"worker/entities.yaml": "item:\n  item_id: " + scalar.kind + "\n",
					"worker/events.yaml":   "item.created:\n  item_id: " + scalar.kind + "\n",
				}, nil)
				runID := correlation.RunIDFromContext(f.ctx)
				rootRequest := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
				rootRequest.Instance = flowidentity.Stored(rootRequest.ContractBundle, ".", runID, runID, runID, "")
				root, err := f.manager.PrepareFlowInstanceActivation(f.ctx, rootRequest)
				if err != nil {
					t.Fatal(err)
				}
				committer := agentFixtureFlowActivationCommitter{store: f.store}
				if _, err := committer.CommitFlowInstanceActivation(f.ctx, root); err != nil {
					t.Fatal(err)
				}
				request := sqliteFlowActivationRequest(f.bundle, "worker", "stored-receiver", "", "")
				request.Instance, err = flowidentity.KeyedChild(request.ContractBundle, root.Identity, "worker", "stored-receiver")
				if err != nil {
					t.Fatal(err)
				}
				request.Instance.EntityID = uuid.NewString()
				if request.Instance.EntityID == flowidentity.EntityID(request.Instance.InstancePath) {
					t.Fatal("proof requires actual, non-derived receiver identity")
				}
				payload, err := json.Marshal(map[string]any{"item_id": scalar.value})
				if err != nil {
					t.Fatal(err)
				}
				request.ConstructorInput, request.ResolvedKey = "item.created", scalar.value
				request.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "item.created", "constructor-fixture", "", payload, 0, runID, events.EventEnvelope{}, request.OccurredAt)
				plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, request)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := committer.CommitFlowInstanceActivation(f.ctx, plan); err != nil {
					t.Fatal(err)
				}
				fact, present := correlation.SourceArtifactFactFromContext(f.ctx)
				if !present {
					t.Fatal("directory proof requires admitted source")
				}
				keys, err := pipeline.AdmitFlowInstanceKeyMaterial(request.ContractBundle, "worker", scalar.value)
				if err != nil {
					t.Fatal(err)
				}
				lookup, err := pipeline.NewDeclaredFlowInstanceLookup(request.ContractBundle, fact, runID, "worker", root.Identity, keys)
				if err != nil {
					t.Fatal(err)
				}
				index := f.store.(pipeline.FlowInstanceIndexReader)
				observed, found, err := index.LookupFlowInstance(f.ctx, lookup)
				if err != nil || !found || observed.Identity() != plan.Identity || observed.InstanceKey() != scalar.material {
					t.Fatalf("typed selector lost actual receiver: %+v found=%t err=%v", observed, found, err)
				}
				missingKeys, err := pipeline.AdmitFlowInstanceKeyMaterial(request.ContractBundle, "worker", scalar.missing)
				if err != nil {
					t.Fatal(err)
				}
				missing, err := pipeline.NewDeclaredFlowInstanceLookup(request.ContractBundle, fact, runID, "worker", root.Identity, missingKeys)
				if err != nil {
					t.Fatal(err)
				}
				if observed, found, err := index.LookupFlowInstance(f.ctx, missing); err != nil || found || observed.Valid() {
					t.Fatalf("missing selector broadened or invented a receiver: %+v found=%t err=%v", observed, found, err)
				}
				proveR7ScalarHistoricalIndex(t, f, request, scalar.value, scalar.material)
				owner := flowidentity.RunScopedFlowInstance{RunID: runID, Route: plan.Identity.Route()}
				if err := f.workflows.MarkTerminated(f.ctx, owner, identity.NormalizeEntityID(plan.Identity.EntityID), request.OccurredAt.Add(time.Hour)); err != nil {
					t.Fatal(err)
				}
				observed, found, err = index.LookupFlowInstance(f.ctx, lookup)
				if err != nil || !found || observed.Identity() != plan.Identity {
					t.Fatalf("terminal stored receiver became a create opportunity: %+v found=%t err=%v", observed, found, err)
				}
				stored, err := observed.WorkflowInstance()
				if err != nil || stored.Status != "terminated" {
					t.Fatalf("directory erased terminal state: %+v %v", stored, err)
				}
			})
		}
	}
}

func proveR7ScalarHistoricalIndex(t *testing.T, f receiverConfigActivationFixture, request pipeline.FlowInstanceActivationRequest, value any, material string) {
	t.Helper()
	runID := correlation.RunIDFromContext(f.ctx)
	marker := eventtest.ExistingRunRootIngress(uuid.NewString(), "checkpoint", "test", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
	if err := commitSemanticPipelineProcessedEventFixture(f.ctx, f.store, marker); err != nil {
		t.Fatal(err)
	}
	forks := f.store.(interface {
		PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
		MaterializeRunFork(context.Context, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
	})
	cut, err := forks.PlanRunFork(f.ctx, runfork.RunForkPlanRequest{SourceRunID: runID, At: marker.ID()})
	if err != nil {
		t.Fatal(err)
	}
	fork, err := forks.MaterializeRunFork(f.ctx, runfork.RunForkMaterializeRequest{SourceRunID: runID, At: marker.ID()})
	if err != nil {
		t.Fatal(err)
	}
	fact, present := correlation.SourceArtifactFactFromContext(f.ctx)
	if !present {
		t.Fatal("historical scalar proof requires admitted source")
	}
	index := f.store.(pipeline.FlowInstanceIndexReader)
	rootRequest, err := pipeline.NewExactFlowInstanceLookup(request.ContractBundle, fact, flowidentity.RunScopedFlowInstance{RunID: fork.ForkRunID, Route: flowidentity.StoredRoute(".", fork.ForkRunID, fork.ForkRunID)})
	if err != nil {
		t.Fatal(err)
	}
	parent, found, err := index.LookupFlowInstance(f.ctx, rootRequest)
	if err != nil || !found {
		t.Fatalf("historical scalar parent lost fixed construction: found=%t err=%v", found, err)
	}
	keys, err := pipeline.AdmitFlowInstanceKeyMaterial(request.ContractBundle, "worker", value)
	if err != nil {
		t.Fatal(err)
	}
	lookup, err := pipeline.NewDeclaredFlowInstanceLookup(request.ContractBundle, fact, fork.ForkRunID, "worker", parent.Identity(), keys)
	if err != nil {
		t.Fatal(err)
	}
	observed, found, err := index.LookupFlowInstance(f.ctx, lookup)
	if err != nil || !found || observed.Identity().EntityID != request.Instance.EntityID || observed.InstanceKey() != material {
		t.Fatalf("historical scalar selector lost non-derived receiver: %+v found=%t err=%v", observed, found, err)
	}
	historical, projected := observed.HistoricalConstruction()
	if !projected || historical.SourceOwner.RunID != runID || historical.SourceOwner.Route.InstancePath != request.Instance.InstancePath || historical.SourceRevision != cut.ForkPoint.Revision {
		t.Fatalf("historical scalar selector lost fixed-cut provenance: %+v", historical)
	}
	if _, ready := observed.Readiness(); ready {
		t.Fatal("materialize-only scalar lookup fabricated readiness")
	}
	if _, native, err := observed.NativeConstruction(); err != nil || native {
		t.Fatalf("materialize-only scalar lookup fabricated native receipt: native=%t err=%v", native, err)
	}
}

func TestR7ConstructionAddressImmutableBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
				"schema.yaml":        "name: immutable-construction\ninstance: item_id\npins:\n  inputs:\n    - item.created\n",
				"events.yaml":        "item.created:\n  item_id: text\n",
				"entities.yaml":      "item:\n  item_id: text\n",
				"detail/schema.yaml": "name: detail\n",
			}, nil)
			runID := correlation.RunIDFromContext(f.ctx)
			request := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
			request.Instance = flowidentity.Stored(request.ContractBundle, ".", runID, runID, runID, "")
			request.ConstructorInput, request.ResolvedKey = "item.created", "original"
			request.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "item.created", "constructor-fixture", "", []byte(`{"item_id":"original"}`), 0, runID, events.EventEnvelope{}, request.OccurredAt)
			plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil {
				t.Fatal(err)
			}
			index := f.store.(pipeline.FlowInstanceIndexReader)
			fact, present := correlation.SourceArtifactFactFromContext(f.ctx)
			if !present {
				t.Fatal("index proof requires the admitted source")
			}
			for _, construction := range plan.ConstructionPlans() {
				owner, err := flowidentity.NewRunScopedFlowInstance(runID, construction.Identity.Route())
				if err != nil {
					t.Fatal(err)
				}
				stored, found, err := f.workflows.Load(f.ctx, owner)
				if err != nil || !found || stored.ParentFlowInstance != construction.Identity.ParentRoute.FlowInstance || stored.InstanceKey != construction.Instance.InstanceKey {
					t.Fatalf("constructed address readback: %+v found=%t err=%v", stored, found, err)
				}
				evidence, err := f.store.(pipeline.FlowConstructionPublicationReader).LoadFlowConstructionPublication(f.ctx, owner, stored.EntityID)
				if err != nil || evidence.InstanceKey != stored.InstanceKey || evidence.Identity != construction.Identity {
					t.Fatalf("immutable address evidence: %+v err=%v", evidence, err)
				}
				lookup, err := pipeline.NewExactFlowInstanceLookup(request.ContractBundle, fact, owner)
				if err != nil {
					t.Fatal(err)
				}
				observed, found, err := index.LookupFlowInstance(f.ctx, lookup)
				if err != nil || !found || observed.Identity() != construction.Identity || observed.InstanceKey() != stored.InstanceKey || observed.RunRevision() <= 0 {
					t.Fatalf("native index lost exact construction: %+v found=%t err=%v", observed, found, err)
				}
				if _, found := observed.Readiness(); !found {
					t.Fatal("native index discarded desired attachment")
				}
			}
			scope, err := pipeline.NewFlowInstanceLookupScope(request.ContractBundle, fact, runID, []string{"detail"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			observed, err := index.ListFlowInstances(f.ctx, scope)
			if err != nil || len(observed) != 1 || observed[0].Identity().TemplateID != "detail" {
				t.Fatalf("bounded index broadened scope: %+v %v", observed, err)
			}
			empty, err := pipeline.NewFlowInstanceLookupScope(request.ContractBundle, fact, runID, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if observed, err := index.ListFlowInstances(f.ctx, empty); err != nil || len(observed) != 0 {
				t.Fatalf("empty index scope became run inventory: %+v %v", observed, err)
			}
			owner, err := flowidentity.NewRunScopedFlowInstance(runID, plan.Identity.Route())
			if err != nil {
				t.Fatal(err)
			}
			record, err := plan.PersistenceRecord()
			if err != nil {
				t.Fatal(err)
			}
			mutation := record.State
			mutation.Transition = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
			mutation.ExpectedState, mutation.ExpectedRevision = mutation.CurrentState, 1
			mutation.UpdatedAt = mutation.CreatedAt.Add(time.Second)
			mutation.Fields = json.RawMessage(`{"item_id":"changed-business-field"}`)
			writer := f.store.(pipeline.WorkflowEngineMutationOwner)
			if result, err := writer.CommitWorkflowEngineMutation(f.ctx, pipeline.WorkflowEngineMutationCommand{State: mutation}); err != nil || !result.Committed {
				t.Fatalf("ordinary field update: committed=%t err=%v", result.Committed, err)
			}
			stored, found, err := f.workflows.Load(f.ctx, owner)
			if err != nil || !found || stored.InstanceKey != "original" || stored.Fields["item_id"] != "changed-business-field" || stored.Revision != 2 {
				t.Fatalf("mutable fields changed construction identity: %+v found=%t err=%v", stored, found, err)
			}
			evidence, err := f.store.(pipeline.FlowConstructionPublicationReader).LoadFlowConstructionPublication(f.ctx, owner, stored.EntityID)
			if err != nil || evidence.InstanceKey != "original" || evidence.Fields["item_id"] != "original" {
				t.Fatalf("field update rewrote the creating receipt: %+v err=%v", evidence, err)
			}
			keys, err := pipeline.AdmitFlowInstanceKeyMaterial(request.ContractBundle, ".", "original")
			if err != nil {
				t.Fatal(err)
			}
			lookup, err := pipeline.NewDeclaredFlowInstanceLookup(request.ContractBundle, fact, runID, ".", flowidentity.Instance{}, keys)
			if err != nil {
				t.Fatal(err)
			}
			selected, found, err := index.LookupFlowInstance(f.ctx, lookup)
			if err != nil || !found || selected.InstanceKey() != "original" || selected.HeaderRevision() != 2 {
				t.Fatalf("index reconstructed key from mutable fields: %+v found=%t err=%v", selected, found, err)
			}
			mutation.ExpectedRevision = 2
			mutation.UpdatedAt = mutation.UpdatedAt.Add(time.Second)
			mutation.InstanceKey = "changed-business-field"
			if result, err := writer.CommitWorkflowEngineMutation(f.ctx, pipeline.WorkflowEngineMutationCommand{State: mutation}); err == nil || result.Committed {
				t.Fatalf("ordinary update rewrote immutable key: committed=%t err=%v", result.Committed, err)
			}
			stored, found, err = f.workflows.Load(f.ctx, owner)
			if err != nil || !found || stored.InstanceKey != "original" || stored.Revision != 2 {
				t.Fatalf("rejected key update mutated state: %+v found=%t err=%v", stored, found, err)
			}
		})
	}
}
