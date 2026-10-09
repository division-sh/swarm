package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestFlowConstructorRootEagerTreeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, keyed := range []bool{false, true} {
			shape := "keyless"
			if keyed {
				shape = "keyed"
			}
			t.Run(backend+"/"+shape, func(t *testing.T) {
				documents := map[string]string{
					"schema.yaml":                "name: root-construction\nstages:\n  pending: {}\n",
					"scout/schema.yaml":          "name: scout\nstages:\n  pending: {}\n",
					"scout/detail/schema.yaml":   "name: detail\n",
					"scout/detail/entities.yaml": "detail:\n  marker: {type: text, initial: original}\n",
				}
				if keyed {
					documents["schema.yaml"] = "name: root-construction\ninstance: request_id\nstages:\n  pending: {}\npins:\n  inputs:\n    - task.started\n"
					documents["events.yaml"] = "task.started:\n  request_id: text\n"
					documents["entities.yaml"] = "request:\n  request_id: text\n"
				}
				f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, documents, nil)
				runID := correlation.RunIDFromContext(f.ctx)
				req := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
				req.Instance = flowidentity.Stored(req.ContractBundle, ".", runID, runID, runID, "")
				if keyed {
					req.ConstructorInput, req.ResolvedKey = "task.started", "r1"
					req.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "task.started", "constructor-fixture", "", []byte(`{"request_id":"r1"}`), 0, correlation.RunIDFromContext(f.ctx), events.EventEnvelope{}, req.OccurredAt)
				}
				plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
				if err != nil {
					t.Fatal(err)
				}
				plans := plan.ConstructionPlans()
				if len(plans) != 3 || plans[0].Identity.InstancePath != runID || plans[0].Identity.EntityID != runID || plans[1].Identity.InstancePath != "scout" || plans[2].Identity.InstancePath != "scout/detail" {
					t.Fatalf("root construction lost concrete ancestry: %+v", plans)
				}
				result, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan)
				if err != nil || !result.Acknowledged || !result.Created {
					t.Fatalf("root tree commit: %+v %v", result, err)
				}
				for _, constructor := range plans {
					owner, err := flowidentity.NewRunScopedFlowInstance(correlation.RunIDFromContext(f.ctx), constructor.Identity.Route())
					if err != nil {
						t.Fatal(err)
					}
					stored, found, err := f.workflows.Load(f.ctx, owner)
					if err != nil || !found || stored.EntityID != constructor.Identity.EntityID || stored.ParentEntityID != constructor.Identity.ParentEntityID {
						t.Fatalf("root/descendant readback: %+v found=%t err=%v", stored, found, err)
					}
				}
			})
		}
	}
}

func newEagerFlowConstructorFixture(t *testing.T, backend string) receiverConfigActivationFixture {
	t.Helper()
	f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
		"schema.yaml":                       "name: eager-construction\n",
		"review/schema.yaml":                "name: review\ninstance: request_id\nstages:\n  pending: {}\npins:\n  inputs:\n    - task.started\n",
		"review/entities.yaml":              "review_item:\n  request_id: text\n",
		"review/events.yaml":                "task.started:\n",
		"review/scout/schema.yaml":          "name: scout\nstages:\n  pending: {}\n",
		"review/scout/detail/schema.yaml":   "name: detail\n",
		"review/scout/detail/entities.yaml": "detail:\n  marker: {type: text, initial: original}\n",
		"review/deferred/schema.yaml":       "name: deferred\ninstance: job_id\npins:\n  inputs:\n    - job.started\n",
		"review/deferred/events.yaml":       "job.started:\n",
		"review/deferred/entities.yaml":     "job:\n  job_id: text\n",
	}, nil)
	runID := correlation.RunIDFromContext(f.ctx)
	request := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
	request.Instance = flowidentity.Stored(request.ContractBundle, ".", runID, runID, runID, "")
	plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil {
		t.Fatal(err)
	}
	return f
}

func eagerFlowConstructorPlan(t *testing.T, f receiverConfigActivationFixture) pipeline.FlowInstanceActivationPlan {
	t.Helper()
	req := f.request("business-key", "r1", "first")
	plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Children) != 1 || len(plan.Children[0].Children) != 1 {
		t.Fatalf("constructor omitted keyless descendants or created a keyed child: %+v", plan)
	}
	return plan
}

func TestFlowConstructorCommitsKeylessDescendantsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newEagerFlowConstructorFixture(t, backend)
			plan := eagerFlowConstructorPlan(t, f)
			parentOwner, err := flowidentity.NewRunScopedFlowInstance(correlation.RunIDFromContext(f.ctx), flowidentity.StoredRoute(".", correlation.RunIDFromContext(f.ctx), correlation.RunIDFromContext(f.ctx)))
			if err != nil {
				t.Fatal(err)
			}
			parent, found, err := f.workflows.Load(f.ctx, parentOwner)
			if err != nil || !found || parent.EntityID != plan.Identity.ParentEntityID {
				t.Fatalf("constructor lost its stored parent: %+v found=%t err=%v", parent, found, err)
			}
			committed, err := agentFixtureFlowActivationCommitter{store: f.store}.CommitFlowInstanceActivation(f.ctx, plan)
			if err != nil || !committed.Acknowledged || !committed.Created {
				t.Fatalf("constructor commit: %+v %v", committed, err)
			}
			runID := correlation.RunIDFromContext(f.ctx)
			for _, coordinates := range []struct{ path, flow string }{{"review/r1", "review"}, {"review/r1/scout", "review/scout"}, {"review/r1/scout/detail", "review/scout/detail"}} {
				path := coordinates.path
				owner, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.StoredRoute(coordinates.flow, flowidentity.LogicalInstanceID(path), path))
				if err != nil {
					t.Fatal(err)
				}
				instance, found, err := f.workflows.Load(f.ctx, owner)
				if err != nil || !found || instance.StorageRef != path || instance.Revision != 1 {
					t.Fatalf("eager header %s: found=%t instance=%+v err=%v", path, found, instance, err)
				}
				if path != "review/r1" && instance.ParentEntityID == "" {
					t.Fatalf("descendant lost parent: %+v", instance)
				}
			}
			for table, want := range map[string]int{"flow_instances": 4, "entity_state": 2, "workflow_instance_initial_materializations": 4, "flow_instance_runtime_readiness": 4} {
				var count int
				if err := f.db.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM "+table+" WHERE run_id=$1", runID).Scan(&count); err != nil || count != want {
					t.Fatalf("atomic constructor %s rows=%d err=%v want=%d", table, count, err, want)
				}
			}
			replayed, err := agentFixtureFlowActivationCommitter{store: f.store}.CommitFlowInstanceActivation(f.ctx, plan)
			if err != nil || !replayed.Acknowledged || replayed.Created {
				t.Fatalf("constructor replay: %+v %v", replayed, err)
			}
			for _, result := range []pipeline.CommittedFlowInstanceActivation{committed, replayed} {
				if err := result.Validate(); err != nil || len(result.Children) != 1 || len(result.Children[0].Children) != 1 ||
					!result.Children[0].Acknowledged || !result.Children[0].Children[0].Acknowledged ||
					result.Children[0].Created != result.Created || result.Children[0].Children[0].Created != result.Created {
					t.Fatalf("constructor lost exact descendant evidence: %+v %v", result, err)
				}
			}
		})
	}
}

func TestA9StoredKeyedParentConstructsAndRestoresDescendantsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newEagerFlowConstructorFixture(t, backend)
			runID := correlation.RunIDFromContext(f.ctx)
			request := f.request("business-parent", "stored-parent", "original")
			request.Instance.EntityID = eventtest.UUID("admitted-parent-entity")
			if request.Instance.EntityID == flowidentity.EntityID(request.Instance.InstancePath) {
				t.Fatal("proof requires a stored, non-derived parent identity")
			}
			parent, err := f.manager.PrepareFlowInstanceActivation(f.ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			committer := agentFixtureFlowActivationCommitter{store: f.store}
			if committed, err := committer.CommitFlowInstanceActivation(f.ctx, parent); err != nil || !committed.Acknowledged || !committed.Created {
				t.Fatalf("stored-parent construction: %+v %v", committed, err)
			}
			child := sqliteFlowActivationRequest(f.bundle, "review/deferred", "stored-leaf", "", "")
			child.Instance, err = flowidentity.KeyedChild(child.ContractBundle, parent.Identity, "review/deferred", "stored-leaf")
			if err != nil {
				t.Fatal(err)
			}
			child.ConstructorInput, child.ResolvedKey = "job.started", "business-leaf"
			child.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "job.started", "constructor-fixture", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, child.OccurredAt)
			leaf, err := f.manager.PrepareFlowInstanceActivation(f.ctx, child)
			if err != nil {
				t.Fatal(err)
			}
			if committed, err := committer.CommitFlowInstanceActivation(f.ctx, leaf); err != nil || !committed.Acknowledged || !committed.Created {
				t.Fatalf("stored-parent keyed descendant: %+v %v", committed, err)
			}
			plans := append(parent.ConstructionPlans(), leaf)
			beforeRefusals, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
			if err != nil {
				t.Fatal(err)
			}
			for _, plan := range plans {
				owner := flowidentity.RunScopedFlowInstance{RunID: runID, Route: plan.Identity.Route()}
				evidence, err := ReadReceiverConstructionPublicationForTest(f.ctx, f.store, owner, plan.Identity.EntityID)
				if err != nil || evidence.Identity != plan.Identity || evidence.CreatingInput != plan.CreatingInput {
					t.Fatalf("native receipt lost actual parent: %#v want=%+v err=%v", evidence, plan.Identity, err)
				}
				if err := evidence.Identity.ValidateConstruction(child.ContractBundle, runID); err != nil {
					t.Fatalf("restored ancestry rejected: %v", err)
				}
				if plan.Identity.ParentRoute.FlowID == "review" && plan.Identity.ParentEntityID != request.Instance.EntityID {
					t.Fatalf("descendant substituted a parent hash: %+v", plan.Identity)
				}
				for _, foreign := range []struct{ run, path, entity string }{
					{uuid.NewString(), plan.Identity.InstancePath, plan.Identity.EntityID},
					{runID, plan.Identity.InstancePath, uuid.NewString()},
					{runID, "other/instance", plan.Identity.EntityID},
					{runID, plan.Identity.TemplateID, plan.Identity.EntityID},
				} {
					coordinate := flowidentity.RunScopedFlowInstance{RunID: foreign.run,
						Route: flowidentity.StoredRoute(plan.Identity.ScopeKey, "", foreign.path)}
					if receipt, err := ReadReceiverConstructionPublicationForTest(f.ctx, f.store, coordinate, foreign.entity); err == nil || receipt.Identity != (flowidentity.Instance{}) {
						t.Fatalf("foreign native coordinate became parent evidence: %#v %v", receipt, err)
					}
				}
				for _, mutate := range []func(*flowidentity.Instance){
					func(i *flowidentity.Instance) { i.ParentRoute.FlowInstance = "other/parent" },
					func(i *flowidentity.Instance) { i.ParentEntityID = uuid.NewString() },
					func(i *flowidentity.Instance) { i.InstancePath = i.TemplateID },
				} {
					bad := evidence.Identity
					mutate(&bad)
					if err := bad.ValidateConstruction(child.ContractBundle, runID); err == nil {
						t.Fatalf("unowned publication coordinate admitted: %+v", bad)
					}
				}
			}
			afterRefusals, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
			if err != nil || !reflect.DeepEqual(beforeRefusals, afterRefusals) {
				t.Fatalf("refused construction observations mutated publication or result journals: %v", err)
			}
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
			if err != nil {
				t.Fatal(err)
			}
			for _, plan := range []pipeline.FlowInstanceActivationPlan{parent, leaf} {
				if replay, err := committer.CommitFlowInstanceActivation(f.ctx, plan); err != nil || !replay.Acknowledged || replay.Created {
					t.Fatalf("exact descendant replay: %+v %v", replay, err)
				}
			}
			after, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("constructor replay changed immutable descendants: %v", err)
			}
		})
	}
}

func TestFlowConstructorDescendantFaultRollsBackTreeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, table := range []string{"flow_instances", "entity_state", "workflow_instance_initial_materializations", "flow_instance_runtime_readiness"} {
			t.Run(backend+"/"+table, func(t *testing.T) {
				f := newEagerFlowConstructorFixture(t, backend)
				plan := eagerFlowConstructorPlan(t, f)
				before, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
				if err != nil {
					t.Fatal(err)
				}
				column := "instance_path"
				if table == "entity_state" {
					column = "flow_instance"
				}
				removeFault := installReceiverComposedFault(t, f.db, backend, table, "INSERT", "NEW."+column+"='review/r1/scout/detail'")
				result, err := agentFixtureFlowActivationCommitter{store: f.store}.CommitFlowInstanceActivation(f.ctx, plan)
				if err == nil || !strings.Contains(err.Error(), "typed_receiver_owner_fault") || result.Acknowledged {
					t.Fatalf("descendant fault escaped atomic tree: %+v %v", result, err)
				}
				after, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("failed tree construction changed its stored parent or children: %v", err)
				}
				removeFault()
				result, err = agentFixtureFlowActivationCommitter{store: f.store}.CommitFlowInstanceActivation(f.ctx, plan)
				if err != nil || !result.Acknowledged || !result.Created || len(result.Children) != 1 {
					t.Fatalf("exact retry after rollback: %+v %v", result, err)
				}
			})
		}
	}
}

func TestFlowConstructorDescendantCancellationAndCorruptionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newEagerFlowConstructorFixture(t, backend)
			plan := eagerFlowConstructorPlan(t, f)
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
			if err != nil {
				t.Fatal(err)
			}
			canceled, cancel := context.WithCancel(f.ctx)
			cancel()
			result, err := agentFixtureFlowActivationCommitter{store: f.store}.CommitFlowInstanceActivation(canceled, plan)
			if !errors.Is(err, context.Canceled) || result.Acknowledged {
				t.Fatalf("canceled construction acquired tree: %+v %v", result, err)
			}
			after, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("canceled tree construction changed its stored parent or children: %v", err)
			}
			if result, err = (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil || !result.Acknowledged {
				t.Fatalf("initial construction: %+v %v", result, err)
			}
			if _, err := f.db.ExecContext(f.ctx, `DELETE FROM workflow_instance_initial_materializations WHERE run_id=$1 AND instance_path='review/r1/scout/detail'`, correlation.RunIDFromContext(f.ctx)); err != nil {
				t.Fatal(err)
			}
			result, err = agentFixtureFlowActivationCommitter{store: f.store}.CommitFlowInstanceActivation(f.ctx, plan)
			if err == nil || !strings.Contains(err.Error(), "missing or conflicting descendant") || result.Acknowledged {
				t.Fatalf("replay repaired corrupt descendant: %+v %v", result, err)
			}
			var receipts int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM workflow_instance_initial_materializations WHERE run_id=$1`, correlation.RunIDFromContext(f.ctx)).Scan(&receipts); err != nil || receipts != 3 {
				t.Fatalf("refused replay changed receipt count=%d err=%v", receipts, err)
			}
		})
	}
}
