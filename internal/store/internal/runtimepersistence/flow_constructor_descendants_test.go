package runtimepersistence

import (
	"context"
	"errors"
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
					"schema.yaml":                "name: root-construction\nstages:\n  pending: {initial: true}\n",
					"scout/schema.yaml":          "name: scout\nstages:\n  pending: {initial: true}\n",
					"scout/detail/schema.yaml":   "name: detail\n",
					"scout/detail/entities.yaml": "detail:\n  marker: {type: text, initial: original}\n",
				}
				if keyed {
					documents["schema.yaml"] = "name: root-construction\ninstance: request_id\nstages:\n  pending: {initial: true}\npins:\n  inputs:\n    events: [task.started]\n"
					documents["events.yaml"] = "task.started:\n  request_id: text\n"
					documents["entities.yaml"] = "request:\n  request_id: text\n"
				}
				f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, documents, nil)
				runID := correlation.RunIDFromContext(f.ctx)
				req := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
				req.Instance = flowidentity.Stored(req.ContractBundle, ".", runID, runID, runID, "")
				if keyed {
					req.ConstructorInput, req.ResolvedKey = "task.started", "r1"
					req.Config = map[string]any{"request_id": "r1"}
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
	return newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
		"schema.yaml":                       "name: eager-construction\n",
		"review/schema.yaml":                "name: review\ninstance: request_id\nstages:\n  pending: {initial: true}\npins:\n  inputs:\n    events: [task.started]\n",
		"review/entities.yaml":              "review_item:\n  request_id: text\n",
		"review/events.yaml":                "task.started:\n",
		"review/scout/schema.yaml":          "name: scout\nstages:\n  pending: {initial: true}\n",
		"review/scout/detail/schema.yaml":   "name: detail\n",
		"review/scout/detail/entities.yaml": "detail:\n  marker: {type: text, initial: original}\n",
		"review/deferred/schema.yaml":       "name: deferred\ninstance: job_id\npins:\n  inputs:\n    events: [job.started]\n",
		"review/deferred/events.yaml":       "job.started:\n",
		"review/deferred/entities.yaml":     "job:\n  job_id: text\n",
	}, nil)
}

func eagerFlowConstructorPlan(t *testing.T, f receiverConfigActivationFixture) pipeline.FlowInstanceActivationPlan {
	t.Helper()
	req := f.request("business-key", "r1", "first")
	req.Config = map[string]any{"request_id": "business-key"}
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
			assertConstructorRows(t, f, backend, 0)
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
			for table, want := range map[string]int{"flow_instances": 3, "entity_state": 2, "workflow_instance_initial_materializations": 3, "flow_instance_runtime_readiness": 3} {
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

func TestFlowConstructorDescendantFaultRollsBackTreeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, table := range []string{"flow_instances", "entity_state", "workflow_instance_initial_materializations", "flow_instance_runtime_readiness"} {
			t.Run(backend+"/"+table, func(t *testing.T) {
				f := newEagerFlowConstructorFixture(t, backend)
				plan := eagerFlowConstructorPlan(t, f)
				column := "instance_path"
				if table == "entity_state" {
					column = "flow_instance"
				}
				removeFault := installReceiverComposedFault(t, f.db, backend, table, "INSERT", "NEW."+column+"='review/r1/scout/detail'")
				result, err := agentFixtureFlowActivationCommitter{store: f.store}.CommitFlowInstanceActivation(f.ctx, plan)
				if err == nil || !strings.Contains(err.Error(), "typed_receiver_owner_fault") || result.Acknowledged {
					t.Fatalf("descendant fault escaped atomic tree: %+v %v", result, err)
				}
				assertConstructorRows(t, f, backend, 0)
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
			canceled, cancel := context.WithCancel(f.ctx)
			cancel()
			result, err := agentFixtureFlowActivationCommitter{store: f.store}.CommitFlowInstanceActivation(canceled, plan)
			if !errors.Is(err, context.Canceled) || result.Acknowledged {
				t.Fatalf("canceled construction acquired tree: %+v %v", result, err)
			}
			assertConstructorRows(t, f, backend, 0)
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
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM workflow_instance_initial_materializations WHERE run_id=$1`, correlation.RunIDFromContext(f.ctx)).Scan(&receipts); err != nil || receipts != 2 {
				t.Fatalf("refused replay changed receipt count=%d err=%v", receipts, err)
			}
		})
	}
}
