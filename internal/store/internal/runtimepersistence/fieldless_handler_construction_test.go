package runtimepersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestSelectedRunTargetOwnersUseConstructedHeadersBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, fielded := range []bool{false, true} {
			shape := "fieldless"
			if fielded {
				shape = "fielded"
			}
			t.Run(backend+"/"+shape, func(t *testing.T) {
				files := map[string]string{
					"schema.yaml":        "name: target-header-proof\n",
					"review/schema.yaml": "name: review\nstages:\n  pending: {initial: true}\n",
				}
				if fielded {
					files["review/entities.yaml"] = "review_item:\n  marker: {type: text, initial: original}\n"
				}
				f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, files, nil)
				runID := correlation.RunIDFromContext(f.ctx)
				root := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
				root.Instance = flowidentity.Stored(root.ContractBundle, ".", runID, runID, runID, "")
				plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, root)
				if err != nil {
					t.Fatal(err)
				}
				if len(plan.Children) != 1 || plan.Children[0].Identity.TemplateID != "review" {
					t.Fatalf("exact eager receiver tree: %+v", plan.Children)
				}
				target := plan.Children[0].Identity
				committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan)
				if err != nil || !committed.Acknowledged || !committed.Created {
					t.Fatalf("construct target: result=%+v err=%v", committed, err)
				}
				selected := f.store.(interface {
					bus.SelectedRunTargetOwnerLister
					bus.ScopedSelectedRunTargetOwnerLister
				})
				lookups := map[string]func(context.Context) ([]bus.ActiveTargetDescriptor, error){
					"all": func(ctx context.Context) ([]bus.ActiveTargetDescriptor, error) {
						return selected.ListSelectedRunTargetOwners(ctx, runID)
					},
					"path": func(ctx context.Context) ([]bus.ActiveTargetDescriptor, error) {
						return selected.ListSelectedRunTargetOwnersForScope(ctx, runID, []string{"review"}, "")
					},
					"source": func(ctx context.Context) ([]bus.ActiveTargetDescriptor, error) {
						return selected.ListSelectedRunTargetOwnersForScope(ctx, runID, nil, target.EntityID)
					},
				}
				check := func(want int) {
					t.Helper()
					for name, lookup := range lookups {
						owners, err := lookup(f.ctx)
						wantCount := want
						if name == "all" {
							wantCount++
						}
						if err != nil || len(owners) != wantCount {
							t.Fatalf("%s owners=%+v err=%v, want %d", name, owners, err, wantCount)
						}
						seen := map[string]bool{}
						for _, owner := range owners {
							if owner.FlowInstance == target.InstancePath && owner.EntityID == target.EntityID && want == 1 {
								seen["review"] = true
							} else if name == "all" && owner.FlowInstance == runID && owner.EntityID == runID {
								seen["root"] = true
							} else {
								t.Fatalf("%s lost exact constructed identity: %+v", name, owner)
							}
						}
						if len(seen) != wantCount {
							t.Fatalf("%s repeated constructed identity: %+v", name, owners)
						}
					}
				}
				check(1)
				foreign, err := selected.ListSelectedRunTargetOwnersForScope(f.ctx, uuid.NewString(), []string{"review"}, target.EntityID)
				if err != nil || len(foreign) != 0 {
					t.Fatalf("foreign run borrowed target header: %+v err=%v", foreign, err)
				}
				// Corruption setup leaves field state intact. It is not construction.
				if _, err := f.db.ExecContext(f.ctx, `DELETE FROM flow_instances WHERE run_id=$1 AND instance_path='review'`, runID); err != nil {
					t.Fatal(err)
				}
				check(0)
				var rows int
				wantFields := 0
				if fielded {
					wantFields = 1
				}
				if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM entity_state WHERE run_id=$1`, runID).Scan(&rows); err != nil || rows != wantFields {
					t.Fatalf("lookup fabricated or repaired field state: rows=%d want=%d err=%v", rows, wantFields, err)
				}
			})
		}
	}
}

func TestOrdinaryHandlerRequiresCanonicalConstructionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, shape := range []struct {
			name, entity, effects, wantMarker string
			gate, emits                       bool
		}{
			{name: "fieldless_stage"},
			{name: "fielded_stage", entity: "review_item", wantMarker: "original"},
			{name: "fielded_accumulation", entity: "review_item", effects: "      data_accumulation:\n        writes:\n          - {target_field: marker, value: changed}\n", wantMarker: "changed"},
			{name: "fieldless_gate", effects: "      sets_gate: {name: approved}\n", gate: true},
			{name: "fieldless_emit", effects: "      emit: {event: work.recorded}\n", emits: true},
		} {
			for _, constructed := range []bool{false, true} {
				name := "absent"
				if constructed {
					name = "constructed"
				}
				t.Run(backend+"/"+shape.name+"/"+name, func(t *testing.T) {
					files := map[string]string{
						"schema.yaml":        "name: fieldless-handler\n",
						"review/schema.yaml": "name: review\nstages:\n  pending: {initial: true}\n  reviewing: {}\n",
						"review/events.yaml": "work.ready:\nwork.recorded:\n",
						"review/nodes.yaml":  "inspect:\n  execution_type: system_node\n  event_handlers:\n    work.ready:\n      advances_to: reviewing\n" + shape.effects,
					}
					if shape.entity != "" {
						files["review/entities.yaml"] = "review_item:\n  marker: {type: text, initial: original}\n"
					}
					f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, files, nil)
					runID := correlation.RunIDFromContext(f.ctx)
					root := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
					root.Instance = flowidentity.Stored(root.ContractBundle, ".", runID, runID, runID, "")
					plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, root)
					if err != nil || len(plan.Children) != 1 {
						t.Fatalf("prepare exact receiver tree: %+v err=%v", plan, err)
					}
					req := sqliteFlowActivationRequest(f.bundle, "review", "review", "", "review")
					req.Instance = plan.Children[0].Identity
					if constructed {
						if err := f.manager.ActivateFlowInstance(f.ctx, root); err != nil {
							t.Fatal(err)
						}
					}
					source := semanticview.Wrap(f.bundle)
					fact, found := correlation.SourceArtifactFactFromContext(f.ctx)
					if !found {
						t.Fatal("fixture lacks admitted source fact")
					}
					descriptors, err := runtimepkg.AuthorActivityEventDescriptors(source)
					if err != nil {
						t.Fatal(err)
					}
					runtimeID := uuid.NewString()
					deliveryAuthority, err := deliverylifecycle.NewNormalExecutionAuthority(fact, "delivery-fixture:"+correlation.RunIDFromContext(f.ctx), 1)
					if err != nil {
						t.Fatal(err)
					}
					emitterScope := authoractivity.BundleScope(runtimeID, fact.BundleHash())
					emitterLease, err := f.store.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(emitterScope, descriptors)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(emitterLease.Release)
					selected := f.store.(storeTestDurableEventBusStore)
					workflow := f.store.(workflowTestSelectedStore)
					work := storeTestWorkOwner(t)
					eventBus, err := newStoreTestEventBus(t, selected, bus.EventBusOptions{
						ContractBundle: source, SourceArtifactFact: fact, WorkOwner: work,
						RuntimeInstanceID: runtimeID,
						DeliveryAuthority: deliveryAuthority,
					})
					if err != nil {
						t.Fatal(err)
					}
					nodes, err := pipeline.LoadWorkflowNodes(source)
					if err != nil || len(nodes) != 1 {
						t.Fatalf("exact node census=%+v err=%v", nodes, err)
					}
					coordinator := pipeline.NewPipelineCoordinatorWithOptions(eventBus, pipeline.PipelineCoordinatorOptions{
						Module: forkFanOutConsumerModule{runForkGateWorkflowModule{source: source}, nodes}, Persistence: pipeline.NewWorkflowPersistence(workflow), DeliveryStore: workflow,
						DeadLetters: workflow, PipelineObligations: workflow.PipelineObligations(), DecisionCards: workflow, ProposedEffects: workflow, HumanTasks: workflow,
						DecisionCardDraftExpiry: workflow, HumanTaskExpiry: workflow, DeliveryRuntime: eventBus, RunLifecycle: workflow,
						SourceArtifactFact: fact, ExecutionPosture: executionposture.Live, ReceiverExecution: eventreceiver.NormalExecution(), WorkOwner: work,
					})
					if coordinator == nil {
						t.Fatal("real coordinator unavailable")
					}
					event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "review/work.ready", "constructor-proof", "", []byte(`{}`), 0,
						correlation.RunIDFromContext(f.ctx), events.EventEnvelope{}, eventtest.RootRoutingSource(req.Instance.EntityID), time.Now().UTC())
					route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(nodes[0].Node), Target: events.MustExistingEntityTarget(events.RouteIdentity{
						FlowID: "review", FlowInstance: req.Instance.InstancePath, EntityID: req.Instance.EntityID,
					})}
					if err := commitSemanticEventFixtureWithRoutes(f.ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
						t.Fatal(err)
					}
					proof, err := selected.ProveHandoff(f.ctx, event.ID(), route)
					if err != nil {
						t.Fatal(err)
					}
					if err := eventBus.AcceptCommittedDeliveryHandoffs([]deliverylifecycle.DurableHandoffProof{proof}); err != nil {
						t.Fatal(err)
					}
					delivery, err := events.NewDeliveryEvent(event, route)
					if err != nil {
						t.Fatal(err)
					}
					executionCtx := correlation.WithRuntimeInstanceID(authoractivity.WithScope(f.ctx, emitterScope), runtimeID)
					ctx, err := eventreceiver.NormalExecution().Bind(executionCtx, event.ExecutionMode())
					if err != nil {
						t.Fatal(err)
					}
					ctx = deliverylifecycle.WithRoute(events.WithDeliveryContext(ctx, route.Context), route)
					_, _, outcome, err := coordinator.InterceptDeliveryRoute(ctx, delivery, route)
					if !constructed {
						disposition, disposed := outcome.Disposition()
						if err != nil || !disposed || disposition.Kind() != pipelineobligation.DispositionDeadLetter || disposition.ReasonCode() != "handler_terminal_failure" || disposition.Failure() == nil || disposition.Failure().Detail.Code != "workflow_target_unconstructed" {
							t.Fatalf("absent handler target acquired construction: outcome=%+v err=%v", outcome, err)
						}
						for _, table := range []string{"flow_instances", "entity_state", "workflow_instance_initial_materializations", "flow_instance_runtime_readiness"} {
							var count int
							if readErr := f.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE run_id=$1", event.RunID()).Scan(&count); readErr != nil || count != 0 {
								t.Fatalf("refused handler repaired %s: count=%d err=%v", table, count, readErr)
							}
						}
						return
					}
					if err != nil || !outcome.ContinueDispatch() {
						disposition, _ := outcome.Disposition()
						t.Fatalf("constructed header did not execute: failure=%+v outcome=%+v err=%v", disposition.Failure(), outcome, err)
					}
					owner, err := flowidentity.NewRunScopedFlowInstance(event.RunID(), req.Instance.Route())
					if err != nil {
						t.Fatal(err)
					}
					instance, found, err := coordinator.Load(ctx, owner)
					wantFields := 0
					if shape.entity != "" {
						wantFields = 1
					}
					if err != nil || !found || !instance.StageDefined || instance.CurrentState != "reviewing" || instance.Revision != 2 || instance.EntityType != shape.entity || len(instance.Fields) != wantFields {
						t.Fatalf("constructed execution readback=%+v found=%t err=%v", instance, found, err)
					}
					if wantFields != 0 && instance.Fields["marker"] != shape.wantMarker {
						t.Fatalf("handler field readback=%+v, want marker=%q", instance.Fields, shape.wantMarker)
					}
					if shape.gate && !instance.Gates["review/approved"] {
						t.Fatalf("constructed handler lost gate: %+v", instance.Gates)
					}
					var emissions int
					if err := f.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='review/work.recorded'`, event.RunID()).Scan(&emissions); err != nil {
						t.Fatal(err)
					}
					wantEmissions := 0
					if shape.emits {
						wantEmissions = 1
					}
					if emissions != wantEmissions {
						t.Fatalf("handler emissions=%d, want %d", emissions, wantEmissions)
					}
					var fields int
					if err := f.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_state WHERE run_id=$1`, event.RunID()).Scan(&fields); err != nil || fields != wantFields {
						t.Fatalf("handler field rows=%d err=%v, want %d", fields, err, wantFields)
					}
				})
			}
		}
	}
}
