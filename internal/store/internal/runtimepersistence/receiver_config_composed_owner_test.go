package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

type receiverComposedFixture struct {
	receiverConfigActivationFixture
	raw         selectedFanOutLifecycleOwner
	postgres    bool
	connector   *stopCommitConnector
	seed        fanOutOwnerFixture
	pub         *bus.EventBus
	parent      events.Event
	claim       deliverylifecycle.Claim
	plans       []engine.DurablePublicationPlan
	activations []pipeline.FlowInstanceActivationPlan
}

func newReceiverComposedFixture(t *testing.T, backend string) *receiverComposedFixture {
	t.Helper()
	raw, db, connector := newP16RaceStore(t, backend)
	root := canonicalrouting.CopyReceiverConfigComposedOwner(t)
	ctx, seed, _, _ := seedDeclaredForkFanOutGenerationFromSource(t, backend, authorActivityReceiptFixture{store: raw.(authorActivityReceiptStore), db: db}, 3, time.Now().UTC().Truncate(time.Microsecond), false, false, root, nil, nil)
	ctx, cancel := context.WithTimeout(storeTestWorkContext(t, ctx), 30*time.Second)
	t.Cleanup(cancel)
	bundle, err := contracts.LoadWorkflowContractBundleFromArtifact(pipeline.WorkflowRepoRoot(), seed.artifact, contracts.DefaultPlatformSpecFile(pipeline.WorkflowRepoRoot()), contracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	actors := sqliteFlowActivationBundle(t)
	bundle.FlowTree.ByID["review"].Agents = actors.FlowTree.ByID["review"].Agents
	bundle.FlowTree.ByID["review"].AgentURIs = actors.FlowTree.ByID["review"].AgentURIs
	bundle.URIRegistry = actors.URIRegistry
	selected := raw.(agentFixtureFlowStore)
	routes := &sqliteFlowActivationBus{}
	workflow := configureAgentFixtureFlowLifecycle(t, selected, routes, bundle)
	fact := mustStoreTestSourceArtifactFact(seed.bundleHash)
	descriptors, err := runtimepkg.AuthorActivityEventDescriptors(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := raw.(selectedFanOutMixedRouteOwner).RegisterAuthorActivityEventCatalog(authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, seed.bundleHash), descriptors)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(catalog.Release)
	planner := ownStoreTestAgentManager(t, manager.NewAgentManagerWithOptions(routes, nil, manager.AgentManagerOptions{
		ExecutionPosture: executionposture.Live, BaseContext: ctx, SourceArtifactFact: fact, SemanticSource: semanticview.Wrap(bundle), WorkflowInstances: workflow,
		WorkOwner: storeTestWorkOwner(t), ReceiverExecution: eventreceiver.NormalExecution(),
	}))
	pub, err := newStoreTestEventBus(t, raw.(storeTestDurableEventBusStore), bus.EventBusOptions{ContractBundle: semanticview.Wrap(bundle), SourceArtifactFact: fact, TemplateInstancePlanner: planner})
	if err != nil {
		t.Fatal(err)
	}
	return &receiverComposedFixture{receiverConfigActivationFixture: receiverConfigActivationFixture{ctx: ctx, db: db, store: selected, manager: planner, workflows: workflow, bus: routes, bundle: bundle}, raw: raw, postgres: backend == "postgres", connector: connector, seed: seed, pub: pub}
}

func (f *receiverComposedFixture) prepareEngine(t *testing.T, keys []string, label string) pipeline.WorkflowEngineMutationCommand {
	t.Helper()
	node := mustPersistenceRootNode("fan-out-source")
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: f.seed.runID, EntityID: f.seed.runID})}
	f.parent = eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "request", "fixture", "", []byte(`{}`), 0, f.seed.runID, events.EventEnvelope{}, eventtest.RootRoutingSource(f.seed.runID), time.Now().UTC())
	selected := f.raw.(storeTestDurableEventBusStore)
	if err := commitSemanticEventFixtureWithRoutes(f.ctx, selected, f.parent, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	claimed, err := claimDeliveryFixture(f.ctx, selected, f.parent, route)
	if err != nil {
		t.Fatal(err)
	}
	f.claim = claimed.Claim
	f.ctx = deliverylifecycle.WithRoute(f.ctx, route)
	before := snapshotForkHistoricalExecutionTables(t, f.db, f.postgres)
	f.plans = f.prepareChildren(t, keys, label)
	if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, f.postgres)) {
		t.Fatal("typed publication preparation mutated durable state")
	}
	state := stateOnlyWorkflowEngineMutationRecord(t, f.seed.runID, ".", f.seed.runID, f.seed.runID, "review", 2, f.seed.createdAt)
	state.EntityType, state.Mode = "root", "static"
	state.Transition = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
	return pipeline.WorkflowEngineMutationCommand{State: state, Publications: f.plans, DeliverySuccess: &pipeline.WorkflowEngineDeliverySuccess{Claim: f.claim, SideEffects: []string{"handler_completed"}, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleSelection()}}
}

func (f *receiverComposedFixture) prepareChildren(t *testing.T, keys []string, label string) []engine.DurablePublicationPlan {
	t.Helper()
	var intents []engine.EmitIntent
	for _, key := range keys {
		payload := []byte(fmt.Sprintf(`{"request_id":%q,"label":%q,"nested":[7,7.0]}`, key, label+key))
		event := eventtest.ChildForProducerWithRoutingSource(uuid.NewString(), "items.child", eventtest.Producer(events.EventProducerNode, mustPersistenceRootNode("fan-out-source").Key()), "", payload, 1, events.LineageFromEvent(f.parent), events.EventEnvelope{}, eventtest.RootRoutingSource(f.seed.runID), time.Now().UTC())
		intents = append(intents, engine.EmitIntent{Event: event})
	}
	plans, err := f.pub.PrepareEnginePublications(f.ctx, intents)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.pub.ReleaseEnginePublications(context.WithoutCancel(f.ctx), plans); err != nil {
			t.Error(err)
		}
	})
	f.activations = nil
	for _, value := range plans {
		command := value.(bus.EnginePublicationPlan).PublicationCommand()
		if len(command.Activations) != 1 {
			t.Fatalf("publication bypassed typed initialization: %d", len(command.Activations))
		}
		plan := command.Activations[0]
		if plan.Instance.Config["enabled"] != true || len(plan.Readiness.Agents) != 1 {
			t.Fatal("typed defaults or agent readiness missing")
		}
		f.activations = append(f.activations, plan)
	}
	return plans
}

func (f *receiverComposedFixture) assertTypedChildren(t *testing.T, count int) {
	t.Helper()
	for table, column := range map[string]string{"flow_instances": "instance_path", "entity_state": "flow_instance", "workflow_instance_initial_materializations": "instance_path", "flow_instance_runtime_readiness": "instance_path"} {
		var actual int
		if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM `+table+` WHERE run_id=$1 AND `+column+`<>$2`, f.seed.runID, f.seed.runID).Scan(&actual); err != nil || actual != count {
			t.Fatalf("%s children=%d want=%d err=%v", table, actual, count, err)
		}
	}
	for _, activation := range f.activations {
		f.requireConfig(t, activation)
	}
	agents, err := f.store.LoadAgents(f.ctx)
	if err != nil || len(agents) != 0 || len(f.bus.routePaths()) != 0 {
		t.Fatalf("commit exposed unready agents/routes: agents=%d err=%v", len(agents), err)
	}
}

func TestReceiverConfigEngineAtomicFaultMatrixBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"healthy", "state", "activation", "publication", "settlement", "story", "revision", "stale_claim", "entry_cancel", "lost_after_commit", "lost_after_rollback", "handoff_failure"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				f := newReceiverComposedFixture(t, backend)
				command := f.prepareEngine(t, []string{"one", "two", "three"}, "committed-")
				if cut == "stale_claim" {
					if _, err := f.raw.(deliverylifecycle.Store).SettleSuccess(f.ctx, f.claim, []string{"already_completed"}, time.Millisecond, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
						t.Fatal(err)
					}
				}
				before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
				remove := func() {}
				fault := map[string][2]string{"state": {"entity_state", "UPDATE"}, "activation": {"flow_instance_runtime_readiness", "INSERT"}, "publication": {"events", "INSERT"}, "settlement": {"event_deliveries", "UPDATE"}, "story": {"author_activity_order", "UPDATE"}, "revision": {"run_fork_revision_heads", "UPDATE"}}
				if target, ok := fault[cut]; ok {
					condition := ""
					if cut == "publication" {
						condition = fmt.Sprintf("NEW.event_id='%s'", f.plans[1].(bus.EnginePublicationPlan).PublicationCommand().Commit.Event.Event().ID())
					}
					if cut == "activation" {
						condition = fmt.Sprintf("NEW.instance_path='%s'", f.activations[1].Identity.Route().InstancePath)
					}
					remove = installReceiverComposedFault(t, f.db, backend, target[0], target[1], condition)
				}
				injected := errors.New("typed receiver commit acknowledgement lost")
				commitCalls := 0
				if strings.HasPrefix(cut, "lost_after_") {
					f.connector.arm(func(tx driver.Tx) error {
						commitCalls++
						if cut == "lost_after_commit" {
							return errors.Join(injected, tx.Commit())
						}
						return errors.Join(injected, tx.Rollback())
					})
				}
				submits := 0
				if cut == "handoff_failure" {
					registration, err := f.raw.(runlifecycle.CandidateRegistrar).RegisterCompletionCandidateSink(f.ctx, runlifecycle.CandidateScope{BundleHash: f.seed.bundleHash}, &completionHandoffEvidenceProbeSink{submit: func(runlifecycle.Candidate) error { submits++; return injected }})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(registration.Release)
				}
				ctx, cancel := context.WithCancel(f.ctx)
				defer cancel()
				if cut == "entry_cancel" {
					cancel()
				}
				owner := f.raw.(pipeline.WorkflowEngineMutationOwner)
				result, err := owner.CommitWorkflowEngineMutation(ctx, command)
				remove()
				committed := cut == "healthy" || cut == "lost_after_commit" || cut == "handoff_failure"
				if cut == "healthy" && err != nil || cut != "healthy" && err == nil {
					t.Fatalf("cut %s error=%v", cut, err)
				}
				if _, injectedFault := fault[cut]; injectedFault && !strings.Contains(err.Error(), "typed_receiver_owner_fault") {
					t.Fatalf("did not reach intended %s fault: %v", cut, err)
				}
				if strings.HasPrefix(cut, "lost_after_") && (!errors.Is(err, injected) || commitCalls != 1) {
					t.Fatalf("native commit cut calls=%d err=%v", commitCalls, err)
				}
				if cut == "lost_after_commit" && !reflect.DeepEqual(result, pipeline.CommittedWorkflowEngineMutation{}) {
					t.Fatal("ambiguous acknowledgement returned confirmed commit evidence")
				}
				if cut == "handoff_failure" && (submits != 1 || result.DeliverySuccess == nil || !errors.Is(err, injected)) {
					t.Fatalf("lost committed owner receipt: %+v submits=%d err=%v", result, submits, err)
				}
				if !committed {
					if !reflect.DeepEqual(result, pipeline.CommittedWorkflowEngineMutation{}) {
						t.Fatal("rollback returned committed owner evidence")
					}
					if after := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres"); !reflect.DeepEqual(before, after) {
						for table := range before {
							if !reflect.DeepEqual(before[table], after[table]) {
								t.Errorf("rollback changed %s", table)
							}
						}
						t.FailNow()
					}
					if cut == "stale_claim" {
						f.activations = nil
						f.assertTypedChildren(t, 0)
						return
					}
					if _, err := owner.CommitWorkflowEngineMutation(f.ctx, command); err != nil {
						t.Fatalf("clean retry: %v", err)
					}
				}
				f.assertTypedChildren(t, 3)
				snapshot, err := f.raw.(deliverylifecycle.Store).Snapshot(f.ctx, f.claim.DeliveryID())
				if err != nil || snapshot.Status != deliverylifecycle.StatusDelivered {
					t.Fatalf("state/children committed without exact settlement: %+v %v", snapshot, err)
				}
				stable := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
				if _, err := owner.CommitWorkflowEngineMutation(f.ctx, command); err == nil {
					t.Fatal("stale settled command replayed")
				}
				if !reflect.DeepEqual(stable, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
					t.Fatal("retry after committed/uncertain result mutated durable state")
				}
			})
		}
	}
}

func installReceiverComposedFault(t *testing.T, db *sql.DB, backend, table, operation, condition string) func() {
	t.Helper()
	name := "receiver_composed_fault"
	if table == "author_activity_order" {
		condition = "NEW.last_sequence<>OLD.last_sequence"
	}
	when := ""
	if condition != "" {
		when = " WHEN (" + condition + ")"
	}
	statement := fmt.Sprintf(`CREATE TRIGGER %s AFTER %s ON %s%s BEGIN SELECT RAISE(ABORT,'typed_receiver_owner_fault'); END`, name, operation, table, when)
	if backend == "postgres" {
		if _, err := db.Exec(`CREATE FUNCTION receiver_composed_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'typed_receiver_owner_fault'; END $$`); err != nil {
			t.Fatal(err)
		}
		statement = fmt.Sprintf(`CREATE TRIGGER %s AFTER %s ON %s FOR EACH ROW%s EXECUTE FUNCTION %s()`, name, operation, table, when, name)
	}
	if _, err := db.Exec(statement); err != nil {
		t.Fatal(err)
	}
	return func() {
		statement := "DROP TRIGGER " + name
		if backend == "postgres" {
			statement += " ON " + table
		}
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
		if backend == "postgres" {
			if _, err := db.Exec(`DROP FUNCTION receiver_composed_fault()`); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestReceiverConfigDeclaredFanOutAtomicBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverComposedFixture(t, backend)
			owner, _, _, _ := grantedFanOutOwnerForTest(t, f.ctx, f.raw, f.seed)
			key := fanoutobligation.IntentKey{RunID: f.seed.runID, TriggeringDeliveryID: f.seed.deliveryID, ElementRef: contracts.FanOutElementRef{FlowPath: f.seed.flowPath, Family: "fan_out", SemanticPath: f.seed.semanticPath}}
			intent, claim, found, err := owner.ClaimFanOutIntent(f.ctx, pipeline.FanOutClaimRequest{Owner: "typed-receiver-config", BundleHash: f.seed.bundleHash, Candidate: &key, Now: time.Now().UTC(), Lease: time.Minute})
			if err != nil || !found {
				t.Fatalf("claim declared fanout: found=%v err=%v", found, err)
			}
			input, err := owner.LoadFanOutEvaluation(f.ctx, claim)
			if err != nil {
				t.Fatal(err)
			}
			group, err := owner.BeginFanOutPublicationGroup(f.ctx, claim)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := group.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			receiver := intent.Request.Capsule.Receiver
			if receiver == nil {
				t.Fatal("declared fanout lost receiver capsule")
			}
			f.ctx = deliverylifecycle.WithRoute(f.ctx, events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(receiver.Node), Target: receiver.Target})
			var requests []pipeline.FanOutPublicationRequest
			for ordinal := 0; ordinal < 3; ordinal++ {
				projection, err := fanoutobligation.PrepareOrdinalEmission(intent, input.Trigger, ordinal)
				if err != nil {
					t.Fatal(err)
				}
				payload := []byte(fmt.Sprintf(`{"request_id":"item-%03d","label":"label-item-%03d","nested":[7,7.0]}`, ordinal, ordinal))
				event, err := projection.NewEvent(events.EventFacts{ID: uuid.NewString(), Type: "items.child", Producer: events.ProducerClaim{Type: events.EventProducerNode, ID: receiver.Node.Key()}, Payload: payload, ChainDepth: intent.Request.Capsule.ChainDepth + 1, RoutingSource: eventtest.RootRoutingSource(f.seed.runID), CreatedAt: time.Now().UTC()})
				if err != nil {
					t.Fatal(err)
				}
				requests = append(requests, pipeline.FanOutPublicationRequest{Ordinal: ordinal, Intent: engine.EmitIntent{Event: event}})
			}
			prepared, err := f.pub.PrepareFanOutPublications(f.ctx, group, requests)
			if err != nil || len(prepared) != 3 {
				t.Fatalf("prepare declared fanout: %d %v", len(prepared), err)
			}
			command := pipeline.FanOutChunkCommand{Claim: claim, Now: time.Now().UTC()}
			for ordinal, p := range prepared {
				if p.Err != nil || p.Publication == nil || p.Ordinal != ordinal {
					t.Fatalf("ordinal %d: %+v", ordinal, p)
				}
				activations := p.Publication.(bus.EnginePublicationPlan).PublicationCommand().Activations
				if len(activations) != 1 {
					t.Fatalf("ordinal %d typed activations=%d", ordinal, len(activations))
				}
				wire, err := canonicaljson.MarshalPreservingNumberKinds(activations[0].Instance.Config)
				if err != nil {
					t.Fatal(err)
				}
				want := fmt.Sprintf(`{"enabled":true,"label":"label-item-%03d","nested":[7,7.0],"request_id":"item-%03d"}`, ordinal, ordinal)
				if string(wire) != want || len(activations[0].Readiness.Agents) != 1 {
					t.Fatalf("ordinal %d exact initialization=%s want=%s agents=%d", ordinal, wire, want, len(activations[0].Readiness.Agents))
				}
				f.activations = append(f.activations, activations[0])
				f.plans = append(f.plans, p.Publication)
				command.Outcomes = append(command.Outcomes, pipeline.FanOutChunkOutcome{Ordinal: ordinal, Publication: p.Publication})
			}
			if err := f.pub.SealFanOutPublications(f.ctx, group, 3, f.plans); err != nil {
				t.Fatal(err)
			}
			before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
			second := f.plans[1].(bus.EnginePublicationPlan).PublicationCommand().Commit.Event.Event().ID()
			remove := installReceiverComposedFault(t, f.db, backend, "events", "INSERT", fmt.Sprintf("NEW.event_id='%s'", second))
			result, err := owner.CommitFanOutChunk(f.ctx, command)
			remove()
			if err == nil || !strings.Contains(err.Error(), "typed_receiver_owner_fault") || len(result.Publications) != 0 {
				t.Fatalf("fanout partial failure: %+v %v", result, err)
			}
			if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
				t.Fatal("fanout prefix rollback left durable residue")
			}
			result, err = owner.CommitFanOutChunk(f.ctx, command)
			if err != nil || len(result.Publications) != 3 {
				t.Fatalf("fanout clean retry: %+v %v", result, err)
			}
			f.assertTypedChildren(t, 3)
			var cursor, outcomes int
			if err := f.db.QueryRowContext(f.ctx, `SELECT cursor,(SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1) FROM fan_out_intents WHERE run_id=$1`, f.seed.runID).Scan(&cursor, &outcomes); err != nil || cursor != 3 || outcomes != 3 {
				t.Fatalf("fanout cursor/outcomes=%d/%d err=%v", cursor, outcomes, err)
			}
		})
	}
}

func TestReceiverConfigPublicationContendersBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cancelContender := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cancel_%t", backend, cancelContender), func(t *testing.T) {
				barrier := newForkContentionBarrier(t, backend, false)
				t.Cleanup(barrier.resume)
				f := newReceiverComposedFixture(t, backend)
				first := f.prepareEngine(t, []string{"same"}, "winner-").Publications[0].(bus.EnginePublicationPlan).PublicationCommand()
				winner := f.activations[0]
				losingPlans := f.prepareChildren(t, []string{"same"}, "loser-")
				second := losingPlans[0].(bus.EnginePublicationPlan).PublicationCommand()
				if !reflect.DeepEqual(winner.Identity, f.activations[0].Identity) || reflect.DeepEqual(winner.Instance.Config, f.activations[0].Instance.Config) {
					t.Fatal("contenders must share exact identity and differ in nonkey config")
				}
				owner := f.raw.(interface {
					CommitPublication(context.Context, bus.PublicationCommand) (bus.CommittedPublication, error)
				})
				firstCall := func() error { _, err := owner.CommitPublication(f.ctx, first); return err }
				secondCtx, cancel := context.WithCancel(f.ctx)
				defer cancel()
				secondOwner := owner
				if backend == "sqlite" {
					var sequence int
					var name, path string
					if err := f.db.QueryRow(`PRAGMA database_list`).Scan(&sequence, &name, &path); err != nil {
						t.Fatal(err)
					}
					observed := barrier.observeSQLiteStore(t, f.raw.(*SQLiteRuntimeStore), path)
					descriptors, err := runtimepkg.AuthorActivityEventDescriptors(semanticview.Wrap(f.bundle))
					if err != nil {
						t.Fatal(err)
					}
					catalog, err := observed.RegisterAuthorActivityEventCatalog(authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, f.seed.bundleHash), descriptors)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(catalog.Release)
					secondOwner = observed
					secondCtx = context.WithValue(secondCtx, forkContentionContextKey{}, barrier)
				}
				secondCall := func() error { _, err := secondOwner.CommitPublication(secondCtx, second); return err }
				cancelWaiting := func() {}
				if cancelContender {
					cancelWaiting = cancel
				}
				firstErr, secondErr := contendReceiverPublicationAtCommit(t, f.ctx, backend, f.db, f.connector, barrier, cancelWaiting, firstCall, secondCall)
				if firstErr != nil || secondErr == nil {
					t.Fatalf("winner=%v loser=%v", firstErr, secondErr)
				}
				if cancelContender && !errors.Is(secondErr, context.Canceled) {
					t.Fatalf("contender cancellation lost: %v", secondErr)
				}
				if !cancelContender {
					failure, ok := failures.As(secondErr)
					if !ok || failure.Failure.Class != failures.ClassConflictingDuplicate {
						t.Fatalf("loser did not reach exact immutable config conflict: %v", secondErr)
					}
				}
				f.activations = []pipeline.FlowInstanceActivationPlan{winner}
				f.assertTypedChildren(t, 1)
				var losingEvents int
				if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM events WHERE event_id=$1`, second.Commit.Event.Event().ID()).Scan(&losingEvents); err != nil || losingEvents != 0 {
					t.Fatalf("losing event escaped rollback: %d %v", losingEvents, err)
				}
				if err := f.pub.ReleaseEnginePublications(f.ctx, losingPlans); err != nil {
					t.Fatal(err)
				}
				retry, err := f.pub.PrepareEnginePublications(f.ctx, []engine.EmitIntent{{Event: second.Commit.Event.Event()}})
				if err != nil || len(retry) != 1 {
					t.Fatalf("reprepare loser: %d %v", len(retry), err)
				}
				t.Cleanup(func() {
					if err := f.pub.ReleaseEnginePublications(context.WithoutCancel(f.ctx), retry); err != nil {
						t.Error(err)
					}
				})
				reused := retry[0].(bus.EnginePublicationPlan).PublicationCommand()
				if len(reused.Activations) != 0 {
					t.Fatal("retry elected another initializer")
				}
				if _, err := owner.CommitPublication(f.ctx, reused); err != nil {
					t.Fatalf("retry winner reuse: %v", err)
				}
				f.assertTypedChildren(t, 1)
			})
		}
	}
}

// Hold the winner after SQL/finalizers until the second owner reaches an actual
// native lock: SQLite BUSY on an independent pool, or a PostgreSQL lock waiter.
func contendReceiverPublicationAtCommit(t *testing.T, ctx context.Context, backend string, db *sql.DB, connector *stopCommitConnector, barrier *forkContentionBarrier, cancel context.CancelFunc, first, second func() error) (error, error) {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); barrier.resume() })
	connector.arm(func(tx driver.Tx) error {
		close(entered)
		select {
		case <-release:
			return tx.Commit()
		case <-ctx.Done():
			return errors.Join(ctx.Err(), tx.Rollback())
		}
	})
	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	go func() { firstDone <- first() }()
	select {
	case <-entered:
	case err := <-firstDone:
		t.Fatalf("winner failed before commit: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	go func() { secondDone <- second() }()
	if backend == "sqlite" {
		select {
		case <-barrier.busy:
		case err := <-secondDone:
			t.Fatalf("contender returned without native BUSY: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	} else {
		forkContentionPoll(t, ctx, func() bool {
			var waiting bool
			if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND cardinality(pg_blocking_pids(pid))>0)`).Scan(&waiting); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-secondDone:
				t.Fatalf("contender returned before native lock wait: %v", err)
			default:
			}
			return waiting
		})
	}
	cancel()
	// PostgreSQL drains its admitted closed SQL unit before observing caller
	// cancellation. Release the winner's lock; do not require early interruption.
	unblock()
	barrier.resume()
	var secondErr error
	select {
	case secondErr = <-secondDone:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	return <-firstDone, secondErr
}
