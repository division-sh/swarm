package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestReceiverFirstMaterializationRawTriggerHistoryBoundaryBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			root := canonicalrouting.CopyForkReceiverBusinessMutationOwnership(t, false)
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := contracts.LoadWorkflowContractBundleWithOptions(repo, root, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
			if err != nil {
				t.Fatal(err)
			}
			runID := uuid.NewString()
			ctx := correlation.WithRunID(seedSelectedActivitySourceRun(t, fixture, runID, semanticview.Wrap(bundle)), runID)
			trigger := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "start.seeded", "operator", "", []byte(`{"token":"first"}`), 0, runID, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), time.Now().UTC().Truncate(time.Microsecond))
			if err := insertCanonicalEventRecordFixture(ctx, fixture.store, trigger); err != nil {
				t.Fatal(err)
			}
			// Corrupt only this fixture's revision fact to prove the loader rejects
			// an otherwise canonical event with missing historical evidence.
			result, err := fixture.db.ExecContext(ctx, `DELETE FROM run_fork_fact_revisions WHERE run_id=$1 AND family='events' AND fact_key=$2`, runID, trigger.ID())
			if err != nil {
				t.Fatal(err)
			}
			if rows, err := result.RowsAffected(); err != nil || rows != 1 {
				t.Fatalf("remove trigger revision fact: rows=%d err=%v", rows, err)
			}
			tx, err := fixture.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			var current, history int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_id=$2`, runID, trigger.ID()).Scan(&current); err != nil {
				t.Fatal(err)
			}
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1 AND family='events' AND fact_key=$2`, runID, trigger.ID()).Scan(&history); err != nil {
				t.Fatal(err)
			}
			if current != 1 || history != 0 {
				t.Fatalf("unrevisioned trigger event=%s current=%d history=%d, want1/0", trigger.ID(), current, history)
			}
			err = validateRunForkRevisionMatrix(ctx, tx, backend.name == "postgres", runID)
			if err == nil || !strings.Contains(err.Error(), "unsupported unrevisioned events facts") {
				t.Fatalf("raw trigger full-validator refusal without receiver loader: %v", err)
			}
			t.Logf("raw trigger event=%s current=%d history=%d; direct full-validator refusal=%v; receiver loader/planner not invoked", trigger.ID(), current, history, err)
		})
	}
}

// Construction and publication use the real named commit. Ordinary observers
// never supply construction or settle unrelated agent obligations.
func TestReceiverConstructionAndObserverIsolationBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, consumer := range []struct {
				name  string
				kind  canonicalrouting.TemplateInstanceConsumer
				count int
			}{{"node_only", canonicalrouting.TemplateInstanceNodeConsumer, 1},
				{"node_agent", canonicalrouting.TemplateInstanceNodeAndAgentConsumer, 2},
				{"node_two_agents", canonicalrouting.TemplateInstanceNodeAndTwoAgentConsumer, 3}} {
				for _, settlement := range []string{"failure", "success", "retry_cancel", "terminal_race", "rollback_retry"} {
					t.Run(consumer.name+"/"+settlement, func(t *testing.T) {
						root := canonicalrouting.CopyTemplateInstanceRoute(t, canonicalrouting.TemplateInstanceRouteOptions{Mode: canonicalrouting.TemplateInstanceRouteSelectOrCreate, Consumer: consumer.kind})
						repo := canonicalrouting.RepoRoot(t)
						bundle, err := contracts.LoadWorkflowContractBundleWithOptions(repo, root, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
						if err != nil {
							t.Fatal(err)
						}
						source := semanticview.Wrap(bundle)
						runID := uuid.NewString()
						ctx := effects.WithExecutionMode(correlation.WithRunID(seedSelectedActivitySourceRun(t, fixture, runID, source), runID), executionmode.Live)
						descriptors, err := runtimepkg.AuthorActivityEventDescriptors(source)
						if err != nil {
							t.Fatal(err)
						}
						scope, ok := authoractivity.ScopeFromContext(ctx)
						if !ok {
							t.Fatal("missing author scope")
						}
						lease, err := fixture.store.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(scope, descriptors)
						if err != nil {
							t.Fatal(err)
						}
						defer lease.Release()
						fact, ok := correlation.SourceArtifactFactFromContext(ctx)
						if !ok {
							t.Fatal("missing source fact")
						}
						workflow := configureAgentFixtureFlowLifecycle(t, fixture.store.(agentFixtureFlowStore), &sqliteFlowActivationBus{}, bundle)
						planner := ownStoreTestAgentManager(t, manager.NewAgentManagerWithOptions(nil, nil, manager.AgentManagerOptions{
							ExecutionPosture: executionposture.Live, BaseContext: ctx, SourceArtifactFact: fact,
							SemanticSource: source, WorkflowInstances: workflow, WorkOwner: storeTestWorkOwner(t), ReceiverExecution: eventreceiver.NormalExecution(),
						}))
						eventBus, err := newStoreTestEventBus(t, fixture.store.(storeTestDurableEventBusStore), bus.EventBusOptions{ContractBundle: source, SourceArtifactFact: fact, TemplateInstancePlanner: planner})
						if err != nil {
							t.Fatal(err)
						}
						src := eventtest.StaticFlowRoutingSource("producer", "producer", uuid.NewString())
						event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "producer/deploy.done", "operator", "", []byte(`{"vertical_id":"first"}`), 0, runID, events.EventEnvelope{}, src, time.Now().UTC().Truncate(time.Microsecond))
						before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
						plans, err := eventBus.PrepareEnginePublications(ctx, []engine.EmitIntent{{Event: event}})
						if err != nil || len(plans) != 1 {
							t.Fatalf("prepare constructor publication: %d %v", len(plans), err)
						}
						defer eventBus.ReleaseEnginePublications(ctx, plans)
						if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
							t.Fatal("preparation mutated persistence")
						}
						command := plans[0].(bus.EnginePublicationPlan).PublicationCommand()
						if len(command.Activations) != 1 || len(command.Commit.DeliveryRoutes) != consumer.count {
							t.Fatalf("constructor/obligation accounting: %+v", command)
						}
						store := fixture.store.(interface {
							CommitPublication(context.Context, bus.PublicationCommand) (bus.CommittedPublication, error)
							LoadPreparedPublishEvent(context.Context, string) (bus.PreparedPublishEvent, bool, error)
							deliverylifecycle.Store
						})
						if settlement == "rollback_retry" {
							remove := installReceiverComposedFault(t, fixture.db, backend.name, "event_deliveries", "INSERT", fmt.Sprintf("NEW.event_id='%s'", event.ID()))
							if result, err := store.CommitPublication(ctx, command); err == nil || result.Acknowledged {
								t.Fatalf("later obligation fault did not roll back constructor: %+v %v", result, err)
							}
							if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
								t.Fatal("publication rollback left constructor/history/obligations")
							}
							remove()
						}
						committed, err := store.CommitPublication(ctx, command)
						if err != nil || !committed.Acknowledged {
							t.Fatalf("commit constructor publication: %+v %v", committed, err)
						}
						target := command.Activations[0].Identity
						var originalProjection []byte
						if err := fixture.db.QueryRowContext(ctx, `SELECT projection FROM workflow_instance_initial_materializations WHERE run_id=$1 AND instance_path=$2`, runID, target.InstancePath).Scan(&originalProjection); err != nil {
							t.Fatal(err)
						}
						assertConstruction := func() {
							t.Helper()
							var projection []byte
							if err := fixture.db.QueryRowContext(ctx, `SELECT projection FROM workflow_instance_initial_materializations WHERE run_id=$1 AND instance_path=$2`, runID, target.InstancePath).Scan(&projection); err != nil {
								t.Fatal(err)
							}
							if !reflect.DeepEqual(projection, originalProjection) {
								t.Fatal("ordinary delivery repeated or changed immutable construction")
							}
							var headers, fields int
							if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1 AND instance_path=$2 AND entity_id=$3`, runID, target.InstancePath, target.EntityID).Scan(&headers); err != nil {
								t.Fatal(err)
							}
							if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_state WHERE run_id=$1 AND flow_instance=$2 AND entity_id=$3`, runID, target.InstancePath, target.EntityID).Scan(&fields); err != nil {
								t.Fatal(err)
							}
							if headers != 1 || fields != 1 {
								t.Fatalf("constructed aggregate lost identity: %d/%d", headers, fields)
							}
						}
						assertConstruction()
						duplicate := command
						duplicate.Commit.DeliveryRoutes = append([]events.DeliveryRoute(nil), command.Commit.DeliveryRoutes...)
						for i, j := 0, len(duplicate.Commit.DeliveryRoutes)-1; i < j; i, j = i+1, j-1 {
							duplicate.Commit.DeliveryRoutes[i], duplicate.Commit.DeliveryRoutes[j] = duplicate.Commit.DeliveryRoutes[j], duplicate.Commit.DeliveryRoutes[i]
						}
						beforeDuplicate := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
						if _, err := store.CommitPublication(ctx, duplicate); err != nil {
							t.Fatal(err)
						}
						if !reflect.DeepEqual(beforeDuplicate, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
							t.Fatal("reordered duplicate changed construction/obligations/history")
						}
						var node deliverylifecycle.Snapshot
						var agents []deliverylifecycle.Snapshot
						for _, route := range command.Commit.DeliveryRoutes {
							if !route.Initialization.FlowLifecycle() || route.Target.Route().EntityID != target.EntityID {
								t.Fatalf("delivery lacks canonical construction receipt: %+v", route)
							}
							snapshot, err := store.Snapshot(ctx, mustReceiverDeliveryID(t, event.ID(), route))
							if err != nil || !reflect.DeepEqual(snapshot.Route, route.Normalized()) {
								t.Fatalf("exact durable receipt: %+v %v", snapshot, err)
							}
							if route.Recipient.IsNode() {
								node = snapshot
							} else {
								agents = append(agents, snapshot)
								requireReceiverConstructionCorruptionRefused(t, ctx, fixture, backend.name, command, route, snapshot.Authority)
								beforeClaim := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
								claim, err := store.ClaimDelivery(ctx, snapshot.Authority, command.Commit.Event.Event(), route)
								if err != nil || claim.Disposition != deliverylifecycle.ClaimDeferred {
									t.Fatalf("unattached agent claim: %+v %v", claim, err)
								}
								if !reflect.DeepEqual(beforeClaim, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
									t.Fatal("readiness deferral appended an attempt")
								}
							}
						}
						claim, err := store.ClaimDelivery(ctx, node.Authority, command.Commit.Event.Event(), node.Route)
						acquired, ok := claim.Acquired()
						if err != nil || !ok {
							t.Fatalf("ordinary node claim: %+v %v", claim, err)
						}
						failure, ok := failures.EnvelopeFromError(failures.New(failures.ClassLifecycleConflict, "test_observer_failed", "receiver_test", "observe", nil))
						if !ok {
							t.Fatal("missing failure envelope")
						}
						settle := func() error {
							if settlement == "success" {
								_, err := store.SettleSuccess(ctx, acquired.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection())
								return err
							}
							disposition := deliverylifecycle.FailureDeadLetter
							if settlement == "retry_cancel" {
								disposition = deliverylifecycle.FailureRetry
							}
							_, err := store.SettleFailure(ctx, acquired.Claim, deliverylifecycle.Settlement{Disposition: disposition, ReasonCode: "test_observer_failed", Failure: &failure, RetryBase: time.Hour, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation()})
							return err
						}
						if settlement == "terminal_race" {
							var workers sync.WaitGroup
							start := make(chan struct{})
							results := make(chan error, len(agents)*8+1)
							for _, agent := range agents {
								for range 8 {
									workers.Add(1)
									go func(agent deliverylifecycle.Snapshot) {
										defer workers.Done()
										<-start
										result, err := store.ClaimDelivery(ctx, agent.Authority, command.Commit.Event.Event(), agent.Route)
										if err == nil && result.Disposition != deliverylifecycle.ClaimDeferred {
											err = fmt.Errorf("node failure altered unattached agent admission: %+v", result)
										}
										results <- err
									}(agent)
								}
							}
							workers.Add(1)
							go func() { defer workers.Done(); <-start; results <- settle() }()
							close(start)
							workers.Wait()
							close(results)
							for err := range results {
								if err != nil {
									t.Fatal(err)
								}
							}
						} else if err := settle(); err != nil {
							t.Fatal(err)
						}
						assertConstruction()
						for _, before := range agents {
							after, err := store.Snapshot(ctx, before.DeliveryID)
							if err != nil || !reflect.DeepEqual(before, after) {
								t.Fatalf("ordinary node settlement changed unrelated agent: %+v %v", after, err)
							}
							outcomes, err := store.Outcomes(ctx, before.DeliveryID)
							if err != nil || len(outcomes) != 0 {
								t.Fatalf("unexecuted agent gained history: %+v %v", outcomes, err)
							}
						}
						if settlement == "retry_cancel" {
							if _, err := store.TerminalizeRun(ctx, runID, "receiver_test_cancel"); err != nil {
								t.Fatal(err)
							}
							for _, agent := range agents {
								after, err := store.Snapshot(ctx, agent.DeliveryID)
								if err != nil || after.Status != deliverylifecycle.StatusDeadLetter || after.ReasonCode != "receiver_test_cancel" || !after.StartedAt.IsZero() {
									t.Fatalf("run cancellation did not own terminal disposition: %+v %v", after, err)
								}
							}
						}
					})
				}
			}
		})
	}
}

func mustReceiverDeliveryID(t *testing.T, eventID string, route events.DeliveryRoute) string {
	t.Helper()
	id, err := deliverylifecycle.DeliveryID(eventID, route)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func requireReceiverConstructionCorruptionRefused(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, backend string, command bus.PublicationCommand, route events.DeliveryRoute, authority deliverylifecycle.ExecutionAuthority) {
	t.Helper()
	store := fixture.store.(interface {
		CommitPublication(context.Context, bus.PublicationCommand) (bus.CommittedPublication, error)
		LoadPreparedPublishEvent(context.Context, string) (bus.PreparedPublishEvent, bool, error)
		deliverylifecycle.Store
	})
	event := command.Commit.Event.Event()
	id := mustReceiverDeliveryID(t, event.ID(), route)
	var original []byte
	if err := fixture.db.QueryRowContext(ctx, `SELECT receiver_materialization_plan FROM event_deliveries WHERE delivery_id=$1`, id).Scan(&original); err != nil {
		t.Fatal(err)
	}
	for _, variant := range []string{"erased", "missing_initialization", "wrong_run", "wrong_event", "wrong_target", "old_kind", "old_dependency", "old_node", "partial"} {
		t.Run("durable_corruption_"+variant, func(t *testing.T) {
			var record, receipt map[string]json.RawMessage
			if err := json.Unmarshal(original, &record); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(record["initialization"], &receipt); err != nil {
				t.Fatal(err)
			}
			switch variant {
			case "wrong_run":
				receipt["run_id"], _ = json.Marshal(uuid.NewString())
			case "wrong_event":
				receipt["event_id"], _ = json.Marshal(uuid.NewString())
			case "wrong_target":
				receipt["target"] = json.RawMessage(`{"kind":"absent"}`)
			case "old_kind":
				receipt["kind"] = json.RawMessage(`"node_delivery"`)
			case "old_node":
				receipt["node"] = json.RawMessage(`"consumer/consumer-node"`)
			case "partial":
				delete(receipt, "event_id")
			}
			record["initialization"], _ = json.Marshal(receipt)
			if variant == "missing_initialization" {
				record["initialization"] = json.RawMessage(`null`)
			}
			if variant == "old_dependency" {
				record["dependency"] = json.RawMessage(`null`)
			}
			bad, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			if variant == "erased" {
				bad = []byte(`null`)
			}
			if _, err := fixture.db.ExecContext(ctx, `UPDATE event_deliveries SET receiver_materialization_plan=$1 WHERE delivery_id=$2`, string(bad), id); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if _, err := fixture.db.ExecContext(ctx, `UPDATE event_deliveries SET receiver_materialization_plan=$1 WHERE delivery_id=$2`, string(original), id); err != nil {
					t.Error(err)
				}
			}()
			before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")
			if _, _, err := store.LoadPreparedPublishEvent(ctx, event.ID()); err == nil {
				t.Fatal("aggregate accepted corrupt construction receipt")
			}
			if _, err := store.CommitPublication(ctx, command); err == nil {
				t.Fatal("duplicate accepted/repaired corrupt construction receipt")
			}
			claim, err := store.ClaimDelivery(ctx, authority, event, route)
			if err == nil && claim.Disposition != deliverylifecycle.ClaimInvariantInvalid {
				t.Fatalf("corrupt receipt was acquired/deferred as valid: %+v", claim)
			}
			if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")) {
				t.Fatal("corrupt receipt refusal mutated execution history")
			}
		})
	}
	if _, found, err := store.LoadPreparedPublishEvent(ctx, event.ID()); err != nil || !found {
		t.Fatalf("canonical receipt no longer readable after fault removal: %v", err)
	}
}
