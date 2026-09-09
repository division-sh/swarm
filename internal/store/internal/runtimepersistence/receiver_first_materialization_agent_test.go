package runtimepersistence

import (
	"context"
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
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

// Producer-boundary probe for E's ordinary source journey. No selected-fork
// runtime or provider is involved, and no future receiver row is fabricated.
func TestReceiverFirstMaterializationNodeAndAgentAdmissionBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, agent := range []string{"", "same-name", "renamed-observer", "competing-materializers"} {
				name := agent
				if name == "" {
					name = "node-only-control"
				}
				for _, settlement := range []string{"failed", "missing", "retry_cancel", "terminal_race", "rollback_retry"} {
					if agent == "competing-materializers" && settlement != "failed" {
						continue
					}
					t.Run(name+"/"+settlement, func(t *testing.T) {
						root := canonicalrouting.CopyForkReceiverBusinessMutationOwnership(t, false)
						if agent != "" {
							declaration := agent
							if agent == "same-name" || agent == "competing-materializers" {
								declaration = "collector"
							}
							root = canonicalrouting.CopyReceiverMaterializationWithAgent(t, declaration)
							if agent == "competing-materializers" {
								root = canonicalrouting.CopyReceiverMaterializationCompetingNodes(t)
							}
						}
						repo := canonicalrouting.RepoRoot(t)
						if agent == "competing-materializers" {
							before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
							_, err := contracts.LoadWorkflowContractBundleWithOptions(repo, root, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
							if err == nil || !strings.Contains(err.Error(), "event consumer/receiver.seeded has multiple authoritative system node owners") {
								t.Fatalf("competing materializers were not rejected at source admission: %v", err)
							}
							if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
								t.Fatal("invalid receiver source changed persistence")
							}
							return
						}
						bundle, err := contracts.LoadWorkflowContractBundleWithOptions(repo, root, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
						if err != nil {
							t.Fatal(err)
						}
						source := semanticview.Wrap(bundle)
						runID := uuid.NewString()
						ctx := correlation.WithRunID(seedSelectedActivitySourceRun(t, fixture, runID, source), runID)
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
						at := time.Now().UTC().Truncate(time.Microsecond)
						trigger := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "start.seeded", "operator", "", []byte(`{"token":"first"}`), 0, runID, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), at)
						if err := insertCanonicalEventRecordFixture(ctx, fixture.store, trigger); err != nil {
							t.Fatal(err)
						}
						node, err := identity.AdmitExecutableNodeDeclaration(".", "controller")
						if err != nil {
							t.Fatal(err)
						}
						emitted := eventtest.ChildForProducerWithRoutingSource(uuid.NewString(), "receiver.seeded", eventtest.Producer(events.EventProducerNode, node.Key()), "", []byte(`{"token":"first"}`), 0,
							events.LineageFromEvent(trigger), events.EventEnvelope{}, eventtest.RootRoutingSource(runID), at.Add(time.Second))
						eventBus, err := newStoreTestEventBus(t, fixture.store.(storeTestDurableEventBusStore), bus.EventBusOptions{ContractBundle: source, SourceArtifactFact: fact})
						if err != nil {
							t.Fatal(err)
						}
						before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
						plans, err := eventBus.PrepareEnginePublications(ctx, []engine.EmitIntent{{Event: emitted}})
						if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
							t.Fatal("preparation changed database")
						}
						if err != nil || len(plans) != 1 {
							t.Fatalf("ordinary first-materialization preparation: plans=%d err=%v", len(plans), err)
						}
						routes := plans[0].(bus.EnginePublicationPlan).PublicationCommand().Commit.DeliveryRoutes
						want := 1
						if agent != "" {
							want = 2
						}
						if len(routes) != want {
							t.Fatalf("prepared routes=%+v want=%d", routes, want)
						}
						for _, route := range routes {
							if !route.Target.MaterializingEntity() || route.Target.Route().FlowID != "consumer" || route.Target.Route().EntityID == runID {
								t.Fatalf("receiver authority not independent future: %+v", route)
							}
						}
						command := plans[0].(bus.EnginePublicationPlan).PublicationCommand()
						if agent == "renamed-observer" {
							command.Commit.DeliveryRoutes = append([]events.DeliveryRoute(nil), routes...)
							for i, j := 0, len(routes)-1; i < j; i, j = i+1, j-1 {
								command.Commit.DeliveryRoutes[i], command.Commit.DeliveryRoutes[j] = command.Commit.DeliveryRoutes[j], command.Commit.DeliveryRoutes[i]
							}
						}
						store := fixture.store.(interface {
							CommitPublication(context.Context, bus.PublicationCommand) (bus.CommittedPublication, error)
							deliverylifecycle.Store
						})
						if _, err := store.CommitPublication(ctx, command); err != nil {
							t.Fatalf("commit actual prepared receiver publication: %v", err)
						}
						duplicate := command
						duplicate.Commit.DeliveryRoutes = append([]events.DeliveryRoute(nil), command.Commit.DeliveryRoutes...)
						for i, j := 0, len(duplicate.Commit.DeliveryRoutes)-1; i < j; i, j = i+1, j-1 {
							duplicate.Commit.DeliveryRoutes[i], duplicate.Commit.DeliveryRoutes[j] = duplicate.Commit.DeliveryRoutes[j], duplicate.Commit.DeliveryRoutes[i]
						}
						beforeDuplicate := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
						if _, err := store.CommitPublication(ctx, duplicate); err != nil {
							t.Fatalf("recipient permutation changed duplicate identity: %v", err)
						}
						if !reflect.DeepEqual(beforeDuplicate, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
							t.Fatal("reordered duplicate changed receiver obligations or history")
						}
						admitted := command.Commit.Event.Event()
						var nodeRoute, agentRoute events.DeliveryRoute
						for _, route := range routes {
							id, err := deliverylifecycle.DeliveryID(admitted.ID(), route)
							if err != nil {
								t.Fatal(err)
							}
							snapshot, err := store.Snapshot(ctx, id)
							if err != nil || !reflect.DeepEqual(snapshot.Route, route.Normalized()) {
								t.Fatalf("exact durable receiver route: %+v %v", snapshot.Route, err)
							}
							if route.Recipient.IsNode() {
								nodeRoute = route
								continue
							}
							agentRoute = route
							if route.Materialization.Empty() {
								t.Fatal("dependent agent lost exact materialization relation")
							}
							beforeClaim := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
							result, err := store.ClaimDelivery(ctx, snapshot.Authority, admitted, route)
							if err != nil || result.Disposition != deliverylifecycle.ClaimDeferred {
								t.Fatalf("premature agent claim: %s %v %v", result.Disposition, result.Invariant, err)
							}
							if !reflect.DeepEqual(beforeClaim, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
								t.Fatal("blocked dependency appended an attempt or mutated history")
							}
							requireReceiverDependencyCorruptionRefused(t, ctx, fixture, backend.name, command, route, snapshot.Authority)
						}
						if agent == "" {
							return
						}
						requireReceiverHistoricalRouteRefusal(t, ctx, fixture, backend.name, runID, admitted.ID())
						nodeID, err := deliverylifecycle.DeliveryID(admitted.ID(), nodeRoute)
						if err != nil {
							t.Fatal(err)
						}
						nodeSnapshot, err := store.Snapshot(ctx, nodeID)
						if err != nil {
							t.Fatal(err)
						}
						result, err := store.ClaimDelivery(ctx, nodeSnapshot.Authority, admitted, nodeRoute)
						claimed, acquired := result.Acquired()
						if err != nil || !acquired {
							t.Fatalf("claim materializer: %+v %v", result, err)
						}
						failure, ok := failures.EnvelopeFromError(failures.New(failures.ClassLifecycleConflict, "test_materializer_failed", "receiver_test", "materialize", nil))
						if !ok {
							t.Fatal("missing failure envelope")
						}
						wantReason := "receiver_materialization_terminal"
						switch settlement {
						case "rollback_retry":
							dependentID := mustReceiverDeliveryID(t, admitted.ID(), agentRoute)
							var original []byte
							if err := fixture.db.QueryRowContext(ctx, `SELECT receiver_materialization_plan FROM event_deliveries WHERE delivery_id=$1`, dependentID).Scan(&original); err != nil {
								t.Fatal(err)
							}
							var corrupt map[string]json.RawMessage
							if err := json.Unmarshal(original, &corrupt); err != nil {
								t.Fatal(err)
							}
							corrupt["run_id"], _ = json.Marshal(uuid.NewString())
							bad, err := json.Marshal(corrupt)
							if err != nil {
								t.Fatal(err)
							}
							if _, err := fixture.db.ExecContext(ctx, `UPDATE event_deliveries SET receiver_materialization_plan=$1 WHERE delivery_id=$2`, string(bad), dependentID); err != nil {
								t.Fatal(err)
							}
							terminal := deliverylifecycle.Settlement{Disposition: deliverylifecycle.FailureDeadLetter, ReasonCode: "test_materializer_failed", Failure: &failure, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleSelection()}
							beforeSettlement := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
							if _, err := store.SettleFailure(ctx, claimed.Claim, terminal); err == nil {
								t.Fatal("terminal settlement accepted contradictory dependent evidence")
							}
							if !reflect.DeepEqual(beforeSettlement, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
								t.Fatal("failed dependent validation partially committed materializer settlement")
							}
							// Restore only the injected corruption, then retry the same
							// still-current claim through the ordinary settlement owner.
							if _, err := fixture.db.ExecContext(ctx, `UPDATE event_deliveries SET receiver_materialization_plan=$1 WHERE delivery_id=$2`, string(original), dependentID); err != nil {
								t.Fatal(err)
							}
							if _, err := store.SettleFailure(ctx, claimed.Claim, terminal); err != nil {
								t.Fatalf("retry settlement after fault removal: %v", err)
							}
						case "terminal_race":
							var workers sync.WaitGroup
							start := make(chan struct{})
							results := make(chan error, 17)
							for range 16 {
								workers.Add(1)
								go func() {
									defer workers.Done()
									<-start
									result, err := store.ClaimDelivery(ctx, nodeSnapshot.Authority, admitted, agentRoute)
									if err == nil && result.Disposition != deliverylifecycle.ClaimDeferred && result.Disposition != deliverylifecycle.ClaimTerminal {
										err = fmt.Errorf("concurrent dependent claim: disposition=%s invariant=%v", result.Disposition, result.Invariant)
									}
									results <- err
								}()
							}
							workers.Add(1)
							go func() {
								defer workers.Done()
								<-start
								_, err := store.SettleFailure(ctx, claimed.Claim, deliverylifecycle.Settlement{
									Disposition: deliverylifecycle.FailureDeadLetter, ReasonCode: "test_materializer_failed", Failure: &failure, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleSelection(),
								})
								results <- err
							}()
							close(start)
							workers.Wait()
							close(results)
							for err := range results {
								if err != nil {
									t.Fatal(err)
								}
							}
						case "missing":
							// A settled callback alone is not proof that the exact node
							// mutation persisted the future entity.
							if _, err := store.SettleSuccess(ctx, claimed.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
								t.Fatal(err)
							}
							wantReason = "receiver_materialization_missing"
						case "retry_cancel":
							if _, err := store.SettleFailure(ctx, claimed.Claim, deliverylifecycle.Settlement{
								Disposition: deliverylifecycle.FailureRetry, ReasonCode: "test_materializer_retry", Failure: &failure, RetryBase: time.Hour, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleSelection(),
							}); err != nil {
								t.Fatal(err)
							}
							beforeRetry := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
							result, err := store.ClaimDelivery(ctx, nodeSnapshot.Authority, admitted, agentRoute)
							if err != nil || result.Disposition != deliverylifecycle.ClaimDeferred {
								t.Fatalf("retrying materializer allowed agent claim: %+v %v", result, err)
							}
							observation, err := store.ObserveDeliveryContinuation(ctx, nodeSnapshot.Authority, mustReceiverDeliveryID(t, admitted.ID(), agentRoute))
							if err != nil {
								t.Fatal(err)
							}
							if !reflect.DeepEqual(beforeRetry, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
								t.Fatalf("retrying dependent observation changed history: %+v", observation)
							}
							if _, err := store.TerminalizeRun(ctx, runID, "receiver_test_cancel"); err != nil {
								t.Fatal(err)
							}
							wantReason = "receiver_test_cancel"
						default:
							if _, err := store.SettleFailure(ctx, claimed.Claim, deliverylifecycle.Settlement{
								Disposition: deliverylifecycle.FailureDeadLetter, ReasonCode: "test_materializer_failed", Failure: &failure, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleSelection(),
							}); err != nil {
								t.Fatal(err)
							}
						}
						agentID, err := deliverylifecycle.DeliveryID(admitted.ID(), agentRoute)
						if err != nil {
							t.Fatal(err)
						}
						dependent, err := store.Snapshot(ctx, agentID)
						if err != nil || dependent.Status != deliverylifecycle.StatusDeadLetter || dependent.ReasonCode != wantReason || !dependent.StartedAt.IsZero() || dependent.ActiveSessionID != "" {
							t.Fatalf("dependent survived terminal materializer: %+v %v", dependent, err)
						}
						outcomes, err := store.Outcomes(ctx, agentID)
						if err != nil || len(outcomes) != 1 || outcomes[0].Outcome != "terminalized" || outcomes[0].ReasonCode != wantReason || len(outcomes[0].SideEffects) != 0 {
							t.Fatalf("unexecuted dependent has false execution history: %+v %v", outcomes, err)
						}
					})
				}
			}
		})
	}
}

func requireReceiverHistoricalRouteRefusal(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, backend, runID, eventID string) {
	t.Helper()
	owner := fixture.store.(interface {
		PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
	})
	before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")
	plan, err := owner.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: runID, At: eventID})
	if err != nil {
		t.Fatalf("historical receiver planning: %v", err)
	}
	refused := false
	for _, blocker := range plan.UnsupportedBlockers {
		refused = refused || blocker.Code == runfork.RunForkBlockerFlowRouteHistoryUnproven
	}
	if plan.ExecutionReady || !refused {
		t.Fatalf("historical route policy was bypassed by the receiver plan: %+v", plan.UnsupportedBlockers)
	}
	if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")) {
		t.Fatal("historical receiver refusal mutated source or created fork work")
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

func requireReceiverDependencyCorruptionRefused(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, backend string, command bus.PublicationCommand, route events.DeliveryRoute, authority deliverylifecycle.ExecutionAuthority) {
	t.Helper()
	store := fixture.store.(interface {
		CommitPublication(context.Context, bus.PublicationCommand) (bus.CommittedPublication, error)
		LoadPreparedPublishEvent(context.Context, string) (bus.PreparedPublishEvent, bool, error)
		deliverylifecycle.Store
	})
	event := command.Commit.Event.Event()
	id, err := deliverylifecycle.DeliveryID(event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	var original []byte
	if err := fixture.db.QueryRowContext(ctx, `SELECT receiver_materialization_plan FROM event_deliveries WHERE delivery_id=$1`, id).Scan(&original); err != nil {
		t.Fatal(err)
	}
	for _, variant := range []string{"erased", "wrong_run", "wrong_event", "missing_materializer", "changed_source", "missing_dependents"} {
		t.Run("durable_corruption_"+variant, func(t *testing.T) {
			var wire map[string]json.RawMessage
			if err := json.Unmarshal(original, &wire); err != nil {
				t.Fatal(err)
			}
			switch variant {
			case "wrong_run":
				wire["run_id"], _ = json.Marshal(uuid.NewString())
			case "wrong_event":
				wire["event_id"], _ = json.Marshal(uuid.NewString())
			case "missing_materializer":
				wire["materializer_route_identity"], _ = json.Marshal("delivery-route-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
			case "changed_source":
				wire["routing_source"] = json.RawMessage(`{"kind":"absent"}`)
			case "missing_dependents":
				wire["dependent_route_identities"] = json.RawMessage(`[]`)
			}
			bad, err := json.Marshal(wire)
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
				t.Fatal("canonical aggregate accepted corrupted dependency")
			}
			if _, err := store.CommitPublication(ctx, command); err == nil {
				t.Fatal("duplicate repaired or accepted corrupted dependency")
			}
			claimed, err := store.ClaimDelivery(ctx, authority, event, route)
			if err == nil && claimed.Disposition != deliverylifecycle.ClaimInvariantInvalid {
				t.Fatalf("corrupted dependency acquired or deferred as valid: %+v", claimed)
			}
			if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")) {
				t.Fatal("corrupted dependency refusal mutated domain history")
			}
		})
	}
	if _, found, err := store.LoadPreparedPublishEvent(ctx, event.ID()); err != nil || !found {
		t.Fatalf("restored canonical dependency failed readback: %v", err)
	}
}
