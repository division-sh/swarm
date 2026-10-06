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
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	delivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestWorkflowNodeNoopSettlementAtomicBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, failure := range []string{"healthy", "renewal", "selection", "completion", "expired", "foreign_token", "stale_version", "agent", "cancelled", "terminal_run", "replaced_authority"} {
			t.Run(backend+"/"+failure, func(t *testing.T) {
				f, record := constructWorkflowMutationFixture(t, backend, "node-noop", time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond))
				owner := f.store.(delivery.Store)
				runID := correlation.RunIDFromContext(f.ctx)
				event := eventtest.ExistingRunRootIngress(uuid.NewString(), "node.noop", "fixture", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
				route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustPersistenceNode("node-noop", "worker")), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "node-noop", FlowInstance: record.Identity.Route.InstancePath, EntityID: record.EntityID})}
				selected := f.store.(stateOnlyAcquisitionStore)
				if err := commitSemanticEventFixtureWithRoutes(f.ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
					t.Fatal(err)
				}
				claimed, err := claimDeliveryFixture(f.ctx, selected, event, route)
				if err != nil {
					t.Fatal(err)
				}
				claim := claimed.Claim
				if failure == "foreign_token" || failure == "stale_version" || failure == "agent" {
					token, version, class := claim.PersistenceToken(), claim.Version(), claim.SubscriberClass()
					if failure == "foreign_token" {
						token = uuid.NewString()
					}
					if failure == "stale_version" {
						version++
					}
					if failure == "agent" {
						class = delivery.SubscriberAgent
					}
					claim, err = delivery.AdmitPersistedClaim(claim.DeliveryID(), claim.RunID(), claim.RouteIdentity(), token, version, class, claim.SubscriberID())
					if err != nil {
						t.Fatal(err)
					}
				}
				if failure == "expired" {
					if _, err := f.db.Exec(`UPDATE event_delivery_attempts SET started_at=$1, lease_expires_at=$2 WHERE delivery_id=$3 AND open_marker=TRUE`, time.Now().UTC().Add(-2*time.Second), time.Now().UTC().Add(-time.Second), claim.DeliveryID()); err != nil {
						t.Fatal(err)
					}
				}
				if failure == "terminal_run" {
					if _, err := markRunTerminalStatusForTest(f.ctx, f.store, runID, "cancelled", nil, time.Now().UTC()); err != nil {
						t.Fatal(err)
					}
				}
				if failure == "replaced_authority" {
					fact, found := correlation.SourceArtifactFactFromContext(f.ctx)
					if !found {
						t.Fatal("missing normal execution source")
					}
					successor, err := delivery.NewNormalExecutionAuthority(fact, "node-noop-successor:"+runID, 2)
					if err != nil {
						t.Fatal(err)
					}
					if err := owner.ActivateDeliveryAuthority(f.ctx, successor); err != nil {
						t.Fatal(err)
					}
				}
				if failure != "terminal_run" {
					source := semanticview.Wrap(f.bundle)
					root, _ := semanticview.WorkflowStageTopology(source, ".")
					flow, _ := semanticview.WorkflowStageTopology(source, "node-noop")
					classifier, err := contracts.NewWorkflowStageClassifier(root, map[string]contracts.WorkflowStageTopology{"node-noop": flow})
					if err != nil {
						t.Fatal(err)
					}
					catalog, err := runlifecycle.NewCompiledFinalCatalog(classifier)
					if err != nil {
						t.Fatal(err)
					}
					lifecycle := f.store.(runLifecycleTerminalTestStore)
					if _, err := lifecycle.RequestCompletionCandidate(f.ctx, runlifecycle.ImmediateCandidate(runID)); err != nil {
						t.Fatal(err)
					}
					blocked, err := executeRunCompletionCandidateForRun(f.ctx, lifecycle, f.bundle.SourceArtifact.BundleHash(), runID, catalog)
					if err != nil || blocked.Outcome != runlifecycle.OutcomeAwaitMutation {
						t.Fatalf("pending delivery must retain mutation-owned completion: %+v %v", blocked, err)
					}
				}
				before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
				var remove []string
				if failure == "renewal" || failure == "selection" || failure == "completion" {
					table, action := "event_delivery_handler_rule_selections", "INSERT"
					if failure == "renewal" {
						table, action = "event_delivery_attempts", "UPDATE OF lease_expires_at"
					}
					if failure == "completion" {
						table, action = "runs", "UPDATE OF completion_revision"
					}
					install := []string{"CREATE TRIGGER noop_cut BEFORE " + action + " ON " + table + " BEGIN SELECT RAISE(ABORT,'node_noop_cut'); END"}
					remove = []string{"DROP TRIGGER noop_cut"}
					if backend == "postgres" {
						install = []string{"CREATE FUNCTION noop_cut_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'node_noop_cut'; RETURN NEW; END $$", "CREATE TRIGGER noop_cut BEFORE " + action + " ON " + table + " FOR EACH ROW EXECUTE FUNCTION noop_cut_fn()"}
						remove = []string{"DROP TRIGGER noop_cut ON " + table, "DROP FUNCTION noop_cut_fn()"}
					}
					for _, statement := range install {
						if _, err := f.db.Exec(statement); err != nil {
							t.Fatal(err)
						}
					}
				}
				collector, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
				if err != nil {
					t.Fatal(err)
				}
				ctx := f.ctx
				if failure == "cancelled" {
					var cancel func()
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				result, settleErr := owner.SettleWorkflowNodeSuccess(ctx, claim, []string{"handler_completed"}, time.Millisecond, delivery.NotApplicableHandlerRuleSelection())
				counts := collector.Snapshot()
				restore()
				for _, statement := range remove {
					if _, err := f.db.Exec(statement); err != nil {
						t.Fatal(err)
					}
				}
				after := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
				if failure != "healthy" {
					if settleErr == nil || result.Acknowledged || result.Snapshot.DeliveryID != "" {
						t.Fatalf("rejected settlement returned commit evidence: %+v %v", result, settleErr)
					}
					if len(remove) != 0 && !strings.Contains(settleErr.Error(), "node_noop_cut") {
						t.Fatalf("wrong failure boundary: %v", settleErr)
					}
					if !reflect.DeepEqual(before, after) {
						t.Fatal("failed atomic settlement changed durable facts")
					}
					return
				}
				if settleErr != nil || !result.Acknowledged || result.Snapshot.Status != delivery.StatusDelivered || !result.Snapshot.MatchesSettlementClaim(claim) {
					t.Fatalf("exact node settlement: %+v %v", result, settleErr)
				}
				if counts.Total.WriteCommits != 1 || counts.ByOperation[transactiontest.DeliverySettle].WriteCommits != 1 || counts.ByOperation[transactiontest.DeliveryRenew].Begun != 0 {
					t.Fatalf("renewal/settlement did not share one transaction: %+v", counts)
				}
				for _, table := range []string{"flow_instances", "entity_state", "flow_instance_runtime_readiness", "events"} {
					if !reflect.DeepEqual(before[table], after[table]) {
						t.Fatalf("mutation-free settlement changed %s", table)
					}
				}
				outcomes, err := owner.Outcomes(f.ctx, claim.DeliveryID())
				if err != nil || len(outcomes) != 1 || len(outcomes[0].SideEffects) != 1 || outcomes[0].SideEffects[0] != "handler_completed" {
					t.Fatalf("exact outcome: %+v %v", outcomes, err)
				}
				if _, err := owner.SettleWorkflowNodeSuccess(f.ctx, claim, nil, 0, delivery.NotApplicableHandlerRuleSelection()); !errors.Is(err, delivery.ErrConflict) {
					t.Fatalf("duplicate claim settlement: %v", err)
				}
			})
		}
	}
}

func TestSelectedWorkflowNodeNoopSettlementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, failure := range []string{"healthy", "expired_execution", "quiesced_execution", "stale_generation"} {
			t.Run(backend+"/"+failure, func(t *testing.T) {
				var fixture selectedCompletionFixture
				if backend == "sqlite" {
					store := newBootstrappedSQLiteRuntimeStoreForTest(t)
					fixture = newSelectedCompletionFixture(t, store, store.backend.ConstructionHandle(), true)
				} else {
					_, db, _ := testutil.StartPostgres(t)
					fixture = newSelectedCompletionFixture(t, admitTestPostgresStore(t, db), db, false)
				}
				ctx := testAuthorActivityContext()
				store := fixture.store.(interface {
					delivery.Store
					selectedForkEventFixtureStore
				})
				route := testEntitylessNodeDeliveryRoute("noop")
				issued, err := fixture.store.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
				if err != nil {
					t.Fatal(err)
				}
				authority, err := fixture.store.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "node-noop-proof", time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				selectedCtx := managedSelectedExecutionStoreTestContext(t, effects.WithAuthority(ctx, authority), authority)
				fact, ok := correlation.SourceArtifactFactFromContext(ctx)
				if !ok {
					t.Fatal("missing selected source fact")
				}
				deliveryAuthority, err := delivery.NewSelectedExecutionAuthority(fact, issued.ExecutionID, fixture.forkRun, issued.Generation)
				if err != nil {
					t.Fatal(err)
				}
				lineage, err := events.NewSelectedForkLineage(fixture.forkRun, fixture.sourceRun, fixture.eventID, "selection:no-op", "node-noop", executionmode.Live)
				if err != nil {
					t.Fatal(err)
				}
				target := route.Target.Route()
				envelope := events.EnvelopeForTargetRoute(events.EnvelopeForTargetSet(events.EventEnvelope{}, []events.RouteIdentity{target}), target)
				event, err := bindSemanticEventFixturePayload(eventtest.SelectedForkReplay(uuid.NewString(), "selected.noop", eventtest.Producer(events.EventProducerNode, "noop"), "node-noop", []byte(`{}`), 1, lineage, envelope, time.Now().UTC()))
				if err != nil {
					t.Fatal(err)
				}
				admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
				if err != nil {
					t.Fatal(err)
				}
				selectedCtx, releaseCatalog, err := semanticEventFixtureContext(selectedCtx, store, event)
				if err != nil {
					t.Fatal(err)
				}
				defer releaseCatalog()
				pipelineOwner := pipelineObligationOwnerForFixture(store)
				publication, err := pipelineOwner.ClaimPublication(selectedCtx, event.ID())
				if err != nil {
					t.Fatal(err)
				}
				defer pipelineOwner.Release(context.WithoutCancel(selectedCtx), publication)
				scope, hasScope := authoractivity.ScopeFromContext(selectedCtx)
				descriptor, hasDescriptor, err := authoractivity.ResolvedEventDescriptorFromContext(selectedCtx, scope, string(event.Type()))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := store.CommitSelectedForkEvent(selectedCtx, bus.CommitSelectedForkEventRequest{
					Commit:      bus.CommitPublishRequest{Event: admitted, DeliveryRoutes: []events.DeliveryRoute{route}, DeliveryAuthority: deliveryAuthority, RouteSettlement: testRouteSettlement(event, []events.DeliveryRoute{route}), ReplayScope: pipelineobligation.ScopeSubscribed, PipelineClaim: publication},
					Lineage:     runfork.RunForkSelectedContractExecutionLineage{ForkRunID: fixture.forkRun, SourceRunID: fixture.sourceRun, SourceEventID: fixture.eventID, ForkEventID: event.ID(), EventName: string(event.Type()), SelectionAuthority: lineage.AuthorityStamp(), CreatedAt: event.CreatedAt()},
					AuthorScope: scope, HasAuthorScope: hasScope, AuthorDescriptor: descriptor, HasAuthorDescriptor: hasDescriptor,
				}); err != nil {
					t.Fatal(err)
				}
				claimResult, err := store.ClaimDelivery(selectedCtx, deliveryAuthority, event, route)
				claimed, acquired := claimResult.Acquired()
				if err != nil || !acquired {
					t.Fatalf("selected claim: %+v %v", claimResult, err)
				}
				switch failure {
				case "expired_execution":
					if _, err := fixture.db.Exec(`UPDATE run_fork_selected_contract_runtime_executions SET lease_expires_at=$1 WHERE execution_id=$2`, time.Now().UTC().Add(-time.Second), issued.ExecutionID); err != nil {
						t.Fatal(err)
					}
				case "quiesced_execution":
					if err := fixture.store.QuiesceRunForkSelectedContractRuntimeExecution(ctx, authority); err != nil {
						t.Fatal(err)
					}
				case "stale_generation":
					if err := fixture.store.QuiesceRunForkSelectedContractRuntimeExecution(ctx, authority); err != nil {
						t.Fatal(err)
					}
					if err := fixture.store.CloseRunForkSelectedContractRuntimeExecution(ctx, issued.ExecutionID); err != nil {
						t.Fatal(err)
					}
					successor, err := fixture.store.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
					if err != nil || successor.Generation != issued.Generation+1 {
						t.Fatalf("replacement execution: %+v %v", successor, err)
					}
					if _, err := fixture.store.ClaimRunForkSelectedContractRuntimeExecution(ctx, successor, "replacement-noop-proof", time.Minute); err != nil {
						t.Fatal(err)
					}
				}
				before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")
				result, err := store.SettleWorkflowNodeSuccess(selectedCtx, claimed.Claim, []string{"handler_completed"}, 0, delivery.NotApplicableHandlerRuleSelection())
				after := snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")
				if failure != "healthy" {
					if err == nil || result.Acknowledged || !reflect.DeepEqual(before, after) {
						t.Fatalf("selected fence bypassed: result=%+v err=%v unchanged=%t", result, err, reflect.DeepEqual(before, after))
					}
					return
				}
				if err != nil || !result.Acknowledged || !result.Snapshot.MatchesSettlementClaim(claimed.Claim) || result.Snapshot.Status != delivery.StatusDelivered {
					t.Fatalf("selected exact settlement: %+v %v", result, err)
				}
				for _, table := range []string{"flow_instances", "entity_state", "flow_instance_runtime_readiness", "events", "run_fork_selected_contract_runtime_executions"} {
					if !reflect.DeepEqual(before[table], after[table]) {
						t.Fatalf("selected no-op settlement changed %s", table)
					}
				}
			})
		}
	}
}

func TestWorkflowNodeNoopSettlementAcknowledgementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, phase := range []string{"healthy", "commit_refused", "commit_ack_lost", "handoff_failed"} {
			t.Run(backend+"/"+phase, func(t *testing.T) {
				f, control := openCompletionOutcomeFixture(t, backend)
				ctx, runID := f.context, f.authority.Target.RunID
				event := eventtest.ExistingRunRootIngress(uuid.NewString(), "node.noop", "fixture", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
				route := testEntitylessNodeDeliveryRoute("noop")
				if err := commitSemanticEventFixtureWithRoutes(ctx, f.store, event, []events.DeliveryRoute{route}); err != nil {
					t.Fatal(err)
				}
				claim, err := claimDeliveryFixture(ctx, f.store, event, route)
				if err != nil {
					t.Fatal(err)
				}
				failure := errors.New("node settlement " + phase)
				var hash string
				if err := f.db.QueryRow(`SELECT bundle_hash FROM runs WHERE run_id=$1`, runID).Scan(&hash); err != nil {
					t.Fatal(err)
				}
				submits := 0
				registration, err := f.store.(runlifecycle.CandidateRegistrar).RegisterCompletionCandidateSink(ctx, runlifecycle.CandidateScope{BundleHash: hash}, &completionHandoffEvidenceProbeSink{submit: func(candidate runlifecycle.Candidate) error {
					submits++
					if candidate.RunID != runID {
						t.Errorf("foreign completion: %+v", candidate)
					}
					if phase == "handoff_failed" {
						return failure
					}
					return nil
				}})
				if err != nil {
					t.Fatal(err)
				}
				defer registration.Release()
				control.phase, control.failure = phase, failure
				control.enabled.Store(true)
				result, settleErr := f.store.(delivery.Store).SettleWorkflowNodeSuccess(ctx, claim.Claim, nil, 0, delivery.NotApplicableHandlerRuleSelection())
				control.enabled.Store(false)
				ack := phase == "healthy" || phase == "handoff_failed"
				if result.Acknowledged != ack || (result.Snapshot.DeliveryID != "") != ack {
					t.Fatalf("wrong commit evidence: %+v %v", result, settleErr)
				}
				if phase == "healthy" && settleErr != nil || phase != "healthy" && !errors.Is(settleErr, failure) {
					t.Fatalf("wrong auxiliary error: %v", settleErr)
				}
				wantSubmits := 0
				if ack {
					wantSubmits = 1
				}
				if submits != wantSubmits {
					t.Fatalf("handoffs=%d want=%d", submits, wantSubmits)
				}
				snapshot, err := f.store.Snapshot(ctx, claim.Claim.DeliveryID())
				if err != nil {
					t.Fatal(err)
				}
				wantStatus := delivery.StatusDelivered
				if phase == "commit_refused" {
					wantStatus = delivery.StatusInProgress
				}
				if snapshot.Status != wantStatus {
					t.Fatalf("durable status=%s want=%s", snapshot.Status, wantStatus)
				}
			})
		}
	}
}
