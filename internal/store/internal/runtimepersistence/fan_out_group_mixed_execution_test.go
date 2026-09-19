package runtimepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// Unlike the preparation-only mixed-route control, this crosses real manager,
// workflow, continuation, publication-group and selected-store boundaries.
func TestFanOutGroupMixedExecutionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			handlers := lifecycleprobe.New()
			f, seen, _ := newMixedExecutionFixture(t, backend, handlers)
			f.prepare(t)
			wantCounts := []int{1, 1, 4, 0}
			for i, plan := range f.plans {
				routes := plan.(bus.EnginePublicationPlan).PublicationCommand().Commit.DeliveryRoutes
				if len(routes) != wantCounts[i] {
					t.Fatalf("ordinal %d routes=%+v, want %d", i, routes, wantCounts[i])
				}
				mixedAssertRecipients(t, i, routes)
			}
			f.seal(t)
			committed, err := f.owner.CommitFanOutChunk(f.ctx, f.command)
			if err != nil || len(committed.Publications) != 4 || committed.Intent.Cursor != 4 || committed.Intent.Status != fanoutobligation.StatusClosed {
				t.Fatalf("one exact mixed range: %+v err=%v", committed, err)
			}
			for i, plan := range f.plans {
				read, found, err := f.raw.(storeTestDurableEventBusStore).LoadPreparedPublishEvent(f.ctx, f.events[i].ID())
				want := plan.(bus.EnginePublicationPlan).PublicationCommand().Commit.DeliveryRoutes
				if err != nil || !found || !reflect.DeepEqual(mixedRouteKeys(t, read.DeliveryRoutes), mixedRouteKeys(t, want)) {
					t.Fatalf("ordinal %d exact persisted routes: %+v err=%v want=%+v", i, read.DeliveryRoutes, err, want)
				}
			}
			if err := f.bus.FinalizeFanOutPublications(f.ctx, f.group, committed.Publications); err != nil {
				t.Fatal(err)
			}
			observed := &mixedExecutionGroup{PublicationGroup: f.group}
			if err := f.bus.DispatchFanOutPublications(f.ctx, observed, committed.Publications); err != nil {
				t.Fatal(err)
			}
			observed.assertAcknowledged(t, f.claims)
			deadline := time.NewTimer(5 * time.Second)
			defer deadline.Stop()
			got := map[string]int{}
			for len(got) < 2 {
				select {
				case event := <-seen:
					if event.RunID() != f.seed.runID || (event.ID() != f.events[1].ID() && event.ID() != f.events[2].ID()) {
						t.Fatalf("agent executed ownerless/unselected event: %s run=%s", event.ID(), event.RunID())
					}
					got[event.ID()]++
					if got[event.ID()] != 1 {
						t.Fatalf("duplicate real agent execution: %s", event.ID())
					}
				case <-deadline.C:
					t.Fatalf("real agent execution missing: %v", got)
				}
			}
			mixedAwaitDeliveries(t, f, 6)
			for i, event := range f.events {
				mixedAssertReceipt(t, f, event.ID(), "success", "pipeline_persisted")
				public, err := f.raw.(interface {
					LoadOperatorEvent(context.Context, string) (operatorread.OperatorEventFull, error)
				}).LoadOperatorEvent(f.ctx, event.ID())
				if err != nil || public.EventID != event.ID() || public.RunID != f.seed.runID || public.EventName != string(event.Type()) || len(public.Deliveries) != wantCounts[i] {
					t.Fatalf("exact public event ordinal %d: %+v err=%v", i, public, err)
				}
				for _, delivery := range public.Deliveries {
					if !delivery.Terminal || delivery.Status != "delivered" || delivery.ClaimVersion < 1 || delivery.Failure != nil {
						t.Fatalf("public delivery was not actually executed and settled: %+v", delivery)
					}
				}
				if i == 3 && (public.NoDelivery == nil || public.NoDelivery.Reason == "") {
					t.Fatalf("no-route member lacks typed public evidence: %+v", public)
				}
			}
			handlerCtx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
			defer cancel()
			for _, plan := range f.plans {
				for _, route := range plan.(bus.EnginePublicationPlan).PublicationCommand().Commit.DeliveryRoutes {
					if !route.Recipient.IsNode() {
						continue
					}
					signal, err := handlers.WaitForHandlerCompleted(handlerCtx, plan.DurablePublicationEventID(), route.Recipient.ID())
					if err != nil || signal.Status != "completed" {
						t.Fatalf("actual selected node completion: %+v err=%v", signal, err)
					}
				}
			}
			select {
			case event := <-seen:
				t.Fatalf("duplicate or unselected agent delivery: %s", event.ID())
			default:
			}
			query := fanoutobligation.ListQuery{RunID: f.seed.runID}
			page, err := f.raw.(operatorread.FanOutReader).ListFanOutIntents(f.ctx, query)
			if err != nil || page.Validate(query) != nil || len(page.Intents) != 1 || page.Intents[0].Cursor != 4 || page.Intents[0].Owed != 0 || page.Intents[0].Status != fanoutobligation.StatusClosed {
				t.Fatalf("public readback after actual mixed dispatch: %+v err=%v", page, err)
			}
		})
	}
}

// The last (no-route) member fails after the three routed members really
// execute. The interceptor supplies a supported domain outcome, never a store
// disposition or a successful settlement acknowledgement.
func TestFanOutGroupMixedDispatchFailureBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, kind := range []string{"terminal", "dead_letter"} {
			t.Run(backend+"/"+kind, func(t *testing.T) {
				f, _, coordinator := newMixedExecutionFixture(t, backend, nil)
				fault := failures.New(failures.ClassComputeFailure, "mixed_dispatch_"+kind, "mixed-group-proof", "dispatch", nil)
				f.bus.SetInterceptors(mixedDispatchFailure{eventID: f.events[3].ID(), deadLetter: kind == "dead_letter", failure: fault}, coordinator)
				f.prepare(t)
				f.seal(t)
				committed, err := f.owner.CommitFanOutChunk(f.ctx, f.command)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.bus.FinalizeFanOutPublications(f.ctx, f.group, committed.Publications); err != nil {
					t.Fatal(err)
				}
				observed := &mixedExecutionGroup{PublicationGroup: f.group}
				err = f.bus.DispatchFanOutPublications(f.ctx, observed, committed.Publications)
				if kind == "terminal" && !errors.Is(err, fault) || kind == "dead_letter" && err != nil {
					t.Fatalf("canonical dispatch error=%v want=%s", err, kind)
				}
				if len(observed.requests) != 4 {
					t.Fatalf("terminal segment members=%d", len(observed.requests))
				}
				for i, request := range observed.requests {
					wantKind, wantReason, wantReceipt := pipelineobligation.DispositionAcknowledged, "pipeline_persisted", "success"
					if i == 3 {
						wantKind, wantReason, wantReceipt = pipelineobligation.DispositionTerminal, "pipeline_outbox_dispatch_failed", "dead_letter"
						wantClass := failures.ClassInternalFailure
						wantDetail := "event_interceptor_failed"
						if kind == "dead_letter" {
							wantKind, wantReceipt, wantReason, wantClass = pipelineobligation.DispositionDeadLetter, "dead_letter", "mixed_dispatch_dead_letter", failures.ClassComputeFailure
							wantDetail = wantReason
						}
						failure := request.Disposition.Failure()
						if failure == nil || failure.Class != wantClass || failure.Detail.Code != wantDetail {
							t.Fatalf("failure precision: %+v", failure)
						}
						mixedAssertFailure(t, f, f.events[i].ID(), wantClass, wantDetail)
					}
					if !reflect.DeepEqual(request.Claim, f.claims[i]) || request.Disposition.Kind() != wantKind || request.Disposition.ReasonCode() != wantReason {
						t.Fatalf("ordinal %d collected outcome=%+v", i, request)
					}
					mixedAssertReceipt(t, f, f.events[i].ID(), wantReceipt, wantReason)
				}
				acknowledged := 0
				for _, result := range observed.results {
					for _, member := range result.Results {
						acknowledged++
						wantHandoff := !reflect.DeepEqual(member.Claim, f.claims[3])
						if !member.Outcome.Committed() || member.Outcome.DeliveryHandoffCommitted() != wantHandoff {
							t.Fatalf("missing exact terminal handoff: %+v", member)
						}
					}
				}
				if acknowledged != 4 {
					t.Fatalf("committed terminal acknowledgements=%d want=4", acknowledged)
				}
				mixedAwaitDeliveries(t, f, 6)
			})
		}
	}
}

type mixedDispatchFailure struct {
	eventID    string
	deadLetter bool
	release    bool
	calls      *atomic.Int32
	failure    error
}

func (p mixedDispatchFailure) Intercept(_ context.Context, event events.Event) (bool, []events.Event, pipelineobligation.ExecutionOutcome, error) {
	if event.ID() != p.eventID {
		return true, nil, pipelineobligation.Continue(), nil
	}
	if p.calls != nil {
		p.calls.Add(1)
	}
	if p.release {
		return false, nil, pipelineobligation.ReleaseForRetry("mixed_recovery_boundary", nil), nil
	}
	if p.deadLetter {
		failure, _ := failures.EnvelopeFromError(p.failure)
		return false, nil, pipelineobligation.DeadLetterExecution(failure.Detail.Code, &failure), nil
	}
	return false, nil, pipelineobligation.Continue(), p.failure
}

// Quarantine is not a general fan-out interceptor result. Exercise its real
// recovery preclassification after the group's exact final callback releases,
// with the existing missing-scope corruption seam; never relabel a fan-out
// event mailbox.card_decided or synthesize a quarantine execution outcome.
func TestFanOutGroupMixedRecoveryQuarantineBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, _, coordinator := newMixedExecutionFixture(t, backend, nil)
			var calls atomic.Int32
			last := f.events[3].ID()
			f.bus.SetInterceptors(mixedDispatchFailure{eventID: last, release: true, calls: &calls}, coordinator)
			f.prepare(t)
			f.seal(t)
			committed, err := f.owner.CommitFanOutChunk(f.ctx, f.command)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.bus.FinalizeFanOutPublications(f.ctx, f.group, committed.Publications); err != nil {
				t.Fatal(err)
			}
			observed := &mixedExecutionGroup{PublicationGroup: f.group}
			if err := f.bus.DispatchFanOutPublications(f.ctx, observed, committed.Publications); err != nil {
				t.Fatal(err)
			}
			observed.assertAcknowledged(t, f.claims[:3])
			if calls.Load() != 1 {
				t.Fatalf("exact release callback count=%d", calls.Load())
			}
			if err := f.group.Close(f.ctx); err != nil {
				t.Fatal(err)
			}
			var receipts int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, last).Scan(&receipts); err != nil {
				t.Fatal(err)
			}
			if receipts != 0 {
				t.Fatal("released member fabricated terminal receipt")
			}
			deleted, err := f.db.ExecContext(f.ctx, `DELETE FROM committed_replay_scopes WHERE event_id=$1`, last)
			if err != nil {
				t.Fatal(err)
			}
			if n, err := deleted.RowsAffected(); err != nil || n != 1 {
				t.Fatalf("exact corruption seam deleted=%d err=%v", n, err)
			}
			// Inspect the owner's classification without settling it in the test.
			work, err := f.store().ClaimEvent(f.ctx, last, pipelineobligation.PurposeRecovery)
			if err != nil {
				t.Fatal(err)
			}
			disposition, classified := work.PreDispatchDisposition()
			if !classified || disposition.Kind() != pipelineobligation.DispositionQuarantined || disposition.ReasonCode() != "committed_pipeline_scope_missing" {
				t.Fatalf("canonical quarantine classification: %+v classified=%v", disposition, classified)
			}
			if err := f.store().Release(f.ctx, work.Claim); err != nil {
				t.Fatal(err)
			}
			result, err := f.bus.ReleaseRunQueue(f.ctx, f.seed.runID, 8)
			if err != nil || result.Settled < 1 {
				t.Fatalf("actual bounded recovery: %+v err=%v", result, err)
			}
			mixedAssertReceipt(t, f, last, "dead_letter", "committed_pipeline_scope_missing")
			mixedAssertFailure(t, f, last, failures.ClassSchemaInvalid, "committed_pipeline_scope_missing")
			if calls.Load() != 1 {
				t.Fatal("quarantined member entered normal dispatch")
			}
			for _, event := range f.events[:3] {
				mixedAssertReceipt(t, f, event.ID(), "success", "pipeline_persisted")
			}
			mixedAwaitDeliveries(t, f, 6)
		})
	}
}

func mixedAssertFailure(t *testing.T, f *groupProofFixture, eventID string, class failures.Class, detail string) {
	t.Helper()
	var raw []byte
	if err := f.db.QueryRowContext(f.ctx, `SELECT failure FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, eventID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	failure, err := failures.UnmarshalEnvelope(raw)
	if err != nil || failure.Class != class || failure.Detail.Code != detail {
		t.Fatalf("persisted precise failure=%s err=%v", raw, err)
	}
}

func mixedAssertReceipt(t *testing.T, f *groupProofFixture, eventID, wantOutcome, wantReason string) {
	t.Helper()
	var count int
	var outcome, reason string
	if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*), COALESCE(MAX(outcome),''), COALESCE(MAX(reason_code),'') FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, eventID).Scan(&count, &outcome, &reason); err != nil {
		t.Fatal(err)
	}
	if count != 1 || outcome != wantOutcome || reason != wantReason {
		t.Fatalf("exact pipeline receipt for %s: %d/%s/%s want=1/%s/%s", eventID, count, outcome, reason, wantOutcome, wantReason)
	}
}

func mixedAssertRecipients(t *testing.T, ordinal int, routes []events.DeliveryRoute) {
	t.Helper()
	node := func(flow, name string) string {
		declaration, err := identity.AdmitExecutableNodeDeclaration(flow, name)
		if err != nil {
			t.Fatal(err)
		}
		return "node:" + declaration.Key()
	}
	want := [][]string{{node("one", "one-node")}, {"agent:agent-only/observer"}, {"agent:multi-a/observer", node("child", "child-node"), node("multi-a", "multi-a-node"), node("multi-b", "multi-b-node")}, {}}
	sort.Strings(want[ordinal])
	got := make([]string, 0, len(routes))
	for _, route := range routes {
		if route.Recipient.IsAgent() {
			got = append(got, "agent:"+route.AgentIdentity.FlowInstance()+"/"+route.Recipient.ID())
		} else if route.Recipient.IsNode() {
			got = append(got, "node:"+route.Recipient.ID())
		} else {
			t.Fatalf("unexpected recipient: %+v", route)
		}
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, want[ordinal]) {
		t.Fatalf("ordinal %d exact recipients=%v want=%v", ordinal, got, want[ordinal])
	}
}

func mixedRouteKeys(t *testing.T, routes []events.DeliveryRoute) []string {
	t.Helper()
	keys := make([]string, 0, len(routes))
	for _, route := range routes {
		id, err := route.Identity()
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(route)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, events.EncodeDeliveryRouteIdentity(id)+":"+string(raw))
	}
	sort.Strings(keys)
	return keys
}

// Only observes acknowledgements returned by the real group; it cannot mint
// a disposition, successful SQL result or continuation owner.
type mixedExecutionGroup struct {
	pipelineobligation.PublicationGroup
	requests []pipelineobligation.PublicationSettlementMember
	results  []pipelineobligation.PublicationGroupOutcome
}

func (g *mixedExecutionGroup) Settle(ctx context.Context, members []pipelineobligation.PublicationSettlementMember) (pipelineobligation.PublicationGroupOutcome, error) {
	result, err := g.PublicationGroup.Settle(ctx, members)
	g.requests = append(g.requests, members...)
	g.results = append(g.results, result)
	return result, err
}

func (g *mixedExecutionGroup) assertAcknowledged(t *testing.T, claims []pipelineobligation.Claim) {
	t.Helper()
	if len(g.requests) != len(claims) {
		t.Fatalf("actual terminal handoff count=%d want=%d", len(g.requests), len(claims))
	}
	for i, request := range g.requests {
		if !reflect.DeepEqual(request.Claim, claims[i]) || request.Disposition.Kind() != pipelineobligation.DispositionAcknowledged || request.Disposition.ReasonCode() != "pipeline_persisted" {
			t.Fatalf("ordinal %d terminal disposition/claim: %+v", i, request)
		}
	}
	count := 0
	for _, result := range g.results {
		for _, member := range result.Results {
			count++
			if !member.Outcome.Committed() || !member.Outcome.DeliveryHandoffCommitted() {
				t.Fatalf("missing actual committed handoff: %+v", member)
			}
		}
	}
	if count != len(claims) {
		t.Fatalf("actual committed handoffs=%d want=%d", count, len(claims))
	}
}

func mixedAwaitDeliveries(t *testing.T, f *groupProofFixture, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var total, delivered int
		if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*), COALESCE(SUM(CASE WHEN status='delivered' THEN 1 ELSE 0 END),0) FROM event_deliveries WHERE run_id=$1 AND event_id<>$2`, f.seed.runID, f.seed.eventID).Scan(&total, &delivered); err != nil {
			t.Fatal(err)
		}
		if total == want && delivered == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("actual selected deliveries: total=%d delivered=%d want=%d", total, delivered, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type mixedRecordingAgent struct {
	id            string
	subscriptions []events.EventType
	seen          chan<- events.Event
	store         deliverylifecycle.Store
}

func (a *mixedRecordingAgent) ID() string                        { return a.id }
func (a *mixedRecordingAgent) Type() string                      { return "recording" }
func (a *mixedRecordingAgent) Subscriptions() []events.EventType { return a.subscriptions }
func (a *mixedRecordingAgent) OnEvent(ctx context.Context, event events.Event) ([]events.Event, error) {
	claim, ok := deliverylifecycle.ClaimFromContext(ctx)
	if !ok || claim.RunID() != event.RunID() || claim.SubscriberID() != a.id {
		return nil, errors.New("mixed agent executed without its exact delivery claim")
	}
	snapshot, err := a.store.Snapshot(ctx, claim.DeliveryID())
	if err != nil {
		return nil, err
	}
	if snapshot.EventID != event.ID() || snapshot.RunID != event.RunID() || snapshot.ClaimVersion != claim.Version() || snapshot.Terminal() {
		return nil, fmt.Errorf("mixed agent execution is not durably claim-owned: %+v", snapshot)
	}
	select {
	case a.seen <- event:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func newMixedExecutionFixture(t *testing.T, backend string, handlers *lifecycleprobe.Probe) (*groupProofFixture, <-chan events.Event, *pipeline.PipelineCoordinator) {
	t.Helper()
	f, seen, coordinator, _ := newMixedExecutionFixtureWithManager(t, backend, handlers)
	return f, seen, coordinator
}

func newMixedExecutionFixtureWithManager(t *testing.T, backend string, handlers *lifecycleprobe.Probe) (*groupProofFixture, <-chan events.Event, *pipeline.PipelineCoordinator, *manager.AgentManager) {
	t.Helper()
	root := canonicalrouting.CopyFanOutMixedAgentRoute(t)
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	repo := pipeline.WorkflowRepoRoot()
	bundle, err := contracts.LoadWorkflowContractBundleFromArtifact(repo, artifact, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	f := &groupProofFixture{postgres: backend == "postgres"}
	if f.postgres {
		_, f.db, _ = testutil.StartPostgres(t)
		f.raw = admitTestPostgresStore(t, f.db)
	} else {
		store := newBootstrappedSQLiteRuntimeStoreForTest(t)
		f.db, f.raw = store.backend.ConstructionHandle(), store
	}
	ctx, cancel := context.WithTimeout(testAuthorActivityContextForBundle(artifact.BundleHash()), 20*time.Second)
	t.Cleanup(cancel)
	f.seed = seedFanOutOwnerFixtureWithArtifact(t, ctx, f.db, f.raw, f.postgres, 4, time.Now().UTC().Truncate(time.Microsecond), artifact)
	runtimeID := authorActivityTestRuntimeInstanceID
	fact := mustStoreTestSourceArtifactFact(f.seed.bundleHash)
	ctx = correlation.WithSourceArtifactFact(correlation.WithRunID(ctx, f.seed.runID), fact)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runtimeID, f.seed.bundleHash))
	setup := pipeline.ScenarioSetupRequest{RunID: f.seed.runID, CreatedAt: f.seed.createdAt}
	for _, flow := range []string{"one", "multi-a", "multi-b", "agent-only"} {
		setup.Entities = append(setup.Entities, pipeline.ScenarioSetupEntityRequest{Alias: flow, EntityID: uuid.NewString(), FlowInstance: flow, EntityType: "test_entity", CurrentState: "active", Fields: map[string]any{}})
	}
	if _, err := f.raw.(interface {
		SetupScenarioEntities(context.Context, pipeline.ScenarioSetupRequest) (pipeline.ScenarioSetupResult, error)
	}).SetupScenarioEntities(ctx, setup); err != nil {
		t.Fatal(err)
	}
	process := worklifetime.NewProcess()
	var am *manager.AgentManager
	f.occurrence, err = process.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: runtimeID, BundleHash: f.seed.bundleHash})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		deadline, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if am != nil {
			if err := am.Shutdown(); err != nil {
				t.Error(err)
			}
		}
		if _, err := f.occurrence.RetireAndWait(deadline); err != nil {
			t.Error(err)
		}
		process.Retire()
		if _, err := process.Join(deadline); err != nil {
			t.Error(err)
		}
		if f.process != nil {
			if err := f.process.Release(deadline); err != nil {
				t.Error(err)
			}
		}
	})
	ctx = worklifetime.WithRuntimeOccurrence(ctx, f.occurrence)
	selected := f.raw.(storeTestDurableEventBusStore)
	authority, err := deliverylifecycle.NewNormalExecutionAuthority(fact, runtimeID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := selected.ActivateDeliveryAuthority(ctx, authority); err != nil {
		t.Fatal(err)
	}
	catalog, err := f.raw.(selectedFanOutMixedRouteOwner).RegisterAuthorActivityEventCatalog(authoractivity.BundleScope(runtimeID, f.seed.bundleHash), []authoractivity.EventDescriptor{
		{EventType: "producer/mixed.one", Disposition: authoractivity.StoryAuthored},
		{EventType: "producer/mixed.agent", Disposition: authoractivity.StoryAuthored},
		{EventType: "producer/mixed.multi", Disposition: authoractivity.StoryAuthored},
		{EventType: "producer/mixed.none", Disposition: authoractivity.StoryAuthored},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(catalog.Release)
	f.bus, err = newStoreTestEventBus(t, selected, bus.EventBusOptions{ContractBundle: source, SourceArtifactFact: fact, WorkOwner: f.occurrence, RuntimeInstanceID: runtimeID, DeliveryAuthority: authority})
	if err != nil {
		t.Fatal(err)
	}
	seen := make(chan events.Event, 16)
	am = manager.NewAgentManagerWithOptions(f.bus, func(cfg actors.AgentConfig) (manager.Agent, error) {
		subs := make([]events.EventType, len(cfg.Subscriptions))
		for i, value := range cfg.Subscriptions {
			subs[i] = events.EventType(value)
		}
		return &mixedRecordingAgent{id: cfg.ID, subscriptions: subs, seen: seen, store: selected}, nil
	}, manager.AgentManagerOptions{
		SourceArtifactFact: fact, SemanticSource: source, DeliveryStore: selected, ExecutionPosture: executionposture.Live,
		PersistenceRoles: manager.PersistenceRoles{AgentRoutes: f.bus, RouteInstaller: f.bus, RouteVerifier: f.bus, RouteRestorer: f.bus, RouteRetirer: f.bus, RouteRemover: f.bus, CreationPublisher: f.bus, DeliveryRuntime: f.bus, LifecycleState: f.raw.(manager.AgentLifecycleStateReader)},
		WorkOwner:        f.occurrence, ReceiverExecution: eventreceiver.NormalExecution(),
	}, f.raw.(manager.ManagerPersistence))
	f.bus.SetCommittedAgentReadinessFinalizer(bus.CommittedAgentReadinessFinalizerFunc(am.FinalizeCommittedAgentReadiness))
	coordinate := agenttopology.SourceCoordinate{BundleHash: f.seed.bundleHash}
	desired, err := am.CompileStaticTopologyDesiredAgents(source, coordinate)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{coordinate}, desired)
	if err != nil {
		t.Fatal(err)
	}
	f.process, err = f.raw.(interface {
		AcquireProcessCapability(context.Context, startupownership.AcquireRequest) (startupownership.ProcessCapability, error)
	}).AcquireProcessCapability(ctx, startupownership.AcquireRequest{OwnerID: "mixed-group", BootID: uuid.NewString(), RuntimeInstanceID: runtimeID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.process.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
		t.Fatal(err)
	}
	f.grant, err = f.process.IssueGenerationGrant(ctx, startupownership.GrantRequest{BundleHash: f.seed.bundleHash, RuntimeInstanceID: runtimeID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision})
	if err != nil {
		t.Fatal(err)
	}
	admission, err := agenttopology.StaticAdmission(plan.Revision, f.seed.bundleHash, agenttopology.LifetimeDurableManaged)
	if err != nil {
		t.Fatal(err)
	}
	if err := am.InstallStartupTopology(f.grant, admission, plan); err != nil {
		t.Fatal(err)
	}
	if err := am.ReconcileStaticTopologyForStartup(ctx, source); err != nil {
		t.Fatal(err)
	}
	if _, err := f.grant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := f.grant.AdmitExecution(ctx); err != nil {
		t.Fatal(err)
	}
	managed, err := managedexecution.New(managedexecution.KindNormalRuntime, runtimeID, 1, "", "mixed-group", f.seed.bundleHash, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx = managedexecution.WithAdmission(ctx, managed)
	if _, err := am.HydrateForStartup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := am.Run(ctx); err != nil {
		t.Fatal(err)
	}
	workflow := f.raw.(workflowTestSelectedStore)
	nodes, err := pipeline.LoadWorkflowNodes(source)
	if err != nil {
		t.Fatal(err)
	}
	coordinator := pipeline.NewPipelineCoordinatorWithOptions(f.bus, pipeline.PipelineCoordinatorOptions{
		Module: forkFanOutConsumerModule{runForkGateWorkflowModule{source: source}, nodes}, Persistence: pipeline.NewWorkflowPersistence(workflow), DeliveryStore: workflow,
		DeadLetters: workflow, PipelineObligations: workflow.PipelineObligations(), DecisionCards: workflow, ProposedEffects: workflow, HumanTasks: workflow,
		DecisionCardDraftExpiry: workflow, HumanTaskExpiry: workflow, DeliveryRuntime: f.bus, RunLifecycle: workflow,
		SourceArtifactFact: fact, ExecutionPosture: executionposture.Live, ReceiverExecution: eventreceiver.NormalExecution(), WorkOwner: f.occurrence,
		TestLifecycleProbe: handlers,
	})
	if coordinator == nil {
		t.Fatal("real mixed pipeline dependencies incomplete")
	}
	f.bus.SetInterceptors(coordinator)
	continuations, err := deliverycontinuation.New(selected, selected, authority, f.occurrence, f.bus, func(_ context.Context, err error) { t.Errorf("real mixed continuation: %v", err) })
	if err != nil {
		t.Fatal(err)
	}
	if err := f.bus.SetDeliveryContinuationOwner(continuations); err != nil {
		t.Fatal(err)
	}
	if err := continuations.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		deadline, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := continuations.Retire(deadline); err != nil {
			t.Error(err)
		}
	})
	route := events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: uuid.NewString()}.Normalized()
	routingSource, err := events.NewStaticFlowRoutingSource(route)
	if err != nil {
		t.Fatal(err)
	}
	producer, err := identity.AdmitExecutableNodeDeclaration("producer", "fan-out-producer")
	if err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := f.db.QueryRowContext(ctx, `SELECT capsule FROM fan_out_intents WHERE run_id=$1`, f.seed.runID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var capsule fanoutobligation.Capsule
	if err := json.Unmarshal(raw, &capsule); err != nil {
		t.Fatal(err)
	}
	capsule.NodeKey, capsule.ExecutionFlowID, capsule.EntityID = producer.Key(), "producer", route.EntityID
	capsule.Route, capsule.ProducerSource = flowidentity.StoredRoute("producer", "producer", "producer"), routingSource
	raw, err = json.Marshal(capsule)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(ctx, `UPDATE fan_out_intents SET capsule=$1 WHERE run_id=$2`, string(raw), f.seed.runID); err != nil {
		t.Fatal(err)
	}
	executor := &captureFanOutExecutor{turns: make(chan capturedFanOutTurn, 1), errors: make(chan error, 1), done: make(chan struct{})}
	workers := 1
	registration, err := startupownership.StartFanOutServing(ctx, f.grant, f.occurrence, &workers, executor)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registration.Close)
	select {
	case turn := <-executor.turns:
		f.owner = turn.owner
		registration.Close()
		<-executor.done
	case err := <-executor.errors:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	key := fanoutobligation.IntentKey{RunID: f.seed.runID, TriggeringDeliveryID: f.seed.deliveryID, ElementRef: contracts.FanOutElementRef{FlowPath: f.seed.flowPath, Family: "fan_out", SemanticPath: f.seed.semanticPath}}
	var found bool
	f.intent, f.claim, found, err = f.owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "mixed-group", BundleHash: f.seed.bundleHash, Candidate: &key, Now: time.Now().UTC(), Lease: time.Minute})
	if err != nil || !found {
		t.Fatalf("exact mixed claim found=%v err=%v", found, err)
	}
	input, err := f.owner.LoadFanOutEvaluation(ctx, f.claim)
	if err != nil {
		t.Fatal(err)
	}
	f.group, err = f.owner.BeginFanOutPublicationGroup(ctx, f.claim)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.group.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	f.ctx = deliverylifecycle.WithRoute(ctx, events.DeliveryRoute{Target: events.MustExistingEntityTarget(route)})
	f.command = pipeline.FanOutChunkCommand{Claim: f.claim, Now: time.Now().UTC()}
	for ordinal, eventType := range []events.EventType{"producer/mixed.one", "producer/mixed.agent", "producer/mixed.multi", "producer/mixed.none"} {
		projection, err := fanoutobligation.PrepareOrdinalEmission(f.intent, input.Trigger, ordinal)
		if err != nil {
			t.Fatal(err)
		}
		event, err := projection.NewEvent(events.EventFacts{ID: uuid.NewString(), Type: eventType, Producer: events.ProducerClaim{Type: events.EventProducerNode, ID: producer.Key()}, Payload: []byte(`{}`), ChainDepth: capsule.ChainDepth + 1, Envelope: events.EnvelopeForSourceRoute(events.EventEnvelope{}, route), RoutingSource: routingSource, CreatedAt: time.Now().UTC().Truncate(time.Microsecond)})
		if err != nil {
			t.Fatal(err)
		}
		f.events = append(f.events, event)
	}
	return f, seen, coordinator, am
}
