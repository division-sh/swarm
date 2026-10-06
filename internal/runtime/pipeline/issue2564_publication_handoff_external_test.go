package pipeline_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	swarmruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

type issue2564PublicationObserver struct {
	probe     *lifecycleprobe.Probe
	persisted func(context.Context, lifecycleprobe.Signal)
	started   func(context.Context, lifecycleprobe.Signal)
}

func (o *issue2564PublicationObserver) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	o.probe.NotifyLifecycle(ctx, signal)
	if signal.Kind == lifecycleprobe.EventPersisted && o.persisted != nil {
		o.persisted(ctx, signal)
	}
	if signal.Kind == lifecycleprobe.HandlerStarted && o.started != nil {
		o.started(ctx, signal)
	}
}

// Delay only the existing dispatch admission. Cancellation returns its own
// cause before any delivery is acquired; execution always delegates to the bus.
type issue2564PublicationDispatchGate struct {
	bus     *runtimebus.EventBus
	entered chan events.Event
	release <-chan struct{}
}

func (g *issue2564PublicationDispatchGate) DispatchDeliveryContinuation(ctx context.Context, event events.Event, route events.DeliveryRoute) deliverycontinuation.DispatchResult {
	select {
	case g.entered <- event:
	case <-ctx.Done():
		return deliverycontinuation.Fatal(ctx.Err())
	}
	select {
	case <-g.release:
		return g.bus.DispatchDeliveryContinuation(ctx, event, route)
	case <-ctx.Done():
		return deliverycontinuation.Fatal(ctx.Err())
	}
}

type issue2564PublicationFixture struct {
	store        timerReplaySelectedStore
	reopen       func() timerReplaySelectedStore
	backend      string
	runID        string
	source       semanticview.Source
	fact         correlation.SourceArtifactFact
	ctx          context.Context
	bus          *runtimebus.EventBus
	pc           *pipeline.PipelineCoordinator
	continuation *deliverycontinuation.Coordinator
	observer     *issue2564PublicationObserver
	failures     chan error
	stop         func()
}

func newIssue2564PublicationFixture(t *testing.T, backend string, observer *issue2564PublicationObserver, dispatchGate *issue2564PublicationDispatchGate) *issue2564PublicationFixture {
	t.Helper()
	bundle := loadPipelineLifecycleFixtureBundle(t, map[string]string{
		"schema.yaml": `name: issue2564-publication-handoff
stages:
  active: {initial: true}
pins:
  inputs: [bump, recurse]
  outputs: [child]
`,
		"entities.yaml": `work:
  count: {type: integer, initial: 0}
  parents: {type: integer, initial: 0}
  children: {type: integer, initial: 0}
`,
		"events.yaml": "bump:\n  token: integer\nrecurse:\n  token: integer\nchild:\n  token: integer\n",
		"nodes.yaml": `writer:
  execution_type: system_node
  event_handlers:
    bump:
      data_accumulation:
        writes: [{target_field: count, value: entity.count + 1}]
    recurse:
      data_accumulation:
        writes: [{target_field: parents, value: entity.parents + 1}]
      emit: {event: child, fields: {token: payload.token}}
child:
  execution_type: system_node
  event_handlers:
    child:
      data_accumulation:
        writes: [{target_field: children, value: entity.children + 1}]
`,
	})
	selected, _, reopen := openTimerReplayNativeStore(t, backend)
	f := &issue2564PublicationFixture{store: selected, reopen: reopen, backend: backend, runID: uuid.NewString(),
		source: semanticview.Wrap(bundle), fact: mustAuthorActivityTestSourceArtifactFactForHash(bundle.SourceArtifact.BundleHash()),
		observer: observer, failures: make(chan error, 16)}
	seedCtx := correlation.WithSourceArtifactFact(context.Background(), f.fact)
	seed := runlifecyclefixture.Fixture{RunID: f.runID, Origin: runlifecyclefixture.ScenarioSetupOrigin(), Artifact: bundle.SourceArtifact}
	if backend == "postgres" {
		runlifecyclefixture.RequirePostgres(t, seedCtx, storetest.DatabaseForTest(selected), seed)
	} else {
		runlifecyclefixture.RequireSQLite(t, seedCtx, storetest.DatabaseForTest(selected), seed)
	}
	f.start(t, 1, dispatchGate)
	commitKeylessConstructorComponent(t, f.ctx, f.selected(), f.pc, f.source)
	return f
}

func (f *issue2564PublicationFixture) selected() gateRecoveryStoreCase {
	return gateRecoveryStoreCase{name: f.backend, postgres: f.backend == "postgres", db: storetest.DatabaseForTest(f.store),
		events: f.store, cards: f.store, lifecycle: f.store, trace: f.store.(gateRecoveryTraceStore), persistence: pipeline.NewWorkflowPersistence(f.store)}
}

func (f *issue2564PublicationFixture) start(t *testing.T, generation uint64, gate *issue2564PublicationDispatchGate) {
	t.Helper()
	runtimeID := uuid.NewString()
	ctx := correlation.WithSourceArtifactFact(correlation.WithRunID(context.Background(), f.runID), f.fact)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runtimeID, f.fact.BundleHash()))
	process := worklifetime.NewProcess()
	work, err := process.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: runtimeID, BundleHash: f.fact.BundleHash()})
	if err != nil {
		t.Fatal(err)
	}
	f.ctx = withLiveGateExecution(worklifetime.WithOccurrence(ctx, work))
	admitter := swarmruntime.NewRuntimePayloadAdmitter(nil, f.source, f.fact)
	f.store.(swarmruntime.EventPayloadAdmissionBinder).SetEventPayloadAdmitter(admitter)
	authority, err := deliverylifecycle.NewNormalExecutionAuthority(f.fact, runtimeID, generation)
	if err != nil {
		t.Fatal(err)
	}
	f.bus, err = newScopedTestEventBus(t, f.store, runtimebus.EventBusOptions{
		ContractBundle: f.source, SourceArtifactFact: f.fact, RuntimeInstanceID: runtimeID, WorkOwner: work,
		PayloadAdmitter: admitter, DeliveryAuthority: authority, TestLifecycleProbe: f.observer,
	})
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := pipeline.LoadWorkflowNodes(f.source)
	if err != nil {
		t.Fatal(err)
	}
	f.pc = newGateRecoveryCoordinator(f.bus, f.selected(), pipeline.PipelineCoordinatorOptions{
		Module: proposedEffectProofModule{source: f.source, nodes: nodes}, SourceArtifactFact: f.fact,
		WorkOwner: work, TestLifecycleProbe: f.observer,
	})
	if f.pc == nil {
		t.Fatal("real publication coordinator rejected its semantic owners")
	}
	f.bus.SetInterceptors(f.pc)
	if err := f.store.ActivateDeliveryAuthority(f.ctx, authority); err != nil {
		t.Fatal(err)
	}
	var dispatcher deliverycontinuation.Dispatcher = f.bus
	if gate != nil {
		gate.bus = f.bus
		dispatcher = gate
	}
	f.continuation, err = deliverycontinuation.New(f.store, f.store, authority, work, dispatcher, func(_ context.Context, err error) {
		f.failures <- err
	})
	if err != nil {
		t.Fatal(err)
	}
	bus, continuation := f.bus, f.continuation
	var stop sync.Once
	f.stop = func() {
		stop.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := continuation.Retire(ctx); err != nil {
				t.Error(err)
			}
			if err := bus.WaitForQuiescence(ctx); err != nil {
				t.Error(err)
			}
			if _, err := work.RetireAndWait(ctx); err != nil {
				t.Error(err)
			}
			process.Retire()
			if _, err := process.Join(ctx); err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(f.stop)
	if err := bus.SetDeliveryContinuationOwner(continuation); err != nil {
		t.Fatal(err)
	}
	if err := continuation.Start(f.ctx); err != nil {
		t.Fatal(err)
	}
}

func (f *issue2564PublicationFixture) event(name string, token int) events.Event {
	return eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(name), "operator", "",
		[]byte(fmt.Sprintf(`{"token":%d}`, token)), 0, f.runID, events.EnvelopeForEntityID(events.EventEnvelope{}, f.runID),
		eventtest.RootRoutingSource(f.runID), time.Now().UTC())
}

func (f *issue2564PublicationFixture) exactRoute(t *testing.T, eventID string) events.DeliveryRoute {
	t.Helper()
	prepared, found, err := f.store.LoadPreparedPublishEvent(f.ctx, eventID)
	if err != nil || !found || len(prepared.DeliveryRoutes) != 1 || !prepared.DeliveryRoutes[0].Recipient.IsNode() {
		t.Fatalf("exact admitted node publication %s: found=%t routes=%+v err=%v", eventID, found, prepared.DeliveryRoutes, err)
	}
	return prepared.DeliveryRoutes[0]
}

func (f *issue2564PublicationFixture) snapshot(t *testing.T, eventID string) deliverylifecycle.Snapshot {
	t.Helper()
	id, err := deliverylifecycle.DeliveryID(eventID, f.exactRoute(t, eventID))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := f.store.Snapshot(f.ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func (f *issue2564PublicationFixture) assertHandoff(t *testing.T, eventID string, want int) {
	t.Helper()
	var count int
	if err := storetest.DatabaseForTest(f.store).QueryRowContext(f.ctx,
		`SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1 AND continuation_handoff_at IS NOT NULL`, eventID).Scan(&count); err != nil || count != want {
		t.Fatalf("persisted handoff for exact event %s=%d want=%d err=%v", eventID, count, want, err)
	}
}

func (f *issue2564PublicationFixture) assertReleased(t *testing.T, eventID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	work, err := f.store.PipelineObligations().ClaimEvent(ctx, eventID, pipelineobligation.PurposeRecovery)
	if err == nil {
		_ = f.store.PipelineObligations().Release(ctx, work.Claim)
	}
	if !errors.Is(err, pipelineobligation.ErrIneligible) {
		t.Fatalf("exact publication %s retained or left unfinished its native claim AFTER acknowledgment: %v", eventID, err)
	}
	var count int
	var outcome, reason string
	if err := storetest.DatabaseForTest(f.store).QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MAX(outcome),''),COALESCE(MAX(reason_code),'')
		FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, eventID).Scan(&count, &outcome, &reason); err != nil || count != 1 || outcome != "success" || reason != "pipeline_persisted" {
		t.Fatalf("exact pipeline acknowledgement %s=%d/%s/%s err=%v", eventID, count, outcome, reason, err)
	}
}

func (f *issue2564PublicationFixture) assertCounters(t *testing.T, count, parents, children int) {
	t.Helper()
	instance, found, err := f.pc.Load(f.ctx, testRunScopedWorkflowInstanceForRun(f.runID, f.runID))
	if err != nil || !found {
		t.Fatalf("load actual constructed entity: found=%t err=%v", found, err)
	}
	raw, err := json.Marshal(instance.Fields)
	if err != nil {
		t.Fatal(err)
	}
	var got struct{ Count, Parents, Children int }
	if err := json.Unmarshal(raw, &got); err != nil || got.Count != count || got.Parents != parents || got.Children != children {
		t.Fatalf("persisted counters=%s err=%v want=%d/%d/%d", raw, err, count, parents, children)
	}
}

func (f *issue2564PublicationFixture) assertDelivered(t *testing.T, eventID string) {
	t.Helper()
	snapshot := f.snapshot(t, eventID)
	outcomes, err := f.store.Outcomes(f.ctx, snapshot.DeliveryID)
	if err != nil || snapshot.Status != deliverylifecycle.StatusDelivered || snapshot.RetryCount != 0 || len(outcomes) != 1 {
		t.Fatalf("exact node delivery %s: snapshot=%+v outcomes=%+v err=%v", eventID, snapshot, outcomes, err)
	}
	f.assertReleased(t, eventID)
}

func TestIssue2564OrdinaryAcknowledgedNodeHandoffBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			persisted, releasePublication := make(chan lifecycleprobe.Signal, 1), make(chan struct{})
			var first sync.Once
			observer := &issue2564PublicationObserver{probe: lifecycleprobe.New(), persisted: func(ctx context.Context, signal lifecycleprobe.Signal) {
				if signal.EventType == "bump" {
					first.Do(func() {
						persisted <- signal
						select {
						case <-releasePublication:
						case <-ctx.Done():
						}
					})
				}
			}}
			releasePersisted := sync.OnceFunc(func() { close(releasePublication) })
			t.Cleanup(releasePersisted)
			releaseExecution := make(chan struct{})
			gate := &issue2564PublicationDispatchGate{entered: make(chan events.Event, 64), release: releaseExecution}
			f := newIssue2564PublicationFixture(t, backend, observer, gate)
			t.Cleanup(releasePersisted)
			// Forty exact nodes exceed the existing 32-job continuation projection.
			const backlog = 40
			accepted := []events.Event{f.event("bump", 0)}
			published := make(chan error, 1)
			go func() { published <- f.bus.PublishAcknowledged(f.ctx, accepted[0]) }()
			select {
			case signal := <-persisted:
				if signal.EventID != accepted[0].ID() {
					t.Fatal("publication barrier observed a different event")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			authority, err := f.bus.DeliveryAuthority()
			if err != nil {
				t.Fatal(err)
			}
			before := f.snapshot(t, accepted[0].ID())
			page, err := f.store.ScanDeliveryContinuations(f.ctx, authority, deliverylifecycle.ContinuationCursor{}, 100)
			if err != nil || !page.Exhausted || len(page.Items) != 0 || before.Status != deliverylifecycle.StatusPending || before.ClaimVersion != 0 {
				t.Fatalf("pre-handoff durable node became executable: snapshot=%+v page=%+v err=%v", before, page, err)
			}
			f.assertHandoff(t, accepted[0].ID(), 0)
			f.assertCounters(t, 0, 0, 0)
			releasePersisted()
			select {
			case err := <-published:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			for i := 1; i < backlog; i++ {
				event := f.event("bump", i)
				if err := f.bus.PublishAcknowledged(f.ctx, event); err != nil {
					t.Fatal(err)
				}
				accepted = append(accepted, event)
			}
			select {
			case event := <-gate.entered:
				if event.RunID() != f.runID {
					t.Fatal("continuation dispatcher escaped the exact run")
				}
			case <-ctx.Done():
				t.Fatal("acknowledged publication never reached the real continuation owner")
			}
			deadline := time.NewTicker(time.Millisecond)
			defer deadline.Stop()
			for {
				page, err = f.store.ScanDeliveryContinuations(f.ctx, authority, deliverylifecycle.ContinuationCursor{}, 100)
				if err != nil {
					t.Fatal(err)
				}
				if page.Exhausted && len(page.Items) == backlog {
					break
				}
				select {
				case <-deadline.C:
				case <-ctx.Done():
					t.Fatalf("durable handoff backlog=%d want=%d: %v", len(page.Items), backlog, ctx.Err())
				}
			}
			remaining := make(map[string]bool, backlog)
			for _, event := range accepted {
				remaining[event.ID()] = true
				f.assertReleased(t, event.ID())
				f.assertHandoff(t, event.ID(), 1)
			}
			for _, item := range page.Items {
				if !remaining[item.Event.ID()] || item.Disposition != deliverylifecycle.ClaimAcquired || item.Snapshot.Status != deliverylifecycle.StatusPending || item.Snapshot.ClaimVersion != 0 {
					t.Fatalf("handoff changed identity or admitted an attempt before execution: %+v", item)
				}
				delete(remaining, item.Event.ID())
			}
			if len(remaining) != 0 {
				t.Fatal("durable continuation page omitted accepted event identities")
			}
			f.assertCounters(t, 0, 0, 0)
			stats := storetest.DatabaseForTest(f.store).Stats()
			t.Logf("blocked acknowledged nodes=%d pool_in_use=%d pool_idle=%d pool_open=%d pool_max=%d", backlog, stats.InUse, stats.Idle, stats.OpenConnections, stats.MaxOpenConnections)
			// Cancel/join only unacquired dispatcher jobs, then genuinely close the
			// selected store. No extra event wakes the reconstructed generation.
			f.stop()
			if err := f.store.Close(); err != nil {
				t.Fatal(err)
			}
			f.store = f.reopen()
			f.start(t, 2, nil)
			writer := externalPipelineSourceNode(t, f.source, ".", "writer")
			for _, event := range accepted {
				if _, err := observer.probe.WaitForHandlerCompleted(ctx, event.ID(), writer.Key()); err != nil {
					t.Fatal(err)
				}
				f.assertDelivered(t, event.ID())
			}
			f.assertCounters(t, backlog, 0, 0)
			if err := f.bus.PublishAcknowledged(f.ctx, accepted[0]); err != nil {
				t.Fatal(err)
			}
			f.assertCounters(t, backlog, 0, 0)
			f.assertDelivered(t, accepted[0].ID())
			select {
			case err := <-f.failures:
				t.Fatal(err)
			default:
			}
		})
	}
}

func TestIssue2564OrdinaryNodeHandoffRecursiveProgressBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			childEntered, releaseChild := make(chan lifecycleprobe.Signal, 1), make(chan struct{})
			release := sync.OnceFunc(func() { close(releaseChild) })
			releaseParent := make(chan struct{})
			releaseOwnedParent := sync.OnceFunc(func() { close(releaseParent) })
			nestedPublished := make(chan error, 1)
			var f *issue2564PublicationFixture
			var firstChild sync.Once
			observer := &issue2564PublicationObserver{probe: lifecycleprobe.New(), started: func(ctx context.Context, signal lifecycleprobe.Signal) {
				if signal.EventType == "recurse" {
					parent, found := correlation.InboundEventFromContext(ctx)
					claim, claimed := deliverylifecycle.ClaimFromContext(ctx)
					route, routed := deliverylifecycle.RouteFromContext(ctx)
					id, identityErr := deliverylifecycle.DeliveryID(parent.ID(), route)
					if !found || !claimed || !routed || identityErr != nil || claim.DeliveryID() != id || parent.ID() != signal.EventID {
						nestedPublished <- errors.New("recursive publication lacks the actual owned parent carrier")
						return
					}
					writer := externalPipelineSourceNode(t, f.source, ".", "writer")
					child := eventtest.ChildForProducerWithRoutingSource(uuid.NewString(), "child",
						eventtest.Producer(events.EventProducerNode, writer.Key()), "", []byte(`{"token":730}`), parent.ChainDepth()+1,
						events.LineageFromEvent(parent), parent.Envelope(), parent.RoutingSource(), time.Now().UTC())
					// This selected-owner barrier exercises a real nested publication
					// before the canonical handler mutation settles its parent delivery.
					nestedPublished <- f.bus.PublishAcknowledged(ctx, child)
					select {
					case <-releaseParent:
					case <-ctx.Done():
					}
				}
				if signal.EventType == "child" {
					firstChild.Do(func() {
						childEntered <- signal
						select {
						case <-releaseChild:
						case <-ctx.Done():
						}
					})
				}
			}}
			f = newIssue2564PublicationFixture(t, backend, observer, nil)
			t.Cleanup(releaseOwnedParent)
			t.Cleanup(release)
			parent := f.event("recurse", 73)
			if err := f.bus.PublishAcknowledged(f.ctx, parent); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-nestedPublished:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("owned parent could not acknowledge a real nested publication")
			}
			var child lifecycleprobe.Signal
			select {
			case child = <-childEntered:
			case <-ctx.Done():
				t.Fatal("owned parent failed to publish/reenter its actual child node")
			}
			parentState, childState := f.snapshot(t, parent.ID()), f.snapshot(t, child.EventID)
			if parentState.Status != deliverylifecycle.StatusInProgress || parentState.ClaimVersion == 0 || childState.Status != deliverylifecycle.StatusInProgress {
				t.Fatalf("recursive progress did not retain the actual parent and child claims: parent=%+v child=%+v", parentState, childState)
			}
			prepared, found, err := f.store.LoadPreparedPublishEvent(f.ctx, child.EventID)
			if err != nil || !found || prepared.Event.Event().ParentEventID() != parent.ID() {
				t.Fatalf("child lost exact committed parent lineage: found=%t err=%v", found, err)
			}
			f.assertReleased(t, parent.ID())
			f.assertReleased(t, child.EventID)
			f.assertCounters(t, 0, 0, 0)
			release()
			childNode := externalPipelineSourceNode(t, f.source, ".", "child")
			if _, err := observer.probe.WaitForHandlerCompleted(ctx, child.EventID, childNode.Key()); err != nil {
				t.Fatal(err)
			}
			f.assertDelivered(t, child.EventID)
			// The same-entity child and a sibling both execute while the original
			// parent still owns its exact uncommitted delivery attempt.
			sibling := f.event("bump", 74)
			if err := f.bus.PublishAcknowledged(f.ctx, sibling); err != nil {
				t.Fatal(err)
			}
			writer := externalPipelineSourceNode(t, f.source, ".", "writer")
			if _, err := observer.probe.WaitForHandlerCompleted(ctx, sibling.ID(), writer.Key()); err != nil {
				t.Fatal(err)
			}
			f.assertDelivered(t, sibling.ID())
			f.assertCounters(t, 1, 0, 1)
			if current := f.snapshot(t, parent.ID()); current.Status != deliverylifecycle.StatusInProgress || current.ClaimVersion != parentState.ClaimVersion {
				t.Fatalf("recursive progress released or replaced its owned parent: %+v", current)
			}
			releaseOwnedParent()
			if _, err := observer.probe.WaitForHandlerCompleted(ctx, parent.ID(), writer.Key()); err != nil {
				t.Fatal(err)
			}
			f.assertDelivered(t, parent.ID())
			f.assertDelivered(t, child.EventID)
			if err := f.bus.WaitForQuiescence(ctx); err != nil {
				t.Fatal(err)
			}
			f.assertCounters(t, 1, 1, 2)
			var children int
			if err := storetest.DatabaseForTest(f.store).QueryRowContext(f.ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND source_event_id=$2`, f.runID, parent.ID()).Scan(&children); err != nil || children != 2 {
				t.Fatalf("actual recursive children (nested plus compiled emit)=%d want=2 err=%v", children, err)
			}
			if err := f.bus.PublishAcknowledged(f.ctx, parent); err != nil {
				t.Fatal(err)
			}
			f.assertCounters(t, 1, 1, 2)
			f.assertDelivered(t, parent.ID())
			select {
			case err := <-f.failures:
				t.Fatal(err)
			default:
			}
		})
	}
}
