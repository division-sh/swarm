package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

// These are selected-store history proofs. The seed/grant/planner helpers are
// shared with the group-owner tests; no mock settlement or revision writer runs.
func TestFanOutPublicationGroupHistoryAtomicCutsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newFanOutGroupHistoryFixture(t, backend, 2)
			old := f.beginSnapshot(t)
			defer old.Rollback()
			before := readFanOutGroupHistory(t, f.ctx, old, f.runID)
			before.requireReceipts(t, before.revision, nil)
			for _, event := range f.events {
				if before.eventRevision(t, event.ID()) != before.revision {
					t.Fatal("group publications did not share their first inclusive revision")
				}
			}
			collector, restore, err := InstallTransactionProbeForTest(f.raw, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if err := f.bus.DispatchFanOutPublications(f.ctx, f.group, f.committed); err != nil {
				t.Fatal(err)
			}
			receipt := collector.Snapshot()
			if receipt.Total.WriteCommits != 1 || receipt.Total.Revision.Finalizations != 1 {
				t.Fatalf("one completed segment must have one physical write/finalizer: %+v", receipt)
			}
			if held := readFanOutGroupHistory(t, f.ctx, old, f.runID); !reflect.DeepEqual(held, before) {
				t.Fatal("pre-commit read snapshot observed part or all of the later segment")
			}
			after := f.snapshot(t)
			if after.revision != before.revision+1 || after.revisions != before.revisions+1 {
				t.Fatalf("segment revision cut: before=%d/%d after=%d/%d", before.revision, before.revisions, after.revision, after.revisions)
			}
			after.requireReceipts(t, before.revision, nil)
			after.requireReceipts(t, after.revision, f.eventIDs())
			f.requireLiveReceipts(t, f.eventIDs())
			for _, event := range f.events {
				f.requireForkPoint(t, event.ID(), before.revision)
			}
		})
	}
}

func TestFanOutPublicationGroupHistoryRollbackBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, fault := range []string{"last_member", "later_segment", "revision"} {
			t.Run(backend+"/"+fault, func(t *testing.T) {
				f := newFanOutGroupHistoryFixture(t, backend, 3)
				requests := f.requests
				var prior []string
				if fault == "later_segment" {
					result, err := f.group.Settle(f.ctx, requests[:1])
					if err != nil || len(result.Results) != 1 || !result.Results[0].Outcome.Committed() {
						t.Fatalf("prior segment: %+v %v", result, err)
					}
					prior = []string{f.events[0].ID()}
					requests = requests[1:]
				}
				before := f.snapshot(t)
				before.requireReceipts(t, before.revision, prior)
				remove := f.installRollbackFault(t, fault)
				result, err := f.group.Settle(f.ctx, requests)
				remove()
				if err == nil || !strings.Contains(err.Error(), "b19_after_member_writes") || len(result.Results) != 0 {
					t.Fatalf("expected real SQL rollback after preceding writes, without acknowledged results: %+v %v", result, err)
				}
				after := f.snapshot(t)
				if !reflect.DeepEqual(before, after) {
					t.Fatal("rolled-back segment changed committed revision/fact history")
				}
				f.requireLiveReceipts(t, prior)
				observed, err := f.group.ReadPublicationSettlement(f.ctx, f.requests)
				if err != nil || len(observed.Rows) != 3 {
					t.Fatalf("rollback observation: %+v %v", observed, err)
				}
				for i, row := range observed.Rows {
					want := pipelineobligation.PublicationSettlementPending
					if fault == "later_segment" && i == 0 {
						want = pipelineobligation.PublicationSettlementSatisfied
					}
					if row.State != want {
						t.Fatalf("member %d observation=%v want=%v", i, row.State, want)
					}
				}
				// A proven rollback retains these exact live claims. The later
				// successful segment must add one cut, not fabricated subrevisions.
				result, err = f.group.Settle(f.ctx, requests)
				if err != nil || len(result.Results) != len(requests) {
					t.Fatalf("exact retained segment: %+v %v", result, err)
				}
				complete := f.snapshot(t)
				if complete.revision != before.revision+1 || complete.revisions != before.revisions+1 {
					t.Fatal("recovered segment did not have exactly one revision")
				}
				complete.requireReceipts(t, before.revision, prior)
				complete.requireReceipts(t, complete.revision, f.eventIDs())
				f.requireLiveReceipts(t, f.eventIDs())
			})
		}
	}
}

func TestFanOutPublicationGroupHistoryNestedCutBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []string{"deferred", "direct_publish", "engine_preparation"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				f := newFanOutGroupHistoryFixture(t, backend, 2)
				before := f.snapshot(t)
				probe := &fanOutGroupHistoryNestedProbe{fixture: f, t: t, mode: mode}
				f.bus.SetInterceptors(probe)
				if err := f.bus.DispatchFanOutPublications(f.ctx, f.group, f.committed); err != nil {
					t.Fatal(err)
				}
				if probe.child.ID() == "" || probe.seenChild != 1 || probe.seenB != 1 {
					t.Fatalf("nested execution not observed exactly: B=%d C=%d", probe.seenB, probe.seenChild)
				}
				after := f.snapshot(t)
				cRevision := after.eventRevision(t, probe.child.ID())
				if cRevision != before.revision+2 {
					t.Fatalf("C first revision=%d, want A-flush then C at %d", cRevision, before.revision+2)
				}
				if after.revision != cRevision+2 {
					t.Fatalf("nested singleton C and final B segment cuts=%d, want %d", after.revision, cRevision+2)
				}
				after.requireReceipts(t, before.revision, nil)
				after.requireReceipts(t, before.revision+1, []string{f.events[0].ID()})
				after.requireReceipts(t, cRevision, []string{f.events[0].ID()})
				after.requireReceipts(t, cRevision+1, []string{f.events[0].ID(), probe.child.ID()})
				after.requireReceipts(t, after.revision, append(f.eventIDs(), probe.child.ID()))
				f.requireLiveReceipts(t, append(f.eventIDs(), probe.child.ID()))
				f.requireForkPoint(t, probe.child.ID(), cRevision)
				for _, event := range f.events {
					f.requireForkPoint(t, event.ID(), before.revision)
				}
				if probe.child.ParentEventID() != f.events[1].ID() || probe.child.RunID() != f.runID {
					t.Fatal("nested C lost exact parent/run identity")
				}
			})
		}
	}
}

func TestFanOutPublicationGroupHistoryIndependentWriterBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newFanOutGroupHistoryFixture(t, backend, 2)
			before := f.snapshot(t)
			probe := &fanOutGroupHistoryHoldProbe{eventID: f.events[1].ID(), reached: make(chan struct{}), release: make(chan struct{})}
			f.bus.SetInterceptors(probe)
			joined := make(chan struct{})
			var dispatchErr error
			go func() {
				defer close(joined)
				dispatchErr = f.bus.DispatchFanOutPublications(f.ctx, f.group, f.committed)
			}()
			defer func() {
				probe.unblock()
				<-joined
				if dispatchErr != nil {
					t.Errorf("group dispatch: %v", dispatchErr)
				}
			}()
			select {
			case <-probe.reached:
			case <-f.ctx.Done():
				t.Fatal("group never reached its held second callback")
			}
			if held := f.snapshot(t); !reflect.DeepEqual(held, before) {
				t.Fatal("collecting A before held B committed settlement prematurely")
			}
			child := eventtest.ChildForProducerWithRoutingSource(
				uuid.NewString(), "producer/mixed.none", eventtest.Producer(events.EventProducerNode, f.producer.Key()),
				"", []byte(`{}`), f.events[1].ChainDepth()+1, events.LineageFromEvent(f.events[1]),
				events.EnvelopeForSourceRoute(events.EventEnvelope{}, f.sourceRoute), f.source,
				f.events[0].CreatedAt().Add(-time.Hour),
			)
			// This independent operation has no enclosing dispatch collector.
			// Same-run causal identity cannot adopt the group's completed A.
			if err := f.bus.Publish(f.ctx, child); err != nil {
				t.Fatal(err)
			}
			independent := f.snapshot(t)
			cRevision := independent.eventRevision(t, child.ID())
			if cRevision != before.revision+1 || independent.revision != cRevision+1 {
				t.Fatalf("independent publication/settlement cuts: before=%d C=%d after=%d", before.revision, cRevision, independent.revision)
			}
			independent.requireReceipts(t, cRevision, nil)
			independent.requireReceipts(t, independent.revision, []string{child.ID()})
			f.requireLiveReceipts(t, []string{child.ID()})
			f.requireForkPoint(t, child.ID(), cRevision)
			probe.unblock()
			<-joined
			after := f.snapshot(t)
			if after.revision != independent.revision+1 || after.revisions != before.revisions+3 {
				t.Fatal("independent same-run writer split or replaced the group's one revision")
			}
			after.requireReceipts(t, cRevision, nil)
			after.requireReceipts(t, independent.revision, []string{child.ID()})
			after.requireReceipts(t, after.revision, append(f.eventIDs(), child.ID()))
			f.requireForkPoint(t, child.ID(), cRevision)
			f.requireLiveReceipts(t, append(f.eventIDs(), child.ID()))
		})
	}
}

func TestFanOutPublicationGroupHistoryMaterializedForkBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newDeclaredFanOutGroupHistoryFixture(t, backend, 2)
			published := f.snapshot(t)
			if err := f.bus.DispatchFanOutPublications(f.ctx, f.group, f.committed); err != nil {
				t.Fatal(err)
			}
			settled := f.snapshot(t)
			if settled.revision != published.revision+1 {
				t.Fatal("declared group did not settle in one revision")
			}
			store := f.raw.(runForkSelectedLifecycleStore)
			request := runfork.RunForkMaterializeRequest{SourceRunID: f.runID, At: f.events[0].ID(), OriginalLoopCarriage: originalCarriageForRun(t, f.raw, f.runID)}
			child, err := store.MaterializeRunFork(f.ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if child.SourceRunID != f.runID || child.ForkRunID == "" || child.ForkRunID == f.runID || child.ForkPoint.Revision != published.revision || child.MaterializedFanOutCount != 1 {
				t.Fatalf("fixed publication cut materialization: %+v", child)
			}
			var cursor, cardinality, claimGeneration, childEvents int
			var status string
			var claimOwner, lease sql.NullString
			if err := f.db.QueryRowContext(f.ctx, `SELECT cursor,cardinality,status,claim_owner,claim_generation,CAST(lease_expires_at AS TEXT) FROM fan_out_intents WHERE run_id=$1`, child.ForkRunID).Scan(&cursor, &cardinality, &status, &claimOwner, &claimGeneration, &lease); err != nil {
				t.Fatal(err)
			}
			if cursor != 2 || cardinality != 2 || status != "closed" || claimOwner.Valid || claimGeneration != 0 || lease.Valid {
				t.Fatalf("fork reopened inherited completed issuance: cursor=%d/%d status=%s claim=%v/%d/%v", cursor, cardinality, status, claimOwner, claimGeneration, lease)
			}
			rows, err := f.db.QueryContext(f.ctx, `SELECT ordinal,CAST(event_id AS TEXT),CAST(source_event_id AS TEXT) FROM fan_out_outcomes WHERE run_id=$1 ORDER BY ordinal`, child.ForkRunID)
			if err != nil {
				t.Fatal(err)
			}
			var inherited []string
			for rows.Next() {
				var ordinal int
				var owned sql.NullString
				var source string
				if err := rows.Scan(&ordinal, &owned, &source); err != nil {
					rows.Close()
					t.Fatal(err)
				}
				if ordinal != len(inherited) || owned.Valid {
					rows.Close()
					t.Fatal("fork duplicated or re-owned a source publication ordinal")
				}
				inherited = append(inherited, source)
			}
			rowErr := errors.Join(rows.Err(), rows.Close())
			if rowErr != nil || !reflect.DeepEqual(inherited, f.eventIDs()) {
				t.Fatalf("inclusive group prefix inheritance=%v want=%v err=%v", inherited, f.eventIDs(), rowErr)
			}
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='items.child'`, child.ForkRunID).Scan(&childEvents); err != nil || childEvents != 0 {
				t.Fatalf("fork replayed grouped source events: count=%d err=%v", childEvents, err)
			}
			if sourceAfter := f.snapshot(t); !reflect.DeepEqual(sourceAfter, settled) {
				t.Fatal("materialization rewrote source group settlement/history")
			}
			fixture := authorActivityReceiptFixture{db: f.db, store: f.raw.(authorActivityReceiptStore)}
			requireCompleteRunForkRevision(t, f.ctx, fixture, f.runID)
			requireCompleteRunForkRevision(t, f.ctx, fixture, child.ForkRunID)
			f.requireForkPoint(t, f.events[0].ID(), published.revision)
			f.requireLiveReceipts(t, f.eventIDs())
			repeated, err := store.MaterializeRunFork(f.ctx, request)
			if err != nil || repeated.ForkRunID != child.ForkRunID || repeated.ForkPoint.Revision != published.revision {
				t.Fatalf("exact materialization repeat: %+v err=%v", repeated, err)
			}
		})
	}
}

type fanOutGroupHistoryHoldProbe struct {
	eventID          string
	reached, release chan struct{}
	once             sync.Once
}

func (p *fanOutGroupHistoryHoldProbe) unblock() { p.once.Do(func() { close(p.release) }) }

func (p *fanOutGroupHistoryHoldProbe) Intercept(ctx context.Context, event events.Event) (bool, []events.Event, pipelineobligation.ExecutionOutcome, error) {
	if event.ID() == p.eventID {
		close(p.reached)
		select {
		case <-p.release:
		case <-ctx.Done():
			return true, nil, pipelineobligation.Continue(), ctx.Err()
		}
	}
	return true, nil, pipelineobligation.Continue(), nil
}

type fanOutGroupHistoryNestedProbe struct {
	fixture          *fanOutGroupHistoryFixture
	t                *testing.T
	child            events.Event
	mode             string
	seenB, seenChild int
}

func (p *fanOutGroupHistoryNestedProbe) Intercept(ctx context.Context, event events.Event) (bool, []events.Event, pipelineobligation.ExecutionOutcome, error) {
	f := p.fixture
	if event.ID() == f.events[1].ID() {
		p.seenB++
		p.child = eventtest.ChildForProducerWithRoutingSource(
			uuid.NewString(), "producer/mixed.none", eventtest.Producer(events.EventProducerNode, f.producer.Key()),
			"", []byte(`{}`), event.ChainDepth()+1, events.LineageFromEvent(event),
			events.EnvelopeForSourceRoute(events.EventEnvelope{}, f.sourceRoute), f.source,
			f.events[0].CreatedAt().Add(-time.Hour),
		)
		if p.mode != "deferred" {
			return true, nil, pipelineobligation.Continue(), f.publishNested(ctx, p.child, p.mode)
		}
		return true, []events.Event{p.child}, pipelineobligation.Continue(), nil
	}
	if p.child.ID() != "" && event.ID() == p.child.ID() {
		p.seenChild++
		atC := f.snapshot(p.t)
		atC.requireReceipts(p.t, atC.revision, []string{f.events[0].ID()})
		f.requireLiveReceipts(p.t, []string{f.events[0].ID()})
		f.requireForkPoint(p.t, event.ID(), atC.revision)
	}
	return true, nil, pipelineobligation.Continue(), nil
}

func (f *fanOutGroupHistoryFixture) publishNested(ctx context.Context, child events.Event, mode string) (err error) {
	if mode == "direct_publish" {
		return f.bus.Publish(ctx, child)
	}
	plans, err := f.bus.PrepareEnginePublications(ctx, []engine.EmitIntent{{Event: child}})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.bus.ReleaseEnginePublications(context.WithoutCancel(ctx), plans)) }()
	if len(plans) != 1 {
		return fmt.Errorf("nested engine requires one exact plan, got %d", len(plans))
	}
	plan := plans[0].(bus.EnginePublicationPlan)
	committed, err := f.raw.(interface {
		CommitPublication(context.Context, bus.PublicationCommand) (bus.CommittedPublication, error)
	}).CommitPublication(ctx, plan.PublicationCommand())
	if err != nil {
		return err
	}
	evidence, err := bus.NewCommittedEnginePublication(plan, committed)
	if err != nil {
		return err
	}
	if err := f.bus.FinalizeEnginePublications(ctx, []engine.CommittedDurablePublication{evidence}); err != nil {
		return err
	}
	return f.bus.EngineDispatcher().DispatchPostCommit(ctx, []engine.EmitIntent{evidence.CommittedDurablePublicationIntent()})
}

type fanOutGroupHistoryFixture struct {
	ctx          context.Context
	raw          selectedFanOutOwner
	db, observer *sql.DB
	postgres     bool
	runID        string
	group        pipelineobligation.PublicationGroup
	bus          *bus.EventBus
	committed    []engine.CommittedDurablePublication
	requests     []pipelineobligation.PublicationSettlementMember
	events       []events.Event
	producer     identity.ExecutableNode
	sourceRoute  events.RouteIdentity
	source       events.RoutingSource
}

func newFanOutGroupHistoryFixture(t *testing.T, backend string, count int) *fanOutGroupHistoryFixture {
	t.Helper()
	f, seed, bundle := seedFanOutGroupHistoryFixture(t, backend, count)
	finalizeFanOutGroupHistorySeed(t, f, seed)
	f = prepareFanOutGroupHistoryFixture(t, f, seed, bundle, count, "producer/mixed.none", func(int) []byte { return []byte(`{}`) })
	tx := f.beginSnapshot(t)
	defer tx.Rollback()
	if err := validateRunForkRevisionMatrix(f.ctx, tx, f.postgres, f.runID); err != nil {
		t.Fatalf("complete fixture history before dispatch: %v", err)
	}
	return f
}

func seedFanOutGroupHistoryFixture(t *testing.T, backend string, count int) (*fanOutGroupHistoryFixture, fanOutOwnerFixture, *contracts.WorkflowContractBundle) {
	t.Helper()
	ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 25*time.Second)
	t.Cleanup(cancel)
	raw, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
	artifact, bundle := fanOutMixedRouteSource(t)
	seed := seedFanOutOwnerFixtureWithArtifact(t, ctx, db, raw, postgres, count, time.Now().UTC().Truncate(time.Microsecond), artifact)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, seed.bundleHash))
	ctx = correlation.WithSourceArtifactFact(correlation.WithRunID(ctx, seed.runID), mustStoreTestSourceArtifactFact(seed.bundleHash))
	f := &fanOutGroupHistoryFixture{ctx: ctx, raw: raw, db: db, observer: db, postgres: postgres, runID: seed.runID,
		sourceRoute: events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: uuid.NewString()}.Normalized()}
	var err error
	f.source, err = events.NewStaticFlowRoutingSource(f.sourceRoute)
	if err != nil {
		t.Fatal(err)
	}
	f.producer, err = identity.AdmitExecutableNodeDeclaration("producer", "fan-out-producer")
	if err != nil {
		t.Fatal(err)
	}
	// This is the same initial capsule setup as the existing group fixture;
	// all publication, settlement and history writes below use real owners.
	var rawCapsule []byte
	if err := db.QueryRowContext(ctx, `SELECT capsule FROM fan_out_intents WHERE run_id=$1`, seed.runID).Scan(&rawCapsule); err != nil {
		t.Fatal(err)
	}
	var capsule fanoutobligation.Capsule
	if err := json.Unmarshal(rawCapsule, &capsule); err != nil {
		t.Fatal(err)
	}
	capsule.NodeKey, capsule.ExecutionFlowID, capsule.EntityID = f.producer.Key(), "producer", f.sourceRoute.EntityID
	capsule.Route, capsule.ProducerSource = flowidentity.StoredRoute("producer", "producer", "producer"), f.source
	rawCapsule, err = json.Marshal(capsule)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE fan_out_intents SET capsule=$1 WHERE run_id=$2`, string(rawCapsule), seed.runID); err != nil {
		t.Fatal(err)
	}
	return f, seed, bundle
}

func newDeclaredFanOutGroupHistoryFixture(t *testing.T, backend string, count int) *fanOutGroupHistoryFixture {
	t.Helper()
	raw, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
	ctx, seed := seedDeclaredForkFanOutFixture(t, backend, authorActivityReceiptFixture{db: db, store: raw.(authorActivityReceiptStore)}, count, time.Now().UTC().Truncate(time.Microsecond))
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	t.Cleanup(cancel)
	bundle, err := contracts.LoadWorkflowContractBundleFromArtifact(pipeline.WorkflowRepoRoot(), seed.artifact, contracts.DefaultPlatformSpecFile(pipeline.WorkflowRepoRoot()), contracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var capsuleBytes []byte
	if err := db.QueryRowContext(ctx, `SELECT capsule FROM fan_out_intents WHERE run_id=$1`, seed.runID).Scan(&capsuleBytes); err != nil {
		t.Fatal(err)
	}
	var capsule fanoutobligation.Capsule
	if err := json.Unmarshal(capsuleBytes, &capsule); err != nil {
		t.Fatal(err)
	}
	f := &fanOutGroupHistoryFixture{ctx: ctx, raw: raw, db: db, observer: db, postgres: postgres, runID: seed.runID,
		producer: mustPersistenceRootNode("fan-out-source"), source: capsule.ProducerSource,
		sourceRoute: capsule.ProducerSource.Route()}
	return prepareFanOutGroupHistoryFixture(t, f, seed, bundle, count, "items.child", func(ordinal int) []byte {
		return []byte(fmt.Sprintf(`{"value":"item-%03d"}`, ordinal))
	})
}

func prepareFanOutGroupHistoryFixture(t *testing.T, f *fanOutGroupHistoryFixture, seed fanOutOwnerFixture, bundle *contracts.WorkflowContractBundle, count int, eventType events.EventType, payload func(int) []byte) *fanOutGroupHistoryFixture {
	t.Helper()
	ctx, raw, db, postgres := f.ctx, f.raw, f.db, f.postgres
	owner, _, _, _ := grantedFanOutOwnerForTest(t, ctx, raw, seed)
	key := fanoutobligation.IntentKey{RunID: seed.runID, TriggeringDeliveryID: seed.deliveryID, ElementRef: contracts.FanOutElementRef{FlowPath: seed.flowPath, Family: "fan_out", SemanticPath: seed.semanticPath}}
	intent, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "b19-history", BundleHash: seed.bundleHash, Candidate: &key, Now: time.Now().UTC(), Lease: time.Minute})
	if err != nil || !found {
		t.Fatalf("claim: found=%v err=%v", found, err)
	}
	input, err := owner.LoadFanOutEvaluation(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	f.group, err = owner.BeginFanOutPublicationGroup(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.group.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	catalog, err := raw.(selectedFanOutMixedRouteOwner).RegisterAuthorActivityEventCatalog(authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, seed.bundleHash), []authoractivity.EventDescriptor{{EventType: string(eventType), Disposition: authoractivity.StoryAuthored}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(catalog.Release)
	f.bus, err = newStoreTestEventBus(t, raw.(storeTestDurableEventBusStore), bus.EventBusOptions{ContractBundle: semanticview.Wrap(bundle), SourceArtifactFact: mustStoreTestSourceArtifactFact(seed.bundleHash), RuntimeInstanceID: authorActivityTestRuntimeInstanceID})
	if err != nil {
		t.Fatal(err)
	}
	var route events.DeliveryRoute
	if receiver := intent.Request.Capsule.Receiver; receiver != nil {
		route.Recipient = events.MustNodeDeliveryRecipient(receiver.Node)
		route.Target = receiver.Target
	} else {
		route.Target = events.MustExistingEntityTarget(f.sourceRoute)
	}
	f.ctx = deliverylifecycle.WithRoute(ctx, route)
	var claims []pipelineobligation.Claim
	var plans []engine.DurablePublicationPlan
	var preparationRequests []pipeline.FanOutPublicationRequest
	command := pipeline.FanOutChunkCommand{Claim: claim, Now: time.Now().UTC()}
	for ordinal := 0; ordinal < count; ordinal++ {
		projection, err := fanoutobligation.PrepareOrdinalEmission(intent, input.Trigger, ordinal)
		if err != nil {
			t.Fatal(err)
		}
		event, err := projection.NewEvent(events.EventFacts{ID: uuid.NewString(), Type: eventType, Producer: events.ProducerClaim{Type: events.EventProducerNode, ID: f.producer.Key()}, Payload: payload(ordinal), ChainDepth: intent.Request.Capsule.ChainDepth + 1, Envelope: events.EnvelopeForSourceRoute(events.EventEnvelope{}, f.sourceRoute), RoutingSource: f.source, CreatedAt: time.Now().UTC().Truncate(time.Microsecond)})
		if err != nil {
			t.Fatal(err)
		}
		f.events = append(f.events, event)
		preparationRequests = append(preparationRequests, pipeline.FanOutPublicationRequest{Ordinal: ordinal, Intent: engine.EmitIntent{Event: event}})
	}
	prepared, err := f.bus.PrepareFanOutPublications(f.ctx, f.group, preparationRequests)
	if err != nil || len(prepared) != count {
		t.Fatalf("bounded group preparation: count=%d err=%v", len(prepared), err)
	}
	for ordinal, preparation := range prepared {
		if preparation.Ordinal != ordinal || preparation.Err != nil || preparation.Publication == nil {
			t.Fatalf("exact prepared ordinal %d: %+v", ordinal, preparation)
		}
		plan := preparation.Publication
		member := plan.(bus.EnginePublicationPlan).PublicationCommand().Commit.PipelineClaim
		claims = append(claims, member)
		plans = append(plans, plan)
		f.requests = append(f.requests, pipelineobligation.PublicationSettlementMember{Claim: member, Disposition: pipelineobligation.Acknowledged("pipeline_persisted")})
		command.Outcomes = append(command.Outcomes, pipeline.FanOutChunkOutcome{Ordinal: ordinal, Publication: plan})
	}
	if err := f.bus.SealFanOutPublications(f.ctx, f.group, count, plans); err != nil {
		t.Fatal(err)
	}
	committed, err := owner.CommitFanOutChunk(f.ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	f.committed = committed.Publications
	if err := f.group.ValidateCommittedMembership(claims); err != nil {
		t.Fatal(err)
	}
	if err := f.group.ValidateCommitted(f.ctx, claims); err != nil {
		t.Fatal(err)
	}
	if err := f.bus.FinalizeFanOutPublications(f.ctx, f.group, f.committed); err != nil {
		t.Fatal(err)
	}
	if !postgres {
		var seq int
		var name, path string
		if err := db.QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &path); err != nil {
			t.Fatal(err)
		}
		f.observer, err = sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.observer.Close() })
	}
	return f
}

type fanOutGroupHistoryFact struct {
	revision    int64
	family, key string
	present     bool
	json        string
}

type fanOutGroupHistorySnapshot struct {
	revision  int64
	revisions int
	facts     []fanOutGroupHistoryFact
}

func (f *fanOutGroupHistoryFixture) beginSnapshot(t *testing.T) *sql.Tx {
	t.Helper()
	opts := &sql.TxOptions{ReadOnly: true}
	if f.postgres {
		opts.Isolation = sql.LevelRepeatableRead
	}
	tx, err := f.observer.BeginTx(f.ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

func (f *fanOutGroupHistoryFixture) snapshot(t *testing.T) fanOutGroupHistorySnapshot {
	t.Helper()
	tx := f.beginSnapshot(t)
	defer tx.Rollback()
	return readFanOutGroupHistory(t, f.ctx, tx, f.runID)
}

func readFanOutGroupHistory(t *testing.T, ctx context.Context, tx *sql.Tx, runID string) fanOutGroupHistorySnapshot {
	t.Helper()
	var out fanOutGroupHistorySnapshot
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(revision),0),COUNT(*) FROM run_fork_revisions WHERE run_id=$1`, runID).Scan(&out.revision, &out.revisions); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.QueryContext(ctx, `SELECT revision,family,fact_key,present,CAST(fact AS TEXT) FROM run_fork_fact_revisions WHERE run_id=$1 ORDER BY revision,family,fact_key`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var fact fanOutGroupHistoryFact
		if err := rows.Scan(&fact.revision, &fact.family, &fact.key, &fact.present, &fact.json); err != nil {
			t.Fatal(err)
		}
		out.facts = append(out.facts, fact)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func (s fanOutGroupHistorySnapshot) requireReceipts(t *testing.T, cut int64, want []string) {
	t.Helper()
	// Independent test oracle over committed canonical facts, not a runtime
	// reader or a timestamp-derived reconstruction of per-member history.
	latest := map[string]fanOutGroupHistoryFact{}
	for _, fact := range s.facts {
		if fact.family == "event_receipts" && fact.revision <= cut {
			latest[fact.key] = fact
		}
	}
	got := []string{}
	for _, fact := range latest {
		if !fact.present {
			continue
		}
		var receipt struct {
			EventID        string `json:"event_id"`
			SubscriberType string `json:"subscriber_type"`
			SubscriberID   string `json:"subscriber_id"`
			Outcome        string `json:"outcome"`
		}
		if err := json.Unmarshal([]byte(fact.json), &receipt); err != nil {
			t.Fatal(err)
		}
		if receipt.SubscriberType == "platform" && receipt.SubscriberID == "pipeline" {
			if receipt.Outcome != "success" {
				t.Fatalf("non-success historical receipt: %s", fact.json)
			}
			got = append(got, receipt.EventID)
		}
	}
	want = append([]string{}, want...)
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("receipt cut R=%d: got=%v want=%v", cut, got, want)
	}
}

func (s fanOutGroupHistorySnapshot) eventRevision(t *testing.T, eventID string) int64 {
	t.Helper()
	for _, fact := range s.facts {
		if fact.family == "events" && fact.key == eventID && fact.present {
			return fact.revision
		}
	}
	t.Fatalf("no first persisted revision for event %s", eventID)
	return 0
}

func (f *fanOutGroupHistoryFixture) eventIDs() []string {
	ids := make([]string, len(f.events))
	for i, event := range f.events {
		ids[i] = event.ID()
	}
	return ids
}

func (f *fanOutGroupHistoryFixture) requireLiveReceipts(t *testing.T, want []string) {
	t.Helper()
	rows, err := f.observer.QueryContext(f.ctx, `SELECT CAST(r.event_id AS TEXT),r.outcome FROM event_receipts r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1 AND r.subscriber_type='platform' AND r.subscriber_id='pipeline' ORDER BY r.event_id`, f.runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := []string{}
	for rows.Next() {
		var id, outcome string
		if err := rows.Scan(&id, &outcome); err != nil {
			t.Fatal(err)
		}
		if outcome != "success" {
			t.Fatalf("non-success live receipt: %s/%s", id, outcome)
		}
		got = append(got, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want = append([]string{}, want...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("live receipts=%v want=%v", got, want)
	}
}

func (f *fanOutGroupHistoryFixture) requireForkPoint(t *testing.T, eventID string, revision int64) {
	t.Helper()
	plan, err := f.raw.(interface {
		PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
	}).PlanRunFork(f.ctx, runfork.RunForkPlanRequest{SourceRunID: f.runID, At: eventID})
	if err != nil {
		t.Fatal(err)
	}
	if plan.ForkPoint.EventID != eventID || plan.ForkPoint.Revision != revision {
		t.Fatalf("fixed event selected a different history cut: %+v want=%d", plan.ForkPoint, revision)
	}
	ids, ok := plan.HistoricalEventIDs(revision)
	if !ok {
		t.Fatal("fork planner did not retain its admitted historical event set")
	}
	for _, want := range f.eventIDs() {
		found := false
		for _, id := range ids {
			if id == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("inclusive group event %s omitted from fork projection", want)
		}
	}
}

func (f *fanOutGroupHistoryFixture) installRollbackFault(t *testing.T, kind string) func() {
	t.Helper()
	name := "b19_history_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	table, condition := "event_receipts", fmt.Sprintf("NEW.event_id='%s' AND NEW.subscriber_type='platform' AND NEW.subscriber_id='pipeline'", f.events[2].ID())
	if kind == "revision" {
		table, condition = "run_fork_revisions", fmt.Sprintf("NEW.run_id='%s'", f.runID)
	}
	// The trigger fires after the last member's insert (or revision insert),
	// and proves preceding member writes really occurred inside this transaction.
	prefix := fmt.Sprintf("(SELECT COUNT(*) FROM event_receipts WHERE event_id IN ('%s','%s') AND subscriber_type='platform' AND subscriber_id='pipeline')=2", f.events[0].ID(), f.events[1].ID())
	query := fmt.Sprintf("CREATE TRIGGER %s AFTER INSERT ON %s WHEN %s BEGIN SELECT CASE WHEN %s THEN RAISE(ABORT,'b19_after_member_writes') ELSE RAISE(ABORT,'b19_missing_prefix') END; END", name, table, condition, prefix)
	if f.postgres {
		function := fmt.Sprintf("CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF %s THEN RAISE EXCEPTION 'b19_after_member_writes'; ELSE RAISE EXCEPTION 'b19_missing_prefix'; END IF; END $$", name, prefix)
		if _, err := f.db.ExecContext(f.ctx, function); err != nil {
			t.Fatal(err)
		}
		query = fmt.Sprintf("CREATE TRIGGER %s AFTER INSERT ON %s FOR EACH ROW WHEN (%s) EXECUTE FUNCTION %s()", name, table, condition, name)
	}
	if _, err := f.db.ExecContext(f.ctx, query); err != nil {
		t.Fatal(err)
	}
	removed := false
	remove := func() {
		if removed {
			return
		}
		removed = true
		query := "DROP TRIGGER " + name
		if f.postgres {
			query += " ON " + table
		}
		if _, err := f.db.ExecContext(context.Background(), query); err != nil {
			t.Error(err)
		}
		if f.postgres {
			if _, err := f.db.ExecContext(context.Background(), "DROP FUNCTION "+name+"()"); err != nil {
				t.Error(err)
			}
		}
	}
	t.Cleanup(remove)
	return remove
}
