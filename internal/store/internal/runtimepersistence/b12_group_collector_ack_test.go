package runtimepersistence

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
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
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

// An actual Deferred second member forces the collector to flush its completed
// predecessor. Uncertain readback cannot authorize that Deferred mutation.
func TestB12GroupCollectorDoesNotRetryUncertainFlushBeforeDeferred(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, phase := range []string{"healthy", "lost_after_commit", "lost_after_rollback"} {
			t.Run(backend+"/"+phase, func(t *testing.T) {
				f, connector := newB12CollectorFixture(t, backend)
				probe := &b12DeferredInterceptor{second: f.events[1].ID(), retryAt: time.Now().UTC().Add(time.Hour), calls: map[string]int{}}
				f.bus.SetInterceptors(probe)
				signals := &b12ContinuationSignals{DeliveryContinuationOwner: f.bus.DeliveryContinuationOwner()}
				if err := f.bus.SetDeliveryContinuationOwner(signals); err != nil {
					t.Fatal(err)
				}
				lost := errors.New("b12_collector_commit_ack_lost")
				observed := &b12ObservedGroup{PublicationGroup: f.group}
				if phase != "healthy" {
					observed.beforeSettle = func() {
						connector.arm(func(tx driver.Tx) error {
							if phase == "lost_after_commit" {
								return errors.Join(lost, tx.Commit())
							}
							return errors.Join(lost, tx.Rollback())
						})
					}
				}
				sink := &b10CandidateSink{runID: f.runID}
				scope, _ := authoractivity.ScopeFromContext(f.ctx)
				registration, err := f.raw.(runlifecycle.CandidateRegistrar).RegisterCompletionCandidateSink(f.ctx, runlifecycle.CandidateScope{BundleHash: scope.BundleHash}, sink)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(registration.Release)
				collector, restore, err := InstallTransactionProbeForTest(f.raw, transactiontest.Options{})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(restore)
				revisions := countP16RunRevisions(t, f.db, f.runID)
				err = f.bus.DispatchFanOutPublications(f.ctx, observed, f.committed)
				if phase == "healthy" {
					if err != nil || len(observed.outcome.Results) != 1 || !observed.outcome.Results[0].Outcome.Committed() || !observed.outcome.Results[0].Outcome.DeliveryHandoffCommitted() {
						t.Fatalf("healthy flush failed: outcome=%+v err=%v", observed.outcome, err)
					}
				} else if !errors.Is(err, lost) || len(observed.outcome.Results) != 0 {
					t.Fatalf("collector forged acknowledgement: %+v err=%v", observed.outcome, err)
				}
				if observed.settles != 1 || probe.calls[f.events[0].ID()] != 1 || probe.calls[f.events[1].ID()] != 1 {
					t.Fatalf("collector blindly retried: settlements=%d intercepts=%v", observed.settles, probe.calls)
				}
				var attempts int
				var status string
				if err := f.db.QueryRow(`SELECT status,attempt_count FROM decision_card_route_obligations WHERE event_id=$1`, f.events[1].ID()).Scan(&status, &attempts); err != nil {
					t.Fatal(err)
				}
				wantAttempts, wantSignals, wantRevisions := 0, 0, revisions
				if phase == "healthy" {
					// Deferred changes retry metadata, not a terminal receipt or
					// delivery fact. Its real singleton COMMIT adds no fork cut.
					wantAttempts, wantSignals, wantRevisions = 1, 1, revisions+1
				}
				if phase == "lost_after_commit" {
					wantRevisions++
				}
				if status != "pending" || attempts != wantAttempts || signals.signals != wantSignals || countP16RunRevisions(t, f.db, f.runID) != wantRevisions {
					t.Fatalf("Deferred/continuation/revision authority: status=%s attempts=%d signals=%d revisions=%d want=%d/%d/%d", status, attempts, signals.signals, countP16RunRevisions(t, f.db, f.runID), wantAttempts, wantSignals, wantRevisions)
				}
				counts := collector.Snapshot().ByOperation[transactiontest.PipelineSettlement]
				if phase == "healthy" {
					if counts.WriteCommits != 2 || sink.submits != 2 || observed.reads != 0 {
						t.Fatalf("healthy group then singleton: counts=%+v sink=%+v reads=%d", counts, sink, observed.reads)
					}
				} else {
					want := pipelineobligation.PublicationSettlementPending
					if phase == "lost_after_commit" {
						want = pipelineobligation.PublicationSettlementSatisfied
					}
					if observed.reads != 1 || len(observed.observation.Rows) != 1 || observed.observation.Rows[0].State != want || counts.CommitAttempts != 1 || counts.CommitFailures != 1 || counts.WriteCommits != 0 || sink.submits != 0 {
						t.Fatalf("uncertain collector observation/attempt/candidate: observed=%+v counts=%+v sink=%+v", observed, counts, sink)
					}
				}
			})
		}
	}
}

type b12DeferredInterceptor struct {
	second  string
	retryAt time.Time
	calls   map[string]int
}

func (p *b12DeferredInterceptor) Intercept(_ context.Context, event events.Event) (bool, []events.Event, pipelineobligation.ExecutionOutcome, error) {
	p.calls[event.ID()]++
	if event.ID() == p.second {
		return true, nil, pipelineobligation.DeferExecution("b12_deferred", p.retryAt, nil), nil
	}
	return true, nil, pipelineobligation.Continue(), nil
}

type b12ContinuationSignals struct {
	bus.DeliveryContinuationOwner
	signals int
}

func (s *b12ContinuationSignals) Signal() { s.signals++; s.DeliveryContinuationOwner.Signal() }

// Observation-only decorator. Every operation and result is the actual owner's;
// only the test driver changes the physical COMMIT acknowledgement boundary.
type b12ObservedGroup struct {
	pipelineobligation.PublicationGroup
	beforeSettle   func()
	settles, reads int
	outcome        pipelineobligation.PublicationGroupOutcome
	observation    pipelineobligation.PublicationSettlementSnapshot
}

func (g *b12ObservedGroup) Settle(ctx context.Context, members []pipelineobligation.PublicationSettlementMember) (pipelineobligation.PublicationGroupOutcome, error) {
	g.settles++
	if g.beforeSettle != nil {
		g.beforeSettle()
	}
	out, err := g.PublicationGroup.Settle(ctx, members)
	g.outcome = out
	return out, err
}
func (g *b12ObservedGroup) ReadPublicationSettlement(ctx context.Context, members []pipelineobligation.PublicationSettlementMember) (pipelineobligation.PublicationSettlementSnapshot, error) {
	g.reads++
	out, err := g.PublicationGroup.ReadPublicationSettlement(ctx, members)
	g.observation = out
	return out, err
}

// Mirrors the existing admitted history fixture, changing only its connector
// to expose the physical COMMIT boundary. No changes to Bernoulli's fixture.
func newB12CollectorFixture(t *testing.T, backend string) (*fanOutGroupHistoryFixture, *stopCommitConnector) {
	t.Helper()
	return newCollectorFixtureWithCardinality(t, backend, 2)
}

func newCollectorFixtureWithCardinality(t *testing.T, backend string, cardinality int) (*fanOutGroupHistoryFixture, *stopCommitConnector) {
	t.Helper()
	if cardinality < 2 || cardinality > fanoutobligation.MaxChunkSize {
		t.Fatalf("collector fixture cardinality %d outside 2..%d", cardinality, fanoutobligation.MaxChunkSize)
	}
	raw, db, connector := newP16RaceStore(t, backend)
	artifact, bundle := fanOutMixedRouteSource(t)
	ctx, cancel := context.WithTimeout(testAuthorActivityContextForBundle(artifact.BundleHash()), 15*time.Second)
	t.Cleanup(cancel)
	seed := seedFanOutOwnerFixtureWithArtifact(t, ctx, db, raw, backend == "postgres", cardinality, time.Now().UTC(), artifact)
	ctx = correlation.WithSourceArtifactFact(correlation.WithRunID(ctx, seed.runID), mustStoreTestSourceArtifactFact(seed.bundleHash))
	f := &fanOutGroupHistoryFixture{ctx: ctx, raw: raw, db: db, observer: db, postgres: backend == "postgres", runID: seed.runID, sourceRoute: events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: uuid.NewString()}.Normalized()}
	var err error
	f.source, err = events.NewStaticFlowRoutingSource(f.sourceRoute)
	if err != nil {
		t.Fatal(err)
	}
	f.producer, err = identity.AdmitExecutableNodeDeclaration("producer", "fan-out-producer")
	if err != nil {
		t.Fatal(err)
	}
	var capsuleBytes []byte
	if err := db.QueryRow(`SELECT capsule FROM fan_out_intents WHERE run_id=$1`, seed.runID).Scan(&capsuleBytes); err != nil {
		t.Fatal(err)
	}
	var capsule fanoutobligation.Capsule
	if err := json.Unmarshal(capsuleBytes, &capsule); err != nil {
		t.Fatal(err)
	}
	capsule.NodeKey, capsule.ExecutionFlowID, capsule.EntityID = f.producer.Key(), "producer", f.sourceRoute.EntityID
	capsule.Route, capsule.ProducerSource = flowidentity.StoredRoute("producer", "producer", "producer"), f.source
	capsuleBytes, err = json.Marshal(capsule)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE fan_out_intents SET capsule=$1 WHERE run_id=$2`, string(capsuleBytes), seed.runID); err != nil {
		t.Fatal(err)
	}
	owner, _, _, _ := grantedFanOutOwnerForTest(t, ctx, raw, seed)
	key := fanoutobligation.IntentKey{RunID: seed.runID, TriggeringDeliveryID: seed.deliveryID, ElementRef: contracts.FanOutElementRef{FlowPath: seed.flowPath, Family: "fan_out", SemanticPath: seed.semanticPath}}
	intent, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "b12-collector", BundleHash: seed.bundleHash, Candidate: &key, Now: time.Now().UTC(), Lease: time.Minute})
	if err != nil || !found {
		t.Fatalf("group claim found=%v err=%v", found, err)
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
	catalog, err := raw.(selectedFanOutMixedRouteOwner).RegisterAuthorActivityEventCatalog(authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, seed.bundleHash), []authoractivity.EventDescriptor{{EventType: "producer/mixed.none", Disposition: authoractivity.StoryAuthored}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(catalog.Release)
	f.bus, err = newStoreTestEventBus(t, raw.(storeTestDurableEventBusStore), bus.EventBusOptions{ContractBundle: semanticview.Wrap(bundle), SourceArtifactFact: mustStoreTestSourceArtifactFact(seed.bundleHash), RuntimeInstanceID: authorActivityTestRuntimeInstanceID})
	if err != nil {
		t.Fatal(err)
	}
	f.ctx = deliverylifecycle.WithRoute(ctx, events.DeliveryRoute{Target: events.MustExistingEntityTarget(f.sourceRoute)})
	var requests []pipeline.FanOutPublicationRequest
	for ordinal := 0; ordinal < cardinality; ordinal++ {
		projection, err := fanoutobligation.PrepareOrdinalEmission(intent, input.Trigger, ordinal)
		if err != nil {
			t.Fatal(err)
		}
		event, err := projection.NewEvent(events.EventFacts{ID: uuid.NewString(), Type: "producer/mixed.none", Producer: events.ProducerClaim{Type: events.EventProducerNode, ID: f.producer.Key()}, Payload: []byte(`{}`), ChainDepth: capsule.ChainDepth + 1, Envelope: events.EnvelopeForSourceRoute(events.EventEnvelope{}, f.sourceRoute), RoutingSource: f.source, CreatedAt: time.Now().UTC().Truncate(time.Microsecond)})
		if err != nil {
			t.Fatal(err)
		}
		f.events = append(f.events, event)
		requests = append(requests, pipeline.FanOutPublicationRequest{Ordinal: ordinal, Intent: engine.EmitIntent{Event: event}})
	}
	prepared, err := f.bus.PrepareFanOutPublications(f.ctx, f.group, requests)
	if err != nil || len(prepared) != cardinality {
		t.Fatalf("prepare group=%+v err=%v", prepared, err)
	}
	command := pipeline.FanOutChunkCommand{Claim: claim, Now: time.Now().UTC()}
	var plans []engine.DurablePublicationPlan
	for i, prepared := range prepared {
		if prepared.Err != nil || prepared.Ordinal != i || prepared.Publication == nil {
			t.Fatalf("prepared ordinal=%+v", prepared)
		}
		plans = append(plans, prepared.Publication)
		command.Outcomes = append(command.Outcomes, pipeline.FanOutChunkOutcome{Ordinal: i, Publication: prepared.Publication})
	}
	if err := f.bus.SealFanOutPublications(f.ctx, f.group, cardinality, plans); err != nil {
		t.Fatal(err)
	}
	committed, err := owner.CommitFanOutChunk(f.ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	f.committed = committed.Publications
	if err := f.bus.FinalizeFanOutPublications(f.ctx, f.group, f.committed); err != nil {
		t.Fatal(err)
	}
	dialect := authoractivityfixture.DialectSQLite
	if f.postgres {
		dialect = authoractivityfixture.DialectPostgres
	}
	insertProducerIdentityDecisionObligation(t, authorActivityReceiptFixture{db: db, dialect: dialect}, f.ctx, f.events[1].ID(), f.runID, time.Now().UTC())
	return f, connector
}
