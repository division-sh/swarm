package runtimepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
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
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestFanOutPublicationGroupClaimLifetimeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 20*time.Second)
			defer cancel()
			raw, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			fixture := seedFanOutOwnerFixture(t, ctx, db, raw, postgres, 3, time.Now().UTC())
			owner, _, _, _ := grantedFanOutOwnerForTest(t, ctx, raw, fixture)
			ctx = testAuthorActivityContextForBundle(fixture.bundleHash)
			key := fanoutobligation.IntentKey{RunID: fixture.runID, TriggeringDeliveryID: fixture.deliveryID, ElementRef: contracts.FanOutElementRef{FlowPath: fixture.flowPath, Family: "fan_out", SemanticPath: fixture.semanticPath}}
			intent, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "publication-group-test", BundleHash: fixture.bundleHash, Candidate: &key, Now: time.Now().UTC(), Lease: time.Minute})
			if err != nil || !found {
				t.Fatalf("claim found=%v err=%v", found, err)
			}
			input, err := owner.LoadFanOutEvaluation(ctx, claim)
			if err != nil {
				t.Fatal(err)
			}
			group, err := owner.BeginFanOutPublicationGroup(ctx, claim)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := group.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			store := raw.(interface {
				PipelineObligations() pipelineobligation.Store
			}).PipelineObligations()
			baselineCapacity := 0
			if postgres {
				baselineCapacity = raw.(*PostgresStore).backend.CapacityReservationsForTest()
			}
			var claimRequests []pipelineobligation.PublicationClaimRequest
			for ordinal := 0; ordinal < 3; ordinal++ {
				projection, err := fanoutobligation.PrepareOrdinalEmission(intent, input.Trigger, ordinal)
				if err != nil {
					t.Fatal(err)
				}
				event, err := projection.NewEvent(events.EventFacts{ID: uuid.NewString(), Type: "items.emitted", Producer: events.ProducerClaim{Type: events.EventProducerNode, ID: intent.Request.Capsule.NodeKey}, Payload: []byte(`{}`), ChainDepth: intent.Request.Capsule.ChainDepth + 1, RoutingSource: intent.Request.Capsule.ProducerSource, CreatedAt: time.Now().UTC()})
				if err != nil {
					t.Fatal(err)
				}
				claimRequests = append(claimRequests, pipelineobligation.PublicationClaimRequest{Ordinal: ordinal, Event: event})
			}
			collector, restore, err := InstallTransactionProbeForTest(raw, transactiontest.Options{Delay: 30 * time.Millisecond, DelayScope: transactiontest.DelayAllCommits})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(restore)
			claims, err := group.ClaimBatch(ctx, claimRequests)
			if err != nil {
				t.Fatal(err)
			}
			if len(claims) != len(claimRequests) {
				t.Fatalf("batch claim count=%d want %d", len(claims), len(claimRequests))
			}
			for i, claim := range claims {
				if claim.EventID() != claimRequests[i].Event.ID() {
					t.Fatal("batch reordered returned claim identities")
				}
			}
			receipt := collector.Snapshot()
			restore()
			if postgres {
				admission := receipt.ByOperation[transactiontest.PipelinePublicationAdmission]
				if receipt.Total.ReadCommits != 1 || receipt.Total.WriteCommits != 0 || admission.ReadCommits != 1 || admission.DelayedCommits != 1 || admission.InjectedDelay != 30*time.Millisecond || receipt.Active != 0 {
					t.Fatalf("one exact parent-fence batch must be one delayed real read commit: %+v", receipt)
				}
			} else if receipt.Total.ReadCommits != 0 || receipt.Total.WriteCommits != 0 {
				t.Fatalf("SQLite claim registration unexpectedly committed: %+v", receipt)
			}
			if _, err := group.Claim(ctx, claimRequests[0].Ordinal, claimRequests[0].Event); err == nil {
				t.Fatal("duplicate group member admitted")
			}
			if postgres {
				selected := raw.(*PostgresStore)
				first, err := selected.pipelinePostgresOwner.PostgresPipelineClaimStateForTest(claims[0])
				if err != nil {
					t.Fatal(err)
				}
				for _, member := range claims[1:] {
					state, err := selected.pipelinePostgresOwner.PostgresPipelineClaimStateForTest(member)
					if err != nil {
						t.Fatal(err)
					}
					if state.LeaseForTest().Session() != first.LeaseForTest().Session() {
						t.Fatal("group claims do not share their designated session")
					}
				}
				if got := selected.backend.CapacityReservationsForTest(); got != baselineCapacity+3 {
					t.Fatalf("group capacity=%d want %d", got, baselineCapacity+3)
				}
			}
			if err := group.Seal(ctx, 3, claims[:2]); err == nil {
				t.Fatal("seal omitted a current claim")
			}
			if err := store.Release(ctx, claims[2]); err != nil {
				t.Fatal(err)
			}
			if err := group.Seal(ctx, 3, claims[:2]); err == nil {
				t.Fatal("unprepared members were sealed")
			}
			for _, member := range claims[:2] {
				if err := store.Release(ctx, member); err != nil {
					t.Fatal(err)
				}
			}
			if err := group.Seal(ctx, 3, nil); err != nil {
				t.Fatalf("released rejected preparations must remain accounted: %v", err)
			}
			if err := group.Seal(ctx, 2, nil); err == nil {
				t.Fatal("reduced prefix accepted without proven rollback")
			}
			if err := group.ValidateCommitted(ctx, claims[:2]); err == nil {
				t.Fatal("uncommitted group authorized dispatch")
			}
			if _, err := group.Settle(ctx, []pipelineobligation.PublicationSettlementMember{{Claim: claims[0], Disposition: pipelineobligation.Acknowledged("pipeline_persisted")}}); err == nil {
				t.Fatal("uncommitted group settled")
			}
			if _, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, 0, 3, time.Now().UTC())); err != nil {
				t.Fatal(err)
			}
			if err := group.ValidateCommitted(ctx, nil); err != nil {
				t.Fatal(err)
			}
			if err := group.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if err := group.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if postgres && raw.(*PostgresStore).backend.CapacityReservationsForTest() != baselineCapacity {
				t.Fatal("group capacity leaked after close")
			}
			for _, member := range claims {
				if err := store.Release(ctx, member); !errors.Is(err, pipelineobligation.ErrStaleClaim) {
					t.Fatalf("closed group member survived: %v", err)
				}
			}
		})
	}
}

func TestFanOutPublicationGroupSettlementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 30*time.Second)
			defer cancel()
			raw, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			artifact, bundle := fanOutMixedRouteSource(t)
			fixture := seedFanOutOwnerFixtureWithArtifact(t, ctx, db, raw, postgres, 3, time.Now().UTC().Truncate(time.Microsecond), artifact)
			ctx = authoractivity.WithScope(testAuthorActivityContextForBundle(fixture.bundleHash), authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, fixture.bundleHash))
			ctx = correlation.WithSourceArtifactFact(correlation.WithRunID(ctx, fixture.runID), mustStoreTestSourceArtifactFact(fixture.bundleHash))
			sourceRoute := events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: uuid.NewString()}.Normalized()
			source, err := events.NewStaticFlowRoutingSource(sourceRoute)
			if err != nil {
				t.Fatal(err)
			}
			producer, err := identity.AdmitExecutableNodeDeclaration("producer", "fan-out-producer")
			if err != nil {
				t.Fatal(err)
			}
			var capsuleRaw []byte
			if err := db.QueryRowContext(ctx, `SELECT capsule FROM fan_out_intents WHERE run_id=$1`, fixture.runID).Scan(&capsuleRaw); err != nil {
				t.Fatal(err)
			}
			var capsule fanoutobligation.Capsule
			if err := json.Unmarshal(capsuleRaw, &capsule); err != nil {
				t.Fatal(err)
			}
			capsule.NodeKey, capsule.ExecutionFlowID, capsule.EntityID = producer.Key(), "producer", sourceRoute.EntityID
			capsule.Route, capsule.ProducerSource = flowidentity.StoredRoute("producer", "producer", "producer"), source
			capsuleRaw, err = json.Marshal(capsule)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `UPDATE fan_out_intents SET capsule=$1 WHERE run_id=$2`, string(capsuleRaw), fixture.runID); err != nil {
				t.Fatal(err)
			}
			owner, _, _, _ := grantedFanOutOwnerForTest(t, ctx, raw, fixture)
			key := fanoutobligation.IntentKey{RunID: fixture.runID, TriggeringDeliveryID: fixture.deliveryID, ElementRef: contracts.FanOutElementRef{FlowPath: fixture.flowPath, Family: "fan_out", SemanticPath: fixture.semanticPath}}
			intent, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "group-settlement", BundleHash: fixture.bundleHash, Candidate: &key, Now: time.Now().UTC(), Lease: time.Minute})
			if err != nil || !found {
				t.Fatalf("claim %v %v", found, err)
			}
			input, err := owner.LoadFanOutEvaluation(ctx, claim)
			if err != nil {
				t.Fatal(err)
			}
			group, err := owner.BeginFanOutPublicationGroup(ctx, claim)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := group.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			catalog, err := raw.(selectedFanOutMixedRouteOwner).RegisterAuthorActivityEventCatalog(authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, fixture.bundleHash), []authoractivity.EventDescriptor{{EventType: "producer/mixed.none", Disposition: authoractivity.StoryAuthored}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(catalog.Release)
			eventBus, err := newStoreTestEventBus(t, raw.(storeTestDurableEventBusStore), bus.EventBusOptions{ContractBundle: semanticview.Wrap(bundle), SourceArtifactFact: mustStoreTestSourceArtifactFact(fixture.bundleHash), RuntimeInstanceID: authorActivityTestRuntimeInstanceID})
			if err != nil {
				t.Fatal(err)
			}
			ctx = deliverylifecycle.WithRoute(ctx, events.DeliveryRoute{Target: events.MustExistingEntityTarget(sourceRoute)})
			var claims []pipelineobligation.Claim
			var requests []pipelineobligation.PublicationSettlementMember
			command := pipeline.FanOutChunkCommand{Claim: claim, Now: time.Now().UTC()}
			for ordinal := 0; ordinal < 3; ordinal++ {
				projection, err := fanoutobligation.PrepareOrdinalEmission(intent, input.Trigger, ordinal)
				if err != nil {
					t.Fatal(err)
				}
				event, err := projection.NewEvent(events.EventFacts{ID: uuid.NewString(), Type: "producer/mixed.none", Producer: events.ProducerClaim{Type: events.EventProducerNode, ID: producer.Key()}, Payload: []byte(`{}`), ChainDepth: capsule.ChainDepth + 1, Envelope: events.EnvelopeForSourceRoute(events.EventEnvelope{}, sourceRoute), RoutingSource: source, CreatedAt: time.Now().UTC().Truncate(time.Microsecond)})
				if err != nil {
					t.Fatal(err)
				}
				plan, err := eventBus.PrepareFanOutPublication(ctx, group, ordinal, engine.EmitIntent{Event: event})
				if err != nil {
					t.Fatal(err)
				}
				member := plan.(bus.EnginePublicationPlan).PublicationCommand().Commit.PipelineClaim
				claims = append(claims, member)
				requests = append(requests, pipelineobligation.PublicationSettlementMember{Claim: member, Disposition: pipelineobligation.Acknowledged("pipeline_persisted")})
				command.Outcomes = append(command.Outcomes, pipeline.FanOutChunkOutcome{Ordinal: ordinal, Publication: plan})
			}
			if err := group.Seal(ctx, 3, claims); err != nil {
				t.Fatal(err)
			}
			if _, err := owner.CommitFanOutChunk(ctx, command); err != nil {
				t.Fatal(err)
			}
			if err := group.ValidateCommitted(ctx, claims[:2]); err == nil {
				t.Fatal("dispatch accepted incomplete member set")
			}
			if err := group.ValidateCommitted(ctx, claims); err != nil {
				t.Fatal(err)
			}
			collector, restore, err := InstallTransactionProbeForTest(raw, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(restore)
			result, err := group.Settle(ctx, requests)
			if err != nil || len(result.Results) != 3 {
				t.Fatalf("settle results=%d err=%v", len(result.Results), err)
			}
			for _, result := range result.Results {
				if !result.Outcome.Committed() || !result.Outcome.DeliveryHandoffCommitted() {
					t.Fatal("missing exact committed outcome")
				}
			}
			receipt := collector.Snapshot()
			if receipt.Total.WriteCommits != 1 || receipt.Total.Revision.Finalizations != 1 {
				t.Fatalf("segment must commit and finalize once: %+v", receipt)
			}
			observed, err := group.ReadPublicationSettlement(ctx, requests)
			if err != nil || len(observed.Rows) != 3 {
				t.Fatalf("observe %+v %v", observed, err)
			}
			for _, row := range observed.Rows {
				if row.State != pipelineobligation.PublicationSettlementSatisfied {
					t.Fatalf("unexpected observation %+v", row)
				}
			}
			if _, err := group.Settle(ctx, requests); err == nil {
				t.Fatal("consumed segment mutated twice")
			}
			if err := group.Close(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFanOutPublicationGroupBatchBusyReleasesPrefixBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 15*time.Second)
			defer cancel()
			raw, sibling, db, postgres := newFanOutOwnerPairForTest(t, backend)
			fixture := seedFanOutOwnerFixture(t, ctx, db, raw, postgres, 3, time.Now().UTC())
			owner, _, _, _ := grantedFanOutOwnerForTest(t, ctx, raw, fixture)
			ctx = testAuthorActivityContextForBundle(fixture.bundleHash)
			key := fanoutobligation.IntentKey{RunID: fixture.runID, TriggeringDeliveryID: fixture.deliveryID, ElementRef: contracts.FanOutElementRef{FlowPath: fixture.flowPath, Family: "fan_out", SemanticPath: fixture.semanticPath}}
			intent, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "group-busy-prefix", BundleHash: fixture.bundleHash, Candidate: &key, Now: time.Now().UTC(), Lease: time.Minute})
			if err != nil || !found {
				t.Fatalf("claim found=%v err=%v", found, err)
			}
			input, err := owner.LoadFanOutEvaluation(ctx, claim)
			if err != nil {
				t.Fatal(err)
			}
			group, err := owner.BeginFanOutPublicationGroup(ctx, claim)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := group.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			var requests []pipelineobligation.PublicationClaimRequest
			for ordinal := 0; ordinal < 3; ordinal++ {
				projection, err := fanoutobligation.PrepareOrdinalEmission(intent, input.Trigger, ordinal)
				if err != nil {
					t.Fatal(err)
				}
				event, err := projection.NewEvent(events.EventFacts{ID: uuid.NewString(), Type: "items.emitted", Producer: events.ProducerClaim{Type: events.EventProducerNode, ID: intent.Request.Capsule.NodeKey}, Payload: []byte(`{}`), ChainDepth: intent.Request.Capsule.ChainDepth + 1, RoutingSource: intent.Request.Capsule.ProducerSource, CreatedAt: time.Now().UTC()})
				if err != nil {
					t.Fatal(err)
				}
				requests = append(requests, pipelineobligation.PublicationClaimRequest{Ordinal: ordinal, Event: event})
			}
			sort.Slice(requests, func(i, j int) bool { return requests[i].Event.ID() < requests[j].Event.ID() })
			// SQLite publication claims are process-local to the selected owner;
			// only PostgreSQL arbitrates another backend through session locks.
			if !postgres {
				sibling = raw
			}
			competing := sibling.(interface {
				PipelineObligations() pipelineobligation.Store
			}).PipelineObligations()
			held, err := competing.ClaimPublication(ctx, requests[2].Event.ID())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = competing.Release(context.Background(), held) })
			baselineCapacity := 0
			if postgres {
				baselineCapacity = raw.(*PostgresStore).backend.CapacityReservationsForTest()
			}
			collector, restore, err := InstallTransactionProbeForTest(raw, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(restore)
			claims, err := group.ClaimBatch(ctx, requests)
			if !errors.Is(err, pipelineobligation.ErrBusy) || len(claims) != 0 {
				t.Fatalf("busy batch returned partial capabilities: claims=%v err=%v", claims, err)
			}
			receipt := collector.Snapshot()
			restore()
			if receipt.Total.ReadCommits != 0 || receipt.Total.WriteCommits != 0 || receipt.Active != 0 {
				t.Fatalf("failed admission committed or retained a transaction: %+v", receipt)
			}
			if postgres && raw.(*PostgresStore).backend.CapacityReservationsForTest() != baselineCapacity {
				t.Fatal("failed batch leaked prefix capacity")
			}
			if _, err := group.ClaimBatch(ctx, requests[:1]); err == nil {
				t.Fatal("failed group permitted another preparation")
			}
			for _, request := range requests[:2] {
				reclaimed, err := competing.ClaimPublication(ctx, request.Event.ID())
				if err != nil {
					t.Fatalf("prefix retained event %s: %v", request.Event.ID(), err)
				}
				if err := competing.Release(ctx, reclaimed); err != nil {
					t.Fatal(err)
				}
			}
			if err := competing.Release(ctx, held); err != nil {
				t.Fatalf("failed batch stole competing claim: %v", err)
			}
		})
	}
}
