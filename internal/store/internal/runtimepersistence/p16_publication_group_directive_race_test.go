package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

// The directive is an independent event in the same run, never a member of the
// fan-out group. Both mutations retain their own receipt and revision boundary.
func TestP16PublicationGroupVersusDirectiveBothCommitOrders(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, winner := range []string{"group", "directive"} {
			t.Run(backend+"/"+winner, func(t *testing.T) {
				raw, db, connector := newP16RaceStore(t, backend)
				fixture, baseCtx, group, members := prepareP16CommittedGroup(t, raw, db, backend)
				ctx, cancel := context.WithTimeout(baseCtx, 10*time.Second)
				defer cancel()
				store := raw.(agentcontrol.DirectiveOperationStore)
				operation, executionOwner := prepareP16IndependentDirective(t, ctx, raw, db, fixture, backend == "postgres")
				failure := agentcontrol.DirectiveExecutionLeaseExpiredFailure()
				at := time.Now().UTC()
				before := readP16PreservationSnapshot(t, db)
				if _, err := store.FinalizeDirectiveFailure(ctx, operation.OperationID, uuid.NewString(), failure, at, time.Hour); !errors.Is(err, agentcontrol.ErrDirectiveInProgress) {
					t.Fatalf("foreign origin changed independent directive while group live: %v", err)
				}
				requireP16PreservationSnapshot(t, db, before)
				beforeRevision := countP16RunRevisions(t, db, fixture.runID)
				settleGroup := func() error {
					result, err := group.Settle(ctx, members)
					if err == nil {
						if len(result.Results) != len(members) {
							t.Errorf("group result count=%d want=%d", len(result.Results), len(members))
						}
						for _, result := range result.Results {
							if !result.Outcome.Committed() || !result.Outcome.DeliveryHandoffCommitted() {
								t.Errorf("group lost acknowledged settlement/handoff: %+v", result)
							}
						}
					}
					return err
				}
				directive := func() error {
					_, err := store.FinalizeDirectiveFailure(ctx, operation.OperationID, executionOwner, failure, at, time.Hour)
					return err
				}
				first, second := settleGroup, directive
				if winner == "directive" {
					first, second = directive, settleGroup
				}
				entered, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				t.Cleanup(unblock)
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
					t.Fatalf("first mutation did not reach physical COMMIT: %v", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				secondStarted := make(chan struct{})
				if backend == "postgres" {
					connector.mu.Lock()
					connector.begin = secondStarted
					connector.mu.Unlock()
				}
				go func() {
					// SQLite's current writer token intentionally precedes BEGIN.
					if backend == "sqlite" {
						close(secondStarted)
					}
					secondDone <- second()
				}()
				select {
				case <-secondStarted:
				case err := <-secondDone:
					t.Fatalf("contender failed before admission: %v", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				unblock()
				for name, done := range map[string]<-chan error{"winner": firstDone, "contender": secondDone} {
					select {
					case err := <-done:
						if err != nil {
							t.Fatalf("%s mutation: %v", name, err)
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				assertDirectiveReceipt(t, db, operation.DirectiveEventID, "error", &failure)
				op, found, err := store.LoadDirectiveOperation(ctx, operation.OperationID)
				if err != nil || !found || op.State != agentcontrol.DirectiveOperationFailed || op.ExecutionOwnerID != executionOwner {
					t.Fatalf("group changed current directive origin/state: %+v found=%v err=%v", op, found, err)
				}
				observed, err := group.ReadPublicationSettlement(ctx, members)
				if err != nil || len(observed.Rows) != 2 {
					t.Fatalf("read exact group members: %+v err=%v", observed, err)
				}
				for _, row := range observed.Rows {
					if row.State != pipelineobligation.PublicationSettlementSatisfied {
						t.Fatalf("directive changed group member settlement: %+v", row)
					}
					assertDirectiveReceipt(t, db, row.Claim.EventID(), "processed", nil)
				}
				if got := countP16RunRevisions(t, db, fixture.runID); got != beforeRevision+2 {
					t.Fatalf("group and directive need separate named revisions: before=%d after=%d", beforeRevision, got)
				}
				settled := readP16PreservationSnapshot(t, db)
				if err := directive(); err != nil {
					t.Fatal(err)
				}
				if _, err := group.Settle(ctx, members); err == nil {
					t.Fatal("consumed group retained mutation authority")
				}
				requireP16PreservationSnapshot(t, db, settled)
			})
		}
	}
}

// Forward the real driver's reuse checks; the COMMIT interceptor must not
// fabricate validity or hide it from the retained-session capability owner.
type p16CommitConnector struct{ *stopCommitConnector }

func (c p16CommitConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.stopCommitConnector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return p16CommitConn{conn.(*stopCommitConn)}, nil
}

type p16CommitConn struct{ *stopCommitConn }

func (c p16CommitConn) IsValid() bool {
	validator, ok := c.Conn.(driver.Validator)
	return ok && validator.IsValid()
}

func (c p16CommitConn) ResetSession(ctx context.Context) error {
	return c.Conn.(driver.SessionResetter).ResetSession(ctx)
}

func newP16RaceStore(t *testing.T, backend string) (selectedFanOutLifecycleOwner, *sql.DB, *stopCommitConnector) {
	t.Helper()
	if backend == "sqlite" {
		return newStopCommitStore(t, backend)
	}
	dsn, _, _ := testutil.StartPostgres(t)
	connector := &stopCommitConnector{driver: &pq.Driver{}, dsn: dsn}
	db := sql.OpenDB(p16CommitConnector{connector})
	db.SetMaxOpenConns(12)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return newStopCommitStoreOwner(t, backend, db), db, connector
}

func prepareP16IndependentDirective(t *testing.T, ctx context.Context, raw selectedFanOutLifecycleOwner, db *sql.DB, fixture fanOutOwnerFixture, postgres bool) (agentcontrol.DirectiveOperation, string) {
	t.Helper()
	store := raw.(agentcontrol.DirectiveOperationStore)
	agent := mustTestAgentIdentityForRun(fixture.runID, "p16-group-agent", "producer")
	seedTestAgentRow(t, ctx, db, postgres, agent, "active")
	operationID, eventID := uuid.NewString(), uuid.NewString()
	now := time.Now().UTC()
	request := agentcontrol.SendDirectiveRequest{AgentID: agent.AgentID(), FlowInstance: agent.FlowInstance(), Directive: "preserve independent origin", RunID: fixture.runID, Source: agentcontrol.DirectiveSourceV1RPC, OperatorID: "p16-test"}
	event, err := agentcontrol.NewDirectiveEvent(request, agentcontrol.RunTargetResolution{RunID: fixture.runID, Mode: agentcontrol.RunResolutionSpecified}, operationID, eventID, now, executionposture.Live)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := store.ReserveDirectiveOperation(ctx, agentcontrol.ReserveDirectiveOperationRequest{Event: admitted, Now: now, Operation: agentcontrol.DirectiveOperation{
		OperationID: operationID, Method: agentcontrol.DirectiveOperationMethod, ActorTokenID: "p16-test", IdempotencyKey: uuid.NewString(), RequestHash: "p16-" + operationID,
		AgentIdentity: agent, Directive: request.Directive, RequestedRunID: fixture.runID, ResolvedRunID: fixture.runID, RunIDResolution: agentcontrol.RunResolutionSpecified,
		Source: request.Source, OperatorID: request.OperatorID, DirectiveEventID: eventID, State: agentcontrol.DirectiveOperationPrepared,
	}})
	if err != nil {
		t.Fatal(err)
	}
	owner := uuid.NewString()
	if _, err := store.AdmitDirectiveExecution(ctx, agentcontrol.DirectiveExecutionAdmissionRequest{OperationID: operationID, OwnerID: owner, Now: now, Lease: time.Minute, ExecutionPosture: executionposture.Live}); err != nil {
		t.Fatal(err)
	}
	return reserved.Operation, owner
}

func prepareP16CommittedGroup(t *testing.T, raw selectedFanOutLifecycleOwner, db *sql.DB, backend string) (fanOutOwnerFixture, context.Context, pipelineobligation.PublicationGroup, []pipelineobligation.PublicationSettlementMember) {
	t.Helper()
	fixture, ctx, group, members, owner, command := prepareP16PublicationGroup(t, raw, db, backend, 2)
	if _, err := owner.CommitFanOutChunk(ctx, command); err != nil {
		t.Fatal(err)
	}
	claims := make([]pipelineobligation.Claim, len(members))
	for i := range members {
		claims[i] = members[i].Claim
	}
	if err := group.ValidateCommitted(ctx, claims); err != nil {
		t.Fatal(err)
	}
	return fixture, ctx, group, members
}

func prepareP16PublicationGroup(t *testing.T, raw selectedFanOutLifecycleOwner, db *sql.DB, backend string, cardinality int) (fanOutOwnerFixture, context.Context, pipelineobligation.PublicationGroup, []pipelineobligation.PublicationSettlementMember, pipeline.FanOutObligationOwner, pipeline.FanOutChunkCommand) {
	t.Helper()
	artifact, bundle := fanOutMixedRouteSource(t)
	ctx := testAuthorActivityContextForBundle(artifact.BundleHash())
	fixture := seedFanOutOwnerFixtureWithArtifact(t, ctx, db, raw, backend == "postgres", cardinality, time.Now().UTC(), artifact)
	route := events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: uuid.NewString()}.Normalized()
	source, err := events.NewStaticFlowRoutingSource(route)
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
	capsule.NodeKey, capsule.ExecutionFlowID, capsule.EntityID = producer.Key(), "producer", route.EntityID
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
	intent, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "p16-group", BundleHash: fixture.bundleHash, Candidate: &key, Now: time.Now().UTC(), Lease: time.Minute})
	if err != nil || !found {
		t.Fatalf("group fan-out claim: found=%v err=%v", found, err)
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
	scope, _ := authoractivity.ScopeFromContext(ctx)
	catalog, err := raw.(selectedFanOutMixedRouteOwner).RegisterAuthorActivityEventCatalog(scope, []authoractivity.EventDescriptor{{EventType: "producer/mixed.none", Disposition: authoractivity.StoryAuthored}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(catalog.Release)
	eventBus, err := newStoreTestEventBus(t, raw.(storeTestDurableEventBusStore), bus.EventBusOptions{ContractBundle: semanticview.Wrap(bundle), SourceArtifactFact: mustStoreTestSourceArtifactFact(fixture.bundleHash), RuntimeInstanceID: authorActivityTestRuntimeInstanceID})
	if err != nil {
		t.Fatal(err)
	}
	ctx = deliverylifecycle.WithRoute(ctx, events.DeliveryRoute{Target: events.MustExistingEntityTarget(route)})
	var claims []pipelineobligation.Claim
	var members []pipelineobligation.PublicationSettlementMember
	command := pipeline.FanOutChunkCommand{Claim: claim, Now: time.Now().UTC()}
	for ordinal := 0; ordinal < 2; ordinal++ {
		projection, err := fanoutobligation.PrepareOrdinalEmission(intent, input.Trigger, ordinal)
		if err != nil {
			t.Fatal(err)
		}
		event, err := projection.NewEvent(events.EventFacts{ID: uuid.NewString(), Type: "producer/mixed.none", Producer: events.ProducerClaim{Type: events.EventProducerNode, ID: producer.Key()}, Payload: []byte(`{}`), ChainDepth: capsule.ChainDepth + 1, Envelope: events.EnvelopeForSourceRoute(events.EventEnvelope{}, route), RoutingSource: source, CreatedAt: time.Now().UTC().Truncate(time.Microsecond)})
		if err != nil {
			t.Fatal(err)
		}
		plan, err := eventBus.PrepareFanOutPublication(ctx, group, ordinal, engine.EmitIntent{Event: event})
		if err != nil {
			t.Fatal(err)
		}
		member := plan.(bus.EnginePublicationPlan).PublicationCommand().Commit.PipelineClaim
		claims = append(claims, member)
		members = append(members, pipelineobligation.PublicationSettlementMember{Claim: member, Disposition: pipelineobligation.Acknowledged("pipeline_persisted")})
		command.Outcomes = append(command.Outcomes, pipeline.FanOutChunkOutcome{Ordinal: ordinal, Publication: plan})
	}
	end := intent.ChunkEndOrdinal()
	if end > 2 {
		command.Outcomes = append(command.Outcomes, rejectedFanOutChunk(claim, 2, end-2, command.Now).Outcomes...)
	}
	if err := group.Seal(ctx, end, claims); err != nil {
		t.Fatal(err)
	}
	return fixture, ctx, group, members, owner, command
}

func countP16RunRevisions(t *testing.T, db *sql.DB, runID string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1`, runID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
