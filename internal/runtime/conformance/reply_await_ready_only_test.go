package conformance

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/templatereply"
	"github.com/google/uuid"
)

type awaitCarrierConformanceStore struct {
	*replyConformanceStore
	requestOperation string
}

func (s *awaitCarrierConformanceStore) CommitPublication(ctx context.Context, command bus.PublicationCommand) (bus.CommittedPublication, error) {
	if command.Commit.CompletionAwait != nil {
		s.mu.Lock()
		s.requestOperation = command.Commit.CompletionAwait.Attempt.OperationID
		s.mu.Unlock()
	}
	return s.replyConformanceStore.CommitPublication(ctx, command)
}

// Planner/dispatcher composition with a synthetic retained binding. Native
// binding admission and atomicity belong to the separate two-store control.
func TestReplyAwaitPlannerRecordsReadinessWithoutRequesterInvocation(t *testing.T) {
	runID := uuid.NewString()
	source := templatereply.LoadSource(t, templatereply.Options{ExplicitCorrelation: true})
	fact := conformanceSourceArtifactFact(t, source)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContextForBundle(context.Background(), fact), runID)
	store := &awaitCarrierConformanceStore{replyConformanceStore: newReplyConformanceStore()}
	receipts := replyConformanceConstructionReceipts(t, source, runID)
	eb, err := newScopedTestEventBus(t, store, bus.EventBusOptions{ContractBundle: source, SourceArtifactFact: fact,
		Durable: bus.DurableDependencies{Instances: receipts, ConstructionPublications: receipts, ReplyContext: store}})
	if err != nil {
		t.Fatal(err)
	}
	request := replyConformanceEventForRun(source.ResolveFlowEventReference(templatereply.RequesterFlowID, templatereply.RequestEvent), uuid.NewString(), runID,
		templatereply.RequesterFlowID, templatereply.RequesterFlowID+"/account-a", map[string]any{"provider_request_id": "await-key", "account_id": "account-a"})
	request = eventtest.ChildWithLineageAndRoutingSource(uuid.NewString(), request.Type(), request.SourceAgent(), "", request.Payload(), 1, events.LineageFromEvent(request), request.Envelope(), request.RoutingSource(), request.CreatedAt())
	publication := effects.CompletionAwaitPublication{Attempt: effects.Attempt{OperationID: "synthetic-operation", AttemptID: "synthetic-attempt", Authority: effects.Authority{Target: effects.UsageTarget{AgentID: request.SourceAgent()}}}, CompletedResults: []byte(`[]`)}
	if err := eb.Publish(effects.WithCompletionAwaitPublication(ctx, request.ID(), publication), request); err != nil {
		t.Fatal(err)
	}
	if store.requestOperation != publication.Attempt.OperationID {
		t.Fatal("ordinary publication preparation lost its exact private await input")
	}
	routes := store.routesFor(request.ID())
	if len(routes) != 1 || routes[0].Context.ReplyContextID() == "" {
		t.Fatalf("genuine paired request planning precondition: %+v", routes)
	}
	id := routes[0].Context.ReplyContextID()
	store.mu.Lock()
	record := store.contexts[id]
	record.Await = &replycontext.AwaitBinding{Version: "reply-await.v1", ReplyContextID: id, RequestEventID: request.ID(), RunID: runID, OriginLife: record.OriginLife,
		DeliveryID: "synthetic-delivery", ClaimVersion: 1, ExecutionOwner: "synthetic-worker", OperationID: "synthetic-operation", AttemptID: "synthetic-attempt",
		BundleHash: fact.BundleHash(), PlanFingerprint: "synthetic-plan", FrameID: "synthetic-frame", ResponseFingerprint: "synthetic-response", CallID: "first", CompletedResults: "[]"}
	store.contexts[id] = record
	store.mu.Unlock()
	if err := record.Validate(); err != nil {
		t.Fatal(err)
	}
	reply := replyConformanceEventForRun(source.ResolveFlowEventReference(templatereply.ProviderFlowID, templatereply.ReplyEvent), uuid.NewString(), runID,
		templatereply.ProviderFlowID, templatereply.ProviderFlowID, map[string]any{"provider_request_id": "await-key", "account_id": "account-a", "result": "ready"})
	replyCtx := events.WithDeliveryContext(ctx, routes[0].Context)
	before := store.claimCalls
	plan, err := eb.CheckPublishRecipientPlan(replyCtx, reply)
	if err != nil || plan.TargetFailure != "" || len(plan.DeliveryRoutes) != 0 || store.claimCalls != before {
		t.Fatalf("ready-only planning created a requester or claimed early: plan=%+v err=%v", plan, err)
	}
	if err := eb.Publish(replyCtx, reply); err != nil {
		t.Fatal(err)
	}
	retained, err := store.LoadReplyContext(ctx, id)
	if err != nil || retained.State != replycontext.StateTerminal || retained.AcceptedReplyEventID != reply.ID() || retained.Await == nil || *retained.Await != *record.Await || len(store.routesFor(reply.ID())) != 0 || store.claimCalls != before+1 {
		t.Fatalf("reply did not remain ready data under its original binding: record=%+v err=%v", retained, err)
	}
}
