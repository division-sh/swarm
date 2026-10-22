package pipeline

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/activityresult"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/google/uuid"
)

// The actual result consumer changes the persisted generation before the pending
// input is accounted by the activity dispatcher. Reopen uses original native
// owners, not an in-memory generation mock. This is not selected-executor
// activation or a SIGKILL/durable-input-settlement proof.
func VerifyReadOnlyPublishedAccountingAfterNativeLoopAdvanceAndReopenForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"value":"unexpected-reexecution"}`))
			}))
			defer server.Close()
			bundle := loadWorkflowTempBundle(t, map[string]string{
				"schema.yaml":   "name: published-read-accounting\nstages:\n  review: {}\n  drafting: {}\n  exhausted: {final: true}\nloops:\n  revision:\n    revision_field: revision_id\n    max_attempts: 3\n    escape: {advances_to: exhausted}\n",
				"entities.yaml": "work: {}\n",
				"events.yaml":   "read.requested:\n  revision_id: text\n",
				"nodes.yaml": `reader:
  execution_type: system_node
  subscribes_to: [read.requested, read.succeeded, read.failed]
  event_handlers:
    read.requested:
      loop: {admit: revision, from: review}
      activity: {id: read, tool: read}
    read.succeeded:
      loop: {repeat: revision, from: review}
      advances_to: drafting
    read.failed:
      guard: {id: exact_activity, check: payload.activity_id == 'read'}
`,
				"tools.yaml": fmt.Sprintf("read:\n  handler_type: http\n  http: {method: GET, url: %q}\n", server.URL),
			})
			fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
			run := correlation.RunIDFromContext(ctx)
			activation, err := loopruntime.New(run, run, ".", "revision", "revision_id", uuid.NewString(), "review", 3, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			buckets := map[string]map[string]any{}
			if err := loopruntime.Store(buckets, activation); err != nil {
				t.Fatal(err)
			}
			instance := materializedWorkflowInstanceForSource(t, pc.SemanticSource(), ctx, WorkflowInstance{InstanceID: run, StorageRef: run, EntityID: run, WorkflowName: ".", WorkflowVersion: bundle.WorkflowVersion(), EntityType: "work", CurrentState: "review", CreatedAt: time.Now().UTC(), EnteredStageAt: time.Now().UTC(), Fields: map[string]any{}, StateBuckets: engine.NewStateCarrier(nil, nil, buckets).PersistedStateBuckets()})
			if err := fixture.Construct(ctx, instance); err != nil {
				t.Fatal(err)
			}
			intent, _, _, accountingCtx := publishedActivityAccountingFixture(t)
			intent.SourceRunID, intent.EntityID, intent.FlowInstance = run, identity.EntityID(run), run
			intent.ExecutionFlowID, intent.Owner = identity.FlowID("."), activityidentity.MustNodeOwner(pipelineNode(t, ".", "reader"))
			intent.ActivityID, intent.Tool, intent.HandlerEventKey = "read", "read", "read.requested"
			intent.SuccessEvent, intent.FailureEvent = "read.succeeded", "read.failed"
			intent.Generation, intent.LoopStage = activation.Generation(), "review"
			intent.RoutingSource = testWorkflowRoutingSource(".", run, run)
			source := eventtest.ExistingRunRootIngressWithRoutingSource(intent.SourceEventID, "read.requested", "test", intent.SourceTaskID, mustJSON(map[string]any{"revision_id": activation.RevisionID}), 0, run, events.EventEnvelope{}, intent.RoutingSource, time.Now().UTC())
			fixture.PublishDirect(ctx, source)
			request, err := activityRequestEmitIntent(intent)
			if err != nil {
				t.Fatal(err)
			}
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(pipelineNode(t, ".", "reader")), Target: events.MustExistingEntityTarget(intent.RoutingSource.Route())}
			fixture.PublishDirect(ctx, request.Event)
			emissions := &pipelineEmissionPlan{}
			if err := (pipelineActivityDispatcher{emissions: emissions}).publishActivitySuccess(ctx, intent, map[string]any{"value": "committed"}); err != nil {
				t.Fatal(err)
			}
			result := eventtest.TargetRouted(emissions.immutableEvents()[0], intent.RoutingSource.Route())
			if err := fixture.PublishNode(ctx, result, route); err != nil {
				t.Fatal(err)
			}
			handled, outcome, err := pc.handleEventResult(withWorkflowNodeDeliveryRoute(ctx, route), result)
			if disposition, failed := outcome.Disposition(); err != nil || !handled || failed {
				t.Fatalf("real result consumption: handled=%t disposition=%+v error=%v", handled, disposition, err)
			}
			owner, err := flowidentity.NewRunScopedFlowInstance(run, flowidentity.StoredRoute(flowidentity.ScopeKey(pc.SemanticSource(), "."), run, run))
			if err != nil {
				t.Fatal(err)
			}
			advanced, found, err := pc.workflowStore.Load(ctx, owner)
			if err != nil || !found || advanced.CurrentState != "drafting" {
				t.Fatalf("consumer did not advance the native loop: %+v found=%t error=%v", advanced, found, err)
			}
			current, err := workflowLoopGenerationCurrent(&advanced, intent.Generation, intent.LoopStage)
			if err != nil || current {
				t.Fatalf("old input still owns fresh work: current=%t error=%v", current, err)
			}
			next := fixture.ReopenExecution()
			nextPC, nextCtx := nativePipelineDeliveryCoordinatorForTest(t, next, pc.module)
			nextCtx = correlation.WithRunID(nextCtx, run)
			retainedRequest, err := next.PublishedEvent(nextCtx, request.Event.ID())
			if err != nil {
				t.Fatal(err)
			}
			retainedResult, err := next.PublishedEvent(nextCtx, result.ID())
			if err != nil {
				t.Fatal(err)
			}
			if err := ValidateRecordedActivityPublication(retainedRequest, retainedResult, result.ID(), result.Payload()); err != nil {
				t.Fatal(err)
			}
			publication, err := activityresult.NewPublishedContinuation(activityIdentityFact(intent), retainedResult.ID(), string(retainedResult.Type()), retainedResult.Payload(), intent.ExecutionMode, activityInputHash(intent.Input), "", retainedResult.CreatedAt())
			if err != nil {
				t.Fatal(err)
			}
			authority, _ := effects.AuthorityFromContext(accountingCtx)
			authority.SelectedFork.ForkRunID = run
			nextCtx = effects.WithAuthority(nextCtx, authority)
			nextCtx, err = WithSelectedActivityPublications(nextCtx, run, []activityresult.PublishedContinuation{publication})
			if err != nil {
				t.Fatal(err)
			}
			retainedIntent, err := activityIntentFromRequestEvent(retainedRequest)
			if err != nil || !retainedIntent.Generation.Equal(intent.Generation) {
				t.Fatalf("reopened request changed its exact old generation: %+v/%v", retainedIntent.Generation, err)
			}
			accounting := &pipelineEmissionPlan{}
			if err := (pipelineActivityDispatcher{coordinator: nextPC, emissions: accounting}).executeActivityIntent(nextCtx, retainedIntent); err != nil {
				t.Fatalf("reopened dispatcher accounting: %v", err)
			}
			if emitted := accounting.immutableEvents(); len(emitted) != 0 {
				t.Fatalf("native result consumer advanced before accounting, which drafted %d new result(s): %s", len(emitted), emitted[0].Payload())
			}
			if calls.Load() != 0 || next.EventIDCount(nextCtx, retainedRequest.ID()) != 1 {
				t.Fatalf("accounting called the provider or dropped its input: calls=%d", calls.Load())
			}
			after, found, err := nextPC.workflowStore.Load(nextCtx, owner)
			if err != nil || !found || after.CurrentState != advanced.CurrentState || after.Revision != advanced.Revision {
				t.Fatalf("accounting changed persisted newer generation: %+v found=%t error=%v", after, found, err)
			}
			failedResult, err := admitActivityPublication(intent, intent.FailureEvent)
			if err != nil {
				t.Fatal(err)
			}
			if success, failure := next.EventIDCount(nextCtx, result.ID()), next.EventIDCount(nextCtx, failedResult.eventID); success != 1 || failure != 0 {
				t.Fatalf("native reopened result cardinality: success=%d failure=%d", success, failure)
			}
		})
	}
}
