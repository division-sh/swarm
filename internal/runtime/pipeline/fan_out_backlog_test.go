package pipeline

import (
	"context"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
	"strings"
	"testing"
	"time"
)

type fanOutBacklogTestOwner struct {
	*singleTurnFanOutOwner
	pending []*singleTurnFanOutOwner
	commits chan struct{}
}

func (o *fanOutBacklogTestOwner) ClaimFanOutIntent(ctx context.Context, r FanOutClaimRequest) (fanoutobligation.Intent, fanoutobligation.Claim, bool, error) {
	if o.intent.Status == fanoutobligation.StatusClosed {
		if len(o.pending) == 0 {
			return fanoutobligation.Intent{}, fanoutobligation.Claim{}, false, nil
		}
		o.singleTurnFanOutOwner, o.pending = o.pending[0], o.pending[1:]
	}
	return o.singleTurnFanOutOwner.ClaimFanOutIntent(ctx, r)
}
func (o *fanOutBacklogTestOwner) CommitFanOutChunk(ctx context.Context, c FanOutChunkCommand) (CommittedFanOutChunk, error) {
	result, err := o.singleTurnFanOutOwner.CommitFanOutChunk(ctx, c)
	if err == nil {
		o.commits <- struct{}{}
	}
	return result, err
}
func VerifyNativeFanOutCompletedIntentRefillsCoalescedBacklogForTest(t *testing.T, backend string, open pipelineDeliveryNativeOpenerForTest) {
	fanOutBacklogProbe(t, backend, open, 0)
}

func VerifyNativeFanOutCoalescedBacklogAcceleratedControlForTest(t *testing.T, backend string, open pipelineDeliveryNativeOpenerForTest) {
	fanOutBacklogProbe(t, backend, open, 10*time.Millisecond)
}

func fanOutBacklogProbe(t *testing.T, backend string, open pipelineDeliveryNativeOpenerForTest, interval time.Duration) {
	fixture, pc, ctx := nativePilotPipelineForTest(t, backend, sqliteDynamicActivationBundle(t), open)
	nativeBus := observeNativePipelineDeliveryBusForTest(t, pc)
	bus := &recordingPipelineBus{}
	parentPath := runtimecorrelation.RunIDFromContext(ctx)
	parentEntityID := parentPath

	parent := eventtest.ExistingRunRootIngress(
		uuid.NewString(),
		events.EventType("component_scaffold.batch_requested"),
		"",
		"",
		mustJSON(map[string]any{
			"components": []any{
				map[string]any{"component_id": "component-a"},
				map[string]any{"component_id": "component-b"},
			},
		}),
		0,
		runtimecorrelation.RunIDFromContext(ctx),
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: ".", FlowInstance: parentPath, EntityID: parentEntityID}),
		time.Now().UTC(),
	)

	constructNativePipelineScenarioForTest(t, fixture, pc, ctx, ".")
	parentNode := pipelineSourceNode(t, pc.SemanticSource(), ".", "fanout-node")
	parentRoute := nativeFanOutTriggerRouteForTest(t, fixture, pc, ctx, parent, parentNode)
	state, err := pc.currentWorkflowState(runtimecorrelation.WithInboundEvent(ctx, parent), testRunScopedWorkflowInstanceFromContext(ctx, parentPath), identity.NormalizeEntityID(parentEntityID))
	if err != nil {
		t.Fatalf("load parent workflow state: %v", err)
	}
	if got := strings.TrimSpace(state.Control.FlowPath); got != parentPath {
		t.Fatalf("parent workflow state flow_path = %q, want %s", got, parentPath)
	}

	handled, err := pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(ctx, parentRoute), parent)
	if err != nil {
		t.Fatalf("dispatch parent fan-out event: %v", err)
	}
	if !handled {
		t.Fatal("parent fan-out dispatch handled=false, want true")
	}
	if got := nativeBus.publishedCount(); got != 0 {
		t.Fatalf("trigger transaction published %d eager child events", got)
	}
	snapshot, err := fixture.NodeDeliverySnapshot(ctx, parent.RunID(), parent.ID(), parentNode.Key())
	if err != nil || snapshot.Status != runtimedelivery.StatusDelivered {
		t.Fatalf("native trigger settlement: %+v/%v", snapshot, err)
	}
	intent := nativeFanOutIntentObservationForTest(t, fixture, pc, ctx, parent, parentNode)
	if intent.Request.Cardinality != 2 || intent.Cursor != 0 || intent.Status != fanoutobligation.StatusOpen || intent.Source.Kind != fanoutobligation.SourceEventPayloadField || intent.Source.EventID != parent.ID() || intent.Source.Field != "components" {
		t.Fatalf("native fan-out intent has wrong cardinality, cursor, status or pinned source: %#v", intent)
	}
	if nativeBus.publishedCount() != 0 || len(nativeBus.runtimeLogEntries()) != 0 {
		t.Fatal("native trigger eagerly published or logged a failure")
	}
	bundleHash := intent.Request.PlanRef.BundleHash

	owner := &singleTurnFanOutOwner{
		intent: intent,
		input: FanOutEvaluationInput{
			StartOrdinal: 0,
			Items: []any{
				map[string]any{"component_id": "component-a"},
				map[string]any{"component_id": "component-b"},
			},
			Trigger: parent,
		},
	}
	pc.sourceArtifactFact = mustPipelineTestSourceArtifactFact(bundleHash)
	pc.workflowStore.fanOutObligations = owner
	contextProbe := &fanOutMaintenanceContextProbe{
		recordingPipelineBus: bus,
		runID:                intent.Request.Key.RunID,
		triggerEventID:       parent.ID(),
	}
	pc.bus = contextProbe

	backlog := &fanOutBacklogTestOwner{singleTurnFanOutOwner: owner, commits: make(chan struct{}, 3)}
	for i := 0; i < 2; i++ {
		sibling := intent
		sibling.Request.Key.TriggeringDeliveryID = uuid.NewString()
		backlog.pending = append(backlog.pending, &singleTurnFanOutOwner{intent: sibling, input: owner.input})
	}
	pc.workflowStore.fanOutObligations = backlog
	pc.testMaintenanceInterval = interval
	// Replace already-fired trigger notification with three deliberately coalesced arrivals.
	wake := make(fanOutTestWake, 1)
	pc.InstallFanOutWorkNotifier(wake)
	pc.signalFanOutWork()
	pc.signalFanOutWork()
	pc.signalFanOutWork()
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); runFanOutTestDriver(runCtx, pc, wake, interval) }()
	defer func() { cancel(); <-done }()
	for i := 0; i < 3; i++ {
		select {
		case <-backlog.commits:
		case <-time.After(1200 * time.Millisecond):
			t.Fatalf("only %d of 3 complete one-chunk intents serviced: coalesced wake + closed-intent return leaves remaining eligible backlog idle", i)
		}
	}
}
