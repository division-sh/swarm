package pipeline

import (
	"context"
	"encoding/json"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
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
func TestFanOutCompletedIntentRefillsCoalescedBacklog(t *testing.T) {
	fanOutBacklogProbe(t, 0)
}

func TestFanOutCoalescedBacklogAcceleratedControl(t *testing.T) {
	fanOutBacklogProbe(t, 10*time.Millisecond)
}

func fanOutBacklogProbe(t *testing.T, interval time.Duration) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	workflowStore := newSQLiteWorkflowInstanceStoreForTest(t, db)
	ctx := sqliteExactOnceRunContext(t, db)
	pc, bus := newSQLiteDynamicActivationCoordinator(t, db, workflowStore)
	parentEntityID := uuid.NewString()
	parentPath := runtimecorrelation.RunIDFromContext(ctx)

	parent := eventtest.RunCreatingRootIngress(
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
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, parentEntityID), parentPath),
		time.Now().UTC(),
	)

	if err := workflowStore.create(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      parentPath,
		StorageRef:      parentPath,
		EntityID:        parentEntityID,
		EntityType:      "parent",
		WorkflowName:    ".",
		WorkflowVersion: "v-test",
		CurrentState:    "pending",
		Fields:          map[string]any{},
		CreatedAt:       time.Now().UTC(),
		UpdatedAt:       time.Now().UTC(),
	})); err != nil {
		t.Fatalf("seed parent workflow instance: %v", err)
	}
	parentNode := pipelineSourceNode(t, pc.SemanticSource(), ".", "fanout-node")
	parentRoute := seedExactOnceEventDelivery(t, pc, ctx, parent, parentNode)
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
	if got := bus.publishedCount(); got != 0 {
		t.Fatalf("trigger transaction published %d eager child events", got)
	}
	assertDeliveryStatusCount(t, workflowStore, ctx, parent.ID(), parentNode.Key(), "delivered", 1)
	var cardinality, cursor int
	var status, sourceKind, sourceEventID, sourceField string
	if err := db.QueryRowContext(ctx, `SELECT cardinality,cursor,status,source_kind,source_event_id,source_field FROM fan_out_intents WHERE run_id=?`, runtimecorrelation.RunIDFromContext(ctx)).Scan(
		&cardinality, &cursor, &status, &sourceKind, &sourceEventID, &sourceField,
	); err != nil {
		t.Fatalf("load durable fan-out intent: %v", err)
	}
	if cardinality != 2 || cursor != 0 || status != "open" || sourceKind != "event_payload_field" || sourceEventID != parent.ID() || sourceField != "components" {
		t.Fatalf("fan-out intent = cardinality:%d cursor:%d status:%s source:%s/%s/%s", cardinality, cursor, status, sourceKind, sourceEventID, sourceField)
	}
	if logs := bus.runtimeLogEntries(); len(logs) != 0 {
		t.Fatalf("runtime logs = %#v, want none", logs)
	}

	var deliveryID, flowPath, family, semanticPath, bundleHash, semanticDigest string
	var capsuleRaw []byte
	if err := db.QueryRowContext(ctx, `SELECT triggering_delivery_id,flow_path,declaration_family,semantic_path,bundle_hash,semantic_digest,capsule FROM fan_out_intents WHERE run_id=?`, runtimecorrelation.RunIDFromContext(ctx)).Scan(
		&deliveryID, &flowPath, &family, &semanticPath, &bundleHash, &semanticDigest, &capsuleRaw,
	); err != nil {
		t.Fatalf("load durable fan-out execution identity: %v", err)
	}
	var capsule fanoutobligation.Capsule
	if err := json.Unmarshal(capsuleRaw, &capsule); err != nil {
		t.Fatalf("decode durable fan-out capsule: %v", err)
	}
	intent := fanoutobligation.Intent{
		Request: fanoutobligation.IntentRequest{
			Key: fanoutobligation.IntentKey{
				RunID:                runtimecorrelation.RunIDFromContext(ctx),
				TriggeringDeliveryID: deliveryID,
				ElementRef:           runtimecontracts.FanOutElementRef{FlowPath: flowPath, Family: family, SemanticPath: semanticPath},
			},
			PlanRef: runtimecontracts.FanOutPlanRef{
				BundleHash: bundleHash, ElementRef: runtimecontracts.FanOutElementRef{FlowPath: flowPath, Family: family, SemanticPath: semanticPath}, SemanticDigest: semanticDigest,
			},
			Source:      fanoutobligation.SourceRef{Kind: fanoutobligation.SourceEventPayloadField, EventID: parent.ID(), Field: "components"},
			Cardinality: 2,
			Capsule:     capsule,
		},
		Source:        fanoutobligation.SourceRef{Kind: fanoutobligation.SourceEventPayloadField, EventID: parent.ID(), Field: "components"},
		Status:        fanoutobligation.StatusOpen,
		NextChunkSize: fanoutobligation.InitialChunkSize,
		CreatedAt:     parent.CreatedAt(),
		UpdatedAt:     parent.CreatedAt(),
	}
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
