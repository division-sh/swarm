package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/contractelementidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

func TestSQLiteFanOutTriggerPersistsOneIntentWithoutEagerDeliveries(t *testing.T) {
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
		WorkflowName:    "root",
		WorkflowVersion: "v-test",
		CurrentState:    "pending",
		Fields:          map[string]any{},
		CreatedAt:       time.Now().UTC(),
		UpdatedAt:       time.Now().UTC(),
	})); err != nil {
		t.Fatalf("seed parent workflow instance: %v", err)
	}
	parentNode := pipelineSourceNode(t, pc.SemanticSource(), "", "fanout-node")
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

	var deliveryID, packageKey, elementID, bundleHash, semanticDigest string
	var capsuleRaw []byte
	if err := db.QueryRowContext(ctx, `SELECT triggering_delivery_id,package_key,element_id,bundle_hash,semantic_digest,capsule FROM fan_out_intents WHERE run_id=?`, runtimecorrelation.RunIDFromContext(ctx)).Scan(
		&deliveryID, &packageKey, &elementID, &bundleHash, &semanticDigest, &capsuleRaw,
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
				ElementRef:           runtimecontracts.FanOutElementRef{PackageKey: packageKey, ElementID: elementID},
			},
			PlanRef: runtimecontracts.FanOutPlanRef{
				BundleHash: bundleHash, ElementRef: runtimecontracts.FanOutElementRef{PackageKey: packageKey, ElementID: elementID}, SemanticDigest: semanticDigest,
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
	pc.bundleSourceFact = mustPipelineTestBundleSourceFact(bundleHash)
	pc.workflowStore.fanOutObligations = owner
	contextProbe := &fanOutMaintenanceContextProbe{
		recordingPipelineBus: bus,
		runID:                intent.Request.Key.RunID,
		triggerEventID:       parent.ID(),
	}
	pc.bus = contextProbe
	more, err := pc.serveFanOutTurn(context.Background(), time.Now().UTC())
	if err != nil {
		t.Fatalf("serve durable fan-out turn: %v", err)
	}
	if more {
		t.Fatal("terminal fan-out turn reported additional issuance work")
	}
	if owner.intent.Cursor != 2 || owner.intent.Status != fanoutobligation.StatusClosed || owner.commitCalls != 1 {
		t.Fatalf("fan-out owner after pump = intent:%#v commits:%d", owner.intent, owner.commitCalls)
	}
	if got := bus.outboxCount(); got != 2 {
		t.Fatalf("finalized fan-out publication evidence = %d, want 2", got)
	}
	if got := bus.publishedCount(); got != 2 {
		t.Fatalf("post-commit fan-out dispatches = %d, want 2", got)
	}
	for ordinal, componentID := range []string{"component-a", "component-b"} {
		published := bus.publishedEvent(ordinal)
		var payload map[string]any
		if err := json.Unmarshal(published.Payload(), &payload); err != nil {
			t.Fatalf("decode fan-out publication %d: %v", ordinal, err)
		}
		if published.Type() != events.EventType("component_scaffold.spawn_requested") || payload["component_id"] != componentID || published.RunID() != parent.RunID() || published.ParentEventID() != parent.ID() {
			t.Fatalf("fan-out publication %d = type:%s payload:%#v run:%s parent:%s; want type:%s component:%s run:%s parent:%s", ordinal, published.Type(), payload, published.RunID(), published.ParentEventID(), events.EventType("component_scaffold.spawn_requested"), componentID, parent.RunID(), parent.ID())
		}
	}

	recoveryIntent := intent
	recoveryIntent.Request.Key.TriggeringDeliveryID = uuid.NewString()
	recoveryIntent.Cursor = 0
	recoveryIntent.Status = fanoutobligation.StatusOpen
	recoveryIntent.ClaimOwner = ""
	recoveryIntent.ClaimGeneration = 0
	recoveryIntent.LeaseExpiresAt = time.Time{}
	recoveryIntent.CreatedAt = time.Now().UTC()
	recoveryIntent.UpdatedAt = recoveryIntent.CreatedAt
	recoveryOwner := newLostWakeFanOutOwner(&singleTurnFanOutOwner{intent: recoveryIntent, input: owner.input})
	pc.workflowStore.fanOutObligations = recoveryOwner
	pc.testMaintenanceInterval = 10 * time.Millisecond
	maintenanceCtx, cancelMaintenance := context.WithCancel(context.Background())
	maintenanceStopped := make(chan struct{})
	go func() {
		defer close(maintenanceStopped)
		pc.RunMaintenance(maintenanceCtx)
	}()
	select {
	case <-recoveryOwner.probed:
	case <-time.After(time.Second):
		cancelMaintenance()
		<-maintenanceStopped
		t.Fatal("maintenance startup did not scan durable fan-out work")
	}
	close(recoveryOwner.ready)
	select {
	case <-recoveryOwner.committed:
	case <-time.After(time.Second):
		cancelMaintenance()
		<-maintenanceStopped
		t.Fatal("periodic maintenance did not recover fan-out work after a lost wake")
	}
	cancelMaintenance()
	<-maintenanceStopped
	if recoveryOwner.intent.Cursor != 2 || recoveryOwner.intent.Status != fanoutobligation.StatusClosed || recoveryOwner.commitCalls != 1 {
		t.Fatalf("recovered fan-out = intent:%#v commits:%d", recoveryOwner.intent, recoveryOwner.commitCalls)
	}
	if got := bus.publishedCount(); got != 4 {
		t.Fatalf("published events after lost-wake recovery = %d, want four exact publications across two intents", got)
	}
	if contextProbe.prepareCalls != 4 {
		t.Fatalf("fan-out maintenance context was checked %d times, want four deferred publications", contextProbe.prepareCalls)
	}
}

type fanOutMaintenanceContextProbe struct {
	*recordingPipelineBus
	runID, triggerEventID string
	prepareCalls          int
}

func (b *fanOutMaintenanceContextProbe) PrepareEnginePublications(ctx context.Context, intents []runtimeengine.EmitIntent) ([]runtimeengine.DurablePublicationPlan, error) {
	if got := runtimecorrelation.RunIDFromContext(ctx); got != b.runID {
		return nil, fmt.Errorf("fan-out maintenance run context = %q, want %q", got, b.runID)
	}
	inbound, ok := runtimecorrelation.InboundEventFromContext(ctx)
	if !ok || inbound.ID() != b.triggerEventID || inbound.RunID() != b.runID {
		return nil, fmt.Errorf("fan-out maintenance inbound context = found:%v event:%s run:%s, want event:%s run:%s", ok, inbound.ID(), inbound.RunID(), b.triggerEventID, b.runID)
	}
	b.prepareCalls += len(intents)
	return b.recordingPipelineBus.PrepareEnginePublications(ctx, intents)
}

func TestSQLiteNestedFanOutCreatesIndependentDurableIntentAndExactLineage(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	workflowStore := newSQLiteWorkflowInstanceStoreForTest(t, db)
	ctx := sqliteExactOnceRunContext(t, db)
	pc, bus := newSQLiteDynamicActivationCoordinator(t, db, workflowStore)
	runID := runtimecorrelation.RunIDFromContext(ctx)
	entityID := uuid.NewString()
	if err := workflowStore.create(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID: runID, StorageRef: runID, EntityID: entityID, EntityType: "parent",
		WorkflowName: "root", WorkflowVersion: "v-test", CurrentState: "pending", Fields: map[string]any{},
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	})); err != nil {
		t.Fatalf("seed nested fan-out parent: %v", err)
	}
	parent := eventtest.RunCreatingRootIngress(
		uuid.NewString(), events.EventType("component_scaffold.batch_requested"), "", "",
		mustJSON(map[string]any{"components": []any{map[string]any{"component_id": "component-a"}}}),
		0, runID, "", events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), runID), time.Now().UTC(),
	)
	parentNode := pipelineSourceNode(t, pc.SemanticSource(), "", "fanout-node")
	parentRoute := seedExactOnceEventDelivery(t, pc, ctx, parent, parentNode)
	if handled, err := pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(ctx, parentRoute), parent); err != nil || !handled {
		t.Fatalf("dispatch parent fan-out = handled:%v err:%v", handled, err)
	}
	parentIntent := loadSQLiteFanOutIntentForTrigger(t, ctx, db, parent, parentNode)
	parentOwner := &singleTurnFanOutOwner{intent: parentIntent, input: FanOutEvaluationInput{
		StartOrdinal: 0,
		Items:        []any{map[string]any{"component_id": "component-a"}},
		Trigger:      parent,
	}}
	pc.bundleSourceFact = mustPipelineTestBundleSourceFact(parentIntent.Request.PlanRef.BundleHash)
	pc.workflowStore.fanOutObligations = parentOwner
	if more, err := pc.serveFanOutTurn(ctx, time.Now().UTC()); err != nil || more {
		t.Fatalf("serve parent fan-out = more:%v err:%v", more, err)
	}
	if bus.publishedCount() != 1 {
		t.Fatalf("parent fan-out publications = %d, want 1", bus.publishedCount())
	}
	child := bus.publishedEvent(0)
	var childPayload map[string]any
	if err := json.Unmarshal(child.Payload(), &childPayload); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(childPayload["nested_items"], []any{"prepare", "publish"}) {
		t.Fatalf("parent publication nested items = %#v", childPayload["nested_items"])
	}

	nestedNode := pipelineSourceNode(t, pc.SemanticSource(), "", "nested-fanout-node")
	nestedRoute := seedExactOnceEventDelivery(t, pc, ctx, child, nestedNode)
	if handled, err := pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(ctx, nestedRoute), child); err != nil || !handled {
		t.Fatalf("dispatch nested fan-out = handled:%v err:%v", handled, err)
	}
	var intentCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=?`, runID).Scan(&intentCount); err != nil {
		t.Fatal(err)
	}
	if intentCount != 2 {
		t.Fatalf("nested durable intent count = %d, want 2 direct obligations", intentCount)
	}
	nestedIntent := loadSQLiteFanOutIntentForTrigger(t, ctx, db, child, nestedNode)
	if nestedIntent.Request.Key == parentIntent.Request.Key || nestedIntent.Request.Key.TriggeringDeliveryID == parentIntent.Request.Key.TriggeringDeliveryID || nestedIntent.Request.Key.ElementRef == parentIntent.Request.Key.ElementRef {
		t.Fatalf("nested intent collapsed into parent identity: parent=%#v nested=%#v", parentIntent.Request.Key, nestedIntent.Request.Key)
	}
	nestedOwner := &singleTurnFanOutOwner{intent: nestedIntent, input: FanOutEvaluationInput{
		StartOrdinal: 0,
		Items:        []any{"prepare", "publish"},
		Trigger:      child,
	}}
	pc.workflowStore.fanOutObligations = nestedOwner
	if more, err := pc.serveFanOutTurn(ctx, time.Now().UTC()); err != nil || more {
		t.Fatalf("serve nested fan-out = more:%v err:%v", more, err)
	}
	if bus.publishedCount() != 3 {
		t.Fatalf("nested fan-out publication count = %d, want parent plus two nested", bus.publishedCount())
	}
	for index, task := range []string{"prepare", "publish"} {
		evt := bus.publishedEvent(index + 1)
		var payload map[string]any
		if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
			t.Fatal(err)
		}
		if evt.Type() != events.EventType("component_scaffold.task_requested") || evt.ParentEventID() != child.ID() || evt.RunID() != runID || payload["component_id"] != "component-a" || payload["task"] != task {
			t.Fatalf("nested publication %d = type:%s lineage:%s/%s payload:%#v", index, evt.Type(), evt.RunID(), evt.ParentEventID(), payload)
		}
	}
}

func loadSQLiteFanOutIntentForTrigger(t *testing.T, ctx context.Context, db *sql.DB, trigger events.Event, node identity.ExecutableNode) fanoutobligation.Intent {
	t.Helper()
	var deliveryID, packageKey, elementID, bundleHash, semanticDigest, sourceKind, sourceEventID, sourceField string
	var cardinality int
	var capsuleRaw []byte
	if err := db.QueryRowContext(ctx, `
		SELECT i.triggering_delivery_id,i.package_key,i.element_id,i.bundle_hash,i.semantic_digest,
		       i.source_kind,i.source_event_id,i.source_field,i.cardinality,i.capsule
		FROM fan_out_intents i
		JOIN event_deliveries d ON d.delivery_id=i.triggering_delivery_id
		WHERE i.run_id=? AND d.event_id=? AND d.subscriber_id=?
	`, trigger.RunID(), trigger.ID(), node.Key()).Scan(&deliveryID, &packageKey, &elementID, &bundleHash, &semanticDigest, &sourceKind, &sourceEventID, &sourceField, &cardinality, &capsuleRaw); err != nil {
		t.Fatalf("load fan-out intent for trigger %s: %v", trigger.ID(), err)
	}
	var capsule fanoutobligation.Capsule
	if err := json.Unmarshal(capsuleRaw, &capsule); err != nil {
		t.Fatalf("decode fan-out capsule for trigger %s: %v", trigger.ID(), err)
	}
	ref := runtimecontracts.FanOutElementRef{PackageKey: packageKey, ElementID: elementID}
	source := fanoutobligation.SourceRef{Kind: fanoutobligation.SourceKind(sourceKind), EventID: sourceEventID, Field: sourceField}
	return fanoutobligation.Intent{
		Request: fanoutobligation.IntentRequest{
			Key:     fanoutobligation.IntentKey{RunID: trigger.RunID(), TriggeringDeliveryID: deliveryID, ElementRef: ref},
			PlanRef: runtimecontracts.FanOutPlanRef{BundleHash: bundleHash, ElementRef: ref, SemanticDigest: semanticDigest},
			Source:  source, Cardinality: cardinality, Capsule: capsule,
		},
		Source: source, Status: fanoutobligation.StatusOpen, NextChunkSize: fanoutobligation.InitialChunkSize,
		CreatedAt: trigger.CreatedAt(), UpdatedAt: trigger.CreatedAt(),
	}
}

type lostWakeFanOutOwner struct {
	*singleTurnFanOutOwner
	ready         chan struct{}
	probed        chan struct{}
	committed     chan struct{}
	probeOnce     sync.Once
	committedOnce sync.Once
}

func newLostWakeFanOutOwner(owner *singleTurnFanOutOwner) *lostWakeFanOutOwner {
	return &lostWakeFanOutOwner{
		singleTurnFanOutOwner: owner,
		ready:                 make(chan struct{}),
		probed:                make(chan struct{}),
		committed:             make(chan struct{}),
	}
}

func (o *lostWakeFanOutOwner) ClaimFanOutIntent(ctx context.Context, request FanOutClaimRequest) (fanoutobligation.Intent, fanoutobligation.Claim, bool, error) {
	select {
	case <-o.ready:
		return o.singleTurnFanOutOwner.ClaimFanOutIntent(ctx, request)
	default:
		o.probeOnce.Do(func() { close(o.probed) })
		return fanoutobligation.Intent{}, fanoutobligation.Claim{}, false, nil
	}
}

func (o *lostWakeFanOutOwner) CommitFanOutChunk(ctx context.Context, command FanOutChunkCommand) (CommittedFanOutChunk, error) {
	committed, err := o.singleTurnFanOutOwner.CommitFanOutChunk(ctx, command)
	if err == nil {
		o.committedOnce.Do(func() { close(o.committed) })
	}
	return committed, err
}

type singleTurnFanOutOwner struct {
	intent      fanoutobligation.Intent
	input       FanOutEvaluationInput
	claim       fanoutobligation.Claim
	claimed     bool
	commitCalls int
}

func (o *singleTurnFanOutOwner) ClaimFanOutIntent(_ context.Context, request FanOutClaimRequest) (fanoutobligation.Intent, fanoutobligation.Claim, bool, error) {
	if err := request.Validate(); err != nil {
		return fanoutobligation.Intent{}, fanoutobligation.Claim{}, false, err
	}
	if o.claimed || request.BundleHash != o.intent.Request.PlanRef.BundleHash {
		return fanoutobligation.Intent{}, fanoutobligation.Claim{}, false, nil
	}
	o.claimed = true
	o.claim = fanoutobligation.Claim{Key: o.intent.Request.Key, Owner: request.Owner, Generation: 1, LeaseUntil: request.Now.Add(request.Lease)}
	return o.intent, o.claim, true, nil
}

func (o *singleTurnFanOutOwner) LoadFanOutEvaluation(_ context.Context, claim fanoutobligation.Claim) (FanOutEvaluationInput, error) {
	if claim != o.claim {
		return FanOutEvaluationInput{}, fanoutobligation.ErrStaleClaim
	}
	return o.input, nil
}

func (o *singleTurnFanOutOwner) CommitFanOutChunk(_ context.Context, command FanOutChunkCommand) (CommittedFanOutChunk, error) {
	if err := command.Validate(); err != nil {
		return CommittedFanOutChunk{}, err
	}
	if command.Claim != o.claim || len(command.Outcomes) != len(o.input.Items) {
		return CommittedFanOutChunk{}, fanoutobligation.ErrStaleClaim
	}
	committed := CommittedFanOutChunk{}
	for index, outcome := range command.Outcomes {
		wantOrdinal := o.input.StartOrdinal + index
		if outcome.Ordinal != wantOrdinal || outcome.Publication == nil {
			return CommittedFanOutChunk{}, fmt.Errorf("fan-out outcome %d did not preserve exact ordinal %d publication", index, wantOrdinal)
		}
		plan, ok := outcome.Publication.(pipelineTestPublicationPlan)
		if !ok {
			return CommittedFanOutChunk{}, fmt.Errorf("fan-out publication %d has unexpected type %T", index, outcome.Publication)
		}
		committed.Publications = append(committed.Publications, pipelineTestCommittedPublication{eventID: plan.DurablePublicationEventID(), intent: plan.intent})
	}
	o.commitCalls++
	o.intent.Cursor = o.intent.Request.Cardinality
	o.intent.Status = fanoutobligation.StatusClosed
	o.intent.LastServedAt = command.Now
	o.intent.UpdatedAt = command.Now
	committed.Intent = o.intent
	return committed, nil
}

func (*singleTurnFanOutOwner) ReleaseFanOutClaim(context.Context, fanoutobligation.Claim) error {
	return fmt.Errorf("successful fan-out turn must not release its committed claim")
}

func (*singleTurnFanOutOwner) ReleaseFanOutRetryable(context.Context, FanOutRetryableRelease) error {
	return fmt.Errorf("successful fan-out turn must not enter retry release")
}

func (*singleTurnFanOutOwner) BlockFanOutClaim(context.Context, FanOutBlockRequest) error {
	return fmt.Errorf("successful fan-out turn must not block")
}

func (*singleTurnFanOutOwner) CancelRunFanOut(context.Context, string, string, time.Time) error {
	return fmt.Errorf("fan-out cancellation is outside the successful pump proof")
}

func (o *singleTurnFanOutOwner) FanOutRunSummary(context.Context, string, time.Time) (fanoutobligation.RunSummary, error) {
	return fanoutobligation.RunSummary{RunID: o.intent.Request.Key.RunID}, nil
}

func newSQLiteDynamicActivationCoordinator(t *testing.T, db *sql.DB, workflowStore *workflowInstanceStore) (*PipelineCoordinator, *recordingPipelineBus) {
	t.Helper()
	bus := &recordingPipelineBus{}
	bundle := sqliteDynamicActivationBundle(t)
	deliveryStore := newPipelineTestDeliveryOwnerForDB(t, db)
	bus.configurePipelineTestDeliveryOwner(deliveryStore)
	pc := newDurablePipelineCoordinatorForTest(bus, db, PipelineCoordinatorOptions{
		Persistence:         workflowPersistenceForTest(workflowStore),
		DeliveryStore:       deliveryStore,
		DeliveryRuntime:     bus,
		PipelineObligations: unavailablePipelineTestObligationOwner{},
		InstanceActivator: func(ctx context.Context, req FlowInstanceActivationRequest) error {
			err := workflowStore.create(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID:         strings.TrimSpace(req.Instance.InstanceID),
				StorageRef:         strings.TrimSpace(req.Instance.InstancePath),
				EntityID:           strings.TrimSpace(req.Instance.EntityID),
				InstanceKind:       "dynamic_flow",
				ParentFlowID:       strings.TrimSpace(req.Instance.ParentRoute.FlowID),
				ParentFlowInstance: strings.TrimSpace(req.Instance.ParentRoute.FlowInstance),
				ParentEntityID:     strings.TrimSpace(req.Instance.ParentEntityID),
				WorkflowName:       strings.TrimSpace(req.Instance.TemplateID),
				WorkflowVersion:    "v-test",
				CurrentState:       "pending",
				Config:             cloneStringAnyMap(req.Config),
				Fields:             map[string]any{"component_id": req.Config["component_id"]},
				Bookkeeping:        map[string]any{"last_source_event": strings.TrimSpace(req.TriggerEvent.ID())},
				CreatedAt:          time.Now().UTC(),
				UpdatedAt:          time.Now().UTC(),
				EntityType:         "test_entity",
			}))
			if err != nil {
				return fmt.Errorf("activate %s entity %s: %w", req.Instance.InstancePath, req.Instance.EntityID, err)
			}
			return nil
		},
		Module: &previewWorkflowModule{
			bundle: bundle,
			workflow: NewWorkflowDefinition("root", []WorkflowStage{
				{Name: "pending"},
			}, nil),
			workflowNodes: []WorkflowNode{
				{
					Node:          pipelineNode(t, "", "fanout-node"),
					Subscriptions: []events.EventType{"component_scaffold.batch_requested"},
					Produces:      []events.EventType{"component_scaffold.spawn_requested"},
				},
				{
					Node:          pipelineNode(t, "", "spawn-node"),
					Subscriptions: []events.EventType{"component_scaffold.spawn_requested"},
				},
				{
					Node:          pipelineNode(t, "", "nested-fanout-node"),
					Subscriptions: []events.EventType{"component_scaffold.spawn_requested"},
					Produces:      []events.EventType{"component_scaffold.task_requested"},
				},
			},
		},
	})
	configurePipelineTestDeliveryOwner(t, pc)
	return pc, bus
}

func sqliteDynamicActivationBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	reviewFlow := &runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "review"},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"fanout-node":        {ID: "fanout-node", ExecutionType: "system_node"},
			"spawn-node":         {ID: "spawn-node", ExecutionType: "system_node"},
			"nested-fanout-node": {ID: "nested-fanout-node", ExecutionType: "system_node"},
		},
		FlowTree: runtimecontracts.FlowTree{
			Root: &runtimecontracts.FlowContractView{
				Children: []runtimecontracts.FlowContractView{*reviewFlow},
				Events: map[string]runtimecontracts.EventCatalogEntry{
					"component_scaffold.batch_requested": {
						Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
							"components": {Type: "[json]"},
						}},
					},
					"component_scaffold.spawn_requested": {
						Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
							"component_id": {Type: "text"},
							"nested_items": {Type: "[text]"},
						}},
					},
					"component_scaffold.task_requested": {
						Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
							"component_id": {Type: "text"},
							"task":         {Type: "text"},
						}},
					},
				},
			},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"review": reviewFlow,
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"review": {
				Name:         "review",
				Mode:         "template",
				InitialState: "pending",
				States:       []string{"pending"},
				Pins: runtimecontracts.FlowPins{
					Inputs: runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{{Event: "component_scaffold.spawn_requested"}}},
				},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name: "root", Version: "v-test",
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"fanout-node": {
					"component_scaffold.batch_requested": {
						FanOut: &runtimecontracts.FanOutSpec{
							ItemsFrom: "payload.components",
							As:        "component",
							Identity:  "component.component_id",
							Emit: runtimecontracts.EmitSpec{
								Event: "component_scaffold.spawn_requested",
								Fields: map[string]runtimecontracts.ExpressionValue{
									"component_id": runtimecontracts.CELExpression("component.component_id"),
									"nested_items": runtimecontracts.LiteralExpression([]any{"prepare", "publish"}),
								},
							},
						},
					},
				},
				"spawn-node": {
					"component_scaffold.spawn_requested": {
						Action: runtimecontracts.ActionSpec{
							ID:             "create_flow_instance",
							Template:       "review",
							InstanceIDFrom: "payload.component_id",
							ConfigFrom: &runtimecontracts.ConfigFromSpec{
								Bindings: map[string]string{
									"component_id": "payload.component_id",
								},
							},
						},
					},
				},
				"nested-fanout-node": {
					"component_scaffold.spawn_requested": {
						FanOut: &runtimecontracts.FanOutSpec{
							ItemsFrom: "payload.nested_items",
							As:        "nested_item",
							Identity:  "nested_item",
							Emit: runtimecontracts.EmitSpec{
								Event: "component_scaffold.task_requested",
								Fields: map[string]runtimecontracts.ExpressionValue{
									"component_id": runtimecontracts.CELExpression("payload.component_id"),
									"task":         runtimecontracts.CELExpression("nested_item"),
								},
							},
						},
					},
				},
			},
		},
	}
	elementID := contractelementidentity.MintContractElementID()
	fanOutHandler := bundle.Semantics.NodeHandlers["fanout-node"]["component_scaffold.batch_requested"]
	fanOutHandler.FanOut.ElementID = elementID
	fanOutOwner, err := identity.AdmitExecutableNodeDeclaration(identity.RootPackageKey, "", "fanout-node")
	if err != nil {
		t.Fatal(err)
	}
	fanOutHandler, err = runtimecontracts.QualifySystemNodeHandlerRuleRefs(fanOutOwner, fanOutHandler)
	if err != nil {
		t.Fatal(err)
	}
	bundle.Semantics.NodeHandlers["fanout-node"]["component_scaffold.batch_requested"] = fanOutHandler
	nestedElementID := contractelementidentity.MintContractElementID()
	nestedHandler := bundle.Semantics.NodeHandlers["nested-fanout-node"]["component_scaffold.spawn_requested"]
	nestedHandler.FanOut.ElementID = nestedElementID
	nestedOwner, err := identity.AdmitExecutableNodeDeclaration(identity.RootPackageKey, "", "nested-fanout-node")
	if err != nil {
		t.Fatal(err)
	}
	nestedHandler, err = runtimecontracts.QualifySystemNodeHandlerRuleRefs(nestedOwner, nestedHandler)
	if err != nil {
		t.Fatal(err)
	}
	bundle.Semantics.NodeHandlers["nested-fanout-node"]["component_scaffold.spawn_requested"] = nestedHandler
	root := t.TempDir()
	packageFile := filepath.Join(root, "package.yaml")
	platformFile := filepath.Join(root, "platform-spec.yaml")
	if err := os.WriteFile(packageFile, []byte("name: sqlite-dynamic-activation-test\nversion: 1.0.0\nflows: []\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(platformFile, []byte("version: 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	bundle.Paths = runtimecontracts.ContractPaths{ContractsRoot: root, ProjectPackageFile: packageFile, PlatformSpecFile: platformFile}
	for _, nodeID := range []string{"fanout-node", "spawn-node", "nested-fanout-node"} {
		node := bundle.Nodes[nodeID]
		node.EventHandlers = bundle.Semantics.NodeHandlers[nodeID]
		bundle.Nodes[nodeID] = node
	}
	bundle.FlowTree.Root.Nodes = bundle.Nodes
	bundle.Events = bundle.FlowTree.Root.Events
	return admitSyntheticEntityContractsForTest(t, bundle, "parent", map[string]string{"review": "test_entity"})
}

func assertSQLiteWorkflowInstancePersisted(t *testing.T, store *workflowInstanceStore, ctx context.Context, storageRef string) {
	t.Helper()
	instance, ok, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, storageRef))
	if err != nil {
		t.Fatalf("load workflow instance %s: %v", storageRef, err)
	}
	if !ok || strings.TrimSpace(instance.StorageRef) != storageRef {
		t.Fatalf("workflow instance %s loaded=%v value=%+v", storageRef, ok, instance)
	}
}
