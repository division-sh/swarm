package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type typedSystemNodeReceiptStore struct {
	deliveryAuthorized bool
	processed          bool
	inProgress         int
	failed             int
	deadLettered       int
	marked             int
}

func (s *typedSystemNodeReceiptStore) SystemNodeDeliveryAuthorized(context.Context, string, string, int) (bool, error) {
	return s.deliveryAuthorized, nil
}

func (s *typedSystemNodeReceiptStore) SystemNodeProcessed(context.Context, string, string) (bool, error) {
	return s.processed, nil
}

func (*typedSystemNodeReceiptStore) SystemNodeDeliveryQuiesced(context.Context, string, string) (bool, error) {
	return false, nil
}

func (s *typedSystemNodeReceiptStore) MarkSystemNodeDeliveryInProgress(context.Context, string, string, int) error {
	s.inProgress++
	return nil
}

func (s *typedSystemNodeReceiptStore) MarkSystemNodeDeliveryFailed(context.Context, string, string, string, *runtimefailures.Envelope, int, int) error {
	s.failed++
	return nil
}

func (s *typedSystemNodeReceiptStore) MarkSystemNodeDeliveryDeadLetter(context.Context, string, string, string, *runtimefailures.Envelope, int, string) error {
	s.processed = true
	s.deadLettered++
	return nil
}

func (s *typedSystemNodeReceiptStore) MarkSystemNodeProcessedAndSettleDelivery(context.Context, string, string, string) error {
	s.processed = true
	s.marked++
	return nil
}

func TestSystemNodeRunner_RecordsDeadLetterRow(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testPipelineRunContext(t, db)
	runner := newSystemNodeRunner("node-a", deadLetterTestBus{}, db, func() []events.EventType { return []events.EventType{"source.evt"} }, func(context.Context, events.Event) error {
		return runtimefailures.New(runtimefailures.ClassConnectorFailure, "test_connector_failure", "pipeline-test", "handle", nil)
	})
	runner.SetRetryPolicyForTest(2, func(int) time.Duration { return 0 })

	evt := eventtest.RootIngress(uuid.NewString(),
		"source.evt",
		"src", "", []byte(`{"entity_id":"`+uuid.NewString()+`"}`), 0, testPipelineRunID, "", events.EventEnvelope{}, time.Now().UTC())

	seedPipelineEventRecord(t, ctx, db, evt)
	seedPipelineNodeDeliveryAuthority(t, db, evt, "node-a")

	runner.ProcessEventForTest(ctx, evt)

	var (
		failureType string
		retryCount  int
		handlerNode string
	)
	if err := db.QueryRowContext(ctx, `
		SELECT failure->>'class', retry_count, COALESCE(handler_node, '')
		FROM dead_letters
		WHERE original_event_id = $1::uuid
	`, evt.ID()).Scan(&failureType, &retryCount, &handlerNode); err != nil {
		t.Fatalf("query dead_letters: %v", err)
	}
	if failureType != string(runtimefailures.ClassRetryExhausted) || retryCount != 2 || handlerNode != "node-a" {
		t.Fatalf("dead_letter row = type=%q retry=%d handler=%q", failureType, retryCount, handlerNode)
	}
	var (
		deliveryStatus  string
		deliveryReason  string
		deliveryFailure []byte
		receiptOutcome  string
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), COALESCE(reason_code, ''), failure
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'node-a'
	`, evt.ID()).Scan(&deliveryStatus, &deliveryReason, &deliveryFailure); err != nil {
		t.Fatalf("query node delivery: %v", err)
	}
	if deliveryStatus != "dead_letter" || deliveryReason != "handler_terminal_failure" || len(deliveryFailure) == 0 {
		t.Fatalf("node delivery = %s/%s failure=%s, want dead_letter/handler_terminal_failure with failure", deliveryStatus, deliveryReason, deliveryFailure)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(outcome, '')
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'node-a'
	`, evt.ID()).Scan(&receiptOutcome); err != nil {
		t.Fatalf("query node receipt: %v", err)
	}
	if receiptOutcome != "dead_letter" {
		t.Fatalf("node receipt outcome = %q, want dead_letter", receiptOutcome)
	}
}

func TestCoordinator_RecordsChainDepthDeadLetterRow(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testPipelineRunContext(t, db)
	entityID := uuid.NewString()
	evt := eventtest.RootIngress(
		uuid.NewString(),
		"chain.start",
		"src",
		"",
		[]byte(`{}`),
		5,
		testPipelineRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		time.Now().UTC(),
	)

	seedPipelineEventRecord(t, ctx, db, evt)

	repoRoot := contractComplianceRepoRoot(t)
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier6-event-loop", "test-chain-depth-limit")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	module, err := newPipelineFixtureWorkflowModule(bundle)
	if err != nil {
		t.Fatalf("newPipelineFixtureWorkflowModule: %v", err)
	}
	pc := NewPipelineCoordinatorWithOptions(noopPipelineBus{}, db, PipelineCoordinatorOptions{
		Module: module,
	})
	if err := pc.workflowStore.Upsert(testPipelineCoordinatorRunContext(t, pc), WorkflowInstance{
		InstanceID:      entityID,
		WorkflowName:    bundle.WorkflowName(),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "pending",
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	seedPipelineNodeDeliveryAuthority(t, db, evt, "node-1")

	if handled := pc.executeNodeHandlerPlan(ctx, "node-1", evt); !handled {
		t.Fatalf("executeNodeHandlerPlan handled = false, want true")
	}

	var (
		failureType string
		chainDepth  int
		handlerNode string
	)
	if err := db.QueryRowContext(ctx, `
		SELECT failure->>'class', chain_depth, COALESCE(handler_node, '')
		FROM dead_letters
		WHERE original_event_id = $1::uuid
	`, evt.ID()).Scan(&failureType, &chainDepth, &handlerNode); err != nil {
		t.Fatalf("query dead_letters: %v", err)
	}
	if failureType != string(runtimefailures.ClassChainDepthExceeded) || chainDepth != 6 || !strings.HasPrefix(handlerNode, "node-1") {
		t.Fatalf("dead_letter row = type=%q chain_depth=%d handler=%q", failureType, chainDepth, handlerNode)
	}
}

func TestSystemNodeRunner_SkipsQuiescedDestructiveResetDelivery(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testPipelineRunContext(t, db)
	eventID := uuid.NewString()
	evt := eventtest.RootIngress(eventID, "source.evt", "src", "", []byte(`{}`), 0, testPipelineRunID, "", events.EventEnvelope{}, time.Now().UTC())
	seedPipelineEventRecord(t, ctx, db, evt)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, reason_code, created_at, delivered_at
		) VALUES (
			$1::uuid, $2::uuid, 'node', 'node-a', 'dead_letter', $3, now(), now()
		)
	`, testPipelineRunID, eventID, runtimedestructivereset.QuiescenceReasonCode); err != nil {
		t.Fatalf("seed quiesced node delivery: %v", err)
	}
	bus := &capturingDeadLetterBus{}
	handled := 0
	runner := newSystemNodeRunner("node-a", bus, db, func() []events.EventType { return []events.EventType{"source.evt"} }, func(context.Context, events.Event) error {
		handled++
		return errors.New("should not run")
	})

	runner.ProcessEventForTest(ctx, evt)

	if handled != 0 {
		t.Fatalf("handler calls = %d, want 0 for quiesced delivery", handled)
	}
	if got := len(bus.published()); got != 0 {
		t.Fatalf("published events = %d, want 0 for quiesced delivery", got)
	}
	var deadLetters int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dead_letters WHERE original_event_id = $1::uuid`, eventID).Scan(&deadLetters); err != nil {
		t.Fatalf("count dead letters: %v", err)
	}
	if deadLetters != 0 {
		t.Fatalf("dead letters = %d, want 0", deadLetters)
	}
}

func TestSystemNodeRunner_NonRetryableRuntimeErrorDeadLettersImmediately(t *testing.T) {
	bus := &capturingDeadLetterBus{}
	attempts := 0
	receipts := &typedSystemNodeReceiptStore{deliveryAuthorized: true}
	runner := newSystemNodeRunnerWithReceiptStoreAndRetryBase("node-a", bus, nil, receipts, func() []events.EventType { return []events.EventType{"source.evt"} }, func(context.Context, events.Event) error {
		attempts++
		return runtimefailures.New(runtimefailures.ClassSchemaInvalid, "invalid_contract", "pipeline", "node_handle", nil)
	}, 0)
	runner.SetRetryPolicyForTest(5, func(int) time.Duration { return 0 })

	runID := uuid.NewString()
	evt := eventtest.InExecutionMode(eventtest.RootIngress(uuid.NewString(),
		"source.evt",
		"src", "task-1", []byte(`{"entity_id":"ent-1"}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC()), executionmode.Mock)

	runner.ProcessEventForTest(testAuthorActivityContext(context.Background()), evt)

	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
	published := bus.published()
	if len(published) != 1 {
		t.Fatalf("published dead letters = %d, want 1", len(published))
	}
	if published[0].Type() != "platform.dead_letter" {
		t.Fatalf("event type = %q, want platform.dead_letter", published[0].Type())
	}
	if got := published[0]; got.RunID() != runID || got.ParentEventID() != evt.ID() || got.TaskID() != "task-1" || got.ExecutionMode() != executionmode.Mock {
		t.Fatalf("dead-letter lineage = run:%q parent:%q task:%q mode:%q", got.RunID(), got.ParentEventID(), got.TaskID(), got.ExecutionMode())
	}
	var payload map[string]any
	if err := json.Unmarshal(published[0].Payload(), &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	failurePayload, _ := payload["failure"].(map[string]any)
	if got := strings.TrimSpace(asString(failurePayload["class"])); got != string(runtimefailures.ClassSchemaInvalid) {
		t.Fatalf("failure.class = %q, want %s", got, runtimefailures.ClassSchemaInvalid)
	}
	if got := asInt(payload["retry_count"]); got != 0 {
		t.Fatalf("retry_count = %d, want 0", got)
	}
	if receipts.deadLettered != 1 || receipts.marked != 0 {
		t.Fatalf("typed receipt owner deadLettered=%d marked=%d, want deadLettered=1 marked=0", receipts.deadLettered, receipts.marked)
	}
}

func TestSystemNodeRunner_UsesAdmittedEventReceiptsForIdempotency(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext(context.Background())
	entityID := uuid.NewString()
	evt := eventtest.RootIngress(
		uuid.NewString(),
		"source.evt",
		"src",
		"",
		[]byte(`{"entity_id":"`+entityID+`"}`),
		0,
		testPipelineRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		time.Now().UTC(),
	)

	seedPipelineEventRecord(t, ctx, db, evt)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (event_id, subscriber_type, subscriber_id, status, created_at)
		VALUES ($1::uuid, 'node', 'node-a', 'pending', now())
	`, evt.ID()); err != nil {
		t.Fatalf("seed node delivery: %v", err)
	}

	attempts := 0
	runner := newSystemNodeRunner("node-a", deadLetterTestBus{}, db, func() []events.EventType { return []events.EventType{"source.evt"} }, func(context.Context, events.Event) error {
		attempts++
		return nil
	})
	runner.ProcessEventForTest(ctx, evt)
	runner.ProcessEventForTest(ctx, evt)

	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1 after idempotent receipt", attempts)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid AND subscriber_id = 'node-a'`, evt.ID()).Scan(&count); err != nil {
		t.Fatalf("count event_receipts: %v", err)
	}
	if count != 1 {
		t.Fatalf("event_receipts rows = %d, want 1", count)
	}
}

func TestSystemNodeRunner_SkipsWithoutPersistedNodeDeliveryAuthority(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext(context.Background())
	entityID := uuid.NewString()
	evt := eventtest.RootIngress(
		uuid.NewString(),
		"source.evt",
		"src",
		"",
		[]byte(`{"entity_id":"`+entityID+`"}`),
		0,
		testPipelineRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		time.Now().UTC(),
	)

	seedPipelineEventRecord(t, ctx, db, evt)

	attempts := 0
	bus := &recordingPipelineBus{}
	runner := newSystemNodeRunner("node-a", bus, db, func() []events.EventType { return []events.EventType{"source.evt"} }, func(context.Context, events.Event) error {
		attempts++
		return nil
	})
	runner.ProcessEventForTest(ctx, evt)

	if attempts != 0 {
		t.Fatalf("attempts = %d, want 0 without persisted node delivery authority", attempts)
	}
	var receipts int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = $1::uuid
	`, evt.ID()).Scan(&receipts); err != nil {
		t.Fatalf("count event_receipts: %v", err)
	}
	if receipts != 0 {
		t.Fatalf("event_receipts rows = %d, want 0 without authority", receipts)
	}
	logs := bus.runtimeLogEntries()
	if len(logs) != 1 || logs[0].Action != "delivery_authority_missing" {
		t.Fatalf("runtime logs = %#v, want one delivery_authority_missing entry", logs)
	}
}

func TestSystemNodeRunner_UsesTypedReceiptOwnerWithoutRawDB(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	receipts := &typedSystemNodeReceiptStore{deliveryAuthorized: true}
	attempts := 0
	runner := newSystemNodeRunnerWithReceiptStoreAndRetryBase("node-a", deadLetterTestBus{}, nil, receipts, func() []events.EventType {
		return []events.EventType{"source.evt"}
	}, func(context.Context, events.Event) error {
		attempts++
		return nil
	}, 0)
	evt := eventtest.RootIngress(uuid.NewString(),
		"source.evt", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	runner.ProcessEventForTest(ctx, evt)
	runner.ProcessEventForTest(ctx, evt)

	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1 after typed receipt owner marks processed", attempts)
	}
	if receipts.marked != 1 {
		t.Fatalf("typed receipt marks = %d, want 1", receipts.marked)
	}
}

func TestCoordinator_InterceptHandlerErrorDoesNotSilentlyFallback(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"node-a": {
					"score.dimension_complete": {
						Rules: []runtimecontracts.HandlerRuleEntry{
							{Condition: "payload.score >=", Emit: runtimecontracts.EmitSpec{Event: "vertical.shortlisted"}},
						},
					},
				},
			},
		},
	}
	pc := NewPipelineCoordinatorWithOptions(previewBus{}, db, PipelineCoordinatorOptions{
		Module: &previewWorkflowModule{
			bundle: bundle,
			workflowNodes: []WorkflowNode{
				{
					ID: "node-a",
					Policies: map[string]WorkflowEventPolicy{
						"score.dimension_complete": {Consume: true},
					},
				},
			},
		},
	})
	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("score.dimension_complete"),
		"analysis-agent",
		"",
		[]byte(`{"entity_id":"`+uuid.NewString()+`","dimension":"expansion_potential","score":74,"tier":3}`),
		0,
		testPipelineRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, uuid.NewString()),
		time.Now().UTC(),
	)

	seedPipelineEventRecord(t, testAuthorActivityContext(context.Background()), db, evt)
	seedPipelineNodeDeliveryAuthority(t, db, evt, "node-a")
	postCommit := make([]func(), 0, 1)
	override := &PipelineReceiptOverride{}
	ctx := WithPipelinePostCommitActions(testAuthorActivityContext(context.Background()), &postCommit)
	ctx = WithPipelineReceiptOverride(ctx, override)

	passthrough, _, err := pc.Intercept(ctx, evt)
	if err != nil {
		t.Fatalf("Intercept error = %v, want nil", err)
	}
	if passthrough {
		t.Fatal("Intercept passthrough = true, want false for consumed handler error")
	}
	status, failure, ok := PipelineReceiptOverrideFromContext(ctx)
	if !ok || status != "dead_letter" || failure == nil {
		t.Fatalf("receipt override = (%q, %#v, %v), want dead_letter with failure", status, failure, ok)
	}
	flushPipelinePostCommitActions(postCommit)

	var (
		failureClass string
		handlerNode  string
	)
	if err := db.QueryRowContext(testAuthorActivityContext(context.Background()), `
		SELECT failure->>'class', COALESCE(handler_node, '')
		FROM dead_letters
		WHERE original_event_id = $1::uuid
	`, evt.ID()).Scan(&failureClass, &handlerNode); err != nil {
		t.Fatalf("query dead_letters: %v", err)
	}
	if failureClass != string(runtimefailures.ClassInternalFailure) || handlerNode != "node-a" {
		t.Fatalf("dead_letter row = class=%q handler=%q", failureClass, handlerNode)
	}
	var (
		deliveryStatus string
		deliveryReason string
		receiptOutcome string
	)
	if err := db.QueryRowContext(testAuthorActivityContext(context.Background()), `
		SELECT COALESCE(status, ''), COALESCE(reason_code, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'node-a'
	`, evt.ID()).Scan(&deliveryStatus, &deliveryReason); err != nil {
		t.Fatalf("query workflow node delivery: %v", err)
	}
	if deliveryStatus != "dead_letter" || deliveryReason != "handler_terminal_failure" {
		t.Fatalf("workflow node delivery = %s/%s, want dead_letter/handler_terminal_failure", deliveryStatus, deliveryReason)
	}
	if err := db.QueryRowContext(testAuthorActivityContext(context.Background()), `
		SELECT COALESCE(outcome, '')
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'node-a'
	`, evt.ID()).Scan(&receiptOutcome); err != nil {
		t.Fatalf("query workflow node receipt: %v", err)
	}
	if receiptOutcome != "dead_letter" {
		t.Fatalf("workflow node receipt outcome = %q, want dead_letter", receiptOutcome)
	}
}

type deadLetterTestBus struct{}

func (deadLetterTestBus) SubscribeInternal(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}

func (deadLetterTestBus) Publish(context.Context, events.Event) error { return nil }

type capturingDeadLetterBus struct {
	mu     sync.Mutex
	events []events.Event
}

func (b *capturingDeadLetterBus) SubscribeInternal(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}

func (b *capturingDeadLetterBus) Publish(_ context.Context, evt events.Event) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, evt)
	return nil
}

func (b *capturingDeadLetterBus) published() []events.Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]events.Event, len(b.events))
	copy(out, b.events)
	return out
}
