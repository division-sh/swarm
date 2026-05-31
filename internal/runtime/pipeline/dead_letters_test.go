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

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimedestructivereset "swarm/internal/runtime/destructivereset"
	runtimerterr "swarm/internal/runtime/rterrors"
	"swarm/internal/testutil"
)

type eventReceiptsCapabilityStub struct {
	enabled bool
	err     error
}

func (s eventReceiptsCapabilityStub) resolve(context.Context) (bool, error) {
	return s.enabled, s.err
}

type typedSystemNodeReceiptStore struct {
	processed bool
	marked    int
}

func (s *typedSystemNodeReceiptStore) SystemNodeProcessed(context.Context, string, string) (bool, error) {
	return s.processed, nil
}

func (*typedSystemNodeReceiptStore) SystemNodeDeliveryQuiesced(context.Context, string, string) (bool, error) {
	return false, nil
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
		return errors.New("boom")
	})
	runner.SetRetryPolicyForTest(2, func(int) time.Duration { return 0 })

	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        "source.evt",
		SourceAgent: "src",
		Payload:     []byte(`{"entity_id":"` + uuid.NewString() + `"}`),
		CreatedAt:   time.Now().UTC(),
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at)
		VALUES ($1::uuid, $2, NULLIF($3,'')::uuid, 'runtime', 'entity', $4::jsonb, 'src', 'agent', now())
	`, evt.ID, string(evt.Type), evt.EntityID(), string(evt.Payload)); err != nil {
		t.Fatalf("seed event: %v", err)
	}

	runner.ProcessEventForTest(ctx, evt)

	var (
		failureType string
		retryCount  int
		handlerNode string
	)
	if err := db.QueryRowContext(ctx, `
		SELECT failure_type, retry_count, COALESCE(handler_node, '')
		FROM dead_letters
		WHERE original_event_id = $1::uuid
	`, evt.ID).Scan(&failureType, &retryCount, &handlerNode); err != nil {
		t.Fatalf("query dead_letters: %v", err)
	}
	if failureType != "retry_exhausted" || retryCount != 2 || handlerNode != "node-a" {
		t.Fatalf("dead_letter row = type=%q retry=%d handler=%q", failureType, retryCount, handlerNode)
	}
}

func TestCoordinator_RecordsChainDepthDeadLetterRow(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	entityID := uuid.NewString()
	evt := (events.Event{
		ID:          uuid.NewString(),
		Type:        "chain.start",
		SourceAgent: "src",
		RunID:       testPipelineRunID,
		Payload:     []byte(`{}`),
		CreatedAt:   time.Now().UTC(),
		ChainDepth:  5,
	}).WithEntityID(entityID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at)
		VALUES ($1::uuid, $2, $3::uuid, 'runtime', 'entity', $4::jsonb, 'src', 'agent', now())
	`, evt.ID, string(evt.Type), entityID, string(evt.Payload)); err != nil {
		t.Fatalf("seed event: %v", err)
	}

	repoRoot := contractComplianceRepoRoot(t)
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier6-event-loop", "test-chain-depth-limit")
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
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

	if handled := pc.executeNodeHandlerPlan(ctx, "node-1", evt); !handled {
		t.Fatalf("executeNodeHandlerPlan handled = false, want true")
	}

	var (
		failureType string
		chainDepth  int
		handlerNode string
	)
	if err := db.QueryRowContext(ctx, `
		SELECT failure_type, chain_depth, COALESCE(handler_node, '')
		FROM dead_letters
		WHERE original_event_id = $1::uuid
	`, evt.ID).Scan(&failureType, &chainDepth, &handlerNode); err != nil {
		t.Fatalf("query dead_letters: %v", err)
	}
	if failureType != "chain_depth_exceeded" || chainDepth != 6 || !strings.HasPrefix(handlerNode, "node-1") {
		t.Fatalf("dead_letter row = type=%q chain_depth=%d handler=%q", failureType, chainDepth, handlerNode)
	}
}

func TestSystemNodeRunner_SkipsQuiescedDestructiveResetDelivery(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testPipelineRunContext(t, db)
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			event_id, run_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'source.evt', 'global', '{}'::jsonb, 'src', 'agent', now()
		)
	`, eventID, testPipelineRunID); err != nil {
		t.Fatalf("seed event: %v", err)
	}
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

	runner.ProcessEventForTest(ctx, events.Event{ID: eventID, RunID: testPipelineRunID, Type: "source.evt", Payload: []byte(`{}`), CreatedAt: time.Now().UTC()})

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
	runner := newSystemNodeRunner("node-a", bus, nil, func() []events.EventType { return []events.EventType{"source.evt"} }, func(context.Context, events.Event) error {
		attempts++
		return runtimerterr.NewRuntimeError("invalid_contract", "pipeline", "node.handle", false, "bad handler config")
	})
	runner.SetRetryPolicyForTest(5, func(int) time.Duration { return 0 })

	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        "source.evt",
		SourceAgent: "src",
		Payload:     []byte(`{"entity_id":"ent-1"}`),
		CreatedAt:   time.Now().UTC(),
	}
	runner.ProcessEventForTest(context.Background(), evt)

	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
	published := bus.published()
	if len(published) != 1 {
		t.Fatalf("published dead letters = %d, want 1", len(published))
	}
	if published[0].Type != "platform.dead_letter" {
		t.Fatalf("event type = %q, want platform.dead_letter", published[0].Type)
	}
	var payload map[string]any
	if err := json.Unmarshal(published[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got := strings.TrimSpace(asString(payload["failure_type"])); got != "handler_error" {
		t.Fatalf("failure_type = %q, want handler_error", got)
	}
	if got := asInt(payload["retry_count"]); got != 0 {
		t.Fatalf("retry_count = %d, want 0", got)
	}
}

func TestSystemNodeRunner_FailsClosedWithoutCanonicalEventReceiptsCapability(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	entityID := uuid.NewString()
	evt := (events.Event{
		ID:          uuid.NewString(),
		Type:        "source.evt",
		SourceAgent: "src",
		Payload:     []byte(`{"entity_id":"` + entityID + `"}`),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(entityID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at)
		VALUES ($1::uuid, $2, $3::uuid, 'runtime', 'entity', $4::jsonb, 'src', 'agent', now())
	`, evt.ID, string(evt.Type), entityID, string(evt.Payload)); err != nil {
		t.Fatalf("seed event: %v", err)
	}

	runner := newSystemNodeRunner("node-a", deadLetterTestBus{}, db, func() []events.EventType { return []events.EventType{"source.evt"} }, func(context.Context, events.Event) error {
		return nil
	}, eventReceiptsCapabilityStub{}.resolve)
	runner.ProcessEventForTest(ctx, evt)

	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid`, evt.ID).Scan(&count); err != nil {
		t.Fatalf("count event_receipts: %v", err)
	}
	if count != 0 {
		t.Fatalf("event_receipts rows = %d, want 0 without canonical capability", count)
	}
}

func TestSystemNodeRunner_UsesCanonicalEventReceiptsCapabilityForIdempotency(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	entityID := uuid.NewString()
	evt := (events.Event{
		ID:          uuid.NewString(),
		Type:        "source.evt",
		SourceAgent: "src",
		Payload:     []byte(`{"entity_id":"` + entityID + `"}`),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(entityID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at)
		VALUES ($1::uuid, $2, $3::uuid, 'runtime', 'entity', $4::jsonb, 'src', 'agent', now())
	`, evt.ID, string(evt.Type), entityID, string(evt.Payload)); err != nil {
		t.Fatalf("seed event: %v", err)
	}

	attempts := 0
	runner := newSystemNodeRunner("node-a", deadLetterTestBus{}, db, func() []events.EventType { return []events.EventType{"source.evt"} }, func(context.Context, events.Event) error {
		attempts++
		return nil
	}, eventReceiptsCapabilityStub{enabled: true}.resolve)
	runner.ProcessEventForTest(ctx, evt)
	runner.ProcessEventForTest(ctx, evt)

	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1 after idempotent receipt", attempts)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid AND subscriber_id = 'node-a'`, evt.ID).Scan(&count); err != nil {
		t.Fatalf("count event_receipts: %v", err)
	}
	if count != 1 {
		t.Fatalf("event_receipts rows = %d, want 1", count)
	}
}

func TestSystemNodeRunner_UsesTypedReceiptOwnerWithoutRawDB(t *testing.T) {
	ctx := context.Background()
	receipts := &typedSystemNodeReceiptStore{}
	attempts := 0
	runner := newSystemNodeRunnerWithReceiptStoreAndRetryBase("node-a", deadLetterTestBus{}, nil, receipts, func() []events.EventType {
		return []events.EventType{"source.evt"}
	}, func(context.Context, events.Event) error {
		attempts++
		return nil
	}, 0, eventReceiptsCapabilityStub{enabled: true}.resolve)
	evt := events.Event{
		ID:        uuid.NewString(),
		Type:      "source.evt",
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}

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
	evt := (events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("score.dimension_complete"),
		SourceAgent: "analysis-agent",
		Payload:     []byte(`{"entity_id":"` + uuid.NewString() + `","dimension":"expansion_potential","score":74,"tier":3}`),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(uuid.NewString())
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO events (event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at)
		VALUES ($1::uuid, $2, NULLIF($3,'')::uuid, 'runtime', 'entity', $4::jsonb, 'analysis-agent', 'agent', now())
	`, evt.ID, string(evt.Type), evt.EntityID(), string(evt.Payload)); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	postCommit := make([]func(), 0, 1)
	override := &PipelineReceiptOverride{}
	ctx := WithPipelinePostCommitActions(context.Background(), &postCommit)
	ctx = WithPipelineReceiptOverride(ctx, override)

	passthrough, _, err := pc.Intercept(ctx, evt)
	if err != nil {
		t.Fatalf("Intercept error = %v, want nil", err)
	}
	if passthrough {
		t.Fatal("Intercept passthrough = true, want false for consumed handler error")
	}
	status, errText, ok := PipelineReceiptOverrideFromContext(ctx)
	if !ok || status != "dead_letter" || strings.TrimSpace(errText) == "" {
		t.Fatalf("receipt override = (%q, %q, %v), want dead_letter with error", status, errText, ok)
	}
	flushPipelinePostCommitActions(postCommit)

	var (
		failureType string
		handlerNode string
		errorText   string
	)
	if err := db.QueryRowContext(context.Background(), `
		SELECT failure_type, COALESCE(handler_node, ''), COALESCE(error_message, '')
		FROM dead_letters
		WHERE original_event_id = $1::uuid
	`, evt.ID).Scan(&failureType, &handlerNode, &errorText); err != nil {
		t.Fatalf("query dead_letters: %v", err)
	}
	if failureType != "handler_error" || handlerNode != "node-a" || strings.TrimSpace(errorText) == "" {
		t.Fatalf("dead_letter row = type=%q handler=%q error=%q", failureType, handlerNode, errorText)
	}
}

type deadLetterTestBus struct{}

func (deadLetterTestBus) Subscribe(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}

func (deadLetterTestBus) Publish(context.Context, events.Event) error { return nil }

type capturingDeadLetterBus struct {
	mu     sync.Mutex
	events []events.Event
}

func (b *capturingDeadLetterBus) Subscribe(string, ...events.EventType) <-chan events.Event {
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
