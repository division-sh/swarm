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
	runtimerterr "swarm/internal/runtime/rterrors"
	"swarm/internal/testutil"
)

func TestSystemNodeRunner_RecordsDeadLetterRow(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
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
	if err := pc.workflowStore.Upsert(context.Background(), WorkflowInstance{
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
