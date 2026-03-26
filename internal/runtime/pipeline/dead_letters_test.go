package pipeline

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/testutil"
	"github.com/google/uuid"
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
	pc := NewFactoryPipelineCoordinatorWithOptions(noopPipelineBus{}, db, FactoryPipelineCoordinatorOptions{
		Module: module,
	})

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
	if failureType != "chain_depth_exceeded" || chainDepth != 5 || handlerNode != "node-1" {
		t.Fatalf("dead_letter row = type=%q chain_depth=%d handler=%q", failureType, chainDepth, handlerNode)
	}
}

type deadLetterTestBus struct{}

func (deadLetterTestBus) Subscribe(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}

func (deadLetterTestBus) Publish(context.Context, events.Event) error { return nil }
