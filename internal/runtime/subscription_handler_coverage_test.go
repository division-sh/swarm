package runtime

import (
	"context"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestHandler_scoring_node_vertical_discovered(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	node := NewScoringNode(bus, pc, db)
	if node == nil {
		t.Fatal("expected scoring node")
	}

	ch := bus.Subscribe("analysis-agent", events.EventType("scoring.requested"))
	verticalID := uuid.NewString()
	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.discovered"),
		SourceAgent: "pipeline-coordinator",
		VerticalID:  verticalID,
		Payload: mustJSON(map[string]any{
			"vertical_id":   verticalID,
			"vertical_name": "Handler Coverage Vertical",
			"geography":     "argentina",
			"mode":          "saas_gap",
			"discovery_context": map[string]any{
				"opportunity_name": "Handler Coverage Vertical",
				"preliminary_icp":  "Operations manager handling compliance workflows",
			},
		}),
		CreatedAt: time.Now().UTC(),
	}
	insertEventForScoringNodeLedger(t, db, evt)
	node.ProcessEventForTest(context.Background(), evt)
	_ = waitForEventType(t, ch, "scoring.requested")
}

func TestHandler_scoring_node_vertical_derived(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	node := NewScoringNode(bus, pc, db)
	if node == nil {
		t.Fatal("expected scoring node")
	}

	parentID := uuid.NewString()
	insertTestVertical(t, db, parentID, "Parent Derived Handler Coverage", "argentina")

	discovered := bus.Subscribe("handler-coverage-derived", events.EventType("vertical.discovered"))
	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.derived"),
		SourceAgent: "analysis-agent",
		VerticalID:  parentID,
		Payload: mustJSON(map[string]any{
			"parent_id":              parentID,
			"generation_depth":       1,
			"generator_agent_id":     "analysis-agent",
			"derivation_rationale":   map[string]any{"summary": "Derived handler coverage"},
			"opportunity_name":       "Derived Handler Coverage Opportunity",
			"signal_strength":        72,
			"geography":              "argentina",
			"discovery_context":      map[string]any{"source": "handler-test"},
			"preliminary_icp":        "Operations manager at SMB clinics handling invoice compliance workflow",
			"opportunity_hypothesis": "Automate compliance reporting queues with recurring workflow embedding",
			"retention_primitives":   []any{"workflow_embedding"},
			"build_sketch": map[string]any{
				"core_features":        []any{"parser", "exception queue"},
				"key_integrations":     []any{"quickbooks"},
				"red_flags":            []any{},
				"retention_primitives": []any{"workflow_embedding"},
			},
			"evidence": map[string]any{
				"competitors": []any{
					map[string]any{"name": "CompA", "pricing": "$99", "source_url": "https://example.com/compa"},
				},
				"pain_signals": []any{
					map[string]any{"signal": "manual rework", "source_url": "https://example.com/pain"},
				},
				"buyer_communities": []any{
					map[string]any{"name": "Community", "source_url": "https://example.com/community"},
				},
				"regulatory": []any{
					map[string]any{"detail": "compliance cadence", "source_url": "https://example.com/reg"},
				},
			},
		}),
		CreatedAt: time.Now().UTC(),
	}
	insertEventForScoringNodeLedger(t, db, evt)
	node.ProcessEventForTest(ctx, evt)
	_ = waitForEventType(t, discovered, "vertical.discovered")
}

func TestHandler_scoring_node_score_dimension_complete(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	node := NewScoringNode(bus, pc, db)
	if node == nil {
		t.Fatal("expected scoring node")
	}

	verticalID := uuid.NewString()
	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("score.dimension_complete"),
		SourceAgent: "analysis-agent",
		VerticalID:  verticalID,
		Payload: mustJSON(map[string]any{
			"vertical_id": verticalID,
			"dimension":   "competition_gap",
			"score":       71,
			"evidence":    "coverage evidence",
			"confidence":  "medium",
		}),
		CreatedAt: time.Now().UTC(),
	}
	insertEventForScoringNodeLedger(t, db, evt)
	node.ProcessEventForTest(context.Background(), evt)

	pc.mu.Lock()
	acc := pc.scoring[verticalID]
	pc.mu.Unlock()
	if acc == nil {
		t.Fatal("expected scoring accumulator to be created")
	}
	got, ok := acc.Received["competition_gap"]
	if !ok || got.Score != 71 {
		t.Fatalf("expected competition_gap score=71 in accumulator, got=%+v ok=%v", got, ok)
	}
}

func TestHandler_scoring_node_scoring_contest_resolved(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	node := NewScoringNode(bus, pc, db)
	if node == nil {
		t.Fatal("expected scoring node")
	}

	verticalID := uuid.NewString()
	pc.mu.Lock()
	pc.scoring[verticalID] = &scoringAccumulator{
		VerticalID: verticalID,
		Rubric:     "universal",
		Expected:   expectedScoringDimensions("universal"),
		Received:   map[string]scoreDimensionResult{},
		Contested: map[string]contestedDimension{
			"competition_gap": {
				Dimension: "competition_gap",
				Scores:    []int{42, 78},
				Evidence:  []string{"score 42", "score 78"},
				Spread:    36,
			},
		},
		ContestNotified: map[string]bool{"competition_gap": true},
		RequestedAt:     time.Now().UTC(),
		LastUpdatedAt:   time.Now().UTC(),
	}
	pc.mu.Unlock()

	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("scoring.contest_resolved"),
		SourceAgent: "empire-coordinator",
		VerticalID:  verticalID,
		Payload: mustJSON(map[string]any{
			"vertical_id":     verticalID,
			"dimension":       "competition_gap",
			"resolved_score":  73,
			"reasoning":       "resolved in handler coverage test",
			"resolution_type": "median",
		}),
		CreatedAt: time.Now().UTC(),
	}
	insertEventForScoringNodeLedger(t, db, evt)
	node.ProcessEventForTest(context.Background(), evt)

	pc.mu.Lock()
	acc := pc.scoring[verticalID]
	_, stillContested := acc.Contested["competition_gap"]
	got := acc.Received["competition_gap"]
	pc.mu.Unlock()
	if stillContested {
		t.Fatal("expected contest dimension to be cleared")
	}
	if got.Score != 73 {
		t.Fatalf("expected resolved score=73, got=%+v", got)
	}
}
