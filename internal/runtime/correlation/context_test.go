package correlation

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"time"
)

func TestWithInboundEvent_RefinesTypedRuntimeLineageSubject(t *testing.T) {
	ctx := WithRuntimeLineage(context.Background(), RuntimeLineage{
		Owner:               "owner",
		RunID:               "run-fork",
		SelectedForkContext: true,
	})
	ctx = WithInboundEvent(ctx, eventtest.RunCreatingRootIngress("evt-selected",
		events.EventType("task.assigned"), "", "", nil, 0, "run-fork", "", events.EventEnvelope{}, time.Time{}))

	lineage, ok := RuntimeLineageFromContext(ctx)
	if !ok {
		t.Fatal("expected runtime lineage")
	}
	if lineage.SubjectEventID != "evt-selected" || lineage.ParentEventID != "evt-selected" || lineage.SubjectEventType != "task.assigned" {
		t.Fatalf("lineage = %#v, want inbound subject/parent", lineage)
	}
}
