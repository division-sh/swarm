package pipeline

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestDynamicFlowCreationDispatchModeValidation(t *testing.T) {
	runID, eventID, parentID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	createdAt := time.Now().UTC()
	plan := DynamicFlowRuntimeReadinessPlan{
		Identity: flowidentity.Instance{TemplateID: "worker", ScopeKey: "worker", InstanceID: "one", InstancePath: "worker/one", EntityID: uuid.NewString(), HasStoredPath: true},
		RunID:    runID, BundleHash: sourceartifactfixture.BundleHash, WorkflowVersion: "1.0.0", ExecutionMode: executionmode.Live,
		CreationEvent: &DynamicFlowRuntimeCreationEventPlan{
			EventID: eventID, EventType: "worker/one/worker.created", RunID: runID, ParentEventID: parentID,
			ExecutionMode: executionmode.Live, Payload: []byte(`{}`), CreatedAt: createdAt,
		},
	}
	event := eventtest.ChildWithLineage(eventID, events.EventType(plan.CreationEvent.EventType), "flow-instance-activator", "", []byte(`{}`), 1,
		events.EventLineage{RunID: runID, ParentEventID: parentID, ExecutionMode: executionmode.Live}, events.EventEnvelope{}, createdAt)
	for _, mode := range []DynamicFlowRuntimeCreationDispatchMode{DynamicFlowRuntimeCreationDispatchAsync, DynamicFlowRuntimeCreationDispatchStartupRecovery, 255} {
		err := (DynamicFlowRuntimeCreationOccurrenceRequest{
			RunID: runID, InstancePath: plan.Identity.InstancePath, Plan: plan, Event: event, OccurredAt: createdAt, DispatchMode: mode,
		}).Validate()
		if mode == 255 {
			if err == nil || !strings.Contains(err.Error(), "unknown dynamic flow creation dispatch mode") {
				t.Fatalf("unknown dispatch mode was not refused: %v", err)
			}
		} else if err != nil {
			t.Fatalf("valid dispatch mode %d: %v", mode, err)
		}
	}
}
