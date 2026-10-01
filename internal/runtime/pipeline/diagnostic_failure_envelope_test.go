package pipeline

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

// These are producer normalization controls. Native lifecycle caller and
// persisted diagnostic proof is required separately; recording is not storage.
func TestWorkflowDiagnosticProducersUseCanonicalFailureEnvelope(t *testing.T) {
	actions := []string{
		"handler_error",
		"workflow_timer_reconcile_failed",
		"workflow_timer_register_failed",
		"workflow_timer_restore_register_failed",
		"workflow_timer_reconcile_retry_failed",
		"workflow_timer_fire_failed",
		"workflow_timer_recurrence_reconcile_failed",
	}
	for _, action := range actions {
		for _, typed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/typed_%t", action, typed), func(t *testing.T) {
				bus := &recordingPipelineBus{}
				cause := errors.New("selected dependency unavailable")
				injected := error(cause)
				if typed {
					injected = fmt.Errorf("causal wrapper: %w", runtimefailures.Wrap(
						runtimefailures.ClassDependencyUnavailable, "diagnostic_selected_dependency", "selected-proof", "read",
						map[string]any{"scope": "exact"}, cause))
				}
				operation := action
				var wantDetail map[string]any
				if action == "handler_error" {
					operation = "execute_handler"
					runID, entityID := uuid.NewString(), uuid.NewString()
					envelope := events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{
						FlowID: ".", FlowInstance: runID, EntityID: entityID,
					})
					event := eventtest.ExistingRunRootIngress(uuid.NewString(), "source.evt", "fixture", "", []byte(`{}`), 0,
						runID, envelope, time.Now().UTC())
					pc := &PipelineCoordinator{bus: bus}
					pc.recordWorkflowHandlerFailure(context.Background(), event, "router", injected)
					if len(bus.runtimeLogs) != 1 {
						t.Fatalf("handler emitted %d diagnostics, want one", len(bus.runtimeLogs))
					}
					entry := bus.runtimeLogs[0]
					if entry.EventID != event.ID() || entry.EventType != string(event.Type()) || entry.EntityID != entityID {
						t.Fatal("handler lost immutable event/entity context")
					}
					wantDetail = map[string]any{"node_id": "router"}
					pc.recordWorkflowHandlerFailure(context.Background(), event, "router", nil)
				} else {
					ref := timeridentity.WorkflowTimerActivationRef{ActivationID: uuid.NewString(), DeclarationKey: "waiting.expired"}
					lifecycle := &WorkflowTimerLifecycle{logger: bus}
					lifecycle.logFailure(context.Background(), action, ref, injected)
					wantDetail = map[string]any{"activation_id": ref.ActivationID, "declaration_key": ref.DeclarationKey}
					lifecycle.logFailure(context.Background(), action, ref, nil)
				}
				if len(bus.runtimeLogs) != 1 {
					t.Fatalf("producer emitted %d diagnostics, want one; nil is not a failure", len(bus.runtimeLogs))
				}
				entry := bus.runtimeLogs[0]
				want := runtimefailures.FromError(injected, runtimeWorkflowID, operation)
				if !errors.Is(want, cause) || entry.Failure == nil || !reflect.DeepEqual(*entry.Failure, want.Failure) {
					t.Fatalf("producer lost canonical envelope/causal chain: got=%+v want=%+v", entry.Failure, want.Failure)
				}
				if typed && (entry.Failure.Class != runtimefailures.ClassDependencyUnavailable || entry.Failure.Detail.Code != "diagnostic_selected_dependency") {
					t.Fatal("producer reclassified a typed failure")
				}
				if !typed && (entry.Failure.Class != runtimefailures.ClassInternalFailure || entry.Failure.Detail.Code != "unclassified_runtime_error") {
					t.Fatal("producer did not consume canonical untyped classification")
				}
				if entry.Level != "error" || entry.Component != runtimeWorkflowID || entry.Action != action || !reflect.DeepEqual(entry.Detail, wantDetail) {
					t.Fatalf("producer changed diagnostic context or retained retired free-text fields: %+v", entry)
				}
			})
		}
	}
}
