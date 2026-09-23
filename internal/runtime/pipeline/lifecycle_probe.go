package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
)

func (pc *PipelineCoordinator) notifyTestWorkflowTerminalCommitted(ctx context.Context) {
	if pc == nil || pc.testLifecycleProbe == nil {
		return
	}
	if evt, ok := runtimecorrelation.InboundEventFromContext(ctx); ok {
		pc.testLifecycleProbe.NotifyLifecycle(ctx, lifecycleNodeSignal(runtimelifecycleprobe.WorkflowTerminalCommitted, "", evt, "committed"))
	}
}

func (pc *PipelineCoordinator) notifyTestFlowTerminationCommitted(ctx context.Context) {
	if pc == nil || pc.testLifecycleProbe == nil {
		return
	}
	if evt, ok := runtimecorrelation.InboundEventFromContext(ctx); ok {
		pc.testLifecycleProbe.NotifyLifecycle(ctx, lifecycleNodeSignal(runtimelifecycleprobe.WorkflowTerminalCommitted, "", evt, "terminated"))
	}
}

func (pc *PipelineCoordinator) notifyTestLifecycleDeliveryStatus(ctx context.Context, nodeID string, evt events.Event, status string) error {
	if pc == nil || pc.testLifecycleProbe == nil {
		return nil
	}
	return pc.notifyTestHandlerAttemptSignal(ctx, lifecycleNodeSignal(runtimelifecycleprobe.DeliveryStatusChanged, nodeID, evt, status))
}

func (pc *PipelineCoordinator) notifyTestLifecycleHandlerStarted(ctx context.Context, nodeID string, evt events.Event) error {
	if pc == nil || pc.testLifecycleProbe == nil {
		return nil
	}
	return pc.notifyTestHandlerAttemptSignal(ctx, lifecycleNodeSignal(runtimelifecycleprobe.HandlerStarted, nodeID, evt, ""))
}

func (pc *PipelineCoordinator) notifyTestLifecycleHandlerCompleted(ctx context.Context, nodeID string, evt events.Event, status string) error {
	if pc == nil || pc.testLifecycleProbe == nil {
		return nil
	}
	return pc.notifyTestHandlerAttemptSignal(ctx, lifecycleNodeSignal(runtimelifecycleprobe.HandlerCompleted, nodeID, evt, status))
}

func (pc *PipelineCoordinator) notifyTestHandlerAttemptSignal(ctx context.Context, signal runtimelifecycleprobe.Signal) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("handler attempt probe %s panic: %v", signal.Kind, recovered)
		}
	}()
	pc.testLifecycleProbe.NotifyLifecycle(ctx, signal)
	return nil
}

func lifecycleNodeSignal(kind runtimelifecycleprobe.Kind, nodeID string, evt events.Event, status string) runtimelifecycleprobe.Signal {
	return runtimelifecycleprobe.Signal{
		Kind:           kind,
		EventID:        evt.ID(),
		EventType:      string(evt.Type()),
		SubscriberType: "node",
		SubscriberID:   strings.TrimSpace(nodeID),
		Status:         strings.TrimSpace(status),
	}
}
