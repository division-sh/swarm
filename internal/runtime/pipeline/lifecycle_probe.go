package pipeline

import (
	"context"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
)

func (pc *PipelineCoordinator) notifyTestLifecycleDeliveryStatus(ctx context.Context, nodeID string, evt events.Event, status string) {
	if pc == nil || pc.testLifecycleProbe == nil {
		return
	}
	pc.testLifecycleProbe.NotifyLifecycle(ctx, lifecycleNodeSignal(runtimelifecycleprobe.DeliveryStatusChanged, nodeID, evt, status))
}

func (pc *PipelineCoordinator) notifyTestLifecycleHandlerStarted(ctx context.Context, nodeID string, evt events.Event) {
	if pc == nil || pc.testLifecycleProbe == nil {
		return
	}
	pc.testLifecycleProbe.NotifyLifecycle(ctx, lifecycleNodeSignal(runtimelifecycleprobe.HandlerStarted, nodeID, evt, ""))
}

func (pc *PipelineCoordinator) notifyTestLifecycleHandlerCompleted(ctx context.Context, nodeID string, evt events.Event, status string) {
	if pc == nil || pc.testLifecycleProbe == nil {
		return
	}
	pc.testLifecycleProbe.NotifyLifecycle(ctx, lifecycleNodeSignal(runtimelifecycleprobe.HandlerCompleted, nodeID, evt, status))
}

func (n *systemNodeRunner) notifyTestLifecycleDeliveryStatus(ctx context.Context, evt events.Event, status string) {
	if n == nil || n.testLifecycleProbe == nil {
		return
	}
	n.testLifecycleProbe.NotifyLifecycle(ctx, lifecycleNodeSignal(runtimelifecycleprobe.DeliveryStatusChanged, n.nodeID, evt, status))
}

func (n *systemNodeRunner) notifyTestLifecycleHandlerStarted(ctx context.Context, evt events.Event) {
	if n == nil || n.testLifecycleProbe == nil {
		return
	}
	n.testLifecycleProbe.NotifyLifecycle(ctx, lifecycleNodeSignal(runtimelifecycleprobe.HandlerStarted, n.nodeID, evt, ""))
}

func (n *systemNodeRunner) notifyTestLifecycleHandlerCompleted(ctx context.Context, evt events.Event, status string) {
	if n == nil || n.testLifecycleProbe == nil {
		return
	}
	n.testLifecycleProbe.NotifyLifecycle(ctx, lifecycleNodeSignal(runtimelifecycleprobe.HandlerCompleted, n.nodeID, evt, status))
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
