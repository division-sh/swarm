package manager

import (
	"context"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
)

func (am *AgentManager) notifyTestDeliveryStatus(ctx context.Context, evt events.Event, agentID string, status runtimedelivery.Status) {
	if am == nil || am.testLifecycleProbe == nil {
		return
	}
	am.testLifecycleProbe.NotifyLifecycle(ctx, runtimelifecycleprobe.Signal{
		Kind:           runtimelifecycleprobe.DeliveryStatusChanged,
		EventID:        strings.TrimSpace(evt.ID()),
		EventType:      strings.TrimSpace(string(evt.Type())),
		SubscriberType: string(runtimedelivery.SubscriberAgent),
		SubscriberID:   strings.TrimSpace(agentID),
		Status:         strings.TrimSpace(string(status)),
	})
}
