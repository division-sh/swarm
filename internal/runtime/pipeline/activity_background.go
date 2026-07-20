package pipeline

import (
	"context"
	"sync"
)

const activityDispatcherSubscriberID = "workflow-activity-dispatcher"

type activityBackgroundNode struct {
	coordinator *PipelineCoordinator
	bus         systemNodeBus
	mu          sync.Mutex
	readyHooks  []func()
}

func newActivityBackgroundNode(coordinator *PipelineCoordinator, bus systemNodeBus) *activityBackgroundNode {
	return &activityBackgroundNode{coordinator: coordinator, bus: bus}
}

func (n *activityBackgroundNode) AddSubscriptionReadyHook(hook func()) {
	if n == nil || hook == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.readyHooks = append(n.readyHooks, hook)
}

func (n *activityBackgroundNode) Run(ctx context.Context) {
	if n == nil || n.coordinator == nil || n.bus == nil {
		return
	}
	bus, ok := n.bus.(ownedInternalSubscriptionBus)
	if !ok {
		return
	}
	ch := bus.SubscribeInternal(activityDispatcherSubscriberID, activityRequestEventType)
	n.mu.Lock()
	hooks := append([]func(){}, n.readyHooks...)
	n.mu.Unlock()
	for _, hook := range hooks {
		hook()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case delivery, ok := <-ch:
			if !ok {
				return
			}
			_, _ = n.coordinator.handleActivityRequestEvent(delivery.Context(), delivery.Event())
			_ = delivery.Complete()
		}
	}
}
