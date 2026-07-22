package pipeline

import (
	"context"
	"sync"

	"github.com/division-sh/swarm/internal/events"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

const activityDispatcherSubscriberID = "workflow-activity-dispatcher"

type ownedInternalSubscriptionBus interface {
	SubscribeInternal(context.Context, string, ...events.EventType) (worklifetime.InternalSubscription, error)
}

type systemNodeBus interface {
	Publish(context.Context, events.Event) error
}

type activityBackgroundNode struct {
	coordinator *PipelineCoordinator
	bus         systemNodeBus
	mu          sync.Mutex
	readyHooks  []func()
	readyOnce   sync.Once
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
	for {
		subscription, err := bus.SubscribeInternal(ctx, activityDispatcherSubscriberID, activityRequestEventType)
		if err != nil {
			return
		}
		subscription.MarkReady()
		n.readyOnce.Do(func() {
			n.mu.Lock()
			hooks := append([]func(){}, n.readyHooks...)
			n.mu.Unlock()
			for _, hook := range hooks {
				hook()
			}
		})
		for {
			select {
			case <-ctx.Done():
				_ = subscription.Complete(false)
				return
			case <-subscription.Retiring():
				restart := ctx.Err() == nil
				_ = subscription.Complete(restart)
				if !restart {
					return
				}
				goto resubscribe
			case delivery := <-subscription.Deliveries():
				if delivery == nil {
					continue
				}
				_, _ = n.coordinator.handleActivityRequestEvent(delivery.Context(), delivery.Event())
				_ = delivery.Complete()
			}
		}
	resubscribe:
	}
}
