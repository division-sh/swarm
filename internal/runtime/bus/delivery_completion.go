package bus

import (
	"context"
	"errors"
	"sync"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

type localDeliveryCompletionGroup struct {
	mu       sync.Mutex
	active   uint64
	finished bool
	drained  chan struct{}
}

type localDeliveryCompletionContextKey struct{}

func newLocalDeliveryCompletionGroup() *localDeliveryCompletionGroup {
	return &localDeliveryCompletionGroup{active: 1, drained: make(chan struct{})}
}

func withLocalDeliveryCompletionGroup(ctx context.Context, group *localDeliveryCompletionGroup) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, localDeliveryCompletionContextKey{}, group)
}

func localDeliveryCompletionGroupFromContext(ctx context.Context) *localDeliveryCompletionGroup {
	if ctx == nil {
		return nil
	}
	group, _ := ctx.Value(localDeliveryCompletionContextKey{}).(*localDeliveryCompletionGroup)
	return group
}

func trackLocalDeliveryCompletion(ctx context.Context, delivery *worklifetime.EventDelivery) error {
	group := localDeliveryCompletionGroupFromContext(ctx)
	if group == nil {
		return nil
	}
	return group.add(delivery)
}

func (g *localDeliveryCompletionGroup) add(delivery *worklifetime.EventDelivery) error {
	if g == nil || delivery == nil {
		return errors.New("local delivery completion tracking requires a delivery")
	}
	g.mu.Lock()
	if g.finished {
		g.mu.Unlock()
		return errors.New("local delivery completion group is already drained")
	}
	g.active++
	g.mu.Unlock()
	if err := delivery.OnComplete(g.completeOne); err != nil {
		g.completeOne()
		return err
	}
	return nil
}

func (g *localDeliveryCompletionGroup) releaseDispatch() {
	if g != nil {
		g.completeOne()
	}
}

func (g *localDeliveryCompletionGroup) completeOne() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.active == 0 {
		return
	}
	g.active--
	if g.active == 0 && !g.finished {
		g.finished = true
		close(g.drained)
	}
}

func (g *localDeliveryCompletionGroup) wait(ctx context.Context) error {
	if g == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-g.drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
