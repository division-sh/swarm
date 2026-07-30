package manager

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

type claimedAttemptLane struct {
	token   chan struct{}
	waiters atomic.Int64
}

func newClaimedAttemptLane() *claimedAttemptLane {
	lane := &claimedAttemptLane{token: make(chan struct{}, 1)}
	lane.token <- struct{}{}
	return lane
}

func (lane *claimedAttemptLane) acquire(ctx context.Context) error {
	lane.waiters.Add(1)
	defer lane.waiters.Add(-1)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-lane.token:
		return nil
	}
}

func (am *AgentManager) acquireClaimedAttemptLane(ctx context.Context, identity agentidentity.Identity) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return nil, fmt.Errorf("claimed-attempt executor identity: %w", err)
	}
	am.deliveryLaneMu.Lock()
	lane := am.deliveryLanes[identity]
	if lane == nil {
		lane = newClaimedAttemptLane()
		am.deliveryLanes[identity] = lane
	}
	am.deliveryLaneMu.Unlock()
	if err := lane.acquire(ctx); err != nil {
		return nil, ctx.Err()
	}
	return func() { lane.token <- struct{}{} }, nil
}
