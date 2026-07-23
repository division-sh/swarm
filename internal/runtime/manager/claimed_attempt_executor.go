package manager

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
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

func (am *AgentManager) acquireClaimedAttemptLane(ctx context.Context, agentID string) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return nil, fmt.Errorf("claimed-attempt executor requires an agent id")
	}
	am.deliveryLaneMu.Lock()
	lane := am.deliveryLanes[agentID]
	if lane == nil {
		lane = newClaimedAttemptLane()
		am.deliveryLanes[agentID] = lane
	}
	am.deliveryLaneMu.Unlock()
	if err := lane.acquire(ctx); err != nil {
		return nil, ctx.Err()
	}
	return func() { lane.token <- struct{}{} }, nil
}
