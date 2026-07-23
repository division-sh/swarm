package manager

import (
	"context"
	"fmt"
	"strings"
)

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
		lane = make(chan struct{}, 1)
		lane <- struct{}{}
		am.deliveryLanes[agentID] = lane
	}
	am.deliveryLaneMu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-lane:
	}
	return func() { lane <- struct{}{} }, nil
}
