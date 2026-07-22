package manager

import (
	"context"
	"fmt"
	"strings"
)

type claimedAttemptLaneContextKey struct{}

func (am *AgentManager) acquireClaimedAttemptLane(ctx context.Context, agentID string) (context.Context, func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return ctx, nil, fmt.Errorf("claimed-attempt executor requires an agent id")
	}
	if claimedAttemptLaneHeld(ctx, agentID) {
		return ctx, func() {}, nil
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
		return ctx, nil, ctx.Err()
	case <-lane:
	}
	owned := context.WithValue(ctx, claimedAttemptLaneContextKey{}, agentID)
	return owned, func() { lane <- struct{}{} }, nil
}

func claimedAttemptLaneHeld(ctx context.Context, agentID string) bool {
	if ctx == nil {
		return false
	}
	held, _ := ctx.Value(claimedAttemptLaneContextKey{}).(string)
	return strings.TrimSpace(held) != "" && strings.TrimSpace(held) == strings.TrimSpace(agentID)
}
