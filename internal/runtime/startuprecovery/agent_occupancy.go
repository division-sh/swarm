package startuprecovery

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
)

// AgentRecoveryOccupancyCandidate counts durable observations, not possessed
// slots, transient directive/chat work or permission to admit or launch work.
type AgentRecoveryOccupancyCandidate struct {
	Image               AgentRecoveryImage
	InProgressByProfile map[string]int
}

func InspectAgentRecoveryOccupancyCandidate(ctx context.Context, reader AgentRecoveryReader) (AgentRecoveryOccupancyCandidate, error) {
	if ctx == nil || reader == nil {
		return AgentRecoveryOccupancyCandidate{}, errors.New("agent occupancy candidate requires context and selected readers")
	}
	if err := ctx.Err(); err != nil {
		return AgentRecoveryOccupancyCandidate{}, err
	}
	var candidate AgentRecoveryOccupancyCandidate
	err := reader.InspectSnapshot(ctx, func(snapshot context.Context) error {
		image, err := readAgentRecoveryImage(snapshot, reader)
		if err != nil {
			return err
		}
		counts, err := countAgentRecoveryProfiles(snapshot, image)
		if err != nil {
			return err
		}
		candidate = AgentRecoveryOccupancyCandidate{Image: image, InProgressByProfile: counts}
		return nil
	})
	if err != nil {
		return AgentRecoveryOccupancyCandidate{}, err
	}
	if err := ctx.Err(); err != nil {
		return AgentRecoveryOccupancyCandidate{}, err
	}
	return candidate, nil
}

func countAgentRecoveryProfiles(ctx context.Context, image AgentRecoveryImage) (map[string]int, error) {
	counts := make(map[string]int)
	for _, member := range image.Members {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if member.Delivery.Status != deliverylifecycle.StatusInProgress {
			continue
		}
		profile, err := agentRecoveryMemberProfile(member)
		if err != nil {
			return nil, fmt.Errorf("agent occupancy delivery %s: %w", member.Delivery.DeliveryID, err)
		}
		counts[profile]++
	}
	return counts, nil
}

func agentRecoveryMemberProfile(member AgentRecoveryMember) (string, error) {
	var captured string
	switch member.CaptureKind {
	case AgentRecoveryOrdinaryFirstClaim:
		if member.FirstClaim == nil || member.Selected != nil {
			return "", errors.New("agent occupancy requires its immutable ordinary capture")
		}
		captured = member.FirstClaim.ProfileID
	case AgentRecoverySelectedPreparation:
		if member.Selected == nil || member.FirstClaim != nil {
			return "", errors.New("agent occupancy requires its immutable selected preparation")
		}
		captured = member.Selected.Backend
	default:
		return "", errors.New("in-progress agent occupancy has no captured profile owner")
	}
	profile, err := selection.ResolveActiveBackend(captured)
	if err != nil || profile.ID != captured {
		return "", errors.New("agent occupancy requires an exact canonical captured profile")
	}
	return profile.ID, nil
}
