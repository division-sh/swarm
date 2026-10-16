package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type OperatorSnapshotCut = private.OperatorSnapshotCut

func InitializeOperatorSnapshotReadBarrier() error {
	return private.InitializeOperatorSnapshotReadBarrierForTest()
}

func OperatorSnapshotTerminalization(identity agentidentity.Identity) OperatorSnapshotCut {
	return private.OperatorSnapshotTerminalization(identity)
}
func OperatorSnapshotActivation(identity agentidentity.Identity) OperatorSnapshotCut {
	return private.OperatorSnapshotActivation(identity)
}
func OperatorSnapshotSessionReplacement(identity agentidentity.Identity, before, after string) OperatorSnapshotCut {
	return private.OperatorSnapshotSessionReplacement(identity, before, after)
}
func OperatorSnapshotDeliverySettlement(deliveryID string) OperatorSnapshotCut {
	return private.OperatorSnapshotDeliverySettlement(deliveryID)
}
func OperatorSnapshotDeliveryFactsSettlement(deliveryID string) OperatorSnapshotCut {
	return private.OperatorSnapshotDeliveryFactsSettlement(deliveryID)
}
func OperatorSnapshotTurnReplacement(turnID, output string) OperatorSnapshotCut {
	return private.OperatorSnapshotTurnReplacement(turnID, output)
}
func OperatorSnapshotDeliveryCorruption(deliveryID string) OperatorSnapshotCut {
	return private.OperatorSnapshotDeliveryCorruption(deliveryID)
}
func OperatorSnapshotEventCorruption(eventID string) OperatorSnapshotCut {
	return private.OperatorSnapshotEventCorruption(eventID)
}

func PrepareOperatorSnapshotReads(ctx context.Context, selected any) error {
	return private.PrepareOperatorSnapshotReadsForTest(ctx, selected)
}
func ReadOperatorSnapshotAcrossCut(ctx context.Context, selected any, cut OperatorSnapshotCut, read func(context.Context) error) error {
	return private.ReadOperatorSnapshotAcrossCutForTest(ctx, selected, cut, read)
}

func ApplyOperatorAgentTerminalized(ctx context.Context, selected any, identity agentidentity.Identity) error {
	return private.ApplyOperatorAgentTerminalizedForTest(ctx, selected, identity)
}
func CorruptOperatorAgentSubscriptions(ctx context.Context, selected any, identity agentidentity.Identity) error {
	return private.CorruptOperatorAgentSubscriptionsForTest(ctx, selected, identity)
}
func CorruptOperatorAgentConfig(ctx context.Context, selected any, identity agentidentity.Identity) error {
	return private.CorruptOperatorAgentConfigForTest(ctx, selected, identity)
}
func CorruptOperatorSessionState(ctx context.Context, selected any, sessionID string) error {
	return private.CorruptOperatorSessionStateForTest(ctx, selected, sessionID)
}
func CorruptOperatorTurnBlocks(ctx context.Context, selected any, turnID string) error {
	return private.CorruptOperatorTurnBlocksForTest(ctx, selected, turnID)
}
func CorruptOperatorDeliveryRoute(ctx context.Context, selected any, deliveryID string) error {
	return private.CorruptOperatorDeliveryRouteForTest(ctx, selected, deliveryID)
}
func CorruptOperatorSnapshotEvent(ctx context.Context, selected any, eventID string) error {
	return private.CorruptOperatorSnapshotEventForTest(ctx, selected, eventID)
}
