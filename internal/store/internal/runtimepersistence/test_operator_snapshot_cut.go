package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/llmpersistence"
)

type operatorSnapshotChange uint8

const (
	operatorSnapshotTerminalize operatorSnapshotChange = iota + 1
	operatorSnapshotActivate
	operatorSnapshotReplaceSession
	operatorSnapshotSettleDelivery
	operatorSnapshotReplaceTurn
	operatorSnapshotCorruptDelivery
	operatorSnapshotCorruptEvent
)

// OperatorSnapshotCut is finite fixture data, not a transaction/executor
// capability. The only executable callback accepted by the bridge is a reader.
type OperatorSnapshotCut struct {
	change                operatorSnapshotChange
	identity              agentidentity.Identity
	before, after, output string
	deliveryBoundary      bool
}

func OperatorSnapshotTerminalization(identity agentidentity.Identity) OperatorSnapshotCut {
	return OperatorSnapshotCut{change: operatorSnapshotTerminalize, identity: identity}
}
func OperatorSnapshotActivation(identity agentidentity.Identity) OperatorSnapshotCut {
	return OperatorSnapshotCut{change: operatorSnapshotActivate, identity: identity}
}
func OperatorSnapshotSessionReplacement(identity agentidentity.Identity, before, after string) OperatorSnapshotCut {
	return OperatorSnapshotCut{change: operatorSnapshotReplaceSession, identity: identity, before: before, after: after}
}
func OperatorSnapshotDeliverySettlement(deliveryID string) OperatorSnapshotCut {
	return OperatorSnapshotCut{change: operatorSnapshotSettleDelivery, before: deliveryID}
}
func OperatorSnapshotDeliveryFactsSettlement(deliveryID string) OperatorSnapshotCut {
	return OperatorSnapshotCut{change: operatorSnapshotSettleDelivery, before: deliveryID, deliveryBoundary: true}
}
func OperatorSnapshotTurnReplacement(turnID, output string) OperatorSnapshotCut {
	return OperatorSnapshotCut{change: operatorSnapshotReplaceTurn, before: turnID, output: output}
}
func OperatorSnapshotDeliveryCorruption(deliveryID string) OperatorSnapshotCut {
	return OperatorSnapshotCut{change: operatorSnapshotCorruptDelivery, before: deliveryID}
}
func OperatorSnapshotEventCorruption(eventID string) OperatorSnapshotCut {
	return OperatorSnapshotCut{change: operatorSnapshotCorruptEvent, before: eventID}
}

func (cut OperatorSnapshotCut) validate(selected any) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	switch cut.change {
	case operatorSnapshotTerminalize, operatorSnapshotActivate:
		return cut.identity.Validate()
	case operatorSnapshotReplaceSession:
		if err := cut.identity.Validate(); err != nil {
			return err
		}
		if err := validateStandaloneStorageEvent(selected, cut.after); err != nil {
			return err
		}
	case operatorSnapshotSettleDelivery, operatorSnapshotReplaceTurn, operatorSnapshotCorruptDelivery, operatorSnapshotCorruptEvent:
	default:
		return errors.New("operator snapshot cut requires a named operation")
	}
	return validateStandaloneStorageEvent(selected, cut.before)
}

func (cut OperatorSnapshotCut) apply(ctx context.Context, tx *sql.Tx, postgres bool) error {
	switch cut.change {
	case operatorSnapshotTerminalize, operatorSnapshotActivate:
		return agentpersistence.SetOperatorSnapshotAgentActiveTx(ctx, tx, cut.identity, cut.change == operatorSnapshotActivate)
	case operatorSnapshotReplaceSession:
		return llmpersistence.ReplaceOperatorSnapshotSessionTx(ctx, tx, cut.identity, cut.before, cut.after, postgres, !postgres)
	case operatorSnapshotSettleDelivery:
		return delivery.SettleOperatorSnapshotDeliveryTx(ctx, tx, cut.before, !postgres && cut.deliveryBoundary)
	case operatorSnapshotReplaceTurn:
		return llmpersistence.ReplaceOperatorSnapshotTurnSummaryTx(ctx, tx, cut.before, cut.output, postgres, !postgres)
	case operatorSnapshotCorruptDelivery:
		return delivery.CorruptOperatorSnapshotDeliveryRouteTx(ctx, tx, cut.before, postgres)
	case operatorSnapshotCorruptEvent:
		return corruptOperatorSnapshotEventTx(ctx, tx, cut.before, postgres)
	default:
		return errors.New("operator snapshot cut requires a named operation")
	}
}

func (cut OperatorSnapshotCut) hold(ctx context.Context, tx *sql.Tx, postgres bool, id int64) error {
	if cut.deliveryBoundary {
		return delivery.HoldOperatorDeliverySnapshotTx(ctx, tx, postgres, id)
	}
	if cut.change == operatorSnapshotReplaceTurn {
		return llmpersistence.HoldOperatorTurnSnapshotTx(ctx, tx, postgres, id)
	}
	return llmpersistence.HoldOperatorSessionSnapshotTx(ctx, tx, postgres, id)
}

func (cut OperatorSnapshotCut) restore(ctx context.Context, tx *sql.Tx) error {
	if cut.deliveryBoundary {
		return delivery.RestoreOperatorDeliverySnapshotTx(ctx, tx)
	}
	if cut.change == operatorSnapshotReplaceTurn {
		return llmpersistence.RestoreOperatorTurnSnapshotTx(ctx, tx)
	}
	return llmpersistence.RestoreOperatorSessionSnapshotTx(ctx, tx)
}

func ApplyOperatorAgentTerminalizedForTest(ctx context.Context, selected any, identity agentidentity.Identity) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return agentpersistence.SetOperatorSnapshotAgentActiveTx(ctx, tx, identity, false)
	})
}

func CorruptOperatorAgentSubscriptionsForTest(ctx context.Context, selected any, identity agentidentity.Identity) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return agentpersistence.CorruptOperatorSnapshotSubscriptionsTx(ctx, tx, identity)
	})
}

func CorruptOperatorAgentConfigForTest(ctx context.Context, selected any, identity agentidentity.Identity) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return agentpersistence.CorruptOperatorSnapshotConfigTx(ctx, tx, identity)
	})
}

func CorruptOperatorSessionStateForTest(ctx context.Context, selected any, sessionID string) error {
	return applyOperatorSnapshotRelatedFault(ctx, selected, sessionID, func(ctx context.Context, tx *sql.Tx, postgres bool) error {
		return llmpersistence.CorruptOperatorSnapshotSessionStateTx(ctx, tx, sessionID, postgres)
	})
}

func CorruptOperatorTurnBlocksForTest(ctx context.Context, selected any, turnID string) error {
	return applyOperatorSnapshotRelatedFault(ctx, selected, turnID, func(ctx context.Context, tx *sql.Tx, postgres bool) error {
		return llmpersistence.CorruptOperatorSnapshotTurnBlocksTx(ctx, tx, turnID, postgres)
	})
}

func CorruptOperatorDeliveryRouteForTest(ctx context.Context, selected any, deliveryID string) error {
	return applyOperatorSnapshotRelatedFault(ctx, selected, deliveryID, func(ctx context.Context, tx *sql.Tx, postgres bool) error {
		return delivery.CorruptOperatorSnapshotDeliveryRouteTx(ctx, tx, deliveryID, postgres)
	})
}

func applyOperatorSnapshotRelatedFault(ctx context.Context, selected any, id string, operation func(context.Context, *sql.Tx, bool) error) error {
	if err := validateStandaloneStorageEvent(selected, id); err != nil {
		return err
	}
	_, postgres := selected.(*PostgresStore)
	return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error { return operation(ctx, tx, postgres) })
}

func requireOperatorSnapshotReadArguments(ctx context.Context, selected any, cut OperatorSnapshotCut, read func(context.Context) error) error {
	if ctx == nil || read == nil {
		return fmt.Errorf("operator snapshot boundary requires a context and the real reader")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return cut.validate(selected)
}
