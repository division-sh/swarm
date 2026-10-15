package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
)

type DeliveryConstraintCase = delivery.DeliveryConstraintCase
type DeliveryConstraintInput = delivery.DeliveryConstraintInput

const (
	DeliveryConstraintEventRunMismatch           = delivery.DeliveryConstraintEventRunMismatch
	DeliveryConstraintMissingOpenAttempt         = delivery.DeliveryConstraintMissingOpenAttempt
	DeliveryConstraintMissingSession             = delivery.DeliveryConstraintMissingSession
	DeliveryConstraintForeignSession             = delivery.DeliveryConstraintForeignSession
	DeliveryConstraintSecondOpenAttempt          = delivery.DeliveryConstraintSecondOpenAttempt
	DeliveryConstraintMissingSettlementEvidence  = delivery.DeliveryConstraintMissingSettlementEvidence
	DeliveryConstraintMissingOutcome             = delivery.DeliveryConstraintMissingOutcome
	DeliveryConstraintMissingCompletionTime      = delivery.DeliveryConstraintMissingCompletionTime
	DeliveryConstraintExpiredDuration            = delivery.DeliveryConstraintExpiredDuration
	DeliveryConstraintOpenDeadLetter             = delivery.DeliveryConstraintOpenDeadLetter
	DeliveryConstraintPartialSettlementReference = delivery.DeliveryConstraintPartialSettlementReference
	DeliveryConstraintContradictoryClosure       = delivery.DeliveryConstraintContradictoryClosure
	DeliveryConstraintMissingTypedFailure        = delivery.DeliveryConstraintMissingTypedFailure
	DeliveryConstraintExpiredDeadLetter          = delivery.DeliveryConstraintExpiredDeadLetter
)

func ProbeDeliveryConstraintForTest(ctx context.Context, selected any, input DeliveryConstraintInput) (bool, error) {
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return false, err
	}
	if err := delivery.ValidateDeliveryConstraintInput(input); err != nil {
		return false, err
	}
	_, postgres := selected.(*PostgresStore)
	attempted := false
	err := runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		attempted = true
		return delivery.ProbeDeliveryConstraintTx(ctx, tx, postgres, input)
	})
	if attempted && ctx.Err() == nil && delivery.IsDeliveryConstraintRejection(err) {
		return true, nil
	}
	return false, err
}
