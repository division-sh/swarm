package storetest

import (
	"context"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type DeliveryConstraintCase = private.DeliveryConstraintCase
type DeliveryConstraintInput = private.DeliveryConstraintInput

const (
	DeliveryConstraintEventRunMismatch           = private.DeliveryConstraintEventRunMismatch
	DeliveryConstraintMissingOpenAttempt         = private.DeliveryConstraintMissingOpenAttempt
	DeliveryConstraintMissingSession             = private.DeliveryConstraintMissingSession
	DeliveryConstraintForeignSession             = private.DeliveryConstraintForeignSession
	DeliveryConstraintSecondOpenAttempt          = private.DeliveryConstraintSecondOpenAttempt
	DeliveryConstraintMissingSettlementEvidence  = private.DeliveryConstraintMissingSettlementEvidence
	DeliveryConstraintMissingOutcome             = private.DeliveryConstraintMissingOutcome
	DeliveryConstraintMissingCompletionTime      = private.DeliveryConstraintMissingCompletionTime
	DeliveryConstraintExpiredDuration            = private.DeliveryConstraintExpiredDuration
	DeliveryConstraintOpenDeadLetter             = private.DeliveryConstraintOpenDeadLetter
	DeliveryConstraintPartialSettlementReference = private.DeliveryConstraintPartialSettlementReference
	DeliveryConstraintContradictoryClosure       = private.DeliveryConstraintContradictoryClosure
	DeliveryConstraintMissingTypedFailure        = private.DeliveryConstraintMissingTypedFailure
	DeliveryConstraintExpiredDeadLetter          = private.DeliveryConstraintExpiredDeadLetter
)

func ProbeDeliveryConstraint(ctx context.Context, selected any, input DeliveryConstraintInput) (bool, error) {
	return private.ProbeDeliveryConstraintForTest(ctx, selected, input)
}
