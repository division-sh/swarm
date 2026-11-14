package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
)

func recoveryPostlaunch(state string) bool {
	return state == string(effects.StateLaunched) || state == string(effects.StateResponseObserved)
}

func (s *EffectPostgresOwner) InspectStartupRecovery(ctx context.Context, now time.Time, available bool) (result effects.StartupRecoveryCounts, err error) {
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		result, err = inspectStartupRecovery(ctx, tx, true, s.delivery, now, available)
		return err
	})
	return result, err
}

func (s *EffectSQLiteOwner) InspectStartupRecovery(ctx context.Context, now time.Time, available bool) (result effects.StartupRecoveryCounts, err error) {
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		result, err = inspectStartupRecovery(ctx, tx, false, s.delivery, now, available)
		return err
	})
	return result, err
}

func inspectStartupRecovery(ctx context.Context, tx *sql.Tx, postgres bool, delivery providerDrainDeliveryOwner, now time.Time, available bool) (effects.StartupRecoveryCounts, error) {
	if now.IsZero() || delivery == nil {
		return effects.StartupRecoveryCounts{}, fmt.Errorf("startup recovery inspection requires its time and delivery reader")
	}
	result, candidates, err := readStartupRecoveryReportBase(ctx, tx, postgres, available)
	if err != nil {
		return result, err
	}
	mode := recoveryReadMode{inspection: true, available: available}
	var attempts []completionRecoveryAttempt
	if postgres {
		attempts, err = loadCompletionRecoveryAttemptsPostgres(ctx, tx, delivery, now, "", nil, mode)
	} else {
		attempts, err = loadCompletionRecoveryAttemptsSQLite(ctx, tx, delivery, now, "", nil, mode)
	}
	if err != nil {
		return effects.StartupRecoveryCounts{}, err
	}
	for _, row := range filterCompletionRecoveryAttempts(attempts, externalEffectRecoveryAttemptSet(candidates)) {
		outcome, err := inspectProviderRecoveryOutcome(ctx, tx, postgres, delivery, row, now)
		if err != nil {
			return effects.StartupRecoveryCounts{}, err
		}
		switch outcome {
		case deliverylifecycle.StatusFailed:
			result.ProviderTurnsToResume++
		case deliverylifecycle.StatusDeadLetter:
			result.ProviderTurnsToDeadLetter++
		}
	}
	return result, nil
}

func readStartupRecoveryReportBase(ctx context.Context, tx *sql.Tx, postgres, available bool) (effects.StartupRecoveryCounts, []externalEffectRecoveryCandidate, error) {
	var result effects.StartupRecoveryCounts
	candidates, err := loadExternalEffectRecoveryCandidates(ctx, tx, postgres, "", recoveryReadMode{inspection: true, available: available})
	if err != nil {
		return result, nil, err
	}
	for _, row := range candidates {
		if _, err := row.recoveryMode(); err != nil {
			return effects.StartupRecoveryCounts{}, nil, err
		}
		if row.EffectKind == string(effects.KindHTTPToolTarget) && recoveryPostlaunch(row.State) {
			result.HTTPToolCallsUnknown++
		}
	}
	return result, candidates, nil
}

func inspectProviderRecoveryOutcome(ctx context.Context, tx *sql.Tx, postgres bool, delivery providerDrainDeliveryOwner, row completionRecoveryAttempt, now time.Time) (deliverylifecycle.Status, error) {
	state := effects.StateTerminalFailure
	if recoveryPostlaunch(row.State) {
		state = effects.StateOutcomeUncertain
	}
	attempt, _, err := completionRecoverySettlement(row, state, nil, now)
	if err != nil {
		return "", err
	}
	resolution, err := resolveProviderDrainRecovery(ctx, tx, attempt, now, postgres, recoveryInspection)
	if err != nil {
		return "", err
	}
	if resolution.Kind == completionSettlementDrained && state == effects.StateTerminalFailure {
		return "", fmt.Errorf("authorized provider attempt %s cannot own a predecessor drain", attempt.AttemptID)
	}
	disposition, err := providerRecoveryDisposition(ctx, tx, postgres, row, attempt, resolution.Expired, now, recoveryInspection)
	if err != nil {
		return "", err
	}
	if attempt.Origin.Kind != effects.CompletionOriginDelivery {
		return "", nil
	}
	outcome, pending, err := delivery.InspectProviderOriginRecoveryTx(ctx, tx, attempt.Origin.Delivery, disposition, resolution.Kind == completionSettlementCurrent)
	if err != nil {
		return "", err
	}
	if resolution.Kind == completionSettlementDrained {
		cancellation, err := readProviderOriginCancellation(ctx, tx, postgres, attempt, pending, recoveryInspection)
		if err != nil {
			return "", err
		}
		if cancellation.disposition != providerOriginNotCanceled {
			return "", nil
		}
	}
	return outcome, nil
}
