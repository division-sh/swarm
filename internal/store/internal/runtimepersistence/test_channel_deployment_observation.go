package runtimepersistence

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/effectpersistence"
	operatorchannelstore "github.com/division-sh/swarm/internal/store/internal/backend/operatorchannel"
)

type ChannelControlCounts struct{ Total, Nonmutating, OriginalRefused int }

func inspectChannelOwnedObservation(ctx context.Context, selected any, read func(context.Context) error) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.InspectSnapshot(ctx, read)
	case *SQLiteRuntimeStore:
		return owner.InspectSnapshot(ctx, read)
	}
	return fmt.Errorf("channel observation requires its original native owner")
}

func inspectChannelBackendObservation(ctx context.Context, selected any, read func(context.Context, any) error) error {
	return inspectChannelOwnedObservation(ctx, selected, func(ctx context.Context) error {
		switch owner := selected.(type) {
		case *PostgresStore:
			return read(ctx, owner.backend)
		case *SQLiteRuntimeStore:
			return read(ctx, owner.backend)
		}
		return fmt.Errorf("channel observation requires its original native owner")
	})
}

func ReadChannelNativeConsumerForTest(ctx context.Context, selected any, operation string, currentOnly bool) (channelnative.TransportParent, error) {
	var raw []byte
	err := inspectChannelBackendObservation(ctx, selected, func(ctx context.Context, backend any) error {
		var err error
		raw, err = channeldelivery.ReadChannelNativeConsumerEvidenceForTest(ctx, backend, operation, currentOnly)
		return err
	})
	if err != nil {
		return channelnative.TransportParent{}, err
	}
	return channelnative.DecodeTransportParent(raw)
}

func ReadChannelNativeHistoryForTest(ctx context.Context, selected any) ([8]string, error) {
	var row [8]string
	err := inspectChannelBackendObservation(ctx, selected, func(ctx context.Context, backend any) error {
		var err error
		row, err = channeldelivery.ReadChannelNativeHistoryForTest(ctx, backend)
		return err
	})
	return row, err
}

func ReadChannelFrozenCardHistoryForTest(ctx context.Context, selected any, card string) (string, error) {
	out := "\n"
	err := inspectChannelBackendObservation(ctx, selected, func(ctx context.Context, backend any) error {
		var err error
		out, err = channeldelivery.ReadChannelFrozenCardHistoryForTest(ctx, backend, card)
		return err
	})
	return out, err
}

func ReadChannelHistoricalReceiptForTest(ctx context.Context, selected any, card string) (string, error) {
	var receipt string
	err := inspectChannelBackendObservation(ctx, selected, func(ctx context.Context, backend any) error {
		var err error
		receipt, err = channeldelivery.ReadChannelHistoricalReceiptForTest(ctx, backend, card)
		return err
	})
	return receipt, err
}

func ReadChannelGenerationZeroSentPlanForTest(ctx context.Context, selected any, card string) (string, error) {
	var plan string
	err := inspectChannelBackendObservation(ctx, selected, func(ctx context.Context, backend any) error {
		var err error
		plan, err = channeldelivery.ReadChannelGenerationZeroSentPlanForTest(ctx, backend, card)
		return err
	})
	return plan, err
}

func ReadChannelControlCountsForTest(ctx context.Context, selected any, token string) (ChannelControlCounts, error) {
	var result ChannelControlCounts
	err := inspectChannelBackendObservation(ctx, selected, func(ctx context.Context, backend any) error {
		var err error
		result.Total, result.Nonmutating, result.OriginalRefused, err = channeldelivery.ReadChannelControlCountsForTest(ctx, backend, token)
		return err
	})
	return result, err
}

func CountChannelSourceCardsForTest(ctx context.Context, selected any, run, source string) (int, error) {
	var count int
	err := inspectChannelOwnedObservation(ctx, selected, func(ctx context.Context) error {
		var err error
		switch owner := selected.(type) {
		case *PostgresStore:
			count, err = owner.decisionPostgresOwner.CountChannelSourceCardsForTest(ctx, run, source)
		case *SQLiteRuntimeStore:
			count, err = owner.decisionSQLiteOwner.CountChannelSourceCardsForTest(ctx, run, source)
		}
		return err
	})
	return count, err
}

func CountChannelEntryIntentsForTest(ctx context.Context, selected any) (int, error) {
	var count int
	err := inspectChannelBackendObservation(ctx, selected, func(ctx context.Context, backend any) error {
		var err error
		count, err = channeldelivery.CountChannelEntryIntentsForTest(ctx, backend)
		return err
	})
	return count, err
}

func CountChannelRejectedEntriesForTest(ctx context.Context, selected any, event string) (int, error) {
	var count int
	err := inspectChannelBackendObservation(ctx, selected, func(ctx context.Context, backend any) error {
		var err error
		count, err = channeldelivery.CountChannelRejectedEntriesForTest(ctx, backend, event)
		return err
	})
	return count, err
}

func ReadChannelProviderActionForTest(ctx context.Context, selected any, event string) (int, string, string, error) {
	var count int
	var state, disposition string
	err := inspectChannelBackendObservation(ctx, selected, func(ctx context.Context, backend any) error {
		var err error
		count, state, disposition, err = channeldelivery.ReadChannelProviderActionForTest(ctx, backend, event)
		return err
	})
	return count, state, disposition, err
}

func CountChannelObservedEventsForTest(ctx context.Context, selected any, run, event string) (int, error) {
	if err := validateSelectedForkStorageIdentity(run); err != nil {
		return 0, err
	}
	if event == "" {
		return 0, fmt.Errorf("lifecycle event cardinality requires an exact event name")
	}
	var count int
	err := inspectChannelOwnedObservation(ctx, selected, func(ctx context.Context) error {
		var err error
		switch owner := selected.(type) {
		case *PostgresStore:
			count, err = owner.eventPostgresOwner.CountChannelObservedEventsForTest(ctx, run, event)
		case *SQLiteRuntimeStore:
			count, err = owner.eventSQLiteOwner.CountChannelObservedEventsForTest(ctx, run, event)
		}
		return err
	})
	return count, err
}

func ReadChannelRegistrationIndexForTest(ctx context.Context, selected any) (string, error) {
	var definition string
	err := inspectChannelOwnedObservation(ctx, selected, func(ctx context.Context) error {
		var err error
		switch owner := selected.(type) {
		case *PostgresStore:
			definition, err = owner.schemaOwner.DeploymentChannelAccountIndexForTest(ctx)
		case *SQLiteRuntimeStore:
			definition, err = owner.schema.DeploymentChannelAccountIndexForTest(ctx)
		}
		return err
	})
	return definition, err
}

func ReadChannelRegistrationCountsForTest(ctx context.Context, selected any) (int, int, int, error) {
	var total, current, retired int
	err := inspectChannelOwnedObservation(ctx, selected, func(ctx context.Context) error {
		var err error
		switch owner := selected.(type) {
		case *PostgresStore:
			total, current, retired, err = owner.channelOnboardingPostgresOwner.ReadChannelRegistrationCountsForTest(ctx)
		case *SQLiteRuntimeStore:
			total, current, retired, err = owner.channelOnboardingSQLiteOwner.ReadChannelRegistrationCountsForTest(ctx)
		}
		return err
	})
	return total, current, retired, err
}

func ReadChannelDeliveryDefaultStateForTest(ctx context.Context, selected any) (string, error) {
	var state string
	err := inspectChannelBackendObservation(ctx, selected, func(ctx context.Context, backend any) error {
		var err error
		state, err = channeldelivery.ReadChannelDeliveryDefaultStateForTest(ctx, backend)
		return err
	})
	return state, err
}

func CountUnsettledChannelResponsibilitiesForTest(ctx context.Context, selected any) (int, error) {
	_, postgres := selected.(*PostgresStore)
	var count int
	err := inspectChannelBackendObservation(ctx, selected, func(ctx context.Context, backend any) error {
		var err error
		count, err = channeldelivery.CountUnsettledChannelResponsibilitiesForTest(ctx, backend, postgres)
		return err
	})
	return count, err
}

type ChannelResetSettlement = channeldelivery.ChannelResetSettlement

func CountUnsettledChannelAfterResetForTest(ctx context.Context, selected any, originals map[string]string) (int, error) {
	_, postgres := selected.(*PostgresStore)
	var count int
	err := inspectChannelBackendObservation(ctx, selected, func(ctx context.Context, backend any) error {
		var err error
		count, err = channeldelivery.CountUnsettledChannelAfterResetForTest(ctx, backend, postgres, originals)
		return err
	})
	return count, err
}

func ReadChannelPreservedHistoryForTest(ctx context.Context, selected any, successors []string) (string, error) {
	var snapshots [][][]any
	for index := 0; index < 15; index++ {
		if err := validateChannelObservationOwner(selected); err != nil {
			return "", err
		}
		var backend any
		switch owner := selected.(type) {
		case *PostgresStore:
			backend = owner.backend
		case *SQLiteRuntimeStore:
			backend = owner.backend
		}
		var snapshot [][]any
		var err error
		switch index {
		case 0, 1, 2, 10:
			snapshot, err = operatorchannelstore.ReadPreservedOperatorChannelHistoryForTest(ctx, backend, index)
		case 13, 14:
			snapshot, err = effectpersistence.ReadPreservedChannelEffectHistoryForTest(ctx, backend, index, successors)
		default:
			snapshot, err = channeldelivery.ReadPreservedChannelDeliveryHistoryForTest(ctx, backend, index, successors)
		}
		if err != nil {
			return "", err
		}
		snapshots = append(snapshots, snapshot)
	}
	body, err := json.Marshal(snapshots)
	return string(body), err
}
