package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

type LatestPipelineReceiptStorage = pipelinepersistence.LatestPipelineReceiptStorage

type TemplateReceiptDiagnosticStorage = pipelinepersistence.TemplateReceiptDiagnosticStorage

type TemplateDeliveryDiagnosticStorage = delivery.TemplateDeliveryDiagnosticStorage

func CountExactRunEventDeliveryStorageForTest(ctx context.Context, selected any, runID, eventID string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID, eventID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountExactRunEventDeliveryStorageTx(ctx, tx, runID, eventID)
	})
}

func CountExactRunNodeEventDeliveryStorageForTest(ctx context.Context, selected any, runID, eventID, nodeID string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID, eventID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountExactRunNodeEventDeliveryStorageTx(ctx, tx, runID, eventID, nodeID)
	})
}

func CountCurrentEventDeliveryOutcomeStorageForTest(ctx context.Context, selected any, eventID, subscriberType, subscriberID, outcome string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{eventID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountCurrentEventDeliveryOutcomeStorageTx(ctx, tx, eventID, subscriberType, subscriberID, outcome)
	})
}

func CountSubscriberEventReceiptOutcomeStorageForTest(ctx context.Context, selected any, eventID, subscriberType, subscriberID, outcome string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{eventID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return pipelinepersistence.CountSubscriberEventReceiptOutcomeStorageTx(ctx, tx, eventID, subscriberType, subscriberID, outcome)
	})
}

func CountExactRunEventReplayScopeStorageForTest(ctx context.Context, selected any, runID, eventID, scope string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID, eventID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return pipelinepersistence.CountExactRunEventReplayScopeStorageTx(ctx, tx, runID, eventID, scope)
	})
}

func CountEventSubscriberReceiptStorageForTest(ctx context.Context, selected any, eventID string, subscribers []string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{eventID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return pipelinepersistence.CountEventSubscriberReceiptStorageTx(ctx, tx, eventID, subscribers)
	})
}

func ReadTemplateEventDeliveryDiagnosticsForTest(ctx context.Context, selected any, event string) ([]TemplateDeliveryDiagnosticStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{event}, func(ctx context.Context, tx *sql.Tx) ([]TemplateDeliveryDiagnosticStorage, error) {
		return delivery.ReadTemplateEventDeliveryDiagnosticsTx(ctx, tx, event)
	})
}

func ReadTemplateEventReceiptDiagnosticsForTest(ctx context.Context, selected any, event string) ([]TemplateReceiptDiagnosticStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{event}, func(ctx context.Context, tx *sql.Tx) ([]TemplateReceiptDiagnosticStorage, error) {
		return pipelinepersistence.ReadTemplateEventReceiptDiagnosticsTx(ctx, tx, event)
	})
}

func CountTemplateNodeDeliveryForTest(ctx context.Context, selected any, event, node string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{event}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountTemplateNodeDeliveryTx(ctx, tx, event, node)
	})
}
func CountTemplateNodeDeliveryChronologyForTest(ctx context.Context, selected any, event, node string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{event}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountTemplateNodeDeliveryChronologyTx(ctx, tx, event, node)
	})
}
func CountTemplateNodeDeliveredAttemptForTest(ctx context.Context, selected any, event, node string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{event}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountTemplateNodeDeliveredAttemptTx(ctx, tx, event, node)
	})
}
func CountTemplateAgentDeliveryForTest(ctx context.Context, selected any, event, agent string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{event}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountTemplateAgentDeliveryTx(ctx, tx, event, agent)
	})
}
func CountTemplatePlaceholderDeliveryForTest(ctx context.Context, selected any, event string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{event}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountTemplatePlaceholderDeliveryTx(ctx, tx, event)
	})
}
func CountTemplateEventDeliveriesForTest(ctx context.Context, selected any, event string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{event}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountTemplateEventDeliveriesTx(ctx, tx, event)
	})
}
func CountTemplateCarrierDeliveriesForTest(ctx context.Context, selected any, event string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{event}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountTemplateCarrierDeliveriesTx(ctx, tx, event)
	})
}
func CountPostgresTemplateNodeTargetForTest(ctx context.Context, selected any, event, target, node string) (int, error) {
	if err := validatePostgresPublicationIdentities(selected, event); err != nil {
		return 0, err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountPostgresTemplateNodeTargetTx(ctx, tx, event, target, node)
	})
}
func CountPostgresTemplateDeliveredNodeTargetForTest(ctx context.Context, selected any, event, node, target string) (int, error) {
	if err := validatePostgresPublicationIdentities(selected, event); err != nil {
		return 0, err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountPostgresTemplateDeliveredNodeTargetTx(ctx, tx, event, node, target)
	})
}
func CountTemplateNodeReceiptForTest(ctx context.Context, selected any, event, node string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{event}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return pipelinepersistence.CountTemplateNodeReceiptTx(ctx, tx, event, node)
	})
}

func CountPostgresArtifactCallbackSettledTargetForTest(ctx context.Context, selected any, event, target, node string) (int, error) {
	if err := validatePostgresPublicationIdentities(selected, event); err != nil {
		return 0, err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountPostgresArtifactCallbackSettledTargetTx(ctx, tx, event, target, node)
	})
}

func ReadPostgresArtifactCallbackTargetDiagnosticsForTest(ctx context.Context, selected any, event string) (string, error) {
	if err := validatePostgresPublicationIdentities(selected, event); err != nil {
		return "", err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return delivery.ReadPostgresArtifactCallbackTargetDiagnosticsTx(ctx, tx, event)
	})
}

func ReadPostgresArtifactCallbackDeliveriesForTest(ctx context.Context, selected any, event string) (string, error) {
	if err := validatePostgresPublicationIdentities(selected, event); err != nil {
		return "", err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return delivery.ReadPostgresArtifactCallbackDeliveriesTx(ctx, tx, event)
	})
}

func ReadPostgresArtifactCallbackReceiptsForTest(ctx context.Context, selected any, event string) (string, error) {
	if err := validatePostgresPublicationIdentities(selected, event); err != nil {
		return "", err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return pipelinepersistence.ReadPostgresArtifactCallbackReceiptsTx(ctx, tx, event)
	})
}

func ReadPostgresArtifactCallbackSettlementDiagnosticsForTest(ctx context.Context, selected any, event string) (string, error) {
	if err := validatePostgresPublicationIdentities(selected, event); err != nil {
		return "", err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (string, error) {
		receipts, err := pipelinepersistence.ReadPostgresArtifactCallbackReceiptsTx(ctx, tx, event)
		if err != nil {
			return "", err
		}
		return delivery.ReadPostgresArtifactCallbackSettlementDiagnosticsTx(ctx, tx, event, receipts)
	})
}
func CountTemplateSubscribedReplayScopeForTest(ctx context.Context, selected any, event string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{event}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return pipelinepersistence.CountTemplateSubscribedReplayScopeTx(ctx, tx, event)
	})
}
func CountTemplateEventReplayScopesForTest(ctx context.Context, selected any, event string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{event}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return pipelinepersistence.CountTemplateEventReplayScopesTx(ctx, tx, event)
	})
}
func CountTemplateInstancesForTest(ctx context.Context, selected any, template string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return pipelinepersistence.CountTemplateInstancesTx(ctx, tx, template)
	})
}
func CountRunOperatingInstancesForTest(ctx context.Context, selected any, run string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return pipelinepersistence.CountRunOperatingInstancesTx(ctx, tx, run)
	})
}

type TemplateInstanceFieldsStorage = pipelinepersistence.TemplateInstanceFieldsStorage

func ReadRunOperatingInstanceFieldsForTest(ctx context.Context, selected any, run string) (TemplateInstanceFieldsStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run}, func(ctx context.Context, tx *sql.Tx) (TemplateInstanceFieldsStorage, error) {
		return pipelinepersistence.ReadRunOperatingInstanceFieldsTx(ctx, tx, run)
	})
}
func CountTemplateActiveAgentRoutesForTest(ctx context.Context, selected any, agent, path string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return pipelinepersistence.CountTemplateActiveAgentRoutesTx(ctx, tx, agent, path)
	})
}
func CountTemplatePlaceholderRoutesForTest(ctx context.Context, selected any, path string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return pipelinepersistence.CountTemplatePlaceholderRoutesTx(ctx, tx, path)
	})
}
func CountTemplatePrefixRoutingRulesForTest(ctx context.Context, selected any, template string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return pipelinepersistence.CountTemplatePrefixRoutingRulesTx(ctx, tx, template)
	})
}
func CountPhysicalRoutingRulesForTest(ctx context.Context, selected any) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, pipelinepersistence.CountPhysicalRoutingRulesTx)
}

type FanOutSourceProgressStorage = delivery.FanOutSourceProgressStorage
type FanOutSemanticOutcomeStorage = delivery.FanOutSemanticOutcomeStorage
type FanOutBlockedSourceStorage = delivery.FanOutBlockedSourceStorage
type FanOutTriggerDeliveryStorage = delivery.FanOutTriggerDeliveryStorage
type FanOutRunProgressStorage = pipelinepersistence.FanOutRunProgressStorage

type NestedFanOutBarrierStorage = pipelinepersistence.NestedFanOutBarrierStorage
type NestedFanOutCursorClaimStorage = pipelinepersistence.NestedFanOutCursorClaimStorage
type NestedFanOutOwedStorage = pipelinepersistence.NestedFanOutOwedStorage

type RepeatedFanOutIntentStorage = pipelinepersistence.RepeatedFanOutIntentStorage

func ReadRepeatedFanOutIntentForTest(ctx context.Context, selected any, key fanoutobligation.IntentKey) (RepeatedFanOutIntentStorage, error) {
	if err := key.Validate(); err != nil {
		return RepeatedFanOutIntentStorage{}, err
	}
	return readFanOutSemanticStorage(ctx, selected, []string{key.RunID, key.TriggeringDeliveryID}, func(ctx context.Context, tx *sql.Tx) (RepeatedFanOutIntentStorage, error) {
		return pipelinepersistence.ReadRepeatedFanOutIntentTx(ctx, tx, key)
	})
}

func CountRepeatedFanOutTriggerOutcomesForTest(ctx context.Context, selected any, run, trigger string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run, trigger}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return pipelinepersistence.CountRepeatedFanOutTriggerOutcomesTx(ctx, tx, run, trigger)
	})
}

func ReadRepeatedFanOutSnapshotForTest(ctx context.Context, selected any, run string) ([8]int64, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run}, func(ctx context.Context, tx *sql.Tx) ([8]int64, error) {
		return delivery.ReadRepeatedFanOutSnapshotTx(ctx, tx, run)
	})
}

func ReadNestedFanOutBarrierStatusForTest(ctx context.Context, selected any, key fanoutobligation.IntentKey) (string, error) {
	if err := key.Validate(); err != nil {
		return "", err
	}
	return readFanOutSemanticStorage(ctx, selected, []string{key.RunID, key.TriggeringDeliveryID}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return pipelinepersistence.ReadNestedFanOutBarrierStatusTx(ctx, tx, key)
	})
}

func ReadNestedFanOutBarrierForTest(ctx context.Context, selected any, key fanoutobligation.IntentKey) (NestedFanOutBarrierStorage, error) {
	if err := key.Validate(); err != nil {
		return NestedFanOutBarrierStorage{}, err
	}
	return readFanOutSemanticStorage(ctx, selected, []string{key.RunID, key.TriggeringDeliveryID}, func(ctx context.Context, tx *sql.Tx) (NestedFanOutBarrierStorage, error) {
		return pipelinepersistence.ReadNestedFanOutBarrierTx(ctx, tx, key)
	})
}

func ReadNestedPipelineReceiptOutcomeForTest(ctx context.Context, selected any, event string) (string, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{event}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return pipelinepersistence.ReadNestedPipelineReceiptOutcomeTx(ctx, tx, event)
	})
}

func CountNestedSuccessfulPipelineReceiptForTest(ctx context.Context, selected any, event string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{event}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return pipelinepersistence.CountNestedSuccessfulPipelineReceiptTx(ctx, tx, event)
	})
}

func ReadNestedFanOutCursorClaimForTest(ctx context.Context, selected any, key fanoutobligation.IntentKey) (NestedFanOutCursorClaimStorage, error) {
	if err := key.Validate(); err != nil {
		return NestedFanOutCursorClaimStorage{}, err
	}
	return readFanOutSemanticStorage(ctx, selected, []string{key.RunID, key.TriggeringDeliveryID}, func(ctx context.Context, tx *sql.Tx) (NestedFanOutCursorClaimStorage, error) {
		return pipelinepersistence.ReadNestedFanOutCursorClaimTx(ctx, tx, key)
	})
}

func CountNestedRunFanOutOutcomesForTest(ctx context.Context, selected any, run string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return pipelinepersistence.CountNestedRunFanOutOutcomesTx(ctx, tx, run)
	})
}

func CountRunFanOutIntentStorageForTest(ctx context.Context, selected any, run string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return pipelinepersistence.CountFanOutIntentsForRun(ctx, tx, run)
	})
}

func CountRunFanOutBarrierStorageForTest(ctx context.Context, selected any, run string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return pipelinepersistence.CountRunFanOutBarrierStorageTx(ctx, tx, run)
	})
}

func ReadNestedRunFanOutOwedForTest(ctx context.Context, selected any, run string) (NestedFanOutOwedStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run}, func(ctx context.Context, tx *sql.Tx) (NestedFanOutOwedStorage, error) {
		return pipelinepersistence.ReadNestedRunFanOutOwedTx(ctx, tx, run)
	})
}

func ReadNestedCommittedOutcomeEventIDsForTest(ctx context.Context, selected any, run string) ([]string, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run}, func(ctx context.Context, tx *sql.Tx) ([]string, error) {
		return pipelinepersistence.ReadNestedCommittedOutcomeEventIDsTx(ctx, tx, run)
	})
}

func ReadNestedAccountIntentKeysForTest(ctx context.Context, selected any, run string) ([]fanoutobligation.IntentKey, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run}, func(ctx context.Context, tx *sql.Tx) ([]fanoutobligation.IntentKey, error) {
		return pipelinepersistence.ReadNestedAccountIntentKeysTx(ctx, tx, run)
	})
}

func CountNestedContinuationHandoffsForTest(ctx context.Context, selected any, event string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{event}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountNestedContinuationHandoffsTx(ctx, tx, event)
	})
}

func readFanOutSemanticStorage[T any](ctx context.Context, selected any, ids []string, query func(context.Context, *sql.Tx) (T, error)) (T, error) {
	var empty, out T
	if err := validateChannelObservationOwner(selected); err != nil {
		return empty, err
	}
	for _, id := range ids {
		if err := validateStandaloneStorageEvent(selected, id); err != nil {
			return empty, err
		}
	}
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = query(ctx, tx)
		return err
	})
	if err != nil {
		return empty, err
	}
	return out, nil
}

func ReadFanOutSourceProgressStorageForTest(ctx context.Context, selected any, run, event string) (FanOutSourceProgressStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run, event}, func(ctx context.Context, tx *sql.Tx) (FanOutSourceProgressStorage, error) {
		return delivery.ReadFanOutSourceProgressStorageTx(ctx, tx, run, event)
	})
}

func ReadFanOutSourceOutcomesStorageForTest(ctx context.Context, selected any, run, event string) ([]FanOutSemanticOutcomeStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run, event}, func(ctx context.Context, tx *sql.Tx) ([]FanOutSemanticOutcomeStorage, error) {
		rows, err := delivery.ReadFanOutSourceOutcomesStorageTx(ctx, tx, run, event)
		if err != nil {
			return nil, err
		}
		return hydrateFanOutOutcomePayloadBytesForTest(ctx, tx, selected, rows)
	})
}

func CountFanOutSourceOutcomesStorageForTest(ctx context.Context, selected any, run, event string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run, event}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountFanOutSourceOutcomesStorageTx(ctx, tx, run, event)
	})
}

func ReadFanOutBlockedSourceStorageForTest(ctx context.Context, selected any, run, event string) (FanOutBlockedSourceStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run, event}, func(ctx context.Context, tx *sql.Tx) (FanOutBlockedSourceStorage, error) {
		return delivery.ReadFanOutBlockedSourceStorageTx(ctx, tx, run, event)
	})
}

func ReadFanOutTriggerDeliveryDiagnosticStorageForTest(ctx context.Context, selected any, run, event string) (FanOutTriggerDeliveryStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run, event}, func(ctx context.Context, tx *sql.Tx) (FanOutTriggerDeliveryStorage, error) {
		return delivery.ReadFanOutTriggerDeliveryDiagnosticStorageTx(ctx, tx, run, event)
	})
}

func CountFanOutDeliveredTriggerStorageForTest(ctx context.Context, selected any, run, event string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run, event}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountFanOutDeliveredTriggerStorageTx(ctx, tx, run, event)
	})
}

func ReadRunFanOutProgressStorageForTest(ctx context.Context, selected any, run string) (FanOutRunProgressStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run}, func(ctx context.Context, tx *sql.Tx) (FanOutRunProgressStorage, error) {
		return pipelinepersistence.ReadRunFanOutProgressStorageTx(ctx, tx, run)
	})
}

func CountLeasedRunFanOutStorageForTest(ctx context.Context, selected any, run string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return pipelinepersistence.CountLeasedRunFanOutStorageTx(ctx, tx, run)
	})
}

func CountFanOutAttemptResidueStorageForTest(ctx context.Context, selected any, command pipeline.FanOutChunkCommand) (int, error) {
	if err := command.Claim.Key.Validate(); err != nil {
		return 0, err
	}
	if len(command.Outcomes) == 0 || command.Outcomes[0].Ordinal < 0 || command.Outcomes[len(command.Outcomes)-1].Ordinal < command.Outcomes[0].Ordinal {
		return 0, fmt.Errorf("attempt residue observation requires its exact nonempty ordinal range")
	}
	return readFanOutSemanticStorage(ctx, selected, []string{command.Claim.Key.RunID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return pipelinepersistence.CountFanOutAttemptResidueStorageTx(ctx, tx, command)
	})
}

type FanInDiagnosticDeliveryStorage = delivery.FanInDiagnosticDeliveryStorage

func ReadFanInDiagnosticDeliveriesForTest(ctx context.Context, selected any) ([]FanInDiagnosticDeliveryStorage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	var out []FanInDiagnosticDeliveryStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = delivery.ReadFanInDiagnosticDeliveriesTx(ctx, tx)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func DeleteFanInSelectedPeriodOwnerForTest(ctx context.Context, selected any, runID, entityID string) (int64, error) {
	for _, id := range []string{runID, entityID} {
		if err := validateStandaloneStorageEvent(selected, id); err != nil {
			return 0, err
		}
	}
	var removed int64
	err := runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		removed, err = pipelinepersistence.DeleteFanInSelectedPeriodOwnerTx(ctx, tx, runID, entityID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return removed, nil
}

type NodeDeliveryDiagnosticStorage struct {
	Rows, DeadLetters, Receipts []string
}

func ReadServedRunDeliverySummaryForTest(ctx context.Context, selected any, runID string) (runtimedelivery.RunSummary, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return runtimedelivery.RunSummary{}, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return runtimedelivery.RunSummary{}, err
	}
	var summary runtimedelivery.RunSummary
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		switch owner := selected.(type) {
		case *PostgresStore:
			summary, err = owner.deliveryPostgresOwner.SummarizeRunTx(ctx, tx, runID)
		case *SQLiteRuntimeStore:
			summary, err = owner.deliverySQLiteOwner.SummarizeRunTx(ctx, tx, runID)
		}
		return err
	})
	if err != nil {
		return runtimedelivery.RunSummary{}, err
	}
	return summary, nil
}

func ReadNodeDeliveryTargetEncodingForTest(ctx context.Context, selected any, eventID, nodeID string) (string, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return "", err
	}
	if nodeID == "" || strings.TrimSpace(nodeID) != nodeID {
		return "", fmt.Errorf("node target observation requires its exact nonempty recipient")
	}
	_, postgres := selected.(*PostgresStore)
	var raw string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		raw, err = delivery.ReadNodeDeliveryTargetEncodingTx(ctx, tx, postgres, eventID, nodeID)
		return err
	})
	if err != nil {
		return "", err
	}
	return raw, nil
}

func ReadNodeDeliveryDiagnosticStorageForTest(ctx context.Context, selected any, eventID string) (NodeDeliveryDiagnosticStorage, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return NodeDeliveryDiagnosticStorage{}, err
	}
	_, postgres := selected.(*PostgresStore)
	var out NodeDeliveryDiagnosticStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out.Rows, err = delivery.ReadNodeDeliveryDiagnosticLinesTx(ctx, tx, postgres, eventID)
		if err != nil {
			return fmt.Errorf("delivery dump: %w", err)
		}
		out.DeadLetters, err = delivery.ReadNodeDeadLetterDiagnosticLinesTx(ctx, tx, postgres, eventID)
		if err != nil {
			return fmt.Errorf("dead letter dump: %w", err)
		}
		out.Receipts, err = pipelinepersistence.ReadNodeReceiptDiagnosticLinesTx(ctx, tx, postgres, eventID)
		if err != nil {
			return fmt.Errorf("receipt dump: %w", err)
		}
		return nil
	})
	if err != nil {
		return NodeDeliveryDiagnosticStorage{}, err
	}
	return out, nil
}

type CausalDeliveryStatusCount = delivery.CausalDeliveryStatusCount

type CausalDeliveryFrontier = delivery.CausalDeliveryFrontier
type CausalDeliveryFrontierProbe = delivery.CausalDeliveryFrontierProbe

const (
	FrontierComplete           = delivery.FrontierComplete
	FrontierMissing            = delivery.FrontierMissing
	FrontierPending            = delivery.FrontierPending
	FrontierDuplicateDelivery  = delivery.FrontierDuplicateDelivery
	FrontierDeadLetterDelivery = delivery.FrontierDeadLetterDelivery
)

func ReadCausalDeliveryFrontierForTest(ctx context.Context, selected any, runID, eventID string) (CausalDeliveryFrontier, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return CausalDeliveryFrontier{}, err
	}
	var out CausalDeliveryFrontier
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = delivery.ReadCausalDeliveryFrontier(ctx, tx, runID, eventID)
		return err
	})
	if err != nil {
		return CausalDeliveryFrontier{}, err
	}
	return out, nil
}

func ProbeCausalDeliveryFrontierForTest(ctx context.Context, selected any, probe CausalDeliveryFrontierProbe) (CausalDeliveryFrontier, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return CausalDeliveryFrontier{}, err
	}
	var out CausalDeliveryFrontier
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = delivery.ProbeCausalDeliveryFrontier(ctx, tx, probe)
		return err
	})
	if err != nil {
		return CausalDeliveryFrontier{}, err
	}
	return out, nil
}

func CountClosedSourceFanOutIssuanceForTest(ctx context.Context, selected any, runID, eventID string, cardinality int) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = pipelinepersistence.CountClosedSourceFanOutIssuance(ctx, tx, runID, eventID, cardinality)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

type DeliveryAttemptDiagnosticRow = delivery.DeliveryAttemptDiagnosticRow

type EventDeliveryDiagnosticRow = delivery.EventDeliveryDiagnosticRow

func ReadEventDeliveryDiagnosticRowsForTest(ctx context.Context, selected any, eventID string) ([]EventDeliveryDiagnosticRow, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	_, postgres := selected.(*PostgresStore)
	var out []EventDeliveryDiagnosticRow
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = delivery.ReadEventDeliveryDiagnosticRows(ctx, tx, eventID, postgres)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func ReadSubscriberEventNamesCreatedSinceForTest(ctx context.Context, selected any, since time.Time, subscriberID string) ([]string, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	var out []string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = delivery.ReadSubscriberEventNamesCreatedSince(ctx, tx, since, subscriberID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func ReadCausalDeliveryStatusCountsForTest(ctx context.Context, selected any, runID, eventID string) ([]CausalDeliveryStatusCount, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	var out []CausalDeliveryStatusCount
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = delivery.ReadCausalDeliveryStatusCounts(ctx, tx, runID, eventID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func ReadNonLogRunDeliveryClaimTotalsForTest(ctx context.Context, selected any, runID string) (deliveries, claims int64, err error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, 0, err
	}
	err = readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		deliveries, claims, err = delivery.ReadNonLogRunDeliveryClaimTotals(ctx, tx, runID)
		return err
	})
	if err != nil {
		return 0, 0, err
	}
	return deliveries, claims, nil
}

func ReadDeliveryAttemptDiagnosticRowsForTest(ctx context.Context, selected any, deliveryID string) ([]DeliveryAttemptDiagnosticRow, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	var out []DeliveryAttemptDiagnosticRow
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = delivery.ReadDeliveryAttemptDiagnosticRows(ctx, tx, deliveryID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

type EventPipelineReceiptObservationRow = pipelinepersistence.EventPipelineReceiptObservationRow

type SourceFanOutIntentDiagnosticRow = pipelinepersistence.SourceFanOutIntentDiagnosticRow

func ReadSourceFanOutIntentDiagnosticRowsForTest(ctx context.Context, selected any, runID, eventID string) ([]SourceFanOutIntentDiagnosticRow, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	var out []SourceFanOutIntentDiagnosticRow
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = pipelinepersistence.ReadSourceFanOutIntentDiagnosticRows(ctx, tx, runID, eventID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func ReadEarliestEventPipelineReceiptRowsForTest(ctx context.Context, selected any) ([]EventPipelineReceiptObservationRow, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	var out []EventPipelineReceiptObservationRow
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = pipelinepersistence.ReadEarliestEventPipelineReceiptRows(ctx, tx)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func ReadLatestPlatformPipelineReceiptStorageForTest(ctx context.Context, selected any, eventID string) (LatestPipelineReceiptStorage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return LatestPipelineReceiptStorage{}, err
	}
	var out LatestPipelineReceiptStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		_, postgres := selected.(*PostgresStore)
		var err error
		out, err = pipelinepersistence.ReadLatestPlatformPipelineReceiptStorage(ctx, tx, postgres, eventID)
		return err
	})
	if err != nil {
		return LatestPipelineReceiptStorage{}, err
	}
	return out, nil
}

func CountDeadLettersForOriginalEventForTest(ctx context.Context, selected any, originalEventID string) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = delivery.CountDeadLettersForOriginalEvent(ctx, tx, originalEventID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

type DeadLetterObservationRow = delivery.DeadLetterObservationRow

type TargetFailureDeadLetterStorage = delivery.TargetFailureDeadLetterStorage

func ReadTargetFailureDeadLetterStorageForTest(ctx context.Context, selected any, eventID string) (TargetFailureDeadLetterStorage, error) {
	if err := validateSelectedForkStorageIdentity(eventID); err != nil {
		return TargetFailureDeadLetterStorage{}, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return TargetFailureDeadLetterStorage{}, err
	}
	var out TargetFailureDeadLetterStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		_, postgres := selected.(*PostgresStore)
		var err error
		out, err = delivery.ReadTargetFailureDeadLetterStorage(ctx, tx, postgres, eventID)
		return err
	})
	if err != nil {
		return TargetFailureDeadLetterStorage{}, err
	}
	return out, nil
}

type ChainDepthDeadLetterStorage = delivery.ChainDepthDeadLetterStorage

func ReadChainDepthDeadLetterStorageForTest(ctx context.Context, selected any, runID, entityID string) (ChainDepthDeadLetterStorage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return ChainDepthDeadLetterStorage{}, err
	}
	var out ChainDepthDeadLetterStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		_, postgres := selected.(*PostgresStore)
		var err error
		out, err = delivery.ReadChainDepthDeadLetterStorage(ctx, tx, postgres, runID, entityID)
		return err
	})
	if err != nil {
		return ChainDepthDeadLetterStorage{}, err
	}
	return out, nil
}

func CountDeadLetterEntityRelationsSinceForTest(ctx context.Context, selected any, since time.Time, entityID string) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		_, postgres := selected.(*PostgresStore)
		var err error
		count, err = delivery.CountDeadLetterEntityRelationsSince(ctx, tx, postgres, since, entityID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func ReadDeadLetterObservationRowsForTest(ctx context.Context, selected any) ([]DeadLetterObservationRow, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	var out []DeadLetterObservationRow
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		_, postgres := selected.(*PostgresStore)
		var err error
		out, err = delivery.ReadDeadLetterObservationRows(ctx, tx, postgres)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Transferred from B70ab4d333 /888cb4958 under6010346696. The three OR arms
// are one exact handoff predicate, not a delivery-active substitute.
func ReadServedIncompletePipelineHandoffCountForTest(ctx context.Context, selected any, runID string) (int, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return 0, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = pipelinepersistence.ReadIncompletePipelineHandoffCount(ctx, tx, runID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func CountRunDeadLetterDeliveriesForTest(ctx context.Context, selected any, runID string) (int, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return 0, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = delivery.CountRunDeadLetterDeliveriesTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func ReadServedDeliveryStatusCountForTest(ctx context.Context, selected any, eventID, subscriberType, subscriberID string, statuses ...string) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	_, postgres := selected.(*PostgresStore)
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = delivery.ReadServedDeliveryStatusCount(ctx, tx, postgres, eventID, subscriberType, subscriberID, statuses...)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func ReadServedRunDebugSummaryForTest(ctx context.Context, selected any, runID string) (string, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return "", err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return "", err
	}
	backend := "sqlite"
	if _, postgres := selected.(*PostgresStore); postgres {
		backend = "postgres"
	}
	sections := make([]string, 0, 11)
	for _, scope := range []string{"runs", "entity_state", "flow_instances", "events", "event_deliveries", "settled_delivery_attempts", "event_receipts", "pipeline_obligations", "dead_letters", "delivery_agents", "runtime_logs"} {
		section := readServedRunDebugInspectionSection(ctx, selected, backend, scope, runID)
		if err := ctx.Err(); err != nil {
			return "", err
		}
		sections = append(sections, section)
	}
	return strings.Join(sections, "\n"), nil
}

func ReadServedRuntimeLogDebugSummaryForTest(ctx context.Context, selected any, runID string) (string, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return "", err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return "", err
	}
	backend := "sqlite"
	if _, postgres := selected.(*PostgresStore); postgres {
		backend = "postgres"
	}
	section := readServedRunDebugInspectionSection(ctx, selected, backend, "runtime_logs", runID)
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return section, nil
}

// Independent section transactions preserve later diagnostics after a failed
// PostgreSQL query, including for an unaccepted read-only process inspector.
func readServedRunDebugInspectionSection(ctx context.Context, selected any, backend, scope, runID string) string {
	var out []string
	var stage string
	inspect := func(scoped context.Context) error {
		var err error
		out, stage, err = readServedRunDebugSection(scoped, selected, backend, scope, runID)
		return err
	}
	var err error
	switch owner := selected.(type) {
	case *PostgresStore:
		err = owner.InspectSnapshot(ctx, inspect)
	case *SQLiteRuntimeStore:
		err = owner.InspectSnapshot(ctx, inspect)
	}
	if err != nil {
		return fmt.Sprintf("%s%s: %v", scope, stage, err)
	}
	if len(out) == 0 {
		return scope + ": []"
	}
	return scope + ": " + strings.Join(out, "; ")
}

func readServedRunDebugSection(ctx context.Context, selected any, backend, scope, runID string) ([]string, string, error) {
	if scope == "runs" || scope == "event_deliveries" || scope == "settled_delivery_attempts" || scope == "delivery_agents" {
		out, stage := []string{}, ""
		err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
			var err error
			if scope == "runs" {
				switch owner := selected.(type) {
				case *PostgresStore:
					out, stage, err = owner.runLifecyclePostgresOwner.ReadRunDebugStorageTx(ctx, tx, runID)
				case *SQLiteRuntimeStore:
					out, stage, err = owner.runLifecycleSQLiteOwner.ReadRunDebugStorageTx(ctx, tx, runID)
				}
			} else {
				out, stage, err = delivery.ReadRunDeliveryDebugSection(ctx, tx, backend == "postgres", scope, runID)
			}
			return err
		})
		return out, stage, err
	}
	sqlText := servedRunDebugSQL(backend, scope)
	if sqlText == "" {
		return nil, "", fmt.Errorf("unsupported debug query")
	}
	out := []string{}
	stage := ""
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, sqlText, runID)
		if err != nil {
			return err
		}
		defer rows.Close()
		columns, err := rows.Columns()
		if err != nil {
			stage = " columns"
			return err
		}
		for rows.Next() {
			values := make([]sql.NullString, len(columns))
			scan := make([]any, len(values))
			for i := range values {
				scan[i] = &values[i]
			}
			if err := rows.Scan(scan...); err != nil {
				stage = " scan"
				return err
			}
			cols := make([]string, len(values))
			for i, value := range values {
				if value.Valid {
					cols[i] = value.String
				}
			}
			out = append(out, fmt.Sprintf("%v", cols))
		}
		if err := rows.Err(); err != nil {
			stage = " rows"
			return err
		}
		return nil
	})
	return out, stage, err
}

func servedRunDebugSQL(backend, scope string) string {
	sqlText := ""
	switch backend {
	case "postgres":
		switch scope {
		case "entity_state":
			sqlText = `SELECT entity_id::text, COALESCE(flow_instance, ''), COALESCE(current_state, '') FROM entity_state WHERE run_id = $1::uuid ORDER BY created_at, entity_id LIMIT 5`
		case "flow_instances":
			sqlText = `SELECT instance_path, flow_template, status FROM flow_instances WHERE run_id = $1::uuid ORDER BY instance_path LIMIT 5`
		case "events":
			sqlText = `SELECT event_id::text, event_name, COALESCE(entity_id::text, ''), COALESCE(flow_instance, '') FROM events WHERE run_id = $1::uuid ORDER BY created_at, event_id LIMIT 5`
		case "event_receipts":
			sqlText = `SELECT r.event_id::text, r.subscriber_type, r.subscriber_id, r.outcome, COALESCE(r.reason_code, ''), COALESCE(r.side_effects::text, '') FROM event_receipts r JOIN events e ON e.event_id = r.event_id WHERE e.run_id = $1::uuid ORDER BY r.processed_at, r.event_id LIMIT 8`
		case "pipeline_obligations":
			sqlText = `SELECT e.event_id::text, e.event_name, COALESCE(r.outcome, ''), COALESCE(o.status, '') FROM events e LEFT JOIN event_receipts r ON r.event_id = e.event_id AND r.subscriber_type = 'platform' AND r.subscriber_id = 'pipeline' LEFT JOIN decision_card_route_obligations o ON o.event_id = e.event_id WHERE e.run_id = $1::uuid ORDER BY e.created_at, e.event_id LIMIT 12`
		case "dead_letters":
			sqlText = `SELECT d.original_event, COALESCE(d.entity_id::text, ''), d.failure::text FROM dead_letters d JOIN events e ON e.event_id = d.original_event_id WHERE e.run_id = $1::uuid ORDER BY d.created_at LIMIT 5`
		case "runtime_logs":
			sqlText = `SELECT payload::text FROM events WHERE run_id = $1::uuid AND event_name = 'platform.runtime_log' ORDER BY created_at LIMIT 8`
		}
	case "sqlite":
		switch scope {
		case "entity_state":
			sqlText = `SELECT entity_id, COALESCE(flow_instance, ''), COALESCE(current_state, '') FROM entity_state WHERE run_id = ? ORDER BY created_at, entity_id LIMIT 5`
		case "flow_instances":
			sqlText = `SELECT instance_path, flow_template, status FROM flow_instances WHERE run_id = ? ORDER BY instance_path LIMIT 5`
		case "events":
			sqlText = `SELECT event_id, event_name, COALESCE(entity_id, ''), COALESCE(flow_instance, '') FROM events WHERE run_id = ? ORDER BY created_at, event_id LIMIT 5`
		case "event_receipts":
			sqlText = `SELECT r.event_id, r.subscriber_type, r.subscriber_id, r.outcome, COALESCE(r.reason_code, ''), COALESCE(r.side_effects, '') FROM event_receipts r JOIN events e ON e.event_id = r.event_id WHERE e.run_id = ? ORDER BY r.processed_at, r.event_id LIMIT 8`
		case "pipeline_obligations":
			sqlText = `SELECT e.event_id, e.event_name, COALESCE(r.outcome, ''), COALESCE(o.status, '') FROM events e LEFT JOIN event_receipts r ON r.event_id = e.event_id AND r.subscriber_type = 'platform' AND r.subscriber_id = 'pipeline' LEFT JOIN decision_card_route_obligations o ON o.event_id = e.event_id WHERE e.run_id = ? ORDER BY e.created_at, e.event_id LIMIT 12`
		case "dead_letters":
			sqlText = `SELECT d.original_event, COALESCE(d.entity_id, ''), d.failure FROM dead_letters d JOIN events e ON e.event_id = d.original_event_id WHERE e.run_id = ? ORDER BY d.created_at LIMIT 5`
		case "runtime_logs":
			sqlText = `SELECT payload FROM events WHERE run_id = ? AND event_name = 'platform.runtime_log' ORDER BY created_at LIMIT 8`
		}
	}
	return sqlText
}
