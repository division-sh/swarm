package runtimepersistence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	"github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	deliveryadapter "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationlog"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
)

func CountSourceNonDiagnosticEventsStorageForTest(ctx context.Context, selected any, runID, sourceEventID, diagnosticClass, diagnosticName string) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return eventrecord.CountSourceNonDiagnosticEventsStorageTx(ctx, tx, runID, sourceEventID, diagnosticClass, diagnosticName)
	})
}
func CountGlobalBusinessEventsStorageForTest(ctx context.Context, selected any) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return eventrecord.CountGlobalBusinessEventsStorageTx(ctx, tx)
	})
}

type ServedForkRuntimeDiagnosticsStorage = eventrecord.ServedForkRuntimeDiagnosticsStorage

func ReadServedForkRuntimeDiagnosticsStorageForTest(ctx context.Context, selected any) (ServedForkRuntimeDiagnosticsStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (ServedForkRuntimeDiagnosticsStorage, error) {
		return eventrecord.ReadServedForkRuntimeDiagnosticsStorageTx(ctx, tx)
	})
}
func ReadTemplateCompletionEventDiagnosticsStorageForTest(ctx context.Context, selected any, runID string) (ServedTemplateCompletionDiagnosticStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (ServedTemplateCompletionDiagnosticStorage, error) {
		out, err := eventrecord.ReadTemplateCompletionEventDiagnosticsStorageTx(ctx, tx, runID)
		return ServedTemplateCompletionDiagnosticStorage(out), err
	})
}
func ReadReporterEventSnapshotStorageForTest(ctx context.Context, selected any, runID string) (ServedTemplateCompletionDiagnosticStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (ServedTemplateCompletionDiagnosticStorage, error) {
		out, err := eventrecord.ReadReporterEventSnapshotStorageTx(ctx, tx, runID)
		return ServedTemplateCompletionDiagnosticStorage(out), err
	})
}
func CountPhysicalEventsStorageForTest(ctx context.Context, selected any) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return eventrecord.CountPhysicalEventsStorageTx(ctx, tx)
	})
}

type StandingEventNameDiagnostic = eventrecord.StandingEventNameDiagnostic

func ReadStandingEventNameDiagnosticsForTest(ctx context.Context, selected any) ([]StandingEventNameDiagnostic, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) ([]StandingEventNameDiagnostic, error) {
		return eventrecord.ReadStandingEventNameDiagnosticsTx(ctx, tx, postgres)
	})
}

func ReadStandingRuntimeLogDiagnosticsForTest(ctx context.Context, selected any) ([]string, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) ([]string, error) {
		return eventrecord.ReadStandingRuntimeLogDiagnosticsTx(ctx, tx, postgres)
	})
}

func ReadWorkflowTimerEventIdentityForTest(ctx context.Context, selected any, runID, eventName string) (string, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return eventrecord.ReadWorkflowTimerEventIdentityTx(ctx, tx, postgres, runID, eventName)
	})
}

func ReadLatestWorkflowTimerRuntimeLogTextForTest(ctx context.Context, selected any) (string, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return eventrecord.ReadLatestWorkflowTimerRuntimeLogTextTx(ctx, tx, postgres)
	})
}

type RunPublicationHistoryStorage struct{ Events, Revisions int }

type A2MapHistoryFootprintStorage struct{ Mutations, Facts, Revisions int }

type A2RetainedSourceFootprintStorage struct {
	Mutations, Facts, Revisions, Publications, Deliveries, Outcomes int
	Cursor, Cardinality                                             int
	LatestRevision                                                  int64
	Status                                                          string
	Diagnostic                                                      []byte
}

func checkedFootprintCountForTest(count int64) (int, error) {
	out := int(count)
	if int64(out) != count {
		return 0, fmt.Errorf("physical footprint count overflows int")
	}
	return out, nil
}

func countFootprintRevisionsForTest(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	count, err := runforkrevision.CountActivityJournalRevisionsForTest(ctx, tx, runID)
	if err != nil {
		return 0, err
	}
	return checkedFootprintCountForTest(count)
}

func ReadA2MapHistoryFootprintStorageForTest(ctx context.Context, selected any, runID string) (A2MapHistoryFootprintStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (A2MapHistoryFootprintStorage, error) {
		var out A2MapHistoryFootprintStorage
		var err error
		out.Mutations, err = mutationlog.CountRunMutationStorageTx(ctx, tx, runID)
		if err != nil {
			return out, err
		}
		revisions, facts, err := runforkrevision.CountNotifyFanOutRevisionStorageForTest(ctx, tx, runID)
		if err != nil {
			return out, err
		}
		out.Facts, err = checkedFootprintCountForTest(facts)
		if err != nil {
			return out, err
		}
		out.Revisions, err = checkedFootprintCountForTest(revisions)
		return out, err
	})
}

func ReadA2JoinHydrationFootprintStorageForTest(ctx context.Context, selected any, runID string) ([5]int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) ([5]int, error) {
		var out [5]int
		for i, count := range [...]func(context.Context, *sql.Tx, string) (int, error){
			mutationlog.CountRunMutationStorageTx,
			countFootprintRevisionsForTest,
			genericschedule.CountTimerRowsForRun,
			eventrecord.CountRunEventStorageTx,
			pipelinepersistence.CountNestedRunFanOutOutcomesTx,
		} {
			var err error
			out[i], err = count(ctx, tx, runID)
			if err != nil {
				return out, err
			}
		}
		return out, nil
	})
}

func ReadA2CancellationFootprintStorageForTest(ctx context.Context, selected any, runID string) ([5]int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) ([5]int, error) {
		var out [5]int
		for i, count := range [...]func(context.Context, *sql.Tx, string) (int, error){
			eventrecord.CountRunEventStorageTx,
			deliveryadapter.CountRunDeliveryStorageTx,
			deliveryadapter.CountRunDeliveryAttemptStorageTx,
			mutationlog.CountRunMutationStorageTx,
			countFootprintRevisionsForTest,
		} {
			var err error
			out[i], err = count(ctx, tx, runID)
			if err != nil {
				return out, err
			}
		}
		return out, nil
	})
}

func ReadA2RetainedSourceFootprintStorageForTest(ctx context.Context, selected any, runID string) (A2RetainedSourceFootprintStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (A2RetainedSourceFootprintStorage, error) {
		var out A2RetainedSourceFootprintStorage
		var err error
		out.Mutations, err = mutationlog.CountRunMutationStorageTx(ctx, tx, runID)
		if err != nil {
			return out, err
		}
		revisions, facts, err := runforkrevision.CountNotifyFanOutRevisionStorageForTest(ctx, tx, runID)
		if err != nil {
			return out, err
		}
		out.Facts, err = checkedFootprintCountForTest(facts)
		if err != nil {
			return out, err
		}
		out.Revisions, err = checkedFootprintCountForTest(revisions)
		if err != nil {
			return out, err
		}
		out.LatestRevision, err = runforkrevision.ReadLatestRunRevisionStorageForTestTx(ctx, tx, runID)
		if err != nil {
			return out, err
		}
		out.Publications, err = eventrecord.CountRunEventStorageTx(ctx, tx, runID)
		if err != nil {
			return out, err
		}
		out.Deliveries, err = deliveryadapter.CountRunEventLinkedDeliveryStorageTx(ctx, tx, runID)
		if err != nil {
			return out, err
		}
		out.Outcomes, err = pipelinepersistence.CountNestedRunFanOutOutcomesTx(ctx, tx, runID)
		if err != nil {
			return out, err
		}
		metadata, err := pipelinepersistence.ReadFanOutSourceFootprintMetadataStorageTx(ctx, tx, runID)
		out.Cursor, out.Cardinality, out.Status, out.Diagnostic = metadata.Cursor, metadata.Cardinality, metadata.Status, metadata.Diagnostic
		return out, err
	})
}

func ReadRunPublicationHistoryStorageForTest(ctx context.Context, selected any, runID string) (RunPublicationHistoryStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (RunPublicationHistoryStorage, error) {
		var out RunPublicationHistoryStorage
		var err error
		out.Events, err = eventrecord.CountRunEventStorageTx(ctx, tx, runID)
		if err != nil {
			return RunPublicationHistoryStorage{}, err
		}
		revisions, err := runforkrevision.CountActivityJournalRevisionsForTest(ctx, tx, runID)
		if err != nil {
			return RunPublicationHistoryStorage{}, err
		}
		out.Revisions = int(revisions)
		if int64(out.Revisions) != revisions {
			return RunPublicationHistoryStorage{}, fmt.Errorf("run history revision count overflows int")
		}
		return out, nil
	})
}

// Connector reads retain physical event scope; no dispatch eligibility or clock is inferred.
func ReadConnectorNormalizedProviderEventIDForTest(ctx context.Context, selected any, runID, entityID, eventName, providerEventID string) (string, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return "", err
	}
	if err := validateStandaloneStorageEvent(selected, entityID); err != nil {
		return "", err
	}
	if eventName == "" || strings.TrimSpace(eventName) != eventName {
		return "", fmt.Errorf("connector storage observation requires eventName")
	}
	if providerEventID == "" || strings.TrimSpace(providerEventID) != providerEventID {
		return "", fmt.Errorf("connector storage observation requires providerEventID")
	}
	_, postgres := selected.(*PostgresStore)
	var out string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadConnectorNormalizedProviderEventIDTx(ctx, tx, postgres, runID, entityID, eventName, providerEventID)
		return err
	})
	if err != nil {
		return "", err
	}
	return out, nil
}

func CountConnectorNamedSourceEventForTest(ctx context.Context, selected any, runID, eventName, sourceEventID string) (int, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return 0, err
	}
	if err := validateStandaloneStorageEvent(selected, sourceEventID); err != nil {
		return 0, err
	}
	if strings.TrimSpace(eventName) == "" {
		return 0, fmt.Errorf("connector storage observation requires eventName")
	}
	var out int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.CountConnectorNamedSourceEventTx(ctx, tx, runID, eventName, sourceEventID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func CountConnectorNamedEventIdentityForTest(ctx context.Context, selected any, eventID, eventName string) (int, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return 0, err
	}
	if strings.TrimSpace(eventName) == "" {
		return 0, fmt.Errorf("connector storage observation requires eventName")
	}
	var out int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.CountConnectorNamedEventIdentityTx(ctx, tx, eventID, eventName)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func ReadConnectorEventChronologyForTest(ctx context.Context, selected any, runID string) (string, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return "", err
	}

	_, postgres := selected.(*PostgresStore)
	var out string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadConnectorEventChronologyTx(ctx, tx, postgres, runID)
		return err
	})
	if err != nil {
		return "", err
	}
	return out, nil
}

func ReadBusRoutingEventNamesForTest(ctx context.Context, selected any) ([]string, error) {
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return nil, err
	}
	var out []string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadBusRoutingEventNamesTx(ctx, tx)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

type BusRoutingEventDiagnostic = eventrecord.BusRoutingEventDiagnostic

func CountUndeclaredBusChildEventsForTest(ctx context.Context, selected any) (int, error) {
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = eventrecord.CountUndeclaredBusChildEventsTx(ctx, tx)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func ReadBusRoutingEventDiagnosticsForTest(ctx context.Context, selected any) ([]BusRoutingEventDiagnostic, error) {
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return nil, err
	}
	_, postgres := selected.(*PostgresStore)
	var out []BusRoutingEventDiagnostic
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadBusRoutingEventDiagnosticsTx(ctx, tx, postgres)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func ReadChannelFirstActivitySourceRouteForTest(ctx context.Context, selected any, runID string) ([]byte, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return nil, err
	}
	var route []byte
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		route, err = eventrecord.ReadChannelFirstActivitySourceRouteTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return route, nil
}

type SelectedCausalDiagnosticStorage struct {
	Events     []eventrecord.LifecycleDiagnosticEventLineage
	Projection []byte
}

type ActivityForkSourceStorage struct {
	Flow, EntityType, Stage, AttemptStatus string
	Fields, Accumulator, Payload           []byte
}

type ActivityForkResultStorage = eventrecord.ActivityForkResultStorage

type ActivityForkAttemptStorage = activityjournal.ActivityForkAttemptStorage

// Preserve the three independent source observations, including the original
// missing-attempt empty status. This is not a new atomic source snapshot.
func ReadActivityForkSourceStorageForTest(ctx context.Context, selected any, runID, entityID, requestID string) (ActivityForkSourceStorage, error) {
	var out ActivityForkSourceStorage
	for _, id := range []string{runID, entityID, requestID} {
		if err := validateStandaloneStorageEvent(selected, id); err != nil {
			return ActivityForkSourceStorage{}, err
		}
	}
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		row, err := pipelinepersistence.ReadActivityForkEntityStorageTx(ctx, tx, runID, entityID)
		out.Flow, out.EntityType, out.Stage = row.Flow, row.EntityType, row.Stage
		out.Fields, out.Accumulator = row.Fields, row.Accumulator
		return err
	})
	if err == nil {
		err = readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
			var err error
			if _, postgres := selected.(*PostgresStore); postgres {
				out.Payload, err = eventrecordpostgres.ReadActivityForkPayloadBytesStorage(ctx, tx, runID, requestID)
			} else {
				out.Payload, err = eventrecordsqlite.ReadActivityForkPayloadBytesStorage(ctx, tx, runID, requestID)
			}
			return err
		})
	}
	if err == nil {
		err = readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
			var err error
			out.AttemptStatus, err = activityjournal.ReadActivityForkAttemptStatusStorageTx(ctx, tx, runID, requestID)
			return err
		})
	}
	if err != nil {
		return ActivityForkSourceStorage{}, err
	}
	return out, nil
}

func ReadActivityForkRequestPayloadStorageForTest(ctx context.Context, selected any, runID, eventID string) ([]byte, error) {
	for _, id := range []string{runID, eventID} {
		if err := validateStandaloneStorageEvent(selected, id); err != nil {
			return nil, err
		}
	}
	var out []byte
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadActivityForkRequestPayloadStorageTx(ctx, tx, runID, eventID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func ReadActivityForkResultStorageForTest(ctx context.Context, selected any, runID, eventID string) (ActivityForkResultStorage, error) {
	for _, id := range []string{runID, eventID} {
		if err := validateStandaloneStorageEvent(selected, id); err != nil {
			return ActivityForkResultStorage{}, err
		}
	}
	var out ActivityForkResultStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadActivityForkResultStorageTx(ctx, tx, runID, eventID)
		return err
	})
	if err != nil {
		return ActivityForkResultStorage{}, err
	}
	return out, nil
}

func ReadActivityForkAttemptStorageForTest(ctx context.Context, selected any, requestID string) (ActivityForkAttemptStorage, error) {
	if err := validateStandaloneStorageEvent(selected, requestID); err != nil {
		return ActivityForkAttemptStorage{}, err
	}
	_, postgres := selected.(*PostgresStore)
	var out ActivityForkAttemptStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = activityjournal.ReadActivityForkAttemptStorageTx(ctx, tx, postgres, requestID)
		return err
	})
	if err != nil {
		return ActivityForkAttemptStorage{}, err
	}
	return out, nil
}

func CountActivityForkAttemptStorageForTest(ctx context.Context, selected any, requestID string) (int, error) {
	if err := validateStandaloneStorageEvent(selected, requestID); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = activityjournal.CountActivityForkAttemptStorageTx(ctx, tx, requestID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func StampHistoricalReplayScopeForTest(ctx context.Context, selected any, runID, eventID string, scope runtimepipelineobligation.CommittedScope, at time.Time) error {
	for _, id := range []string{runID, eventID} {
		if err := validateStandaloneStorageEvent(selected, id); err != nil {
			return err
		}
	}
	_, postgres := selected.(*PostgresStore)
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		identity, err := eventrecord.ReadPublicationEventIdentityStorageTx(ctx, tx, eventID)
		if err != nil {
			return err
		}
		if identity.RunID != runID {
			return fmt.Errorf("historical replay scope event belongs to a different run")
		}
		return pipelinepersistence.StampHistoricalReplayScopeFixtureTx(ctx, tx, postgres, runID, eventID, scope, at)
	})
}

func ReadSelectedSourceRefusalStorageForTest(ctx context.Context, selected any, sourceRunID, sourceEventID string) (map[string]int, error) {
	for _, id := range []string{sourceRunID, sourceEventID} {
		if err := validateStandaloneStorageEvent(selected, id); err != nil {
			return nil, err
		}
	}
	var out map[string]int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = runforkpersistence.ReadSelectedSourceRefusalStorageTx(ctx, tx, sourceRunID)
		if err != nil {
			return err
		}
		out["fork_events"], err = eventrecord.CountSourceEventForeignRunStorageTx(ctx, tx, sourceEventID, sourceRunID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func ReadSelectedForkRefusalStorageForTest(ctx context.Context, selected any, forkRunID string) (map[string]int, error) {
	if err := validateStandaloneStorageEvent(selected, forkRunID); err != nil {
		return nil, err
	}
	var out map[string]int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = runforkpersistence.ReadSelectedForkRefusalStorageTx(ctx, tx, forkRunID)
		if err != nil {
			return err
		}
		out["events"], err = eventrecord.CountRunEventStorageTx(ctx, tx, forkRunID)
		if err != nil {
			return err
		}
		out["event_deliveries"], err = deliveryadapter.CountSelectedForkDeliveryStorageTx(ctx, tx, forkRunID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func CountRunEventOrSourceEventStorageForTest(ctx context.Context, selected any, runID, eventID string) (int, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return 0, err
	}
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return 0, err
	}
	var out int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.CountRunEventOrSourceEventStorageTx(ctx, tx, runID, eventID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func CountRunExactEventStorageForTest(ctx context.Context, selected any, runID, eventID string) (int, error) {
	for _, id := range []string{runID, eventID} {
		if err := validateStandaloneStorageEvent(selected, id); err != nil {
			return 0, err
		}
	}
	var out int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.CountRunExactEventStorageTx(ctx, tx, runID, eventID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func CountApplicationRunEventStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return eventrecord.CountApplicationRunEventStorageTx(ctx, tx, runID)
	})
}

func ReadAgentSettledAttemptCountForTest(ctx context.Context, selected any, eventID, agentID string) (int, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return 0, err
	}
	_, postgres := selected.(*PostgresStore)
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = deliveryadapter.ReadAgentSettledAttemptCountTx(ctx, tx, postgres, eventID, agentID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func SetEventChainDepthForTest(ctx context.Context, selected any, eventID string, depth int) error {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return err
	}
	_, postgres := selected.(*PostgresStore)
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return eventrecord.SetFixtureEventChainDepthTx(ctx, tx, postgres, eventID, depth)
	})
}

func ClearEventRunForRecoveryForTest(ctx context.Context, selected any, eventID string) error {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return err
	}
	_, postgres := selected.(*PostgresStore)
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return eventrecord.ClearEventRunForRecoveryTx(ctx, tx, postgres, eventID)
	})
}

type SelectedCausalDiagnosticConservation struct {
	Events, Pending int
}

func ReadSelectedCausalDiagnosticStorageForTest(ctx context.Context, selected any, outboxID string) (SelectedCausalDiagnosticStorage, error) {
	if err := validateSelectedForkStorageIdentity(outboxID); err != nil {
		return SelectedCausalDiagnosticStorage{}, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return SelectedCausalDiagnosticStorage{}, err
	}
	var out SelectedCausalDiagnosticStorage
	_, postgres := selected.(*PostgresStore)
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out.Events, err = eventrecord.ReadLifecycleDiagnosticEventLineage(ctx, tx, postgres, outboxID)
		if err != nil {
			return err
		}
		out.Projection, err = eventpersistence.ReadLifecycleDiagnosticProjection(ctx, tx, outboxID)
		return err
	})
	if err != nil {
		return SelectedCausalDiagnosticStorage{}, err
	}
	return out, nil
}

func ReadSelectedCausalDiagnosticConservationForTest(ctx context.Context, selected any, outboxID string) (SelectedCausalDiagnosticConservation, error) {
	if err := validateSelectedForkStorageIdentity(outboxID); err != nil {
		return SelectedCausalDiagnosticConservation{}, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return SelectedCausalDiagnosticConservation{}, err
	}
	var out SelectedCausalDiagnosticConservation
	_, postgres := selected.(*PostgresStore)
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out.Events, err = eventrecord.CountLifecycleDiagnosticEvents(ctx, tx, postgres, outboxID)
		if err != nil {
			return err
		}
		out.Pending, err = eventpersistence.CountPendingLifecycleDiagnostic(ctx, tx, outboxID)
		return err
	})
	if err != nil {
		return SelectedCausalDiagnosticConservation{}, err
	}
	return out, nil
}

type CausalEventStorageRow = eventrecord.CausalObservationRow

type GlobalEventChronologyRow = eventrecord.GlobalEventChronologyRow

func ReadGlobalEventChronologyForTest(ctx context.Context, selected any) ([]GlobalEventChronologyRow, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	_, postgres := selected.(*PostgresStore)
	var out []GlobalEventChronologyRow
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadGlobalEventChronologyTx(ctx, tx, postgres)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

type ScatterGatherPhysicalCounts struct {
	Entities, Timers, FanOutIntents, DomainEvents int
}

func ReadScatterGatherPhysicalCountsForTest(ctx context.Context, selected any, runID string) (ScatterGatherPhysicalCounts, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return ScatterGatherPhysicalCounts{}, err
	}
	var out ScatterGatherPhysicalCounts
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		if out.Entities, err = pipelinepersistence.CountWorkflowFieldRowsForRun(ctx, tx, runID); err != nil {
			return err
		}
		if out.Timers, err = genericschedule.CountTimerRowsForRun(ctx, tx, runID); err != nil {
			return err
		}
		if out.FanOutIntents, err = pipelinepersistence.CountFanOutIntentsForRun(ctx, tx, runID); err != nil {
			return err
		}
		out.DomainEvents, err = eventrecord.CountScatterGatherDomainEvents(ctx, tx, runID)
		return err
	})
	if err != nil {
		return ScatterGatherPhysicalCounts{}, err
	}
	return out, nil
}

func CountRunEventNameStorageForTest(ctx context.Context, selected any, run, name string) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = eventrecord.CountRunNamedEventStorageTx(ctx, tx, run, name)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func CountA2MembershipRefusalEventsForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return eventrecord.CountA2MembershipRefusalEventsTx(ctx, tx, runID)
	})
}

func SetA2AccumulatorPublicationFailureForTest(ctx context.Context, selected any, enabled bool) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	_, postgres := selected.(*PostgresStore)
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return eventrecord.SetA2AccumulatorPublicationFailureTx(ctx, tx, postgres, enabled)
	})
}

type SourceRouteSettlementStorage = eventrecord.SourceRouteSettlementStorage

type FanInDiagnosticEventStorage = eventrecord.FanInDiagnosticEventStorage

func hydrateFanOutOutcomePayloadBytesForTest(ctx context.Context, tx *sql.Tx, selected any, rows []FanOutSemanticOutcomeStorage) ([]FanOutSemanticOutcomeStorage, error) {
	var ids []string
	seen := make(map[string]struct{})
	for _, row := range rows {
		if row.EventIDPresent {
			if _, present := seen[row.EventID]; present {
				continue
			}
			seen[row.EventID] = struct{}{}
			ids = append(ids, row.EventID)
		}
	}
	var payloads map[string][]byte
	var err error
	if _, postgres := selected.(*PostgresStore); postgres {
		payloads, err = eventrecordpostgres.ReadFanOutOutcomePayloadBytesStorage(ctx, tx, ids)
	} else {
		payloads, err = eventrecordsqlite.ReadFanOutOutcomePayloadBytesStorage(ctx, tx, ids)
	}
	if err != nil {
		return nil, err
	}
	for i := range rows {
		if !rows[i].EventIDPresent {
			continue
		}
		payload, found := payloads[rows[i].EventID]
		if !found {
			return nil, fmt.Errorf("fan-out outcome lost its event within the same native read snapshot: %s", rows[i].EventID)
		}
		rows[i].Payload = append([]byte(nil), payload...)
	}
	return rows, nil
}

func ReadLastThreeFanOutRuntimeLogPayloadsForTest(ctx context.Context, selected any, run string) ([][]byte, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run}, func(ctx context.Context, tx *sql.Tx) ([][]byte, error) {
		return eventrecord.ReadLastThreeFanOutRuntimeLogPayloadsTx(ctx, tx, run)
	})
}

func ReadStageLifecycleRuntimeLogPayloadsForTest(ctx context.Context, selected any) ([]string, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) ([]string, error) {
		return eventrecord.ReadStageLifecycleRuntimeLogPayloadsTx(ctx, tx)
	})
}

func CountPostgresRunStatusSemanticEventsForTest(ctx context.Context, selected any, runID string) (int, error) {
	if err := validatePostgresPublicationIdentities(selected, runID); err != nil {
		return 0, err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return eventrecord.CountPostgresRunStatusSemanticEventsTx(ctx, tx, runID)
	})
}

func ReadPostgresRecoveryFailedPayloadStorageForTest(ctx context.Context, selected any) ([]byte, error) {
	if err := validatePostgresPublicationIdentities(selected); err != nil {
		return nil, err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, eventrecord.ReadPostgresRecoveryFailedPayloadStorageTx)
}

func ReadPostgresResetPayloadStorageForTest(ctx context.Context, selected any) ([]byte, error) {
	if err := validatePostgresPublicationIdentities(selected); err != nil {
		return nil, err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, eventrecord.ReadPostgresResetPayloadStorageTx)
}

func ReadNestedSourceEventIDsForTest(ctx context.Context, selected any, run, name, parent string) ([]string, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run, parent}, func(ctx context.Context, tx *sql.Tx) ([]string, error) {
		return eventrecord.ReadNestedSourceEventIDsTx(ctx, tx, run, name, parent)
	})
}

type ArtifactCallbackEventContextStorage = eventrecord.ArtifactCallbackEventContextStorage

func ReadPostgresArtifactCallbackEventContextForTest(ctx context.Context, selected any, event string) (ArtifactCallbackEventContextStorage, error) {
	if err := validatePostgresPublicationIdentities(selected, event); err != nil {
		return ArtifactCallbackEventContextStorage{}, err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (ArtifactCallbackEventContextStorage, error) {
		return eventrecord.ReadPostgresArtifactCallbackEventContextTx(ctx, tx, event)
	})
}

func ReadPostgresArtifactCallbackSourceRouteForTest(ctx context.Context, selected any, event string) (string, error) {
	if err := validatePostgresPublicationIdentities(selected, event); err != nil {
		return "", err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return eventrecord.ReadPostgresArtifactCallbackSourceRouteTx(ctx, tx, event)
	})
}

func ReadTemplateNamedEventIdentityForTest(ctx context.Context, selected any, name string) (string, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return eventrecord.ReadTemplateNamedEventIdentityTx(ctx, tx, name)
	})
}

func CountGateSourceNamedEventsForTest(ctx context.Context, selected any, name, source string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{source}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return eventrecord.CountGateSourceNamedEventsTx(ctx, tx, name, source)
	})
}

type A2WorkerOutputStorage = eventrecord.A2WorkerOutputStorage

func ReadA2WorkerOutputsForTest(ctx context.Context, selected any, runID, sourceEventID string) (A2WorkerOutputStorage, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, []string{runID, sourceEventID}, func(ctx context.Context, tx *sql.Tx) (A2WorkerOutputStorage, error) {
		return eventrecord.ReadA2WorkerOutputsTx(ctx, tx, postgres, runID, sourceEventID)
	})
}

func CountSourceEventPublicationsForTest(ctx context.Context, selected any, sourceEventID string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{sourceEventID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return eventrecord.CountSourceEventPublicationsTx(ctx, tx, sourceEventID)
	})
}

func ReadSourceEventPublicationIdentityForTest(ctx context.Context, selected any, sourceEventID string) (string, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{sourceEventID}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return eventrecord.ReadSourceEventPublicationIdentityTx(ctx, tx, sourceEventID)
	})
}

func ReadSourceNamedChildEventIDsForTest(ctx context.Context, selected any, parentID, name string) ([]string, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{parentID}, func(ctx context.Context, tx *sql.Tx) ([]string, error) {
		return eventrecord.ReadSourceNamedChildEventIDsTx(ctx, tx, parentID, name)
	})
}

func ReadGateRunEventNamesForTest(ctx context.Context, selected any, run string) ([]string, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run}, func(ctx context.Context, tx *sql.Tx) ([]string, error) {
		return eventrecord.ReadGateRunEventNamesTx(ctx, tx, run)
	})
}
func ReadTemplateSourceNamedEventIdentityForTest(ctx context.Context, selected any, name, source string) (string, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{source}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return eventrecord.ReadTemplateSourceNamedEventIdentityTx(ctx, tx, name, source)
	})
}
func ReadTemplateEventPayloadTextForTest(ctx context.Context, selected any, event string) (string, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{event}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return eventrecord.ReadTemplateEventPayloadTextTx(ctx, tx, event)
	})
}
func CountTemplateEventIdentityForTest(ctx context.Context, selected any, event string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{event}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return eventrecord.CountTemplateEventIdentityTx(ctx, tx, event)
	})
}

func ReadNestedRunNamedEventIDsForTest(ctx context.Context, selected any, run, name string) ([]string, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run}, func(ctx context.Context, tx *sql.Tx) ([]string, error) {
		return eventrecord.ReadNestedRunNamedEventIDsTx(ctx, tx, run, name)
	})
}

func ReadNestedSiblingSourceEventIDForTest(ctx context.Context, selected any, run, event string) (string, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run, event}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return eventrecord.ReadNestedSiblingSourceEventIDTx(ctx, tx, run, event)
	})
}

func CountNestedRunEventIdentityForTest(ctx context.Context, selected any, run, event string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run, event}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return eventrecord.CountNestedRunEventIdentityTx(ctx, tx, run, event)
	})
}

func ReadRepeatedFanOutCompletionEventIDsForTest(ctx context.Context, selected any, run string) ([]string, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run}, func(ctx context.Context, tx *sql.Tx) ([]string, error) {
		return eventrecord.ReadRepeatedFanOutCompletionEventIDsTx(ctx, tx, run)
	})
}

func ReadFanInPeriodReportedEventIDsForTest(ctx context.Context, selected any) ([]string, error) {
	return readFanInReportedEventIDsForTest(ctx, selected, false)
}

func ReadFanInOperatingReportedEventIDsForTest(ctx context.Context, selected any) ([]string, error) {
	return readFanInReportedEventIDsForTest(ctx, selected, true)
}

func readFanInReportedEventIDsForTest(ctx context.Context, selected any, operating bool) ([]string, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	var out []string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		if operating {
			out, err = eventrecord.ReadFanInOperatingReportedEventIDsTx(ctx, tx)
		} else {
			out, err = eventrecord.ReadFanInPeriodReportedEventIDsTx(ctx, tx)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func ReadFanInEventRouteSettlementForTest(ctx context.Context, selected any, eventID string) (string, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return "", err
	}
	var out string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadFanInEventRouteSettlementTx(ctx, tx, eventID)
		return err
	})
	if err != nil {
		return "", err
	}
	return out, nil
}

func ReadFanInDiagnosticEventsForTest(ctx context.Context, selected any) ([]FanInDiagnosticEventStorage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	var out []FanInDiagnosticEventStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadFanInDiagnosticEventsTx(ctx, tx)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func ReadSourceRouteSettlementStorageForTest(ctx context.Context, selected any, runID, eventID string) (SourceRouteSettlementStorage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return SourceRouteSettlementStorage{}, err
	}
	var out SourceRouteSettlementStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadSourceRouteSettlementStorage(ctx, tx, runID, eventID)
		return err
	})
	if err != nil {
		return SourceRouteSettlementStorage{}, err
	}
	return out, nil
}

func CountChainDepthDiagnosticStorageForTest(ctx context.Context, selected any, runID, entityID, handlerNode string) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		_, postgres := selected.(*PostgresStore)
		var err error
		count, err = eventrecord.CountChainDepthDiagnosticStorage(ctx, tx, postgres, runID, entityID, handlerNode)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func ReadLatestHandlerErrorLogForTest(ctx context.Context, selected any) (json.RawMessage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	var out json.RawMessage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		_, postgres := selected.(*PostgresStore)
		var err error
		out, err = eventrecord.ReadLatestHandlerErrorLog(ctx, tx, postgres)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func ReadCausalEventStorageSinceForTest(ctx context.Context, selected any, since time.Time) ([]CausalEventStorageRow, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	var out []CausalEventStorageRow
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		_, postgres := selected.(*PostgresStore)
		var err error
		out, err = eventrecord.ReadCausalObservationSince(ctx, tx, postgres, since)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Record-bearing fixture evidence uses the publication fixture's closed codec.
// Callers receive detached facts, never another record interpreter.
type SemanticEventFixtureEvidence struct {
	Record                      eventrecord.Record
	RecordFound                 bool
	RevisionCount               int
	PipelineReceiptCount        int
	PipelineReceiptOutcome      string
	PipelineReceiptReason       string
	PipelineReceiptFailure      *runtimefailures.Envelope
	CommittedScope              string
	CommittedScopeFound         bool
	RunStatus                   string
	RunEventCount               int
	SettledDeliveryAttemptCount int
	DeliveryProjections         map[string][18]string
	DeliveryStatuses            map[string]string
	NonPlatformReceiptCount     int
}

func loadCanonicalFixtureRecordTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID string) (eventrecord.Record, bool, error) {
	if postgres {
		return eventrecordpostgres.Load(ctx, tx, eventID)
	}
	return eventrecordsqlite.Load(ctx, tx, eventID)
}

// LoadCanonicalEventRecordForTest requires presence; storage reading and
// complete decoding belong only to ReadCanonicalEventRecordForTest.
func LoadCanonicalEventRecordForTest(ctx context.Context, selected any, eventID string) (events.Event, error) {
	var empty events.Event
	event, found, err := ReadCanonicalEventRecordForTest(ctx, selected, eventID)
	if err != nil {
		return empty, err
	}
	if !found {
		return empty, fmt.Errorf("canonical event record %s is missing", eventID)
	}
	return event, nil
}
func CommitSemanticEventFixtureForTest(ctx context.Context, selected any, admitted events.AdmittedEvent, settlement events.RouteSettlement, routes []events.DeliveryRoute, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition) (bool, error) {
	return commitSelectedSemanticEventFixtureForTest(ctx, selected, admitted, settlement, routes, scope, disposition, false, false)
}

func ReadLatestRuntimeLogRecordForTest(ctx context.Context, selected any) (events.Event, error) {
	return readLatestRuntimeLogRecordForTest(ctx, selected, false)
}

func ReadLatestStartupRecoveryDecisionRecordForTest(ctx context.Context, selected any) (events.Event, error) {
	return readLatestRuntimeLogRecordForTest(ctx, selected, true)
}

func readLatestRuntimeLogRecordForTest(ctx context.Context, selected any, startupDecision bool) (events.Event, error) {
	var empty events.Event
	if err := validateChannelObservationOwner(selected); err != nil {
		return empty, err
	}
	_, postgres := selected.(*PostgresStore)
	query := `SELECT CAST(event_id AS TEXT) FROM events WHERE event_name='platform.runtime_log' ORDER BY created_at DESC LIMIT 1`
	if startupDecision {
		query = `SELECT CAST(event_id AS TEXT) FROM events WHERE event_name='platform.runtime_log' AND json_extract(payload,'$.details.component')='runtime' AND json_extract(payload,'$.details.action')='startup_recovery_decision' ORDER BY created_at DESC LIMIT 1`
		if postgres {
			query = `SELECT CAST(event_id AS TEXT) FROM events WHERE event_name='platform.runtime_log' AND payload->'details'->>'component'='runtime' AND payload->'details'->>'action'='startup_recovery_decision' ORDER BY created_at DESC LIMIT 1`
		}
	}
	var record eventrecord.Record
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var id string
		if err := tx.QueryRowContext(ctx, query).Scan(&id); err != nil {
			return err
		}
		var found bool
		var err error
		record, found, err = loadCanonicalFixtureRecordTx(ctx, tx, postgres, id)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("selected runtime log %s is absent from its original read snapshot", id)
		}
		return nil
	})
	if err != nil {
		return empty, err
	}
	admitted, err := record.Decode()
	if err != nil {
		return empty, err
	}
	return admitted.Event(), nil
}

func readFixtureEventLineageStorage(ctx context.Context, selected any, tx *sql.Tx, eventID string) (string, string, error) {
	switch selected.(type) {
	case *PostgresStore:
		return eventrecordpostgres.ReadFixtureLineageStorage(ctx, tx, eventID)
	case *SQLiteRuntimeStore:
		return eventrecordsqlite.ReadFixtureLineageStorage(ctx, tx, eventID)
	default:
		return "", "", fmt.Errorf("event lineage observation requires the original selected read owner")
	}
}

func ReadCanonicalEventRecordForTest(ctx context.Context, selected any, eventID string) (events.Event, bool, error) {
	var empty events.Event
	if err := validateChannelObservationOwner(selected); err != nil {
		return empty, false, err
	}
	var record eventrecord.Record
	var found bool
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		_, postgres := selected.(*PostgresStore)
		record, found, err = loadCanonicalFixtureRecordTx(ctx, tx, postgres, eventID)
		return err
	})
	if err != nil || !found {
		return empty, false, err
	}
	admitted, err := record.Decode()
	if err != nil {
		return empty, false, fmt.Errorf("decode canonical event record %s: %w", eventID, err)
	}
	return admitted.Event(), true, nil
}

func insertCanonicalFixtureRecordForTest(ctx context.Context, attempt *mutationprotocol.Attempt, dialect authoractivityfixture.Dialect, record eventrecord.Record) (bool, error) {
	var inserted bool
	var err error
	if dialect == authoractivityfixture.DialectPostgres {
		inserted, err = eventrecordpostgres.Insert(ctx, attempt, record)
	} else {
		inserted, err = eventrecordsqlite.Insert(ctx, attempt, record)
	}
	if err != nil || inserted {
		return inserted, err
	}
	var existing eventrecord.Record
	var found bool
	err = attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		if dialect == authoractivityfixture.DialectPostgres {
			existing, found, err = eventrecordpostgres.Load(ctx, tx, record.EventID)
		} else {
			existing, found, err = eventrecordsqlite.Load(ctx, tx, record.EventID)
		}
		return err
	})
	if err != nil {
		return false, err
	}
	if !found || !record.Equal(existing) {
		return false, fmt.Errorf("canonical event fixture %s conflicts with its persisted record", record.EventID)
	}
	return false, nil
}

// CommitRevisionedSemanticEventFixtureForTest exercises the canonical event
// and delivery adapters, including their fork revision projection.
func CommitRevisionedSemanticEventFixtureForTest(ctx context.Context, selected any, admitted events.AdmittedEvent, settlement events.RouteSettlement, routes []events.DeliveryRoute, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition) (bool, error) {
	return commitSelectedSemanticEventFixtureForTest(ctx, selected, admitted, settlement, routes, scope, disposition, true, false)
}

// CommitSemanticForkFrontierForTest captures all selected-fork fact families
// in the same PostgreSQL fixture transaction.
func CommitSemanticForkFrontierForTest(ctx context.Context, selected any, admitted events.AdmittedEvent, settlement events.RouteSettlement, routes []events.DeliveryRoute, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition) (bool, error) {
	if store, ok := selected.(*PostgresStore); !ok || store == nil {
		return false, fmt.Errorf("semantic fork frontier fixture requires the original PostgreSQL owner, got %T", selected)
	}
	return commitSelectedSemanticEventFixtureForTest(ctx, selected, admitted, settlement, routes, scope, disposition, true, true)
}

// Event-only history must not gain a pipeline publication scope or delivery plan.
func InstallProspectivePublicationCreationFailureForTest(ctx context.Context, selected any) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	_, postgres := selected.(*PostgresStore)
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return eventrecord.InstallProspectivePublicationCreationFailureTx(ctx, tx, postgres)
	})
}

func RemoveProspectivePublicationCreationFailureForTest(ctx context.Context, selected any) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	_, postgres := selected.(*PostgresStore)
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return eventrecord.RemoveProspectivePublicationCreationFailureTx(ctx, tx, postgres)
	})
}

func InsertNativeExistingRunRootEventForTest(ctx context.Context, selected any, eventID, runID string, eventType events.EventType, producer events.ProducerIdentity, payload []byte, envelope events.EventEnvelope, at time.Time) (events.Event, error) {
	var empty events.Event
	dialect, err := eventFixtureDialectForTest(selected)
	if err != nil {
		return empty, err
	}
	var event events.Event
	err = runEventFixtureMutationForTest(ctx, selected, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		var err error
		event, err = eventfixture.ExistingRunRoot(ctx, attempt, dialect, eventID, runID, eventType, producer, payload, envelope, at)
		return err
	})
	if err != nil {
		return empty, err
	}
	return event, nil
}

func InsertNativeCanonicalEventRecordForTest(ctx context.Context, selected any, admitted events.AdmittedEvent, settlement events.RouteSettlement) (bool, error) {
	dialect, err := eventFixtureDialectForTest(selected)
	if err != nil {
		return false, err
	}
	if admitted.Class() == events.EventAdmissionSelectedForkReplay {
		return false, fmt.Errorf("selected-fork replay fixture requires exact lineage persistence")
	}
	record, err := eventrecord.FromAdmitted(admitted, settlement)
	if err != nil {
		return false, err
	}
	var inserted bool
	err = runEventFixtureMutationForTest(ctx, selected, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		var err error
		switch dialect {
		case authoractivityfixture.DialectPostgres:
			inserted, err = eventrecordpostgres.Insert(ctx, attempt, record)
		case authoractivityfixture.DialectSQLite:
			inserted, err = eventrecordsqlite.Insert(ctx, attempt, record)
		}
		if err != nil || inserted {
			return err
		}
		return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			var existing eventrecord.Record
			var found bool
			var err error
			switch dialect {
			case authoractivityfixture.DialectPostgres:
				existing, found, err = eventrecordpostgres.Load(ctx, tx, record.EventID)
			case authoractivityfixture.DialectSQLite:
				existing, found, err = eventrecordsqlite.Load(ctx, tx, record.EventID)
			}
			if err != nil {
				return err
			}
			if !found || !record.Equal(existing) {
				return fmt.Errorf("canonical event record fixture %s conflicts with its persisted record", record.EventID)
			}
			return nil
		})
	})
	if err != nil {
		return false, err
	}
	return inserted, nil
}

func CountPostgresForkTypedRuntimeDiagnosticForTest(ctx context.Context, selected any, run, event, owner string) (int, error) {
	if pg, ok := selected.(*PostgresStore); !ok || pg == nil || owner == "" {
		return 0, fmt.Errorf("typed fork diagnostics require the original PostgreSQL owner and lineage owner")
	}
	return readCLIForkCount(ctx, selected, []string{run, event}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return eventrecord.CountPostgresForkTypedRuntimeDiagnosticTx(ctx, tx, run, event, owner)
	})
}

func ReadPostgresGlobalRuntimeDiagnosticStorageForTest(ctx context.Context, selected any) (string, error) {
	if pg, ok := selected.(*PostgresStore); !ok || pg == nil {
		return "", fmt.Errorf("global runtime diagnostics require the original PostgreSQL owner")
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return "", err
	}
	var out string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadPostgresGlobalRuntimeDiagnosticStorageTx(ctx, tx)
		return err
	})
	if err != nil {
		return "", err
	}
	return out, nil
}

func eventFixtureDialectForTest(selected any) (authoractivityfixture.Dialect, error) {
	switch store := selected.(type) {
	case *PostgresStore:
		if store != nil && store.backend != nil && store.pipelinePostgresOwner != nil {
			if err := store.requireCurrentSchema(); err != nil {
				return "", err
			}
			return authoractivityfixture.DialectPostgres, nil
		}
	case *SQLiteRuntimeStore:
		if store != nil && store.backend != nil && store.pipelineSQLiteOwner != nil {
			if err := store.requireCurrentSchema(); err != nil {
				return "", err
			}
			return authoractivityfixture.DialectSQLite, nil
		}
	}
	return "", fmt.Errorf("event fixture selected store %T is not initialized", selected)
}

func runEventFixtureMutationForTest(ctx context.Context, selected any, write func(context.Context, *mutationprotocol.Attempt) error) error {
	apply := func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		return struct{}{}, write(txctx, attempt)
	}
	switch store := selected.(type) {
	case *PostgresStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("postgres event fixture store is required")
		}
		return mutationprotocol.RunPostgres(ctx, store.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, store.runLifecycleCandidates, apply).Err()
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("sqlite event fixture store is required")
		}
		return mutationprotocol.RunSQLite(ctx, store.backend, "canonical event fixture", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, store.runLifecycleCandidates, apply).Err()
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}

func runUnrevisionedEventFixtureTransactionForTest(ctx context.Context, selected any, write func(context.Context, *sql.Tx) error) error {
	switch store := selected.(type) {
	case *PostgresStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("postgres event fixture store is required")
		}
		return store.backend.RunTransaction(ctx, write)
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("sqlite event fixture store is required")
		}
		return store.backend.RunTransaction(ctx, "unrevisioned event fixture", write)
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}

func commitSelectedSemanticEventFixtureForTest(ctx context.Context, selectedStore any, admitted events.AdmittedEvent, settlement events.RouteSettlement, routes []events.DeliveryRoute, scope runtimepipelineobligation.CommittedScope, pipelineDisposition *runtimepipelineobligation.Disposition, revisioned, captureForkFrontier bool) (bool, error) {
	if admitted.Class() == events.EventAdmissionSelectedForkReplay {
		return false, fmt.Errorf("selected-fork replay events require their closed named persistence operation")
	}
	if err := settlement.Validate(routes); err != nil {
		return false, err
	}
	record, err := eventrecord.FromAdmitted(admitted, settlement)
	if err != nil {
		return false, err
	}
	dialect, err := eventFixtureDialectForTest(selectedStore)
	if err != nil {
		return false, err
	}
	postgres := dialect == authoractivityfixture.DialectPostgres
	deliveryAdapter, err := deliveryadapter.NewAdapter(deliveryadapter.Dialect(dialect))
	if err != nil {
		return false, err
	}
	if !revisioned {
		return commitUnrevisionedSemanticEventFixtureForTest(ctx, selectedStore, dialect, record, routes, scope, pipelineDisposition, deliveryAdapter)
	}
	inserted := false
	err = runEventFixtureMutationForTest(ctx, selectedStore, func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
		var err error
		inserted, err = insertCanonicalFixtureRecordForTest(txctx, attempt, dialect, record)
		if err != nil || !inserted {
			return err
		}
		var authority runtimedelivery.ExecutionAuthority
		if err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			var err error
			authority, err = persistedEventDeliveryFixtureAuthority(txctx, tx, record.RunID, !postgres)
			return err
		}); err != nil {
			return err
		}
		if _, err := deliveryAdapter.CommitInitial(txctx, attempt, record.EventID, record.RunID, events.NormalizeDeliveryRoutes(routes), authority); err != nil {
			return err
		}
		if err := commitRevisionedEventFixtureFactsForTest(txctx, selectedStore, attempt, record.EventID, scope, pipelineDisposition, time.Now().UTC()); err != nil {
			return err
		}
		if captureForkFrontier {
			if !postgres {
				return fmt.Errorf("semantic fork frontier fixture requires PostgreSQL, got %T", selectedStore)
			}
			for _, family := range []runforkrevision.Family{
				runforkrevision.FamilyEvents,
				runforkrevision.FamilyEventDeliveries,
				runforkrevision.FamilyCommittedReplayScopes,
				runforkrevision.FamilyEventReceipts,
			} {
				if err := attempt.AddWholeFamily(record.RunID, family); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return inserted, nil
}

func commitUnrevisionedSemanticEventFixtureForTest(
	ctx context.Context,
	selectedStore any,
	dialect authoractivityfixture.Dialect,
	record eventrecord.Record,
	routes []events.DeliveryRoute,
	scope runtimepipelineobligation.CommittedScope,
	pipelineDisposition *runtimepipelineobligation.Disposition,
	adapter *deliveryadapter.Adapter,
) (bool, error) {
	inserted := false
	var authority runtimedelivery.ExecutionAuthority
	err := runUnrevisionedEventFixtureTransactionForTest(ctx, selectedStore, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		if dialect == authoractivityfixture.DialectPostgres {
			inserted, err = eventrecordpostgres.InsertUnrevisionedFixtureRecord(ctx, tx, record)
		} else {
			inserted, err = eventrecordsqlite.InsertUnrevisionedFixtureRecord(ctx, tx, record)
		}
		if err != nil || !inserted {
			return err
		}
		authority, err = persistedEventDeliveryFixtureAuthority(ctx, tx, record.RunID, dialect == authoractivityfixture.DialectSQLite)
		if err != nil {
			return err
		}
		if err := events.ValidateDeliveryRoutes(routes); err != nil {
			return err
		}
		admitted, err := record.Decode()
		if err != nil {
			return err
		}
		if err := events.ValidateReceiverMaterializations(admitted.Event(), routes); err != nil {
			return err
		}
		for _, route := range events.NormalizeDeliveryRoutes(routes) {
			if err := insertUnrevisionedDeliveryFixtureForTest(ctx, tx, dialect, adapter, record.EventID, record.RunID, route, authority); err != nil {
				return err
			}
		}
		return commitUnrevisionedEventFixtureFactsForTest(ctx, selectedStore, tx, record.EventID, scope, pipelineDisposition, time.Now().UTC())
	})
	if err != nil {
		return false, err
	}
	if !inserted {
		return false, nil
	}
	reader, ok := selectedStore.(interface {
		Snapshot(context.Context, string) (runtimedelivery.Snapshot, error)
	})
	if !ok {
		return false, fmt.Errorf("unrevisioned semantic fixture store %T cannot read delivery snapshots", selectedStore)
	}
	for _, route := range events.NormalizeDeliveryRoutes(routes) {
		obligation, err := runtimedelivery.NewObligation(record.EventID, record.RunID, route, authority)
		if err != nil {
			return false, err
		}
		snapshot, err := reader.Snapshot(ctx, obligation.DeliveryID())
		if err != nil {
			return false, fmt.Errorf("read unrevisioned delivery fixture %s: %w", obligation.DeliveryID(), err)
		}
		wantRoute, err := json.Marshal(obligation.Route())
		if err != nil {
			return false, err
		}
		gotRoute, err := json.Marshal(snapshot.Route.Normalized())
		if err != nil {
			return false, err
		}
		if snapshot.DeliveryID != obligation.DeliveryID() || snapshot.EventID != record.EventID || snapshot.RunID != record.RunID ||
			events.EncodeDeliveryRouteIdentity(snapshot.RouteIdentity) != events.EncodeDeliveryRouteIdentity(obligation.RouteIdentity()) ||
			snapshot.SubscriberClass != obligation.SubscriberClass() || snapshot.SubscriberID != obligation.SubscriberID() ||
			snapshot.Status != runtimedelivery.StatusPending || snapshot.RetryCount != 0 || snapshot.MaxRetries != obligation.MaxRetries() ||
			snapshot.ClaimVersion != 0 || snapshot.NextEligibleAt.IsZero() || !snapshot.Authority.Equal(authority) ||
			!bytes.Equal(gotRoute, wantRoute) {
			return false, fmt.Errorf("unrevisioned delivery fixture %s differs from canonical route/authority projection", obligation.DeliveryID())
		}
	}
	return true, nil
}

func insertUnrevisionedDeliveryFixtureForTest(ctx context.Context, tx *sql.Tx, dialect authoractivityfixture.Dialect, adapter *deliveryadapter.Adapter, eventID, runID string, route events.DeliveryRoute, authority runtimedelivery.ExecutionAuthority) error {
	obligation, err := runtimedelivery.NewObligation(eventID, runID, route, authority)
	if err != nil {
		return err
	}
	_, err = adapter.InsertUnrevisionedFixtureObligationTx(ctx, tx, obligation)
	return err
}

func commitRevisionedEventFixtureFactsForTest(ctx context.Context, selected any, attempt *mutationprotocol.Attempt, eventID string, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition, at time.Time) error {
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.CommitRevisionedEventFixtureFactsTx(ctx, attempt, eventID, scope, disposition, at)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.CommitRevisionedEventFixtureFactsTx(ctx, attempt, eventID, scope, disposition, at)
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}

func commitUnrevisionedEventFixtureFactsForTest(ctx context.Context, selected any, tx *sql.Tx, eventID string, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition, at time.Time) error {
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.CommitUnrevisionedEventFixtureFactsTx(ctx, tx, eventID, scope, disposition, at)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.CommitUnrevisionedEventFixtureFactsTx(ctx, tx, eventID, scope, disposition, at)
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}

type GenericSchedulePublicationModes = eventrecord.GenericSchedulePublicationModes

func ReadGenericSchedulePublicationModesForTest(ctx context.Context, selected any, taskID string) (GenericSchedulePublicationModes, error) {
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return GenericSchedulePublicationModes{}, err
	}
	if strings.TrimSpace(taskID) == "" {
		return GenericSchedulePublicationModes{}, errors.New("generic schedule publication observation requires a task ID")
	}
	var out GenericSchedulePublicationModes
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadGenericSchedulePublicationModesTx(ctx, tx, taskID)
		return err
	})
	if err != nil {
		return GenericSchedulePublicationModes{}, err
	}
	return out, nil
}

func ReadGenericScheduleDistinctPublicationModesForTest(ctx context.Context, selected any, taskID string) (GenericSchedulePublicationModes, error) {
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return GenericSchedulePublicationModes{}, err
	}
	if strings.TrimSpace(taskID) == "" {
		return GenericSchedulePublicationModes{}, errors.New("generic schedule publication observation requires a task ID")
	}
	var out GenericSchedulePublicationModes
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadGenericScheduleDistinctPublicationModesTx(ctx, tx, taskID)
		return err
	})
	if err != nil {
		return GenericSchedulePublicationModes{}, err
	}
	return out, nil
}

func CountGenericSchedulePublicationsForTest(ctx context.Context, selected any, taskID string) (int, error) {
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return 0, err
	}
	if strings.TrimSpace(taskID) == "" {
		return 0, errors.New("generic schedule publication observation requires a task ID")
	}
	var out int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.CountGenericSchedulePublicationsTx(ctx, tx, taskID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

type NumericRegistrationStorage = eventrecord.NumericRegistrationStorage

func ReadNumericRegistrationStorageForTest(ctx context.Context, selected any, runID string) ([]NumericRegistrationStorage, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return nil, err
	}
	var out []NumericRegistrationStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadNumericRegistrationStorageTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Read the physical identity at the caller's existing run/name cut. An
// ambiguous fixture is a refusal, never permission to choose an arbitrary row.
func ReadRunNamedEventIdentityStorageForTest(ctx context.Context, selected any, runID, eventName string) (string, error) {
	if err := validateRunNamedEventObservation(selected, runID, eventName); err != nil {
		return "", err
	}
	var eventID string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		ids, err := eventrecord.ReadRunNamedEventIdentityCandidatesTx(ctx, tx, runID, eventName)
		if err != nil {
			return err
		}
		switch len(ids) {
		case 0:
			return sql.ErrNoRows
		case 1:
			eventID = ids[0]
			return nil
		default:
			return fmt.Errorf("ambiguous physical event identity for run %s and event %s", runID, eventName)
		}
	})
	if err != nil {
		return "", err
	}
	return eventID, nil
}

func ReadRunNamedEventIdentitiesStorageForTest(ctx context.Context, selected any, runID, eventName string) ([]string, error) {
	if err := validateRunNamedEventObservation(selected, runID, eventName); err != nil {
		return nil, err
	}
	var ids []string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		ids, err = eventrecord.ReadRunNamedEventIdentityHistoryTx(ctx, tx, runID, eventName)
		return err
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

type DeploymentRuntimeLogStorage = eventrecord.DeploymentRuntimeLogStorage
type DeploymentRecentEventStorage = eventrecord.DeploymentRecentEventStorage

func ReadFirstRootReadyEventStorageForTest(ctx context.Context, selected any, runID string) (string, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return "", err
	}
	var out string
	var found bool
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadFirstRootReadyEventStorageTx(ctx, tx, runID)
		if err == sql.ErrNoRows {
			return nil
		}
		found = err == nil
		return err
	})
	if err != nil {
		return "", err
	}
	// Return exact absence only after the read has settled successfully. Joined
	// cancellation or cleanup errors must not be mistaken for an empty poll.
	if !found {
		return "", sql.ErrNoRows
	}
	return out, nil
}

func ReadDeploymentRuntimeLogStorageForTest(ctx context.Context, selected any, runID string) ([]DeploymentRuntimeLogStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, eventrecord.ReadDeploymentRuntimeLogStorageTx)
}

type ClosedReceiverEmissionStorage = eventrecord.ClosedReceiverEmissionStorage

func ReadClosedReceiverEmissionStorageForTest(ctx context.Context, selected any, runID, eventName string) (ClosedReceiverEmissionStorage, error) {
	if eventName != "task.completed" && eventName != "task.finalized" {
		return ClosedReceiverEmissionStorage{}, fmt.Errorf("closed receiver emission observation requires a chain event")
	}
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (ClosedReceiverEmissionStorage, error) {
		return eventrecord.ReadClosedReceiverEmissionStorageTx(ctx, tx, postgres, runID, eventName)
	})
}

func ReadClosedReceiverRuntimeLogStorageForTest(ctx context.Context, selected any, runID string) ([]string, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) ([]string, error) {
		return eventrecord.ReadClosedReceiverRuntimeLogStorageTx(ctx, tx, postgres, runID)
	})
}

func ReadDeploymentRecentEventStorageForTest(ctx context.Context, selected any, runID string) ([]DeploymentRecentEventStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, eventrecord.ReadDeploymentRecentEventStorageTx)
}

func ReadRunLifecycleProjectionLogPayloadsForTest(ctx context.Context, selected any, runID string) ([][]byte, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return nil, err
	}
	var out [][]byte
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadRunLifecycleProjectionLogPayloadsTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func ReadLifecycleProjectionLogPayloadsForTest(ctx context.Context, selected any) ([][]byte, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	var out [][]byte
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadLifecycleProjectionLogPayloadsTx(ctx, tx)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func CountRunNamedEventStorageForTest(ctx context.Context, selected any, runID, eventName string) (int, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return 0, err
	}
	var out int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.CountRunNamedEventStorageTx(ctx, tx, runID, eventName)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func CountEventNameStorageForTest(ctx context.Context, selected any, eventName string) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = eventrecord.CountGlobalNamedEventStorageTx(ctx, tx, eventName)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func ReadLifecycleEventCardinalityForTest(ctx context.Context, selected any, runID, eventName string) (int, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return 0, err
	}
	if eventName == "" {
		return 0, fmt.Errorf("lifecycle event cardinality requires an exact event name")
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return 0, err
	}
	var count int
	read := func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = eventrecord.CountRunNamedEventStorageTx(ctx, tx, runID, eventName)
		return err
	}
	var err error
	switch owner := selected.(type) {
	case *PostgresStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	case *SQLiteRuntimeStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	}
	if err != nil {
		return 0, err
	}
	return count, nil
}

type PublicationEventProjection = eventrecord.PublicationEventProjection

type PublicationEventIdentityStorage = eventrecord.PublicationEventIdentityStorage

type OperatorReferenceStorage = eventrecord.OperatorReferenceStorage

type RootTemplatePublicationStorage = eventrecord.RootTemplatePublicationStorage

type RunStartStorage struct {
	Header  runlifecycle.RunStartHeaderStorage
	Event   eventrecord.PublicationEventProjection
	EventID string
}

func ReadPostgresRootTemplatePublicationForTest(ctx context.Context, selected any, eventID string) (RootTemplatePublicationStorage, error) {
	if err := validatePostgresPublicationIdentities(selected, eventID); err != nil {
		return RootTemplatePublicationStorage{}, err
	}
	var out RootTemplatePublicationStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadPostgresRootTemplatePublicationTx(ctx, tx, eventID)
		return err
	})
	if err != nil {
		return RootTemplatePublicationStorage{}, err
	}
	return out, nil
}

func ReadPostgresRunStartStorageForTest(ctx context.Context, selected any, runID, eventName string) (RunStartStorage, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return RunStartStorage{}, err
	}
	if _, ok := selected.(*PostgresStore); !ok {
		return RunStartStorage{}, fmt.Errorf("run.start physical projection requires selected PostgreSQL storage")
	}
	var out RunStartStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out.Header, err = runlifecycle.ReadRunStartHeaderStorageTx(ctx, tx, runID)
		if err != nil {
			return err
		}
		out.Event, err = eventrecord.ReadRunStartEventProjectionTx(ctx, tx, runID, eventName)
		if err != nil {
			return err
		}
		out.EventID, err = eventrecord.ReadRunStartEventIdentityTx(ctx, tx, runID, eventName)
		return err
	})
	if err != nil {
		return RunStartStorage{}, err
	}
	return out, nil
}

func ReadPublicationEventProjectionForTest(ctx context.Context, selected any, eventID string) (PublicationEventProjection, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return PublicationEventProjection{}, err
	}
	_, postgres := selected.(*PostgresStore)
	var out PublicationEventProjection
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadPublicationEventProjectionTx(ctx, tx, postgres, eventID)
		return err
	})
	if err != nil {
		return PublicationEventProjection{}, err
	}
	return out, nil
}

func ReadPublicationEventIdentityStorageForTest(ctx context.Context, selected any, eventID string) (PublicationEventIdentityStorage, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return PublicationEventIdentityStorage{}, err
	}
	var out PublicationEventIdentityStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadPublicationEventIdentityStorageTx(ctx, tx, eventID)
		return err
	})
	if err != nil {
		return PublicationEventIdentityStorage{}, err
	}
	return out, nil
}

func ReadPhysicalPublicationEntityIdentityStorageForTest(ctx context.Context, selected any, eventID string) (string, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{eventID}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return eventrecord.ReadPhysicalPublicationEntityIdentityStorageTx(ctx, tx, eventID)
	})
}

func ReadServedEventRunIDStorageForTest(ctx context.Context, selected any, eventID string) (string, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return "", err
	}
	var runID string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		runID, err = eventrecord.ReadEventRunIDStorageTx(ctx, tx, eventID)
		return err
	})
	if err != nil {
		return "", err
	}
	return runID, nil
}

func ReadOperatorReferenceStorageForTest(ctx context.Context, selected any, eventID string) (OperatorReferenceStorage, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return OperatorReferenceStorage{}, err
	}
	var out OperatorReferenceStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadOperatorReferenceStorageTx(ctx, tx, eventID)
		return err
	})
	if err != nil {
		return OperatorReferenceStorage{}, err
	}
	return out, nil
}

func CorruptReplayEventTargetRouteForTest(ctx context.Context, selected any, eventID string) error {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return err
	}
	_, postgres := selected.(*PostgresStore)
	return runEventFixtureMutationForTest(ctx, selected, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return eventrecord.CorruptReplayEventTargetRouteTx(ctx, tx, postgres, eventID)
		})
	})
}

func RestoreReplayEventTargetRouteForTest(ctx context.Context, selected any, original events.Event) error {
	if err := validateStandaloneStorageEvent(selected, original.ID()); err != nil {
		return err
	}
	_, postgres := selected.(*PostgresStore)
	return runEventFixtureMutationForTest(ctx, selected, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return eventrecord.RestoreReplayEventTargetRouteTx(ctx, tx, postgres, original)
		})
	})
}

func ReadChannelBudgetBudgetEventsDiagnosticsStorageForTest(ctx context.Context, selected any) ([]map[string]string, error) {
	var out []map[string]string
	if err := validateChannelObservationOwner(selected); err != nil {
		return out, err
	}
	missing := false
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadChannelBudgetBudgetEventsDiagnosticsStorageTx(ctx, tx)
		if err == sql.ErrNoRows {
			missing = true
			return nil
		}
		return err
	})
	if err == nil && missing {
		err = sql.ErrNoRows
	}
	return out, err
}
