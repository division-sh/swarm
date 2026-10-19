package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/store/testutil/runforkrevisionfixture"
	"github.com/google/uuid"
)

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

func ReadRunStatusStorageForTest(ctx context.Context, selected any, runID string) (string, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return "", err
	}
	var out string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = runlifecycle.ReadRunStatusStorageTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return "", err
	}
	return out, nil
}

// Fixed physical witnesses retain the predecessor/fork/global scopes of the
// refusal proofs. Delivery SQL belongs to the closed delivery projection owner.
func ReadSelectedForkExecutableCardinalityForTest(ctx context.Context, selected any, runID string) (map[string]string, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return nil, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	queries := map[string]string{
		"events":             `SELECT COUNT(*) FROM events WHERE run_id=$1`,
		"entity_state":       `SELECT COUNT(*) FROM entity_state WHERE run_id=$1`,
		"timers":             `SELECT COUNT(*) FROM timers WHERE run_id=$1`,
		"readiness":          `SELECT COUNT(*) FROM flow_instance_runtime_readiness WHERE run_id=$1`,
		"agents":             `SELECT COUNT(*) FROM agents WHERE CAST($1 AS TEXT) IS NOT NULL`,
		"flow_instances":     `SELECT COUNT(*) FROM flow_instances WHERE CAST($1 AS TEXT) IS NOT NULL`,
		"execution_lineage":  `SELECT COUNT(*) FROM run_fork_selected_contract_executions WHERE fork_run_id=$1`,
		"runtime_executions": `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1`,
	}
	result := make(map[string]string, len(queries)+2)
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var status string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id=$1`, runID).Scan(&status); err != nil {
			return err
		}
		result["run_status"] = status
		for name, query := range queries {
			var count int
			if err := tx.QueryRowContext(ctx, query, runID).Scan(&count); err != nil {
				return err
			}
			result[name] = strconv.Itoa(count)
		}
		// platform_tables.retired_instance_routing forbids the old membership
		// mirror. Keep its global physical refusal witness at the canonical
		// persisted delivery facts, independently of this run's deliveries.
		routes, err := delivery.FixtureDeliveryCardinalityTx(ctx, tx)
		if err != nil {
			return err
		}
		result["persisted_delivery_routes"] = strconv.Itoa(routes)
		count, err := delivery.CountSelectedForkDeliveryStorageTx(ctx, tx, runID)
		result["deliveries"] = strconv.Itoa(count)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// The receipt is the exact finalizer result from the acknowledged transaction,
// not a subsequent head query that could observe another writer's revision.
func CaptureSelectedRunForkRevisionForTest(ctx context.Context, selected any, runID string, families ...runforkrevision.Family) (int64, error) {
	runID = strings.TrimSpace(runID)
	if _, err := uuid.Parse(runID); err != nil {
		return 0, fmt.Errorf("run-fork snapshot fixture requires a run identity: %w", err)
	}
	dialect, err := eventFixtureDialectForTest(selected)
	if err != nil {
		return 0, err
	}
	if len(families) == 0 {
		families = runforkrevision.AllFamilies()
	}
	var revision int64
	err = runEventFixtureMutationForTest(ctx, selected, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			var err error
			if dialect == authoractivityfixture.DialectPostgres {
				revision, err = runforkrevisionfixture.Capture(ctx, tx, runID, families...)
			} else {
				revision, err = runforkrevisionfixture.CaptureSQLite(ctx, tx, runID, families...)
			}
			return err
		})
	})
	if err != nil {
		return 0, err
	}
	return revision, nil
}
