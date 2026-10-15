package runtimepersistence

import (
	"context"
	"database/sql"
	"strconv"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
)

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
		count, err := delivery.CountSelectedForkDeliveryStorageTx(ctx, tx, runID)
		result["deliveries"] = strconv.Itoa(count)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func ReadSelectedForkGlobalCardinalityForTest(ctx context.Context, selected any) (map[string]int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	result := make(map[string]int, 4)
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		for _, table := range []string{"agents", "flow_instances", "flow_instance_runtime_readiness", "timers"} {
			var count int
			if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
				return err
			}
			result[table] = count
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func ReadSelectedForkDescendantCardinalityForTest(ctx context.Context, selected any, sourceRunID string) (map[string]int, error) {
	if err := validateSelectedForkStorageIdentity(sourceRunID); err != nil {
		return nil, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	queries := map[string]string{
		"runs":               `SELECT COUNT(*) FROM runs WHERE forked_from_run_id=$1`,
		"bindings":           `SELECT COUNT(*) FROM run_fork_selected_contract_bindings WHERE source_run_id=$1`,
		"route recoveries":   `SELECT COUNT(*) FROM run_fork_selected_contract_route_recoveries WHERE source_run_id=$1`,
		"branch divergences": `SELECT COUNT(*) FROM run_fork_selected_contract_branch_divergences WHERE source_run_id=$1`,
		"execution lineage":  `SELECT COUNT(*) FROM run_fork_selected_contract_executions WHERE source_run_id=$1`,
		"events":             `SELECT COUNT(*) FROM events WHERE run_id IN (SELECT run_id FROM runs WHERE forked_from_run_id=$1)`,
		"timers":             `SELECT COUNT(*) FROM timers WHERE forked_from_run_id=$1`,
	}
	result := make(map[string]int, len(queries)+1)
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		for name, query := range queries {
			var count int
			if err := tx.QueryRowContext(ctx, query, sourceRunID).Scan(&count); err != nil {
				return err
			}
			result[name] = count
		}
		runtimeExecutions, err := runforkpersistence.CountSelectedSourceRuntimeExecutionsStorageTx(ctx, tx, sourceRunID)
		if err != nil {
			return err
		}
		result["runtime executions"] = runtimeExecutions
		count, err := delivery.CountSelectedForkDescendantDeliveryStorageTx(ctx, tx, sourceRunID)
		result["deliveries"] = count
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
