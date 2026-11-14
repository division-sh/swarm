package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/manager"
)

// ForecastStartupRecovery returns only a report. No projected generation,
// recovery row or authority can leave this entry or enter mutation selection.
func (s *EffectPostgresOwner) ForecastStartupRecovery(ctx context.Context, now time.Time) (result effects.StartupRecoveryCounts, err error) {
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		result, err = forecastStartupRecovery(ctx, tx, true, s.delivery, now)
		return err
	})
	return result, err
}

func (s *EffectSQLiteOwner) ForecastStartupRecovery(ctx context.Context, now time.Time) (result effects.StartupRecoveryCounts, err error) {
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		result, err = forecastStartupRecovery(ctx, tx, false, s.delivery, now)
		return err
	})
	return result, err
}

func forecastStartupRecovery(ctx context.Context, tx *sql.Tx, postgres bool, delivery providerDrainDeliveryOwner, now time.Time) (effects.StartupRecoveryCounts, error) {
	if now.IsZero() || delivery == nil {
		return effects.StartupRecoveryCounts{}, fmt.Errorf("startup recovery forecast requires its time and delivery reader")
	}
	result, candidates, err := readStartupRecoveryReportBase(ctx, tx, postgres, true)
	if err != nil {
		return result, err
	}
	generations, err := readForecastTakeoverGenerations(ctx, tx)
	if err != nil {
		return effects.StartupRecoveryCounts{}, err
	}
	var rows []completionRecoveryAttempt
	if postgres {
		rows, err = readCompletionRecoveryAttemptsPostgres(ctx, tx, "", nil, recoveryInspection)
	} else {
		rows, err = readCompletionRecoveryAttemptsSQLite(ctx, tx, "", nil, recoveryInspection)
	}
	if err != nil {
		return effects.StartupRecoveryCounts{}, err
	}
	allowed := externalEffectRecoveryAttemptSet(candidates)
	observations := newCompletionRecoveryObservations()
	for _, row := range rows {
		observation, err := observations.readAuthority(ctx, tx, row, now, !postgres)
		if err != nil {
			return effects.StartupRecoveryCounts{}, err
		}
		generation := observation.actor.State.Generation
		if projected, found := generations[agentidentity.StorageFields{
			RunID: row.AgentRunID, AgentID: row.AgentID, NameOwner: row.AgentNameOwner, NameSource: row.AgentNameSource,
			RoutePresence: row.AgentRoutePresence, FlowScopeKey: row.FlowScopeKey,
			FlowInstanceID: row.FlowInstanceID, FlowInstancePath: row.FlowInstance,
		}]; found {
			generation = projected
		}
		wouldRecover, err := observations.recoveryDecision(ctx, tx, delivery, observation, generation, now, !postgres)
		if err != nil {
			return effects.StartupRecoveryCounts{}, err
		}
		if _, admitted := allowed[row.AttemptID]; !wouldRecover || !admitted {
			continue
		}
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

// Keep the previous report's population and overflow refusal. The map is local
// to the forecast; actual selection reads only native state in its transaction.
func readForecastTakeoverGenerations(ctx context.Context, tx *sql.Tx) (map[agentidentity.StorageFields]uint64, error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(run_id AS TEXT), agent_id, agent_name_owner, agent_name_source,
		agent_route_presence, flow_scope_key, flow_instance_id, flow_instance, lifecycle_generation FROM agents`)
	if err != nil {
		return nil, fmt.Errorf("read forecast agent generations: %w", err)
	}
	defer rows.Close()
	result := make(map[agentidentity.StorageFields]uint64)
	for rows.Next() {
		var fields agentidentity.StorageFields
		var generation int64
		if err := rows.Scan(&fields.RunID, &fields.AgentID, &fields.NameOwner, &fields.NameSource,
			&fields.RoutePresence, &fields.FlowScopeKey, &fields.FlowInstanceID, &fields.FlowInstancePath, &generation); err != nil {
			return nil, err
		}
		if generation < 0 || generation == math.MaxInt64 {
			return nil, fmt.Errorf("invalid retained agent lifecycle generation %d", generation)
		}
		result[fields] = manager.NextLifecycleGeneration(uint64(generation))
	}
	return result, rows.Err()
}
