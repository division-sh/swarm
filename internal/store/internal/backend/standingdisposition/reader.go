package standingdisposition

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

type queryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// ReadByRun is the selected-store fact reader for the total exact-current
// disposition. Both mutation/recovery owners and diagnostics consume it.
func ReadByRun(ctx context.Context, q queryRower, postgres bool, runID string) (runtimepipeline.StandingRestartDisposition, error) {
	runID = strings.TrimSpace(runID)
	if q == nil {
		return runtimepipeline.StandingRestartDisposition{}, errors.New("standing restart selected-store reader is required")
	}
	if runID == "" {
		return runtimepipeline.StandingRestartDisposition{}, errors.New("standing restart run_id is required")
	}
	if _, err := uuid.Parse(runID); err != nil {
		return runtimepipeline.StandingRestartDisposition{}, fmt.Errorf("standing restart run_id: %w", err)
	}
	query := `
		SELECT ss.service_id, ss.package_key, ss.flow_id, ss.instance_id, ss.entity_id,
		       ss.current_run_id, ss.current_generation,
		       ss.declaration_present, ss.effective_state, ss.operator_override,
		       COALESCE(r.status, ''), COALESCE(r.origin_kind, ''),
		       COALESCE(r.origin_service_id, ''), COALESCE(r.origin_generation, 0),
		       (SELECT COUNT(*) FROM standing_services owners WHERE owners.current_run_id = ?),
		       (SELECT COUNT(*) FROM standing_service_generations generations
		        WHERE generations.service_id = ss.service_id
		          AND generations.generation = ss.current_generation
		          AND generations.run_id = ss.current_run_id
		          AND generations.retired_at IS NULL)
		FROM standing_services ss
		LEFT JOIN runs r ON r.run_id = ss.current_run_id
		WHERE ss.current_run_id = ?
	`
	args := []any{runID, runID}
	if postgres {
		query = `
			SELECT ss.service_id::text, ss.package_key, ss.flow_id, ss.instance_id, ss.entity_id::text,
			       ss.current_run_id::text, ss.current_generation,
			       ss.declaration_present, ss.effective_state, ss.operator_override,
			       COALESCE(r.status, ''), COALESCE(r.origin_kind, ''),
			       COALESCE(r.origin_service_id::text, ''), COALESCE(r.origin_generation, 0),
			       (SELECT COUNT(*) FROM standing_services owners WHERE owners.current_run_id = $1::uuid),
			       (SELECT COUNT(*) FROM standing_service_generations generations
			        WHERE generations.service_id = ss.service_id
			          AND generations.generation = ss.current_generation
			          AND generations.run_id = ss.current_run_id
			          AND generations.retired_at IS NULL)
			FROM standing_services ss
			LEFT JOIN runs r ON r.run_id = ss.current_run_id
			WHERE ss.current_run_id = $1::uuid
		`
		args = []any{runID}
	}
	var fact runtimepipeline.StandingRestartFact
	var packageKey, flowID, instanceID, entityID, originKind, originServiceID string
	var originGeneration int64
	var owners, generationRelations int
	err := q.QueryRowContext(ctx, query, args...).Scan(
		&fact.ServiceID, &packageKey, &flowID, &instanceID, &entityID,
		&fact.RunID, &fact.Generation, &fact.DeclarationPresent,
		&fact.EffectiveState, &fact.OperatorOverride, &fact.RunState,
		&originKind, &originServiceID, &originGeneration, &owners, &generationRelations,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimepipeline.ClassifyStandingRestart(runtimepipeline.StandingRestartFact{})
	}
	if err != nil {
		return runtimepipeline.StandingRestartDisposition{}, fmt.Errorf("read standing restart disposition for run %s: %w", runID, err)
	}
	if owners != 1 {
		return runtimepipeline.StandingRestartDisposition{}, fmt.Errorf("standing restart run %s has %d exact current owners", runID, owners)
	}
	if strings.TrimSpace(packageKey) == "" || strings.TrimSpace(flowID) == "" || strings.TrimSpace(instanceID) == "" {
		return runtimepipeline.StandingRestartDisposition{}, fmt.Errorf("standing restart run %s has incomplete service identity", runID)
	}
	wantServiceID := runtimeflowidentity.StandingServiceID(packageKey, flowID)
	if fact.ServiceID != wantServiceID {
		return runtimepipeline.StandingRestartDisposition{}, fmt.Errorf(
			"standing restart run %s service identity %s does not match package/flow owner %s",
			runID,
			fact.ServiceID,
			wantServiceID,
		)
	}
	if _, err := uuid.Parse(strings.TrimSpace(entityID)); err != nil {
		return runtimepipeline.StandingRestartDisposition{}, fmt.Errorf("standing restart run %s entity identity: %w", runID, err)
	}
	if generationRelations != 1 {
		return runtimepipeline.StandingRestartDisposition{}, fmt.Errorf(
			"standing restart run %s has %d exact active generation relations",
			runID,
			generationRelations,
		)
	}
	if strings.TrimSpace(fact.RunState) == "" {
		return runtimepipeline.StandingRestartDisposition{}, fmt.Errorf("standing restart run %s current pointer has no referenced run", runID)
	}
	if originKind != "standing_generation" || originServiceID != fact.ServiceID || originGeneration != fact.Generation {
		return runtimepipeline.StandingRestartDisposition{}, fmt.Errorf(
			"standing restart run %s origin identity mismatch: kind=%s service_id=%s generation=%d",
			runID,
			originKind,
			originServiceID,
			originGeneration,
		)
	}
	fact.ExactCurrent = true
	return runtimepipeline.ClassifyStandingRestart(fact)
}
