package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	storeentity "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
)

func (s *PipelinePostgresOwner) RequireWorkflowJoinAdmissionTx(ctx context.Context, tx *sql.Tx, fences []pipeline.WorkflowJoinAdmissionFence) error {
	return requireWorkflowJoinAdmissionTx(ctx, tx, fences, true)
}

func (s *PipelineSQLiteOwner) RequireWorkflowJoinAdmissionTx(ctx context.Context, tx *sql.Tx, fences []pipeline.WorkflowJoinAdmissionFence) error {
	return requireWorkflowJoinAdmissionTx(ctx, tx, fences, false)
}

func requireWorkflowJoinAdmissionTx(ctx context.Context, tx *sql.Tx, fences []pipeline.WorkflowJoinAdmissionFence, postgres bool) error {
	ordered := append([]pipeline.WorkflowJoinAdmissionFence(nil), fences...)
	sort.Slice(ordered, func(i, j int) bool {
		left, right := ordered[i], ordered[j]
		return left.Owner.RunID+":"+left.Owner.Route.InstancePath+":"+left.EntityID < right.Owner.RunID+":"+right.Owner.Route.InstancePath+":"+right.EntityID
	})
	for _, fence := range ordered {
		if err := fence.Validate(); err != nil {
			return err
		}
		query := `SELECT current_state, bookkeeping, accumulator FROM entity_state WHERE run_id = ? AND entity_id = ? AND flow_instance = ?`
		if postgres {
			// Share the constructor's lock so absence cannot race an initial arm.
			lock := fmt.Sprintf("%d:%s%s", len(fence.Owner.RunID), fence.Owner.RunID, fence.Owner.Route.InstancePath)
			if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lock); err != nil {
				return err
			}
			query = `SELECT current_state, bookkeeping, accumulator FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid AND flow_instance = $3 FOR UPDATE`
		}
		var stage string
		var bookkeepingRaw, stateBucketsRaw any
		err := tx.QueryRowContext(ctx, query, fence.Owner.RunID, fence.EntityID, fence.Owner.Route.InstancePath).Scan(&stage, &bookkeepingRaw, &stateBucketsRaw)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		matches := err == sql.ErrNoRows && fence.Entry.Empty()
		if err == nil {
			bookkeeping, err := storeentity.DecodeJSONMap(bookkeepingRaw)
			if err != nil {
				return fmt.Errorf("decode join admission lifecycle: %w", err)
			}
			stateBuckets, err := storeentity.DecodeJSONMap(stateBucketsRaw)
			if err != nil {
				return fmt.Errorf("decode join admission arms: %w", err)
			}
			matches, err = fence.MatchesCurrent(stage, bookkeeping, stateBuckets)
			if err != nil {
				return err
			}
		}
		if !matches {
			return failures.New(failures.ClassLifecycleConflict, "join_publication_entry_changed", "join-admission", "commit_publication", map[string]any{
				"flow_instance": fence.Owner.Route.InstancePath, "expected_stage_entry": fence.Entry,
			})
		}
	}
	return nil
}
