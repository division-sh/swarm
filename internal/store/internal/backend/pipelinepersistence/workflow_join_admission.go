package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	storeentity "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
	"github.com/division-sh/swarm/internal/store/internal/workflowheader"
)

func (s *PipelinePostgresOwner) RequireWorkflowJoinAdmissionTx(ctx context.Context, tx *sql.Tx, fences []pipeline.WorkflowJoinAdmissionFence) error {
	return requireWorkflowJoinAdmissionTx(ctx, tx, fences, true)
}

func (s *PipelineSQLiteOwner) RequireWorkflowJoinAdmissionTx(ctx context.Context, tx *sql.Tx, fences []pipeline.WorkflowJoinAdmissionFence) error {
	return requireWorkflowJoinAdmissionTx(ctx, tx, fences, false)
}

func requireWorkflowJoinAdmissionTx(ctx context.Context, tx *sql.Tx, fences []pipeline.WorkflowJoinAdmissionFence, postgres bool) error {
	owners := make([]flowidentity.RunScopedFlowInstance, 0, len(fences))
	for _, fence := range fences {
		owners = append(owners, fence.Owner)
	}
	if err := lockWorkflowInstancesTx(ctx, tx, postgres, owners); err != nil {
		return err
	}
	ordered := append([]pipeline.WorkflowJoinAdmissionFence(nil), fences...)
	sort.Slice(ordered, func(i, j int) bool {
		left, right := ordered[i], ordered[j]
		return left.Owner.RunID+":"+left.Owner.Route.InstancePath+":"+left.EntityID < right.Owner.RunID+":"+right.Owner.Route.InstancePath+":"+right.EntityID
	})
	for _, fence := range ordered {
		if err := fence.Validate(); err != nil {
			return err
		}
		header, found, err := workflowheader.LoadForMutation(ctx, tx, postgres, fence.Owner.RunID, fence.EntityID, fence.Owner.Route.InstancePath)
		if err != nil {
			return err
		}
		matches := !found && fence.Entry.Empty()
		if found {
			if header.FlowTemplate != fence.Owner.Route.ScopeKey {
				return fmt.Errorf("join admission disagrees with its constructed flow owner")
			}
			bookkeeping, err := storeentity.DecodeJSONMap(header.Bookkeeping)
			if err != nil {
				return fmt.Errorf("decode join admission lifecycle: %w", err)
			}
			stateBuckets, err := storeentity.DecodeJSONMap(header.Accumulator)
			if err != nil {
				return fmt.Errorf("decode join admission arms: %w", err)
			}
			matches, err = fence.MatchesCurrent(header.Stage, bookkeeping, stateBuckets)
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
