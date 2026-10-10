package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	storeentity "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func runForkTransferredJoinEvidence(runID string, sources []genericschedule.TransferredJoinOccurrence) ([]runForkArrivalScheduleRecord, error) {
	seen := map[string]bool{}
	rows := make([]runForkArrivalScheduleRecord, 0, len(sources))
	for _, source := range sources {
		if err := source.Validate(); err != nil {
			return nil, err
		}
		id := source.Publication.EventID
		if source.Command.RunID != runID || seen[id] {
			return nil, fmt.Errorf("transferred join inventory repeats or changes its exact owner")
		}
		seen[id] = true
		digest, err := source.EvidenceDigest()
		if err != nil {
			return nil, err
		}
		rows = append(rows, runForkArrivalScheduleRecord{id, digest})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	return rows, nil
}

// Read immutable transfer frames from the same canonical accumulator used by
// execution and fixed-cut history. Lifecycle closure may change, lineage may not.
func readRunForkTransferredJoinInventory(ctx context.Context, attempt *mutationprotocol.Attempt, plan runfork.RunForkPlan, childRunID string) ([]genericschedule.TransferredJoinOccurrence, error) {
	var expected []genericschedule.TransferredJoinOccurrence
	for _, entity := range plan.Entities {
		if len(entity.PublishedArrivals)+len(entity.TransferredJoins) == 0 {
			continue
		}
		projection, err := projectRunForkEntityOwnership(plan.SourceRunID, childRunID, entity.EntityID, entity.MaterializationMetadata.FlowInstance)
		if err != nil {
			return nil, err
		}
		_, raw, _, err := projectRunForkEntityExecutionState(entity, plan.SourceRunID, childRunID, projection)
		if err != nil {
			return nil, err
		}
		rows, err := runForkTransferredJoinAccumulator(raw)
		if err != nil {
			return nil, err
		}
		expected = append(expected, rows...)
	}
	if len(expected) == 0 {
		return nil, nil
	}
	var actual []genericschedule.TransferredJoinOccurrence
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT accumulator FROM flow_instances WHERE run_id=$1`, childRunID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var raw any
			if err := rows.Scan(&raw); err != nil {
				return err
			}
			accumulator, err := storeentity.DecodeJSONMap(raw)
			if err != nil {
				return err
			}
			transfers, err := runForkTransferredJoinAccumulator(accumulator)
			if err != nil {
				return err
			}
			actual = append(actual, transfers...)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	want, err := runForkTransferredJoinEvidence(childRunID, expected)
	if err != nil {
		return nil, err
	}
	got, err := runForkTransferredJoinEvidence(childRunID, actual)
	if err != nil || len(want) != len(got) {
		return nil, fmt.Errorf("inherited transferred join inventory differs from its exact projection: %w", err)
	}
	for index := range want {
		if want[index] != got[index] {
			return nil, fmt.Errorf("inherited transferred join changed its retained publication evidence")
		}
	}
	return actual, nil
}

func runForkTransferredJoinAccumulator(raw map[string]any) ([]genericschedule.TransferredJoinOccurrence, error) {
	buckets, err := joinruntime.PersistedBuckets(raw)
	if err != nil {
		return nil, err
	}
	joins, err := joinruntime.List(buckets)
	if err != nil {
		return nil, err
	}
	var sources []genericschedule.TransferredJoinOccurrence
	for _, join := range joins {
		if join.TransferredPublication == nil {
			continue
		}
		source, err := genericschedule.NewTransferredJoinOccurrence(join, *join.TransferredPublication)
		if err != nil {
			return nil, err
		}
		sources = append(sources, source)
	}
	return sources, nil
}
