package generationauthority

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
)

func ReadSourceSetTx(ctx context.Context, tx *sql.Tx, sqlite, lock bool) (agenttopology.SourceSetPlan, bool, error) {
	return readSourceSet(ctx, tx, sqlite, lock)
}

func ReadSourceSetQuery(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, sqlite bool) (agenttopology.SourceSetPlan, bool, error) {
	return readSourceSet(ctx, q, sqlite, false)
}

func readSourceSet(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, sqlite, lock bool) (agenttopology.SourceSetPlan, bool, error) {
	query := `SELECT revision,plan FROM agent_topology_source_set_head WHERE singleton_id = 1`
	if !sqlite && lock {
		query += ` FOR SHARE`
	}
	var plan agenttopology.SourceSetPlan
	var raw []byte
	if err := q.QueryRowContext(ctx, query).Scan(&plan.Revision, &raw); err != nil {
		if err == sql.ErrNoRows {
			return agenttopology.SourceSetPlan{}, false, nil
		}
		return agenttopology.SourceSetPlan{}, false, err
	}
	if err := json.Unmarshal(raw, &plan); err != nil {
		return agenttopology.SourceSetPlan{}, false, err
	}
	if err := plan.Validate(); err != nil {
		return agenttopology.SourceSetPlan{}, false, err
	}
	return plan, true, nil
}

func SourceSetContains(plan agenttopology.SourceSetPlan, source agenttopology.SourceCoordinate) bool {
	for _, candidate := range plan.Sources {
		if candidate.Normalize() == source {
			return true
		}
	}
	return false
}

func SourceSetContainsBundle(plan agenttopology.SourceSetPlan, hash string) bool {
	for _, candidate := range plan.Sources {
		if candidate.BundleHash == hash {
			return true
		}
	}
	return false
}
