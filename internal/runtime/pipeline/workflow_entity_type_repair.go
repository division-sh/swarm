package pipeline

import (
	"context"
	"fmt"
	"strings"

	"swarm/internal/runtime/entityruntime"
)

type contractEntityTypeRepair struct {
	RunID        string
	EntityID     string
	FlowInstance string
	EntityType   string
}

func (pc *PipelineCoordinator) RepairContractEntityTypes(ctx context.Context) (int, error) {
	if pc == nil || pc.db == nil {
		return 0, nil
	}
	source := pc.SemanticSource()
	if source == nil {
		return 0, fmt.Errorf("workflow semantic source is required to repair contract entity types")
	}
	rows, err := pc.db.QueryContext(ctx, `
		SELECT
			run_id::text,
			entity_id::text,
			COALESCE(flow_instance, '')
		FROM entity_state
		WHERE COALESCE(NULLIF(BTRIM(entity_type), ''), 'default') = 'default'
		  AND COALESCE(BTRIM(flow_instance), '') <> ''
		ORDER BY run_id::text, entity_id::text
	`)
	if err != nil {
		return 0, fmt.Errorf("scan contract-resolvable default entity types: %w", err)
	}
	defer rows.Close()

	repairs := []contractEntityTypeRepair{}
	for rows.Next() {
		var item contractEntityTypeRepair
		if err := rows.Scan(&item.RunID, &item.EntityID, &item.FlowInstance); err != nil {
			return 0, fmt.Errorf("scan default entity type row: %w", err)
		}
		contract, ok := entityruntime.ResolveForFlowInstance(source, item.FlowInstance)
		if !ok {
			continue
		}
		item.EntityType = strings.TrimSpace(contract.EntityType)
		if item.EntityType == "" {
			return 0, fmt.Errorf("flow_instance %q resolved to an empty entity contract type", item.FlowInstance)
		}
		repairs = append(repairs, item)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read default entity type rows: %w", err)
	}
	if len(repairs) == 0 {
		return 0, nil
	}

	tx, err := pc.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin contract entity type repair: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	repaired := 0
	for _, repair := range repairs {
		res, err := tx.ExecContext(ctx, `
			UPDATE entity_state
			SET entity_type = $4,
			    updated_at = now()
			WHERE run_id = $1::uuid
			  AND entity_id = $2::uuid
			  AND flow_instance = $3
			  AND COALESCE(NULLIF(BTRIM(entity_type), ''), 'default') = 'default'
		`, repair.RunID, repair.EntityID, repair.FlowInstance, repair.EntityType)
		if err != nil {
			return 0, fmt.Errorf("repair entity_type for entity %s in run %s: %w", repair.EntityID, repair.RunID, err)
		}
		if affected, err := res.RowsAffected(); err == nil {
			repaired += int(affected)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit contract entity type repair: %w", err)
	}
	committed = true
	return repaired, nil
}
