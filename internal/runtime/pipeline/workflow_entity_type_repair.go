package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	runforkrevision "github.com/division-sh/swarm/internal/runtime/runforkrevision"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/runbundle"
)

type contractEntityTypeRepair struct {
	RunID              string
	EntityID           string
	FlowInstance       string
	FlowTemplate       string
	WorkflowVersion    string
	BundleAvailability runbundle.Availability
	EntityType         string
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
			es.run_id::text,
			es.entity_id::text,
			COALESCE(es.flow_instance, ''),
			COALESCE(fi.flow_template, ''),
			COALESCE(fi.config->>'workflow_version', '')
		FROM entity_state es
		JOIN runs r ON r.run_id = es.run_id
		LEFT JOIN flow_instances fi ON fi.instance_id = es.flow_instance
		WHERE COALESCE(NULLIF(BTRIM(es.entity_type), ''), 'default') = 'default'
		  AND COALESCE(BTRIM(es.flow_instance), '') <> ''
		ORDER BY es.run_id::text, es.entity_id::text
	`)
	if err != nil {
		return 0, fmt.Errorf("scan contract-resolvable default entity types: %w", err)
	}
	defer rows.Close()

	repairs := []contractEntityTypeRepair{}
	for rows.Next() {
		var item contractEntityTypeRepair
		if err := rows.Scan(
			&item.RunID,
			&item.EntityID,
			&item.FlowInstance,
			&item.FlowTemplate,
			&item.WorkflowVersion,
		); err != nil {
			return 0, fmt.Errorf("scan default entity type row: %w", err)
		}
		contract, ok := entityruntime.ResolveForFlowInstance(source, item.FlowInstance)
		if !ok {
			continue
		}
		availability, err := runbundle.LoadAvailability(ctx, pc.db, item.RunID)
		if err != nil {
			return 0, err
		}
		if availability.DataIntegrityError() {
			return 0, fmt.Errorf("contract entity type repair blocked by run bundle availability: %s", availability.DetailString())
		}
		item.BundleAvailability = availability
		if !contractEntityTypeRepairMatchesCurrentSource(source, contract, item, "") {
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
	changes := make([]runforkrevision.Change, 0, len(repairs))
	for _, repair := range repairs {
		changes = append(changes, runforkrevision.Change{RunID: repair.RunID, Families: []runforkrevision.Family{runforkrevision.FamilyEntityMetadata}})
	}
	if _, err := runforkrevision.CaptureChanges(ctx, tx, changes...); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit contract entity type repair: %w", err)
	}
	committed = true
	return repaired, nil
}

func contractEntityTypeRepairMatchesCurrentSource(source semanticview.Source, contract entityruntime.Contract, item contractEntityTypeRepair, currentBundleHash string) bool {
	if source == nil {
		return false
	}
	workflowVersion := strings.TrimSpace(item.WorkflowVersion)
	if workflowVersion == "" || workflowVersion != strings.TrimSpace(source.WorkflowVersion()) {
		return false
	}
	if !item.BundleAvailability.Available() {
		return false
	}
	currentBundleHash = strings.TrimSpace(currentBundleHash)
	if currentBundleHash == "" || currentBundleHash != item.BundleAvailability.BundleHash {
		return false
	}
	flowTemplate := strings.Trim(strings.TrimSpace(item.FlowTemplate), "/")
	if flowTemplate == "" {
		return false
	}
	contractFlowID := strings.Trim(strings.TrimSpace(contract.FlowID), "/")
	if contractFlowID == "" {
		contractFlowID = strings.Trim(strings.TrimSpace(source.WorkflowName()), "/")
	}
	return contractFlowID != "" && flowTemplate == contractFlowID
}
