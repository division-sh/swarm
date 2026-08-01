package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	"github.com/division-sh/swarm/internal/runtime/entityquery"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
)

func (r *recordingRuntimeMutationRunner) CountWorkflowEntities(ctx context.Context, request entityquery.Request) (int, error) {
	if r == nil || r.db == nil {
		return 0, fmt.Errorf("test workflow entity query reader is required")
	}
	if err := request.Validate(); err != nil {
		return 0, err
	}
	runID, err := runtimecurrentstate.ValidateRunID(request.RunID)
	if err != nil {
		return 0, err
	}
	query := `SELECT fields, current_state, flow_instance FROM entity_state WHERE run_id = $1::uuid ORDER BY entity_id`
	if r.dialect == workflowStoreDialectSQLite {
		query = `SELECT fields, current_state, flow_instance FROM entity_state WHERE run_id = ? ORDER BY entity_id`
	}
	rows, err := r.db.QueryContext(ctx, query, runID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	flowRoot := runtimeflowidentity.ScopeKey(request.Source, request.Contract.FlowID)
	count := 0
	for rows.Next() {
		var fieldsRaw []byte
		var currentState string
		var flowInstance string
		if err := rows.Scan(&fieldsRaw, &currentState, &flowInstance); err != nil {
			return 0, err
		}
		flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
		if flowRoot != "" && flowInstance != flowRoot && !strings.HasPrefix(flowInstance, flowRoot+"/") {
			continue
		}
		fields := map[string]any{}
		if err := json.Unmarshal(fieldsRaw, &fields); err != nil {
			return 0, err
		}
		materialized, err := entityruntime.Materialize(request.Contract, entityruntime.DeclaredValues(request.Contract, fields))
		if err != nil {
			return 0, err
		}
		if entityquery.Matches(map[string]any{
			"fields":         materialized,
			"current_state":  strings.TrimSpace(currentState),
			"entity_type":    request.Contract.EntityType,
			"flow_instance":  flowRoot,
			"workflow_name":  request.Contract.FlowID,
			"workflow_state": strings.TrimSpace(currentState),
		}, request.Predicate) {
			count++
		}
	}
	return count, rows.Err()
}
