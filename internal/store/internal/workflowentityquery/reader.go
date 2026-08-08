package workflowentityquery

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	"github.com/division-sh/swarm/internal/runtime/entityquery"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
)

type Queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func CountPostgres(ctx context.Context, db Queryer, request entityquery.Request) (int, error) {
	return count(ctx, db, request, `
		SELECT fields, current_state, flow_instance
		FROM entity_state
		WHERE run_id = $1::uuid
		ORDER BY entity_id
	`, request.RunID)
}

func CountSQLite(ctx context.Context, db Queryer, request entityquery.Request) (int, error) {
	return count(ctx, db, request, `
		SELECT fields, current_state, flow_instance
		FROM entity_state
		WHERE run_id = ?
		ORDER BY entity_id
	`, request.RunID)
}

func count(ctx context.Context, db Queryer, request entityquery.Request, query string, args ...any) (int, error) {
	if db == nil {
		return 0, fmt.Errorf("workflow entity query requires selected store")
	}
	if err := request.Validate(); err != nil {
		return 0, err
	}
	runID, err := runtimecurrentstate.ValidateRunID(request.RunID)
	if err != nil {
		return 0, err
	}
	if runID != request.RunID {
		args[0] = runID
	}
	flowRoot := runtimeflowidentity.ScopeKey(request.Source, request.Contract.FlowID)
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("query workflow entities: %w", err)
	}
	defer rows.Close()

	matched := 0
	for rows.Next() {
		var fieldsValue any
		var currentState string
		var flowInstance string
		if err := rows.Scan(&fieldsValue, &currentState, &flowInstance); err != nil {
			return 0, fmt.Errorf("scan workflow entity: %w", err)
		}
		flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
		if flowRoot != "" && flowInstance != flowRoot && !strings.HasPrefix(flowInstance, flowRoot+"/") {
			continue
		}
		fields, err := decodeFields(fieldsValue)
		if err != nil {
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
			matched++
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate workflow entities: %w", err)
	}
	return matched, nil
}

func decodeFields(value any) (map[string]any, error) {
	var raw []byte
	switch typed := value.(type) {
	case []byte:
		raw = typed
	case string:
		raw = []byte(typed)
	default:
		return nil, fmt.Errorf("workflow entity fields have unsupported type %T", value)
	}
	fields := map[string]any{}
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("decode workflow entity fields: %w", err)
	}
	return fields, nil
}
