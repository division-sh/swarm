package workflowentityquery

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	"github.com/division-sh/swarm/internal/runtime/entityquery"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

type Postgres struct{ backend *postgresbackend.Backend }
type SQLite struct{ backend *sqlitebackend.Backend }

func NewPostgres(backend *postgresbackend.Backend) (*Postgres, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("postgres workflow-entity-query owner requires backend")
	}
	return &Postgres{backend: backend}, nil
}

func NewSQLite(backend *sqlitebackend.Backend) (*SQLite, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("sqlite workflow-entity-query owner requires backend")
	}
	return &SQLite{backend: backend}, nil
}

func (o *Postgres) Count(ctx context.Context, request entityquery.Request) (int, error) {
	return count(request, func(runID string) (rowIterator, error) {
		return o.backend.QueryContext(ctx, `
		SELECT fields, current_state, flow_instance
		FROM entity_state
		WHERE run_id = $1::uuid
		ORDER BY entity_id
	`, runID)
	})
}

func (o *SQLite) Count(ctx context.Context, request entityquery.Request) (int, error) {
	return count(request, func(runID string) (rowIterator, error) {
		return o.backend.QueryContext(ctx, `
		SELECT fields, current_state, flow_instance
		FROM entity_state
		WHERE run_id = ?
		ORDER BY entity_id
	`, runID)
	})
}

type rowIterator interface {
	Next() bool
	Scan(...any) error
	Err() error
	Close() error
}

func count(request entityquery.Request, query func(string) (rowIterator, error)) (int, error) {
	if err := request.Validate(); err != nil {
		return 0, err
	}
	runID, err := runtimecurrentstate.ValidateRunID(request.RunID)
	if err != nil {
		return 0, err
	}
	flowRoot := runtimeflowidentity.ScopeKey(request.Source, request.Contract.FlowID)
	rows, err := query(runID)
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
