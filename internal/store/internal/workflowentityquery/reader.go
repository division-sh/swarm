package workflowentityquery

import (
	"context"
	"fmt"
	"strings"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	"github.com/division-sh/swarm/internal/runtime/entityquery"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	entitystore "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/workflowheader"
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
		return o.backend.QueryContext(ctx, `SELECT `+workflowheader.DataReadColumns+` `+workflowheader.DataReadFrom+`
		WHERE es.run_id = $1::uuid ORDER BY es.entity_id`, runID)
	})
}

func (o *SQLite) Count(ctx context.Context, request entityquery.Request) (int, error) {
	return count(request, func(runID string) (rowIterator, error) {
		return o.backend.QueryContext(ctx, `SELECT `+workflowheader.DataReadColumns+` `+workflowheader.DataReadFrom+`
		WHERE es.run_id = ? ORDER BY es.entity_id`, runID)
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
		item, err := workflowheader.ScanData(rows, entitystore.DecodeEntityTime)
		if err != nil {
			return 0, fmt.Errorf("scan workflow entity: %w", err)
		}
		flowInstance := strings.Trim(strings.TrimSpace(item.Entity.FlowInstance), "/")
		if flowRoot != "" && flowInstance != flowRoot && !strings.HasPrefix(flowInstance, flowRoot+"/") {
			continue
		}
		materialized, err := entityruntime.NormalizeState(request.Contract, entityruntime.DeclaredValues(request.Contract, item.Fields))
		if err != nil {
			return 0, err
		}
		var currentState any
		if item.Entity.CurrentState != nil {
			currentState = *item.Entity.CurrentState
		}
		matches, err := entityquery.Matches(map[string]any{
			"fields":         materialized,
			"current_state":  currentState,
			"entity_type":    request.Contract.EntityType,
			"flow_instance":  flowRoot,
			"workflow_name":  request.Contract.FlowID,
			"workflow_state": currentState,
		}, request.Predicate)
		if err != nil {
			return 0, err
		}
		if matches {
			matched++
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate workflow entities: %w", err)
	}
	return matched, nil
}
