package workflowroute

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeworkflowroute "github.com/division-sh/swarm/internal/runtime/workflowroute"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

type Postgres struct{ backend *postgresbackend.Backend }
type SQLite struct{ backend *sqlitebackend.Backend }

func NewPostgres(backend *postgresbackend.Backend) (*Postgres, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("postgres workflow-route owner requires backend")
	}
	return &Postgres{backend: backend}, nil
}

func NewSQLite(backend *sqlitebackend.Backend) (*SQLite, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("sqlite workflow-route owner requires backend")
	}
	return &SQLite{backend: backend}, nil
}

func (o *Postgres) LoadActive(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance) (runtimeworkflowroute.RecoveryRecord, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return runtimeworkflowroute.RecoveryRecord{}, err
	}
	return scanActive(o.backend.QueryRowContext(ctx, `
		SELECT fi.flow_template, fi.config, COALESCE(owner.entity_id, ''), COALESCE(owner.owner_count, 0)
		  FROM flow_instances fi
		  LEFT JOIN (
			SELECT candidates.run_id, candidates.flow_instance,
			       candidates.entity_id,
			       COUNT(*) OVER (PARTITION BY candidates.run_id, candidates.flow_instance) AS owner_count
			  FROM (
				SELECT DISTINCT state.run_id, state.flow_instance, state.entity_id::text AS entity_id
				  FROM entity_state AS state
				  JOIN runs AS run ON run.run_id = state.run_id
				 WHERE state.run_id = $1::uuid AND LOWER(BTRIM(run.status)) IN ('running', 'paused')
			  ) AS candidates
		  ) AS owner ON owner.run_id = fi.run_id AND owner.flow_instance = fi.instance_path
		 WHERE fi.run_id = $1::uuid AND fi.instance_path = $2 AND fi.status = 'active' AND fi.terminated_at IS NULL
	`, identity.RunID, identity.Route.InstancePath), identity.Route.InstancePath)
}

func (o *SQLite) LoadActive(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance) (runtimeworkflowroute.RecoveryRecord, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return runtimeworkflowroute.RecoveryRecord{}, err
	}
	return scanActive(o.backend.QueryRowContext(ctx, `
		SELECT fi.flow_template, fi.config, COALESCE(owner.entity_id, ''), COALESCE(owner.owner_count, 0)
		  FROM flow_instances fi
		  LEFT JOIN (
			SELECT candidates.run_id, candidates.flow_instance,
			       candidates.entity_id,
			       COUNT(*) OVER (PARTITION BY candidates.run_id, candidates.flow_instance) AS owner_count
			  FROM (
				SELECT DISTINCT state.run_id, state.flow_instance, CAST(state.entity_id AS TEXT) AS entity_id
				  FROM entity_state AS state
				  JOIN runs AS run ON run.run_id = state.run_id
				 WHERE state.run_id = ? AND LOWER(TRIM(run.status)) IN ('running', 'paused')
			  ) AS candidates
		  ) AS owner ON owner.run_id = fi.run_id AND owner.flow_instance = fi.instance_path
		 WHERE fi.run_id = ? AND fi.instance_path = ? AND fi.status = 'active' AND fi.terminated_at IS NULL
	`, identity.RunID, identity.RunID, identity.Route.InstancePath), identity.Route.InstancePath)
}

type rowScanner interface{ Scan(...any) error }

func scanActive(row rowScanner, instancePath string) (runtimeworkflowroute.RecoveryRecord, error) {
	var record runtimeworkflowroute.RecoveryRecord
	var config any
	var ownerCount int
	if err := row.Scan(&record.WorkflowName, &config, &record.EntityID, &ownerCount); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtimeworkflowroute.RecoveryRecord{}, &runtimeworkflowroute.ActiveRouteNotFound{InstancePath: instancePath}
		}
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("load active flow instance for route recovery %s: %w", instancePath, err)
	}
	record.WorkflowName = strings.TrimSpace(record.WorkflowName)
	if record.WorkflowName == "" {
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("flow instance %s has empty flow_template for route recovery", instancePath)
	}
	record.EntityID = strings.TrimSpace(record.EntityID)
	if ownerCount != 1 || record.EntityID == "" {
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("flow instance %s does not have exactly one current persisted entity owner (owners=%d entity_id=%q)", instancePath, ownerCount, record.EntityID)
	}
	switch typed := config.(type) {
	case []byte:
		record.Config = append([]byte(nil), typed...)
	case string:
		record.Config = append([]byte(nil), typed...)
	default:
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("flow instance %s config has unsupported type %T", instancePath, config)
	}
	if len(record.Config) == 0 {
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("flow instance %s has empty config", instancePath)
	}
	return record, nil
}
