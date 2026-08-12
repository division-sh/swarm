package workflowroute

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

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

func (o *Postgres) LoadActive(ctx context.Context, instancePath string) (runtimeworkflowroute.RecoveryRecord, error) {
	instancePath, err := validateInstancePath(instancePath)
	if err != nil {
		return runtimeworkflowroute.RecoveryRecord{}, err
	}
	return scanActive(o.backend.QueryRowContext(ctx, `
		SELECT fi.flow_template, fi.config, COALESCE(owner.entity_id, ''), COALESCE(owner.owner_count, 0)
		  FROM flow_instances fi
		  LEFT JOIN (
			SELECT candidates.flow_instance,
			       candidates.entity_id,
			       COUNT(*) OVER (PARTITION BY candidates.flow_instance) AS owner_count
			  FROM (
				SELECT DISTINCT state.flow_instance, state.entity_id::text AS entity_id
				  FROM entity_state AS state
				  JOIN runs AS run ON run.run_id = state.run_id
				 WHERE LOWER(BTRIM(run.status)) IN ('running', 'paused')
			  ) AS candidates
		  ) AS owner ON owner.flow_instance = fi.instance_id
		 WHERE fi.instance_id = $1 AND fi.status = 'active' AND fi.terminated_at IS NULL
	`, instancePath), instancePath)
}

func (o *SQLite) LoadActive(ctx context.Context, instancePath string) (runtimeworkflowroute.RecoveryRecord, error) {
	instancePath, err := validateInstancePath(instancePath)
	if err != nil {
		return runtimeworkflowroute.RecoveryRecord{}, err
	}
	return scanActive(o.backend.QueryRowContext(ctx, `
		SELECT fi.flow_template, fi.config, COALESCE(owner.entity_id, ''), COALESCE(owner.owner_count, 0)
		  FROM flow_instances fi
		  LEFT JOIN (
			SELECT candidates.flow_instance,
			       candidates.entity_id,
			       COUNT(*) OVER (PARTITION BY candidates.flow_instance) AS owner_count
			  FROM (
				SELECT DISTINCT state.flow_instance, CAST(state.entity_id AS TEXT) AS entity_id
				  FROM entity_state AS state
				  JOIN runs AS run ON run.run_id = state.run_id
				 WHERE LOWER(TRIM(run.status)) IN ('running', 'paused')
			  ) AS candidates
		  ) AS owner ON owner.flow_instance = fi.instance_id
		 WHERE fi.instance_id = ? AND fi.status = 'active' AND fi.terminated_at IS NULL
	`, instancePath), instancePath)
}

func validateInstancePath(instancePath string) (string, error) {
	instancePath = strings.Trim(strings.TrimSpace(instancePath), "/")
	if instancePath == "" {
		return "", fmt.Errorf("flow-instance route recovery path is required")
	}
	return instancePath, nil
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
