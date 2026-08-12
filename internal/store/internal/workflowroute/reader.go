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
		SELECT fi.flow_template, fi.config, COALESCE(MIN(es.entity_id::text), ''), COUNT(DISTINCT es.entity_id)
		  FROM flow_instances fi
		  LEFT JOIN entity_state es ON es.flow_instance = fi.instance_id
		 WHERE fi.instance_id = $1 AND fi.status = 'active' AND fi.terminated_at IS NULL
		 GROUP BY fi.instance_id, fi.flow_template, fi.config`, instancePath), instancePath)
}

func (o *SQLite) LoadActive(ctx context.Context, instancePath string) (runtimeworkflowroute.RecoveryRecord, error) {
	instancePath, err := validateInstancePath(instancePath)
	if err != nil {
		return runtimeworkflowroute.RecoveryRecord{}, err
	}
	return scanActive(o.backend.QueryRowContext(ctx, `
		SELECT fi.flow_template, fi.config, COALESCE(MIN(CAST(es.entity_id AS TEXT)), ''), COUNT(DISTINCT es.entity_id)
		  FROM flow_instances fi
		  LEFT JOIN entity_state es ON es.flow_instance = fi.instance_id
		 WHERE fi.instance_id = ? AND fi.status = 'active' AND fi.terminated_at IS NULL
		 GROUP BY fi.instance_id, fi.flow_template, fi.config`, instancePath), instancePath)
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
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("flow instance %s does not have exactly one persisted entity owner (owners=%d entity_id=%q)", instancePath, ownerCount, record.EntityID)
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
