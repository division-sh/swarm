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
	return scanActive(o.backend.QueryRowContext(ctx, `SELECT flow_template, config FROM flow_instances WHERE instance_id = $1 AND status = 'active' AND terminated_at IS NULL`, instancePath), instancePath)
}

func (o *SQLite) LoadActive(ctx context.Context, instancePath string) (runtimeworkflowroute.RecoveryRecord, error) {
	instancePath, err := validateInstancePath(instancePath)
	if err != nil {
		return runtimeworkflowroute.RecoveryRecord{}, err
	}
	return scanActive(o.backend.QueryRowContext(ctx, `SELECT flow_template, config FROM flow_instances WHERE instance_id = ? AND status = 'active' AND terminated_at IS NULL`, instancePath), instancePath)
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
	if err := row.Scan(&record.WorkflowName, &config); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtimeworkflowroute.RecoveryRecord{}, &runtimeworkflowroute.ActiveRouteNotFound{InstancePath: instancePath}
		}
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("load active flow instance for route recovery %s: %w", instancePath, err)
	}
	record.WorkflowName = strings.TrimSpace(record.WorkflowName)
	if record.WorkflowName == "" {
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("flow instance %s has empty flow_template for route recovery", instancePath)
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
