package workflowroute

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
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
	active := runlifecycle.ActiveStates()
	return scanActive(o.backend.QueryRowContext(ctx, `
		SELECT fi.flow_template, fi.config, fi.entity_id::text, fi.entity_type,
			(SELECT COUNT(*) FROM entity_state fields WHERE fields.run_id = fi.run_id AND fields.flow_instance = fi.instance_path),
			es.entity_id::text, es.entity_type
		FROM flow_instances fi
		JOIN runs run ON run.run_id = fi.run_id
		LEFT JOIN entity_state es ON es.run_id = fi.run_id AND es.flow_instance = fi.instance_path
		WHERE fi.run_id = $1::uuid AND fi.instance_path = $2
			AND fi.status = 'active' AND fi.terminated_at IS NULL AND run.status IN ($3, $4)
	`, identity.RunID, identity.Route.InstancePath, active[0], active[1]), identity.Route.InstancePath)
}

func (o *SQLite) LoadActive(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance) (runtimeworkflowroute.RecoveryRecord, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return runtimeworkflowroute.RecoveryRecord{}, err
	}
	active := runlifecycle.ActiveStates()
	return scanActive(o.backend.QueryRowContext(ctx, `
		SELECT fi.flow_template, fi.config, fi.entity_id, fi.entity_type,
			(SELECT COUNT(*) FROM entity_state fields WHERE fields.run_id = fi.run_id AND fields.flow_instance = fi.instance_path),
			es.entity_id, es.entity_type
		FROM flow_instances fi
		JOIN runs run ON run.run_id = fi.run_id
		LEFT JOIN entity_state es ON es.run_id = fi.run_id AND es.flow_instance = fi.instance_path
		WHERE fi.run_id = ? AND fi.instance_path = ?
			AND fi.status = 'active' AND fi.terminated_at IS NULL AND run.status IN (?, ?)
	`, identity.RunID, identity.Route.InstancePath, active[0], active[1]), identity.Route.InstancePath)
}

type rowScanner interface{ Scan(...any) error }

func scanActive(row rowScanner, instancePath string) (runtimeworkflowroute.RecoveryRecord, error) {
	var record runtimeworkflowroute.RecoveryRecord
	var config any
	var fieldCount int
	var contract, fieldEntityID, fieldContract sql.NullString
	if err := row.Scan(&record.WorkflowName, &config, &record.EntityID, &contract, &fieldCount, &fieldEntityID, &fieldContract); err != nil {
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
	if record.EntityID == "" {
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("flow instance %s requires exact constructed header identity", instancePath)
	}
	if contract.Valid {
		if fieldCount != 1 || fieldEntityID.String != record.EntityID || fieldContract.String != contract.String {
			return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("constructed flow %s requires exactly one matching declared field row (rows=%d)", instancePath, fieldCount)
		}
	} else if fieldCount != 0 {
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("fieldless flow %s cannot have entity state rows", instancePath)
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
