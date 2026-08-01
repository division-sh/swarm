package workflowroute

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	runtimeworkflowroute "github.com/division-sh/swarm/internal/runtime/workflowroute"
)

type QueryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func LoadActivePostgres(ctx context.Context, db QueryRower, instancePath string) (runtimeworkflowroute.RecoveryRecord, error) {
	return loadActive(ctx, db, `
		SELECT flow_template, config
		FROM flow_instances
		WHERE instance_id = $1
		  AND status = 'active'
		  AND terminated_at IS NULL
	`, instancePath)
}

func LoadActiveSQLite(ctx context.Context, db QueryRower, instancePath string) (runtimeworkflowroute.RecoveryRecord, error) {
	return loadActive(ctx, db, `
		SELECT flow_template, config
		FROM flow_instances
		WHERE instance_id = ?
		  AND status = 'active'
		  AND terminated_at IS NULL
	`, instancePath)
}

func loadActive(ctx context.Context, db QueryRower, query, instancePath string) (runtimeworkflowroute.RecoveryRecord, error) {
	instancePath = strings.Trim(strings.TrimSpace(instancePath), "/")
	if instancePath == "" {
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("flow-instance route recovery path is required")
	}
	if db == nil {
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("workflow route recovery reader is required")
	}
	var record runtimeworkflowroute.RecoveryRecord
	var config any
	err := db.QueryRowContext(ctx, query, instancePath).Scan(&record.WorkflowName, &config)
	if err == sql.ErrNoRows {
		return runtimeworkflowroute.RecoveryRecord{}, &runtimeworkflowroute.ActiveRouteNotFound{InstancePath: instancePath}
	}
	if err != nil {
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("load active flow instance for route recovery %s: %w", instancePath, err)
	}
	record.WorkflowName = strings.TrimSpace(record.WorkflowName)
	if record.WorkflowName == "" {
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("flow instance %s has empty flow_template for route recovery", instancePath)
	}
	switch typed := config.(type) {
	case []byte:
		record.Config = append(record.Config[:0], typed...)
	case string:
		record.Config = append(record.Config[:0], typed...)
	default:
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("flow instance %s config has unsupported type %T", instancePath, config)
	}
	if len(record.Config) == 0 {
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("flow instance %s has empty config", instancePath)
	}
	return record, nil
}
