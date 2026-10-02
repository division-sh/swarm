package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	runtimeactivityresult "github.com/division-sh/swarm/internal/runtime/activityresult"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	"github.com/division-sh/swarm/internal/runtime/entityquery"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimeworkflowroute "github.com/division-sh/swarm/internal/runtime/workflowroute"
)

func (r *recordingRuntimeMutationRunner) LoadRecordedActivityResult(ctx context.Context, request runtimeactivityresult.Query) (runtimeactivityresult.Record, bool, error) {
	if r == nil || r.db == nil {
		return runtimeactivityresult.Record{}, false, fmt.Errorf("test activity result reader is required")
	}
	query := `SELECT event_id::text, event_name FROM events WHERE event_id IN ($1::uuid, $2::uuid) ORDER BY event_id`
	if r.dialect != workflowStoreDialectPostgres {
		query = `SELECT event_id, event_name FROM events WHERE event_id IN (?, ?) ORDER BY event_id`
	}
	rows, err := r.db.QueryContext(ctx, query, request.SuccessEventID, request.FailureEventID)
	if err != nil {
		return runtimeactivityresult.Record{}, false, err
	}
	defer rows.Close()
	found := make([]runtimeactivityresult.Record, 0, 2)
	for rows.Next() {
		var record runtimeactivityresult.Record
		if err := rows.Scan(&record.EventID, &record.EventType); err != nil {
			return runtimeactivityresult.Record{}, false, err
		}
		found = append(found, record)
	}
	if err := rows.Err(); err != nil {
		return runtimeactivityresult.Record{}, false, err
	}
	if len(found) == 0 {
		return runtimeactivityresult.Record{}, false, nil
	}
	if len(found) != 1 {
		return runtimeactivityresult.Record{}, false, fmt.Errorf("activity request %s has both success and failure results recorded", request.RequestEventID)
	}
	return found[0], true, nil
}

func (r *recordingRuntimeMutationRunner) LoadActiveWorkflowRoute(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance) (runtimeworkflowroute.RecoveryRecord, error) {
	if r == nil || r.db == nil {
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("test workflow route recovery reader is required")
	}
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return runtimeworkflowroute.RecoveryRecord{}, err
	}
	// Component adapter: identity belongs to the header, not an inferred field-row owner.
	query := `
		SELECT fi.flow_template, fi.config, fi.entity_id::text, fi.entity_type,
			(SELECT COUNT(*) FROM entity_state fields WHERE fields.run_id = fi.run_id AND fields.flow_instance = fi.instance_path),
			es.entity_id::text, es.entity_type
		FROM flow_instances fi
		JOIN runs run ON run.run_id = fi.run_id
		LEFT JOIN entity_state es ON es.run_id = fi.run_id AND es.flow_instance = fi.instance_path
		WHERE fi.run_id = $1::uuid AND fi.instance_path = $2
			AND fi.status = 'active' AND fi.terminated_at IS NULL AND run.status IN ('running', 'paused')
	`
	args := []any{identity.RunID, identity.Route.InstancePath}
	if r.dialect != workflowStoreDialectPostgres {
		query = `
			SELECT fi.flow_template, fi.config, fi.entity_id, fi.entity_type,
				(SELECT COUNT(*) FROM entity_state fields WHERE fields.run_id = fi.run_id AND fields.flow_instance = fi.instance_path),
				es.entity_id, es.entity_type
			FROM flow_instances fi
			JOIN runs run ON run.run_id = fi.run_id
			LEFT JOIN entity_state es ON es.run_id = fi.run_id AND es.flow_instance = fi.instance_path
			WHERE fi.run_id = ? AND fi.instance_path = ?
				AND fi.status = 'active' AND fi.terminated_at IS NULL AND run.status IN ('running', 'paused')
		`
	}
	var record runtimeworkflowroute.RecoveryRecord
	var config any
	var fieldCount int
	var contract, fieldEntityID, fieldContract sql.NullString
	err := r.db.QueryRowContext(ctx, query, args...).Scan(&record.WorkflowName, &config, &record.EntityID, &contract, &fieldCount, &fieldEntityID, &fieldContract)
	if err == sql.ErrNoRows {
		return runtimeworkflowroute.RecoveryRecord{}, &runtimeworkflowroute.ActiveRouteNotFound{InstancePath: identity.Route.InstancePath}
	}
	if err != nil {
		return runtimeworkflowroute.RecoveryRecord{}, err
	}
	record.WorkflowName = strings.TrimSpace(record.WorkflowName)
	if record.WorkflowName == "" {
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("flow instance %s has empty flow_template for route recovery", identity.Route.InstancePath)
	}
	record.EntityID = strings.TrimSpace(record.EntityID)
	if record.EntityID == "" {
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("flow instance %s requires exact constructed header identity", identity.Route.InstancePath)
	}
	if contract.Valid {
		if fieldCount != 1 || fieldEntityID.String != record.EntityID || fieldContract.String != contract.String {
			return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("constructed flow %s requires exactly one matching declared field row (rows=%d)", identity.Route.InstancePath, fieldCount)
		}
	} else if fieldCount != 0 {
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("fieldless flow %s cannot have entity state rows", identity.Route.InstancePath)
	}
	switch typed := config.(type) {
	case []byte:
		record.Config = append(record.Config, typed...)
	case string:
		record.Config = append(record.Config, typed...)
	default:
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("unsupported test workflow route config type %T", config)
	}
	if len(record.Config) == 0 {
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("flow instance %s has empty config", identity.Route.InstancePath)
	}
	return record, nil
}

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
	if r.dialect != workflowStoreDialectPostgres {
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
		materialized, err := entityruntime.NormalizeState(request.Contract, entityruntime.DeclaredValues(request.Contract, fields))
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

func (r *recordingRuntimeMutationRunner) RequireGateRouteAdmitted(ctx context.Context, runID string) error {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return errors.New("gate route run id is required")
	}
	query := `SELECT status FROM runs WHERE run_id = $1::uuid`
	args := []any{runID}
	queryer := interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	}(r.db)
	if r.dialect == workflowStoreDialectSQLite {
		query = `SELECT status FROM runs WHERE run_id = ?`
	}
	if tx, ok := PipelineSQLTxFromContext(ctx); ok && tx != nil {
		queryer = tx
	}
	var status string
	if err := queryer.QueryRowContext(ctx, query, args...).Scan(&status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("gate route run %s is unavailable", runID)
		}
		return err
	}
	if status != string(runtimerunlifecycle.StateRunning) {
		return fmt.Errorf("gate route run %s is not routable in status %s", runID, status)
	}
	return nil
}
