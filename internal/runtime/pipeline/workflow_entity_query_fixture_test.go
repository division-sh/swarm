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
	query := `SELECT event_id::text, event_name, payload::text FROM events WHERE event_id IN ($1::uuid, $2::uuid) ORDER BY event_id`
	if r.dialect != workflowStoreDialectPostgres {
		query = `SELECT event_id, event_name, payload FROM events WHERE event_id IN (?, ?) ORDER BY event_id`
	}
	rows, err := r.db.QueryContext(ctx, query, request.SuccessEventID, request.FailureEventID)
	if err != nil {
		return runtimeactivityresult.Record{}, false, err
	}
	defer rows.Close()
	found := make([]runtimeactivityresult.Record, 0, 2)
	for rows.Next() {
		var record runtimeactivityresult.Record
		var payload string
		if err := rows.Scan(&record.EventID, &record.EventType, &payload); err != nil {
			return runtimeactivityresult.Record{}, false, err
		}
		record.Payload = append(record.Payload, payload...)
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

func (r *recordingRuntimeMutationRunner) LoadActiveWorkflowRoute(ctx context.Context, instancePath string) (runtimeworkflowroute.RecoveryRecord, error) {
	if r == nil || r.db == nil {
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("test workflow route recovery reader is required")
	}
	query := `
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
	`
	if r.dialect != workflowStoreDialectPostgres {
		query = `
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
		`
	}
	var record runtimeworkflowroute.RecoveryRecord
	var config any
	var ownerCount int
	err := r.db.QueryRowContext(ctx, query, instancePath).Scan(&record.WorkflowName, &config, &record.EntityID, &ownerCount)
	if err == sql.ErrNoRows {
		return runtimeworkflowroute.RecoveryRecord{}, &runtimeworkflowroute.ActiveRouteNotFound{InstancePath: instancePath}
	}
	if err != nil {
		return runtimeworkflowroute.RecoveryRecord{}, err
	}
	record.EntityID = strings.TrimSpace(record.EntityID)
	if ownerCount != 1 || record.EntityID == "" {
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("flow instance %s does not have exactly one current persisted entity owner (owners=%d entity_id=%q)", instancePath, ownerCount, record.EntityID)
	}
	switch typed := config.(type) {
	case []byte:
		record.Config = append(record.Config, typed...)
	case string:
		record.Config = append(record.Config, typed...)
	default:
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("unsupported test workflow route config type %T", config)
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
