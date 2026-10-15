package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

type WorkflowControlProjectionStorage struct {
	CurrentState, ControlStatus string
	Fields                      json.RawMessage
}

type WorkflowRunFieldsRevisionStorage struct {
	Fields   string
	Revision int64
}

type WorkflowAccumulatorStorage struct {
	Revision            int64
	Fields, Accumulator []byte
}

func ReadRootFlowAccumulatorStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]byte, error) {
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT accumulator FROM flow_instances WHERE run_id=$1 AND entity_id=$1`, runID).Scan(&raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func ReadWorkflowAccumulatorStorageTx(ctx context.Context, tx *sql.Tx, runID, path string) (WorkflowAccumulatorStorage, error) {
	var out WorkflowAccumulatorStorage
	err := tx.QueryRowContext(ctx, `SELECT revision, fields, accumulator FROM entity_state WHERE run_id=$1 AND flow_instance=$2`, runID, path).
		Scan(&out.Revision, &out.Fields, &out.Accumulator)
	if err != nil {
		return WorkflowAccumulatorStorage{}, err
	}
	return out, nil
}

func ReadBoundReplyFlowHeadersStorageTx(ctx context.Context, tx *sql.Tx, runID, path string) ([][]any, error) {
	return readBoundReplyPhysicalRows(ctx, tx, `SELECT * FROM flow_instances WHERE run_id=$1 AND instance_path=$2 ORDER BY instance_path`, runID, path)
}

func ReadBoundReplyEntityStatesStorageTx(ctx context.Context, tx *sql.Tx, runID, path string) ([][]any, error) {
	return readBoundReplyPhysicalRows(ctx, tx, `SELECT * FROM entity_state WHERE run_id=$1 AND flow_instance=$2 ORDER BY entity_id`, runID, path)
}

func readBoundReplyPhysicalRows(ctx context.Context, tx *sql.Tx, query, runID, path string) (out [][]any, err error) {
	rows, err := tx.QueryContext(ctx, query, runID, path)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
		if err != nil {
			out = nil
		}
	}()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		values, destinations := make([]any, len(columns)), make([]any, len(columns))
		for index := range values {
			destinations[index] = &values[index]
		}
		if err := rows.Scan(destinations...); err != nil {
			return nil, err
		}
		for index, value := range values {
			if raw, ok := value.([]byte); ok {
				values[index] = string(raw)
			}
		}
		out = append(out, values)
	}
	return out, rows.Err()
}

func ReadWorkflowRunFieldsRevisionStorageTx(ctx context.Context, tx *sql.Tx, runID string) (WorkflowRunFieldsRevisionStorage, error) {
	var out WorkflowRunFieldsRevisionStorage
	err := tx.QueryRowContext(ctx, `SELECT CAST(fields AS TEXT), revision FROM entity_state WHERE run_id=$1`, runID).Scan(&out.Fields, &out.Revision)
	return out, err
}

func ReadWorkflowRunHeaderRevisionStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int64, error) {
	var revision int64
	err := tx.QueryRowContext(ctx, `SELECT revision FROM flow_instances WHERE run_id=$1`, runID).Scan(&revision)
	return revision, err
}

func ReadWorkflowRunFieldsStorageTx(ctx context.Context, tx *sql.Tx, runID string) (string, error) {
	var fields string
	err := tx.QueryRowContext(ctx, `SELECT CAST(fields AS TEXT) FROM entity_state WHERE run_id=$1`, runID).Scan(&fields)
	return fields, err
}

type ActivityForkEntityStorage struct {
	Flow, EntityType, Stage string
	Fields, Accumulator     []byte
}

func ReadActivityForkEntityStorageTx(ctx context.Context, tx *sql.Tx, runID, entityID string) (ActivityForkEntityStorage, error) {
	var out ActivityForkEntityStorage
	err := tx.QueryRowContext(ctx, `SELECT flow_instance,entity_type,current_state,fields,accumulator FROM entity_state WHERE run_id=$1 AND entity_id=$2`, runID, entityID).
		Scan(&out.Flow, &out.EntityType, &out.Stage, &out.Fields, &out.Accumulator)
	if err != nil {
		return ActivityForkEntityStorage{}, err
	}
	return out, nil
}

func CountWorkflowFieldRowsForRun(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_state WHERE run_id=$1`, runID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func CountWorkflowHeadersCreatedSince(ctx context.Context, tx *sql.Tx, since time.Time) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_instances WHERE created_at >= $1`, since).Scan(&count)
	return count, err
}

func CountWorkflowHeadersForPathCreatedSince(ctx context.Context, tx *sql.Tx, path string, since time.Time) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_instances WHERE instance_path=$1 AND created_at >= $2`, path, since).Scan(&count)
	return count, err
}

func ReadLatestWorkflowFieldsForPath(ctx context.Context, tx *sql.Tx, path string) (json.RawMessage, bool, error) {
	var fields []byte
	err := tx.QueryRowContext(ctx, `SELECT e.fields FROM flow_instances f
JOIN entity_state e ON e.run_id=f.run_id AND e.flow_instance=f.instance_path
WHERE f.instance_path=$1 ORDER BY f.created_at DESC LIMIT 1`, path).Scan(&fields)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return append(json.RawMessage(nil), fields...), true, nil
}

type WorkflowDuplicateProjectionStorage struct {
	Revision  int
	FieldName string
	Fields    json.RawMessage
}

type WorkflowStateObservationRow struct {
	EntityID, FlowInstance, CurrentState string
}

func ReadWorkflowStateObservationRows(ctx context.Context, tx *sql.Tx, postgres bool) ([]WorkflowStateObservationRow, error) {
	query := `SELECT entity_id::text, COALESCE(flow_instance, ''), current_state FROM entity_state ORDER BY created_at ASC`
	if !postgres {
		query = `SELECT entity_id, COALESCE(flow_instance, ''), current_state FROM entity_state ORDER BY created_at ASC`
	}
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []WorkflowStateObservationRow{}
	for rows.Next() {
		var row WorkflowStateObservationRow
		if err := rows.Scan(&row.EntityID, &row.FlowInstance, &row.CurrentState); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func ReadWorkflowControlProjectionStorageForTest(ctx context.Context, tx *sql.Tx, postgres bool, run, entity string) (WorkflowControlProjectionStorage, error) {
	status := "json_extract(fi.config, '$.status')"
	if postgres {
		status = "fi.config->>'status'"
	}
	var out WorkflowControlProjectionStorage
	var fields []byte
	err := tx.QueryRowContext(ctx, `SELECT es.current_state, es.fields, COALESCE(`+status+`, '')
		FROM entity_state es JOIN flow_instances fi ON fi.run_id=es.run_id AND fi.instance_path=es.flow_instance
		WHERE es.run_id=$1 AND es.entity_id=$2`, run, entity).Scan(&out.CurrentState, &fields, &out.ControlStatus)
	if err != nil {
		return WorkflowControlProjectionStorage{}, err
	}
	out.Fields = append(json.RawMessage(nil), fields...)
	return out, nil
}

func ReadWorkflowDuplicateProjectionStorageForTest(ctx context.Context, tx *sql.Tx, postgres bool, run, entity string) (WorkflowDuplicateProjectionStorage, error) {
	name := "json_extract(es.fields, '$.name')"
	if postgres {
		name = "es.fields->>'name'"
	}
	var out WorkflowDuplicateProjectionStorage
	var fields []byte
	err := tx.QueryRowContext(ctx, `SELECT es.revision, COALESCE(`+name+`, ''), es.fields
		FROM entity_state es JOIN flow_instances fi ON fi.run_id=es.run_id AND fi.instance_path=es.flow_instance
		WHERE es.run_id=$1 AND es.entity_id=$2`, run, entity).Scan(&out.Revision, &out.FieldName, &fields)
	if err != nil {
		return WorkflowDuplicateProjectionStorage{}, err
	}
	out.Fields = append(json.RawMessage(nil), fields...)
	return out, nil
}

func CountWorkflowInstanceHeadersForTest(ctx context.Context, tx *sql.Tx) (int64, error) {
	var count int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_instances`).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func ReadWorkflowTransitionEvidenceWireForTest(ctx context.Context, tx *sql.Tx, run, path string) (json.RawMessage, error) {
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT config FROM flow_instances WHERE run_id=$1 AND instance_path=$2`, run, path).Scan(&raw); err != nil {
		return nil, err
	}
	return append(json.RawMessage(nil), raw...), nil
}

type WorkflowEnginePhysicalCounts struct {
	EntityStates, ConstructedHeaders, MutationJournal int64
}

func ReadWorkflowEnginePhysicalCountsForTest(ctx context.Context, tx *sql.Tx) (WorkflowEnginePhysicalCounts, error) {
	var out WorkflowEnginePhysicalCounts
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_state`).Scan(&out.EntityStates); err != nil {
		return WorkflowEnginePhysicalCounts{}, err
	}
	var err error
	out.ConstructedHeaders, err = CountWorkflowInstanceHeadersForTest(ctx, tx)
	if err != nil {
		return WorkflowEnginePhysicalCounts{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_mutations`).Scan(&out.MutationJournal); err != nil {
		return WorkflowEnginePhysicalCounts{}, err
	}
	return out, nil
}
