package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

type SelectedForkStorageTableSnapshot struct {
	Columns []string
	Rows    []string
}

func (s *PipelinePostgresOwner) ReadSelectedForkRecoveredStorageSnapshotForTest(ctx context.Context, runID string) (map[string]SelectedForkStorageTableSnapshot, error) {
	var snapshot map[string]SelectedForkStorageTableSnapshot
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		snapshot, err = readSelectedForkRecoveredStorageSnapshot(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (s *PipelineSQLiteOwner) ReadSelectedForkRecoveredStorageSnapshotForTest(ctx context.Context, runID string) (map[string]SelectedForkStorageTableSnapshot, error) {
	var snapshot map[string]SelectedForkStorageTableSnapshot
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		snapshot, err = readSelectedForkRecoveredStorageSnapshot(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func readSelectedForkRecoveredStorageSnapshot(ctx context.Context, tx *sql.Tx, runID string) (map[string]SelectedForkStorageTableSnapshot, error) {
	snapshot := make(map[string]SelectedForkStorageTableSnapshot, 4)
	for _, table := range []string{"entity_state", "flow_instances", "flow_instance_runtime_readiness", "events"} {
		rows, err := tx.QueryContext(ctx, "SELECT * FROM "+table+" WHERE run_id = $1", runID)
		if err != nil {
			return nil, err
		}
		evidence, err := readRecoveredSnapshotRows(rows)
		if err != nil {
			return nil, err
		}
		snapshot[table] = evidence
	}
	return snapshot, nil
}

func readRecoveredSnapshotRows(rows *sql.Rows) (SelectedForkStorageTableSnapshot, error) {
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return SelectedForkStorageTableSnapshot{}, err
	}
	evidence := SelectedForkStorageTableSnapshot{Columns: append([]string(nil), columns...), Rows: []string{}}
	for rows.Next() {
		values, pointers := make([]any, len(columns)), make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return SelectedForkStorageTableSnapshot{}, err
		}
		encoded, err := EncodeSelectedForkSnapshotValues(columns, values)
		if err != nil {
			return SelectedForkStorageTableSnapshot{}, err
		}
		evidence.Rows = append(evidence.Rows, encoded)
	}
	if err := rows.Err(); err != nil {
		return SelectedForkStorageTableSnapshot{}, err
	}
	if err := rows.Close(); err != nil {
		return SelectedForkStorageTableSnapshot{}, err
	}
	sort.Strings(evidence.Rows)
	return evidence, nil
}

func EncodeSelectedForkSnapshotValues(columns []string, values []any) (string, error) {
	if len(columns) == 0 || len(columns) != len(values) {
		return "", fmt.Errorf("physical snapshot column/value cardinality mismatch")
	}
	encodedValues := make([]struct {
		Column string
		Type   string
		Value  any
	}, len(values))
	for i, value := range values {
		if stamp, ok := value.(time.Time); ok {
			value = stamp.UTC()
		}
		encodedValues[i].Column = columns[i]
		encodedValues[i].Type = fmt.Sprintf("%T", value)
		encodedValues[i].Value = value
	}
	encoded, err := json.Marshal(encodedValues)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}
