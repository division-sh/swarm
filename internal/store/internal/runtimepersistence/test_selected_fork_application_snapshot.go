package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/schemastore"
)

type SelectedForkStorageTableSnapshot struct {
	Columns []string
	Rows    []string
}

// Recovery refusal compares all physical columns in these four exact-run
// families. The table inventory is private and fixed, never caller-selected.
func ReadSelectedForkRecoveredStorageSnapshotForTest(ctx context.Context, selected any, runID string) (map[string]SelectedForkStorageTableSnapshot, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return nil, err
	}
	if !selectedForkSnapshotObserverInitialized(selected) {
		return nil, fmt.Errorf("recovered storage snapshot requires an initialized native observer, got %T", selected)
	}
	var snapshot map[string]SelectedForkStorageTableSnapshot
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		snapshot = make(map[string]SelectedForkStorageTableSnapshot, 4)
		for _, table := range []string{"entity_state", "flow_instances", "flow_instance_runtime_readiness", "events"} {
			rows, err := tx.QueryContext(ctx, "SELECT * FROM "+table+" WHERE run_id = $1", runID)
			if err != nil {
				return err
			}
			evidence, err := readSelectedForkSnapshotRows(rows)
			if err != nil {
				return err
			}
			snapshot[table] = evidence
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

// This is one closed whole-store immutability witness. Callers cannot select
// tables, columns, queries or predicates, or acquire persistence authority.
func ReadSelectedForkApplicationStorageSnapshotForTest(ctx context.Context, selected any) (map[string]SelectedForkStorageTableSnapshot, error) {
	// Physical inspection is not runtime admission. An independently opened
	// native observer remains schema-unaccepted and gains no writer authority.
	if !selectedForkSnapshotObserverInitialized(selected) {
		return nil, fmt.Errorf("application storage snapshot requires an initialized native observer, got %T", selected)
	}
	_, postgres := selected.(*PostgresStore)
	var snapshot map[string]SelectedForkStorageTableSnapshot
	read := func(ctx context.Context, tx *sql.Tx) error {
		var err error
		snapshot, err = readSelectedForkSnapshot(ctx, tx, postgres)
		return err
	}
	var err error
	switch owner := selected.(type) {
	case *PostgresStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	case *SQLiteRuntimeStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	}
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func selectedForkSnapshotObserverInitialized(selected any) bool {
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner != nil && owner.backend != nil && owner.schemaOwner != nil && owner.pipelinePostgresOwner != nil
	case *SQLiteRuntimeStore:
		return owner != nil && owner.backend != nil && owner.schema != nil && owner.pipelineSQLiteOwner != nil
	default:
		return false
	}
}

func readSelectedForkSnapshot(ctx context.Context, tx *sql.Tx, postgres bool) (map[string]SelectedForkStorageTableSnapshot, error) {
	tables, err := readSelectedForkSnapshotTables(ctx, tx, postgres)
	if err != nil {
		return nil, err
	}
	snapshot := make(map[string]SelectedForkStorageTableSnapshot, len(tables))
	for _, table := range tables {
		evidence, err := readSelectedForkSnapshotTable(ctx, tx, table)
		if err != nil {
			return nil, err
		}
		snapshot[table] = evidence
	}
	return snapshot, nil
}

func readSelectedForkSnapshotTables(ctx context.Context, tx *sql.Tx, postgres bool) ([]string, error) {
	if postgres {
		return schemastore.PostgresDataTables(ctx, tx)
	}
	return schemastore.SQLiteDataTables(ctx, tx)
}

func readSelectedForkSnapshotTable(ctx context.Context, tx *sql.Tx, table string) (SelectedForkStorageTableSnapshot, error) {
	rows, err := tx.QueryContext(ctx, `SELECT * FROM "`+strings.ReplaceAll(table, `"`, `""`)+`"`)
	if err != nil {
		return SelectedForkStorageTableSnapshot{}, err
	}
	return readSelectedForkSnapshotRows(rows)
}

func readSelectedForkSnapshotRows(rows *sql.Rows) (SelectedForkStorageTableSnapshot, error) {
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return SelectedForkStorageTableSnapshot{}, err
	}
	evidence := SelectedForkStorageTableSnapshot{Columns: append([]string(nil), columns...), Rows: []string{}}
	for rows.Next() {
		encoded, err := encodeSelectedForkSnapshotRow(rows, columns)
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

func encodeSelectedForkSnapshotRow(rows *sql.Rows, columns []string) (string, error) {
	values, pointers := make([]any, len(columns)), make([]any, len(columns))
	for i := range values {
		pointers[i] = &values[i]
	}
	if err := rows.Scan(pointers...); err != nil {
		return "", err
	}
	return encodeSelectedForkSnapshotValues(columns, values)
}

func encodeSelectedForkSnapshotValues(columns []string, values []any) (string, error) {
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
