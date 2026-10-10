package runtimepersistence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/schemastore"
)

type SelectedForkStorageTableSnapshot struct {
	Columns []string
	Rows    []string
}

type SelectedForkSnapshotCell struct {
	Column string
	Type   string
	Value  json.RawMessage
}

func (c SelectedForkSnapshotCell) Text() (string, error) {
	if string(c.Value) == "null" {
		return "", nil
	}
	switch c.Type {
	case "string":
		var value string
		err := json.Unmarshal(c.Value, &value)
		return value, err
	case "[]uint8":
		var value []byte
		err := json.Unmarshal(c.Value, &value)
		return string(value), err
	default:
		return "", fmt.Errorf("physical column %s has non-text storage type %s", c.Column, c.Type)
	}
}

func EncodeSelectedForkSnapshotValuesForTest(columns []string, values []any) (string, error) {
	return encodeSelectedForkSnapshotValues(columns, values)
}

func validateSelectedForkSnapshotCell(cell SelectedForkSnapshotCell) error {
	var target any
	switch cell.Type {
	case "<nil>":
		if string(cell.Value) != "null" {
			return fmt.Errorf("physical NULL cell %s has a non-NULL value", cell.Column)
		}
		return nil
	case "string":
		target = new(string)
	case "[]uint8":
		target = new([]byte)
	case "bool":
		target = new(bool)
	case "int64":
		target = new(int64)
	case "float64":
		target = new(float64)
	case "time.Time":
		target = new(time.Time)
	default:
		return fmt.Errorf("physical column %s has unsupported storage type %s", cell.Column, cell.Type)
	}
	if string(cell.Value) == "null" && cell.Type != "[]uint8" {
		return fmt.Errorf("physical column %s has an untyped NULL value", cell.Column)
	}
	return json.Unmarshal(cell.Value, target)
}

func DecodeSelectedForkSnapshotRowsForTest(table SelectedForkStorageTableSnapshot) ([]map[string]SelectedForkSnapshotCell, error) {
	if len(table.Columns) == 0 {
		return nil, fmt.Errorf("physical snapshot is missing its column inventory")
	}
	seen := map[string]bool{}
	for _, column := range table.Columns {
		if column == "" || seen[column] {
			return nil, fmt.Errorf("physical snapshot has an empty or duplicate column %q", column)
		}
		seen[column] = true
	}
	var out []map[string]SelectedForkSnapshotCell
	for _, encoded := range table.Rows {
		var cells []SelectedForkSnapshotCell
		decoder := json.NewDecoder(bytes.NewReader([]byte(encoded)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&cells); err != nil {
			return nil, err
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			return nil, fmt.Errorf("physical snapshot has trailing row data: %v", err)
		}
		if len(cells) != len(table.Columns) {
			return nil, fmt.Errorf("physical snapshot row width=%d, want %d", len(cells), len(table.Columns))
		}
		row := make(map[string]SelectedForkSnapshotCell, len(cells))
		for index, cell := range cells {
			if cell.Column != table.Columns[index] || cell.Type == "" || !json.Valid(cell.Value) {
				return nil, fmt.Errorf("physical snapshot cell %d lost its original column/type/value", index)
			}
			if err := validateSelectedForkSnapshotCell(cell); err != nil {
				return nil, err
			}
			row[cell.Column] = cell
		}
		out = append(out, row)
	}
	return out, nil
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
