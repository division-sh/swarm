package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type SelectedForkStorageTableSnapshot struct {
	Columns []string
	Rows    []string
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
	query := `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`
	if postgres {
		query = `SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' ORDER BY table_name`
	}
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tables, rows.Close()
}

func readSelectedForkSnapshotTable(ctx context.Context, tx *sql.Tx, table string) (SelectedForkStorageTableSnapshot, error) {
	rows, err := tx.QueryContext(ctx, `SELECT * FROM "`+strings.ReplaceAll(table, `"`, `""`)+`"`)
	if err != nil {
		return SelectedForkStorageTableSnapshot{}, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return SelectedForkStorageTableSnapshot{}, err
	}
	evidence := SelectedForkStorageTableSnapshot{Columns: append([]string(nil), columns...), Rows: []string{}}
	for rows.Next() {
		encoded, err := encodeSelectedForkSnapshotRow(rows, len(columns))
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

func encodeSelectedForkSnapshotRow(rows *sql.Rows, columns int) (string, error) {
	values, pointers := make([]any, columns), make([]any, columns)
	for i := range values {
		pointers[i] = &values[i]
	}
	if err := rows.Scan(pointers...); err != nil {
		return "", err
	}
	for i, value := range values {
		if raw, ok := value.([]byte); ok {
			values[i] = string(raw)
		}
	}
	encoded, err := json.Marshal(values)
	return string(encoded), err
}
