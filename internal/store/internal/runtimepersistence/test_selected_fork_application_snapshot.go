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
	initialized := false
	switch owner := selected.(type) {
	case *PostgresStore:
		initialized = owner != nil && owner.backend != nil && owner.schemaOwner != nil && owner.pipelinePostgresOwner != nil
	case *SQLiteRuntimeStore:
		initialized = owner != nil && owner.backend != nil && owner.schema != nil && owner.pipelineSQLiteOwner != nil
	}
	if !initialized {
		return nil, fmt.Errorf("application storage snapshot requires an initialized native observer, got %T", selected)
	}
	query := `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`
	if _, postgres := selected.(*PostgresStore); postgres {
		query = `SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' ORDER BY table_name`
	}
	var snapshot map[string]SelectedForkStorageTableSnapshot
	read := func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, query)
		if err != nil {
			return err
		}
		var tables []string
		for rows.Next() {
			var table string
			if err := rows.Scan(&table); err != nil {
				rows.Close()
				return err
			}
			tables = append(tables, table)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		snapshot = make(map[string]SelectedForkStorageTableSnapshot, len(tables))
		for _, table := range tables {
			rows, err := tx.QueryContext(ctx, `SELECT * FROM "`+strings.ReplaceAll(table, `"`, `""`)+`"`)
			if err != nil {
				return err
			}
			columns, err := rows.Columns()
			if err != nil {
				rows.Close()
				return err
			}
			evidence := SelectedForkStorageTableSnapshot{Columns: append([]string(nil), columns...), Rows: []string{}}
			for rows.Next() {
				values, pointers := make([]any, len(columns)), make([]any, len(columns))
				for i := range values {
					pointers[i] = &values[i]
				}
				if err := rows.Scan(pointers...); err != nil {
					rows.Close()
					return err
				}
				for i, value := range values {
					if raw, ok := value.([]byte); ok {
						values[i] = string(raw)
					}
				}
				encoded, err := json.Marshal(values)
				if err != nil {
					rows.Close()
					return err
				}
				evidence.Rows = append(evidence.Rows, string(encoded))
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				return err
			}
			if err := rows.Close(); err != nil {
				return err
			}
			sort.Strings(evidence.Rows)
			snapshot[table] = evidence
		}
		return nil
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
