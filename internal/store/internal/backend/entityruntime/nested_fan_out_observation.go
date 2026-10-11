package entitystore

import (
	"context"
	"database/sql"
	"errors"
)

type NestedFanOutEntityStorage struct {
	Path, State string
	Fields      []byte
	Revision    int64
}

func ReadNestedRunEntityStateFieldsTx(ctx context.Context, tx *sql.Tx, run string) (out []NestedFanOutEntityStorage, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT current_state,fields FROM entity_state WHERE run_id=$1`, run)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
		if err != nil {
			out = nil
		}
	}()
	for rows.Next() {
		var row NestedFanOutEntityStorage
		if err := rows.Scan(&row.State, &row.Fields); err != nil {
			return nil, err
		}
		row.Fields = append([]byte(nil), row.Fields...)
		out = append(out, row)
	}
	return out, rows.Err()
}

func ReadNestedRunEntityStatesTx(ctx context.Context, tx *sql.Tx, run string) (out []NestedFanOutEntityStorage, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT flow_instance,current_state,fields FROM entity_state WHERE run_id=$1`, run)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
		if err != nil {
			out = nil
		}
	}()
	for rows.Next() {
		var row NestedFanOutEntityStorage
		if err := rows.Scan(&row.Path, &row.State, &row.Fields); err != nil {
			return nil, err
		}
		row.Fields = append([]byte(nil), row.Fields...)
		out = append(out, row)
	}
	return out, rows.Err()
}

func ReadNestedEntityFieldsRevisionTx(ctx context.Context, tx *sql.Tx, run, path string) (NestedFanOutEntityStorage, error) {
	var out NestedFanOutEntityStorage
	err := tx.QueryRowContext(ctx, `SELECT current_state,fields,revision FROM entity_state WHERE run_id=$1 AND flow_instance=$2`, run, path).Scan(&out.State, &out.Fields, &out.Revision)
	if err != nil {
		return NestedFanOutEntityStorage{}, err
	}
	out.Fields = append([]byte(nil), out.Fields...)
	return out, nil
}
