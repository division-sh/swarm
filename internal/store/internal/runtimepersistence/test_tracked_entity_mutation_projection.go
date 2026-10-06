package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

type TrackedProjectionMutationStorage struct {
	Domain, Path string
	NewValue     json.RawMessage
}

type TrackedEntityMutationProjectionStorage struct {
	CurrentState                            string
	Fields, Bookkeeping, Gates, Accumulator json.RawMessage
	Mutations                               []TrackedProjectionMutationStorage
}

func ReadTrackedEntityMutationProjectionStorageForTest(ctx context.Context, selected any, run, entity string) (TrackedEntityMutationProjectionStorage, error) {
	for _, identity := range []string{run, entity} {
		if _, err := uuid.Parse(identity); err != nil {
			return TrackedEntityMutationProjectionStorage{}, fmt.Errorf("projection observation requires exact run and entity identities: %w", err)
		}
	}
	var read func(context.Context, func(context.Context, *sql.Tx) error) error
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return TrackedEntityMutationProjectionStorage{}, fmt.Errorf("observation requires an initialized postgres read owner")
		}
		if err := owner.requireCurrentSchema(); err != nil {
			return TrackedEntityMutationProjectionStorage{}, err
		}
		read = owner.backend.RunReadTransaction
	case *SQLiteRuntimeStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return TrackedEntityMutationProjectionStorage{}, fmt.Errorf("observation requires an initialized sqlite read owner")
		}
		if err := owner.requireCurrentSchema(); err != nil {
			return TrackedEntityMutationProjectionStorage{}, err
		}
		read = owner.backend.RunReadTransaction
	default:
		return TrackedEntityMutationProjectionStorage{}, fmt.Errorf("observation requires the original native owner, got %T", selected)
	}
	var evidence TrackedEntityMutationProjectionStorage
	err := read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var fields, bookkeeping, gates, accumulator []byte
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(current_state,''),COALESCE(fields,'{}'),COALESCE(bookkeeping,'{}'),COALESCE(gates,'{}'),COALESCE(accumulator,'{}') FROM entity_state WHERE run_id=$1 AND entity_id=$2`, run, entity).
			Scan(&evidence.CurrentState, &fields, &bookkeeping, &gates, &accumulator); err != nil {
			return fmt.Errorf("load entity_state projection: %w", err)
		}
		evidence.Fields, evidence.Bookkeeping = append(json.RawMessage(nil), fields...), append(json.RawMessage(nil), bookkeeping...)
		evidence.Gates, evidence.Accumulator = append(json.RawMessage(nil), gates...), append(json.RawMessage(nil), accumulator...)
		rows, err := tx.QueryContext(ctx, `SELECT domain,path,new_value FROM entity_mutations WHERE run_id=$1 AND entity_id=$2 ORDER BY created_at ASC,mutation_id ASC`, run, entity)
		if err != nil {
			return fmt.Errorf("query mutations: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var row TrackedProjectionMutationStorage
			var value []byte
			if err := rows.Scan(&row.Domain, &row.Path, &value); err != nil {
				return fmt.Errorf("scan mutation: %w", err)
			}
			row.NewValue = append(json.RawMessage(nil), value...)
			evidence.Mutations = append(evidence.Mutations, row)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("read mutations: %w", err)
		}
		return nil
	})
	if err != nil {
		return TrackedEntityMutationProjectionStorage{}, err
	}
	return evidence, nil
}
