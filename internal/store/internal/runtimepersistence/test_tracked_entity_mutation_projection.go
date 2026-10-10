package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/store/internal/workflowheader"
	"github.com/google/uuid"
)

type TrackedProjectionMutationStorage struct {
	Domain, Path                      string
	OldValue, NewValue                json.RawMessage
	WriterType, WriterID, HandlerStep string
}

type TrackedEntityMutationProjectionStorage struct {
	EntityType                              string
	CurrentState                            *string
	Revision                                *int64
	Fields, Bookkeeping, Gates, Accumulator json.RawMessage
	Mutations                               []TrackedProjectionMutationStorage
}

func ReadTrackedEntityMutationProjectionStorageForTest(ctx context.Context, selected any, run, entity string) (TrackedEntityMutationProjectionStorage, error) {
	for _, identity := range []string{run, entity} {
		if _, err := uuid.Parse(identity); err != nil {
			return TrackedEntityMutationProjectionStorage{}, fmt.Errorf("projection observation requires exact run and entity identities: %w", err)
		}
	}
	read, err := selectedTrackedProjectionReadForTest(selected)
	if err != nil {
		return TrackedEntityMutationProjectionStorage{}, err
	}
	var evidence TrackedEntityMutationProjectionStorage
	err = read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		inventory, err := workflowheader.InventoryForRead(ctx, tx, run)
		if err != nil {
			return fmt.Errorf("load current header/data projection: %w", err)
		}
		evidence, err = trackedProjectionStorageFromInventory(inventory, entity)
		if err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT domain,path,old_value,new_value,writer_type,writer_id,COALESCE(handler_step,'') FROM entity_mutations WHERE run_id=$1 AND entity_id=$2 ORDER BY created_at ASC,mutation_id ASC`, run, entity)
		if err != nil {
			return fmt.Errorf("query mutations: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var row TrackedProjectionMutationStorage
			var oldValue, value []byte
			if err := rows.Scan(&row.Domain, &row.Path, &oldValue, &value, &row.WriterType, &row.WriterID, &row.HandlerStep); err != nil {
				return fmt.Errorf("scan mutation: %w", err)
			}
			row.NewValue = append(json.RawMessage(nil), value...)
			row.OldValue = append(json.RawMessage(nil), oldValue...)
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

func trackedProjectionStorageFromInventory(inventory workflowheader.ReadInventory, entity string) (TrackedEntityMutationProjectionStorage, error) {
	var evidence TrackedEntityMutationProjectionStorage
	found := false
	for index, member := range append(inventory.Constructed, inventory.StateOnly...) {
		if member.EntityID != entity {
			continue
		}
		if found {
			return TrackedEntityMutationProjectionStorage{}, fmt.Errorf("current projection has multiple owners for entity %s", entity)
		}
		found = true
		evidence.EntityType = member.EntityType
		if index < len(inventory.Constructed) {
			evidence.CurrentState = mutationlog.PresentLifecycleState(member.Stage)
			revision := member.Revision
			evidence.Revision = &revision
		}
		// Keep private control/loop facts intact; public entity projections
		// intentionally hide them and cannot serve as reconstruction operands.
		for _, bucket := range []struct {
			raw any
			dst *json.RawMessage
		}{{member.Fields, &evidence.Fields}, {member.Bookkeeping, &evidence.Bookkeeping}, {member.Gates, &evidence.Gates}, {member.Accumulator, &evidence.Accumulator}} {
			switch raw := bucket.raw.(type) {
			case nil:
			case string:
				*bucket.dst = append(json.RawMessage(nil), raw...)
			case []byte:
				*bucket.dst = append(json.RawMessage(nil), raw...)
			case json.RawMessage:
				*bucket.dst = append(json.RawMessage(nil), raw...)
			default:
				return TrackedEntityMutationProjectionStorage{}, fmt.Errorf("unexpected admitted projection transport %T", raw)
			}
		}
	}
	if !found {
		return TrackedEntityMutationProjectionStorage{}, fmt.Errorf("load current entity projection: %w", sql.ErrNoRows)
	}
	return evidence, nil
}

// CorruptRegistryVerdictForTest deliberately bypasses history for one existing
// authored field. It is a closed hostile fixture, not an executable writer.
func CorruptRegistryVerdictForTest(ctx context.Context, selected any, run, entity, verdict string) error {
	for _, identity := range []string{run, entity} {
		if _, err := uuid.Parse(identity); err != nil {
			return fmt.Errorf("registry corruption requires exact run and entity identities: %w", err)
		}
	}
	if err := requireIssue2564EvidenceOwner(selected); err != nil {
		return err
	}
	apply := func(ctx context.Context, tx *sql.Tx) error {
		var raw []byte
		if err := tx.QueryRowContext(ctx, `SELECT fields FROM entity_state WHERE run_id=$1 AND entity_id=$2`, run, entity).Scan(&raw); err != nil {
			return err
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return err
		}
		var original string
		if err := json.Unmarshal(fields["verdict"], &original); err != nil {
			return fmt.Errorf("registry fixture requires an assigned text verdict: %w", err)
		}
		if original == verdict {
			return fmt.Errorf("registry fixture must change the existing verdict")
		}
		encoded, err := json.Marshal(verdict)
		if err != nil {
			return err
		}
		fields["verdict"] = encoded
		raw, err = json.Marshal(fields)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE entity_state SET fields=$1 WHERE run_id=$2 AND entity_id=$3`, string(raw), run, entity)
		if err != nil {
			return err
		}
		return requireMailboxFixtureRow(result)
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.backend.RunTransaction(ctx, apply)
	case *SQLiteRuntimeStore:
		return owner.backend.RunTransaction(ctx, "registry verdict corruption fixture", apply)
	default:
		return fmt.Errorf("registry fixture requires the original native owner")
	}
}

func selectedTrackedProjectionReadForTest(selected any) (func(context.Context, func(context.Context, *sql.Tx) error) error, error) {
	var read func(context.Context, func(context.Context, *sql.Tx) error) error
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner == nil || owner.backend == nil || owner.pipelinePostgresOwner == nil || !owner.backend.Valid() {
			return nil, fmt.Errorf("observation requires an initialized postgres read owner")
		}
		if err := owner.requireCurrentSchema(); err != nil {
			return nil, err
		}
		read = owner.backend.RunReadTransaction
	case *SQLiteRuntimeStore:
		if owner == nil || owner.backend == nil || owner.pipelineSQLiteOwner == nil || !owner.backend.Valid() {
			return nil, fmt.Errorf("observation requires an initialized sqlite read owner")
		}
		if err := owner.requireCurrentSchema(); err != nil {
			return nil, err
		}
		read = owner.backend.RunReadTransaction
	default:
		return nil, fmt.Errorf("observation requires the original native owner, got %T", selected)
	}
	return read, nil
}

func ReadRunEntityMutationHistoryStorageForTest(ctx context.Context, selected any, run string) ([]operatorread.RunDebugMutation, error) {
	if err := validateWorkflowProjectionFaultRun(run); err != nil {
		return nil, err
	}
	read, err := selectedTrackedProjectionReadForTest(selected)
	if err != nil {
		return nil, err
	}
	var evidence []operatorread.RunDebugMutation
	err = read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT entity_id,domain,path,COALESCE(new_value,'null') FROM entity_mutations WHERE run_id=$1 ORDER BY created_at DESC,mutation_id DESC`, run)
		if err != nil {
			return fmt.Errorf("query mutations: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var row operatorread.RunDebugMutation
			var value []byte
			if err := rows.Scan(&row.EntityID, &row.Domain, &row.Path, &value); err != nil {
				return fmt.Errorf("scan mutation: %w", err)
			}
			row.NewValue = append(json.RawMessage(nil), value...)
			evidence = append(evidence, row)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("read mutations: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return evidence, nil
}
