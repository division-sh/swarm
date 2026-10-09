package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

func (s *RunForkPostgresOwner) InspectRunMutationDrift(ctx context.Context, runID string) (mutationlog.DriftReport, error) {
	var report mutationlog.DriftReport
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		report, err = inspectRunMutationDrift(ctx, tx, runID)
		return err
	})
	return report, err
}

func (s *RunForkSQLiteOwner) InspectRunMutationDrift(ctx context.Context, runID string) (mutationlog.DriftReport, error) {
	var report mutationlog.DriftReport
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		report, err = inspectRunMutationDrift(ctx, tx, runID)
		return err
	})
	return report, err
}

func inspectRunMutationDrift(ctx context.Context, tx *sql.Tx, runID string) (mutationlog.DriftReport, error) {
	if _, err := uuid.Parse(runID); err != nil {
		return mutationlog.DriftReport{}, fmt.Errorf("invalid run identity: %w", err)
	}
	var found string
	if err := tx.QueryRowContext(ctx, `SELECT CAST(run_id AS TEXT) FROM runs WHERE run_id=$1`, runID).Scan(&found); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return mutationlog.DriftReport{}, &runlifecycle.RunNotFoundError{RunID: runID}
		}
		return mutationlog.DriftReport{}, err
	}
	var revision int64
	err := tx.QueryRowContext(ctx, `SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1`, runID).Scan(&revision)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return mutationlog.DriftReport{}, err
	}
	physical, err := readPhysicalRunMutations(ctx, tx, runID)
	if err != nil {
		return mutationlog.DriftReport{}, err
	}
	if err := validateRunMutationCoordinates(ctx, tx, runID, revision); err != nil {
		return mutationlog.DriftReport{}, err
	}
	snapshot := &runForkRevisionSnapshot{RunID: runID, Revision: revision}
	if revision > 0 {
		snapshot, err = loadRunForkRevisionSnapshotScope(ctx, tx, runID, revision, true)
		if err != nil {
			return mutationlog.DriftReport{}, err
		}
	}
	for i, fact := range snapshot.EntityMutations {
		row, ok := physical[fact.MutationID]
		if !ok {
			return mutationlog.DriftReport{}, runHistoryError(runID, fact.EntityID, fact.MutationID, "physical_mutation_missing", "ordering fact has no physical mutation")
		}
		if row.EntityID != fact.EntityID || row.Domain != fact.Domain || row.Path != fact.Path {
			return mutationlog.DriftReport{}, runHistoryError(runID, row.EntityID, row.MutationID, "invalid_mutation_order", "physical mutation disagrees with ordering identity/domain/path")
		}
		// Only the existing ledger coordinates/order are reused. Fold operands
		// always come from the physical log, including changed/null values.
		row.runForkRevisionedFact = fact.runForkRevisionedFact
		snapshot.EntityMutations[i] = row
		delete(physical, fact.MutationID)
	}
	for _, row := range physical {
		return mutationlog.DriftReport{}, runHistoryError(runID, row.EntityID, row.MutationID, "mutation_order_missing", "physical mutation has no committed ordering fact")
	}
	entities, err := loadRunForkEntityStates(snapshot)
	if err != nil {
		return mutationlog.DriftReport{}, runHistoryError(runID, "", "", "invalid_mutation_history", err.Error())
	}
	folded := make(map[string]mutationlog.EntityStateProjection, len(entities))
	for _, entity := range entities {
		folded[entity.EntityID] = mutationlog.EntityStateProjection{CurrentState: entity.CurrentState, Fields: entity.Fields, Bookkeeping: entity.Bookkeeping, Gates: entity.Gates, Accumulator: entity.Accumulator}
	}
	stored, err := readPhysicalRunState(ctx, tx, runID)
	if err != nil {
		return mutationlog.DriftReport{}, err
	}
	return mutationlog.CompareEntityStateProjections(runID, folded, stored)
}

func runHistoryError(run, entity, mutation, code, reason string) error {
	return &mutationlog.HistoryError{RunID: run, EntityID: entity, MutationID: mutation, Code: code, Reason: reason}
}

func readPhysicalRunMutations(ctx context.Context, tx *sql.Tx, runID string) (_ map[string]runForkRevisionEntityMutation, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(mutation_id AS TEXT),CAST(entity_id AS TEXT),domain,path,new_value,created_at FROM entity_mutations WHERE run_id=$1`, runID)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	out := map[string]runForkRevisionEntityMutation{}
	for rows.Next() {
		var row runForkRevisionEntityMutation
		var raw sql.NullString
		var at any
		if err := rows.Scan(&row.MutationID, &row.EntityID, &row.Domain, &row.Path, &raw, &at); err != nil {
			return nil, err
		}
		if _, duplicate := out[row.MutationID]; duplicate {
			return nil, runHistoryError(runID, row.EntityID, row.MutationID, "duplicate_mutation", "duplicate physical identity")
		}
		var err error
		row.CreatedAt, _, err = sqliteTimeValue(at)
		if err != nil {
			return nil, runHistoryError(runID, row.EntityID, row.MutationID, "invalid_mutation_history", err.Error())
		}
		if !raw.Valid {
			row.NewValue = json.RawMessage("null")
		} else {
			row.NewValue = json.RawMessage(raw.String)
			if !json.Valid(row.NewValue) {
				return nil, runHistoryError(runID, row.EntityID, row.MutationID, "invalid_mutation_history", "physical new_value must be valid JSON or SQL NULL")
			}
		}
		out[row.MutationID] = row
	}
	return out, rows.Err()
}

func validateRunMutationCoordinates(ctx context.Context, tx *sql.Tx, runID string, head int64) (err error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(f.run_id AS TEXT),f.fact_key,f.revision,f.present,r.revision
		FROM run_fork_fact_revisions f LEFT JOIN run_fork_revisions r ON r.run_id=f.run_id AND r.revision=f.revision
		WHERE f.family='entity_mutations' AND (f.run_id=$1 OR f.fact_key IN (SELECT CAST(mutation_id AS TEXT) FROM entity_mutations WHERE run_id=$1))`, runID)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	seen := map[string]struct{}{}
	for rows.Next() {
		var owner, key string
		var revision int64
		var present bool
		var committed sql.NullInt64
		if err := rows.Scan(&owner, &key, &revision, &present, &committed); err != nil {
			return err
		}
		if owner != runID || !committed.Valid || revision <= 0 || revision > head || !present {
			return runHistoryError(runID, "", key, "invalid_mutation_order", fmt.Sprintf("owner=%s revision=%d committed=%t present=%t head=%d", owner, revision, committed.Valid, present, head))
		}
		if _, duplicate := seen[key]; duplicate {
			return runHistoryError(runID, "", key, "mutation_order_duplicate", "one physical mutation has multiple ordering coordinates")
		}
		seen[key] = struct{}{}
	}
	return rows.Err()
}

func readPhysicalRunState(ctx context.Context, tx *sql.Tx, runID string) (_ map[string]mutationlog.EntityStateProjection, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(entity_id AS TEXT),COALESCE(current_state,''),fields,bookkeeping,gates,accumulator FROM entity_state WHERE run_id=$1`, runID)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	out := map[string]mutationlog.EntityStateProjection{}
	for rows.Next() {
		var entity string
		var state mutationlog.EntityStateProjection
		var fields, bookkeeping, gates, accumulator []byte
		if err := rows.Scan(&entity, &state.CurrentState, &fields, &bookkeeping, &gates, &accumulator); err != nil {
			return nil, err
		}
		for _, bucket := range []struct {
			raw    []byte
			target *map[string]any
		}{{fields, &state.Fields}, {bookkeeping, &state.Bookkeeping}, {gates, &state.Gates}, {accumulator, &state.Accumulator}} {
			value, err := decodeRunForkMutationValue(bucket.raw)
			if err != nil {
				return nil, runHistoryError(runID, entity, "", "invalid_entity_projection", err.Error())
			}
			object, ok := value.(map[string]any)
			if !ok {
				return nil, runHistoryError(runID, entity, "", "invalid_entity_projection", "domain bucket must be an object")
			}
			*bucket.target = object
		}
		if _, duplicate := out[entity]; duplicate {
			return nil, runHistoryError(runID, entity, "", "duplicate_entity_projection", "multiple state rows for one entity")
		}
		out[entity] = state
	}
	return out, rows.Err()
}
