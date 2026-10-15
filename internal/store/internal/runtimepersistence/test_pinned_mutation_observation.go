package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationlog"
	"github.com/google/uuid"
)

func ReadPinnedAuthoredMutationStorageForTest(ctx context.Context, selected any, source fanoutobligation.SourceRef) (json.RawMessage, error) {
	if err := validatePinnedAuthoredMutationSourceForTest(source); err != nil {
		return nil, err
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return nil, err
	}
	var value []byte
	read := func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT CAST(new_value AS TEXT) FROM entity_mutations
			WHERE mutation_id=$1 AND run_id=$2 AND entity_id=$3 AND domain='authored_field' AND path=$4`,
			source.MutationID, source.RunID, source.EntityID, source.Field).Scan(&value)
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
	return append(json.RawMessage(nil), value...), nil
}

func validatePinnedAuthoredMutationSourceForTest(source fanoutobligation.SourceRef) error {
	if source.Kind != fanoutobligation.SourceEntityField {
		return fmt.Errorf("pinned mutation evidence requires an entity-field source")
	}
	if err := source.Validate(true); err != nil {
		return err
	}
	for _, raw := range []string{source.RunID, source.EntityID, source.MutationID} {
		id, err := uuid.Parse(raw)
		if err != nil || id == uuid.Nil || id.String() != raw {
			return fmt.Errorf("pinned mutation evidence requires exact canonical identities")
		}
	}
	return nil
}

func SetPinnedAuthoredMutationWireForTest(ctx context.Context, selected any, source fanoutobligation.SourceRef, raw []byte) (int64, error) {
	if err := validatePinnedAuthoredMutationSourceForTest(source); err != nil {
		return 0, err
	}
	var changed int64
	err := runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		changed, err = mutationlog.SetPinnedAuthoredMutationWireForTest(ctx, tx, source, raw)
		return err
	})
	if err != nil {
		return 0, err
	}
	return changed, nil
}
