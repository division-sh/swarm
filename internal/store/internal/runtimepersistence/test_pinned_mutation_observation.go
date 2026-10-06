package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

func ReadPinnedAuthoredMutationStorageForTest(ctx context.Context, selected any, source fanoutobligation.SourceRef) (json.RawMessage, error) {
	if source.Kind != fanoutobligation.SourceEntityField {
		return nil, fmt.Errorf("pinned mutation evidence requires an entity-field source")
	}
	if err := source.Validate(true); err != nil {
		return nil, err
	}
	for _, raw := range []string{source.RunID, source.EntityID, source.MutationID} {
		id, err := uuid.Parse(raw)
		if err != nil || id == uuid.Nil || id.String() != raw {
			return nil, fmt.Errorf("pinned mutation evidence requires exact canonical identities")
		}
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
