package mutationlog

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

func SetPinnedAuthoredMutationWireForTest(ctx context.Context, tx *sql.Tx, source fanoutobligation.SourceRef, raw []byte) (int64, error) {
	result, err := tx.ExecContext(ctx, `UPDATE entity_mutations SET new_value=$1
		WHERE mutation_id=$2 AND run_id=$3 AND entity_id=$4 AND domain='authored_field' AND path=$5`,
		string(raw), source.MutationID, source.RunID, source.EntityID, source.Field)
	if err != nil {
		return 0, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if changed != 1 {
		return 0, fmt.Errorf("pinned authored mutation fault requires exactly one existing row, got %d", changed)
	}
	return changed, nil
}
