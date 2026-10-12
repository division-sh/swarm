package budgetspendstore

import (
	"context"
	"database/sql"
)

type SelectedCompletionModelAliasStorage struct{ Total, Canonical int }

func ReadSelectedCompletionModelAliasStorageTx(ctx context.Context, tx *sql.Tx, executionID, adapter, modelAlias string) (SelectedCompletionModelAliasStorage, error) {
	var out SelectedCompletionModelAliasStorage
	err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*), COUNT(*) FILTER (WHERE l.model_alias = $3)
		FROM spend_ledger l
		JOIN runtime_external_effect_attempts a ON a.attempt_id = l.external_effect_attempt_id
		JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id
		WHERE o.selected_execution_id = $1
		  AND a.adapter = $2
	`, executionID, adapter, modelAlias).Scan(&out.Total, &out.Canonical)
	return out, err
}
