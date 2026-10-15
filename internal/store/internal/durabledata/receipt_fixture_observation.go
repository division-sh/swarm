package durabledata

import (
	"context"
	"database/sql"
)

func ReadRunCreationRequestHashStorageTx(ctx context.Context, tx *sql.Tx, run string) (string, error) {
	var out string
	err := tx.QueryRowContext(ctx, `SELECT request_hash FROM resource_run_creation_operations WHERE run_id=$1`, run).Scan(&out)
	return out, err
}

func ReadSourceRequestWireTx(ctx context.Context, tx *sql.Tx, id string) ([]byte, error) {
	var out []byte
	err := tx.QueryRowContext(ctx, `SELECT request_json FROM resource_source_invocations WHERE source_invocation_id=$1`, id).Scan(&out)
	return out, err
}

func ReadSourceResultWireTx(ctx context.Context, tx *sql.Tx, id string) ([]byte, error) {
	var out []byte
	err := tx.QueryRowContext(ctx, `SELECT result_json FROM resource_source_invocations WHERE source_invocation_id=$1`, id).Scan(&out)
	return out, err
}

type SourceEvaluationResultWire struct{ Evaluation, Result []byte }

func ReadSourceEvaluationResultWireTx(ctx context.Context, tx *sql.Tx, id string) (SourceEvaluationResultWire, error) {
	var out SourceEvaluationResultWire
	err := tx.QueryRowContext(ctx, `SELECT evaluation_json,result_json FROM resource_source_invocations WHERE source_invocation_id=$1`, id).Scan(&out.Evaluation, &out.Result)
	return out, err
}

func ReadImportProvenanceWireTx(ctx context.Context, tx *sql.Tx, id string) ([]byte, error) {
	var out []byte
	err := tx.QueryRowContext(ctx, `SELECT provenance_json FROM resource_version_provenance WHERE producer_kind='import' AND producer_id=$1`, id).Scan(&out)
	return out, err
}

type RunCreationRequestEvidenceWire struct{ Request, Evidence []byte }

func ReadRunCreationRequestEvidenceWireTx(ctx context.Context, tx *sql.Tx, run string) (RunCreationRequestEvidenceWire, error) {
	var out RunCreationRequestEvidenceWire
	err := tx.QueryRowContext(ctx, `SELECT request_json,evidence_json FROM resource_run_creation_operations WHERE run_id=$1`, run).Scan(&out.Request, &out.Evidence)
	return out, err
}

func ReadRunCreationChildContextWireTx(ctx context.Context, tx *sql.Tx, run, source string) ([]byte, error) {
	var out []byte
	err := tx.QueryRowContext(ctx, `SELECT context_json FROM resource_run_creation_child_evaluations WHERE parent_run_id=$1 AND source_invocation_id=$2`, run, source).Scan(&out)
	return out, err
}

func ReadFirstResourceProvenanceWireTx(ctx context.Context, tx *sql.Tx, version string) ([]byte, error) {
	var out []byte
	err := tx.QueryRowContext(ctx, `SELECT provenance_json FROM resource_version_provenance WHERE version_id=$1 AND provenance_sequence=1`, version).Scan(&out)
	return out, err
}

func CountPruneInvocationStorageTx(ctx context.Context, tx *sql.Tx, id string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM resource_prune_invocations WHERE prune_invocation_id=$1`, id).Scan(&out)
	return out, err
}
