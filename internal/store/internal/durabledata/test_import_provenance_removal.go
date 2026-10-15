package durabledata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/google/uuid"
)

type ImportProvenanceRemoval struct {
	source   string
	original importProvenanceStorage
}

type importProvenanceStorage struct {
	Version, Kind, Producer, Actor string
	Sequence                       int64
	Wire                           []byte
	Committed                      any
}

func CaptureImportProvenanceRemovalTx(ctx context.Context, tx *sql.Tx, source string) (*ImportProvenanceRemoval, error) {
	if uuid.Validate(source) != nil {
		return nil, errors.New("import provenance fault requires its original source invocation")
	}
	f := &ImportProvenanceRemoval{source: source}
	var err error
	f.original, err = f.readTx(ctx, tx, false)
	return f, err
}

func (f *ImportProvenanceRemoval) readTx(ctx context.Context, tx *sql.Tx, lock bool) (importProvenanceStorage, error) {
	var out importProvenanceStorage
	query := `SELECT version_id,provenance_sequence,producer_kind,CAST(producer_id AS TEXT),actor,provenance_json,committed_at
		FROM resource_version_provenance WHERE producer_kind='import' AND producer_id=$1`
	if lock {
		query += ` FOR UPDATE`
	}
	rows, err := tx.QueryContext(ctx, query, f.source)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return out, err
		}
		return out, sql.ErrNoRows
	}
	if err := rows.Scan(&out.Version, &out.Sequence, &out.Kind, &out.Producer, &out.Actor, &out.Wire, &out.Committed); err != nil {
		return out, err
	}
	if rows.Next() {
		return out, errors.New("source invocation has multiple import provenance rows")
	}
	return out, rows.Err()
}

func (f *ImportProvenanceRemoval) installTx(ctx context.Context, tx *sql.Tx, postgres bool) error {
	current, err := f.readTx(ctx, tx, postgres)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, f.original) {
		return errors.New("import provenance changed before exact removal")
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM resource_version_provenance WHERE producer_kind='import' AND producer_id=$1`, f.source)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n != 1 {
		err = fmt.Errorf("import provenance removal affected %d rows, want 1", n)
	}
	return err
}

func (f *ImportProvenanceRemoval) restoreTx(ctx context.Context, tx *sql.Tx, postgres bool) error {
	current, err := f.readTx(ctx, tx, postgres)
	if err == nil {
		if reflect.DeepEqual(current, f.original) {
			return nil
		}
		return errors.New("import provenance replacement refuses historical cleanup")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	r := f.original
	_, err = tx.ExecContext(ctx, `INSERT INTO resource_version_provenance
		(version_id,provenance_sequence,producer_kind,producer_id,actor,provenance_json,committed_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`, r.Version, r.Sequence, r.Kind, r.Producer, r.Actor, r.Wire, r.Committed)
	return err
}
