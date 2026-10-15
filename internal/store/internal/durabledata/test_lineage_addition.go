package durabledata

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	"github.com/google/uuid"
)

type ProvenanceReadArmAddition struct{ row importProvenanceStorage }

// The read-arm fixture covers the two additional supported provenance variants;
// it does not grant execution authority to either referenced run identity.
func CaptureProvenanceReadArmTx(ctx context.Context, tx *sql.Tx, version, kind, producer string, committed time.Time) (*ProvenanceReadArmAddition, error) {
	sequence := uint64(0)
	switch kind {
	case "normal_run":
		sequence = 2
	case "fork_candidate_promotion":
		sequence = 3
	default:
		return nil, fmt.Errorf("unknown provenance read arm %q", kind)
	}
	ref, err := runtimedata.NewProvenanceRef(kind, producer)
	if err != nil {
		return nil, err
	}
	return captureProvenanceAddition(ctx, tx, runtimedata.Provenance{
		Sequence: sequence, VersionID: runtimedata.VersionID(version), ProducerRef: ref, Actor: "operator", CommittedAt: committed,
	})
}

func CaptureNonMutatingProvenanceTx(ctx context.Context, tx *sql.Tx, version, source string) (*ProvenanceReadArmAddition, error) {
	if err := runtimedata.VersionID(version).Validate(); err != nil {
		return nil, err
	}
	if uuid.Validate(source) != nil {
		return nil, errors.New("non-mutating provenance fault requires its exact source invocation")
	}
	var sequence uint64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(provenance_sequence),0)+1 FROM resource_version_provenance WHERE version_id=$1`, version).Scan(&sequence); err != nil {
		return nil, err
	}
	raw, err := ReadSourceResultWireTx(ctx, tx, source)
	if err != nil {
		return nil, err
	}
	var result runtimedata.SourceOperationResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, err
	}
	if result.Outcome == "accepted" {
		return nil, errors.New("non-mutating provenance fault requires a rejected source receipt")
	}
	raw, err = ReadSourceRequestWireTx(ctx, tx, source)
	if err != nil {
		return nil, err
	}
	var command runtimedata.SourceCommand
	if err := json.Unmarshal(raw, &command); err != nil {
		return nil, err
	}
	return captureProvenanceAddition(ctx, tx, runtimedata.Provenance{
		Sequence: sequence, VersionID: runtimedata.VersionID(version),
		ProducerRef: runtimedata.ProvenanceRef{Kind: "import", SourceInvocationID: source},
		Actor:       command.Actor, CommittedAt: result.CompletedAt,
	})
}

func captureProvenanceAddition(ctx context.Context, tx *sql.Tx, p runtimedata.Provenance) (*ProvenanceReadArmAddition, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	producer, err := p.ProducerRef.ProducerID()
	if err != nil {
		return nil, err
	}
	f := &ProvenanceReadArmAddition{row: importProvenanceStorage{
		Version: string(p.VersionID), Sequence: int64(p.Sequence), Kind: p.ProducerRef.Kind,
		Producer: producer, Actor: p.Actor, Wire: raw, Committed: p.CommittedAt,
	}}
	if _, err := f.readTx(ctx, tx, false); !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("provenance addition requires an absent identity: %v", err)
	}
	return f, nil
}

func (f *ProvenanceReadArmAddition) readTx(ctx context.Context, tx *sql.Tx, postgres bool) (importProvenanceStorage, error) {
	var row importProvenanceStorage
	query := `SELECT version_id,provenance_sequence,producer_kind,CAST(producer_id AS TEXT),actor,provenance_json,committed_at
		FROM resource_version_provenance WHERE version_id=$1 AND provenance_sequence=$2`
	if postgres {
		query += ` FOR UPDATE`
	}
	err := tx.QueryRowContext(ctx, query, f.row.Version, f.row.Sequence).Scan(&row.Version, &row.Sequence, &row.Kind, &row.Producer, &row.Actor, &row.Wire, &row.Committed)
	return row, err
}

func (f *ProvenanceReadArmAddition) installTx(ctx context.Context, tx *sql.Tx, postgres bool) error {
	if _, err := f.readTx(ctx, tx, postgres); !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("provenance addition identity is no longer absent: %v", err)
	}
	r := f.row
	if _, err := tx.ExecContext(ctx, `INSERT INTO resource_version_provenance
		(version_id,provenance_sequence,producer_kind,producer_id,actor,provenance_json,committed_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`, r.Version, r.Sequence, r.Kind, r.Producer, r.Actor, r.Wire, r.Committed); err != nil {
		return err
	}
	// Capture the native physical clock representation, never a reconstructed one.
	var err error
	f.row, err = f.readTx(ctx, tx, false)
	return err
}

func (f *ProvenanceReadArmAddition) restoreTx(ctx context.Context, tx *sql.Tx, postgres bool) error {
	row, err := f.readTx(ctx, tx, postgres)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(row, f.row) {
		return errors.New("provenance replacement refuses exact fixture cleanup")
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM resource_version_provenance WHERE version_id=$1 AND provenance_sequence=$2`, f.row.Version, f.row.Sequence)
	return err
}
