package generationauthority

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
)

// RequireCurrentRuntimeMutationTx consumes original owner evidence under the
// same order as generation advancement. Run/source/selected authorization is
// additional domain admission and is not replaced by this common boundary.
func RequireCurrentRuntimeMutationTx(ctx context.Context, tx *sql.Tx, original runtimeownership.MutationAuthority, sqlite bool) error {
	if err := original.Validate(); err != nil {
		return err
	}
	if original.HasRuntimeGrant() {
		return requireCurrentRuntimeGrantTx(ctx, tx, original, sqlite)
	}
	current, err := ProcessGenerationCurrent(ctx, tx, original.ProcessAuthority().LeaseStamp(), sqlite)
	if err != nil {
		return err
	}
	if !current {
		return errors.New("runtime mutation process generation is no longer current")
	}
	return nil
}

func requireCurrentRuntimeGrantTx(ctx context.Context, tx *sql.Tx, original runtimeownership.MutationAuthority, sqlite bool) error {
	expected, process := original.GrantEvidence(), original.ProcessAuthority().LeaseStamp()
	if tx == nil {
		return errors.New("current generation grant requires its native transaction")
	}
	if grantAdmittedInNativeTransaction(ctx, tx, expected, sqlite) && processAdmittedInNativeTransaction(ctx, tx, process, sqlite) {
		return nil
	}
	if err := FenceMutation(ctx, tx, sqlite); err != nil {
		return err
	}
	// The grant observation shares the locked process row's snapshot, after the
	// order fence; it never supplies authority for another native transaction.
	query := `SELECT generation, CAST(lease_id AS TEXT), owner_id, CAST(boot_id AS TEXT),
		CAST(runtime_instance_id AS TEXT), backend,
		(SELECT snapshot FROM runtime_generation_grants WHERE grant_id=$1 ORDER BY state_version DESC LIMIT 1)
		FROM runtime_process_generation WHERE singleton_id=1`
	if !sqlite {
		query += " FOR UPDATE"
	}
	var generation int64
	var lease, owner, boot, runtime, backend sql.NullString
	var raw []byte
	err := tx.QueryRowContext(ctx, query, expected.GrantID).Scan(&generation, &lease, &owner, &boot, &runtime, &backend, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrProcessGenerationMissing
	}
	if err != nil {
		return fmt.Errorf("read runtime mutation generation: %w", err)
	}
	if raw == nil {
		return fmt.Errorf("load run execution generation grant: %w", sql.ErrNoRows)
	}
	if err := validateCurrentGrantSnapshot(raw, expected); err != nil {
		return err
	}
	current, err := decodeProcessGeneration(ctx, generation, lease, owner, boot, runtime, backend, sqlite)
	if err != nil {
		return err
	}
	if current != process {
		return errors.New("runtime mutation process generation is no longer current")
	}
	return nil
}

// RequireCurrentGrantTx reads after the established order fence, before domain
// locks. A snapshot that predates the wait must conflict rather than authorize
// its historical predecessor.
func RequireCurrentGrantTx(ctx context.Context, tx *sql.Tx, expected runtimeownership.GrantEvidence, sqlite bool) error {
	if tx == nil {
		return errors.New("current generation grant requires its native transaction")
	}
	if grantAdmittedInNativeTransaction(ctx, tx, expected, sqlite) {
		return nil
	}
	if err := FenceMutation(ctx, tx, sqlite); err != nil {
		return err
	}
	return ObserveCurrentGrantTx(ctx, tx, expected)
}

// ObserveCurrentGrantTx is a same-snapshot observation, not write admission.
func ObserveCurrentGrantTx(ctx context.Context, tx *sql.Tx, expected runtimeownership.GrantEvidence) error {
	if tx == nil {
		return errors.New("generation grant observation requires its transaction")
	}
	if err := expected.Validate(); err != nil {
		return err
	}
	if expected.State == runtimeownership.GrantRetired {
		return errors.New("runtime generation grant is retired")
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT snapshot FROM runtime_generation_grants WHERE grant_id=$1 ORDER BY state_version DESC LIMIT 1`, expected.GrantID).Scan(&raw); err != nil {
		return fmt.Errorf("load run execution generation grant: %w", err)
	}
	return validateCurrentGrantSnapshot(raw, expected)
}

func validateCurrentGrantSnapshot(raw []byte, expected runtimeownership.GrantEvidence) error {
	var current runtimeownership.GrantEvidence
	if err := canonicaljson.DecodeInto(raw, &current); err != nil {
		return err
	}
	if err := current.Validate(); err != nil {
		return err
	}
	actual, err := canonicaljson.Bytes(current)
	if err != nil {
		return err
	}
	want, err := canonicaljson.Bytes(expected)
	if err != nil {
		return err
	}
	if string(actual) != string(want) {
		return errors.New("run execution generation grant is no longer current")
	}
	return nil
}
