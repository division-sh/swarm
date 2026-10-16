package startupownership

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

// ReadActivityExecutionGrantTx resolves immutable attribution, not current
// execution permission. Its caller must hold the original writer's fences.
func ReadActivityExecutionGrantTx(ctx context.Context, tx *sql.Tx, grantID string, stateVersion uint64) (runtimeownership.GrantEvidence, error) {
	var absent runtimeownership.GrantEvidence
	id, err := uuid.Parse(grantID)
	if tx == nil || err != nil || id == uuid.Nil || id.String() != grantID || stateVersion == 0 || stateVersion > math.MaxInt64 {
		return absent, errors.New("activity grant history requires its native transaction and exact grant/version")
	}
	var row struct {
		grantID, processAuthorityID, processOwnerID, state, bundleHash, runtimeInstanceID string
		stateVersion, runtimeGeneration                                                   uint64
		sourceSetRevision, selectedBindingID, selectedRunID, selectedExecutionID          sql.NullString
		snapshot                                                                          []byte
	}
	err = tx.QueryRowContext(ctx, `SELECT CAST(grant_id AS TEXT) AS grant_id,
		CAST(process_authority_id AS TEXT) AS process_authority_id, process_owner_id,
		state_version, state, bundle_hash, CAST(runtime_instance_id AS TEXT) AS runtime_instance_id,
		runtime_generation, source_set_revision, CAST(selected_binding_id AS TEXT) AS selected_binding_id,
		CAST(selected_fork_run_id AS TEXT) AS selected_fork_run_id,
		CAST(selected_execution_id AS TEXT) AS selected_execution_id, snapshot
		FROM runtime_generation_grants WHERE grant_id=$1 AND state_version=$2`, grantID, stateVersion).Scan(
		&row.grantID, &row.processAuthorityID, &row.processOwnerID, &row.stateVersion, &row.state, &row.bundleHash,
		&row.runtimeInstanceID, &row.runtimeGeneration, &row.sourceSetRevision,
		&row.selectedBindingID, &row.selectedRunID, &row.selectedExecutionID, &row.snapshot,
	)
	if err != nil {
		return absent, fmt.Errorf("read exact activity grant history: %w", err)
	}
	var grant runtimeownership.GrantEvidence
	if err := canonicaljson.DecodeInto(row.snapshot, &grant); err != nil {
		return absent, fmt.Errorf("decode exact activity grant history: %w", err)
	}
	if err := grant.Validate(); err != nil {
		return absent, err
	}
	if err := grant.ProcessCoordinates().Validate(); err != nil {
		return absent, err
	}
	if grant.GrantID != grantID || row.grantID != grantID || grant.StateVersion != stateVersion || row.stateVersion != stateVersion ||
		grant.ProcessAuthorityID != row.processAuthorityID || grant.ProcessOwnerID != row.processOwnerID ||
		string(grant.State) != row.state || grant.BundleHash != row.bundleHash ||
		grant.RuntimeInstanceID != row.runtimeInstanceID || grant.RuntimeGeneration != row.runtimeGeneration {
		return absent, errors.New("activity grant snapshot differs from its exact durable projections")
	}
	if grant.SelectedFork == nil {
		if !row.sourceSetRevision.Valid || grant.SourceSetRevision != row.sourceSetRevision.String ||
			row.selectedBindingID.Valid || row.selectedRunID.Valid || row.selectedExecutionID.Valid {
			return absent, errors.New("normal activity grant history has contradictory source or selected projections")
		}
	} else if row.sourceSetRevision.Valid || !row.selectedBindingID.Valid || !row.selectedRunID.Valid || !row.selectedExecutionID.Valid ||
		grant.SelectedFork.BindingID != row.selectedBindingID.String || grant.SelectedFork.ForkRunID != row.selectedRunID.String ||
		grant.SelectedFork.ExecutionID != row.selectedExecutionID.String {
		return absent, errors.New("selected activity grant history differs from its exact execution projections")
	}
	return grant, nil
}
