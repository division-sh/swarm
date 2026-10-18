package startupownership

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
)

// FreezeLogoutProcessTx joins the original SDK owner's grant to the current
// native stamp. The caller's binding is attribution, never permission itself.
func FreezeLogoutProcessTx(ctx context.Context, tx *sql.Tx, binding processbinding.Binding, sqlite bool) (processbinding.LeaseStamp, error) {
	var zero processbinding.LeaseStamp
	if binding.Validate() != nil {
		return zero, fmt.Errorf("logout requires its original generation binding")
	}
	current, err := generationauthority.ReadProcessGenerationTx(ctx, tx, sqlite)
	if err != nil {
		return zero, err
	}
	if current.Validate() != nil || current.LeaseID != binding.ProcessAuthorityID || current.OwnerID != binding.ProcessOwnerID ||
		current.BootID != binding.ProcessBootID || current.RuntimeInstanceID != binding.RuntimeInstanceID {
		return zero, fmt.Errorf("logout generation binding is not current")
	}
	grant, err := admitLogoutGrantTx(ctx, tx, binding, current)
	if err != nil {
		return zero, err
	}
	plan, found, err := loadSourceSetTx(ctx, tx, sqlite)
	if err != nil {
		return zero, err
	}
	if !found || plan.Revision != grant.SourceSetRevision || !sourceSetContains(plan, agenttopology.SourceCoordinate{BundleHash: binding.BundleHash}) {
		return zero, fmt.Errorf("logout original generation source set is not current")
	}
	return current, ctx.Err()
}

func admitLogoutGrantTx(ctx context.Context, tx *sql.Tx, binding processbinding.Binding, current processbinding.LeaseStamp) (runtimeownership.GrantEvidence, error) {
	var grant runtimeownership.GrantEvidence
	var raw []byte
	var id, bundle, runtime, state, authorityID, ownerID string
	var generation, version uint64
	var sourceSet, selectedBinding, selectedRun, selectedExecution sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT snapshot,grant_id,bundle_hash,runtime_instance_id,runtime_generation,state_version,state,
		process_authority_id,process_owner_id,source_set_revision,selected_binding_id,selected_fork_run_id,selected_execution_id
		FROM runtime_generation_grants WHERE grant_id=$1 ORDER BY state_version DESC LIMIT 1`, binding.GenerationGrantID).
		Scan(&raw, &id, &bundle, &runtime, &generation, &version, &state, &authorityID, &ownerID, &sourceSet, &selectedBinding, &selectedRun, &selectedExecution)
	if err != nil {
		return grant, err
	}
	if err := canonicaljson.DecodeInto(raw, &grant); err != nil {
		return grant, err
	}
	if grant.Validate() != nil || grant.SelectedFork != nil || grant.State != runtimeownership.GrantAdmitted ||
		grant.GrantID != binding.GenerationGrantID || grant.BundleHash != binding.BundleHash ||
		grant.RuntimeInstanceID != binding.RuntimeInstanceID || grant.RuntimeGeneration != binding.RuntimeGeneration ||
		grant.ProcessCoordinates() != current.Coordinates() || id != grant.GrantID || bundle != grant.BundleHash ||
		runtime != grant.RuntimeInstanceID || generation != grant.RuntimeGeneration || version != grant.StateVersion || state != string(grant.State) ||
		authorityID != grant.ProcessAuthorityID || ownerID != grant.ProcessOwnerID || !sourceSet.Valid || sourceSet.String != grant.SourceSetRevision ||
		selectedBinding.Valid || selectedRun.Valid || selectedExecution.Valid {
		return grant, fmt.Errorf("logout generation grant contradicts its original process binding")
	}
	return grant, nil
}
