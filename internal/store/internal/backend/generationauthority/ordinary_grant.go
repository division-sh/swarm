package generationauthority

import (
	"context"
	"database/sql"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

// RequireAdmittedOrdinaryGrantTx composes the existing process, grant and
// source-set owners. A retained handoff can never promote a selected grant.
func RequireAdmittedOrdinaryGrantTx(ctx context.Context, tx *sql.Tx, evidence startupownership.GrantEvidence, bundleHash string, sqlite bool) error {
	if err := evidence.Validate(); err != nil {
		return err
	}
	if evidence.SelectedFork != nil || evidence.State != startupownership.GrantAdmitted || evidence.BundleHash != bundleHash {
		return errors.New("live handoff requires its admitted ordinary source grant")
	}
	if err := FenceMutation(ctx, tx, sqlite); err != nil {
		return err
	}
	current, err := ProcessCoordinatesCurrentTx(ctx, tx, evidence.ProcessCoordinates(), sqlite)
	if err != nil {
		return err
	}
	if !current {
		return errors.New("live handoff ordinary process is no longer current")
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT snapshot FROM runtime_generation_grants WHERE grant_id=$1 ORDER BY state_version DESC LIMIT 1`, evidence.GrantID).Scan(&raw); err != nil {
		return err
	}
	var actual startupownership.GrantEvidence
	if err := canonicaljson.DecodeInto(raw, &actual); err != nil {
		return err
	}
	actualRaw, err := canonicaljson.Bytes(actual)
	if err != nil {
		return err
	}
	expectedRaw, err := canonicaljson.Bytes(evidence)
	if err != nil {
		return err
	}
	if string(actualRaw) != string(expectedRaw) {
		return errors.New("live handoff ordinary grant is no longer current")
	}
	plan, present, err := ReadSourceSetTx(ctx, tx, sqlite, true)
	if err != nil {
		return err
	}
	root, rootPresent, err := correlation.RootAdmissionFromContext(ctx)
	if err != nil || !rootPresent || root.BundleHash() != bundleHash {
		return errors.Join(err, errors.New("live handoff ordinary grant requires its exact admitted root"))
	}
	coordinate, err := agenttopology.NewSourceCoordinate(root)
	if err != nil {
		return err
	}
	if !present || plan.Revision != evidence.SourceSetRevision || !SourceSetContains(plan, coordinate) {
		return errors.New("live handoff ordinary source set is no longer current")
	}
	return nil
}
