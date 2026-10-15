package durabledata

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
)

// The public pagination witness has one finite shape: MaxPublicPageItems+5
// existing native runs and exact explicit pins with the original fixture clocks.
func MaterializePruneWitnessPinsTx(ctx context.Context, o *Owner, tx *sql.Tx, version runtimedata.VersionID, ref runtimedata.DeclarationRef, schema runtimedata.SchemaDigest) error {
	if o == nil {
		return fmt.Errorf("prune pin witness requires the selected durable data owner")
	}
	if err := version.Validate(); err != nil {
		return err
	}
	if err := ref.Validate(); err != nil {
		return err
	}
	stored, found, err := o.loadStoredVersion(ctx, tx, ref, version)
	if err != nil {
		return err
	}
	if !found || stored.PrunedAt != nil || stored.Manifest.SchemaDigest != schema {
		return fmt.Errorf("prune pin witness requires the exact materialized version and schema")
	}
	for index := range runtimedata.MaxPublicPageItems + 5 {
		run := fmt.Sprintf("00000000-0000-4000-8000-%012d", index+1)
		bundle, err := o.loadRunBundleHashTx(ctx, tx, run)
		if err != nil {
			return err
		}
		declaration, err := o.loadCatalogDeclaration(ctx, tx, bundle, ref)
		if err != nil {
			return err
		}
		if declaration.SchemaDigest != schema {
			return fmt.Errorf("prune witness run %s has a foreign declaration schema", run)
		}
		if pins, err := o.loadRunPinsTx(ctx, tx, run); err != nil || len(pins) != 0 {
			return fmt.Errorf("prune witness run %s already owns pins: %v", run, err)
		}
		if err := o.insertPinTx(ctx, tx, runtimedata.Pin{
			RunID: run, Declaration: ref, SchemaDigest: schema, VersionID: version, Selection: "explicit",
		}, time.Unix(int64(index+1), 0).UTC()); err != nil {
			return err
		}
	}
	return nil
}
