package durabledata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	"github.com/google/uuid"
)

// MaterializeForkPinsTx projects one complete exact target-bundle pin set from
// the source run plus explicit overrides. It inserts on fresh materialization
// and validates the immutable set on deterministic replay.
func MaterializeForkPinsTx(
	o *Owner,
	ctx context.Context,
	tx *sql.Tx,
	sourceRunID string,
	forkRunID string,
	targetBundleHash string,
	overrides []runtimedata.ExplicitPin,
	replay bool,
	now time.Time,
) ([]runtimedata.Pin, error) {
	if o == nil || tx == nil {
		return nil, fmt.Errorf("durable data owner and fork transaction are required")
	}
	for field, raw := range map[string]string{"source_run_id": sourceRunID, "fork_run_id": forkRunID} {
		parsed, err := uuid.Parse(strings.TrimSpace(raw))
		if err != nil || parsed == uuid.Nil || parsed.String() != raw {
			return nil, fmt.Errorf("%s must be one canonical non-zero UUID", field)
		}
	}
	canonicalOverrides, err := runtimedata.CanonicalExplicitPins(overrides)
	if err != nil {
		return nil, runtimedata.NewDomainError(runtimedata.CodePinConflict, "invalid fork pin overrides: %v", err)
	}

	sourceBundleHash, err := o.loadRunBundleHashTx(ctx, tx, sourceRunID)
	if err != nil {
		return nil, err
	}
	sourceDeclarations, err := o.loadBundleDeclarationsTx(ctx, tx, sourceBundleHash)
	if err != nil {
		return nil, err
	}
	sourcePins, err := o.loadRunPinsTx(ctx, tx, sourceRunID)
	if err != nil {
		return nil, err
	}
	if err := o.validateRunPinSetTx(ctx, tx, sourceRunID, sourceDeclarations, sourcePins); err != nil {
		return nil, err
	}
	sourceByDeclaration := make(map[string]runtimedata.Pin, len(sourcePins))
	for _, pin := range sourcePins {
		sourceByDeclaration[pin.Declaration.Key()] = pin
	}

	targetDeclarations, err := o.loadBundleDeclarationsTx(ctx, tx, strings.TrimSpace(targetBundleHash))
	if err != nil {
		return nil, err
	}
	targetByDeclaration := make(map[string]runtimedata.Declaration, len(targetDeclarations))
	for _, declaration := range targetDeclarations {
		targetByDeclaration[declaration.Ref.Key()] = declaration
	}
	overrideByDeclaration := make(map[string]runtimedata.ExplicitPin, len(canonicalOverrides))
	for _, override := range canonicalOverrides {
		key := override.Declaration.Key()
		if _, ok := targetByDeclaration[key]; !ok {
			return nil, runtimedata.NewDomainError(runtimedata.CodePinConflict, "fork override selects declaration %s outside exact target bundle %s", key, targetBundleHash)
		}
		overrideByDeclaration[key] = override
	}
	for key := range sourceByDeclaration {
		if _, ok := targetByDeclaration[key]; !ok {
			return nil, runtimedata.NewDomainError(runtimedata.CodePinConflict, "source pinned declaration %s is absent from exact target bundle %s", key, targetBundleHash)
		}
	}

	pins := make([]runtimedata.Pin, 0, len(sourcePins)+len(canonicalOverrides))
	for _, declaration := range targetDeclarations {
		key := declaration.Ref.Key()
		selection := ""
		var versionID runtimedata.VersionID
		if override, ok := overrideByDeclaration[key]; ok {
			selection = "fork_override"
			versionID = override.VersionID
		} else if inherited, ok := sourceByDeclaration[key]; ok {
			selection = "fork_inherited"
			versionID = inherited.VersionID
		} else {
			continue
		}
		version, found, err := o.loadStoredVersion(ctx, tx, declaration.Ref, versionID)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, runtimedata.NewDomainError(runtimedata.CodeVersionMissing, "fork pin version %s does not exist for declaration %s", versionID, key)
		}
		if version.PrunedAt != nil {
			return nil, runtimedata.NewDomainError(runtimedata.CodePayloadPruned, "fork pin version %s payload is pruned", versionID)
		}
		if version.Manifest.SchemaDigest != declaration.SchemaDigest {
			return nil, runtimedata.NewDomainErrorWithDetails(runtimedata.CodeSchemaMismatch, map[string]any{
				"declaration": key, "version_id": versionID, "version_schema_digest": version.Manifest.SchemaDigest,
				"bundle_schema_digest": declaration.SchemaDigest,
			}, "fork pin version %s schema does not match exact target bundle declaration %s", versionID, key)
		}
		pins = append(pins, runtimedata.Pin{
			RunID: forkRunID, RunState: "paused", Declaration: declaration.Ref,
			SchemaDigest: declaration.SchemaDigest, VersionID: versionID, Selection: selection,
		})
	}
	runtimedata.SortPins(pins)

	if replay {
		persisted, err := o.loadRunPinsTx(ctx, tx, forkRunID)
		if err != nil {
			return nil, err
		}
		if !samePinSet(persisted, pins) {
			return nil, runtimedata.NewDomainError(runtimedata.CodePinConflict, "fork %s persisted pin set conflicts with exact inheritance and overrides", forkRunID)
		}
		return persisted, nil
	}
	if existing, err := o.loadRunPinsTx(ctx, tx, forkRunID); err != nil {
		return nil, err
	} else if len(existing) != 0 {
		return nil, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "fresh fork %s already has resource pins", forkRunID)
	}
	for _, pin := range pins {
		if _, err := tx.ExecContext(ctx, o.query(`
			INSERT INTO resource_version_pins
			(run_id, flow_path, event_name, schema_digest, version_id, selection, pinned_at)
			VALUES (%s, %s, %s, %s, %s, %s, %s)
		`, 7), pin.RunID, pin.Declaration.FlowPath, pin.Declaration.EventName, pin.SchemaDigest, pin.VersionID, pin.Selection, now.UTC()); err != nil {
			return nil, fmt.Errorf("insert fork resource pin: %w", err)
		}
	}
	return pins, nil
}

func (o *Owner) loadRunBundleHashTx(ctx context.Context, tx *sql.Tx, runID string) (string, error) {
	var bundleHash sql.NullString
	if err := tx.QueryRowContext(ctx, o.query(`SELECT bundle_hash FROM runs WHERE run_id = %s`, 1), runID).Scan(&bundleHash); errors.Is(err, sql.ErrNoRows) {
		return "", runtimedata.NewDomainError(runtimedata.CodeDependencyMissing, "source run %s does not exist", runID)
	} else if err != nil {
		return "", err
	}
	if !bundleHash.Valid || strings.TrimSpace(bundleHash.String) == "" {
		return "", runtimedata.NewDomainError(runtimedata.CodeIntegrity, "source run %s has no exact bundle identity", runID)
	}
	return strings.TrimSpace(bundleHash.String), nil
}

func (o *Owner) loadRunPinsTx(ctx context.Context, tx *sql.Tx, runID string) ([]runtimedata.Pin, error) {
	rows, err := tx.QueryContext(ctx, o.query(`
		SELECT p.run_id, r.status, p.flow_path, p.event_name, p.schema_digest, p.version_id, p.selection
		FROM resource_version_pins p JOIN runs r ON r.run_id = p.run_id
		WHERE p.run_id = %s ORDER BY p.flow_path, p.event_name
	`, 1), runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	pins := make([]runtimedata.Pin, 0)
	for rows.Next() {
		var pin runtimedata.Pin
		if err := rows.Scan(&pin.RunID, &pin.RunState, &pin.Declaration.FlowPath, &pin.Declaration.EventName, &pin.SchemaDigest, &pin.VersionID, &pin.Selection); err != nil {
			return nil, err
		}
		pins = append(pins, pin)
	}
	return pins, rows.Err()
}

func (o *Owner) validateRunPinSetTx(ctx context.Context, tx *sql.Tx, runID string, declarations []runtimedata.Declaration, pins []runtimedata.Pin) error {
	declarationByKey := make(map[string]runtimedata.Declaration, len(declarations))
	for _, declaration := range declarations {
		declarationByKey[declaration.Ref.Key()] = declaration
	}
	for _, pin := range pins {
		declaration, ok := declarationByKey[pin.Declaration.Key()]
		if !ok || pin.RunID != runID || pin.Declaration != declaration.Ref || pin.SchemaDigest != declaration.SchemaDigest {
			return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "run %s pin aggregate contradicts exact bundle declaration %s", runID, pin.Declaration.Key())
		}
		switch pin.Selection {
		case "explicit", "fused_import", "fork_inherited", "fork_override":
		default:
			return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "run %s pin aggregate has invalid selection %q", runID, pin.Selection)
		}
		version, found, err := o.loadStoredVersion(ctx, tx, declaration.Ref, pin.VersionID)
		if err != nil {
			return err
		}
		if !found || version.PrunedAt != nil || version.Manifest.SchemaDigest != pin.SchemaDigest {
			return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "run %s pin references an invalid version aggregate for %s", runID, declaration.Ref.Key())
		}
	}
	return nil
}

func samePinSet(actual, expected []runtimedata.Pin) bool {
	if len(actual) != len(expected) {
		return false
	}
	runtimedata.SortPins(actual)
	runtimedata.SortPins(expected)
	for index := range actual {
		if actual[index].RunID != expected[index].RunID || actual[index].Declaration != expected[index].Declaration ||
			actual[index].SchemaDigest != expected[index].SchemaDigest || actual[index].VersionID != expected[index].VersionID ||
			actual[index].Selection != expected[index].Selection {
			return false
		}
	}
	return true
}
