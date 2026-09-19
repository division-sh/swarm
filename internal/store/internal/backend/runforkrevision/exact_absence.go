package runforkrevision

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// A missing run-scoped projection is not a deletion when its globally keyed
// record still exists elsewhere. Reuse the projection's relation and key owner;
// entity metadata and fan-out keys are intentionally scoped by run instead.
func validateExactAbsence(ctx context.Context, q queryer, change declaredChange, family Family, refs []FactRef, current []canonicalFact, changes []declaredChange) error {
	if family == FamilyEntityMetadata || family == FamilyFanOutObligations {
		return nil
	}
	present := make(map[string]bool, len(current))
	for _, fact := range current {
		present[fact.key] = true
	}
	var missing []string
	for _, ref := range refs {
		if !present[ref.key] {
			missing = append(missing, ref.key)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	spec, ok := canonicalProjectionSpec(family)
	if !ok || spec.source == "" || spec.runAlias == "" || len(spec.parts) != 1 {
		return fmt.Errorf("exact %s absence has no canonical ownership projection", family)
	}
	fields, err := factKeyFields(family)
	if err != nil {
		return err
	}
	keyColumn := spec.parts[0].alias + "." + fields[0]
	for start := 0; start < len(missing); start += exactFactReadBatch {
		keys := missing[start:min(start+exactFactReadBatch, len(missing))]
		args := make([]any, len(keys))
		binds := make([]string, len(keys))
		for i, key := range keys {
			args[i], binds[i] = key, fmt.Sprintf("$%d", i+1)
		}
		query := "SELECT CAST(" + keyColumn + " AS TEXT), CAST(" + spec.runAlias + ".run_id AS TEXT) FROM " + spec.source + " WHERE " + keyColumn + " IN (" + strings.Join(binds, ",") + ")"
		rows, err := q.QueryContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("query exact %s absence ownership: %w", family, err)
		}
		err = checkAbsentOwners(rows, change.runID, family, changes)
		closeErr := rows.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

func checkAbsentOwners(rows *sql.Rows, oldRunID string, family Family, changes []declaredChange) error {
	seen := map[string]bool{}
	for rows.Next() {
		var key string
		var owner sql.NullString
		if err := rows.Scan(&key, &owner); err != nil {
			return err
		}
		if seen[key] {
			return fmt.Errorf("duplicate exact %s ownership for %s", family, key)
		}
		seen[key] = true
		if !owner.Valid || owner.String == "" || owner.String == oldRunID || !declaresFact(changes, owner.String, family, key) {
			return fmt.Errorf("exact %s fact %s remains owned by run %q without a complete declared move from %s; not a deletion", family, key, owner.String, oldRunID)
		}
	}
	return rows.Err()
}

func declaresFact(changes []declaredChange, runID string, family Family, key string) bool {
	for _, change := range changes {
		if change.runID != runID {
			continue
		}
		for _, selected := range change.families {
			if selected != family {
				continue
			}
			refs, exact := change.exact[family]
			if !exact {
				return true
			}
			for _, ref := range refs {
				if ref.key == key {
					return true
				}
			}
		}
	}
	return false
}
