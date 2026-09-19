package runforkrevision

import (
	"context"
	"fmt"
	"strings"
)

func latestFactReadQuery(runID string, families []Family) (string, []any, map[Family]bool, error) {
	// Fold the revision index once, then hydrate only the latest row of each key.
	// All families and tombstones retain read validation. CROSS JOIN keeps SQLite
	// from driving the join with every historical row.
	const latestFactsQuery = `
SELECT r.family, r.fact_key, %s, r.present
FROM (
	SELECT family, fact_key, MAX(revision) AS revision
	FROM run_fork_fact_revisions
	WHERE run_id=$1
	GROUP BY family, fact_key
) latest CROSS JOIN run_fork_fact_revisions r
WHERE r.run_id=$1 AND r.family=latest.family
	AND r.fact_key=latest.fact_key AND r.revision=latest.revision
`
	if len(families) == 0 {
		return "", nil, nil, fmt.Errorf("latest revision projection requires declared families")
	}
	args := []any{runID}
	wanted := make(map[Family]bool, len(families))
	var binds []string
	for _, family := range families {
		if !ValidFamily(family) {
			return "", nil, nil, fmt.Errorf("unsupported revision projection family %q", family)
		}
		if wanted[family] {
			continue
		}
		wanted[family] = true
		args = append(args, string(family))
		binds = append(binds, fmt.Sprintf("$%d", len(args)))
	}
	body := "r.fact"
	if len(wanted) == len(allFamilies) {
		args = args[:1]
	} else {
		body = "CASE WHEN r.family IN (" + strings.Join(binds, ",") + ") THEN r.fact END"
	}
	return fmt.Sprintf(latestFactsQuery, body), args, wanted, nil
}

func readLatestFacts(ctx context.Context, q queryer, runID string, families []Family) (ledgerFactsByFamily, error) {
	query, args, wanted, err := latestFactReadQuery(runID, families)
	if err != nil {
		return nil, err
	}
	return readLedgerFacts(ctx, q, query, args, wanted)
}

func readSelectedLatestFacts(ctx context.Context, q queryer, change declaredChange) (ledgerFactsByFamily, error) {
	if len(change.exact) == 0 {
		return readLatestFacts(ctx, q, change.runID, change.families)
	}
	return readAffectedLatestFacts(ctx, q, change)
}

// A whole-family contribution in an exact transaction stays family-scoped even
// when the exact keys require multiple reads. It must not become a run scan.
func readAffectedLatestFacts(ctx context.Context, q queryer, change declaredChange) (ledgerFactsByFamily, error) {
	count := 0
	for _, refs := range change.exact {
		count += len(refs)
	}
	if count > exactFactReadBatch {
		all := ledgerFactsByFamily{}
		for _, family := range change.families {
			refs, exact := change.exact[family]
			if !exact {
				facts, err := readAffectedLatestFacts(ctx, q, declaredChange{runID: change.runID, families: []Family{family}})
				if err != nil {
					return nil, err
				}
				all[family] = facts[family]
				continue
			}
			all[family] = map[string]ledgerFact{}
			for start := 0; start < len(refs); start += exactFactReadBatch {
				end := min(start+exactFactReadBatch, len(refs))
				part := declaredChange{runID: change.runID, families: []Family{family}, exact: map[Family][]FactRef{family: refs[start:end]}}
				facts, err := readAffectedLatestFacts(ctx, q, part)
				if err != nil {
					return nil, err
				}
				for key, fact := range facts[family] {
					if _, duplicate := all[family][key]; duplicate {
						return nil, fmt.Errorf("duplicate latest %s revision fact %s", family, key)
					}
					all[family][key] = fact
				}
			}
		}
		return all, nil
	}
	args := []any{change.runID}
	wanted := make(map[Family]bool, len(change.families))
	var selections []string
	for _, family := range change.families {
		if !ValidFamily(family) {
			return nil, fmt.Errorf("unsupported revision projection family %q", family)
		}
		wanted[family] = true
		args = append(args, string(family))
		condition := fmt.Sprintf("family=$%d", len(args))
		if refs, exact := change.exact[family]; exact {
			if len(refs) == 0 {
				return nil, fmt.Errorf("exact ledger selection has no fact coordinates")
			}
			binds := make([]string, len(refs))
			for i, ref := range refs {
				if err := ref.validate(change.runID); err != nil {
					return nil, err
				}
				if ref.family != family {
					return nil, fmt.Errorf("exact ledger selection has a foreign family")
				}
				args = append(args, ref.key)
				binds[i] = fmt.Sprintf("$%d", len(args))
			}
			condition += " AND fact_key IN (" + strings.Join(binds, ",") + ")"
		}
		selections = append(selections, "("+condition+")")
	}
	if len(selections) == 0 {
		return nil, fmt.Errorf("latest revision projection requires declared families")
	}
	query := `SELECT r.family,r.fact_key,r.fact,r.present
		FROM (SELECT family,fact_key,MAX(revision) AS revision FROM run_fork_fact_revisions
		WHERE run_id=$1 AND (` + strings.Join(selections, " OR ") + `)
		GROUP BY family,fact_key) latest CROSS JOIN run_fork_fact_revisions r
		WHERE r.run_id=$1 AND r.family=latest.family
		AND r.fact_key=latest.fact_key AND r.revision=latest.revision`
	return readLedgerFacts(ctx, q, query, args, wanted)
}

func readLedgerFacts(ctx context.Context, q queryer, query string, args []any, wanted map[Family]bool) (ledgerFactsByFamily, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	facts := ledgerFactsByFamily{}
	var family Family
	var key string
	var fact []byte
	var present bool
	for rows.Next() {
		// Keep every family's coordinate/presence validation, including unknown
		// and tombstoned families. Only unused JSON body transfer is omitted.
		if err := rows.Scan(&family, &key, &fact, &present); err != nil {
			return nil, err
		}
		if !ValidFamily(family) {
			return nil, fmt.Errorf("decode unsupported run fork revision fact family %q", family)
		}
		if !wanted[family] {
			continue
		}
		if facts[family] == nil {
			facts[family] = map[string]ledgerFact{}
		}
		if _, duplicate := facts[family][key]; duplicate {
			return nil, fmt.Errorf("duplicate latest %s revision fact %s", family, key)
		}
		// Scan into *[]byte already copies driver memory into caller ownership;
		// unlike RawBytes, this slice survives the next row without another copy.
		facts[family][key] = ledgerFact{fact: fact, present: present}
	}
	return facts, rows.Err()
}
