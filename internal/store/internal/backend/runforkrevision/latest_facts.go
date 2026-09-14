package runforkrevision

import (
	"context"
	"fmt"
	"strings"
)

// Fold the revision index once, then hydrate only the latest row of each key.
// All families and tombstones remain in the result and pass the same read
// validation; this is not a dirty-family filter or a second latest-state store.
// CROSS JOIN keeps SQLite from driving the join with every historical row.
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

func latestFactReadQuery(runID string, families []Family) (string, []any, map[Family]bool, error) {
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
		facts[family][key] = ledgerFact{fact: append([]byte(nil), fact...), present: present}
	}
	return facts, rows.Err()
}
