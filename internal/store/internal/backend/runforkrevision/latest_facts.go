package runforkrevision

// Fold the revision index once, then hydrate only the latest row of each key.
// All families and tombstones remain in the result and pass the same read
// validation; this is not a dirty-family filter or a second latest-state store.
// CROSS JOIN keeps SQLite from driving the join with every historical row.
const latestFactsQuery = `
SELECT r.family, r.fact_key, r.fact, r.present
FROM (
	SELECT family, fact_key, MAX(revision) AS revision
	FROM run_fork_fact_revisions
	WHERE run_id=$1
	GROUP BY family, fact_key
) latest CROSS JOIN run_fork_fact_revisions r
WHERE r.run_id=$1 AND r.family=latest.family
	AND r.fact_key=latest.fact_key AND r.revision=latest.revision
`
