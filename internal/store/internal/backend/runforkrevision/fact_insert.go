package runforkrevision

import (
	"context"
	"fmt"
	"strings"
)

// Six parameters per row keep even a full chunk below SQLite's 999-variable
// baseline. Chunks share the caller's transaction and allocated revision.
const revisionFactInsertBatch = 128

type revisionFactInsert struct {
	family  Family
	key     string
	fact    []byte
	present bool
}

func insertRevisionFacts(ctx context.Context, tx revisionSQL, postgres bool, runID string, revision int64, facts []revisionFactInsert) error {
	for start := 0; start < len(facts); start += revisionFactInsertBatch {
		batch := facts[start:min(start+revisionFactInsertBatch, len(facts))]
		var query strings.Builder
		query.WriteString(`INSERT INTO run_fork_fact_revisions (run_id,revision,family,fact_key,fact,present) VALUES `)
		args := make([]any, 0, 6*len(batch))
		for i, fact := range batch {
			if i != 0 {
				query.WriteByte(',')
			}
			n := 6 * i
			cast := ""
			var body any = string(fact.fact)
			if postgres {
				cast, body = "::jsonb", fact.fact
			}
			fmt.Fprintf(&query, "($%d,$%d,$%d,$%d,$%d%s,$%d)", n+1, n+2, n+3, n+4, n+5, cast, n+6)
			args = append(args, runID, revision, fact.family, fact.key, body, fact.present)
		}
		if _, err := tx.ExecContext(ctx, query.String(), args...); err != nil {
			if len(batch) == 1 {
				fact := batch[0]
				return fmt.Errorf("record run fork %s fact %s at revision %d: %w", fact.family, fact.key, revision, err)
			}
			coordinates := make([]string, len(batch))
			for i, fact := range batch {
				coordinates[i] = fmt.Sprintf("%s/%s", fact.family, fact.key)
			}
			return fmt.Errorf("record run fork facts [%s] for run %s at revision %d: %w", strings.Join(coordinates, ", "), runID, revision, err)
		}
	}
	return nil
}
