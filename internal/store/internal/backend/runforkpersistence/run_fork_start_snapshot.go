package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func resolveRunForkStartPoint(ctx context.Context, tx *sql.Tx, runID string) (runForkEventCursor, error) {
	revision, _, err := runforkrevision.LoadStartProjection(ctx, tx, runID)
	if err != nil {
		return runForkEventCursor{}, err
	}
	return runForkEventCursor{Kind: runfork.RunForkPointRunStart, Revision: revision}, nil
}

func loadRunForkPointSnapshot(ctx context.Context, tx *sql.Tx, runID string, point runfork.RunForkPoint) (*runForkRevisionSnapshot, error) {
	if err := point.Validate(); err != nil {
		return nil, err
	}
	if point.Kind != runfork.RunForkPointRunStart {
		return loadRunForkRevisionSnapshot(ctx, tx, runID, point.Revision)
	}
	revision, projection, err := runforkrevision.LoadStartProjection(ctx, tx, runID)
	if err != nil {
		return nil, err
	}
	if revision != point.Revision {
		return nil, fmt.Errorf("fixed start point differs from immutable creation revision")
	}
	snapshot := &runForkRevisionSnapshot{RunID: runID, Revision: revision, StartProjection: &projection}
	const batchSize = 128
	for offset := 0; offset < len(projection.Facts); offset += batchSize {
		end := min(offset+batchSize, len(projection.Facts))
		if err := loadRunForkStartFacts(ctx, tx, snapshot, projection.Facts[offset:end]); err != nil {
			return nil, err
		}
	}
	snapshot.sort()
	return snapshot, nil
}

// Restrict the database read before ranking or decoding; the creating ingress
// and receivers constructed by its routes are not members of this cut.
func loadRunForkStartFacts(ctx context.Context, tx *sql.Tx, snapshot *runForkRevisionSnapshot, facts []runforkrevision.StartFact) error {
	args := []any{snapshot.RunID, snapshot.Revision}
	predicates := make([]string, 0, len(facts))
	for _, fact := range facts {
		args = append(args, string(fact.Family), fact.Key)
		predicates = append(predicates, fmt.Sprintf("(family=$%d AND fact_key=$%d)", len(args)-1, len(args)))
	}
	rows, err := tx.QueryContext(ctx, `
		WITH bounded AS (
			SELECT run_id, family, fact_key, revision, fact, present,
			       MIN(revision) OVER (PARTITION BY family, fact_key) AS first_revision,
			       ROW_NUMBER() OVER (PARTITION BY family, fact_key ORDER BY revision DESC) AS latest_rank
			FROM run_fork_fact_revisions
			WHERE run_id=$1 AND revision<=$2 AND (`+strings.Join(predicates, " OR ")+`)
		)
		SELECT run_id, family, fact_key, first_revision, revision, fact
		FROM bounded WHERE latest_rank=1 AND present
		ORDER BY family, first_revision, fact_key`, args...)
	if err != nil {
		return fmt.Errorf("load run start facts: %w", err)
	}
	defer rows.Close()
	wanted := make(map[runForkHistoricalFactKey]bool, len(facts))
	for _, fact := range facts {
		wanted[runForkHistoricalFactKey{family: fact.Family, key: fact.Key}] = true
	}
	for rows.Next() {
		var fact runForkHistoricalFactContext
		var raw []byte
		if err := rows.Scan(&fact.RunID, &fact.Family, &fact.Key, &fact.FirstRevision, &fact.Revision, &raw); err != nil {
			return fmt.Errorf("scan run start fact: %w", err)
		}
		key := runForkHistoricalFactKey{family: fact.Family, key: fact.Key}
		if !wanted[key] || fact.FirstRevision != snapshot.Revision || fact.Revision != snapshot.Revision {
			return fmt.Errorf("run start fact is outside original creation coordinates")
		}
		if err := appendRunForkHistoricalFact(snapshot, fact, raw); err != nil {
			return err
		}
		delete(wanted, key)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read run start facts: %w", err)
	}
	if len(wanted) != 0 {
		return fmt.Errorf("run start projection references missing initial facts")
	}
	return nil
}

func loadRunForkStartFirstTurn(ctx context.Context, tx *sql.Tx, snapshot *runForkRevisionSnapshot) (*runfork.InputPublication, error) {
	if snapshot.StartProjection == nil || snapshot.StartProjection.FirstTurnEventID == "" {
		return nil, nil
	}
	var context runForkHistoricalFactContext
	var raw []byte
	err := tx.QueryRowContext(ctx, `SELECT run_id, fact_key, revision, fact
		FROM run_fork_fact_revisions WHERE run_id=$1 AND family='events'
		AND fact_key=$2 AND revision=$3 AND present`, snapshot.RunID,
		snapshot.StartProjection.FirstTurnEventID, snapshot.Revision).Scan(&context.RunID, &context.Key, &context.Revision, &raw)
	if err != nil {
		return nil, fmt.Errorf("read original start first turn: %w", err)
	}
	context.Family, context.FirstRevision = runforkrevision.FamilyEvents, context.Revision
	intention := &runForkRevisionSnapshot{RunID: snapshot.RunID, Revision: snapshot.Revision}
	if err := appendRunForkHistoricalFact(intention, context, raw); err != nil {
		return nil, fmt.Errorf("admit original start first turn: %w", err)
	}
	admitted, err := decodeRunForkRevisionEvent(intention.Events[0])
	if err != nil {
		return nil, err
	}
	input, err := runfork.InputPublicationFromEvent(admitted.Event())
	if err != nil {
		return nil, err
	}
	if _, present := input.Event(); !present || input.Coordinates().Schema.BundleHash != snapshot.StartProjection.SourceBundleHash {
		return nil, fmt.Errorf("start first turn lacks exact original input/schema ownership")
	}
	return &input, nil
}
