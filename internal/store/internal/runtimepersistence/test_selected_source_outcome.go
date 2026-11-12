package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type SelectedSourceOutcomeFixture struct {
	RunID, EventID, EntityID string
	CreatedAt                time.Time
	Failure                  failures.Envelope
}

// These two exact predecessor facts deliberately must not suppress fork work.
// Caller SQL and transaction authority never cross the fixture boundary.
func SeedSelectedSourceOutcomeForTest(ctx context.Context, selected any, fixture SelectedSourceOutcomeFixture) error {
	for _, identity := range []string{fixture.RunID, fixture.EventID, fixture.EntityID} {
		if err := validateSelectedForkStorageIdentity(identity); err != nil {
			return err
		}
	}
	source, ok := correlation.SourceArtifactFactFromContext(ctx)
	if !ok || source.BundleHash() == "" || fixture.CreatedAt.IsZero() {
		return fmt.Errorf("source outcome fixture requires exact source and time")
	}
	failure, err := json.Marshal(fixture.Failure)
	if err != nil {
		return err
	}
	return runEventFixtureMutationForTest(ctx, selected, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			var hash, eventRun string
			if err := tx.QueryRowContext(ctx, `SELECT r.bundle_hash,e.run_id FROM runs r JOIN events e ON e.run_id=r.run_id WHERE r.run_id=$1 AND e.event_id=$2`, fixture.RunID, fixture.EventID).Scan(&hash, &eventRun); err != nil {
				return err
			}
			if hash != source.BundleHash() || eventRun != fixture.RunID {
				return fmt.Errorf("source outcome fixture does not own the persisted event")
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO event_receipts (event_id,subscriber_type,subscriber_id,entity_id,flow_instance,outcome,reason_code,side_effects,processed_at)
				VALUES ($1,'platform','old-source-node',$2,'flow-a/1','success','source_outcome_must_not_suppress_fork','{}',$3)`, fixture.EventID, fixture.EntityID, fixture.CreatedAt); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx, `INSERT INTO dead_letters (original_event_id,original_event,entity_id,flow_instance,failure,handler_node,created_at)
				VALUES ($1,'item.received',$2,'flow-a/1',$3,'old-source-node',$4)`, fixture.EventID, fixture.EntityID, string(failure), fixture.CreatedAt)
			return err
		})
	})
}
