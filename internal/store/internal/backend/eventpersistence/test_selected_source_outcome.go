package eventpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type SelectedSourceOutcomeFixture struct {
	RunID, EventID, EntityID string
	CreatedAt                time.Time
	Failure                  failures.Envelope
}

func (s *EventPostgresOwner) SeedSelectedSourceOutcomeForTest(ctx context.Context, fixture SelectedSourceOutcomeFixture, sourceHash, failure string) error {
	if s == nil || s.backend == nil || s.RunLifecyclePostgresOwner == nil {
		return fmt.Errorf("postgres event fixture owner is required")
	}
	return runNativeEventFixtureMutation(ctx, s, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		return seedSelectedSourceOutcomeTx(ctx, attempt, fixture, sourceHash, failure)
	})
}

func (s *EventSQLiteOwner) SeedSelectedSourceOutcomeForTest(ctx context.Context, fixture SelectedSourceOutcomeFixture, sourceHash, failure string) error {
	if s == nil || s.backend == nil || s.RunLifecycleSQLiteOwner == nil {
		return fmt.Errorf("sqlite event fixture owner is required")
	}
	return runNativeEventFixtureMutation(ctx, s, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		return seedSelectedSourceOutcomeTx(ctx, attempt, fixture, sourceHash, failure)
	})
}

func seedSelectedSourceOutcomeTx(ctx context.Context, attempt *mutationprotocol.Attempt, fixture SelectedSourceOutcomeFixture, sourceHash, failure string) error {
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var hash, eventRun string
		if err := tx.QueryRowContext(ctx, `SELECT r.bundle_hash,e.run_id FROM runs r JOIN events e ON e.run_id=r.run_id WHERE r.run_id=$1 AND e.event_id=$2`, fixture.RunID, fixture.EventID).Scan(&hash, &eventRun); err != nil {
			return err
		}
		if hash != sourceHash || eventRun != fixture.RunID {
			return fmt.Errorf("source outcome fixture does not own the persisted event")
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO event_receipts (event_id,subscriber_type,subscriber_id,entity_id,flow_instance,outcome,reason_code,side_effects,processed_at)
			VALUES ($1,'platform','old-source-node',$2,'flow-a/1','success','source_outcome_must_not_suppress_fork','{}',$3)`, fixture.EventID, fixture.EntityID, fixture.CreatedAt); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO dead_letters (original_event_id,original_event,entity_id,flow_instance,failure,handler_node,created_at)
			VALUES ($1,'item.received',$2,'flow-a/1',$3,'old-source-node',$4)`, fixture.EventID, fixture.EntityID, failure, fixture.CreatedAt)
		return err
	})
}
