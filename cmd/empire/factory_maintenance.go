package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"empireai/internal/events"
	"empireai/internal/runtime"
	"github.com/google/uuid"
)

// Marginal maintenance implements spec v2.0 GAP 5:
// - scheduled 14-day re-evaluation for parked marginals
// - kill stale marginals parked > 60 days
//
// Factory scoring/validation is still lightweight, but these transitions prevent
// marginals from becoming silent dead-ends.
func marginalMaintenanceLoop(ctx context.Context, db *sql.DB, bus *runtime.EventBus) {
	if db == nil {
		return
	}
	ticker := time.NewTicker(12 * time.Hour)
	defer ticker.Stop()

	run := func() {
		// Kill stale marginals.
		{
			rows, err := db.QueryContext(ctx, `
				UPDATE verticals
				SET stage = 'killed',
				    killed_at_stage = 'marginal_review',
				    kill_reason = COALESCE(NULLIF(kill_reason,''), 'stale marginal parked >60 days'),
				    parked_at = NULL,
				    updated_at = now()
				WHERE stage = 'marginal_review'
				  AND parked_at IS NOT NULL
				  AND parked_at <= now() - interval '60 days'
				RETURNING id::text
			`)
			if err == nil {
				for rows.Next() {
					var id string
					if err := rows.Scan(&id); err == nil && bus != nil {
						payload, _ := json.Marshal(map[string]any{
							"vertical_id": id,
							"reason":      "stale_marginal",
						})
						_ = bus.Publish(context.Background(), events.Event{
							ID:          uuid.NewString(),
							Type:        events.EventType("factory.marginal_killed"),
							SourceAgent: "runtime",
							VerticalID:  id,
							Payload:     payload,
							CreatedAt:   time.Now(),
						})
					}
				}
				_ = rows.Close()
			}
		}

		// Requeue parked marginals for re-scoring (scheduled review).
		{
			rows, err := db.QueryContext(ctx, `
				UPDATE verticals
				SET stage = 'scoring',
				    parked_at = NULL,
				    updated_at = now()
				WHERE stage = 'marginal_review'
				  AND parked_at IS NOT NULL
				  AND parked_at <= now() - interval '14 days'
				RETURNING id::text
			`)
			if err != nil {
				log.Printf("marginal unpark update failed: %v", err)
				return
			}
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err == nil && bus != nil {
					payload, _ := json.Marshal(map[string]any{
						"vertical_id": id,
						"reason":      "scheduled_14d_review",
					})
					_ = bus.Publish(context.Background(), events.Event{
						ID:          uuid.NewString(),
						Type:        events.EventType("factory.marginal_requeued"),
						SourceAgent: "runtime",
						VerticalID:  id,
						Payload:     payload,
						CreatedAt:   time.Now(),
					})
				}
			}
			_ = rows.Close()
		}
	}

	run()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}
