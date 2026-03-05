package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"empireai/internal/config"
	"empireai/internal/events"
	"empireai/internal/runtime"
	"github.com/google/uuid"
)

func humanTaskExpiryLoop(ctx context.Context, db *sql.DB, cfg *config.Config, bus *runtime.EventBus) {
	if db == nil || cfg == nil || bus == nil {
		return
	}
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	expire := func() {
		hours := cfg.Budget.HumanTasks.AutoExpireHours
		if hours <= 0 {
			hours = 168
		}

		// First: requeue any deferred tasks whose requeue_date has arrived.
		// This drives the spec's "next budget cycle" behavior without requiring
		// an LLM decision loop.
		{
			type dueRow struct {
				ID        string
				Requester string
				Vertical  string
				Category  string
				Desc      string
			}
			due := make([]dueRow, 0, 32)
			rows, err := db.QueryContext(ctx, `
				SELECT
					id::text,
					requesting_agent,
					COALESCE(vertical_id::text, ''),
					category,
					description
				FROM human_tasks
				WHERE status = 'deferred'
				  AND COALESCE(review_decision->>'requeue_date','') <> ''
				  AND (review_decision->>'requeue_date')::timestamptz <= now()
				ORDER BY created_at ASC
				LIMIT 200
			`)
			if err != nil {
				log.Printf("human task requeue query failed: %v", err)
			} else {
				for rows.Next() {
					var r dueRow
					if err := rows.Scan(&r.ID, &r.Requester, &r.Vertical, &r.Category, &r.Desc); err != nil {
						log.Printf("human task requeue scan failed: %v", err)
						_ = rows.Close()
						break
					}
					due = append(due, r)
				}
				_ = rows.Close()
			}
			for _, r := range due {
				// Clear the requeue_date to avoid immediate re-trigger.
				if _, err := db.ExecContext(ctx, `
					UPDATE human_tasks
					SET status = 'pending_review',
					    reviewed_at = NULL,
					    review_decision = NULL
					WHERE id = $1::uuid
					  AND status = 'deferred'
				`, r.ID); err != nil {
					continue
				}
				b, _ := json.Marshal(map[string]any{
					"task_id":          strings.TrimSpace(r.ID),
					"requesting_agent": strings.TrimSpace(r.Requester),
					"vertical_id":      strings.TrimSpace(r.Vertical),
					"category":         strings.TrimSpace(r.Category),
					"description":      strings.TrimSpace(r.Desc),
					"priority":         "medium",
					"requeued":         true,
					"requeue_reason":   "budget_cycle_reset",
				})
				_ = bus.Publish(ctx, events.Event{
					ID:          uuid.NewString(),
					Type:        events.EventType("human_task.requested"),
					SourceAgent: "runtime",
					VerticalID:  strings.TrimSpace(r.Vertical),
					Payload:     b,
					CreatedAt:   time.Now(),
				})
			}
		}

		// Fetch a bounded batch of candidates to avoid unbounded work on startup.
		rows, err := db.QueryContext(ctx, `
			SELECT
				id::text,
				requesting_agent,
				COALESCE(vertical_id::text, ''),
				COALESCE(requeue_count, 0),
				created_at,
				deadline
			FROM human_tasks
			WHERE status IN ('pending_review', 'approved', 'assigned')
			  AND (
					(deadline IS NOT NULL AND deadline <= now())
					OR
					(created_at <= now() - ($1::int * interval '1 hour'))
			  )
			ORDER BY created_at ASC
			LIMIT 200
		`, hours)
		if err != nil {
			log.Printf("human task expiry query failed: %v", err)
			return
		}
		defer rows.Close()

		type taskRow struct {
			ID        string
			Requester string
			Vertical  string
			Requeue   int
			CreatedAt time.Time
			Deadline  sql.NullTime
		}
		candidates := make([]taskRow, 0, 64)
		for rows.Next() {
			var tr taskRow
			if err := rows.Scan(&tr.ID, &tr.Requester, &tr.Vertical, &tr.Requeue, &tr.CreatedAt, &tr.Deadline); err != nil {
				log.Printf("human task expiry scan failed: %v", err)
				return
			}
			candidates = append(candidates, tr)
		}
		if err := rows.Err(); err != nil {
			log.Printf("human task expiry rows failed: %v", err)
			return
		}

		for _, tr := range candidates {
			reason := "auto_expire_hours_reached"
			if tr.Deadline.Valid && tr.Deadline.Time.Before(time.Now().Add(1*time.Second)) {
				reason = "deadline_passed"
			}
			var updated bool
			if err := db.QueryRowContext(ctx, `
				UPDATE human_tasks
				SET status = 'expired',
				    outcome = COALESCE(NULLIF(outcome,''), 'expired'),
				    completed_at = now()
				WHERE id = $1::uuid
				  AND status IN ('pending_review', 'approved', 'assigned')
				RETURNING true
			`, tr.ID).Scan(&updated); err != nil {
				// If the row was already updated elsewhere, ignore.
				continue
			}

			payload := map[string]any{
				"task_id":          strings.TrimSpace(tr.ID),
				"requesting_agent": strings.TrimSpace(tr.Requester),
				"vertical_id":      strings.TrimSpace(tr.Vertical),
				"expiry_reason":    reason,
			}
			b, _ := json.Marshal(payload)
			_ = bus.Publish(ctx, events.Event{
				ID:          uuid.NewString(),
				Type:        events.EventType("human_task.expired"),
				SourceAgent: "runtime",
				VerticalID:  strings.TrimSpace(tr.Vertical),
				Payload:     b,
				CreatedAt:   time.Now(),
			})

			// Spec v2.0 expiry behavior:
			// - If stale (deadline passed): expire permanently.
			// - If requeued 2+ times: escalate to mailbox.
			// - Else: auto-requeue for next weekly budget cycle (deferred + requeue_date).
			if reason == "deadline_passed" {
				_, _ = db.ExecContext(ctx, `
					UPDATE human_tasks
					SET outcome = 'expired_permanent'
					WHERE id = $1::uuid
				`, tr.ID)
				continue
			}

			if tr.Requeue >= 2 {
				// Escalate to mailbox: human decides whether to increase budget or drop the task.
				summary := fmt.Sprintf("Human task requeued %d times; decision needed (task %s)", tr.Requeue, strings.TrimSpace(tr.ID)[:8])
				_, _ = db.ExecContext(ctx, `
					INSERT INTO mailbox (
						id, event_id, vertical_id, from_agent, type, priority, status,
						context, summary, timeout_at, created_at
					) VALUES (
						$1::uuid,
						NULL,
						NULLIF($2,'')::uuid,
						$3,
						$4,
						$5,
						'pending',
						$6::jsonb,
						$7,
						$8,
						now()
					)
				`,
					uuid.NewString(),
					strings.TrimSpace(tr.Vertical),
					"empire-coordinator",
					"human_task_requeue_escalation",
					"critical",
					string(b),
					summary,
					time.Now().Add(7*24*time.Hour),
				)
				continue
			}

			requeueAt := runtime.NextWeekResetUTC(time.Now(), cfg.Budget.HumanTasks.BudgetReset).UTC().Format(time.RFC3339)
			decisionObj := map[string]any{
				"decision":     "deferred",
				"defer_reason": "auto_requeue_next_cycle",
				"requeue_date": requeueAt,
				"decided_by":   "runtime",
				"decided_at":   time.Now().UTC().Format(time.RFC3339),
			}
			decisionJSON, _ := json.Marshal(decisionObj)
			if _, err := db.ExecContext(ctx, `
				UPDATE human_tasks
				SET status = 'deferred',
				    reviewed_at = now(),
				    review_decision = $2::jsonb,
				    requeue_count = COALESCE(requeue_count, 0) + 1,
				    assigned_to = NULL,
				    completed_at = NULL
				WHERE id = $1::uuid
			`, tr.ID, string(decisionJSON)); err == nil {
				_ = bus.Publish(ctx, events.Event{
					ID:          uuid.NewString(),
					Type:        events.EventType("human_task.deferred"),
					SourceAgent: "runtime",
					VerticalID:  strings.TrimSpace(tr.Vertical),
					Payload: mustJSON(map[string]any{
						"task_id":          strings.TrimSpace(tr.ID),
						"requesting_agent": strings.TrimSpace(tr.Requester),
						"vertical_id":      strings.TrimSpace(tr.Vertical),
						"defer_reason":     "auto_requeue_next_cycle",
						"requeue_date":     requeueAt,
					}),
					CreatedAt: time.Now(),
				})
			}
		}
	}

	expire()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			expire()
		}
	}
}
