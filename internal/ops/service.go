package ops

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimeproductpolicy "empireai/internal/runtime/productpolicy"
	runtimetools "empireai/internal/runtime/tools"
	"github.com/google/uuid"
)

type Service struct {
	DB      *sql.DB
	Mailbox runtimetools.MailboxPersistence
}

func NewService(db *sql.DB, mailbox runtimetools.MailboxPersistence) *Service {
	return &Service{DB: db, Mailbox: mailbox}
}

func controlPlaneMailboxSender() string {
	return strings.TrimSpace(runtimeproductpolicy.ControlPlaneAgentID())
}

type MetricInput struct {
	VerticalID       string
	PeriodStart      time.Time
	PeriodEnd        time.Time
	UsersTotal       int
	UsersNew         int
	UsersChurned     int
	MRRCents         int
	SupportTickets   int
	BugsReported     int
	BugsFixed        int
	FeaturesShipped  int
	OutreachSent     int
	OutreachResponse int
	APICostCents     int
	InfraCostCents   int
}

func (s *Service) RecordMetrics(ctx context.Context, in MetricInput) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("ops service requires postgres db")
	}
	if strings.TrimSpace(in.VerticalID) == "" {
		return fmt.Errorf("vertical id is required")
	}
	if in.PeriodStart.IsZero() {
		in.PeriodStart = time.Now().UTC().Truncate(24 * time.Hour)
	}
	if in.PeriodEnd.IsZero() {
		in.PeriodEnd = in.PeriodStart.Add(24 * time.Hour)
	}

	_, err := s.DB.ExecContext(ctx, `
		INSERT INTO vertical_metrics (
			vertical_id, period_start, period_end, users_total, users_new, users_churned, mrr_cents,
			support_tickets, bugs_reported, bugs_fixed, features_shipped, outreach_sent, outreach_responses,
			api_cost_cents, infra_cost_cents, created_at
		)
		VALUES (
			$1::uuid, $2::date, $3::date, $4, $5, $6, $7,
			$8, $9, $10, $11, $12, $13, $14, $15, now()
		)
		ON CONFLICT (vertical_id, period_start) DO UPDATE
		SET period_end = EXCLUDED.period_end,
		    users_total = EXCLUDED.users_total,
		    users_new = EXCLUDED.users_new,
		    users_churned = EXCLUDED.users_churned,
		    mrr_cents = EXCLUDED.mrr_cents,
		    support_tickets = EXCLUDED.support_tickets,
		    bugs_reported = EXCLUDED.bugs_reported,
		    bugs_fixed = EXCLUDED.bugs_fixed,
		    features_shipped = EXCLUDED.features_shipped,
		    outreach_sent = EXCLUDED.outreach_sent,
		    outreach_responses = EXCLUDED.outreach_responses,
		    api_cost_cents = EXCLUDED.api_cost_cents,
		    infra_cost_cents = EXCLUDED.infra_cost_cents
	`, in.VerticalID, in.PeriodStart, in.PeriodEnd, in.UsersTotal, in.UsersNew, in.UsersChurned, in.MRRCents,
		in.SupportTickets, in.BugsReported, in.BugsFixed, in.FeaturesShipped, in.OutreachSent, in.OutreachResponse,
		in.APICostCents, in.InfraCostCents)
	if err != nil {
		return fmt.Errorf("record vertical metrics: %w", err)
	}
	return nil
}

type TickSummary struct {
	KillCandidates   int
	BudgetAlerts     int
	RoutingProposals int
}

func (s *Service) Tick(ctx context.Context) (TickSummary, error) {
	out := TickSummary{}
	n, err := s.evaluateKillCriteria(ctx)
	if err != nil {
		return out, err
	}
	out.KillCandidates = n
	n, err = s.evaluateBudgetPressure(ctx)
	if err != nil {
		return out, err
	}
	out.BudgetAlerts = n
	n, err = s.analyzeRoutingPatterns(ctx, 3)
	if err != nil {
		return out, err
	}
	out.RoutingProposals = n
	return out, nil
}

func (s *Service) evaluateKillCriteria(ctx context.Context) (int, error) {
	if s.DB == nil || s.Mailbox == nil {
		return 0, nil
	}
	rows, err := s.DB.QueryContext(ctx, `
		WITH latest AS (
			SELECT DISTINCT ON (vertical_id)
				vertical_id, users_total, mrr_cents, users_churned, users_new, period_end
			FROM vertical_metrics
			ORDER BY vertical_id, period_end DESC
		)
		SELECT v.id::text, v.name, l.users_total, l.mrr_cents, l.users_churned, l.users_new
		FROM verticals v
		INNER JOIN latest l ON l.vertical_id = v.id
		WHERE v.mode = 'operating'
		  AND v.stage = 'operating'
	`)
	if err != nil {
		return 0, fmt.Errorf("query kill criteria: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var verticalID, name string
		var users, mrr, churn, usersNew int
		if err := rows.Scan(&verticalID, &name, &users, &mrr, &churn, &usersNew); err != nil {
			return count, fmt.Errorf("scan kill criteria row: %w", err)
		}
		if users > 5 || mrr > 3000 || usersNew >= churn {
			continue
		}
		ctxPayload := mustJSON(map[string]any{
			"vertical_id":    verticalID,
			"users_total":    users,
			"mrr_cents":      mrr,
			"users_churned":  churn,
			"users_new":      usersNew,
			"recommendation": "consider_kill_or_pivot",
		})
		if _, err := s.Mailbox.InsertMailboxItem(ctx, runtimetools.MailboxItem{
			VerticalID: verticalID,
			FromAgent:  controlPlaneMailboxSender(),
			Type:       "vertical_approval",
			Priority:   "critical",
			Status:     "pending",
			Context:    ctxPayload,
			Summary:    fmt.Sprintf("Kill criteria warning: %s (users=%d mrr=%d)", name, users, mrr),
		}); err != nil {
			return count, fmt.Errorf("create kill criteria mailbox item: %w", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("iterate kill criteria rows: %w", err)
	}
	return count, nil
}

func (s *Service) evaluateBudgetPressure(ctx context.Context) (int, error) {
	if s.DB == nil || s.Mailbox == nil {
		return 0, nil
	}
	rows, err := s.DB.QueryContext(ctx, `
		WITH m AS (
			SELECT DISTINCT ON (vertical_id)
				vertical_id, COALESCE(mrr_cents, 0) AS mrr_cents
			FROM vertical_metrics
			ORDER BY vertical_id, period_end DESC
		),
		sp AS (
			SELECT vertical_id, COALESCE(SUM(amount_cents), 0) AS spend_cents
			FROM spend_ledger
			WHERE created_at >= now() - interval '30 days'
			GROUP BY vertical_id
		)
		SELECT v.id::text, v.name, COALESCE(m.mrr_cents, 0), COALESCE(sp.spend_cents, 0)
		FROM verticals v
		LEFT JOIN m ON m.vertical_id = v.id
		LEFT JOIN sp ON sp.vertical_id = v.id
		WHERE v.mode = 'operating'
		  AND v.stage = 'operating'
	`)
	if err != nil {
		return 0, fmt.Errorf("query budget pressure: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var verticalID, name string
		var mrr, spend int
		if err := rows.Scan(&verticalID, &name, &mrr, &spend); err != nil {
			return count, fmt.Errorf("scan budget row: %w", err)
		}
		if spend <= 0 {
			continue
		}
		if mrr > 0 && float64(spend) <= float64(mrr)*1.2 {
			continue
		}
		ctxPayload := mustJSON(map[string]any{
			"vertical_id":    verticalID,
			"mrr_cents":      mrr,
			"spend_cents":    spend,
			"window_days":    30,
			"recommendation": "throttle_growth_spend",
		})
		if _, err := s.Mailbox.InsertMailboxItem(ctx, runtimetools.MailboxItem{
			VerticalID: verticalID,
			FromAgent:  controlPlaneMailboxSender(),
			Type:       "budget_increase",
			Priority:   "normal",
			Status:     "pending",
			Context:    ctxPayload,
			Summary:    fmt.Sprintf("Budget pressure: %s spend=%d mrr=%d", name, spend, mrr),
		}); err != nil {
			return count, fmt.Errorf("create budget mailbox item: %w", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("iterate budget rows: %w", err)
	}
	return count, nil
}

func (s *Service) analyzeRoutingPatterns(ctx context.Context, minVerticals int) (int, error) {
	if s.DB == nil || s.Mailbox == nil {
		return 0, nil
	}
	if minVerticals <= 1 {
		minVerticals = 3
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT event_pattern, COUNT(DISTINCT vertical_id) AS verticals
		FROM routing_rules
		WHERE status = 'active'
		  AND source IN ('discovered','retrospective')
		GROUP BY event_pattern
		HAVING COUNT(DISTINCT vertical_id) >= $1
		ORDER BY verticals DESC
	`, minVerticals)
	if err != nil {
		return 0, fmt.Errorf("query routing pattern analysis: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var pattern string
		var verticals int
		if err := rows.Scan(&pattern, &verticals); err != nil {
			return count, fmt.Errorf("scan routing analysis row: %w", err)
		}
		desc := fmt.Sprintf("Pattern %q observed in %d verticals", pattern, verticals)
		if _, err := s.DB.ExecContext(ctx, `
			INSERT INTO technical_patterns (id, pattern_type, description, vertical_ids, confidence, cto_notes, action_taken, created_at, updated_at)
			VALUES ($1::uuid, 'routing', $2, ARRAY[]::uuid[], 'observed', NULL, 'proposed_bootstrap_upgrade', now(), now())
		`, uuid.NewString(), desc); err != nil {
			return count, fmt.Errorf("insert technical pattern: %w", err)
		}
		ctxPayload := mustJSON(map[string]any{
			"pattern":   pattern,
			"verticals": verticals,
			"proposal":  "promote to bootstrap route",
		})
		if _, err := s.Mailbox.InsertMailboxItem(ctx, runtimetools.MailboxItem{
			FromAgent: "operations-analyst",
			Type:      "escalation",
			Priority:  "normal",
			Status:    "pending",
			Context:   ctxPayload,
			Summary:   fmt.Sprintf("Bootstrap upgrade proposal: %s (%d verticals)", pattern, verticals),
		}); err != nil {
			return count, fmt.Errorf("create routing proposal mailbox item: %w", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("iterate routing analysis rows: %w", err)
	}
	return count, nil
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	return b
}
