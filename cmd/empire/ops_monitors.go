package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"empireai/internal/digest"
	"empireai/internal/events"
	"empireai/internal/mailbox"
	"empireai/internal/runtime"
	"github.com/google/uuid"
)

func ensurePortfolioDigestSchedule(ctx context.Context, store runtime.SchedulePersistence) error {
	if store == nil {
		return nil
	}
	cron := strings.TrimSpace(os.Getenv("EMPIREAI_DIGEST_CRON"))
	if cron == "" {
		// Spec: 09:00 local time. We default to 09:00 UTC for deterministic behavior.
		cron = "0 9 * * *"
	}
	payload := []byte(`{"trigger":"daily"}`)
	return store.UpsertSchedule(ctx, runtime.Schedule{
		AgentID:   "empire-coordinator",
		EventType: "timer.portfolio_digest",
		Mode:      "cron",
		Cron:      cron,
		Payload:   payload,
	})
}

func ensureMarginalReviewSchedule(ctx context.Context, store runtime.SchedulePersistence) error {
	if store == nil {
		return nil
	}
	cron := strings.TrimSpace(os.Getenv("EMPIREAI_MARGINAL_REVIEW_CRON"))
	if cron == "" {
		// Every 14 days at 09:00 UTC by default.
		cron = "0 9 */14 * *"
	}
	payload := []byte(`{"trigger":"marginal_review"}`)
	return store.UpsertSchedule(ctx, runtime.Schedule{
		AgentID:   "empire-coordinator",
		EventType: "timer.marginal_review",
		Mode:      "cron",
		Cron:      cron,
		Payload:   payload,
	})
}

func ensureInfraHealthCheckSchedule(ctx context.Context, store runtime.SchedulePersistence) error {
	if store == nil {
		return nil
	}
	cron := strings.TrimSpace(os.Getenv("EMPIREAI_INFRA_HEALTH_CRON"))
	if cron == "" {
		// Hourly health check.
		cron = "0 * * * *"
	}
	payload := []byte(`{"trigger":"infra_health_check"}`)
	return store.UpsertSchedule(ctx, runtime.Schedule{
		AgentID:   "holding-devops",
		EventType: "timer.infra_health_check",
		Mode:      "cron",
		Cron:      cron,
		Payload:   payload,
	})
}

func portfolioDigestLoop(
	ctx context.Context,
	bus *runtime.EventBus,
	digestStore runtime.DigestPersistence,
	mailboxStore runtime.MailboxPersistence,
) {
	if bus == nil || digestStore == nil || mailboxStore == nil {
		return
	}

	topN := 10
	if raw := strings.TrimSpace(os.Getenv("EMPIREAI_DIGEST_TOPN")); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 && v <= 100 {
			topN = v
		}
	}

	var tg *mailbox.TelegramNotifier
	{
		tgToken := telegramBotTokenFromEnv()
		tgChat := telegramChatIDFromEnv()
		if tgToken != "" && tgChat != "" {
			tg = &mailbox.TelegramNotifier{
				BotToken: tgToken,
				ChatID:   tgChat,
				BaseURL:  telegramBaseURLFromEnv(),
			}
		}
	}

	compile := func(trigger string, triggerEvent events.Event) {
		trigger = strings.TrimSpace(trigger)
		if trigger == "" {
			trigger = string(triggerEvent.Type)
		}
		snap, err := digest.BuildSnapshot(context.Background(), digestStore, mailboxStore, topN)
		if err != nil {
			log.Printf("portfolio digest compile failed trigger=%s err=%v", trigger, err)
			return
		}
		text := digest.RenderText(snap)

		payload, _ := json.Marshal(map[string]any{
			"digest_text":           text,
			"trigger_reason":        trigger,
			"trigger_event_id":      triggerEvent.ID,
			"action_required_count": snap.MailboxPending,
			"snapshot": map[string]any{
				"active_verticals": snap.ActiveVerticals,
				"mailbox_pending":  snap.MailboxPending,
				"mailbox_critical": snap.MailboxCritical,
				"top_verticals":    snap.TopVerticals,
			},
			"compiled_at": time.Now().UTC().Format(time.RFC3339),
		})
		evt := events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("portfolio.digest_compiled"),
			SourceAgent: "empire-coordinator",
			Payload:     payload,
			CreatedAt:   time.Now(),
		}
		if err := bus.Publish(context.Background(), evt); err != nil {
			log.Printf("portfolio digest publish failed: %v", err)
		}

		// Delivery: Telegram push (compact summary) if configured.
		if tg != nil {
			compact := renderCompactDigest(trigger, snap)
			sendCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := tg.NotifyText(sendCtx, compact)
			cancel()
			if err != nil {
				log.Printf("portfolio digest telegram push failed: %v", err)
			}
		}
	}

	// Initial heartbeat confirms the system is alive after runtime bootstrap.
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
			compile("startup", events.Event{ID: "", Type: "system.started"})
		}
	}()

	ch := bus.Subscribe(
		"portfolio-digest-manager",
		events.EventType("timer.portfolio_digest"),
		events.EventType("mailbox.critical_notified"),
		events.EventType("opco.ceo_report"),
		// Back-compat triggers that exist in the current event vocabulary.
		events.EventType("product_report"),
		events.EventType("growth_report"),
		events.EventType("cross_domain_report"),
	)
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-ch:
			if !ok {
				return
			}
			compile(string(evt.Type), evt)
		}
	}
}

func renderCompactDigest(trigger string, snap digest.Snapshot) string {
	var b strings.Builder
	b.WriteString("[EmpireAI] portfolio digest\n")
	b.WriteString(fmt.Sprintf("trigger=%s\n", strings.TrimSpace(trigger)))
	b.WriteString(fmt.Sprintf("action_required: pending=%d critical=%d\n", snap.MailboxPending, snap.MailboxCritical))
	b.WriteString(fmt.Sprintf("active_verticals: %d\n", snap.ActiveVerticals))
	b.WriteString("top_verticals:\n")
	for _, v := range snap.TopVerticals {
		b.WriteString(fmt.Sprintf("- %s (%s) users=%d mrr=$%.2f\n", v.Name, v.Stage, v.UsersTotal, float64(v.MRRCents)/100.0))
	}
	b.WriteString("Full: run `empire digest`\n")
	return b.String()
}

func verticalHealthMonitorLoop(ctx context.Context, bus *runtime.EventBus, db *sql.DB, mailboxStore runtime.MailboxPersistence) {
	if bus == nil || db == nil {
		return
	}
	ch := bus.Subscribe(
		"vertical-health-monitor",
		events.EventType("opco.ceo_report"),
		// Back-compat: VP/CoS reports can still drive health evaluation if CEO synthesis isn't emitted yet.
		events.EventType("product_report"),
		events.EventType("growth_report"),
		events.EventType("cross_domain_report"),
	)
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-ch:
			if !ok {
				return
			}
			verticalID := strings.TrimSpace(evt.VerticalID)
			if verticalID == "" {
				var p struct {
					VerticalID string `json:"vertical_id"`
				}
				_ = json.Unmarshal(evt.Payload, &p)
				verticalID = strings.TrimSpace(p.VerticalID)
			}
			if verticalID == "" {
				continue
			}

			// Emit steady-state when the metrics indicate stabilization.
			if err := maybeEmitSteadyState(ctx, bus, db, verticalID); err != nil {
				log.Printf("steady-state evaluation failed vertical=%s err=%v", verticalID, err)
			}

			w, ok, err := evaluateVerticalHealth(ctx, db, verticalID)
			if err != nil {
				log.Printf("health evaluation failed vertical=%s err=%v", verticalID, err)
				continue
			}
			if !ok {
				continue
			}

			payload, _ := json.Marshal(map[string]any{
				"vertical_id":      verticalID,
				"severity":         w.Severity,
				"breached_metrics": w.BreachedMetrics,
				"trend_data":       w.TrendData,
				"recommendation":   w.Recommendation,
				"evaluated_at":     time.Now().UTC().Format(time.RFC3339),
			})
			_ = bus.Publish(context.Background(), events.Event{
				ID:          uuid.NewString(),
				Type:        events.EventType("vertical.health_warning"),
				SourceAgent: "empire-coordinator",
				Payload:     payload,
				CreatedAt:   time.Now(),
			})

			if mailboxStore != nil {
				priority := "normal"
				if w.Severity == "red" {
					priority = "critical"
				}
				summary := fmt.Sprintf("Vertical health %s: %s", strings.ToUpper(w.Severity), w.Recommendation)
				_, _ = mailboxStore.InsertMailboxItem(context.Background(), runtime.MailboxItem{
					ID:         uuid.NewString(),
					VerticalID: verticalID,
					FromAgent:  "empire-coordinator",
					Type:       "escalation",
					Priority:   priority,
					Status:     "pending",
					Summary:    summary,
					Context:    payload,
					TimeoutAt:  time.Now().Add(7 * 24 * time.Hour),
				})
			}
		}
	}
}

type healthWarning struct {
	Severity        string
	BreachedMetrics []map[string]any
	TrendData       map[string]any
	Recommendation  string
}

func evaluateVerticalHealth(ctx context.Context, db *sql.DB, verticalID string) (healthWarning, bool, error) {
	var launchedAt sql.NullTime
	var stage string
	if err := db.QueryRowContext(ctx, `
		SELECT launched_at, stage
		FROM verticals
		WHERE id = $1::uuid
	`, verticalID).Scan(&launchedAt, &stage); err != nil {
		return healthWarning{}, false, err
	}
	if !launchedAt.Valid {
		// Pre-launch verticals do not participate in health thresholds.
		return healthWarning{}, false, nil
	}
	weeksSinceLaunch := int(time.Since(launchedAt.Time) / (7 * 24 * time.Hour))
	if weeksSinceLaunch < 0 {
		weeksSinceLaunch = 0
	}

	// Latest metrics snapshot.
	var users int
	var mrrCents int
	var csat sql.NullFloat64
	var usersChurned int
	var periodEnd time.Time
	if err := db.QueryRowContext(ctx, `
		SELECT users_total, mrr_cents, csat_avg, users_churned, period_end
		FROM vertical_metrics
		WHERE vertical_id = $1::uuid
		ORDER BY period_end DESC
		LIMIT 1
	`, verticalID).Scan(&users, &mrrCents, &csat, &usersChurned, &periodEnd); err != nil {
		if err == sql.ErrNoRows {
			return healthWarning{}, false, nil
		}
		return healthWarning{}, false, err
	}

	var spend30d int
	_ = db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(amount_cents), 0)
		FROM spend_ledger
		WHERE vertical_id = $1::uuid
		  AND created_at >= now() - interval '30 days'
	`, verticalID).Scan(&spend30d)

	breached := make([]map[string]any, 0, 5)
	severity := "yellow"
	recommendation := "invest"

	// Users thresholds.
	if weeksSinceLaunch >= 10 && users < 3 {
		breached = append(breached, map[string]any{"metric": "users", "value": users, "threshold": "<3 paying after 10 weeks"})
		severity = "red"
		recommendation = "kill"
	} else if weeksSinceLaunch >= 6 && users < 5 {
		breached = append(breached, map[string]any{"metric": "users", "value": users, "threshold": "<5 paying after 6 weeks"})
	}

	// Unit economics: rough guardrail using 30d spend vs current MRR.
	if mrrCents > 0 {
		if spend30d > 2*mrrCents {
			breached = append(breached, map[string]any{"metric": "unit_economics", "value": spend30d, "threshold": "cost > 2x revenue (30d)"})
			severity = "red"
			recommendation = "kill"
		} else if spend30d > mrrCents {
			breached = append(breached, map[string]any{"metric": "unit_economics", "value": spend30d, "threshold": "cost > revenue (30d)"})
		}
	}

	// Support (CSAT).
	if csat.Valid {
		if csat.Float64 < 2.5 {
			breached = append(breached, map[string]any{"metric": "support", "value": csat.Float64, "threshold": "CSAT < 2.5"})
			severity = "red"
			recommendation = "pivot_or_kill"
		} else if csat.Float64 < 3.0 {
			breached = append(breached, map[string]any{"metric": "support", "value": csat.Float64, "threshold": "CSAT < 3.0"})
			if recommendation == "invest" {
				recommendation = "pivot"
			}
		}
	}

	// Churn heuristic: if churned users is high relative to current base.
	if users > 0 {
		churnRate := float64(usersChurned) / float64(users)
		if churnRate > 0.30 {
			breached = append(breached, map[string]any{"metric": "churn", "value": churnRate, "threshold": ">30% (period)"})
			severity = "red"
			recommendation = "pivot_or_kill"
		} else if churnRate > 0.25 {
			breached = append(breached, map[string]any{"metric": "churn", "value": churnRate, "threshold": ">25% (period)"})
			if recommendation == "invest" {
				recommendation = "pivot"
			}
		}
	}

	if len(breached) == 0 {
		return healthWarning{}, false, nil
	}
	return healthWarning{
		Severity:        severity,
		BreachedMetrics: breached,
		TrendData: map[string]any{
			"weeks_since_launch": weeksSinceLaunch,
			"stage":              stage,
			"users_total":        users,
			"mrr_cents":          mrrCents,
			"spend_30d_cents":    spend30d,
			"metrics_period_end": periodEnd.Format("2006-01-02"),
		},
		Recommendation: recommendation,
	}, true, nil
}

func maybeEmitSteadyState(ctx context.Context, bus *runtime.EventBus, db *sql.DB, verticalID string) error {
	if bus == nil || db == nil {
		return nil
	}
	// Only emit once.
	var exists int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE type = 'opco.steady_state_reached'
		  AND vertical_id = $1::uuid
	`, verticalID).Scan(&exists); err == nil && exists > 0 {
		return nil
	}

	var launchedAt sql.NullTime
	if err := db.QueryRowContext(ctx, `SELECT launched_at FROM verticals WHERE id = $1::uuid`, verticalID).Scan(&launchedAt); err != nil {
		return err
	}
	if !launchedAt.Valid {
		return nil
	}
	weeks := int(time.Since(launchedAt.Time) / (7 * 24 * time.Hour))
	if weeks < 4 {
		return nil
	}

	var users int
	var mrrCents int
	if err := db.QueryRowContext(ctx, `
		SELECT users_total, mrr_cents
		FROM vertical_metrics
		WHERE vertical_id = $1::uuid
		ORDER BY period_end DESC
		LIMIT 1
	`, verticalID).Scan(&users, &mrrCents); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return err
	}
	if users <= 0 || mrrCents <= 0 {
		return nil
	}

	var spend30d int
	_ = db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(amount_cents), 0)
		FROM spend_ledger
		WHERE vertical_id = $1::uuid
		  AND created_at >= now() - interval '30 days'
	`, verticalID).Scan(&spend30d)

	payload, _ := json.Marshal(map[string]any{
		"vertical_id":        verticalID,
		"weeks_since_launch": weeks,
		"current_metrics": map[string]any{
			"users_total":     users,
			"mrr_cents":       mrrCents,
			"spend_30d_cents": spend30d,
			"launched_at":     launchedAt.Time.UTC().Format(time.RFC3339),
		},
		"emitted_at": time.Now().UTC().Format(time.RFC3339),
	})

	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("opco.steady_state_reached"),
		SourceAgent: fmt.Sprintf("opco-ceo-%s", verticalID),
		VerticalID:  verticalID,
		Payload:     payload,
		CreatedAt:   time.Now(),
	}
	return bus.Publish(context.Background(), evt)
}

func budgetHeartbeatLoop(ctx context.Context, tracker *runtime.BudgetTracker) {
	if tracker == nil {
		return
	}
	ticker := time.NewTicker(15 * time.Minute)
	defer ticker.Stop()

	eval := func() {
		eCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		tracker.EvaluateAll(eCtx)
		cancel()
	}
	eval()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			eval()
		}
	}
}
