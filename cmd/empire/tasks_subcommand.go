package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"time"

	"empireai/internal/config"
	"empireai/internal/events"
	"empireai/internal/runtime"
	"github.com/google/uuid"
)

func claimHumanTask(ctx context.Context, stores storeBundle, taskID, assignedTo string) error {
	taskID = strings.TrimSpace(taskID)
	assignedTo = strings.TrimSpace(assignedTo)
	if taskID == "" {
		return fmt.Errorf("task id is required")
	}
	if assignedTo == "" {
		assignedTo = "founder"
	}

	var requestingAgent string
	var verticalID string
	if err := stores.SQLDB.QueryRowContext(ctx, `
		UPDATE human_tasks
		SET status = 'assigned',
		    assigned_to = $2
		WHERE id = $1::uuid
		RETURNING requesting_agent, COALESCE(vertical_id::text, '')
	`, taskID, assignedTo).Scan(&requestingAgent, &verticalID); err != nil {
		return err
	}

	payload := map[string]any{
		"task_id":          taskID,
		"requesting_agent": strings.TrimSpace(requestingAgent),
		"vertical_id":      strings.TrimSpace(verticalID),
		"assigned_to":      assignedTo,
	}
	if err := appendTargetedEvent(ctx, stores, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("human_task.assigned"),
		SourceAgent: "cli",
		VerticalID:  verticalID,
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now(),
	}, []string{strings.TrimSpace(requestingAgent)}); err != nil {
		return err
	}

	fmt.Printf("task claimed: id=%s assigned_to=%s\n", taskID, assignedTo)
	return nil
}

func completeHumanTask(ctx context.Context, stores storeBundle, taskID, result, outcome string, followUp bool) error {
	taskID = strings.TrimSpace(taskID)
	result = strings.TrimSpace(result)
	outcome = strings.TrimSpace(outcome)
	if taskID == "" {
		return fmt.Errorf("task id is required")
	}
	if result == "" {
		return fmt.Errorf("result is required")
	}
	if outcome == "" {
		outcome = "success"
	}

	var requestingAgent string
	var verticalID string
	if err := stores.SQLDB.QueryRowContext(ctx, `
		UPDATE human_tasks
		SET status = 'completed',
		    result = $2,
		    outcome = $3,
		    follow_up_needed = $4,
		    completed_at = now()
		WHERE id = $1::uuid
		RETURNING requesting_agent, COALESCE(vertical_id::text, '')
	`, taskID, result, outcome, followUp).Scan(&requestingAgent, &verticalID); err != nil {
		return err
	}

	payload := map[string]any{
		"task_id":          taskID,
		"requesting_agent": strings.TrimSpace(requestingAgent),
		"vertical_id":      strings.TrimSpace(verticalID),
		"result_text":      result,
		"outcome":          outcome,
		"follow_up_needed": followUp,
	}
	if err := appendTargetedEvent(ctx, stores, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("human_task.completed"),
		SourceAgent: "cli",
		VerticalID:  verticalID,
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now(),
	}, []string{strings.TrimSpace(requestingAgent)}); err != nil {
		return err
	}

	fmt.Printf("task completed: id=%s outcome=%s follow_up_needed=%v\n", taskID, outcome, followUp)
	return nil
}

func rejectHumanTask(ctx context.Context, stores storeBundle, cfg *config.Config, taskID, reason string) error {
	taskID = strings.TrimSpace(taskID)
	reason = strings.TrimSpace(reason)
	if taskID == "" {
		return fmt.Errorf("task id is required")
	}

	resetDay := "monday"
	if cfg != nil && strings.TrimSpace(cfg.Budget().HumanTasks.BudgetReset) != "" {
		resetDay = strings.TrimSpace(cfg.Budget().HumanTasks.BudgetReset)
	}
	requeueAt := runtime.NextWeekResetUTC(time.Now(), resetDay).UTC().Format(time.RFC3339)

	decisionObj := map[string]any{
		"decision":     "deferred",
		"defer_reason": "human_pushback",
		"human_reason": reason,
		"requeue_date": requeueAt,
		"decided_by":   "human",
		"decided_at":   time.Now().UTC().Format(time.RFC3339),
	}
	decisionJSON, _ := json.Marshal(decisionObj)

	var requestingAgent string
	var verticalID string
	if err := stores.SQLDB.QueryRowContext(ctx, `
		UPDATE human_tasks
		SET status = 'deferred',
		    reviewed_at = now(),
		    review_decision = $2::jsonb,
		    requeue_count = COALESCE(requeue_count, 0) + 1,
		    assigned_to = NULL
		WHERE id = $1::uuid
		RETURNING requesting_agent, COALESCE(vertical_id::text, '')
	`, taskID, string(decisionJSON)).Scan(&requestingAgent, &verticalID); err != nil {
		return err
	}

	payload := map[string]any{
		"task_id":          taskID,
		"requesting_agent": strings.TrimSpace(requestingAgent),
		"vertical_id":      strings.TrimSpace(verticalID),
		"defer_reason":     "human_pushback",
		"human_reason":     reason,
		"requeue_date":     requeueAt,
	}
	if err := appendTargetedEvent(ctx, stores, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("human_task.deferred"),
		SourceAgent: "cli",
		VerticalID:  verticalID,
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now(),
	}, withControlPlaneRecipient(strings.TrimSpace(requestingAgent))); err != nil {
		return err
	}

	fmt.Printf("task rejected (pushback): id=%s requeue_date=%s\n", taskID, requeueAt)
	return nil
}

func runTasksSubcommand(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: empire tasks <list|view|claim|complete|reject|stats> [flags]")
	}
	switch args[0] {
	case "list":
		fs := flag.NewFlagSet("tasks list", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		status := fs.String("status", "open", "Task status filter (open|all|pending_review|approved|assigned|completed|rejected|deferred|expired)")
		category := fs.String("category", "", "Filter by category (optional)")
		outcome := fs.String("outcome", "", "Filter by outcome (optional; completed tasks only)")
		limit := fs.Int("limit", 50, "Max rows")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		if stores.SQLDB == nil {
			return fmt.Errorf("tasks commands require postgres store")
		}
		return printHumanTasks(ctx, stores.SQLDB, strings.TrimSpace(*status), strings.TrimSpace(*category), strings.TrimSpace(*outcome), *limit)
	case "view":
		fs := flag.NewFlagSet("tasks view", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if len(fs.Args()) < 1 {
			return fmt.Errorf("usage: empire tasks view <id>")
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		if stores.SQLDB == nil {
			return fmt.Errorf("tasks commands require postgres store")
		}
		return printHumanTask(ctx, stores.SQLDB, strings.TrimSpace(fs.Args()[0]))
	case "claim":
		fs := flag.NewFlagSet("tasks claim", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		assignedTo := fs.String("assigned-to", "founder", "Human identifier")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if len(fs.Args()) < 1 {
			return fmt.Errorf("usage: empire tasks claim <id> [--assigned-to ...]")
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		if stores.SQLDB == nil || stores.EventStore == nil {
			return fmt.Errorf("tasks claim requires postgres store")
		}
		return claimHumanTask(ctx, stores, strings.TrimSpace(fs.Args()[0]), strings.TrimSpace(*assignedTo))
	case "complete":
		fs := flag.NewFlagSet("tasks complete", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		result := fs.String("result", "", "Result text")
		outcome := fs.String("outcome", "success", "Outcome (success|partial|failed)")
		followUp := fs.Bool("follow-up", false, "Whether follow-up is needed")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if len(fs.Args()) < 1 {
			return fmt.Errorf("usage: empire tasks complete <id> --result \"...\" [--outcome success|partial|failed] [--follow-up]")
		}
		if strings.TrimSpace(*result) == "" {
			return fmt.Errorf("--result is required")
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		if stores.SQLDB == nil || stores.EventStore == nil {
			return fmt.Errorf("tasks complete requires postgres store")
		}
		return completeHumanTask(ctx, stores, strings.TrimSpace(fs.Args()[0]), strings.TrimSpace(*result), strings.TrimSpace(*outcome), *followUp)
	case "reject":
		fs := flag.NewFlagSet("tasks reject", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		reason := fs.String("reason", "", "Human pushback reason")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if len(fs.Args()) < 1 {
			return fmt.Errorf("usage: empire tasks reject <id> [--reason \"...\"]")
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		if stores.SQLDB == nil || stores.EventStore == nil {
			return fmt.Errorf("tasks reject requires postgres store")
		}
		return rejectHumanTask(ctx, stores, cfg, strings.TrimSpace(fs.Args()[0]), strings.TrimSpace(*reason))
	case "stats":
		fs := flag.NewFlagSet("tasks stats", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		if stores.SQLDB == nil {
			return fmt.Errorf("tasks stats requires postgres store")
		}
		return printHumanTaskStats(ctx, stores.SQLDB, cfg)
	default:
		return fmt.Errorf("unknown tasks command: %s", args[0])
	}
}

func printHumanTasks(ctx context.Context, db *sql.DB, status string, category string, outcome string, limit int) error {
	if db == nil {
		return fmt.Errorf("db unavailable")
	}
	status = strings.TrimSpace(status)
	category = strings.TrimSpace(category)
	outcome = strings.TrimSpace(outcome)
	if status == "" {
		status = "open"
	}
	if limit <= 0 {
		limit = 50
	}

	where := []string{"1=1"}
	args := []any{}
	if status != "all" && status != "open" {
		args = append(args, status)
		where = append(where, fmt.Sprintf("t.status = $%d", len(args)))
	} else if status == "open" {
		where = append(where, "t.status IN ('pending_review', 'approved', 'assigned')")
	}
	if category != "" {
		args = append(args, category)
		where = append(where, fmt.Sprintf("t.category = $%d", len(args)))
	}
	if outcome != "" {
		args = append(args, outcome)
		where = append(where, fmt.Sprintf("COALESCE(t.outcome,'') = $%d", len(args)))
	}
	args = append(args, limit)
	q := fmt.Sprintf(`
		SELECT
			t.id::text,
			t.status,
			COALESCE(t.priority, 'medium'),
			t.category,
			COALESCE(v.slug, ''),
			COALESCE(t.vertical_id::text, ''),
			t.requesting_agent,
			COALESCE(t.assigned_to, ''),
			t.created_at,
			t.deadline
		FROM human_tasks t
		LEFT JOIN verticals v ON v.id = t.vertical_id
		WHERE %s
		ORDER BY t.created_at DESC
		LIMIT $%d
	`, strings.Join(where, " AND "), len(args))
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	fmt.Printf("tasks: status=%s\n", status)
	fmt.Printf("%-10s  %-14s  %-6s  %-18s  %-16s  %-16s  %-20s  %-12s  %-20s  %-20s\n",
		"id", "status", "prio", "category", "vertical", "vertical_id", "requesting_agent", "assigned_to", "created_at", "deadline")
	for rows.Next() {
		var id, st, prio, cat, slug, vid, reqAgent, assigned string
		var created time.Time
		var deadline sql.NullTime
		if err := rows.Scan(&id, &st, &prio, &cat, &slug, &vid, &reqAgent, &assigned, &created, &deadline); err != nil {
			return err
		}
		vert := slug
		if strings.TrimSpace(vert) == "" {
			vert = "-"
		}
		deadlineText := "-"
		if deadline.Valid {
			deadlineText = deadline.Time.UTC().Format(time.RFC3339)
		}
		fmt.Printf("%-10s  %-14s  %-6s  %-18s  %-16s  %-16s  %-20s  %-12s  %-20s  %-20s\n",
			id[:min(10, len(id))],
			st,
			prio,
			cat,
			vert,
			trunc(vid, 16),
			trunc(reqAgent, 20),
			trunc(assigned, 12),
			created.UTC().Format(time.RFC3339),
			deadlineText,
		)
	}
	return rows.Err()
}

func printHumanTask(ctx context.Context, db *sql.DB, taskID string) error {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return fmt.Errorf("task id is required")
	}
	var (
		id, requesting, verticalID, slug, category, description, priority, status, assignedTo, result, outcome string
		created                                                                                                time.Time
		deadline, reviewed, completed                                                                          sql.NullTime
		followUp                                                                                               bool
	)
	err := db.QueryRowContext(ctx, `
		SELECT
			t.id::text,
			t.requesting_agent,
			COALESCE(t.vertical_id::text, ''),
			COALESCE(v.slug, ''),
			t.category,
			t.description,
			COALESCE(t.priority, 'medium'),
			t.status,
			COALESCE(t.assigned_to, ''),
			COALESCE(t.result, ''),
			COALESCE(t.outcome, ''),
			COALESCE(t.follow_up_needed, false),
			t.created_at,
			t.deadline,
			t.reviewed_at,
			t.completed_at
		FROM human_tasks t
		LEFT JOIN verticals v ON v.id = t.vertical_id
		WHERE t.id = $1::uuid
	`, taskID).Scan(
		&id, &requesting, &verticalID, &slug, &category, &description, &priority, &status,
		&assignedTo, &result, &outcome, &followUp, &created, &deadline, &reviewed, &completed,
	)
	if err != nil {
		return err
	}
	fmt.Printf("id: %s\n", id)
	fmt.Printf("status: %s\n", status)
	fmt.Printf("priority: %s\n", priority)
	fmt.Printf("category: %s\n", category)
	fmt.Printf("vertical: %s (%s)\n", slug, verticalID)
	fmt.Printf("requesting_agent: %s\n", requesting)
	fmt.Printf("assigned_to: %s\n", assignedTo)
	fmt.Printf("created_at: %s\n", created.UTC().Format(time.RFC3339))
	if deadline.Valid {
		fmt.Printf("deadline: %s\n", deadline.Time.UTC().Format(time.RFC3339))
	}
	if reviewed.Valid {
		fmt.Printf("reviewed_at: %s\n", reviewed.Time.UTC().Format(time.RFC3339))
	}
	if completed.Valid {
		fmt.Printf("completed_at: %s\n", completed.Time.UTC().Format(time.RFC3339))
	}
	fmt.Printf("follow_up_needed: %v\n", followUp)
	fmt.Printf("\nDESCRIPTION:\n%s\n", description)
	if result != "" {
		fmt.Printf("\nRESULT:\n%s\n", result)
		fmt.Printf("\nOUTCOME:\n%s\n", outcome)
	}
	return nil
}

func printHumanTaskStats(ctx context.Context, db *sql.DB, cfg *config.Config) error {
	if db == nil {
		return fmt.Errorf("db unavailable")
	}
	resetDay := "monday"
	maxPerWeek := 0
	if cfg != nil {
		if strings.TrimSpace(cfg.Budget().HumanTasks.BudgetReset) != "" {
			resetDay = strings.TrimSpace(cfg.Budget().HumanTasks.BudgetReset)
		}
		maxPerWeek = cfg.Budget().HumanTasks.MaxTasksPerWeek
	}
	weekStart := runtime.WeekStartUTC(time.Now(), resetDay)
	var approvedThisWeek int
	_ = db.QueryRowContext(ctx, `
		SELECT COALESCE(count(*), 0)
		FROM human_tasks
		WHERE reviewed_at >= $1
		  AND status IN ('approved', 'assigned', 'completed')
	`, weekStart).Scan(&approvedThisWeek)

	rows, err := db.QueryContext(ctx, `
		SELECT category, COALESCE(status,''), COALESCE(outcome,''), count(*)
		FROM human_tasks
		WHERE created_at >= now() - interval '30 days'
		GROUP BY category, COALESCE(status,''), COALESCE(outcome,'')
		ORDER BY category ASC
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	type stat struct {
		Category string
		Status   string
		Outcome  string
		Count    int
	}
	stats := make([]stat, 0, 64)
	for rows.Next() {
		var s stat
		if err := rows.Scan(&s.Category, &s.Status, &s.Outcome, &s.Count); err != nil {
			return err
		}
		stats = append(stats, s)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	fmt.Printf("tasks stats (30d)\n")
	fmt.Printf("weekly budget: %d/%d (week_start=%s reset=%s)\n", approvedThisWeek, maxPerWeek, weekStart.UTC().Format(time.RFC3339), resetDay)
	fmt.Printf("%-18s  %-12s  %-10s  %-6s\n", "category", "status", "outcome", "count")
	for _, s := range stats {
		fmt.Printf("%-18s  %-12s  %-10s  %-6d\n", s.Category, s.Status, s.Outcome, s.Count)
	}
	return nil
}

func trunc(v string, n int) string {
	v = strings.TrimSpace(v)
	if n <= 0 {
		return v
	}
	if len(v) <= n {
		return v
	}
	return v[:n]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
