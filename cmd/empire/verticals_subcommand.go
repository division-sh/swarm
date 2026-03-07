package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"strings"
	"time"

	"empireai/internal/config"
	"empireai/internal/runtime/workspace"
)

func runAgentsSubcommand(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: empire agents <vertical-id|slug> [flags]")
	}
	target := strings.TrimSpace(args[0])
	if target == "" {
		return fmt.Errorf("usage: empire agents <vertical-id|slug> [flags]")
	}
	return runVerticalTeamSubcommand(target, args[1:])
}

func runVerticalsSubcommand(args []string) error {
	if len(args) == 0 {
		args = []string{"list"}
	}
	switch args[0] {
	case "list":
		return runVerticalsListSubcommand(args[1:], false)
	case "operating":
		return runVerticalsListSubcommand(args[1:], true)
	default:
		return fmt.Errorf("usage: empire verticals <list|operating> [flags]")
	}
}

func runVerticalsListSubcommand(args []string, operatingOnly bool) error {
	fs := flag.NewFlagSet("verticals list", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	limit := fs.Int("limit", 100, "Row limit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	if stores.SQLDB == nil {
		return fmt.Errorf("verticals command requires persistent store mode (use -store postgres)")
	}
	query := `
		SELECT id::text, name, COALESCE(slug,''), stage, mode
		FROM verticals
	`
	if operatingOnly {
		query += ` WHERE mode = 'operating'`
	}
	query += ` ORDER BY created_at DESC LIMIT $1`
	rows, err := stores.SQLDB.QueryContext(ctx, query, *limit)
	if err != nil {
		return fmt.Errorf("list verticals: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, name, slug, stage, mode string
		if err := rows.Scan(&id, &name, &slug, &stage, &mode); err != nil {
			return fmt.Errorf("scan vertical row: %w", err)
		}
		fmt.Printf("- id=%s slug=%s mode=%s stage=%s name=%s\n", id, slug, mode, stage, name)
	}
	return rows.Err()
}

func runVerticalSubcommand(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: empire vertical <id|slug> <metrics|team|logs|kill> [flags]")
	}
	target := strings.TrimSpace(args[0])
	sub := strings.TrimSpace(args[1])
	switch sub {
	case "metrics":
		return runVerticalMetricsSubcommand(target, args[2:])
	case "team":
		return runVerticalTeamSubcommand(target, args[2:])
	case "logs":
		return runVerticalLogsSubcommand(target, args[2:])
	case "kill":
		return runVerticalKillSubcommand(target, args[2:])
	default:
		return fmt.Errorf("unknown vertical subcommand: %s", sub)
	}
}

func runVerticalMetricsSubcommand(target string, args []string) error {
	fs := flag.NewFlagSet("vertical metrics", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	if stores.SQLDB == nil {
		return fmt.Errorf("vertical metrics requires persistent store mode (use -store postgres)")
	}
	verticalID, err := resolveVerticalID(ctx, stores.SQLDB, target)
	if err != nil {
		return err
	}
	var users, mrr, apiCost, infraCost int
	err = stores.SQLDB.QueryRowContext(ctx, `
		SELECT users_total, mrr_cents, api_cost_cents, infra_cost_cents
		FROM vertical_metrics
		WHERE vertical_id = $1::uuid
		ORDER BY period_end DESC
		LIMIT 1
	`, verticalID).Scan(&users, &mrr, &apiCost, &infraCost)
	if err == sql.ErrNoRows {
		fmt.Printf("no metrics found for vertical %s\n", verticalID)
		return nil
	}
	if err != nil {
		return fmt.Errorf("load vertical metrics: %w", err)
	}
	fmt.Printf("vertical metrics\nid: %s\nusers_total: %d\nmrr_cents: %d\napi_cost_cents: %d\ninfra_cost_cents: %d\n",
		verticalID, users, mrr, apiCost, infraCost)
	return nil
}

func runVerticalTeamSubcommand(target string, args []string) error {
	fs := flag.NewFlagSet("vertical team", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	if stores.SQLDB == nil {
		return fmt.Errorf("vertical team requires persistent store mode (use -store postgres)")
	}
	verticalID, err := resolveVerticalID(ctx, stores.SQLDB, target)
	if err != nil {
		return err
	}
	rows, err := stores.SQLDB.QueryContext(ctx, `
		SELECT id, role, status, COALESCE(parent_agent_id, '')
		FROM agents
		WHERE vertical_id = $1::uuid
		ORDER BY started_at ASC
	`, verticalID)
	if err != nil {
		return fmt.Errorf("list team agents: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, role, status, parent string
		if err := rows.Scan(&id, &role, &status, &parent); err != nil {
			return fmt.Errorf("scan team row: %w", err)
		}
		fmt.Printf("- id=%s role=%s status=%s parent=%s\n", id, role, status, parent)
	}
	return rows.Err()
}

func runVerticalLogsSubcommand(target string, args []string) error {
	fs := flag.NewFlagSet("vertical logs", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	agent := fs.String("agent", "", "Filter by source agent")
	limit := fs.Int("limit", 20, "Max events")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	if stores.SQLDB == nil {
		return fmt.Errorf("vertical logs requires persistent store mode (use -store postgres)")
	}
	verticalID, err := resolveVerticalID(ctx, stores.SQLDB, target)
	if err != nil {
		return err
	}
	query := `
		SELECT id::text, type, source_agent, created_at
		FROM events
		WHERE vertical_id = $1::uuid
	`
	argsQ := []any{verticalID}
	if strings.TrimSpace(*agent) != "" {
		query += ` AND source_agent = $2`
		argsQ = append(argsQ, strings.TrimSpace(*agent))
		query += ` ORDER BY created_at DESC LIMIT $3`
		argsQ = append(argsQ, *limit)
	} else {
		query += ` ORDER BY created_at DESC LIMIT $2`
		argsQ = append(argsQ, *limit)
	}
	rows, err := stores.SQLDB.QueryContext(ctx, query, argsQ...)
	if err != nil {
		return fmt.Errorf("list vertical logs: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, typ, source string
		var created time.Time
		if err := rows.Scan(&id, &typ, &source, &created); err != nil {
			return fmt.Errorf("scan event row: %w", err)
		}
		fmt.Printf("- id=%s type=%s source=%s at=%s\n", id, typ, source, created.UTC().Format(time.RFC3339))
	}
	return rows.Err()
}

func runVerticalKillSubcommand(target string, args []string) error {
	fs := flag.NewFlagSet("vertical kill", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	notes := fs.String("notes", "", "Kill notes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	if stores.SQLDB == nil {
		return fmt.Errorf("vertical kill requires persistent store mode (use -store postgres)")
	}
	verticalID, err := resolveVerticalID(ctx, stores.SQLDB, target)
	if err != nil {
		return err
	}
	if _, err := stores.SQLDB.ExecContext(ctx, `
		UPDATE verticals
		SET stage = 'winding_down',
		    kill_reason = NULLIF($2,''),
		    updated_at = now()
		WHERE id = $1::uuid
	`, verticalID, strings.TrimSpace(*notes)); err != nil {
		return fmt.Errorf("kill vertical: %w", err)
	}
	if envBool("EMPIREAI_ENABLE_DOCKER_WORKSPACES", true) {
		workspaces := workspace.NewDockerManager(stores.SQLDB)
		if err := workspaces.StopVerticalWorkspace(ctx, verticalID); err != nil && envBool("EMPIREAI_REQUIRE_DOCKER_WORKSPACES", false) {
			return fmt.Errorf("stop vertical workspace: %w", err)
		}
	}
	fmt.Printf("vertical marked winding_down id=%s\n", verticalID)
	return nil
}

func runDeploymentsSubcommand(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: empire deployments <list|health> [flags]")
	}
	switch args[0] {
	case "list":
		return runDeploymentsListSubcommand(args[1:])
	case "health":
		return runDeploymentsHealthSubcommand(args[1:])
	default:
		return fmt.Errorf("unknown deployments subcommand: %s", args[0])
	}
}

func runDeploymentsListSubcommand(args []string) error {
	fs := flag.NewFlagSet("deployments list", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	limit := fs.Int("limit", 50, "Row limit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	if stores.SQLDB == nil {
		return fmt.Errorf("deployments list requires persistent store mode (use -store postgres)")
	}
	rows, err := stores.SQLDB.QueryContext(ctx, `
		SELECT id::text, COALESCE(vertical_id::text,''), status, COALESCE(url,''), COALESCE(environment,'production'), COALESCE(version,1), COALESCE(health_status,'unknown')
		FROM deployments
		ORDER BY created_at DESC
		LIMIT $1
	`, *limit)
	if err != nil {
		return fmt.Errorf("list deployments: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, verticalID, status, url, env, health string
		var version int
		if err := rows.Scan(&id, &verticalID, &status, &url, &env, &version, &health); err != nil {
			return fmt.Errorf("scan deployment row: %w", err)
		}
		fmt.Printf("- id=%s vertical=%s env=%s version=%d status=%s health=%s url=%s\n", id, verticalID, env, version, status, health, url)
	}
	return rows.Err()
}

func runDeploymentsHealthSubcommand(args []string) error {
	fs := flag.NewFlagSet("deployments health", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	if stores.SQLDB == nil {
		return fmt.Errorf("deployments health requires persistent store mode (use -store postgres)")
	}
	rows, err := stores.SQLDB.QueryContext(ctx, `
		SELECT COALESCE(health_status, 'unknown') AS health, COUNT(*)
		FROM deployments
		GROUP BY health
		ORDER BY health
	`)
	if err != nil {
		return fmt.Errorf("deployment health aggregation: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var health string
		var count int
		if err := rows.Scan(&health, &count); err != nil {
			return fmt.Errorf("scan health row: %w", err)
		}
		fmt.Printf("- health=%s count=%d\n", health, count)
	}
	return rows.Err()
}

func resolveVerticalID(ctx context.Context, db *sql.DB, target string) (string, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return "", fmt.Errorf("vertical id or slug is required")
	}
	var id string
	if err := db.QueryRowContext(ctx, `
		SELECT id::text
		FROM verticals
		WHERE id::text = $1 OR slug = $1
		ORDER BY CASE WHEN id::text = $1 THEN 0 ELSE 1 END
		LIMIT 1
	`, target).Scan(&id); err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("vertical not found: %s", target)
		}
		return "", fmt.Errorf("resolve vertical id: %w", err)
	}
	return id, nil
}
