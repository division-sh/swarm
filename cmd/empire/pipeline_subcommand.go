package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"strings"
	"time"

	"empireai/internal/config"
)

func runPipelineSubcommand(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: empire pipeline <status|trace|campaigns|stuck|drops> [flags]")
	}
	switch args[0] {
	case "status":
		fs := flag.NewFlagSet("pipeline status", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if fs.NArg() < 1 {
			return fmt.Errorf("usage: empire pipeline status <vertical_id_or_slug>")
		}
		stores, ctx, err := openPipelineStores(*cfgPath, *storeMode, *migrate, *migrationFile)
		if err != nil {
			return err
		}
		return printPipelineStatus(ctx, stores.SQLDB, fs.Arg(0))
	case "trace":
		fs := flag.NewFlagSet("pipeline trace", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		last := fs.Int("last", 40, "Max transitions")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if fs.NArg() < 1 {
			return fmt.Errorf("usage: empire pipeline trace <vertical_id_or_slug> [--last N]")
		}
		if *last <= 0 {
			*last = 40
		}
		stores, ctx, err := openPipelineStores(*cfgPath, *storeMode, *migrate, *migrationFile)
		if err != nil {
			return err
		}
		return printPipelineTrace(ctx, stores.SQLDB, fs.Arg(0), *last)
	case "campaigns":
		fs := flag.NewFlagSet("pipeline campaigns", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		limit := fs.Int("limit", 100, "Max campaigns")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		stores, ctx, err := openPipelineStores(*cfgPath, *storeMode, *migrate, *migrationFile)
		if err != nil {
			return err
		}
		return printPipelineCampaigns(ctx, stores.SQLDB, *limit)
	case "stuck":
		fs := flag.NewFlagSet("pipeline stuck", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		threshold := fs.String("threshold", "1h", "Threshold duration")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		d, err := time.ParseDuration(strings.TrimSpace(*threshold))
		if err != nil || d <= 0 {
			return fmt.Errorf("invalid threshold: %q", *threshold)
		}
		stores, ctx, err := openPipelineStores(*cfgPath, *storeMode, *migrate, *migrationFile)
		if err != nil {
			return err
		}
		return printPipelineStuck(ctx, stores.SQLDB, d)
	case "drops":
		fs := flag.NewFlagSet("pipeline drops", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		last := fs.String("last", "24h", "Window duration")
		vertical := fs.String("vertical", "", "Optional vertical id or slug")
		limit := fs.Int("limit", 200, "Max rows")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		d, err := time.ParseDuration(strings.TrimSpace(*last))
		if err != nil || d <= 0 {
			return fmt.Errorf("invalid --last: %q", *last)
		}
		stores, ctx, err := openPipelineStores(*cfgPath, *storeMode, *migrate, *migrationFile)
		if err != nil {
			return err
		}
		return printPipelineDrops(ctx, stores.SQLDB, d, strings.TrimSpace(*vertical), *limit)
	default:
		return fmt.Errorf("usage: empire pipeline <status|trace|campaigns|stuck|drops> [flags]")
	}
}

func openPipelineStores(cfgPath, storeMode string, migrate bool, migrationFile string) (storeBundle, context.Context, error) {
	ctx := context.Background()
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return storeBundle{}, nil, err
	}
	stores := buildStores(ctx, storeMode, cfg, migrate, migrationFile)
	if stores.SQLDB == nil {
		return storeBundle{}, nil, fmt.Errorf("pipeline commands require persistent store mode (use -store postgres)")
	}
	return stores, ctx, nil
}

func resolvePipelineVertical(ctx context.Context, db *sql.DB, idOrSlug string) (string, string, string, time.Time, error) {
	var id, slug, stage string
	var updated time.Time
	if err := db.QueryRowContext(ctx, `
		SELECT id::text, COALESCE(slug,''), stage, updated_at
		FROM verticals
		WHERE id::text = $1 OR slug = $1
		LIMIT 1
	`, strings.TrimSpace(idOrSlug)).Scan(&id, &slug, &stage, &updated); err != nil {
		if err == sql.ErrNoRows {
			return "", "", "", time.Time{}, fmt.Errorf("vertical not found: %s", idOrSlug)
		}
		return "", "", "", time.Time{}, err
	}
	return id, slug, stage, updated, nil
}

func printPipelineStatus(ctx context.Context, db *sql.DB, idOrSlug string) error {
	id, slug, stage, updatedAt, err := resolvePipelineVertical(ctx, db, idOrSlug)
	if err != nil {
		return err
	}
	var transitionCount int
	_ = db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM pipeline_transitions
		WHERE pipeline_id = $1::uuid
	`, id).Scan(&transitionCount)
	var lastType, lastAction, dropReason string
	var lastAt time.Time
	_ = db.QueryRowContext(ctx, `
		SELECT COALESCE(event_type,''), COALESCE(action,''), COALESCE(drop_reason,''), created_at
		FROM pipeline_transitions
		WHERE pipeline_id = $1::uuid
		ORDER BY created_at DESC
		LIMIT 1
	`, id).Scan(&lastType, &lastAction, &dropReason, &lastAt)

	fmt.Printf("Vertical: %s (%s)\n", nullable(slug, id), id)
	fmt.Printf("Status: %s\n", stage)
	fmt.Printf("Updated: %s\n", updatedAt.UTC().Format(time.RFC3339))
	fmt.Printf("Transitions: %d\n", transitionCount)
	if !lastAt.IsZero() {
		fmt.Printf("Last transition: %s action=%s at=%s\n", nullable(lastType, "-"), nullable(lastAction, "-"), lastAt.UTC().Format(time.RFC3339))
		if strings.TrimSpace(dropReason) != "" {
			fmt.Printf("Last drop reason: %s\n", dropReason)
		}
	}
	return nil
}

func printPipelineTrace(ctx context.Context, db *sql.DB, idOrSlug string, last int) error {
	id, slug, _, _, err := resolvePipelineVertical(ctx, db, idOrSlug)
	if err != nil {
		return err
	}
	rows, err := db.QueryContext(ctx, `
		SELECT created_at, COALESCE(event_type,''), COALESCE(handler,''), COALESCE(action,''), COALESCE(drop_reason,''), COALESCE(error,''), COALESCE(array_to_string(events_emitted, ','),'')
		FROM pipeline_transitions
		WHERE pipeline_id = $1::uuid
		ORDER BY created_at DESC
		LIMIT $2
	`, id, last)
	if err != nil {
		return err
	}
	defer rows.Close()
	fmt.Printf("Pipeline trace for %s (%s)\n", nullable(slug, id), id)
	n := 0
	for rows.Next() {
		var at time.Time
		var evtType, handler, action, dropReason, errText, emitted string
		if err := rows.Scan(&at, &evtType, &handler, &action, &dropReason, &errText, &emitted); err != nil {
			return err
		}
		n++
		fmt.Printf("- %s  %s  %s  %s\n", at.UTC().Format(time.RFC3339), nullable(evtType, "-"), nullable(handler, "-"), nullable(action, "-"))
		if strings.TrimSpace(dropReason) != "" {
			fmt.Printf("  drop_reason: %s\n", dropReason)
		}
		if strings.TrimSpace(errText) != "" {
			fmt.Printf("  error: %s\n", errText)
		}
		if strings.TrimSpace(emitted) != "" {
			fmt.Printf("  emitted: %s\n", emitted)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if n == 0 {
		fmt.Println("- (no transitions)")
	}
	return nil
}

func printPipelineCampaigns(ctx context.Context, db *sql.DB, limit int) error {
	if limit <= 0 {
		limit = 100
	}
	rows, err := db.QueryContext(ctx, `
		SELECT c.id::text, COALESCE(g.name,''), c.mode, c.status, c.priority, c.created_at,
			COALESCE(c.completed_at, c.started_at, c.created_at) AS last_activity,
			COALESCE(c.discoveries,0)
		FROM scan_campaigns c
		LEFT JOIN geographies g ON g.id = c.geography_id
		ORDER BY COALESCE(c.completed_at, c.started_at, c.created_at) DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return err
	}
	defer rows.Close()
	fmt.Println("Campaigns")
	n := 0
	for rows.Next() {
		var id, geography, mode, status, priority string
		var createdAt, lastActivityAt time.Time
		var discoveries int
		if err := rows.Scan(&id, &geography, &mode, &status, &priority, &createdAt, &lastActivityAt, &discoveries); err != nil {
			return err
		}
		n++
		fmt.Printf("- %s geo=%s mode=%s status=%s priority=%s discoveries=%d last_activity=%s\n",
			id, nullable(geography, "-"), mode, status, priority, discoveries, lastActivityAt.UTC().Format(time.RFC3339))
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if n == 0 {
		fmt.Println("- (none)")
	}
	return nil
}

func printPipelineStuck(ctx context.Context, db *sql.DB, threshold time.Duration) error {
	fmt.Printf("Stuck pipelines (threshold=%s)\n", threshold.String())
	rows, err := db.QueryContext(ctx, `
		SELECT id::text, COALESCE(slug,''), stage, updated_at
		FROM verticals
		WHERE stage IN ('discovered','scoring','shortlisted','marginal_review','researching','mvp_speccing','spec_review','cto_spec_review','branding','ready_for_review')
		  AND updated_at <= now() - ($1::bigint * interval '1 second')
		ORDER BY updated_at ASC
		LIMIT 200
	`, int64(threshold.Seconds()))
	if err != nil {
		return err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var id, slug, stage string
		var updatedAt time.Time
		if err := rows.Scan(&id, &slug, &stage, &updatedAt); err != nil {
			return err
		}
		n++
		fmt.Printf("- vertical=%s stage=%s last_update=%s\n", nullable(slug, id), stage, updatedAt.UTC().Format(time.RFC3339))
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if n == 0 {
		fmt.Println("- (none)")
	}
	return nil
}

func printPipelineDrops(ctx context.Context, db *sql.DB, window time.Duration, vertical string, limit int) error {
	where := []string{"action = 'dropped'", "created_at >= now() - ($1::bigint * interval '1 second')"}
	args := []any{int64(window.Seconds())}
	if vertical != "" {
		verticalID, _, _, _, err := resolvePipelineVertical(ctx, db, vertical)
		if err != nil {
			return err
		}
		args = append(args, verticalID)
		where = append(where, fmt.Sprintf("pipeline_id = $%d::uuid", len(args)))
	}
	if limit <= 0 {
		limit = 200
	}
	args = append(args, limit)
	q := fmt.Sprintf(`
		SELECT created_at, COALESCE(event_type,''), pipeline_id::text, COALESCE(handler,''), COALESCE(drop_reason,''), COALESCE(error,'')
		FROM pipeline_transitions
		WHERE %s
		ORDER BY created_at DESC
		LIMIT $%d
	`, strings.Join(where, " AND "), len(args))

	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	fmt.Printf("Dropped transitions (window=%s)\n", window.String())
	n := 0
	for rows.Next() {
		var at time.Time
		var evtType, pipelineID, handler, dropReason, errText string
		if err := rows.Scan(&at, &evtType, &pipelineID, &handler, &dropReason, &errText); err != nil {
			return err
		}
		n++
		fmt.Printf("- %s event=%s pipeline=%s handler=%s reason=%s\n",
			at.UTC().Format(time.RFC3339),
			nullable(evtType, "-"),
			nullable(pipelineID, "-"),
			nullable(handler, "-"),
			nullable(dropReason, errText),
		)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if n == 0 {
		fmt.Println("- (none)")
	}
	return nil
}
