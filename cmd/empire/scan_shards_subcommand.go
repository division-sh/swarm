package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

func runScanShardsSubcommand(args []string) error {
	fs := flag.NewFlagSet("scan shards", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: empire scan shards <scan_id>")
	}
	scanIDInput := strings.TrimSpace(fs.Arg(0))
	if scanIDInput == "" {
		return fmt.Errorf("scan_id is required")
	}
	stores, ctx, err := openPipelineStores(*cfgPath, *storeMode, *migrate, *migrationFile)
	if err != nil {
		return err
	}
	scanUUID := stableUUIDLikeRuntime(scanIDInput)

	rows, err := stores.SQLDB.QueryContext(ctx, `
		SELECT
			id::text,
			stage,
			shard_index,
			shard_count,
			shard_key,
			status,
			COALESCE(agent_id, ''),
			retry_count,
			budget_cents,
			spend_cents,
			deadline_at,
			assigned_at,
			completed_at,
			COALESCE(error, '')
		FROM shards
		WHERE scan_id = $1::uuid
		ORDER BY shard_index ASC, created_at ASC
	`, scanUUID)
	if err != nil {
		return err
	}
	defer rows.Close()

	type shardRow struct {
		ID         string
		Stage      string
		Index      int
		Count      int
		Key        string
		Status     string
		AgentID    string
		RetryCount int
		BudgetCts  int
		SpendCts   int
		DeadlineAt time.Time
		AssignedAt *time.Time
		Completed  *time.Time
		Err        string
	}
	out := make([]shardRow, 0, 64)
	for rows.Next() {
		var row shardRow
		if err := rows.Scan(
			&row.ID,
			&row.Stage,
			&row.Index,
			&row.Count,
			&row.Key,
			&row.Status,
			&row.AgentID,
			&row.RetryCount,
			&row.BudgetCts,
			&row.SpendCts,
			&row.DeadlineAt,
			&row.AssignedAt,
			&row.Completed,
			&row.Err,
		); err != nil {
			return err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(out) == 0 {
		fmt.Printf("No shards found for scan_id=%s (normalized=%s)\n", scanIDInput, scanUUID)
		return nil
	}

	var mode, geography string
	_ = stores.SQLDB.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(scope->>'mode'), ''), COALESCE(MAX(scope->>'geography'), '')
		FROM shards
		WHERE scan_id = $1::uuid
	`, scanUUID).Scan(&mode, &geography)

	completed := 0
	failed := 0
	assigned := 0
	pending := 0
	totalSpend := 0
	for _, row := range out {
		totalSpend += row.SpendCts
		switch strings.TrimSpace(row.Status) {
		case "completed":
			completed++
		case "failed", "timed_out":
			failed++
		case "assigned":
			assigned++
		case "pending":
			pending++
		}
	}

	fmt.Printf("Scan %s shards=%d mode=%s geography=%s completed=%d failed=%d assigned=%d pending=%d spend=$%.2f\n",
		scanIDInput,
		len(out),
		nullable(mode, "-"),
		nullable(geography, "-"),
		completed,
		failed,
		assigned,
		pending,
		float64(totalSpend)/100.0,
	)
	for _, row := range out {
		activityAt := row.DeadlineAt
		if row.Completed != nil && !row.Completed.IsZero() {
			activityAt = row.Completed.UTC()
		}
		fmt.Printf("- shard=%s %d/%d stage=%s status=%s retries=%d spend=$%.2f budget=$%.2f agent=%s at=%s key=%s\n",
			row.ID,
			row.Index+1,
			row.Count,
			row.Stage,
			row.Status,
			row.RetryCount,
			float64(row.SpendCts)/100.0,
			float64(row.BudgetCts)/100.0,
			nullable(row.AgentID, "-"),
			activityAt.Format(time.RFC3339),
			row.Key,
		)
		if strings.TrimSpace(row.Err) != "" {
			fmt.Printf("  error: %s\n", row.Err)
		}
	}
	return nil
}

func runScanShardSubcommand(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: empire scan shard <shard_id> | empire scan shard <retry|cancel> <shard_id>")
	}
	action := strings.TrimSpace(args[0])
	switch action {
	case "retry":
		return runScanShardActionSubcommand("retry", args[1:])
	case "cancel":
		return runScanShardActionSubcommand("cancel", args[1:])
	default:
		return runScanShardDetailSubcommand(args)
	}
}

func runScanShardDetailSubcommand(args []string) error {
	fs := flag.NewFlagSet("scan shard", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: empire scan shard <shard_id>")
	}
	shardID := strings.TrimSpace(fs.Arg(0))
	if _, err := uuid.Parse(shardID); err != nil {
		return fmt.Errorf("invalid shard_id %q: %w", shardID, err)
	}
	stores, ctx, err := openPipelineStores(*cfgPath, *storeMode, *migrate, *migrationFile)
	if err != nil {
		return err
	}

	var (
		scanID                                  string
		stage, shardKey, status, agentID, errTx string
		shardIndex, shardCount                  int
		retries, budgetCts, spendCts            int
		scopeRaw                                []byte
		deadlineAt                              time.Time
		assignedAt, completedAt                 *time.Time
	)
	err = stores.SQLDB.QueryRowContext(ctx, `
		SELECT
			COALESCE(scan_id::text, ''),
			stage,
			shard_index,
			shard_count,
			shard_key,
			scope,
			status,
			COALESCE(agent_id, ''),
			retry_count,
			budget_cents,
			spend_cents,
			deadline_at,
			assigned_at,
			completed_at,
			COALESCE(error, '')
		FROM shards
		WHERE id = $1::uuid
	`, shardID).Scan(
		&scanID,
		&stage,
		&shardIndex,
		&shardCount,
		&shardKey,
		&scopeRaw,
		&status,
		&agentID,
		&retries,
		&budgetCts,
		&spendCts,
		&deadlineAt,
		&assignedAt,
		&completedAt,
		&errTx,
	)
	if err != nil {
		return err
	}
	var scope any
	_ = json.Unmarshal(scopeRaw, &scope)

	fmt.Printf("Shard %s\n", shardID)
	fmt.Printf("scan_id=%s stage=%s index=%d/%d key=%s\n", nullable(scanID, "-"), stage, shardIndex+1, shardCount, shardKey)
	fmt.Printf("status=%s retries=%d agent=%s spend=$%.2f budget=$%.2f\n", status, retries, nullable(agentID, "-"), float64(spendCts)/100.0, float64(budgetCts)/100.0)
	reportsCount, highSignalCount, statsErr := shardEventStats(ctx, stores.SQLDB, stage, scanID, agentID)
	if statsErr != nil {
		return statsErr
	}
	fmt.Printf("reports=%d high_signal=%d\n", reportsCount, highSignalCount)
	fmt.Printf("deadline=%s assigned=%s completed=%s\n",
		deadlineAt.UTC().Format(time.RFC3339),
		nullableTime(assignedAt),
		nullableTime(completedAt),
	)
	if strings.TrimSpace(errTx) != "" {
		fmt.Printf("error=%s\n", errTx)
	}
	if scope != nil {
		pretty, _ := json.MarshalIndent(scope, "", "  ")
		fmt.Printf("scope=%s\n", string(pretty))
	}
	return nil
}

func runScanShardActionSubcommand(action string, args []string) error {
	fs := flag.NewFlagSet("scan shard "+action, flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: empire scan shard %s <shard_id>", action)
	}
	shardID := strings.TrimSpace(fs.Arg(0))
	if _, err := uuid.Parse(shardID); err != nil {
		return fmt.Errorf("invalid shard_id %q: %w", shardID, err)
	}
	stores, ctx, err := openPipelineStores(*cfgPath, *storeMode, *migrate, *migrationFile)
	if err != nil {
		return err
	}

	switch action {
	case "retry":
		res, err := stores.SQLDB.ExecContext(ctx, `
			UPDATE shards
			SET status = 'pending',
			    agent_id = NULL,
			    assigned_at = NULL,
			    completed_at = NULL,
			    error = NULL
			WHERE id = $1::uuid
			  AND status IN ('failed', 'timed_out')
		`, shardID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return fmt.Errorf("shard %s is not retryable (expected status failed or timed_out)", shardID)
		}
		fmt.Printf("shard %s queued for retry\n", shardID)
		return nil
	case "cancel":
		res, err := stores.SQLDB.ExecContext(ctx, `
			UPDATE shards
			SET status = 'failed',
			    completed_at = now(),
			    error = COALESCE(NULLIF(error, ''), 'manual cancel via CLI')
			WHERE id = $1::uuid
			  AND status IN ('pending', 'assigned')
		`, shardID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return fmt.Errorf("shard %s is not cancelable (expected status pending or assigned)", shardID)
		}
		fmt.Printf("shard %s canceled\n", shardID)
		return nil
	default:
		return fmt.Errorf("unsupported shard action: %s", action)
	}
}

func stableUUIDLikeRuntime(raw string) string {
	raw = strings.TrimSpace(raw)
	if parsed, err := uuid.Parse(raw); err == nil {
		return parsed.String()
	}
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(raw)).String()
}

func nullableTime(t *time.Time) string {
	if t == nil || t.IsZero() {
		return "-"
	}
	return t.UTC().Format(time.RFC3339)
}

func shardEventStats(ctx context.Context, db *sql.DB, stage, scanID, agentID string) (reportsCount, highSignalCount int, err error) {
	stage = strings.TrimSpace(stage)
	scanID = strings.TrimSpace(scanID)
	agentID = strings.TrimSpace(agentID)
	if db == nil || stage == "" || scanID == "" || agentID == "" {
		return 0, 0, nil
	}
	eventType := shardCompletionEventTypeForStage(stage)
	if eventType == "" {
		return 0, 0, nil
	}
	rows, err := db.QueryContext(ctx, `
		SELECT payload
		FROM events
		WHERE source_agent = $1
		  AND type = $2
		  AND COALESCE(payload->>'scan_id', '') = $3
		  AND COALESCE(payload->'shard'->>'terminal', 'false') <> 'true'
	`, agentID, eventType, scanID)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()

	for rows.Next() {
		var payloadRaw []byte
		if err := rows.Scan(&payloadRaw); err != nil {
			return 0, 0, err
		}
		payload := map[string]any{}
		if len(payloadRaw) > 0 {
			_ = json.Unmarshal(payloadRaw, &payload)
		}
		if n := int(math.Round(asFloatAny(payload["reports_count"]))); n > 0 {
			reportsCount += n
		} else {
			reportsCount++
		}
		if n := int(math.Round(asFloatAny(payload["high_signal_count"]))); n > 0 {
			highSignalCount += n
			continue
		}
		if asFloatAny(payload["signal_strength"]) >= 70 {
			highSignalCount++
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, err
	}
	return reportsCount, highSignalCount, nil
}

func shardCompletionEventTypeForStage(stage string) string {
	switch strings.TrimSpace(stage) {
	case "market_research":
		return "market_research.scan_complete"
	case "trend_research":
		return "trend_research.scan_complete"
	default:
		return ""
	}
}

func asFloatAny(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case int32:
		return float64(t)
	case json.Number:
		f, _ := t.Float64()
		return f
	case string:
		f, _ := strconv.ParseFloat(strings.TrimSpace(t), 64)
		return f
	default:
		return 0
	}
}
