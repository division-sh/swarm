package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"empireai/internal/config"
	"empireai/internal/ops"
)

func runOpsSubcommand(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: empire ops <tick|record-metrics> [flags]")
	}
	switch args[0] {
	case "tick":
		fs := flag.NewFlagSet("ops tick", flag.ContinueOnError)
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
		svc := ops.NewService(stores.SQLDB, stores.MailboxStore)
		sum, err := svc.Tick(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("ops tick complete kill_candidates=%d budget_alerts=%d routing_proposals=%d\n",
			sum.KillCandidates, sum.BudgetAlerts, sum.RoutingProposals)
		return nil
	case "record-metrics":
		fs := flag.NewFlagSet("ops record-metrics", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		verticalID := fs.String("vertical-id", "", "Vertical UUID")
		usersTotal := fs.Int("users-total", 0, "Total users")
		usersNew := fs.Int("users-new", 0, "New users")
		usersChurned := fs.Int("users-churned", 0, "Churned users")
		mrr := fs.Int("mrr-cents", 0, "MRR in cents")
		apiCost := fs.Int("api-cost-cents", 0, "API cost in cents")
		infraCost := fs.Int("infra-cost-cents", 0, "Infra cost in cents")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if strings.TrimSpace(*verticalID) == "" {
			return fmt.Errorf("--vertical-id is required")
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		svc := ops.NewService(stores.SQLDB, stores.MailboxStore)
		now := time.Now().UTC()
		start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		err = svc.RecordMetrics(ctx, ops.MetricInput{
			VerticalID:     strings.TrimSpace(*verticalID),
			PeriodStart:    start,
			PeriodEnd:      start.Add(24 * time.Hour),
			UsersTotal:     *usersTotal,
			UsersNew:       *usersNew,
			UsersChurned:   *usersChurned,
			MRRCents:       *mrr,
			APICostCents:   *apiCost,
			InfraCostCents: *infraCost,
		})
		if err != nil {
			return err
		}
		fmt.Printf("metrics recorded vertical_id=%s\n", strings.TrimSpace(*verticalID))
		return nil
	default:
		return fmt.Errorf("unknown ops command: %s", args[0])
	}
}
