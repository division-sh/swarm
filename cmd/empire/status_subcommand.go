package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"empireai/internal/config"
	"empireai/internal/mailbox"
)

func runDigestSubcommand(args []string) error {
	fs := flag.NewFlagSet("digest", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	topN := fs.Int("top", 10, "Top verticals in digest")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	return runOperatorActions(ctx, stores, operatorOptions{
		digestGenerate: true,
		digestTopN:     *topN,
	})
}

func runStatusSubcommand(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	vertical := fs.String("vertical", "", "Optional vertical id or slug")
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
		return fmt.Errorf("status requires persistent store mode (use -store postgres)")
	}

	if strings.TrimSpace(*vertical) != "" {
		verticalID, err := resolveVerticalID(ctx, stores.SQLDB, strings.TrimSpace(*vertical))
		if err != nil {
			return err
		}
		var name, slug, stage, mode, templateVersion string
		if err := stores.SQLDB.QueryRowContext(ctx, `
			SELECT name, COALESCE(slug,''), stage, mode, COALESCE(template_version,'')
			FROM verticals
			WHERE id = $1::uuid
		`, verticalID).Scan(&name, &slug, &stage, &mode, &templateVersion); err != nil {
			return fmt.Errorf("load vertical status: %w", err)
		}
		fmt.Printf("vertical status\nid: %s\nslug: %s\nname: %s\nstage: %s\nmode: %s\ntemplate_version: %s\n",
			verticalID, slug, name, stage, mode, templateVersion)
		return nil
	}

	var total, operating, factory int
	if err := stores.SQLDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM verticals`).Scan(&total); err != nil {
		return err
	}
	if err := stores.SQLDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM verticals WHERE mode = 'operating'`).Scan(&operating); err != nil {
		return err
	}
	factory = total - operating
	fmt.Printf("status\nverticals_total: %d\nverticals_factory: %d\nverticals_operating: %d\n", total, factory, operating)
	if stores.MailboxStore != nil {
		st, err := mailbox.GetStatus(ctx, stores.MailboxStore)
		if err != nil {
			return err
		}
		fmt.Printf("mailbox_pending: %d\nmailbox_critical: %d\n", st.Pending, st.Critical)
	}
	return nil
}

func runBudgetSubcommand(args []string) error {
	fs := flag.NewFlagSet("budget", flag.ContinueOnError)
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
		return fmt.Errorf("budget requires persistent store mode (use -store postgres)")
	}
	monthStart := time.Now().UTC()
	monthStart = time.Date(monthStart.Year(), monthStart.Month(), 1, 0, 0, 0, 0, time.UTC)
	var spent int64
	if err := stores.SQLDB.QueryRowContext(ctx, `
		SELECT COALESCE(sum(amount_cents),0)
		FROM spend_ledger
		WHERE created_at >= $1
	`, monthStart).Scan(&spent); err != nil {
		return fmt.Errorf("load budget spend: %w", err)
	}
	portfolioCap := cfg.Budget.PortfolioMonthlyCap
	perVerticalCap := cfg.Budget.PerVerticalMonthlyCap
	factoryCap := cfg.Budget.FactoryMonthlyCap
	portfolioPct := 0.0
	if portfolioCap > 0 {
		portfolioPct = (float64(spent) / float64(portfolioCap)) * 100
	}
	fmt.Printf("budget\nmonth_start: %s\nspent_cents: %d\n", monthStart.Format(time.RFC3339), spent)
	fmt.Printf("portfolio_monthly_cap_cents: %d\n", portfolioCap)
	fmt.Printf("per_vertical_monthly_cap_cents: %d\n", perVerticalCap)
	fmt.Printf("factory_monthly_cap_cents: %d\n", factoryCap)
	if portfolioCap > 0 {
		fmt.Printf("portfolio_used_pct: %.2f\n", portfolioPct)
	} else {
		fmt.Printf("portfolio_used_pct: n/a\n")
	}
	return nil
}
