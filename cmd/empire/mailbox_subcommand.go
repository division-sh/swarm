package main

import (
	"context"
	"flag"
	"fmt"
	"strings"

	"empireai/internal/config"
)

func runMailboxSubcommand(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: empire mailbox <list|view|decide|approve-spend|reject-spend|review|respond> [flags]")
	}
	switch args[0] {
	case "list":
		fs := flag.NewFlagSet("mailbox list", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		critical := fs.Bool("critical", false, "Show only critical pending items")
		reviews := fs.Bool("reviews", false, "Show only founder review gate items")
		limit := fs.Int("limit", 20, "Max rows")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		return runOperatorActions(ctx, stores, operatorOptions{
			mailboxList:         true,
			mailboxListCritical: *critical,
			mailboxListReviews:  *reviews,
			mailboxLimit:        *limit,
		})
	case "view":
		fs := flag.NewFlagSet("mailbox view", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if len(fs.Args()) < 1 {
			return fmt.Errorf("usage: empire mailbox view <id>")
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		return runOperatorActions(ctx, stores, operatorOptions{
			mailboxViewID: strings.TrimSpace(fs.Args()[0]),
		})
	case "decide":
		fs := flag.NewFlagSet("mailbox decide", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		action := fs.String("action", "", "Decision action (approve|reject|more-data|kill|revise|skip)")
		notes := fs.String("notes", "", "Decision notes")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if len(fs.Args()) < 1 {
			return fmt.Errorf("usage: empire mailbox decide <id> --action <action> [--notes ...]")
		}
		if strings.TrimSpace(*action) == "" {
			return fmt.Errorf("--action is required")
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		return runOperatorActions(ctx, stores, operatorOptions{
			mailboxDecideID: strings.TrimSpace(fs.Args()[0]),
			mailboxDecision: strings.TrimSpace(*action),
			mailboxNotes:    *notes,
		})
	case "approve-spend":
		return runMailboxDecisionAlias(args[1:], "approve")
	case "reject-spend":
		return runMailboxDecisionAlias(args[1:], "reject")
	case "review":
		return runMailboxReviewAlias(args[1:])
	case "respond":
		return runMailboxResponseAlias(args[1:])
	default:
		return fmt.Errorf("unknown mailbox command: %s", args[0])
	}
}

func runMailboxDecisionAlias(args []string, forcedAction string) error {
	fs := flag.NewFlagSet("mailbox decision alias", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	notes := fs.String("notes", "", "Decision notes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(fs.Args()) < 1 {
		return fmt.Errorf("mailbox item id is required")
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	return runOperatorActions(ctx, stores, operatorOptions{
		mailboxDecideID: strings.TrimSpace(fs.Args()[0]),
		mailboxDecision: forcedAction,
		mailboxNotes:    *notes,
	})
}

func runMailboxReviewAlias(args []string) error {
	fs := flag.NewFlagSet("mailbox review", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	action := fs.String("action", "", "Review action (approve|revise|skip)")
	notes := fs.String("notes", "", "Review notes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(fs.Args()) < 1 {
		return fmt.Errorf("mailbox item id is required")
	}
	if strings.TrimSpace(*action) == "" {
		return fmt.Errorf("--action is required")
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	return runOperatorActions(ctx, stores, operatorOptions{
		mailboxDecideID: strings.TrimSpace(fs.Args()[0]),
		mailboxDecision: strings.TrimSpace(*action),
		mailboxNotes:    *notes,
	})
}

func runMailboxResponseAlias(args []string) error {
	fs := flag.NewFlagSet("mailbox respond", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	notes := fs.String("notes", "", "Response notes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(fs.Args()) < 1 {
		return fmt.Errorf("mailbox item id is required")
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	return runOperatorActions(ctx, stores, operatorOptions{
		mailboxDecideID: strings.TrimSpace(fs.Args()[0]),
		mailboxDecision: "respond",
		mailboxNotes:    *notes,
	})
}
