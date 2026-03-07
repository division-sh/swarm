package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"

	"empireai/internal/config"
)

func runDirectiveSubcommand(args []string) error {
	fs := flag.NewFlagSet("directive", flag.ContinueOnError)
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
	if err := syncRuntimeGlobalAgents(ctx, stores.ManagerStore); err != nil {
		log.Printf("directive command global agents sync failed (continuing): %v", err)
	}
	if len(fs.Args()) < 1 {
		return fmt.Errorf("usage: empire directive \"message\"  (or legacy: empire directive <target> \"message\")")
	}

	targetRaw := "empire-coordinator"
	msgArgs := fs.Args()
	if len(msgArgs) >= 2 {
		targetRaw = msgArgs[0]
		msgArgs = msgArgs[1:]
	}
	msg := strings.TrimSpace(strings.Join(msgArgs, " "))
	if msg == "" {
		return fmt.Errorf("directive message is required")
	}

	target, err := resolveTargetAgent(ctx, stores, targetRaw)
	if err != nil {
		return err
	}
	if err := ensureTargetAgentRegistered(ctx, stores, target); err != nil {
		return err
	}
	if err := requireSystemStarted(ctx, stores.SQLDB); err != nil {
		return err
	}

	eventID, err := dispatchSystemDirective(ctx, stores, target, msg)
	if err != nil {
		return err
	}
	fmt.Printf("directive queued event=%s target=%s\n", eventID, target.ID)
	return nil
}
