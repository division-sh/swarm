package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"empireai/internal/config"
)

func runAgentSubcommand(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: empire agent prompt <agent_id> [--edit|--set-from <file>|--revert|--diff]")
	}
	switch strings.TrimSpace(args[0]) {
	case "prompt":
		fs := flag.NewFlagSet("agent prompt", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		edit := fs.Bool("edit", false, "Open $EDITOR with current prompt content")
		revert := fs.Bool("revert", false, "Delete prompt override for this agent")
		diff := fs.Bool("diff", false, "Show override vs template prompt")
		setFrom := fs.String("set-from", "", "Set override prompt from file")
		source := fs.String("source", "cli", "Override source label")
		notes := fs.String("notes", "", "Optional override notes")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if len(fs.Args()) < 1 {
			return fmt.Errorf("usage: empire agent prompt <agent_id> [--edit|--set-from <file>|--revert|--diff]")
		}
		agentID := strings.TrimSpace(fs.Args()[0])
		if agentID == "" {
			return fmt.Errorf("agent_id is required")
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		if stores.SQLDB == nil {
			return fmt.Errorf("agent prompt requires postgres db")
		}
		templatePrompt, overridePrompt, hasOverride, err := loadAgentPromptState(ctx, stores.SQLDB, agentID)
		if err != nil {
			return err
		}
		if *revert {
			if _, err := stores.SQLDB.ExecContext(ctx, `DELETE FROM prompt_overrides WHERE agent_id = $1`, agentID); err != nil {
				return fmt.Errorf("revert prompt override: %w", err)
			}
			fmt.Printf("prompt override reverted agent_id=%s\n", agentID)
			fmt.Println("note: restart/reconfigure the agent for immediate effect")
			return nil
		}
		if *diff {
			for _, line := range renderPromptDiffCLI(templatePrompt, overridePrompt) {
				fmt.Println(line)
			}
			return nil
		}

		newPrompt := ""
		switch {
		case strings.TrimSpace(*setFrom) != "":
			b, err := os.ReadFile(strings.TrimSpace(*setFrom))
			if err != nil {
				return fmt.Errorf("read --set-from file: %w", err)
			}
			newPrompt = strings.TrimSpace(string(b))
		case *edit:
			seed := templatePrompt
			if hasOverride {
				seed = overridePrompt
			}
			p, err := editPromptInEditor(seed)
			if err != nil {
				return err
			}
			newPrompt = strings.TrimSpace(p)
		}
		if newPrompt != "" {
			prev := strings.TrimSpace(templatePrompt)
			if hasOverride {
				prev = strings.TrimSpace(overridePrompt)
			}
			if _, err := stores.SQLDB.ExecContext(ctx, `
				INSERT INTO prompt_overrides (agent_id, prompt, previous_prompt, source, notes, created_at, updated_at)
				VALUES ($1, $2, NULLIF($3,''), $4, NULLIF($5,''), now(), now())
				ON CONFLICT (agent_id) DO UPDATE SET
					prompt = EXCLUDED.prompt,
					previous_prompt = EXCLUDED.previous_prompt,
					source = EXCLUDED.source,
					notes = EXCLUDED.notes,
					updated_at = now()
			`, agentID, newPrompt, prev, strings.TrimSpace(*source), strings.TrimSpace(*notes)); err != nil {
				return fmt.Errorf("set prompt override: %w", err)
			}
			fmt.Printf("prompt override set agent_id=%s\n", agentID)
			fmt.Println("note: restart/reconfigure the agent for immediate effect")
			return nil
		}

		fmt.Printf("agent prompt\nagent_id: %s\nhas_override: %t\n", agentID, hasOverride)
		fmt.Println("effective_prompt:")
		if hasOverride {
			fmt.Println(overridePrompt)
		} else {
			fmt.Println(templatePrompt)
		}
		return nil
	default:
		return fmt.Errorf("unknown agent subcommand: %s", args[0])
	}
}
