package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"empireai/internal/config"
	"empireai/internal/events"
	"empireai/internal/specaudit"
	"empireai/internal/templateops"
	"github.com/google/uuid"
)

func runTemplateSubcommand(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: empire template <publish|list|current|diff|plan|apply> [flags]")
	}
	switch args[0] {
	case "publish":
		fs := flag.NewFlagSet("template publish", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		version := fs.String("version", "", "Template version")
		agentsFile := fs.String("agents-file", "", "Path to template agents json (legacy)")
		bootstrapFile := fs.String("bootstrap-routes-file", "", "Path to bootstrap routes json (legacy)")
		seededFile := fs.String("seeded-routes-file", "", "Path to seeded routes json (legacy)")
		agentsDir := fs.String("agents-dir", "configs/agents/templates", "Path to YAML agent templates directory")
		routesYAML := fs.String("routes-yaml", "configs/agents/templates/routes.yaml", "Path to YAML routing template")
		createdBy := fs.String("created-by", "factory-cto", "Publisher agent")
		description := fs.String("description", "", "Template description")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if strings.TrimSpace(*version) == "" {
			return fmt.Errorf("--version is required")
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		svc := templateops.NewService(stores.SQLDB, stores.MailboxStore)

		var agents, bootstrapRoutes, seededRoutes []byte
		if strings.TrimSpace(*agentsFile) != "" || strings.TrimSpace(*bootstrapFile) != "" || strings.TrimSpace(*seededFile) != "" {
			agents, err = readOptionalJSONFile(*agentsFile, []byte("[]"))
			if err != nil {
				return err
			}
			bootstrapRoutes, err = readOptionalJSONFile(*bootstrapFile, []byte("[]"))
			if err != nil {
				return err
			}
			seededRoutes, err = readOptionalJSONFile(*seededFile, []byte("[]"))
			if err != nil {
				return err
			}
		} else {
			agents, bootstrapRoutes, seededRoutes, err = templateops.CompileTemplateFromYAML(*agentsDir, *routesYAML)
			if err != nil {
				return err
			}
			env := mustJSON(map[string]any{
				"version":          strings.TrimSpace(*version),
				"agents":           json.RawMessage(agents),
				"bootstrap_routes": json.RawMessage(bootstrapRoutes),
				"seeded_routes":    json.RawMessage(seededRoutes),
				"notes":            strings.TrimSpace(*description),
			})
			res := specaudit.Validate("template", env)
			if !res.Passed {
				fmt.Printf("template publish blocked by spec audit issues=%d\n", len(res.Issues))
				for _, issue := range res.Issues {
					fmt.Printf("- [%s] %s at %s: %s\n", issue.Severity, issue.Code, issue.Location, issue.Message)
				}
				return fmt.Errorf("template publish failed spec audit")
			}
		}

		if stores.EventStore != nil {
			reqPayload := map[string]any{
				"version":      strings.TrimSpace(*version),
				"created_by":   strings.TrimSpace(*createdBy),
				"description":  strings.TrimSpace(*description),
				"agents_dir":   strings.TrimSpace(*agentsDir),
				"routes_yaml":  strings.TrimSpace(*routesYAML),
				"requested_at": time.Now().UTC().Format(time.RFC3339),
			}
			if err := appendTargetedEvent(ctx, stores, events.Event{
				ID:          uuid.NewString(),
				Type:        events.EventType("template.publish_requested"),
				SourceAgent: "human",
				Payload:     mustJSON(reqPayload),
				CreatedAt:   time.Now(),
			}, []string{"factory-cto"}); err != nil {
				log.Printf("template publish_requested append failed: %v", err)
			}
		}

		if err := svc.PublishTemplate(ctx, *version, agents, bootstrapRoutes, seededRoutes, *createdBy, *description); err != nil {
			return err
		}
		fmt.Printf("template published version=%s\n", *version)
		return nil
	case "list":
		fs := flag.NewFlagSet("template list", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		limit := fs.Int("limit", 20, "Max templates to list")
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
			return fmt.Errorf("template list requires persistent store mode (use -store postgres)")
		}
		if *limit <= 0 {
			*limit = 20
		}
		rows, err := stores.SQLDB.QueryContext(ctx, `
			SELECT version, COALESCE(created_by,''), COALESCE(description,''), created_at
			FROM org_templates
			ORDER BY created_at DESC
			LIMIT $1
		`, *limit)
		if err != nil {
			return fmt.Errorf("list templates: %w", err)
		}
		defer rows.Close()
		fmt.Println("template versions")
		n := 0
		for rows.Next() {
			var version, createdBy, desc string
			var createdAt time.Time
			if err := rows.Scan(&version, &createdBy, &desc, &createdAt); err != nil {
				return fmt.Errorf("scan template row: %w", err)
			}
			n++
			fmt.Printf("- version=%s created_by=%s created_at=%s description=%q\n",
				version, nullable(createdBy, "-"), createdAt.UTC().Format(time.RFC3339), desc)
		}
		if n == 0 {
			fmt.Println("- (none)")
		}
		return nil
	case "current":
		fs := flag.NewFlagSet("template current", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		verticalID := fs.String("vertical-id", "", "Optional vertical id to resolve effective template")
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
			return fmt.Errorf("template current requires persistent store mode (use -store postgres)")
		}
		if strings.TrimSpace(*verticalID) != "" {
			var version string
			if err := stores.SQLDB.QueryRowContext(ctx, `
				SELECT COALESCE(template_version, '')
				FROM verticals
				WHERE id = $1::uuid
			`, strings.TrimSpace(*verticalID)).Scan(&version); err != nil {
				return fmt.Errorf("load vertical template version: %w", err)
			}
			if strings.TrimSpace(version) == "" {
				fmt.Printf("vertical template\nvertical_id: %s\ntemplate_version: (none)\n", strings.TrimSpace(*verticalID))
				return nil
			}
			fmt.Printf("vertical template\nvertical_id: %s\ntemplate_version: %s\n", strings.TrimSpace(*verticalID), version)
			return nil
		}
		var version, createdBy string
		var createdAt time.Time
		if err := stores.SQLDB.QueryRowContext(ctx, `
			SELECT version, COALESCE(created_by,''), created_at
			FROM org_templates
			ORDER BY created_at DESC
			LIMIT 1
		`).Scan(&version, &createdBy, &createdAt); err != nil {
			return fmt.Errorf("load current template: %w", err)
		}
		fmt.Printf("current template\nversion: %s\ncreated_by: %s\ncreated_at: %s\n",
			version, nullable(createdBy, "-"), createdAt.UTC().Format(time.RFC3339))
		return nil
	case "diff":
		fs := flag.NewFlagSet("template diff", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		fromVersion := fs.String("from-version", "", "Source template version")
		toVersion := fs.String("to-version", "", "Target template version")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if strings.TrimSpace(*fromVersion) == "" || strings.TrimSpace(*toVersion) == "" {
			return fmt.Errorf("--from-version and --to-version are required")
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		if stores.SQLDB == nil {
			return fmt.Errorf("template diff requires persistent store mode (use -store postgres)")
		}
		return renderTemplateDiff(ctx, stores.SQLDB, strings.TrimSpace(*fromVersion), strings.TrimSpace(*toVersion))
	case "plan":
		fs := flag.NewFlagSet("template plan", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		toVersion := fs.String("to-version", "", "Target template version")
		requestedBy := fs.String("requested-by", "factory-cto", "Planner agent")
		limit := fs.Int("limit", 50, "Max verticals to plan")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if strings.TrimSpace(*toVersion) == "" {
			return fmt.Errorf("--to-version is required")
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		svc := templateops.NewService(stores.SQLDB, stores.MailboxStore)
		n, err := svc.PlanMigrations(ctx, *toVersion, *requestedBy, *limit)
		if err != nil {
			return err
		}
		fmt.Printf("template migration plans created=%d to_version=%s\n", n, *toVersion)
		return nil
	case "apply":
		fs := flag.NewFlagSet("template apply", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		executedBy := fs.String("executed-by", "empire-coordinator", "Executor agent")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if len(fs.Args()) < 1 {
			return fmt.Errorf("usage: empire template apply <migration-id>")
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		if err := applyTemplateMigrationWithPrimitives(ctx, cfg.LLM.RuntimeMode, stores, strings.TrimSpace(fs.Args()[0]), *executedBy); err != nil {
			return err
		}
		fmt.Printf("template migration applied id=%s\n", strings.TrimSpace(fs.Args()[0]))
		return nil
	default:
		return fmt.Errorf("unknown template command: %s", args[0])
	}
}

func loadAgentPromptState(ctx context.Context, db *sql.DB, agentID string) (templatePrompt string, overridePrompt string, hasOverride bool, err error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return "", "", false, fmt.Errorf("agent_id is required")
	}
	var cfgRaw []byte
	if err := db.QueryRowContext(ctx, `
		SELECT config
		FROM agents
		WHERE id = $1
		  AND status <> 'terminated'
	`, agentID).Scan(&cfgRaw); err != nil {
		if err == sql.ErrNoRows {
			return "", "", false, fmt.Errorf("agent not found: %s", agentID)
		}
		return "", "", false, fmt.Errorf("load agent config: %w", err)
	}
	templatePrompt = extractSystemPromptFromConfigCLI(cfgRaw)
	err = db.QueryRowContext(ctx, `
		SELECT prompt
		FROM prompt_overrides
		WHERE agent_id = $1
	`, agentID).Scan(&overridePrompt)
	if err != nil {
		if err == sql.ErrNoRows {
			return templatePrompt, "", false, nil
		}
		return "", "", false, fmt.Errorf("load prompt override: %w", err)
	}
	return templatePrompt, strings.TrimSpace(overridePrompt), true, nil
}

func extractSystemPromptFromConfigCLI(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return ""
	}
	v, _ := obj["system_prompt"].(string)
	return strings.TrimSpace(v)
}

func renderPromptDiffCLI(templatePrompt, overridePrompt string) []string {
	templatePrompt = strings.TrimSpace(templatePrompt)
	overridePrompt = strings.TrimSpace(overridePrompt)
	if templatePrompt == overridePrompt {
		return []string{"(no diff)"}
	}
	left := strings.Split(templatePrompt, "\n")
	right := strings.Split(overridePrompt, "\n")
	if len(left) == 1 && left[0] == "" {
		left = nil
	}
	if len(right) == 1 && right[0] == "" {
		right = nil
	}
	max := len(left)
	if len(right) > max {
		max = len(right)
	}
	out := make([]string, 0, max*2)
	for i := 0; i < max; i++ {
		lv := ""
		rv := ""
		if i < len(left) {
			lv = left[i]
		}
		if i < len(right) {
			rv = right[i]
		}
		if lv == rv {
			continue
		}
		if lv != "" {
			out = append(out, "- "+lv)
		}
		if rv != "" {
			out = append(out, "+ "+rv)
		}
	}
	if len(out) == 0 {
		return []string{"(no diff)"}
	}
	return out
}

func editPromptInEditor(initial string) (string, error) {
	editor := strings.TrimSpace(os.Getenv("EDITOR"))
	if editor == "" {
		editor = "vi"
	}
	f, err := os.CreateTemp("", "empire-prompt-*.txt")
	if err != nil {
		return "", fmt.Errorf("create temp file: %w", err)
	}
	path := f.Name()
	defer os.Remove(path)
	if _, err := f.WriteString(initial); err != nil {
		_ = f.Close()
		return "", fmt.Errorf("write temp file: %w", err)
	}
	_ = f.Close()
	cmd := exec.Command(editor, path)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("open editor %q: %w", editor, err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read edited prompt: %w", err)
	}
	return string(b), nil
}
