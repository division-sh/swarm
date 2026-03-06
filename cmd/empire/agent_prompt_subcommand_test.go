package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"empireai/internal/models"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestRunAgentSubcommand_PromptLifecycle(t *testing.T) {
	root := repoRootFromCmd(t)
	dsn, db, _ := testutil.StartPostgres(t)
	cfgPath := writeTempConfig(t, dsn)
	ctx := context.Background()

	agentID := "empire-coordinator"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ($1, 'stub', 'empire-coordinator', 'holding', 'active', '{"system_prompt":"Template prompt"}'::jsonb, now(), now())
	`, agentID); err != nil {
		t.Fatalf("seed agent: %v", err)
	}

	migrationFile := filepath.Join(root, "contracts", "ddl-canonical.sql")

	if err := runAgentSubcommand([]string{
		"prompt",
		"--config", cfgPath,
		"--store", "postgres",
		"--migrate=true",
		"--migration-file", migrationFile,
		agentID,
	}); err != nil {
		t.Fatalf("print prompt: %v", err)
	}

	overrideFile := filepath.Join(t.TempDir(), "prompt.txt")
	if err := os.WriteFile(overrideFile, []byte("Override prompt from file"), 0o644); err != nil {
		t.Fatalf("write override file: %v", err)
	}
	if err := runAgentSubcommand([]string{
		"prompt",
		"--config", cfgPath,
		"--store", "postgres",
		"--migration-file", migrationFile,
		"--set-from", overrideFile,
		"--source", "test",
		"--notes", "coverage",
		agentID,
	}); err != nil {
		t.Fatalf("set prompt override: %v", err)
	}

	var prompt string
	if err := db.QueryRowContext(ctx, `SELECT prompt FROM prompt_overrides WHERE agent_id = $1`, agentID).Scan(&prompt); err != nil {
		t.Fatalf("read prompt override: %v", err)
	}
	if strings.TrimSpace(prompt) != "Override prompt from file" {
		t.Fatalf("unexpected override prompt: %q", prompt)
	}

	if err := runAgentSubcommand([]string{
		"prompt",
		"--config", cfgPath,
		"--store", "postgres",
		"--migration-file", migrationFile,
		"--diff",
		agentID,
	}); err != nil {
		t.Fatalf("diff prompt override: %v", err)
	}

	if err := runAgentSubcommand([]string{
		"prompt",
		"--config", cfgPath,
		"--store", "postgres",
		"--migration-file", migrationFile,
		"--revert",
		agentID,
	}); err != nil {
		t.Fatalf("revert prompt override: %v", err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM prompt_overrides WHERE agent_id = $1`, agentID).Scan(&count); err != nil {
		t.Fatalf("count prompt overrides: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected prompt override deleted, count=%d", count)
	}
}

func TestAgentPromptHelpersAndErrors(t *testing.T) {
	if err := runAgentSubcommand(nil); err == nil {
		t.Fatal("expected usage error when no args")
	}
	if err := runAgentSubcommand([]string{"unknown"}); err == nil {
		t.Fatal("expected unknown agent subcommand error")
	}

	if got := extractSystemPromptFromConfigCLI(nil); got != "" {
		t.Fatalf("expected empty prompt for nil config, got %q", got)
	}
	if got := extractSystemPromptFromConfigCLI([]byte(`{"system_prompt":"  X  "}`)); got != "X" {
		t.Fatalf("expected trimmed prompt, got %q", got)
	}
	if got := extractSystemPromptFromConfigCLI([]byte(`{bad-json`)); got != "" {
		t.Fatalf("expected empty prompt for invalid json, got %q", got)
	}

	noDiff := renderPromptDiffCLI("same", "same")
	if len(noDiff) != 1 || noDiff[0] != "(no diff)" {
		t.Fatalf("expected no diff sentinel, got %+v", noDiff)
	}
	diff := renderPromptDiffCLI("a\nb", "a\nc")
	if len(diff) == 0 {
		t.Fatalf("expected line-level diff output")
	}

	cfgNoPrompt := models.AgentConfig{Config: []byte(`{"tools":["x"]}`)}
	if hasSystemPrompt(cfgNoPrompt) {
		t.Fatal("expected hasSystemPrompt false when key missing")
	}
	cfgWithPrompt := models.AgentConfig{Config: []byte(`{"system_prompt":"hello"}`)}
	if !hasSystemPrompt(cfgWithPrompt) {
		t.Fatal("expected hasSystemPrompt true")
	}

	t.Setenv("EDITOR", "definitely-not-a-real-editor-binary")
	_, err := editPromptInEditor("initial")
	if err == nil {
		t.Fatal("expected editor launch error with nonexistent editor")
	}
}

func TestLoadAgentPromptState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()

	agentID := "agent-" + uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ($1, 'stub', 'tester', 'holding', 'active', '{"system_prompt":"Template"}'::jsonb, now(), now())
	`, agentID); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS prompt_overrides (
			agent_id TEXT PRIMARY KEY REFERENCES agents(id) ON DELETE CASCADE,
			prompt TEXT NOT NULL,
			previous_prompt TEXT,
			source TEXT,
			notes TEXT,
			created_at TIMESTAMPTZ DEFAULT now(),
			updated_at TIMESTAMPTZ DEFAULT now()
		)
	`); err != nil {
		t.Fatalf("create prompt_overrides: %v", err)
	}

	template, override, has, err := loadAgentPromptState(ctx, db, agentID)
	if err != nil {
		t.Fatalf("load prompt without override: %v", err)
	}
	if template != "Template" || override != "" || has {
		t.Fatalf("unexpected state without override: template=%q override=%q has=%v", template, override, has)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO prompt_overrides (agent_id, prompt, previous_prompt, source, notes)
		VALUES ($1, 'Override', 'Template', 'test', 'n')
	`, agentID); err != nil {
		t.Fatalf("seed override: %v", err)
	}
	template, override, has, err = loadAgentPromptState(ctx, db, agentID)
	if err != nil {
		t.Fatalf("load prompt with override: %v", err)
	}
	if template != "Template" || override != "Override" || !has {
		t.Fatalf("unexpected state with override: template=%q override=%q has=%v", template, override, has)
	}

	if _, _, _, err := loadAgentPromptState(ctx, db, "missing-agent"); err == nil {
		t.Fatal("expected missing agent error")
	}
}
