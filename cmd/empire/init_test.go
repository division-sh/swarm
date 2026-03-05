package main

import "testing"

func TestParseInitOptions_Defaults(t *testing.T) {
	opts, err := parseInitOptions(nil)
	if err != nil {
		t.Fatalf("parse defaults: %v", err)
	}
	if opts.TemplateVersion != "2.0.48" {
		t.Fatalf("expected template version 2.0.48, got %q", opts.TemplateVersion)
	}
	if opts.AgentsDir != "configs/agents" {
		t.Fatalf("unexpected agents dir: %q", opts.AgentsDir)
	}
	if opts.TemplateAgentsDir != "configs/agents/templates" {
		t.Fatalf("unexpected template agents dir: %q", opts.TemplateAgentsDir)
	}
	if opts.TemplateRoutesYML != "configs/agents/templates/routes.yaml" {
		t.Fatalf("unexpected template routes: %q", opts.TemplateRoutesYML)
	}
}

func TestParseInitOptions_Overrides(t *testing.T) {
	opts, err := parseInitOptions([]string{
		"--template-version", "9.9.9",
		"--agents-dir", "/tmp/a",
		"--template-agents-dir", "/tmp/t",
		"--template-routes-yaml", "/tmp/routes.yaml",
		"--self-check=false",
	})
	if err != nil {
		t.Fatalf("parse overrides: %v", err)
	}
	if opts.TemplateVersion != "9.9.9" {
		t.Fatalf("template version override failed: %q", opts.TemplateVersion)
	}
	if opts.AgentsDir != "/tmp/a" || opts.TemplateAgentsDir != "/tmp/t" || opts.TemplateRoutesYML != "/tmp/routes.yaml" {
		t.Fatalf("unexpected override values: %+v", opts)
	}
	if opts.SelfCheck {
		t.Fatal("expected self-check=false")
	}
}

func TestRunInitSubcommand_RejectsNonPostgresStore(t *testing.T) {
	err := runInitSubcommand([]string{"--store", "sqlite"})
	if err == nil {
		t.Fatal("expected non-postgres store rejection")
	}
}
