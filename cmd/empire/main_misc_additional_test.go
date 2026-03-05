package main

import (
	"context"
	"path/filepath"
	"testing"

	"empireai/internal/store"
	"empireai/internal/testutil"
)

func TestNormalizeAgentAlias_AllMappings(t *testing.T) {
	cases := map[string]string{
		"ceo":             "opco-ceo",
		"head-of-product": "vp-product",
		"hog":             "vp-growth",
		"head-of-growth":  "vp-growth",
		"cto":             "cto-agent",
		"pm":              "pm-agent",
		"support":         "support-agent",
		"marketing":       "marketing-agent",
		"frontend":        "frontend-agent",
		"qa":              "qa-agent",
		"devops":          "devops-agent",
		"cos":             "chief-of-staff",
	}
	for in, want := range cases {
		if got := normalizeAgentAlias(in); got != want {
			t.Fatalf("normalizeAgentAlias(%q)=%q want=%q", in, got, want)
		}
	}
}

func TestApplyManagedMigrations_ErrorBranches(t *testing.T) {
	if err := applyManagedMigrations(context.Background(), nil, nil); err == nil {
		t.Fatal("expected postgres store required error")
	}

	root := repoRootFromCmd(t)
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}

	err := applyManagedMigrations(context.Background(), pg, []migrationSpec{
		{Version: 99, Name: "missing", Path: filepath.Join(root, "migrations", "does-not-exist.sql")},
	})
	if err == nil {
		t.Fatal("expected migration apply error for missing file")
	}
}

func TestBuildWorkspaceLifecycle_DockerFailureFallsBack(t *testing.T) {
	ctx := context.Background()
	if got := buildWorkspaceLifecycle(ctx, nil); got != nil {
		t.Fatalf("expected nil lifecycle when db=nil, got %#v", got)
	}

	_, db, _ := testutil.StartPostgres(t)
	t.Setenv("EMPIREAI_ENABLE_DOCKER_WORKSPACES", "true")
	t.Setenv("EMPIREAI_REQUIRE_DOCKER_WORKSPACES", "false")
	t.Setenv("EMPIREAI_DOCKER_BIN", "definitely-not-a-real-docker-binary")
	if got := buildWorkspaceLifecycle(ctx, db); got != nil {
		t.Fatalf("expected fallback nil lifecycle when docker bootstrap fails, got %#v", got)
	}
}

func TestEditPromptInEditor_SuccessPath(t *testing.T) {
	t.Setenv("EDITOR", "true")
	out, err := editPromptInEditor("hello prompt")
	if err != nil {
		t.Fatalf("editPromptInEditor should succeed with EDITOR=true: %v", err)
	}
	if out != "hello prompt" {
		t.Fatalf("unexpected edited content: %q", out)
	}
}

