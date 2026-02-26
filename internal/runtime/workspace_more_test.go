package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"empireai/internal/models"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestDockerWorkspaceManager_RunDocker_ExecSuccessAndErrors(t *testing.T) {
	m := NewDockerWorkspaceManager(nil)
	m.cfg.DockerBin = "sh"

	out, err := m.runDocker(context.Background(), "-c", "printf 'ok'")
	if err != nil {
		t.Fatalf("expected success, err=%v", err)
	}
	if strings.TrimSpace(out) != "ok" {
		t.Fatalf("expected ok, got %q", out)
	}

	// Non-empty stderr should be included in the error text.
	_, err = m.runDocker(context.Background(), "-c", "echo bad 1>&2; exit 2")
	if err == nil || !strings.Contains(err.Error(), "bad") {
		t.Fatalf("expected error containing stderr, got %v", err)
	}

	// Empty output should still return an error (without appending an empty out string).
	_, err = m.runDocker(context.Background(), "-c", "exit 2")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDockerWorkspaceManager_InspectContainer_NoSuchObject(t *testing.T) {
	m := NewDockerWorkspaceManager(nil)
	m.runDockerFn = func(_ context.Context, args ...string) (string, error) {
		if len(args) > 0 && args[0] == "inspect" {
			return "", errors.New("Error: No such object: whatever")
		}
		return "", fmt.Errorf("unexpected args: %v", args)
	}

	exists, running, err := m.inspectContainer(context.Background(), "c1")
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if exists || running {
		t.Fatalf("expected not exists, got exists=%v running=%v", exists, running)
	}
}

func TestDockerWorkspaceManager_EnsureContainerRunning_CreateStart_AndAlreadyRunning(t *testing.T) {
	m := NewDockerWorkspaceManager(nil)
	var calls []string
	inspected := false
	m.runDockerFn = func(_ context.Context, args ...string) (string, error) {
		calls = append(calls, strings.Join(args, " "))
		if len(args) == 0 {
			return "", errors.New("missing args")
		}
		switch args[0] {
		case "inspect":
			if !inspected {
				inspected = true
				return "", errors.New("no such object")
			}
			return "false", nil
		case "create":
			return "created", nil
		case "start":
			return "", errors.New("already running")
		case "network":
			return "connected", nil
		default:
			return "", fmt.Errorf("unexpected: %v", args)
		}
	}

	if err := m.ensureContainerRunning(context.Background(), "c1", []string{"img", "sleep", "infinity"}); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if len(calls) < 3 {
		t.Fatalf("expected inspect/create/start calls, got %v", calls)
	}
}

func TestDockerWorkspaceManager_EnsureContainerRunning_StartFailure(t *testing.T) {
	m := NewDockerWorkspaceManager(nil)
	m.runDockerFn = func(_ context.Context, args ...string) (string, error) {
		if len(args) == 0 {
			return "", errors.New("missing args")
		}
		switch args[0] {
		case "inspect":
			return "false", nil
		case "start":
			return "", errors.New("boom")
		case "network":
			return "connected", nil
		default:
			return "", nil
		}
	}

	if err := m.ensureContainerRunning(context.Background(), "c1", []string{"img"}); err == nil {
		t.Fatal("expected error")
	}
}

func TestDockerWorkspaceManager_ResolveWorkspace_RoleAndVertical(t *testing.T) {
	m := NewDockerWorkspaceManager(nil)
	m.runDockerFn = func(_ context.Context, args ...string) (string, error) {
		// Pretend everything is already running.
		if len(args) > 0 && args[0] == "inspect" {
			return "true", nil
		}
		if len(args) > 1 && args[0] == "network" && args[1] == "connect" {
			return "connected", nil
		}
		return "", nil
	}

	// Role-based workspace.
	target, err := m.ResolveWorkspace(context.Background(), models.AgentConfig{Role: "factory-cto"})
	if err != nil {
		t.Fatalf("resolve role: %v", err)
	}
	if target == nil || target.Container != m.cfg.FactoryContainer || target.Workdir != m.cfg.FactoryWorkdir {
		t.Fatalf("unexpected target: %+v", target)
	}

	// Vertical-based workspace (db nil -> slug derived from vertical id).
	target2, err := m.ResolveWorkspace(context.Background(), models.AgentConfig{Role: "backend-agent", VerticalID: "VERT_123"})
	if err != nil {
		t.Fatalf("resolve vertical: %v", err)
	}
	if target2 == nil || !strings.HasPrefix(target2.Container, m.cfg.VerticalContainerPrefix) {
		t.Fatalf("unexpected target2: %+v", target2)
	}
	if target2.Workdir != m.cfg.VerticalWorkdir {
		t.Fatalf("unexpected workdir: %q", target2.Workdir)
	}
}

func TestDockerWorkspaceManager_LookupVerticalSlug_Branches(t *testing.T) {
	ctx := context.Background()

	m := NewDockerWorkspaceManager(nil)
	if _, err := m.lookupVerticalSlug(ctx, " "); err == nil {
		t.Fatal("expected vertical_id required error")
	}

	// No DB configured: fall back to sanitizing the ID.
	slug, err := m.lookupVerticalSlug(ctx, "  Ab_Cd / 123  ")
	if err != nil {
		t.Fatalf("lookup fallback: %v", err)
	}
	if slug != "ab-cd-123" {
		t.Fatalf("expected sanitized slug, got %q", slug)
	}

	// With DB: load slug, sanitize, and error on missing slug.
	_, db, _ := testutil.StartPostgres(t)
	m2 := NewDockerWorkspaceManager(db)
	m2.runDockerFn = func(_ context.Context, args ...string) (string, error) {
		if len(args) > 0 && args[0] == "inspect" {
			return "true", nil
		}
		return "", nil
	}

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid,'V',' My_Slug ','us','operating','operating','{}'::jsonb, now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	got, err := m2.lookupVerticalSlug(ctx, verticalID)
	if err != nil {
		t.Fatalf("lookup db: %v", err)
	}
	if got != "my-slug" {
		t.Fatalf("expected sanitized db slug, got %q", got)
	}

	verticalNoSlug := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid,'V','', 'us','operating','operating','{}'::jsonb, now(), now())
	`, verticalNoSlug); err != nil {
		t.Fatalf("seed no slug: %v", err)
	}
	if _, err := m2.lookupVerticalSlug(ctx, verticalNoSlug); err == nil {
		t.Fatal("expected error for missing slug")
	}
}

func TestDockerWorkspaceManager_StopContainer_AndInspectError(t *testing.T) {
	m := NewDockerWorkspaceManager((*sql.DB)(nil))
	stopped := false
	m.runDockerFn = func(_ context.Context, args ...string) (string, error) {
		if len(args) == 0 {
			return "", errors.New("missing args")
		}
		switch args[0] {
		case "inspect":
			return "true", nil
		case "stop":
			stopped = true
			return "stopped", nil
		default:
			return "", nil
		}
	}

	if err := m.stopContainer(context.Background(), "c1"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if !stopped {
		t.Fatal("expected stop to be called when running")
	}

	// inspectContainer should surface errors that are not "no such object".
	m2 := NewDockerWorkspaceManager(nil)
	m2.runDockerFn = func(_ context.Context, args ...string) (string, error) {
		return "", errors.New("permission denied")
	}
	if _, _, err := m2.inspectContainer(context.Background(), "c1"); err == nil {
		t.Fatal("expected inspect error")
	}
}

func TestDockerWorkspaceManager_EnsureContainerNetwork_AlreadyConnectedIsNonFatal(t *testing.T) {
	m := NewDockerWorkspaceManager(nil)
	m.cfg.WorkspaceNetwork = "empireai_default"
	m.runDockerFn = func(_ context.Context, args ...string) (string, error) {
		if len(args) >= 4 && args[0] == "network" && args[1] == "connect" {
			return "", errors.New("endpoint with name empireai-factory already exists in network empireai_default")
		}
		return "", nil
	}
	if err := m.ensureContainerNetwork(context.Background(), "empireai-factory"); err != nil {
		t.Fatalf("expected already-connected error to be ignored, got %v", err)
	}
}
