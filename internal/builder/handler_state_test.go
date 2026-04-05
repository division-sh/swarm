package builder

import (
	"context"
	"errors"
	"testing"
)

type projectControlStub struct {
	current ProjectStatus
}

func (s projectControlStub) OpenProject(context.Context, string) (ProjectStatus, error) {
	return s.current, nil
}
func (s projectControlStub) ReloadProject(context.Context, string) (ProjectStatus, error) {
	return s.current, nil
}
func (s projectControlStub) CloseProject(context.Context) (ProjectStatus, error) {
	return s.current, nil
}
func (s projectControlStub) CurrentProject() ProjectStatus { return s.current }

func TestHandlerHealthSnapshot_ProjectsAndErrors(t *testing.T) {
	h := &handler{
		version: "builder-test",
		health: func(context.Context) (map[string]any, error) {
			return nil, errors.New("db unavailable")
		},
		projectControl: projectControlStub{current: ProjectStatus{
			ProjectDir:      "/tmp/project",
			Loaded:          true,
			WorkflowName:    "demo",
			WorkflowVersion: "v1",
		}},
	}

	snapshot := h.healthSnapshot(context.Background())
	if snapshot.Status != "degraded" {
		t.Fatalf("status = %q, want degraded", snapshot.Status)
	}
	if snapshot.DatabaseErr != "db unavailable" {
		t.Fatalf("database_error = %q", snapshot.DatabaseErr)
	}
	if snapshot.Version != "builder-test" {
		t.Fatalf("version = %q", snapshot.Version)
	}
}
