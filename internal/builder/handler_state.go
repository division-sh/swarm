package builder

import (
	"context"
	"fmt"
	"strings"
	"time"

	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
)

func (h *handler) builderVersion() string {
	if version := strings.TrimSpace(h.version); version != "" {
		return version
	}
	return "dev"
}

func (h *handler) currentSemanticSource() semanticview.Source {
	if h.currentSource != nil {
		if source := h.currentSource(); source != nil {
			return source
		}
	}
	return h.semanticSource
}

func (h *handler) currentProjectStatus() ProjectStatus {
	if h.projectControl == nil {
		return ProjectStatus{}
	}
	return h.projectControl.CurrentProject()
}

func (h *handler) runFullValidation(_ context.Context) ValidationResult {
	startedAt := time.Now()
	flowCount := 0
	source := h.currentSemanticSource()
	if source != nil {
		flowCount = len(source.FlowSchemaEntries())
	}
	result := ValidationResult{
		Status:   "pass",
		Errors:   []ValidationIssue{},
		Warnings: []ValidationIssue{},
		Summary: ValidationSummary{
			FlowsChecked: flowCount,
		},
	}
	if source == nil {
		result.Status = "fail"
		result.Errors = append(result.Errors, ValidationIssue{
			CheckID:  "engine_source_unavailable",
			Severity: "error",
			Message:  "semantic source is not configured",
		})
		result.Summary.Errors = len(result.Errors)
		result.Summary.DurationMS = time.Since(startedAt).Milliseconds()
		return result
	}
	report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{
		Credentials:       h.credentials,
		CheckMCPReachable: true,
	})
	for _, finding := range report.Findings {
		issue := ValidationIssue{
			CheckID:  finding.CheckID,
			Severity: finding.Severity,
			Message:  finding.Message,
		}
		if finding.CheckID == "credential_key_exists" && finding.Severity == "warning" {
			issue.Suggestion = "set the credential with credentials.set before executing dependent tools"
		}
		if finding.Severity == "warning" {
			result.Warnings = append(result.Warnings, issue)
			continue
		}
		result.Errors = append(result.Errors, issue)
	}
	if len(result.Errors) > 0 {
		result.Status = "fail"
	}
	result.Summary.Errors = len(result.Errors)
	result.Summary.Warnings = len(result.Warnings)
	result.Summary.DurationMS = time.Since(startedAt).Milliseconds()
	return result
}

func (h *handler) legacyBuilderListEntities(ctx context.Context) ([]store.OperatorEntitySummary, error) {
	if h == nil || h.entities == nil {
		return nil, fmt.Errorf("entity reader is not configured")
	}
	opts := store.OperatorEntityListOptions{Limit: 500}
	out := []store.OperatorEntitySummary{}
	for {
		result, err := h.entities.ListOperatorEntities(ctx, opts)
		if err != nil {
			return nil, err
		}
		out = append(out, result.Entities...)
		if strings.TrimSpace(result.NextCursor) == "" {
			return out, nil
		}
		opts.Cursor = result.NextCursor
	}
}

func (h *handler) healthSnapshot(ctx context.Context) EngineHealth {
	snapshot := EngineHealth{
		Status:    "ok",
		Version:   h.builderVersion(),
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	if h.health == nil {
		return snapshot
	}
	checks, err := h.health(ctx)
	if err != nil {
		snapshot.Status = "degraded"
		snapshot.DatabaseErr = err.Error()
		return snapshot
	}
	if runtimeCheck, ok := checks["runtime"].(map[string]any); ok {
		snapshot.Runtime = runtimeCheck
		if ready, ok := runtimeCheck["ready"].(bool); ok {
			snapshot.Ready = ready
		}
	}
	if dbCheck, ok := checks["database"].(map[string]any); ok {
		snapshot.Database = dbCheck
	}
	if dbErr, ok := checks["database_error"].(string); ok {
		snapshot.DatabaseErr = strings.TrimSpace(dbErr)
		if snapshot.DatabaseErr != "" {
			snapshot.Status = "degraded"
		}
	}
	snapshot.Project = h.currentProjectStatus()
	return snapshot
}
