package builder

import (
	"context"
	"strings"
	"time"

	runtimebootverify "swarm/internal/runtime/bootverify"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
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

func entityPayload(instance runtimepipeline.WorkflowInstance) map[string]any {
	entity := map[string]any{"state": strings.TrimSpace(instance.CurrentState)}
	for key, value := range instance.Metadata {
		key = strings.TrimSpace(key)
		if key == "" || key == "gates" {
			continue
		}
		switch key {
		case "slug", "name", "entity_type", "storage_ref", "instance_id", "flow_path", "instance_kind", "template_version", "last_source_event", "status", "transition_history":
			continue
		}
		entity[key] = value
	}
	return entity
}

func entityGates(instance runtimepipeline.WorkflowInstance) map[string]any {
	gates, _ := instance.Metadata["gates"].(map[string]any)
	if len(gates) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(gates))
	for key, value := range gates {
		out[key] = value
	}
	return out
}

func entityAccumulated(instance runtimepipeline.WorkflowInstance) map[string]any {
	if len(instance.StateBuckets) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(instance.StateBuckets))
	for key, value := range instance.StateBuckets {
		out[key] = value
	}
	return out
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
