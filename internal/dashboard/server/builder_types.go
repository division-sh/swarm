package server

import (
	"context"
	"strings"
)

type builderRPCRequest struct {
	JSONRPC string         `json:"jsonrpc,omitempty"`
	ID      any            `json:"id"`
	Method  string         `json:"method"`
	Params  map[string]any `json:"params,omitempty"`
}

type builderRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

type builderRPCResponse struct {
	JSONRPC string           `json:"jsonrpc,omitempty"`
	ID      any              `json:"id"`
	Result  any              `json:"result,omitempty"`
	Error   *builderRPCError `json:"error,omitempty"`
}

type builderWSSubscribeFrame struct {
	Type    string `json:"type"`
	Channel string `json:"channel"`
}

type builderWSUnsubscribeFrame struct {
	Type    string `json:"type"`
	Channel string `json:"channel"`
}

type builderWSEventFrame struct {
	Type    string `json:"type"`
	Channel string `json:"channel"`
	Data    any    `json:"data"`
}

type builderValidationIssue struct {
	CheckID    string `json:"check_id"`
	Severity   string `json:"severity"`
	Message    string `json:"message"`
	FlowPath   string `json:"flow_path,omitempty"`
	NodeID     string `json:"node_id,omitempty"`
	AgentID    string `json:"agent_id,omitempty"`
	Suggestion string `json:"suggestion,omitempty"`
}

type builderValidationSummary struct {
	Errors       int   `json:"errors"`
	Warnings     int   `json:"warnings"`
	FlowsChecked int   `json:"flows_checked"`
	DurationMS   int64 `json:"duration_ms"`
}

type builderValidationResult struct {
	Status   string                   `json:"status"`
	Errors   []builderValidationIssue `json:"errors"`
	Warnings []builderValidationIssue `json:"warnings"`
	Summary  builderValidationSummary `json:"summary"`
}

type builderEngineHealth struct {
	Status      string               `json:"status"`
	Version     string               `json:"version"`
	Timestamp   string               `json:"timestamp"`
	Ready       bool                 `json:"ready,omitempty"`
	Runtime     map[string]any       `json:"runtime,omitempty"`
	Database    map[string]any       `json:"database,omitempty"`
	DatabaseErr string               `json:"database_error,omitempty"`
	Project     BuilderProjectStatus `json:"project,omitempty"`
}

type BuilderProjectStatus struct {
	ProjectDir      string `json:"project_dir,omitempty"`
	Loaded          bool   `json:"loaded"`
	WorkflowName    string `json:"workflow_name,omitempty"`
	WorkflowVersion string `json:"workflow_version,omitempty"`
}

type BuilderProjectController interface {
	OpenProject(ctx context.Context, projectDir string) (BuilderProjectStatus, error)
	ReloadProject(ctx context.Context, projectDir string) (BuilderProjectStatus, error)
	CloseProject(ctx context.Context) (BuilderProjectStatus, error)
	CurrentProject() BuilderProjectStatus
}

func normalizeBuilderCheckID(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return "validation"
	}
	replacer := strings.NewReplacer(" ", "_", "-", "_", ".", "_", "/", "_")
	return replacer.Replace(raw)
}
