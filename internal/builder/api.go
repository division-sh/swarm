package builder

import (
	"context"
	"net/http"
	"strings"
	"time"

	runtimepkg "swarm/internal/runtime"
	runtimecredentials "swarm/internal/runtime/credentials"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/store"
)

type HealthChecker func(ctx context.Context) (map[string]any, error)

type InstanceReader interface {
	List(ctx context.Context) ([]runtimepipeline.WorkflowInstance, error)
	Load(ctx context.Context, instanceID string) (runtimepipeline.WorkflowInstance, bool, error)
}

type RuntimeController interface {
	PauseIngress()
	ResumeIngress()
	ResetState() error
}

type SourceProvider func() semanticview.Source

type RuntimeProvider func() *runtimepkg.Runtime

type RunDebugReader interface {
	LoadRunDebugReport(ctx context.Context, runID string, opts store.RunDebugQueryOptions) (store.RunDebugReport, error)
}

type ProjectStatus struct {
	ProjectDir      string `json:"project_dir,omitempty"`
	Loaded          bool   `json:"loaded"`
	WorkflowName    string `json:"workflow_name,omitempty"`
	WorkflowVersion string `json:"workflow_version,omitempty"`
}

type ProjectController interface {
	OpenProject(ctx context.Context, projectDir string) (ProjectStatus, error)
	ReloadProject(ctx context.Context, projectDir string) (ProjectStatus, error)
	CloseProject(ctx context.Context) (ProjectStatus, error)
	CurrentProject() ProjectStatus
}

type Options struct {
	Health         HealthChecker
	Instances      InstanceReader
	Runtime        RuntimeController
	Credentials    runtimecredentials.Store
	AuthToken      string
	Version        string
	SemanticSource semanticview.Source
	RuntimeRef     *runtimepkg.Runtime
	CurrentSource  SourceProvider
	CurrentRuntime RuntimeProvider
	ProjectControl ProjectController
	RunDebug       RunDebugReader
}

type Request struct {
	JSONRPC string         `json:"jsonrpc,omitempty"`
	ID      any            `json:"id"`
	Method  string         `json:"method"`
	Params  map[string]any `json:"params,omitempty"`
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

type RPCResponse struct {
	JSONRPC string    `json:"jsonrpc,omitempty"`
	ID      any       `json:"id"`
	Result  any       `json:"result,omitempty"`
	Error   *RPCError `json:"error,omitempty"`
}

type WSSubscribeFrame struct {
	Type    string `json:"type"`
	Channel string `json:"channel"`
}

type WSUnsubscribeFrame struct {
	Type    string `json:"type"`
	Channel string `json:"channel"`
}

type WSEventFrame struct {
	Type    string `json:"type"`
	Channel string `json:"channel"`
	Data    any    `json:"data"`
}

type ValidationIssue struct {
	CheckID    string `json:"check_id"`
	Severity   string `json:"severity"`
	Message    string `json:"message"`
	FlowPath   string `json:"flow_path,omitempty"`
	NodeID     string `json:"node_id,omitempty"`
	AgentID    string `json:"agent_id,omitempty"`
	Suggestion string `json:"suggestion,omitempty"`
}

type ValidationSummary struct {
	Errors       int   `json:"errors"`
	Warnings     int   `json:"warnings"`
	FlowsChecked int   `json:"flows_checked"`
	DurationMS   int64 `json:"duration_ms"`
}

type ValidationResult struct {
	Status   string            `json:"status"`
	Errors   []ValidationIssue `json:"errors"`
	Warnings []ValidationIssue `json:"warnings"`
	Summary  ValidationSummary `json:"summary"`
}

type CredentialRequirement struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

type CredentialRecord struct {
	Key        string                  `json:"key"`
	Present    bool                    `json:"present"`
	Source     string                  `json:"source,omitempty"`
	Writable   bool                    `json:"writable"`
	UpdatedAt  string                  `json:"updated_at,omitempty"`
	RequiredBy []CredentialRequirement `json:"required_by,omitempty"`
}

type EngineHealth struct {
	Status      string         `json:"status"`
	Version     string         `json:"version"`
	Timestamp   string         `json:"timestamp"`
	Ready       bool           `json:"ready,omitempty"`
	Runtime     map[string]any `json:"runtime,omitempty"`
	Database    map[string]any `json:"database,omitempty"`
	DatabaseErr string         `json:"database_error,omitempty"`
	Project     ProjectStatus  `json:"project,omitempty"`
}

func NewHandler(opts Options) http.Handler {
	h := &handler{
		health:         opts.Health,
		instances:      opts.Instances,
		runtime:        opts.Runtime,
		credentials:    opts.Credentials,
		authToken:      strings.TrimSpace(opts.AuthToken),
		version:        strings.TrimSpace(opts.Version),
		semanticSource: opts.SemanticSource,
		currentSource:  opts.CurrentSource,
		currentRuntime: opts.CurrentRuntime,
		projectControl: opts.ProjectControl,
	}
	if h.currentRuntime == nil && opts.RuntimeRef != nil {
		h.currentRuntime = func() *runtimepkg.Runtime { return opts.RuntimeRef }
	}
	if h.currentRuntime != nil {
		h.runHub = newRunHub(
			h.currentRuntime,
			func() error {
				if h.runtime == nil {
					return errUnavailable("runtime controller is not configured")
				}
				return h.runtime.ResetState()
			},
			func() error {
				if h.runtime == nil {
					return errUnavailable("runtime controller is not configured")
				}
				h.runtime.PauseIngress()
				return nil
			},
			func() error {
				if h.runtime == nil {
					return errUnavailable("runtime controller is not configured")
				}
				h.runtime.ResumeIngress()
				return nil
			},
			opts.RunDebug,
		)
	}
	return h
}

func SetHealthHeartbeatIntervalForTest(d time.Duration) func() {
	old := healthHeartbeatInterval
	healthHeartbeatInterval = d
	return func() {
		healthHeartbeatInterval = old
	}
}

func SetRunCompletionTimeoutForTest(d time.Duration) func() {
	old := runCompletionTimeout
	runCompletionTimeout = d
	return func() {
		runCompletionTimeout = old
	}
}

func HandleRuntimeLogForTest(h http.Handler, entry runtimepkg.RuntimeLogEntry) bool {
	typed, ok := h.(*handler)
	if !ok || typed == nil || typed.runHub == nil {
		return false
	}
	typed.runHub.handleRuntimeLog(entry)
	return true
}
