package builder

import (
	"context"
	"net/http"
	"strings"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
)

type HealthChecker func(ctx context.Context) (map[string]any, error)

type EntityReader interface {
	ListOperatorEntities(ctx context.Context, opts store.OperatorEntityListOptions) (store.OperatorEntityListResult, error)
	LoadOperatorEntity(ctx context.Context, entityID, runID string) (store.OperatorEntityFull, error)
}

type RuntimeController interface {
	PauseIngress() error
	ResumeIngress() error
}

type SourceProvider func() semanticview.Source

type RuntimeUse interface {
	Runtime() *runtimepkg.Runtime
	WorkContext() context.Context
	Done() error
}

type RuntimeAcquirer interface {
	AcquireCurrentRuntime(context.Context) (RuntimeUse, error)
	AcquireRunRuntime(context.Context, string) (RuntimeUse, error)
}

type RunDebugReader interface {
	ListOperatorEvents(ctx context.Context, opts store.OperatorEventListOptions) (store.OperatorEventListResult, error)
	ListOperatorRuntimeLogs(ctx context.Context, opts store.OperatorRuntimeLogListOptions) (store.OperatorRuntimeLogListResult, error)
	LoadRunLifecycleSnapshot(ctx context.Context, runID string) (runtimebus.RunLifecycleSnapshot, error)
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
	Health           HealthChecker
	Entities         EntityReader
	Runtime          RuntimeController
	Credentials      runtimecredentials.Store
	AuthToken        string
	Version          string
	SemanticSource   semanticview.Source
	CurrentSource    SourceProvider
	RuntimeAcquirer  RuntimeAcquirer
	ProjectControl   ProjectController
	RunDebug         RunDebugReader
	ProcessWorkOwner *worklifetime.Process
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
	CheckID     string   `json:"check_id"`
	Severity    string   `json:"severity"`
	Message     string   `json:"message"`
	Remediation string   `json:"remediation,omitempty"`
	Evidence    []string `json:"evidence,omitempty"`
	FlowPath    string   `json:"flow_path,omitempty"`
	NodeID      string   `json:"node_id,omitempty"`
	AgentID     string   `json:"agent_id,omitempty"`
	Suggestion  string   `json:"suggestion,omitempty"`
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
	Shadowed   bool                    `json:"shadowed"`
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
		health:           opts.Health,
		entities:         opts.Entities,
		runtime:          opts.Runtime,
		credentials:      opts.Credentials,
		authToken:        strings.TrimSpace(opts.AuthToken),
		version:          strings.TrimSpace(opts.Version),
		semanticSource:   opts.SemanticSource,
		currentSource:    opts.CurrentSource,
		runtimeAcquirer:  opts.RuntimeAcquirer,
		processWorkOwner: opts.ProcessWorkOwner,
		projectControl:   opts.ProjectControl,
	}
	if h.runtimeAcquirer != nil {
		h.runHub = newRunHub(
			h.runtimeAcquirer,
			func() error {
				if h.runtime == nil {
					return errUnavailable("runtime controller is not configured")
				}
				return h.runtime.PauseIngress()
			},
			func() error {
				if h.runtime == nil {
					return errUnavailable("runtime controller is not configured")
				}
				return h.runtime.ResumeIngress()
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
