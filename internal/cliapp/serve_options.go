package cliapp

import (
	"io"
	"time"

	"github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

type ServeOptions struct {
	ConfigPath                       string
	Backend                          string
	ContractsPath                    string
	DataSource                       string
	WorkspaceBackend                 string
	WorkspaceBackendSet              bool
	BundleHash                       string
	BundleHashes                     []string
	PlatformSpecPath                 string
	StoreMode                        string
	StoreModeSet                     bool
	SwarmDir                         string
	SwarmDirSet                      bool
	ContextName                      string
	ContextNameSet                   bool
	APITokenFile                     string
	APITokenFileFlagSet              bool
	APIListenAddr                    string
	MCPListenAddr                    string
	ShutdownGrace                    time.Duration
	Dev                              bool
	SelfCheck                        bool
	RequireBundleMatch               bool
	NoRequireBundleMatch             bool
	AbandonActiveRuns                bool
	Verbose                          bool
	NoFeed                           bool
	NoColor                          bool
	Output                           io.Writer
	ErrorOutput                      io.Writer
	LocalRun                         bool
	TestEntityStateHook              func(entityID, state string)
	TestWorkflowNodeHandlerStartHook runtimepipeline.WorkflowNodeHandlerStartHook
	TestLifecycleProbe               runtimelifecycleprobe.Observer
	TestLLMRuntime                   runtimellm.Runtime
	TestOutboxSweeperConfig          runtimebus.OutboxSweeperConfig
	TestRuntimeReadyHook             func(*runtime.Runtime)
	TestRuntimeContextsReadyHook     func(*runtime.RuntimeContextManager)
	TestBeforeReadinessCommit        func() error
	TestAfterAuthorActivityHead      func() error
}

func DefaultServeOptions() ServeOptions {
	return ServeOptions{
		StoreMode:          storebackend.ActiveDefaultBackend().String(),
		APIListenAddr:      "127.0.0.1:8081",
		MCPListenAddr:      "127.0.0.1:8082",
		ShutdownGrace:      runtime.DefaultShutdownGrace,
		SelfCheck:          true,
		RequireBundleMatch: true,
	}
}
