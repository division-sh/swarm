package runtime

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type delayedSubscriptionBackgroundNode struct {
	started chan struct{}
	release chan struct{}

	mu    sync.Mutex
	hooks []func()
}

func newDelayedSubscriptionBackgroundNode() *delayedSubscriptionBackgroundNode {
	return &delayedSubscriptionBackgroundNode{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (n *delayedSubscriptionBackgroundNode) Run(ctx context.Context) {
	close(n.started)
	select {
	case <-ctx.Done():
		return
	case <-n.release:
	}
	n.mu.Lock()
	hooks := append([]func(){}, n.hooks...)
	n.mu.Unlock()
	for _, hook := range hooks {
		hook()
	}
	<-ctx.Done()
}

func (n *delayedSubscriptionBackgroundNode) AddSubscriptionReadyHook(fn func()) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.hooks = append(n.hooks, fn)
}

func (*delayedSubscriptionBackgroundNode) String() string {
	return "delayed-system-node"
}

type startupReadinessWorkflowModule struct{}

func (startupReadinessWorkflowModule) SemanticSource() semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
}

func (startupReadinessWorkflowModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return &runtimepipeline.WorkflowDefinition{}
}

func (startupReadinessWorkflowModule) WorkflowNodes() []runtimepipeline.WorkflowNode {
	return nil
}

func (startupReadinessWorkflowModule) GuardRegistry() runtimepipeline.GuardRegistry {
	return nil
}

func (startupReadinessWorkflowModule) ActionRegistry() runtimepipeline.ActionRegistry {
	return nil
}

func newStartupReadinessTestRuntime(nodes ...runtimepipeline.BackgroundNode) *Runtime {
	return &Runtime{
		Config:      testOperationalRuntimeConfig(),
		SystemNodes: nodes,
		Options: RuntimeOptions{
			DisablePersistentStartupRecovery: true,
			WorkflowModule:                   startupReadinessWorkflowModule{},
		},
	}
}

func TestRuntimeStartWaitsForSystemNodeSubscriptionReadiness(t *testing.T) {
	node := newDelayedSubscriptionBackgroundNode()
	rt := newStartupReadinessTestRuntime(node)
	startErr := make(chan error, 1)
	go func() {
		startErr <- rt.Start(testAuthorActivityContext(context.Background()))
	}()

	select {
	case <-node.started:
	case err := <-startErr:
		t.Fatalf("Start returned before node subscribed: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for system node to start")
	}

	select {
	case err := <-startErr:
		t.Fatalf("Start returned before subscription readiness: %v", err)
	default:
	}

	close(node.release)
	select {
	case err := <-startErr:
		if err != nil {
			t.Fatalf("Start after subscription readiness: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Start after subscription readiness")
	}
	if err := rt.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestRuntimeStartFailsClosedWhenSystemNodeSubscriptionReadinessIsCanceled(t *testing.T) {
	node := newDelayedSubscriptionBackgroundNode()
	rt := newStartupReadinessTestRuntime(node)
	ctx, cancel := context.WithCancel(testAuthorActivityContext(context.Background()))
	startErr := make(chan error, 1)
	go func() {
		startErr <- rt.Start(ctx)
	}()

	select {
	case <-node.started:
	case err := <-startErr:
		t.Fatalf("Start returned before node subscribed: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for system node to start")
	}
	cancel()

	select {
	case err := <-startErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Start error = %v, want context canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for canceled Start")
	}
}

type nonReportingBackgroundNode struct {
	ran bool
}

func (n *nonReportingBackgroundNode) Run(context.Context) {
	n.ran = true
}

func TestRuntimeStartRejectsSystemNodeWithoutSubscriptionReadiness(t *testing.T) {
	node := &nonReportingBackgroundNode{}
	rt := newStartupReadinessTestRuntime(node)
	err := rt.Start(testAuthorActivityContext(context.Background()))
	if err == nil || !strings.Contains(err.Error(), "cannot report subscription readiness") {
		t.Fatalf("Start error = %v, want subscription readiness reporting failure", err)
	}
	if node.ran {
		t.Fatal("system node ran despite missing subscription readiness reporting")
	}
}
