package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
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

func newStartupReadinessTestRuntime(t testing.TB, nodes ...runtimepipeline.BackgroundNode) *Runtime {
	t.Helper()
	fact := testSourceArtifactFact(t, runtimeTestBundleHash)
	_, grant, err := newRuntimeTestProcessCapability(t, nil, nil, fact, authorActivityTestRuntimeInstanceID)
	if err != nil {
		t.Fatalf("construct startup readiness generation grant: %v", err)
	}
	return &Runtime{
		Config:         testOperationalRuntimeConfig(),
		SystemNodes:    nodes,
		workOccurrence: runtimeTestOccurrence(t, runtimeTestBundleHash),
		startupGrant:   grant,
		Options: RuntimeOptions{
			DisablePersistentStartupRecovery: true,
			WorkflowModule:                   startupReadinessWorkflowModule{},
			SourceArtifactFact:               fact,
		},
	}
}

func TestRuntimeStartWaitsForSystemNodeSubscriptionReadiness(t *testing.T) {
	node := newDelayedSubscriptionBackgroundNode()
	rt := newStartupReadinessTestRuntime(t, node)
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

func TestPreparedRuntimeStartupKeepsFirstCandidateUnadmittedWhileSecondBlocksOrFails(t *testing.T) {
	for _, failSecond := range []bool{false, true} {
		t.Run(fmt.Sprint(failSecond), func(t *testing.T) {
			ctx, cancel := context.WithCancel(testAuthorActivityContext(context.Background()))
			defer cancel()
			first := newStartupReadinessTestRuntime(t)
			firstStart, err := first.PrepareStart(ctx)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := first.Shutdown(); err != nil {
					t.Error(err)
				}
			})
			node := newDelayedSubscriptionBackgroundNode()
			second := newStartupReadinessTestRuntime(t, node)
			t.Cleanup(func() {
				if err := second.Shutdown(); err != nil {
					t.Error(err)
				}
			})
			type preparedResult struct {
				start *PreparedStartup
				err   error
			}
			done := make(chan preparedResult, 1)
			go func() { start, err := second.PrepareStart(ctx); done <- preparedResult{start, err} }()
			select {
			case <-node.started:
			case <-time.After(5 * time.Second):
				t.Fatal("second candidate did not reach subscription barrier")
			}
			grant, err := first.CurrentStartupGrantEvidence()
			if err != nil || grant.State != startupownership.GrantPrepared {
				t.Fatalf("first candidate escaped before complete-set preparation: %+v, %v", grant, err)
			}
			if failSecond {
				cancel()
			} else {
				close(node.release)
			}
			var result preparedResult
			select {
			case result = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("second preparation did not settle")
			}
			if failSecond {
				if !errors.Is(result.err, context.Canceled) {
					t.Fatalf("second failure = %v", result.err)
				}
				if err := first.Shutdown(); err != nil {
					t.Fatal(err)
				}
				if err := firstStart.Start(); err == nil {
					t.Fatal("retired prepared runtime reopened execution")
				}
				return
			}
			if result.err != nil {
				t.Fatal(result.err)
			}
			for _, start := range []*PreparedStartup{firstStart, result.start} {
				if err := start.Start(); err != nil {
					t.Fatal(err)
				}
				if err := start.Start(); err == nil {
					t.Fatal("prepared startup released execution twice")
				}
			}
			grant, err = first.CurrentStartupGrantEvidence()
			if err != nil || grant.State != startupownership.GrantAdmitted {
				t.Fatalf("converged startup = %+v, %v", grant, err)
			}
		})
	}
}

func TestPreparedRuntimeStartupRejectsCancellationBeforeExecutionRelease(t *testing.T) {
	rt := newStartupReadinessTestRuntime(t)
	ctx, cancel := context.WithCancel(testAuthorActivityContext(context.Background()))
	defer cancel()
	t.Cleanup(func() {
		if err := rt.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	prepared, err := rt.PrepareStart(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := prepared.Start(); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled preparation execution release = %v", err)
	}
	grant, err := rt.CurrentStartupGrantEvidence()
	if err == nil && grant.State == startupownership.GrantAdmitted {
		t.Fatalf("canceled preparation admitted execution: %+v", grant)
	}
	if err := prepared.Start(); err == nil {
		t.Fatal("canceled preparation can be retried")
	}
}

func TestRuntimeStartFailsClosedWhenSystemNodeSubscriptionReadinessIsCanceled(t *testing.T) {
	node := newDelayedSubscriptionBackgroundNode()
	rt := newStartupReadinessTestRuntime(t, node)
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
	rt := newStartupReadinessTestRuntime(t, node)
	err := rt.Start(testAuthorActivityContext(context.Background()))
	if err == nil || !strings.Contains(err.Error(), "cannot report subscription readiness") {
		t.Fatalf("Start error = %v, want subscription readiness reporting failure", err)
	}
	if node.ran {
		t.Fatal("system node ran despite missing subscription readiness reporting")
	}
}
