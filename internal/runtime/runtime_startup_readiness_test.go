package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
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

type startupReadinessWorkflowModule struct {
	source semanticview.Source
}

func (m startupReadinessWorkflowModule) SemanticSource() semanticview.Source {
	return m.source
}

func (startupReadinessWorkflowModule) WorkflowNodes() []runtimepipeline.WorkflowNode {
	return nil
}

func (startupReadinessWorkflowModule) GuardRegistry() runtimepipeline.GuardRegistry {
	return nil
}

func newStartupReadinessTestRuntime(t testing.TB, nodes ...runtimepipeline.BackgroundNode) *Runtime {
	return newStartupReadinessTestRuntimeWithSession(t, nil, nodes...)
}

func newStartupReadinessTestRuntimeWithSession(t testing.TB, decorate func(*runtimeTestRetainedSession) startupownership.RetainedSession, nodes ...runtimepipeline.BackgroundNode) *Runtime {
	t.Helper()
	fact := testSourceArtifactFact(t, runtimeTestBundleHash)
	rooted, _ := sourceartifactfixture.Rooted(t, "runtime-only", sourceartifactfixture.Artifact())
	bundle, err := runtimecontracts.LoadWorkflowContractBundleFromRootedSource(canonicalrouting.RepoRoot(t), rooted, "", runtimecontracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var decorators []func(*runtimeTestRetainedSession) startupownership.RetainedSession
	if decorate != nil {
		decorators = append(decorators, decorate)
	}
	_, grant, err := newRuntimeTestProcessCapabilityWithSession(t, nil, nil, fact, authorActivityTestRuntimeInstanceID, nil, decorators...)
	if err != nil {
		t.Fatalf("construct startup readiness generation grant: %v", err)
	}
	return &Runtime{
		Config:         testOperationalRuntimeConfig(),
		SystemNodes:    nodes,
		workOccurrence: runtimeTestOccurrence(t, runtimeTestBundleHash),
		startupGrant:   grant,
		deliveryStore:  &managedNativeRecoveryDeliveryStore{},
		Options: RuntimeOptions{
			DisablePersistentStartupRecovery: true,
			WorkflowModule:                   startupReadinessWorkflowModule{source: semanticview.Wrap(bundle)},
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
				if err := firstStart.Start(nil); err == nil {
					t.Fatal("retired prepared runtime reopened execution")
				}
				return
			}
			if result.err != nil {
				t.Fatal(result.err)
			}
			for _, start := range []*PreparedStartup{firstStart, result.start} {
				if err := start.Start(nil); err != nil {
					t.Fatal(err)
				}
				if err := start.Start(nil); err == nil {
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
	if err := prepared.Start(nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled preparation execution release = %v", err)
	}
	grant, err := rt.CurrentStartupGrantEvidence()
	if err == nil && grant.State == startupownership.GrantAdmitted {
		t.Fatalf("canceled preparation admitted execution: %+v", grant)
	}
	if err := prepared.Start(nil); err == nil {
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

type startupAbortCatalogRegistrar struct {
	registry *authoractivity.EventCatalogRegistry
	fail     func() error
}

func TestRuntimeStartNilReceiverReturnsError(t *testing.T) {
	var rt *Runtime
	if err := rt.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "runtime is nil") {
		t.Fatalf("nil runtime startup error = %v", err)
	}
}

type startupClaimBarrierContext struct {
	context.Context
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *startupClaimBarrierContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return c.Context.Done()
}

func TestRuntimeConcurrentStartDoesNotAbortWinningPreparation(t *testing.T) {
	for _, preparedContender := range []bool{false, true} {
		t.Run(fmt.Sprintf("prepared_contender=%t", preparedContender), func(t *testing.T) {
			rt := newStartupReadinessTestRuntime(t)
			ctx := testAuthorActivityContext(context.Background())
			barrier := &startupClaimBarrierContext{Context: ctx, entered: make(chan struct{}), release: make(chan struct{})}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(barrier.release) }) }
			defer release()
			t.Cleanup(func() {
				if err := rt.Shutdown(); err != nil {
					t.Error(err)
				}
			})
			slow := make(chan error, 1)
			go func() { slow <- rt.Start(barrier) }()
			select {
			case <-barrier.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("startup did not reach the pre-claim barrier")
			}
			// Without the shared preparation gate, deliberately let the
			// contender install cancelStart before the first call resumes.
			serialized := !rt.startupPrepareMu.TryLock()
			if !serialized {
				rt.startupPrepareMu.Unlock()
			}
			type result struct {
				prepared *PreparedStartup
				err      error
			}
			fast := make(chan result, 1)
			go func() {
				if preparedContender {
					prepared, err := rt.PrepareStart(ctx)
					fast <- result{prepared, err}
				} else {
					fast <- result{err: rt.Start(ctx)}
				}
			}()
			if serialized {
				release()
			}
			var contender result
			select {
			case contender = <-fast:
			case <-time.After(5 * time.Second):
				t.Fatal("contending startup did not settle")
			}
			release()
			var firstErr error
			select {
			case firstErr = <-slow:
			case <-time.After(5 * time.Second):
				t.Fatal("first startup did not settle")
			}
			if (firstErr == nil) == (contender.err == nil) {
				t.Fatalf("expected one startup owner: first=%v contender=%v", firstErr, contender.err)
			}
			if rt.shutdownAdmissionClosed() {
				t.Fatal("losing Start aborted the winning startup")
			}
			if contender.prepared != nil {
				if err := contender.prepared.Start(nil); err != nil {
					t.Fatalf("winning prepared startup was retired: %v", err)
				}
			}
			if grant, err := rt.CurrentStartupGrantEvidence(); err != nil || grant.State != startupownership.GrantAdmitted {
				t.Fatalf("loser retired winning authority: %+v, %v", grant, err)
			}
		})
	}
}

func TestRuntimeDuplicateStartDoesNotAbortExistingStartup(t *testing.T) {
	rt := newStartupReadinessTestRuntime(t)
	t.Cleanup(func() {
		if err := rt.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	ctx := testAuthorActivityContext(context.Background())
	if err := rt.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := rt.Start(ctx); err == nil || !strings.Contains(err.Error(), "already started") {
		t.Fatalf("duplicate startup refusal: %v", err)
	}
	if rt.shutdownAdmissionClosed() || rt.cancelStart == nil {
		t.Fatal("duplicate invocation aborted the existing startup")
	}
	if grant, err := rt.CurrentStartupGrantEvidence(); err != nil || grant.State != startupownership.GrantAdmitted {
		t.Fatalf("duplicate invocation retired existing authority: %+v, %v", grant, err)
	}
}

func (r startupAbortCatalogRegistrar) RegisterAuthorActivityEventCatalog(scope authoractivity.Scope, descriptors []authoractivity.EventDescriptor) (*authoractivity.EventCatalogLease, error) {
	if r.fail != nil {
		return nil, r.fail()
	}
	return r.registry.Register(scope, descriptors)
}

type startupReadinessFailingSession struct {
	startupownership.RetainedSession
	cause error
}

func (s startupReadinessFailingSession) RecordGenerationGrantTransition(ctx context.Context, previous *startupownership.GrantEvidence, next startupownership.GrantEvidence) error {
	if next.State == startupownership.GrantProbeSettled {
		return s.cause
	}
	return s.RetainedSession.RecordGenerationGrantTransition(ctx, previous, next)
}

func TestRuntimeStartOwnsEarlyFailureAndPanicCleanup(t *testing.T) {
	for _, phase := range []string{"catalog_error", "catalog_panic", "retired_preparation", "grant_panic", "prepare_panic", "release_error", "release_panic"} {
		t.Run(phase, func(t *testing.T) {
			cause := errors.New("startup boundary failure")
			var decorate func(*runtimeTestRetainedSession) startupownership.RetainedSession
			if phase == "release_error" {
				decorate = func(session *runtimeTestRetainedSession) startupownership.RetainedSession {
					return startupReadinessFailingSession{RetainedSession: session, cause: cause}
				}
			}
			rt := newStartupReadinessTestRuntimeWithSession(t, decorate)
			grant := rt.startupGrant
			registries := []*authoractivity.EventCatalogRegistry{
				authoractivity.NewEventCatalogRegistry(), authoractivity.NewEventCatalogRegistry(), authoractivity.NewEventCatalogRegistry(),
			}
			rt.authorActivityScope = authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, runtimeTestBundleHash)
			rt.authorActivityDescriptors = []authoractivity.EventDescriptor{{EventType: "startup.probe", Disposition: authoractivity.StoryAuthored}}
			for i, registry := range registries {
				registrar := startupAbortCatalogRegistrar{registry: registry}
				if i == 2 && strings.HasPrefix(phase, "catalog") {
					registrar.fail = func() error {
						if phase == "catalog_panic" {
							panic(cause)
						}
						return cause
					}
				}
				rt.authorActivityRegistrars = append(rt.authorActivityRegistrars, registrar)
			}
			if phase == "retired_preparation" {
				rt.CloseAdmission()
			}
			if phase == "grant_panic" {
				restore, err := startupownership.InstallGenerationGrantFaultProbeForTest(grant, startupownership.GenerationGrantFaultProbeForTest{
					EvidenceError: func() error { panic(cause) },
				})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(restore)
			}
			rt.Options.BootProgress = func(event BootProgressEvent) {
				if (phase == "prepare_panic" && event.Step == 5) || (phase == "release_panic" && event.Step == 16) {
					panic(cause)
				}
			}
			var startErr error
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				startErr = rt.Start(testAuthorActivityContext(context.Background()))
			}()
			if strings.HasSuffix(phase, "panic") {
				if recovered != cause {
					t.Fatalf("panic changed: %v", recovered)
				}
			} else if recovered != nil || startErr == nil {
				t.Fatalf("startup failure = %v, panic = %v", startErr, recovered)
			} else if phase != "retired_preparation" && !errors.Is(startErr, cause) {
				t.Fatalf("lost independent cause: %v", startErr)
			}
			for i, registry := range registries {
				if registry.HasScope(rt.authorActivityScope) {
					t.Errorf("registrar %d retained descriptors", i)
				}
			}
			select {
			case <-grant.Done():
			default:
				t.Error("standalone failure retained generation grant")
			}
			if rt.WorkOccurrence().ActiveCount() != 0 || rt.startupGrant != nil || rt.cancelStart != nil {
				t.Error("standalone failure did not finish runtime cleanup")
			}
			if err := rt.Shutdown(); err != nil {
				t.Errorf("duplicate shutdown: %v", err)
			}
		})
	}
}
