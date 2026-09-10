package serveapp

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

// Keep real grants and persistence; inject only at the selected startup boundary.
type startupAbortGrant struct {
	startupownership.GenerationGrant
	evidenceErr error
	settleErr   error
	retireErr   error
	onRetire    func()
	onSettle    func()
}

func (g *startupAbortGrant) Evidence() (startupownership.GrantEvidence, error) {
	if g.evidenceErr != nil {
		return startupownership.GrantEvidence{}, g.evidenceErr
	}
	return g.GenerationGrant.Evidence()
}

func (g *startupAbortGrant) MarkProbesSettled(ctx context.Context, ids []string) (startupownership.GrantEvidence, error) {
	if g.onSettle != nil {
		g.onSettle()
	}
	if g.settleErr != nil {
		return startupownership.GrantEvidence{}, g.settleErr
	}
	return g.GenerationGrant.MarkProbesSettled(ctx, ids)
}

func (g *startupAbortGrant) Retire(ctx context.Context) error {
	if g.onRetire != nil {
		g.onRetire()
	}
	return errors.Join(g.GenerationGrant.Retire(ctx), g.retireErr)
}

type startupAbortUnreadyNode struct{}

func (startupAbortUnreadyNode) Run(context.Context) {}
func (startupAbortUnreadyNode) String() string      { return "startup-abort-unready" }

func TestServeStartupAbortFailureMatrixBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, reset := range []bool{false, true} {
			for _, phase := range []string{"catalog", "lifecycle", "preparation", "preparation_cancel", "registration", "release", "release_cancel", "prepare_panic", "release_panic", "release_cleanup_error"} {
				for failed := 0; failed < 3; failed++ {
					t.Run(fmt.Sprintf("%s/reset=%t/%s/context=%d", backend, reset, phase, failed), func(t *testing.T) {
						testServeStartupAbortFailure(t, backend, reset, phase, failed)
					})
				}
			}
		}
	}
}

func testServeStartupAbortFailure(t *testing.T, backend string, reset bool, phase string, failed int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stores := openStandingRuntimeContextStore(t, backend, "startup-abort")
	cfg, err := config.Load(writeServeRuntimeTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	stubServeRuntimeWorkspaceLifecycle(t)
	process := worklifetime.NewProcess()
	instance := uuid.NewString()
	ctx = correlation.WithRuntimeInstanceID(ctx, instance)
	ctx = authoractivity.WithScope(ctx, authoractivity.RuntimeScope(instance))
	capability, err := stores.StartupOwnership().AcquireProcessCapability(ctx, startupownership.AcquireRequest{
		OwnerID: "startup-abort:" + instance, BootID: uuid.NewString(), RuntimeInstanceID: instance,
	})
	if err != nil {
		t.Fatal(err)
	}
	manager, err := runtime.NewRuntimeContextManager(stores.RunBundleAvailability())
	if err != nil {
		t.Fatal(err)
	}
	var candidates []serveRuntimeBundleContext
	t.Cleanup(func() {
		cancel()
		if err := manager.QuiesceAllRuntimeContexts(context.Background()); err != nil {
			t.Error(err)
		}
		// A manager does not own candidates that failed before registration.
		for _, candidate := range candidates {
			if err := candidate.runtime.Shutdown(); err != nil {
				t.Error(err)
			}
		}
		if err := closeSelectedStoreTestProcess(process, capability); err != nil {
			t.Error(err)
			return
		}
		for _, candidate := range candidates {
			if err := candidate.loaded.cleanup(); err != nil {
				t.Error(err)
			}
		}
	})
	credentials := processIngressCredentialStore{"telegram_bot_token": "abort-test-token", "webhook_signing.telegram": "abort-test-secret"}
	roots := []string{
		writeStandingTelegramServeFixture(t, "http://127.0.0.1:1"),
		writeServedEventPublishFollowUpFixture(t),
		writeServeRuntimeAgentSlugFixture(t, "startup-abort-third", "abort-third-agent"),
	}
	for _, root := range roots {
		loaded, err := loadServeRuntimeBundle(ctx, repoRootForTest(), stores.SourceArtifactStore(), cliapp.CLISourcePlatformSpecPaths{
			SourceRoot: root, PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath),
		}, cliapp.ServeOptions{}, testPlatformPackBaseGenerations(t))
		if err != nil {
			t.Fatal(err)
		}
		candidate, err := buildServeRuntimeBundleContext(serveRuntimeBundleContextRequest{
			ExecutionPosture: executionposture.Live,
			Ctx:              ctx, Stores: projectServeRuntimePersistence(stores), Config: cfg, Loaded: loaded,
			WorkspaceBackend:       cliapp.WorkspaceBackendSelection{Backend: "host"},
			ProviderTriggerCatalog: testProviderTriggerCatalog(t), ProcessWorkOwner: process, RuntimeInstanceID: instance,
			Credentials: credentials, ProviderCredentials: credentials,
			Options: cliapp.ServeOptions{TestLLMRuntime: servedNoopLLMRuntime{}, ShutdownGrace: 5 * time.Second},
		})
		if err != nil {
			_ = loaded.cleanup()
			t.Fatal(err)
		}
		candidates = append(candidates, candidate)
	}
	plan, err := compileServeSourceSetPlan(candidates)
	if err != nil {
		t.Fatal(err)
	}
	if err := installServeSourceSet(ctx, capability, plan); err != nil {
		t.Fatal(err)
	}
	grants := make([]*startupAbortGrant, len(candidates))
	for i, candidate := range candidates {
		grant, err := capability.IssueGenerationGrant(ctx, startupownership.GrantRequest{
			BundleHash: candidate.sourceArtifactFact.BundleHash(), RuntimeInstanceID: instance,
			RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
		})
		if err != nil {
			t.Fatal(err)
		}
		grants[i] = &startupAbortGrant{GenerationGrant: grant}
		if err := candidate.runtime.InstallStartupGrant(grants[i]); err != nil {
			t.Fatal(err)
		}
	}
	reconciled, err := reconcileServeStandingServices(ctx, candidates[0].runtime.Pipeline, candidates)
	if err != nil {
		t.Fatal(err)
	}
	for i := range candidates {
		targets, activations, err := reconcileServeRuntimeStandingTargets(candidates[i].runtime, reconciled)
		if err != nil {
			t.Fatal(err)
		}
		candidates[i].startupStandingTargets, candidates[i].startupStandingActivations = targets, activations
	}
	definitions, err := plannedServeRuntimeContexts(candidates)
	if err != nil {
		t.Fatal(err)
	}
	if reset {
		if err := manager.StageRecoveredRuntimeContexts(definitions...); err != nil {
			t.Fatal(err)
		}
	}
	startCause := errors.New("independent startup refusal")
	cleanupCause := errors.New("independent grant retirement receipt failure")
	registrar := projectServeRuntimePersistence(stores).deps.EventStore.(runtime.AuthorActivityCatalogRegistrar)
	var conflictingCatalog *authoractivity.EventCatalogLease
	if phase == "catalog" {
		conflictingCatalog, err = registrar.RegisterAuthorActivityEventCatalog(authoractivity.BundleScope(instance, candidates[failed].sourceArtifactFact.BundleHash()), []authoractivity.EventDescriptor{
			{EventType: "startup.abort.conflicting.catalog", Disposition: authoractivity.StoryAuthored},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer conflictingCatalog.Release()
	}
	var completed [3]bool
	for i := range candidates {
		candidates[i].runtime.Options.BootProgress = func(event runtime.BootProgressEvent) {
			if event.Step == 19 && event.Status == "ok" {
				completed[i] = true
			}
			if i == failed && phase == "preparation_cancel" && event.Step == 5 {
				cancel()
			}
			if i == failed && ((phase == "prepare_panic" && event.Step == 5) || (phase == "release_panic" && event.Step == 16 && event.Status == "ok")) {
				panic(startCause)
			}
		}
		grants[i].onRetire = func() {
			for _, candidate := range candidates {
				lease, err := candidate.runtime.WorkOccurrence().Begin(context.Background())
				if err == nil {
					_ = lease.Done()
					t.Error("unregistered sibling not fenced when grant cleanup began")
				}
				use, _, _ := manager.AcquireBundleHash(context.Background(), candidate.sourceArtifactFact.BundleHash())
				if use != nil {
					_ = use.Done()
					t.Error("sibling selectable when grant cleanup began")
				}
			}
		}
	}
	switch phase {
	case "lifecycle":
		grants[failed].evidenceErr = startCause
	case "preparation":
		candidates[failed].runtime.SystemNodes = append(candidates[failed].runtime.SystemNodes, startupAbortUnreadyNode{})
	case "registration":
		// Normal registration fails after earlier registrations; reset fails
		// inside complete-set publication after earlier child construction.
		candidates[failed].packInventoryDigest = "invalid-startup-abort-digest"
	case "release", "release_cleanup_error":
		grants[failed].settleErr = startCause
		if phase == "release_cleanup_error" {
			for _, grant := range grants {
				grant.retireErr = cleanupCause
			}
		}
	case "release_cancel":
		grants[failed].onSettle = cancel
		grants[failed].settleErr = context.Canceled
	}
	var release func() error
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		if reset {
			release, err = prepareResetServeRuntimeContexts(ctx, candidates, manager)
		} else {
			release, err = prepareServeRuntimeContexts(ctx, candidates, manager)
		}
		if err != nil {
			return
		}
		if !strings.HasPrefix(phase, "release") {
			t.Fatal("expected preparation failure")
		}
		if candidates[0].runtime.WorkOccurrence().ActiveCount() == 0 {
			t.Fatal("release probe has no real registered standing child")
		}
		for _, candidate := range candidates {
			lookup := manager.LookupBundleHashStatus(candidate.sourceArtifactFact.BundleHash())
			if !lookup.Loaded() {
				t.Fatal("release failed before real complete-set publication")
			}
		}
		if reset {
			err = manager.ReleaseResetExecution(definitions...)
			if err != nil {
				t.Fatal(err)
			}
		}
		err = release()
	}()
	if strings.HasSuffix(phase, "panic") {
		if recovered != startCause {
			t.Fatalf("panic identity changed: %v", recovered)
		}
	} else {
		if recovered != nil {
			t.Fatalf("unexpected panic: %v", recovered)
		}
		if strings.HasSuffix(phase, "cancel") {
			if !errors.Is(err, context.Canceled) || ctx.Err() != context.Canceled {
				t.Fatalf("owning cancellation lost: %v", err)
			}
		} else if err == nil || errors.Is(err, context.Canceled) || ctx.Err() != nil {
			t.Fatalf("independent failure not preserved: %v", err)
		}
		if phase == "lifecycle" || phase == "release" || phase == "release_cleanup_error" {
			if !errors.Is(err, startCause) {
				t.Fatalf("startup cause lost: %v", err)
			}
		}
		if phase == "release_cleanup_error" && !errors.Is(err, cleanupCause) {
			t.Fatalf("cleanup cause lost: %v", err)
		}
		if phase == "registration" && !strings.Contains(err.Error(), "pack inventory digest") {
			t.Fatalf("wrong registration/publication failure: %v", err)
		}
		if phase == "catalog" && !strings.Contains(err.Error(), "conflicts") {
			t.Fatalf("wrong catalog failure: %v", err)
		}
	}
	if conflictingCatalog != nil {
		conflictingCatalog.Release()
	}
	if process.ActiveCount() != 0 {
		t.Errorf("abort returned with %d runtime parent leases", process.ActiveCount())
	}
	for i, candidate := range candidates {
		if strings.HasPrefix(phase, "release") && completed[i] != (i < failed) {
			t.Errorf("context %d completed=%t; failure position=%d", i, completed[i], failed)
		}
		if candidate.runtime.WorkOccurrence().ActiveCount() != 0 || candidate.runtime.Manager.IsRunning() {
			t.Errorf("context %d leaked runtime work", i)
		}
		select {
		case <-grants[i].Done():
		default:
			t.Errorf("context %d retained its generation grant", i)
		}
		if _, err := candidate.runtime.CurrentStartupGrantEvidence(); err == nil {
			t.Errorf("context %d retained grant visibility", i)
		}
		lookup := manager.LookupBundleHashStatus(candidate.sourceArtifactFact.BundleHash())
		if lookup.Loaded() {
			t.Errorf("context %d retained loaded visibility", i)
		}
		if !reset && (strings.HasPrefix(phase, "preparation") || phase == "registration" || phase == "prepare_panic") && lookup.Found != (i < failed) {
			t.Errorf("context %d registration boundary not exercised: found=%t", i, lookup.Found)
		}
		// A different descriptor set conflicts if even one runtime lease remains.
		lease, err := registrar.RegisterAuthorActivityEventCatalog(authoractivity.BundleScope(instance, candidate.sourceArtifactFact.BundleHash()), []authoractivity.EventDescriptor{
			{EventType: "startup.abort.descriptor.probe", Disposition: authoractivity.StoryAuthored},
		})
		if err != nil {
			t.Errorf("context %d retained event descriptors: %v", i, err)
		} else {
			lease.Release()
		}
		grants[i].retireErr = nil
		if err := candidate.runtime.Shutdown(); err != nil {
			t.Errorf("duplicate stop: %v", err)
		}
	}
	if release != nil {
		if err := release(); err == nil {
			t.Error("duplicate release reopened aborted execution")
		}
	}
	// Exact parent settlement precedes releasing retained store possession.
	if err := closeSelectedStoreTestProcess(process, capability); err != nil {
		t.Fatal(err)
	}
	next, err := stores.StartupOwnership().AcquireProcessCapability(context.Background(), startupownership.AcquireRequest{
		OwnerID: "startup-abort-successor", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("aborted composition retained store possession: %v", err)
	}
	if err := next.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}
