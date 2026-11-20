package serveapp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/publicingress"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
)

func TestInboundStopJoinsRegistrationWriterBeforeOrdinaryFenceBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, expires := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/expiry=%t", backend, expires), func(t *testing.T) {
				proveInboundStopRegistrationWriter(t, backend, expires)
			})
		}
	}
}

func proveInboundStopRegistrationWriter(t *testing.T, backend servedparity.Backend, expires bool) {
	t.Helper()
	managers := make(chan *runtime.RuntimeContextManager, 1)
	h := prepareChannelAnchorPublicJourney(t, backend, "writer-drain-token", false, 0, func(opts *cliapp.ServeOptions) {
		opts.TestRuntimeContextsReadyHook = func(m *runtime.RuntimeContextManager) { managers <- m }
	})
	h.start(t)
	var adopted bool
	t.Cleanup(func() {
		code := h.process.stop()
		// The fixture host observes the retained Runtime result of the adopted stop.
		if code != 0 && !(adopted && expires && strings.Contains(h.process.outputString(), "shutdown · failed ·") &&
			strings.Contains(h.process.outputString(), context.DeadlineExceeded.Error())) {
			t.Errorf("fixture host exit=%d\n%s", code, h.process.outputString())
		}
	})
	hash := channelAnchorBundleHash(t, h.rpcEndpoint())
	manager := <-managers
	loaded := manager.LoadedContexts()
	if len(loaded) != 1 || loaded[0].BundleHash() != hash {
		t.Fatal("writer control requires one exact native runtime")
	}
	catalog, err := serveChannelOnboardingCatalog(manager)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := catalog.Resolve(channelonboarding.CandidateSelection{Provider: "telegram"})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := packs.NewOutboundBindingPlanWithRegistration("writer-control", candidate.Plan, "420079", nil,
		map[string]string{candidate.ProviderCredentialRole: "telegram.telegram_bot_token", candidate.SigningCredentialRole: "telegram.webhook_signing_secret"}, candidate.Target.Selector)
	if err != nil {
		t.Fatal(err)
	}
	learned := serveChannelTargetEnablementReader(manager)
	check := func(ctx context.Context) error {
		learnedOK, learnedErr := learned(ctx, candidate)
		declaredOK, declaredErr := serveDeclaredRegistrationEnabled(ctx, manager, loaded[0], binding)
		if !learnedOK || !declaredOK || learnedErr != nil || declaredErr != nil {
			return fmt.Errorf("registration ordinary acquisition learned=%t declared=%t: %w", learnedOK, declaredOK, errors.Join(learnedErr, declaredErr))
		}
		return nil
	}
	listener := reserveChannelOnboardingListener(t)
	readiness := publicingress.NewReadinessOwner(true)
	client := &http.Client{Transport: servePublicIngressRoundTripper(func(req *http.Request) (*http.Response, error) {
		local := req.Clone(req.Context())
		local.URL.Scheme, local.URL.Host = "http", listener.Addr().String()
		return http.DefaultTransport.RoundTrip(local)
	})}
	entered, release := make(chan context.Context, 1), make(chan struct{})
	independent := errors.New("admitted registration writer failed independently")
	var once sync.Once
	resume := func() { once.Do(func() { close(release) }) }
	armed := false // Changed only after the synchronous Start callback returns.
	exposure, err := publicingress.NewController(publicingress.Options{Mode: publicingress.ModeExternalOrigin,
		PublicOrigin: "https://writer-control.example.test", ListenAddress: listener.Addr().String(), Listener: listener,
		Handler: http.NotFoundHandler(), HTTPClient: client, Readiness: readiness,
		OnGeneration: func(ctx context.Context, _ publicingress.Generation) error {
			if armed {
				entered <- ctx
				<-release
				if expires {
					return errors.Join(ctx.Err(), independent)
				}
			}
			return check(ctx)
		}})
	if err != nil {
		t.Fatal(err)
	}
	if err := exposure.Start(t.Context()); err != nil {
		t.Fatalf("initial exact registration check: %v", err)
	}
	armed = true
	t.Cleanup(func() { _ = exposure.Stop(t.Context()) })
	defer resume()
	proof, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	renewed := make(chan error, 1)
	go func() {
		selected, probeErr, registrationErr := exposure.RenewRegistrations(proof)
		if !selected {
			probeErr = errors.Join(probeErr, errors.New("periodic writer was not admitted"))
		}
		renewed <- errors.Join(probeErr, registrationErr)
	}()
	var writer context.Context
	select {
	case writer = <-entered:
	case err := <-renewed:
		t.Fatalf("renewal did not reach the admitted registration callback: %v", err)
	case <-proof.Done():
		t.Fatal("admitted registration callback was not reached")
	}

	// Use the native-backed manager and runtime, as in the managed COMMIT-cut control.
	s := newProcessLifecycleSupervisor(nil, h.runtime)
	s.SetRuntimeContextManager(manager, h.runtime.Options.SourceArtifactFact)
	s.shutdownOptions = runtime.ShutdownOptions{Grace: 5 * time.Second}
	if expires {
		s.shutdownOptions.Grace = time.Nanosecond
	}
	prepared, finished := make(chan struct{}), make(chan error, 1)
	go func() {
		stop := s.beginInboundStop(exposure)
		defer s.inboundStopCancel()
		close(prepared)
		stop.contexts.Drain()
		ctx := s.inboundStopCtx
		finished <- errors.Join(exposure.CloseListener(ctx), manager.CloseInboundRequests(ctx), exposure.Stop(ctx), stop.Finish())
	}()
	select {
	case <-exposure.UpdateAdmissionClosed():
	case <-prepared:
		t.Error("full fence reached before sealing the admitted writer")
	case <-proof.Done():
		t.Error("stop did not reach the writer-admission cut")
	}
	if expires {
		select {
		case <-writer.Done():
		case <-proof.Done():
			t.Error("original stop deadline did not cancel the admitted writer")
		}
	}
	select {
	case <-prepared:
		t.Error("full fence passed the held writer")
	default:
	}
	if readiness.Snapshot(time.Now()).Exposure == nil {
		t.Error("exposure revoked while the registration writer was held")
	}
	if err := check(proof); err != nil {
		t.Errorf("writer dependency fenced before join: %v", err)
	}
	resume()
	select {
	case err := <-renewed:
		if expires && !errors.Is(err, independent) || !expires && err != nil {
			t.Errorf("admitted registration writer returned %v", err)
		}
	case <-proof.Done():
		t.Error("admitted writer was not joined")
	}
	select {
	case err := <-finished:
		adopted = true
		if expires {
			if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, independent) || strings.Count(err.Error(), independent.Error()) != 1 {
				t.Errorf("stop lost or repeated writer expiry/failure: %v", err)
			}
		} else if err != nil {
			t.Errorf("stop failed after registration writer joined: %v", err)
		}
		if readiness.Snapshot(time.Now()).Exposure != nil {
			t.Error("final stop retained public exposure after the writer joined")
		}
	case <-proof.Done():
		t.Error("stop did not join before the proof bound")
	}
}

func TestInboundStopRetainsOriginalDeadlineThroughOuterFinalization(t *testing.T) {
	for _, expired := range []bool{false, true} {
		name, grace := "live", 5*time.Second
		if expired {
			name, grace = "expired", time.Nanosecond
		}
		t.Run(name, func(t *testing.T) {
			s := newProcessLifecycleSupervisor(nil, nil)
			s.runtimeContexts = &runtime.RuntimeContextManager{}
			s.shutdownOptions = runtime.ShutdownOptions{Grace: grace}
			ctx := s.prepareInboundStop()
			defer s.inboundStopCancel()
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Fatal("missing original stop deadline")
			}
			if expired {
				<-ctx.Done()
			}
			stop := s.beginInboundStop(nil)
			if err := s.runtimeContexts.CloseInboundRequests(t.Context()); err != nil {
				t.Fatal(err)
			}
			result := stop.Finish()
			if expired {
				if !errors.Is(result, context.DeadlineExceeded) || ctx.Err() != context.DeadlineExceeded {
					t.Fatalf("shared expiry hidden or replaced: stop=%v ctx=%v", result, ctx.Err())
				}
			} else if result != nil || ctx.Err() != nil {
				t.Fatalf("outer finalization received premature cancellation: stop=%v ctx=%v", result, ctx.Err())
			}
			again := s.prepareInboundStop()
			if next, _ := again.Deadline(); again != ctx || next != deadline {
				t.Fatal("later stop phase renewed or replaced the original deadline")
			}
		})
	}
}

func TestInboundStopJoinsOwnedCurrentRuntimeBeforeRegistration(t *testing.T) {
	definition := serveExecutionContextForTest(t)
	rt := definition.Runtime
	subscription, err := rt.Bus.SubscribeInternal(t.Context(), "partial-startup")
	if err != nil {
		t.Fatal(err)
	}
	completed := make(chan error, 1)
	go func() {
		<-subscription.Retiring()
		completed <- subscription.Complete(false)
	}()
	t.Cleanup(func() {
		if err := rt.Bus.ResetInMemoryState(); err != nil {
			t.Error(err)
		}
		if err := <-completed; err != nil {
			t.Error(err)
		}
	})
	s := newProcessLifecycleSupervisor(nil, rt)
	s.runtimeContexts = &runtime.RuntimeContextManager{}
	s.shutdownOptions = runtime.DefaultShutdownOptions()
	stop := s.beginInboundStop(nil)
	defer s.inboundStopCancel()
	if err := s.runtimeContexts.CloseInboundRequests(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := stop.Finish(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-subscription.Retiring():
	default:
		t.Fatal("owned unregistered runtime escaped the process stop tail")
	}
}

func TestInboundStopPreparationFencesFreshDispatchWithoutCancelingAcceptedBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			_, start := clockDeploymentHarness(t, backend, canonicalrouting.CopyRootInputFeed(t, canonicalrouting.DeploymentFeedSameFlowRoot))
			process, _ := start()
			t.Cleanup(func() {
				if code := process.stop(); code != 0 {
					t.Errorf("joined native stop exit=%d", code)
				}
			})
			rt := servedTestProcessRuntime(t, process)
			accepted, err := rt.WorkOccurrence().Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := accepted.Done(); err != nil {
					t.Error(err)
				}
			}()
			s := newProcessLifecycleSupervisor(nil, rt)
			s.runtimeContexts = &runtime.RuntimeContextManager{}
			s.shutdownOptions = runtime.DefaultShutdownOptions()
			_ = s.prepareInboundStop()
			defer s.inboundStopCancel()
			fresh, err := rt.WorkOccurrence().BeginDispatch(t.Context())
			if fresh != nil {
				if err := fresh.Done(); err != nil {
					t.Error(err)
				}
			}
			if fresh != nil || !errors.Is(err, worklifetime.ErrDispatchClosed) {
				t.Fatalf("preparation admitted fresh dispatch before dependency drain: %v", err)
			}
			if accepted.Context().Err() != nil {
				t.Fatal("fresh-dispatch seal canceled previously accepted work")
			}
			if grant, err := rt.CurrentStartupGrantEvidence(); err != nil || grant.State != startupownership.GrantAdmitted {
				t.Fatalf("preparation withdrew the native generation: %+v err=%v", grant, err)
			}
		})
	}
}
