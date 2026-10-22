package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
)

const (
	runtimeContextTestHashA = "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	runtimeContextTestHashB = "bundle-v2:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	runtimeContextTestHashC = "bundle-v2:sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
)

func TestRuntimeContextManagerRegistersAndLooksUpPinnedContexts(t *testing.T) {
	var contexts [2]BundleContext
	for i, event := range []string{"alpha.requested", "beta.requested"} {
		bundle, rooted, _ := loadS1EffectiveProviderScope(t, map[string][]byte{
			"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
			"schema.yaml":   []byte("stages: []\n"),
			"events.yaml":   []byte(event + ":\n"),
		})
		contexts[i] = rootedContextForSource(t, semanticview.Wrap(bundle), rooted)
		providers, err := packadmission.AdmitProviderSource(rooted, bundle.Platform)
		if err != nil {
			t.Fatal(err)
		}
		scope, err := providers.Scope(".")
		if err != nil {
			t.Fatal(err)
		}
		catalog, err := scope.TriggerCatalog()
		if err != nil {
			t.Fatal(err)
		}
		contexts[i].ProviderTriggerCatalog = catalog
		applyRuntimeAdmissionCatalog(t, &contexts[i], catalog)
	}
	a, b := contexts[0], contexts[1]
	availability := fakeRunBundleAvailability{
		rows: map[string]runbundle.Availability{
			"run-b": {
				RunID:                 "run-b",
				BundleHash:            b.BundleHash(),
				RootReference:         b.RootAdmission.Reference(),
				RootAdmissionPresent:  true,
				SourceArtifactPresent: true,
			},
		},
	}
	manager, err := newTestRuntimeContextManager(t, availability,
		b, a,
	)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	if manager.Len() != 2 || !manager.MultiContext() {
		t.Fatalf("manager Len/MultiContext = %d/%v, want 2/true", manager.Len(), manager.MultiContext())
	}
	wantHashes := []string{a.BundleHash(), b.BundleHash()}
	sort.Strings(wantHashes)
	if got, want := manager.BundleHashes(), wantHashes; !reflect.DeepEqual(got, want) {
		t.Fatalf("BundleHashes = %#v, want %#v", got, want)
	}
	primary, ok := manager.Primary()
	if !ok || primary.BundleHash() != b.BundleHash() {
		t.Fatalf("Primary = %#v/%v, want first registered semantic primary %s", primary, ok, b.BundleHash())
	}
	ctx := testAuthorActivityContextForBundle(context.Background(), b.BundleHash())
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, b.SourceArtifactFact)
	ctx = runtimecorrelation.WithRootAdmission(ctx, b.RootAdmission)
	contextDef, availabilityResult, loaded, err := manager.LookupRun(ctx, "run-b")
	if err != nil {
		t.Fatalf("LookupRun: %v", err)
	}
	if !loaded || contextDef == nil || contextDef.BundleHash() != b.BundleHash() {
		t.Fatalf("LookupRun context = %#v loaded=%v, want %s", contextDef, loaded, b.BundleHash())
	}
	if availabilityResult.BundleHash != b.BundleHash() || !availabilityResult.Available() {
		t.Fatalf("LookupRun availability = %#v, want available %s", availabilityResult, b.BundleHash())
	}
}

func TestRuntimeContextManagerRejectsDuplicateBundleHashes(t *testing.T) {
	contextDef := capturedOrdinaryRuntimeContext(t, "alpha.requested")
	if _, err := newTestRuntimeContextManager(t, nil,
		contextDef, contextDef,
	); err == nil {
		t.Fatal("NewRuntimeContextManager duplicate error = nil")
	}
}

func TestRuntimeContextManagerRejectsDuplicateEffectiveAgentIDs(t *testing.T) {
	var contexts [2]BundleContext
	for i, local := range []string{"alpha", "beta"} {
		bundle, rooted, _ := loadS1EffectiveProviderScope(t, map[string][]byte{
			"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
			"schema.yaml":   []byte("stages: []\n"),
			"events.yaml":   []byte(local + ".requested:\n"),
			"agents.yaml":   []byte(fmt.Sprintf("%s:\n  id: shared-worker\n  role: %s\n  intent: {inline: %s}\n", local, local, local)),
		})
		providers, err := packadmission.AdmitProviderSource(rooted, bundle.Platform)
		if err != nil {
			t.Fatal(err)
		}
		scope, err := providers.Scope(".")
		if err != nil {
			t.Fatal(err)
		}
		catalog, err := scope.TriggerCatalog()
		if err != nil {
			t.Fatal(err)
		}
		contexts[i] = runtimeAdmissionTestContext(t, semanticview.Wrap(bundle), catalog, "")
	}
	a, b := contexts[0], contexts[1]
	if a.BundleHash() == b.BundleHash() {
		t.Fatal("effective-name collision requires distinct captured sources")
	}
	_, err := newTestRuntimeContextManager(t, nil, a, b)
	if err == nil {
		t.Fatal("NewRuntimeContextManager duplicate effective agent_id error = nil")
	}
	for _, want := range []string{
		`duplicate runtime context agent_id "shared-worker"`,
		a.BundleHash(),
		b.BundleHash(),
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("duplicate effective agent_id error missing %q:\n%s", want, err.Error())
		}
	}
}

func TestRuntimeContextManagerRegisterRejectsDuplicateAgentSlugWithoutMutatingManager(t *testing.T) {
	var contexts [2]BundleContext
	for i, event := range []string{"alpha.requested", "beta.requested"} {
		bundle, rooted, _ := loadS1EffectiveProviderScope(t, map[string][]byte{
			"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
			"schema.yaml":   []byte("stages: []\n"),
			"events.yaml":   []byte(event + ":\n"),
			"agents.yaml":   []byte("shared-worker:\n  role: shared-worker\n  intent: {inline: work}\n"),
		})
		providers, err := packadmission.AdmitProviderSource(rooted, bundle.Platform)
		if err != nil {
			t.Fatal(err)
		}
		scope, err := providers.Scope(".")
		if err != nil {
			t.Fatal(err)
		}
		catalog, err := scope.TriggerCatalog()
		if err != nil {
			t.Fatal(err)
		}
		contexts[i] = runtimeAdmissionTestContext(t, semanticview.Wrap(bundle), catalog, "")
	}
	a, b := contexts[0], contexts[1]
	if a.BundleHash() == b.BundleHash() {
		t.Fatal("incremental registration requires distinct captured sources")
	}
	manager, err := newTestRuntimeContextManager(t, nil, a)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	err = manager.Register(b)
	if err == nil {
		t.Fatal("Register duplicate agent slug error = nil")
	}
	for _, want := range []string{
		`duplicate runtime context agent_id "shared-worker"`,
		a.BundleHash(),
		b.BundleHash(),
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Register duplicate agent slug error missing %q:\n%s", want, err.Error())
		}
	}
	if manager.Len() != 1 || manager.MultiContext() {
		t.Fatalf("manager Len/MultiContext after rejected register = %d/%v, want 1/false", manager.Len(), manager.MultiContext())
	}
	if got, want := manager.BundleHashes(), []string{a.BundleHash()}; !reflect.DeepEqual(got, want) {
		t.Fatalf("BundleHashes after rejected register = %#v, want %#v", got, want)
	}
	if _, ok := manager.LookupBundleHash(b.BundleHash()); ok {
		t.Fatal("LookupBundleHash found rejected duplicate context")
	}
}

func TestRuntimeContextManagerAllowsDistinctAgentSlugs(t *testing.T) {
	var contexts [2]BundleContext
	for i, local := range []string{"alpha", "beta"} {
		bundle, rooted, _ := loadS1EffectiveProviderScope(t, map[string][]byte{
			"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
			"schema.yaml":   []byte("stages: []\n"),
			"events.yaml":   []byte(local + ".requested:\n"),
			"agents.yaml":   []byte(fmt.Sprintf("%s-worker:\n  role: %s-worker\n  intent: {inline: %s}\n", local, local, local)),
		})
		providers, err := packadmission.AdmitProviderSource(rooted, bundle.Platform)
		if err != nil {
			t.Fatal(err)
		}
		scope, err := providers.Scope(".")
		if err != nil {
			t.Fatal(err)
		}
		catalog, err := scope.TriggerCatalog()
		if err != nil {
			t.Fatal(err)
		}
		contexts[i] = runtimeAdmissionTestContext(t, semanticview.Wrap(bundle), catalog, "")
	}
	if contexts[0].BundleHash() == contexts[1].BundleHash() {
		t.Fatal("distinct-agent coexistence requires distinct captured sources")
	}
	manager, err := newTestRuntimeContextManager(t, nil, contexts[0], contexts[1])
	if err != nil {
		t.Fatalf("NewRuntimeContextManager distinct agent slugs: %v", err)
	}
	if manager.Len() != 2 || !manager.MultiContext() {
		t.Fatalf("manager Len/MultiContext = %d/%v, want 2/true", manager.Len(), manager.MultiContext())
	}
}

func TestRuntimeContextManagerDeactivatesPinnedContextFailClosed(t *testing.T) {
	contextA := capturedOrdinaryRuntimeContext(t, "alpha.requested")
	contextB := capturedOrdinaryRuntimeContext(t, "beta.requested")
	if contextA.BundleHash() == contextB.BundleHash() {
		t.Fatal("deactivation fixture requires distinct captured sources")
	}
	availability := fakeRunBundleAvailability{
		rows: map[string]runbundle.Availability{
			"run-b": {
				RunID:                 "run-b",
				BundleHash:            contextB.BundleHash(),
				RootReference:         contextB.RootAdmission.Reference(),
				RootAdmissionPresent:  true,
				SourceArtifactPresent: true,
			},
		},
	}
	manager, err := newTestRuntimeContextManager(t, availability, contextA, contextB)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}

	result := manager.DeactivateBundleHash(contextB.BundleHash(), RuntimeContextCauseUnloaded)
	if !result.Found || !result.Changed || result.State != RuntimeContextStateUnloaded || result.Cause != RuntimeContextCauseUnloaded {
		t.Fatalf("DeactivateBundleHash result = %#v, want changed unloaded", result)
	}
	if contextB.Runtime == nil || !contextB.Runtime.shutdownAdmissionClosed() {
		t.Fatal("deactivated runtime shutdown admission is still open")
	}
	if manager.Len() != 2 || !manager.MultiContext() {
		t.Fatalf("manager Len/MultiContext after deactivation = %d/%v, want 2/true", manager.Len(), manager.MultiContext())
	}
	if _, ok := manager.LookupBundleHash(contextB.BundleHash()); ok {
		t.Fatal("LookupBundleHash loaded deactivated bundle = true")
	}
	lookup := manager.LookupBundleHashStatus(contextB.BundleHash())
	if lookup.Loaded() || !lookup.Found || lookup.State != RuntimeContextStateUnloaded || lookup.Cause != RuntimeContextCauseUnloaded {
		t.Fatalf("LookupBundleHashStatus = %#v, want found unloaded", lookup)
	}
	primary, ok := manager.Primary()
	if !ok || primary.BundleHash() != contextA.BundleHash() {
		t.Fatalf("Primary after deactivation = %#v/%v, want %s", primary, ok, contextA.BundleHash())
	}
	lookupCtx := testAuthorActivityContextForBundle(context.Background(), contextB.BundleHash())
	lookupCtx = runtimecorrelation.WithSourceArtifactFact(lookupCtx, contextB.SourceArtifactFact)
	lookupCtx = runtimecorrelation.WithRootAdmission(lookupCtx, contextB.RootAdmission)
	runLookup, availabilityResult, err := manager.LookupRunStatus(lookupCtx, "run-b")
	if err != nil {
		t.Fatalf("LookupRunStatus: %v", err)
	}
	if runLookup.Loaded() || runLookup.Cause != RuntimeContextCauseUnloaded {
		t.Fatalf("LookupRunStatus lookup = %#v, want unloaded", runLookup)
	}
	if availabilityResult.BundleHash != contextB.BundleHash() || !availabilityResult.Available() {
		t.Fatalf("LookupRunStatus availability = %#v, want available %s", availabilityResult, contextB.BundleHash())
	}

	second := manager.DeactivateBundleHash(contextB.BundleHash(), RuntimeContextCauseUnavailable)
	if !second.Found || second.Changed || second.Cause != RuntimeContextCauseUnloaded {
		t.Fatalf("second DeactivateBundleHash result = %#v, want idempotent original cause", second)
	}
}

func TestRuntimeContextManagerDeactivationClosesAdmissionBeforeWithdrawingVisibility(t *testing.T) {
	contextDef := capturedOrdinaryRuntimeContext(t, "alpha.requested")
	release, admitted := contextDef.Runtime.shutdownGate.Begin()
	if !admitted {
		t.Fatal("runtime admission unexpectedly closed")
	}
	manager, err := newTestRuntimeContextManager(t, nil, contextDef)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	done := make(chan RuntimeContextDeactivationResult, 1)
	go func() {
		done <- manager.DeactivateBundleHash(contextDef.BundleHash(), RuntimeContextCauseUnloaded)
	}()
	select {
	case <-done:
		t.Fatal("deactivation withdrew visibility before active admission completed")
	default:
	}
	release()
	result := <-done
	if !result.Changed || !contextDef.Runtime.shutdownAdmissionClosed() {
		t.Fatalf("deactivation result/gate = %#v/%v", result, contextDef.Runtime.shutdownAdmissionClosed())
	}
}

func TestRuntimeContextUseOwnsOneIngressFrameUntilDone(t *testing.T) {
	contextDef := capturedOrdinaryRuntimeContext(t, "alpha.requested")
	manager, err := newTestRuntimeContextManager(t, nil, contextDef)
	if err != nil {
		t.Fatal(err)
	}
	first, _, err := manager.AcquireBundleHash(t.Context(), contextDef.BundleHash())
	if err != nil || first == nil {
		t.Fatalf("first context admission: %v", err)
	}
	t.Cleanup(func() { _ = first.Done() })
	second, _, err := manager.AcquireBundleHash(t.Context(), contextDef.BundleHash())
	if err != nil || second == nil {
		t.Fatalf("second context admission: %v", err)
	}
	t.Cleanup(func() { _ = second.Done() })
	gate := &contextDef.Runtime.shutdownGate
	gate.Close()
	if gate.closedAndDrained() {
		t.Fatal("loaded context uses are absent from ingress drain")
	}
	if err := errors.Join(first.Done(), first.Done()); err != nil {
		t.Fatal(err)
	}
	if gate.closedAndDrained() || contextDef.WorkOwner.ActiveCount() != 1 {
		t.Fatal("duplicate first settlement consumed the second context use")
	}
	if err := second.Done(); err != nil || !gate.closedAndDrained() || contextDef.WorkOwner.ActiveCount() != 0 {
		t.Fatalf("last context use did not settle exact ingress and work ownership: %v", err)
	}
	use, _, err := manager.AcquireBundleHash(t.Context(), contextDef.BundleHash())
	if use != nil || !errors.Is(err, worklifetime.ErrAdmissionFenced) {
		t.Fatalf("closed ingress admitted a new context use: %v", err)
	}
}

func TestRuntimeContextUseOwnerRefusalReleasesIngressFrame(t *testing.T) {
	contextDef := capturedOrdinaryRuntimeContext(t, "alpha.requested")
	manager, err := newTestRuntimeContextManager(t, nil, contextDef)
	if err != nil {
		t.Fatal(err)
	}
	if err := contextDef.WorkOwner.Fence(); err != nil {
		t.Fatal(err)
	}
	use, _, err := manager.AcquireBundleHash(t.Context(), contextDef.BundleHash())
	if use != nil || !errors.Is(err, worklifetime.ErrAdmissionFenced) {
		t.Fatalf("fenced occurrence acquired execution ownership: %v", err)
	}
	contextDef.Runtime.shutdownGate.Close()
	if !contextDef.Runtime.shutdownGate.closedAndDrained() || contextDef.WorkOwner.ActiveCount() != 0 {
		t.Fatal("owner refusal leaked a provisional ingress frame or execution lease")
	}
}

func TestRuntimeContextManagerResolvedRequestCannotAdmitAfterDeactivation(t *testing.T) {
	contextDef := capturedOrdinaryRuntimeContext(t, "alpha.requested")
	manager, err := newTestRuntimeContextManager(t, nil, contextDef)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	resolved := manager.LookupBundleHashStatus(contextDef.BundleHash())
	if !resolved.Loaded() || resolved.Context == nil || resolved.Context.Runtime != nil {
		t.Fatalf("pre-deactivation lookup = %#v, want non-executable loaded metadata", resolved)
	}
	result := manager.DeactivateBundleHash(contextDef.BundleHash(), RuntimeContextCauseUnloaded)
	if !result.Changed {
		t.Fatalf("DeactivateBundleHash = %#v, want changed", result)
	}
	use, lookup, err := manager.AcquireBundleHash(context.Background(), contextDef.BundleHash())
	if err != nil || use != nil || lookup.Loaded() || lookup.Cause != RuntimeContextCauseUnloaded {
		t.Fatalf("post-deactivation acquire = use=%#v lookup=%#v err=%v, want unavailable", use, lookup, err)
	}
}

func TestRuntimeContextManagerDeactivationBoundsStuckAdmissionByConfiguredGrace(t *testing.T) {
	contextDef := capturedOrdinaryRuntimeContext(t, "alpha.requested")
	admissionCtx, release, admitted := contextDef.Runtime.shutdownGate.BeginContext(testAuthorActivityContext(context.Background()))
	if !admitted {
		t.Fatal("runtime admission unexpectedly closed")
	}
	manager, err := newTestRuntimeContextManager(t, nil, contextDef)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	grace := 20 * time.Millisecond
	done := make(chan RuntimeContextDeactivationResult, 1)
	go func() {
		done <- manager.DeactivateBundleHashWithOptions(contextDef.BundleHash(), RuntimeContextCauseUnloaded, ShutdownOptions{Grace: grace})
	}()
	select {
	case <-admissionCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("stuck admitted request context was not canceled at grace expiry")
	}
	select {
	case result := <-done:
		t.Fatalf("deactivation abandoned admitted work after timeout: %#v", result)
	case <-time.After(2 * grace):
	}
	release()
	result := <-done
	if result.ShutdownErr == nil || !strings.Contains(result.ShutdownErr.Error(), "runtime ingress admission drain timed out after 20ms") {
		t.Fatalf("bounded deactivation error = %v", result.ShutdownErr)
	}
	select {
	case <-admissionCtx.Done():
	default:
		t.Fatal("stuck admitted request context was not canceled at grace expiry")
	}
	if _, admitted := contextDef.Runtime.shutdownGate.Begin(); admitted {
		t.Fatal("new admission succeeded after bounded deactivation")
	}
}

func TestRuntimeContextManagerDeactivateAllFencesCompleteSetBeforeFirstDrain(t *testing.T) {
	first := capturedOrdinaryRuntimeContext(t, "alpha.requested")
	second := capturedOrdinaryRuntimeContext(t, "beta.requested")
	if first.BundleHash() == second.BundleHash() {
		t.Fatal("aggregate drain requires distinct captured sources")
	}
	secondEvent := events.EventType("beta.requested")
	// DeactivateAll drains in hash order; hold its actual first source.
	if first.BundleHash() > second.BundleHash() {
		first, second = second, first
		secondEvent = "alpha.requested"
	}
	manager, err := newTestRuntimeContextManager(t, nil, first, second)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := &shutdownBlockedDispatcher{entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
	defer func() {
		select {
		case <-dispatcher.release:
		default:
			close(dispatcher.release)
		}
	}()
	event := eventtest.RunCreatingRootIngress(eventtest.UUID("sibling-shutdown-event"), secondEvent, "", "", []byte(`{}`), 0, agentidentitytest.DefaultRunID, "", events.EventEnvelope{}, time.Now().UTC())
	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("beta"), AgentIdentity: agentidentitytest.RootRuntime(t, "beta", "sibling-shutdown")}
	id, err := runtimedelivery.DeliveryID(event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := second.Runtime.Bus.DeliveryAuthority()
	if err != nil {
		t.Fatal(err)
	}
	page := &shutdownContinuationPage{item: runtimedelivery.ContinuationItem{
		DeliveryID: id, Event: event, Disposition: runtimedelivery.ClaimAcquired,
		Snapshot: runtimedelivery.Snapshot{DeliveryID: id, Route: route, Status: runtimedelivery.StatusPending, Authority: authority},
	}}
	coordinator, err := deliverycontinuation.New(page, startupRecoveryDispositionMap{}, authority, second.WorkOwner, dispatcher, nil)
	if err != nil {
		t.Fatal(err)
	}
	second.Runtime.deliveryContinuations = coordinator
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-dispatcher.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("sibling continuation did not reach held dispatch")
	}
	active, release, admitted := first.Runtime.shutdownGate.BeginContext(testAuthorActivityContext(context.Background()))
	if !admitted {
		t.Fatal("first admission rejected")
	}
	defer release()
	done := make(chan []RuntimeContextDeactivationResult, 1)
	go func() {
		done <- manager.DeactivateAllWithOptions(RuntimeContextCauseUnloaded, ShutdownOptions{Grace: 20 * time.Millisecond})
	}()
	// Cancellation marks the point where shutdown has reached the first drain,
	// but the admitted request deliberately has not settled yet.
	select {
	case <-active.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("first context never reached drain")
	}
	if !second.Runtime.shutdownAdmissionClosed() || manager.LookupBundleHashStatus(second.BundleHash()).Loaded() {
		t.Fatal("later context remains executable while predecessor is draining")
	}
	if !errors.Is(dispatcher.ctx.Err(), context.Canceled) {
		t.Fatal("sibling continuation admission survives whole-set withdrawal until its individual join")
	}
	use, lookup, err := manager.AcquireBundleHash(context.Background(), second.BundleHash())
	if use != nil {
		_ = use.Done()
	}
	if err != nil || use != nil || lookup.Loaded() {
		t.Fatalf("later context admission: use=%v lookup=%+v err=%v", use, lookup, err)
	}
	select {
	case <-done:
		t.Fatal("shutdown abandoned the first admitted request")
	default:
	}
	release()
	close(dispatcher.release)
	results := <-done
	if len(results) != 2 || results[0].ShutdownErr == nil || results[1].ShutdownErr != nil {
		t.Fatalf("unexpected drain results: %+v", results)
	}
}

func TestRuntimeContextManagerRetriesShutdownAfterPriorFailure(t *testing.T) {
	contextDef := capturedOrdinaryRuntimeContext(t, "alpha.requested")
	manager, err := newTestRuntimeContextManager(t, nil, contextDef)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	first := manager.DeactivateBundleHashWithOptions(contextDef.BundleHash(), RuntimeContextCauseUnloaded, ShutdownOptions{Grace: -1})
	if first.ShutdownErr == nil || !first.Changed {
		t.Fatalf("first deactivation = %#v, want changed shutdown failure", first)
	}
	entry := manager.contexts[contextDef.BundleHash()]
	if entry == nil || entry.shutdownComplete {
		t.Fatal("failed shutdown was marked complete")
	}
	second := manager.DeactivateBundleHash(contextDef.BundleHash(), RuntimeContextCauseUnloaded)
	if second.ShutdownErr != nil || second.Changed {
		t.Fatalf("retry deactivation = %#v, want successful retry without visibility transition", second)
	}
	if !entry.shutdownComplete {
		t.Fatal("successful shutdown retry was not marked complete")
	}
}

func TestRuntimeContextManagerRejectedDrainJoinsRetiredStandingChildren(t *testing.T) {
	for _, aggregate := range []bool{false, true} {
		t.Run(fmt.Sprintf("aggregate_%t", aggregate), func(t *testing.T) {
			contextDef := capturedOrdinaryRuntimeContext(t, "alpha.requested")
			manager, err := newTestRuntimeContextManager(t, nil, contextDef)
			if err != nil {
				t.Fatal(err)
			}
			standing, err := contextDef.WorkOwner.NewStanding(t.Context(), worklifetime.StandingIdentity{ServiceID: "service", RunID: "standing-run", Generation: 1})
			if err != nil {
				t.Fatal(err)
			}
			entry := manager.contexts[contextDef.BundleHash()]
			entry.standing = map[string]*worklifetime.StandingOccurrence{"service": standing}
			lease, err := standing.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = lease.Done() })
			done := make(chan RuntimeContextDeactivationResult, 1)
			go func() {
				if aggregate {
					done <- manager.DeactivateAllWithOptions(RuntimeContextCauseUnloaded, ShutdownOptions{Grace: -1})[0]
				} else {
					done <- manager.DeactivateBundleHashWithOptions(contextDef.BundleHash(), RuntimeContextCauseUnloaded, ShutdownOptions{Grace: -1})
				}
			}()
			select {
			case <-lease.Context().Done():
			case <-time.After(time.Second):
				t.Fatal("withdrawal did not retire its standing child")
			}
			if err := lease.Done(); err != nil {
				t.Fatal(err)
			}
			result := <-done
			if result.ShutdownErr == nil || !strings.Contains(result.ShutdownErr.Error(), "shutdown grace") || entry.shutdownComplete {
				t.Fatalf("rejected drain lost its error or marked runtime cleanup complete: %+v", result)
			}
			if len(entry.standing) != 0 || contextDef.WorkOwner.ActiveCount() != 0 {
				t.Fatal("rejected drain returned without acknowledging its retired standing child")
			}
		})
	}
}

func TestRuntimeContextManagerTerminalDrainKeepsRefreshIncompleteAndFencesWholeSet(t *testing.T) {
	first := capturedOrdinaryRuntimeContext(t, "alpha.requested")
	second := capturedOrdinaryRuntimeContext(t, "beta.requested")
	if first.BundleHash() == second.BundleHash() {
		t.Fatal("terminal drain requires distinct captured sources")
	}
	if first.BundleHash() > second.BundleHash() {
		first, second = second, first
	}
	manager, err := newTestRuntimeContextManager(t, nil, first, second)
	if err != nil {
		t.Fatal(err)
	}
	firstCoordinate, err := agenttopology.NewSourceCoordinate(first.RootAdmission)
	if err != nil {
		t.Fatal(err)
	}
	secondCoordinate, err := agenttopology.NewSourceCoordinate(second.RootAdmission)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{firstCoordinate, secondCoordinate}, nil)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := newRuntimeSourceSetTransitionAdmission(plan.Revision)
	if err != nil {
		t.Fatal(err)
	}
	manager.pendingSourceSetTransition = admission
	if result := manager.DeactivateBundleHash(first.BundleHash(), RuntimeContextCauseUnloaded); result.ShutdownErr == nil {
		t.Fatal("individual retirement bypassed aggregate transition ownership")
	}
	active, release, admitted := first.Runtime.shutdownGate.BeginContext(testAuthorActivityContext(context.Background()))
	if !admitted {
		t.Fatal("accepted request was refused")
	}
	defer release()
	// Retained preparation excludes terminal withdrawal just as a live Commit
	// does. Shutdown must not publish its drain signal while this owner exists.
	manager.sourceSetMu.Lock()
	started := make(chan struct{})
	done := make(chan []RuntimeContextDeactivationResult, 1)
	go func() {
		close(started)
		done <- manager.DeactivateAllWithOptions(RuntimeContextCauseUnloaded, ShutdownOptions{Grace: time.Second})
	}()
	<-started
	select {
	case <-active.Done():
		manager.sourceSetMu.Unlock()
		t.Fatal("shutdown crossed preparation exclusion")
	case <-admission.TerminalDrain():
		manager.sourceSetMu.Unlock()
		t.Fatal("terminal drain published before preparation settled")
	case <-time.After(20 * time.Millisecond):
	}
	manager.sourceSetMu.Unlock()
	select {
	case <-admission.TerminalDrain():
	case <-time.After(5 * time.Second):
		t.Fatal("aggregate terminal owner did not release drain waiters")
	}
	for _, definition := range []BundleContext{first, second} {
		if !definition.Runtime.shutdownAdmissionClosed() || manager.LookupBundleHashStatus(definition.SourceArtifactFact.BundleHash()).Loaded() {
			t.Fatal("terminal drain preceded whole-set admission fencing")
		}
	}
	select {
	case <-admission.Done():
		t.Fatal("terminal drain falsely completed the refresh")
	default:
	}
	if retry, err := manager.PreparePendingSourceSetTransition(context.Background(), plan); err == nil || retry != nil {
		t.Fatalf("terminal refresh resumed: retry=%v err=%v", retry, err)
	}
	select {
	case <-done:
		t.Fatal("terminal drain discarded the accepted request")
	default:
	}
	release()
	select {
	case results := <-done:
		if len(results) != 2 || results[0].ShutdownErr != nil || results[1].ShutdownErr != nil {
			t.Fatalf("aggregate settlement results=%+v", results)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("aggregate shutdown did not join accepted settlement")
	}
	select {
	case <-admission.Done():
		t.Fatal("retirement joins falsely completed the refresh")
	default:
	}
}

func capturedOrdinaryRuntimeContext(t *testing.T, eventName string) BundleContext {
	t.Helper()
	bundle, rooted, _ := loadS1EffectiveProviderScope(t, map[string][]byte{
		"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"schema.yaml":   []byte("stages: []\n"),
		"events.yaml":   []byte(eventName + ":\n"),
	})
	providers, err := packadmission.AdmitProviderSource(rooted, bundle.Platform)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := providers.Scope(".")
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := scope.TriggerCatalog()
	if err != nil {
		t.Fatal(err)
	}
	return runtimeAdmissionTestContext(t, semanticview.Wrap(bundle), catalog, "")
}

func testBundleContext(t *testing.T, bundleHash, eventName string) BundleContext {
	t.Helper()
	return testBundleContextWithAgents(t, bundleHash, eventName)
}

func newTestRuntimeContextManager(t testing.TB, availability RunBundleAvailabilityReader, contexts ...BundleContext) (*RuntimeContextManager, error) {
	t.Helper()
	manager, err := NewRuntimeContextManager(availability, contexts...)
	registerRuntimeContextManagerCleanup(t, manager)
	return manager, err
}

func registerRuntimeContextManagerCleanup(t testing.TB, manager *RuntimeContextManager) {
	t.Helper()
	if manager == nil {
		return
	}
	t.Cleanup(func() {
		for _, result := range manager.DeactivateAll(RuntimeContextCauseUnloaded) {
			if result.ShutdownErr != nil {
				t.Errorf("shutdown runtime context %s: %v", result.BundleHash, result.ShutdownErr)
			}
		}
	})
}

func testBundleContextWithAgents(t *testing.T, bundleHash, eventName string, agentIDs ...string) BundleContext {
	t.Helper()
	agents := map[string]runtimecontracts.AgentRegistryEntry{}
	for _, agentID := range agentIDs {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		agents[agentID] = runtimecontracts.AgentRegistryEntry{ID: agentID, Role: agentID}
	}
	return testBundleContextWithAgentEntries(t, bundleHash, eventName, agents)
}

func testBundleContextWithAgentEntries(t *testing.T, bundleHash, eventName string, agents map[string]runtimecontracts.AgentRegistryEntry) BundleContext {
	t.Helper()
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{Name: "review", Version: "1.0.0"},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			eventName: {},
		},
		Agents: agents,
	}
	source := semanticviewtest.WrapRootAgents(bundle)
	workOwner := runtimeTestOccurrence(t, bundleHash)
	bus, err := newRuntimeTestEventBusWithOptions(t, nil, runtimebus.EventBusOptions{
		WorkOwner:          workOwner,
		ContractBundle:     source,
		SourceArtifactFact: testSourceArtifactFact(t, bundleHash),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	return BundleContext{
		SourceArtifactFact: testSourceArtifactFact(t, bundleHash),
		BundleIdentity:     runtimecontracts.BundleIdentity{WorkflowName: "review", WorkflowVersion: "1.0.0"},
		Source:             source,
		WorkOwner:          workOwner,
		Runtime:            &Runtime{Bus: bus, workOccurrence: workOwner},
	}
}

type fakeRunBundleAvailability struct {
	rows map[string]runbundle.Availability
}

func (f fakeRunBundleAvailability) LoadRunBundleAvailability(_ context.Context, runID string) (runbundle.Availability, error) {
	row, ok := f.rows[runID]
	if !ok {
		return runbundle.Availability{}, runbundle.ErrRunNotFound
	}
	if row.ErrorCode == "" && !row.SourceArtifactPresent {
		return row, errors.New("invalid fake row without source artifact")
	}
	return row, nil
}
