package runtime

import (
	"context"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	"github.com/google/uuid"
)

type blockingCredentialSnapshotStore struct {
	runtimecredentials.Store
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingCredentialSnapshotStore) Snapshot(ctx context.Context, key string) (runtimecredentials.AtomicSnapshot, error) {
	s.once.Do(func() { close(s.entered) })
	select {
	case <-ctx.Done():
		return runtimecredentials.AtomicSnapshot{}, ctx.Err()
	case <-s.release:
	}
	return s.Store.(runtimecredentials.Snapshotter).Snapshot(ctx, key)
}

func TestRuntimeContextManagerEvaluatesExactTargetCredentialAtReadTime(t *testing.T) {
	ctx := context.Background()
	catalog := runtimeAdmissionTestCatalog(t, "a")
	contextDef := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", catalog)
	manager, err := newTestRuntimeContextManagerWithAdmission(t, nil, runtimeAdmissionTestState(t, catalog), contextDef)
	if err != nil {
		t.Fatal(err)
	}
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	owner, err := runtimecredentials.NewSnapshotOwner(store)
	if err != nil {
		t.Fatal(err)
	}
	assertEffectiveTargetStatus := func(want packs.SubjectStatus, wantRequirement string) {
		t.Helper()
		subjects, err := manager.EvaluatedCapabilitySubjects(ctx, owner)
		if err != nil {
			t.Fatal(err)
		}
		for _, subject := range subjects {
			if subject.Applicability != "effective" {
				continue
			}
			if subject.Status != want || len(subject.Requirements) != 1 || subject.Requirements[0].Status != wantRequirement {
				t.Fatalf("effective subject = %#v, want %s/%s", subject, want, wantRequirement)
			}
			return
		}
		t.Fatal("effective target subject missing")
	}
	assertEffectiveTargetStatus(packs.StatusNotReady, packs.RequirementStatusUnbound)
	const secretValue = "issue-1944-secret-value-must-not-leak"
	if err := store.Set(ctx, "webhook_signing.acme", secretValue); err != nil {
		t.Fatal(err)
	}
	assertEffectiveTargetStatus(packs.StatusReady, packs.RequirementStatusBound)
	boundSubjects, err := manager.EvaluatedCapabilitySubjects(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	for _, subject := range boundSubjects {
		if strings.Contains(packs.RenderSubject(subject, true), secretValue) {
			t.Fatal("credential value leaked into capability readback")
		}
	}
	if err := store.Delete(ctx, "webhook_signing.acme"); err != nil {
		t.Fatal(err)
	}
	assertEffectiveTargetStatus(packs.StatusNotReady, packs.RequirementStatusUnbound)
}

func TestRuntimeContextManagerRejectsCredentialProjectionStaleAfterSuppression(t *testing.T) {
	ctx := context.Background()
	catalog := runtimeAdmissionTestCatalog(t, "a")
	contextDef := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", catalog)
	manager, err := newTestRuntimeContextManagerWithAdmission(t, nil, runtimeAdmissionTestState(t, catalog), contextDef)
	if err != nil {
		t.Fatal(err)
	}
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Set(ctx, "webhook_signing.acme", "secret"); err != nil {
		t.Fatal(err)
	}
	blocking := &blockingCredentialSnapshotStore{Store: store, entered: make(chan struct{}), release: make(chan struct{})}
	owner, err := runtimecredentials.NewSnapshotOwner(blocking)
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() {
		_, projectionErr := manager.EvaluatedCapabilitySubjects(ctx, owner)
		errCh <- projectionErr
	}()
	<-blocking.entered
	if err := manager.SuppressStandingServiceTargets("service-primary"); err != nil {
		t.Fatal(err)
	}
	close(blocking.release)
	if err := <-errCh; err == nil || !strings.Contains(err.Error(), "became stale") {
		t.Fatalf("stale projection error = %v", err)
	}
}

func TestRuntimeContextManagerInvalidatesCredentialProjectionAcrossOrdinaryUnload(t *testing.T) {
	ctx := context.Background()
	catalog := runtimeAdmissionTestCatalog(t, "a")
	contextDef := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", catalog)
	manager, err := newTestRuntimeContextManagerWithAdmission(t, nil, runtimeAdmissionTestState(t, catalog), contextDef)
	if err != nil {
		t.Fatal(err)
	}
	blocking, owner := runtimeAdmissionBlockingCredentialOwner(t, "secret")
	errCh := make(chan error, 1)
	go func() {
		_, projectionErr := manager.EvaluatedCapabilitySubjects(ctx, owner)
		errCh <- projectionErr
	}()
	<-blocking.entered
	result := manager.DeactivateBundleHash(runtimeContextTestHashA, RuntimeContextCauseUnloaded)
	if !result.Changed || result.ShutdownErr != nil {
		t.Fatalf("deactivation = %#v, want successful visibility withdrawal", result)
	}
	close(blocking.release)
	assertRuntimeAdmissionProjectionStale(t, <-errCh)
	assertRuntimeAdmissionEffectiveSubjectCount(t, manager.BaseCapabilitySubjects(), 0)
	subjects, err := manager.EvaluatedCapabilitySubjects(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	assertRuntimeAdmissionEffectiveSubjectCount(t, subjects, 0)
}

func TestRuntimeContextManagerInvalidatesCredentialProjectionAcrossReplacementWithdrawalAndRestore(t *testing.T) {
	ctx := context.Background()
	catalog := runtimeAdmissionTestCatalog(t, "a")
	predecessor := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", catalog)
	manager, err := newTestRuntimeContextManagerWithAdmission(t, nil, runtimeAdmissionTestState(t, catalog), predecessor)
	if err != nil {
		t.Fatal(err)
	}
	blocking, owner := runtimeAdmissionBlockingCredentialOwner(t, "secret")
	errCh := make(chan error, 1)
	go func() {
		_, projectionErr := manager.EvaluatedCapabilitySubjects(ctx, owner)
		errCh <- projectionErr
	}()
	<-blocking.entered
	candidate := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", catalog)
	if _, err := manager.BeginBundleHashReplacement(ctx, runtimeContextTestHashA, candidate); err != nil {
		t.Fatal(err)
	}
	close(blocking.release)
	assertRuntimeAdmissionProjectionStale(t, <-errCh)
	assertRuntimeAdmissionEffectiveSubjectCount(t, manager.BaseCapabilitySubjects(), 0)

	restored := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", catalog)
	if err := manager.PublishRestoredBundleHashReplacement(runtimeContextTestHashA, restored); err != nil {
		t.Fatal(err)
	}
	assertRuntimeAdmissionEffectiveSubjectCount(t, manager.BaseCapabilitySubjects(), 1)
	subjects, err := manager.EvaluatedCapabilitySubjects(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	assertRuntimeAdmissionEffectiveSubjectCount(t, subjects, 1)
}

func TestRuntimeContextManagerInvalidatesCredentialProjectionAcrossSourceSetFenceAndAbort(t *testing.T) {
	ctx := context.Background()
	catalog := runtimeAdmissionTestCatalog(t, "a")
	contextDef := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", catalog)
	manager, err := newTestRuntimeContextManagerWithAdmission(t, nil, runtimeAdmissionTestState(t, catalog), contextDef)
	if err != nil {
		t.Fatal(err)
	}
	blocking, owner := runtimeAdmissionBlockingCredentialOwner(t, "secret")
	errCh := make(chan error, 1)
	go func() {
		_, projectionErr := manager.EvaluatedCapabilitySubjects(ctx, owner)
		errCh <- projectionErr
	}()
	<-blocking.entered
	manager.mu.Lock()
	entry := manager.contexts[runtimeContextTestHashA]
	err = manager.publishRuntimeContextVisibilityLocked(runtimeContextVisibilityUpdate{
		entry: entry, state: RuntimeContextStateUnloaded, cause: RuntimeContextCauseSourceSetTransition,
	})
	manager.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	close(blocking.release)
	assertRuntimeAdmissionProjectionStale(t, <-errCh)
	assertRuntimeAdmissionEffectiveSubjectCount(t, manager.BaseCapabilitySubjects(), 0)

	transition := &PreparedRuntimeSourceSetTransition{
		manager: manager,
		entries: []runtimeSourceSetTransitionEntry{{bundleHash: runtimeContextTestHashA, entry: entry, fresh: true}},
	}
	if err := transition.abortFresh(); err != nil {
		t.Fatal(err)
	}
	assertRuntimeAdmissionEffectiveSubjectCount(t, manager.BaseCapabilitySubjects(), 1)

	manager.mu.Lock()
	err = manager.publishRuntimeContextVisibilityLocked(runtimeContextVisibilityUpdate{
		entry: entry, state: RuntimeContextStateUnloaded, cause: RuntimeContextCauseSourceSetTransition,
	})
	manager.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	assertRuntimeAdmissionEffectiveSubjectCount(t, manager.BaseCapabilitySubjects(), 0)
	if err := transition.publishCommittedVisibility(); err != nil {
		t.Fatal(err)
	}
	assertRuntimeAdmissionEffectiveSubjectCount(t, manager.BaseCapabilitySubjects(), 1)
	subjects, err := manager.EvaluatedCapabilitySubjects(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	assertRuntimeAdmissionEffectiveSubjectCount(t, subjects, 1)
}

func runtimeAdmissionBlockingCredentialOwner(t *testing.T, value string) (*blockingCredentialSnapshotStore, *runtimecredentials.SnapshotOwner) {
	t.Helper()
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Set(context.Background(), "webhook_signing.acme", value); err != nil {
		t.Fatal(err)
	}
	blocking := &blockingCredentialSnapshotStore{Store: store, entered: make(chan struct{}), release: make(chan struct{})}
	owner, err := runtimecredentials.NewSnapshotOwner(blocking)
	if err != nil {
		t.Fatal(err)
	}
	return blocking, owner
}

func assertRuntimeAdmissionProjectionStale(t *testing.T, err error) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "became stale") {
		t.Fatalf("credential projection error = %v, want stale", err)
	}
}

func assertRuntimeAdmissionEffectiveSubjectCount(t *testing.T, subjects []packs.Subject, want int) {
	t.Helper()
	got := 0
	for _, subject := range subjects {
		if subject.Kind == packs.SubjectProviderTrigger && subject.Applicability == "effective" {
			got++
		}
	}
	if got != want {
		t.Fatalf("effective provider trigger subjects = %d, want %d: %#v", got, want, subjects)
	}
}

func TestValidateRuntimeContextSetWithAdmissionDoesNotActivateStandingOccurrences(t *testing.T) {
	catalog := runtimeAdmissionTestCatalog(t, "a")
	contextDef := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", catalog)
	owner := contextDef.Runtime.WorkOccurrence()
	before := owner.ActiveCount()

	if err := ValidateRuntimeContextSetWithAdmission(runtimeAdmissionTestState(t, catalog), contextDef); err != nil {
		t.Fatalf("ValidateRuntimeContextSetWithAdmission: %v", err)
	}
	if got := owner.ActiveCount(); got != before {
		t.Fatalf("validation activated %d runtime lease(s), want unchanged count %d", got, before)
	}
}

func TestRuntimeContextManagerPublishesOneAdmissionGenerationAcrossAllContexts(t *testing.T) {
	oldCatalog := runtimeAdmissionTestCatalog(t, "a")
	newCatalog := runtimeAdmissionTestCatalog(t, "b")
	oldState := runtimeAdmissionTestState(t, oldCatalog)
	newState := runtimeAdmissionTestState(t, newCatalog)

	primary := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", oldCatalog)
	survivor := runtimeAdmissionTestContext(t, runtimeContextTestHashB, "survivor", oldCatalog)
	manager, err := newTestRuntimeContextManagerWithAdmission(t, nil, oldState, primary, survivor)
	if err != nil {
		t.Fatalf("NewRuntimeContextManagerWithAdmission: %v", err)
	}
	candidate := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", newCatalog)
	survivingTargets := map[string][]StandingTarget{
		runtimeContextTestHashB: runtimeAdmissionTestContext(t, runtimeContextTestHashB, "survivor", newCatalog).StandingTargets,
	}
	if err := manager.ValidateProcessAdmissionReplacement(runtimeContextTestHashA, candidate, survivingTargets, newState); err != nil {
		t.Fatalf("ValidateProcessAdmissionReplacement: %v", err)
	}

	stop := make(chan struct{})
	errCh := make(chan error, 1)
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			subjects := manager.BaseCapabilitySubjects()
			lookup := manager.LookupIngress("survivor", "acme")
			if !lookup.Loaded() {
				select {
				case errCh <- &mixedAdmissionGenerationError{first: "loaded", second: "missing lookup"}:
				default:
				}
				return
			}
			lookupGeneration := lookup.Target.AdmissionPlan.Generation()
			if !lookupGeneration.Equal(oldCatalog.Generation()) && !lookupGeneration.Equal(newCatalog.Generation()) {
				select {
				case errCh <- &mixedAdmissionGenerationError{first: oldCatalog.Generation().Diagnostic() + " or " + newCatalog.Generation().Diagnostic(), second: lookupGeneration.Diagnostic()}:
				default:
				}
				return
			}
			generation := ""
			for _, subject := range subjects {
				if subject.TriggerAdmission == nil {
					continue
				}
				got := subject.TriggerAdmission.CatalogGeneration
				if generation == "" {
					generation = got
				}
				if got != generation {
					select {
					case errCh <- &mixedAdmissionGenerationError{first: generation, second: got}:
					default:
					}
					return
				}
			}
		}
	}()

	if _, err := manager.BeginBundleHashReplacement(context.Background(), runtimeContextTestHashA, candidate); err != nil {
		t.Fatalf("BeginBundleHashReplacement: %v", err)
	}
	if err := manager.PublishBundleHashReplacementWithAdmission(runtimeContextTestHashA, candidate, survivingTargets, newState); err != nil {
		t.Fatalf("PublishBundleHashReplacementWithAdmission: %v", err)
	}
	close(stop)
	readers.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}

	if got := manager.AdmissionState().Generation; !got.Equal(newCatalog.Generation()) {
		t.Fatalf("process generation = %q, want %q", got.Diagnostic(), newCatalog.Generation().Diagnostic())
	}
	for _, alias := range []string{"primary", "survivor"} {
		lookup := manager.LookupIngress(alias, "acme")
		if !lookup.Loaded() || !lookup.Target.AdmissionPlan.Generation().Equal(newCatalog.Generation()) {
			t.Fatalf("lookup %q = %#v, want loaded new generation", alias, lookup)
		}
	}
	subjects := manager.BaseCapabilitySubjects()
	assertRuntimeAdmissionSubjectGeneration(t, subjects, newCatalog.Generation(), 2)
	for _, subject := range subjects {
		if subject.Applicability != "effective" || subject.TriggerAdmission == nil {
			continue
		}
		if subject.TriggerAdmission.Pack == nil || subject.TriggerAdmission.Pack.ManifestHash != "sha256:"+strings.Repeat("b", 64) {
			t.Fatalf("effective subject retained stale pack identity: %#v", subject)
		}
	}
}

func TestRuntimeContextManagerAdmissionReplacementPublishesExactExecutableCandidate(t *testing.T) {
	for _, changedHash := range []bool{false, true} {
		changedHash := changedHash
		name := "same_hash"
		if changedHash {
			name = "changed_hash"
		}
		t.Run(name, func(t *testing.T) {
			oldCatalog := runtimeAdmissionTestCatalog(t, "a")
			newCatalog := runtimeAdmissionTestCatalog(t, "b")
			predecessor := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", oldCatalog)
			survivor := runtimeAdmissionTestContext(t, runtimeContextTestHashB, "survivor", oldCatalog)
			manager, err := newTestRuntimeContextManagerWithAdmission(
				t, nil, runtimeAdmissionTestState(t, oldCatalog), predecessor, survivor,
			)
			if err != nil {
				t.Fatal(err)
			}

			candidateHash := runtimeContextTestHashA
			if changedHash {
				candidateHash = "bundle-v1:sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
			}
			candidate := runtimeAdmissionTestContext(t, candidateHash, "primary", newCatalog)
			updates := map[string][]StandingTarget{
				runtimeContextTestHashB: runtimeAdmissionTestContext(t, runtimeContextTestHashB, "survivor", newCatalog).StandingTargets,
			}
			state := runtimeAdmissionTestState(t, newCatalog)
			if _, err := manager.BeginBundleHashReplacement(context.Background(), runtimeContextTestHashA, candidate); err != nil {
				t.Fatalf("BeginBundleHashReplacement: %v", err)
			}
			prepared, err := manager.PrepareBundleHashReplacementPublicationWithAdmission(
				runtimeContextTestHashA, candidate, updates, state,
			)
			if err != nil {
				t.Fatalf("PrepareBundleHashReplacementPublicationWithAdmission: %v", err)
			}
			if err := prepared.Publish(); err != nil {
				t.Fatalf("Publish: %v", err)
			}

			lookup := manager.LookupBundleHashStatus(candidateHash)
			if !lookup.Loaded() || lookup.Context == nil || lookup.Context.Runtime != nil || lookup.Context.WorkOwner != nil {
				t.Fatalf("published candidate metadata = %#v, want loaded without raw execution authority", lookup)
			}
			use, acquired, err := manager.AcquireBundleHash(context.Background(), candidateHash)
			if err != nil || use == nil || !acquired.Loaded() || use.Runtime() != candidate.Runtime {
				t.Fatalf("published candidate acquisition = use:%#v lookup:%#v err:%v", use, acquired, err)
			}
			if err := use.Done(); err != nil {
				t.Fatalf("settle candidate acquisition: %v", err)
			}
			if changedHash {
				if stale := manager.LookupBundleHashStatus(runtimeContextTestHashA); stale.Found {
					t.Fatalf("changed-hash predecessor still registered: %#v", stale)
				}
			}
			if err := prepared.Withdraw(context.Background()); err != nil {
				t.Fatalf("Withdraw: %v", err)
			}
			predecessorLookup := manager.LookupBundleHashStatus(runtimeContextTestHashA)
			if predecessorLookup.Loaded() || predecessorLookup.Cause != RuntimeContextCauseReplacing {
				t.Fatalf("withdrawn replacement predecessor = %#v, want unavailable replacing", predecessorLookup)
			}
			if changedHash {
				if stale := manager.LookupBundleHashStatus(candidateHash); stale.Found {
					t.Fatalf("withdrawn changed-hash candidate still registered: %#v", stale)
				}
			}
			if got := manager.AdmissionState().Generation; !got.Equal(oldCatalog.Generation()) {
				t.Fatalf("restored process generation = %q, want %q", got.Diagnostic(), oldCatalog.Generation().Diagnostic())
			}
			survivorLookup := manager.LookupIngress("survivor", "acme")
			if !survivorLookup.Loaded() || !survivorLookup.Target.AdmissionPlan.Generation().Equal(oldCatalog.Generation()) {
				t.Fatalf("restored survivor admission = %#v, want old generation", survivorLookup)
			}
			subjects := manager.BaseCapabilitySubjects()
			assertRuntimeAdmissionSubjectGeneration(t, subjects, oldCatalog.Generation(), 1)
			survivorSubject, err := survivor.StandingTargets[0].CapabilitySubject()
			if err != nil {
				t.Fatal(err)
			}
			for _, subject := range subjects {
				if subject.ID == survivorSubject.ID {
					return
				}
			}
			t.Fatalf("restored capability subjects = %#v, want republished survivor %q only", subjects, survivorSubject.ID)
		})
	}
}

func TestRuntimeContextManagerReplacementParksAndRehydratesStandingSchedules(t *testing.T) {
	for _, changedHash := range []bool{false, true} {
		name := "same_hash"
		if changedHash {
			name = "changed_hash"
		}
		t.Run(name, func(t *testing.T) {
			catalog := runtimeAdmissionTestCatalog(t, "a")
			predecessor := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", catalog)
			predecessor.Runtime.Scheduler = runtimeContextTestScheduler(t, predecessor.WorkOwner, nil)
			manager, err := newTestRuntimeContextManagerWithAdmission(t, nil, runtimeAdmissionTestState(t, catalog), predecessor)
			if err != nil {
				t.Fatal(err)
			}
			serviceID := predecessor.StandingTargets[0].ServiceID
			standing := manager.contexts[runtimeContextTestHashA].standing[serviceID]
			managerOwner, err := worklifetime.NewManagerRunOccurrence(
				context.Background(), predecessor.WorkOwner, worklifetime.ManagerRunIdentity{Generation: 1},
			)
			if err != nil {
				t.Fatalf("create replacement Manager owner: %v", err)
			}
			t.Cleanup(func() {
				if managerOwner != nil {
					if err := managerOwner.RetireAndWait(context.Background()); err != nil {
						t.Errorf("retire replacement Manager owner: %v", err)
					}
				}
			})
			managerWork, err := managerOwner.Begin(context.Background(), standing)
			if err != nil {
				t.Fatalf("begin Manager-composed replacement work: %v", err)
			}
			ownerCtx := managerWork.Context()
			replacementSchedules := []runtimegenericschedule.Wakeup{
				runtimeContextTestWakeup(t, "future-once", time.Now().Add(time.Hour)),
				runtimeContextTestWakeup(t, "recurring", time.Now().Add(time.Hour)),
			}
			for _, wakeup := range replacementSchedules {
				if err := predecessor.Runtime.Scheduler.RegisterGenericScheduleWakeup(ownerCtx, wakeup); err != nil {
					t.Fatalf("register %s schedule: %v", wakeup.ActivationID(), err)
				}
			}
			if err := managerWork.Done(); err != nil {
				t.Fatalf("settle Manager-composed replacement work: %v", err)
			}
			route, err := predecessor.WorkOwner.NewRoute(context.Background(), worklifetime.RouteIdentity{
				RuntimeEpoch: 1,
				Agent:        agentidentitytest.RootRuntime(t, "replacement-route", "runtime-context-test"),
				Generation:   1,
			})
			if err != nil {
				t.Fatalf("create replacement route: %v", err)
			}
			routeProducer, err := managerOwner.Begin(context.Background(), standing)
			if err != nil {
				t.Fatalf("begin Manager-composed route producer: %v", err)
			}
			routeEvent := eventtest.RuntimeControl(
				uuid.NewString(), events.EventType("standing.replacement.route"), "replacement-proof", "", []byte(`{}`),
				0, uuid.NewString(), "", events.EventEnvelope{}, time.Now(),
			)
			delivery, err := route.NewEventDelivery(routeProducer.Context(), routeEvent)
			if err != nil {
				t.Fatalf("create Manager-composed replacement delivery: %v", err)
			}
			if err := routeProducer.Done(); err != nil {
				t.Fatalf("settle Manager-composed route producer: %v", err)
			}

			candidateHash := runtimeContextTestHashA
			if changedHash {
				candidateHash = "bundle-v1:sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
			}
			candidate := runtimeAdmissionTestContext(t, candidateHash, "primary", catalog)
			candidateIncumbentStarted := make(chan struct{}, 1)
			releaseCandidateIncumbent := make(chan struct{})
			var candidatePublished atomic.Bool
			var candidateActive atomic.Int32
			var candidateOverlap atomic.Bool
			candidate.Runtime.Scheduler = runtimeContextTestScheduler(t, candidate.WorkOwner, func(context.Context, runtimegenericschedule.Wakeup) {
				if candidateActive.Add(1) > 1 {
					candidateOverlap.Store(true)
				}
				defer candidateActive.Add(-1)
				if candidatePublished.Load() {
					return
				}
				select {
				case candidateIncumbentStarted <- struct{}{}:
				default:
				}
				<-releaseCandidateIncumbent
			})
			replacementDone := make(chan error, 1)
			go func() {
				_, err := manager.BeginBundleHashReplacement(context.Background(), runtimeContextTestHashA, candidate)
				replacementDone <- err
			}()
			for {
				select {
				case err := <-replacementDone:
					t.Fatalf("replacement completed before fencing the Manager-composed standing owner: %v", err)
				default:
				}
				probe, err := standing.Begin(context.Background())
				if err != nil {
					break
				}
				if err := probe.Done(); err != nil {
					t.Fatalf("settle replacement fence probe: %v", err)
				}
				goruntime.Gosched()
			}
			select {
			case err := <-replacementDone:
				t.Fatalf("replacement completed while Manager-composed routed descendant remained live: %v", err)
			default:
			}
			if err := delivery.Complete(); err != nil {
				t.Fatalf("complete Manager-composed replacement delivery: %v", err)
			}
			select {
			case err := <-replacementDone:
				if err != nil {
					t.Fatalf("begin replacement with parked schedules: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("replacement did not complete after Manager-composed routed descendant")
			}
			if err := route.RetireAndWait(context.Background()); err != nil {
				t.Fatalf("retire replacement route: %v", err)
			}
			prepared, err := manager.PrepareBundleHashReplacementPublicationWithAdmission(
				runtimeContextTestHashA, candidate, nil, runtimeAdmissionTestState(t, catalog),
			)
			if err != nil {
				t.Fatalf("prepare replacement publication: %v", err)
			}
			preparedStanding := prepared.publication.standing[serviceID]
			adoptedCandidateTimer, err := runtimegenericschedule.NewWakeup(replacementSchedules[1].ActivationID(), time.Now().Add(time.Millisecond))
			if err != nil {
				t.Fatalf("create adopted candidate wakeup: %v", err)
			}
			if err := candidate.Runtime.Scheduler.RegisterGenericScheduleWakeup(
				worklifetime.WithOccurrence(context.Background(), preparedStanding), adoptedCandidateTimer,
			); err != nil {
				t.Fatalf("register adopted candidate timer: %v", err)
			}
			select {
			case <-candidateIncumbentStarted:
			case <-time.After(time.Second):
				t.Fatal("adopted candidate timer did not start before publication")
			}
			publishDone := make(chan error, 1)
			go func() { publishDone <- prepared.Publish() }()
			select {
			case err := <-publishDone:
				t.Fatalf("replacement published before exact target incumbent settled: %v", err)
			default:
			}
			candidatePublished.Store(true)
			close(releaseCandidateIncumbent)
			if err := <-publishDone; err != nil {
				t.Fatalf("publish replacement: %v", err)
			}
			if candidateOverlap.Load() {
				t.Fatal("replacement timer overlapped adopted candidate incumbent")
			}
			freshStanding := manager.contexts[candidateHash].standing[serviceID]
			if err := managerOwner.RetireAndWait(context.Background()); err != nil {
				t.Fatalf("retire predecessor Manager owner: %v", err)
			}
			managerOwner = nil
			parked, err := candidate.Runtime.Scheduler.ParkOccurrence(context.Background(), freshStanding)
			if err != nil {
				t.Fatalf("inspect rehydrated schedules: %v", err)
			}
			if parked.Count() != 2 {
				t.Fatalf("rehydrated schedules = %#v, want future one-shot and recurring cron", parked)
			}
		})
	}
}

func TestRuntimeContextReplacementAggregateFailureLeavesNoPartialCandidateAndRetries(t *testing.T) {
	catalog := runtimeAdmissionTestCatalog(t, "a")
	state := runtimeAdmissionTestState(t, catalog)
	predecessor := testBundleContext(t, runtimeContextTestHashA, "standing.aggregate")
	predecessor.Runtime.Scheduler = runtimeContextTestScheduler(t, predecessor.WorkOwner, nil)
	predecessor.StandingTargets = aggregateReplacementStandingTargets(t, runtimeContextTestHashA, catalog)
	manager, err := newTestRuntimeContextManagerWithAdmission(t, nil, state, predecessor)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range predecessor.StandingTargets {
		owner := manager.contexts[runtimeContextTestHashA].standing[target.ServiceID]
		if err := predecessor.Runtime.Scheduler.RegisterGenericScheduleWakeup(
			worklifetime.WithOccurrence(context.Background(), owner),
			runtimeContextTestWakeup(t, target.ServiceID, time.Now().Add(time.Hour)),
		); err != nil {
			t.Fatalf("register predecessor schedule for %s: %v", target.ServiceID, err)
		}
	}

	failedCandidate := testBundleContext(t, runtimeContextTestHashA, "standing.aggregate")
	failedCandidate.Runtime.Scheduler = runtimeContextTestScheduler(t, failedCandidate.WorkOwner, nil)
	failedCandidate.StandingTargets = aggregateReplacementStandingTargets(t, runtimeContextTestHashA, catalog)
	if _, err := manager.BeginBundleHashReplacement(context.Background(), runtimeContextTestHashA, failedCandidate); err != nil {
		t.Fatalf("begin aggregate replacement: %v", err)
	}
	failedPublication, err := manager.PrepareBundleHashReplacementPublicationWithAdmission(runtimeContextTestHashA, failedCandidate, nil, state)
	if err != nil {
		t.Fatalf("prepare failed candidate publication: %v", err)
	}
	if err := failedPublication.publication.standing["service-b"].RetireAndWait(context.Background()); err != nil {
		t.Fatalf("retire middle successor owner: %v", err)
	}
	if err := failedPublication.Publish(); err == nil {
		t.Fatal("aggregate publication succeeded with a retired middle successor owner")
	}
	lookup := manager.LookupBundleHashStatus(runtimeContextTestHashA)
	if lookup.Loaded() || lookup.Cause != RuntimeContextCauseReplacing {
		t.Fatalf("failed aggregate publication lookup = %#v, want unavailable replacement", lookup)
	}
	assertRuntimeAdmissionEffectiveSubjectCount(t, manager.BaseCapabilitySubjects(), 0)
	failedCandidate.Runtime.Scheduler.Stop()
	if err := failedCandidate.Runtime.Scheduler.Wait(context.Background()); err != nil {
		t.Fatalf("wait failed candidate scheduler: %v", err)
	}
	if err := failedPublication.Discard(); err != nil {
		t.Fatalf("discard failed candidate publication: %v", err)
	}

	retryCandidate := testBundleContext(t, runtimeContextTestHashA, "standing.aggregate")
	retryCandidate.Runtime.Scheduler = runtimeContextTestScheduler(t, retryCandidate.WorkOwner, nil)
	retryCandidate.StandingTargets = aggregateReplacementStandingTargets(t, runtimeContextTestHashA, catalog)
	retryPublication, err := manager.PrepareBundleHashReplacementPublicationWithAdmission(runtimeContextTestHashA, retryCandidate, nil, state)
	if err != nil {
		t.Fatalf("prepare fresh-candidate retry: %v", err)
	}
	if err := retryPublication.Publish(); err != nil {
		t.Fatalf("publish fresh-candidate retry: %v", err)
	}
	assertRuntimeAdmissionEffectiveSubjectCount(t, manager.BaseCapabilitySubjects(), len(retryCandidate.StandingTargets))
	for _, target := range retryCandidate.StandingTargets {
		owner := retryPublication.publication.standing[target.ServiceID]
		parked, err := retryCandidate.Runtime.Scheduler.ParkOccurrence(context.Background(), owner)
		if err != nil {
			t.Fatalf("inspect retry schedules for %s: %v", target.ServiceID, err)
		}
		if parked.Count() != 1 {
			t.Fatalf("retry schedules for %s = %d, want 1", target.ServiceID, parked.Count())
		}
	}
}

func aggregateReplacementStandingTargets(t *testing.T, bundleHash string, catalog *providertriggers.CatalogSnapshot) []StandingTarget {
	t.Helper()
	targets := make([]StandingTarget, 0, 3)
	for _, suffix := range []string{"a", "b", "c"} {
		plan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{
			Alias: "aggregate-" + suffix, Provider: "acme", SigningSecret: "webhook_signing.acme",
		})
		if err != nil {
			t.Fatalf("compile aggregate admission %s: %v", suffix, err)
		}
		targets = append(targets, StandingTarget{
			BundleHash: bundleHash, ServiceID: "service-" + suffix, FlowID: "flow-" + suffix,
			Alias: "aggregate-" + suffix, Provider: "acme", RunID: "run-" + suffix, Generation: 1,
			FlowInstance: "flow-" + suffix + "/instance", EntityID: "entity-" + suffix,
			SigningSecret: "webhook_signing.acme", AdmissionPlan: plan,
		})
	}
	return targets
}

func TestStandingServiceTransitionRollbackRestoresExactOwnerSchedulesBeforeAdmission(t *testing.T) {
	catalog := runtimeAdmissionTestCatalog(t, "a")
	contextDef := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", catalog)
	contextDef.Runtime.Scheduler = runtimeContextTestScheduler(t, contextDef.WorkOwner, nil)
	manager, err := newTestRuntimeContextManagerWithAdmission(t, nil, runtimeAdmissionTestState(t, catalog), contextDef)
	if err != nil {
		t.Fatal(err)
	}
	serviceID := contextDef.StandingTargets[0].ServiceID
	standing := manager.contexts[runtimeContextTestHashA].standing[serviceID]
	managerOwner, err := worklifetime.NewManagerRunOccurrence(
		context.Background(), contextDef.WorkOwner, worklifetime.ManagerRunIdentity{Generation: 1},
	)
	if err != nil {
		t.Fatalf("create rollback Manager owner: %v", err)
	}
	managerWork, err := managerOwner.Begin(context.Background(), standing)
	if err != nil {
		t.Fatalf("begin rollback Manager work: %v", err)
	}
	ownerCtx := managerWork.Context()
	for _, wakeup := range []runtimegenericschedule.Wakeup{
		runtimeContextTestWakeup(t, "future-once", time.Now().Add(time.Hour)),
		runtimeContextTestWakeup(t, "recurring", time.Now().Add(time.Hour)),
	} {
		if err := contextDef.Runtime.Scheduler.RegisterGenericScheduleWakeup(ownerCtx, wakeup); err != nil {
			t.Fatalf("register %s schedule: %v", wakeup.ActivationID(), err)
		}
	}
	if err := managerWork.Done(); err != nil {
		t.Fatalf("settle rollback Manager work: %v", err)
	}

	transition, err := manager.BeginStandingServiceTransition(context.Background(), serviceID)
	if err != nil {
		t.Fatalf("begin standing transition: %v", err)
	}
	if err := transition.Wait(context.Background()); err != nil {
		t.Fatalf("drain standing transition: %v", err)
	}
	if err := transition.Restore(context.Background()); err != nil {
		t.Fatalf("restore standing transition: %v", err)
	}
	lease, err := standing.Begin(context.Background())
	if err != nil {
		t.Fatalf("standing admission after rollback: %v", err)
	}
	lease.Done()
	if err := managerOwner.RetireAndWait(context.Background()); err != nil {
		t.Fatalf("retire rollback Manager owner: %v", err)
	}
	waitCtx, cancelWait := context.WithTimeout(context.Background(), time.Second)
	defer cancelWait()
	if err := contextDef.Runtime.Scheduler.Wait(waitCtx); err != nil {
		t.Fatalf("Manager retirement did not join rollback-restored schedules: %v", err)
	}
	parked, err := contextDef.Runtime.Scheduler.ParkOccurrence(context.Background(), standing)
	if err != nil {
		t.Fatalf("inspect schedules after Manager retirement: %v", err)
	}
	if parked.Count() != 0 {
		t.Fatalf("rollback weakened Manager-composed schedules to standing-only ownership: %#v", parked)
	}
}

func TestStandingServiceTransitionRollbackFailsClosedWhenOriginalManagerRetires(t *testing.T) {
	catalog := runtimeAdmissionTestCatalog(t, "a")
	contextDef := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", catalog)
	contextDef.Runtime.Scheduler = runtimeContextTestScheduler(t, contextDef.WorkOwner, nil)
	manager, err := newTestRuntimeContextManagerWithAdmission(t, nil, runtimeAdmissionTestState(t, catalog), contextDef)
	if err != nil {
		t.Fatal(err)
	}
	serviceID := contextDef.StandingTargets[0].ServiceID
	standing := manager.contexts[runtimeContextTestHashA].standing[serviceID]
	managerOwner, err := worklifetime.NewManagerRunOccurrence(
		context.Background(), contextDef.WorkOwner, worklifetime.ManagerRunIdentity{Generation: 1},
	)
	if err != nil {
		t.Fatalf("create rollback Manager owner: %v", err)
	}
	managerWork, err := managerOwner.Begin(context.Background(), standing)
	if err != nil {
		t.Fatalf("begin rollback Manager work: %v", err)
	}
	if err := contextDef.Runtime.Scheduler.RegisterGenericScheduleWakeup(managerWork.Context(), runtimeContextTestWakeup(t, "retired-manager", time.Now().Add(time.Hour))); err != nil {
		t.Fatalf("register Manager-composed schedule: %v", err)
	}
	if err := managerWork.Done(); err != nil {
		t.Fatalf("settle rollback Manager work: %v", err)
	}

	transition, err := manager.BeginStandingServiceTransition(context.Background(), serviceID)
	if err != nil {
		t.Fatalf("begin standing transition: %v", err)
	}
	if err := transition.Wait(context.Background()); err != nil {
		t.Fatalf("drain standing transition: %v", err)
	}
	if err := managerOwner.RetireAndWait(context.Background()); err != nil {
		t.Fatalf("retire original Manager owner: %v", err)
	}
	if err := transition.Restore(context.Background()); err == nil {
		t.Fatal("rollback succeeded after the exact original Manager owner retired")
	}
	if lease, err := standing.Begin(context.Background()); err == nil {
		_ = lease.Done()
		t.Fatal("failed rollback reopened standing admission")
	}
	if !manager.standingServiceSuppressedLocked(serviceID) {
		t.Fatal("failed rollback republished standing ingress")
	}
	if err := standing.RetireAndWait(context.Background()); err != nil {
		t.Fatalf("retire failed-rollback standing occurrence: %v", err)
	}
}

func TestPreparedStandingSuccessorOwnsSchedulesBeforePublication(t *testing.T) {
	catalog := runtimeAdmissionTestCatalog(t, "a")
	contextDef := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", catalog)
	contextDef.Runtime.Scheduler = runtimeContextTestScheduler(t, contextDef.WorkOwner, nil)
	manager, err := newTestRuntimeContextManagerWithAdmission(t, nil, runtimeAdmissionTestState(t, catalog), contextDef)
	if err != nil {
		t.Fatal(err)
	}
	serviceID := contextDef.StandingTargets[0].ServiceID
	predecessor := manager.contexts[runtimeContextTestHashA].standing[serviceID]
	transition, err := manager.BeginStandingServiceTransition(context.Background(), serviceID)
	if err != nil {
		t.Fatalf("begin standing reset transition: %v", err)
	}
	if err := transition.Wait(context.Background()); err != nil {
		t.Fatalf("drain standing reset transition: %v", err)
	}
	if err := transition.Retire(context.Background()); err != nil {
		t.Fatalf("retire standing reset predecessor: %v", err)
	}

	const successorRunID = "run-successor"
	successorGeneration := contextDef.StandingTargets[0].Generation + 1
	prepared, err := manager.PrepareStandingServicePublication(serviceID, successorRunID, successorGeneration)
	if err != nil {
		t.Fatalf("prepare standing successor: %v", err)
	}
	for _, wakeup := range []runtimegenericschedule.Wakeup{
		runtimeContextTestWakeup(t, "successor-once", time.Now().Add(time.Hour)),
		runtimeContextTestWakeup(t, "successor-recurring", time.Now().Add(time.Hour)),
	} {
		if err := contextDef.Runtime.Scheduler.RegisterGenericScheduleWakeup(prepared.WorkContext(context.Background()), wakeup); err != nil {
			t.Fatalf("register prepared %s schedule: %v", wakeup.ActivationID(), err)
		}
	}
	successorTargets := append([]StandingTarget(nil), contextDef.StandingTargets...)
	for i := range successorTargets {
		successorTargets[i].RunID = successorRunID
		successorTargets[i].Generation = successorGeneration
	}
	if err := prepared.Publish(successorTargets); err != nil {
		t.Fatalf("publish standing successor: %v", err)
	}
	successor := manager.contexts[runtimeContextTestHashA].standing[serviceID]
	if successor == nil || successor == predecessor {
		t.Fatal("prepared standing publication did not install a fresh occurrence")
	}
	parked, err := contextDef.Runtime.Scheduler.ParkOccurrence(context.Background(), successor)
	if err != nil {
		t.Fatalf("inspect prepared successor schedules: %v", err)
	}
	if parked.Count() != 2 {
		t.Fatalf("prepared successor schedules = %#v, want future one-shot and recurring cron", parked)
	}
}

func runtimeContextTestScheduler(t *testing.T, owner worklifetime.Occurrence, callback func(context.Context, runtimegenericschedule.Wakeup)) *runtimepipeline.Scheduler {
	t.Helper()
	scheduler := runtimepipeline.NewSchedulerWithWorkOwner(owner)
	if callback == nil {
		callback = func(context.Context, runtimegenericschedule.Wakeup) {}
	}
	if err := scheduler.BindGenericScheduleLifecycle(callback); err != nil {
		t.Fatalf("bind generic schedule lifecycle: %v", err)
	}
	return scheduler
}

func runtimeContextTestWakeup(t *testing.T, key string, dueAt time.Time) runtimegenericschedule.Wakeup {
	t.Helper()
	activationID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("runtime-context:"+key)).String()
	wakeup, err := runtimegenericschedule.NewWakeup(activationID, dueAt)
	if err != nil {
		t.Fatalf("create generic schedule wakeup: %v", err)
	}
	return wakeup
}

func TestRuntimeContextManagerBlockedStandingDescendantLeavesReplacementUnavailable(t *testing.T) {
	catalog := runtimeAdmissionTestCatalog(t, "a")
	predecessor := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", catalog)
	manager, err := newTestRuntimeContextManagerWithAdmission(
		t, nil, runtimeAdmissionTestState(t, catalog), predecessor,
	)
	if err != nil {
		t.Fatal(err)
	}
	standing := manager.contexts[runtimeContextTestHashA].standing["service-primary"]
	descendant, err := standing.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin standing descendant: %v", err)
	}
	settled := false
	defer func() {
		if !settled {
			_ = descendant.Done()
		}
	}()

	candidate := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", catalog)
	timedOut, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := manager.BeginBundleHashReplacement(timedOut, runtimeContextTestHashA, candidate); err == nil || !strings.Contains(err.Error(), "drain predecessor standing occurrence") {
		t.Fatalf("blocked replacement error = %v", err)
	}
	lookup := manager.LookupBundleHashStatus(runtimeContextTestHashA)
	if lookup.Loaded() || lookup.Cause != RuntimeContextCauseReplacing || lookup.Context != nil {
		t.Fatalf("blocked replacement lookup = %#v, want unavailable replacing", lookup)
	}
	if use, acquired, err := manager.AcquireBundleHash(context.Background(), runtimeContextTestHashA); use != nil || acquired.Loaded() {
		t.Fatalf("blocked replacement acquisition = use:%#v lookup:%#v err:%v", use, acquired, err)
	}
	if err := descendant.Done(); err != nil {
		t.Fatalf("settle standing descendant: %v", err)
	}
	settled = true
	if err := standing.RetireAndWait(context.Background()); err != nil {
		t.Fatalf("retire timed-out predecessor standing occurrence: %v", err)
	}
}

func TestRuntimeContextManagerRejectsIncompleteAdmissionGenerationWithoutMutation(t *testing.T) {
	oldCatalog := runtimeAdmissionTestCatalog(t, "a")
	newCatalog := runtimeAdmissionTestCatalog(t, "b")
	primary := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", oldCatalog)
	survivor := runtimeAdmissionTestContext(t, runtimeContextTestHashB, "survivor", oldCatalog)
	manager, err := newTestRuntimeContextManagerWithAdmission(t, nil, runtimeAdmissionTestState(t, oldCatalog), primary, survivor)
	if err != nil {
		t.Fatal(err)
	}
	candidate := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", newCatalog)
	err = manager.ValidateProcessAdmissionReplacement(runtimeContextTestHashA, candidate, nil, runtimeAdmissionTestState(t, newCatalog))
	if err == nil || !strings.Contains(err.Error(), "did not recompile loaded runtime context") {
		t.Fatalf("validation error = %v", err)
	}
	if got := manager.AdmissionState().Generation; !got.Equal(oldCatalog.Generation()) {
		t.Fatalf("failed candidate changed generation to %q", got.Diagnostic())
	}
	for _, alias := range []string{"primary", "survivor"} {
		lookup := manager.LookupIngress(alias, "acme")
		if !lookup.Loaded() || !lookup.Target.AdmissionPlan.Generation().Equal(oldCatalog.Generation()) {
			t.Fatalf("failed candidate changed lookup %q: %#v", alias, lookup)
		}
	}
	assertRuntimeAdmissionSubjectGeneration(t, manager.BaseCapabilitySubjects(), oldCatalog.Generation(), 2)
}

func TestRuntimeContextManagerAdmissionGenerationDoesNotDependOnPrimaryPackUse(t *testing.T) {
	oldCatalog := runtimeAdmissionTestCatalog(t, "a")
	newCatalog := runtimeAdmissionTestCatalog(t, "b")
	primary := testBundleContext(t, runtimeContextTestHashA, "primary.event")
	survivor := runtimeAdmissionTestContext(t, runtimeContextTestHashB, "survivor", oldCatalog)
	manager, err := newTestRuntimeContextManagerWithAdmission(t, nil, runtimeAdmissionTestState(t, oldCatalog), primary, survivor)
	if err != nil {
		t.Fatal(err)
	}
	candidatePrimary := testBundleContext(t, runtimeContextTestHashA, "primary.event")
	newSurvivor := runtimeAdmissionTestContext(t, runtimeContextTestHashB, "survivor", newCatalog)
	updates := map[string][]StandingTarget{runtimeContextTestHashB: newSurvivor.StandingTargets}
	state := runtimeAdmissionTestState(t, newCatalog)
	if err := manager.ValidateProcessAdmissionReplacement(runtimeContextTestHashA, candidatePrimary, updates, state); err != nil {
		t.Fatalf("primary-without-pack validation: %v", err)
	}
	if _, err := manager.BeginBundleHashReplacement(context.Background(), runtimeContextTestHashA, candidatePrimary); err != nil {
		t.Fatal(err)
	}
	if err := manager.PublishBundleHashReplacementWithAdmission(runtimeContextTestHashA, candidatePrimary, updates, state); err != nil {
		t.Fatal(err)
	}
	if got := manager.LookupIngress("survivor", "acme"); !got.Loaded() || !got.Target.AdmissionPlan.Generation().Equal(newCatalog.Generation()) {
		t.Fatalf("surviving pack target = %#v", got)
	}
	if primary, ok := manager.LookupBundleHash(runtimeContextTestHashA); !ok || len(primary.StandingTargets) != 0 {
		t.Fatalf("primary context unexpectedly acquired pack target: %#v/%t", primary, ok)
	}
}

func TestRuntimeContextManagerRejectsCandidatePackRemovalAcrossContexts(t *testing.T) {
	source, oldCatalog := standingTelegramDeclarationSource(t, "inbound.telegram")
	emptyCatalog, err := providertriggers.NewCatalogSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	primary := testBundleContext(t, runtimeContextTestHashA, "primary.event")
	survivor := testBundleContext(t, runtimeContextTestHashB, "inbound.telegram")
	survivor.Source = source
	plan, err := oldCatalog.CompileAdmission(providertriggers.CompileAdmissionRequest{
		Alias: "chat", Provider: "telegram", SigningSecret: "webhook_signing.telegram",
	})
	if err != nil {
		t.Fatal(err)
	}
	survivor.StandingTargets = []StandingTarget{{
		BundleHash: runtimeContextTestHashB, ServiceID: "service-chat", FlowID: "coordinator", Alias: "chat", Provider: "telegram",
		RunID: "run-chat", Generation: 1, FlowInstance: "coordinator/chat", EntityID: "entity-chat",
		SigningSecret: "webhook_signing.telegram", AdmissionPlan: plan,
	}}
	manager, err := newTestRuntimeContextManagerWithAdmission(t, nil, runtimeAdmissionTestState(t, oldCatalog), primary, survivor)
	if err != nil {
		t.Fatal(err)
	}
	candidatePrimary := testBundleContext(t, runtimeContextTestHashA, "primary.event")
	if _, err = RecompileStandingTargetAdmissions(survivor.Source, emptyCatalog, survivor.StandingTargets); err == nil || !strings.Contains(err.Error(), `provider "telegram" is pack-required`) {
		t.Fatalf("actual pack removal recompile error = %v", err)
	}
	if got := manager.AdmissionState().Generation; !got.Equal(oldCatalog.Generation()) {
		t.Fatalf("pack removal changed process generation to %q", got.Diagnostic())
	}
	if got := manager.LookupIngress("chat", "telegram"); !got.Loaded() || !got.Target.AdmissionPlan.Generation().Equal(oldCatalog.Generation()) {
		t.Fatalf("pack removal changed survivor: %#v", got)
	}
	if _, ok := manager.LookupBundleHash(candidatePrimary.BundleHash()); !ok {
		t.Fatal("pack removal failure withdrew unchanged primary context")
	}
}

func TestRuntimeContextManagerRejectsTwoContextIngressCollisionWithoutMutation(t *testing.T) {
	oldCatalog := runtimeAdmissionTestCatalog(t, "a")
	newCatalog := runtimeAdmissionTestCatalog(t, "b")
	primary := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", oldCatalog)
	survivor := runtimeAdmissionTestContext(t, runtimeContextTestHashB, "survivor", oldCatalog)
	manager, err := newTestRuntimeContextManagerWithAdmission(t, nil, runtimeAdmissionTestState(t, oldCatalog), primary, survivor)
	if err != nil {
		t.Fatal(err)
	}
	candidate := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "survivor", newCatalog)
	updates := map[string][]StandingTarget{
		runtimeContextTestHashB: runtimeAdmissionTestContext(t, runtimeContextTestHashB, "survivor", newCatalog).StandingTargets,
	}
	err = manager.ValidateProcessAdmissionReplacement(runtimeContextTestHashA, candidate, updates, runtimeAdmissionTestState(t, newCatalog))
	if err == nil || !strings.Contains(err.Error(), `duplicate standing ingress alias "survivor"`) {
		t.Fatalf("collision validation error = %v", err)
	}
	if got := manager.AdmissionState().Generation; !got.Equal(oldCatalog.Generation()) {
		t.Fatalf("collision changed generation to %q", got.Diagnostic())
	}
	for alias, hash := range map[string]string{"primary": runtimeContextTestHashA, "survivor": runtimeContextTestHashB} {
		lookup := manager.LookupIngress(alias, "acme")
		if !lookup.Loaded() || lookup.Context.BundleHash() != hash || !lookup.Target.AdmissionPlan.Generation().Equal(oldCatalog.Generation()) {
			t.Fatalf("collision changed %s lookup: %#v", alias, lookup)
		}
	}
}

func TestRuntimeContextManagerSignedToUnsignedTransitionRequiresAcknowledgedRecompileAcrossContexts(t *testing.T) {
	signed := runtimeAdmissionTestCatalog(t, "a")
	unsigned := runtimeAdmissionUnsignedTestCatalog(t, "b")
	primary := testBundleContext(t, runtimeContextTestHashA, "primary.event")
	survivor := runtimeAdmissionTestContext(t, runtimeContextTestHashB, "survivor", signed)
	manager, err := newTestRuntimeContextManagerWithAdmission(t, nil, runtimeAdmissionTestState(t, signed), primary, survivor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unsigned.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: "survivor", Provider: "acme"}); err == nil || !strings.Contains(err.Error(), "admission.acknowledge: unsigned_webhook") {
		t.Fatalf("unacknowledged transition compile error = %v", err)
	}
	if got := manager.LookupIngress("survivor", "acme"); !got.Loaded() || got.Target.AdmissionPlan.RequestAuthentication() != providertriggers.RequestAuthenticationTokenEquality {
		t.Fatalf("failed transition changed predecessor: %#v", got)
	}

	unsignedPlan, err := unsigned.CompileAdmission(providertriggers.CompileAdmissionRequest{
		Alias: "survivor", Provider: "acme",
		Declaration: providertriggers.AdmissionDeclaration{Acknowledge: providertriggers.UnsignedWebhookAcknowledgement},
	})
	if err != nil {
		t.Fatal(err)
	}
	newSurvivor := survivor
	newSurvivor.StandingTargets = append([]StandingTarget(nil), survivor.StandingTargets...)
	newSurvivor.StandingTargets[0].SigningSecret = ""
	newSurvivor.StandingTargets[0].AdmissionPlan = unsignedPlan
	candidatePrimary := testBundleContext(t, runtimeContextTestHashA, "primary.event")
	updates := map[string][]StandingTarget{runtimeContextTestHashB: newSurvivor.StandingTargets}
	state := runtimeAdmissionTestState(t, unsigned)
	if err := manager.ValidateProcessAdmissionReplacement(runtimeContextTestHashA, candidatePrimary, updates, state); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.BeginBundleHashReplacement(context.Background(), runtimeContextTestHashA, candidatePrimary); err != nil {
		t.Fatal(err)
	}
	if err := manager.PublishBundleHashReplacementWithAdmission(runtimeContextTestHashA, candidatePrimary, updates, state); err != nil {
		t.Fatal(err)
	}
	lookup := manager.LookupIngress("survivor", "acme")
	if !lookup.Loaded() || lookup.Target.AdmissionPlan.RequestAuthentication() != providertriggers.RequestAuthenticationNone || !lookup.Target.AdmissionPlan.Generation().Equal(unsigned.Generation()) {
		t.Fatalf("acknowledged transition lookup = %#v", lookup)
	}
	for _, subject := range manager.BaseCapabilitySubjects() {
		if subject.TriggerAdmission != nil && subject.Applicability == "effective" && subject.TriggerAdmission.RequestAuthentication != "UNAUTHENTICATED" {
			t.Fatalf("transition readback retained stale authentication: %#v", subject)
		}
	}
}

func TestRuntimeContextManagerRejectsAdmissionTargetsOnLegacyPublishAndRestoresPredecessor(t *testing.T) {
	catalog := runtimeAdmissionTestCatalog(t, "a")
	predecessor := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", catalog)
	manager, err := newTestRuntimeContextManagerWithAdmission(t, nil, runtimeAdmissionTestState(t, catalog), predecessor)
	if err != nil {
		t.Fatal(err)
	}
	restored := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", catalog)
	if _, err := manager.BeginBundleHashReplacement(context.Background(), runtimeContextTestHashA, restored); err != nil {
		t.Fatal(err)
	}
	err = manager.PublishBundleHashReplacement(runtimeContextTestHashA, restored)
	if err == nil || !strings.Contains(err.Error(), "PublishBundleHashReplacementWithAdmission") {
		t.Fatalf("legacy publish error = %v", err)
	}
	if err := manager.PublishRestoredBundleHashReplacement(runtimeContextTestHashA, restored); err != nil {
		t.Fatalf("PublishRestoredBundleHashReplacement: %v", err)
	}
	lookup := manager.LookupIngress("primary", "acme")
	if !lookup.Loaded() || !lookup.Target.AdmissionPlan.Generation().Equal(catalog.Generation()) {
		t.Fatalf("restored lookup = %#v", lookup)
	}
	assertRuntimeAdmissionSubjectGeneration(t, manager.BaseCapabilitySubjects(), catalog.Generation(), 1)
}

type mixedAdmissionGenerationError struct{ first, second string }

func (e *mixedAdmissionGenerationError) Error() string {
	return "capability snapshot mixed admission generations " + e.first + " and " + e.second
}

func runtimeAdmissionTestCatalog(t *testing.T, hashToken string) *providertriggers.CatalogSnapshot {
	t.Helper()
	manifest := providertriggers.Manifest{
		Provider: "acme", Secret: providertriggers.SecretManifest{Required: true},
		Signature:  providertriggers.SignatureManifest{Type: "token_equality", Header: "X-Acme-Token"},
		DeliveryID: providertriggers.ValueSource{Header: "X-Acme-Delivery", Required: true},
		EventType:  providertriggers.ValueSource{Literal: "event", Required: true},
		EventName:  providertriggers.EventNameManifest{Literal: "inbound.acme"},
		Ack:        providertriggers.AckManifest{Mode: "after_publish"},
	}
	catalog, err := providertriggers.NewCatalogSnapshot(providertriggers.CatalogEntry{
		Identity: providertriggers.PackIdentity{
			ID: "provider.acme", Version: "1.0.0", ManifestHash: "sha256:" + strings.Repeat(hashToken, 64), Provenance: packs.ProvenanceExternal,
		},
		Manifest: manifest, Source: "test", SourcePath: "/packs/acme",
	})
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func runtimeAdmissionUnsignedTestCatalog(t *testing.T, hashToken string) *providertriggers.CatalogSnapshot {
	t.Helper()
	manifest := providertriggers.Manifest{
		Provider: "acme", Secret: providertriggers.SecretManifest{Required: false},
		DeliveryID: providertriggers.ValueSource{Header: "X-Acme-Delivery", Required: true},
		EventType:  providertriggers.ValueSource{Literal: "event", Required: true},
		EventName:  providertriggers.EventNameManifest{Literal: "inbound.acme"},
		Ack:        providertriggers.AckManifest{Mode: "after_publish"},
	}
	catalog, err := providertriggers.NewCatalogSnapshot(providertriggers.CatalogEntry{
		Identity: providertriggers.PackIdentity{
			ID: "provider.acme", Version: "1.0.0", ManifestHash: "sha256:" + strings.Repeat(hashToken, 64), Provenance: packs.ProvenanceExternal,
		},
		Manifest: manifest, Source: "test", SourcePath: "/packs/acme",
	})
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func runtimeAdmissionTestState(t *testing.T, catalog *providertriggers.CatalogSnapshot) ProcessAdmissionState {
	t.Helper()
	installed, err := catalog.InstalledCapabilitySubjects()
	if err != nil {
		t.Fatal(err)
	}
	return ProcessAdmissionState{Generation: catalog.Generation(), InstalledSubjects: installed}
}

func runtimeAdmissionTestContext(t *testing.T, hash, alias string, catalog *providertriggers.CatalogSnapshot) BundleContext {
	t.Helper()
	contextDef := testBundleContext(t, hash, "inbound.acme")
	plan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{
		Alias: alias, Provider: "acme", SigningSecret: "webhook_signing.acme",
	})
	if err != nil {
		t.Fatal(err)
	}
	contextDef.StandingTargets = []StandingTarget{{
		BundleHash: hash, ServiceID: "service-" + alias, FlowID: "acme-flow", Alias: alias, Provider: "acme", RunID: "run-" + alias,
		Generation: 1, FlowInstance: "acme-flow/" + alias, EntityID: "entity-" + alias,
		SigningSecret: "webhook_signing.acme", AdmissionPlan: plan,
	}}
	return contextDef
}

func assertRuntimeAdmissionSubjectGeneration(t *testing.T, subjects []packs.Subject, generation triggergeneration.Generation, wantEffective int) {
	t.Helper()
	effective := 0
	for _, subject := range subjects {
		if subject.TriggerAdmission == nil {
			continue
		}
		effective++
		if subject.TriggerAdmission.CatalogGeneration != generation.Diagnostic() {
			t.Fatalf("subject %q generation = %q, want %q", subject.ID, subject.TriggerAdmission.CatalogGeneration, generation.Diagnostic())
		}
	}
	if effective != wantEffective {
		t.Fatalf("effective trigger subjects = %d, want %d", effective, wantEffective)
	}
}
