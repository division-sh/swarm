package runtime

import (
	"context"
	goruntime "runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

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
			subjects := manager.CapabilitySubjects()
			lookup := manager.LookupIngress("survivor", "acme")
			if !lookup.Loaded() {
				select {
				case errCh <- &mixedAdmissionGenerationError{first: "loaded", second: "missing lookup"}:
				default:
				}
				return
			}
			lookupGeneration := lookup.Target.AdmissionPlan.GenerationID()
			if lookupGeneration != oldCatalog.GenerationID() && lookupGeneration != newCatalog.GenerationID() {
				select {
				case errCh <- &mixedAdmissionGenerationError{first: oldCatalog.GenerationID() + " or " + newCatalog.GenerationID(), second: lookupGeneration}:
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

	if got := manager.AdmissionState().GenerationID; got != newCatalog.GenerationID() {
		t.Fatalf("process generation = %q, want %q", got, newCatalog.GenerationID())
	}
	for _, alias := range []string{"primary", "survivor"} {
		lookup := manager.LookupIngress(alias, "acme")
		if !lookup.Loaded() || lookup.Target.AdmissionPlan.GenerationID() != newCatalog.GenerationID() {
			t.Fatalf("lookup %q = %#v, want loaded new generation", alias, lookup)
		}
	}
	subjects := manager.CapabilitySubjects()
	assertRuntimeAdmissionSubjectGeneration(t, subjects, newCatalog.GenerationID(), 2)
	for _, subject := range subjects {
		if subject.Applicability != "effective" || subject.TriggerAdmission == nil {
			continue
		}
		if subject.TriggerAdmission.Pack == nil || subject.TriggerAdmission.Pack.ManifestHash != strings.Repeat("b", 64) {
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
			predecessor.Runtime.Scheduler = runtimepipeline.NewSchedulerWithWorkOwner(predecessor.WorkOwner)
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
				if err := managerOwner.RetireAndWait(context.Background()); err != nil {
					t.Errorf("retire replacement Manager owner: %v", err)
				}
			})
			managerWork, err := managerOwner.Begin(context.Background(), standing)
			if err != nil {
				t.Fatalf("begin Manager-composed replacement work: %v", err)
			}
			ownerCtx := managerWork.Context()
			for _, schedule := range []runtimepipeline.Schedule{
				{RunID: "run-primary", AgentID: "timer-agent", EventType: "timer.once", Mode: "once", At: time.Now().Add(time.Hour), TaskID: "future-once"},
				{RunID: "run-primary", AgentID: "timer-agent", EventType: "timer.cron", Mode: "cron", Cron: "@every 1h", TaskID: "recurring-cron"},
			} {
				if err := predecessor.Runtime.Scheduler.Register(ownerCtx, schedule); err != nil {
					t.Fatalf("register %s schedule: %v", schedule.Mode, err)
				}
			}
			if err := managerWork.Done(); err != nil {
				t.Fatalf("settle Manager-composed replacement work: %v", err)
			}
			route, err := predecessor.WorkOwner.NewRoute(context.Background(), worklifetime.RouteIdentity{
				RuntimeEpoch: 1, AgentID: "replacement-route", Generation: 1,
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
			candidate.Runtime.Scheduler = runtimepipeline.NewSchedulerWithWorkOwner(candidate.WorkOwner)
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
			if err := prepared.Publish(); err != nil {
				t.Fatalf("publish replacement: %v", err)
			}
			freshStanding := manager.contexts[candidateHash].standing[serviceID]
			parked, err := candidate.Runtime.Scheduler.ParkOccurrence(context.Background(), freshStanding)
			if err != nil {
				t.Fatalf("inspect rehydrated schedules: %v", err)
			}
			if len(parked) != 2 {
				t.Fatalf("rehydrated schedules = %#v, want future one-shot and recurring cron", parked)
			}
			modes := map[string]bool{}
			for _, schedule := range parked {
				modes[schedule.Mode] = true
			}
			if !modes["once"] || !modes["cron"] {
				t.Fatalf("rehydrated schedule modes = %#v, want once and cron", modes)
			}
		})
	}
}

func TestStandingServiceTransitionRollbackRestoresExactOwnerSchedulesBeforeAdmission(t *testing.T) {
	catalog := runtimeAdmissionTestCatalog(t, "a")
	contextDef := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", catalog)
	contextDef.Runtime.Scheduler = runtimepipeline.NewSchedulerWithWorkOwner(contextDef.WorkOwner)
	manager, err := newTestRuntimeContextManagerWithAdmission(t, nil, runtimeAdmissionTestState(t, catalog), contextDef)
	if err != nil {
		t.Fatal(err)
	}
	serviceID := contextDef.StandingTargets[0].ServiceID
	standing := manager.contexts[runtimeContextTestHashA].standing[serviceID]
	ownerCtx := worklifetime.WithOccurrence(context.Background(), standing)
	for _, schedule := range []runtimepipeline.Schedule{
		{RunID: "run-primary", AgentID: "timer-agent", EventType: "timer.once", Mode: "once", At: time.Now().Add(time.Hour), TaskID: "future-once"},
		{RunID: "run-primary", AgentID: "timer-agent", EventType: "timer.cron", Mode: "cron", Cron: "@every 1h", TaskID: "recurring-cron"},
	} {
		if err := contextDef.Runtime.Scheduler.Register(ownerCtx, schedule); err != nil {
			t.Fatalf("register %s schedule: %v", schedule.Mode, err)
		}
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
	parked, err := contextDef.Runtime.Scheduler.ParkOccurrence(context.Background(), standing)
	if err != nil {
		t.Fatalf("inspect restored schedules: %v", err)
	}
	if len(parked) != 2 {
		t.Fatalf("restored schedules = %#v, want future one-shot and recurring cron", parked)
	}
	modes := map[string]bool{}
	for _, schedule := range parked {
		modes[schedule.Mode] = true
	}
	if !modes["once"] || !modes["cron"] {
		t.Fatalf("restored schedule modes = %#v, want once and cron", modes)
	}
}

func TestPreparedStandingSuccessorOwnsSchedulesBeforePublication(t *testing.T) {
	catalog := runtimeAdmissionTestCatalog(t, "a")
	contextDef := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", catalog)
	contextDef.Runtime.Scheduler = runtimepipeline.NewSchedulerWithWorkOwner(contextDef.WorkOwner)
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
	for _, schedule := range []runtimepipeline.Schedule{
		{RunID: successorRunID, AgentID: "timer-agent", EventType: "timer.once", Mode: "once", At: time.Now().Add(time.Hour), TaskID: "future-once"},
		{RunID: successorRunID, AgentID: "timer-agent", EventType: "timer.cron", Mode: "cron", Cron: "@every 1h", TaskID: "recurring-cron"},
	} {
		if err := contextDef.Runtime.Scheduler.Register(prepared.WorkContext(context.Background()), schedule); err != nil {
			t.Fatalf("register prepared %s schedule: %v", schedule.Mode, err)
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
	if len(parked) != 2 {
		t.Fatalf("prepared successor schedules = %#v, want future one-shot and recurring cron", parked)
	}
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
	if got := manager.AdmissionState().GenerationID; got != oldCatalog.GenerationID() {
		t.Fatalf("failed candidate changed generation to %q", got)
	}
	for _, alias := range []string{"primary", "survivor"} {
		lookup := manager.LookupIngress(alias, "acme")
		if !lookup.Loaded() || lookup.Target.AdmissionPlan.GenerationID() != oldCatalog.GenerationID() {
			t.Fatalf("failed candidate changed lookup %q: %#v", alias, lookup)
		}
	}
	assertRuntimeAdmissionSubjectGeneration(t, manager.CapabilitySubjects(), oldCatalog.GenerationID(), 2)
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
	if got := manager.LookupIngress("survivor", "acme"); !got.Loaded() || got.Target.AdmissionPlan.GenerationID() != newCatalog.GenerationID() {
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
	if got := manager.AdmissionState().GenerationID; got != oldCatalog.GenerationID() {
		t.Fatalf("pack removal changed process generation to %q", got)
	}
	if got := manager.LookupIngress("chat", "telegram"); !got.Loaded() || got.Target.AdmissionPlan.GenerationID() != oldCatalog.GenerationID() {
		t.Fatalf("pack removal changed survivor: %#v", got)
	}
	if _, ok := manager.LookupBundleHash(candidatePrimary.BundleHash); !ok {
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
	if got := manager.AdmissionState().GenerationID; got != oldCatalog.GenerationID() {
		t.Fatalf("collision changed generation to %q", got)
	}
	for alias, hash := range map[string]string{"primary": runtimeContextTestHashA, "survivor": runtimeContextTestHashB} {
		lookup := manager.LookupIngress(alias, "acme")
		if !lookup.Loaded() || lookup.Context.BundleHash != hash || lookup.Target.AdmissionPlan.GenerationID() != oldCatalog.GenerationID() {
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
	if !lookup.Loaded() || lookup.Target.AdmissionPlan.RequestAuthentication() != providertriggers.RequestAuthenticationNone || lookup.Target.AdmissionPlan.GenerationID() != unsigned.GenerationID() {
		t.Fatalf("acknowledged transition lookup = %#v", lookup)
	}
	for _, subject := range manager.CapabilitySubjects() {
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
	if !lookup.Loaded() || lookup.Target.AdmissionPlan.GenerationID() != catalog.GenerationID() {
		t.Fatalf("restored lookup = %#v", lookup)
	}
	assertRuntimeAdmissionSubjectGeneration(t, manager.CapabilitySubjects(), catalog.GenerationID(), 1)
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
			ID: "provider.acme", Version: "1.0.0", ManifestHash: strings.Repeat(hashToken, 64), Provenance: packs.ProvenanceExternal,
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
			ID: "provider.acme", Version: "1.0.0", ManifestHash: strings.Repeat(hashToken, 64), Provenance: packs.ProvenanceExternal,
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
	return ProcessAdmissionState{GenerationID: catalog.GenerationID(), InstalledSubjects: installed}
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

func assertRuntimeAdmissionSubjectGeneration(t *testing.T, subjects []packs.Subject, generation string, wantEffective int) {
	t.Helper()
	effective := 0
	for _, subject := range subjects {
		if subject.TriggerAdmission == nil {
			continue
		}
		effective++
		if subject.TriggerAdmission.CatalogGeneration != generation {
			t.Fatalf("subject %q generation = %q, want %q", subject.ID, subject.TriggerAdmission.CatalogGeneration, generation)
		}
	}
	if effective != wantEffective {
		t.Fatalf("effective trigger subjects = %d, want %d", effective, wantEffective)
	}
}
