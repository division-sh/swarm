package runtime

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
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
	manager, err := newTestRuntimeContextManager(t, nil, contextDef)
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
	manager, err := newTestRuntimeContextManager(t, nil, contextDef)
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
	manager, err := newTestRuntimeContextManager(t, nil, contextDef)
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

func TestRuntimeContextManagerInvalidatesCredentialProjectionAcrossSourceSetFenceAndAbort(t *testing.T) {
	ctx := context.Background()
	module := loadRuntimeOwnershipWorkflowModule(t)
	fact := testBundleSourceFact(t, runtimeTestBundleHash)
	authority, err := runtimestartupownership.NewColdAuthority(runtimestartupownership.AcquireRequest{
		OwnerID: "runtime-test-process", BootID: uuid.NewString(), RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
	}, "runtime_test")
	if err != nil {
		t.Fatalf("new cold authority: %v", err)
	}
	selected := &runtimeTestRetainedSession{authority: authority, agents: map[string]runtimemanager.PersistedAgent{}}
	cfg := &config.Config{}
	cfg.Runtime.ExecutionPosture = "live"
	rt, err := NewRuntime(testAuthorActivityContext(ctx), RuntimeDeps{
		Config: cfg,
		ManagerPersistenceRoles: runtimemanager.PersistenceRoles{
			LifecycleCensus: selected,
		},
		Options: RuntimeOptions{
			SelfCheck: false, WorkflowModule: module, LLMRuntime: noopLLMRuntime{},
			RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
			BundleSourceFact:  fact,
			ProcessWorkOwner:  runtimeTestProcessWorkOwner(t),
		},
	})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	t.Cleanup(func() {
		if shutdownErr := rt.Shutdown(); shutdownErr != nil {
			t.Errorf("Shutdown: %v", shutdownErr)
		}
	})
	capability, grant, err := newRuntimeTestProcessCapabilityWithSession(t, rt.Manager, module.source, fact, authorActivityTestRuntimeInstanceID, selected)
	if err != nil {
		t.Fatalf("new process capability: %v", err)
	}
	if err := rt.InstallStartupGrant(grant); err != nil {
		t.Fatalf("InstallStartupGrant: %v", err)
	}
	if err := rt.Start(testAuthorActivityContext(ctx)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	plan, err := grant.SourceSetPlan(ctx)
	if err != nil {
		t.Fatalf("SourceSetPlan: %v", err)
	}
	if err := plan.Validate(); err != nil {
		t.Fatalf("source-set plan: %v", err)
	}
	bundleHash, bundleSource := fact.StorageValues()
	if len(plan.Sources) != 1 || plan.Sources[0].BundleHash != bundleHash || plan.Sources[0].BundleSource != bundleSource {
		t.Fatalf("source-set coordinates = %#v, want exact runtime coordinate", plan.Sources)
	}

	bundle, ok := semanticview.Bundle(module.source)
	if !ok || bundle == nil || bundle.PackInventory == nil {
		t.Fatal("runtime bundle effective pack inventory is required")
	}
	catalog, _, err := providertriggers.NewCatalogSnapshotFromInventory(bundle.PackInventory, bundle.Platform.Platform.Version)
	if err != nil {
		t.Fatalf("derive runtime bundle provider-trigger catalog: %v", err)
	}
	planAdmission, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{
		Alias: "primary", Provider: "telegram", SigningSecret: "webhook_signing.telegram",
	})
	if err != nil {
		t.Fatalf("compile bundle-owned admission: %v", err)
	}
	contextDef := testBundleContext(t, runtimeTestBundleHash, "inbound.telegram")
	contextDef.BundleSourceFact = fact
	contextDef.Source = module.source
	contextDef.Runtime = rt
	contextDef.WorkOwner = rt.WorkOccurrence()
	contextDef.StandingTargets = []StandingTarget{{
		BundleHash: runtimeTestBundleHash, ServiceID: "service-primary", FlowID: "telegram-flow", Alias: "primary", Provider: "telegram",
		RunID: "run-primary", Generation: 1, FlowInstance: "telegram-flow/primary", EntityID: "entity-primary",
		SigningSecret: "webhook_signing.telegram", AdmissionPlan: planAdmission,
	}}
	applyRuntimeAdmissionCatalog(t, &contextDef, catalog)
	contextDef.PackInventoryDigest = bundle.PackInventory.Digest()
	manager, err := newTestRuntimeContextManager(t, nil, contextDef)
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
	transition, err := manager.PrepareSourceSetTransition(ctx, plan)
	if err != nil {
		t.Fatalf("PrepareSourceSetTransition: %v", err)
	}
	close(blocking.release)
	assertRuntimeAdmissionProjectionStale(t, <-errCh)
	assertRuntimeAdmissionEffectiveSubjectCount(t, manager.BaseCapabilitySubjects(), 0)
	if err := transition.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	assertRuntimeAdmissionEffectiveSubjectCount(t, manager.BaseCapabilitySubjects(), 1)

	transition, err = manager.PrepareSourceSetTransition(ctx, plan)
	if err != nil {
		t.Fatalf("PrepareSourceSetTransition retry: %v", err)
	}
	assertRuntimeAdmissionEffectiveSubjectCount(t, manager.BaseCapabilitySubjects(), 0)
	if err := transition.Commit(ctx, capability); err != nil {
		t.Fatalf("Commit: %v", err)
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

func TestValidateRuntimeContextSetDoesNotActivateStandingOccurrences(t *testing.T) {
	catalog := runtimeAdmissionTestCatalog(t, "a")
	contextDef := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", catalog)
	owner := contextDef.Runtime.WorkOccurrence()
	before := owner.ActiveCount()

	if err := ValidateRuntimeContextSet(contextDef); err != nil {
		t.Fatalf("ValidateRuntimeContextSet: %v", err)
	}
	if got := owner.ActiveCount(); got != before {
		t.Fatalf("validation activated %d runtime lease(s), want unchanged count %d", got, before)
	}
}

func TestStandingServiceTransitionRollbackRestoresExactOwnerSchedulesBeforeAdmission(t *testing.T) {
	catalog := runtimeAdmissionTestCatalog(t, "a")
	contextDef := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", catalog)
	contextDef.Runtime.Scheduler = runtimeContextTestScheduler(t, contextDef.WorkOwner, nil)
	manager, err := newTestRuntimeContextManager(t, nil, contextDef)
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
	manager, err := newTestRuntimeContextManager(t, nil, contextDef)
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
	manager, err := newTestRuntimeContextManager(t, nil, contextDef)
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
	applyRuntimeAdmissionCatalog(t, &contextDef, catalog)
	return contextDef
}

func applyRuntimeAdmissionCatalog(t testing.TB, contextDef *BundleContext, catalog *providertriggers.CatalogSnapshot) {
	t.Helper()
	contextDef.ProviderTriggerGeneration = catalog.Generation()
	installed, err := catalog.InstalledCapabilitySubjects()
	if err != nil {
		t.Fatal(err)
	}
	contextDef.InstalledTriggerSubjects = installed
	if bundle, ok := semanticview.Bundle(contextDef.Source); ok && bundle != nil && bundle.PackInventory != nil {
		contextDef.PackInventoryDigest = bundle.PackInventory.Digest()
	}
}

func projectTelegramAdmissionContext(t *testing.T, hash, alias, payloadObjectError string) BundleContext {
	t.Helper()
	project := t.TempDir()
	if err := os.WriteFile(filepath.Join(project, "package.yaml"), []byte("name: same-id-pack-proof\nversion: 1.0.0\nplatform_version: '>=0.7.0 <0.8.0'\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	base := packfixture.EmbeddedBase(t)
	if changed, err := packartifact.ImportEmbeddedPack(project, "provider.telegram", base); err != nil || !changed {
		t.Fatalf("import Telegram pack changed=%t: %v", changed, err)
	}
	manifestPath := filepath.Join(project, packartifact.ProjectPackDirectory, "provider.telegram", packartifact.TriggerManifestFileName)
	body, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	edited := strings.Replace(string(body), "telegram update object is required", payloadObjectError, 1)
	if edited == string(body) {
		t.Fatal("Telegram payload error edit found no canonical field")
	}
	if err := os.WriteFile(manifestPath, []byte(edited), 0o644); err != nil {
		t.Fatal(err)
	}
	projectPacks, err := packartifact.LoadProjectPackSet(project)
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := packartifact.NewEffectivePackInventory(base, projectPacks.Sources)
	if err != nil {
		t.Fatal(err)
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics:     runtimecontracts.WorkflowSemanticView{Name: "same_id_pack", Version: "1.0.0"},
		Events:        map[string]runtimecontracts.EventCatalogEntry{"inbound.telegram": {}, "inbound.telegram.text_message": {}},
		PackInventory: inventory,
	}
	platformBody, err := os.ReadFile(runtimecontracts.DefaultPlatformSpecFile(runtimepipeline.WorkflowRepoRoot()))
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(platformBody, &bundle.Platform); err != nil {
		t.Fatal(err)
	}
	admitRuntimeTestBundle(t, bundle)
	projection, err := packadmission.FromBundle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	catalog := projection.ProviderTriggers
	plan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{
		Alias: alias, Provider: "telegram", SigningSecret: "webhook_signing.telegram",
	})
	if err != nil {
		t.Fatal(err)
	}
	contextDef := testBundleContext(t, hash, "inbound.telegram")
	contextDef.Source = semanticview.Wrap(bundle)
	contextDef.StandingTargets = []StandingTarget{{
		BundleHash: hash, ServiceID: "service-" + alias, FlowID: "telegram-flow", Alias: alias, Provider: "telegram",
		RunID: "run-" + alias, Generation: 1, FlowInstance: "telegram-flow/" + alias, EntityID: "entity-" + alias,
		SigningSecret: "webhook_signing.telegram", AdmissionPlan: plan,
	}}
	applyRuntimeAdmissionCatalog(t, &contextDef, catalog)
	return contextDef
}

func assertProjectTelegramAdmissionMessage(t *testing.T, manager *RuntimeContextManager, alias, want string) {
	t.Helper()
	lookup := manager.LookupIngress(alias, "telegram")
	if !lookup.Loaded() {
		t.Fatalf("lookup %s = %#v", alias, lookup)
	}
	identity, ok := lookup.Target.AdmissionPlan.PackIdentity()
	if !ok || identity.ID != "provider.telegram" || identity.Provenance != packartifact.ProvenanceProject {
		t.Fatalf("%s admission identity = %#v, present=%t", alias, identity, ok)
	}
	subject, err := lookup.Target.CapabilitySubject()
	if err != nil {
		t.Fatalf("%s capability subject: %v", alias, err)
	}
	if subject.Provenance != packartifact.ProvenanceProject || subject.TriggerAdmission == nil || subject.TriggerAdmission.Pack == nil ||
		subject.TriggerAdmission.Pack.ID != identity.ID || subject.TriggerAdmission.Pack.Version != identity.Version ||
		subject.TriggerAdmission.Pack.ManifestHash != identity.ManifestHash || subject.TriggerAdmission.Pack.Provenance != identity.Provenance {
		t.Fatalf("%s capability subject identity = %#v, want %#v", alias, subject, identity)
	}
	_, err = lookup.Target.AdmissionPlan.Accept(providertriggers.Request{
		Provider: "telegram", Target: providertriggers.Target{EntityID: "entity-" + alias, WebhookSecret: "telegram-secret"},
		Method: http.MethodPost, Headers: http.Header{"X-Telegram-Bot-Api-Secret-Token": []string{"telegram-secret"}},
		Payload: []any{}, Body: []byte(`[]`), ContentType: "application/json",
	})
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("%s project admission error = %v, want containing %q", alias, err, want)
	}
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
