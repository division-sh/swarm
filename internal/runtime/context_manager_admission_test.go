package runtime

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
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

func TestRuntimeContextManagerReadbackSeparatesPresenceFromFrozenAdmission(t *testing.T) {
	ctx := context.Background()
	catalog := runtimeAdmissionTestCatalog(t, "a")
	contextDef := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", catalog)
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	freezeTestStandingCredentialAdmission(t, contextDef, store)
	manager, err := newTestRuntimeContextManager(t, nil, contextDef)
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
	assertEffectiveTargetStatus(packs.StatusNotReady, packs.RequirementStatusBound)
	// A separately admitted runtime may enable it; a diagnostic read may not.
	freezeTestStandingCredentialAdmission(t, contextDef, store)
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
	module := loadRuntimeOwnershipWorkflowModule(t)
	ctx, fact := runtimeOwnershipFixtureContext(t, module)
	authority, err := runtimeTestProcessAttribution(runtimestartupownership.AcquireRequest{
		OwnerID: "runtime-test-process", BootID: uuid.NewString(), RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
	})
	if err != nil {
		t.Fatalf("new cold authority: %v", err)
	}
	selected := &runtimeTestRetainedSession{authority: authority, agents: map[string]runtimemanager.PersistedAgent{}}
	cfg := &config.Config{}
	rt, err := NewRuntime(ctx, completeRuntimeRecoveryTestDeps(t, RuntimeDeps{
		Config:       cfg,
		ManagerStore: selected,
		ManagerPersistenceRoles: runtimemanager.PersistenceRoles{
			LifecycleCensus: selected,
		},
		Options: RuntimeOptions{
			SelfCheck: false, WorkflowModule: module, LLMRuntime: noopLLMRuntime{},
			ExecutionPosture:   executionposture.Live,
			RuntimeInstanceID:  authorActivityTestRuntimeInstanceID,
			SourceArtifactFact: fact,
			ProcessWorkOwner:   runtimeTestProcessWorkOwner(t),
		},
	}))
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	t.Cleanup(func() {
		if shutdownErr := rt.Shutdown(); shutdownErr != nil {
			t.Errorf("Shutdown: %v", shutdownErr)
		}
	})
	var retained *startupRecoveryFanOutSession
	capability, grant, err := newRuntimeTestProcessCapabilityWithSession(t, rt.Manager, module.source, fact, authorActivityTestRuntimeInstanceID, selected, func(session *runtimeTestRetainedSession) runtimestartupownership.RetainedSession {
		retained = &startupRecoveryFanOutSession{runtimeTestRetainedSession: session, capacity: runtimestartupownership.SQLiteFanOutCapacity()}
		return retained
	})
	if err != nil {
		t.Fatalf("new process capability: %v", err)
	}
	if err := rt.InstallStartupGrant(grant); err != nil {
		t.Fatalf("InstallStartupGrant: %v", err)
	}
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	retained.requireActivityGrant(t, grant)
	plan, err := grant.SourceSetPlan(ctx)
	if err != nil {
		t.Fatalf("SourceSetPlan: %v", err)
	}
	if err := plan.Validate(); err != nil {
		t.Fatalf("source-set plan: %v", err)
	}
	bundleHash := fact.BundleHash()
	if len(plan.Sources) != 1 || plan.Sources[0].BundleHash != bundleHash {
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
	contextDef := testBundleContext(t, bundleHash, "inbound.telegram")
	contextDef.SourceArtifactFact = fact
	contextDef.Source = module.source
	contextDef.Runtime = rt
	contextDef.WorkOwner = rt.WorkOccurrence()
	contextDef.StandingTargets = []StandingTarget{{
		BundleHash: bundleHash, FlowPath: "telegram-flow", Alias: "primary", Provider: "telegram",
		RunID:         "run-primary",
		SigningSecret: "webhook_signing.telegram", AdmissionPlan: planAdmission,
	}}
	applyRuntimeAdmissionCatalog(t, &contextDef, catalog)
	contextDef.PackInventoryDigest = bundle.PackInventory.Digest()
	freezeTestStandingCredentialAdmission(t, contextDef, identityInboundCredentialStore{})
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

func runtimeAdmissionTestCatalog(t *testing.T, hashToken string) *providertriggers.CatalogSnapshot {
	t.Helper()
	manifest, err := providertriggers.ParseManifest([]byte(`provider: acme
secret: {required: true}
signature: {type: token_equality, header: X-Acme-Token}
delivery_id: {header: X-Acme-Delivery, required: true}
event_type: {literal: event, required: true}
event_name: {literal: inbound.acme}
`))
	if err != nil {
		t.Fatal(err)
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
		BundleHash: hash, FlowPath: "acme-flow", Alias: alias, Provider: "acme", RunID: "run-" + alias,
		SigningSecret: "webhook_signing.acme", AdmissionPlan: plan,
	}}
	applyRuntimeAdmissionCatalog(t, &contextDef, catalog)
	freezeTestStandingCredentialAdmission(t, contextDef, identityInboundCredentialStore{})
	return contextDef
}

func freezeTestStandingCredentialAdmission(t *testing.T, contextDef BundleContext, store runtimecredentials.Store) {
	t.Helper()
	owner, err := runtimecredentials.NewSnapshotOwner(store)
	if err != nil {
		t.Fatal(err)
	}
	admission := &standingCredentialAdmission{bindings: map[string]standingBindingCredentials{}, projection: owner.BeginSecretBindingProjection()}
	for _, target := range contextDef.StandingTargets {
		binding := standingBindingCredentials{alias: target.Alias, plan: target.AdmissionPlan, enabled: true, signingKey: target.SigningSecret}
		if target.AdmissionPlan.RequiresSecret() {
			observed, err := admission.projection.ObserveActivationCredential(context.Background(), target.SigningSecret)
			if err != nil {
				t.Fatal(err)
			}
			binding.enabled = observed.Present
			binding.credentialKeys = []string{target.SigningSecret}
		}
		admission.bindings[standingIngressSelector(target.FlowPath, target.Provider)] = binding
	}
	contextDef.Runtime.Options.SourceArtifactFact = contextDef.SourceArtifactFact
	contextDef.Runtime.standingCredentialAdmission = admission
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
