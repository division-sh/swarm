package runtime

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
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
	catalog, source := runtimeAdmissionTestCatalog(t, "a", "primary")
	contextDef := runtimeAdmissionTestContext(t, source, catalog, uuid.NewString())
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
	if err := store.Set(ctx, contextDef.StandingTargets[0].SigningSecret, secretValue); err != nil {
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
	if err := store.Delete(ctx, contextDef.StandingTargets[0].SigningSecret); err != nil {
		t.Fatal(err)
	}
	assertEffectiveTargetStatus(packs.StatusNotReady, packs.RequirementStatusUnbound)
}

func TestRuntimeContextManagerInvalidatesCredentialProjectionAcrossOrdinaryUnload(t *testing.T) {
	ctx := context.Background()
	catalog, source := runtimeAdmissionTestCatalog(t, "a", "primary")
	contextDef := runtimeAdmissionTestContext(t, source, catalog, uuid.NewString())
	manager, err := newTestRuntimeContextManager(t, nil, contextDef)
	if err != nil {
		t.Fatal(err)
	}
	blocking, owner := runtimeAdmissionBlockingCredentialOwner(t, "secret")
	if err := blocking.Store.Set(ctx, contextDef.StandingTargets[0].SigningSecret, "secret"); err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() {
		_, projectionErr := manager.EvaluatedCapabilitySubjects(ctx, owner)
		errCh <- projectionErr
	}()
	<-blocking.entered
	result := manager.DeactivateBundleHash(contextDef.BundleHash(), RuntimeContextCauseUnloaded)
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
	catalog, source := runtimeAdmissionTestCatalog(t, "source-set", "source-set-fixture")
	module := semanticOnlyWorkflowRuntime{source: source}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		t.Fatal("source-set fixture requires its compiled captured source")
	}
	rooted, err := bundle.RootedSource()
	if err != nil {
		t.Fatal(err)
	}
	fact := sourceartifactfixture.FactFor(rooted.Artifact())
	ctx := testAuthorActivityContextForBundle(context.Background(), fact.BundleHash())
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, fact)
	ctx = runtimecorrelation.WithRootAdmission(ctx, rooted.Admission())
	coordinate, err := runtimeagenttopology.NewSourceCoordinate(rooted.Admission())
	if err != nil {
		t.Fatal(err)
	}
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
			ProviderTriggerCatalog: catalog,
			ProviderCredentials:    identityInboundCredentialStore{},
			ExecutionPosture:       executionposture.Live,
			RuntimeInstanceID:      authorActivityTestRuntimeInstanceID,
			SourceArtifactFact:     fact,
			ProcessWorkOwner:       runtimeTestProcessWorkOwner(t),
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
	t.Cleanup(func() { _ = capability.Release(context.Background()) })
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
	if len(plan.Sources) != 1 || plan.Sources[0] != coordinate {
		t.Fatalf("source-set coordinates = %#v, want exact runtime coordinate", plan.Sources)
	}

	contextDef := BundleContext{
		SourceArtifactFact: fact, RootAdmission: rooted.Admission(), Source: source,
		Runtime: rt, WorkOwner: rt.WorkOccurrence(), ProviderTriggerCatalog: catalog,
	}
	contextDef.BundleIdentity, err = runtimecontracts.BootBundleIdentity(bundle)
	if err != nil {
		t.Fatal(err)
	}
	contextDef.StandingTargets, err = rt.PlanStandingTargets()
	if err != nil {
		t.Fatal(err)
	}
	contextDef.StandingTargets = bindStandingContextFixtureTargets(t, source, contextDef.StandingTargets, "run-primary")
	applyRuntimeAdmissionCatalog(t, &contextDef, catalog)
	contextDef.PackInventoryDigest = bundle.PackInventory.Digest()
	freezeTestStandingCredentialAdmission(t, contextDef, identityInboundCredentialStore{})
	manager, err := newTestRuntimeContextManager(t, nil, contextDef)
	if err != nil {
		t.Fatal(err)
	}
	blocking, owner := runtimeAdmissionBlockingCredentialOwner(t, "secret")
	if len(contextDef.StandingTargets) != 1 {
		t.Fatal("source-set fixture requires one captured standing declaration")
	}
	if err := blocking.Set(ctx, contextDef.StandingTargets[0].SigningSecret, "secret"); err != nil {
		t.Fatal(err)
	}
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
	catalog, source := runtimeAdmissionTestCatalog(t, "a", "primary")
	contextDef := runtimeAdmissionTestContext(t, source, catalog, uuid.NewString())
	owner := contextDef.Runtime.WorkOccurrence()
	before := owner.ActiveCount()

	if err := ValidateRuntimeContextSet(contextDef); err != nil {
		t.Fatalf("ValidateRuntimeContextSet: %v", err)
	}
	if got := owner.ActiveCount(); got != before {
		t.Fatalf("validation activated %d runtime lease(s), want unchanged count %d", got, before)
	}
}

func runtimeAdmissionTestCatalog(t *testing.T, revision, directory string) (*providertriggers.CatalogSnapshot, semanticview.Source) {
	t.Helper()
	artifact, err := sourceartifact.AdmitMembers(map[string][]byte{
		"manifest.yaml":                []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"schema.yaml":                  []byte("# fixture directory " + directory + "\nstages: []\n"),
		"acme-flow/manifest.yaml":      []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"acme-flow/schema.yaml":        []byte("stages: []\npins: {inputs: [inbound.acme.raw]}\n"),
		"acme-flow/acme/manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {emit_events: [raw], verify_secret: webhook_signing_secret}, cannot: [bypass journal]}\n"),
		"acme-flow/acme/trigger.yaml": []byte("# captured revision " + revision + "\n" + `secret: {required: true}
signature: {type: token_equality, header: X-Acme-Token}
delivery_id: {header: X-Acme-Delivery, required: true}
event_type: {literal: event, required: true}
event_name: {literal: raw}
`),
	})
	if err != nil {
		t.Fatal(err)
	}
	rooted, _ := sourceartifactfixture.Rooted(t, directory, artifact)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleFromRootedSource(canonicalrouting.RepoRoot(t), rooted, "", runtimecontracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	admitRuntimeTestBundle(t, bundle)
	providers, err := packadmission.AdmitProviderSource(rooted, bundle.Platform)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := providers.Scope("acme-flow")
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := scope.TriggerCatalog()
	if err != nil {
		t.Fatal(err)
	}
	source, err := SourceWithProviderTriggerEvents(semanticview.Wrap(bundle), catalog)
	if err != nil {
		t.Fatal(err)
	}
	return catalog, source
}

func runtimeAdmissionTestContext(t *testing.T, source semanticview.Source, catalog *providertriggers.CatalogSnapshot, runID string) BundleContext {
	t.Helper()
	bundle, present := semanticview.Bundle(source)
	if !present {
		t.Fatal("admission fixture requires its compiled captured source")
	}
	rooted, err := bundle.RootedSource()
	if err != nil {
		t.Fatal(err)
	}
	contextDef := rootedContextForSource(t, source, rooted)
	contextDef.ProviderTriggerCatalog = catalog
	contextDef.BundleIdentity, err = runtimecontracts.BootBundleIdentity(bundle)
	if err != nil {
		t.Fatal(err)
	}
	contextDef.RuntimeInstanceID = contextDef.WorkOwner.Identity().RuntimeInstanceID
	contextDef.Runtime.Options = RuntimeOptions{
		WorkflowModule: semanticOnlyWorkflowRuntime{source: source}, ProviderTriggerCatalog: catalog,
		ProviderCredentials: identityInboundCredentialStore{}, SourceArtifactFact: contextDef.SourceArtifactFact,
		RuntimeInstanceID: contextDef.RuntimeInstanceID,
		ProcessWorkOwner:  contextDef.Runtime.Options.ProcessWorkOwner,
	}
	contextDef.StandingTargets, err = contextDef.Runtime.PlanStandingTargets()
	if err != nil {
		t.Fatal(err)
	}
	if len(contextDef.StandingTargets) > 0 && runID == "" {
		t.Fatal("standing admission fixture requires its bound run identity")
	}
	contextDef.StandingTargets = bindStandingContextFixtureTargets(t, source, contextDef.StandingTargets, runID)
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
