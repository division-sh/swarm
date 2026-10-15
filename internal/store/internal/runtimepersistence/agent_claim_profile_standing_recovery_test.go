package runtimepersistence

import (
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestAgentFirstClaimProfileStandingProcessTakeoverBeforeAttemptBothStores(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process-death evidence pipe requires inherited file descriptors")
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			location := filepath.Join(t.TempDir(), "standing-profile-process.db")
			if backend == "postgres" {
				location = testutil.StartPostgresDSN(t)
			}
			previous := killAgentProfileProcessAtClaim(t, backend, location, "SWARM_S6_PROFILE_PROCESS_STANDING=1")
			selected := openAgentProfileProcessStore(t, backend, location)
			next := newStandingAgentProfileProcessFixture(t, selected)
			proveStandingAgentProfileProcessRecovery(t, next, previous)
		})
	}
}

func newStandingAgentProfileProcessFixture(t *testing.T, selected agentFixtureFlowStore) receiverConfigActivationFixture {
	t.Helper()
	// All agent and intent declarations are in the source artifact; reopening
	// must not depend on the receiver fixture's synthetic actor injection.
	bundle := loadLifecyclePersistenceFixtureForTest(t, map[string]string{
		"schema.yaml":                "name: s6-standing-profile\n",
		"review/schema.yaml":         "name: review\nstages:\n  active: {}\n",
		"review/events.yaml":         "task.started:\n",
		"review/agents.yaml":         "reviewer:\n  role: reviewer\n  model: regular\n  intent: prompts/reviewer.md\n  subscriptions: [task.started]\n",
		"review/prompts/reviewer.md": "Review the admitted task.\n",
	})
	fact := mustStoreTestSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	ctx := correlation.WithRunID(storeTestWorkContext(t, testAuthorActivityContextForBundle(fact.BundleHash())), uuid.NewString())
	requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(),
		RunID: correlation.RunIDFromContext(ctx), Artifact: bundle.SourceArtifact, BundleHash: fact.BundleHash()})
	coordinate := agenttopology.SourceCoordinate{BundleHash: fact.BundleHash()}
	process, err := agentfixture.ProcessCapability(t, ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	previous, found, err := process.CurrentSourceSet(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sources := append([]agenttopology.SourceCoordinate(nil), previous.Sources...)
	present := false
	for _, source := range sources {
		if source == coordinate {
			present = true
		}
	}
	if !present {
		sources = append(sources, coordinate)
	}
	plan, err := agenttopology.NewSourceSetPlan(sources, previous.Agents)
	if err != nil {
		t.Fatal(err)
	}
	if found && plan.Revision != previous.Revision {
		if _, err := process.RestoreSourceSet(ctx, agenttopology.SourceSetCommitRequest{
			OperationID: uuid.NewString(), ExpectedRevision: previous.Revision, Plan: plan,
		}); err != nil {
			t.Fatal(err)
		}
	}
	grant, err := agentfixture.AdmitGeneration(t, ctx, selected, plan, coordinate)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := grant.Evidence()
	if err != nil || evidence.State != startupownership.GrantPrepared {
		t.Fatalf("standing boot requires the actual prepared grant: %+v err=%v", evidence, err)
	}
	ctx = correlation.WithRuntimeInstanceID(ctx, evidence.RuntimeInstanceID)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(evidence.RuntimeInstanceID, fact.BundleHash()))
	bus := &sqliteFlowActivationBus{}
	workflows := configureAgentFixtureFlowLifecycle(t, selected, bus, bundle, nil)
	am := ownStoreTestAgentManager(t, manager.NewAgentManagerWithOptions(bus, nil, manager.AgentManagerOptions{
		ExecutionPosture: executionposture.Live, BaseContext: ctx, SourceArtifactFact: fact,
		SemanticSource: semanticview.Wrap(bundle), WorkflowInstances: workflows, LLMBackend: "anthropic",
		DeliveryStore: selected, WorkOwner: storeTestWorkOwner(t), ReceiverExecution: eventreceiver.NormalExecution(),
		PersistenceRoles: manager.PersistenceRoles{LifecycleCensus: selected, LifecycleState: selected, LifecycleOutcome: selected,
			StandingRestarts: selected.(runlifecycle.StandingRestartDispositionReader), AgentRoutes: bus,
			FlowActivation: agentFixtureFlowActivationCommitter{store: selected}, RouteInstaller: bus, RouteVerifier: bus, RouteRestorer: bus},
	}, selected))
	admission, err := agenttopology.StaticAdmission(plan.Revision, fact.BundleHash(), agenttopology.LifetimeDurableManaged)
	if err != nil {
		t.Fatal(err)
	}
	if err := am.InstallStartupTopology(grant, admission, plan); err != nil {
		t.Fatal(err)
	}
	return receiverConfigActivationFixture{ctx: ctx, store: selected, manager: am, workflows: workflows, bus: bus, bundle: bundle, grant: grant}
}

func startStandingAgentProfileProcessFixture(t *testing.T, f receiverConfigActivationFixture) agentClaimProfileFixture {
	t.Helper()
	fact, present := correlation.SourceArtifactFactFromContext(f.ctx)
	if !present {
		t.Fatal("standing source fixture requires its immutable artifact")
	}
	source := semanticview.Wrap(f.bundle)
	serviceID := flowidentity.StandingServiceID("review")
	observed := time.Now().UTC()
	results, finish, err := f.workflows.PrepareStandingTargets(f.ctx, pipeline.StandingTargetMutationRequest{
		ObservedAt: observed, Targets: []pipeline.StandingTargetMutation{{
			Candidate: pipeline.StandingServiceCandidate{BindingEnabled: true, ServiceID: serviceID, FlowPath: "review", Source: fact},
			Activation: pipeline.FlowInstanceActivationRequest{ContractBundle: source,
				Instance: flowidentity.StandingForService(source, "review", serviceID), OccurredAt: observed},
		}},
	}, f.manager)
	if err != nil || len(results) != 1 || finish == nil || !results[0].Reconciliation.RestartDisposition.Executable() {
		t.Fatalf("exact native standing generation: results=%+v finish=%t err=%v", results, finish != nil, err)
	}
	admitReceiverConfigFixtureGrant(t, f.ctx, f.grant)
	if err := f.manager.PrepareAdmittedDynamicFlowAgentsForStart(f.ctx); err != nil {
		t.Fatal(err)
	}
	grant, err := f.grant.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	admission, err := managedexecution.New(managedexecution.KindNormalRuntime, grant.RuntimeInstanceID, grant.RuntimeGeneration,
		"", "s6-standing-profile-source-proof", fact.BundleHash(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.manager.Run(managedexecution.WithAdmission(f.ctx, admission)); err != nil {
		t.Fatal(err)
	}
	if err := finish(); err != nil {
		t.Fatal(err)
	}
	configs := f.manager.ListAgentConfigs()
	if len(configs) != 1 || configs[0].Identity.RunID != results[0].Reconciliation.RunID || configs[0].Identity.FlowInstance() != results[0].Instance.InstancePath {
		t.Fatalf("standing constructor did not publish its exact single actor: %+v", configs)
	}
	authority, err := deliverylifecycle.NewNormalExecutionAuthority(fact, grant.GrantID, grant.RuntimeGeneration)
	if err != nil {
		t.Fatal(err)
	}
	var target events.DeliveryTargetOwnership
	coordinate := events.RouteIdentity{FlowID: results[0].Instance.TemplateID,
		FlowInstance: results[0].Instance.InstancePath, EntityID: results[0].Instance.EntityID}
	if results[0].Instance.EntityID != "" {
		target = events.MustExistingEntityTarget(coordinate)
	} else {
		target = events.MustEntitylessReceiverTarget(coordinate)
	}
	return agentClaimProfileFixture{ctx: correlation.WithRunID(f.ctx, results[0].Reconciliation.RunID), store: f.store, source: fact,
		authority: authority, route: events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(configs[0].ID),
			AgentIdentity: configs[0].Identity, Target: target}}
}

func proveStandingAgentProfileProcessRecovery(t *testing.T, next receiverConfigActivationFixture, previous agentProfileProcessEvidence) {
	t.Helper()
	if next.bundle.SourceArtifact.BundleHash() != previous.Profile.BundleHash {
		t.Fatal("successor standing declaration differs from the captured source")
	}
	process, err := agentfixture.ProcessCapability(t, next.ctx, next.store)
	if err != nil {
		t.Fatal(err)
	}
	current, err := process.Evidence()
	if err != nil || previous.Process.Validate() != nil || current.Validate() != nil || current.AuthorityID == previous.Process.AuthorityID ||
		current.AuthorityGeneration != previous.Process.AuthorityGeneration+1 || current.Backend != previous.Process.Backend || current.BootID == previous.Process.BootID {
		t.Fatalf("standing process possession was not replaced: old=%+v current=%+v err=%v", previous.Process, current, err)
	}
	if err := next.manager.RebindLifecycleExecutionForStartup(next.ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := next.store.Snapshot(next.ctx, previous.Profile.DeliveryID)
	if err != nil || !previous.Profile.MatchesDelivery(snapshot) {
		t.Fatalf("standing recovery lost exact pre-attempt claim: %+v err=%v", snapshot, err)
	}
	f := agentClaimProfileFixture{ctx: correlation.WithRunID(next.ctx, previous.Profile.Identity.RunID), store: next.store,
		source: mustStoreTestSourceArtifactFact(previous.Profile.BundleHash), route: snapshot.Route}
	assertStandingAgentProfileLifecycleRebind(t, next, previous)
	before := profileFixtureSnapshot(t, f)
	if !reflect.DeepEqual(previous.Attempts, before["event_delivery_attempts"]) {
		t.Fatal("standing takeover changed the first attempt before authority activation")
	}
	assertAgentProfileRecoveryCapture(t, f, snapshot, previous.Profile)
	standing, err := next.store.(runlifecycle.StandingRestartDispositionReader).StandingRunRestartDisposition(f.ctx, previous.Profile.Identity.RunID)
	if err != nil || standing.Validate() != nil || previous.Standing == nil || standing != *previous.Standing ||
		standing.ServiceID != flowidentity.StandingServiceID("review") || !standing.Executable() || standing.UsesGenericRecovery() {
		t.Fatalf("standing recovery lost its exact healthy generation: %+v err=%v", standing, err)
	}
	assertAgentProfileProcessNoAttempts(t, f)
	if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
		t.Fatal("standing pre-attempt recovery observations changed application storage")
	}
	if err := next.manager.RebindLifecycleExecutionForStartup(next.ctx); err != nil {
		t.Fatal(err)
	}
	if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
		t.Fatal("exact standing takeover retry changed application storage")
	}
	f = startStandingAgentProfileProcessFixture(t, next)
	if f.route.AgentIdentity != previous.Profile.Identity {
		t.Fatal("same standing generation was replaced instead of rebound")
	}
	assertStandingAgentProfileReadinessRestart(t, next, previous, standing)
	if err := f.store.ActivateDeliveryAuthority(f.ctx, f.authority); err != nil {
		t.Fatal(err)
	}
	assertAgentProfileProcessRecoveredClaim(t, f, previous)
}

func assertStandingAgentProfileLifecycleRebind(t *testing.T, next receiverConfigActivationFixture, previous agentProfileProcessEvidence) {
	t.Helper()
	binding, err := next.grant.ProcessExecutionBinding()
	if err != nil {
		t.Fatal(err)
	}
	state, found, err := next.store.LoadAgentLifecycleState(next.ctx, previous.Profile.Identity)
	if err != nil || !found || state.Generation != previous.State.Generation+1 || state.RuntimeEpoch != previous.State.RuntimeEpoch+1 ||
		state.ConfigRevision != previous.State.ConfigRevision || !state.ProcessBinding.Equal(binding) || state.Phase != previous.State.Phase ||
		state.RunMode != previous.State.RunMode || !state.Topology.Equal(previous.State.Topology) {
		t.Fatalf("standing lifecycle startup rebind: old=%+v current=%+v found=%t err=%v", previous.State, state, found, err)
	}
}

func assertStandingAgentProfileReadinessRestart(t *testing.T, next receiverConfigActivationFixture, previous agentProfileProcessEvidence, standing runlifecycle.StandingRestartDisposition) {
	t.Helper()
	row, found, err := next.workflows.LoadDynamicFlowRuntimeReadiness(next.ctx, standing.RunID, flowidentity.RouteForInstancePath(previous.Profile.Identity.FlowInstance()))
	if err != nil || !found || !row.Eligible() || row.Pending() || row.Phase != pipeline.FlowAttachmentReady {
		t.Fatalf("standing restart did not finalize exact readiness: %+v found=%t err=%v", row, found, err)
	}
	state, found, err := next.store.LoadAgentLifecycleState(next.ctx, previous.Profile.Identity)
	old, current := previous.State.Topology.Authority.Readiness, state.Topology.Authority.Readiness
	if err != nil || !found || old == nil || current == nil || old.AttemptID == current.AttemptID || current.Preparation ||
		state.ConfigRevision != previous.Profile.ConfigurationRevision {
		t.Fatalf("standing new-process readiness did not adopt its exact new attempt: old=%+v current=%+v state=%+v err=%v", old, current, state, err)
	}
	ordinal, err := flowidentity.ParseActivationAttemptID(current.AttemptID)
	if err != nil || ordinal != row.AttemptOrdinal {
		t.Fatalf("standing lifecycle adopted a foreign readiness attempt: row=%+v current=%+v err=%v", row, current, err)
	}
	after, err := next.store.(runlifecycle.StandingRestartDispositionReader).StandingRunRestartDisposition(next.ctx, standing.RunID)
	if err != nil || after != standing {
		t.Fatalf("standing startup replaced the healthy intrinsic generation: old=%+v current=%+v err=%v", standing, after, err)
	}
}
