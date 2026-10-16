package runtimepersistence

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func newMockFirstClaimProfileFixture(t *testing.T, backend string) agentClaimProfileFixture {
	t.Helper()
	root := t.TempDir()
	for path, content := range map[string]string{
		"schema.yaml":            "name: mock-profile-root\nstages:\n  active: {}\n  done: {final: true}\n",
		"global/schema.yaml":     "name: global\nstages:\n  active: {}\n  done: {final: true}\n",
		"global/agents.yaml":     "profile-worker:\n  role: worker\n  intent: {inline: 'Return the compiled response.'}\n  model: regular\n  mock: {kind: python, module: mocks/worker.py}\n",
		"global/mocks/worker.py": "def handle(input):\n    return {'text': 'compiled-profile-response'}\n",
	} {
		writeStateOnlyAcquisitionFixtureFile(t, filepath.Join(root, path), content)
	}
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(pipeline.WorkflowRepoRoot(), root, contracts.DefaultPlatformSpecFile(pipeline.WorkflowRepoRoot()))
	if err != nil {
		t.Fatal(err)
	}
	declarations := semanticview.AgentDeclarations(semanticview.Wrap(bundle))
	if len(declarations) != 1 || !declarations[0].Entry.Mock.Configured() {
		t.Fatalf("compiled mock declaration missing: %+v", declarations)
	}
	ctx, store := newAgentFixtureAuthorityStore(t, backend)
	fact := sourceartifactfixture.RequireArtifact(t, ctx, store, bundle.SourceArtifact)
	ctx = correlation.WithRunID(correlation.WithSourceArtifactFact(ctx, fact), uuid.NewString())
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, fact.BundleHash()))
	identity := mustTestAgentIdentityForRun(correlation.RunIDFromContext(ctx), "profile-worker", "global")
	record := agentFixtureStaticRecord(t, identity)
	record.Config.Mock, record.Config.LLMBackend = declarations[0].Entry.Mock, selection.BackendAnthropic
	live, err := selection.ResolveLiveBackend(selection.BackendAnthropic)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := llm.ResolveAgentExecution(executionposture.MockOnly, live, selection.EffectiveModelAliases(nil), record.Config)
	if err != nil {
		t.Fatal(err)
	}
	record.Config = resolved.Actor
	if err := agentfixture.UpsertStaticForSource(t, ctx, store, record, fact); err != nil {
		t.Fatal(err)
	}
	authority := admitAgentClaimProfileAuthority(t, ctx, store, fact)
	return agentClaimProfileFixture{ctx: ctx, store: store, record: record, authority: authority, source: fact, route: events.DeliveryRoute{
		Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity,
		Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "global", FlowInstance: identity.FlowInstance()}),
	}}
}

func TestAgentFirstClaimProfileCapturesCompiledMockBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newMockFirstClaimProfileFixture(t, backend)
			before := f.record.Config
			event, claimed, profile := f.claim(t)
			if claimed.Claim.Version() != 1 || profile.ProfileID != selection.BackendMock ||
				!profile.MatchesFirstAdmission(event, f.route, f.authority) || !reflect.DeepEqual(f.record.Config, before) {
				t.Fatalf("first mock capture lost immutable identity: %+v", profile)
			}
			state, found, err := f.store.LoadAgentLifecycleState(f.ctx, f.route.AgentIdentity)
			if err != nil || !found || profile.ConfigurationRevision != state.ConfigRevision ||
				profile.AgentGeneration != state.Generation || profile.AgentRuntimeEpoch != state.RuntimeEpoch {
				t.Fatalf("capture differs from issued lifecycle: profile=%+v state=%+v found=%t error=%v", profile, state, found, err)
			}
			assertMockFirstProfileSurvivesReplacementAndReclaim(t, f, event, claimed, profile)
		})
	}
}

func assertMockFirstProfileSurvivesReplacementAndReclaim(t *testing.T, f agentClaimProfileFixture, event events.Event, first deliverylifecycle.ClaimedObligation, original deliverylifecycle.AgentClaimProfile) {
	t.Helper()
	replacement := f.record
	replacement.Config.MaxTurnsPerTask++
	if err := agentfixture.UpsertStaticForSource(t, f.ctx, f.store, replacement, f.source); err != nil {
		t.Fatal(err)
	}
	f.authority = admitAgentClaimProfileAuthority(t, f.ctx, f.store, f.source)
	state, found, err := f.store.LoadAgentLifecycleState(f.ctx, f.route.AgentIdentity)
	if err != nil || !found || state.ConfigRevision == original.ConfigurationRevision {
		t.Fatalf("mock replacement failed to change its admitted revision: %+v found=%t error=%v", state, found, err)
	}
	if err := ExpireExactDeliveryClaimFaultForTest(f.ctx, f.store, first.Claim); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ActivateDeliveryAuthority(f.ctx, f.authority); err != nil {
		t.Fatal(err)
	}
	result, err := f.store.ClaimDelivery(f.ctx, f.authority, event, f.route)
	next, acquired := result.Acquired()
	if err != nil || !acquired || next.Claim.Version() != 2 {
		t.Fatalf("mock successor reclaim: disposition=%s error=%v", result.Disposition, err)
	}
	profile, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, next.Snapshot)
	if err != nil || profile != original {
		t.Fatalf("mock replacement/reclaim rewrote original receipt: %+v error=%v", profile, err)
	}
}

func TestAgentFirstClaimProfileRefusesMalformedMockWithoutAttemptBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for name, corrupt := range map[string]func(*actors.AgentConfig){
				"mode":      func(a *actors.AgentConfig) { a.ExecutionMode = executionposture.Live.RootMode() },
				"provider":  func(a *actors.AgentConfig) { a.ResolvedLLMProvider = "foreign" },
				"transport": func(a *actors.AgentConfig) { a.ResolvedLLMTransport = "foreign" },
				"model":     func(a *actors.AgentConfig) { a.ResolvedModel = "" },
				"artifact":  func(a *actors.AgentConfig) { a.Mock.Source = nil },
				"pin":       func(a *actors.AgentConfig) { a.LLMBackend = selection.BackendMock },
			} {
				t.Run(name, func(t *testing.T) {
					assertMalformedMockFirstClaim(t, backend, corrupt)
				})
			}
		})
	}
}

func assertMalformedMockFirstClaim(t *testing.T, backend string, corrupt func(*actors.AgentConfig)) {
	t.Helper()
	f := newMockFirstClaimProfileFixture(t, backend)
	bad := f.record
	corrupt(&bad.Config)
	beforeAdmission := profileFixtureSnapshot(t, f)
	if admissionErr := agentfixture.UpsertStaticForSource(t, f.ctx, f.store, bad, f.source); admissionErr != nil {
		after := profileFixtureSnapshot(t, f)
		for _, table := range []string{"event_deliveries", "event_delivery_attempts"} {
			original, beforeOK := beforeAdmission[table]
			current, afterOK := after[table]
			if !beforeOK || !afterOK || !reflect.DeepEqual(original, current) {
				t.Fatalf("refused descriptor admission leaked or omitted %s", table)
			}
		}
		t.Logf("malformed descriptor refused before claim: %v", admissionErr)
		return
	}
	f.authority = admitAgentClaimProfileAuthority(t, f.ctx, f.store, f.source)
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "test.profile_claim", "operator", "", []byte(`{}`), 0,
		f.route.AgentIdentity.RunID, events.EventEnvelope{}, time.Now().UTC())
	if err := commitSemanticEventFixtureWithRoutes(f.ctx, f.store, event, []events.DeliveryRoute{f.route}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ActivateDeliveryAuthority(f.ctx, f.authority); err != nil {
		t.Fatal(err)
	}
	before := profileFixtureSnapshot(t, f)
	result, err := f.store.ClaimDelivery(f.ctx, f.authority, event, f.route)
	_, acquired := result.Acquired()
	if err == nil || acquired || !reflect.DeepEqual(before, profileFixtureSnapshot(t, f)) {
		t.Fatalf("malformed first claim accepted or leaked attempt/profile: disposition=%s error=%v", result.Disposition, err)
	}
}
