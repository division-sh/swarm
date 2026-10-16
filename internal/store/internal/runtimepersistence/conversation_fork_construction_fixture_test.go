package runtimepersistence

import (
	"context"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/google/uuid"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
)

func conversationForkConstructionBundle(t *testing.T) *contracts.WorkflowContractBundle {
	t.Helper()
	files := canonicalrouting.ConversationForkConstructionDocuments()
	files["schema.yaml"] = canonicalrouting.ConversationForkHistoryRootDocument()
	files["events.yaml"] = "conversation.created:\n  name: text\n"
	return loadLifecyclePersistenceFixtureForTest(t, files)
}

func createConversationForkLifecycleDiagnostic(t *testing.T, ctx context.Context, selected lifecycleDiagnosticTestStore, bundle *contracts.WorkflowContractBundle) diaglog.LifecycleDiagnostic {
	t.Helper()
	identity := testAgentIdentity(t, "diagnostic-worker", "")
	now := time.Now().UTC().Truncate(time.Microsecond)
	requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: identity.RunID, Artifact: bundle.SourceArtifact, BundleHash: bundle.SourceArtifact.BundleHash(), StartedAt: now})
	if err := agentfixture.UpsertStaticForSource(t, ctx, selected, manager.PersistedAgent{
		Config: withRuntimePersistenceTestIntent(t, actors.AgentConfig{ID: identity.AgentID(), Identity: identity, Type: "sonnet", Role: "worker", FlowID: "global", Model: "regular", ExecutionMode: "live", Config: []byte("{}")}),
		Status: "active", HiredBy: "test", StartedAt: now,
	}, mustStoreTestSourceArtifactFact(bundle.SourceArtifact.BundleHash())); err != nil {
		t.Fatal(err)
	}
	rows, err := selected.ListPendingAgentLifecycleDiagnostics(ctx, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Identity.RunID == identity.RunID && row.AgentID == identity.AgentID() {
			return row
		}
	}
	t.Fatal("source-bound lifecycle producer did not enqueue its diagnostic")
	return diaglog.LifecycleDiagnostic{}
}

func prepareNativeConversationForkConstruction(t *testing.T, ctx context.Context, selected any, bundle *contracts.WorkflowContractBundle, fixture ConversationForkSourceFixture) ConversationForkConstruction {
	t.Helper()
	workflows := configureAgentFixtureFlowLifecycle(t, selected.(agentFixtureFlowStore), &sqliteFlowActivationBus{}, bundle)
	planner := manager.NewAgentManagerWithOptions(nil, nil, manager.AgentManagerOptions{
		BaseContext: ctx, SemanticSource: semanticview.Wrap(bundle), SourceArtifactFact: mustStoreTestSourceArtifactFact(bundle.SourceArtifact.BundleHash()),
		WorkflowInstances: workflows, ExecutionPosture: executionposture.Live, ReceiverExecution: eventreceiver.NormalExecution(),
	})
	fixture.Construction.CreatingEvent = eventtest.ExistingRunRootIngress(
		uuid.NewSHA1(uuid.MustParse(fixture.EntityID), []byte("conversation-fork-construction")).String(),
		"flow/conversation.created", "conversation-construction-fixture", "", []byte(`{"name":"Before"}`), 0,
		fixture.RunID, events.EventEnvelope{}, fixture.Turn1At.Add(-30*time.Second))
	plan, err := PrepareConversationForkConstructionForTest(ctx, selected, semanticview.Wrap(bundle), planner, fixture)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}
