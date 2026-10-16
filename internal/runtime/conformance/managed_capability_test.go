package conformance

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

func managedConformanceExecutionContextForBundle(t testing.TB, ctx context.Context, authorityID string, fact runtimecorrelation.SourceArtifactFact) context.Context {
	t.Helper()
	admission, err := managedexecution.New(
		managedexecution.KindNormalRuntime,
		authorityID,
		1,
		"",
		"conformance-actors",
		fact.BundleHash(),
		nil,
	)
	if err != nil {
		t.Fatalf("build conformance managed execution admission: %v", err)
	}
	return managedexecution.WithAdmission(ctx, admission)
}

// persistConformanceAgentTurnReadbackFixture seeds the immutable projection
// exercised by conformance readers through the production completion owner.
func persistConformanceAgentTurnReadbackFixture(
	t testing.TB,
	ctx context.Context,
	selected *store.PostgresStore,
	rec runtimellm.AgentTurnRecord,
) error {
	t.Helper()
	if rec.Identity.IsZero() {
		rec.Identity = conformanceAgentMemoryIdentity(t, rec.RunID, rec.AgentID)
		rec.FlowInstance = rec.Identity.FlowInstance()
	}
	eventID := uuid.NewString()
	eventType := events.EventType("conformance.turn.requested")
	event := eventtest.ExistingRunRootIngressWithRoutingSource(
		eventID, eventType, "conformance", "", []byte(`{}`), 0, rec.RunID,
		events.EventEnvelope{}, events.NoRoutingSource(), time.Now().UTC(),
	)
	probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
	storetest.CommitSemanticEvent(t, ctx, selected, event)
	if counts := probe.Snapshot(); counts.Total.WriteCommits == 0 || counts.Active != 0 {
		t.Fatalf("conversation trigger publication escaped original selected coordinator: %+v", counts)
	}
	storetest.PersistManagedAgentTurnFixture(t, ctx, storetest.ManagedAgentTurnFixture{
		Store: selected, Selected: selected, Identity: rec.Identity,
		RunID: rec.RunID, SessionID: rec.SessionID, TurnID: uuid.NewString(), Memory: rec.Memory,
		EntityID: rec.EntityID, TaskID: rec.TaskID, Event: event, TurnBlocks: rec.TurnBlocks,
		ParseOK: rec.ParseOK, Latency: rec.Latency, CreatedAt: time.Now().UTC(),
	})
	return nil
}

func admitConformancePreparedAgentExecution(t testing.TB, ctx context.Context, selected *store.PostgresStore) context.Context {
	t.Helper()
	fact, found := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !found {
		t.Fatal("native conformance execution requires its original source")
	}
	authority, err := agentfixture.AdmitExecutionAuthority(t, ctx, selected, fact)
	if err != nil {
		t.Fatal(err)
	}
	capability, err := agentfixture.ProcessCapability(t, ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	plan, found, err := capability.CurrentSourceSet(ctx)
	if err != nil || !found {
		t.Fatalf("conformance native source plan: found=%t error=%v", found, err)
	}
	admission, err := managedexecution.New(managedexecution.KindNormalRuntime, authority.ExecutionID(), authority.Generation(), "", plan.Revision, fact.BundleHash(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return managedexecution.WithAdmission(ctx, admission)
}
