package conformance

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

func managedConformanceExecutionContext(t testing.TB, ctx context.Context, authorityID string) context.Context {
	t.Helper()
	admission, err := managedexecution.New(
		managedexecution.KindNormalRuntime,
		authorityID,
		1,
		"",
		"conformance-actors",
		"bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
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
	db *sql.DB,
	selected *store.PostgresStore,
	rec runtimellm.AgentTurnRecord,
) error {
	t.Helper()
	if rec.Identity.Agent.IsZero() {
		rec.Identity = conformanceAgentMemoryIdentity(t, rec.RunID, rec.AgentID)
		rec.FlowInstance = rec.Identity.FlowInstance()
	}
	eventID := uuid.NewString()
	eventType := events.EventType("conformance.turn.requested")
	event := storetest.InsertExistingRunRootEventRecord(
		t, ctx, db, authoractivityfixture.DialectPostgres, eventID, rec.RunID, eventType,
		eventtest.Producer(events.EventProducerExternal, "conformance"), []byte(`{}`), events.EventEnvelope{}, time.Now().UTC(),
	)
	storetest.PersistManagedAgentTurnFixture(t, ctx, storetest.ManagedAgentTurnFixture{
		Store: selected, Selected: selected, Identity: rec.Identity.Agent,
		RunID: rec.RunID, SessionID: rec.SessionID, TurnID: uuid.NewString(), Memory: rec.Memory,
		EntityID: rec.EntityID, TaskID: rec.TaskID, Event: event, TurnBlocks: rec.TurnBlocks,
		ParseOK: rec.ParseOK, Latency: rec.Latency, CreatedAt: time.Now().UTC(),
	})
	return nil
}
