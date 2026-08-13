package runtimepersistence

import (
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	agentfixture "github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type lifecycleSourceSetRebindStore interface {
	agentfixture.Store
	runtimemanager.AgentLifecycleStateReader
}

func TestAgentLifecycleSourceSetRebindParity(t *testing.T) {
	t.Run("sqlite", func(t *testing.T) {
		proveAgentLifecycleSourceSetRebind(t, newBootstrappedSQLiteRuntimeStoreForTest(t))
	})
	t.Run("postgres", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		proveAgentLifecycleSourceSetRebind(t, admitTestPostgresStore(t, db))
	})
}

func proveAgentLifecycleSourceSetRebind(t *testing.T, store lifecycleSourceSetRebindStore) {
	t.Helper()
	ctx := testAuthorActivityContext()
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	agentID := "source-set-rebind-agent"
	identity := testAgentIdentity(t, agentID, "global")
	record := runtimemanager.PersistedAgent{
		Config: withRuntimePersistenceTestIntent(t, runtimeactors.AgentConfig{
			ID: agentID, Identity: identity, Role: "worker", Type: "sonnet", Model: "regular", FlowID: "global",
			ExecutionMode: runtimeeffects.ExecutionModeLive, Memory: agentmemory.Authored(true), FlowPath: "global",
			Config: []byte(`{}`),
		}),
		Status: "active", HiredBy: "source-set-rebind-test", StartedAt: now,
	}
	if err := agentfixture.Upsert(ctx, store, record); err != nil {
		t.Fatalf("seed lifecycle cell: %v", err)
	}
	before, exists, err := store.LoadAgentLifecycleState(ctx, identity)
	if err != nil || !exists {
		t.Fatalf("load lifecycle cell before rebind: exists=%v err=%v", exists, err)
	}

	request := runtimemanager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "source_set_rebind", RequestHash: "source-set-rebind-v1",
		Identity: identity, AgentID: agentID, Trigger: "source_set_rebind",
		ExpectedEpoch: before.RuntimeEpoch, ExpectedGeneration: before.Generation, ExpectedPhase: before.Phase,
		TargetEpoch: before.RuntimeEpoch, TargetGeneration: before.Generation, TargetPhase: before.Phase,
		ConfigRevision: before.ConfigRevision, RunMode: before.RunMode, Topology: before.Topology, Now: now.Add(time.Second),
	}
	result, err := agentfixture.Commit(ctx, store, request)
	if err != nil {
		t.Fatalf("commit source-set rebind: %v", err)
	}
	if result.RuntimeEpoch != before.RuntimeEpoch || result.Generation != before.Generation || result.Phase != before.Phase ||
		result.ConfigRevision != before.ConfigRevision || result.RunMode != before.RunMode || !result.Topology.Equal(before.Topology) {
		t.Fatalf("source-set rebind result = %#v, want unchanged lifecycle with exact topology", result)
	}
	replayed, err := agentfixture.Commit(ctx, store, request)
	if err != nil || !replayed.Replayed || replayed.TransitionID != result.TransitionID {
		t.Fatalf("exact source-set rebind replay = %#v err=%v, want transition %s", replayed, err, result.TransitionID)
	}
	changed := request
	changed.RequestHash = "source-set-rebind-conflict"
	if _, err := agentfixture.Commit(ctx, store, changed); err == nil {
		t.Fatal("changed source-set rebind duplicate was accepted")
	} else {
		var failure *runtimefailures.Error
		if !errors.As(err, &failure) || failure.Failure.Class != runtimefailures.ClassConflictingDuplicate {
			t.Fatalf("changed source-set rebind duplicate error = %v, want conflicting duplicate", err)
		}
	}
	after, exists, err := store.LoadAgentLifecycleState(ctx, identity)
	if err != nil || !exists {
		t.Fatalf("load lifecycle cell after rebind: exists=%v err=%v", exists, err)
	}
	if after.RuntimeEpoch != before.RuntimeEpoch || after.Generation != before.Generation || after.Phase != before.Phase ||
		after.ConfigRevision != before.ConfigRevision || after.RunMode != before.RunMode || !after.Topology.Equal(before.Topology) {
		t.Fatalf("lifecycle readback after source-set rebind = %#v, want %#v", after, before)
	}
}
