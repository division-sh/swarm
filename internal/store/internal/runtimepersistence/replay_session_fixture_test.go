package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

func TestReplayDeliverySessionFixturePreservesExactUnleasedAssociationBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			event := runForkSnapshotFixtureEvent(t, ctx, fixture.store)
			identity := agentidentitytest.RuntimeForRun(t, event.RunID(), "session-witness", "replay-session-test", "trace", "session-witness", "trace/session-witness")
			if err := agentfixture.UpsertStatic(t, ctx, fixture.store.(agentfixture.Store), runtimemanager.PersistedAgent{
				Config: withRuntimePersistenceTestIntent(t, runtimeactors.AgentConfig{
					Identity: identity, ID: identity.AgentID(), Role: "operator-replay-test", Type: "stub",
					Model: "regular", ExecutionMode: "live", ResolvedLLMBackend: "anthropic",
					FlowPath: identity.FlowInstance(), Config: []byte(`{}`),
				}),
				Status: "active", StartedAt: time.Now().UTC(),
			}); err != nil {
				t.Fatal(err)
			}
			sessionID := uuid.NewString()
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if err := SeedReplayDeliverySessionForTest(ctx, fixture.store, sessionID, identity); err != nil {
				t.Fatal(err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.WriteCommits != 1 || counts.Total.Revision.Finalizations != 0 || counts.Active != 0 {
				t.Fatalf("session seed escaped the original unrevisioned writer: %+v", counts)
			}
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := DecodeSelectedForkSnapshotRowsForTest(before["agent_sessions"])
			if err != nil || len(rows) != 1 {
				t.Fatalf("session storage rows=%d: %v", len(rows), err)
			}
			fields, err := identity.StorageFields()
			if err != nil {
				t.Fatal(err)
			}
			for column, want := range map[string]string{
				"session_id": sessionID, "run_id": fields.RunID, "agent_id": fields.AgentID,
				"agent_name_owner": fields.NameOwner, "agent_name_source": fields.NameSource,
				"agent_route_presence": fields.RoutePresence, "flow_scope_key": fields.FlowScopeKey,
				"flow_instance_id": fields.FlowInstanceID, "flow_instance": fields.FlowInstancePath,
			} {
				cell, found := rows[0][column]
				got, err := cell.Text()
				if !found || err != nil || got != want {
					t.Fatalf("session association %s=%q/%v, want %q", column, got, err, want)
				}
			}
			for _, column := range []string{"lease_holder", "lease_grant_id", "lease_expires_at"} {
				cell, found := rows[0][column]
				if !found || cell.Type != "<nil>" || string(cell.Value) != "null" {
					t.Fatalf("storage fixture manufactured live authority: %s=%+v", column, cell)
				}
			}
			if err := SeedReplayDeliverySessionForTest(ctx, fixture.store, sessionID, identity); err == nil {
				t.Fatal("duplicate historical session succeeded")
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if err := SeedReplayDeliverySessionForTest(cancelled, fixture.store, uuid.NewString(), identity); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled seed did not refuse: %v", err)
			}
			if err := SeedReplayDeliverySessionForTest(ctx, fixture.store, "invalid", identity); err == nil {
				t.Fatal("invalid session identity accepted")
			}
			if err := SeedReplayDeliverySessionForTest(ctx, fixture.store, uuid.NewString(), agentidentity.Identity{}); err == nil {
				t.Fatal("invalid agent identity accepted")
			}
			if err := SeedReplayDeliverySessionForTest(ctx, struct{}{}, uuid.NewString(), identity); err == nil {
				t.Fatal("foreign owner accepted")
			}
			after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("refused seeds changed storage: %v", err)
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if err := SeedReplayDeliverySessionForTest(ctx, fixture.store, uuid.NewString(), identity); err == nil {
				t.Fatal("closed owner accepted")
			}
		})
	}
}
