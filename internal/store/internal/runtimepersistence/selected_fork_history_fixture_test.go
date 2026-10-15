package runtimepersistence

import (
	"context"
	"encoding/json"
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

func TestSelectedForkHistoricalSourceRowsKeepSeparateNativeCommitsAndClocksBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			event := runForkSnapshotFixtureEvent(t, ctx, fixture.store)
			identity := agentidentitytest.RuntimeForRun(t, event.RunID(), "session-witness", "selected-fork-history", "trace", "session-witness", "trace/session-witness")
			at := time.Unix(1700002305, 0).UTC()
			if err := agentfixture.UpsertStatic(t, ctx, fixture.store.(agentfixture.Store), runtimemanager.PersistedAgent{
				Config: withRuntimePersistenceTestIntent(t, runtimeactors.AgentConfig{
					Identity: identity, ID: identity.AgentID(), Role: "operator-replay-test", Type: "stub",
					Model: "regular", ExecutionMode: "live", ResolvedLLMBackend: "anthropic",
					FlowPath: identity.FlowInstance(), Config: []byte(`{}`),
				}), Status: "active", StartedAt: at,
			}); err != nil {
				t.Fatal(err)
			}
			sessionID, auditID := uuid.NewString(), uuid.NewString()
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if err := SeedSelectedForkSourceSessionForTest(ctx, fixture.store, sessionID, identity, at); err != nil {
				t.Fatal(err)
			}
			if err := SeedSelectedForkSourceConversationAuditForTest(ctx, fixture.store, auditID, event.RunID(), identity, at.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 2 || counts.Total.WriteCommits != 2 || counts.Total.ReadCommits != 0 || counts.Total.Revision.Finalizations != 0 || counts.Active != 0 {
				t.Fatalf("history writes merged their cuts or escaped original selected writer: %+v", counts)
			}
			restore()
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
			if err != nil {
				t.Fatal(err)
			}
			assertSelectedForkHistoricalRow(t, before["agent_sessions"], sessionID, at, true)
			assertSelectedForkHistoricalRow(t, before["agent_conversation_audits"], auditID, at.Add(time.Minute), false)
			assertSelectedForkHistoryUnionCounts(t, ctx, fixture.store, event.RunID(), sessionID, auditID)
			assertSelectedForkHistoryRefusals(t, ctx, fixture.store, sessionID, identity, at, before)
		})
	}
}

func assertSelectedForkHistoryRefusals(t *testing.T, ctx context.Context, owner any, sessionID string, identity agentidentity.Identity, at time.Time, before map[string]SelectedForkStorageTableSnapshot) {
	t.Helper()
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := SeedSelectedForkSourceSessionForTest(cancelled, owner, uuid.NewString(), identity, at); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled session write=%v", err)
	}
	if err := SeedSelectedForkSourceConversationAuditForTest(cancelled, owner, uuid.NewString(), identity.RunID, identity, at); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled audit write=%v", err)
	}
	if err := SeedSelectedForkSourceSessionForTest(ctx, owner, sessionID, identity, at); err == nil {
		t.Fatal("duplicate history was silently accepted")
	}
	after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, owner)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("refused writes or union reads changed historical rows/clocks: %v", err)
	}
}

func assertSelectedForkHistoricalRow(t *testing.T, table SelectedForkStorageTableSnapshot, id string, at time.Time, memory bool) {
	t.Helper()
	rows, err := DecodeSelectedForkSnapshotRowsForTest(table)
	if err != nil || len(rows) != 1 {
		t.Fatalf("history row inventory=%d/%v", len(rows), err)
	}
	row := rows[0]
	if got, err := row["session_id"].Text(); err != nil || got != id {
		t.Fatalf("historical identity=%q/%v", got, err)
	}
	if got, err := row["status"].Text(); err != nil || got != "active" {
		t.Fatalf("historical status=%q/%v", got, err)
	}
	for _, column := range []string{"created_at", "updated_at"} {
		cell := row[column]
		var stored time.Time
		if cell.Type == "time.Time" {
			err = json.Unmarshal(cell.Value, &stored)
		} else {
			var text string
			text, err = cell.Text()
			if err == nil {
				stored, _, err = parseSQLiteTimeString(text)
			}
		}
		if err != nil || !stored.Equal(at) {
			t.Fatalf("historical %s changed exact clock: %s/%v, want %s", column, stored, err, at)
		}
	}
	want := "false"
	if memory {
		want = "true"
	}
	cell := row["memory_enabled"]
	if cell.Type == "int64" {
		want = "0"
		if memory {
			want = "1"
		}
	}
	if string(cell.Value) != want {
		t.Fatalf("historical memory posture changed: %+v, want %s", cell, want)
	}
	for _, column := range []string{"lease_holder", "lease_grant_id", "lease_expires_at"} {
		if cell, found := row[column]; found && (cell.Type != "<nil>" || string(cell.Value) != "null") {
			t.Fatalf("historical fixture manufactured live authority: %s=%+v", column, cell)
		}
	}
}

func assertSelectedForkHistoryUnionCounts(t *testing.T, ctx context.Context, owner any, runID, sessionID, auditID string) {
	t.Helper()
	for _, ids := range []struct {
		run, session, audit string
		want                int
	}{
		{uuid.NewString(), sessionID, auditID, 1},
		{runID, uuid.NewString(), uuid.NewString(), 1},
		{uuid.NewString(), uuid.NewString(), uuid.NewString(), 0},
	} {
		if got, err := CountForkOrSourceSessionStorageForTest(ctx, owner, ids.run, ids.session); err != nil || got != ids.want {
			t.Fatalf("session union was narrowed or widened: %d/%v, want%d", got, err, ids.want)
		}
		if got, err := CountForkOrSourceConversationAuditStorageForTest(ctx, owner, ids.run, ids.audit); err != nil || got != ids.want {
			t.Fatalf("audit union was narrowed or widened: %d/%v, want%d", got, err, ids.want)
		}
	}
}
