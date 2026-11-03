package runtimepersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

func proveContextCompactionCheckpointCustody(t *testing.T, fixture completionSettlementFixture) {
	t.Helper()
	const adapter = "anthropic_api"
	original := runtimeeffects.WithLifecycleToken(fixture.context, fixture.authority.Normal)
	identity := fixture.authority.Target.AgentIdentity
	lease := &sessions.Lease{SessionID: fixture.sessionID, Identity: identity, GrantID: fixture.grantID, LockOwner: fixture.leaseHolder, ExpiresAt: time.Now().Add(time.Minute)}
	record := runtimellm.ConversationRecord{SessionID: fixture.sessionID, AgentID: fixture.agentID, Identity: identity, Memory: fixture.authority.Target.Memory,
		RunID: identity.RunID, Status: "active", Messages: []runtimellm.Message{{Role: "user", Content: "exact old fact"}, {Role: "user", Content: "retained tail"}}}
	conversations := fixture.store.(runtimellm.ConversationPersistence)
	if err := conversations.UpsertConversation(original, lease, record); err != nil {
		t.Fatal(err)
	}
	ctx := withManagedCompletionTestSurface(t, original, fixture.authority, adapter)
	frame := managedCompletionTestFrame(t, fixture.authority, adapter)
	if source, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx); ok {
		frame = managedCompletionTestFrameForSource(t, fixture.authority, adapter, managedCompletionTestEvent(fixture.authority), source.BundleHash())
	}
	bound, err := runtimeeffects.BindCompletionRequest(fixture.authority, runtimeeffects.AuthorizeRequest{AgentFrame: &frame})
	if err != nil {
		t.Fatal(err)
	}
	ctx = runtimeeffects.WithCompletionRequest(ctx, bound.CompletionRequest)
	const payload = `{"messages":[{"role":"user","content":"summarize"}]}`
	checkpoint := runtimellm.ContextCompactionCheckpoint{Version: 1, SessionID: fixture.sessionID, Method: "platform_summary", Route: "exact-route", Model: "claude-test", CoveredMessages: 1, FirstKeptMessage: 1, OperationID: uuid.NewString()}
	checkpoint.CanonicalFingerprint, _ = canonicaljson.Hash(record.Messages[:1])
	checkpoint.SystemFingerprint, _ = canonicaljson.Hash("exact system")
	checkpoint.ToolsFingerprint, _ = canonicaljson.Hash([]string{"exact tool"})
	prepared := runtimellm.ContextCompactionPreparation{Checkpoint: checkpoint, RequestFingerprint: runtimeeffects.Fingerprint([]byte(payload))}
	prepared.ConversationFingerprint, _ = canonicaljson.Hash(record.Messages)
	owner := fixture.store.(runtimellm.ContextCompactionPersistence)
	if _, found, err := owner.LoadContextCompaction(ctx, identity, prepared); err != nil || found {
		t.Fatalf("unexecuted maintenance read: found=%t err=%v", found, err)
	}
	handle, err := runtimeeffects.BeginContextCompaction(ctx, adapter, checkpoint.OperationID, frame, []byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.MarkLaunched(ctx); err != nil {
		t.Fatal(err)
	}
	if err := handle.MarkResponseObserved(ctx, map[string]any{"response_fingerprint": "exact summary response"}); err != nil {
		t.Fatal(err)
	}
	checkpoint.AttemptID, checkpoint.ResultFingerprint, checkpoint.Summary = handle.Attempt().AttemptID, "exact summary response", "The old fact remains exact."
	checkpoint.ID, err = checkpoint.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	result := runtimellm.ContextCompactionResult{Preparation: prepared, Checkpoint: checkpoint}
	settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, adapter, "", "")
	settlement.AgentTurn, settlement.ProviderHead = nil, nil
	settlement.MaintenanceSurfaceID = frame.Turn.Capability.SurfaceID
	settlement.Usage.Evidence.Request = handle.Attempt().Authority.CompletionRequest
	settlement.Spend.InvocationType = runtimeeffects.ContextCompactionPurpose
	settlement.Settlement.Evidence = map[string]any{"context_compaction": result}
	ack, err := handle.SettleCompletion(ctx, settlement)
	if err != nil || !ack.Committed {
		t.Fatalf("maintenance settlement: %+v %v", ack, err)
	}
	loaded, found, err := owner.LoadContextCompaction(ctx, identity, prepared)
	if err != nil || !found || loaded.Checkpoint.ID != checkpoint.ID {
		t.Fatalf("acknowledged result retry: found=%t err=%v", found, err)
	}
	before := snapshotRotationTableRows(t, fixture.original, fixture.db, `SELECT * FROM agent_sessions WHERE session_id=$1`, fixture.sessionID)
	foreign := *lease
	foreign.GrantID = uuid.NewString()
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	for _, cell := range []struct {
		name   string
		ctx    context.Context
		lease  *sessions.Lease
		result runtimellm.ContextCompactionResult
	}{
		{"cancelled", cancelled, lease, loaded},
		{"foreign grant", ctx, &foreign, loaded},
		{"missing original", context.Background(), lease, loaded},
		{"unacknowledged result", ctx, lease, result},
	} {
		t.Run(cell.name, func(t *testing.T) {
			if cell.name == "unacknowledged result" {
				cell.result.Checkpoint.Summary = "invented"
				cell.result.Checkpoint.ID = "invented"
			}
			if err := owner.InstallContextCompaction(cell.ctx, cell.lease, cell.result); err == nil {
				t.Fatal("invalid installation was accepted")
			}
			if after := snapshotRotationTableRows(t, fixture.original, fixture.db, `SELECT * FROM agent_sessions WHERE session_id=$1`, fixture.sessionID); !reflect.DeepEqual(before, after) {
				t.Fatal("refused installation changed session")
			}
		})
	}
	changed := record
	changed.Messages = append(append([]runtimellm.Message(nil), record.Messages...), runtimellm.Message{Role: "user", Content: "concurrent committed input"})
	if err := conversations.UpsertConversation(ctx, lease, changed); err != nil {
		t.Fatal(err)
	}
	if err := owner.InstallContextCompaction(ctx, lease, loaded); err == nil {
		t.Fatal("stale canonical snapshot installed")
	}
	if err := conversations.UpsertConversation(ctx, lease, record); err != nil {
		t.Fatal(err)
	}
	if err := owner.InstallContextCompaction(ctx, lease, loaded); err != nil {
		t.Fatal(err)
	}
	installed := snapshotRotationTableRows(t, fixture.original, fixture.db, `SELECT * FROM agent_sessions WHERE session_id=$1`, fixture.sessionID)
	if err := owner.InstallContextCompaction(ctx, lease, loaded); err != nil {
		t.Fatal(err)
	}
	if after := snapshotRotationTableRows(t, fixture.original, fixture.db, `SELECT * FROM agent_sessions WHERE session_id=$1`, fixture.sessionID); !reflect.DeepEqual(installed, after) {
		t.Fatal("idempotent installation rewrote session")
	}
	if err := owner.InstallContextCompaction(ctx, &foreign, loaded); err == nil {
		t.Fatal("receipt bypassed current grant")
	}
	var turns, spends int
	if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_turns WHERE session_id=$1`, fixture.sessionID).Scan(&turns); err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM spend_ledger WHERE external_effect_attempt_id=$1`, handle.Attempt().AttemptID).Scan(&spends); err != nil {
		t.Fatal(err)
	}
	if turns != 0 || spends != 1 {
		t.Fatalf("maintenance fabricated business completion or duplicate spend: turns=%d spends=%d", turns, spends)
	}
}
