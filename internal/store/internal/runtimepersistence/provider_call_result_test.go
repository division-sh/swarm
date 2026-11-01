package runtimepersistence

import (
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/google/uuid"
)

func proveProviderCallResultJournal(t *testing.T, fixture completionSettlementFixture) {
	t.Helper()
	const adapter = "anthropic_api"
	authority := fixture.authority
	authority.BudgetScopes = nil
	ctx := withManagedCompletionTestSurface(t, fixture.contextFor(authority), authority, adapter)
	old, _ := managedcapabilities.FromContext(ctx)
	profile, err := llmselection.ResolveActiveBackend(llmselection.BackendAnthropic)
	if err != nil {
		t.Fatal(err)
	}
	contract, err := runtimellm.ProviderContractForProfile(profile)
	if err != nil {
		t.Fatal(err)
	}
	rawContract, _ := json.Marshal(contract)
	surface, err := managedcapabilities.New(managedcapabilities.Plan{ActorIdentity: old.ActorIdentity,
		RuntimeMode: old.RuntimeMode, Provider: old.Provider, Transport: old.Transport,
		ProviderContract: runtimeeffects.Fingerprint(rawContract), Authority: old.Authority, CreatedAt: old.CreatedAt})
	if err != nil {
		t.Fatal(err)
	}
	ctx = managedcapabilities.WithContext(ctx, surface)
	event := managedCompletionTestEvent(authority)
	frame := managedCompletionTestFrameWithSurface(t, authority, event,
		managedCompletionTestFrame(t, authority, adapter).Session.SourceArtifact.Hash, surface)
	call := runtimeeffects.ProviderCall{SessionID: authority.Target.SessionID, Memory: authority.Target.Memory,
		Messages: []byte(`[{"Role":"user","Content":"continue"}]`), ProviderContract: rawContract,
		Request: runtimeeffects.ProviderCallRequest{Method: "POST", Destination: "https://api.anthropic.com/v1/messages",
			Headers: map[string]string{"content-type": "application/json", "anthropic-version": "2023-06-01"}, Body: []byte(`{"model":"test"}`)}}
	handle, err := runtimeeffects.BeginManagedProviderCall(ctx, adapter, frame, call)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.MarkLaunched(ctx); err != nil {
		t.Fatal(err)
	}
	query := `SELECT CAST(authority_evidence AS TEXT) FROM runtime_external_effect_attempts WHERE attempt_id=$1`
	var original string
	if err := fixture.db.QueryRow(query, handle.Attempt().AttemptID).Scan(&original); err != nil {
		t.Fatal(err)
	}
	write := func(raw []byte) {
		t.Helper()
		if _, err := fixture.db.Exec(`UPDATE runtime_external_effect_attempts SET authority_evidence=$1 WHERE attempt_id=$2`, string(raw), handle.Attempt().AttemptID); err != nil {
			t.Fatal(err)
		}
	}
	for _, shape := range []string{"missing", "malformed", "version", "operation", "attempt", "body", "header", "foreign_authority"} {
		t.Run("result_journal_"+shape, func(t *testing.T) {
			var evidence map[string]json.RawMessage
			if err := json.Unmarshal([]byte(original), &evidence); err != nil {
				t.Fatal(err)
			}
			var journal runtimeeffects.ProviderCallJournal
			if err := json.Unmarshal(evidence["provider_call_journal"], &journal); err != nil {
				t.Fatal(err)
			}
			switch shape {
			case "version":
				journal.Version = "unknown"
			case "operation":
				journal.OperationID = uuid.NewString()
			case "attempt":
				journal.AttemptID = uuid.NewString()
			case "body":
				journal.Call.Request.Body = []byte(`{"model":"foreign"}`)
			case "header":
				journal.Call.Request.Headers["anthropic-version"] = "foreign"
			case "foreign_authority":
				evidence["foreign_authority"] = json.RawMessage(`true`)
			}
			evidence["provider_call_journal"], _ = json.Marshal(journal)
			if shape == "missing" {
				delete(evidence, "provider_call_journal")
			} else if shape == "malformed" {
				evidence["provider_call_journal"] = json.RawMessage(`"not a request journal"`)
			}
			raw, _ := json.Marshal(evidence)
			write(raw)
			before := historicalContextDatabaseRows(t, fixture.db, !fixture.sqlite)
			if err := handle.MarkResponseObserved(ctx, map[string]any{"response_fingerprint": "exact-result"}); err == nil {
				t.Fatal("result accepted a missing, foreign or contradictory journal/authority")
			}
			historicalContextRequireUnchanged(t, before, historicalContextDatabaseRows(t, fixture.db, !fixture.sqlite))
			write([]byte(original))
		})
	}
	if err := handle.MarkResponseObserved(ctx, map[string]any{"response_fingerprint": "exact-result"}); err != nil {
		t.Fatalf("exact launched result with its immutable journal refused: %v", err)
	}
}
