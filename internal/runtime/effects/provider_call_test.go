package effects

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/google/uuid"
)

func proveProviderCallJournalBinding(t *testing.T, ctx context.Context, req AuthorizeRequest) {
	t.Helper()
	authority, ok := AuthorityFromContext(ctx)
	if !ok {
		t.Fatal("missing managed authority")
	}
	contract := []byte(`{"RuntimeMode":"api","Provider":"anthropic","Transport":"api","NativeTools":{"Capabilities":{"Bash":false,"WebSearch":false,"FileIO":false}}}`)
	old := req.CapabilitySurface
	surface, err := managedcapabilities.New(managedcapabilities.Plan{ActorIdentity: old.ActorIdentity, RuntimeMode: old.RuntimeMode,
		Provider: old.Provider, Transport: old.Transport, ProviderContract: Fingerprint(contract), Authority: old.Authority, CreatedAt: old.CreatedAt})
	if err != nil {
		t.Fatal(err)
	}
	event, ok := correlation.InboundEventFromContext(ctx)
	if !ok {
		t.Fatal("missing exact causal event")
	}
	frame := managedFrameBindingFrame(t, authority, surface, event, req.AgentFrame.Session.SourceArtifact.Hash)
	req.AgentFrame, req.CapabilitySurface = &frame, &surface
	req.ProviderCall = &ProviderCall{SessionID: authority.Target.SessionID, Memory: authority.Target.Memory,
		Messages: []byte(`[{"Role":"user","Content":"continue"}]`), ProviderContract: contract,
		Request: ProviderCallRequest{Method: "POST", Destination: "https://api.anthropic.com/v1/messages",
			Headers: map[string]string{"content-type": "application/json", "anthropic-version": "2023-06-01"}, Body: []byte(`{"model":"binding-model","messages":[]}`)}}
	req.RequestFingerprint, err = req.ProviderCall.Request.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	handle, err := BeginManagedProviderCall(managedcapabilities.WithContext(ctx, surface), req.Adapter, frame, *req.ProviderCall)
	if err != nil || !handle.Attempt().AuthorizationAcknowledged {
		t.Fatalf("exact frozen provider input was not acknowledged: %v", err)
	}
	raw, err := EncodeAttemptAuthorityEvidence(authority, req)
	if err != nil {
		t.Fatal(err)
	}
	var evidence struct {
		Journal ProviderCallJournal `json:"provider_call_journal"`
	}
	if err := json.Unmarshal(raw, &evidence); err != nil {
		t.Fatal(err)
	}
	if err := evidence.Journal.Validate(authority, req); err != nil || !evidence.Journal.ReadOnly(surface) {
		t.Fatalf("exact request journal refused: %v", err)
	}
	operationEvidence, _ := json.Marshal(authority.Evidence())
	if bytes.Contains(operationEvidence, []byte("provider_call_journal")) {
		t.Fatal("request journal changed operation authority")
	}
	for _, scenario := range []string{"body", "destination", "version_header", "session", "frame", "surface", "attempt", "source", "version"} {
		t.Run("journal_"+scenario, func(t *testing.T) {
			var copy struct {
				Journal ProviderCallJournal `json:"provider_call_journal"`
			}
			if err := json.Unmarshal(raw, &copy); err != nil {
				t.Fatal(err)
			}
			j := &copy.Journal
			switch scenario {
			case "body":
				j.Call.Request.Body = []byte(`{"model":"other"}`)
			case "destination":
				j.Call.Request.Destination += "/other"
			case "version_header":
				j.Call.Request.Headers["anthropic-version"] = "other"
			case "session":
				j.Call.SessionID = uuid.NewString()
			case "frame":
				j.FrameID = "other"
			case "surface":
				j.SurfaceID = uuid.NewString()
			case "attempt":
				j.AttemptID = uuid.NewString()
			case "source":
				j.SourceHash = "other"
			case "version":
				j.Version = "other"
			}
			if j.Validate(authority, req) == nil {
				t.Fatal("foreign request journal admitted")
			}
		})
	}
	for _, destination := range []string{"https://secret@example.org/v1/messages", "https://example.org/v1?api_key=secret"} {
		request := req.ProviderCall.Request
		request.Destination = destination
		if _, err := request.Fingerprint(); err == nil {
			t.Fatal("credential-bearing destination admitted")
		}
	}
}
