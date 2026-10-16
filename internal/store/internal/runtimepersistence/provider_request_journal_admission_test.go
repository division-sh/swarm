package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

func TestProviderRequestJournalAdmissionNativeCommitBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, memory := range []bool{false, true} {
			for _, phase := range []string{"healthy", "commit_refused", "commit_ack_lost"} {
				t.Run(backend+"/memory_"+map[bool]string{false: "off", true: "on"}[memory]+"/"+phase, func(t *testing.T) {
					fixture, control := openProviderJournalFixture(t, backend, agentmemory.Plan{Enabled: memory})
					ctx, journal, frame, surface, body := providerJournalAdmissionInputs(t, fixture)
					before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
					if err != nil {
						t.Fatal(err)
					}
					control.phase, control.failure = phase, errors.New("provider journal native commit diagnostic")
					control.statement = func(query string) bool {
						return strings.HasPrefix(strings.Join(strings.Fields(strings.ToUpper(query)), " "), "INSERT INTO RUNTIME_EXTERNAL_EFFECT_ATTEMPTS (")
					}
					control.enabled.Store(true)
					handle, admissionErr := runtimeeffects.BeginJournaledManagedCompletion(ctx, "anthropic_api", body, frame, journal, nil)
					control.enabled.Store(false)
					if control.writes.Load() != 1 {
						t.Fatalf("authorized attempt insert count=%d, want 1: %v", control.writes.Load(), admissionErr)
					}
					after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
					if err != nil {
						t.Fatal(err)
					}
					if phase == "commit_refused" {
						if admissionErr == nil || handle != nil || !reflect.DeepEqual(before, after) {
							t.Fatalf("refused journal admission changed durable rows: %v", admissionErr)
						}
						return
					}
					if phase == "healthy" && (admissionErr != nil || handle == nil || !handle.Attempt().AuthorizationAcknowledged) {
						t.Fatalf("journal authorization was not acknowledged: %v", admissionErr)
					}
					if phase == "commit_ack_lost" && (admissionErr == nil || handle != nil) {
						t.Fatalf("unacknowledged admission returned launch authority: %v", admissionErr)
					}
					rows, err := DecodeSelectedForkSnapshotRowsForTest(after["runtime_external_effect_attempts"])
					if err != nil || len(rows) != 1 {
						t.Fatalf("physical attempt count=%d error=%v", len(rows), err)
					}
					raw, err := rows[0]["provider_request_journal"].Text()
					if err != nil || raw == "" {
						t.Fatalf("authorized attempt has no durable request journal: %v", err)
					}
					retained, err := runtimeeffects.DecodeProviderRequestJournal([]byte(raw))
					if err != nil {
						t.Fatal(err)
					}
					if _, err := runtimellm.RebuildProviderRequestJournal(retained, frame, surface, runtimeeffects.Fingerprint(body)); err != nil {
						t.Fatal(err)
					}
					state, err := rows[0]["state"].Text()
					if err != nil || state != "authorized" || string(rows[0]["launched_at"].Value) != "null" || string(rows[0]["resume_from_attempt_id"].Value) != "null" {
						t.Fatal("journal admission launched work or minted a resume link")
					}
					if phase == "healthy" {
						proveProviderJournalNativeRefusals(t, ctx, fixture, handle.Attempt(), journal, frame, surface, body, after)
					}
					if phase == "commit_ack_lost" {
						if _, err := runtimeeffects.BeginJournaledManagedCompletion(ctx, "anthropic_api", body, frame, journal, nil); err == nil {
							t.Fatal("ordinary admission replay became a resume permit")
						}
						repeated, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
						if err != nil || !reflect.DeepEqual(after, repeated) {
							t.Fatalf("unacknowledged admission replay changed durable rows: %v", err)
						}
					}
				})
			}
		}
	}
}

func proveProviderJournalNativeRefusals(t *testing.T, ctx context.Context, fixture completionSettlementFixture, attempt runtimeeffects.Attempt, journal runtimeeffects.ProviderRequestJournal, frame agentframe.Frame, surface managedcapabilities.Surface, body []byte, expected map[string]SelectedForkStorageTableSnapshot) {
	t.Helper()
	encoded, err := runtimeeffects.EncodeProviderRequestJournal(journal)
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"frame", "surface", "session", "memory", "history", "logical_operation", "operation_id", "body_pin", "adapter", "semantic_header"} {
		t.Run("refuse_"+scenario, func(t *testing.T) {
			changed, err := runtimeeffects.DecodeProviderRequestJournal(encoded)
			if err != nil {
				t.Fatal(err)
			}
			request := runtimeeffects.AuthorizeRequest{
				OperationID: attempt.OperationID, AttemptID: attempt.AttemptID,
				Kind: runtimeeffects.KindProviderTurn, Class: runtimeeffects.EffectWriteOrUnknown, Adapter: "anthropic_api", Transport: "http",
				CompletionRequest: attempt.Authority.CompletionRequest, RequestFingerprint: runtimeeffects.Fingerprint(body),
				AgentFrame: &frame, CapabilitySurface: &surface, ProviderRequestJournal: &changed, Origin: attempt.Origin,
				SessionGrantID: attempt.SessionGrantID, SessionLockOwner: attempt.SessionLockOwner, Now: time.Now().UTC(),
			}
			switch scenario {
			case "frame":
				changed.FrameContentHash = "foreign-frame"
			case "surface":
				changed.SurfaceID = uuid.NewString()
			case "session":
				changed.SessionID = uuid.NewString()
			case "memory":
				changed.Memory.Enabled = !changed.Memory.Enabled
			case "history":
				var rendering map[string]json.RawMessage
				if err := json.Unmarshal(changed.Rendering, &rendering); err != nil {
					t.Fatal(err)
				}
				rendering["messages"] = json.RawMessage(`[{"role":"user","content":"different retained history"}]`)
				changed.Rendering, err = json.Marshal(rendering)
				if err != nil {
					t.Fatal(err)
				}
			case "logical_operation":
				changed.LogicalOperationIdentity = "different-provider-turn"
			case "operation_id":
				request.OperationID = uuid.NewString()
			case "body_pin":
				request.RequestFingerprint = runtimeeffects.Fingerprint([]byte("different admitted body"))
			case "adapter":
				request.Adapter = "openai_compatible"
			case "semantic_header":
				changed.SemanticHeaders["anthropic-version"] = "different-version"
			}
			if _, err := fixture.store.AuthorizeExternalAttempt(ctx, attempt.Authority, request); err == nil || !strings.Contains(err.Error(), "journal") {
				t.Fatalf("native journal refusal was missing or reached ordinary replay: %v", err)
			}
			actual, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
			if err != nil || !reflect.DeepEqual(expected, actual) {
				t.Fatalf("native journal refusal changed durable rows: %v", err)
			}
		})
	}
}

func openProviderJournalFixture(t *testing.T, backend string, memory agentmemory.Plan) (completionSettlementFixture, *completionOutcomeConnector) {
	t.Helper()
	var native driver.Connector
	if backend == "postgres" {
		dsn, _, _ := testutil.StartPostgres(t)
		var err error
		native, err = pq.NewConnector(dsn)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		native = pipelineCrashSQLiteConnector{path: filepath.Join(t.TempDir(), "provider-journal.db")}
	}
	control := &completionOutcomeConnector{Connector: native}
	db := sql.OpenDB(control)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	var store completionSettlementTestStore
	if backend == "postgres" {
		store = admitTestPostgresStore(t, db)
	} else {
		selected := NewSQLiteRuntimeStoreForTest(db)
		if err := selected.BootstrapSchema(testAuthorActivityContext(), canonicalSchemaBootstrapTestRequest(t)); err != nil {
			t.Fatal(err)
		}
		selected.SetEventPayloadAdmitter(storeTestPayloadAdmitter)
		registerTestAuthorActivityCatalog(t, selected)
		store = selected
	}
	fixture := newCompletionSettlementFixtureWithActor(t, store, db, backend == "sqlite", memory, "global", executionposture.Live,
		runtimeactors.AgentConfig{ExecutionMode: "live", LLMBackend: "anthropic", ResolvedLLMBackend: "anthropic"})
	return fixture, control
}

func providerJournalAdmissionInputs(t *testing.T, fixture completionSettlementFixture) (context.Context, runtimeeffects.ProviderRequestJournal, agentframe.Frame, managedcapabilities.Surface, []byte) {
	t.Helper()
	ctx := withManagedCompletionTestSurface(t, fixture.context, fixture.authority, "anthropic_api")
	ctx = runtimeeffects.WithLogicalOperationIdentity(ctx, "provider-journal-admission")
	surface, found := managedcapabilities.FromContext(ctx)
	if !found {
		t.Fatal("native provider fixture has no admitted capability surface")
	}
	frame := managedCompletionTestFrame(t, fixture.authority, "anthropic_api")
	role, content, err := frame.ProviderInput()
	if err != nil {
		t.Fatal(err)
	}
	input := runtimellm.Message{Role: role, Content: content}
	body, err := json.Marshal(struct {
		Model     string               `json:"model"`
		MaxTokens int                  `json:"max_tokens"`
		System    string               `json:"system,omitempty"`
		Messages  []runtimellm.Message `json:"messages"`
	}{frame.Session.Provider.Model, 1024, frame.Session.ProviderPrompt, []runtimellm.Message{{Role: role, Content: strings.TrimSpace(content)}}})
	if err != nil {
		t.Fatal(err)
	}
	rendering, err := json.Marshal(struct {
		Version      string                      `json:"version"`
		Model        string                      `json:"model"`
		SystemPrompt string                      `json:"system_prompt"`
		Messages     []runtimellm.Message        `json:"messages"`
		Tools        []runtimellm.ToolDefinition `json:"tools"`
		Input        runtimellm.Message          `json:"input"`
	}{runtimeeffects.ProviderRequestJournalVersion, frame.Session.Provider.Model, frame.Session.ProviderPrompt, nil, nil, input})
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := surface.ContinuationFingerprint()
	if err != nil {
		t.Fatal(err)
	}
	journal, err := runtimeeffects.NewProviderRequestJournal(runtimeeffects.ProviderRequestJournal{
		Version: runtimeeffects.ProviderRequestJournalVersion, Adapter: "anthropic_api", FrameID: frame.FrameID, FrameContentHash: frame.ContentHash,
		SurfaceID: surface.ID, SurfaceFingerprint: fingerprint, SessionID: fixture.sessionID, TurnOrdinal: 1,
		LogicalOperationIdentity: "provider-journal-admission", Memory: &fixture.authority.Target.Memory,
		Method: http.MethodPost, Destination: "http://127.0.0.1:1234/v1/messages",
		SemanticHeaders: map[string]string{"content-type": "application/json", "anthropic-version": "2023-06-01"}, Rendering: rendering,
	}, body)
	if err != nil {
		t.Fatal(err)
	}
	return ctx, journal, frame, surface, body
}
