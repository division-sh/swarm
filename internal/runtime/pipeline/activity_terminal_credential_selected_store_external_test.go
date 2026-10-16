package pipeline_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/managedcredentials"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestActivityCredentialTerminalTimestampReplayBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, posture := range []string{"static_present", "static_absent", "connector_absent", "managed_present", "managed_absent"} {
			t.Run(backend+"/"+posture, func(t *testing.T) {
				ctx := testAuthorActivityContext(t, context.Background())
				selected, closeStore, reopen := openActivityReplayStore(t, backend)
				intent := runtimepipeline.NonIdempotentActivityIntentForTest(uuid.NewString(), uuid.NewString(), uuid.NewString())
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					if r.Header.Get("Authorization") != "Bearer provider-secret" {
						t.Error("provider did not receive the admitted credential")
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"echoed_authorization": r.Header.Get("Authorization")})
				}))
				t.Cleanup(server.Close)
				credentialStore, err := credentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
				if err != nil {
					t.Fatal(err)
				}
				if posture == "static_present" {
					if err := credentialStore.Set(ctx, "provider_token", "provider-secret"); err != nil {
						t.Fatal(err)
					}
				}
				tool := runtimecontracts.MustToolSchemaEntry(
					runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),

					runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)),
					runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: server.URL, Headers: map[string]string{"Authorization": "Bearer {{credentials.provider_token}}"}}),
					runtimecontracts.WithToolCredentials("provider_token"),
				)
				if posture == "connector_absent" {
					intent.Tool = "telegram.send_message"
					intent.ActivityID = "telegram_send_message"
					intent.Input = runtimepipeline.ActivityInputForTest(map[string]any{"chat_id": "42", "text": "hello"})
					tool = runtimepipeline.TelegramConnectorToolForTest(server.URL)
				}
				tool, managed := activityReplayManagedCredentials(t, posture, tool)
				source := activityJournalProofSource(t, map[string]runtimecontracts.ToolSchemaEntry{intent.Tool: tool})
				ctx = requireActivityReplayRun(t, selected, source, intent.SourceRunID)
				bus := newActivityJournalProofBus(t, selected, source)
				pc := newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{Module: gateRecoveryModule{source: source}, Credentials: credentialStore, ManagedCredentials: managed})
				intent = seedSelectedActivitySource(t, ctx, selected, pc, intent)
				_, releaseJournal := installActivityReplayJournal(t, ctx, selected, bus, pc)
				if err := runtimepipeline.ExecuteActivityIntentForTest(ctx, pc, nil, intent); err != nil {
					t.Fatal(err)
				}
				receipt, found, err := activityReplayJournal(selected).LoadActivityAttempt(ctx, runtimepipeline.ActivityAttemptStartForTest(intent).RequestEventID)
				if err != nil || !found || receipt.CompletedAt == nil || receipt.CompletedAt.IsZero() {
					t.Fatalf("missing durable credentialed outcome: found=%t err=%v", found, err)
				}
				if posture == "static_present" || posture == "managed_present" {
					if receipt.Status != runtimepipeline.ActivityAttemptStatusSucceeded || calls.Load() != 1 || receipt.ResultPayload["result"].(map[string]any)["echoed_authorization"] != "Bearer [REDACTED]" {
						t.Fatalf("credentialed dispatch or redaction changed: status=%s calls=%d failure=%+v", receipt.Status, calls.Load(), receipt.Failure)
					}
				} else if receipt.Status != runtimepipeline.ActivityAttemptStatusFailed || calls.Load() != 0 || receipt.Failure == nil || receipt.Failure.Class != runtimefailures.ClassAuthenticationNeeded || receipt.Failure.Detail.Code != "activity_credential_required" {
					t.Fatalf("missing credential did not journal the exact pre-dispatch refusal: status=%s calls=%d failure=%+v", receipt.Status, calls.Load(), receipt.Failure)
				}
				wantManagedReads := int32(0)
				if posture == "managed_present" || posture == "managed_absent" {
					wantManagedReads = 1
				}
				if managed.reads.Load() != wantManagedReads {
					t.Fatalf("managed credential reads=%d, want %d", managed.reads.Load(), wantManagedReads)
				}
				original := loadActivityResultForProof(t, ctx, selected, receipt.ResultEventID)
				if !original.Event.Event().CreatedAt().Equal(receipt.CompletedAt.UTC().Truncate(time.Microsecond)) {
					t.Fatal("credentialed outcome reminted its terminal timestamp")
				}
				before := storetest.ObserveActivityResultPublicationStorage(t, ctx, selected.events)
				if err := bus.WaitForQuiescence(ctx); err != nil {
					t.Fatal(err)
				}
				releaseJournal()
				if err := closeStore(); err != nil {
					t.Fatal(err)
				}
				selected = reopen()
				replayBus := newActivityJournalProofBus(t, selected, source)
				unavailableCredentials := &activityReplayCredentialProbe{}
				unavailableManaged := &activityReplayManagedCredentialProbe{Store: managedcredentials.NewMemoryStore()}
				replay := newGateRecoveryCoordinator(replayBus, selected, runtimepipeline.PipelineCoordinatorOptions{Module: gateRecoveryModule{source: source}, Credentials: unavailableCredentials, ManagedCredentials: unavailableManaged})
				if err := runtimepipeline.ExecuteActivityIntentForTest(ctx, replay, nil, intent); err != nil {
					t.Fatalf("credentialed terminal reopen replay: %v", err)
				}
				stored, found, err := activityReplayJournal(selected).LoadActivityAttempt(ctx, receipt.RequestEventID)
				if err != nil || !found || !reflect.DeepEqual(receipt, stored) || !reflect.DeepEqual(original, loadActivityResultForProof(t, ctx, selected, receipt.ResultEventID)) || !reflect.DeepEqual(before, storetest.ObserveActivityResultPublicationStorage(t, ctx, selected.events)) {
					t.Fatal("credentialed replay changed the journal, result, or durable side effects")
				}
				wantCalls := int32(0)
				if posture == "static_present" || posture == "managed_present" {
					wantCalls = 1
				}
				if calls.Load() != wantCalls || unavailableCredentials.reads.Load() != 0 || unavailableManaged.reads.Load() != 0 {
					t.Fatalf("terminal replay redispatched or reread credentials: calls=%d static=%d managed=%d", calls.Load(), unavailableCredentials.reads.Load(), unavailableManaged.reads.Load())
				}
			})
		}
	}
}

type activityReplayManagedCredentialProbe struct {
	managedcredentials.Store
	reads atomic.Int32
}

func (p *activityReplayManagedCredentialProbe) Get(ctx context.Context, key string) (managedcredentials.Record, bool, error) {
	p.reads.Add(1)
	return p.Store.Get(ctx, key)
}

func activityReplayManagedCredentials(t *testing.T, posture string, tool runtimecontracts.ToolSchemaEntry) (runtimecontracts.ToolSchemaEntry, *activityReplayManagedCredentialProbe) {
	t.Helper()
	store := &activityReplayManagedCredentialProbe{Store: managedcredentials.NewMemoryStore()}
	if posture != "managed_present" && posture != "managed_absent" {
		return tool, store
	}
	httpSpec, _ := tool.HTTP()
	httpSpec.Headers = nil
	tool = runtimecontracts.MustToolSchemaEntry(
		runtimecontracts.WithToolCategory("provider_connector"),
		runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
		runtimecontracts.WithToolSchemas(tool.InputSchema(), tool.OutputSchema()),
		runtimecontracts.WithToolHTTP(httpSpec),
		runtimecontracts.WithToolResponseSuccess(runtimecontracts.HTTPResponseSuccess{Kind: "http_status_2xx"}),
		runtimecontracts.WithToolManagedCredential(runtimecontracts.ManagedCredentialRef{Key: "provider_token", Header: "Authorization", Prefix: "Bearer ", GrantType: managedcredentials.GrantClientCredentials}),
	)
	if posture == "managed_present" {
		if err := store.Put(context.Background(), managedcredentials.Record{Key: "provider_token", Provider: "fixture", GrantType: managedcredentials.GrantClientCredentials, AccessToken: "provider-secret", TokenType: "Bearer", Status: managedcredentials.StatusConnected, ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
			t.Fatal(err)
		}
	}
	return tool, store
}
