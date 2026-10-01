package pipeline_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/providerconnectors"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestActivityMockTerminalTimestampReplayDoesNotRequireCurrentResponsePlan(t *testing.T) {
	for _, backend := range activityTerminalReplayStoreCases() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := testAuthorActivityContext(t, context.Background())
			selected, closeStore, reopen := openActivityReplayStore(t, backend.name)
			runID := uuid.NewString()
			requireActivityReplayRun(t, ctx, selected, runID)
			var httpCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				httpCalls.Add(1)
			}))
			defer server.Close()
			tool := runtimepipeline.TelegramConnectorToolForTest(server.URL)
			output := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object"), runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{"ok": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("boolean"))}), runtimecontracts.ToolSchemaRequired("ok"))
			tool, _ = tool.WithSchemas(tool.InputSchema(), output)

			source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{
				"telegram.send_message": tool,
			}})
			plan, err := providerconnectors.NewMockResponsePlan(map[string]map[string]any{
				"telegram.send_message": {"ok": true},
			})
			if err != nil {
				t.Fatalf("NewMockResponsePlan: %v", err)
			}
			intent := runtimepipeline.NonIdempotentActivityIntentForTest(runID, uuid.NewString(), uuid.NewString())
			seedSelectedActivitySource(t, ctx, selected, intent)
			intent.Tool = "telegram.send_message"
			intent.ActivityID = "telegram_send_message"
			intent.ExecutionMode = executionmode.Mock
			intent.Input = runtimepipeline.ActivityInputForTest(map[string]any{"chat_id": "42", "text": "hello"})

			firstBus := newActivityJournalProofBus(t, selected, source)
			first := newGateRecoveryCoordinator(firstBus, selected, runtimepipeline.PipelineCoordinatorOptions{
				Module: gateRecoveryModule{source: source}, Persistence: selected.persistence,
				MockConnectorResponses: plan,
			})
			if err := runtimepipeline.ExecuteActivityIntentForTest(ctx, first, nil, intent); err != nil {
				t.Fatalf("execute initial mock activity: %v", err)
			}
			receipt, found, err := activityReplayJournal(selected).LoadActivityAttempt(ctx, runtimepipeline.ActivityAttemptStartForTest(intent).RequestEventID)
			if err != nil || !found || receipt.CompletedAt == nil || len(firstBus.publishes) != 1 || !firstBus.publishes[0].CreatedAt().Equal(receipt.CompletedAt.UTC().Truncate(time.Microsecond)) {
				t.Fatalf("initial publications = %#v", firstBus.publishes)
			}
			original := loadActivityResultForProof(t, ctx, selected, receipt.ResultEventID)
			before := storetest.ObserveActivityResultPublicationStorage(t, ctx, selected.events)
			if err := firstBus.WaitForQuiescence(ctx); err != nil {
				t.Fatal(err)
			}
			if err := closeStore(); err != nil {
				t.Fatal(err)
			}
			selected = reopen()

			restartBus := newActivityJournalProofBus(t, selected, source)
			credentials := &activityReplayCredentialProbe{}
			restarted := newGateRecoveryCoordinator(restartBus, selected, runtimepipeline.PipelineCoordinatorOptions{
				Module: gateRecoveryModule{source: source}, Persistence: selected.persistence,
				Credentials: credentials,
			})
			if err := runtimepipeline.ExecuteActivityIntentForTest(ctx, restarted, nil, intent); err != nil {
				t.Fatalf("replay terminal mock activity without current plan: %v", err)
			}
			if len(restartBus.publishes) != 1 || restartBus.publishes[0].ID() != firstBus.publishes[0].ID() || !restartBus.publishes[0].CreatedAt().Equal(firstBus.publishes[0].CreatedAt()) {
				t.Fatalf("restart publications = %#v, want journaled event %q", restartBus.publishes, firstBus.publishes[0].ID())
			}
			replayed := loadActivityResultForProof(t, ctx, selected, receipt.ResultEventID)
			stored, found, err := activityReplayJournal(selected).LoadActivityAttempt(ctx, receipt.RequestEventID)
			if err != nil || !found || !reflect.DeepEqual(receipt, stored) || !reflect.DeepEqual(original, replayed) || !reflect.DeepEqual(before, storetest.ObserveActivityResultPublicationStorage(t, ctx, selected.events)) {
				t.Fatalf("reopened mock replay changed journal, result, or durable side effects: found=%t err=%v", found, err)
			}
			if httpCalls.Load() != 0 || credentials.reads.Load() != 0 {
				t.Fatalf("replay launched live dependencies: HTTP=%d credentials=%d", httpCalls.Load(), credentials.reads.Load())
			}
		})
	}
}

type activityReplayCredentialProbe struct{ reads atomic.Int32 }

func (p *activityReplayCredentialProbe) Get(context.Context, string) (string, bool, error) {
	p.reads.Add(1)
	return "", false, nil
}
func (*activityReplayCredentialProbe) Set(context.Context, string, string) error { return nil }
func (*activityReplayCredentialProbe) List(context.Context) ([]string, error)    { return nil, nil }
func (*activityReplayCredentialProbe) Delete(context.Context, string) error      { return nil }
