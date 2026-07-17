package apiv1

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestMailboxDecideHTTPReleasesProposedEffectThroughProviderOnBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) (any, *sql.DB)
	}{
		{
			name: "sqlite",
			open: func(t *testing.T) (any, *sql.DB) {
				selected := storetest.StartSQLiteRuntimeStoreWithContext(t, context.Background())
				return selected, selected.DB
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T) (any, *sql.DB) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return storetest.AdmitPostgresRuntimeStore(t, db), db
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			bodySeen := make(chan string, 1)
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				rawBody, _ := io.ReadAll(r.Body)
				raw, _ := canonicaljson.Decode(rawBody)
				textValue, _ := raw.Lookup("text")
				text, _ := textValue.String()
				bodySeen <- text
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"message_id":"provider-1"}`))
			}))
			defer provider.Close()

			persistence, db := tc.open(t)
			bundle := mailboxWriteSupportedSurfaceBundle(t)
			bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
				"provider_write": {
					HandlerType: "http", EffectClass: string(runtimecontracts.ActivityEffectClassNonIdempotentWrite),
					InputSchema: runtimecontracts.ToolInputSchema{
						Type: "object", Required: []string{"text"},
						Properties: map[string]runtimecontracts.ToolInputSchema{"text": {Type: "string"}},
					},
					OutputSchema: runtimecontracts.ToolInputSchema{Type: "object"},
					HTTP: &runtimecontracts.HTTPToolSpec{
						Method: http.MethodPost, URL: provider.URL,
						Body: map[string]any{"text": "{{input.text}}"},
					},
				},
			}
			activityNode := runtimecontracts.SystemNodeContract{
				ID: "activity-runtime", ExecutionType: runtimecontracts.SystemNodeExecutionType,
				SubscribesTo: []string{"mailbox.card_decided", "platform.activity_requested"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"mailbox.card_decided":        {},
					"platform.activity_requested": {},
				},
			}
			bundle.Nodes[activityNode.ID] = activityNode
			if bundle.Semantics.NodeHandlers == nil {
				bundle.Semantics.NodeHandlers = map[string]map[string]runtimecontracts.SystemNodeEventHandler{}
			}
			bundle.Semantics.NodeHandlers[activityNode.ID] = activityNode.EventHandlers
			if bundle.Semantics.EventOwners == nil {
				bundle.Semantics.EventOwners = map[string][]string{}
			}
			bundle.Semantics.EventOwners["platform.activity_requested"] = []string{activityNode.ID}
			bundle.Semantics.EventOwners["mailbox.card_decided"] = []string{activityNode.ID}

			source := semanticview.Wrap(bundle)
			fact := bundleSourceFactForTestBundle(t, bundle)
			handler, bus := newProposedEffectMailboxHandler(t, persistence, db, source, fact)

			runID, entityID := uuid.NewString(), uuid.NewString()
			insertProposedEffectAPIRun(t, db, tc.name, runID)
			cards := persistence.(decisioncard.Store)
			card, continuation := proposedEffectAPICard(t, runID, entityID, fact, source.WorkflowVersion())
			if err := cards.(decisioncard.ProposedEffectStore).CreateProposedEffectCard(testAuthorActivityContextForSource(context.Background(), fact), card, continuation); err != nil {
				t.Fatal(err)
			}

			response := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"decide","method":"mailbox.decide","params":{"card_id":%q,"verdict":"approve","fields":{},"observed_content_hash":%q,"idempotency_key":%q}}`, card.CardID, card.CardContentHash, "approve-proposed-effect-"+tc.name))
			if response.Error != nil {
				t.Fatalf("mailbox.decide error = %#v", response.Error)
			}
			if result := asMap(t, response.Result); result["status"] != decisioncard.StatusDecided || result["verdict"] != "approve" {
				t.Fatalf("mailbox.decide result = %#v", result)
			}
			decisionEventID := stringValue(t, asMap(t, response.Result)["decision_event_id"], "decision_event_id")
			waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := bus.WaitForQuiescence(waitCtx); err != nil {
				t.Fatalf("wait for proposed-effect route: %v", err)
			}
			select {
			case got := <-bodySeen:
				if got != "Exact operator-approved content" {
					t.Fatalf("provider text = %q", got)
				}
			case <-waitCtx.Done():
				readback, readbackErr := cards.(decisioncard.ProposedEffectStore).ProposedEffectReadback(context.Background(), card.CardID)
				t.Fatalf("provider did not receive approved effect; readback=%#v error=%v", readback, readbackErr)
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("provider calls = %d, want 1", got)
			}
			decisionEvent := loadMailboxWritePersistedEvent(t, db, tc.name, decisionEventID)
			if decisionEvent.ID() != decisionEventID {
				t.Fatalf("persisted decision event id = %q, want %q", decisionEvent.ID(), decisionEventID)
			}
			requestEvent := loadMailboxWritePersistedEvent(t, db, tc.name, continuation.RequestEventID)
			if requestEvent.Type() != events.EventType("platform.activity_requested") {
				t.Fatalf("persisted request event type = %q", requestEvent.Type())
			}
			readback, err := cards.(decisioncard.ProposedEffectStore).ProposedEffectReadback(context.Background(), card.CardID)
			if err != nil || readback.ContinuationState != decisioncard.ProposedEffectRequestReleased || readback.DispatchState != "succeeded" {
				t.Fatalf("proposed-effect readback = %#v, %v", readback, err)
			}
			assertProposedEffectAPIExecutionRows(t, db, tc.name, runID)
		})
	}
}

func newProposedEffectMailboxHandler(
	t *testing.T,
	persistence any,
	db *sql.DB,
	source semanticview.Source,
	fact runtimecorrelation.BundleSourceFact,
) (*Handler, *runtimebus.EventBus) {
	t.Helper()
	var coordinator *runtimepipeline.PipelineCoordinator
	bus, err := newScopedAPITestEventBus(t, persistence.(runtimebus.EventStore), runtimebus.EventBusOptions{
		ContractBundle:    source,
		BundleFingerprint: fact.BundleFingerprint,
		BundleSourceFact:  fact,
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if coordinator == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{coordinator}
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	if sqliteStore, ok := persistence.(*store.SQLiteRuntimeStore); ok {
		workflowStore = runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, sqliteStore)
	}
	cards, ok := persistence.(decisioncard.Store)
	if !ok {
		t.Fatal("persistence store does not implement decisioncard.Store")
	}
	coordinator = runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
		Module: newRunCompletionSystemNodeModule(t, source), WorkflowStore: workflowStore,
		DecisionCards: cards,
		BundleHash:    fact.BundleHash,
	})
	bus.RegisterRuntimeActiveAgentDescriptor(runtimebus.ActiveAgentDescriptor{AgentID: "workflow-runtime"})
	bus.Subscribe("workflow-runtime", events.EventType("mailbox.card_decided"), events.EventType("platform.activity_requested"))
	mailbox, ok := persistence.(MailboxAPIStore)
	if !ok {
		t.Fatal("persistence store does not implement MailboxAPIStore")
	}
	runs, ok := persistence.(RunReadStore)
	if !ok {
		t.Fatal("persistence store does not implement RunReadStore")
	}
	observability, ok := persistence.(ObservabilityReadStore)
	if !ok {
		t.Fatal("persistence store does not implement ObservabilityReadStore")
	}
	idempotency, ok := persistence.(APIIdempotencyStore)
	if !ok {
		t.Fatal("persistence store does not implement APIIdempotencyStore")
	}
	runBundleContext, _ := persistence.(RunBundleContextStore)
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now: func() time.Time { return time.Now().UTC() }, Ready: func() bool { return true }, Database: fakePinger{},
			Runs: runs, Observability: observability, Idempotency: idempotency, Events: bus, Source: source,
			RunBundleContext: runBundleContext, Mailbox: mailbox, DecisionCards: cards, DecisionAuthority: workflowStore,
			Bundle: runtimecontracts.BundleIdentity{
				WorkflowName: source.WorkflowName(), WorkflowVersion: source.WorkflowVersion(),
				Fingerprint: fact.BundleFingerprint, BundleHash: fact.BundleHash,
			},
		}),
	})
	return handler, bus
}

func proposedEffectAPICard(t *testing.T, runID, entityID string, fact runtimecorrelation.BundleSourceFact, workflowVersion string) (decisioncard.Card, decisioncard.ProposedEffectContinuation) {
	t.Helper()
	now := time.Date(2026, 7, 14, 23, 0, 0, 0, time.UTC)
	sourceEventID := uuid.NewString()
	requestEventID := activityidentity.RequestEventID(activityidentity.Fact{
		RunID: runID, SourceEventID: sourceEventID, EntityID: entityID,
		NodeID: "activity-runtime", HandlerEventKey: "support.reply_drafted",
		ActivityID: "send_support_reply", Tool: "provider_write", Attempt: 1,
	})
	input, err := canonicaljson.FromGo(map[string]any{"text": "Exact operator-approved content"})
	if err != nil {
		t.Fatal(err)
	}
	continuation := decisioncard.ProposedEffectContinuation{
		CardID: decisioncard.ProposedEffectCardID(requestEventID, "support_reply"), RunID: runID,
		RequestEventID: requestEventID, ActivityID: "send_support_reply", Tool: "provider_write",
		BundleHash: fact.BundleHash, WorkflowVersion: workflowVersion, Input: input,
		EffectClass:  runtimecontracts.ActivityEffectClassNonIdempotentWrite,
		SuccessEvent: "send_support_reply.succeeded", FailureEvent: "send_support_reply.failed",
		RevisionEvent: "send_support_reply.revision_requested", RejectedEvent: "send_support_reply.rejected",
		RetryMaxAttempts: 1, ForkPolicy: runtimecontracts.ActivityForkRequireConfirmation,
		EntityID: entityID, NodeID: "activity-runtime", FlowInstance: "root", HandlerEventKey: "support.reply_drafted",
		SourceEventID: sourceEventID, SourceRunID: runID, SourceTaskID: "task-1",
		ExecutionMode: "live", State: decisioncard.ProposedEffectPending, CreatedAt: now, UpdatedAt: now,
	}.Canonical()
	effect, err := continuation.EffectValue()
	if err != nil {
		t.Fatal(err)
	}
	continuation.EffectContentHash, err = canonicaljson.HashValue(effect)
	if err != nil {
		t.Fatal(err)
	}
	anchor, err := decisioncard.NewProposedEffectAnchor(decisioncard.ProposedEffectAnchor{
		RequestEventID: requestEventID, ActivityID: continuation.ActivityID, Decision: "support_reply",
		Scope: decisioncard.Scope{Kind: decisioncard.ScopeEntity, FlowInstance: "root", EntityID: entityID},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := decisioncard.FreezeSnapshot("support_reply", "", map[string]any{"input": input.Interface()}, map[string]runtimecontracts.WorkflowGateOutcomePlan{
		"approve": {Verdict: "approve"},
		"revise":  {Verdict: "revise", Input: map[string]runtimecontracts.WorkflowGateInputField{"feedback": {Type: "text", Required: true}}},
		"reject":  {Verdict: "reject", Input: map[string]runtimecontracts.WorkflowGateInputField{"reason": {Type: "text"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	card, err := decisioncard.New(decisioncard.Card{
		CardID: continuation.CardID, RunID: runID, Anchor: anchor, Snapshot: snapshot,
		ExecutionMode:     "live",
		EffectContentHash: continuation.EffectContentHash, BundleHash: fact.BundleHash,
		WorkflowVersion: workflowVersion, CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return card, continuation
}

func insertProposedEffectAPIRun(t *testing.T, db *sql.DB, backend, runID string) {
	t.Helper()
	query := `INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`
	if backend == "postgres" {
		query = `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $2)`
	}
	if _, err := db.ExecContext(context.Background(), query, runID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
}

func assertProposedEffectAPIExecutionRows(t *testing.T, db *sql.DB, backend, runID string) {
	t.Helper()
	query := `SELECT
		(SELECT COUNT(*) FROM events WHERE run_id = ? AND event_name = 'platform.activity_requested'),
		(SELECT COUNT(*) FROM activity_attempts WHERE run_id = ? AND status = 'succeeded')`
	args := []any{runID, runID}
	if backend == "postgres" {
		query = `SELECT
			(SELECT COUNT(*) FROM events WHERE run_id = $1::uuid AND event_name = 'platform.activity_requested'),
			(SELECT COUNT(*) FROM activity_attempts WHERE run_id = $1::uuid AND status = 'succeeded')`
		args = []any{runID}
	}
	var requests, attempts int
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&requests, &attempts); err != nil {
		t.Fatal(err)
	}
	if requests != 1 || attempts != 1 {
		t.Fatalf("approved effect execution rows = requests:%d attempts:%d, want 1/1", requests, attempts)
	}
}
