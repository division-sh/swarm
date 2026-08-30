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
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
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
				return selected, storetest.DatabaseForTest(selected)
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
			bundle := proposedEffectSupportedSurfaceBundle(t, provider.URL)

			source := semanticview.Wrap(bundle)
			fact := sourceArtifactFactForTestBundle(t, bundle)
			handler, bus := newProposedEffectMailboxHandler(t, persistence, db, source, fact)

			runID, entityID := uuid.NewString(), uuid.NewString()
			cards := persistence.(decisioncard.Store)
			card, continuation := proposedEffectAPICard(t, runID, entityID, fact, source.WorkflowVersion())
			fixtureCtx := testAuthorActivityContextForSource(context.Background(), fact)
			insertProposedEffectAPIRun(t, fixtureCtx, db, tc.name, runID, fact)
			storetest.CommitSemanticEvent(t, fixtureCtx, persistence, eventtest.ExistingRunRootIngress(
				continuation.SourceEventID, events.EventType("thing.created"), "operator", continuation.SourceTaskID,
				[]byte(`{"amount":250,"who":"alice"}`), 0, runID,
				events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), continuation.FlowInstance),
				continuation.CreatedAt.Add(-time.Second),
			))
			if err := cards.(decisioncard.ProposedEffectStore).CreateProposedEffectCard(fixtureCtx, card, continuation); err != nil {
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
	fact runtimecorrelation.SourceArtifactFact,
) (*Handler, *runtimebus.EventBus) {
	t.Helper()
	var coordinator *runtimepipeline.PipelineCoordinator
	bus, err := newScopedAPITestEventBus(t, persistence.(runtimebus.EventStore), runtimebus.EventBusOptions{
		ContractBundle:     source,
		SourceArtifactFact: fact,
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
	workflowPersistence := runtimepipeline.NewWorkflowPersistence(persistence.(apiTestRuntimeMutationOwner))
	if sqliteStore, ok := persistence.(*store.SQLiteRuntimeStore); ok {
		workflowPersistence = runtimepipeline.NewWorkflowPersistence(sqliteStore)
	}
	deliveryOwner := persistence.(runtimedelivery.Store)
	obligationOwner := persistence.(interface {
		PipelineObligations() runtimepipelineobligation.Store
	}).PipelineObligations()
	runLifecycle := persistence.(runtimerunlifecycle.OperationOwner)
	cards, ok := persistence.(decisioncard.Store)
	if !ok {
		t.Fatal("persistence store does not implement decisioncard.Store")
	}
	coordinator = runtimepipeline.NewPipelineCoordinatorWithOptions(bus, completeAPITestDurableWorkflowOptions(t, persistence, bus, runtimepipeline.PipelineCoordinatorOptions{
		Module: newRunCompletionSystemNodeModule(t, source), Persistence: workflowPersistence,
		DecisionCards: cards, ProposedEffects: persistence.(decisioncard.ProposedEffectStore),
		DeliveryStore: deliveryOwner, DeliveryRuntime: bus, PipelineObligations: obligationOwner,
		RunLifecycle: runLifecycle, SourceArtifactFact: fact,
	}))

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
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			Now: func() time.Time { return time.Now().UTC() }, Ready: func() bool { return true }, Database: fakePinger{},
			Runs: runs, Observability: observability, Idempotency: idempotency, Events: bus, Source: source,
			RunBundleContext: runBundleContext, Mailbox: mailbox, DecisionCards: cards, DecisionAuthority: coordinator,
			Bundle: runtimecontracts.BundleIdentity{
				WorkflowName: source.WorkflowName(), WorkflowVersion: source.WorkflowVersion(),
				BundleHash: fact.BundleHash(),
			},
		}),
	})
	return handler, bus
}

func proposedEffectAPICard(t *testing.T, runID, entityID string, fact runtimecorrelation.SourceArtifactFact, workflowVersion string) (decisioncard.Card, decisioncard.ProposedEffectContinuation) {
	t.Helper()
	now := time.Date(2026, 7, 14, 23, 0, 0, 0, time.UTC)
	sourceEventID := uuid.NewString()
	activityNode := identitytest.RootNode(t, "support-agent")
	activityOwner := activityidentity.MustNodeOwner(activityNode)
	requestEventID := activityidentity.RequestEventID(activityidentity.Fact{
		RunID: runID, SourceEventID: sourceEventID, EntityID: entityID,
		Owner: activityOwner, ExecutionFlowID: ".", HandlerEventKey: "support.reply_drafted",
		ActivityID: "send_support_reply", Tool: "provider_write", Attempt: 1,
	})
	input, err := canonicaljson.FromGo(map[string]any{"text": "Exact operator-approved content"})
	if err != nil {
		t.Fatal(err)
	}
	continuation := decisioncard.ProposedEffectContinuation{
		CardID: decisioncard.ProposedEffectCardID(requestEventID, "support_reply"), RunID: runID,
		RequestEventID: requestEventID, ActivityID: "send_support_reply", Tool: "provider_write",
		BundleHash: fact.BundleHash(), WorkflowVersion: workflowVersion, Input: input,
		EffectClass:  runtimecontracts.ActivityEffectClassNonIdempotentWrite,
		SuccessEvent: "send_support_reply.succeeded", FailureEvent: "send_support_reply.failed",
		RevisionEvent: "send_support_reply.revision_requested", RejectedEvent: "send_support_reply.rejected",
		RetryMaxAttempts: 1, ForkPolicy: runtimecontracts.ActivityForkRequireConfirmation,
		EntityID: entityID, NodeID: activityOwner.Key(), FlowID: ".", FlowInstance: runID, HandlerEventKey: "support.reply_drafted",
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
		Scope: decisioncard.Scope{Kind: decisioncard.ScopeEntity, FlowInstance: runID, EntityID: entityID}, Source: eventtest.RootRoutingSource(entityID),
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
		EffectContentHash: continuation.EffectContentHash, BundleHash: fact.BundleHash(),
		WorkflowVersion: workflowVersion, CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return card, continuation
}

func proposedEffectSupportedSurfaceBundle(t *testing.T, providerURL string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	writeRunCompletionFixtureFile(t, root+"/schema.yaml", `name: proposed-effect-supported-surface
initial_state: new
terminal_states: [done]
states: [new, done]
pins:
  inputs:
    events: [thing.created]
`)
	writeRunCompletionFixtureFile(t, root+"/events.yaml", `thing.created:
  text: text
`)
	writeRunCompletionFixtureFile(t, root+"/entities.yaml", `review:
  text: text
`)
	writeRunCompletionFixtureFile(t, root+"/nodes.yaml", `support-agent:
  id: support-agent
  execution_type: system_node
  subscribes_to: [thing.created]
  event_handlers:
    thing.created:
      create_entity: true
      advances_to: done
      activity:
        id: send_support_reply
        tool: provider_write
        input:
          text: {ref: payload.text}
        approval: {decision: support_reply}
`)
	writeRunCompletionFixtureFile(t, root+"/tools.yaml", fmt.Sprintf(`provider_write:
  description: Send the exact approved support reply.
  handler_type: http
  effect_class: non_idempotent_write
  http:
    method: POST
    url: %q
    body: {text: "{{input.text}}"}
  input_schema:
    type: object
    required: [text]
    properties: {text: {type: string}}
  output_schema:
    type: object
    required: [message_id]
    properties: {message_id: {type: string}}
`, providerURL))
	repoRoot := runCompletionRepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		root,
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load proposed-effect supported-surface bundle: %v", err)
	}
	return bundle
}

func insertProposedEffectAPIRun(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	backend, runID string,
	source runtimecorrelation.SourceArtifactFact,
) {
	t.Helper()
	if backend == "postgres" {
		runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, Source: source})
	} else {
		runlifecyclefixture.RequireSQLite(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, Source: source})
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
