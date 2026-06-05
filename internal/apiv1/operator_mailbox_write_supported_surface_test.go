package apiv1

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

const mailboxWriteSupportedSurfaceFingerprint = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
const mailboxWriteSupportedSurfaceBundleHash = "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

func TestOperatorMailboxWriteSupportedSurfacePublishesAndReadsAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		setup func(*testing.T, context.Context, semanticview.Source, runtimecorrelation.BundleSourceFact) (*Handler, *sql.DB)
	}{
		{
			name: "sqlite_default_no_selector",
			setup: func(t *testing.T, ctx context.Context, source semanticview.Source, fact runtimecorrelation.BundleSourceFact) (*Handler, *sql.DB) {
				t.Helper()
				sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
				handler := newMailboxWriteSupportedSurfaceHandler(t, ctx, sqliteStore, sqliteStore.DB, source, fact, sqliteStore)
				return handler, sqliteStore.DB
			},
		},
		{
			name: "postgres_explicit_opt_in",
			setup: func(t *testing.T, _ context.Context, source semanticview.Source, fact runtimecorrelation.BundleSourceFact) (*Handler, *sql.DB) {
				t.Helper()
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				pg := &store.PostgresStore{DB: db}
				handler := newMailboxWriteSupportedSurfaceHandler(t, context.Background(), pg, db, source, fact, pg)
				return handler, db
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := mailboxWriteSupportedSurfaceBundle(t)
			source := semanticview.Wrap(bundle)
			fact := bundleSourceFactForTestBundle(t, bundle)
			handler, db := tc.setup(t, ctx, source, fact)

			published := rpcCall(t, handler, eventPublishBodyWithoutBundle("", "thing.created", `{"amount":250,"who":"alice"}`, "", "idem-mailbox-write-"+tc.name))
			if published.Error != nil {
				t.Fatalf("event.publish error = %#v", published.Error)
			}
			result := asMap(t, published.Result)
			eventID := stringValue(t, result["event_id"], "event_id")
			runID := stringValue(t, result["run_id"], "run_id")
			deliveries := asSlice(t, result["deliveries"])
			if len(deliveries) != 2 {
				t.Fatalf("event.publish deliveries = %#v, want workflow-runtime and reviewer deliveries", deliveries)
			}
			seenWorkflowRuntime := false
			seenReviewer := false
			for _, rawDelivery := range deliveries {
				delivery := asMap(t, rawDelivery)
				if strings.TrimSpace(stringValue(t, delivery["delivery_id"], "delivery_id")) == "" || !validEventPublishSubscriberType(fmt.Sprint(delivery["subscriber_type"])) {
					t.Fatalf("event.publish delivery identity = %#v, want persisted typed delivery identity", delivery)
				}
				switch delivery["subscriber_id"] {
				case "workflow-runtime":
					seenWorkflowRuntime = delivery["status"] == "pending"
				case "reviewer":
					seenReviewer = delivery["status"] == "delivered"
				}
			}
			if !seenWorkflowRuntime || !seenReviewer {
				t.Fatalf("event.publish deliveries = %#v, want pending workflow-runtime and delivered reviewer", deliveries)
			}

			waitForMailboxWriteSupportedSurface(t, handler, db, runID, eventID, tc.name)
		})
	}
}

func TestOperatorRuleMailboxWriteSupportedSurfaceIsBranchScopedAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		setup func(*testing.T, context.Context, semanticview.Source, runtimecorrelation.BundleSourceFact) (*Handler, *sql.DB)
	}{
		{
			name: "sqlite_default_no_selector",
			setup: func(t *testing.T, ctx context.Context, source semanticview.Source, fact runtimecorrelation.BundleSourceFact) (*Handler, *sql.DB) {
				t.Helper()
				sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
				handler := newMailboxWriteSupportedSurfaceHandler(t, ctx, sqliteStore, sqliteStore.DB, source, fact, sqliteStore)
				return handler, sqliteStore.DB
			},
		},
		{
			name: "postgres_explicit_opt_in",
			setup: func(t *testing.T, _ context.Context, source semanticview.Source, fact runtimecorrelation.BundleSourceFact) (*Handler, *sql.DB) {
				t.Helper()
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				pg := &store.PostgresStore{DB: db}
				handler := newMailboxWriteSupportedSurfaceHandler(t, context.Background(), pg, db, source, fact, pg)
				return handler, db
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := conditionalRuleMailboxWriteSupportedSurfaceBundle(t)
			source := semanticview.Wrap(bundle)
			fact := bundleSourceFactForTestBundle(t, bundle)
			handler, db := tc.setup(t, ctx, source, fact)

			auto := rpcCall(t, handler, eventPublishBodyWithoutBundle("", "thing.created", `{"amount":50,"who":"alice"}`, "", "idem-rule-mailbox-write-auto-"+tc.name))
			if auto.Error != nil {
				t.Fatalf("auto event.publish error = %#v", auto.Error)
			}
			autoRunID := stringValue(t, asMap(t, auto.Result)["run_id"], "run_id")
			waitForConditionalRuleEntityState(t, db, autoRunID, tc.name, "approved", 50)
			assertMailboxListCount(t, handler, autoRunID, 0)

			human := rpcCall(t, handler, eventPublishBodyWithoutBundle("", "thing.created", `{"amount":250,"who":"bob"}`, "", "idem-rule-mailbox-write-human-"+tc.name))
			if human.Error != nil {
				t.Fatalf("human event.publish error = %#v", human.Error)
			}
			humanResult := asMap(t, human.Result)
			humanEventID := stringValue(t, humanResult["event_id"], "event_id")
			humanRunID := stringValue(t, humanResult["run_id"], "run_id")
			waitForConditionalRuleEntityState(t, db, humanRunID, tc.name, "awaiting_human", 250)
			waitForConditionalRuleMailboxWrite(t, handler, humanRunID, humanEventID)
		})
	}
}

func TestOperatorMailboxWriteSupportedSurfaceMissingMaterializerIsLoud(t *testing.T) {
	ctx := context.Background()
	bundle := mailboxWriteSupportedSurfaceBundle(t)
	source := semanticview.Wrap(bundle)
	fact := bundleSourceFactForTestBundle(t, bundle)
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	handler := newMailboxWriteSupportedSurfaceHandler(t, ctx, sqliteStore, sqliteStore.DB, source, fact, nil)

	published := rpcCall(t, handler, eventPublishBodyWithoutBundle("", "thing.created", `{"amount":250,"who":"alice"}`, "", "idem-mailbox-write-missing-materializer"))
	if published.Error != nil {
		t.Fatalf("event.publish missing materializer should return with diagnostic receipt, got %#v", published.Error)
	}
	outcome, reason, errText := waitForSQLitePipelineReceipt(t, sqliteStore.DB)
	if outcome != "dead_letter" || reason != "pipeline_error" || !strings.Contains(errText, "mailbox_write requires mailbox materialization store") {
		t.Fatalf("sqlite pipeline receipt = outcome:%q reason:%q error:%q, want loud mailbox materializer failure", outcome, reason, errText)
	}
}

func newMailboxWriteSupportedSurfaceHandler(
	t *testing.T,
	_ context.Context,
	persistence any,
	db *sql.DB,
	source semanticview.Source,
	fact runtimecorrelation.BundleSourceFact,
	materializer runtimepipeline.MailboxWriteMaterializationStore,
) *Handler {
	t.Helper()
	var coordinator *runtimepipeline.PipelineCoordinator
	bus, err := runtimebus.NewEventBusWithOptions(persistence.(runtimebus.EventStore), runtimebus.EventBusOptions{
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
	module := newRunCompletionSystemNodeModule(t, source)
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	if _, ok := persistence.(*store.SQLiteRuntimeStore); ok {
		workflowStore = runtimepipeline.NewSQLiteWorkflowInstanceStore(db)
	}
	coordinator = runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
		Module:                  module,
		WorkflowStore:           workflowStore,
		MailboxMaterializer:     materializer,
		EventReceiptsCapability: eventReceiptsCapability(persistence),
		BundleFingerprint:       fact.BundleFingerprint,
	})
	bus.RegisterRuntimeActiveAgentDescriptor(runtimebus.ActiveAgentDescriptor{AgentID: "workflow-runtime"})
	bus.Subscribe("workflow-runtime", events.EventType("thing.created"))
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
	return testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:              func() time.Time { return time.Now().UTC() },
			Ready:            func() bool { return true },
			Database:         fakePinger{},
			Runs:             runs,
			Observability:    observability,
			Idempotency:      idempotency,
			Events:           bus,
			Source:           source,
			RunBundleContext: runBundleContext,
			Mailbox:          mailbox,
			Bundle: runtimecontracts.BundleIdentity{
				WorkflowName:    source.WorkflowName(),
				WorkflowVersion: source.WorkflowVersion(),
				Fingerprint:     fact.BundleFingerprint,
				BundleHash:      fact.BundleHash,
			},
		}),
	})
}

func eventReceiptsCapability(persistence any) func(context.Context) (bool, error) {
	provider, ok := persistence.(interface {
		CanonicalEventReceiptsCapability(context.Context) (bool, error)
	})
	if !ok || provider == nil {
		return nil
	}
	return provider.CanonicalEventReceiptsCapability
}

func waitForMailboxWriteSupportedSurface(t *testing.T, handler *Handler, db *sql.DB, runID, eventID, backend string) {
	t.Helper()
	requireAPIV1Convergence(t, fmt.Sprintf("mailbox_write supported surface for %s", backend), func() (bool, error) {
		listed := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"mailbox-list","method":"mailbox.list","params":{"status":"pending","run_id":%q,"limit":10}}`, runID))
		if listed.Error != nil {
			t.Fatalf("mailbox.list error = %#v", listed.Error)
		}
		items := asSlice(t, asMap(t, listed.Result)["items"])
		if len(items) == 1 {
			item := asMap(t, items[0])
			if err := assertMailboxWriteSupportedSurfaceItem(t, handler, item, runID, eventID); err != nil {
				return false, err
			}
			assertMailboxWriteEntityState(t, db, runID, backend)
			return true, nil
		}
		return false, fmt.Errorf("mailbox.list returned %d items", len(items))
	})
}

func waitForConditionalRuleMailboxWrite(t *testing.T, handler *Handler, runID, eventID string) {
	t.Helper()
	requireAPIV1Convergence(t, "rule mailbox_write supported surface", func() (bool, error) {
		listed := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"rule-mailbox-list","method":"mailbox.list","params":{"status":"pending","run_id":%q,"limit":10}}`, runID))
		if listed.Error != nil {
			t.Fatalf("mailbox.list error = %#v", listed.Error)
		}
		items := asSlice(t, asMap(t, listed.Result)["items"])
		if len(items) == 1 {
			item := asMap(t, items[0])
			if err := assertConditionalRuleMailboxItem(t, handler, item, runID, eventID); err != nil {
				return false, err
			}
			return true, nil
		}
		return false, fmt.Errorf("mailbox.list returned %d items", len(items))
	})
}

func assertConditionalRuleMailboxItem(t *testing.T, handler *Handler, item map[string]any, runID, eventID string) error {
	t.Helper()
	mailboxID := stringValue(t, item["mailbox_id"], "mailbox_id")
	if item["type"] != "approval" || item["status"] != "pending" || item["priority"] != "normal" || item["source_event_id"] != eventID || item["source_entity_id"] != runID {
		return fmt.Errorf("mailbox.list item = %#v, want approval pending normal for source event/entity", item)
	}
	payload := asMap(t, item["payload"])
	if payload["who"] != "bob" || payload["amount"] != float64(250) || payload["review_kind"] != "conditional" {
		return fmt.Errorf("mailbox.list payload = %#v, want selected rule payload", payload)
	}
	detail := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"rule-mailbox-get","method":"mailbox.get","params":{"mailbox_id":%q}}`, mailboxID))
	if detail.Error != nil {
		return fmt.Errorf("mailbox.get error = %#v", detail.Error)
	}
	detailPayload := asMap(t, asMap(t, detail.Result)["payload"])
	if detailPayload["who"] != "bob" || detailPayload["amount"] != float64(250) || detailPayload["review_kind"] != "conditional" {
		return fmt.Errorf("mailbox.get payload = %#v, want selected rule payload", detailPayload)
	}
	return nil
}

func assertMailboxListCount(t *testing.T, handler *Handler, runID string, want int) {
	t.Helper()
	listed := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"mailbox-list-count","method":"mailbox.list","params":{"status":"pending","run_id":%q,"limit":10}}`, runID))
	if listed.Error != nil {
		t.Fatalf("mailbox.list error = %#v", listed.Error)
	}
	items := asSlice(t, asMap(t, listed.Result)["items"])
	if len(items) != want {
		t.Fatalf("mailbox.list returned %d items for run %s, want %d: %#v", len(items), runID, want, items)
	}
}

func assertMailboxWriteSupportedSurfaceItem(t *testing.T, handler *Handler, item map[string]any, runID, eventID string) error {
	t.Helper()
	mailboxID := stringValue(t, item["mailbox_id"], "mailbox_id")
	if item["type"] != "review_request" || item["status"] != "pending" || item["priority"] != "high" || item["source_event_id"] != eventID || item["source_entity_id"] != runID {
		return fmt.Errorf("mailbox.list item = %#v, want review_request pending high for source event/entity", item)
	}
	payload := asMap(t, item["payload"])
	if payload["who"] != "alice" || payload["amount"] != float64(250) || payload["review_kind"] != "validation" {
		return fmt.Errorf("mailbox.list payload = %#v, want materialized handler payload", payload)
	}
	detail := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"mailbox-get","method":"mailbox.get","params":{"mailbox_id":%q}}`, mailboxID))
	if detail.Error != nil {
		return fmt.Errorf("mailbox.get error = %#v", detail.Error)
	}
	detailPayload := asMap(t, asMap(t, detail.Result)["payload"])
	if detailPayload["who"] != "alice" || detailPayload["amount"] != float64(250) || detailPayload["review_kind"] != "validation" {
		return fmt.Errorf("mailbox.get payload = %#v, want materialized handler payload", detailPayload)
	}
	return nil
}

func assertMailboxWriteEntityState(t *testing.T, db *sql.DB, runID, backend string) {
	t.Helper()
	state, fields, err := loadMailboxWriteEntityState(t, db, runID, backend)
	if err != nil {
		t.Fatalf("load %s entity_state: %v", backend, err)
	}
	if state != "done" {
		t.Fatalf("%s entity state = %q, want done", backend, state)
	}
	if fields["who"] != "alice" || fields["amount"] != float64(250) {
		t.Fatalf("%s entity fields = %#v, want accumulated payload", backend, fields)
	}
}

func waitForConditionalRuleEntityState(t *testing.T, db *sql.DB, runID, backend, wantState string, wantAmount int) {
	t.Helper()
	requireAPIV1Convergence(t, fmt.Sprintf("%s entity state to %s", backend, wantState), func() (bool, error) {
		state, fields, err := loadMailboxWriteEntityState(t, db, runID, backend)
		if err == nil {
			if state == wantState && fields["amount"] == float64(wantAmount) {
				return true, nil
			}
			return false, fmt.Errorf("state=%q fields=%#v", state, fields)
		}
		return false, err
	})
}

func loadMailboxWriteEntityState(t *testing.T, db *sql.DB, runID, backend string) (string, map[string]any, error) {
	t.Helper()
	var state string
	var fieldsRaw []byte
	switch backend {
	case "sqlite_default_no_selector":
		if err := db.QueryRow(`
				SELECT current_state, fields
				FROM entity_state
				WHERE run_id = ?
			`, runID).Scan(&state, &fieldsRaw); err != nil {
			return "", nil, err
		}
	default:
		if err := db.QueryRow(`
				SELECT current_state, fields
				FROM entity_state
				WHERE run_id = $1::uuid
			`, runID).Scan(&state, &fieldsRaw); err != nil {
			return "", nil, err
		}
	}
	return state, decodeJSONMap(t, json.RawMessage(fieldsRaw)), nil
}

func waitForSQLitePipelineReceipt(t *testing.T, db *sql.DB) (string, string, string) {
	t.Helper()
	var outcome, reason, errText string
	requireAPIV1Convergence(t, "sqlite pipeline receipt", func() (bool, error) {
		var gotOutcome, gotReason, gotErrText string
		if err := db.QueryRow(`
			SELECT outcome, COALESCE(reason_code, ''), COALESCE(json_extract(side_effects, '$.error'), '')
			FROM event_receipts
			WHERE subscriber_type = 'platform' AND subscriber_id = 'pipeline'
			ORDER BY processed_at DESC
			LIMIT 1
		`).Scan(&gotOutcome, &gotReason, &gotErrText); err == nil {
			// Assign only after Scan succeeds so callers never observe partial values.
			outcome, reason, errText = gotOutcome, gotReason, gotErrText
			return true, nil
		} else {
			return false, err
		}
	})
	return outcome, reason, errText
}

func bundleSourceFactForTestBundle(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle) runtimecorrelation.BundleSourceFact {
	t.Helper()
	if bundle == nil {
		t.Fatal("test bundle is nil")
	}
	return runtimecorrelation.BundleSourceFact{
		BundleHash:        mailboxWriteSupportedSurfaceBundleHash,
		BundleSource:      storerunlifecycle.BundleSourceEphemeral,
		BundleFingerprint: mailboxWriteSupportedSurfaceFingerprint,
	}
}

func mailboxWriteSupportedSurfaceBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	handler := runtimecontracts.SystemNodeEventHandler{
		CreateEntity: true,
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "amount", Value: runtimecontracts.RefExpression("payload.amount")},
				{TargetField: "who", Value: runtimecontracts.RefExpression("payload.who")},
			},
		},
		AdvancesTo: "done",
		Action: runtimecontracts.ActionSpec{
			ID: "mailbox_write",
			Mailbox: &runtimecontracts.MailboxWriteSpec{
				ItemType: runtimecontracts.LiteralExpression("review_request"),
				Severity: runtimecontracts.LiteralExpression("urgent"),
				Summary:  runtimecontracts.LiteralExpression("Review validation package"),
				EntityID: runtimecontracts.RefExpression("event.entity_id"),
				Payload: map[string]runtimecontracts.ExpressionValue{
					"review_kind": runtimecontracts.LiteralExpression("validation"),
					"who":         runtimecontracts.RefExpression("payload.who"),
					"amount":      runtimecontracts.RefExpression("payload.amount"),
				},
			},
		},
	}
	return &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:         "mailbox-write-supported-surface",
			Version:      "1.0.0",
			InitialStage: "new",
			Stages: []runtimecontracts.WorkflowStageContract{
				{ID: "new"},
				{ID: "done"},
			},
			TerminalStages: []string{"done"},
			Transitions: []runtimecontracts.WorkflowTransitionContract{{
				ID:      "reviewer-completes-thing",
				From:    []string{"new"},
				To:      "done",
				Trigger: "thing.created",
				Node:    "reviewer",
			}},
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"reviewer": {"thing.created": handler},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"thing.created": {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"reviewer": {
				ID:            "reviewer",
				ExecutionType: "system_node",
				SubscribesTo:  []string{"thing.created"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"thing.created": handler,
				},
			},
		},
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Name:           "mailbox-write-supported-surface",
			InitialState:   "new",
			TerminalStates: []string{"done"},
			States:         []string{"new", "done"},
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"thing.created"}},
			},
		},
	}
}

func conditionalRuleMailboxWriteSupportedSurfaceBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	handler := runtimecontracts.SystemNodeEventHandler{
		CreateEntity: true,
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "amount", Value: runtimecontracts.RefExpression("payload.amount")},
				{TargetField: "who", Value: runtimecontracts.RefExpression("payload.who")},
			},
		},
		Rules: []runtimecontracts.HandlerRuleEntry{
			{
				ID:         "auto_approve",
				Condition:  "payload.amount < 100",
				AdvancesTo: "approved",
			},
			{
				ID:         "needs_human",
				Condition:  "payload.amount >= 100",
				AdvancesTo: "awaiting_human",
				Action: runtimecontracts.ActionSpec{
					ID: "mailbox_write",
					Mailbox: &runtimecontracts.MailboxWriteSpec{
						ItemType: runtimecontracts.LiteralExpression("approval"),
						Summary:  runtimecontracts.LiteralExpression("Review refund"),
						EntityID: runtimecontracts.RefExpression("event.entity_id"),
						Payload: map[string]runtimecontracts.ExpressionValue{
							"review_kind": runtimecontracts.LiteralExpression("conditional"),
							"who":         runtimecontracts.RefExpression("payload.who"),
							"amount":      runtimecontracts.RefExpression("payload.amount"),
						},
					},
				},
			},
		},
	}
	return &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:         "rule-mailbox-write-supported-surface",
			Version:      "1.0.0",
			InitialStage: "new",
			Stages: []runtimecontracts.WorkflowStageContract{
				{ID: "new"},
				{ID: "approved"},
				{ID: "awaiting_human"},
			},
			TerminalStages: []string{"approved", "awaiting_human"},
			Transitions: []runtimecontracts.WorkflowTransitionContract{
				{
					ID:      "auto-approve",
					From:    []string{"new"},
					To:      "approved",
					Trigger: "thing.created",
					Node:    "reviewer",
				},
				{
					ID:      "needs-human",
					From:    []string{"new"},
					To:      "awaiting_human",
					Trigger: "thing.created",
					Node:    "reviewer",
					Actions: []string{"mailbox_write"},
				},
			},
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"reviewer": {"thing.created": handler},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"thing.created": {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"reviewer": {
				ID:            "reviewer",
				ExecutionType: "system_node",
				SubscribesTo:  []string{"thing.created"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"thing.created": handler,
				},
			},
		},
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Name:           "rule-mailbox-write-supported-surface",
			InitialState:   "new",
			TerminalStates: []string{"approved", "awaiting_human"},
			States:         []string{"new", "approved", "awaiting_human"},
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"thing.created"}},
			},
		},
	}
}
