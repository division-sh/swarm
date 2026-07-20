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
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest"
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
		setup func(*testing.T, context.Context, semanticview.Source, runtimecorrelation.BundleSourceFact) (*Handler, *sql.DB, *runtimebus.EventBus, *runtimelifecycleprobe.Probe)
	}{
		{
			name: "sqlite_default_no_selector",
			setup: func(t *testing.T, ctx context.Context, source semanticview.Source, fact runtimecorrelation.BundleSourceFact) (*Handler, *sql.DB, *runtimebus.EventBus, *runtimelifecycleprobe.Probe) {
				t.Helper()
				sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
				probe := runtimelifecycleprobe.New()
				handler, bus := newMailboxWriteSupportedSurfaceHandler(t, ctx, sqliteStore, sqliteStore.DB, source, fact, sqliteStore, probe)
				return handler, sqliteStore.DB, bus, probe
			},
		},
		{
			name: "postgres_explicit_opt_in",
			setup: func(t *testing.T, _ context.Context, source semanticview.Source, fact runtimecorrelation.BundleSourceFact) (*Handler, *sql.DB, *runtimebus.EventBus, *runtimelifecycleprobe.Probe) {
				t.Helper()
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				pg := storetest.AdmitPostgresRuntimeStore(t, db)
				probe := runtimelifecycleprobe.New()
				handler, bus := newMailboxWriteSupportedSurfaceHandler(t, context.Background(), pg, db, source, fact, pg, probe)
				return handler, db, bus, probe
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := mailboxWriteSupportedSurfaceBundle(t)
			source := semanticview.Wrap(bundle)
			fact := bundleSourceFactForTestBundle(t, bundle)
			handler, db, bus, probe := tc.setup(t, ctx, source, fact)

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
				subscriberType := fmt.Sprint(delivery["subscriber_type"])
				subscriberID := fmt.Sprint(delivery["subscriber_id"])
				status := fmt.Sprint(delivery["status"])
				if strings.TrimSpace(stringValue(t, delivery["delivery_id"], "delivery_id")) == "" || !validEventPublishSubscriberType(subscriberType) {
					t.Fatalf("event.publish delivery identity = %#v, want persisted typed delivery identity", delivery)
				}
				switch subscriberID {
				case "workflow-runtime":
					seenWorkflowRuntime = subscriberType == "agent" && status == "pending"
				case "reviewer":
					seenReviewer = subscriberType == "node" && (status == "pending" || status == "in_progress" || status == "delivered")
				}
			}
			if !seenWorkflowRuntime || !seenReviewer {
				t.Fatalf("event.publish deliveries = %#v, want durable workflow-runtime and reviewer node snapshot", deliveries)
			}

			releaseMailboxWritePendingNodeDeliveries(t, db, bus, probe, tc.name, eventID)
			waitForMailboxWriteSupportedSurface(t, handler, db, bus, runID, eventID, tc.name)
		})
	}
}

func TestOperatorRuleMailboxWriteSupportedSurfaceIsBranchScopedAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		setup func(*testing.T, context.Context, semanticview.Source, runtimecorrelation.BundleSourceFact) (*Handler, *sql.DB, *runtimebus.EventBus, *runtimelifecycleprobe.Probe)
	}{
		{
			name: "sqlite_default_no_selector",
			setup: func(t *testing.T, ctx context.Context, source semanticview.Source, fact runtimecorrelation.BundleSourceFact) (*Handler, *sql.DB, *runtimebus.EventBus, *runtimelifecycleprobe.Probe) {
				t.Helper()
				sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
				probe := runtimelifecycleprobe.New()
				handler, bus := newMailboxWriteSupportedSurfaceHandler(t, ctx, sqliteStore, sqliteStore.DB, source, fact, sqliteStore, probe)
				return handler, sqliteStore.DB, bus, probe
			},
		},
		{
			name: "postgres_explicit_opt_in",
			setup: func(t *testing.T, _ context.Context, source semanticview.Source, fact runtimecorrelation.BundleSourceFact) (*Handler, *sql.DB, *runtimebus.EventBus, *runtimelifecycleprobe.Probe) {
				t.Helper()
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				pg := storetest.AdmitPostgresRuntimeStore(t, db)
				probe := runtimelifecycleprobe.New()
				handler, bus := newMailboxWriteSupportedSurfaceHandler(t, context.Background(), pg, db, source, fact, pg, probe)
				return handler, db, bus, probe
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := conditionalRuleMailboxWriteSupportedSurfaceBundle(t)
			source := semanticview.Wrap(bundle)
			fact := bundleSourceFactForTestBundle(t, bundle)
			handler, db, bus, probe := tc.setup(t, ctx, source, fact)

			auto := rpcCall(t, handler, eventPublishBodyWithoutBundle("", "thing.created", `{"amount":50,"who":"alice"}`, "", "idem-rule-mailbox-write-auto-"+tc.name))
			if auto.Error != nil {
				t.Fatalf("auto event.publish error = %#v", auto.Error)
			}
			autoResult := asMap(t, auto.Result)
			autoEventID := stringValue(t, autoResult["event_id"], "event_id")
			autoRunID := stringValue(t, autoResult["run_id"], "run_id")
			releaseMailboxWritePendingNodeDeliveries(t, db, bus, probe, tc.name, autoEventID)
			waitForConditionalRuleEntityState(t, db, autoRunID, tc.name, "approved", 50)
			assertMailboxListCount(t, handler, autoRunID, 0)

			human := rpcCall(t, handler, eventPublishBodyWithoutBundle("", "thing.created", `{"amount":250,"who":"bob"}`, "", "idem-rule-mailbox-write-human-"+tc.name))
			if human.Error != nil {
				t.Fatalf("human event.publish error = %#v", human.Error)
			}
			humanResult := asMap(t, human.Result)
			humanEventID := stringValue(t, humanResult["event_id"], "event_id")
			humanRunID := stringValue(t, humanResult["run_id"], "run_id")
			releaseMailboxWritePendingNodeDeliveries(t, db, bus, probe, tc.name, humanEventID)
			waitForConditionalRuleMailboxWrite(t, handler, db, bus, humanRunID, humanEventID, tc.name)
			waitForConditionalRuleEntityState(t, db, humanRunID, tc.name, "awaiting_human", 250)
		})
	}
}

func TestOperatorMailboxWriteSupportedSurfaceMissingMaterializerIsLoud(t *testing.T) {
	ctx := context.Background()
	bundle := mailboxWriteSupportedSurfaceBundle(t)
	source := semanticview.Wrap(bundle)
	fact := bundleSourceFactForTestBundle(t, bundle)
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	probe := runtimelifecycleprobe.New()
	handler, _ := newMailboxWriteSupportedSurfaceHandler(t, ctx, sqliteStore, sqliteStore.DB, source, fact, nil, probe)

	published := rpcCall(t, handler, eventPublishBodyWithoutBundle("", "thing.created", `{"amount":250,"who":"alice"}`, "", "idem-mailbox-write-missing-materializer"))
	if published.Error != nil {
		t.Fatalf("event.publish missing materializer should return with diagnostic receipt, got %#v", published.Error)
	}
	result := asMap(t, published.Result)
	eventID := stringValue(t, result["event_id"], "event_id")
	waitForSQLiteNodeMaterializerFailure(t, sqliteStore.DB, probe, eventID, "reviewer")
}

func newMailboxWriteSupportedSurfaceHandler(
	t *testing.T,
	_ context.Context,
	persistence any,
	db *sql.DB,
	source semanticview.Source,
	fact runtimecorrelation.BundleSourceFact,
	materializer runtimepipeline.MailboxWriteMaterializationStore,
	probe *runtimelifecycleprobe.Probe,
) (*Handler, *runtimebus.EventBus) {
	t.Helper()
	var coordinator *runtimepipeline.PipelineCoordinator
	workOwner := newAPITestRuntimeWorkOccurrence(t, authorActivityTestRuntimeInstanceID, fact.BundleHash)
	bus, err := newScopedAPITestEventBus(t, persistence.(runtimebus.EventStore), runtimebus.EventBusOptions{
		ContractBundle:     source,
		BundleFingerprint:  fact.BundleFingerprint,
		BundleSourceFact:   fact,
		WorkOwner:          workOwner,
		TestLifecycleProbe: probe,
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
	if sqliteStore, ok := persistence.(*store.SQLiteRuntimeStore); ok {
		workflowStore = runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, sqliteStore)
	}
	coordinator = runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
		Module:              module,
		WorkflowStore:       workflowStore,
		MailboxMaterializer: materializer,
		BundleHash:          fact.BundleHash,
		TestLifecycleProbe:  probe,
	})
	bus.RegisterRuntimeActiveAgentDescriptor(runtimebus.ActiveAgentDescriptor{AgentID: "workflow-runtime"})
	workflowDeliveries := bus.Subscribe("workflow-runtime", events.EventType("thing.created"))
	workerOwner := worklifetime.NewProcess()
	workerLease, err := workerOwner.Begin(context.Background())
	if err != nil {
		t.Fatalf("admit workflow runtime test carrier: %v", err)
	}
	stopWorker := make(chan struct{})
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		defer workerLease.Done()
		for {
			select {
			case <-stopWorker:
				return
			case delivery := <-workflowDeliveries:
				if delivery != nil {
					_ = delivery.Complete()
				}
			}
		}
	}()
	t.Cleanup(func() {
		close(stopWorker)
		<-workerDone
		workerOwner.Retire()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := workerOwner.Join(ctx); err != nil {
			t.Errorf("join workflow runtime test carrier: %v", err)
		}
	})
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
	return handler, bus
}

func releaseMailboxWritePendingNodeDeliveries(t *testing.T, db *sql.DB, bus *runtimebus.EventBus, probe *runtimelifecycleprobe.Probe, backend, eventID string) {
	t.Helper()
	if bus == nil {
		t.Fatalf("%s runtime bus is required to release pending node deliveries", backend)
	}
	nodeID := mailboxWriteNodeDeliverySubscriberID(t, db, backend, eventID)
	waitForMailboxWriteLifecycleDeliveryStatus(t, probe, backend, eventID, nodeID, "pending")
	if status := mailboxWriteNodeDeliveryStatus(t, db, backend, eventID, nodeID); status == "pending" {
		evt := loadMailboxWritePersistedEvent(t, db, backend, eventID)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := bus.ReleasePendingPersistedDeliveriesForEvent(ctx, evt); err != nil {
			t.Fatalf("%s release pending node deliveries for event %s: %v", backend, eventID, err)
		}
	}
	waitForMailboxWriteNodeDeliveryStatus(t, db, backend, eventID, nodeID, "delivered")
}

func waitForMailboxWriteLifecycleDeliveryStatus(t *testing.T, probe *runtimelifecycleprobe.Probe, backend, eventID, nodeID, status string) {
	t.Helper()
	if probe == nil {
		t.Fatalf("%s lifecycle probe is required for node delivery %s on event %s node %s", backend, status, eventID, nodeID)
	}
	lifecycletest.Wrap(t, probe, lifecycletest.WithTimeout(apiv1ConvergenceTimeout)).
		RequireNodeStatus(eventID, nodeID, status)
}

func waitForMailboxWriteNodeDeliveryStatus(t *testing.T, db *sql.DB, backend, eventID, nodeID, wantStatus string) {
	t.Helper()
	requireAPIV1Convergence(t, fmt.Sprintf("%s node delivery %s/%s status %s", backend, eventID, nodeID, wantStatus), func() (bool, error) {
		status := mailboxWriteNodeDeliveryStatus(t, db, backend, eventID, nodeID)
		if status == wantStatus {
			return true, nil
		}
		return false, fmt.Errorf("delivery status = %q, want %q", status, wantStatus)
	})
}

func mailboxWriteNodeDeliveryStatus(t *testing.T, db *sql.DB, backend, eventID, nodeID string) string {
	t.Helper()
	if strings.TrimSpace(eventID) == "" || strings.TrimSpace(nodeID) == "" {
		t.Fatalf("%s node delivery status lookup requires event id and node id", backend)
	}
	sqlText := ""
	args := []any{eventID, nodeID}
	if strings.HasPrefix(backend, "sqlite") {
		sqlText = `SELECT status FROM event_deliveries WHERE event_id = ? AND subscriber_type = 'node' AND subscriber_id = ?`
	} else {
		sqlText = `SELECT status FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2`
	}
	status := ""
	if err := db.QueryRowContext(context.Background(), sqlText, args...).Scan(&status); err != nil {
		t.Fatalf("%s node delivery status lookup for %s/%s: %v", backend, eventID, nodeID, err)
	}
	return status
}

func mailboxWriteNodeDeliverySubscriberID(t *testing.T, db *sql.DB, backend, eventID string) string {
	t.Helper()
	if strings.TrimSpace(eventID) == "" {
		t.Fatalf("%s node delivery subscriber lookup requires event id", backend)
	}
	sqlText := ""
	args := []any{eventID, eventReplayScopeSubscriberID}
	if strings.HasPrefix(backend, "sqlite") {
		sqlText = `SELECT subscriber_id FROM event_deliveries WHERE event_id = ? AND subscriber_type = 'node' AND subscriber_id <> ? ORDER BY subscriber_id LIMIT 1`
	} else {
		sqlText = `SELECT subscriber_id FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id <> $2 ORDER BY subscriber_id LIMIT 1`
	}
	nodeID := ""
	if err := db.QueryRowContext(context.Background(), sqlText, args...).Scan(&nodeID); err != nil {
		t.Fatalf("%s node delivery subscriber lookup for %s: %v", backend, eventID, err)
	}
	if strings.TrimSpace(nodeID) == "" {
		t.Fatalf("%s node delivery subscriber lookup for %s returned empty subscriber_id", backend, eventID)
	}
	return nodeID
}

func loadMailboxWritePersistedEvent(t *testing.T, db *sql.DB, backend, eventID string) events.Event {
	t.Helper()
	sqlText := ""
	if strings.HasPrefix(backend, "sqlite") {
		sqlText = `
			SELECT event_id, COALESCE(run_id, ''), event_name, COALESCE(produced_by, ''),
			       COALESCE(entity_id, ''), COALESCE(flow_instance, ''), COALESCE(scope, 'global'),
			       payload, created_at, COALESCE(source_event_id, ''),
			       COALESCE(source_route, '{}'), COALESCE(target_route, '{}'), COALESCE(target_set, '[]')
			FROM events
			WHERE event_id = ?
		`
	} else {
		sqlText = `
			SELECT event_id::text, COALESCE(run_id::text, ''), event_name, COALESCE(produced_by, ''),
			       COALESCE(entity_id::text, ''), COALESCE(flow_instance, ''), COALESCE(scope, 'global'),
			       payload, created_at, COALESCE(source_event_id::text, ''),
			       COALESCE(source_route, '{}'::jsonb), COALESCE(target_route, '{}'::jsonb), COALESCE(target_set, '[]'::jsonb)
			FROM events
			WHERE event_id = $1::uuid
		`
	}
	var id, runID, eventName, producedBy, entityID, flowInstance, scope, sourceEventID string
	var payloadRaw, createdAtRaw, sourceRouteRaw, targetRouteRaw, targetSetRaw any
	if err := db.QueryRowContext(context.Background(), sqlText, eventID).Scan(
		&id,
		&runID,
		&eventName,
		&producedBy,
		&entityID,
		&flowInstance,
		&scope,
		&payloadRaw,
		&createdAtRaw,
		&sourceEventID,
		&sourceRouteRaw,
		&targetRouteRaw,
		&targetSetRaw,
	); err != nil {
		t.Fatalf("%s load event %s: %v", backend, eventID, err)
	}
	return eventtest.PersistedProjection(
		id,
		events.EventType(eventName),
		producedBy,
		"",
		mailboxWriteDBJSON(payloadRaw, "{}"),
		0,
		runID,
		sourceEventID,
		mailboxWriteDBEnvelope(t, entityID, flowInstance, scope, sourceRouteRaw, targetRouteRaw, targetSetRaw),
		mailboxWriteDBTime(createdAtRaw))

}

func mailboxWriteDBEnvelope(t *testing.T, entityID, flowInstance, scope string, sourceRouteRaw, targetRouteRaw, targetSetRaw any) events.EventEnvelope {
	t.Helper()
	envelope := events.EventEnvelope{
		EntityID:     strings.TrimSpace(entityID),
		FlowInstance: strings.Trim(strings.TrimSpace(flowInstance), "/"),
		Scope:        events.EventScope(strings.TrimSpace(scope)),
	}
	if err := json.Unmarshal(mailboxWriteDBJSON(sourceRouteRaw, "{}"), &envelope.Source); err != nil {
		t.Fatalf("decode source_route: %v", err)
	}
	if err := json.Unmarshal(mailboxWriteDBJSON(targetRouteRaw, "{}"), &envelope.Target); err != nil {
		t.Fatalf("decode target_route: %v", err)
	}
	if err := json.Unmarshal(mailboxWriteDBJSON(targetSetRaw, "[]"), &envelope.TargetSet); err != nil {
		t.Fatalf("decode target_set: %v", err)
	}
	return envelope.Normalized()
}

func mailboxWriteDBJSON(raw any, fallback string) json.RawMessage {
	switch v := raw.(type) {
	case nil:
		return json.RawMessage(fallback)
	case json.RawMessage:
		if len(v) == 0 {
			return json.RawMessage(fallback)
		}
		return v
	case []byte:
		if len(v) == 0 {
			return json.RawMessage(fallback)
		}
		return json.RawMessage(v)
	case string:
		if strings.TrimSpace(v) == "" {
			return json.RawMessage(fallback)
		}
		return json.RawMessage(v)
	default:
		encoded, err := json.Marshal(v)
		if err != nil || len(encoded) == 0 {
			return json.RawMessage(fallback)
		}
		return json.RawMessage(encoded)
	}
}

func mailboxWriteDBTime(raw any) time.Time {
	switch v := raw.(type) {
	case time.Time:
		return v.UTC()
	case []byte:
		return mailboxWriteParseDBTime(string(v))
	case string:
		return mailboxWriteParseDBTime(v)
	default:
		return time.Now().UTC()
	}
}

func mailboxWriteParseDBTime(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
	} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.UTC()
		}
	}
	return time.Now().UTC()
}

func waitForMailboxWriteBusQuiescence(t *testing.T, bus *runtimebus.EventBus, description string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := drainMailboxWriteBus(ctx, bus); err != nil {
		t.Fatalf("%s bus drain: %v", description, err)
	}
}

func waitForMailboxWriteSupportedSurface(t *testing.T, handler *Handler, db *sql.DB, bus *runtimebus.EventBus, runID, eventID, backend string) {
	t.Helper()
	requireAPIV1Convergence(t, fmt.Sprintf("mailbox_write supported surface for %s", backend), func() (bool, error) {
		if err := drainMailboxWriteBusPoll(bus); err != nil {
			return false, err
		}
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

func waitForConditionalRuleMailboxWrite(t *testing.T, handler *Handler, db *sql.DB, bus *runtimebus.EventBus, runID, eventID, backend string) {
	t.Helper()
	requireAPIV1Convergence(t, "rule mailbox_write supported surface", func() (bool, error) {
		if err := drainMailboxWriteBusPoll(bus); err != nil {
			return false, err
		}
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
		return false, fmt.Errorf("mailbox.list returned %d items\n%s", len(items), mailboxWriteDebugSummary(t, db, backend, runID, eventID))
	})
}

func drainMailboxWriteBusPoll(bus *runtimebus.EventBus) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return drainMailboxWriteBus(ctx, bus)
}

func drainMailboxWriteBus(ctx context.Context, bus *runtimebus.EventBus) error {
	if bus == nil {
		return nil
	}
	for i := 0; i < 4; i++ {
		if err := bus.WaitForQuiescence(ctx); err != nil {
			return err
		}
		swept, err := bus.SweepUndispatched(ctx, time.Hour, 10)
		if err != nil {
			return err
		}
		if swept == 0 {
			return bus.WaitForQuiescence(ctx)
		}
	}
	return bus.WaitForQuiescence(ctx)
}

func mailboxWriteDebugSummary(t *testing.T, db *sql.DB, backend, runID, eventID string) string {
	t.Helper()
	sections := []string{
		mailboxWriteDebugQuery(t, db, backend, "entity_state", runID, eventID),
		mailboxWriteDebugQuery(t, db, backend, "events", runID, eventID),
		mailboxWriteDebugQuery(t, db, backend, "event_deliveries", runID, eventID),
		mailboxWriteDebugQuery(t, db, backend, "event_receipts", runID, eventID),
		mailboxWriteDebugQuery(t, db, backend, "mailbox", runID, eventID),
	}
	return strings.Join(sections, "\n")
}

func mailboxWriteDebugQuery(t *testing.T, db *sql.DB, backend, scope, runID, eventID string) string {
	t.Helper()
	sqlText := ""
	args := []any{runID, eventID}
	if backend == "sqlite_default_no_selector" {
		switch scope {
		case "entity_state":
			sqlText = `SELECT entity_id, current_state, fields FROM entity_state WHERE run_id = ? ORDER BY created_at, entity_id LIMIT 5`
			args = []any{runID}
		case "events":
			sqlText = `SELECT event_id, event_name, COALESCE(entity_id, '') FROM events WHERE run_id = ? OR event_id = ? ORDER BY created_at, event_id LIMIT 8`
		case "event_deliveries":
			sqlText = `SELECT event_id, subscriber_type, subscriber_id, status, COALESCE(reason_code, '') FROM event_deliveries WHERE run_id = ? OR event_id = ? ORDER BY created_at, event_id, subscriber_id LIMIT 8`
		case "event_receipts":
			sqlText = `SELECT r.event_id, r.subscriber_type, r.subscriber_id, r.outcome, COALESCE(r.reason_code, '') FROM event_receipts r JOIN events e ON e.event_id = r.event_id WHERE e.run_id = ? OR r.event_id = ? ORDER BY r.processed_at, r.event_id LIMIT 8`
		case "mailbox":
			sqlText = `SELECT item_id, status, item_type, source_event_id, COALESCE(entity_id, ''), payload FROM mailbox WHERE source_event_id = ? ORDER BY created_at, item_id LIMIT 5`
			args = []any{eventID}
		}
	} else {
		switch scope {
		case "entity_state":
			sqlText = `SELECT entity_id::text, current_state, fields::text FROM entity_state WHERE run_id = $1::uuid ORDER BY created_at, entity_id LIMIT 5`
			args = []any{runID}
		case "events":
			sqlText = `SELECT event_id::text, event_name, COALESCE(entity_id::text, '') FROM events WHERE run_id = $1::uuid OR event_id = $2::uuid ORDER BY created_at, event_id LIMIT 8`
		case "event_deliveries":
			sqlText = `SELECT event_id::text, subscriber_type, subscriber_id, status, COALESCE(reason_code, '') FROM event_deliveries WHERE run_id = $1::uuid OR event_id = $2::uuid ORDER BY created_at, event_id, subscriber_id LIMIT 8`
		case "event_receipts":
			sqlText = `SELECT r.event_id::text, r.subscriber_type, r.subscriber_id, r.outcome, COALESCE(r.reason_code, '') FROM event_receipts r JOIN events e ON e.event_id = r.event_id WHERE e.run_id = $1::uuid OR r.event_id = $2::uuid ORDER BY r.processed_at, r.event_id LIMIT 8`
		case "mailbox":
			sqlText = `SELECT item_id::text, status, item_type, source_event_id::text, COALESCE(entity_id::text, ''), payload::text FROM mailbox WHERE source_event_id = $1::uuid ORDER BY created_at, item_id LIMIT 5`
			args = []any{eventID}
		}
	}
	if sqlText == "" {
		return scope + ": unsupported debug query"
	}
	rows, err := db.QueryContext(context.Background(), sqlText, args...)
	if err != nil {
		return fmt.Sprintf("%s: %v", scope, err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Sprintf("%s columns: %v", scope, err)
	}
	out := []string{}
	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		scan := make([]any, len(values))
		for i := range values {
			scan[i] = &values[i]
		}
		if err := rows.Scan(scan...); err != nil {
			return fmt.Sprintf("%s scan: %v", scope, err)
		}
		cols := make([]string, len(values))
		for i, value := range values {
			if value.Valid {
				cols[i] = value.String
			}
		}
		out = append(out, fmt.Sprintf("%v", cols))
	}
	if err := rows.Err(); err != nil {
		return fmt.Sprintf("%s rows: %v", scope, err)
	}
	if len(out) == 0 {
		return scope + ": <none>"
	}
	return scope + ": " + strings.Join(out, "; ")
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

func waitForSQLiteNodeMaterializerFailure(t *testing.T, db *sql.DB, probe *runtimelifecycleprobe.Probe, eventID, nodeID string) {
	t.Helper()
	if probe == nil {
		t.Fatalf("sqlite lifecycle probe is required for node/%s materializer failure on event %s", nodeID, eventID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := probe.WaitForDeliveryStatus(ctx, eventID, "node", nodeID, "dead_letter"); err != nil {
		t.Fatalf("sqlite node/%s materializer failure lifecycle for event %s: %v", nodeID, eventID, err)
	}
	var lastStatus, lastReason, lastFailureCode, lastReceiptOutcome, lastReceiptReason, lastReceiptFailureCode string
	if err := db.QueryRow(`
			SELECT
				COALESCE(d.status, ''),
				COALESCE(d.reason_code, ''),
				COALESCE(json_extract(d.failure, '$.detail.code'), ''),
				COALESCE(r.outcome, ''),
				COALESCE(r.reason_code, ''),
				COALESCE(json_extract(r.failure, '$.detail.code'), '')
			FROM event_deliveries d
			LEFT JOIN event_receipts r
			  ON r.event_id = d.event_id
			 AND r.subscriber_type = 'node'
			 AND r.subscriber_id = d.subscriber_id
			WHERE d.event_id = ?
			  AND d.subscriber_type = 'node'
			  AND d.subscriber_id = ?
			LIMIT 1
		`, eventID, nodeID).Scan(&lastStatus, &lastReason, &lastFailureCode, &lastReceiptOutcome, &lastReceiptReason, &lastReceiptFailureCode); err != nil {
		t.Fatalf("sqlite node/%s materializer failure row for event %s: %v", nodeID, eventID, err)
	}
	if lastStatus != "dead_letter" || lastReceiptOutcome != "dead_letter" ||
		lastFailureCode == "" || lastReceiptFailureCode != lastFailureCode {
		t.Fatalf("sqlite node/%s materializer failure = delivery status:%q reason:%q failure:%q receipt outcome:%q reason:%q failure:%q, want dead_letter canonical materializer failure", nodeID, lastStatus, lastReason, lastFailureCode, lastReceiptOutcome, lastReceiptReason, lastReceiptFailureCode)
	}
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
				EntityID: runtimecontracts.RefExpression("_entity.id"),
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
						EntityID: runtimecontracts.RefExpression("_entity.id"),
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
