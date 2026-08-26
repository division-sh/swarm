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
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

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
				handler, bus := newMailboxWriteSupportedSurfaceHandler(t, ctx, sqliteStore, storetest.DatabaseForTest(sqliteStore), source, fact, sqliteStore, probe)
				return handler, storetest.DatabaseForTest(sqliteStore), bus, probe
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
			reviewerNodeID := identitytest.RootNode(t, "reviewer").Key()
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
				case reviewerNodeID:
					seenReviewer = subscriberType == "node" && (status == "pending" || status == "in_progress" || status == "delivered")
				}
			}
			if !seenWorkflowRuntime || !seenReviewer {
				t.Fatalf("event.publish deliveries = %#v, want durable workflow-runtime and reviewer node snapshot", deliveries)
			}

			releaseMailboxWritePendingNodeDeliveries(t, testAuthorActivityContextForSource(ctx, fact), db, bus, probe, tc.name, eventID)
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
				handler, bus := newMailboxWriteSupportedSurfaceHandler(t, ctx, sqliteStore, storetest.DatabaseForTest(sqliteStore), source, fact, sqliteStore, probe)
				return handler, storetest.DatabaseForTest(sqliteStore), bus, probe
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
			releaseMailboxWritePendingNodeDeliveries(t, testAuthorActivityContextForSource(ctx, fact), db, bus, probe, tc.name, autoEventID)
			waitForConditionalRuleEntityState(t, db, autoRunID, tc.name, "approved", 50)
			assertMailboxListCount(t, handler, autoRunID, 0)

			human := rpcCall(t, handler, eventPublishBodyWithoutBundle("", "thing.created", `{"amount":250,"who":"bob"}`, "", "idem-rule-mailbox-write-human-"+tc.name))
			if human.Error != nil {
				t.Fatalf("human event.publish error = %#v", human.Error)
			}
			humanResult := asMap(t, human.Result)
			humanEventID := stringValue(t, humanResult["event_id"], "event_id")
			humanRunID := stringValue(t, humanResult["run_id"], "run_id")
			releaseMailboxWritePendingNodeDeliveries(t, testAuthorActivityContextForSource(ctx, fact), db, bus, probe, tc.name, humanEventID)
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
	handler, _ := newMailboxWriteSupportedSurfaceHandler(t, ctx, sqliteStore, storetest.DatabaseForTest(sqliteStore), source, fact, nil, probe)

	published := rpcCall(t, handler, eventPublishBodyWithoutBundle("", "thing.created", `{"amount":250,"who":"alice"}`, "", "idem-mailbox-write-missing-materializer"))
	if published.Error != nil {
		t.Fatalf("event.publish missing materializer should return with diagnostic receipt, got %#v", published.Error)
	}
	result := asMap(t, published.Result)
	eventID := stringValue(t, result["event_id"], "event_id")
	waitForSQLiteNodeMaterializerFailure(t, storetest.DatabaseForTest(sqliteStore), probe, eventID, identitytest.RootNode(t, "reviewer").Key())
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
	workOwner := newAPITestRuntimeWorkOccurrence(t, authorActivityTestRuntimeInstanceID, fact.BundleHash())
	bus, err := newScopedAPITestEventBus(t, persistence.(runtimebus.EventStore), runtimebus.EventBusOptions{
		ContractBundle:     source,
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
	workflowPersistence := runtimepipeline.NewWorkflowPersistence(persistence.(apiTestRuntimeMutationOwner))
	if sqliteStore, ok := persistence.(*store.SQLiteRuntimeStore); ok {
		workflowPersistence = runtimepipeline.NewWorkflowPersistence(sqliteStore)
	}
	deliveryOwner := persistence.(runtimedelivery.Store)
	obligationOwner := persistence.(interface {
		PipelineObligations() runtimepipelineobligation.Store
	}).PipelineObligations()
	runLifecycle := persistence.(runtimerunlifecycle.OperationOwner)
	coordinator = runtimepipeline.NewPipelineCoordinatorWithOptions(bus, completeAPITestDurableWorkflowOptions(t, persistence, bus, runtimepipeline.PipelineCoordinatorOptions{
		Module:              module,
		Persistence:         workflowPersistence,
		DeliveryStore:       deliveryOwner,
		DeliveryRuntime:     bus,
		PipelineObligations: obligationOwner,
		RunLifecycle:        runLifecycle,
		MailboxMaterializer: materializer,
		BundleSourceFact:    fact,
		TestLifecycleProbe:  probe,
	}))

	bus.RegisterRuntimeActiveAgentDescriptor(runtimebus.ActiveAgentDescriptor{
		Identity: runtimebustest.Identity(t, "workflow-runtime", ""),
	})
	workflowDeliveries := runtimebustest.Subscribe(t, bus, "workflow-runtime", events.EventType("thing.created"))
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
		Handlers: testOperatorHandlers(testOperatorCapabilities{
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
				BundleHash:      fact.BundleHash(),
			},
		}),
	})
	return handler, bus
}

func releaseMailboxWritePendingNodeDeliveries(t *testing.T, parent context.Context, db *sql.DB, bus *runtimebus.EventBus, probe *runtimelifecycleprobe.Probe, backend, eventID string) {
	t.Helper()
	if bus == nil {
		t.Fatalf("%s runtime bus is required to release pending node deliveries", backend)
	}
	nodeID := mailboxWriteNodeDeliverySubscriberID(t, db, backend, eventID)
	waitForMailboxWriteLifecycleDeliveryStatus(t, probe, backend, eventID, nodeID, "pending")
	_ = parent
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
		status, reason, failure := mailboxWriteNodeDeliveryStatus(t, db, backend, eventID, nodeID)
		if status == wantStatus {
			return true, nil
		}
		if status == "dead_letter" {
			return false, fmt.Errorf("delivery status = %q, want %q; reason=%q failure=%s; diagnostics=%s", status, wantStatus, reason, failure, mailboxWriteRuntimeDiagnostics(t, db, backend, eventID))
		}
		return false, fmt.Errorf("delivery status = %q, want %q", status, wantStatus)
	})
}

func mailboxWriteRuntimeDiagnostics(t *testing.T, db *sql.DB, backend, eventID string) string {
	t.Helper()
	query := `
		SELECT payload
		FROM events
		WHERE event_name = 'platform.runtime_log'
		  AND run_id = (SELECT run_id FROM events WHERE event_id = ?)
		ORDER BY created_at, event_id
	`
	if !strings.HasPrefix(backend, "sqlite") {
		query = `
			SELECT payload::text
			FROM events
			WHERE event_name = 'platform.runtime_log'
			  AND run_id = (SELECT run_id FROM events WHERE event_id = $1::uuid)
			ORDER BY created_at, event_id
		`
	}
	rows, err := db.QueryContext(context.Background(), query, eventID)
	if err != nil {
		return err.Error()
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return err.Error()
		}
		out = append(out, payload)
	}
	if err := rows.Err(); err != nil {
		return err.Error()
	}
	return strings.Join(out, " | ")
}

func mailboxWriteNodeDeliveryStatus(t *testing.T, db *sql.DB, backend, eventID, nodeID string) (string, string, string) {
	t.Helper()
	if strings.TrimSpace(eventID) == "" || strings.TrimSpace(nodeID) == "" {
		t.Fatalf("%s node delivery status lookup requires event id and node id", backend)
	}
	sqlText := ""
	args := []any{eventID, nodeID}
	if strings.HasPrefix(backend, "sqlite") {
		sqlText = `SELECT status, COALESCE(reason_code, ''), COALESCE(failure, '') FROM event_deliveries WHERE event_id = ? AND subscriber_type = 'node' AND subscriber_id = ?`
	} else {
		sqlText = `SELECT status, COALESCE(reason_code, ''), COALESCE(failure::text, '') FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2`
	}
	status, reason, failure := "", "", ""
	if err := db.QueryRowContext(context.Background(), sqlText, args...).Scan(&status, &reason, &failure); err != nil {
		t.Fatalf("%s node delivery status lookup for %s/%s: %v", backend, eventID, nodeID, err)
	}
	return status, reason, failure
}

func mailboxWriteNodeDeliverySubscriberID(t *testing.T, db *sql.DB, backend, eventID string) string {
	t.Helper()
	if strings.TrimSpace(eventID) == "" {
		t.Fatalf("%s node delivery subscriber lookup requires event id", backend)
	}
	sqlText := ""
	args := []any{eventID}
	if strings.HasPrefix(backend, "sqlite") {
		sqlText = `SELECT subscriber_id FROM event_deliveries WHERE event_id = ? AND subscriber_type = 'node' ORDER BY subscriber_id LIMIT 1`
	} else {
		sqlText = `SELECT subscriber_id FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_type = 'node' ORDER BY subscriber_id LIMIT 1`
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
			       COALESCE(routing_source_kind, 'absent'), COALESCE(routing_source_authority, ''),
			       COALESCE(source_route, '{}'), COALESCE(target_route, '{}'), COALESCE(target_set, '[]')
			FROM events
			WHERE event_id = ?
		`
	} else {
		sqlText = `
			SELECT event_id::text, COALESCE(run_id::text, ''), event_name, COALESCE(produced_by, ''),
			       COALESCE(entity_id::text, ''), COALESCE(flow_instance, ''), COALESCE(scope, 'global'),
			       payload, created_at, COALESCE(source_event_id::text, ''),
			       COALESCE(routing_source_kind, 'absent'), COALESCE(routing_source_authority, ''),
			       COALESCE(source_route, '{}'::jsonb), COALESCE(target_route, '{}'::jsonb), COALESCE(target_set, '[]'::jsonb)
			FROM events
			WHERE event_id = $1::uuid
		`
	}
	var id, runID, eventName, producedBy, entityID, flowInstance, scope, sourceEventID string
	var routingSourceKind, routingSourceAuthority string
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
		&routingSourceKind,
		&routingSourceAuthority,
		&sourceRouteRaw,
		&targetRouteRaw,
		&targetSetRaw,
	); err != nil {
		t.Fatalf("%s load event %s: %v", backend, eventID, err)
	}
	envelope := mailboxWriteDBEnvelope(t, entityID, flowInstance, scope, sourceRouteRaw, targetRouteRaw, targetSetRaw)
	routingSource, err := events.RestoreRoutingSource(routingSourceKind, envelope.Source, routingSourceAuthority)
	if err != nil {
		t.Fatalf("%s restore event %s routing source: %v", backend, eventID, err)
	}
	return eventtest.PersistedProjectionWithRoutingSource(
		id,
		events.EventType(eventName),
		producedBy,
		"",
		mailboxWriteDBJSON(payloadRaw, "{}"),
		0,
		runID,
		sourceEventID,
		envelope,
		routingSource,
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
		result, err := bus.SweepPipelineObligations(ctx, 10)
		if err != nil {
			return err
		}
		if result.Exhausted || result.Blocked {
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
	flowInstance := stringValue(t, item["source_flow"], "source_flow")
	if item["type"] != "approval" || item["status"] != "pending" || item["priority"] != "normal" || item["source_event_id"] != eventID || item["source_entity_id"] != runtimeflowidentity.EntityID(flowInstance) {
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
	flowInstance := stringValue(t, item["source_flow"], "source_flow")
	if item["type"] != "review_request" || item["status"] != "pending" || item["priority"] != "high" || item["source_event_id"] != eventID || item["source_entity_id"] != runtimeflowidentity.EntityID(flowInstance) {
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
	var lastStatus, lastReason, lastFailureCode, lastOutcome, lastOutcomeReason, lastOutcomeFailureCode string
	if err := db.QueryRow(`
			SELECT
				COALESCE(d.status, ''),
				COALESCE(d.reason_code, ''),
				COALESCE(json_extract(d.failure, '$.detail.code'), ''),
				COALESCE(o.outcome, ''),
				COALESCE(o.reason_code, ''),
				COALESCE(json_extract(o.failure, '$.detail.code'), '')
			FROM event_deliveries d
			LEFT JOIN event_delivery_outcomes o ON o.delivery_id = d.delivery_id
			WHERE d.event_id = ?
			  AND d.subscriber_type = 'node'
			  AND d.subscriber_id = ?
			LIMIT 1
		`, eventID, nodeID).Scan(&lastStatus, &lastReason, &lastFailureCode, &lastOutcome, &lastOutcomeReason, &lastOutcomeFailureCode); err != nil {
		t.Fatalf("sqlite node/%s materializer failure row for event %s: %v", nodeID, eventID, err)
	}
	if lastStatus != "dead_letter" || lastOutcome != "dead_letter" ||
		lastFailureCode == "" || lastOutcomeFailureCode != lastFailureCode || lastOutcomeReason != lastReason {
		t.Fatalf("sqlite node/%s materializer failure = delivery status:%q reason:%q failure:%q outcome:%q reason:%q failure:%q, want one canonical dead-letter outcome", nodeID, lastStatus, lastReason, lastFailureCode, lastOutcome, lastOutcomeReason, lastOutcomeFailureCode)
	}
}

func bundleSourceFactForTestBundle(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle) runtimecorrelation.BundleSourceFact {
	t.Helper()
	if bundle == nil {
		t.Fatal("test bundle is nil")
	}
	return mustAPITestBundleSourceFact(mailboxWriteSupportedSurfaceBundleHash)
}

func mailboxWriteSupportedSurfaceBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	return loadMailboxWriteSupportedSurfaceBundle(t, false)
}

func conditionalRuleMailboxWriteSupportedSurfaceBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	return loadMailboxWriteSupportedSurfaceBundle(t, true)
}

func loadMailboxWriteSupportedSurfaceBundle(t *testing.T, conditional bool) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	name := "mailbox-write-supported-surface"
	states := "  - new\n  - done\n"
	terminal := "  - done\n"
	handler := `      create_entity: true
      data_accumulation:
        writes:
          - source_field: amount
            target_field: amount
          - source_field: who
            target_field: who
      advances_to: done
      action:
        id: mailbox_write
        mailbox:
          item_type: {literal: review_request}
          severity: {literal: urgent}
          summary: {literal: Review validation package}
          entity_id: {ref: _entity.id}
          payload:
            review_kind: {literal: validation}
            who: {ref: payload.who}
            amount: {ref: payload.amount}
`
	if conditional {
		name = "rule-mailbox-write-supported-surface"
		states = "  - new\n  - approved\n  - awaiting_human\n"
		terminal = "  - approved\n  - awaiting_human\n"
		handler = `      create_entity: true
      data_accumulation:
        writes:
          - source_field: amount
            target_field: amount
          - source_field: who
            target_field: who
      rules:
        auto_approve:
          element_id: 00000000-0000-4000-8000-000000005101
          condition: payload.amount < 100
          advances_to: approved
        needs_human:
          element_id: 00000000-0000-4000-8000-000000005102
          condition: payload.amount >= 100
          advances_to: awaiting_human
          action:
            id: mailbox_write
            mailbox:
              item_type: {literal: approval}
              summary: {literal: Review refund}
              entity_id: {ref: _entity.id}
              payload:
                review_kind: {literal: conditional}
                who: {ref: payload.who}
                amount: {ref: payload.amount}
`
	}
	writeRunCompletionFixtureFile(t, root+"/package.yaml", "name: "+name+"\nversion: 1.0.0\n")
	writeRunCompletionFixtureFile(t, root+"/schema.yaml", "name: "+name+"\ninitial_state: new\nterminal_states:\n"+terminal+"states:\n"+states+"pins:\n  inputs:\n    events: [thing.created]\n")
	writeRunCompletionFixtureFile(t, root+"/events.yaml", "thing.created:\n  amount: integer\n  who: string\n")
	writeRunCompletionFixtureFile(t, root+"/entities.yaml", "review:\n  amount: integer\n  who: string\n")
	writeRunCompletionFixtureFile(t, root+"/nodes.yaml", "reviewer:\n  id: reviewer\n  execution_type: system_node\n  subscribes_to: [thing.created]\n  event_handlers:\n    thing.created:\n"+handler)
	repoRoot := runCompletionRepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("load mailbox write supported-surface bundle: %v", err)
	}
	return bundle
}
