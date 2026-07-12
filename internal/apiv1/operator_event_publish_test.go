package apiv1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestOperatorEventPublishHandlersPersistEventReportDeliveriesAndReplayIdempotency(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &store.PostgresStore{DB: db}
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := runtimebus.NewEventBusWithOptions(pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	seedActiveAPIV1RuntimeBusAgent(t, context.Background(), pg, "scan-orchestrator")
	ch := bus.Subscribe("scan-orchestrator", events.EventType("scan.requested"))
	defer bus.Unsubscribe("scan-orchestrator")
	handler := eventPublishTestHandler(t, pg, bus, source)
	body := eventPublishBody("", runStartTestFingerprint, "scan.requested", `{"topic":"medicine"}`, "", "idem-publish")

	published := rpcCall(t, handler, body)
	if published.Error != nil {
		t.Fatalf("event.publish error = %#v", published.Error)
	}
	result := asMap(t, published.Result)
	eventID := stringValue(t, result["event_id"], "event_id")
	runID := stringValue(t, result["run_id"], "run_id")
	if _, err := uuid.Parse(eventID); err != nil {
		t.Fatalf("event_id = %q, want UUID", eventID)
	}
	if _, err := uuid.Parse(runID); err != nil {
		t.Fatalf("run_id = %q, want UUID", runID)
	}
	if result["new_run_created"] != true {
		t.Fatalf("new_run_created = %#v, want true", result["new_run_created"])
	}
	deliveries := asSlice(t, result["deliveries"])
	if len(deliveries) != 2 {
		t.Fatalf("deliveries = %#v, want persisted node and agent deliveries", deliveries)
	}
	assertEventPublishDeliveriesContain(t, deliveries, "agent", "scan-orchestrator", "pending", 1)
	assertEventPublishDeliveriesContain(t, deliveries, "node", "scan-orchestrator", "pending", 1)
	if count := countEventsByName(t, db, "scan.requested"); count != 1 {
		t.Fatalf("scan.requested event count = %d, want 1", count)
	}
	assertEventPublishPersistence(t, db, runID, eventID, "scan.requested", "cli-publish:"+actorTokenID(testToken))
	if count := countAPIIdempotencyRows(t, db); count != 1 {
		t.Fatalf("api_idempotency rows = %d, want 1", count)
	}
	got := requireAPIV1RuntimeBusEvent(t, ch, "event.publish delivery")
	if got.ID() != eventID {
		t.Fatalf("delivered event = %s, want %s", got.ID(), eventID)
	}

	replay := rpcCall(t, handler, body)
	if replay.Error != nil {
		t.Fatalf("event.publish replay error = %#v", replay.Error)
	}
	replayResult := asMap(t, replay.Result)
	if replayResult["event_id"] != eventID || replayResult["run_id"] != runID {
		t.Fatalf("event.publish replay result = %#v, want original event/run", replayResult)
	}
	replayDeliveries := asSlice(t, replayResult["deliveries"])
	if len(replayDeliveries) != 2 {
		t.Fatalf("event.publish replay deliveries = %#v, want persisted node and agent deliveries", replayDeliveries)
	}
	assertEventPublishDeliveriesContain(t, replayDeliveries, "agent", "scan-orchestrator", "pending", 1)
	assertEventPublishDeliveriesContain(t, replayDeliveries, "node", "scan-orchestrator", "pending", 1)
	if count := countEventsByName(t, db, "scan.requested"); count != 1 {
		t.Fatalf("scan.requested event count after replay = %d, want 1", count)
	}

	conflict := rpcCall(t, handler, eventPublishBody("", runStartTestFingerprint, "scan.requested", `{"topic":"changed"}`, "", "idem-publish"))
	if conflict.Error == nil {
		t.Fatal("event.publish idempotency conflict error = nil")
	}
	if data := asMap(t, conflict.Error.Data); data["code"] != IdempotencyConflictCode {
		t.Fatalf("event.publish conflict data = %#v", data)
	}
	if count := countEventsByName(t, db, "scan.requested"); count != 1 {
		t.Fatalf("scan.requested event count after conflict = %d, want 1", count)
	}
}

func TestOperatorEventPublishReturnsDurableAckBeforePostCommitDispatchCompletes(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &store.PostgresStore{DB: db}
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	probe := lifecycletest.New(t)
	opts := runStartTestEventBusOptions(source)
	opts.TestLifecycleProbe = probe
	opts.Interceptors = []runtimebus.EventInterceptor{blockingAPIV1PublishInterceptor{started: started, release: release}}
	bus, err := runtimebus.NewEventBusWithOptions(pg, opts)
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ctx := context.Background()
	seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "scan-orchestrator")
	ch := bus.Subscribe("scan-orchestrator", events.EventType("scan.requested"))
	defer bus.Unsubscribe("scan-orchestrator")
	handler := eventPublishTestHandler(t, pg, bus, source)
	body := eventPublishBody("", runStartTestFingerprint, "scan.requested", `{"topic":"medicine"}`, "", "idem-quick-ack-publish")

	respCh := make(chan rpcResponse, 1)
	go func() {
		respCh <- rpcCall(t, handler, body)
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("post-commit dispatch did not reach the blocking interceptor")
	}
	var published rpcResponse
	select {
	case published = <-respCh:
	case <-time.After(5 * time.Second):
		t.Fatal("event.publish did not return before post-commit dispatch completed")
	}
	if published.Error != nil {
		t.Fatalf("event.publish error = %#v", published.Error)
	}
	result := asMap(t, published.Result)
	eventID := stringValue(t, result["event_id"], "event_id")
	runID := stringValue(t, result["run_id"], "run_id")
	deliveries := asSlice(t, result["deliveries"])
	assertEventPublishDeliveriesContain(t, deliveries, "agent", "scan-orchestrator", "pending", 1)
	assertEventPublishDeliveriesContain(t, deliveries, "node", "scan-orchestrator", "pending", 1)
	assertEventPublishPersistence(t, db, runID, eventID, "scan.requested", "cli-publish:"+actorTokenID(testToken))
	if count := countAPIIdempotencyRows(t, db); count != 1 {
		t.Fatalf("api_idempotency rows before dispatch release = %d, want 1", count)
	}

	probe.RequirePostCommitDispatchStarted(eventID)
	requireNoAPIV1RuntimeBusEvent(t, ch, "event.publish delivery before post-commit release")
	if got := countPipelineReceiptsForEvent(t, ctx, db, eventID); got != 0 {
		t.Fatalf("pipeline receipts before post-commit release = %d, want 0", got)
	}

	releaseOnce.Do(func() { close(release) })
	got := requireAPIV1RuntimeBusEvent(t, ch, "event.publish delivery after post-commit release")
	if got.ID() != eventID {
		t.Fatalf("delivered event = %s, want %s", got.ID(), eventID)
	}
	probe.RequirePostCommitDispatchCompleted(eventID)
	if got := countPipelineReceiptsForEvent(t, ctx, db, eventID); got != 1 {
		t.Fatalf("pipeline receipts after post-commit release = %d, want 1", got)
	}
}

func TestOperatorEventPublishSQLiteIdempotentFirstEventPublishesWithoutLock(t *testing.T) {
	ctx := context.Background()
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx, testutil.SQLiteDefaultTemp())
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := runtimebus.NewEventBusWithOptions(sqliteStore, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := eventPublishTestHandlerWithStores(t, sqliteStore, sqliteStore, sqliteStore, bus, source)
	body := eventPublishBody("", runStartTestFingerprint, "scan.requested", `{"topic":"medicine"}`, "", "idem-sqlite-publish")

	published := rpcCall(t, handler, body)
	if published.Error != nil {
		t.Fatalf("sqlite event.publish error = %#v", published.Error)
	}
	result := asMap(t, published.Result)
	eventID := stringValue(t, result["event_id"], "event_id")
	runID := stringValue(t, result["run_id"], "run_id")
	if result["new_run_created"] != true {
		t.Fatalf("sqlite new_run_created = %#v, want true", result["new_run_created"])
	}
	deliveries := asSlice(t, result["deliveries"])
	if len(deliveries) != 1 {
		t.Fatalf("sqlite deliveries = %#v, want one persisted delivery", deliveries)
	}
	assertEventPublishDeliveryIdentity(t, asMap(t, deliveries[0]), "node", "scan-orchestrator", "pending", 1)
	assertSQLiteEventPublishRows(t, sqliteStore.DB, runID, eventID, "scan.requested", "cli-publish:"+actorTokenID(testToken))
	if count := countSQLiteAPIIdempotencyRows(t, sqliteStore.DB); count != 1 {
		t.Fatalf("sqlite api_idempotency rows = %d, want 1", count)
	}

	replay := rpcCall(t, handler, body)
	if replay.Error != nil {
		t.Fatalf("sqlite event.publish replay error = %#v", replay.Error)
	}
	replayResult := asMap(t, replay.Result)
	if replayResult["event_id"] != eventID || replayResult["run_id"] != runID {
		t.Fatalf("sqlite replay result = %#v, want original event/run", replayResult)
	}
	if count := countSQLiteEventsByName(t, sqliteStore.DB, "scan.requested"); count != 1 {
		t.Fatalf("sqlite event rows after replay = %d, want 1", count)
	}

	conflict := rpcCall(t, handler, eventPublishBody("", runStartTestFingerprint, "scan.requested", `{"topic":"changed"}`, "", "idem-sqlite-publish"))
	if conflict.Error == nil {
		t.Fatal("sqlite event.publish idempotency conflict error = nil")
	}
	if data := asMap(t, conflict.Error.Data); data["code"] != IdempotencyConflictCode {
		t.Fatalf("sqlite idempotency conflict data = %#v", data)
	}
	if count := countSQLiteEventsByName(t, sqliteStore.DB, "scan.requested"); count != 1 {
		t.Fatalf("sqlite event rows after conflict = %d, want 1", count)
	}

	nonIDEM := rpcCall(t, handler, eventPublishBody("", runStartTestFingerprint, "scan.requested", `{"topic":"second"}`, "", ""))
	if nonIDEM.Error != nil {
		t.Fatalf("sqlite non-idempotent event.publish error = %#v", nonIDEM.Error)
	}
	if count := countSQLiteEventsByName(t, sqliteStore.DB, "scan.requested"); count != 2 {
		t.Fatalf("sqlite event rows after non-idempotent publish = %d, want 2", count)
	}
	if count := countSQLiteAPIIdempotencyRows(t, sqliteStore.DB); count != 1 {
		t.Fatalf("sqlite api_idempotency rows after non-idempotent publish = %d, want 1", count)
	}
}

func TestOperatorEventPublishSQLitePayloadFailureLeavesNoIdempotencyCompletionOrRows(t *testing.T) {
	ctx := context.Background()
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx, testutil.SQLiteDefaultTemp())
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := runtimebus.NewEventBusWithOptions(sqliteStore, runtimebus.EventBusOptions{
		ContractBundle:   source,
		BundleSourceFact: runStartTestBundleSourceFact(),
		PayloadValidator: func(eventType string, _ []byte) error {
			if eventType == "scan.requested" {
				return errors.New("schema violation")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := eventPublishTestHandlerWithStores(t, sqliteStore, sqliteStore, sqliteStore, bus, source)

	resp := rpcCall(t, handler, eventPublishBody("", runStartTestFingerprint, "scan.requested", `{"topic":"medicine"}`, "", "idem-sqlite-payload-fails"))
	if resp.Error == nil {
		t.Fatal("sqlite event.publish payload validation error = nil")
	}
	if data := asMap(t, resp.Error.Data); data["code"] != PayloadValidationFailedCode {
		t.Fatalf("sqlite payload validation data = %#v", data)
	}
	if count := countSQLiteEventsByName(t, sqliteStore.DB, "scan.requested"); count != 0 {
		t.Fatalf("sqlite event rows after failed publish = %d, want 0", count)
	}
	if count := countSQLiteAllRunRows(t, sqliteStore.DB); count != 0 {
		t.Fatalf("sqlite run rows after failed publish = %d, want 0", count)
	}
	if count := countSQLiteAPIIdempotencyRows(t, sqliteStore.DB); count != 0 {
		t.Fatalf("sqlite api_idempotency rows after failed publish = %d, want 0", count)
	}
}

func TestOperatorEventPublishResolvesFlowScopedContractEventName(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &store.PostgresStore{DB: db}
	source := semanticview.Wrap(flowScopedEventPublishTestBundle())
	canonicalEventName := "repo-scaffold/repo_scaffold.repo_commit_succeeded"
	bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		ContractBundle:   source,
		BundleSourceFact: runStartTestBundleSourceFact(),
		PayloadValidator: func(eventType string, _ []byte) error {
			if eventType != canonicalEventName {
				return fmt.Errorf("event type = %q, want %s", eventType, canonicalEventName)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ctx := context.Background()
	seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "repo-observer")
	ch := bus.Subscribe("repo-observer", events.EventType(canonicalEventName))
	defer bus.Unsubscribe("repo-observer")
	handler := eventPublishTestHandler(t, pg, bus, source)
	body := eventPublishBody("", runStartTestFingerprint, "repo-scaffold/repo_scaffold.repo_commit_succeeded", `{"topic":"medicine"}`, "", "idem-flow-scoped")

	published := rpcCall(t, handler, body)
	if published.Error != nil {
		t.Fatalf("event.publish flow-scoped error = %#v", published.Error)
	}
	result := asMap(t, published.Result)
	eventID := stringValue(t, result["event_id"], "event_id")
	runID := stringValue(t, result["run_id"], "run_id")
	if got := countEventsByName(t, db, canonicalEventName); got != 1 {
		t.Fatalf("%s event count = %d, want 1", canonicalEventName, got)
	}
	assertEventPublishPersistence(t, db, runID, eventID, canonicalEventName, "cli-publish:"+actorTokenID(testToken))
	deliveries := asSlice(t, result["deliveries"])
	if len(deliveries) != 2 {
		t.Fatalf("deliveries = %#v, want typed agent and node deliveries", deliveries)
	}
	assertEventPublishDeliveriesContain(t, deliveries, "agent", "repo-observer", "pending", 1)
	assertEventPublishDeliveriesContain(t, deliveries, "node", "repo-observer", "pending", 1)
	got := requireAPIV1RuntimeBusEvent(t, ch, "flow-scoped event.publish delivery")
	if got.ID() != eventID || string(got.Type()) != canonicalEventName {
		t.Fatalf("delivered event = %#v, want %s/%s", got, eventID, canonicalEventName)
	}
}

func TestOperatorEventPublishRootEventNameWinsOverFlowLeafAliases(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &store.PostgresStore{DB: db}
	source := semanticview.Wrap(rootAndAmbiguousFlowScopedEventPublishTestBundle())
	bus, err := runtimebus.NewEventBusWithOptions(pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := eventPublishTestHandler(t, pg, bus, source)

	published := rpcCall(t, handler, eventPublishBody("", runStartTestFingerprint, "workflow.completed", `{"topic":"medicine"}`, "", "idem-root-collision"))
	if published.Error != nil {
		t.Fatalf("event.publish root collision error = %#v", published.Error)
	}
	result := asMap(t, published.Result)
	eventID := stringValue(t, result["event_id"], "event_id")
	runID := stringValue(t, result["run_id"], "run_id")
	if got := countEventsByName(t, db, "workflow.completed"); got != 1 {
		t.Fatalf("workflow.completed event count = %d, want 1", got)
	}
	for _, flowEventName := range []string{"alpha-flow/workflow.completed", "beta-flow/workflow.completed"} {
		if got := countEventsByName(t, db, flowEventName); got != 0 {
			t.Fatalf("%s event count = %d, want 0", flowEventName, got)
		}
	}
	assertEventPublishPersistence(t, db, runID, eventID, "workflow.completed", "cli-publish:"+actorTokenID(testToken))
}

func TestOperatorEventPublishFlowScopedEventNameFailuresFailClosed(t *testing.T) {
	t.Run("unknown flow scoped event", func(t *testing.T) {
		_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
		pg := &store.PostgresStore{DB: db}
		source := semanticview.Wrap(flowScopedEventPublishTestBundle())
		bus, err := runtimebus.NewEventBusWithOptions(pg, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := eventPublishTestHandler(t, pg, bus, source)

		resp := rpcCall(t, handler, eventPublishBody("", runStartTestFingerprint, "repo-scaffold/repo_scaffold.missing", `{"topic":"medicine"}`, "", "idem-flow-scoped-missing"))
		if resp.Error == nil {
			t.Fatal("event.publish unknown flow-scoped event error = nil")
		}
		data := asMap(t, resp.Error.Data)
		if data["code"] != EventNotDeclaredCode {
			t.Fatalf("unknown flow-scoped data = %#v, want %s", data, EventNotDeclaredCode)
		}
		details := asMap(t, data["details"])
		if details["event_name"] != "repo-scaffold/repo_scaffold.missing" || details["reason"] != "unknown_flow_scoped_event" {
			t.Fatalf("unknown flow-scoped details = %#v", details)
		}
		assertNoFlowScopedEventPublishPersistence(t, db)
	})

	t.Run("ambiguous unscoped leaf", func(t *testing.T) {
		_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
		pg := &store.PostgresStore{DB: db}
		source := semanticview.Wrap(ambiguousFlowScopedEventPublishTestBundle())
		bus, err := runtimebus.NewEventBusWithOptions(pg, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := eventPublishTestHandler(t, pg, bus, source)

		resp := rpcCall(t, handler, eventPublishBody("", runStartTestFingerprint, "workflow.completed", `{"topic":"medicine"}`, "", "idem-flow-scoped-ambiguous"))
		if resp.Error == nil {
			t.Fatal("event.publish ambiguous leaf error = nil")
		}
		data := asMap(t, resp.Error.Data)
		if data["code"] != EventNotDeclaredCode {
			t.Fatalf("ambiguous leaf data = %#v, want %s", data, EventNotDeclaredCode)
		}
		details := asMap(t, data["details"])
		if details["event_name"] != "workflow.completed" || details["reason"] != "ambiguous_event_name" {
			t.Fatalf("ambiguous leaf details = %#v", details)
		}
		assertNoFlowScopedEventPublishPersistence(t, db)
	})
}

func TestOperatorEventPublishHandlersRequireCanonicalBundleHashForCreateNewWork(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &store.PostgresStore{DB: db}
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := runtimebus.NewEventBusWithOptions(pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := eventPublishTestHandler(t, pg, bus, source)

	resp := rpcCall(t, handler, eventPublishBodyWithLegacyFingerprint("", runStartTestFingerprint, "scan.requested", `{"topic":"medicine"}`, "", "idem-publish-legacy"))
	if resp.Error == nil {
		t.Fatal("event.publish missing canonical bundle_hash error = nil")
	}
	if data := asMap(t, resp.Error.Data); data["code"] != BundleScopeRequiredCode {
		t.Fatalf("bundle scope required data = %#v", data)
	}
	assertNoEventPublishPersistence(t, db)
}

func TestOperatorEventPublishHandlersUseActiveEphemeralBundleScopeForCreateNewWork(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &store.PostgresStore{DB: db}
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := runtimebus.NewEventBusWithOptions(pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	seedActiveAPIV1RuntimeBusAgent(t, context.Background(), pg, "scan-orchestrator")
	ch := bus.Subscribe("scan-orchestrator", events.EventType("scan.requested"))
	defer bus.Unsubscribe("scan-orchestrator")
	handler := eventPublishTestHandler(t, pg, bus, source)

	published := rpcCall(t, handler, eventPublishBodyWithoutBundle("", "scan.requested", `{"topic":"medicine"}`, "", "idem-publish-no-bundle"))
	if published.Error != nil {
		t.Fatalf("event.publish active ephemeral bundle scope error = %#v", published.Error)
	}
	result := asMap(t, published.Result)
	eventID := stringValue(t, result["event_id"], "event_id")
	runID := stringValue(t, result["run_id"], "run_id")
	assertEventPublishPersistence(t, db, runID, eventID, "scan.requested", "cli-publish:"+actorTokenID(testToken))
	if got := countEventsByName(t, db, "scan.requested"); got != 1 {
		t.Fatalf("scan.requested event count = %d, want 1", got)
	}
	got := requireAPIV1RuntimeBusEvent(t, ch, "event.publish delivery")
	if got.ID() != eventID {
		t.Fatalf("delivered event = %s, want %s", got.ID(), eventID)
	}
}

func TestOperatorEventPublishPersistsIdempotencyBeforeReadbackFailure(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &store.PostgresStore{DB: db}
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := runtimebus.NewEventBusWithOptions(pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	seedActiveAPIV1RuntimeBusAgent(t, context.Background(), pg, "scan-orchestrator")
	ch := bus.Subscribe("scan-orchestrator", events.EventType("scan.requested"))
	defer bus.Unsubscribe("scan-orchestrator")
	observability := &failOnceEventReadStore{
		ObservabilityReadStore: pg,
		err:                    errors.New("transient event readback failure"),
	}
	handler := eventPublishTestHandlerWithObservability(t, pg, bus, source, observability)
	body := eventPublishBody("", runStartTestFingerprint, "scan.requested", `{"topic":"medicine"}`, "", "idem-readback")

	var first rpcResponse
	logOutput := captureProcessLog(t, func() {
		first = rpcCall(t, handler, body)
	})
	requireRPCFailure(t, first.Error, runtimefailures.ClassInternalFailure, "unclassified_runtime_error")
	if first.Error.Code != codeInternalError {
		t.Fatalf("first event.publish code = %d, want %d", first.Error.Code, codeInternalError)
	}
	for _, want := range []string{
		"runtime.error component=api",
		"json-rpc internal error",
		`"method":"event.publish"`,
		`"correlation_id":"publish"`,
		`"event_name":"scan.requested"`,
		"platform.internal_failure",
		"unclassified_runtime_error",
	} {
		if !strings.Contains(logOutput, want) {
			t.Fatalf("readback failure log = %q, want substring %q", logOutput, want)
		}
	}
	if strings.Contains(logOutput, "transient event readback failure") {
		t.Fatalf("readback failure log leaked raw error prose: %q", logOutput)
	}
	if count := countEventsByName(t, db, "scan.requested"); count != 1 {
		t.Fatalf("scan.requested event count after readback failure = %d, want 1", count)
	}
	if count := countAPIIdempotencyRows(t, db); count != 1 {
		t.Fatalf("api_idempotency rows after readback failure = %d, want 1", count)
	}
	requireAPIV1RuntimeBusEvent(t, ch, "event delivery after readback failure")

	replay := rpcCall(t, handler, body)
	if replay.Error != nil {
		t.Fatalf("event.publish replay after readback recovery error = %#v", replay.Error)
	}
	replayResult := asMap(t, replay.Result)
	eventID := stringValue(t, replayResult["event_id"], "event_id")
	runID := stringValue(t, replayResult["run_id"], "run_id")
	if _, err := uuid.Parse(eventID); err != nil {
		t.Fatalf("event_id = %q, want UUID", eventID)
	}
	if _, err := uuid.Parse(runID); err != nil {
		t.Fatalf("run_id = %q, want UUID", runID)
	}
	if count := countEventsByName(t, db, "scan.requested"); count != 1 {
		t.Fatalf("scan.requested event count after replay = %d, want 1", count)
	}
	deliveries := asSlice(t, replayResult["deliveries"])
	if len(deliveries) != 2 {
		t.Fatalf("replay deliveries = %#v, want typed persisted delivery results", deliveries)
	}
	assertEventPublishDeliveriesContain(t, deliveries, "agent", "scan-orchestrator", "pending", 1)
	assertEventPublishDeliveriesContain(t, deliveries, "node", "scan-orchestrator", "pending", 1)
}

func TestOperatorEventPublishPostCommitReceiptFailureReplaysWithoutDuplicate(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &store.PostgresStore{DB: db}
	failing := &failStandalonePipelineReceiptOnceStore{
		PostgresStore: pg,
		err:           errors.New("simulated post-commit receipt failure"),
	}
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := runtimebus.NewEventBusWithOptions(failing, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ctx := context.Background()
	seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "scan-orchestrator")
	ch := bus.Subscribe("scan-orchestrator", events.EventType("scan.requested"))
	defer bus.Unsubscribe("scan-orchestrator")
	handler := eventPublishTestHandlerWithStores(t, failing, failing, failing, bus, source)
	body := eventPublishBody("", runStartTestFingerprint, "scan.requested", `{"topic":"medicine"}`, "", "idem-post-commit-receipt")

	published := rpcCall(t, handler, body)
	if published.Error != nil {
		t.Fatalf("event.publish post-commit receipt failure error = %#v", published.Error)
	}
	result := asMap(t, published.Result)
	eventID := stringValue(t, result["event_id"], "event_id")
	if count := countEventsByName(t, db, "scan.requested"); count != 1 {
		t.Fatalf("scan.requested event count after post-commit receipt failure = %d, want 1", count)
	}
	if got := countEventDeliveriesForEvent(t, ctx, db, eventID); got != 1 {
		t.Fatalf("event deliveries after post-commit receipt failure = %d, want 1", got)
	}
	if count := countAPIIdempotencyRows(t, db); count != 1 {
		t.Fatalf("api_idempotency rows after post-commit receipt failure = %d, want 1", count)
	}
	if got := countPipelineReceiptsForEvent(t, ctx, db, eventID); got != 0 {
		t.Fatalf("pipeline receipts after injected failure = %d, want 0", got)
	}
	missing, err := pg.ListEventsMissingPipelineReceipt(ctx, time.Now().Add(-time.Hour), 10)
	if err != nil {
		t.Fatalf("ListEventsMissingPipelineReceipt: %v", err)
	}
	if !containsMissingPipelineReceiptEvent(missing, eventID) {
		t.Fatalf("missing pipeline receipt events = %#v, want %s", missing, eventID)
	}
	requireAPIV1RuntimeBusEvent(t, ch, "event delivery after post-commit receipt failure")

	replay := rpcCall(t, handler, body)
	if replay.Error != nil {
		t.Fatalf("event.publish replay after post-commit receipt failure error = %#v", replay.Error)
	}
	if replayEventID := stringValue(t, asMap(t, replay.Result)["event_id"], "event_id"); replayEventID != eventID {
		t.Fatalf("event.publish replay event_id = %q, want original %q", replayEventID, eventID)
	}
	if count := countEventsByName(t, db, "scan.requested"); count != 1 {
		t.Fatalf("scan.requested event count after replay = %d, want 1", count)
	}
	if count := countAPIIdempotencyRows(t, db); count != 1 {
		t.Fatalf("api_idempotency rows after replay = %d, want 1", count)
	}
}

func TestOperatorEventPublishPostCommitCompletionFailureReplaysWithoutDuplicate(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &store.PostgresStore{DB: db}
	failing := &failNormalRunCompletionStore{
		PostgresStore: pg,
		err:           errors.New("simulated normal-run completion failure"),
	}
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	probe := lifecycletest.New(t, lifecycletest.WithTimeout(5*time.Second))
	opts := runStartTestEventBusOptions(source)
	opts.TestLifecycleProbe = probe
	bus, err := runtimebus.NewEventBusWithOptions(failing, opts)
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ctx := context.Background()
	seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "scan-orchestrator")
	ch := bus.Subscribe("scan-orchestrator", events.EventType("scan.requested"))
	defer bus.Unsubscribe("scan-orchestrator")
	handler := eventPublishTestHandlerWithStores(t, failing, failing, failing, bus, source)
	body := eventPublishBody("", runStartTestFingerprint, "scan.requested", `{"topic":"medicine"}`, "", "idem-post-commit-completion")

	published := rpcCall(t, handler, body)
	if published.Error != nil {
		t.Fatalf("event.publish post-commit completion failure error = %#v", published.Error)
	}
	result := asMap(t, published.Result)
	eventID := stringValue(t, result["event_id"], "event_id")
	if count := countEventsByName(t, db, "scan.requested"); count != 1 {
		t.Fatalf("scan.requested event count after post-commit completion failure = %d, want 1", count)
	}
	if got := countEventDeliveriesForEvent(t, ctx, db, eventID); got != 1 {
		t.Fatalf("event deliveries after post-commit completion failure = %d, want 1", got)
	}
	if count := countAPIIdempotencyRows(t, db); count != 1 {
		t.Fatalf("api_idempotency rows after post-commit completion failure = %d, want 1", count)
	}
	probe.RequirePostCommitDispatchCompleted(eventID)
	outcome, failure := loadPipelineReceiptOutcomeAndFailure(t, ctx, db, eventID)
	if outcome != "dead_letter" || failure == nil || failure.Class != runtimefailures.ClassDependencyUnavailable || failure.Detail.Code != "normal_run_completion_failed" {
		t.Fatalf("pipeline receipt outcome=%q failure=%#v, want canonical completion failure", outcome, failure)
	}
	requireAPIV1RuntimeBusEvent(t, ch, "event delivery after post-commit completion failure")

	replay := rpcCall(t, handler, body)
	if replay.Error != nil {
		t.Fatalf("event.publish replay after post-commit completion failure error = %#v", replay.Error)
	}
	if replayEventID := stringValue(t, asMap(t, replay.Result)["event_id"], "event_id"); replayEventID != eventID {
		t.Fatalf("event.publish replay event_id = %q, want original %q", replayEventID, eventID)
	}
	if count := countEventsByName(t, db, "scan.requested"); count != 1 {
		t.Fatalf("scan.requested event count after replay = %d, want 1", count)
	}
	if count := countAPIIdempotencyRows(t, db); count != 1 {
		t.Fatalf("api_idempotency rows after replay = %d, want 1", count)
	}
}

func TestOperatorEventPublishPreCommitFailureFailsClosedWithDeclaredError(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &store.PostgresStore{DB: db}
	failing := &failCommittedReplayScopeStore{
		PostgresStore: pg,
		err:           errors.New("simulated pre-commit replay scope failure"),
	}
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := runtimebus.NewEventBusWithOptions(failing, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ctx := context.Background()
	seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "scan-orchestrator")
	handler := eventPublishTestHandlerWithStores(t, failing, failing, failing, bus, source)
	body := eventPublishBody("", runStartTestFingerprint, "scan.requested", `{"topic":"medicine"}`, "", "idem-pre-commit")

	published := rpcCall(t, handler, body)
	if published.Error == nil {
		t.Fatal("event.publish pre-commit failure error = nil")
	}
	data := asMap(t, published.Error.Data)
	if data["code"] != EventPublishFailedCode {
		t.Fatalf("event.publish pre-commit error data = %#v, want %s", data, EventPublishFailedCode)
	}
	details := asMap(t, data["details"])
	if details["event_name"] != "scan.requested" || details["phase"] != "publish" || !strings.Contains(fmt.Sprint(details["reason"]), "simulated pre-commit replay scope failure") {
		t.Fatalf("event.publish pre-commit error details = %#v", details)
	}
	assertNoEventPublishPersistence(t, db)
	if got := countAllEventDeliveries(t, db); got != 0 {
		t.Fatalf("event_deliveries rows after pre-commit failure = %d, want 0", got)
	}
	if _, err := db.ExecContext(ctx, `SELECT 1`); err != nil {
		t.Fatalf("database unusable after pre-commit failure: %v", err)
	}
}

func TestOperatorEventPublishFailsClosedWithoutDurableAckPublisher(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &store.PostgresStore{DB: db}
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	publisher := &legacyOnlyEventPublisher{}
	handler := eventPublishTestHandlerWithStores(t, pg, pg, pg, publisher, source)
	body := eventPublishBody("", runStartTestFingerprint, "scan.requested", `{"topic":"medicine"}`, "", "idem-no-ack-publisher")

	published := rpcCall(t, handler, body)
	if published.Error == nil {
		t.Fatal("event.publish without durable ack publisher error = nil")
	}
	data := asMap(t, published.Error.Data)
	if data["code"] != EventPublishFailedCode {
		t.Fatalf("event.publish without durable ack publisher data = %#v, want %s", data, EventPublishFailedCode)
	}
	details := asMap(t, data["details"])
	if details["event_name"] != "scan.requested" || details["phase"] != "publish" || !strings.Contains(fmt.Sprint(details["reason"]), "durable event.publish acknowledgment requires acknowledged publisher") {
		t.Fatalf("event.publish without durable ack publisher details = %#v", details)
	}
	if publisher.publishCalls != 0 {
		t.Fatalf("legacy Publish calls = %d, want 0", publisher.publishCalls)
	}
	assertNoEventPublishPersistence(t, db)
	if got := countAllEventDeliveries(t, db); got != 0 {
		t.Fatalf("event_deliveries rows after missing durable ack publisher = %d, want 0", got)
	}
}

func TestOperatorEventPublishExplicitRunTargetRequiresExistingNonterminalRun(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &store.PostgresStore{DB: db}
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := runtimebus.NewEventBusWithOptions(pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	seedActiveAPIV1RuntimeBusAgent(t, context.Background(), pg, "scan-orchestrator")
	ch := bus.Subscribe("scan-orchestrator", events.EventType("scan.requested"))
	defer bus.Unsubscribe("scan-orchestrator")
	handler := eventPublishTestHandler(t, pg, bus, source)

	initial := rpcCall(t, handler, eventPublishBody("", runStartTestFingerprint, "scan.requested", `{"topic":"first"}`, "", "idem-new-run"))
	if initial.Error != nil {
		t.Fatalf("initial event.publish error = %#v", initial.Error)
	}
	runID := stringValue(t, asMap(t, initial.Result)["run_id"], "run_id")
	requireAPIV1RuntimeBusEvent(t, ch, "initial explicit-run target delivery")

	targeted := rpcCall(t, handler, eventPublishBody(runID, runStartTestFingerprint, "scan.requested", `{"topic":"second"}`, "operator-test", "idem-existing-run"))
	if targeted.Error != nil {
		t.Fatalf("targeted event.publish error = %#v", targeted.Error)
	}
	targetedResult := asMap(t, targeted.Result)
	targetedEventID := stringValue(t, targetedResult["event_id"], "event_id")
	if targetedResult["run_id"] != runID || targetedResult["new_run_created"] != false {
		t.Fatalf("targeted result = %#v, want existing run", targetedResult)
	}
	deliveries := asSlice(t, targetedResult["deliveries"])
	if len(deliveries) != 2 {
		t.Fatalf("targeted deliveries = %#v, want typed agent and node deliveries", deliveries)
	}
	assertEventPublishDeliveriesContain(t, deliveries, "agent", "scan-orchestrator", "pending", 1)
	assertEventPublishDeliveriesContain(t, deliveries, "node", "scan-orchestrator", "pending", 1)
	if count := countEventsByName(t, db, "scan.requested"); count != 2 {
		t.Fatalf("scan.requested events after targeted publish = %d, want 2", count)
	}
	got := requireAPIV1RuntimeBusEvent(t, ch, "targeted explicit-run delivery")
	if got.ID() != targetedEventID || got.RunID() != runID {
		t.Fatalf("targeted delivered event id/run = %s/%s, want %s/%s", got.ID(), got.RunID(), targetedEventID, runID)
	}

	mismatch := rpcCall(t, handler, eventPublishBody(runID, "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "scan.requested", `{"topic":"mismatch"}`, "", "idem-existing-run-mismatch"))
	if mismatch.Error == nil {
		t.Fatal("mismatched run bundle event.publish error = nil")
	}
	if data := asMap(t, mismatch.Error.Data); data["code"] != BundleMismatchCode {
		t.Fatalf("mismatched run bundle data = %#v, want %s", data, BundleMismatchCode)
	}
	if count := countEventsByName(t, db, "scan.requested"); count != 2 {
		t.Fatalf("scan.requested events after mismatched target = %d, want 2", count)
	}

	missingRunID := uuid.NewString()
	missing := rpcCall(t, handler, eventPublishBody(missingRunID, runStartTestFingerprint, "scan.requested", `{"topic":"missing"}`, "", "idem-missing-run"))
	if missing.Error == nil {
		t.Fatal("missing run event.publish error = nil")
	}
	if data := asMap(t, missing.Error.Data); data["code"] != RunNotFoundCode {
		t.Fatalf("missing run data = %#v, want %s", data, RunNotFoundCode)
	}

	if _, err := db.Exec(`UPDATE runs SET status = 'completed', ended_at = now() WHERE run_id = $1::uuid`, runID); err != nil {
		t.Fatalf("mark run terminal: %v", err)
	}
	terminal := rpcCall(t, handler, eventPublishBody(runID, runStartTestFingerprint, "scan.requested", `{"topic":"terminal"}`, "", "idem-terminal-run"))
	if terminal.Error == nil {
		t.Fatal("terminal run event.publish error = nil")
	}
	if data := asMap(t, terminal.Error.Data); data["code"] != RunAlreadyTerminalCode {
		t.Fatalf("terminal run data = %#v, want %s", data, RunAlreadyTerminalCode)
	}
	if count := countEventsByName(t, db, "scan.requested"); count != 2 {
		t.Fatalf("scan.requested events after failed targets = %d, want 2", count)
	}
}

func TestOperatorEventPublishExplicitRunFollowUpRequiresRecipientBeforePersistence(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &store.PostgresStore{DB: db}
	source := semanticview.Wrap(eventPublishFollowUpTestBundle())
	bus, err := runtimebus.NewEventBusWithOptions(pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ctx := context.Background()
	seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "scan-orchestrator")
	initialCh := bus.Subscribe("scan-orchestrator", events.EventType("scan.requested"))
	followUpCh := bus.Subscribe("scan-orchestrator", events.EventType("scan.followup"))
	defer bus.Unsubscribe("scan-orchestrator")
	handler := eventPublishTestHandler(t, pg, bus, source)

	initial := rpcCall(t, handler, eventPublishBody("", runStartTestFingerprint, "scan.requested", `{"topic":"first"}`, "", "idem-followup-initial"))
	if initial.Error != nil {
		t.Fatalf("initial event.publish error = %#v", initial.Error)
	}
	initialResult := asMap(t, initial.Result)
	runID := stringValue(t, initialResult["run_id"], "run_id")
	if initialResult["new_run_created"] != true {
		t.Fatalf("initial result = %#v, want new run", initialResult)
	}
	requireAPIV1RuntimeBusEvent(t, initialCh, "initial delivery")

	followUp := rpcCall(t, handler, eventPublishBody(runID, runStartTestFingerprint, "scan.followup", `{"topic":"second"}`, "operator-test", "idem-followup-existing"))
	if followUp.Error != nil {
		t.Fatalf("follow-up event.publish error = %#v", followUp.Error)
	}
	followUpResult := asMap(t, followUp.Result)
	followUpEventID := stringValue(t, followUpResult["event_id"], "event_id")
	if followUpResult["run_id"] != runID || followUpResult["new_run_created"] != false {
		t.Fatalf("follow-up result = %#v, want selected existing run", followUpResult)
	}
	deliveries := asSlice(t, followUpResult["deliveries"])
	if len(deliveries) != 1 {
		t.Fatalf("follow-up deliveries = %#v, want one delivery", deliveries)
	}
	if delivery := asMap(t, deliveries[0]); delivery["subscriber_id"] != "scan-orchestrator" {
		t.Fatalf("follow-up delivery = %#v, want scan-orchestrator", delivery)
	} else {
		assertEventPublishDeliveryIdentity(t, delivery, "agent", "scan-orchestrator", "pending", 1)
	}
	assertEventPublishEventRow(t, db, runID, followUpEventID, "scan.followup", "operator-test")
	if got := countRunRowsByID(t, db, runID); got != 1 {
		t.Fatalf("run rows for selected run = %d, want 1", got)
	}
	if got := countAllRunRows(t, db); got != 1 {
		t.Fatalf("all run rows after follow-up = %d, want 1", got)
	}
	if got := countEventRowsByRunID(t, db, runID); got != 2 {
		t.Fatalf("events for selected run = %d, want 2", got)
	}
	if got := countEventDeliveriesForEvent(t, ctx, db, followUpEventID); got != 1 {
		t.Fatalf("event_deliveries for follow-up = %d, want 1", got)
	}
	got := requireAPIV1RuntimeBusEvent(t, followUpCh, "follow-up delivery")
	if got.ID() != followUpEventID || got.RunID() != runID {
		t.Fatalf("follow-up delivered event id/run = %s/%s, want %s/%s", got.ID(), got.RunID(), followUpEventID, runID)
	}

	rejected := rpcCall(t, handler, eventPublishBody(runID, runStartTestFingerprint, "scan.unhandled", `{"topic":"lost"}`, "operator-test", "idem-followup-unhandled"))
	if rejected.Error == nil {
		t.Fatal("unhandled follow-up event.publish error = nil")
	}
	data := asMap(t, rejected.Error.Data)
	if data["code"] != EventNotDeclaredCode {
		t.Fatalf("unhandled follow-up data = %#v, want %s", data, EventNotDeclaredCode)
	}
	details := asMap(t, data["details"])
	if details["reason"] != "declared_event_has_no_selected_run_recipient" {
		t.Fatalf("unhandled follow-up details = %#v", details)
	}
	if got := countAllEventRows(t, db); got != 2 {
		t.Fatalf("event rows after rejected follow-up = %d, want 2", got)
	}
	if got := countAPIIdempotencyRows(t, db); got != 2 {
		t.Fatalf("api_idempotency rows after rejected follow-up = %d, want 2", got)
	}
}

func TestOperatorEventPublishExistingRunTargetRouteValidatesAndPersistsCanonicalTarget(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &store.PostgresStore{DB: db}
	source := semanticview.Wrap(eventPublishTargetRouteTestBundle())
	bus, err := runtimebus.NewEventBusWithOptions(pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "bootstrap-node")
	initialCh := bus.Subscribe("bootstrap-node", events.EventType("bootstrap.requested"))
	defer bus.Unsubscribe("bootstrap-node")
	handler := eventPublishTestHandler(t, pg, bus, source)

	initial := rpcCall(t, handler, eventPublishBody("", runStartTestFingerprint, "bootstrap.requested", `{"topic":"first"}`, "", "idem-target-route-initial"))
	if initial.Error != nil {
		t.Fatalf("initial event.publish error = %#v", initial.Error)
	}
	runID := stringValue(t, asMap(t, initial.Result)["run_id"], "run_id")
	requireAPIV1RuntimeBusEvent(t, initialCh, "initial delivery")

	targetFlowInstance := "operating/inst-1"
	targetEntityID := runtimeflowidentity.EntityID(targetFlowInstance)
	seedEventPublishEntityState(t, db, runID, targetEntityID, targetFlowInstance, "waiting")
	if err := bus.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("operating", "inst-1")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}

	targeted := rpcCall(t, handler, eventPublishBodyWithTarget(runID, "", runStartTestFingerprint, "operating/opco.product_initialization_requested", `{"topic":"targeted"}`, "operator-test", "idem-target-route-positive", targetFlowInstance, targetEntityID))
	if targeted.Error != nil {
		t.Fatalf("targeted event.publish error = %#v", targeted.Error)
	}
	result := asMap(t, targeted.Result)
	eventID := stringValue(t, result["event_id"], "event_id")
	if result["run_id"] != runID || result["new_run_created"] != false {
		t.Fatalf("targeted result = %#v, want selected existing run", result)
	}
	assertEventPublishTargetRouteRow(t, db, runID, eventID, "operating/opco.product_initialization_requested", targetFlowInstance, targetEntityID)
	assertEventPublishDeliveryTargetRoute(t, db, eventID, "node", "lifecycle-orchestrator", targetFlowInstance, targetEntityID)
	if got := countEventRowsByRunID(t, db, runID); got != 2 {
		t.Fatalf("events for selected run after targeted publish = %d, want 2", got)
	}
	if got := countAPIIdempotencyRows(t, db); got != 2 {
		t.Fatalf("api_idempotency rows after targeted publish = %d, want 2", got)
	}

	payloadOnly := rpcCall(t, handler, eventPublishBody(runID, runStartTestFingerprint, "operating/opco.product_initialization_requested", fmt.Sprintf(`{"entity_id":%q,"topic":"payload-only"}`, targetEntityID), "operator-test", "idem-target-route-payload-only"))
	if payloadOnly.Error == nil {
		t.Fatal("payload-only target route event.publish error = nil")
	}
	payloadOnlyData := asMap(t, payloadOnly.Error.Data)
	if payloadOnlyData["code"] != EventNotDeclaredCode {
		t.Fatalf("payload-only target route data = %#v, want %s", payloadOnlyData, EventNotDeclaredCode)
	}
	if got := countEventRowsByRunID(t, db, runID); got != 2 {
		t.Fatalf("events for selected run after payload-only target = %d, want 2", got)
	}
}

func TestOperatorEventPublishRootEventTemplateInputNameCollisionPayloadEntityIDDoesNotSelectTarget(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &store.PostgresStore{DB: db}
	source := semanticview.Wrap(eventPublishRootTemplateCollisionTestBundle())
	bus, err := runtimebus.NewEventBusWithOptions(pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "root-orchestrator")
	ch := bus.Subscribe("root-orchestrator", events.EventType("review.requested"))
	defer bus.Unsubscribe("root-orchestrator")
	handler := eventPublishTestHandler(t, pg, bus, source)

	initial := rpcCall(t, handler, eventPublishBody("", runStartTestFingerprint, "review.requested", `{"topic":"first"}`, "", "idem-root-template-collision-initial"))
	if initial.Error != nil {
		t.Fatalf("initial event.publish error = %#v", initial.Error)
	}
	runID := stringValue(t, asMap(t, initial.Result)["run_id"], "run_id")
	requireAPIV1RuntimeBusEvent(t, ch, "initial root/template collision delivery")

	flowInstance := "operating/inst-1"
	entityID := runtimeflowidentity.EntityID(flowInstance)
	seedEventPublishEntityState(t, db, runID, entityID, flowInstance, "waiting")

	followUp := rpcCall(t, handler, eventPublishBody(runID, runStartTestFingerprint, "review.requested", fmt.Sprintf(`{"entity_id":%q,"topic":"root-follow-up"}`, entityID), "operator-test", "idem-root-template-collision-follow-up"))
	if followUp.Error != nil {
		t.Fatalf("root/template collision follow-up event.publish error = %#v", followUp.Error)
	}
	eventID := stringValue(t, asMap(t, followUp.Result)["event_id"], "event_id")
	requireAPIV1RuntimeBusEvent(t, ch, "follow-up root/template collision delivery")

	var gotEventName, gotEntityID, gotFlowInstance, gotTargetRoute string
	var gotPayload json.RawMessage
	if err := db.QueryRow(`
		SELECT event_name, COALESCE(entity_id::text, ''), COALESCE(flow_instance, ''), COALESCE(target_route::text, '{}'), payload
		FROM events
		WHERE event_id = $1::uuid
	`, eventID).Scan(&gotEventName, &gotEntityID, &gotFlowInstance, &gotTargetRoute, &gotPayload); err != nil {
		t.Fatalf("load root/template collision event row: %v", err)
	}
	if gotEventName != "review.requested" || gotEntityID != "" || gotFlowInstance != "" {
		t.Fatalf("event row = name:%q entity:%q flow:%q, want unscoped review.requested despite payload entity_id %s/%s", gotEventName, gotEntityID, gotFlowInstance, entityID, flowInstance)
	}
	if gotTargetRoute != "{}" {
		t.Fatalf("event target_route = %s, want empty non-target route", gotTargetRoute)
	}
	var decoded map[string]any
	if err := json.Unmarshal(gotPayload, &decoded); err != nil {
		t.Fatalf("decode root/template collision payload: %v", err)
	}
	if decoded["entity_id"] != entityID || decoded["topic"] != "root-follow-up" {
		t.Fatalf("event payload = %#v, want payload entity_id preserved as business data only", decoded)
	}
}

func TestOperatorEventPublishExistingRunTargetRouteRejectsInvalidTargetBeforePersistence(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &store.PostgresStore{DB: db}
	source := semanticview.Wrap(eventPublishTargetRouteTestBundle())
	bus, err := runtimebus.NewEventBusWithOptions(pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "bootstrap-node")
	initialCh := bus.Subscribe("bootstrap-node", events.EventType("bootstrap.requested"))
	defer bus.Unsubscribe("bootstrap-node")
	handler := eventPublishTestHandler(t, pg, bus, source)

	initial := rpcCall(t, handler, eventPublishBody("", runStartTestFingerprint, "bootstrap.requested", `{"topic":"first"}`, "", "idem-target-route-invalid-initial"))
	if initial.Error != nil {
		t.Fatalf("initial event.publish error = %#v", initial.Error)
	}
	runID := stringValue(t, asMap(t, initial.Result)["run_id"], "run_id")
	requireAPIV1RuntimeBusEvent(t, initialCh, "initial delivery")

	targetFlowInstance := "operating/inst-1"
	targetEntityID := runtimeflowidentity.EntityID(targetFlowInstance)
	seedEventPublishEntityState(t, db, runID, targetEntityID, targetFlowInstance, "waiting")
	if err := bus.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("operating", "inst-1")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	mismatchEntityID := uuid.NewString()
	seedEventPublishEntityState(t, db, runID, mismatchEntityID, "operating/other", "waiting")
	unroutableEntityID := uuid.NewString()
	seedEventPublishEntityState(t, db, runID, unroutableEntityID, "orphan/inst-1", "waiting")

	tests := []struct {
		name       string
		body       string
		wantCode   string
		wantReason string
	}{
		{
			name:     "blank target requires object",
			body:     fmt.Sprintf(`{"jsonrpc":"2.0","id":"publish","method":"event.publish","params":{"bundle_hash":%q,"event_name":"operating/opco.product_initialization_requested","payload":{},"run_id":%q,"target":null,"idempotency_key":"idem-target-null"}}`, runStartTestBundleHash, runID),
			wantCode: "INVALID_PARAMS",
		},
		{
			name:     "missing flow instance",
			body:     fmt.Sprintf(`{"jsonrpc":"2.0","id":"publish","method":"event.publish","params":{"bundle_hash":%q,"event_name":"operating/opco.product_initialization_requested","payload":{},"run_id":%q,"target":{"entity_id":%q},"idempotency_key":"idem-target-missing-flow"}}`, runStartTestBundleHash, runID, targetEntityID),
			wantCode: "INVALID_PARAMS",
		},
		{
			name:     "bad target entity uuid",
			body:     fmt.Sprintf(`{"jsonrpc":"2.0","id":"publish","method":"event.publish","params":{"bundle_hash":%q,"event_name":"operating/opco.product_initialization_requested","payload":{},"run_id":%q,"target":{"flow_instance":"operating/inst-1","entity_id":"not-a-uuid"},"idempotency_key":"idem-target-bad-uuid"}}`, runStartTestBundleHash, runID),
			wantCode: "INVALID_PARAMS",
		},
		{
			name:       "nonexistent entity",
			body:       eventPublishBodyWithTarget(runID, "", runStartTestFingerprint, "operating/opco.product_initialization_requested", `{"topic":"missing-entity"}`, "operator-test", "idem-target-missing-entity", targetFlowInstance, uuid.NewString()),
			wantCode:   EventNotDeclaredCode,
			wantReason: "selected_target_entity_not_found",
		},
		{
			name:       "mismatched entity flow",
			body:       eventPublishBodyWithTarget(runID, "", runStartTestFingerprint, "operating/opco.product_initialization_requested", `{"topic":"mismatch"}`, "operator-test", "idem-target-mismatch", targetFlowInstance, mismatchEntityID),
			wantCode:   EventNotDeclaredCode,
			wantReason: "selected_target_flow_instance_mismatch",
		},
		{
			name:       "event not routable for target flow",
			body:       eventPublishBodyWithTarget(runID, "", runStartTestFingerprint, "operating/opco.product_initialization_requested", `{"topic":"unroutable"}`, "operator-test", "idem-target-unroutable", "orphan/inst-1", unroutableEntityID),
			wantCode:   EventNotDeclaredCode,
			wantReason: "selected_run_target_not_routable",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := rpcCall(t, handler, tc.body)
			if resp.Error == nil {
				t.Fatal("event.publish error = nil")
			}
			if tc.wantCode == "INVALID_PARAMS" {
				if resp.Error.Code != codeInvalidParams {
					t.Fatalf("error code = %d, want %d; data=%#v", resp.Error.Code, codeInvalidParams, resp.Error.Data)
				}
				return
			}
			data := asMap(t, resp.Error.Data)
			if data["code"] != tc.wantCode {
				t.Fatalf("error data = %#v, want code %s", data, tc.wantCode)
			}
			if tc.wantReason != "" {
				details := asMap(t, data["details"])
				if details["reason"] != tc.wantReason {
					t.Fatalf("error details = %#v, want reason %s", details, tc.wantReason)
				}
			}
			if got := countEventRowsByRunID(t, db, runID); got != 1 {
				t.Fatalf("events for selected run after rejected target = %d, want 1", got)
			}
			if got := countAPIIdempotencyRows(t, db); got != 1 {
				t.Fatalf("api_idempotency rows after rejected target = %d, want 1", got)
			}
		})
	}
}

func TestOperatorEventPublishExplicitRunRequiresRecipientPlanCheckerBeforePersistence(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &store.PostgresStore{DB: db}
	source := semanticview.Wrap(eventPublishFollowUpTestBundle())
	bus, err := runtimebus.NewEventBusWithOptions(pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "scan-orchestrator")
	initialCh := bus.Subscribe("scan-orchestrator", events.EventType("scan.requested"))
	defer bus.Unsubscribe("scan-orchestrator")
	handler := eventPublishTestHandler(t, pg, bus, source)

	initial := rpcCall(t, handler, eventPublishBody("", runStartTestFingerprint, "scan.requested", `{"topic":"first"}`, "", "idem-followup-missing-plan-initial"))
	if initial.Error != nil {
		t.Fatalf("initial event.publish error = %#v", initial.Error)
	}
	runID := stringValue(t, asMap(t, initial.Result)["run_id"], "run_id")
	requireAPIV1RuntimeBusEvent(t, initialCh, "initial delivery")

	noCheckerHandler := eventPublishTestHandlerWithStores(t, pg, pg, pg, failingRunStartPublisher{}, source)
	rejected := rpcCall(t, noCheckerHandler, eventPublishBody(runID, runStartTestFingerprint, "scan.followup", `{"topic":"second"}`, "operator-test", "idem-followup-missing-plan"))
	if rejected.Error == nil {
		t.Fatal("missing recipient-plan checker event.publish error = nil")
	}
	data := asMap(t, rejected.Error.Data)
	if data["code"] != EventPublishFailedCode {
		t.Fatalf("missing recipient-plan checker data = %#v, want %s", data, EventPublishFailedCode)
	}
	details := asMap(t, data["details"])
	if details["phase"] != "publish" {
		t.Fatalf("missing recipient-plan checker details = %#v, want phase=publish", details)
	}
	if reason := stringValue(t, details["reason"], "reason"); !strings.Contains(reason, "recipient planning unavailable") {
		t.Fatalf("missing recipient-plan checker reason = %q", reason)
	}
	if got := countAllRunRows(t, db); got != 1 {
		t.Fatalf("run rows after missing recipient-plan checker = %d, want 1", got)
	}
	if got := countAllEventRows(t, db); got != 1 {
		t.Fatalf("event rows after missing recipient-plan checker = %d, want 1", got)
	}
	if got := countAPIIdempotencyRows(t, db); got != 1 {
		t.Fatalf("api_idempotency rows after missing recipient-plan checker = %d, want 1", got)
	}
}

func TestOperatorEventPublishSQLiteExplicitRunFollowUpUsesSelectedRun(t *testing.T) {
	ctx := context.Background()
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx, testutil.SQLiteDefaultTemp())
	source := semanticview.Wrap(eventPublishFollowUpTestBundle())
	bus, err := runtimebus.NewEventBusWithOptions(sqliteStore, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	initialCh := bus.Subscribe("scan-orchestrator", events.EventType("scan.requested"))
	followUpCh := bus.Subscribe("scan-orchestrator", events.EventType("scan.followup"))
	defer bus.Unsubscribe("scan-orchestrator")
	handler := eventPublishTestHandlerWithStores(t, sqliteStore, sqliteStore, sqliteStore, bus, source)

	initial := rpcCall(t, handler, eventPublishBody("", runStartTestFingerprint, "scan.requested", `{"topic":"first"}`, "", "idem-sqlite-followup-initial"))
	if initial.Error != nil {
		t.Fatalf("sqlite initial event.publish error = %#v", initial.Error)
	}
	runID := stringValue(t, asMap(t, initial.Result)["run_id"], "run_id")
	requireAPIV1RuntimeBusEvent(t, initialCh, "sqlite initial delivery")

	followUp := rpcCall(t, handler, eventPublishBody(runID, runStartTestFingerprint, "scan.followup", `{"topic":"second"}`, "operator-test", "idem-sqlite-followup-existing"))
	if followUp.Error != nil {
		t.Fatalf("sqlite follow-up event.publish error = %#v", followUp.Error)
	}
	result := asMap(t, followUp.Result)
	eventID := stringValue(t, result["event_id"], "event_id")
	if result["run_id"] != runID || result["new_run_created"] != false {
		t.Fatalf("sqlite follow-up result = %#v, want selected existing run", result)
	}
	if got := countSQLiteAllRunRows(t, sqliteStore.DB); got != 1 {
		t.Fatalf("sqlite run rows after follow-up = %d, want 1", got)
	}
	if got := countSQLiteEventRowsByRunID(t, sqliteStore.DB, runID); got != 2 {
		t.Fatalf("sqlite events for selected run = %d, want 2", got)
	}
	if got := countSQLiteEventsByName(t, sqliteStore.DB, "scan.followup"); got != 1 {
		t.Fatalf("sqlite scan.followup rows = %d, want 1", got)
	}
	deliveries := asSlice(t, result["deliveries"])
	if len(deliveries) != 1 {
		t.Fatalf("sqlite follow-up deliveries = %#v, want 1", deliveries)
	}
	assertEventPublishDeliveryIdentity(t, asMap(t, deliveries[0]), "agent", "scan-orchestrator", "pending", 1)
	got := requireAPIV1RuntimeBusEvent(t, followUpCh, "sqlite follow-up delivery")
	if got.ID() != eventID || got.RunID() != runID {
		t.Fatalf("sqlite follow-up delivered id/run = %s/%s, want %s/%s", got.ID(), got.RunID(), eventID, runID)
	}
}

func TestOperatorEventPublishRejectsCallerEntityIDForCreateEntityBeforePersistence(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &store.PostgresStore{DB: db}
	source := semanticview.Wrap(eventPublishCreateEntityTestBundle())
	bus, err := runtimebus.NewEventBusWithOptions(pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := eventPublishTestHandler(t, pg, bus, source)

	resp := rpcCall(t, handler, eventPublishBody("", runStartTestFingerprint, "thing.created", `{"entity_id":"11111111-1111-4111-8111-111111111111","amount":50}`, "", "idem-create-entity-supplied-id"))
	if resp.Error == nil {
		t.Fatal("create-entity supplied entity_id error = nil")
	}
	data := asMap(t, resp.Error.Data)
	if data["code"] != PayloadValidationFailedCode {
		t.Fatalf("create-entity supplied entity_id data = %#v, want %s", data, PayloadValidationFailedCode)
	}
	details := asMap(t, data["details"])
	violations := asSlice(t, details["violations"])
	if len(violations) != 1 {
		t.Fatalf("violations = %#v, want one", violations)
	}
	if violation := asMap(t, violations[0]); violation["field_path"] != "$.entity_id" || violation["rule"] != "create_entity_mints_entity_id" {
		t.Fatalf("violation = %#v", violation)
	}
	if got := countAllRunRows(t, db); got != 0 {
		t.Fatalf("run rows after create-entity rejection = %d, want 0", got)
	}
	if got := countAllEventRows(t, db); got != 0 {
		t.Fatalf("event rows after create-entity rejection = %d, want 0", got)
	}
	if got := countAPIIdempotencyRows(t, db); got != 0 {
		t.Fatalf("api_idempotency rows after create-entity rejection = %d, want 0", got)
	}
}

func TestOperatorEventPublishSQLiteRejectsCallerEntityIDForCreateEntityBeforePersistence(t *testing.T) {
	ctx := context.Background()
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx, testutil.SQLiteDefaultTemp())
	source := semanticview.Wrap(eventPublishCreateEntityTestBundle())
	bus, err := runtimebus.NewEventBusWithOptions(sqliteStore, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := eventPublishTestHandlerWithStores(t, sqliteStore, sqliteStore, sqliteStore, bus, source)

	resp := rpcCall(t, handler, eventPublishBody("", runStartTestFingerprint, "thing.created", `{"entity_id":"11111111-1111-4111-8111-111111111111","amount":50}`, "", "idem-sqlite-create-entity-supplied-id"))
	if resp.Error == nil {
		t.Fatal("sqlite create-entity supplied entity_id error = nil")
	}
	data := asMap(t, resp.Error.Data)
	if data["code"] != PayloadValidationFailedCode {
		t.Fatalf("sqlite create-entity supplied entity_id data = %#v, want %s", data, PayloadValidationFailedCode)
	}
	details := asMap(t, data["details"])
	violations := asSlice(t, details["violations"])
	if len(violations) != 1 {
		t.Fatalf("sqlite violations = %#v, want one", violations)
	}
	if violation := asMap(t, violations[0]); violation["field_path"] != "$.entity_id" || violation["rule"] != "create_entity_mints_entity_id" {
		t.Fatalf("sqlite violation = %#v", violation)
	}
	if got := countSQLiteAllRunRows(t, sqliteStore.DB); got != 0 {
		t.Fatalf("sqlite run rows after create-entity rejection = %d, want 0", got)
	}
	if got := countSQLiteAllEventRows(t, sqliteStore.DB); got != 0 {
		t.Fatalf("sqlite event rows after create-entity rejection = %d, want 0", got)
	}
	if got := countSQLiteAPIIdempotencyRows(t, sqliteStore.DB); got != 0 {
		t.Fatalf("sqlite api_idempotency rows after create-entity rejection = %d, want 0", got)
	}
}

func TestOperatorEventPublishSourceEventIDValidatesSameRunLineage(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &store.PostgresStore{DB: db}
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := runtimebus.NewEventBusWithOptions(pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ctx := context.Background()
	seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "scan-orchestrator")
	ch := bus.Subscribe("scan-orchestrator", events.EventType("scan.requested"))
	defer bus.Unsubscribe("scan-orchestrator")
	handler := eventPublishTestHandler(t, pg, bus, source)

	parent := rpcCall(t, handler, eventPublishBody("", runStartTestFingerprint, "scan.requested", `{"topic":"medicine"}`, "", "idem-source-parent"))
	if parent.Error != nil {
		t.Fatalf("parent event.publish error = %#v", parent.Error)
	}
	parentResult := asMap(t, parent.Result)
	parentEventID := stringValue(t, parentResult["event_id"], "event_id")
	parentRunID := stringValue(t, parentResult["run_id"], "run_id")
	requireAPIV1RuntimeBusEvent(t, ch, "parent source_event_id delivery")

	child := rpcCall(t, handler, eventPublishBodyWithSource(parentRunID, parentEventID, runStartTestFingerprint, "scan.requested", `{"topic":"checkpoint"}`, "operator-test", "idem-source-child"))
	if child.Error != nil {
		t.Fatalf("child event.publish error = %#v", child.Error)
	}
	childResult := asMap(t, child.Result)
	childEventID := stringValue(t, childResult["event_id"], "event_id")
	if childResult["run_id"] != parentRunID || childResult["new_run_created"] != false {
		t.Fatalf("child result = %#v, want existing run", childResult)
	}
	if childResult["source_event_id"] != parentEventID {
		t.Fatalf("child source_event_id = %#v, want %s", childResult["source_event_id"], parentEventID)
	}
	assertEventSourceEventID(t, db, childEventID, parentEventID)
	if count := countEventsByName(t, db, "scan.requested"); count != 2 {
		t.Fatalf("scan.requested events after sourced publish = %d, want 2", count)
	}
	if count := countAPIIdempotencyRows(t, db); count != 2 {
		t.Fatalf("api_idempotency rows after sourced publish = %d, want 2", count)
	}
	got := requireAPIV1RuntimeBusEvent(t, ch, "child source_event_id delivery")
	if got.ID() != childEventID || got.RunID() != parentRunID {
		t.Fatalf("child delivered event id/run = %s/%s, want %s/%s", got.ID(), got.RunID(), childEventID, parentRunID)
	}
}

func TestOperatorEventPublishSourceEventIDRejectsInvalidLineageBeforePersistence(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	pg := &store.PostgresStore{DB: db}
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := runtimebus.NewEventBusWithOptions(pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := eventPublishTestHandler(t, pg, bus, source)

	first := rpcCall(t, handler, eventPublishBody("", runStartTestFingerprint, "scan.requested", `{"topic":"first"}`, "", "idem-source-first"))
	if first.Error != nil {
		t.Fatalf("first event.publish error = %#v", first.Error)
	}
	firstRunID := stringValue(t, asMap(t, first.Result)["run_id"], "run_id")
	firstEventID := stringValue(t, asMap(t, first.Result)["event_id"], "event_id")

	second := rpcCall(t, handler, eventPublishBody("", runStartTestFingerprint, "scan.requested", `{"topic":"second"}`, "", "idem-source-second"))
	if second.Error != nil {
		t.Fatalf("second event.publish error = %#v", second.Error)
	}
	secondEventID := stringValue(t, asMap(t, second.Result)["event_id"], "event_id")

	cases := []struct {
		name          string
		body          string
		wantJSONCode  int
		wantAppCode   string
		wantField     string
		mutateBefore  func()
		wantEventRows int
		wantIDEMRows  int
	}{
		{
			name:          "source without explicit run",
			body:          eventPublishBodyWithSource("", firstEventID, runStartTestFingerprint, "scan.requested", `{"topic":"no-run"}`, "", "idem-source-no-run"),
			wantJSONCode:  codeInvalidParams,
			wantField:     "run_id",
			wantEventRows: 2,
			wantIDEMRows:  2,
		},
		{
			name:          "invalid source uuid",
			body:          eventPublishBodyWithSource(firstRunID, "not-a-uuid", runStartTestFingerprint, "scan.requested", `{"topic":"bad-source"}`, "", "idem-source-invalid"),
			wantJSONCode:  codeInvalidParams,
			wantField:     "source_event_id",
			wantEventRows: 2,
			wantIDEMRows:  2,
		},
		{
			name:          "missing source event",
			body:          eventPublishBodyWithSource(firstRunID, uuid.NewString(), runStartTestFingerprint, "scan.requested", `{"topic":"missing-source"}`, "", "idem-source-missing"),
			wantAppCode:   EventNotFoundCode,
			wantEventRows: 2,
			wantIDEMRows:  2,
		},
		{
			name:          "missing target run with source event",
			body:          eventPublishBodyWithSource(uuid.NewString(), firstEventID, runStartTestFingerprint, "scan.requested", `{"topic":"missing-run-source"}`, "", "idem-source-missing-run"),
			wantAppCode:   RunNotFoundCode,
			wantEventRows: 2,
			wantIDEMRows:  2,
		},
		{
			name:          "cross run source event",
			body:          eventPublishBodyWithSource(firstRunID, secondEventID, runStartTestFingerprint, "scan.requested", `{"topic":"cross-run"}`, "", "idem-source-cross-run"),
			wantJSONCode:  codeInvalidParams,
			wantField:     "source_event_id",
			wantEventRows: 2,
			wantIDEMRows:  2,
		},
		{
			name: "terminal run with source",
			body: eventPublishBodyWithSource(firstRunID, firstEventID, runStartTestFingerprint, "scan.requested", `{"topic":"terminal"}`, "", "idem-source-terminal"),
			mutateBefore: func() {
				if _, err := db.Exec(`UPDATE runs SET status = 'completed', ended_at = now() WHERE run_id = $1::uuid`, firstRunID); err != nil {
					t.Fatalf("mark run terminal: %v", err)
				}
			},
			wantAppCode:   RunAlreadyTerminalCode,
			wantEventRows: 2,
			wantIDEMRows:  2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.mutateBefore != nil {
				tc.mutateBefore()
			}
			resp := rpcCall(t, handler, tc.body)
			if resp.Error == nil {
				t.Fatal("event.publish source_event_id error = nil")
			}
			if tc.wantAppCode != "" {
				if data := asMap(t, resp.Error.Data); data["code"] != tc.wantAppCode {
					t.Fatalf("application error data = %#v, want %s", data, tc.wantAppCode)
				}
			} else if resp.Error.Code != tc.wantJSONCode {
				t.Fatalf("json-rpc error code = %d, want %d", resp.Error.Code, tc.wantJSONCode)
			} else if details := asMap(t, asMap(t, resp.Error.Data)["details"]); details["field"] != tc.wantField {
				t.Fatalf("invalid params details = %#v, want field %s", details, tc.wantField)
			}
			if count := countEventsByName(t, db, "scan.requested"); count != tc.wantEventRows {
				t.Fatalf("scan.requested event rows = %d, want %d", count, tc.wantEventRows)
			}
			if count := countAPIIdempotencyRows(t, db); count != tc.wantIDEMRows {
				t.Fatalf("api_idempotency rows = %d, want %d", count, tc.wantIDEMRows)
			}
		})
	}
}

func TestOperatorEventPublishHandlersFailClosedBeforePersistence(t *testing.T) {
	t.Run("non-routable bundle hash", func(t *testing.T) {
		_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
		pg := &store.PostgresStore{DB: db}
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := runtimebus.NewEventBusWithOptions(pg, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := eventPublishTestHandler(t, pg, bus, source)

		resp := rpcCall(t, handler, eventPublishBody("", "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "scan.requested", `{"topic":"medicine"}`, "", "idem-event-mismatch"))
		if resp.Error == nil {
			t.Fatal("event.publish non-routable bundle error = nil")
		}
		if data := asMap(t, resp.Error.Data); data["code"] != BundleUnavailableCode {
			t.Fatalf("bundle unavailable data = %#v", data)
		}
		assertNoEventPublishPersistence(t, db)
	})

	t.Run("invalid canonical bundle hash", func(t *testing.T) {
		_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
		pg := &store.PostgresStore{DB: db}
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := runtimebus.NewEventBusWithOptions(pg, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := eventPublishTestHandler(t, pg, bus, source)

		resp := rpcCall(t, handler, eventPublishBodyWithBundleHash("", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "scan.requested", `{"topic":"medicine"}`, "", "idem-event-invalid-bundle-hash"))
		if resp.Error == nil {
			t.Fatal("event.publish invalid bundle_hash error = nil")
		}
		if data := asMap(t, resp.Error.Data); data["code"] != UnsupportedBundleHashCode {
			t.Fatalf("unsupported bundle hash data = %#v", data)
		}
		assertNoEventPublishPersistence(t, db)
	})

	t.Run("canonical and legacy bundle params conflict", func(t *testing.T) {
		_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
		pg := &store.PostgresStore{DB: db}
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := runtimebus.NewEventBusWithOptions(pg, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := eventPublishTestHandler(t, pg, bus, source)

		resp := rpcCall(t, handler, eventPublishBodyWithBothBundleInputs("", runStartTestBundleHash, runStartTestFingerprint, "scan.requested", `{"topic":"medicine"}`, "", "idem-event-bundle-conflict"))
		if resp.Error == nil {
			t.Fatal("event.publish bundle input conflict error = nil")
		}
		if data := asMap(t, resp.Error.Data); data["code"] != UnsupportedBundleHashCode {
			t.Fatalf("bundle input conflict data = %#v", data)
		}
		assertNoEventPublishPersistence(t, db)
	})

	t.Run("undeclared event", func(t *testing.T) {
		_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
		pg := &store.PostgresStore{DB: db}
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := runtimebus.NewEventBusWithOptions(pg, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := eventPublishTestHandler(t, pg, bus, source)

		resp := rpcCall(t, handler, eventPublishBody("", runStartTestFingerprint, "scan.missing", `{"topic":"medicine"}`, "", "idem-event-missing"))
		if resp.Error == nil {
			t.Fatal("event.publish undeclared event error = nil")
		}
		if data := asMap(t, resp.Error.Data); data["code"] != EventNotDeclaredCode {
			t.Fatalf("undeclared event data = %#v", data)
		}
		assertNoEventPublishPersistence(t, db)
	})

	t.Run("payload validation", func(t *testing.T) {
		_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
		pg := &store.PostgresStore{DB: db}
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
			ContractBundle:   source,
			BundleSourceFact: runStartTestBundleSourceFact(),
			PayloadValidator: func(eventType string, payload []byte) error {
				if eventType != "scan.requested" {
					return fmt.Errorf("unexpected event type %q", eventType)
				}
				return errors.New("schema violation")
			},
		})
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := eventPublishTestHandler(t, pg, bus, source)

		resp := rpcCall(t, handler, eventPublishBody("", runStartTestFingerprint, "scan.requested", `{"topic":"medicine"}`, "", "idem-event-invalid-payload"))
		if resp.Error == nil {
			t.Fatal("event.publish payload validation error = nil")
		}
		if data := asMap(t, resp.Error.Data); data["code"] != PayloadValidationFailedCode {
			t.Fatalf("payload validation data = %#v", data)
		}
		assertNoEventPublishPersistence(t, db)
	})

	t.Run("invalid run id", func(t *testing.T) {
		_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
		pg := &store.PostgresStore{DB: db}
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := runtimebus.NewEventBusWithOptions(pg, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := eventPublishTestHandler(t, pg, bus, source)

		resp := rpcCall(t, handler, eventPublishBody("abc", runStartTestFingerprint, "scan.requested", `{"topic":"medicine"}`, "", "idem-event-invalid-run-id"))
		if resp.Error == nil || resp.Error.Code != codeInvalidParams {
			t.Fatalf("event.publish invalid run_id error = %#v, want invalid params", resp.Error)
		}
		assertNoEventPublishPersistence(t, db)
	})
}

func TestOperatorEventPublishQueuesWhileRuntimePaused(t *testing.T) {
	_, db, cleanup := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	t.Cleanup(cleanup)
	pg := &store.PostgresStore{DB: db}
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := runtimebus.NewEventBusWithOptions(pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	controller := runtimeingress.NewController(pg, bus, runtimeingress.Options{})
	t.Cleanup(runtimebus.ResumeRuntimeIngress)
	bus.SetRuntimeIngressDispatchGate(controller)

	ctx := context.Background()
	agentID := "scan-orchestrator"
	seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, agentID)
	ch := bus.Subscribe(agentID, events.EventType("scan.requested"))
	defer bus.Unsubscribe(agentID)

	if _, err := controller.Pause(ctx, runtimeingress.TransitionRequest{
		Reason:       "test_pause",
		ControlledBy: "test",
		Now:          time.Now().UTC(),
	}); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	handler := eventPublishTestHandler(t, pg, bus, source)
	published := rpcCall(t, handler, eventPublishBody("", runStartTestFingerprint, "scan.requested", `{"topic":"paused"}`, "", "idem-paused-publish"))
	if published.Error != nil {
		t.Fatalf("paused event.publish error = %#v", published.Error)
	}
	eventID := stringValue(t, asMap(t, published.Result)["event_id"], "event_id")
	deliveries := asSlice(t, asMap(t, published.Result)["deliveries"])
	if len(deliveries) != 2 {
		t.Fatalf("paused event.publish deliveries = %#v, want typed agent and node deliveries", deliveries)
	}
	assertEventPublishDeliveriesContain(t, deliveries, "agent", "scan-orchestrator", "pending", 1)
	assertEventPublishDeliveriesContain(t, deliveries, "node", "scan-orchestrator", "pending", 1)
	requireNoAPIV1RuntimeBusEvent(t, ch, "paused event.publish before resume")
	if got := countEventDeliveriesForEvent(t, ctx, db, eventID); got != 1 {
		t.Fatalf("paused event deliveries = %d, want 1 queued route", got)
	}
	if got := countPipelineReceiptsForEvent(t, ctx, db, eventID); got != 0 {
		t.Fatalf("paused pipeline receipts = %d, want 0", got)
	}

	resumed, err := controller.Resume(ctx, runtimeingress.TransitionRequest{
		Reason:       "test_resume",
		ControlledBy: "test",
		Now:          time.Now().UTC().Add(time.Second),
	})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if resumed.ReleasedCount != 1 {
		t.Fatalf("released count = %d, want 1", resumed.ReleasedCount)
	}
	got := requireAPIV1RuntimeBusEvent(t, ch, "queued event.publish release")
	if got.ID() != eventID {
		t.Fatalf("released event = %s, want %s", got.ID(), eventID)
	}
	if got := countPipelineReceiptsForEvent(t, ctx, db, eventID); got != 1 {
		t.Fatalf("pipeline receipts after resume = %d, want 1", got)
	}
}

func eventPublishTestHandler(t *testing.T, pg *store.PostgresStore, bus *runtimebus.EventBus, source semanticview.Source) *Handler {
	t.Helper()
	return eventPublishTestHandlerWithObservability(t, pg, bus, source, pg)
}

func eventPublishTestHandlerWithObservability(t *testing.T, pg *store.PostgresStore, bus *runtimebus.EventBus, source semanticview.Source, observability ObservabilityReadStore) *Handler {
	t.Helper()
	return eventPublishTestHandlerWithStores(t, pg, observability, pg, bus, source)
}

func eventPublishTestHandlerWithStores(t *testing.T, runs RunReadStore, observability ObservabilityReadStore, idempotency APIIdempotencyStore, publisher EventPublisher, source semanticview.Source) *Handler {
	t.Helper()
	runBundleContext, _ := runs.(RunBundleContextStore)
	entities, _ := runs.(EntityReadStore)
	return testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:              func() time.Time { return time.Now().UTC() },
			Ready:            func() bool { return true },
			Database:         fakePinger{},
			Runs:             runs,
			Observability:    observability,
			Entities:         entities,
			Idempotency:      idempotency,
			Events:           publisher,
			Source:           source,
			RunBundleContext: runBundleContext,
			Bundle: runtimecontracts.BundleIdentity{
				WorkflowName:    "review",
				WorkflowVersion: "1.0.0",
				Fingerprint:     runStartTestFingerprint,
			},
		}),
	})
}

type blockingAPIV1PublishInterceptor struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (i blockingAPIV1PublishInterceptor) Intercept(_ context.Context, _ events.Event) (bool, []events.Event, error) {
	select {
	case i.started <- struct{}{}:
	default:
	}
	<-i.release
	return true, nil, nil
}

type legacyOnlyEventPublisher struct {
	publishCalls int
}

func (p *legacyOnlyEventPublisher) Publish(context.Context, events.Event) error {
	p.publishCalls++
	return nil
}

func (p *legacyOnlyEventPublisher) WithBundleFingerprint(ctx context.Context) context.Context {
	return runtimecorrelation.WithBundleSourceFact(ctx, runStartTestBundleSourceFact())
}

type failStandalonePipelineReceiptOnceStore struct {
	*store.PostgresStore
	err error
}

func (s *failStandalonePipelineReceiptOnceStore) UpsertPipelineReceipt(ctx context.Context, eventID, status string, failure *runtimefailures.Envelope) error {
	return s.UpsertPipelineReceiptTx(ctx, nil, eventID, status, failure)
}

func (s *failStandalonePipelineReceiptOnceStore) UpsertPipelineReceiptTx(ctx context.Context, tx *sql.Tx, eventID, status string, failure *runtimefailures.Envelope) error {
	if tx == nil && s.err != nil {
		err := s.err
		s.err = nil
		return err
	}
	return s.PostgresStore.UpsertPipelineReceiptTx(ctx, tx, eventID, status, failure)
}

type failCommittedReplayScopeStore struct {
	*store.PostgresStore
	err error
}

type failCommittedReplayScopeMutation struct {
	ctx   context.Context
	tx    *sql.Tx
	store *failCommittedReplayScopeStore
}

func (s *failCommittedReplayScopeStore) RunEventMutation(ctx context.Context, fn func(runtimebus.EventMutation) error) error {
	if fn == nil {
		return nil
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	postCommit := make([]func(), 0, 4)
	txctx := runtimepipeline.WithPipelineSQLTxContext(ctx, tx)
	txctx = runtimepipeline.WithPipelinePostCommitActions(txctx, &postCommit)
	mutation := &failCommittedReplayScopeMutation{tx: tx, store: s}
	mutation.ctx = runtimebus.WithEventMutationContext(txctx, mutation)
	if err := fn(mutation); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	runtimepipeline.FlushPipelinePostCommitActions(postCommit)
	return nil
}

func (m *failCommittedReplayScopeMutation) Context() context.Context {
	return m.ctx
}

func (m *failCommittedReplayScopeMutation) AppendEvent(ctx context.Context, evt events.Event) error {
	return m.store.AppendEventTx(ctx, m.tx, evt)
}

func (m *failCommittedReplayScopeMutation) InsertEventDeliveries(ctx context.Context, eventID string, agentIDs []string) error {
	return m.store.InsertEventDeliveriesTx(ctx, m.tx, eventID, agentIDs)
}

func (m *failCommittedReplayScopeMutation) InsertEventDeliveriesWithTargets(ctx context.Context, eventID string, agentIDs []string, deliveryTargets map[string]events.RouteIdentity) error {
	return m.store.InsertEventDeliveriesWithTargetsTx(ctx, m.tx, eventID, agentIDs, deliveryTargets)
}

func (m *failCommittedReplayScopeMutation) InsertEventDeliveryRoutes(ctx context.Context, eventID string, routes []events.DeliveryRoute) error {
	return m.store.InsertEventDeliveryRoutesTx(ctx, m.tx, eventID, routes)
}

func (m *failCommittedReplayScopeMutation) UpsertCommittedReplayScope(ctx context.Context, eventID string, scope runtimereplayclaim.CommittedReplayScope) error {
	return m.store.UpsertCommittedReplayScopeTx(ctx, m.tx, eventID, scope)
}

func (m *failCommittedReplayScopeMutation) UpsertPipelineReceipt(ctx context.Context, eventID, status string, failure *runtimefailures.Envelope) error {
	return m.store.UpsertPipelineReceiptTx(ctx, m.tx, eventID, status, failure)
}

func (m *failCommittedReplayScopeMutation) RecordDeadLetter(ctx context.Context, rec runtimedeadletters.Record) error {
	return m.store.RecordDeadLetterTx(ctx, m.tx, rec)
}

func (s *failCommittedReplayScopeStore) UpsertCommittedReplayScopeTx(ctx context.Context, tx *sql.Tx, eventID string, scope runtimereplayclaim.CommittedReplayScope) error {
	if s.err != nil {
		return s.err
	}
	return s.PostgresStore.UpsertCommittedReplayScopeTx(ctx, tx, eventID, scope)
}

type failNormalRunCompletionStore struct {
	*store.PostgresStore
	err error
}

func (s *failNormalRunCompletionStore) ConvergeNormalRunCompletion(context.Context, string, []string, map[string][]string) error {
	return s.err
}

type failOnceEventReadStore struct {
	ObservabilityReadStore
	err error
}

func (s *failOnceEventReadStore) LoadOperatorEvent(ctx context.Context, eventID string) (store.OperatorEventFull, error) {
	if s.err != nil {
		err := s.err
		s.err = nil
		return store.OperatorEventFull{}, err
	}
	return s.ObservabilityReadStore.LoadOperatorEvent(ctx, eventID)
}

func eventPublishBody(runID, fingerprint, eventName, payload, emitter, idempotencyKey string) string {
	return eventPublishBodyWithSource(runID, "", fingerprint, eventName, payload, emitter, idempotencyKey)
}

func eventPublishBodyWithBundleHash(runID, bundleHash, eventName, payload, emitter, idempotencyKey string) string {
	parts := []string{
		fmt.Sprintf(`"bundle_hash":%q`, bundleHash),
		fmt.Sprintf(`"event_name":%q`, eventName),
		fmt.Sprintf(`"payload":%s`, payload),
		fmt.Sprintf(`"idempotency_key":%q`, idempotencyKey),
	}
	if strings.TrimSpace(runID) != "" {
		parts = append(parts, fmt.Sprintf(`"run_id":%q`, runID))
	}
	if strings.TrimSpace(emitter) != "" {
		parts = append(parts, fmt.Sprintf(`"emitter":%q`, emitter))
	}
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":"publish","method":"event.publish","params":{%s}}`, strings.Join(parts, ","))
}

func eventPublishBodyWithoutBundle(runID, eventName, payload, emitter, idempotencyKey string) string {
	parts := []string{
		fmt.Sprintf(`"event_name":%q`, eventName),
		fmt.Sprintf(`"payload":%s`, payload),
		fmt.Sprintf(`"idempotency_key":%q`, idempotencyKey),
	}
	if strings.TrimSpace(runID) != "" {
		parts = append(parts, fmt.Sprintf(`"run_id":%q`, runID))
	}
	if strings.TrimSpace(emitter) != "" {
		parts = append(parts, fmt.Sprintf(`"emitter":%q`, emitter))
	}
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":"publish","method":"event.publish","params":{%s}}`, strings.Join(parts, ","))
}

func eventPublishBodyWithBothBundleInputs(runID, bundleHash, fingerprint, eventName, payload, emitter, idempotencyKey string) string {
	parts := []string{
		fmt.Sprintf(`"bundle_hash":%q`, bundleHash),
		fmt.Sprintf(`"bundle_ref":{"fingerprint":%q}`, fingerprint),
		fmt.Sprintf(`"event_name":%q`, eventName),
		fmt.Sprintf(`"payload":%s`, payload),
		fmt.Sprintf(`"idempotency_key":%q`, idempotencyKey),
	}
	if strings.TrimSpace(runID) != "" {
		parts = append(parts, fmt.Sprintf(`"run_id":%q`, runID))
	}
	if strings.TrimSpace(emitter) != "" {
		parts = append(parts, fmt.Sprintf(`"emitter":%q`, emitter))
	}
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":"publish","method":"event.publish","params":{%s}}`, strings.Join(parts, ","))
}

func eventPublishBodyWithSource(runID, sourceEventID, fingerprint, eventName, payload, emitter, idempotencyKey string) string {
	parts := []string{
		fmt.Sprintf(`"bundle_hash":%q`, runStartTestBundleHashForFingerprint(fingerprint)),
		fmt.Sprintf(`"event_name":%q`, eventName),
		fmt.Sprintf(`"payload":%s`, payload),
		fmt.Sprintf(`"idempotency_key":%q`, idempotencyKey),
	}
	if strings.TrimSpace(runID) != "" {
		parts = append(parts, fmt.Sprintf(`"run_id":%q`, runID))
	}
	if strings.TrimSpace(sourceEventID) != "" {
		parts = append(parts, fmt.Sprintf(`"source_event_id":%q`, sourceEventID))
	}
	if strings.TrimSpace(emitter) != "" {
		parts = append(parts, fmt.Sprintf(`"emitter":%q`, emitter))
	}
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":"publish","method":"event.publish","params":{%s}}`, strings.Join(parts, ","))
}

func eventPublishBodyWithTarget(runID, sourceEventID, fingerprint, eventName, payload, emitter, idempotencyKey, flowInstance, entityID string) string {
	parts := []string{
		fmt.Sprintf(`"bundle_hash":%q`, runStartTestBundleHashForFingerprint(fingerprint)),
		fmt.Sprintf(`"event_name":%q`, eventName),
		fmt.Sprintf(`"payload":%s`, payload),
		fmt.Sprintf(`"idempotency_key":%q`, idempotencyKey),
		fmt.Sprintf(`"target":{"flow_instance":%q,"entity_id":%q}`, flowInstance, entityID),
	}
	if strings.TrimSpace(runID) != "" {
		parts = append(parts, fmt.Sprintf(`"run_id":%q`, runID))
	}
	if strings.TrimSpace(sourceEventID) != "" {
		parts = append(parts, fmt.Sprintf(`"source_event_id":%q`, sourceEventID))
	}
	if strings.TrimSpace(emitter) != "" {
		parts = append(parts, fmt.Sprintf(`"emitter":%q`, emitter))
	}
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":"publish","method":"event.publish","params":{%s}}`, strings.Join(parts, ","))
}

func eventPublishBodyWithLegacyFingerprint(runID, fingerprint, eventName, payload, emitter, idempotencyKey string) string {
	parts := []string{
		fmt.Sprintf(`"bundle_ref":{"fingerprint":%q}`, fingerprint),
		fmt.Sprintf(`"event_name":%q`, eventName),
		fmt.Sprintf(`"payload":%s`, payload),
		fmt.Sprintf(`"idempotency_key":%q`, idempotencyKey),
	}
	if strings.TrimSpace(runID) != "" {
		parts = append(parts, fmt.Sprintf(`"run_id":%q`, runID))
	}
	if strings.TrimSpace(emitter) != "" {
		parts = append(parts, fmt.Sprintf(`"emitter":%q`, emitter))
	}
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":"publish","method":"event.publish","params":{%s}}`, strings.Join(parts, ","))
}

func flowScopedEventPublishTestBundle() *runtimecontracts.WorkflowContractBundle {
	return flowScopedEventPublishBundle(map[string]string{
		"repo-scaffold": "repo_scaffold.repo_commit_succeeded",
	})
}

func ambiguousFlowScopedEventPublishTestBundle() *runtimecontracts.WorkflowContractBundle {
	return flowScopedEventPublishBundle(map[string]string{
		"alpha-flow": "workflow.completed",
		"beta-flow":  "workflow.completed",
	})
}

func rootAndAmbiguousFlowScopedEventPublishTestBundle() *runtimecontracts.WorkflowContractBundle {
	bundle := ambiguousFlowScopedEventPublishTestBundle()
	bundle.Events = map[string]runtimecontracts.EventCatalogEntry{
		"workflow.completed": {},
	}
	return bundle
}

func flowScopedEventPublishBundle(eventsByFlow map[string]string) *runtimecontracts.WorkflowContractBundle {
	flows := make([]runtimecontracts.FlowContractView, 0, len(eventsByFlow))
	byID := make(map[string]*runtimecontracts.FlowContractView, len(eventsByFlow))
	for flowID, eventName := range eventsByFlow {
		flowID = strings.TrimSpace(flowID)
		eventName = strings.TrimSpace(eventName)
		nodeID := flowID + "-observer"
		if flowID == "repo-scaffold" {
			nodeID = "repo-observer"
		}
		flows = append(flows, runtimecontracts.FlowContractView{
			Paths: runtimecontracts.FlowContractPaths{ID: flowID, Flow: flowID},
			Path:  flowID,
			Events: map[string]runtimecontracts.EventCatalogEntry{
				eventName: {},
			},
			Nodes: map[string]runtimecontracts.SystemNodeContract{
				nodeID: {
					ID:           nodeID,
					SubscribesTo: []string{eventName},
				},
			},
		})
	}
	sort.Slice(flows, func(i, j int) bool {
		return strings.TrimSpace(flows[i].Paths.ID) < strings.TrimSpace(flows[j].Paths.ID)
	})
	root := runtimecontracts.FlowContractView{Children: flows}
	for i := range root.Children {
		flow := &root.Children[i]
		byID[strings.TrimSpace(flow.Paths.ID)] = flow
	}
	return &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{Name: "review", Version: "1.0.0"},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: byID,
		},
	}
}

func eventPublishFollowUpTestBundle() *runtimecontracts.WorkflowContractBundle {
	eventsByName := map[string]runtimecontracts.EventCatalogEntry{
		"scan.requested": {},
		"scan.followup":  {},
		"scan.unhandled": {},
	}
	node := runtimecontracts.SystemNodeContract{
		ID:           "scan-orchestrator",
		SubscribesTo: []string{"scan.requested", "scan.followup"},
	}
	flow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "discovery", Flow: "discovery"},
		Path:  "discovery",
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"scan.requested"}},
			},
		},
		Events: eventsByName,
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scan-orchestrator": node,
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}}
	return &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{Name: "review", Version: "1.0.0"},
		Events:    eventsByName,
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scan-orchestrator": node,
		},
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"scan.requested"}},
			},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"discovery": &root.Children[0],
			},
		},
	}
}

func eventPublishTargetRouteTestBundle() *runtimecontracts.WorkflowContractBundle {
	bootstrapEvent := "bootstrap.requested"
	targetEvent := "opco.product_initialization_requested"
	bootstrapNode := runtimecontracts.SystemNodeContract{
		ID:           "bootstrap-node",
		SubscribesTo: []string{bootstrapEvent},
	}
	operatingNode := runtimecontracts.SystemNodeContract{
		ID:            "lifecycle-orchestrator",
		ExecutionType: "system_node",
		SubscribesTo:  []string{targetEvent},
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
			targetEvent: {},
		},
	}
	operating := runtimecontracts.FlowContractView{
		Path:  "operating",
		Paths: runtimecontracts.FlowContractPaths{ID: "operating", Flow: "operating", Mode: "template"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: "template",
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{targetEvent}},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			targetEvent: {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"lifecycle-orchestrator": operatingNode,
		},
	}
	root := runtimecontracts.FlowContractView{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			bootstrapEvent: {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"bootstrap-node": bootstrapNode,
		},
		Children: []runtimecontracts.FlowContractView{operating},
	}
	return &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{Name: "review", Version: "1.0.0"},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			bootstrapEvent: {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"bootstrap-node": bootstrapNode,
		},
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{bootstrapEvent}},
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"operating": operating.Schema,
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"operating": &root.Children[0],
			},
		},
	}
}

func eventPublishRootTemplateCollisionTestBundle() *runtimecontracts.WorkflowContractBundle {
	eventName := "review.requested"
	rootNode := runtimecontracts.SystemNodeContract{
		ID:           "root-orchestrator",
		SubscribesTo: []string{eventName},
	}
	operating := runtimecontracts.FlowContractView{
		Path:  "operating",
		Paths: runtimecontracts.FlowContractPaths{ID: "operating", Flow: "operating", Mode: "template"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: "template",
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{eventName}},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			eventName: {},
		},
	}
	root := runtimecontracts.FlowContractView{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			eventName: {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"root-orchestrator": rootNode,
		},
		Children: []runtimecontracts.FlowContractView{operating},
	}
	return &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{Name: "review", Version: "1.0.0"},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			eventName: {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"root-orchestrator": rootNode,
		},
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{eventName}},
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"operating": operating.Schema,
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"operating": &root.Children[0],
			},
		},
	}
}

func eventPublishCreateEntityTestBundle() *runtimecontracts.WorkflowContractBundle {
	const eventName = "thing.created"
	handler := runtimecontracts.SystemNodeEventHandler{CreateEntity: true}
	node := runtimecontracts.SystemNodeContract{
		ID:           "thing-writer",
		SubscribesTo: []string{eventName},
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
			eventName: handler,
		},
	}
	flow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "factory", Flow: "factory"},
		Path:  "factory",
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{eventName}},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			eventName: {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"thing-writer": node,
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}}
	return &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:    "factory",
			Version: "1.0.0",
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"thing-writer": {
					eventName: handler,
				},
			},
			EventOwners: map[string][]string{
				eventName: []string{"thing-writer"},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			eventName: {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"thing-writer": node,
		},
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{eventName}},
			},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"factory": &root.Children[0],
			},
		},
	}
}

func seedEventPublishEntityState(t *testing.T, db *sql.DB, runID, entityID, flowInstance, currentState string) {
	t.Helper()
	if _, err := db.Exec(`
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, $3, 'widget', $4,
			'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, now(), now(), now()
		)
	`, runID, entityID, flowInstance, currentState); err != nil {
		t.Fatalf("seed entity_state: %v", err)
	}
}

func assertEventPublishEventRow(t *testing.T, db *sql.DB, runID, eventID, eventName, producedBy string) {
	t.Helper()
	var gotRunID, gotEventName, gotProducedBy, gotEntityID string
	if err := db.QueryRow(`
		SELECT run_id::text, event_name, produced_by, COALESCE(entity_id::text, '')
		FROM events
		WHERE event_id = $1::uuid
	`, eventID).Scan(&gotRunID, &gotEventName, &gotProducedBy, &gotEntityID); err != nil {
		t.Fatalf("load event.publish event row: %v", err)
	}
	if gotRunID != runID || gotEventName != eventName || gotProducedBy != producedBy {
		t.Fatalf("event row run/event/producer = %q/%q/%q, want %q/%q/%q", gotRunID, gotEventName, gotProducedBy, runID, eventName, producedBy)
	}
	if gotEntityID != "" {
		t.Fatalf("event row entity_id = %q, want empty stateful-context inference for follow-up", gotEntityID)
	}
}

func assertEventPublishTargetRouteRow(t *testing.T, db *sql.DB, runID, eventID, eventName, flowInstance, entityID string) {
	t.Helper()
	var gotRunID, gotEventName, gotEntityID, gotFlowInstance, targetRoute string
	if err := db.QueryRow(`
		SELECT run_id::text, event_name, COALESCE(entity_id::text, ''), COALESCE(flow_instance, ''), COALESCE(target_route::text, '{}')
		FROM events
		WHERE event_id = $1::uuid
	`, eventID).Scan(&gotRunID, &gotEventName, &gotEntityID, &gotFlowInstance, &targetRoute); err != nil {
		t.Fatalf("load target event row: %v", err)
	}
	if gotRunID != runID || gotEventName != eventName || gotEntityID != entityID || gotFlowInstance != flowInstance {
		t.Fatalf("target event row = run:%q event:%q entity:%q flow:%q, want %q/%q/%q/%q", gotRunID, gotEventName, gotEntityID, gotFlowInstance, runID, eventName, entityID, flowInstance)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(targetRoute), &decoded); err != nil {
		t.Fatalf("decode event target_route: %v", err)
	}
	if decoded["flow_instance"] != flowInstance || decoded["entity_id"] != entityID {
		t.Fatalf("event target_route = %#v, want flow/entity %s/%s", decoded, flowInstance, entityID)
	}
}

func assertEventPublishDeliveryTargetRoute(t *testing.T, db *sql.DB, eventID, subscriberType, subscriberID, flowInstance, entityID string) {
	t.Helper()
	var targetRoute string
	if err := db.QueryRow(`
		SELECT COALESCE(delivery_target_route::text, '{}')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = $2
		  AND subscriber_id = $3
		LIMIT 1
	`, eventID, subscriberType, subscriberID).Scan(&targetRoute); err != nil {
		t.Fatalf("load delivery target route: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(targetRoute), &decoded); err != nil {
		t.Fatalf("decode delivery target_route: %v", err)
	}
	if decoded["flow_instance"] != flowInstance || decoded["entity_id"] != entityID {
		t.Fatalf("delivery target_route = %#v, want flow/entity %s/%s", decoded, flowInstance, entityID)
	}
}

func assertEventPublishPersistence(t *testing.T, db *sql.DB, runID, eventID, eventName, producedBy string) {
	t.Helper()
	var runStatus, triggerType, triggerID, bundleHash, bundleSource, legacyFingerprint string
	if err := db.QueryRow(`
		SELECT status, trigger_event_type, trigger_event_id::text, COALESCE(bundle_hash, ''), bundle_source, COALESCE(bundle_fingerprint, '')
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&runStatus, &triggerType, &triggerID, &bundleHash, &bundleSource, &legacyFingerprint); err != nil {
		t.Fatalf("load event.publish run row: %v", err)
	}
	if runStatus != "running" || triggerType != eventName || triggerID != eventID {
		t.Fatalf("run row status=%q trigger=%q/%q, want running/%s/%s", runStatus, triggerType, triggerID, eventName, eventID)
	}
	if bundleHash != runStartTestBundleHash || bundleSource != storerunlifecycle.BundleSourceEphemeral || legacyFingerprint != runStartTestFingerprint {
		t.Fatalf("run row bundle identity = hash:%q source:%q fingerprint:%q, want %s/%s/%s", bundleHash, bundleSource, legacyFingerprint, runStartTestBundleHash, storerunlifecycle.BundleSourceEphemeral, runStartTestFingerprint)
	}
	var entityID, gotProducedBy string
	var payload json.RawMessage
	if err := db.QueryRow(`
		SELECT entity_id::text, produced_by, payload
		FROM events
		WHERE event_id = $1::uuid
	`, eventID).Scan(&entityID, &gotProducedBy, &payload); err != nil {
		t.Fatalf("load event.publish event row: %v", err)
	}
	if entityID != runID {
		t.Fatalf("event entity_id = %q, want run_id %q", entityID, runID)
	}
	if gotProducedBy != producedBy {
		t.Fatalf("event produced_by = %q, want %q", gotProducedBy, producedBy)
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode event.publish payload: %v", err)
	}
	if _, ok := decoded["entity_id"]; ok {
		t.Fatalf("event.publish payload must not carry envelope entity_id: %#v", decoded)
	}
	if decoded["topic"] != "medicine" {
		t.Fatalf("event.publish payload = %#v", decoded)
	}
}

func assertSQLiteEventPublishRows(t *testing.T, db *sql.DB, runID, eventID, eventName, producedBy string) {
	t.Helper()
	var runStatus, triggerType, triggerID string
	if err := db.QueryRow(`
		SELECT status, COALESCE(trigger_event_type, ''), COALESCE(trigger_event_id, '')
		FROM runs
		WHERE run_id = ?
	`, runID).Scan(&runStatus, &triggerType, &triggerID); err != nil {
		t.Fatalf("load sqlite event.publish run row: %v", err)
	}
	if runStatus != "running" || triggerType != eventName || triggerID != eventID {
		t.Fatalf("sqlite run row status=%q trigger=%q/%q, want running/%s/%s", runStatus, triggerType, triggerID, eventName, eventID)
	}
	var entityID, gotProducedBy, payloadText string
	if err := db.QueryRow(`
		SELECT COALESCE(entity_id, ''), COALESCE(produced_by, ''), payload
		FROM events
		WHERE event_id = ?
	`, eventID).Scan(&entityID, &gotProducedBy, &payloadText); err != nil {
		t.Fatalf("load sqlite event.publish event row: %v", err)
	}
	if entityID != runID {
		t.Fatalf("sqlite event entity_id = %q, want run_id %q", entityID, runID)
	}
	if gotProducedBy != producedBy {
		t.Fatalf("sqlite event produced_by = %q, want %q", gotProducedBy, producedBy)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(payloadText), &decoded); err != nil {
		t.Fatalf("decode sqlite event.publish payload: %v", err)
	}
	if _, ok := decoded["entity_id"]; ok {
		t.Fatalf("sqlite event.publish payload must not carry envelope entity_id: %#v", decoded)
	}
	if decoded["topic"] != "medicine" {
		t.Fatalf("sqlite event.publish payload = %#v", decoded)
	}
}

func assertEventSourceEventID(t *testing.T, db *sql.DB, eventID, wantSourceEventID string) {
	t.Helper()
	var got string
	if err := db.QueryRow(`SELECT COALESCE(source_event_id::text, '') FROM events WHERE event_id = $1::uuid`, eventID).Scan(&got); err != nil {
		t.Fatalf("load event source_event_id: %v", err)
	}
	if got != wantSourceEventID {
		t.Fatalf("event source_event_id = %q, want %q", got, wantSourceEventID)
	}
}

func assertNoEventPublishPersistence(t *testing.T, db *sql.DB) {
	t.Helper()
	if count := countAllRunRows(t, db); count != 0 {
		t.Fatalf("run rows = %d, want 0", count)
	}
	if count := countEventsByName(t, db, "scan.requested"); count != 0 {
		t.Fatalf("scan.requested event rows = %d, want 0", count)
	}
	if count := countAPIIdempotencyRows(t, db); count != 0 {
		t.Fatalf("api_idempotency rows = %d, want 0", count)
	}
}

func assertNoFlowScopedEventPublishPersistence(t *testing.T, db *sql.DB) {
	t.Helper()
	if count := countAllRunRows(t, db); count != 0 {
		t.Fatalf("run rows = %d, want 0", count)
	}
	if count := countAllEventRows(t, db); count != 0 {
		t.Fatalf("event rows = %d, want 0", count)
	}
	if count := countAPIIdempotencyRows(t, db); count != 0 {
		t.Fatalf("api_idempotency rows = %d, want 0", count)
	}
}

func countAllEventDeliveries(t *testing.T, db *sql.DB) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM event_deliveries`).Scan(&count); err != nil {
		t.Fatalf("count event_deliveries rows: %v", err)
	}
	return count
}

func countAllEventRows(t *testing.T, db *sql.DB) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&count); err != nil {
		t.Fatalf("count all event rows: %v", err)
	}
	return count
}

func countSQLiteEventsByName(t *testing.T, db *sql.DB, eventName string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_name = ?`, eventName).Scan(&count); err != nil {
		t.Fatalf("count sqlite events: %v", err)
	}
	return count
}

func countSQLiteEventRowsByRunID(t *testing.T, db *sql.DB, runID string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id = ?`, runID).Scan(&count); err != nil {
		t.Fatalf("count sqlite event rows: %v", err)
	}
	return count
}

func countSQLiteAllEventRows(t *testing.T, db *sql.DB) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&count); err != nil {
		t.Fatalf("count sqlite all event rows: %v", err)
	}
	return count
}

func countSQLiteAllRunRows(t *testing.T, db *sql.DB) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM runs`).Scan(&count); err != nil {
		t.Fatalf("count sqlite runs: %v", err)
	}
	return count
}

func countSQLiteAPIIdempotencyRows(t *testing.T, db *sql.DB) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM api_idempotency`).Scan(&count); err != nil {
		t.Fatalf("count sqlite api_idempotency rows: %v", err)
	}
	return count
}

func loadPipelineReceiptOutcomeAndFailure(t *testing.T, ctx context.Context, db *sql.DB, eventID string) (string, *runtimefailures.Envelope) {
	t.Helper()
	var outcome string
	var raw []byte
	if err := db.QueryRowContext(ctx, `
		SELECT outcome, failure
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&outcome, &raw); err != nil {
		t.Fatalf("load pipeline receipt for %s: %v", eventID, err)
	}
	failure, err := runtimefailures.UnmarshalEnvelope(raw)
	if err != nil {
		t.Fatalf("decode pipeline receipt failure for %s: %v", eventID, err)
	}
	return outcome, &failure
}

func containsMissingPipelineReceiptEvent(items []events.PersistedReplayEvent, eventID string) bool {
	for _, evt := range items {
		if strings.TrimSpace(evt.Event.ID()) == strings.TrimSpace(eventID) {
			return true
		}
	}
	return false
}

func stringValue(t *testing.T, value any, field string) string {
	t.Helper()
	text, ok := value.(string)
	if !ok || strings.TrimSpace(text) == "" {
		t.Fatalf("%s = %#v, want non-empty string", field, value)
	}
	return strings.TrimSpace(text)
}

func assertEventPublishDeliveryIdentity(t *testing.T, delivery map[string]any, wantSubscriberType, wantSubscriberID, wantStatus string, wantAttempt int) {
	t.Helper()
	if strings.TrimSpace(stringValue(t, delivery["delivery_id"], "delivery_id")) == "" {
		t.Fatalf("delivery = %#v, want non-empty delivery_id", delivery)
	}
	if delivery["subscriber_type"] != wantSubscriberType ||
		delivery["subscriber_id"] != wantSubscriberID ||
		delivery["status"] != wantStatus ||
		delivery["attempt"] != float64(wantAttempt) {
		t.Fatalf("delivery = %#v, want %s/%s %s attempt %d", delivery, wantSubscriberType, wantSubscriberID, wantStatus, wantAttempt)
	}
}

func assertEventPublishDeliveriesContain(t *testing.T, deliveries []any, wantSubscriberType, wantSubscriberID, wantStatus string, wantAttempt int) {
	t.Helper()
	for _, raw := range deliveries {
		delivery := asMap(t, raw)
		if delivery["subscriber_type"] == wantSubscriberType &&
			delivery["subscriber_id"] == wantSubscriberID &&
			delivery["status"] == wantStatus &&
			delivery["attempt"] == float64(wantAttempt) {
			assertEventPublishDeliveryIdentity(t, delivery, wantSubscriberType, wantSubscriberID, wantStatus, wantAttempt)
			return
		}
	}
	t.Fatalf("deliveries = %#v, want %s/%s %s attempt %d", deliveries, wantSubscriberType, wantSubscriberID, wantStatus, wantAttempt)
}

func validEventPublishSubscriberType(value string) bool {
	switch strings.TrimSpace(value) {
	case "agent", "node":
		return true
	default:
		return false
	}
}

func asSlice(t *testing.T, value any) []any {
	t.Helper()
	items, ok := value.([]any)
	if !ok {
		t.Fatalf("value = %#v, want []any", value)
	}
	return items
}
