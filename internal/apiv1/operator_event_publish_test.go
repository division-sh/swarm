package apiv1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/division-sh/swarm/internal/testutil/flowroutefixture"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/durabledata"
	operatorread "github.com/division-sh/swarm/internal/operatorread"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	storerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	"github.com/division-sh/swarm/internal/store/storetest"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestEventPublicationPayloadProjectsSemanticNumbers(t *testing.T) {
	payload, _, err := eventPublicationPayload(map[string]any{
		"payload": map[string]any{
			"integer": int64(75), "double": float64(75),
			"decimal": json.Number("75.0"), "exponent": json.Number("75e0"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(payload), `{"decimal":75,"double":75,"exponent":75,"integer":75}`; got != want {
		t.Fatalf("event publication payload = %s, want %s", got, want)
	}
}

type canonicalEventPublishProofStore interface {
	runtimebus.EventStore
	RunReadStore
	ObservabilityReadStore
	APIIdempotencyStore
}

func TestEventPublishCanonicalClassAndProvenanceReadbackParity(t *testing.T) {
	type fixture struct {
		store   canonicalEventPublishProofStore
		db      *sql.DB
		dialect authoractivityfixture.Dialect
	}
	for _, backend := range []struct {
		name string
		open func(*testing.T, context.Context) fixture
	}{
		{
			name: "sqlite",
			open: func(t *testing.T, ctx context.Context) fixture {
				sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
				return fixture{store: sqliteStore, db: storetest.DatabaseForTest(sqliteStore), dialect: authoractivityfixture.DialectSQLite}
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T, _ context.Context) fixture {
				_, db, _ := testutil.StartPostgres(t)
				return fixture{store: storetest.AdmitPostgresRuntimeStore(t, db), db: db, dialect: authoractivityfixture.DialectPostgres}
			},
		},
	} {
		t.Run(backend.name, func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			f := backend.open(t, ctx)
			source := semanticview.Wrap(runStartTestBundle("scan.requested"))
			bus, err := newScopedAPITestEventBus(t, f.store, runStartTestEventBusOptions(source))
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			handler := eventPublishTestHandlerWithStores(t, f.store, f.store, f.store, bus, source)

			publish := func(label, body string) (string, string) {
				t.Helper()
				response := rpcCall(t, handler, body)
				if response.Error != nil {
					t.Fatalf("%s event.publish error = %#v", label, response.Error)
				}
				result := asMap(t, response.Result)
				return stringValue(t, result["event_id"], label+" event_id"), stringValue(t, result["run_id"], label+" run_id")
			}
			rootID, runID := publish("new-run root", eventPublishBody("", runStartTestBundleHash, "scan.requested", `{"topic":"root"}`, "", "canonical-root"))
			operatorID, operatorRunID := publish("existing-run operator", eventPublishBody(runID, runStartTestBundleHash, "scan.requested", `{"topic":"operator"}`, "", "canonical-operator"))
			if operatorRunID != runID {
				t.Fatalf("existing-run operator run_id = %s, want %s", operatorRunID, runID)
			}
			referencedID, referencedRunID := publish("referenced operator", eventPublishBodyWithSource(runID, rootID, runStartTestBundleHash, "scan.requested", `{"topic":"referenced"}`, "", "canonical-referenced"))
			if referencedRunID != runID {
				t.Fatalf("referenced operator run_id = %s, want %s", referencedRunID, runID)
			}

			wantProducer, err := events.NewProducerIdentity(events.EventProducerExternal, "cli-publish:"+actorTokenID(testToken))
			if err != nil {
				t.Fatal(err)
			}
			for _, expected := range []struct {
				name         string
				eventID      string
				class        events.EventAdmissionClass
				referenceID  string
				hasReference bool
			}{
				{"new-run root", rootID, events.EventAdmissionRootIngress, "", false},
				{"existing-run operator", operatorID, events.EventAdmissionOperatorInjected, "", false},
				{"referenced operator", referencedID, events.EventAdmissionOperatorInjected, rootID, true},
			} {
				persisted, err := eventfixture.Load(ctx, f.db, f.dialect, expected.eventID)
				if err != nil {
					t.Fatalf("canonical readback %s: %v", expected.name, err)
				}
				if persisted.AdmissionClass() != expected.class || persisted.RunID() != runID || persisted.ParentEventID() != "" || !persisted.Producer().Equal(wantProducer) {
					t.Fatalf("%s canonical identity = class:%s run:%s parent:%s producer:%v", expected.name, persisted.AdmissionClass(), persisted.RunID(), persisted.ParentEventID(), persisted.Producer())
				}
				reference, ok := persisted.OperatorReference()
				if ok != expected.hasReference {
					t.Fatalf("%s operator reference present = %v, want %v", expected.name, ok, expected.hasReference)
				}
				if ok && reference.ReferencedEventID() != expected.referenceID {
					t.Fatalf("%s operator reference = %s, want %s", expected.name, reference.ReferencedEventID(), expected.referenceID)
				}
			}
		})
	}
}

func TestOperatorEventPublishHandlersPersistEventReportDeliveriesAndReplayIdempotency(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	_, targetRunID := seedActiveAPIV1RuntimeBusAgentNewRun(t, context.Background(), pg, "scan-orchestrator")
	ch := runtimebustest.SubscribeForRun(t, bus, targetRunID, "scan-orchestrator", events.EventType("scan.requested"))
	defer runtimebustest.Unsubscribe(bus, "scan-orchestrator")
	handler := eventPublishTestHandler(t, pg, bus, source)
	body := eventPublishBody(targetRunID, runStartTestBundleHash, "scan.requested", `{"topic":"medicine"}`, "", "idem-publish")

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
	if runID != targetRunID || result["new_run_created"] != false {
		t.Fatalf("event.publish run = %s created=%#v, want existing %s", runID, result["new_run_created"], targetRunID)
	}
	deliveries := asSlice(t, result["deliveries"])
	if len(deliveries) != 2 {
		t.Fatalf("deliveries = %#v, want persisted node and agent deliveries", deliveries)
	}
	assertEventPublishDeliveriesContain(t, deliveries, "agent", "scan-orchestrator", "pending", 1)
	assertEventPublishDeliveriesContain(t, deliveries, "node", eventPublishScanNodeID(t), "pending", 1)
	if count := countEventsByName(t, db, "scan.requested"); count != 1 {
		t.Fatalf("scan.requested event count = %d, want 1", count)
	}
	assertExistingRunEventPublishPersistence(t, db, runID, eventID, "cli-publish:"+actorTokenID(testToken))
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
	assertEventPublishDeliveriesContain(t, replayDeliveries, "node", eventPublishScanNodeID(t), "pending", 1)
	if count := countEventsByName(t, db, "scan.requested"); count != 1 {
		t.Fatalf("scan.requested event count after replay = %d, want 1", count)
	}

	conflict := rpcCall(t, handler, eventPublishBody("", runStartTestBundleHash, "scan.requested", `{"topic":"changed"}`, "", "idem-publish"))
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
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	probe := lifecycletest.New(t)
	opts := runStartTestEventBusOptions(source)
	opts.TestLifecycleProbe = probe
	opts.Interceptors = []runtimebus.EventInterceptor{blockingAPIV1PublishInterceptor{started: started, release: release}}
	bus, err := newScopedAPITestEventBus(t, pg, opts)
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ctx, targetRunID := seedActiveAPIV1RuntimeBusAgentNewRun(t, context.Background(), pg, "scan-orchestrator")
	ch := runtimebustest.SubscribeForRun(t, bus, targetRunID, "scan-orchestrator", events.EventType("scan.requested"))
	defer runtimebustest.Unsubscribe(bus, "scan-orchestrator")
	handler := eventPublishTestHandler(t, pg, bus, source)
	body := eventPublishBody(targetRunID, runStartTestBundleHash, "scan.requested", `{"topic":"medicine"}`, "", "idem-quick-ack-publish")

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
	assertEventPublishDeliveriesContain(t, deliveries, "node", eventPublishScanNodeID(t), "pending", 1)
	assertExistingRunEventPublishPersistence(t, db, runID, eventID, "cli-publish:"+actorTokenID(testToken))
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
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := newScopedAPITestEventBus(t, sqliteStore, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := eventPublishTestHandlerWithStores(t, sqliteStore, sqliteStore, sqliteStore, bus, source)
	body := eventPublishBody("", runStartTestBundleHash, "scan.requested", `{"topic":"medicine"}`, "", "idem-sqlite-publish")

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
	assertEventPublishDeliveryIdentity(t, asMap(t, deliveries[0]), "node", eventPublishScanNodeID(t), "pending", 1)
	assertSQLiteEventPublishRows(t, storetest.DatabaseForTest(sqliteStore), runID, eventID, "scan.requested", "cli-publish:"+actorTokenID(testToken))
	if count := countSQLiteAPIIdempotencyRows(t, storetest.DatabaseForTest(sqliteStore)); count != 1 {
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
	if count := countSQLiteEventsByName(t, storetest.DatabaseForTest(sqliteStore), "scan.requested"); count != 1 {
		t.Fatalf("sqlite event rows after replay = %d, want 1", count)
	}

	conflict := rpcCall(t, handler, eventPublishBody("", runStartTestBundleHash, "scan.requested", `{"topic":"changed"}`, "", "idem-sqlite-publish"))
	if conflict.Error == nil {
		t.Fatal("sqlite event.publish idempotency conflict error = nil")
	}
	if data := asMap(t, conflict.Error.Data); data["code"] != IdempotencyConflictCode {
		t.Fatalf("sqlite idempotency conflict data = %#v", data)
	}
	if count := countSQLiteEventsByName(t, storetest.DatabaseForTest(sqliteStore), "scan.requested"); count != 1 {
		t.Fatalf("sqlite event rows after conflict = %d, want 1", count)
	}

	nonIDEM := rpcCall(t, handler, eventPublishBody("", runStartTestBundleHash, "scan.requested", `{"topic":"second"}`, "", ""))
	if nonIDEM.Error != nil {
		t.Fatalf("sqlite non-idempotent event.publish error = %#v", nonIDEM.Error)
	}
	if count := countSQLiteEventsByName(t, storetest.DatabaseForTest(sqliteStore), "scan.requested"); count != 2 {
		t.Fatalf("sqlite event rows after non-idempotent publish = %d, want 2", count)
	}
	if count := countSQLiteAPIIdempotencyRows(t, storetest.DatabaseForTest(sqliteStore)); count != 1 {
		t.Fatalf("sqlite api_idempotency rows after non-idempotent publish = %d, want 1", count)
	}
}

func TestOperatorEventPublishSQLitePayloadFailureLeavesNoIdempotencyCompletionOrRows(t *testing.T) {
	ctx := context.Background()
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := newScopedAPITestEventBus(t, sqliteStore, runtimebus.EventBusOptions{
		ContractBundle:     source,
		SourceArtifactFact: runStartTestSourceArtifactFact(),
		PayloadAdmitter: func(_ context.Context, event events.Event, _ string) (events.PayloadAdmission, error) {
			if event.Type() == "scan.requested" {
				return events.PayloadAdmission{}, errors.New("schema violation")
			}
			return eventtest.PayloadAdmission(event, "", string(event.Type()))
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := eventPublishTestHandlerWithStores(t, sqliteStore, sqliteStore, sqliteStore, bus, source)

	resp := rpcCall(t, handler, eventPublishBody("", runStartTestBundleHash, "scan.requested", `{"topic":"medicine"}`, "", "idem-sqlite-payload-fails"))
	if resp.Error == nil {
		t.Fatal("sqlite event.publish payload validation error = nil")
	}
	if data := asMap(t, resp.Error.Data); data["code"] != PayloadValidationFailedCode {
		t.Fatalf("sqlite payload validation data = %#v", data)
	}
	if count := countSQLiteEventsByName(t, storetest.DatabaseForTest(sqliteStore), "scan.requested"); count != 0 {
		t.Fatalf("sqlite event rows after failed publish = %d, want 0", count)
	}
	if count := countSQLiteAllRunRows(t, storetest.DatabaseForTest(sqliteStore)); count != 0 {
		t.Fatalf("sqlite run rows after failed publish = %d, want 0", count)
	}
	if count := countSQLiteAPIIdempotencyRows(t, storetest.DatabaseForTest(sqliteStore)); count != 0 {
		t.Fatalf("sqlite api_idempotency rows after failed publish = %d, want 0", count)
	}
}

func TestOperatorEventPublishRejectsPrivateFlowDespiteLiveRecipient(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	source := semanticview.Wrap(flowScopedEventPublishTestBundle())
	canonicalEventName := "repo-scaffold/repo_scaffold.repo_commit_succeeded"
	bus, err := newScopedAPITestEventBus(t, pg, runtimebus.EventBusOptions{
		ContractBundle:     source,
		SourceArtifactFact: runStartTestSourceArtifactFact(),
		PayloadAdmitter: func(_ context.Context, event events.Event, flowID string) (events.PayloadAdmission, error) {
			if string(event.Type()) != canonicalEventName {
				return events.PayloadAdmission{}, fmt.Errorf("event type = %q, want %s", event.Type(), canonicalEventName)
			}
			return eventtest.PayloadAdmission(event, flowID, string(event.Type()))
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ctx := testAuthorActivityContextForSource(context.Background(), runStartTestSourceArtifactFact())
	runID := uuid.NewString()
	seedActiveAPIV1RuntimeBusAgentForRun(t, ctx, pg, runID, "repo-observer", "repo-scaffold")
	admission, err := semanticview.AdmitFlowOwnedAgentSubscriptions(source, semanticview.FlowOwnedAgentSubscriptionRequest{
		AgentID:       "repo-observer",
		FlowID:        "repo-scaffold",
		FlowPath:      "repo-scaffold",
		Subscriptions: []string{"repo_scaffold.repo_commit_succeeded"},
	})
	if err != nil {
		t.Fatalf("AdmitFlowOwnedAgentSubscriptions: %v", err)
	}
	ch := runtimebustest.SubscribeIdentity(t, bus, runtimebustest.IdentityForRun(t, runID, "repo-observer", "repo-scaffold"), admission)
	if ch == nil {
		t.Fatal("SubscribeAgent returned nil admitted carrier")
	}
	defer runtimebustest.Unsubscribe(bus, "repo-observer")
	handler := eventPublishTestHandler(t, pg, bus, source)
	body := eventPublishBody(runID, runStartTestBundleHash, "repo-scaffold/repo_scaffold.repo_commit_succeeded", `{"topic":"medicine"}`, "", "idem-flow-scoped")

	published := rpcCall(t, handler, body)
	if published.Error == nil {
		t.Fatal("live private recipient authorized public publication")
	}
	data := asMap(t, published.Error.Data)
	if data["code"] != EventNotDeclaredCode || asMap(t, data["details"])["reason"] != "selected_root_input_required" {
		t.Fatalf("private recipient rejection = %#v", published.Error)
	}
	if got := countAllEventRows(t, db); got != 0 {
		t.Fatalf("private publication wrote %d events", got)
	}
	if got := countAPIIdempotencyRows(t, db); got != 0 {
		t.Fatalf("private publication wrote %d receipts", got)
	}
	select {
	case evt := <-ch:
		t.Fatalf("private publication dispatched %s", evt.ID())
	default:
	}
}

func TestFlowScopedEventPublishDescriptorUsesCanonicalAuthoredIdentity(t *testing.T) {
	source := semanticview.Wrap(flowScopedEventPublishTestBundle())
	const eventName = "repo-scaffold/repo_scaffold.repo_commit_succeeded"
	descriptors, err := runtimepkg.AuthorActivityEventDescriptors(source)
	if err != nil {
		t.Fatalf("AuthorActivityEventDescriptors: %v", err)
	}
	proof := semanticview.ResolveFlowEventProof(source, "repo-scaffold", eventName)
	if !proof.HasSchema || !proof.IsAuthored(source) {
		t.Fatalf("publication proof = %#v, authored catalog = %#v", proof, source.AuthoredResolvedEventCatalog())
	}
	for _, descriptor := range descriptors {
		if descriptor.EventType != eventName {
			continue
		}
		if descriptor.Disposition != "authored" {
			t.Fatalf("registered descriptor = %#v, publication proof = %#v", descriptor, proof)
		}
		return
	}
	t.Fatalf("descriptor %q missing from %#v", eventName, descriptors)
}

func TestOperatorEventPublishSQLiteRejectsPrivateOrdinaryFlowEndpoint(t *testing.T) {
	ctx := context.Background()
	selected := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	source := semanticview.Wrap(flowScopedEventPublishTestBundle())
	const canonicalEventName = "repo-scaffold/repo_scaffold.repo_commit_succeeded"
	bus, err := newScopedAPITestEventBus(t, selected, runtimebus.EventBusOptions{
		ContractBundle: source, SourceArtifactFact: runStartTestSourceArtifactFact(),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := eventPublishTestHandlerWithStores(t, selected, selected, selected, bus, source)
	body := eventPublishBody("", runStartTestBundleHash, canonicalEventName, `{"topic":"medicine"}`, "", "idem-flow-scoped-sqlite")

	published := rpcCall(t, handler, body)
	if published.Error == nil {
		t.Fatal("private static input was publicly admitted")
	}
	data := asMap(t, published.Error.Data)
	if data["code"] != EventNotDeclaredCode || asMap(t, data["details"])["reason"] != "selected_root_input_required" {
		t.Fatalf("private input rejection = %#v", published.Error)
	}
	if got := countSQLiteEventsByName(t, storetest.DatabaseForTest(selected), canonicalEventName); got != 0 {
		t.Fatalf("private publication wrote %d events", got)
	}
	if countSQLiteAllRunRows(t, storetest.DatabaseForTest(selected)) != 0 || countSQLiteAPIIdempotencyRows(t, storetest.DatabaseForTest(selected)) != 0 {
		t.Fatal("private publication wrote a run or receipt")
	}
}

func TestOperatorEventPublishRootEventNameWinsOverFlowLeafAliases(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	source := semanticview.Wrap(rootAndAmbiguousFlowScopedEventPublishTestBundle(t))
	bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := eventPublishTestHandler(t, pg, bus, source)

	published := rpcCall(t, handler, eventPublishBody("", runStartTestBundleHashForSource(source), "item.received", `{"item_id":"medicine","topic":"medicine"}`, "", "idem-root-collision"))
	if published.Error != nil {
		t.Fatalf("event.publish root collision error = %#v", published.Error)
	}
	result := asMap(t, published.Result)
	eventID := stringValue(t, result["event_id"], "event_id")
	runID := stringValue(t, result["run_id"], "run_id")
	if got := countEventsByName(t, db, "item.received"); got != 1 {
		t.Fatalf("item.received event count = %d, want 1", got)
	}
	for _, flowEventName := range []string{"alpha-flow/item.received", "beta-flow/item.received"} {
		if got := countEventsByName(t, db, flowEventName); got != 0 {
			t.Fatalf("%s event count = %d, want 0", flowEventName, got)
		}
	}
	assertEventPublishPersistence(t, db, runID, eventID, "item.received", "cli-publish:"+actorTokenID(testToken), runStartTestBundleHashForSource(source))
}

func TestOperatorEventPublishFlowScopedEventNameFailuresFailClosed(t *testing.T) {
	t.Run("unknown flow scoped event", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := storetest.AdmitPostgresRuntimeStore(t, db)
		source := semanticview.Wrap(flowScopedEventPublishTestBundle())
		bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := eventPublishTestHandler(t, pg, bus, source)

		resp := rpcCall(t, handler, eventPublishBody("", runStartTestBundleHash, "repo-scaffold/repo_scaffold.missing", `{"topic":"medicine"}`, "", "idem-flow-scoped-missing"))
		if resp.Error == nil {
			t.Fatal("event.publish unknown flow-scoped event error = nil")
		}
		data := asMap(t, resp.Error.Data)
		if data["code"] != EventNotDeclaredCode {
			t.Fatalf("unknown flow-scoped data = %#v, want %s", data, EventNotDeclaredCode)
		}
		details := asMap(t, data["details"])
		if details["event_name"] != "repo-scaffold/repo_scaffold.missing" || details["reason"] != "selected_root_input_required" {
			t.Fatalf("unknown flow-scoped details = %#v", details)
		}
		assertNoFlowScopedEventPublishPersistence(t, db)
	})

	t.Run("ambiguous unscoped leaf", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := storetest.AdmitPostgresRuntimeStore(t, db)
		source := semanticview.Wrap(ambiguousFlowScopedEventPublishTestBundle())
		bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := eventPublishTestHandler(t, pg, bus, source)

		resp := rpcCall(t, handler, eventPublishBody("", runStartTestBundleHash, "workflow.completed", `{"topic":"medicine"}`, "", "idem-flow-scoped-ambiguous"))
		if resp.Error == nil {
			t.Fatal("event.publish ambiguous leaf error = nil")
		}
		data := asMap(t, resp.Error.Data)
		if data["code"] != EventNotDeclaredCode {
			t.Fatalf("ambiguous leaf data = %#v, want %s", data, EventNotDeclaredCode)
		}
		details := asMap(t, data["details"])
		if details["event_name"] != "workflow.completed" || details["reason"] != "selected_root_input_required" {
			t.Fatalf("ambiguous leaf details = %#v", details)
		}
		assertNoFlowScopedEventPublishPersistence(t, db)
	})
}

func TestOperatorEventPublishHandlersRequireCanonicalBundleHashForCreateNewWork(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := eventPublishTestHandler(t, pg, bus, source)

	resp := rpcCall(t, handler, eventPublishBodyWithRetiredBundleInput("", runStartTestBundleHash, "scan.requested", `{"topic":"medicine"}`, "", "idem-publish-retired"))
	assertInvalidRunStartParam(t, resp, retiredBundleInputName())
	assertNoEventPublishPersistence(t, db)
}

func TestOperatorEventPublishPostgresUsesPublisherScopeWithPlainRequestContext(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	_, targetRunID := seedActiveAPIV1RuntimeBusAgentNewRun(t, context.Background(), pg, "scan-orchestrator")
	ch := runtimebustest.SubscribeForRun(t, bus, targetRunID, "scan-orchestrator", events.EventType("scan.requested"))
	defer runtimebustest.Unsubscribe(bus, "scan-orchestrator")
	handler := eventPublishTestHandler(t, pg, bus, source)

	published := rpcCallWithPlainRequestContext(t, handler, eventPublishBodyWithoutBundle(targetRunID, "scan.requested", `{"topic":"medicine"}`, "", "idem-publish-no-bundle"))
	if published.Error != nil {
		t.Fatalf("event.publish active ephemeral bundle scope error = %#v", published.Error)
	}
	result := asMap(t, published.Result)
	eventID := stringValue(t, result["event_id"], "event_id")
	runID := stringValue(t, result["run_id"], "run_id")
	assertExistingRunEventPublishPersistence(t, db, runID, eventID, "cli-publish:"+actorTokenID(testToken))
	if got := countEventsByName(t, db, "scan.requested"); got != 1 {
		t.Fatalf("scan.requested event count = %d, want 1", got)
	}
	got := requireAPIV1RuntimeBusEvent(t, ch, "event.publish delivery")
	if got.ID() != eventID {
		t.Fatalf("delivered event = %s, want %s", got.ID(), eventID)
	}

	explicit := rpcCallWithPlainRequestContext(t, handler, eventPublishBodyWithBundleHash(targetRunID, runStartTestBundleHash, "scan.requested", `{"topic":"medicine"}`, "", "idem-publish-explicit-bundle"))
	if explicit.Error != nil {
		t.Fatalf("event.publish explicit active bundle scope error = %#v", explicit.Error)
	}
	explicitResult := asMap(t, explicit.Result)
	explicitEventID := stringValue(t, explicitResult["event_id"], "event_id")
	explicitRunID := stringValue(t, explicitResult["run_id"], "run_id")
	assertExistingRunEventPublishPersistence(t, db, explicitRunID, explicitEventID, "cli-publish:"+actorTokenID(testToken))
	explicitEvent := requireAPIV1RuntimeBusEventID(t, ch, explicitEventID, "explicit-bundle event.publish delivery")
	if explicitEvent.ID() != explicitEventID {
		t.Fatalf("explicit-bundle delivered event = %s, want %s", explicitEvent.ID(), explicitEventID)
	}
}

func TestOperatorEventPublishSQLiteUsesPublisherScopeWithPlainRequestContext(t *testing.T) {
	ctx := context.Background()
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := newScopedAPITestEventBus(t, sqliteStore, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ctx, targetRunID := seedActiveAPIV1RuntimeBusAgentNewRun(t, ctx, sqliteStore, "scan-orchestrator")
	ch := runtimebustest.SubscribeForRun(t, bus, targetRunID, "scan-orchestrator", events.EventType("scan.requested"))
	defer runtimebustest.Unsubscribe(bus, "scan-orchestrator")
	handler := eventPublishTestHandlerWithStores(t, sqliteStore, sqliteStore, sqliteStore, bus, source)

	published := rpcCallWithPlainRequestContext(t, handler, eventPublishBodyWithoutBundle(targetRunID, "scan.requested", `{"topic":"medicine"}`, "", "idem-sqlite-publish-no-bundle"))
	if published.Error != nil {
		t.Fatalf("sqlite event.publish active ephemeral bundle scope error = %#v", published.Error)
	}
	result := asMap(t, published.Result)
	eventID := stringValue(t, result["event_id"], "event_id")
	runID := stringValue(t, result["run_id"], "run_id")
	assertSQLiteExistingRunEventPublishRows(t, storetest.DatabaseForTest(sqliteStore), runID, eventID, "cli-publish:"+actorTokenID(testToken))
	got := requireAPIV1RuntimeBusEvent(t, ch, "sqlite event.publish delivery")
	if got.ID() != eventID {
		t.Fatalf("delivered event = %s, want %s", got.ID(), eventID)
	}

	explicit := rpcCallWithPlainRequestContext(t, handler, eventPublishBodyWithBundleHash(targetRunID, runStartTestBundleHash, "scan.requested", `{"topic":"medicine"}`, "", "idem-sqlite-publish-explicit-bundle"))
	if explicit.Error != nil {
		t.Fatalf("sqlite event.publish explicit active bundle scope error = %#v", explicit.Error)
	}
	explicitResult := asMap(t, explicit.Result)
	explicitEventID := stringValue(t, explicitResult["event_id"], "event_id")
	explicitRunID := stringValue(t, explicitResult["run_id"], "run_id")
	assertSQLiteExistingRunEventPublishRows(t, storetest.DatabaseForTest(sqliteStore), explicitRunID, explicitEventID, "cli-publish:"+actorTokenID(testToken))
	explicitEvent := requireAPIV1RuntimeBusEventID(t, ch, explicitEventID, "sqlite explicit-bundle event.publish delivery")
	if explicitEvent.ID() != explicitEventID {
		t.Fatalf("sqlite explicit-bundle delivered event = %s, want %s", explicitEvent.ID(), explicitEventID)
	}
}

func rpcCallWithPlainRequestContext(t *testing.T, handler *Handler, body string) rpcResponse {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+testToken)
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	var response rpcResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode rpc response: %v body=%s", err, recorder.Body.String())
	}
	return response
}

func TestOperatorEventPublishReturnsStoredCompletionWithoutPostCommitReadback(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	_, targetRunID := seedActiveAPIV1RuntimeBusAgentNewRun(t, context.Background(), pg, "scan-orchestrator")
	ch := runtimebustest.SubscribeForRun(t, bus, targetRunID, "scan-orchestrator", events.EventType("scan.requested"))
	defer runtimebustest.Unsubscribe(bus, "scan-orchestrator")
	observability := &failOnceEventReadStore{
		ObservabilityReadStore: pg,
		err:                    errors.New("transient event readback failure"),
	}
	handler := eventPublishTestHandlerWithObservability(t, pg, bus, source, observability)
	body := eventPublishBody(targetRunID, runStartTestBundleHash, "scan.requested", `{"topic":"medicine"}`, "", "idem-readback")

	first := rpcCall(t, handler, body)
	if first.Error != nil {
		t.Fatalf("first event.publish error = %#v", first.Error)
	}
	if count := countEventsByName(t, db, "scan.requested"); count != 1 {
		t.Fatalf("scan.requested event count = %d, want 1", count)
	}
	if count := countAPIIdempotencyRows(t, db); count != 1 {
		t.Fatalf("api_idempotency rows = %d, want 1", count)
	}
	var storedResponse []byte
	if err := db.QueryRow(`SELECT response FROM api_idempotency WHERE method='event.publish' AND idempotency_key='idem-readback'`).Scan(&storedResponse); err != nil {
		t.Fatalf("load stored event.publish response: %v", err)
	}
	var storedResult any
	if err := json.Unmarshal(storedResponse, &storedResult); err != nil {
		t.Fatalf("decode stored event.publish response: %v", err)
	}
	if !reflect.DeepEqual(storedResult, first.Result) {
		t.Fatalf("first result = %#v, want stored response %#v", first.Result, storedResult)
	}
	if observability.calls != 0 {
		t.Fatalf("operator event readback calls = %d, want 0", observability.calls)
	}
	firstResult := asMap(t, first.Result)
	firstEventID := stringValue(t, firstResult["event_id"], "event_id")
	firstRunID := stringValue(t, firstResult["run_id"], "run_id")
	firstDeliveries := asSlice(t, firstResult["deliveries"])
	if len(firstDeliveries) != 2 {
		t.Fatalf("first deliveries = %#v, want typed stored delivery results", firstDeliveries)
	}
	assertEventPublishDeliveriesContain(t, firstDeliveries, "agent", "scan-orchestrator", "pending", 1)
	assertEventPublishDeliveriesContain(t, firstDeliveries, "node", eventPublishScanNodeID(t), "pending", 1)
	requireAPIV1RuntimeBusEvent(t, ch, "event delivery after stored completion")

	replay := rpcCall(t, handler, body)
	if replay.Error != nil {
		t.Fatalf("event.publish replay error = %#v", replay.Error)
	}
	if !reflect.DeepEqual(replay.Result, first.Result) {
		t.Fatalf("replay result = %#v, want original stored result %#v", replay.Result, first.Result)
	}
	replayResult := asMap(t, replay.Result)
	eventID := stringValue(t, replayResult["event_id"], "event_id")
	runID := stringValue(t, replayResult["run_id"], "run_id")
	if eventID != firstEventID || runID != firstRunID {
		t.Fatalf("replay identity = %s/%s, want stored %s/%s", eventID, runID, firstEventID, firstRunID)
	}
	if count := countEventsByName(t, db, "scan.requested"); count != 1 {
		t.Fatalf("scan.requested event count after replay = %d, want 1", count)
	}
	if observability.calls != 0 {
		t.Fatalf("operator event readback calls after replay = %d, want 0", observability.calls)
	}
	deliveries := asSlice(t, replayResult["deliveries"])
	if len(deliveries) != 2 {
		t.Fatalf("replay deliveries = %#v, want typed persisted delivery results", deliveries)
	}
	assertEventPublishDeliveriesContain(t, deliveries, "agent", "scan-orchestrator", "pending", 1)
	assertEventPublishDeliveriesContain(t, deliveries, "node", eventPublishScanNodeID(t), "pending", 1)
}

func TestOperatorEventPublishPostCommitReceiptFailureReplaysWithoutDuplicate(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	failing := &failStandalonePipelineReceiptOnceStore{
		PostgresStore: pg,
		obligations: &failAPIPipelineSettlementOnceStore{
			Store: pg.PipelineObligations(),
			err:   errors.New("simulated post-commit receipt failure"),
		},
	}
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	probe := lifecycletest.New(t, lifecycletest.WithTimeout(5*time.Second))
	opts := runStartTestEventBusOptions(source)
	opts.TestLifecycleProbe = probe
	bus, err := newScopedAPITestEventBus(t, failing, opts)
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ctx, targetRunID := seedActiveAPIV1RuntimeBusAgentNewRun(t, context.Background(), pg, "scan-orchestrator")
	ch := runtimebustest.SubscribeForRun(t, bus, targetRunID, "scan-orchestrator", events.EventType("scan.requested"))
	defer runtimebustest.Unsubscribe(bus, "scan-orchestrator")
	handler := eventPublishTestHandlerWithStores(t, failing, failing, failing, bus, source)
	body := eventPublishBody(targetRunID, runStartTestBundleHash, "scan.requested", `{"topic":"medicine"}`, "", "idem-post-commit-receipt")

	published := rpcCall(t, handler, body)
	if published.Error != nil {
		t.Fatalf("event.publish post-commit receipt failure error = %#v", published.Error)
	}
	result := asMap(t, published.Result)
	eventID := stringValue(t, result["event_id"], "event_id")
	probe.RequirePostCommitDispatchCompleted(eventID)
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
	missing, ok, err := claimNextAPIPipelineWork(t, ctx, pg.PipelineObligations())
	if err != nil {
		t.Fatalf("claim recoverable pipeline obligation: %v", err)
	}
	if !ok || missing.Event.ID() != eventID {
		t.Fatalf("recoverable pipeline obligation = %#v, %t; want %s", missing.Event, ok, eventID)
	}
	if err := pg.PipelineObligations().Release(ctx, missing.Claim); err != nil {
		t.Fatalf("release recoverable pipeline obligation: %v", err)
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
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	failing := &failNormalRunCompletionStore{
		PostgresStore: pg,
		err:           errors.New("simulated normal-run completion failure"),
	}
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	probe := lifecycletest.New(t, lifecycletest.WithTimeout(5*time.Second))
	opts := runStartTestEventBusOptions(source)
	opts.TestLifecycleProbe = probe
	bus, err := newScopedAPITestEventBus(t, failing, opts)
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ctx, targetRunID := seedActiveAPIV1RuntimeBusAgentNewRun(t, context.Background(), pg, "scan-orchestrator")
	ch := runtimebustest.SubscribeForRun(t, bus, targetRunID, "scan-orchestrator", events.EventType("scan.requested"))
	defer runtimebustest.Unsubscribe(bus, "scan-orchestrator")
	handler := eventPublishTestHandlerWithStores(t, failing, failing, failing, bus, source)
	body := eventPublishBody(targetRunID, runStartTestBundleHash, "scan.requested", `{"topic":"medicine"}`, "", "idem-post-commit-completion")

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
	if outcome != "success" || failure != nil {
		t.Fatalf("pipeline receipt outcome=%q failure=%#v, want successful processing acknowledgement", outcome, failure)
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
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	failing := &failCommittedReplayScopeStore{
		PostgresStore: pg,
		err:           errors.New("simulated pre-commit replay scope failure"),
	}
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := newScopedAPITestEventBus(t, failing, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ctx := context.Background()
	handler := eventPublishTestHandlerWithStores(t, failing, failing, failing, bus, source)
	body := eventPublishBody("", runStartTestBundleHash, "scan.requested", `{"topic":"medicine"}`, "", "idem-pre-commit")

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

func TestOperatorEventPublishIsUnavailableWithoutDurableAckPublisher(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	publisher := &plainEventPublisher{}
	handler := eventPublishTestHandlerWithStores(t, pg, pg, pg, publisher, source)
	body := eventPublishBody("", runStartTestBundleHash, "scan.requested", `{"topic":"medicine"}`, "", "idem-no-ack-publisher")

	published := rpcCall(t, handler, body)
	if published.Error == nil {
		t.Fatal("event.publish without durable ack publisher error = nil")
	}
	data := asMap(t, published.Error.Data)
	if data["code"] != MethodUnavailableCode {
		t.Fatalf("event.publish without durable ack publisher data = %#v, want %s", data, MethodUnavailableCode)
	}
	details := asMap(t, data["details"])
	if details["method"] != "event.publish" {
		t.Fatalf("event.publish without durable ack publisher details = %#v", details)
	}
	if publisher.publishCalls != 0 {
		t.Fatalf("plain Publish calls = %d, want 0", publisher.publishCalls)
	}
	assertNoEventPublishPersistence(t, db)
	if got := countAllEventDeliveries(t, db); got != 0 {
		t.Fatalf("event_deliveries rows after missing durable ack publisher = %d, want 0", got)
	}
}

func TestOperatorEventPublishExplicitRunTargetRequiresExistingNonterminalRun(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := eventPublishTestHandler(t, pg, bus, source)

	initial := rpcCall(t, handler, eventPublishBody("", runStartTestBundleHash, "scan.requested", `{"topic":"first"}`, "", "idem-new-run"))
	if initial.Error != nil {
		t.Fatalf("initial event.publish error = %#v", initial.Error)
	}
	runID := stringValue(t, asMap(t, initial.Result)["run_id"], "run_id")
	seedActiveAPIV1RuntimeBusAgentForRun(t, testAuthorActivityContext(context.Background()), pg, runID, "scan-orchestrator", "")
	ch := runtimebustest.SubscribeForRun(t, bus, runID, "scan-orchestrator", events.EventType("scan.requested"))
	defer runtimebustest.Unsubscribe(bus, "scan-orchestrator")

	targeted := rpcCall(t, handler, eventPublishBody(runID, runStartTestBundleHash, "scan.requested", `{"topic":"second"}`, "operator-test", "idem-existing-run"))
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
	assertEventPublishDeliveriesContain(t, deliveries, "node", eventPublishScanNodeID(t), "pending", 1)
	if count := countEventsByName(t, db, "scan.requested"); count != 2 {
		t.Fatalf("scan.requested events after targeted publish = %d, want 2", count)
	}
	got := requireAPIV1RuntimeBusEventID(t, ch, targetedEventID, "targeted explicit-run delivery")
	if got.ID() != targetedEventID || got.RunID() != runID {
		t.Fatalf("targeted delivered event id/run = %s/%s, want %s/%s", got.ID(), got.RunID(), targetedEventID, runID)
	}

	mismatch := rpcCall(t, handler, eventPublishBody(runID, "bundle-v2:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "scan.requested", `{"topic":"mismatch"}`, "", "idem-existing-run-mismatch"))
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
	missing := rpcCall(t, handler, eventPublishBody(missingRunID, runStartTestBundleHash, "scan.requested", `{"topic":"missing"}`, "", "idem-missing-run"))
	if missing.Error == nil {
		t.Fatal("missing run event.publish error = nil")
	}
	if data := asMap(t, missing.Error.Data); data["code"] != RunNotFoundCode {
		t.Fatalf("missing run data = %#v, want %s", data, RunNotFoundCode)
	}

	if _, _, err := storetest.TerminalizeRun(testAuthorActivityContext(context.Background()), pg, storerunlifecycle.TerminalRequest{
		RunID:   runID,
		State:   storerunlifecycle.StateCancelled,
		EndedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("mark run terminal: %v", err)
	}
	terminal := rpcCall(t, handler, eventPublishBody(runID, runStartTestBundleHash, "scan.requested", `{"topic":"terminal"}`, "", "idem-terminal-run"))
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
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	source := semanticview.Wrap(eventPublishFollowUpTestBundle())
	bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ctx := context.Background()
	handler := eventPublishTestHandler(t, pg, bus, source)

	initial := rpcCall(t, handler, eventPublishBody("", runStartTestBundleHash, "scan.requested", `{"topic":"first"}`, "", "idem-followup-initial"))
	if initial.Error != nil {
		t.Fatalf("initial event.publish error = %#v", initial.Error)
	}
	initialResult := asMap(t, initial.Result)
	runID := stringValue(t, initialResult["run_id"], "run_id")
	if initialResult["new_run_created"] != true {
		t.Fatalf("initial result = %#v, want new run", initialResult)
	}
	seedActiveAPIV1RuntimeBusAgentForRun(t, testAuthorActivityContext(ctx), pg, runID, "scan-orchestrator", "")
	followUpCh := runtimebustest.SubscribeForRun(t, bus, runID, "scan-orchestrator", events.EventType("scan.followup"))
	defer runtimebustest.Unsubscribe(bus, "scan-orchestrator")

	followUp := rpcCall(t, handler, eventPublishBody(runID, runStartTestBundleHash, "scan.followup", `{"topic":"second"}`, "operator-test", "idem-followup-existing"))
	if followUp.Error != nil {
		t.Fatalf("follow-up event.publish error = %#v", followUp.Error)
	}
	followUpResult := asMap(t, followUp.Result)
	followUpEventID := stringValue(t, followUpResult["event_id"], "event_id")
	if followUpResult["run_id"] != runID || followUpResult["new_run_created"] != false {
		t.Fatalf("follow-up result = %#v, want selected existing run", followUpResult)
	}
	deliveries := asSlice(t, followUpResult["deliveries"])
	if len(deliveries) != 2 {
		t.Fatalf("follow-up deliveries = %#v, want agent and node", deliveries)
	}
	assertEventPublishDeliveriesContain(t, deliveries, "agent", "scan-orchestrator", "pending", 1)
	assertEventPublishDeliveriesContain(t, deliveries, "node", identitytest.RootNode(t, "scan-orchestrator").Key(), "pending", 1)
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
		t.Fatalf("agent event_deliveries for follow-up = %d, want 1", got)
	}
	got := requireAPIV1RuntimeBusEventID(t, followUpCh, followUpEventID, "follow-up delivery")
	if got.ID() != followUpEventID || got.RunID() != runID {
		t.Fatalf("follow-up delivered event id/run = %s/%s, want %s/%s", got.ID(), got.RunID(), followUpEventID, runID)
	}

	rejected := rpcCall(t, handler, eventPublishBody(runID, runStartTestBundleHash, "scan.unhandled", `{"topic":"lost"}`, "operator-test", "idem-followup-unhandled"))
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

func TestOperatorEventPublishPrivateTargetCannotAuthorizePublication(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	source := semanticview.Wrap(eventPublishTargetRouteTestBundle(t))
	bundleHash := runStartTestBundleHashForSource(source)
	bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := eventPublishTestHandler(t, pg, bus, source)

	initial := rpcCall(t, handler, eventPublishBody("", bundleHash, "bootstrap.requested", `{"topic":"first"}`, "", "idem-target-route-initial"))
	if initial.Error != nil {
		t.Fatalf("initial event.publish error = %#v", initial.Error)
	}
	runID := stringValue(t, asMap(t, initial.Result)["run_id"], "run_id")
	seedActiveAPIV1RuntimeBusAgentForRun(t, testAuthorActivityContextForSource(ctx, mustAPITestSourceArtifactFact(bundleHash)), pg, runID, "bootstrap-node", "")
	runtimebustest.SubscribeForRun(t, bus, runID, "bootstrap-node", events.EventType("bootstrap.requested"))
	defer runtimebustest.Unsubscribe(bus, "bootstrap-node")

	targetFlowInstance := "operating/inst-1"
	targetEntityID := runtimeflowidentity.EntityID(targetFlowInstance)
	seedEventPublishEntityState(t, db, source, runID, targetEntityID, targetFlowInstance, "pending")
	if _, err := db.ExecContext(ctx, `
		UPDATE entity_state
		SET fields = '{"entity_id":"authored-lookalike","flow_instance":"authored/lookalike"}'::jsonb,
		    bookkeeping = '{"entity_id":"bookkeeping-lookalike","flow_instance":"bookkeeping/lookalike"}'::jsonb
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, runID, targetEntityID); err != nil {
		t.Fatalf("seed hostile entity value-map identity lookalikes: %v", err)
	}
	if err := flowroutefixture.StageAndPublish(runtimecorrelation.WithRunID(ctx, runID), bus, runtimebus.FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.RunScopedFlowInstance{
		RunID: runID,
		Route: runtimeflowidentity.DeriveRoute("operating", "inst-1"),
	}}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}

	targeted := rpcCall(t, handler, eventPublishBodyWithTarget(runID, "", bundleHash, "operating/opco.product_initialization_requested", `{"topic":"targeted"}`, "operator-test", "idem-target-route-positive", targetFlowInstance, targetEntityID))
	if targeted.Error == nil {
		t.Fatal("complete target authorized a private input")
	}
	data := asMap(t, targeted.Error.Data)
	if data["code"] != EventNotDeclaredCode || asMap(t, data["details"])["reason"] != "selected_root_input_required" {
		t.Fatalf("private target rejection = %#v", targeted.Error)
	}
	if got := countEventRowsByRunID(t, db, runID); got != 1 {
		t.Fatalf("private target wrote events: %d", got)
	}
	if got := countAPIIdempotencyRows(t, db); got != 1 {
		t.Fatalf("private target wrote an idempotency receipt: %d", got)
	}
}

func TestOperatorEventPublishRootEventTemplateInputNameCollisionPayloadEntityIDDoesNotSelectTarget(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	source := semanticview.Wrap(eventPublishRootTemplateCollisionSource(t))
	bundleHash := runStartTestBundleHashForSource(source)
	bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := eventPublishTestHandler(t, pg, bus, source)

	initial := rpcCall(t, handler, eventPublishBody("", bundleHash, "review.requested", `{"topic":"first"}`, "", "idem-root-template-collision-initial"))
	if initial.Error != nil {
		t.Fatalf("initial event.publish error = %#v", initial.Error)
	}
	runID := stringValue(t, asMap(t, initial.Result)["run_id"], "run_id")
	seedActiveAPIV1RuntimeBusAgentForRun(t, testAuthorActivityContextForSource(ctx, mustAPITestSourceArtifactFact(bundleHash)), pg, runID, "root-orchestrator", "")
	ch := runtimebustest.SubscribeForRun(t, bus, runID, "root-orchestrator", events.EventType("review.requested"))
	defer runtimebustest.Unsubscribe(bus, "root-orchestrator")

	flowInstance := "operating/inst-1"
	entityID := runtimeflowidentity.EntityID(flowInstance)
	seedEventPublishEntityState(t, db, source, runID, entityID, flowInstance, "pending")

	followUp := rpcCall(t, handler, eventPublishBody(runID, bundleHash, "review.requested", fmt.Sprintf(`{"entity_id":%q,"topic":"root-follow-up"}`, entityID), "operator-test", "idem-root-template-collision-follow-up"))
	if followUp.Error != nil {
		t.Fatalf("root/template collision follow-up event.publish error = %#v", followUp.Error)
	}
	eventID := stringValue(t, asMap(t, followUp.Result)["event_id"], "event_id")
	requireAPIV1RuntimeBusEvent(t, ch, "follow-up root/template collision delivery")

	var gotEventName, gotEntityID, gotFlowInstance, gotTargetRoute, gotTargetSet string
	var gotPayload json.RawMessage
	if err := db.QueryRow(`
		SELECT event_name, COALESCE(entity_id::text, ''), COALESCE(flow_instance, ''), COALESCE(target_route::text, '{}'), COALESCE(target_set::text, '[]'), payload
		FROM events
		WHERE event_id = $1::uuid
	`, eventID).Scan(&gotEventName, &gotEntityID, &gotFlowInstance, &gotTargetRoute, &gotTargetSet, &gotPayload); err != nil {
		t.Fatalf("load root/template collision event row: %v", err)
	}
	if gotEventName != "review.requested" || gotEntityID != "" || gotFlowInstance != "" {
		t.Fatalf("event row = name:%q entity:%q flow:%q, want mixed root-receiver projection despite payload entity_id %s/%s", gotEventName, gotEntityID, gotFlowInstance, entityID, flowInstance)
	}
	assertStoredEventUntargeted(t, gotTargetRoute, gotTargetSet)
	var decoded map[string]any
	if err := json.Unmarshal(gotPayload, &decoded); err != nil {
		t.Fatalf("decode root/template collision payload: %v", err)
	}
	if decoded["entity_id"] != entityID || decoded["topic"] != "root-follow-up" {
		t.Fatalf("event payload = %#v, want payload entity_id preserved as business data only", decoded)
	}
	runtimebustest.Unsubscribe(bus, "workflow-runtime")
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := bus.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("wait for root/template collision publications: %v", err)
	}
}

func TestOperatorEventPublishPrivateTemplateDeniedBeforePublication(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	source := semanticview.Wrap(eventPublishTargetRouteTestBundle(t))
	bus, err := newScopedAPITestEventBus(t, sqliteStore, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatal(err)
	}
	publisher := &publicInputPublishProbe{EventBus: bus}
	rootHandler := eventPublishTestHandlerWithStores(t, sqliteStore, sqliteStore, sqliteStore, bus, source)
	initial := rpcCall(t, rootHandler, eventPublishBody("", runStartTestBundleHashForSource(source), "bootstrap.requested", `{}`, "operator-test", uuid.NewString()))
	if initial.Error != nil {
		t.Fatalf("create admitted root run: %+v", initial.Error)
	}
	existingRunID := stringValue(t, asMap(t, initial.Result)["run_id"], "run_id")
	handler := eventPublishTestHandlerWithStores(t, sqliteStore, sqliteStore, sqliteStore, publisher, source)
	for _, runID := range []string{"", existingRunID} {
		for _, name := range []string{"opco.product_initialization_requested", "operating/opco.product_initialization_requested"} {
			response := rpcCall(t, handler, eventPublishBody(runID, runStartTestBundleHashForSource(source), name,
				`{"topic":"private-template"}`, "operator-test", uuid.NewString()))
			if response.Error == nil || asMap(t, response.Error.Data)["code"] != EventNotDeclaredCode {
				t.Fatalf("private template publication %q run %q: error=%+v result=%#v", name, runID, response.Error, response.Result)
			}
		}
	}
	if publisher.publicInputCalls != 0 {
		t.Fatalf("private endpoint reached publisher %d times", publisher.publicInputCalls)
	}
}

func TestPublicEventInputEligibilityIgnoresChildAndCatalogOnlyOwners(t *testing.T) {
	const name = "review.requested"
	root := eventPublishRootTemplateCollisionTestBundle()
	if _, err := resolveEventPublicationEventName(semanticview.Wrap(root), name); err != nil {
		t.Fatal(err)
	}
	root.RootSchema.Pins.Inputs.EventPins = nil
	mustCompileEventPublishTestBundle(root)
	if _, err := resolveEventPublicationEventName(semanticview.Wrap(root), name); err == nil {
		t.Fatal("root catalog and private template acquired public authority without root input")
	}
	for _, source := range []semanticview.Source{
		semanticview.Wrap(flowScopedEventPublishTestBundle()),
		semanticview.Wrap(eventPublishTemplateInputTestBundle(name, false)),
	} {
		for _, event := range []string{name, "operating/" + name, "repo-scaffold/repo_scaffold.repo_commit_succeeded"} {
			if _, err := resolveEventPublicationEventName(source, event); err == nil {
				t.Fatalf("private %q admitted", event)
			}
		}
	}
}

func TestOperatorEventPublishMissingTemplateInputFailsClosedBeforeLowerPrecedencePublication(t *testing.T) {
	type fixture struct {
		store canonicalEventPublishProofStore
		db    *sql.DB
	}
	for _, backend := range []struct {
		name string
		open func(*testing.T, context.Context) fixture
	}{
		{
			name: "sqlite",
			open: func(t *testing.T, ctx context.Context) fixture {
				selected := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
				return fixture{store: selected, db: storetest.DatabaseForTest(selected)}
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T, _ context.Context) fixture {
				_, db, _ := testutil.StartPostgres(t)
				return fixture{store: storetest.AdmitPostgresRuntimeStore(t, db), db: db}
			},
		},
	} {
		t.Run(backend.name, func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			f := backend.open(t, ctx)
			for _, eventName := range []string{"review.requested", "operating/review.requested"} {
				eventName := eventName
				t.Run(strings.ReplaceAll(eventName, "/", "_"), func(t *testing.T) {
					bundle := eventPublishTemplateInputTestBundle("review.requested", false)
					bundle.FlowSchemas["operating"] = runtimecontracts.FlowSchemaDocument{Mode: "template"}
					bundle.FlowTree.Root.Children[0].Schema = bundle.FlowSchemas["operating"]
					consumer := runtimecontracts.SystemNodeContract{
						ExecutionType: "system_node",
						SubscribesTo:  []string{"review.requested"},
						EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"review.requested": {}},
					}
					bundle.FlowTree.Root.Children[0].Nodes = map[string]runtimecontracts.SystemNodeContract{"lower-precedence-consumer": consumer}
					mustCompileEventPublishTestBundle(bundle)
					source := semanticview.Wrap(bundle)
					bus, err := newScopedAPITestEventBus(t, f.store, runStartTestEventBusOptions(source))
					if err != nil {
						t.Fatalf("NewEventBusWithOptions: %v", err)
					}
					handler := eventPublishTestHandlerWithStores(t, f.store, f.store, f.store, bus, source)
					response := rpcCall(t, handler, eventPublishBody("", runStartTestBundleHash, eventName, `{"topic":"must reject"}`, "operator-test", "idem-missing-template-pin-"+strings.ReplaceAll(eventName, "/", "-")))
					if response.Error == nil {
						t.Fatal("event.publish error = nil")
					}
					data := asMap(t, response.Error.Data)
					if data["code"] != EventNotDeclaredCode {
						t.Fatalf("event.publish data = %#v, want %s", data, EventNotDeclaredCode)
					}
					details := asMap(t, data["details"])
					if details["reason"] != "selected_root_input_required" {
						t.Fatalf("event.publish details = %#v, want selected_root_input_required", details)
					}
					if got := countAllEventRows(t, f.db); got != 0 {
						t.Fatalf("event rows after missing template input = %d, want 0", got)
					}
					if got := countAPIIdempotencyRows(t, f.db); got != 0 {
						t.Fatalf("API idempotency rows after missing template input = %d, want 0", got)
					}
				})
			}
		})
	}
}

func TestOperatorEventPublishExistingRunTargetRouteRejectsInvalidTargetBeforePersistence(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	source := semanticview.Wrap(eventPublishTargetRouteTestBundle(t))
	bundleHash := runStartTestBundleHashForSource(source)
	bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := eventPublishTestHandler(t, pg, bus, source)

	initial := rpcCall(t, handler, eventPublishBody("", bundleHash, "bootstrap.requested", `{"topic":"first"}`, "", "idem-target-route-invalid-initial"))
	if initial.Error != nil {
		t.Fatalf("initial event.publish error = %#v", initial.Error)
	}
	runID := stringValue(t, asMap(t, initial.Result)["run_id"], "run_id")
	seedActiveAPIV1RuntimeBusAgentForRun(t, testAuthorActivityContextForSource(ctx, mustAPITestSourceArtifactFact(bundleHash)), pg, runID, "bootstrap-node", "")
	runtimebustest.SubscribeForRun(t, bus, runID, "bootstrap-node", events.EventType("bootstrap.requested"))
	defer runtimebustest.Unsubscribe(bus, "bootstrap-node")

	targetFlowInstance := "operating/inst-1"
	targetEntityID := runtimeflowidentity.EntityID(targetFlowInstance)
	seedEventPublishEntityState(t, db, source, runID, targetEntityID, targetFlowInstance, "pending")
	if err := flowroutefixture.StageAndPublish(runtimecorrelation.WithRunID(ctx, runID), bus, runtimebus.FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.RunScopedFlowInstance{
		RunID: runID,
		Route: runtimeflowidentity.DeriveRoute("operating", "inst-1"),
	}}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	mismatchEntityID := uuid.NewString()
	seedEventPublishEntityState(t, db, source, runID, mismatchEntityID, "operating/other", "pending")
	unroutableEntityID := uuid.NewString()
	seedEventPublishEntityState(t, db, source, runID, unroutableEntityID, "orphan/inst-1", "pending")

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
			name:     "entity id is not a flow instance address",
			body:     fmt.Sprintf(`{"jsonrpc":"2.0","id":"publish","method":"event.publish","params":{"bundle_hash":%q,"event_name":"operating/opco.product_initialization_requested","payload":{},"run_id":%q,"target":{"flow_instance":%q,"entity_id":%q},"idempotency_key":"idem-target-entity-address"}}`, runStartTestBundleHash, runID, targetEntityID, targetEntityID),
			wantCode: "INVALID_PARAMS",
		},
		{
			name:       "nonexistent entity",
			body:       eventPublishBodyWithTarget(runID, "", bundleHash, "operating/opco.product_initialization_requested", `{"topic":"missing-entity"}`, "operator-test", "idem-target-missing-entity", targetFlowInstance, uuid.NewString()),
			wantCode:   EventNotDeclaredCode,
			wantReason: "selected_root_input_required",
		},
		{
			name:       "mismatched entity flow",
			body:       eventPublishBodyWithTarget(runID, "", bundleHash, "operating/opco.product_initialization_requested", `{"topic":"mismatch"}`, "operator-test", "idem-target-mismatch", targetFlowInstance, mismatchEntityID),
			wantCode:   EventNotDeclaredCode,
			wantReason: "selected_root_input_required",
		},
		{
			name:       "event not routable for target flow",
			body:       eventPublishBodyWithTarget(runID, "", bundleHash, "operating/opco.product_initialization_requested", `{"topic":"unroutable"}`, "operator-test", "idem-target-unroutable", "orphan/inst-1", unroutableEntityID),
			wantCode:   EventNotDeclaredCode,
			wantReason: "selected_root_input_required",
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

func TestOperatorEventPublishExplicitRunIsUnavailableWithoutRecipientPlanChecker(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	source := semanticview.Wrap(eventPublishFollowUpTestBundle())
	bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := eventPublishTestHandler(t, pg, bus, source)

	initial := rpcCall(t, handler, eventPublishBody("", runStartTestBundleHash, "scan.requested", `{"topic":"first"}`, "", "idem-followup-missing-plan-initial"))
	if initial.Error != nil {
		t.Fatalf("initial event.publish error = %#v", initial.Error)
	}
	runID := stringValue(t, asMap(t, initial.Result)["run_id"], "run_id")
	seedActiveAPIV1RuntimeBusAgentForRun(t, testAuthorActivityContext(ctx), pg, runID, "scan-orchestrator", "")

	noCheckerHandler := eventPublishTestHandlerWithStores(t, pg, pg, pg, failingRunStartPublisher{}, source)
	rejected := rpcCall(t, noCheckerHandler, eventPublishBody(runID, runStartTestBundleHash, "scan.followup", `{"topic":"second"}`, "operator-test", "idem-followup-missing-plan"))
	if rejected.Error == nil {
		t.Fatal("missing recipient-plan checker event.publish error = nil")
	}
	data := asMap(t, rejected.Error.Data)
	if data["code"] != MethodUnavailableCode {
		t.Fatalf("missing recipient-plan checker data = %#v, want %s", data, MethodUnavailableCode)
	}
	details := asMap(t, data["details"])
	if details["method"] != "event.publish" {
		t.Fatalf("missing recipient-plan checker details = %#v", details)
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
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	source := semanticview.Wrap(eventPublishFollowUpTestBundle())
	bus, err := newScopedAPITestEventBus(t, sqliteStore, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := eventPublishTestHandlerWithStores(t, sqliteStore, sqliteStore, sqliteStore, bus, source)

	initial := rpcCall(t, handler, eventPublishBody("", runStartTestBundleHash, "scan.requested", `{"topic":"first"}`, "", "idem-sqlite-followup-initial"))
	if initial.Error != nil {
		t.Fatalf("sqlite initial event.publish error = %#v", initial.Error)
	}
	runID := stringValue(t, asMap(t, initial.Result)["run_id"], "run_id")
	seedActiveAPIV1RuntimeBusAgentForRun(t, testAuthorActivityContext(ctx), sqliteStore, runID, "scan-orchestrator", "")
	followUpCh := runtimebustest.SubscribeForRun(t, bus, runID, "scan-orchestrator", events.EventType("scan.followup"))
	defer runtimebustest.Unsubscribe(bus, "scan-orchestrator")

	followUp := rpcCall(t, handler, eventPublishBody(runID, runStartTestBundleHash, "scan.followup", `{"topic":"second"}`, "operator-test", "idem-sqlite-followup-existing"))
	if followUp.Error != nil {
		t.Fatalf("sqlite follow-up event.publish error = %#v", followUp.Error)
	}
	result := asMap(t, followUp.Result)
	eventID := stringValue(t, result["event_id"], "event_id")
	if result["run_id"] != runID || result["new_run_created"] != false {
		t.Fatalf("sqlite follow-up result = %#v, want selected existing run", result)
	}
	if got := countSQLiteAllRunRows(t, storetest.DatabaseForTest(sqliteStore)); got != 1 {
		t.Fatalf("sqlite run rows after follow-up = %d, want 1", got)
	}
	if got := countSQLiteEventRowsByRunID(t, storetest.DatabaseForTest(sqliteStore), runID); got != 2 {
		t.Fatalf("sqlite events for selected run = %d, want 2", got)
	}
	if got := countSQLiteEventsByName(t, storetest.DatabaseForTest(sqliteStore), "scan.followup"); got != 1 {
		t.Fatalf("sqlite scan.followup rows = %d, want 1", got)
	}
	deliveries := asSlice(t, result["deliveries"])
	if len(deliveries) != 2 {
		t.Fatalf("sqlite follow-up deliveries = %#v, want agent and node", deliveries)
	}
	assertEventPublishDeliveriesContain(t, deliveries, "agent", "scan-orchestrator", "pending", 1)
	assertEventPublishDeliveriesContain(t, deliveries, "node", identitytest.RootNode(t, "scan-orchestrator").Key(), "pending", 1)
	got := requireAPIV1RuntimeBusEventID(t, followUpCh, eventID, "sqlite follow-up delivery")
	if got.ID() != eventID || got.RunID() != runID {
		t.Fatalf("sqlite follow-up delivered id/run = %s/%s, want %s/%s", got.ID(), got.RunID(), eventID, runID)
	}
}

func TestOperatorEventPublishRejectsCallerEntityIDForCreateEntityBeforePersistence(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	source := semanticview.Wrap(eventPublishCreateEntityTestBundle())
	bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := eventPublishTestHandler(t, pg, bus, source)

	resp := rpcCall(t, handler, eventPublishBody("", runStartTestBundleHash, "thing.created", `{"entity_id":"11111111-1111-4111-8111-111111111111","amount":50}`, "", "idem-create-entity-supplied-id"))
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
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	source := semanticview.Wrap(eventPublishCreateEntityTestBundle())
	bus, err := newScopedAPITestEventBus(t, sqliteStore, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := eventPublishTestHandlerWithStores(t, sqliteStore, sqliteStore, sqliteStore, bus, source)

	resp := rpcCall(t, handler, eventPublishBody("", runStartTestBundleHash, "thing.created", `{"entity_id":"11111111-1111-4111-8111-111111111111","amount":50}`, "", "idem-sqlite-create-entity-supplied-id"))
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
	if got := countSQLiteAllRunRows(t, storetest.DatabaseForTest(sqliteStore)); got != 0 {
		t.Fatalf("sqlite run rows after create-entity rejection = %d, want 0", got)
	}
	if got := countSQLiteAllEventRows(t, storetest.DatabaseForTest(sqliteStore)); got != 0 {
		t.Fatalf("sqlite event rows after create-entity rejection = %d, want 0", got)
	}
	if got := countSQLiteAPIIdempotencyRows(t, storetest.DatabaseForTest(sqliteStore)); got != 0 {
		t.Fatalf("sqlite api_idempotency rows after create-entity rejection = %d, want 0", got)
	}
}

func TestOperatorEventPublishRenamedConnectedCreateEntityRejectsCallerIdentityBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) (canonicalEventPublishProofStore, *sql.DB)
	}{
		{"sqlite", func(t *testing.T) (canonicalEventPublishProofStore, *sql.DB) {
			selected := storetest.StartSQLiteRuntimeStoreWithContext(t, context.Background())
			return selected, storetest.DatabaseForTest(selected)
		}},
		{"postgres", func(t *testing.T) (canonicalEventPublishProofStore, *sql.DB) {
			_, db, _ := testutil.StartPostgres(t)
			return storetest.AdmitPostgresRuntimeStore(t, db), db
		}},
	} {
		t.Run(backend.name, func(t *testing.T) {
			selected, db := backend.open(t)
			source := semanticview.Wrap(eventPublishRenamedConnectedCreateEntityTestBundle())
			bus, err := newScopedAPITestEventBus(t, selected, runStartTestEventBusOptions(source))
			if err != nil {
				t.Fatal(err)
			}
			handler := eventPublishTestHandlerWithStores(t, selected, selected, selected, bus, source)
			payload := `{"entity_id":"11111111-1111-4111-8111-111111111111","amount":50}`
			for _, method := range []struct {
				name string
				body string
			}{
				{"event.publish", eventPublishBody("", runStartTestBundleHash, "thing.requested", payload, "", "renamed-create-publish")},
				{"run.start", runStartBody("", runStartTestBundleHash, "thing.requested", payload, "renamed-create-start")},
			} {
				t.Run(method.name, func(t *testing.T) {
					response := rpcCall(t, handler, method.body)
					if response.Error == nil {
						t.Fatal("renamed create-entity receiver accepted caller-supplied entity_id")
					}
					data := asMap(t, response.Error.Data)
					if data["code"] != PayloadValidationFailedCode {
						t.Fatalf("renamed create-entity rejection = %#v", response.Error)
					}
					violations := asSlice(t, asMap(t, data["details"])["violations"])
					if len(violations) != 1 || asMap(t, violations[0])["rule"] != "create_entity_mints_entity_id" {
						t.Fatalf("renamed create-entity violations = %#v", violations)
					}
					for _, table := range []string{"runs", "events", "event_deliveries", "api_idempotency"} {
						var count int
						if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 0 {
							t.Fatalf("%s rows after rejection = %d, err=%v", table, count, err)
						}
					}
				})
			}
			created := rpcCall(t, handler, eventPublishBody("", runStartTestBundleHash, "thing.requested", `{"amount":50}`, "", "renamed-create-valid"))
			if created.Error != nil {
				t.Fatalf("connected create without caller identity was rejected: %#v", created.Error)
			}
			createdResult := asMap(t, created.Result)
			runID := stringValue(t, createdResult["run_id"], "run_id")
			createdDeliveries := asSlice(t, createdResult["deliveries"])
			if len(createdDeliveries) != 2 {
				t.Fatalf("valid connected creation did not persist both root and creator deliveries: %#v", createdResult)
			}
			assertEventPublishDeliveriesContain(t, createdDeliveries, "node", identitytest.RootNode(t, "observer").Key(), "pending", 1)
			assertEventPublishDeliveriesContain(t, createdDeliveries, "node", identitytest.FlowNode(t, "factory", "thing-writer").Key(), "pending", 1)
			for _, table := range []string{"runs", "events", "api_idempotency"} {
				var count int
				if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 1 {
					t.Fatalf("%s rows after valid creation = %d, err=%v", table, count, err)
				}
			}
			targeted := rpcCall(t, handler, eventPublishBodyWithTarget(runID, "", runStartTestBundleHash, "thing.requested", `{"amount":51}`, "", "renamed-create-target", "factory", "11111111-1111-4111-8111-111111111111"))
			if targeted.Error == nil {
				t.Fatal("selected create-entity receiver accepted a caller target")
			}
			targetData := asMap(t, targeted.Error.Data)
			if targetData["code"] != PayloadValidationFailedCode {
				t.Fatalf("selected creator target rejection = %#v", targeted.Error)
			}
			targetViolations := asSlice(t, asMap(t, targetData["details"])["violations"])
			if len(targetViolations) != 1 || asMap(t, targetViolations[0])["field_path"] != "$.target.entity_id" || asMap(t, targetViolations[0])["rule"] != "create_entity_mints_entity_id" {
				t.Fatalf("selected creator target violations = %#v", targetViolations)
			}
			for _, table := range []string{"runs", "events", "event_deliveries", "api_idempotency"} {
				var count int
				want := 1
				if table == "event_deliveries" {
					want = 2
				}
				if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != want {
					t.Fatalf("%s rows after forbidden target = %d, want %d, err=%v", table, count, want, err)
				}
			}
			plain := rpcCall(t, handler, eventPublishBody("", runStartTestBundleHash, "note.requested", `{"entity_id":"11111111-1111-4111-8111-111111111111","amount":52}`, "", "noncreating-identity-valid"))
			if plain.Error != nil {
				t.Fatalf("non-creation event rejected valid caller identity: %#v", plain.Error)
			}
			if stringValue(t, asMap(t, plain.Result)["run_id"], "run_id") == runID {
				t.Fatal("separate non-creation publication reused the creation run")
			}
			plainDeliveries := asSlice(t, asMap(t, plain.Result)["deliveries"])
			if len(plainDeliveries) != 1 {
				t.Fatalf("unconnected same-name creator received non-creation publication: %#v", plainDeliveries)
			}
			assertEventPublishDeliveriesContain(t, plainDeliveries, "node", identitytest.RootNode(t, "observer").Key(), "pending", 1)
			for _, table := range []string{"runs", "events", "api_idempotency"} {
				var count int
				if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 2 {
					t.Fatalf("%s rows after valid non-creation = %d, err=%v", table, count, err)
				}
			}
		})
	}
}

func TestEventPublicationExplicitRootTargetExcludesConnectedCreator(t *testing.T) {
	source := semanticview.Wrap(eventPublishRenamedConnectedCreateEntityTestBundle())
	runID := uuid.NewString()
	if !eventPublicationHasCreateEntityHandler(source, "thing.requested", events.RouteIdentity{}, runID) {
		t.Fatal("untargeted root publication did not select the connected creator")
	}
	if eventPublicationHasCreateEntityHandler(source, "thing.requested", events.RouteIdentity{FlowID: ".", FlowInstance: runID}, runID) {
		t.Fatal("explicit root target selected a connected child creator")
	}
}

func TestOperatorEventPublishOperatorReferenceValidatesSameRunProvenance(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ctx := context.Background()
	handler := eventPublishTestHandler(t, pg, bus, source)

	parent := rpcCall(t, handler, eventPublishBody("", runStartTestBundleHash, "scan.requested", `{"topic":"medicine"}`, "", "idem-source-parent"))
	if parent.Error != nil {
		t.Fatalf("parent event.publish error = %#v", parent.Error)
	}
	parentResult := asMap(t, parent.Result)
	parentEventID := stringValue(t, parentResult["event_id"], "event_id")
	parentRunID := stringValue(t, parentResult["run_id"], "run_id")
	seedActiveAPIV1RuntimeBusAgentForRun(t, testAuthorActivityContext(ctx), pg, parentRunID, "scan-orchestrator", "")
	ch := runtimebustest.SubscribeForRun(t, bus, parentRunID, "scan-orchestrator", events.EventType("scan.requested"))
	defer runtimebustest.Unsubscribe(bus, "scan-orchestrator")

	child := rpcCall(t, handler, eventPublishBodyWithSource(parentRunID, parentEventID, runStartTestBundleHash, "scan.requested", `{"topic":"checkpoint"}`, "operator-test", "idem-source-child"))
	if child.Error != nil {
		t.Fatalf("child event.publish error = %#v", child.Error)
	}
	childResult := asMap(t, child.Result)
	childEventID := stringValue(t, childResult["event_id"], "event_id")
	if childResult["run_id"] != parentRunID || childResult["new_run_created"] != false {
		t.Fatalf("child result = %#v, want existing run", childResult)
	}
	if _, present := childResult["source_event_id"]; present {
		t.Fatalf("operator-injected event exposed causal source_event_id: %#v", childResult)
	}
	if childResult["operator_reference_event_id"] != parentEventID {
		t.Fatalf("child operator_reference_event_id = %#v, want %s", childResult["operator_reference_event_id"], parentEventID)
	}
	assertOperatorEventReference(t, db, childEventID, parentEventID)
	if count := countEventsByName(t, db, "scan.requested"); count != 2 {
		t.Fatalf("scan.requested events after sourced publish = %d, want 2", count)
	}
	if count := countAPIIdempotencyRows(t, db); count != 2 {
		t.Fatalf("api_idempotency rows after sourced publish = %d, want 2", count)
	}
	got := requireAPIV1RuntimeBusEventID(t, ch, childEventID, "operator-injected event delivery")
	if got.ID() != childEventID || got.RunID() != parentRunID {
		t.Fatalf("child delivered event id/run = %s/%s, want %s/%s", got.ID(), got.RunID(), childEventID, parentRunID)
	}
}

func TestOperatorEventPublishOperatorReferenceRejectsInvalidReferenceBeforePersistence(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := eventPublishTestHandler(t, pg, bus, source)

	first := rpcCall(t, handler, eventPublishBody("", runStartTestBundleHash, "scan.requested", `{"topic":"first"}`, "", "idem-source-first"))
	if first.Error != nil {
		t.Fatalf("first event.publish error = %#v", first.Error)
	}
	firstRunID := stringValue(t, asMap(t, first.Result)["run_id"], "run_id")
	firstEventID := stringValue(t, asMap(t, first.Result)["event_id"], "event_id")

	second := rpcCall(t, handler, eventPublishBody("", runStartTestBundleHash, "scan.requested", `{"topic":"second"}`, "", "idem-source-second"))
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
			body:          eventPublishBodyWithSource("", firstEventID, runStartTestBundleHash, "scan.requested", `{"topic":"no-run"}`, "", "idem-source-no-run"),
			wantJSONCode:  codeInvalidParams,
			wantField:     "run_id",
			wantEventRows: 2,
			wantIDEMRows:  2,
		},
		{
			name:          "invalid source uuid",
			body:          eventPublishBodyWithSource(firstRunID, "not-a-uuid", runStartTestBundleHash, "scan.requested", `{"topic":"bad-source"}`, "", "idem-source-invalid"),
			wantJSONCode:  codeInvalidParams,
			wantField:     "source_event_id",
			wantEventRows: 2,
			wantIDEMRows:  2,
		},
		{
			name:          "missing source event",
			body:          eventPublishBodyWithSource(firstRunID, uuid.NewString(), runStartTestBundleHash, "scan.requested", `{"topic":"missing-source"}`, "", "idem-source-missing"),
			wantAppCode:   EventNotFoundCode,
			wantEventRows: 2,
			wantIDEMRows:  2,
		},
		{
			name:          "missing target run with source event",
			body:          eventPublishBodyWithSource(uuid.NewString(), firstEventID, runStartTestBundleHash, "scan.requested", `{"topic":"missing-run-source"}`, "", "idem-source-missing-run"),
			wantAppCode:   RunNotFoundCode,
			wantEventRows: 2,
			wantIDEMRows:  2,
		},
		{
			name:          "cross run source event",
			body:          eventPublishBodyWithSource(firstRunID, secondEventID, runStartTestBundleHash, "scan.requested", `{"topic":"cross-run"}`, "", "idem-source-cross-run"),
			wantJSONCode:  codeInvalidParams,
			wantField:     "source_event_id",
			wantEventRows: 2,
			wantIDEMRows:  2,
		},
		{
			name: "terminal run with source",
			body: eventPublishBodyWithSource(firstRunID, firstEventID, runStartTestBundleHash, "scan.requested", `{"topic":"terminal"}`, "", "idem-source-terminal"),
			mutateBefore: func() {
				if _, _, err := storetest.TerminalizeRun(testAuthorActivityContext(context.Background()), pg, storerunlifecycle.TerminalRequest{
					RunID:   firstRunID,
					State:   storerunlifecycle.StateCancelled,
					EndedAt: time.Now().UTC(),
				}); err != nil {
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
		_, db, _ := testutil.StartPostgres(t)
		pg := storetest.AdmitPostgresRuntimeStore(t, db)
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := eventPublishTestHandler(t, pg, bus, source)

		resp := rpcCall(t, handler, eventPublishBody("", "bundle-v2:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "scan.requested", `{"topic":"medicine"}`, "", "idem-event-mismatch"))
		if resp.Error == nil {
			t.Fatal("event.publish non-routable bundle error = nil")
		}
		if data := asMap(t, resp.Error.Data); data["code"] != BundleUnavailableCode {
			t.Fatalf("bundle unavailable data = %#v", data)
		}
		assertNoEventPublishPersistence(t, db)
	})

	t.Run("invalid canonical bundle hash", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := storetest.AdmitPostgresRuntimeStore(t, db)
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
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

	t.Run("retired bundle input is rejected", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := storetest.AdmitPostgresRuntimeStore(t, db)
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := eventPublishTestHandler(t, pg, bus, source)

		resp := rpcCall(t, handler, eventPublishBodyWithCanonicalAndRetiredInput("", runStartTestBundleHash, "scan.requested", `{"topic":"medicine"}`, "", "idem-event-retired-input"))
		assertInvalidRunStartParam(t, resp, retiredBundleInputName())
		assertNoEventPublishPersistence(t, db)
	})

	t.Run("undeclared event", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := storetest.AdmitPostgresRuntimeStore(t, db)
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := eventPublishTestHandler(t, pg, bus, source)

		resp := rpcCall(t, handler, eventPublishBody("", runStartTestBundleHash, "scan.missing", `{"topic":"medicine"}`, "", "idem-event-missing"))
		if resp.Error == nil {
			t.Fatal("event.publish undeclared event error = nil")
		}
		if data := asMap(t, resp.Error.Data); data["code"] != EventNotDeclaredCode {
			t.Fatalf("undeclared event data = %#v", data)
		}
		assertNoEventPublishPersistence(t, db)
	})

	t.Run("payload validation", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := storetest.AdmitPostgresRuntimeStore(t, db)
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := newScopedAPITestEventBus(t, pg, runtimebus.EventBusOptions{
			ContractBundle:     source,
			SourceArtifactFact: runStartTestSourceArtifactFact(),
			PayloadAdmitter: func(_ context.Context, event events.Event, _ string) (events.PayloadAdmission, error) {
				if event.Type() != "scan.requested" {
					return events.PayloadAdmission{}, fmt.Errorf("unexpected event type %q", event.Type())
				}
				return events.PayloadAdmission{}, errors.New("schema violation")
			},
		})
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := eventPublishTestHandler(t, pg, bus, source)

		resp := rpcCall(t, handler, eventPublishBody("", runStartTestBundleHash, "scan.requested", `{"topic":"medicine"}`, "", "idem-event-invalid-payload"))
		if resp.Error == nil {
			t.Fatal("event.publish payload validation error = nil")
		}
		if data := asMap(t, resp.Error.Data); data["code"] != PayloadValidationFailedCode {
			t.Fatalf("payload validation data = %#v", data)
		}
		assertNoEventPublishPersistence(t, db)
	})

	t.Run("invalid run id", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		pg := storetest.AdmitPostgresRuntimeStore(t, db)
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		handler := eventPublishTestHandler(t, pg, bus, source)

		resp := rpcCall(t, handler, eventPublishBody("abc", runStartTestBundleHash, "scan.requested", `{"topic":"medicine"}`, "", "idem-event-invalid-run-id"))
		if resp.Error == nil || resp.Error.Code != codeInvalidParams {
			t.Fatalf("event.publish invalid run_id error = %#v, want invalid params", resp.Error)
		}
		assertNoEventPublishPersistence(t, db)
	})
}

func TestOperatorEventPublishQueuesWhileRuntimePaused(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := newScopedAPITestEventBus(t, pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	controller := runtimeingress.NewController(pg, bus, runtimeingress.Options{ExecutionPosture: executionposture.Live})
	t.Cleanup(runtimebus.ResumeRuntimeIngress)
	bus.SetRuntimeIngressDispatchGate(controller)

	agentID := "scan-orchestrator"
	ctx, targetRunID := seedActiveAPIV1RuntimeBusAgentNewRun(t, context.Background(), pg, agentID)
	ch := runtimebustest.SubscribeForRun(t, bus, targetRunID, agentID, events.EventType("scan.requested"))
	defer runtimebustest.Unsubscribe(bus, agentID)

	if _, err := controller.Pause(ctx, runtimeingress.TransitionRequest{
		Reason:       "test_pause",
		ControlledBy: "test",
		Now:          time.Now().UTC(),
	}); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	handler := eventPublishTestHandler(t, pg, bus, source)
	published := rpcCall(t, handler, eventPublishBody(targetRunID, runStartTestBundleHash, "scan.requested", `{"topic":"paused"}`, "", "idem-paused-publish"))
	if published.Error != nil {
		t.Fatalf("paused event.publish error = %#v", published.Error)
	}
	eventID := stringValue(t, asMap(t, published.Result)["event_id"], "event_id")
	deliveries := asSlice(t, asMap(t, published.Result)["deliveries"])
	if len(deliveries) != 2 {
		t.Fatalf("paused event.publish deliveries = %#v, want typed agent and node deliveries", deliveries)
	}
	assertEventPublishDeliveriesContain(t, deliveries, "agent", "scan-orchestrator", "pending", 1)
	assertEventPublishDeliveriesContain(t, deliveries, "node", eventPublishScanNodeID(t), "pending", 1)
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
		Handlers: testOperatorHandlers(testOperatorCapabilities{
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
				BundleHash:      runStartTestBundleHashForSource(source),
			},
		}),
	})
}

type blockingAPIV1PublishInterceptor struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (i blockingAPIV1PublishInterceptor) Intercept(_ context.Context, _ events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	select {
	case i.started <- struct{}{}:
	default:
	}
	<-i.release
	return true, nil, runtimepipelineobligation.Continue(), nil
}

type plainEventPublisher struct {
	publishCalls int
}

func (p *plainEventPublisher) Publish(context.Context, events.Event) error {
	p.publishCalls++
	return nil
}

func (p *plainEventPublisher) AdmitSourceArtifactFact(ctx context.Context) (context.Context, error) {
	return runtimecorrelation.WithSourceArtifactFact(ctx, runStartTestSourceArtifactFact()), nil
}

type publicInputPublishProbe struct {
	*runtimebus.EventBus
	publicInputCalls int
	endpoint         runtimebus.APIEventPublicationEndpointReadback
}

func (p *publicInputPublishProbe) LookupAPIEventPublication(ctx context.Context, request apiidempotency.Request) (apiidempotency.Completion, bool, error) {
	return p.EventBus.LookupAPIEventPublication(ctx, request)
}

func (p *publicInputPublishProbe) PublishAPIEventAcknowledged(ctx context.Context, evt events.Event, endpoint *runtimebus.APIEventPublicationEndpoint, request apiidempotency.Request, completion apiidempotency.Completion) (apiidempotency.Completion, bool, error) {
	p.publicInputCalls++
	if endpoint != nil {
		p.endpoint = endpoint.Readback()
	}
	return p.EventBus.PublishAPIEventAcknowledged(ctx, evt, endpoint, request, completion)
}

func (p *publicInputPublishProbe) PublishAPIEventWithRunCreationAcknowledged(ctx context.Context, evt events.Event, endpoint *runtimebus.APIEventPublicationEndpoint, request apiidempotency.Request, completion apiidempotency.Completion, runCreation *durabledata.RunCreationCommand) (apiidempotency.Completion, bool, error) {
	p.publicInputCalls++
	if endpoint != nil {
		p.endpoint = endpoint.Readback()
	}
	return completion, false, nil
}

type failStandalonePipelineReceiptOnceStore struct {
	*store.PostgresStore
	obligations runtimepipelineobligation.Store
}

func (s *failStandalonePipelineReceiptOnceStore) PipelineObligations() runtimepipelineobligation.Store {
	return s.obligations
}

type failAPIPipelineSettlementOnceStore struct {
	runtimepipelineobligation.Store
	err error
}

func (s *failAPIPipelineSettlementOnceStore) Settle(ctx context.Context, claim runtimepipelineobligation.Claim, disposition runtimepipelineobligation.Disposition) (runtimepipelineobligation.SettlementOutcome, error) {
	if s.err != nil {
		err := s.err
		s.err = nil
		return runtimepipelineobligation.SettlementOutcome{}, err
	}
	return s.Store.Settle(ctx, claim, disposition)
}

type failCommittedReplayScopeStore struct {
	*store.PostgresStore
	err error
}

func (s *failCommittedReplayScopeStore) CommitPublication(ctx context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	if s.err != nil {
		return runtimebus.CommittedPublication{}, s.err
	}
	return s.PostgresStore.CommitPublication(ctx, command)
}

func (s *failCommittedReplayScopeStore) CommitAPIEventPublication(ctx context.Context, command runtimebus.APIEventPublicationCommand) (runtimebus.CommittedAPIEventPublication, error) {
	if s.err != nil {
		return runtimebus.CommittedAPIEventPublication{}, s.err
	}
	return s.PostgresStore.CommitAPIEventPublication(ctx, command)
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
	err   error
	calls int
}

func (s *failOnceEventReadStore) LoadOperatorEvent(ctx context.Context, eventID string) (operatorread.OperatorEventFull, error) {
	s.calls++
	if s.err != nil {
		err := s.err
		s.err = nil
		return operatorread.OperatorEventFull{}, err
	}
	return s.ObservabilityReadStore.LoadOperatorEvent(ctx, eventID)
}

func eventPublishBody(runID, bundleHash, eventName, payload, emitter, idempotencyKey string) string {
	return eventPublishBodyWithSource(runID, "", bundleHash, eventName, payload, emitter, idempotencyKey)
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

func eventPublishBodyWithCanonicalAndRetiredInput(runID, bundleHash, eventName, payload, emitter, idempotencyKey string) string {
	parts := []string{
		fmt.Sprintf(`"bundle_hash":%q`, bundleHash),
		fmt.Sprintf(`%q:{%q:%q}`, retiredBundleInputName(), retiredBundleInputFieldName(), bundleHash),
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

func eventPublishBodyWithSource(runID, sourceEventID, bundleHash, eventName, payload, emitter, idempotencyKey string) string {
	parts := []string{
		fmt.Sprintf(`"bundle_hash":%q`, bundleHash),
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

func eventPublishBodyWithTarget(runID, sourceEventID, bundleHash, eventName, payload, emitter, idempotencyKey, flowInstance, entityID string) string {
	parts := []string{
		fmt.Sprintf(`"bundle_hash":%q`, bundleHash),
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

func eventPublishBodyWithRetiredBundleInput(runID, bundleHash, eventName, payload, emitter, idempotencyKey string) string {
	parts := []string{
		fmt.Sprintf(`%q:{%q:%q}`, retiredBundleInputName(), retiredBundleInputFieldName(), bundleHash),
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
	return mustCompileEventPublishTestBundle(flowScopedEventPublishBundle(map[string]string{
		"repo-scaffold": "repo_scaffold.repo_commit_succeeded",
	}))
}

func ambiguousFlowScopedEventPublishTestBundle() *runtimecontracts.WorkflowContractBundle {
	return mustCompileEventPublishTestBundle(flowScopedEventPublishBundle(map[string]string{
		"alpha-flow": "workflow.completed",
		"beta-flow":  "workflow.completed",
	}))
}

func rootAndAmbiguousFlowScopedEventPublishTestBundle(t testing.TB) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := canonicalrouting.CopyExample(t, canonicalrouting.RootIngress)
	files := map[string]string{
		"events.yaml": `item.received:
  item_id: text
  topic: text
item.processed:
  item_id: text?
`,
		"alpha-flow/schema.yaml": `name: alpha-flow
mode: static
`,
		"alpha-flow/events.yaml": `item.received:
  item_id: text
`,
		"beta-flow/schema.yaml": `name: beta-flow
mode: static
`,
		"beta-flow/events.yaml": `item.received:
  item_id: text
`,
	}
	for relative, contents := range files {
		path := filepath.Join(root, filepath.FromSlash(relative))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("create event-publish fixture directory: %v", err)
		}
		if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
			t.Fatalf("write event-publish fixture %s: %v", relative, err)
		}
	}
	repoRoot := filepath.Join("..", "..")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("load root/flow event-name collision fixture: %v", err)
	}
	entry, ok := bundle.AuthoredResolvedEventCatalog()["item.received"]
	if !ok {
		t.Fatal("loaded collision fixture omitted root item.received declaration")
	}
	// Preserve the API's exact root-name lookup while compiled schema evidence
	// remains owned by the loader-admitted project declaration.
	bundle.Events = map[string]runtimecontracts.EventCatalogEntry{"item.received": entry}
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
			Paths: runtimecontracts.FlowContractPaths{FlowPath: flowID},
			Path:  flowID,
			Events: map[string]runtimecontracts.EventCatalogEntry{
				eventName: topicEventCatalogEntry(),
			},
			Nodes: map[string]runtimecontracts.SystemNodeContract{
				nodeID: {
					SubscribesTo: []string{eventName},
					EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
						eventName: {},
					},
				},
			},
		})
	}
	sort.Slice(flows, func(i, j int) bool {
		return strings.TrimSpace(flows[i].Paths.FlowPath) < strings.TrimSpace(flows[j].Paths.FlowPath)
	})
	root := runtimecontracts.FlowContractView{Children: flows}
	for i := range root.Children {
		flow := &root.Children[i]
		byID[strings.TrimSpace(flow.Paths.FlowPath)] = flow
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
		"scan.requested": topicEventCatalogEntry(),
		"scan.followup":  topicEventCatalogEntry(),
		"scan.unhandled": topicEventCatalogEntry(),
	}
	node := runtimecontracts.SystemNodeContract{
		SubscribesTo: []string{"scan.requested", "scan.followup"},
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
			"scan.requested": {},
			"scan.followup":  {},
		},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{Name: "review", Version: "1.0.0"},
		Events:    eventsByName,
		Nodes:     map[string]runtimecontracts.SystemNodeContract{"scan-orchestrator": node},
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{{Event: "scan.requested"}, {Event: "scan.followup"}, {Event: "scan.unhandled"}}},
			},
		},
	}
	root := &runtimecontracts.FlowContractView{
		Paths:  runtimecontracts.FlowContractPaths{FlowPath: "."},
		Schema: *bundle.RootSchema, Events: eventsByName, Nodes: bundle.Nodes,
	}
	bundle.FlowTree = flowmodel.Tree[runtimecontracts.FlowContractView]{Root: root, ByID: map[string]*runtimecontracts.FlowContractView{".": root}}
	return mustCompileEventPublishTestBundle(bundle)
}

func eventPublishTargetRouteTestBundle(t testing.TB) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := canonicalrouting.WritePublicTemplateInputRoute(t)
	repoRoot := filepath.Join("..", "..")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("load event-publish target-route fixture: %v", err)
	}
	return bundle
}

func eventPublishRootTemplateCollisionTestBundle() *runtimecontracts.WorkflowContractBundle {
	return eventPublishTemplateInputTestBundle("review.requested", true)
}

func eventPublishRootTemplateCollisionSource(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	writeRunCompletionFixtureFile(t, root+"/schema.yaml", `name: review
pins:
  inputs:
    events: [review.requested]
`)
	writeRunCompletionFixtureFile(t, root+"/events.yaml", `review.requested:
  topic: text
  entity_id: text?
`)
	writeRunCompletionFixtureFile(t, root+"/agents.yaml", `workflow-runtime:
  role: review_observer
  model: regular
  intent: {inline: "Observe review requests."}
  subscriptions: [review.requested]
  emit_events: []
`)
	writeRunCompletionFixtureFile(t, root+"/operating/schema.yaml", `name: operating
mode: template
pins:
  inputs:
    events: [review.requested]
`)
	writeRunCompletionFixtureFile(t, root+"/operating/events.yaml", `review.requested:
  topic: text
  entity_id: text?
`)
	repoRoot := runCompletionRepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		root,
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load root/template event collision source: %v", err)
	}
	return bundle
}

func eventPublishTemplateInputTestBundle(eventName string, authoredRoot bool) *runtimecontracts.WorkflowContractBundle {
	operating := runtimecontracts.FlowContractView{
		Path:  "operating",
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "operating"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: "template",
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{{Event: eventName}}},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			eventName: topicAndEntityIDEventCatalogEntry(),
		},
	}
	root := runtimecontracts.FlowContractView{
		Paths:    runtimecontracts.FlowContractPaths{FlowPath: "."},
		Children: []runtimecontracts.FlowContractView{operating},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics:  runtimecontracts.WorkflowSemanticView{Name: "review", Version: "1.0.0"},
		RootSchema: &runtimecontracts.FlowSchemaDocument{},
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
	if !authoredRoot {
		return mustCompileEventPublishTestBundle(bundle)
	}
	rootNode := runtimecontracts.SystemNodeContract{
		SubscribesTo: []string{eventName},
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
			eventName: {},
		},
	}
	bundle.Events = map[string]runtimecontracts.EventCatalogEntry{eventName: topicAndEntityIDEventCatalogEntry()}
	bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{"root-orchestrator": rootNode}
	bundle.RootSchema.Pins.Inputs.EventPins = []runtimecontracts.FlowInputEventPin{{Event: eventName}}
	bundle.FlowTree.Root.Events = map[string]runtimecontracts.EventCatalogEntry{eventName: topicAndEntityIDEventCatalogEntry()}
	bundle.FlowTree.Root.Nodes = map[string]runtimecontracts.SystemNodeContract{"root-orchestrator": rootNode}
	return mustCompileEventPublishTestBundle(bundle)
}

func eventPublishCreateEntityTestBundle() *runtimecontracts.WorkflowContractBundle {
	const eventName = "thing.created"
	handler := runtimecontracts.SystemNodeEventHandler{CreateEntity: true}
	node := runtimecontracts.SystemNodeContract{
		SubscribesTo: []string{eventName},
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
			eventName: handler,
		},
	}
	root := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "."},
		Path:  ".",
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{{Event: eventName}}},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			eventName: {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"thing-writer": node,
		},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics:  runtimecontracts.WorkflowSemanticView{Name: "factory", Version: "1.0.0"},
		Events:     root.Events,
		Nodes:      root.Nodes,
		RootSchema: &root.Schema,
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				".": &root,
			},
		},
	}
	return mustCompileEventPublishTestBundle(bundle)
}

func eventPublishRenamedConnectedCreateEntityTestBundle() *runtimecontracts.WorkflowContractBundle {
	const rootEvent, receiverEvent, plainEvent = "thing.requested", "thing.created", "note.requested"
	payload := runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
		"entity_id": {Type: "uuid"}, "amount": {Type: "integer"},
	}}
	child := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "factory"}, Path: "factory",
		Schema: runtimecontracts.FlowSchemaDocument{Pins: runtimecontracts.FlowPins{
			Inputs: runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{{Event: receiverEvent}}},
		}},
		Events: map[string]runtimecontracts.EventCatalogEntry{receiverEvent: {Payload: payload}},
		Nodes: map[string]runtimecontracts.SystemNodeContract{"thing-writer": {
			ExecutionType: "system_node", SubscribesTo: []string{receiverEvent},
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{receiverEvent: {CreateEntity: true}},
		}},
	}
	shadow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "shadow"}, Path: "shadow",
		Schema: runtimecontracts.FlowSchemaDocument{Pins: runtimecontracts.FlowPins{
			Inputs: runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{{Event: plainEvent}}},
		}},
		Events: map[string]runtimecontracts.EventCatalogEntry{plainEvent: {Payload: payload}},
		Nodes: map[string]runtimecontracts.SystemNodeContract{"shadow-writer": {
			ExecutionType: "system_node", SubscribesTo: []string{plainEvent},
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{plainEvent: {CreateEntity: true}},
		}},
	}
	root := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "."}, Path: ".",
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs:  runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{{Event: rootEvent}, {Event: plainEvent}}},
				Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{{Event: rootEvent}}},
			},
			Connect: []runtimecontracts.FlowConnect{{Event: rootEvent, From: ".", To: "factory", Rename: receiverEvent, SourceFile: "schema.yaml", SourceLine: 1}},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{rootEvent: {Payload: payload}, plainEvent: {Payload: payload}},
		Nodes: map[string]runtimecontracts.SystemNodeContract{"observer": {
			ExecutionType: "system_node", SubscribesTo: []string{rootEvent, plainEvent},
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{rootEvent: {}, plainEvent: {}},
		}},
		Children: []runtimecontracts.FlowContractView{child, shadow},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		SourceArtifact: authorActivityTestSourceArtifact,
		Semantics:      runtimecontracts.WorkflowSemanticView{Name: "factory", Version: "1.0.0"},
		Events:         root.Events, Nodes: root.Nodes, RootSchema: &root.Schema,
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{"factory": child.Schema, "shadow": shadow.Schema},
		FlowSources: map[string]runtimecontracts.FlowSource{
			".":       {FlowPath: ".", Schema: "schema.yaml", Events: "events.yaml"},
			"factory": {FlowPath: "factory", Schema: "factory/schema.yaml", Events: "factory/events.yaml", Nodes: "factory/nodes.yaml"},
			"shadow":  {FlowPath: "shadow", Schema: "shadow/schema.yaml", Events: "shadow/events.yaml", Nodes: "shadow/nodes.yaml"},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root, ByID: map[string]*runtimecontracts.FlowContractView{".": &root, "factory": &root.Children[0], "shadow": &root.Children[1]},
		},
	}
	return mustCompileEventPublishTestBundle(bundle)
}

func mustCompileEventPublishTestBundle(bundle *runtimecontracts.WorkflowContractBundle) *runtimecontracts.WorkflowContractBundle {
	if bundle != nil && bundle.SourceArtifact == nil {
		bundle.SourceArtifact = authorActivityTestSourceArtifact
	}
	if bundle != nil && bundle.RootSchema != nil && bundle.FlowTree.Root != nil {
		bundle.FlowTree.Root.Schema = *bundle.RootSchema
	}
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		panic(fmt.Sprintf("compile event-publish test bundle: %v", err))
	}
	return bundle
}

func topicEventCatalogEntry() runtimecontracts.EventCatalogEntry {
	return runtimecontracts.EventCatalogEntry{Payload: runtimecontracts.EventPayloadSpec{
		Properties: map[string]runtimecontracts.EventFieldSpec{"topic": {Type: "text"}},
		Required:   []string{"topic"},
	}}
}

func topicAndEntityIDEventCatalogEntry() runtimecontracts.EventCatalogEntry {
	entry := topicEventCatalogEntry()
	entry.Payload.Properties["entity_id"] = runtimecontracts.EventFieldSpec{Type: "uuid"}
	return entry
}

func seedEventPublishEntityState(t *testing.T, db *sql.DB, source semanticview.Source, runID, entityID, flowInstance, currentState string) {
	t.Helper()
	var bundleHash string
	if err := db.QueryRow(`SELECT bundle_hash FROM runs WHERE run_id = $1::uuid`, runID).Scan(&bundleHash); err != nil {
		t.Fatalf("read flow readiness source for run %s: %v", runID, err)
	}
	if bundleHash != runStartTestBundleHashForSource(source) {
		t.Fatal("flow readiness fixture source differs from the run's admitted artifact")
	}
	flowTemplate := strings.SplitN(strings.TrimSpace(flowInstance), "/", 2)[0]
	readiness, err := (runtimepipeline.DynamicFlowRuntimeReadinessPlan{
		Identity: runtimeflowidentity.Instance{
			TemplateID: flowTemplate,
			ScopeKey:   flowTemplate,
			InstanceID: runtimeflowidentity.LogicalInstanceID(flowInstance), InstancePath: flowInstance,
			EntityID: entityID, HasStoredPath: true,
		},
		RunID: runID, BundleHash: bundleHash,
		WorkflowVersion: source.WorkflowVersion(), ExecutionMode: "live",
	}).Normalized()
	if err != nil {
		t.Fatalf("normalize exact flow readiness fixture: %v", err)
	}
	readinessJSON, err := json.Marshal(readiness)
	if err != nil {
		t.Fatalf("marshal exact flow readiness fixture: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO flow_instances (run_id, instance_path, flow_template, mode, config, status, created_at)
		VALUES ($1::uuid, $2, $3, 'template', '{}'::jsonb, 'active', now())
		ON CONFLICT (run_id, instance_path) DO NOTHING
	`, runID, flowInstance, flowTemplate); err != nil {
		t.Fatalf("seed exact flow instance lifecycle: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO flow_instance_runtime_readiness (run_id, instance_path, plan, created_at, updated_at)
		VALUES ($1::uuid, $2, $3::jsonb, now(), now())
		ON CONFLICT (run_id, instance_path) DO UPDATE SET plan = EXCLUDED.plan, updated_at = EXCLUDED.updated_at
	`, runID, flowInstance, readinessJSON); err != nil {
		t.Fatalf("seed exact flow instance readiness: %v", err)
	}
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
	route := asMap(t, decoded["route"])
	if decoded["kind"] != "existing_entity" || route["flow_instance"] != flowInstance || route["entity_id"] != entityID {
		t.Fatalf("delivery target_route = %#v, want flow/entity %s/%s", decoded, flowInstance, entityID)
	}
}

func assertEventPublishPersistence(t *testing.T, db *sql.DB, runID, eventID, eventName, producedBy, expectedBundleHash string) {
	t.Helper()
	var runStatus, triggerType, triggerID, bundleHash string
	if err := db.QueryRow(`
		SELECT status, trigger_event_type, trigger_event_id::text, bundle_hash
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&runStatus, &triggerType, &triggerID, &bundleHash); err != nil {
		t.Fatalf("load event.publish run row: %v", err)
	}
	if runStatus != "running" || triggerType != eventName || triggerID != eventID {
		t.Fatalf("run row status=%q trigger=%q/%q, want running/%s/%s", runStatus, triggerType, triggerID, eventName, eventID)
	}
	if bundleHash != expectedBundleHash {
		t.Fatalf("run row source artifact hash = %q, want %s", bundleHash, expectedBundleHash)
	}
	assertPostgresEventPublishRows(t, db, runID, eventID, producedBy)
}

func assertExistingRunEventPublishPersistence(t *testing.T, db *sql.DB, runID, eventID, producedBy string) {
	t.Helper()
	var runStatus, triggerType, triggerID, bundleHash string
	if err := db.QueryRow(`
		SELECT status, COALESCE(trigger_event_type, ''), COALESCE(trigger_event_id::text, ''), bundle_hash
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&runStatus, &triggerType, &triggerID, &bundleHash); err != nil {
		t.Fatalf("load existing event.publish run row: %v", err)
	}
	if runStatus != "running" || triggerType != "" || triggerID != "" {
		t.Fatalf("existing run row status=%q trigger=%q/%q, want running with no creation trigger", runStatus, triggerType, triggerID)
	}
	if bundleHash != runStartTestBundleHash {
		t.Fatalf("existing run source artifact = %q, want %s", bundleHash, runStartTestBundleHash)
	}
	assertPostgresEventPublishRows(t, db, runID, eventID, producedBy)
}

func assertPostgresEventPublishRows(t *testing.T, db *sql.DB, runID, eventID, producedBy string) {
	t.Helper()
	var entityID, flowInstance, gotProducedBy, targetRoute, targetSet string
	var payload json.RawMessage
	if err := db.QueryRow(`
		SELECT COALESCE(entity_id::text, ''), COALESCE(flow_instance, ''), produced_by, COALESCE(target_route::text, '{}'), COALESCE(target_set::text, '[]'), payload
		FROM events
		WHERE event_id = $1::uuid
	`, eventID).Scan(&entityID, &flowInstance, &gotProducedBy, &targetRoute, &targetSet, &payload); err != nil {
		t.Fatalf("load event.publish event row: %v", err)
	}
	assertStoredEventProjectionMatchesDeliveries(t, entityID, flowInstance, targetRoute, targetSet, runID, postgresEventDeliveryTargetRoutes(t, db, eventID))
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
	assertSQLiteEventPublishEventRow(t, db, runID, eventID, producedBy)
}

func assertSQLiteExistingRunEventPublishRows(t *testing.T, db *sql.DB, runID, eventID, producedBy string) {
	t.Helper()
	var runStatus, triggerType, triggerID string
	if err := db.QueryRow(`
		SELECT status, COALESCE(trigger_event_type, ''), COALESCE(trigger_event_id, '')
		FROM runs
		WHERE run_id = ?
	`, runID).Scan(&runStatus, &triggerType, &triggerID); err != nil {
		t.Fatalf("load existing sqlite event.publish run row: %v", err)
	}
	if runStatus != "running" || triggerType != "" || triggerID != "" {
		t.Fatalf("existing sqlite run row status=%q trigger=%q/%q, want running with no creation trigger", runStatus, triggerType, triggerID)
	}
	assertSQLiteEventPublishEventRow(t, db, runID, eventID, producedBy)
}

func assertSQLiteEventPublishEventRow(t *testing.T, db *sql.DB, runID, eventID, producedBy string) {
	t.Helper()
	var entityID, flowInstance, gotProducedBy, targetRoute, targetSet, payloadText string
	if err := db.QueryRow(`
		SELECT COALESCE(entity_id, ''), COALESCE(flow_instance, ''), COALESCE(produced_by, ''), COALESCE(target_route, '{}'), COALESCE(target_set, '[]'), payload
		FROM events
		WHERE event_id = ?
	`, eventID).Scan(&entityID, &flowInstance, &gotProducedBy, &targetRoute, &targetSet, &payloadText); err != nil {
		t.Fatalf("load sqlite event.publish event row: %v", err)
	}
	assertStoredEventProjectionMatchesDeliveries(t, entityID, flowInstance, targetRoute, targetSet, runID, sqliteEventDeliveryTargetRoutes(t, db, eventID))
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

func postgresEventDeliveryTargetRoutes(t *testing.T, db *sql.DB, eventID string) []events.RouteIdentity {
	t.Helper()
	rows, err := db.Query(`
		SELECT COALESCE(delivery_target_route::text, '{}')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		ORDER BY subscriber_type, subscriber_id, delivery_id
	`, eventID)
	if err != nil {
		t.Fatalf("load postgres delivery targets: %v", err)
	}
	defer rows.Close()
	return scanDeliveryTargetRoutes(t, rows)
}

func sqliteEventDeliveryTargetRoutes(t *testing.T, db *sql.DB, eventID string) []events.RouteIdentity {
	t.Helper()
	rows, err := db.Query(`
		SELECT COALESCE(delivery_target_route, '{}')
		FROM event_deliveries
		WHERE event_id = ?
		ORDER BY subscriber_type, subscriber_id, delivery_id
	`, eventID)
	if err != nil {
		t.Fatalf("load sqlite delivery targets: %v", err)
	}
	defer rows.Close()
	return scanDeliveryTargetRoutes(t, rows)
}

func scanDeliveryTargetRoutes(t *testing.T, rows *sql.Rows) []events.RouteIdentity {
	t.Helper()
	var routes []events.RouteIdentity
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			t.Fatalf("scan delivery target: %v", err)
		}
		var owner events.DeliveryTargetOwnership
		if err := json.Unmarshal([]byte(raw), &owner); err != nil {
			t.Fatalf("decode delivery target %s: %v", raw, err)
		}
		routes = append(routes, owner.Route())
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate delivery targets: %v", err)
	}
	return routes
}

func assertStoredEventProjectionMatchesDeliveries(t *testing.T, entityID, flowInstance, targetRouteRaw, targetSetRaw, authoredEntityID string, deliveryTargets []events.RouteIdentity) {
	t.Helper()
	var target events.RouteIdentity
	if err := json.Unmarshal([]byte(targetRouteRaw), &target); err != nil {
		t.Fatalf("decode event target_route: %v", err)
	}
	var targetSet []events.RouteIdentity
	if err := json.Unmarshal([]byte(targetSetRaw), &targetSet); err != nil {
		t.Fatalf("decode event target_set: %v", err)
	}
	nonEmptyTargets := make([]events.RouteIdentity, 0, len(deliveryTargets))
	for _, route := range deliveryTargets {
		if !route.Empty() && !containsStoredRoute(nonEmptyTargets, route) {
			nonEmptyTargets = append(nonEmptyTargets, route)
		}
	}
	switch {
	case len(nonEmptyTargets) == 0:
		if !target.Empty() || len(targetSet) != 0 || entityID != authoredEntityID || flowInstance != "" {
			t.Fatalf("untargeted event projection = entity:%q flow:%q target:%#v set:%#v, want authored entity %q", entityID, flowInstance, target, targetSet, authoredEntityID)
		}
	case len(deliveryTargets) == 1 && len(nonEmptyTargets) == 1:
		want := nonEmptyTargets[0]
		if !events.SameRouteIdentity(target, want) || len(targetSet) != 0 || entityID != want.EntityID || flowInstance != want.FlowInstance {
			t.Fatalf("singular event projection = entity:%q flow:%q target:%#v set:%#v, want %#v", entityID, flowInstance, target, targetSet, want)
		}
	default:
		if !target.Empty() || entityID != "" || flowInstance != "" || !sameStoredRouteSet(targetSet, nonEmptyTargets) {
			t.Fatalf("fan-out/mixed event projection = entity:%q flow:%q target:%#v set:%#v, want set %#v from delivery slots %#v", entityID, flowInstance, target, targetSet, nonEmptyTargets, deliveryTargets)
		}
	}
}

func sameStoredRouteSet(left, right []events.RouteIdentity) bool {
	if len(left) != len(right) {
		return false
	}
	for _, route := range left {
		if !containsStoredRoute(right, route) {
			return false
		}
	}
	return true
}

func containsStoredRoute(routes []events.RouteIdentity, want events.RouteIdentity) bool {
	for _, route := range routes {
		if events.SameRouteIdentity(route, want) {
			return true
		}
	}
	return false
}

func assertStoredEventUntargeted(t *testing.T, targetRouteRaw, targetSetRaw string) {
	t.Helper()
	var target events.RouteIdentity
	if err := json.Unmarshal([]byte(targetRouteRaw), &target); err != nil {
		t.Fatalf("decode event target_route: %v", err)
	}
	if !target.Empty() {
		t.Fatalf("event target_route = %#v, want untargeted root publication", target)
	}
	var targetSet []events.RouteIdentity
	if err := json.Unmarshal([]byte(targetSetRaw), &targetSet); err != nil {
		t.Fatalf("decode event target_set: %v", err)
	}
	if len(targetSet) != 0 {
		t.Fatalf("event target_set = %#v, want untargeted root publication", targetSet)
	}
}

func assertOperatorEventReference(t *testing.T, db *sql.DB, eventID, wantReferenceEventID string) {
	t.Helper()
	var sourceEventID, referenceEventID string
	if err := db.QueryRow(`
		SELECT COALESCE(source_event_id::text, ''), COALESCE(operator_reference_event_id::text, '')
		FROM events WHERE event_id = $1::uuid
	`, eventID).Scan(&sourceEventID, &referenceEventID); err != nil {
		t.Fatalf("load operator event provenance: %v", err)
	}
	if sourceEventID != "" || referenceEventID != wantReferenceEventID {
		t.Fatalf("operator event causal/reference ids = %q/%q, want empty/%q", sourceEventID, referenceEventID, wantReferenceEventID)
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
	if len(raw) == 0 {
		return outcome, nil
	}
	failure, err := runtimefailures.UnmarshalEnvelope(raw)
	if err != nil {
		t.Fatalf("decode pipeline receipt failure for %s: %v", eventID, err)
	}
	return outcome, &failure
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

func eventPublishScanNodeID(t testing.TB) string {
	t.Helper()
	return identitytest.FlowNode(t, "discovery", "scan-orchestrator").Key()
}

func eventPublishRepoObserverNodeID(t testing.TB) string {
	t.Helper()
	return identitytest.FlowNode(t, "repo-scaffold", "repo-observer").Key()
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
