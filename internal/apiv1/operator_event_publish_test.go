package apiv1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeingress "swarm/internal/runtime/ingress"
	runtimereplayclaim "swarm/internal/runtime/replayclaim"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/store"
	storerunlifecycle "swarm/internal/store/runlifecycle"
	"swarm/internal/testutil"
)

func TestOperatorEventPublishHandlersPersistEventReportDeliveriesAndReplayIdempotency(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
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
	if len(deliveries) != 1 {
		t.Fatalf("deliveries = %#v, want one persisted delivery", deliveries)
	}
	delivery := asMap(t, deliveries[0])
	if delivery["subscriber_id"] != "scan-orchestrator" || delivery["status"] != "pending" || delivery["attempt"] != float64(1) {
		t.Fatalf("delivery = %#v, want scan-orchestrator pending attempt 1", delivery)
	}
	if count := countEventsByName(t, db, "scan.requested"); count != 1 {
		t.Fatalf("scan.requested event count = %d, want 1", count)
	}
	assertEventPublishPersistence(t, db, runID, eventID, "scan.requested", "cli-publish:"+actorTokenID(testToken))
	if count := countAPIIdempotencyRows(t, db); count != 1 {
		t.Fatalf("api_idempotency rows = %d, want 1", count)
	}
	select {
	case got := <-ch:
		if got.ID != eventID {
			t.Fatalf("delivered event = %s, want %s", got.ID, eventID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event.publish delivery")
	}

	replay := rpcCall(t, handler, body)
	if replay.Error != nil {
		t.Fatalf("event.publish replay error = %#v", replay.Error)
	}
	replayResult := asMap(t, replay.Result)
	if replayResult["event_id"] != eventID || replayResult["run_id"] != runID {
		t.Fatalf("event.publish replay result = %#v, want original event/run", replayResult)
	}
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

func TestOperatorEventPublishHandlersRequireCanonicalBundleHashForCreateNewWork(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
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

func TestOperatorEventPublishPersistsIdempotencyBeforeReadbackFailure(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
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

	first := rpcCall(t, handler, body)
	if first.Error == nil || !strings.Contains(fmt.Sprintf("%#v", first.Error.Data), "transient event readback failure") {
		t.Fatalf("first event.publish error = %#v, want transient readback failure", first.Error)
	}
	if count := countEventsByName(t, db, "scan.requested"); count != 1 {
		t.Fatalf("scan.requested event count after readback failure = %d, want 1", count)
	}
	if count := countAPIIdempotencyRows(t, db); count != 1 {
		t.Fatalf("api_idempotency rows after readback failure = %d, want 1", count)
	}
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event delivery after readback failure")
	}

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
	if got := len(asSlice(t, replayResult["deliveries"])); got != 1 {
		t.Fatalf("replay deliveries = %d, want persisted delivery result", got)
	}
}

func TestOperatorEventPublishPostCommitReceiptFailureReplaysWithoutDuplicate(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
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
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event delivery after post-commit receipt failure")
	}

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
	pg := &store.PostgresStore{DB: db}
	failing := &failNormalRunCompletionStore{
		PostgresStore: pg,
		err:           errors.New("simulated normal-run completion failure"),
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
	outcome, errText := loadPipelineReceiptOutcomeAndError(t, ctx, db, eventID)
	if outcome != "dead_letter" || !strings.Contains(errText, "simulated normal-run completion failure") {
		t.Fatalf("pipeline receipt outcome=%q error=%q, want dead_letter with completion failure", outcome, errText)
	}
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event delivery after post-commit completion failure")
	}

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

func TestOperatorEventPublishExplicitRunTargetRequiresExistingNonterminalRun(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := runtimebus.NewEventBusWithOptions(pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	seedActiveAPIV1RuntimeBusAgent(t, context.Background(), pg, "scan-orchestrator")
	handler := eventPublishTestHandler(t, pg, bus, source)

	initial := rpcCall(t, handler, eventPublishBody("", runStartTestFingerprint, "scan.requested", `{"topic":"first"}`, "", "idem-new-run"))
	if initial.Error != nil {
		t.Fatalf("initial event.publish error = %#v", initial.Error)
	}
	runID := stringValue(t, asMap(t, initial.Result)["run_id"], "run_id")

	targeted := rpcCall(t, handler, eventPublishBody(runID, runStartTestFingerprint, "scan.requested", `{"topic":"second"}`, "operator-test", "idem-existing-run"))
	if targeted.Error != nil {
		t.Fatalf("targeted event.publish error = %#v", targeted.Error)
	}
	targetedResult := asMap(t, targeted.Result)
	if targetedResult["run_id"] != runID || targetedResult["new_run_created"] != false {
		t.Fatalf("targeted result = %#v, want existing run", targetedResult)
	}
	if count := countEventsByName(t, db, "scan.requested"); count != 2 {
		t.Fatalf("scan.requested events after targeted publish = %d, want 2", count)
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

func TestOperatorEventPublishSourceEventIDValidatesSameRunLineage(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	bus, err := runtimebus.NewEventBusWithOptions(pg, runStartTestEventBusOptions(source))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := eventPublishTestHandler(t, pg, bus, source)

	parent := rpcCall(t, handler, eventPublishBody("", runStartTestFingerprint, "scan.requested", `{"topic":"medicine"}`, "", "idem-source-parent"))
	if parent.Error != nil {
		t.Fatalf("parent event.publish error = %#v", parent.Error)
	}
	parentResult := asMap(t, parent.Result)
	parentEventID := stringValue(t, parentResult["event_id"], "event_id")
	parentRunID := stringValue(t, parentResult["run_id"], "run_id")

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
}

func TestOperatorEventPublishSourceEventIDRejectsInvalidLineageBeforePersistence(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
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
		_, db, _ := testutil.StartPostgres(t)
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
		_, db, _ := testutil.StartPostgres(t)
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
		_, db, _ := testutil.StartPostgres(t)
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
		_, db, _ := testutil.StartPostgres(t)
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
		_, db, _ := testutil.StartPostgres(t)
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
		_, db, _ := testutil.StartPostgres(t)
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
	_, db, cleanup := testutil.StartPostgres(t)
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
	if got := len(asSlice(t, asMap(t, published.Result)["deliveries"])); got != 1 {
		t.Fatalf("paused event.publish deliveries = %d, want 1", got)
	}
	select {
	case got := <-ch:
		t.Fatalf("paused event.publish delivered event %s before resume", got.ID)
	case <-time.After(150 * time.Millisecond):
	}
	if got := countEventDeliveriesForEvent(t, ctx, db, eventID); got != 1 {
		t.Fatalf("paused event deliveries = %d, want 1", got)
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
	select {
	case got := <-ch:
		if got.ID != eventID {
			t.Fatalf("released event = %s, want %s", got.ID, eventID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queued event.publish release")
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
	return testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:              func() time.Time { return time.Now().UTC() },
			Ready:            func() bool { return true },
			Database:         fakePinger{},
			Runs:             runs,
			Observability:    observability,
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

type failStandalonePipelineReceiptOnceStore struct {
	*store.PostgresStore
	err error
}

func (s *failStandalonePipelineReceiptOnceStore) UpsertPipelineReceipt(ctx context.Context, eventID, status, errText string) error {
	return s.UpsertPipelineReceiptTx(ctx, nil, eventID, status, errText)
}

func (s *failStandalonePipelineReceiptOnceStore) UpsertPipelineReceiptTx(ctx context.Context, tx *sql.Tx, eventID, status, errText string) error {
	if tx == nil && s.err != nil {
		err := s.err
		s.err = nil
		return err
	}
	return s.PostgresStore.UpsertPipelineReceiptTx(ctx, tx, eventID, status, errText)
}

type failCommittedReplayScopeStore struct {
	*store.PostgresStore
	err error
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
	if decoded["entity_id"] != runID || decoded["topic"] != "medicine" {
		t.Fatalf("event.publish payload = %#v", decoded)
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

func countAllEventDeliveries(t *testing.T, db *sql.DB) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM event_deliveries`).Scan(&count); err != nil {
		t.Fatalf("count event_deliveries rows: %v", err)
	}
	return count
}

func loadPipelineReceiptOutcomeAndError(t *testing.T, ctx context.Context, db *sql.DB, eventID string) (string, string) {
	t.Helper()
	var outcome, errText string
	if err := db.QueryRowContext(ctx, `
		SELECT outcome, COALESCE(side_effects->>'error', '')
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&outcome, &errText); err != nil {
		t.Fatalf("load pipeline receipt for %s: %v", eventID, err)
	}
	return outcome, errText
}

func containsMissingPipelineReceiptEvent(items []events.PersistedReplayEvent, eventID string) bool {
	for _, evt := range items {
		if strings.TrimSpace(evt.Event.ID) == strings.TrimSpace(eventID) {
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

func asSlice(t *testing.T, value any) []any {
	t.Helper()
	items, ok := value.([]any)
	if !ok {
		t.Fatalf("value = %#v, want []any", value)
	}
	return items
}
