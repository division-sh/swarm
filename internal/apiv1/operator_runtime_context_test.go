package apiv1

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	swruntime "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	storerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

const runtimeContextTestBundleHashB = "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
const runtimeContextTestBundleHashC = "bundle-v1:sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

func TestOperatorRuntimeContextManagerRoutesCreateNewWorkToSelectedBundle(t *testing.T) {
	fixture := newOperatorRuntimeContextFixture(t)
	handler := fixture.handler(t)
	chPrimary := runtimebustest.Subscribe(t, fixture.busA, "scan-orchestrator", events.EventType("triage.requested"))
	defer runtimebustest.Unsubscribe(fixture.busA, "scan-orchestrator")
	chSelected := runtimebustest.Subscribe(t, fixture.busB, "scan-orchestrator", events.EventType("triage.requested"))
	defer runtimebustest.Unsubscribe(fixture.busB, "scan-orchestrator")

	published := rpcCall(t, handler, eventPublishBodyWithBundleHash("", runtimeContextTestBundleHashB, "triage.requested", `{"topic":"context-b"}`, "", "idem-context-publish"))
	if published.Error != nil {
		t.Fatalf("event.publish error = %#v", published.Error)
	}
	publishedResult := asMap(t, published.Result)
	publishedRunID := stringValue(t, publishedResult["run_id"], "run_id")
	publishedEventID := stringValue(t, publishedResult["event_id"], "event_id")
	assertRunBundleIdentity(t, fixture.db, publishedRunID, runtimeContextTestBundleHashB, storerunlifecycle.BundleSourcePersisted)
	if got := countEventsByName(t, fixture.db, "triage.requested"); got != 1 {
		t.Fatalf("triage.requested count after event.publish = %d, want 1", got)
	}
	got := requireAPIV1RuntimeBusEvent(t, chSelected, "selected context delivery")
	if got.ID() != publishedEventID {
		t.Fatalf("selected context delivered event = %s, want %s", got.ID(), publishedEventID)
	}
	requireNoAPIV1RuntimeBusEvent(t, chPrimary, "primary context selected bundle route")

	runID := uuid.NewString()
	started := rpcCall(t, handler, runStartBodyWithBundleHash(runID, runtimeContextTestBundleHashB, "triage.requested", `{"topic":"context-b-start"}`, "idem-context-start"))
	if started.Error != nil {
		t.Fatalf("run.start error = %#v", started.Error)
	}
	assertRunBundleIdentity(t, fixture.db, runID, runtimeContextTestBundleHashB, storerunlifecycle.BundleSourcePersisted)
	if got := countEventsByName(t, fixture.db, "triage.requested"); got != 2 {
		t.Fatalf("triage.requested count after run.start = %d, want 2", got)
	}
}

func TestOperatorEventPublishIdempotencyReplayDoesNotRequireLoadedRuntimeContext(t *testing.T) {
	fixture := newOperatorRuntimeContextFixture(t)
	handler := fixture.handler(t)
	selected := runtimebustest.Subscribe(t, fixture.busB, "scan-orchestrator", events.EventType("triage.requested"))
	defer runtimebustest.Unsubscribe(fixture.busB, "scan-orchestrator")
	body := eventPublishBodyWithBundleHash("", runtimeContextTestBundleHashB, "triage.requested", `{"topic":"replay-after-unload"}`, "", "idem-context-replay-after-unload")

	first := rpcCall(t, handler, body)
	if first.Error != nil {
		t.Fatalf("first event.publish error = %#v", first.Error)
	}
	firstEventID := stringValue(t, asMap(t, first.Result)["event_id"], "event_id")
	requireAPIV1RuntimeBusEventID(t, selected, firstEventID, "selected context delivery before unload")

	deactivated := fixture.manager.DeactivateBundleHash(runtimeContextTestBundleHashB, "idempotency replay proof")
	if !deactivated.Found || !deactivated.Changed || deactivated.ShutdownErr != nil {
		t.Fatalf("deactivate selected runtime = %#v", deactivated)
	}

	replay := rpcCall(t, handler, body)
	if replay.Error != nil {
		t.Fatalf("event.publish replay after runtime unload error = %#v", replay.Error)
	}
	if replayEventID := stringValue(t, asMap(t, replay.Result)["event_id"], "event_id"); replayEventID != firstEventID {
		t.Fatalf("event.publish replay event_id = %q, want stored %q", replayEventID, firstEventID)
	}
	if got := countEventsByName(t, fixture.db, "triage.requested"); got != 1 {
		t.Fatalf("triage.requested count after replay = %d, want 1", got)
	}
}

func TestOperatorRuntimeContextManagerRoutesExistingRunByStoredBundle(t *testing.T) {
	fixture := newOperatorRuntimeContextFixture(t)
	handler := fixture.handler(t)
	chSelected := runtimebustest.Subscribe(t, fixture.busB, "scan-orchestrator", events.EventType("triage.requested"))
	defer runtimebustest.Unsubscribe(fixture.busB, "scan-orchestrator")
	runID := uuid.NewString()
	started := rpcCall(t, handler, runStartBodyWithBundleHash(runID, runtimeContextTestBundleHashB, "triage.requested", `{"topic":"seed-existing"}`, "idem-existing-seed"))
	if started.Error != nil {
		t.Fatalf("seed run.start error = %#v", started.Error)
	}
	got := requireAPIV1RuntimeBusEvent(t, chSelected, "seed existing-run delivery")
	if got.RunID() != runID {
		t.Fatalf("seed existing-run delivery run = %s, want %s", got.RunID(), runID)
	}

	body := fmt.Sprintf(
		`{"jsonrpc":"2.0","id":"publish-existing","method":"event.publish","params":{"run_id":%q,"event_name":"triage.requested","payload":{"topic":"existing-run"},"idempotency_key":"idem-existing-context"}}`,
		runID,
	)
	resp := rpcCall(t, handler, body)
	if resp.Error != nil {
		t.Fatalf("event.publish existing run error = %#v", resp.Error)
	}
	result := asMap(t, resp.Result)
	eventID := stringValue(t, result["event_id"], "event_id")
	deliveries := asSlice(t, result["deliveries"])
	if len(deliveries) != 2 {
		t.Fatalf("event.publish existing run deliveries = %#v, want typed agent and node rows", deliveries)
	}
	assertEventPublishDeliveriesContain(t, deliveries, "agent", "scan-orchestrator", "pending", 1)
	assertEventPublishDeliveriesContain(t, deliveries, "node", identitytest.FlowNode(t, "discovery", "scan-orchestrator").Key(), "pending", 1)
	if got := countEventRowsByRunID(t, fixture.db, runID); got != 2 {
		t.Fatalf("event rows for existing run = %d, want 2", got)
	}
	assertRunBundleIdentity(t, fixture.db, runID, runtimeContextTestBundleHashB, storerunlifecycle.BundleSourcePersisted)
	got = requireAPIV1RuntimeBusEventID(t, chSelected, eventID, "existing-run selected-context delivery")
	if got.ID() != eventID || got.RunID() != runID {
		t.Fatalf("existing-run delivery id/run = %s/%s, want %s/%s", got.ID(), got.RunID(), eventID, runID)
	}
}

func TestOperatorRuntimeContextManagerRejectsExistingRunSameHashDifferentSourceBeforeMutation(t *testing.T) {
	fixture := newOperatorRuntimeContextFixture(t)
	handler := fixture.handler(t)
	runID := uuid.NewString()
	seedRuntimeContextRunBundle(t, fixture.pg, runID, runtimeContextTestBundleHashB, storerunlifecycle.BundleSourceEphemeral)

	resp := rpcCall(t, handler, eventPublishExistingRunBody(runID, "", "idem-context-source-mismatch"))
	assertRuntimeContextBundleError(t, resp, "event.publish", BundleDataIntegrityErrorCode, "runtime_source_fact_mismatch")
	if got := countEventRowsByRunID(t, fixture.db, runID); got != 0 {
		t.Fatalf("event rows for source-mismatched run = %d, want 0", got)
	}
	if got := countAPIIdempotencyRows(t, fixture.db); got != 0 {
		t.Fatalf("api_idempotency rows for source-mismatched run = %d, want 0", got)
	}
}

func TestOperatorRuntimeContextManagerRejectsExistingRunUnavailableSourceStates(t *testing.T) {
	tests := []struct {
		name          string
		bundleHash    string
		bundleSource  string
		seedBundleRow bool
		wantCode      string
		wantCause     string
	}{
		{
			name:         "deleted",
			bundleHash:   runtimeContextTestBundleHashB,
			bundleSource: storerunlifecycle.BundleSourceDeleted,
			wantCode:     BundleUnavailableCode,
			wantCause:    storerunlifecycle.BundleSourceDeleted,
		},
		{
			name:         "persisted missing bundle row",
			bundleHash:   runtimeContextTestBundleHashC,
			bundleSource: storerunlifecycle.BundleSourcePersisted,
			wantCode:     BundleDataIntegrityErrorCode,
			wantCause:    "persisted_missing_bundle_row",
		},
		{
			name:          "persisted unloaded context",
			bundleHash:    runtimeContextTestBundleHashC,
			bundleSource:  storerunlifecycle.BundleSourcePersisted,
			seedBundleRow: true,
			wantCode:      BundleUnavailableCode,
			wantCause:     "runtime_context_not_loaded",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newOperatorRuntimeContextFixture(t)
			if tt.seedBundleRow {
				seedOperatorBundleDeleteBundle(t, context.Background(), fixture.db, tt.bundleHash)
			}
			handler := fixture.handler(t)
			runID := uuid.NewString()
			if tt.bundleSource == storerunlifecycle.BundleSourceDeleted ||
				(tt.bundleSource == storerunlifecycle.BundleSourcePersisted && !tt.seedBundleRow) {
				runlifecyclefixture.RequireCorruptPostgresSnapshot(t, context.Background(), fixture.db, runlifecyclefixture.CorruptSnapshot{OriginKind: runlifecyclefixture.ScenarioSetupOriginKind(),
					RunID: runID, State: string(storerunlifecycle.StateRunning),
					BundleHash: tt.bundleHash, BundleSource: tt.bundleSource,
				})
			} else {
				seedRuntimeContextRunBundle(t, fixture.pg, runID, tt.bundleHash, tt.bundleSource)
			}
			keyName := strings.ReplaceAll(tt.name, " ", "-")

			calls := []struct {
				method string
				body   string
			}{
				{
					method: "event.publish",
					body:   eventPublishExistingRunBody(runID, "", "idem-context-publish-"+keyName),
				},
				{
					method: "run.start",
					body:   runStartBodyWithoutBundle(runID, "triage.requested", `{"topic":"blocked"}`, "idem-context-start-"+keyName),
				},
			}
			for _, call := range calls {
				resp := rpcCall(t, handler, call.body)
				assertRuntimeContextBundleError(t, resp, call.method, tt.wantCode, tt.wantCause)
				if got := countEventRowsByRunID(t, fixture.db, runID); got != 0 {
					t.Fatalf("%s event rows for unavailable run = %d, want 0", call.method, got)
				}
			}
		})
	}
}

func TestOperatorRuntimeContextManagerRejectsExistingRunRequestedHashMismatch(t *testing.T) {
	tests := []struct {
		method string
		body   func(string) string
	}{
		{
			method: "event.publish",
			body: func(runID string) string {
				return eventPublishExistingRunBody(runID, runStartTestBundleHash, "idem-context-publish-mismatch")
			},
		},
		{
			method: "run.start",
			body: func(runID string) string {
				return runStartBodyWithBundleHash(runID, runStartTestBundleHash, "triage.requested", `{"topic":"mismatch"}`, "idem-context-start-mismatch")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			fixture := newOperatorRuntimeContextFixture(t)
			handler := fixture.handler(t)
			runID := uuid.NewString()
			seedRuntimeContextRunBundle(t, fixture.pg, runID, runtimeContextTestBundleHashB, storerunlifecycle.BundleSourcePersisted)

			resp := rpcCall(t, handler, tt.body(runID))
			assertRuntimeContextBundleError(t, resp, tt.method, BundleMismatchCode, "")
			if got := countEventRowsByRunID(t, fixture.db, runID); got != 0 {
				t.Fatalf("%s event rows for mismatched run = %d, want 0", tt.method, got)
			}
		})
	}
}

func TestOperatorRuntimeContextManagerRoutesEventReplayByOriginalRunBundle(t *testing.T) {
	fixture := newOperatorRuntimeContextFixture(t)
	handler := fixture.handler(t)
	ctx := testAuthorActivityContext(context.Background())
	seedActiveOperatorReplayAgent(t, ctx, fixture.pg, "agent-a")
	chPrimary := subscribeOperatorReplayAgent(t, fixture.busA, "agent-a")
	defer runtimebustest.Unsubscribe(fixture.busA, "agent-a")
	chSelected := subscribeOperatorReplayAgent(t, fixture.busB, "agent-a")
	defer runtimebustest.Unsubscribe(fixture.busB, "agent-a")
	original := seedReplayableOperatorEvent(t, ctx, fixture.pg, "triage.requested", []string{"agent-a"}, runtimedelivery.StatusDelivered)
	if _, err := storetest.ReviseRunSource(testAuthorActivityContext(context.Background()), fixture.pg, storerunlifecycle.SourceRevisionRequest{
		RunID:  original.RunID,
		Source: runtimeContextTestSourceFact(runtimeContextTestBundleHashB),
	}); err != nil {
		t.Fatalf("revise replay source run bundle: %v", err)
	}

	resp := rpcCall(t, handler, eventReplayBody(original.EventID, []string{"agent-a"}, "idem-context-event-replay"))
	if resp.Error != nil {
		t.Fatalf("event.replay error = %#v", resp.Error)
	}
	result := asMap(t, resp.Result)
	replayEventID := stringValue(t, result["replay_event_id"], "replay_event_id")
	auditEventID := stringValue(t, result["audit_event_id"], "audit_event_id")
	assertReplayEventDelivered(t, chSelected, replayEventID, original.EventID)
	assertNoReplayEvent(t, chPrimary)
	assertReplayPersistence(t, fixture.db, original.EventID, replayEventID, auditEventID, 1)
}

func TestOperatorRuntimeContextManagerRoutesRunControlByStoredBundle(t *testing.T) {
	fixture := newOperatorRuntimeContextFixture(t)
	baseStore := &recordingRuntimeContextRunControlStore{}
	selectedStore := &recordingRuntimeContextRunControlStore{}
	baseControl := runtimeruncontrol.NewController(baseStore, nil, runtimeruncontrol.Options{})
	selectedControl := runtimeruncontrol.NewController(selectedStore, nil, runtimeruncontrol.Options{})
	manager := runtimeContextManagerWithRuntimes(t, fixture,
		&swruntime.Runtime{Bus: fixture.busA, RunControl: baseControl},
		&swruntime.Runtime{Bus: fixture.busB, RunControl: selectedControl},
	)
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			Now:              func() time.Time { return time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC) },
			Idempotency:      fixture.pg,
			RunBundleContext: fixture.pg,
			RunControl:       baseControl,
			RuntimeContexts:  manager,
		}),
	})

	for _, method := range []string{"run.pause", "run.continue", "run.stop"} {
		runID := uuid.NewString()
		seedRuntimeContextRunBundle(t, fixture.pg, runID, runtimeContextTestBundleHashB, storerunlifecycle.BundleSourcePersisted)
		resp := rpcCall(t, handler, runControlBody(method, runID, "idem-context-"+method))
		if resp.Error != nil {
			t.Fatalf("%s error = %#v", method, resp.Error)
		}
	}
	if baseStore.totalCalls() != 0 {
		t.Fatalf("base run control calls = %d, want 0", baseStore.totalCalls())
	}
	if selectedStore.pauseCalls != 1 || selectedStore.continueCalls != 1 || selectedStore.stopCalls != 1 {
		t.Fatalf("selected run control calls pause/continue/stop = %d/%d/%d, want 1/1/1", selectedStore.pauseCalls, selectedStore.continueCalls, selectedStore.stopCalls)
	}
}

func TestOperatorRuntimeContextManagerRoutesAgentDirectiveByStoredBundle(t *testing.T) {
	fixture := newOperatorRuntimeContextFixture(t)
	baseAgent := &directiveIntegrationAgent{id: "agent-1"}
	selectedAgent := &directiveIntegrationAgent{id: "agent-1"}
	baseManager := runtimeContextTestAgentManager(t, fixture.pg, fixture.busA, fixture.ownerA, baseAgent, runtimeContextTestSourceFact(runStartTestBundleHash))
	selectedManager := runtimeContextTestAgentManager(t, fixture.pg, fixture.busB, fixture.ownerB, selectedAgent, runtimeContextTestSourceFact(runtimeContextTestBundleHashB))
	manager := runtimeContextManagerWithRuntimes(t, fixture,
		&swruntime.Runtime{Bus: fixture.busA, Manager: baseManager},
		&swruntime.Runtime{Bus: fixture.busB, Manager: selectedManager},
	)
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			Now:              func() time.Time { return time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC) },
			Idempotency:      fixture.pg,
			RunBundleContext: fixture.pg,
			AgentControl:     baseManager,
			RuntimeContexts:  manager,
		}),
	})
	runID := uuid.NewString()
	seedRuntimeContextRunBundle(t, fixture.pg, runID, runtimeContextTestBundleHashB, storerunlifecycle.BundleSourcePersisted)

	resp := rpcCall(t, handler, agentDirectiveBodyWithRun("agent-1", runID, "inspect context", "idem-context-agent-directive"))
	if resp.Error != nil {
		t.Fatalf("agent.send_directive error = %#v", resp.Error)
	}
	if selectedAgent.calls != 1 {
		t.Fatalf("selected agent calls = %d, want 1", selectedAgent.calls)
	}
	if baseAgent.calls != 0 {
		t.Fatalf("base agent calls = %d, want 0", baseAgent.calls)
	}
}

type rejectingPrimaryDecisionCards struct {
	decisioncard.Store
	calls int
}

func (s *rejectingPrimaryDecisionCards) reject() error {
	s.calls++
	return errors.New("primary decision-card owner must not be used")
}

func (s *rejectingPrimaryDecisionCards) GetDecisionCard(context.Context, string) (decisioncard.Card, error) {
	return decisioncard.Card{}, s.reject()
}

func (s *rejectingPrimaryDecisionCards) DeferDecisionCard(context.Context, decisioncard.DeferRequest) (decisioncard.DecisionOutcome, error) {
	return decisioncard.DecisionOutcome{}, s.reject()
}

func (s *rejectingPrimaryDecisionCards) CancelDecisionCardInput(context.Context, decisioncard.CancelInputRequest) (decisioncard.InputDraft, error) {
	return decisioncard.InputDraft{}, s.reject()
}

func TestOperatorRuntimeContextManagerRoutesEveryDecisionMutationThroughSelectedPipeline(t *testing.T) {
	fixture := newOperatorRuntimeContextFixture(t)
	now := time.Date(2026, 7, 14, 14, 0, 0, 0, time.UTC)
	runID := uuid.NewString()
	storetest.RequirePostgresRun(t, testAuthorActivityContext(context.Background()), fixture.db, storetest.RunFixture{
		Origin: storetest.ScenarioSetupOrigin(), RunID: runID,
		BundleHash: runtimeContextTestBundleHashB, BundleSource: storerunlifecycle.BundleSourcePersisted,
	})
	card, continuation := newAPIHumanTaskAckLossCard(t, runID, now)
	card.BundleHash = runtimeContextTestBundleHashB
	continuation.BudgetBundleHash = runtimeContextTestBundleHashB
	if err := fixture.pg.CreateHumanTaskCard(testAuthorActivityContextForSource(context.Background(), runtimeContextTestSourceFact(runtimeContextTestBundleHashB)), card, continuation); err != nil {
		t.Fatalf("create selected-bundle human-task card: %v", err)
	}

	moduleA := newRunCompletionSystemNodeModule(t, fixture.sourceA)
	moduleB := newRunCompletionSystemNodeModule(t, fixture.sourceB)
	primaryCards := &rejectingPrimaryDecisionCards{Store: fixture.pg}
	primaryPipeline := runtimepipeline.NewPipelineCoordinatorWithOptions(fixture.busA, completeAPITestDurableWorkflowOptions(t, fixture.pg, fixture.busA, runtimepipeline.PipelineCoordinatorOptions{
		Module: moduleA, Persistence: runtimepipeline.NewWorkflowPersistence(fixture.pg),
		DecisionCards: primaryCards, BundleSourceFact: runtimeContextTestSourceFact(runStartTestBundleHash),
	}))

	selectedPipeline := runtimepipeline.NewPipelineCoordinatorWithOptions(fixture.busB, completeAPITestDurableWorkflowOptions(t, fixture.pg, fixture.busB, runtimepipeline.PipelineCoordinatorOptions{
		Module: moduleB, Persistence: runtimepipeline.NewWorkflowPersistence(fixture.pg),
	}))

	manager := runtimeContextManagerWithRuntimes(t, fixture,
		&swruntime.Runtime{Bus: fixture.busA, Pipeline: primaryPipeline},
		&swruntime.Runtime{Bus: fixture.busB, Pipeline: selectedPipeline},
	)
	handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: testOperatorHandlers(testOperatorCapabilities{
		Now: func() time.Time { return now.Add(time.Minute) }, Idempotency: fixture.pg,
		Mailbox: fixture.pg, DecisionCards: fixture.pg, DecisionAuthority: primaryPipeline, Events: fixture.busA,
		RunBundleContext: fixture.pg, RuntimeContexts: manager, Source: fixture.sourceA,
		Bundle: runtimecontracts.BundleIdentity{WorkflowName: "review", WorkflowVersion: "1.0.0", BundleHash: runStartTestBundleHash},
	})})

	begin := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"begin","method":"mailbox.begin_input","params":{"card_id":%q,"verdict":"reject","observed_content_hash":%q,"idempotency_key":"selected-begin"}}`, card.CardID, card.CardContentHash))
	if begin.Error != nil {
		t.Fatalf("selected mailbox.begin_input error = %#v", begin.Error)
	}
	draftID := stringValue(t, asMap(t, begin.Result)["input_draft_id"], "input_draft_id")
	cancel := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"cancel","method":"mailbox.cancel_input","params":{"card_id":%q,"input_draft_id":%q,"idempotency_key":"selected-cancel"}}`, card.CardID, draftID))
	if cancel.Error != nil {
		t.Fatalf("selected mailbox.cancel_input error = %#v", cancel.Error)
	}
	deferBody := fmt.Sprintf(`{"jsonrpc":"2.0","id":"defer","method":"mailbox.defer","params":{"card_id":%q,"until":"2026-07-15T14:00:00Z","idempotency_key":"selected-defer"}}`, card.CardID)
	if deferred := rpcCall(t, handler, deferBody); deferred.Error != nil {
		t.Fatalf("selected mailbox.defer error = %#v", deferred.Error)
	}
	decideBody := fmt.Sprintf(`{"jsonrpc":"2.0","id":"decide","method":"mailbox.decide","params":{"card_id":%q,"verdict":"approve","fields":{},"observed_content_hash":%q,"idempotency_key":"selected-decide"}}`, card.CardID, card.CardContentHash)
	if decided := rpcCall(t, handler, decideBody); decided.Error != nil {
		t.Fatalf("selected mailbox.decide error = %#v", decided.Error)
	}
	replay := rpcCall(t, handler, decideBody)
	if replay.Error != nil || asMap(t, replay.Result)["idempotency_replayed"] != true {
		t.Fatalf("selected mailbox.decide replay = result %#v error %#v", replay.Result, replay.Error)
	}
	if primaryCards.calls != 0 {
		t.Fatalf("primary decision path calls = %d, want 0", primaryCards.calls)
	}
}

func TestOperatorRuntimeContextManagerFailsClosedForUnloadedBundle(t *testing.T) {
	fixture := newOperatorRuntimeContextFixture(t)
	handler := fixture.handler(t)
	resp := rpcCall(t, handler, eventPublishBodyWithBundleHash("", runtimeContextTestBundleHashC, "triage.requested", `{"topic":"missing"}`, "", "idem-unloaded-context"))
	if resp.Error == nil {
		t.Fatal("event.publish unloaded bundle error = nil")
	}
	data := asMap(t, resp.Error.Data)
	details := asMap(t, data["details"])
	if data["code"] != BundleUnavailableCode || details["cause"] != "runtime_context_not_loaded" {
		t.Fatalf("event.publish unloaded bundle error data = %#v", data)
	}
	if got := countAllRunRows(t, fixture.db); got != 0 {
		t.Fatalf("run rows after unloaded bundle = %d, want 0", got)
	}

	executor := &recordingRunForkExecutor{}
	forkHandler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			Now:                 func() time.Time { return time.Unix(1700000000, 0).UTC() },
			RunForkAvailability: &recordingRunForkAvailability{rows: map[string]runbundle.Availability{runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, runStartTestBundleHash)}},
			RunFork:             executor,
			Idempotency:         newMutatingProbeIdempotencyStore(),
			RuntimeContexts:     fixture.manager,
		}),
	})
	forkResp := rpcCall(t, forkHandler, fmt.Sprintf(
		`{"jsonrpc":"2.0","id":"fork","method":"run.fork","params":{"source_run_id":%q,"bundle_hash":%q,"confirm_source_freeze":true,"idempotency_key":"fork-unloaded-context"}}`,
		runForkTestSourceRunID,
		runtimeContextTestBundleHashC,
	))
	if forkResp.Error == nil {
		t.Fatal("run.fork unloaded target error = nil")
	}
	forkData := asMap(t, forkResp.Error.Data)
	forkDetails := asMap(t, forkData["details"])
	if forkData["code"] != BundleUnavailableCode || forkDetails["cause"] != "runtime_context_not_loaded" {
		t.Fatalf("run.fork unloaded target error data = %#v", forkData)
	}
	if executor.calls != 0 {
		t.Fatalf("run.fork executor calls = %d, want 0 for unloaded target", executor.calls)
	}
}

func TestOperatorRuntimeContextManagerFailsClosedForDeactivatedBundle(t *testing.T) {
	fixture := newOperatorRuntimeContextFixture(t)
	result := fixture.manager.DeactivateBundleHash(runtimeContextTestBundleHashB, swruntime.RuntimeContextCauseUnloaded)
	if !result.Found || !result.Changed {
		t.Fatalf("DeactivateBundleHash result = %#v, want changed", result)
	}
	handler := fixture.handler(t)

	explicit := rpcCall(t, handler, eventPublishBodyWithBundleHash("", runtimeContextTestBundleHashB, "triage.requested", `{"topic":"deactivated"}`, "", "idem-deactivated-context"))
	if explicit.Error == nil {
		t.Fatal("event.publish deactivated explicit hash error = nil")
	}
	data := asMap(t, explicit.Error.Data)
	details := asMap(t, data["details"])
	if data["code"] != BundleUnavailableCode || details["cause"] != swruntime.RuntimeContextCauseUnloaded {
		t.Fatalf("event.publish deactivated explicit hash error data = %#v", data)
	}
	if got := countAllRunRows(t, fixture.db); got != 0 {
		t.Fatalf("run rows after explicit deactivated bundle = %d, want 0", got)
	}

	runID := uuid.NewString()
	seedRuntimeContextRunBundle(t, fixture.pg, runID, runtimeContextTestBundleHashB, storerunlifecycle.BundleSourcePersisted)
	existing := rpcCall(t, handler, eventPublishBodyWithoutBundle(runID, "triage.requested", `{"topic":"existing-deactivated"}`, "", "idem-existing-deactivated-context"))
	if existing.Error == nil {
		t.Fatal("event.publish deactivated run context error = nil")
	}
	data = asMap(t, existing.Error.Data)
	details = asMap(t, data["details"])
	if data["code"] != BundleUnavailableCode || details["cause"] != swruntime.RuntimeContextCauseUnloaded {
		t.Fatalf("event.publish deactivated run context error data = %#v", data)
	}
	if got := countEventRowsByRunID(t, fixture.db, runID); got != 0 {
		t.Fatalf("event rows for deactivated existing run = %d, want 0", got)
	}

	executor := &recordingRunForkExecutor{}
	forkHandler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			Now:                 func() time.Time { return time.Unix(1700000000, 0).UTC() },
			RunForkAvailability: &recordingRunForkAvailability{rows: map[string]runbundle.Availability{runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, runStartTestBundleHash)}},
			RunFork:             executor,
			Idempotency:         newMutatingProbeIdempotencyStore(),
			RuntimeContexts:     fixture.manager,
		}),
	})
	forkResp := rpcCall(t, forkHandler, fmt.Sprintf(
		`{"jsonrpc":"2.0","id":"fork","method":"run.fork","params":{"source_run_id":%q,"bundle_hash":%q,"confirm_source_freeze":true,"idempotency_key":"fork-deactivated-context"}}`,
		runForkTestSourceRunID,
		runtimeContextTestBundleHashB,
	))
	if forkResp.Error == nil {
		t.Fatal("run.fork deactivated target error = nil")
	}
	forkData := asMap(t, forkResp.Error.Data)
	forkDetails := asMap(t, forkData["details"])
	if forkData["code"] != BundleUnavailableCode || forkDetails["cause"] != swruntime.RuntimeContextCauseUnloaded {
		t.Fatalf("run.fork deactivated target error data = %#v", forkData)
	}
	if executor.calls != 0 {
		t.Fatalf("run.fork executor calls = %d, want 0 for deactivated target", executor.calls)
	}
}

func TestRunForkExecutorForBundleContextRebindsSelectedContractSelection(t *testing.T) {
	primarySource := semanticview.Wrap(runStartTestBundle("scan.requested"))
	targetBundle := runStartTestBundle("triage.requested")
	targetBundle.Semantics.Name = "target-review"
	targetBundle.Semantics.Version = "2.0.0"
	targetSource := semanticview.Wrap(targetBundle)
	executor := SelectedContractRunForkExecutor{
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            runfork.RunForkContractSelectionModeSelectedContracts,
			ContractsRoot:   "/tmp/primary-contracts",
			WorkflowName:    primarySource.WorkflowName(),
			WorkflowVersion: primarySource.WorkflowVersion(),
		},
	}

	module := newRunCompletionSystemNodeModule(t, targetSource)
	sourceFact := runtimeContextTestSourceFact(runtimeContextTestBundleHashB)
	effectiveIdentity, err := scenarioexecution.NewEffectiveSourceIdentity(sourceFact, "sha256:"+strings.Repeat("b", 64))
	if err != nil {
		t.Fatalf("create target effective source identity: %v", err)
	}
	selectedRuntime := &swruntime.Runtime{Options: swruntime.RuntimeOptions{WorkflowModule: module}}
	rebound, err := executor.SelectRunForkExecutor(&swruntime.BundleContext{
		Source: targetSource, ContractsRoot: "/tmp/target-contracts",
		BundleSourceFact: sourceFact, EffectiveSourceIdentity: effectiveIdentity,
	}, selectedRuntime)
	if err != nil {
		t.Fatalf("select run fork executor: %v", err)
	}

	selected, ok := rebound.(SelectedContractRunForkExecutor)
	if !ok {
		t.Fatalf("rebound executor type = %T, want SelectedContractRunForkExecutor", rebound)
	}
	if selected.ContractSelection.Mode != runfork.RunForkContractSelectionModeSelectedContracts ||
		selected.ContractSelection.ContractsRoot != "/tmp/target-contracts" ||
		selected.ContractSelection.WorkflowName != "target-review" ||
		selected.ContractSelection.WorkflowVersion != "2.0.0" {
		t.Fatalf("rebound contract selection = %#v", selected.ContractSelection)
	}
}

func TestOperatorRuntimeContextManagerFailsClosedForAmbiguousRuntimeConsumers(t *testing.T) {
	fixture := newOperatorRuntimeContextFixture(t)
	ingress := &recordingRuntimeIngress{}
	runtimeHandler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			Now:             func() time.Time { return time.Unix(1700000000, 0).UTC() },
			RuntimeIngress:  ingress,
			Idempotency:     newMutatingProbeIdempotencyStore(),
			RuntimeContexts: fixture.manager,
		}),
	})
	for _, method := range []string{"runtime.pause", "runtime.resume"} {
		runtimeResp := rpcCall(t, runtimeHandler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"runtime-control","method":%q,"params":{"idempotency_key":%q}}`, method, "idem-"+method))
		if runtimeResp.Error == nil {
			t.Fatalf("%s error = nil", method)
		}
		if data := asMap(t, runtimeResp.Error.Data); data["code"] != BundleScopeRequiredCode {
			t.Fatalf("%s error data = %#v, want %s", method, data, BundleScopeRequiredCode)
		}
	}
	if ingress.called {
		t.Fatal("runtime control called singleton ingress in multi-context mode")
	}

	executor := &recordingRunForkExecutor{
		result: RunForkExecutionResult{
			Owner:              "runtime.run_fork.selected_contract_execution",
			SourceRunID:        runForkTestSourceRunID,
			SourceRunStatus:    runfork.RunForkSourceFrozenStatus,
			SourceFrozen:       true,
			ForkRunID:          runForkTestForkRunID,
			ForkEventID:        runForkTestEventID,
			ForkRunStatus:      "running",
			ExecutedEventCount: 1,
		},
	}
	forkHandler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			Now:                 func() time.Time { return time.Unix(1700000000, 0).UTC() },
			RunForkAvailability: &recordingRunForkAvailability{rows: map[string]runbundle.Availability{runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, runStartTestBundleHash)}},
			RunFork:             executor,
			Idempotency:         newMutatingProbeIdempotencyStore(),
			RuntimeContexts:     fixture.manager,
		}),
	})
	forkResp := rpcCall(t, forkHandler, fmt.Sprintf(
		`{"jsonrpc":"2.0","id":"fork","method":"run.fork","params":{"source_run_id":%q,"bundle_hash":%q,"confirm_source_freeze":true,"idempotency_key":"fork-context"}}`,
		runForkTestSourceRunID,
		runtimeContextTestBundleHashB,
	))
	if forkResp.Error != nil {
		t.Fatalf("run.fork error = %#v", forkResp.Error)
	}
	if executor.calls != 1 {
		t.Fatalf("run.fork executor calls = %d, want 1", executor.calls)
	}
	if executor.last.BundleHash != runtimeContextTestBundleHashB ||
		!executor.last.ConfirmSourceFreeze ||
		executor.last.ContractSelection.Mode != runfork.RunForkContractSelectionModeBundleHash ||
		executor.last.ContractSelection.BundleHash != runtimeContextTestBundleHashB {
		t.Fatalf("run.fork executor request = %#v", executor.last)
	}

	agentControl := &fakeAgentControlController{}
	agentHandler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			Now:             func() time.Time { return time.Unix(1700000000, 0).UTC() },
			AgentControl:    agentControl,
			Idempotency:     newMutatingProbeIdempotencyStore(),
			RuntimeContexts: fixture.manager,
		}),
	})
	resp := rpcCall(t, agentHandler, agentControlBody("agent.restart", "agent-1", "idem-agent.restart"))
	if resp.Error == nil {
		t.Fatal("agent.restart error = nil")
	}
	if data := asMap(t, resp.Error.Data); data["code"] != BundleScopeRequiredCode {
		t.Fatalf("agent.restart error data = %#v, want %s", data, BundleScopeRequiredCode)
	}
	if agentControl.restartCalls != 0 {
		t.Fatalf("agent singleton restart calls = %d, want 0", agentControl.restartCalls)
	}

}

type operatorRuntimeContextFixture struct {
	db      *sql.DB
	pg      *store.PostgresStore
	sourceA semanticview.Source
	sourceB semanticview.Source
	busA    *runtimebus.EventBus
	busB    *runtimebus.EventBus
	ownerA  *worklifetime.RuntimeOccurrence
	ownerB  *worklifetime.RuntimeOccurrence
	manager *swruntime.RuntimeContextManager
}

func newOperatorRuntimeContextFixture(t *testing.T) operatorRuntimeContextFixture {
	t.Helper()
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	ctx := testAuthorActivityContext(context.Background())
	seedOperatorBundleDeleteBundle(t, ctx, db, runStartTestBundleHash)
	seedOperatorBundleDeleteBundle(t, ctx, db, runtimeContextTestBundleHashB)
	seedActiveAPIV1RuntimeBusAgent(t, ctx, pg, "scan-orchestrator")

	sourceA := semanticview.Wrap(runStartTestBundle("scan.requested"))
	sourceB := semanticview.Wrap(runStartTestBundle("triage.requested"))
	busA, ownerA := newRuntimeContextTestBus(t, pg, sourceA, runStartTestBundleHash)
	busB, ownerB := newRuntimeContextTestBus(t, pg, sourceB, runtimeContextTestBundleHashB)
	manager, err := swruntime.NewRuntimeContextManager(pg,
		swruntime.BundleContext{
			BundleSourceFact: runtimeContextTestSourceFact(runStartTestBundleHash),
			BundleIdentity:   runtimecontracts.BundleIdentity{WorkflowName: "review", WorkflowVersion: "1.0.0"},
			Source:           sourceA,
			Runtime:          runtimeContextTestRuntime(&swruntime.Runtime{Bus: busA}, runStartTestBundleHash),
			WorkOwner:        ownerA,
		},
		swruntime.BundleContext{
			BundleSourceFact: runtimeContextTestSourceFact(runtimeContextTestBundleHashB),
			BundleIdentity:   runtimecontracts.BundleIdentity{WorkflowName: "review", WorkflowVersion: "1.0.0"},
			Source:           sourceB,
			Runtime:          runtimeContextTestRuntime(&swruntime.Runtime{Bus: busB}, runtimeContextTestBundleHashB),
			WorkOwner:        ownerB,
		},
	)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	return operatorRuntimeContextFixture{
		db:      db,
		pg:      pg,
		sourceA: sourceA,
		sourceB: sourceB,
		busA:    busA,
		busB:    busB,
		ownerA:  ownerA,
		ownerB:  ownerB,
		manager: manager,
	}
}

func (f operatorRuntimeContextFixture) handler(t *testing.T) *Handler {
	t.Helper()
	return testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: testOperatorHandlers(testOperatorCapabilities{
			Now:                func() time.Time { return time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC) },
			Ready:              func() bool { return true },
			Database:           fakePinger{},
			Runs:               f.pg,
			Observability:      f.pg,
			AgentConversations: f.pg,
			Idempotency:        f.pg,
			Events:             f.busA,
			Source:             f.sourceA,
			RunBundleContext:   f.pg,
			RuntimeContexts:    f.manager,
			Bundle: runtimecontracts.BundleIdentity{
				WorkflowName:    "review",
				WorkflowVersion: "1.0.0",
				BundleHash:      runStartTestBundleHash,
			},
		}),
	})
}

func seedRuntimeContextRunBundle(t *testing.T, pg *store.PostgresStore, runID, bundleHash, bundleSource string) {
	t.Helper()
	storetest.RequireRun(t, context.Background(), pg, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(),
		RunID:        runID,
		BundleHash:   strings.TrimSpace(bundleHash),
		BundleSource: strings.TrimSpace(bundleSource),
	})
}

func runtimeContextManagerWithRuntimes(t *testing.T, fixture operatorRuntimeContextFixture, runtimeA, runtimeB *swruntime.Runtime) *swruntime.RuntimeContextManager {
	t.Helper()
	runtimeA = runtimeContextTestRuntime(runtimeA, runStartTestBundleHash)
	runtimeB = runtimeContextTestRuntime(runtimeB, runtimeContextTestBundleHashB)
	manager, err := swruntime.NewRuntimeContextManager(fixture.pg,
		swruntime.BundleContext{
			BundleSourceFact: runtimeContextTestSourceFact(runStartTestBundleHash),
			BundleIdentity:   runtimecontracts.BundleIdentity{WorkflowName: "review", WorkflowVersion: "1.0.0"},
			Source:           fixture.sourceA,
			Runtime:          runtimeA,
			WorkOwner:        fixture.ownerA,
		},
		swruntime.BundleContext{
			BundleSourceFact: runtimeContextTestSourceFact(runtimeContextTestBundleHashB),
			BundleIdentity:   runtimecontracts.BundleIdentity{WorkflowName: "review", WorkflowVersion: "1.0.0"},
			Source:           fixture.sourceB,
			Runtime:          runtimeB,
			WorkOwner:        fixture.ownerB,
		},
	)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager with runtimes: %v", err)
	}
	return manager
}

func runtimeContextTestAgentManager(t *testing.T, pg *store.PostgresStore, bus *runtimebus.EventBus, workOwner *worklifetime.RuntimeOccurrence, agent *directiveIntegrationAgent, fact runtimecorrelation.BundleSourceFact) *runtimemanager.AgentManager {
	t.Helper()
	manager := runtimemanager.NewAgentManagerWithOptions(bus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return agent, nil
	}, runtimemanager.AgentManagerOptions{
		ExecutionPosture: executionposture.Live,
		BaseContext:      testAuthorActivityContextForSource(context.Background(), fact),
		WorkOwner:        workOwner,
		PersistenceRoles: runtimemanager.PersistenceRoles{
			DirectiveOperations: pg,
			DirectiveTargets:    pg,
		}, ReceiverExecution: eventreceiver.NormalExecution(),
	}, pg)
	t.Cleanup(func() {
		if err := manager.Shutdown(); err != nil {
			t.Errorf("shutdown runtime context agent manager: %v", err)
		}
	})
	materializeAPITestAgent(t, testAuthorActivityContext(context.Background()), pg, manager, withAPITestIntent(t, runtimeactors.AgentConfig{ExecutionMode: "live", ID: agent.id, Model: "regular", ResolvedLLMBackend: "anthropic"}))
	return manager
}

func runtimeContextTestRuntime(rt *swruntime.Runtime, bundleHash string) *swruntime.Runtime {
	if rt == nil {
		return nil
	}
	rt.ExecutionPosture = executionposture.Live
	rt.Options.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
	rt.Options.BundleSourceFact = runtimeContextTestSourceFact(bundleHash)
	return rt
}

func eventPublishExistingRunBody(runID, bundleHash, idempotencyKey string) string {
	parts := []string{
		fmt.Sprintf(`"run_id":%q`, runID),
		`"event_name":"triage.requested"`,
		`"payload":{"topic":"blocked"}`,
		fmt.Sprintf(`"idempotency_key":%q`, idempotencyKey),
	}
	if strings.TrimSpace(bundleHash) != "" {
		parts = append(parts, fmt.Sprintf(`"bundle_hash":%q`, bundleHash))
	}
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":"publish","method":"event.publish","params":{%s}}`, strings.Join(parts, ","))
}

func assertRuntimeContextBundleError(t *testing.T, resp rpcResponse, method, wantCode, wantCause string) {
	t.Helper()
	if resp.Error == nil {
		t.Fatalf("%s error = nil, want %s", method, wantCode)
	}
	data := asMap(t, resp.Error.Data)
	if data["code"] != wantCode {
		t.Fatalf("%s error data = %#v, want code %s", method, data, wantCode)
	}
	if strings.TrimSpace(wantCause) == "" {
		return
	}
	details := asMap(t, data["details"])
	if details["cause"] != wantCause {
		t.Fatalf("%s error details = %#v, want cause %s", method, details, wantCause)
	}
}

type recordingRuntimeContextRunControlStore struct {
	stopCalls     int
	pauseCalls    int
	continueCalls int
}

func (s *recordingRuntimeContextRunControlStore) StopRunControl(_ context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	s.stopCalls++
	return runtimeruncontrol.State{RunID: req.RunID, Status: runtimeruncontrol.StatusCancelled, ControlStatus: runtimeruncontrol.StatusStopped}, nil
}

func (s *recordingRuntimeContextRunControlStore) PauseRunControl(_ context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	s.pauseCalls++
	return runtimeruncontrol.State{RunID: req.RunID, Status: runtimeruncontrol.StatusPaused, ControlStatus: runtimeruncontrol.StatusPaused}, nil
}

func (s *recordingRuntimeContextRunControlStore) ContinueRunControl(_ context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	s.continueCalls++
	return runtimeruncontrol.State{RunID: req.RunID, Status: runtimeruncontrol.StatusRunning, ControlStatus: runtimeruncontrol.StatusRunning}, nil
}

func (*recordingRuntimeContextRunControlStore) RunDispatchBlocked(context.Context, string) (bool, error) {
	return false, nil
}

func (s *recordingRuntimeContextRunControlStore) totalCalls() int {
	return s.stopCalls + s.pauseCalls + s.continueCalls
}

func newRuntimeContextTestBus(t *testing.T, pg *store.PostgresStore, source semanticview.Source, bundleHash string) (*runtimebus.EventBus, *worklifetime.RuntimeOccurrence) {
	t.Helper()
	workOwner := newAPITestRuntimeWorkOccurrence(t, authorActivityTestRuntimeInstanceID, bundleHash)
	bus, err := newScopedAPITestEventBus(t, pg, runtimebus.EventBusOptions{
		ContractBundle:   source,
		BundleSourceFact: runtimeContextTestSourceFact(bundleHash),
		WorkOwner:        workOwner,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	return bus, workOwner
}

func runtimeContextTestSourceFact(bundleHash string) runtimecorrelation.BundleSourceFact {
	return mustAPITestPersistedBundleSourceFact(strings.TrimSpace(bundleHash))
}

type recordingRuntimeIngress struct {
	called bool
}

func (r *recordingRuntimeIngress) Pause(context.Context, runtimeingress.TransitionRequest) (runtimeingress.TransitionResult, error) {
	r.called = true
	return runtimeingress.TransitionResult{Status: runtimeingress.StatusPaused}, nil
}

func (r *recordingRuntimeIngress) Resume(context.Context, runtimeingress.TransitionRequest) (runtimeingress.TransitionResult, error) {
	r.called = true
	return runtimeingress.TransitionResult{Status: runtimeingress.StatusRunning}, nil
}
