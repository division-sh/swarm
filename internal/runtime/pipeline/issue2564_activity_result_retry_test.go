package pipeline_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// M25: an actual HTTP activity journals its result before an ordinary authored
// result consumer loses a real backend CAS, then folds against the fresh state.
// This is not the boring fixture's unconsumed activity-result publication proof.
func TestIssue2564ActivityResultHandlerNativeCASRetryBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != http.MethodPost || r.URL.Query().Get("url") != "https://example.com/m25" {
					t.Errorf("activity HTTP input changed: %s %s", r.Method, r.URL)
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"title": "M25 external"})
			}))
			t.Cleanup(server.Close)
			site := contracts.ActivitySite{Node: externalPipelineNode(t, ".", "scanner"), HandlerEventKey: "source.requested", RuleIndex: -1, Spec: contracts.ActivitySpec{Tool: "source_scrape"}}
			resultEvents := contracts.ActivityResultEventsForSite(site)
			bundle := issue2564M25Bundle(t, server.URL, resultEvents.SuccessEvent)
			source := semanticview.Wrap(bundle)
			generated := bundle.GeneratedActivityEventSchemas()[resultEvents.SuccessEvent].Schema
			properties, _ := generated["properties"].(map[string]any)
			output, _ := properties["result"].(map[string]any)
			outputProperties, _ := output["properties"].(map[string]any)
			if outputProperties["title"] == nil {
				t.Fatalf("generated activity result omitted authored payload.result.title: %#v", generated)
			}
			fact, err := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
			if err != nil {
				t.Fatal(err)
			}
			runID := uuid.NewString()
			ctx := withLiveGateExecution(correlation.WithRunID(testAuthorActivityContextForSource(t, context.Background(), fact), runID))
			selected := backend.open(t)
			storetest.RequireBundleDataCatalog(t, ctx, selected.events.(storetest.DurableDataCatalogStore), bundle)
			if _, err := selected.events.CreateRun(ctx, runlifecycle.CreateRequest{RunID: runID, Origin: runlifecycle.ScenarioSetupRunOrigin(), Source: fact, StartedAt: time.Now().UTC()}); err != nil {
				t.Fatal(err)
			}
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
				ContractBundle: source, SourceArtifactFact: fact, WorkOwner: pipelineExternalTestWorkOwnerForSource(t, fact),
			})
			if err != nil {
				t.Fatal(err)
			}
			owner := &issue2564M25CASOwner{WorkflowPersistenceOwner: selected.events.(pipeline.WorkflowPersistenceOwner), events: selected.events.(operatorread.ObservabilityReader), t: t, success: resultEvents.SuccessEvent, source: fact}
			selected.persistence = pipeline.NewWorkflowPersistence(owner)
			module := proposedEffectProofModule{source: source, nodes: []pipeline.WorkflowNode{
				{Node: externalPipelineSourceNode(t, source, ".", "scanner"), Subscriptions: []events.EventType{"source.requested"}, Produces: []events.EventType{events.EventType(resultEvents.SuccessEvent), events.EventType(resultEvents.FailureEvent)}, ExecutionType: contracts.SystemNodeExecutionType},
				{Node: externalPipelineSourceNode(t, source, ".", "consumer"), Subscriptions: []events.EventType{events.EventType(resultEvents.SuccessEvent)}, ExecutionType: contracts.SystemNodeExecutionType},
			}}
			pc := newGateRecoveryCoordinator(bus, selected, pipeline.PipelineCoordinatorOptions{Module: module})
			commitKeylessConstructorComponent(t, ctx, selected, pc, source)
			bus.SetInterceptors(pc)
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "source.requested", "operator", "", []byte(`{"url":"https://example.com/m25"}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
			if err := bus.PublishAcknowledged(ctx, event); err != nil {
				t.Fatal(err)
			}
			wait, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := bus.WaitForQuiescence(wait); err != nil {
				t.Fatal(err)
			}
			owner.mu.Lock()
			attempts, losses, rival, committed, claim, eventID, drafts := owner.attempts, owner.losses, owner.rival, owner.committed, owner.claim, owner.resultID, append([]float64(nil), owner.drafts...)
			owner.mu.Unlock()
			if calls.Load() != 1 || attempts != 2 || losses != 1 || rival != 1 || committed != 1 || !reflect.DeepEqual(drafts, []float64{1, 41}) {
				t.Fatalf("M25 actual HTTP/CAS/fresh fold: calls=%d attempts=%d losses=%d rival=%d committed=%d drafts=%v", calls.Load(), attempts, losses, rival, committed, drafts)
			}
			entityID := runID
			state, found, err := owner.LoadWorkflowEntityState(ctx, testRunScopedWorkflowInstanceForRun(runID, runID), identity.NormalizeEntityID(entityID))
			if err != nil || !found {
				t.Fatalf("load actual M25 entity: found=%t err=%v", found, err)
			}
			raw := state.Fields
			var fields map[string]any
			if err := json.Unmarshal(raw, &fields); err != nil || fields["local"] != float64(40) || fields["folded"] != float64(41) || fields["received"] != float64(1) || fields["title"] != "M25 external" {
				t.Fatalf("result consumer lost local/result effects: %s %v", raw, err)
			}
			requestID := owner.requestEventID(ctx, runID)
			journal, found, err := activityReplayJournal(selected).LoadActivityAttempt(ctx, requestID)
			if err != nil || !found || journal.Status != pipeline.ActivityAttemptStatusSucceeded || journal.Attempt != 1 || journal.SourceEventID != event.ID() || journal.ResultEventID != eventID || journal.CompletedAt == nil {
				t.Fatalf("actual result journal: found=%t journal=%+v err=%v", found, journal, err)
			}
			result := loadActivityResultForProof(t, ctx, selected, eventID)
			var payload map[string]any
			journalJSON, journalErr := json.Marshal(journal.ResultPayload)
			decodeErr := json.Unmarshal(result.Event.Event().Payload(), &payload)
			payloadJSON, payloadErr := json.Marshal(payload)
			if decodeErr != nil || journalErr != nil || payloadErr != nil || string(payloadJSON) != string(journalJSON) || payload["activity_id"] != resultEvents.ActivityID || payload["tool"] != "source_scrape" {
				t.Fatalf("generated publication/journal payload mismatch: event=%s journal=%s errors=%v/%v/%v", payloadJSON, journalJSON, decodeErr, journalErr, payloadErr)
			}
			issue2564M25AssertReceipt(t, selected, runID, eventID, claim)
			// Replaying the original request and retained result must not redispatch
			// the provider or consume the ordinary result delivery a second time.
			request := loadActivityResultForProof(t, ctx, selected, requestID).Event.Event()
			for _, duplicate := range []events.Event{request, result.Event.Event()} {
				if err := bus.PublishAcknowledged(ctx, duplicate); err != nil {
					t.Fatal(err)
				}
			}
			if err := bus.WaitForQuiescence(wait); err != nil {
				t.Fatal(err)
			}
			stored, found, err := activityReplayJournal(selected).LoadActivityAttempt(ctx, requestID)
			if err != nil || !found || !reflect.DeepEqual(journal, stored) || calls.Load() != 1 || !reflect.DeepEqual(result, loadActivityResultForProof(t, ctx, selected, eventID)) {
				t.Fatal("M25 replay changed the retained result or repeated HTTP")
			}
			after, found, err := owner.LoadWorkflowEntityState(ctx, testRunScopedWorkflowInstanceForRun(runID, runID), identity.NormalizeEntityID(entityID))
			if err != nil || !found || string(after.Fields) != string(raw) {
				t.Fatalf("M25 duplicate result repeated field fold: %s found=%t err=%v", after.Fields, found, err)
			}
			issue2564M25AssertReceipt(t, selected, runID, eventID, claim)
			t.Logf("M25 store=%s HTTP=1 journal=1 result=1 native_CAS_losses=1 result_attempts=2 delivery_claim=%s/%d retry_count=0 folded=41 received=1 source=%s", backend.name, claim.DeliveryID(), claim.Version(), fact.BundleHash())
		})
	}
}

type issue2564M25CASOwner struct {
	pipeline.WorkflowPersistenceOwner
	events                             operatorread.ObservabilityReader
	t                                  *testing.T
	mu                                 sync.Mutex
	success                            string
	source                             correlation.SourceArtifactFact
	attempts, losses, rival, committed int
	claim                              deliverylifecycle.Claim
	selection                          deliverylifecycle.HandlerRuleSelectionFact
	resultID                           string
	drafts                             []float64
}

func (o *issue2564M25CASOwner) requestEventID(ctx context.Context, runID string) string {
	o.t.Helper()
	page, err := o.events.ListOperatorEvents(ctx, operatorread.OperatorEventListOptions{
		Filter: operatorread.OperatorEventListFilter{RunID: runID, EventName: "platform.activity_requested"}, Limit: 2,
	})
	if err != nil || len(page.Events) != 1 || page.NextCursor != "" {
		o.t.Fatalf("M25 exact durable activity request: count=%d cursor=%s err=%v", len(page.Events), page.NextCursor, err)
	}
	return page.Events[0].EventID
}

func (o *issue2564M25CASOwner) CommitWorkflowEngineMutation(ctx context.Context, command pipeline.WorkflowEngineMutationCommand) (pipeline.CommittedWorkflowEngineMutation, error) {
	event, found := correlation.InboundEventFromContext(ctx)
	if !found || string(event.Type()) != o.success || command.DeliverySuccess == nil {
		return o.WorkflowPersistenceOwner.CommitWorkflowEngineMutation(ctx, command)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.attempts++
	if source, ok := correlation.SourceArtifactFactFromContext(ctx); !ok || !source.Matches(o.source) {
		o.t.Fatal("M25 result retry lost the admitted source fact")
	}
	selection := command.DeliverySuccess.RuleSelection
	if o.attempts == 1 {
		o.claim, o.selection, o.resultID = command.DeliverySuccess.Claim, selection, event.ID()
	} else if !o.claim.Same(command.DeliverySuccess.Claim) || !selection.Equal(o.selection) || event.ID() != o.resultID {
		o.t.Fatal("M25 CAS retry changed exact claim, selected rule or retained result")
	}
	if command.DeliverySuccess.RuleSelection.DisplayLabel() != "consume_result" {
		o.t.Fatalf("M25 bypassed authored result selection: %+v", command.DeliverySuccess.RuleSelection)
	}
	var draft map[string]any
	if err := json.Unmarshal(command.State.Fields, &draft); err != nil {
		o.t.Fatal(err)
	}
	o.drafts = append(o.drafts, draft["folded"].(float64))
	if o.attempts == 1 {
		journal, found, err := o.LoadActivityAttempt(ctx, o.requestEventID(ctx, command.State.Identity.RunID))
		storage := storetest.ObserveActivityResultPublicationStorage(o.t, ctx, o.WorkflowPersistenceOwner)
		if err != nil || !found || journal.Status != pipeline.ActivityAttemptStatusSucceeded || journal.ResultEventID != event.ID() || journal.RunID != command.State.Identity.RunID || storage.SuccessfulActivityAttempts != 1 {
			o.t.Fatalf("M25 result reached CAS before its durable journal: count=%d found=%t journal=%+v err=%v", storage.SuccessfulActivityAttempts, found, journal, err)
		}
		current, err := o.WorkflowPersistenceOwner.LoadWorkflowTargetPersistence(ctx, command.State.Identity, identity.NormalizeEntityID(command.State.EntityID))
		if err != nil || int64(current.State.Revision) != command.State.ExpectedRevision {
			o.t.Fatalf("M25 command not anchored on result R1: revision=%d expected=%d err=%v", current.State.Revision, command.State.ExpectedRevision, err)
		}
		var fields map[string]any
		if err := json.Unmarshal(current.State.Fields, &fields); err != nil || fields["local"] != float64(0) || fields["received"] != float64(0) {
			o.t.Fatalf("M25 rival used uncommitted result draft: %s %v", current.State.Fields, err)
		}
		fields["local"] = 40
		literal := pipeline.WorkflowEngineMutationCommand{State: command.State}
		literal.State.Fields, _ = json.Marshal(fields)
		literal.State.Bookkeeping, literal.State.Gates, literal.State.Accumulator = current.State.Bookkeeping, current.State.Gates, current.State.Accumulator
		literal.State.UpdatedAt = time.Now().UTC().Truncate(time.Microsecond)
		done := make(chan struct{})
		var rival pipeline.CommittedWorkflowEngineMutation
		var rivalErr error
		go func() {
			defer close(done)
			rival, rivalErr = o.WorkflowPersistenceOwner.CommitWorkflowEngineMutation(ctx, literal)
		}()
		<-done
		if rivalErr != nil || !rival.Committed {
			o.t.Fatalf("M25 real competing canonical commit: ack=%t err=%v", rival.Committed, rivalErr)
		}
		o.rival++
	}
	result, err := o.WorkflowPersistenceOwner.CommitWorkflowEngineMutation(ctx, command)
	if o.attempts == 1 {
		if result.Committed || !failures.IsStateContention(err) {
			o.t.Fatalf("M25 expected native stale CAS, got ack=%t err=%v", result.Committed, err)
		}
		o.losses++
	} else if result.Committed {
		o.committed++
	}
	return result, err
}

func issue2564M25AssertReceipt(t *testing.T, selected gateRecoveryStoreCase, runID, resultID string, claim deliverylifecycle.Claim) {
	t.Helper()
	snapshot, err := selected.events.Snapshot(context.Background(), claim.DeliveryID())
	if err != nil || snapshot.RunID != runID || snapshot.EventID != resultID || snapshot.Status != deliverylifecycle.StatusDelivered || snapshot.RetryCount != 0 || snapshot.ClaimVersion != claim.Version() || snapshot.ClaimVersion != 1 {
		t.Fatalf("M25 result delivery not same successful attempt: snapshot=%+v err=%v", snapshot, err)
	}
	ctx := context.Background()
	delivery := storetest.ObserveDeliveryEventEvidence(t, ctx, selected.events, resultID)
	mutations := storetest.ObserveActivityResultMutations(t, ctx, selected.events, resultID)
	var attempts, delivered, selections int
	for _, row := range delivery.Deliveries {
		attempts += len(row.Attempts)
		selections += row.HandlerSelections
		for _, attempt := range row.Attempts {
			if attempt.ClaimVersion == 1 && attempt.ClosureKind == "settled" && attempt.Outcome == "delivered" {
				delivered++
			}
		}
	}
	for name, check := range map[string][2]int{
		"events":       {storetest.ObserveEventCardinality(t, ctx, selected.events, resultID), 1},
		"all attempts": {attempts, 1}, "delivered version-one attempts": {delivered, 1},
		"all selections": {selections, 1}, "dead letters": {delivery.DeadLetters, 0},
		"received mutations": {mutations.Received, 1}, "local mutations": {mutations.Local, 1},
	} {
		if check[0] != check[1] {
			t.Fatalf("M25 exact retained effect %s count=%d want=%d", name, check[0], check[1])
		}
	}
}

func issue2564M25Bundle(t *testing.T, serverURL, success string) *contracts.WorkflowContractBundle {
	t.Helper()
	return loadPipelineLifecycleFixtureBundle(t, map[string]string{
		"schema.yaml":   "name: issue2564-m25\nstages: {pending: {initial: true}}\n",
		"entities.yaml": "test_entity:\n  local: {type: integer, initial: 0}\n  folded: {type: integer, initial: 0}\n  received: {type: integer, initial: 0}\n  title: text\n",
		"events.yaml":   "source.requested: {url: text}\n",
		"nodes.yaml": fmt.Sprintf(`scanner:
  execution_type: system_node
  subscribes_to: [source.requested]
  event_handlers:
    source.requested:
      activity:
        tool: source_scrape
        input: {url: payload.url}
consumer:
  execution_type: system_node
  subscribes_to: [%s]
  event_handlers:
    %s:
      rules:
        - id: consume_result
          when: payload.result.title == 'M25 external'
          data_accumulation:
            writes:
              - {target_field: title, value: payload.result.title}
              - {target_field: folded, value: entity.local + 1}
              - {target_field: received, value: entity.received + 1}
        - id: unexpected_result
          else: true
          data_accumulation:
            writes:
              - {target_field: title, value: 'unexpected result'}
`, success, success),
		"tools.yaml": fmt.Sprintf(`source_scrape:
  handler_type: http
  effect_class: non_idempotent_write
  http:
    method: POST
    url: %q
  input_schema:
    type: object
    required: [url]
    properties:
      url: {type: string}
  output_schema:
    type: object
    required: [title]
    properties:
      title: {type: string}
`, strings.TrimRight(serverURL, "/")+"?url={{input.url}}"),
	})
}
