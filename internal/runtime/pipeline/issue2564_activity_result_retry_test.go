package pipeline_test

import (
	"context"
	"database/sql"
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
			owner := &issue2564M25CASOwner{WorkflowPersistenceOwner: selected.events.(pipeline.WorkflowPersistenceOwner), db: selected.db, t: t, success: resultEvents.SuccessEvent, source: fact}
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
			var raw []byte
			var entityID string
			if err := selected.db.QueryRow(`SELECT entity_id,fields FROM entity_state WHERE run_id=$1`, runID).Scan(&entityID, &raw); err != nil {
				t.Fatal(err)
			}
			var fields map[string]any
			if err := json.Unmarshal(raw, &fields); err != nil || fields["local"] != float64(40) || fields["folded"] != float64(41) || fields["received"] != float64(1) || fields["title"] != "M25 external" {
				t.Fatalf("result consumer lost local/result effects: %s %v", raw, err)
			}
			var requestID string
			if err := selected.db.QueryRow(`SELECT request_event_id FROM activity_attempts WHERE run_id=$1`, runID).Scan(&requestID); err != nil {
				t.Fatal(err)
			}
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
			var after []byte
			if err := selected.db.QueryRow(`SELECT fields FROM entity_state WHERE run_id=$1 AND entity_id=$2`, runID, entityID).Scan(&after); err != nil || string(after) != string(raw) {
				t.Fatalf("M25 duplicate result repeated field fold: %s %v", after, err)
			}
			issue2564M25AssertReceipt(t, selected, runID, eventID, claim)
			t.Logf("M25 store=%s HTTP=1 journal=1 result=1 native_CAS_losses=1 result_attempts=2 delivery_claim=%s/%d retry_count=0 folded=41 received=1 source=%s", backend.name, claim.DeliveryID(), claim.Version(), fact.BundleHash())
		})
	}
}

type issue2564M25CASOwner struct {
	pipeline.WorkflowPersistenceOwner
	db                                 *sql.DB
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
		var journaled int
		if err := o.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM activity_attempts WHERE run_id=$1 AND result_event_id=$2 AND status='succeeded'`, command.State.Identity.RunID, event.ID()).Scan(&journaled); err != nil || journaled != 1 {
			o.t.Fatalf("M25 result reached CAS before its durable journal: count=%d err=%v", journaled, err)
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
	var status string
	var retries, version int
	if err := selected.db.QueryRow(`SELECT status,retry_count,claim_version FROM event_deliveries WHERE delivery_id=$1 AND run_id=$2 AND event_id=$3`, claim.DeliveryID(), runID, resultID).Scan(&status, &retries, &version); err != nil || status != "delivered" || retries != 0 || int64(version) != claim.Version() || version != 1 {
		t.Fatalf("M25 result delivery not same successful attempt: status=%s retries=%d version=%d err=%v", status, retries, version, err)
	}
	for query, want := range map[string]int{
		`SELECT COUNT(*) FROM events WHERE event_id=$1`: 1,
		`SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries d ON d.delivery_id=a.delivery_id WHERE d.event_id=$1`:                                                                              1,
		`SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries d ON d.delivery_id=a.delivery_id WHERE d.event_id=$1 AND a.claim_version=1 AND a.closure_kind='settled' AND a.outcome='delivered'`: 1,
		`SELECT COUNT(*) FROM event_delivery_handler_rule_selections s JOIN event_deliveries d ON d.delivery_id=s.delivery_id WHERE d.event_id=$1`:                                                               1,
		`SELECT COUNT(*) FROM dead_letters WHERE original_event_id=$1`:                                                                                                                                           0,
		`SELECT COUNT(*) FROM entity_mutations WHERE caused_by_event=$1 AND domain='authored_field' AND path='received'`:                                                                                         1,
		`SELECT COUNT(*) FROM entity_mutations WHERE caused_by_event=$1 AND domain='authored_field' AND path='local'`:                                                                                            1,
	} {
		var count int
		if err := selected.db.QueryRow(query, resultID).Scan(&count); err != nil || count != want {
			t.Fatalf("M25 exact retained effect count=%d want=%d err=%v query=%s", count, want, err, query)
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
