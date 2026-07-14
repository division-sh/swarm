package runforkexecution

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestExecuteSelectedContractRunForkExecutesOrReusesLoopActivityThroughRuntimeContainer(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()

	var connectorCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		connectorCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"value":"fork-result"}`))
	}))
	t.Cleanup(server.Close)

	tests := []selectedContractActivityForkCase{
		{
			name:               "read-only reexecutes exactly once",
			effectClass:        runtimecontracts.ActivityEffectClassReadOnly,
			forkPolicy:         runtimecontracts.ActivityForkReexecuteRead,
			resultEventType:    "activity.succeeded",
			wantConnectorCalls: 1,
		},
		{
			name:                  "succeeded write publishes recorded result",
			effectClass:           runtimecontracts.ActivityEffectClassNonIdempotentWrite,
			forkPolicy:            runtimecontracts.ActivityForkReuseRecordedResult,
			sourceAttemptStatus:   "succeeded",
			resultEventType:       "activity.succeeded",
			wantForkAttemptStatus: "succeeded",
		},
		{
			name:                  "failed write publishes recorded typed failure",
			effectClass:           runtimecontracts.ActivityEffectClassNonIdempotentWrite,
			forkPolicy:            runtimecontracts.ActivityForkReuseRecordedResult,
			sourceAttemptStatus:   "failed",
			resultEventType:       "activity.failed",
			failureClass:          string(runtimefailures.ClassDependencyUnavailable),
			failureCode:           "provider_unavailable",
			wantForkAttemptStatus: "failed",
		},
		{
			name:                  "uncertain write stays uncertain and publishes outcome-uncertain failure",
			effectClass:           runtimecontracts.ActivityEffectClassNonIdempotentWrite,
			forkPolicy:            runtimecontracts.ActivityForkReuseRecordedResult,
			sourceAttemptStatus:   "uncertain",
			resultEventType:       "activity.failed",
			failureClass:          string(runtimefailures.ClassOutcomeUncertain),
			failureCode:           "activity_provider_outcome_uncertain",
			wantForkAttemptStatus: "uncertain",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			beforeCalls := connectorCalls.Load()
			sourceRunID := uuid.NewString()
			entityID := uuid.NewString()
			initiatingEventID := uuid.NewString()
			at := time.Now().UTC().Truncate(time.Microsecond)
			activation, err := loopruntime.New(sourceRunID, entityID, "flow-a", "revision", "revision_id", uuid.NewString(), "review", 3, at.Add(-time.Minute))
			if err != nil {
				t.Fatalf("create source loop activation: %v", err)
			}
			sourceGeneration := activation.Generation()
			sourceFact := activityidentity.Fact{
				RunID: sourceRunID, SourceEventID: initiatingEventID, EntityID: entityID,
				FlowID: "flow-a", NodeID: "test-node", HandlerEventKey: "review.requested",
				ActivityID: "connector", Tool: "provider.connector", Attempt: 1,
				RevisionID: sourceGeneration.RevisionID,
			}
			sourceRequestEventID := activityidentity.RequestEventID(sourceFact)
			seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceRequestEventID, "platform.activity_requested", at)
			seedSelectedContractActivityLoop(t, db, sourceRunID, entityID, sourceRequestEventID, activation, at)
			seedSelectedContractActivityRequest(t, db, sourceRunID, sourceRequestEventID, selectedContractActivityRequestPayload{
				ActivityID: "connector", Tool: "provider.connector", Input: map[string]any{"value": "x"},
				EffectClass: string(tt.effectClass), SuccessEvent: "activity.succeeded", FailureEvent: "activity.failed",
				RetryMaxAttempts: 1, ForkPolicy: string(tt.forkPolicy), EntityID: entityID,
				NodeID: "test-node", FlowID: "flow-a", HandlerEventKey: "review.requested",
				SourceEventID: initiatingEventID, SourceRunID: sourceRunID, Attempt: 1,
				Generation: sourceGeneration, LoopStage: "review",
			})
			captureSelectedExecutionSourceRevision(t, db, sourceRunID)
			if tt.sourceAttemptStatus != "" {
				seedSelectedContractActivityAttempt(t, db, sourceFact, sourceGeneration, tt.sourceAttemptStatus, tt.resultEventType, tt.failureClass, tt.failureCode, at)
			}

			source := selectedContractActivitySource(server.URL, tt.effectClass)
			selection := store.RunForkContractSelection{
				Mode: "selected_contracts", ContractsRoot: "/tmp/selected-contract-activity-proof",
				WorkflowName: source.WorkflowName(), WorkflowVersion: source.WorkflowVersion(),
			}
			loader := &fakeSelectedContractSourceLoader{loaded: selectedContractActivityLoadedSource(source, selection)}
			result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
				SourceRunID: sourceRunID, At: sourceRequestEventID, Store: pg,
				SourceLoader: loader, ContractSelection: selection,
			})
			if err != nil {
				t.Fatalf("ExecuteSelectedContractRunFork: %v", err)
			}
			if got := connectorCalls.Load() - beforeCalls; got != tt.wantConnectorCalls {
				t.Fatalf("connector calls = %d, want %d", got, tt.wantConnectorCalls)
			}
			if result.ExecutedEventCount != 1 || len(result.ForkEvents) != 1 {
				t.Fatalf("selected execution result = %#v", result)
			}

			forkRequest := loadSelectedContractActivityRequest(t, db, result.Materialization.ForkRunID, result.ForkEvents[0].ForkEventID)
			if forkRequest.SourceRunID != result.Materialization.ForkRunID || forkRequest.SourceEventID != result.ForkEvents[0].ForkEventID || !forkRequest.Generation.Valid() || forkRequest.Generation.RevisionID == sourceGeneration.RevisionID {
				t.Fatalf("fork request identity = %#v, source generation = %#v", forkRequest, sourceGeneration)
			}
			forkFact := activityidentity.Fact{
				RunID: result.Materialization.ForkRunID, SourceEventID: forkRequest.SourceEventID,
				ParentEventID: forkRequest.ParentEventID, EntityID: entityID, FlowID: "flow-a",
				NodeID: "test-node", HandlerEventKey: "review.requested", ActivityID: "connector",
				Tool: "provider.connector", Attempt: 1, RevisionID: forkRequest.Generation.RevisionID,
			}
			forkRequestEventID := activityidentity.RequestEventID(forkFact)
			forkResultEventID := activityidentity.ResultEventID(forkFact, tt.resultEventType)
			if forkRequestEventID == sourceRequestEventID || forkResultEventID == activityidentity.ResultEventID(sourceFact, tt.resultEventType) {
				t.Fatalf("fork activity reused source identity: request=%s result=%s", forkRequestEventID, forkResultEventID)
			}

			var forkAttemptCount int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM activity_attempts WHERE request_event_id = $1::uuid`, forkRequestEventID).Scan(&forkAttemptCount); err != nil {
				t.Fatalf("count fork attempts: %v", err)
			}
			if tt.wantForkAttemptStatus == "" {
				if forkAttemptCount != 0 {
					t.Fatalf("read-only fork copied or journaled %d activity attempts, want 0", forkAttemptCount)
				}
			} else {
				if forkAttemptCount != 1 {
					t.Fatalf("fork activity attempts = %d, want 1", forkAttemptCount)
				}
				assertSelectedContractForkActivityAttempt(t, db, forkRequestEventID, result.Materialization.ForkRunID, forkResultEventID, forkRequest.Generation, tt)
			}

			published := loadSelectedContractActivityResult(t, db, result.Materialization.ForkRunID, forkResultEventID, tt.resultEventType)
			if published["revision_id"] != forkRequest.Generation.RevisionID {
				t.Fatalf("published revision_id = %#v, want %s", published["revision_id"], forkRequest.Generation.RevisionID)
			}
			if tt.failureClass != "" {
				failure, _ := published["failure"].(map[string]any)
				detail, _ := failure["detail"].(map[string]any)
				if failure["class"] != tt.failureClass || detail["code"] != tt.failureCode {
					t.Fatalf("published failure = %#v, want class=%s code=%s", failure, tt.failureClass, tt.failureCode)
				}
			}
		})
	}
}

type selectedContractActivityForkCase struct {
	name                  string
	effectClass           runtimecontracts.ActivityEffectClass
	forkPolicy            runtimecontracts.ActivityForkPolicy
	sourceAttemptStatus   string
	resultEventType       string
	failureClass          string
	failureCode           string
	wantConnectorCalls    int64
	wantForkAttemptStatus string
}

type selectedContractActivityRequestPayload struct {
	ActivityID       string                       `json:"activity_id"`
	Tool             string                       `json:"tool"`
	Input            map[string]any               `json:"input"`
	EffectClass      string                       `json:"effect_class"`
	SuccessEvent     string                       `json:"success_event"`
	FailureEvent     string                       `json:"failure_event"`
	RetryMaxAttempts int                          `json:"retry_max_attempts"`
	RetryBackoff     string                       `json:"retry_backoff"`
	ForkPolicy       string                       `json:"fork_policy"`
	EntityID         string                       `json:"entity_id"`
	NodeID           string                       `json:"node_id"`
	FlowID           string                       `json:"flow_id"`
	HandlerEventKey  string                       `json:"handler_event_key"`
	SourceEventID    string                       `json:"source_event_id"`
	SourceRunID      string                       `json:"source_run_id"`
	SourceTaskID     string                       `json:"source_task_id"`
	ParentEventID    string                       `json:"parent_event_id"`
	ChainDepth       int                          `json:"chain_depth"`
	Attempt          int                          `json:"attempt"`
	Generation       attemptgeneration.Generation `json:"loop_generation,omitempty"`
	LoopStage        string                       `json:"loop_stage,omitempty"`
}

func selectedContractActivitySource(serverURL string, effectClass runtimecontracts.ActivityEffectClass) semanticview.Source {
	node := runtimecontracts.SystemNodeContract{
		ID: "test-node", ExecutionType: runtimecontracts.SystemNodeExecutionType,
		SubscribesTo: []string{"platform.activity_requested"},
	}
	flow := runtimecontracts.FlowContractView{
		Paths:  runtimecontracts.FlowContractPaths{ID: "flow-a", PackageKey: "activity-fork-proof", Dir: "flows/flow-a"},
		Schema: runtimecontracts.FlowSchemaDocument{Name: "flow-a", InitialState: "pending", States: []string{"pending"}},
		Nodes:  map[string]runtimecontracts.SystemNodeContract{"test-node": node}, Path: "flow-a",
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name: "activity-fork-proof", Version: "v1", InitialStage: "pending",
			FlowInitial: map[string]string{"flow-a": "pending"}, FlowStates: map[string][]string{"flow-a": {"pending"}},
			EventOwners: map[string][]string{"platform.activity_requested": {"test-node"}},
			EffectiveNodes: map[string]runtimecontracts.SystemNodeEffectiveSemantics{
				"test-node": {ID: "test-node", ExecutionType: runtimecontracts.SystemNodeExecutionType, RuntimeSubscriptions: []string{"platform.activity_requested"}},
			},
		},
		FlowTree: runtimecontracts.FlowTree{Root: &flow, ByPath: map[string]*runtimecontracts.FlowContractView{"flow-a": &flow}, ByID: map[string]*runtimecontracts.FlowContractView{"flow-a": &flow}},
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"provider.connector": {
				HandlerType: "http", EffectClass: string(effectClass),
				InputSchema: runtimecontracts.ToolInputSchema{Type: "object"}, OutputSchema: runtimecontracts.ToolInputSchema{Type: "object"},
				HTTP: &runtimecontracts.HTTPToolSpec{Method: "POST", URL: strings.TrimRight(serverURL, "/")},
			},
		},
	}
	return semanticview.Wrap(bundle)
}

func selectedContractActivityLoadedSource(source semanticview.Source, selection store.RunForkContractSelection) LoadedSelectedContractSource {
	workflow := runtimepipeline.NewWorkflowDefinition("activity-fork-proof", []runtimepipeline.WorkflowStage{{Name: "pending"}}, nil)
	nodes := []runtimepipeline.WorkflowNode{{
		ID: "test-node", Subscriptions: []events.EventType{"platform.activity_requested"}, ExecutionType: runtimecontracts.SystemNodeExecutionType,
		Policies: map[string]runtimepipeline.WorkflowEventPolicy{"platform.activity_requested": {Consume: true, RequireEntity: true}},
	}}
	return LoadedSelectedContractSource{
		Selection: selection, Source: source,
		Module: selectedContractWorkflowModule{
			source: source, workflow: workflow, nodes: nodes,
			guardRegistry: runtimepipeline.NewContractGuardRegistry(source), actionRegistry: runtimepipeline.NewContractActionRegistry(source),
		},
	}
}

func seedSelectedContractActivityLoop(t *testing.T, db *sql.DB, runID, entityID, requestEventID string, activation loopruntime.Activation, at time.Time) {
	t.Helper()
	buckets := map[string]map[string]any{}
	if err := loopruntime.Store(buckets, activation); err != nil {
		t.Fatalf("store source loop activation: %v", err)
	}
	accumulator := selectedContractActivityJSON(t, buckets)
	handlerLoops := selectedContractActivityJSON(t, buckets[loopruntime.BucketKey])
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `UPDATE entity_state SET accumulator = $3::jsonb WHERE run_id = $1::uuid AND entity_id = $2::uuid`, runID, entityID, accumulator); err != nil {
		t.Fatalf("seed source loop accumulator: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event,
			writer_type, writer_id, handler_step, created_at
		) VALUES ($1::uuid, $2::uuid, 'accumulator.handler_loops', 'null'::jsonb, $3::jsonb, $4::uuid, 'platform', 'activity-fork-proof', 'seed', $5)
	`, runID, entityID, handlerLoops, requestEventID, at); err != nil {
		t.Fatalf("seed source loop mutation: %v", err)
	}
}

func seedSelectedContractActivityRequest(t *testing.T, db *sql.DB, runID, requestEventID string, payload selectedContractActivityRequestPayload) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), `UPDATE events SET payload = $3::jsonb WHERE run_id = $1::uuid AND event_id = $2::uuid`, runID, requestEventID, selectedContractActivityJSON(t, payload)); err != nil {
		t.Fatalf("seed source activity request: %v", err)
	}
}

func seedSelectedContractActivityAttempt(t *testing.T, db *sql.DB, fact activityidentity.Fact, generation attemptgeneration.Generation, status, resultEventType, failureClass, failureCode string, at time.Time) {
	t.Helper()
	resultPayload := map[string]any{
		"activity_id": "connector", "tool": "provider.connector", "effect_class": string(runtimecontracts.ActivityEffectClassNonIdempotentWrite),
		"attempt": 1, "revision_id": generation.RevisionID,
	}
	var failure any
	var failureJSON any
	if failureClass == "" {
		resultPayload["result"] = map[string]any{"value": "recorded-result"}
	} else {
		typed, ok := runtimefailures.EnvelopeFromError(runtimefailures.New(runtimefailures.Class(failureClass), failureCode, "activity-runtime", "execute_non_idempotent_http", map[string]any{"tool": "provider.connector"}))
		if !ok {
			t.Fatalf("construct source activity failure %s/%s", failureClass, failureCode)
		}
		failure = typed
		resultPayload["failure"] = failure
		failureJSON = selectedContractActivityJSON(t, failure)
	}
	requestEventID := activityidentity.RequestEventID(fact)
	resultEventID := activityidentity.ResultEventID(fact, resultEventType)
	inputHash := sha256.Sum256([]byte(`{"value":"x"}`))
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO activity_attempts (
			request_event_id, run_id, source_event_id, entity_id, flow_instance, node_id, handler_event_key,
			activity_id, tool, effect_class, attempt, status, success_event, failure_event,
			result_event_id, result_event_type, result_payload, failure, input_hash, loop_generation, loop_stage,
			started_at, completed_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, $3::uuid, $4::uuid, 'flow-a/1', 'test-node', 'review.requested',
			'connector', 'provider.connector', 'non_idempotent_write', 1, $5, 'activity.succeeded', 'activity.failed',
			$6::uuid, $7, $8::jsonb, $9::jsonb, $10, $11::jsonb, 'review', $12, $12, $12
		)
	`, requestEventID, fact.RunID, fact.SourceEventID, fact.EntityID, status, resultEventID, resultEventType,
		selectedContractActivityJSON(t, resultPayload), failureJSON, fmt.Sprintf("sha256:%x", inputHash[:]), selectedContractActivityJSON(t, generation), at); err != nil {
		t.Fatalf("seed source activity attempt: %v", err)
	}
}

func loadSelectedContractActivityRequest(t *testing.T, db *sql.DB, runID, eventID string) selectedContractActivityRequestPayload {
	t.Helper()
	var raw []byte
	if err := db.QueryRowContext(context.Background(), `SELECT payload FROM events WHERE run_id = $1::uuid AND event_id = $2::uuid`, runID, eventID).Scan(&raw); err != nil {
		t.Fatalf("load fork activity request: %v", err)
	}
	var payload selectedContractActivityRequestPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode fork activity request: %v", err)
	}
	return payload
}

func assertSelectedContractForkActivityAttempt(t *testing.T, db *sql.DB, requestEventID, forkRunID, resultEventID string, generation attemptgeneration.Generation, tt selectedContractActivityForkCase) {
	t.Helper()
	var gotRunID, gotStatus, gotResultID string
	var rawGeneration, rawFailure []byte
	if err := db.QueryRowContext(context.Background(), `
		SELECT run_id::text, status, result_event_id::text, loop_generation, COALESCE(failure, 'null'::jsonb)
		FROM activity_attempts WHERE request_event_id = $1::uuid
	`, requestEventID).Scan(&gotRunID, &gotStatus, &gotResultID, &rawGeneration, &rawFailure); err != nil {
		t.Fatalf("load fork activity attempt: %v", err)
	}
	var gotGeneration attemptgeneration.Generation
	if err := json.Unmarshal(rawGeneration, &gotGeneration); err != nil {
		t.Fatalf("decode fork activity generation: %v", err)
	}
	if gotRunID != forkRunID || gotStatus != tt.wantForkAttemptStatus || gotResultID != resultEventID || !gotGeneration.Equal(generation) {
		t.Fatalf("fork attempt = run:%s status:%s result:%s generation:%#v", gotRunID, gotStatus, gotResultID, gotGeneration)
	}
	if tt.failureClass != "" {
		var failure map[string]any
		if err := json.Unmarshal(rawFailure, &failure); err != nil {
			t.Fatalf("decode fork activity failure: %v", err)
		}
		detail, _ := failure["detail"].(map[string]any)
		if failure["class"] != tt.failureClass || detail["code"] != tt.failureCode {
			t.Fatalf("fork attempt failure = %#v, want class=%s code=%s", failure, tt.failureClass, tt.failureCode)
		}
	}
}

func loadSelectedContractActivityResult(t *testing.T, db *sql.DB, runID, eventID, eventType string) map[string]any {
	t.Helper()
	var gotType string
	var raw []byte
	if err := db.QueryRowContext(context.Background(), `SELECT event_name, payload FROM events WHERE run_id = $1::uuid AND event_id = $2::uuid`, runID, eventID).Scan(&gotType, &raw); err != nil {
		t.Fatalf("load fork activity result: %v", err)
	}
	if gotType != eventType {
		t.Fatalf("fork result event type = %s, want %s", gotType, eventType)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode fork activity result: %v", err)
	}
	return payload
}

func selectedContractActivityJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal activity proof value: %v", err)
	}
	return string(raw)
}
