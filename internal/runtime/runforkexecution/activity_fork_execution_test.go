package runforkexecution

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	runforkrevision "github.com/division-sh/swarm/internal/store/testutil/runforkrevisionfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
)

func TestExecuteSelectedContractRunForkExecutesOrReusesLoopActivityThroughRuntimeContainer(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runSelectedContractActivityForkCases(t, newActivityForkFixture(t, backend))
		})
	}
}

func runSelectedContractActivityForkCases(t *testing.T, fixture activityForkFixture) {
	db := fixture.db
	ctx := runForkTestContext(t)

	var connectorCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		connectorCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"value":"fork-result"}`))
	}))
	t.Cleanup(server.Close)

	tests := []selectedContractActivityForkCase{
		{
			name:            "manual confirmation does not authorize another write",
			effectClass:     runtimecontracts.ActivityEffectClassNonIdempotentWrite,
			forkPolicy:      runtimecontracts.ActivityForkRequireConfirmation,
			resultEventType: "flow_a/connector.succeeded",
			wantError:       "fork policy \"require_manual_confirmation\" is not executable",
		},
		{
			name:               "read-only reexecutes exactly once",
			effectClass:        runtimecontracts.ActivityEffectClassReadOnly,
			forkPolicy:         runtimecontracts.ActivityForkReexecuteRead,
			resultEventType:    "flow_a/connector.succeeded",
			wantConnectorCalls: 1,
		},
		{
			name:                  "succeeded write publishes recorded result",
			effectClass:           runtimecontracts.ActivityEffectClassNonIdempotentWrite,
			forkPolicy:            runtimecontracts.ActivityForkReuseRecordedResult,
			sourceAttemptStatus:   "succeeded",
			resultEventType:       "flow_a/connector.succeeded",
			wantForkAttemptStatus: "succeeded",
		},
		{
			name:                  "failed write publishes recorded typed failure",
			effectClass:           runtimecontracts.ActivityEffectClassNonIdempotentWrite,
			forkPolicy:            runtimecontracts.ActivityForkReuseRecordedResult,
			sourceAttemptStatus:   "failed",
			resultEventType:       "flow_a/connector.failed",
			failureClass:          string(runtimefailures.ClassDependencyUnavailable),
			failureCode:           "provider_unavailable",
			wantForkAttemptStatus: "failed",
		},
		{
			name:                  "uncertain write stays uncertain and publishes outcome-uncertain failure",
			effectClass:           runtimecontracts.ActivityEffectClassNonIdempotentWrite,
			forkPolicy:            runtimecontracts.ActivityForkReuseRecordedResult,
			sourceAttemptStatus:   "uncertain",
			resultEventType:       "flow_a/connector.failed",
			failureClass:          string(runtimefailures.ClassOutcomeUncertain),
			failureCode:           "activity_provider_outcome_uncertain",
			wantForkAttemptStatus: "uncertain",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selection := runfork.RunForkContractSelection{Mode: "selected_contracts"}
			loaded := selectedContractActivityDeclaredSource(t, server.URL, tt.effectClass, selection)
			activityNode := mustRunForkNode("flow_a", "test-node")
			beforeCalls := connectorCalls.Load()
			sourceRunID := uuid.NewString()
			entityID := uuid.NewString()
			initiatingEventID := uuid.NewString()
			at := time.Now().UTC().Truncate(time.Microsecond)
			activation, err := loopruntime.New(sourceRunID, entityID, "flow_a", "revision", "revision_id", uuid.NewString(), "review", 3, at.Add(-time.Minute))
			if err != nil {
				t.Fatalf("create source loop activation: %v", err)
			}
			sourceGeneration := activation.Generation()
			sourceFact := activityidentity.Fact{
				RunID: sourceRunID, SourceEventID: initiatingEventID, EntityID: entityID,
				Owner: activityidentity.MustNodeOwner(activityNode), ExecutionFlowID: "flow_a", HandlerEventKey: "review.requested",
				ActivityID: "connector", Tool: "provider.connector", Attempt: 1,
				RevisionID: sourceGeneration.RevisionID,
			}
			sourceRequestEventID := activityidentity.RequestEventID(sourceFact)
			routingSource := eventtest.StaticFlowRoutingSource("flow_a", "flow_a", entityID)
			activityRoute := events.DeliveryRoute{
				Recipient: events.MustNodeDeliveryRecipient(activityNode),
				Target:    events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "flow_a", FlowInstance: "flow_a"}),
			}
			fixture.seedSource(t, loaded, sourceRunID, entityID, sourceRequestEventID, at, activityRoute, routingSource)
			if _, err := db.ExecContext(ctx, `UPDATE entity_state SET flow_instance = 'flow_a' WHERE run_id = $1::uuid AND entity_id = $2::uuid`, sourceRunID, entityID); err != nil {
				t.Fatalf("canonicalize source activity workflow state route: %v", err)
			}
			seedSelectedContractActivityLoop(t, db, sourceRunID, entityID, sourceRequestEventID, activation, at)
			seedSelectedContractActivityRequest(t, db, sourceRunID, sourceRequestEventID, selectedContractActivityRequestPayload{
				ActivityID: "connector", Tool: "provider.connector", Input: map[string]any{"value": "x"},
				EffectClass: string(tt.effectClass), SuccessEvent: "flow_a/connector.succeeded", FailureEvent: "flow_a/connector.failed",
				RetryMaxAttempts: 1, ForkPolicy: string(tt.forkPolicy), EntityID: entityID, FlowInstance: "flow_a",
				NodeID: activityidentity.MustNodeOwner(activityNode).Key(), FlowID: "flow_a", HandlerEventKey: "review.requested",
				SourceEventID: initiatingEventID, SourceRunID: sourceRunID, Attempt: 1,
				Generation: sourceGeneration, LoopStage: "review",
			})
			fixture.capture(t, sourceRunID)
			if tt.sourceAttemptStatus != "" {
				seedSelectedContractActivityAttempt(t, db, sourceFact, sourceGeneration, tt.sourceAttemptStatus, tt.resultEventType, tt.failureClass, tt.failureCode, at)
			}
			sourceBefore := readActivityForkSourceEvidence(t, db, sourceRunID, entityID, sourceRequestEventID)

			descriptors, err := runtimepkg.AuthorActivityEventDescriptors(loaded.Source)
			if err != nil {
				t.Fatalf("project activity event descriptors: %v", err)
			}
			if !selectedContractActivityDescriptorExists(descriptors, tt.resultEventType) {
				t.Fatalf("selected activity source has no descriptor for %s: %#v", tt.resultEventType, descriptors)
			}
			loader := &fakeSelectedContractSourceLoader{loaded: loaded}
			result, err := executeLiveSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
				SourceRunID: sourceRunID, At: sourceRequestEventID, AllowSourceFreeze: true, Owner: fixture.owner,
				SourceLoader: loader, ContractSelection: selection,
				AgentRuntime: SelectedContractAgentRuntimeOptions{ProcessCapability: fixture.owner.ports.contexts.capability},
			})
			if tt.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) || connectorCalls.Load() != beforeCalls || result.Activation.Activated {
					t.Fatalf("unsupported root executed: calls=%d result=%+v err=%v", connectorCalls.Load()-beforeCalls, result, err)
				}
				if after := readActivityForkSourceEvidence(t, db, sourceRunID, entityID, sourceRequestEventID); !reflect.DeepEqual(sourceBefore, after) {
					t.Fatal("refused fork changed source state, request or activity evidence")
				}
				return
			}
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
			wantGeneration, err := loopruntime.ForkGeneration(sourceGeneration, result.Materialization.ForkRunID, entityID)
			if err != nil || !forkRequest.Generation.Equal(wantGeneration) {
				t.Fatalf("full child generation=%#v want=%#v err=%v", forkRequest.Generation, wantGeneration, err)
			}
			if forkRequest.SourceRunID != result.Materialization.ForkRunID || forkRequest.SourceEventID != result.ForkEvents[0].ForkEventID || !forkRequest.Generation.Valid() || forkRequest.Generation.RevisionID == sourceGeneration.RevisionID {
				t.Fatalf("fork request identity = %#v, source generation = %#v", forkRequest, sourceGeneration)
			}
			forkFact := activityidentity.Fact{
				RunID: result.Materialization.ForkRunID, SourceEventID: forkRequest.SourceEventID,
				ParentEventID: forkRequest.ParentEventID, EntityID: entityID,
				Owner: activityidentity.MustNodeOwner(activityNode), ExecutionFlowID: "flow_a",
				HandlerEventKey: "review.requested", ActivityID: "connector",
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
			original, err := semanticview.CompileOriginalLoopCarriage(loaded.Source)
			if err != nil {
				t.Fatal(err)
			}
			reloaded, err := fixture.owner.ports.replay.LoadRunForkSelectedContractSourceEvents(ctx, sourceRunID, result.Materialization.ForkRunID, []string{sourceRequestEventID}, original)
			// Preparation is not a public read: activation froze the source run.
			// The old source may not grant another preparation after execution.
			if err == nil || !strings.Contains(err.Error(), "source event preparation state forked is unsupported") || len(reloaded) != 0 {
				t.Fatalf("post-activation preparation must refuse: count=%d err=%v", len(reloaded), err)
			}
			repeated := loadSelectedContractActivityRequest(t, db, result.Materialization.ForkRunID, result.ForkEvents[0].ForkEventID)
			if !reflect.DeepEqual(forkRequest, repeated) {
				t.Fatalf("repeat persisted request read changed correspondence: %#v want=%#v", repeated, forkRequest)
			}
			if after := readActivityForkSourceEvidence(t, db, sourceRunID, entityID, sourceRequestEventID); !reflect.DeepEqual(sourceBefore, after) {
				t.Fatalf("child execution/preparation changed source evidence\nbefore=%#v\nafter=%#v", sourceBefore, after)
			}
			var repeatedAttempts int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM activity_attempts WHERE request_event_id=$1::uuid`, forkRequestEventID).Scan(&repeatedAttempts); err != nil || repeatedAttempts != forkAttemptCount || connectorCalls.Load()-beforeCalls != tt.wantConnectorCalls {
				t.Fatalf("repeat preparation duplicated activity effects: attempts=%d calls=%d err=%v", repeatedAttempts, connectorCalls.Load()-beforeCalls, err)
			}
		})
	}
}

// The source state and activity journal are explicit fixtures. Fork admission,
// materialization, runtime execution, connector calls and final publication are real.
type activityForkFixture struct {
	db     activityForkFixtureDB
	owner  SelectedContractExecutionOwner
	sqlite *store.SQLiteRuntimeStore
}

type activityForkFixtureDB struct {
	*sql.DB
	sqlite bool
}

type activityForkSourceEvidence struct {
	Flow, EntityType, Stage, AttemptStatus string
	Fields, Accumulator, Payload           []byte
}

func readActivityForkSourceEvidence(t *testing.T, db activityForkFixtureDB, runID, entityID, requestID string) activityForkSourceEvidence {
	t.Helper()
	var out activityForkSourceEvidence
	if err := db.QueryRowContext(context.Background(), `SELECT flow_instance,entity_type,current_state,fields,accumulator FROM entity_state WHERE run_id=$1::uuid AND entity_id=$2::uuid`, runID, entityID).Scan(&out.Flow, &out.EntityType, &out.Stage, &out.Fields, &out.Accumulator); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(context.Background(), `SELECT payload_bytes FROM events WHERE event_id=$1::uuid AND run_id=$2::uuid`, requestID, runID).Scan(&out.Payload); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(context.Background(), `SELECT COALESCE((SELECT status FROM activity_attempts WHERE request_event_id=$1::uuid AND run_id=$2::uuid),'')`, requestID, runID).Scan(&out.AttemptStatus); err != nil {
		t.Fatal(err)
	}
	return out
}

var activityForkFixtureParameter = regexp.MustCompile(`\$([0-9]+)`)

func (db activityForkFixtureDB) query(query string) string {
	if !db.sqlite {
		return query
	}
	// Only the fixture SQL below is adapted, never production SQL or evidence.
	query = strings.NewReplacer("::uuid", "", "::jsonb", "", "::bytea", "", "::text", "").Replace(query)
	return activityForkFixtureParameter.ReplaceAllString(query, `?$1`)
}

func (db activityForkFixtureDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return db.DB.ExecContext(ctx, db.query(query), args...)
}

func (db activityForkFixtureDB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return db.DB.QueryRowContext(ctx, db.query(query), args...)
}

func newActivityForkFixture(t *testing.T, backend string) activityForkFixture {
	t.Helper()
	switch backend {
	case "sqlite":
		selected := storetest.StartSQLiteRuntimeStore(t)
		return activityForkFixture{
			db:    activityForkFixtureDB{DB: storetest.Database(selected), sqlite: true},
			owner: selectedContractSQLiteExecutionOwnerForTest(t, selected), sqlite: selected,
		}
	case "postgres":
		_, db, _ := testutil.StartPostgres(t)
		selected := storetest.AdmitPostgresRuntimeStore(t, db)
		return activityForkFixture{db: activityForkFixtureDB{DB: db}, owner: selectedContractExecutionOwnerForTest(t, selected)}
	default:
		t.Fatalf("unknown activity proof backend %q", backend)
		return activityForkFixture{}
	}
}

func (f activityForkFixture) seedSource(t *testing.T, loaded LoadedSelectedContractSource, runID, entityID, eventID string, at time.Time, route events.DeliveryRoute, source events.RoutingSource) {
	t.Helper()
	envelope := events.EnvelopeForSourceRoute(events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "flow_a"), source.Route())
	if f.sqlite == nil {
		seedSelectedExecutionSourceRunWithPrimaryRouteAndSource(t, f.db.DB, runID, entityID, eventID, "platform.activity_requested", at,
			"test_entity", route, nil, source, envelope, loaded.SourceArtifactFact)
		return
	}
	ctx := runtimecorrelation.WithSourceArtifactFact(runForkTestContext(t), loaded.SourceArtifactFact)
	ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(runForkTestRuntimeInstanceID, loaded.SourceArtifactFact.BundleHash()))
	artifact := selectedExecutionSourceArtifact(t, loaded.SourceArtifactFact.BundleHash())
	if _, err := f.sqlite.EnsureSourceArtifact(ctx, artifact); err != nil {
		t.Fatalf("admit SQLite source artifact: %v", err)
	}
	runlifecyclefixture.RequireSQLite(t, ctx, f.db.DB, runlifecyclefixture.Fixture{
		Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, StartedAt: at.Add(-time.Minute), Source: loaded.SourceArtifactFact,
	})
	payload, err := json.Marshal(map[string]any{"entity_id": entityID})
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.ExistingRunRootIngressWithRoutingSource(eventID, "platform.activity_requested", "source-runtime", "", payload, 0, runID, envelope, source, at)
	storetest.CommitSemanticEventWithRoutes(t, ctx, f.sqlite, event, []events.DeliveryRoute{route}, pipelineobligation.ScopeSubscribed)
	if _, err := f.db.ExecContext(ctx, `INSERT INTO entity_mutations
		(run_id, entity_id, domain, path, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at)
		VALUES ($1, $2, 'lifecycle_state', '', 'null', '"pending"', $3, 'platform', 'selected-execution-test', 'seed', $4),
		($1, $2, 'authored_field', 'name', 'null', '"Selected Execution Entity"', $3, 'platform', 'selected-execution-test', 'seed', $4)`, runID, entityID, eventID, at); err != nil {
		t.Fatalf("seed SQLite source mutations: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `INSERT INTO entity_state
		(run_id, entity_id, flow_instance, entity_type, name, current_state, gates, fields, accumulator, revision, entered_state_at, created_at, updated_at)
		VALUES ($1, $2, 'flow_a', 'test_entity', 'Selected Execution Entity', 'pending', '{}', '{"name":"Selected Execution Entity"}', '{}', 1, $3, $3, $3)`, runID, entityID, at); err != nil {
		t.Fatalf("seed SQLite source state: %v", err)
	}
}

func (f activityForkFixture) capture(t *testing.T, runID string) {
	t.Helper()
	if f.sqlite == nil {
		captureSelectedExecutionSourceRevision(t, f.db.DB, runID)
		return
	}
	ctx := context.Background()
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := runforkrevision.CaptureSQLite(ctx, tx, runID, runforkrevision.AllFamilies()...); err != nil {
		t.Fatalf("capture SQLite activity source revision: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func selectedContractActivityDescriptorExists(descriptors []runtimeauthoractivity.EventDescriptor, eventType string) bool {
	for _, descriptor := range descriptors {
		if descriptor.EventType == eventType {
			return true
		}
	}
	return false
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
	wantError             string
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
	FlowInstance     string                       `json:"flow_instance"`
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
	return selectedContractActivitySourceWithMode(serverURL, effectClass, runtimecontracts.FlowModeStatic)
}

func selectedContractActivityDeclaredSource(t *testing.T, serverURL string, effectClass runtimecontracts.ActivityEffectClass, selection runfork.RunForkContractSelection) LoadedSelectedContractSource {
	t.Helper()
	root := t.TempDir()
	for path, contents := range map[string]string{
		"schema.yaml":   "name: activity-fork-proof\nstages:\n  pending: {initial: true}\n",
		"entities.yaml": "root: {}\n",
		"flow_a/schema.yaml": `name: flow_a
mode: static
stages:
  pending: {initial: true}
  review: {}
  closed: {terminal: true}
  exhausted: {terminal: true}
loops:
  revision:
    revision_field: revision_id
    max_attempts: 3
    escape: {advances_to: exhausted}
`,
		"flow_a/entities.yaml": "test_entity:\n  name: text\n",
		"flow_a/events.yaml":   "review.requested:\n  revision_id: text\nreview.start: {}\nreview.retry:\n  revision_id: text\nreview.close:\n  revision_id: text\n",
		"flow_a/nodes.yaml": `test-node:
  id: test-node
  execution_type: system_node
  subscribes_to: [review.start, review.requested, review.retry, review.close]
  event_handlers:
    review.start:
      loop: {start: revision, from: pending}
      advances_to: review
    review.requested:
      loop: {admit: revision, from: review}
      advances_to: review
      activity: {id: connector, tool: provider.connector}
    review.retry:
      loop: {repeat: revision, from: review}
      advances_to: review
    review.close:
      loop: {close: revision, from: review}
      advances_to: closed
`,
		"tools.yaml": fmt.Sprintf(`provider.connector:
  handler_type: http
  effect_class: %s
  input_schema: {type: object}
  output_schema: {type: object}
  http: {method: POST, url: %q}
`, effectClass, serverURL),
	} {
		writeSelectedContractFixtureFile(t, filepath.Join(root, path), contents)
	}
	repoRoot := runForkExecutionRepoRoot(t)
	loaded, err := (admittedFixtureSelectedContractSourceLoader{RepoRoot: repoRoot, SourceRoot: root, PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repoRoot)}).LoadRunForkSelectedContractSource(context.Background(), selection)
	if err != nil {
		t.Fatalf("load declared activity producer fixture: %v", err)
	}
	bundle, ok := semanticview.Bundle(loaded.Source)
	if !ok {
		t.Fatal("activity fixture requires its admitted source bundle")
	}
	entityType, _, ok := bundle.FlowPrimaryEntityContract("flow_a")
	if !ok || entityType != "test_entity" {
		t.Fatalf("activity fixture must declare its producer entity in flow_a: %q, present=%v", entityType, ok)
	}
	return loaded
}

func selectedContractActivitySourceWithMode(serverURL string, effectClass runtimecontracts.ActivityEffectClass, mode string) semanticview.Source {
	handler := runtimecontracts.SystemNodeEventHandler{
		Activity: runtimecontracts.ActivitySpec{ID: "connector", Tool: "provider.connector"},
		Loop:     &runtimecontracts.LoopOperationSpec{Admit: "revision", From: "pending"},
	}
	node := runtimecontracts.SystemNodeContract{
		ExecutionType: runtimecontracts.SystemNodeExecutionType,
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"review.requested": handler},
	}
	flow := runtimecontracts.FlowContractView{
		Paths:  runtimecontracts.FlowContractPaths{FlowPath: "flow_a"},
		Schema: runtimecontracts.FlowSchemaDocument{Name: "flow_a", Mode: mode, InitialState: "pending", States: []string{"pending"}},
		Nodes:  map[string]runtimecontracts.SystemNodeContract{"test-node": node},
		Events: map[string]runtimecontracts.EventCatalogEntry{"review.requested": {}}, Path: "flow_a",
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootEntities: runtimecontracts.EntityContractsDocument{"test_entity": {}},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name: "activity-fork-proof", Version: "v1", InitialStage: "pending",
			FlowInitial: map[string]string{"flow_a": "pending"}, FlowStates: map[string][]string{"flow_a": {"pending"}},
			Loops: []runtimecontracts.WorkflowLoopPlan{{
				FlowID: "flow_a", ID: "revision", RevisionField: "revision_id", MaxAttempts: runtimecontracts.LoopAttemptLimit{Literal: 3},
				Escape: runtimecontracts.LoopEscapeSpec{AdvancesTo: "pending"},
			}},
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"test-node": {"review.requested": handler},
			},
			EffectiveNodes: map[string]runtimecontracts.SystemNodeEffectiveSemantics{
				"test-node": {
					ID: "test-node", ExecutionType: runtimecontracts.SystemNodeExecutionType,
					Produces: []string{"flow_a/connector.succeeded", "flow_a/connector.failed"},
				},
			},
		},
		FlowTree:    runtimecontracts.FlowTree{Root: &flow, ByPath: map[string]*runtimecontracts.FlowContractView{"flow_a": &flow}, ByID: map[string]*runtimecontracts.FlowContractView{"flow_a": &flow}},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{"flow_a": flow.Schema},
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"provider.connector": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")), runtimecontracts.WithToolEffect(runtimecontracts.NormalizeActivityEffectClass(string(effectClass))), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object")), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object"))), runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: strings.TrimRight(serverURL, "/")})),
		},
	}
	return semanticview.Wrap(bundle)
}

func seedSelectedContractActivityLoop(t *testing.T, db activityForkFixtureDB, runID, entityID, requestEventID string, activation loopruntime.Activation, at time.Time) {
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
			run_id, entity_id, domain, path, old_value, new_value, caused_by_event,
			writer_type, writer_id, handler_step, created_at
		) VALUES ($1::uuid, $2::uuid, 'accumulator', 'handler_loops', 'null'::jsonb, $3::jsonb, $4::uuid, 'platform', 'activity-fork-proof', 'seed', $5)
	`, runID, entityID, handlerLoops, requestEventID, at); err != nil {
		t.Fatalf("seed source loop mutation: %v", err)
	}
}

func seedSelectedContractActivityRequest(t *testing.T, db activityForkFixtureDB, runID, requestEventID string, payload selectedContractActivityRequestPayload) {
	t.Helper()
	raw := selectedContractActivityJSON(t, payload)
	if _, err := db.ExecContext(context.Background(), `UPDATE events SET payload = $3::jsonb, payload_bytes = $4::bytea WHERE run_id = $1::uuid AND event_id = $2::uuid`, runID, requestEventID, raw, []byte(raw)); err != nil {
		t.Fatalf("seed source activity request: %v", err)
	}
}

func seedSelectedContractActivityAttempt(t *testing.T, db activityForkFixtureDB, fact activityidentity.Fact, generation attemptgeneration.Generation, status, resultEventType, failureClass, failureCode string, at time.Time) {
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
			request_event_id, run_id, execution_mode, source_event_id, entity_id, flow_instance, node_id, handler_event_key,
			activity_id, tool, effect_class, attempt, status, success_event, failure_event,
			result_event_id, result_event_type, result_payload, failure, input_hash, loop_generation, loop_stage,
			started_at, completed_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, 'live', $3::uuid, $4::uuid, 'flow_a', $13, 'review.requested',
			'connector', 'provider.connector', 'non_idempotent_write', 1, $5, 'flow_a/connector.succeeded', 'flow_a/connector.failed',
			$6::uuid, $7, $8::jsonb, $9::jsonb, $10, $11::jsonb, 'review', $12, $12, $12
		)
	`, requestEventID, fact.RunID, fact.SourceEventID, fact.EntityID, status, resultEventID, resultEventType,
		selectedContractActivityJSON(t, resultPayload), failureJSON, fmt.Sprintf("sha256:%x", inputHash[:]), selectedContractActivityJSON(t, generation), at, fact.Owner.Key()); err != nil {
		t.Fatalf("seed source activity attempt: %v", err)
	}
}

func loadSelectedContractActivityRequest(t *testing.T, db activityForkFixtureDB, runID, eventID string) selectedContractActivityRequestPayload {
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

func assertSelectedContractForkActivityAttempt(t *testing.T, db activityForkFixtureDB, requestEventID, forkRunID, resultEventID string, generation attemptgeneration.Generation, tt selectedContractActivityForkCase) {
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

func loadSelectedContractActivityResult(t *testing.T, db activityForkFixtureDB, runID, eventID, eventType string) map[string]any {
	t.Helper()
	var gotType string
	var raw []byte
	if err := db.QueryRowContext(context.Background(), `SELECT event_name, payload FROM events WHERE run_id = $1::uuid AND event_id = $2::uuid`, runID, eventID).Scan(&gotType, &raw); err != nil {
		t.Fatalf("load fork activity result %s/%s: %v", eventID, eventType, err)
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
