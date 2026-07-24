package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/testutil"
)

type startupRecoveryPipelineOwner struct {
	mu       sync.Mutex
	issuer   *runtimepipelineobligation.ClaimIssuer
	work     []events.PersistedReplayEvent
	claimErr error
	claims   map[string]runtimepipelineobligation.Claim
}

func newStartupRecoveryPipelineOwner(work []events.PersistedReplayEvent, claimErr error) *startupRecoveryPipelineOwner {
	return &startupRecoveryPipelineOwner{
		issuer:   runtimepipelineobligation.NewClaimIssuer(),
		work:     append([]events.PersistedReplayEvent(nil), work...),
		claimErr: claimErr,
		claims:   map[string]runtimepipelineobligation.Claim{},
	}
}

func (s *startupRecoveryPipelineOwner) issue(eventID string, purpose runtimepipelineobligation.Purpose) (runtimepipelineobligation.Claim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, busy := s.claims[eventID]; busy {
		return runtimepipelineobligation.Claim{}, runtimepipelineobligation.ErrBusy
	}
	claim, err := s.issuer.Issue(eventID, purpose)
	if err == nil {
		s.claims[eventID] = claim
	}
	return claim, err
}

func (s *startupRecoveryPipelineOwner) ClaimPublication(_ context.Context, eventID string) (runtimepipelineobligation.Claim, error) {
	return s.issue(eventID, runtimepipelineobligation.PurposePublication)
}

func (s *startupRecoveryPipelineOwner) ClaimEvent(_ context.Context, eventID string, purpose runtimepipelineobligation.Purpose) (runtimepipelineobligation.ClaimedWork, error) {
	claim, err := s.issue(eventID, purpose)
	if err != nil {
		return runtimepipelineobligation.ClaimedWork{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, item := range s.work {
		if item.Event.ID() == eventID {
			return runtimepipelineobligation.ClaimedWork{
				Event: item.Event, Scope: runtimepipelineobligation.ScopeSubscribed, Claim: claim,
			}, nil
		}
	}
	delete(s.claims, eventID)
	return runtimepipelineobligation.ClaimedWork{}, runtimepipelineobligation.ErrIneligible
}

func (s *startupRecoveryPipelineOwner) ClaimNext(_ context.Context, query runtimepipelineobligation.ClaimQuery) (runtimepipelineobligation.ClaimedWork, bool, error) {
	if err := query.Validate(); err != nil {
		return runtimepipelineobligation.ClaimedWork{}, false, err
	}
	if s.claimErr != nil {
		return runtimepipelineobligation.ClaimedWork{}, false, s.claimErr
	}
	s.mu.Lock()
	var event events.Event
	for _, item := range s.work {
		if query.RunID != "" && item.Event.RunID() != query.RunID {
			continue
		}
		if _, busy := s.claims[item.Event.ID()]; !busy {
			event = item.Event
			break
		}
	}
	s.mu.Unlock()
	if event.ID() == "" {
		return runtimepipelineobligation.ClaimedWork{}, false, nil
	}
	claim, err := s.issue(event.ID(), query.Purpose)
	if err != nil {
		return runtimepipelineobligation.ClaimedWork{}, false, err
	}
	return runtimepipelineobligation.ClaimedWork{
		Event: event, Scope: runtimepipelineobligation.ScopeSubscribed, Claim: claim,
	}, true, nil
}

func (s *startupRecoveryPipelineOwner) verify(claim runtimepipelineobligation.Claim) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.claims[claim.EventID()]
	if !ok {
		return runtimepipelineobligation.ErrStaleClaim
	}
	currentToken, err := s.issuer.Token(current)
	if err != nil {
		return err
	}
	claimToken, err := s.issuer.Token(claim)
	if err != nil {
		return err
	}
	if currentToken != claimToken {
		return runtimepipelineobligation.ErrStaleClaim
	}
	return nil
}

func (s *startupRecoveryPipelineOwner) MarkDecisionProcessed(_ context.Context, claim runtimepipelineobligation.Claim) error {
	return s.verify(claim)
}

func (s *startupRecoveryPipelineOwner) Settle(_ context.Context, claim runtimepipelineobligation.Claim, disposition runtimepipelineobligation.Disposition) error {
	if err := disposition.ValidateFor(claim.Purpose()); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.claims[claim.EventID()]
	if !ok {
		return runtimepipelineobligation.ErrStaleClaim
	}
	currentToken, err := s.issuer.Token(current)
	if err != nil {
		return err
	}
	claimToken, err := s.issuer.Token(claim)
	if err != nil {
		return err
	}
	if currentToken != claimToken {
		return runtimepipelineobligation.ErrStaleClaim
	}
	if err := s.issuer.Verify(claim, claim.EventID(), claim.Purpose()); err != nil {
		return err
	}
	delete(s.claims, claim.EventID())
	if disposition.Kind() != runtimepipelineobligation.DispositionDeferred {
		remaining := s.work[:0]
		for _, item := range s.work {
			if item.Event.ID() != claim.EventID() {
				remaining = append(remaining, item)
			}
		}
		s.work = remaining
	}
	return nil
}

func (s *startupRecoveryPipelineOwner) Release(_ context.Context, claim runtimepipelineobligation.Claim) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.claims[claim.EventID()]
	if !ok {
		return runtimepipelineobligation.ErrStaleClaim
	}
	currentToken, err := s.issuer.Token(current)
	if err != nil {
		return err
	}
	claimToken, err := s.issuer.Token(claim)
	if err != nil {
		return err
	}
	if currentToken != claimToken {
		return runtimepipelineobligation.ErrStaleClaim
	}
	if err := s.issuer.Verify(claim, claim.EventID(), claim.Purpose()); err != nil {
		return err
	}
	delete(s.claims, claim.EventID())
	return nil
}

func (s *startupRecoveryPipelineOwner) GlobalWorkPresence(context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return runtimepipelineobligation.GlobalWorkPresence{ProcessingEligible: len(s.work) > 0}, nil
}

func (s *startupRecoveryPipelineOwner) SummarizeRun(_ context.Context, runID string) (runtimepipelineobligation.RunSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	summary := runtimepipelineobligation.RunSummary{RunID: strings.TrimSpace(runID)}
	for _, item := range s.work {
		if item.Event.RunID() == summary.RunID {
			summary.Replayable++
		}
	}
	return summary, nil
}

func (*startupRecoveryPipelineOwner) TerminalizeRun(context.Context, string, runtimepipelineobligation.Disposition, time.Time) (int, error) {
	return 0, nil
}

type startupRecoveryManagerStore struct {
	loadErr error
	agents  []runtimemanager.PersistedAgent
}

func startupRecoveryLifecycleResult(req runtimemanager.AgentLifecycleTransition) runtimemanager.AgentLifecycleTransitionResult {
	return runtimemanager.AgentLifecycleTransitionResult{
		OperationID: req.OperationID, TransitionID: req.OperationID, AgentID: req.AgentID,
		PreviousEpoch: req.ExpectedEpoch, RuntimeEpoch: req.TargetEpoch,
		PreviousGeneration: req.ExpectedGeneration, Generation: req.TargetGeneration,
		PreviousPhase: req.ExpectedPhase, Phase: req.TargetPhase,
		ConfigRevision: req.ConfigRevision, RunMode: req.RunMode,
	}
}

func (startupRecoveryManagerStore) CommitAgentLifecycleTransition(_ context.Context, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	return startupRecoveryLifecycleResult(req), nil
}

func (s startupRecoveryManagerStore) UpsertAgent(context.Context, runtimemanager.PersistedAgent) error {
	return nil
}

func (s startupRecoveryManagerStore) LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error) {
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	return append([]runtimemanager.PersistedAgent(nil), s.agents...), nil
}

func (startupRecoveryManagerStore) EnsureEntitySchema(context.Context, string) error { return nil }

type startupRecoveryFlakyManagerStore struct {
	remainingFailures int
	loadErr           error
}

func (*startupRecoveryFlakyManagerStore) CommitAgentLifecycleTransition(_ context.Context, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	return startupRecoveryLifecycleResult(req), nil
}

func (s *startupRecoveryFlakyManagerStore) UpsertAgent(context.Context, runtimemanager.PersistedAgent) error {
	return nil
}

func (s *startupRecoveryFlakyManagerStore) LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error) {
	if s.remainingFailures > 0 {
		s.remainingFailures--
		if s.loadErr != nil {
			return nil, s.loadErr
		}
	}
	return nil, nil
}

func (*startupRecoveryFlakyManagerStore) EnsureEntitySchema(context.Context, string) error {
	return nil
}

type startupManagerReplayRuntimeStore struct {
	agents []runtimemanager.PersistedAgent
}

func (*startupManagerReplayRuntimeStore) CommitAgentLifecycleTransition(_ context.Context, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	return startupRecoveryLifecycleResult(req), nil
}

func (*startupManagerReplayRuntimeStore) UpsertAgent(context.Context, runtimemanager.PersistedAgent) error {
	return nil
}

func (s *startupManagerReplayRuntimeStore) LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error) {
	return append([]runtimemanager.PersistedAgent(nil), s.agents...), nil
}

func (*startupManagerReplayRuntimeStore) EnsureEntitySchema(context.Context, string) error {
	return nil
}

type startupManagerReplayRuntimeAgent struct{ id string }

func (a startupManagerReplayRuntimeAgent) ID() string                      { return a.id }
func (startupManagerReplayRuntimeAgent) Type() string                      { return "generic" }
func (startupManagerReplayRuntimeAgent) Subscriptions() []events.EventType { return nil }
func (startupManagerReplayRuntimeAgent) OnEvent(_ context.Context, evt events.Event) ([]events.Event, error) {
	switch evt.Type() {
	case events.EventType("support.replay.drop"):
		return nil, errors.New("boom")
	case events.EventType("support.replay.leased"):
		return nil, errors.New("session currently leased")
	default:
		return nil, nil
	}
}

type startupRecoveryMinimalEventStore struct{}

func (startupRecoveryMinimalEventStore) PipelineObligations() runtimepipelineobligation.Store {
	return newStartupRecoveryPipelineOwner(nil, nil)
}

func (startupRecoveryMinimalEventStore) RegisterAuthorActivityEventCatalog(scope runtimeauthoractivity.Scope, descriptors []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error) {
	return runtimeauthoractivity.NewEventCatalogRegistry().Register(scope, descriptors)
}

func (startupRecoveryMinimalEventStore) CommitPublish(ctx context.Context, plan runtimebus.CommitPublishPlan) (runtimebus.PreparedPublish, error) {
	return runtimebustest.CommitPublishNoop(ctx, plan)
}

func (startupRecoveryMinimalEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, nil
}

func (startupRecoveryMinimalEventStore) SupportsPersistedReplay() bool { return false }

type startupRecoveryEventStore struct {
	missing     []events.PersistedReplayEvent
	routes      []runtimeflowidentity.Route
	claimErr    error
	obligations *startupRecoveryPipelineOwner
}

func (s *startupRecoveryEventStore) PipelineObligations() runtimepipelineobligation.Store {
	if s.obligations == nil {
		s.obligations = newStartupRecoveryPipelineOwner(s.missing, s.claimErr)
	}
	return s.obligations
}

func (startupRecoveryEventStore) RegisterAuthorActivityEventCatalog(scope runtimeauthoractivity.Scope, descriptors []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error) {
	return runtimeauthoractivity.NewEventCatalogRegistry().Register(scope, descriptors)
}

func (startupRecoveryEventStore) CommitPublish(ctx context.Context, plan runtimebus.CommitPublishPlan) (runtimebus.PreparedPublish, error) {
	return runtimebustest.CommitPublishNoop(ctx, plan)
}

func (startupRecoveryEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, nil
}

func (startupRecoveryEventStore) UpsertFlowInstanceRoute(context.Context, runtimebus.FlowInstanceRouteRecord) error {
	return nil
}

func (startupRecoveryEventStore) DeleteFlowInstanceRoute(context.Context, runtimeflowidentity.Route) error {
	return nil
}

func (s startupRecoveryEventStore) ListFlowInstanceRoutes(context.Context) ([]runtimeflowidentity.Route, error) {
	return append([]runtimeflowidentity.Route(nil), s.routes...), nil
}

func testRecoveryDiagnosticsConfig(recoveryOnStartup bool) *config.Config {
	return &config.Config{
		Runtime: config.RuntimeConfig{
			RecoveryOnStartup: recoveryOnStartup,
		},
		LLM: config.LLMConfig{
			Backend: "anthropic",
		},
	}
}

func latestStartupRecoveryDecisionLog(t *testing.T, db *sql.DB) (level, message string, failure *runtimefailures.Envelope, detail map[string]any) {
	t.Helper()
	var payloadRaw []byte
	if err := db.QueryRowContext(testAuthorActivityContext(context.Background()), `
		SELECT payload
		FROM events
		WHERE event_name = 'platform.runtime_log'
		  AND payload->'details'->>'component' = 'runtime'
		  AND payload->'details'->>'action' = 'startup_recovery_decision'
		ORDER BY created_at DESC
		LIMIT 1
	`).Scan(&payloadRaw); err != nil {
		t.Fatalf("load startup recovery decision runtime log: %v", err)
	}
	payload, err := DecodeCanonicalRuntimeLogPayload(payloadRaw)
	if err != nil {
		t.Fatalf("DecodeCanonicalRuntimeLogPayload: %v", err)
	}
	return payload.LogLevel, payload.Message, payload.Failure, payload.Detail
}

type runtimeAftermathLog struct {
	level     string
	action    string
	failure   *runtimefailures.Envelope
	eventType string
	detail    map[string]any
}

func requireFailureCode(t testing.TB, failure *runtimefailures.Envelope, code string) {
	t.Helper()
	if failure == nil || failure.Detail.Code != code {
		t.Fatalf("failure = %#v, want detail code %q", failure, code)
	}
}

func requireNestedFailureCode(t testing.TB, detail map[string]any, key, code string) {
	t.Helper()
	raw, err := json.Marshal(detail[key])
	if err != nil {
		t.Fatalf("marshal %s: %v", key, err)
	}
	failure, err := runtimefailures.UnmarshalEnvelope(raw)
	if err != nil {
		t.Fatalf("decode %s: %v (value=%#v)", key, err, detail[key])
	}
	if failure.Detail.Code != code {
		t.Fatalf("%s failure = %#v, want detail code %q", key, failure, code)
	}
}

func listRuntimeLogsByAction(t *testing.T, db *sql.DB, action string) []runtimeAftermathLog {
	t.Helper()
	rows, err := db.QueryContext(testAuthorActivityContext(context.Background()), `
		SELECT payload
		FROM events
		WHERE event_name = 'platform.runtime_log'
		  AND payload->'details'->>'action' = $1
		ORDER BY created_at ASC
	`, action)
	if err != nil {
		t.Fatalf("query runtime logs by action: %v", err)
	}
	defer rows.Close()

	out := []runtimeAftermathLog{}
	for rows.Next() {
		var payloadRaw []byte
		if err := rows.Scan(&payloadRaw); err != nil {
			t.Fatalf("scan runtime log payload: %v", err)
		}
		payload, err := DecodeCanonicalRuntimeLogPayload(payloadRaw)
		if err != nil {
			t.Fatalf("DecodeCanonicalRuntimeLogPayload: %v", err)
		}
		out = append(out, runtimeAftermathLog{
			level:     payload.LogLevel,
			action:    payload.Action,
			failure:   payload.Failure,
			eventType: payload.EventType,
			detail:    payload.Detail,
		})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("runtime log rows: %v", err)
	}
	return out
}

func detailString(v any) string {
	switch typed := v.(type) {
	case string:
		return strings.TrimSpace(typed)
	default:
		return strings.TrimSpace(asString(v))
	}
}

func detailBool(v any) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return false
}

func detailInt(v any) int {
	switch typed := v.(type) {
	case float64:
		return int(typed)
	case int:
		return typed
	default:
		return 0
	}
}

func detailClasses(v any) []string {
	out := []string{}
	switch typed := v.(type) {
	case []string:
		for _, item := range typed {
			if text := detailString(item); text != "" {
				out = append(out, text)
			}
		}
	case []any:
		for _, item := range typed {
			if text := detailString(item); text != "" {
				out = append(out, text)
			}
		}
	}
	return out
}

func assertContainsClass(t *testing.T, classes []string, want string) {
	t.Helper()
	for _, item := range classes {
		if item == want {
			return
		}
	}
	t.Fatalf("recoverable_work_classes = %#v, want %q present", classes, want)
}

func TestRuntimeStart_RecoveryDisabledEmitsDeniedDecisionForActiveSchedules(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)
	scheduleStore := &recordingRuntimeScheduleStore{
		active: []runtimepipeline.Schedule{{
			AgentID:   "runtime",
			EventType: "timer.check",
			Mode:      "once",
			At:        time.Now().Add(time.Minute),
			TaskID:    "recover-me",
		}},
	}

	rt, err := newScopedTestRuntime(t, ctx, RuntimeDeps{Config: testRecoveryDiagnosticsConfig(false), Stores: Stores{
		SQLDB:           db,
		PipelineStore:   runtimepipeline.NewWorkflowInstanceStore(db),
		RuntimeLogStore: runtimeLogPersistenceStub{db: db},
		EventStore:      startupRecoveryMinimalEventStore{},
		ManagerStore:    &recoveryGuardManagerStore{},
		ScheduleStore:   scheduleStore,
	}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}

	err = rt.Start(ctx)
	if err == nil || !strings.Contains(err.Error(), "runtime.recovery_on_startup=false") {
		t.Fatalf("Start error = %v, want explicit startup denial", err)
	}

	level, _, failure, detail := latestStartupRecoveryDecisionLog(t, db)
	if level != "warn" {
		t.Fatalf("log level = %q, want warn", level)
	}
	requireFailureCode(t, failure, "startup_recovery_disabled_with_work")
	if got := detailString(detail["decision_outcome"]); got != "denied" {
		t.Fatalf("decision_outcome = %q, want denied", got)
	}
	if got := detailString(detail["decision_reason_code"]); got != string(startupRecoveryReasonDisabledWithWork) {
		t.Fatalf("decision_reason_code = %q, want %q", got, startupRecoveryReasonDisabledWithWork)
	}
	if got := detailInt(detail["active_schedule_count"]); got != 1 {
		t.Fatalf("active_schedule_count = %d, want 1", got)
	}
	assertContainsClass(t, detailClasses(detail["recoverable_work_classes"]), "active schedules")
}

func TestRuntimeStart_RecoveryDisabledAllowsAndLogsManagerSnapshotWork(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)
	eventStore := &startupRecoveryEventStore{
		missing: []events.PersistedReplayEvent{{
			Event: eventtest.RunCreatingRootIngress(eventtest.UUID("startup-recovery-manager-work"), "support.item_created", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
		}},
		routes: []runtimeflowidentity.Route{
			runtimeflowidentity.DeriveRoute("child", "inst-1"),
		},
	}
	managerStore := &startupRecoveryManagerStore{
		agents: []runtimemanager.PersistedAgent{{
			Config: runtimeactors.AgentConfig{ExecutionMode: "live", ID: "persisted-agent"},
		}},
	}

	rt, err := newScopedTestRuntime(t, ctx, RuntimeDeps{Config: testRecoveryDiagnosticsConfig(false), Stores: Stores{
		SQLDB:           db,
		PipelineStore:   runtimepipeline.NewWorkflowInstanceStore(db),
		RuntimeLogStore: runtimeLogPersistenceStub{db: db},
		EventStore:      eventStore,
		ManagerStore:    managerStore,
	}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := rt.Shutdown(); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	}()

	level, message, failure, detail := latestStartupRecoveryDecisionLog(t, db)
	if level != "warn" {
		t.Fatalf("log level = %q, want warn", level)
	}
	if message != "Runtime startup allowed with manager recovery skipped" {
		t.Fatalf("log message = %q, want manager recovery skipped", message)
	}
	if failure != nil {
		t.Fatalf("log failure = %#v, want nil", failure)
	}
	if got := detailString(detail["decision_outcome"]); got != "allowed" {
		t.Fatalf("decision_outcome = %q, want allowed", got)
	}
	if got := detailString(detail["decision_reason_code"]); got != string(startupRecoveryReasonDisabledWithManagerWork) {
		t.Fatalf("decision_reason_code = %q, want %q", got, startupRecoveryReasonDisabledWithManagerWork)
	}
	if !detailBool(detail["manager_recoverable_work_present"]) {
		t.Fatalf("manager_recoverable_work_present = %#v, want true", detail["manager_recoverable_work_present"])
	}
	if detailBool(detail["startup_blocking_recoverable_work_present"]) {
		t.Fatalf("startup_blocking_recoverable_work_present = %#v, want false", detail["startup_blocking_recoverable_work_present"])
	}
	if got := detailInt(detail["persisted_agent_count"]); got != 1 {
		t.Fatalf("persisted_agent_count = %d, want 1", got)
	}
	if got := detailInt(detail["persisted_flow_instance_route_count"]); got != 1 {
		t.Fatalf("persisted_flow_instance_route_count = %d, want 1", got)
	}
	if !detailBool(detail["replay_eligible_event_present"]) {
		t.Fatalf("replay_eligible_event_present = %#v, want true", detail["replay_eligible_event_present"])
	}
	classes := detailClasses(detail["recoverable_work_classes"])
	assertContainsClass(t, classes, "persisted agents")
	assertContainsClass(t, classes, "persisted flow instance routes")
	assertContainsClass(t, classes, "events missing pipeline receipts")
}

func TestRuntimeStart_RecoveryEnabledEmitsAllowedDecisionSummary(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)
	scheduleStore := &recordingRuntimeScheduleStore{
		active: []runtimepipeline.Schedule{{
			AgentID:   "runtime",
			EventType: "timer.check",
			Mode:      "once",
			At:        time.Now().Add(time.Minute),
			TaskID:    "recover-me",
		}},
	}

	rt, err := newScopedTestRuntime(t, ctx, RuntimeDeps{Config: testRecoveryDiagnosticsConfig(true), Stores: Stores{
		SQLDB:           db,
		PipelineStore:   runtimepipeline.NewWorkflowInstanceStore(db),
		DeliveryStore:   newRuntimeShutdownDeliveryStore(t),
		RuntimeLogStore: runtimeLogPersistenceStub{db: db},
		EventStore:      startupRecoveryMinimalEventStore{},
		ManagerStore:    &recoveryGuardManagerStore{},
		ScheduleStore:   scheduleStore,
	}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := rt.Shutdown(); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	}()

	level, _, failure, detail := latestStartupRecoveryDecisionLog(t, db)
	if level != "info" {
		t.Fatalf("log level = %q, want info", level)
	}
	if failure != nil {
		t.Fatalf("log failure = %#v, want nil", failure)
	}
	if got := detailString(detail["decision_outcome"]); got != "allowed" {
		t.Fatalf("decision_outcome = %q, want allowed", got)
	}
	if got := detailString(detail["decision_reason_code"]); got != string(startupRecoveryReasonEnabledWithWork) {
		t.Fatalf("decision_reason_code = %q, want %q", got, startupRecoveryReasonEnabledWithWork)
	}
	if !detailBool(detail["schedule_restore_attempted"]) {
		t.Fatalf("schedule_restore_attempted = %#v, want true", detail["schedule_restore_attempted"])
	}
	if got := detailInt(detail["schedule_replayed_count"]); got != 1 {
		t.Fatalf("schedule_replayed_count = %d, want 1", got)
	}
	if got := detailInt(detail["schedule_skipped_count"]); got != 0 {
		t.Fatalf("schedule_skipped_count = %d, want 0", got)
	}
	if got := detailInt(detail["schedule_dropped_count"]); got != 0 {
		t.Fatalf("schedule_dropped_count = %d, want 0", got)
	}
	assertContainsClass(t, detailClasses(detail["recoverable_work_classes"]), "active schedules")
}

func TestRuntimeStart_RecoveryEnabledEmitsTimerRecoveryAftermathAndSummary(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)
	scheduleStore := &recordingRuntimeScheduleStore{
		active: []runtimepipeline.Schedule{
			{
				AgentID:   "runtime",
				EventType: "timer.replay",
				Mode:      "once",
				At:        time.Now().Add(time.Minute),
				TaskID:    "replay-me",
			},
			{
				AgentID:   "runtime",
				EventType: "timer.skip",
				Mode:      "once",
				At:        time.Now().Add(2 * time.Minute),
				TaskID:    "skip-me",
			},
			{
				AgentID:   "runtime",
				EventType: "timer.drop",
				Mode:      "once",
				At:        time.Now().Add(3 * time.Minute),
				TaskID:    "drop-me",
			},
		},
		claims: []recordedScheduleClaim{
			{Claimed: true},
			{Claimed: false},
			{Err: errors.New("claim failed")},
		},
	}

	rt, err := newScopedTestRuntime(t, ctx, RuntimeDeps{Config: testRecoveryDiagnosticsConfig(true), Stores: Stores{
		SQLDB:           db,
		PipelineStore:   runtimepipeline.NewWorkflowInstanceStore(db),
		DeliveryStore:   newRuntimeShutdownDeliveryStore(t),
		RuntimeLogStore: runtimeLogPersistenceStub{db: db},
		EventStore:      startupRecoveryMinimalEventStore{},
		ManagerStore:    &recoveryGuardManagerStore{},
		ScheduleStore:   scheduleStore,
	}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := rt.Shutdown(); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	}()

	level, _, failure, detail := latestStartupRecoveryDecisionLog(t, db)
	if level != "error" {
		t.Fatalf("log level = %q, want error", level)
	}
	requireFailureCode(t, failure, "schedule_restore_failed")
	if got := detailString(detail["decision_outcome"]); got != "degraded" {
		t.Fatalf("decision_outcome = %q, want degraded", got)
	}
	if got := detailString(detail["decision_reason_code"]); got != string(startupRecoveryReasonScheduleRestore) {
		t.Fatalf("decision_reason_code = %q, want %q", got, startupRecoveryReasonScheduleRestore)
	}
	if got := detailInt(detail["schedule_replayed_count"]); got != 1 {
		t.Fatalf("schedule_replayed_count = %d, want 1", got)
	}
	if got := detailInt(detail["schedule_skipped_count"]); got != 1 {
		t.Fatalf("schedule_skipped_count = %d, want 1", got)
	}
	if got := detailInt(detail["schedule_dropped_count"]); got != 1 {
		t.Fatalf("schedule_dropped_count = %d, want 1", got)
	}

	logs := listRuntimeLogsByAction(t, db, startupTimerRecoveryAction)
	if len(logs) != 3 {
		t.Fatalf("timer recovery runtime logs = %d, want 3", len(logs))
	}
	findByEventType := func(eventType string) runtimeAftermathLog {
		t.Helper()
		for _, entry := range logs {
			if strings.TrimSpace(entry.eventType) == eventType {
				return entry
			}
		}
		t.Fatalf("missing timer recovery log for event type %q in %#v", eventType, logs)
		return runtimeAftermathLog{}
	}

	replayed := findByEventType("timer.replay")
	if replayed.level != "info" {
		t.Fatalf("replayed log level = %q, want info", replayed.level)
	}
	if got := detailString(replayed.detail["decision_outcome"]); got != "replayed" {
		t.Fatalf("replayed decision_outcome = %q, want replayed", got)
	}
	if got := detailString(replayed.detail["decision_reason_code"]); got != string(startupTimerRecoveryReasonRestored) {
		t.Fatalf("replayed decision_reason_code = %q, want %q", got, startupTimerRecoveryReasonRestored)
	}

	skipped := findByEventType("timer.skip")
	if skipped.level != "info" {
		t.Fatalf("skipped log level = %q, want info", skipped.level)
	}
	if got := detailString(skipped.detail["decision_outcome"]); got != "skipped" {
		t.Fatalf("skipped decision_outcome = %q, want skipped", got)
	}
	if got := detailString(skipped.detail["decision_reason_code"]); got != string(startupTimerRecoveryReasonClaimNotAcquired) {
		t.Fatalf("skipped decision_reason_code = %q, want %q", got, startupTimerRecoveryReasonClaimNotAcquired)
	}

	dropped := findByEventType("timer.drop")
	if dropped.level != "warn" {
		t.Fatalf("dropped log level = %q, want warn", dropped.level)
	}
	if got := detailString(dropped.detail["decision_outcome"]); got != "dropped" {
		t.Fatalf("dropped decision_outcome = %q, want dropped", got)
	}
	if got := detailString(dropped.detail["decision_reason_code"]); got != string(startupTimerRecoveryReasonRestoreFailed) {
		t.Fatalf("dropped decision_reason_code = %q, want %q", got, startupTimerRecoveryReasonRestoreFailed)
	}
	requireFailureCode(t, dropped.failure, "schedule_restore_failed")
}

func TestRuntimeStart_RecoveryEnabledEmitsManagerReplayAftermathAndSummary(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)
	managerStore := &startupManagerReplayRuntimeStore{
		agents: []runtimemanager.PersistedAgent{{
			Config:    runtimeactors.AgentConfig{ExecutionMode: "live", ID: "agent-a"},
			StartedAt: time.Now().UTC(),
		}},
	}
	deliveryStore := newRuntimeShutdownDeliveryStore(t)
	runID := eventtest.UUID("runtime-recovery-manager-run")
	for index, eventType := range []string{
		"support.replay.ok",
		"support.replay.skip",
		"support.replay.leased",
		"support.replay.drop",
	} {
		deliveryStore.seedAgentDelivery(t, ctx,
			eventtest.PersistedProjection(eventtest.UUID("runtime-recovery-manager-"+eventType), events.EventType(eventType), "", "", nil, 0, runID, "", events.EventEnvelope{}, time.Now().Add(time.Duration(index-4)*time.Minute).UTC()),
			"agent-a",
		)
	}

	rt, err := newScopedTestRuntime(t, ctx, RuntimeDeps{Config: testRecoveryDiagnosticsConfig(true), Stores: Stores{
		SQLDB:           db,
		PipelineStore:   runtimepipeline.NewWorkflowInstanceStore(db),
		DeliveryStore:   deliveryStore,
		RuntimeLogStore: runtimeLogPersistenceStub{db: db},
		EventStore:      startupRecoveryMinimalEventStore{},
		ManagerStore:    managerStore,
	}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Manager.Shutdown(); err != nil {
		t.Fatalf("retire constructed manager before test replacement: %v", err)
	}
	rt.Manager = runtimemanager.NewAgentManagerWithOptions(rt.Bus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return startupManagerReplayRuntimeAgent{id: cfg.ID}, nil
	}, runtimemanager.AgentManagerOptions{
		SemanticSource:                 module.SemanticSource(),
		RuntimeShutdownAdmissionClosed: rt.shutdownAdmissionClosed,
		WorkOwner:                      rt.WorkOccurrence(),
		DeliveryStore:                  deliveryStore,
	}, managerStore)

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := rt.Shutdown(); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	}()

	level, _, failure, detail := latestStartupRecoveryDecisionLog(t, db)
	if level != "error" {
		t.Fatalf("log level = %q, want error", level)
	}
	requireFailureCode(t, failure, "unclassified_runtime_error")
	if got := detailString(detail["decision_outcome"]); got != "degraded" {
		t.Fatalf("decision_outcome = %q, want degraded", got)
	}
	if got := detailString(detail["decision_reason_code"]); got != string(startupRecoveryReasonRecoverFailed) {
		t.Fatalf("decision_reason_code = %q, want %q", got, startupRecoveryReasonRecoverFailed)
	}
	if got := detailInt(detail["manager_replayed_count"]); got != 2 {
		t.Fatalf("manager_replayed_count = %d, want 2", got)
	}
	if got := detailInt(detail["manager_skipped_count"]); got != 0 {
		t.Fatalf("manager_skipped_count = %d, want 0", got)
	}
	if got := detailInt(detail["manager_dropped_count"]); got != 2 {
		t.Fatalf("manager_dropped_count = %d, want 2", got)
	}

	logs := listRuntimeLogsByAction(t, db, "startup_recovery_manager_replay_aftermath")
	if len(logs) != 4 {
		t.Fatalf("manager replay runtime logs = %d, want 4", len(logs))
	}
	findByEventType := func(eventType string) runtimeAftermathLog {
		t.Helper()
		for _, entry := range logs {
			if strings.TrimSpace(entry.eventType) == eventType {
				return entry
			}
		}
		t.Fatalf("missing manager replay log for event type %q in %#v", eventType, logs)
		return runtimeAftermathLog{}
	}
	replayed := findByEventType("support.replay.ok")
	if got := detailString(replayed.detail["decision_outcome"]); got != "replayed" {
		t.Fatalf("replayed decision_outcome = %q, want replayed", got)
	}
	replayedSecond := findByEventType("support.replay.skip")
	if got := detailString(replayedSecond.detail["decision_reason_code"]); got != "persisted_event_replayed" {
		t.Fatalf("second replay decision_reason_code = %q, want persisted_event_replayed", got)
	}
	droppedLeased := findByEventType("support.replay.leased")
	if got := detailString(droppedLeased.detail["decision_outcome"]); got != "dropped" {
		t.Fatalf("leased decision_outcome = %q, want dropped without prose classification", got)
	}
	if got := detailString(droppedLeased.detail["decision_reason_code"]); got != "event_processing_failed" {
		t.Fatalf("leased decision_reason_code = %q, want event_processing_failed", got)
	}
	requireFailureCode(t, droppedLeased.failure, "unclassified_runtime_error")
	dropped := findByEventType("support.replay.drop")
	if got := detailString(dropped.detail["decision_outcome"]); got != "dropped" {
		t.Fatalf("dropped decision_outcome = %q, want dropped", got)
	}
	requireFailureCode(t, dropped.failure, "unclassified_runtime_error")
}

func TestRuntimeStart_RecoveryFailureEmitsDegradedDecisionSummary(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)
	eventStore := &startupRecoveryEventStore{
		missing: []events.PersistedReplayEvent{{
			Event: eventtest.RunCreatingRootIngress(eventtest.UUID("startup-recovery-claim-failure"), "support.item_created", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
		}},
		claimErr: errors.New("claim failed"),
	}

	rt, err := newScopedTestRuntime(t, ctx, RuntimeDeps{Config: testRecoveryDiagnosticsConfig(true), Stores: Stores{
		SQLDB:           db,
		PipelineStore:   runtimepipeline.NewWorkflowInstanceStore(db),
		DeliveryStore:   newRuntimeShutdownDeliveryStore(t),
		RuntimeLogStore: runtimeLogPersistenceStub{db: db},
		EventStore:      eventStore,
		ManagerStore:    &recoveryGuardManagerStore{},
	}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := rt.Shutdown(); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	}()

	level, _, failure, detail := latestStartupRecoveryDecisionLog(t, db)
	if level != "error" {
		t.Fatalf("log level = %q, want error", level)
	}
	requireFailureCode(t, failure, "startup_manager_recovery_failed")
	if got := detailString(detail["decision_outcome"]); got != "degraded" {
		t.Fatalf("decision_outcome = %q, want degraded", got)
	}
	if got := detailString(detail["decision_reason_code"]); got != string(startupRecoveryReasonRecoverFailed) {
		t.Fatalf("decision_reason_code = %q, want %q", got, startupRecoveryReasonRecoverFailed)
	}
	if !detailBool(detail["manager_recovery_attempted"]) {
		t.Fatalf("manager_recovery_attempted = %#v, want true", detail["manager_recovery_attempted"])
	}
	if !detailBool(detail["manager_reset_attempted"]) {
		t.Fatalf("manager_reset_attempted = %#v, want true", detail["manager_reset_attempted"])
	}
	assertContainsClass(t, detailClasses(detail["recoverable_work_classes"]), "events missing pipeline receipts")
}

func TestRuntimeStart_RecoveryInspectionFailureDoesNotBlockRecoveryEnabledStartup(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)

	rt, err := newScopedTestRuntime(t, ctx, RuntimeDeps{Config: testRecoveryDiagnosticsConfig(true), Stores: Stores{
		SQLDB:           db,
		PipelineStore:   runtimepipeline.NewWorkflowInstanceStore(db),
		DeliveryStore:   newRuntimeShutdownDeliveryStore(t),
		RuntimeLogStore: runtimeLogPersistenceStub{db: db},
		EventStore:      startupRecoveryMinimalEventStore{},
		ManagerStore:    startupRecoveryManagerStore{loadErr: errors.New("load agents failed")},
	}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := rt.Shutdown(); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	}()

	level, _, failure, detail := latestStartupRecoveryDecisionLog(t, db)
	if level != "error" {
		t.Fatalf("log level = %q, want error", level)
	}
	requireFailureCode(t, failure, "startup_manager_recovery_failed")
	if got := detailString(detail["decision_outcome"]); got != "degraded" {
		t.Fatalf("decision_outcome = %q, want degraded", got)
	}
	if got := detailString(detail["decision_reason_code"]); got != string(startupRecoveryReasonRecoverFailed) {
		t.Fatalf("decision_reason_code = %q, want %q", got, startupRecoveryReasonRecoverFailed)
	}
	if got := detailBool(detail["recovery_inspection_complete"]); got {
		t.Fatalf("recovery_inspection_complete = %#v, want false", detail["recovery_inspection_complete"])
	}
	requireNestedFailureCode(t, detail, "recovery_inspection_failure", "startup_recovery_inspection_failed")
	if !detailBool(detail["manager_recovery_attempted"]) {
		t.Fatalf("manager_recovery_attempted = %#v, want true", detail["manager_recovery_attempted"])
	}
}

func TestRuntimeStart_InspectionFailurePreservesDecisionErrorAcrossTimerSkipAndDrop(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)
	scheduleStore := &recordingRuntimeScheduleStore{
		active: []runtimepipeline.Schedule{
			{
				AgentID:   "runtime",
				EventType: "timer.skip",
				Mode:      "once",
				At:        time.Now().Add(2 * time.Minute),
				TaskID:    "skip-me",
			},
			{
				AgentID:   "runtime",
				EventType: "timer.drop",
				Mode:      "once",
				At:        time.Now().Add(3 * time.Minute),
				TaskID:    "drop-me",
			},
		},
		claims: []recordedScheduleClaim{
			{Claimed: false},
			{Err: errors.New("claim failed")},
		},
	}
	managerStore := &startupRecoveryFlakyManagerStore{
		remainingFailures: 1,
		loadErr:           errors.New("load agents failed"),
	}

	rt, err := newScopedTestRuntime(t, ctx, RuntimeDeps{Config: testRecoveryDiagnosticsConfig(true), Stores: Stores{
		SQLDB:           db,
		PipelineStore:   runtimepipeline.NewWorkflowInstanceStore(db),
		DeliveryStore:   newRuntimeShutdownDeliveryStore(t),
		RuntimeLogStore: runtimeLogPersistenceStub{db: db},
		EventStore:      startupRecoveryMinimalEventStore{},
		ManagerStore:    managerStore,
		ScheduleStore:   scheduleStore,
	}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := rt.Shutdown(); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	}()

	level, _, failure, detail := latestStartupRecoveryDecisionLog(t, db)
	if level != "error" {
		t.Fatalf("log level = %q, want error", level)
	}
	requireFailureCode(t, failure, "startup_recovery_inspection_failed")
	if got := detailString(detail["decision_outcome"]); got != "degraded" {
		t.Fatalf("decision_outcome = %q, want degraded", got)
	}
	if got := detailString(detail["decision_reason_code"]); got != string(startupRecoveryReasonInspectFailed) {
		t.Fatalf("decision_reason_code = %q, want %q", got, startupRecoveryReasonInspectFailed)
	}
	if got := detailBool(detail["recovery_inspection_complete"]); got {
		t.Fatalf("recovery_inspection_complete = %#v, want false", detail["recovery_inspection_complete"])
	}
	requireNestedFailureCode(t, detail, "recovery_inspection_failure", "startup_recovery_inspection_failed")
	if !detailBool(detail["manager_recovery_attempted"]) {
		t.Fatalf("manager_recovery_attempted = %#v, want true", detail["manager_recovery_attempted"])
	}
	if got := detailInt(detail["schedule_replayed_count"]); got != 0 {
		t.Fatalf("schedule_replayed_count = %d, want 0", got)
	}
	if got := detailInt(detail["schedule_skipped_count"]); got != 1 {
		t.Fatalf("schedule_skipped_count = %d, want 1", got)
	}
	if got := detailInt(detail["schedule_dropped_count"]); got != 1 {
		t.Fatalf("schedule_dropped_count = %d, want 1", got)
	}

	logs := listRuntimeLogsByAction(t, db, startupTimerRecoveryAction)
	if len(logs) != 2 {
		t.Fatalf("timer recovery runtime logs = %d, want 2", len(logs))
	}
	findByEventType := func(eventType string) runtimeAftermathLog {
		t.Helper()
		for _, entry := range logs {
			if strings.TrimSpace(entry.eventType) == eventType {
				return entry
			}
		}
		t.Fatalf("missing timer recovery log for event type %q in %#v", eventType, logs)
		return runtimeAftermathLog{}
	}

	skipped := findByEventType("timer.skip")
	if got := detailString(skipped.detail["decision_outcome"]); got != "skipped" {
		t.Fatalf("skipped decision_outcome = %q, want skipped", got)
	}
	dropped := findByEventType("timer.drop")
	if got := detailString(dropped.detail["decision_outcome"]); got != "dropped" {
		t.Fatalf("dropped decision_outcome = %q, want dropped", got)
	}
	requireFailureCode(t, dropped.failure, "schedule_restore_failed")
}
