package conformance

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeagents "github.com/division-sh/swarm/internal/runtime/agents"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/core/ownership"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimesemanticview "github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	canonicalrouting "github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type sessionScopeConformanceCase struct {
	name               string
	actor              runtimeactors.AgentConfig
	bootInFlow         bool
	runtimeScopeKey    string
	bootErrContains    string
	buildErrContains   string
	acquireErrContains string
	persistErrContains string
	recoverErrContains string
	expectLoadFound    bool
	expectScope        string
	expectScopeKey     string
}

func TestSessionScopeConformance(t *testing.T) {
	canonicalrouting.ProveSource(t, canonicalrouting.SourceID("internal/runtime/conformance/session_scope_conformance_test.go:file-scope"))
	canonicalrouting.ProveSource(t, canonicalrouting.SourceID("internal/runtime/conformance/session_scope_conformance_test.go:conformanceAgentYAML"))
	ctx := context.Background()
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}

	cases := []sessionScopeConformanceCase{
		{
			name:               "task_stateless",
			actor:              runtimeactors.AgentConfig{ID: "task-agent", Role: "task_agent", ConversationMode: runtimesessions.RuntimeModeTask.String()},
			expectLoadFound:    false,
			acquireErrContains: "task-scoped sessions are stateless",
		},
		{
			name:               "task_rejects_session_scope",
			actor:              runtimeactors.AgentConfig{ID: "task-invalid", Role: "task_invalid", ConversationMode: runtimesessions.RuntimeModeTask.String(), SessionScope: runtimesessions.SessionScopeGlobal.String()},
			buildErrContains:   "task mode does not use sessions; session_scope must be absent",
			acquireErrContains: "task mode does not use sessions; session_scope must be absent",
			persistErrContains: "task mode does not use sessions; session_scope must be absent",
			recoverErrContains: "task mode does not use sessions; session_scope must be absent",
		},
		{
			name:               "session_global_root_rejected_for_authored_agent",
			actor:              runtimeactors.AgentConfig{ID: "global-agent", Role: "global_agent", ConversationMode: runtimesessions.RuntimeModeSession.String(), SessionScope: runtimesessions.SessionScopeGlobal.String()},
			bootErrContains:    "session_scope flow requires flow-scoped declaration",
			buildErrContains:   "authored normal agents cannot declare session_scope global",
			acquireErrContains: "authored normal agents cannot declare session_scope global",
			persistErrContains: "authored normal agents cannot declare session_scope global",
			recoverErrContains: "authored normal agents cannot declare session_scope global",
		},
		{
			name:               "session_requires_explicit_scope",
			actor:              runtimeactors.AgentConfig{ID: "session-invalid", Role: "session_invalid", ConversationMode: runtimesessions.RuntimeModeSession.String()},
			bootErrContains:    "session_scope flow requires flow-scoped declaration",
			buildErrContains:   "session mode requires explicit session_scope flow",
			acquireErrContains: "session mode requires explicit session_scope flow",
			persistErrContains: "session mode requires explicit session_scope flow",
			recoverErrContains: "session mode requires explicit session_scope flow",
		},
		{
			name:            "session_flow_in_flow",
			actor:           runtimeactors.AgentConfig{ID: "flow-agent", Role: "flow_agent", ConversationMode: runtimesessions.RuntimeModeSession.String(), SessionScope: runtimesessions.SessionScopeFlow.String(), FlowPath: "support/inst-1"},
			bootInFlow:      true,
			expectLoadFound: true,
			expectScope:     runtimesessions.SessionScopeFlow.String(),
			expectScopeKey:  "support/inst-1",
		},
		{
			name:               "session_flow_requires_flow_metadata",
			actor:              runtimeactors.AgentConfig{ID: "flow-root-invalid", Role: "flow_root_invalid", ConversationMode: runtimesessions.RuntimeModeSession.String(), SessionScope: runtimesessions.SessionScopeFlow.String()},
			bootErrContains:    "session_scope flow requires flow-scoped declaration",
			buildErrContains:   "session_scope flow requires flow path metadata",
			acquireErrContains: "session_scope flow requires actor flow path",
			persistErrContains: "session_scope flow requires actor flow path",
			recoverErrContains: "session_scope flow requires flow path metadata",
		},
		{
			name:            "session_per_entity_in_flow",
			actor:           runtimeactors.AgentConfig{ID: "entity-agent", Role: "entity_agent", ConversationMode: runtimesessions.RuntimeModeSessionPerEntity.String(), SessionScope: runtimesessions.SessionScopeEntity.String(), FlowPath: "support/inst-1"},
			bootInFlow:      true,
			runtimeScopeKey: "11111111-1111-1111-1111-111111111111",
			expectLoadFound: true,
			expectScope:     runtimesessions.SessionScopeEntity.String(),
			expectScopeKey:  "11111111-1111-1111-1111-111111111111",
		},
		{
			name:               "session_per_entity_rejects_global_scope",
			actor:              runtimeactors.AgentConfig{ID: "entity-global-invalid", Role: "entity_global_invalid", ConversationMode: runtimesessions.RuntimeModeSessionPerEntity.String(), SessionScope: runtimesessions.SessionScopeGlobal.String(), FlowPath: "support/inst-1"},
			bootInFlow:         true,
			buildErrContains:   "session_per_entity does not support global scope",
			acquireErrContains: "session_per_entity does not support global scope",
			persistErrContains: "session_per_entity does not support global scope",
			recoverErrContains: "session_per_entity does not support global scope",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if strings.TrimSpace(tc.actor.Model) == "" {
				tc.actor.Model = "regular"
			}
			assertBootVerificationBoundary(t, tc)
			assertAgentConstructionBoundary(t, tc)
			assertSessionAcquisitionBoundary(t, tc)
			assertConversationPersistenceBoundary(t, ctx, pg, tc)
			assertRecoveryBoundary(t, tc)
		})
	}
}

func assertBootVerificationBoundary(t *testing.T, tc sessionScopeConformanceCase) {
	t.Helper()
	source := loadSessionScopeConformanceSource(t, tc)
	report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
	if strings.TrimSpace(tc.bootErrContains) == "" {
		if report.HasErrors() {
			t.Fatalf("bootverify errors = %#v, want none", report.Errors())
		}
		return
	}
	if !reportContainsSessionScopeError(report.Errors(), tc.bootErrContains) {
		t.Fatalf("bootverify errors = %#v, want session_scope error containing %q", report.Errors(), tc.bootErrContains)
	}
}

func assertAgentConstructionBoundary(t *testing.T, tc sessionScopeConformanceCase) {
	t.Helper()
	_, err := runtimeagents.NewLLMAgent(tc.actor, runtimellm.NoopRuntime{}, conformanceToolExecutor{}, nil)
	assertErrorContains(t, "agent construction", err, tc.buildErrContains)
}

func assertSessionAcquisitionBoundary(t *testing.T, tc sessionScopeConformanceCase) {
	t.Helper()
	registry := runtimesessions.NewInMemoryRegistry(0)
	ctx := runtimeactors.WithActor(context.Background(), tc.actor)
	lease, err := registry.Acquire(ctx, tc.actor.ID, conversationModeOrTask(tc.actor), runtimesessions.NormalizeSessionScope(tc.actor.SessionScope), "conformance", tc.scopeKey())
	if strings.TrimSpace(tc.acquireErrContains) != "" {
		assertErrorContains(t, "session acquisition", err, tc.acquireErrContains)
		return
	}
	if err != nil {
		t.Fatalf("session acquisition: %v", err)
	}
	if lease == nil {
		t.Fatal("session acquisition returned nil lease")
	}
	if lease.SessionScope.String() != tc.expectScope {
		t.Fatalf("lease.SessionScope = %q, want %q", lease.SessionScope, tc.expectScope)
	}
	if lease.ScopeKey != tc.expectScopeKey {
		t.Fatalf("lease.ScopeKey = %q, want %q", lease.ScopeKey, tc.expectScopeKey)
	}
}

func assertConversationPersistenceBoundary(t *testing.T, ctx context.Context, pg *store.PostgresStore, tc sessionScopeConformanceCase) {
	t.Helper()
	ctx = runtimeactors.WithActor(ctx, tc.actor)
	if strings.TrimSpace(tc.persistErrContains) == "" {
		if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
			Config:  tc.actor,
			Status:  "active",
			HiredBy: "session-scope-conformance",
		}); err != nil {
			t.Fatalf("seed persisted agent: %v", err)
		}
	}
	sessionID := uuid.NewString()
	if strings.TrimSpace(tc.persistErrContains) == "" && !conversationModeOrTask(tc.actor).IsStateless() {
		registry := runtimesessions.NewPostgresRegistry(pg.DB, 30*time.Second)
		lease, err := registry.Acquire(ctx, tc.actor.ID, conversationModeOrTask(tc.actor), runtimesessions.NormalizeSessionScope(tc.actor.SessionScope), "conformance", tc.scopeKey())
		if err != nil {
			t.Fatalf("seed live session lease: %v", err)
		}
		sessionID = lease.SessionID
		if err := registry.Release(ctx, lease); err != nil {
			t.Fatalf("release seeded live session lease: %v", err)
		}
	}
	rec := runtimellm.ConversationRecord{
		SessionID:    sessionID,
		AgentID:      tc.actor.ID,
		SessionScope: tc.actor.SessionScope,
		ScopeKey:     tc.scopeKey(),
		Mode:         conversationModeOrTask(tc.actor).String(),
		Messages:     []runtimellm.Message{{Role: "assistant", Content: "ok"}},
		Summary:      "ok",
		TurnCount:    1,
		Status:       "active",
	}
	err := pg.UpsertConversation(ctx, rec)
	if strings.TrimSpace(tc.persistErrContains) != "" {
		assertErrorContains(t, "conversation persistence", err, tc.persistErrContains)
		return
	}
	if err != nil {
		t.Fatalf("conversation persistence: %v", err)
	}

	loaded, ok, err := pg.LoadActiveConversation(ctx, tc.actor.ID, conversationModeOrTask(tc.actor).String(), tc.actor.SessionScope, tc.scopeKey())
	if err != nil {
		t.Fatalf("conversation load: %v", err)
	}
	if ok != tc.expectLoadFound {
		t.Fatalf("conversation load found = %v, want %v", ok, tc.expectLoadFound)
	}
	if !ok {
		return
	}
	if loaded.SessionScope != tc.expectScope {
		t.Fatalf("loaded.SessionScope = %q, want %q", loaded.SessionScope, tc.expectScope)
	}
	if loaded.ScopeKey != tc.expectScopeKey {
		t.Fatalf("loaded.ScopeKey = %q, want %q", loaded.ScopeKey, tc.expectScopeKey)
	}
}

func assertRecoveryBoundary(t *testing.T, tc sessionScopeConformanceCase) {
	t.Helper()
	store := &conformanceRecoveryStore{
		agents: []runtimemanager.PersistedAgent{{
			Config:    tc.actor,
			StartedAt: time.Now().UTC(),
		}},
	}
	bus := &conformanceRecoveryBus{}
	am := runtimemanager.NewAgentManager(bus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return runtimeagents.NewLLMAgent(cfg, runtimellm.NoopRuntime{}, conformanceToolExecutor{}, nil)
	}, store)

	err := am.Recover(context.Background())
	if strings.TrimSpace(tc.recoverErrContains) != "" {
		assertErrorContains(t, "recovery", err, tc.recoverErrContains)
		return
	}
	if err != nil {
		t.Fatalf("recovery: %v", err)
	}
	cfg, ok := am.GetAgentConfig(tc.actor.ID)
	if !ok {
		t.Fatalf("recover did not restore agent %s", tc.actor.ID)
	}
	if cfg.ConversationMode != conversationModeOrTask(tc.actor).String() {
		t.Fatalf("recovered ConversationMode = %q, want %q", cfg.ConversationMode, conversationModeOrTask(tc.actor).String())
	}
	if cfg.SessionScope != tc.actor.SessionScope {
		t.Fatalf("recovered SessionScope = %q, want %q", cfg.SessionScope, tc.actor.SessionScope)
	}
}

func (tc sessionScopeConformanceCase) scopeKey() string {
	if scopeKey := strings.TrimSpace(tc.runtimeScopeKey); scopeKey != "" {
		return scopeKey
	}
	switch strings.TrimSpace(tc.actor.SessionScope) {
	case runtimesessions.SessionScopeGlobal.String():
		return runtimesessions.SessionScopeGlobal.String()
	case runtimesessions.SessionScopeFlow.String():
		return tc.actor.CanonicalFlowPath()
	default:
		return ""
	}
}

func conversationModeOrTask(actor runtimeactors.AgentConfig) runtimesessions.RuntimeMode {
	if mode := strings.TrimSpace(actor.ConversationMode); mode != "" {
		return runtimesessions.NormalizeConversationRuntimeMode(mode)
	}
	return runtimesessions.RuntimeModeTask
}

func reportContainsSessionScopeError(findings []runtimebootverify.Finding, want string) bool {
	want = strings.TrimSpace(want)
	for _, finding := range findings {
		if finding.CheckID != "invalid_field_detection" {
			continue
		}
		if !strings.Contains(finding.Message, "session_scope") {
			continue
		}
		if strings.Contains(finding.Message, want) {
			return true
		}
	}
	return false
}

func assertErrorContains(t *testing.T, boundary string, err error, want string) {
	t.Helper()
	want = strings.TrimSpace(want)
	if want == "" {
		if err != nil {
			t.Fatalf("%s error = %v, want nil", boundary, err)
		}
		return
	}
	if err == nil {
		t.Fatalf("%s error = nil, want substring %q", boundary, want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("%s error = %q, want substring %q", boundary, err.Error(), want)
	}
}

func loadSessionScopeConformanceSource(t *testing.T, tc sessionScopeConformanceCase) runtimesemanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := t.TempDir()
	flows := " []"
	if tc.bootInFlow {
		flows = "\n  - id: support\n    flow: support\n    mode: static"
	}
	writeSessionScopeFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: session-scope-conformance
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:`+flows+`
`)
	writeSessionScopeFixtureFile(t, filepath.Join(root, "entities.yaml"), `
item: {}
`)
	writeSessionScopeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: session-scope-conformance\n")
	writeSessionScopeFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeSessionScopeFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeSessionScopeFixtureFile(t, filepath.Join(root, "events.yaml"), `
item.created:
  entity_id: string
`)
	rootAgents := "{}\n"
	flowAgents := "{}\n"
	if tc.bootInFlow {
		flowAgents = conformanceAgentYAML(tc, "support/item.created")
		writeSessionScopeFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), `
name: support
initial_state: waiting
states:
  - waiting
  - done
`)
		writeSessionScopeFixtureFile(t, filepath.Join(root, "flows", "support", "policy.yaml"), "{}\n")
		writeSessionScopeFixtureFile(t, filepath.Join(root, "flows", "support", "events.yaml"), `
support/item.created:
  entity_id: string
`)
		writeSessionScopeFixtureFile(t, filepath.Join(root, "flows", "support", "agents.yaml"), flowAgents)
	} else {
		rootAgents = conformanceAgentYAML(tc, "item.created")
	}
	writeSessionScopeFixtureFile(t, filepath.Join(root, "agents.yaml"), rootAgents)

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return runtimesemanticview.Wrap(bundle)
}

func conformanceAgentYAML(tc sessionScopeConformanceCase, subscription string) string {
	lines := []string{
		fmt.Sprintf("%s:", tc.actor.ID),
		fmt.Sprintf("  id: %s", tc.actor.ID),
		"  model: regular",
		fmt.Sprintf("  mode: %s", conversationModeOrTask(tc.actor)),
	}
	lines = append(lines, "  subscriptions:", fmt.Sprintf("    - %s", subscription))
	return strings.Join(lines, "\n") + "\n"
}

func writeSessionScopeFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

type conformanceToolExecutor struct{}

func (conformanceToolExecutor) Execute(context.Context, string, any) (any, error) { return nil, nil }
func (conformanceToolExecutor) ToolCapabilitiesForActor(runtimeactors.AgentConfig, []string, map[string]struct{}) toolcapabilities.Set {
	return toolcapabilities.Set{}
}
func (conformanceToolExecutor) ToolDefinitionsForActor(runtimeactors.AgentConfig) []runtimellm.ToolDefinition {
	return nil
}

type conformanceRecoveryBus struct{}

func (*conformanceRecoveryBus) Publish(context.Context, events.Event) error { return nil }
func (*conformanceRecoveryBus) PublishDirect(context.Context, events.Event, []string) error {
	return nil
}
func (*conformanceRecoveryBus) PublishPersistedRecipients(context.Context, events.Event, []string) error {
	return nil
}
func (*conformanceRecoveryBus) Subscribe(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}
func (*conformanceRecoveryBus) Unsubscribe(string)        {}
func (*conformanceRecoveryBus) ResetInMemoryState() error { return nil }
func (*conformanceRecoveryBus) LogRuntime(context.Context, runtimepipeline.RuntimeLogEntry) error {
	return nil
}
func (b *conformanceRecoveryBus) Store() runtimebus.EventStore { return b }
func (*conformanceRecoveryBus) AppendEvent(context.Context, events.Event) error {
	return nil
}
func (*conformanceRecoveryBus) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}
func (*conformanceRecoveryBus) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return []string{}, nil
}
func (*conformanceRecoveryBus) ClaimPipelineReplay(context.Context, string) (runtimeownership.Lease, bool, error) {
	return conformanceRecoveryReplayLease{}, true, nil
}
func (*conformanceRecoveryBus) ListEventsMissingPipelineReceipt(context.Context, time.Time, int) ([]events.PersistedReplayEvent, error) {
	return nil, nil
}

type conformanceRecoveryStore struct {
	agents []runtimemanager.PersistedAgent
}

func (s *conformanceRecoveryStore) UpsertAgent(context.Context, runtimemanager.PersistedAgent) error {
	return nil
}
func (s *conformanceRecoveryStore) LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error) {
	return append([]runtimemanager.PersistedAgent(nil), s.agents...), nil
}
func (*conformanceRecoveryStore) MarkAgentTerminated(context.Context, string) error { return nil }
func (*conformanceRecoveryStore) EnsureEntitySchema(context.Context, string) error  { return nil }
func (*conformanceRecoveryStore) UpsertEventReceipt(context.Context, string, string, runtimemanager.ReceiptStatus, *runtimefailures.Envelope) error {
	return nil
}
func (*conformanceRecoveryStore) ListPendingEventsForAgent(context.Context, string, time.Time, int) ([]events.Event, error) {
	return nil, nil
}
func (*conformanceRecoveryStore) ListPendingSubscribedEvents(context.Context, string, []events.EventType, time.Time, int) ([]events.Event, error) {
	return nil, nil
}

type conformanceRecoveryReplayLease struct{}

func (conformanceRecoveryReplayLease) Key() string                   { return "conformance-replay" }
func (conformanceRecoveryReplayLease) Refresh(context.Context) error { return nil }
func (conformanceRecoveryReplayLease) Release(context.Context) error { return nil }
