package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const (
	claudeAttemptProofEventType events.EventType = "claude.attempt.proof.requested"
	claudeAttemptProofEntityID                   = "11111111-1111-4111-8111-111111111111"
)

type claudeAttemptProofSurface struct {
	name         string
	runtimeMode  runtimesessions.RuntimeMode
	sessionScope runtimesessions.SessionScope
	outputFormat string
}

func defaultClaudeAttemptProofSurface() claudeAttemptProofSurface {
	return claudeAttemptProofSurface{
		name:         "session_json",
		runtimeMode:  runtimesessions.RuntimeModeSession,
		sessionScope: runtimesessions.SessionScopeFlow,
		outputFormat: "json",
	}
}

type claudeAttemptProofStore interface {
	runtimebus.EventStore
	runtimemanager.ManagerPersistence
	runtimemanager.AgentLifecyclePersistence
	runtimeeffects.Store
	runtimeeffects.RecoveryStore
	runtimellm.TurnPersistence
	runtimellm.ConversationPersistence
	GetEventReceipt(context.Context, string, string) (runtimemanager.EventReceipt, bool, error)
}

type claudeAttemptProofWorkspace struct{}

func (claudeAttemptProofWorkspace) ResolveWorkspace(context.Context, runtimeactors.AgentConfig) (*workspace.Target, error) {
	return &workspace.Target{Backend: workspace.BackendDocker, Container: "claude-attempt-proof", Workdir: workspace.LogicalWorkspaceMount}, nil
}

type claudeAttemptProofAgent struct {
	runtime *runtimellm.ClaudeCLIRuntime
	config  runtimeactors.AgentConfig
	calls   *atomic.Int32
	session *runtimellm.Session
}

func (a *claudeAttemptProofAgent) ID() string { return a.config.ID }

func (*claudeAttemptProofAgent) Type() string { return "claude-attempt-proof" }

func (*claudeAttemptProofAgent) Subscriptions() []events.EventType {
	return []events.EventType{claudeAttemptProofEventType}
}

func (a *claudeAttemptProofAgent) OnEvent(ctx context.Context, _ events.Event) ([]events.Event, error) {
	a.calls.Add(1)
	ctx = runtimeactors.WithActor(ctx, a.config)
	scopeKey, err := runtimesessions.DeclaredScopeKey(a.config)
	if err != nil {
		return nil, err
	}
	ctx = runtimesessions.WithScope(ctx, a.config.ConversationMode, a.config.SessionScope, scopeKey)
	if a.session == nil {
		session, err := a.runtime.StartSession(ctx, a.config.ID, "Reply exactly ok.", nil)
		if err != nil {
			return nil, err
		}
		a.session = session
	}
	_, err = a.runtime.ContinueSession(ctx, a.session, runtimellm.Message{Role: "user", Content: "Reply exactly ok."})
	return nil, err
}

type claudeAttemptProofBackend struct {
	name     string
	store    claudeAttemptProofStore
	db       *sql.DB
	sessions runtimesessions.Registry
}

type claudeAttemptProofProviderHeadFaultStore struct {
	claudeAttemptProofStore
	err error
}

func (s claudeAttemptProofProviderHeadFaultStore) SettleExternalAttemptAndPromoteProviderHead(context.Context, runtimeeffects.ProviderHeadSettlement) error {
	return s.err
}

func TestClaudeAttemptStartRejectionRetriesThroughSelectedStore(t *testing.T) {
	for _, backendName := range []string{"sqlite", "postgres"} {
		t.Run(backendName, func(t *testing.T) {
			backend := newClaudeAttemptProofBackend(t, backendName, testutil.PostgresRowState(), testutil.SQLiteFreshFile())
			t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "proof-oauth-token")
			t.Setenv("SWARM_CLAUDE_USE_MCP", "0")
			captureDir := t.TempDir()
			t.Setenv("SWARM_CLAUDE_ATTEMPT_PROOF_CAPTURE", captureDir)
			t.Setenv("SWARM_CLAUDE_ATTEMPT_PROOF_MODE", "success")
			dockerBin := filepath.Join(t.TempDir(), "docker")
			calls := &atomic.Int32{}
			manager, eventBus := newClaudeAttemptProofManager(t, backend, dockerBin, calls)
			if err := manager.SpawnAgent(claudeAttemptProofAgentConfig()); err != nil {
				t.Fatalf("spawn claude proof agent: %v", err)
			}
			manager.Run(context.Background())
			t.Cleanup(func() { _ = manager.Shutdown() })

			eventID := publishClaudeAttemptProofEvent(t, eventBus)
			if err := manager.ReplayAgentBacklog(context.Background(), claudeAttemptProofAgentConfig().ID); err != nil {
				t.Fatalf("process initial Claude proof delivery: %v", err)
			}
			first := waitClaudeAttemptProofReceipt(t, backend, eventID, runtimemanager.ReceiptStatusError, calls)
			if first.RetryCount != 1 || first.Failure == nil || first.Failure.Detail.Code != "claude_cli_process_start_failed" {
				t.Fatalf("first receipt = %#v, want retryable start rejection", first)
			}
			writeClaudeAttemptProofDocker(t, dockerBin)
			makeClaudeAttemptProofDeliveryRetryEligible(t, backend, eventID)
			if err := manager.ReplayAgentBacklog(context.Background(), claudeAttemptProofAgentConfig().ID); err != nil {
				t.Fatalf("replay claude proof delivery: %v", err)
			}
			processed := waitClaudeAttemptProofReceipt(t, backend, eventID, runtimemanager.ReceiptStatusProcessed, calls)
			if processed.RetryCount != 1 || calls.Load() != 2 {
				t.Fatalf("processed receipt=%#v agent_calls=%d, want one real retry", processed, calls.Load())
			}

			attempts := loadClaudeAttemptProofAttempts(t, backend)
			if len(attempts) != 2 || attempts[0].ordinal != 1 || attempts[0].state != string(runtimeeffects.StateTerminalFailure) || attempts[1].ordinal != 2 || attempts[1].state != string(runtimeeffects.StateSettled) || attempts[0].id == attempts[1].id {
				t.Fatalf("attempts = %#v, want terminal ordinal 1 and settled fresh ordinal 2", attempts)
			}
			if got := readClaudeAttemptProofValue(t, filepath.Join(captureDir, "last_session_id")); got != attempts[1].id {
				t.Fatalf("launched provider child = %q, want durable ordinal-2 attempt %q", got, attempts[1].id)
			}
			if got := loadClaudeAttemptProofProviderHead(t, backend); got != attempts[1].id {
				t.Fatalf("confirmed provider head = %q, want %q", got, attempts[1].id)
			}
			if got := readClaudeAttemptProofCount(t, captureDir); got != 1 {
				t.Fatalf("provider process count = %d, want one started process", got)
			}
		})
	}
}

func makeClaudeAttemptProofDeliveryRetryEligible(t *testing.T, backend claudeAttemptProofBackend, eventID string) {
	t.Helper()
	eligibleAt := time.Now().UTC().Add(-2 * time.Minute)
	query := `UPDATE event_deliveries SET delivered_at = ? WHERE event_id = ? AND subscriber_type = 'agent' AND subscriber_id = ?`
	args := []any{eligibleAt, eventID, claudeAttemptProofAgentConfig().ID}
	if backend.name == "postgres" {
		query = `UPDATE event_deliveries SET delivered_at = $1 WHERE event_id = $2 AND subscriber_type = 'agent' AND subscriber_id = $3`
	}
	result, err := backend.db.ExecContext(context.Background(), query, args...)
	if err != nil {
		t.Fatalf("make Claude proof delivery retry eligible: %v", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		t.Fatalf("retry eligibility rows = %d err=%v, want 1", affected, err)
	}
}

func TestClaudePostlaunchFailureTerminalizesAndRestartSkips(t *testing.T) {
	for _, backendName := range []string{"sqlite", "postgres"} {
		t.Run(backendName, func(t *testing.T) {
			backend := newClaudeAttemptProofBackend(t, backendName, testutil.PostgresRowState(), testutil.SQLiteFreshFile())
			t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "proof-oauth-token")
			t.Setenv("SWARM_CLAUDE_USE_MCP", "0")
			captureDir := t.TempDir()
			t.Setenv("SWARM_CLAUDE_ATTEMPT_PROOF_CAPTURE", captureDir)
			t.Setenv("SWARM_CLAUDE_ATTEMPT_PROOF_MODE", "postlaunch_failure")
			dockerBin := filepath.Join(t.TempDir(), "docker")
			writeClaudeAttemptProofDocker(t, dockerBin)
			calls := &atomic.Int32{}
			manager, eventBus := newClaudeAttemptProofManager(t, backend, dockerBin, calls)
			if err := manager.SpawnAgent(claudeAttemptProofAgentConfig()); err != nil {
				t.Fatalf("spawn claude proof agent: %v", err)
			}
			manager.Run(context.Background())

			eventID := publishClaudeAttemptProofEvent(t, eventBus)
			if err := manager.ReplayAgentBacklog(context.Background(), claudeAttemptProofAgentConfig().ID); err != nil {
				t.Fatalf("process initial Claude proof delivery: %v", err)
			}
			receipt := waitClaudeAttemptProofReceipt(t, backend, eventID, runtimemanager.ReceiptStatusDeadLetter, calls)
			if receipt.RetryCount != 0 || receipt.Failure == nil || receipt.Failure.Detail.Code != "claude_cli_attempt_outcome_unconfirmed" {
				t.Fatalf("terminal receipt = %#v, want original outcome-uncertain evidence and zero retries", receipt)
			}
			if reason := loadClaudeAttemptProofDeliveryReason(t, backend, eventID); reason != "terminal_failure" {
				t.Fatalf("terminal delivery reason = %q, want terminal_failure", reason)
			}
			attempts := loadClaudeAttemptProofAttempts(t, backend)
			if len(attempts) != 1 || attempts[0].state != string(runtimeeffects.StateOutcomeUncertain) {
				t.Fatalf("postlaunch attempts = %#v, want one outcome-uncertain attempt", attempts)
			}
			if err := manager.Shutdown(); err != nil {
				t.Fatalf("shutdown first manager: %v", err)
			}

			restarted, _ := newClaudeAttemptProofManager(t, backend, dockerBin, calls)
			if _, err := restarted.RecoverWithStartupReplayDiagnostics(context.Background()); err != nil {
				t.Fatalf("recover restarted manager: %v", err)
			}
			if calls.Load() != 1 || readClaudeAttemptProofCount(t, captureDir) != 1 {
				t.Fatalf("restart replay agent_calls=%d process_calls=%d, want one original invocation", calls.Load(), readClaudeAttemptProofCount(t, captureDir))
			}
			if got := loadClaudeAttemptProofAttempts(t, backend); len(got) != 1 || got[0].id != attempts[0].id || got[0].state != string(runtimeeffects.StateOutcomeUncertain) {
				t.Fatalf("attempts after restart = %#v, want unchanged uncertain attempt", got)
			}
		})
	}
}

func TestClaudeProviderHeadCommitFailureSettlesUncertain(t *testing.T) {
	for _, backendName := range []string{"sqlite", "postgres"} {
		t.Run(backendName, func(t *testing.T) {
			backend := newClaudeAttemptProofBackend(t, backendName, testutil.PostgresRowState(), testutil.SQLiteFreshFile())
			baseStore := backend.store
			backend.store = claudeAttemptProofProviderHeadFaultStore{
				claudeAttemptProofStore: baseStore,
				err: runtimefailures.New(
					runtimefailures.ClassOutcomeUncertain,
					"provider_head_commit_injected",
					"claude-attempt-proof",
					"settle_provider_head",
					nil,
				),
			}
			t.Setenv("SWARM_CLAUDE_USE_MCP", "0")
			captureDir := t.TempDir()
			t.Setenv("SWARM_CLAUDE_ATTEMPT_PROOF_CAPTURE", captureDir)
			t.Setenv("SWARM_CLAUDE_ATTEMPT_PROOF_MODE", "success")
			dockerBin := filepath.Join(t.TempDir(), "docker")
			writeClaudeAttemptProofDocker(t, dockerBin)
			calls := &atomic.Int32{}
			manager, eventBus := newClaudeAttemptProofManager(t, backend, dockerBin, calls)
			if err := manager.SpawnAgent(claudeAttemptProofAgentConfig()); err != nil {
				t.Fatalf("spawn Claude provider-head fault agent: %v", err)
			}
			manager.Run(context.Background())
			eventID := publishClaudeAttemptProofEvent(t, eventBus)
			receipt := waitClaudeAttemptProofReceipt(t, backend, eventID, runtimemanager.ReceiptStatusDeadLetter, calls)
			if receipt.RetryCount != 0 || receipt.Failure == nil || receipt.Failure.Detail.Code != "provider_head_commit_injected" {
				t.Fatalf("provider-head fault receipt = %#v, want original terminal failure", receipt)
			}
			attempts := loadClaudeAttemptProofAttempts(t, backend)
			if len(attempts) != 1 || attempts[0].state != string(runtimeeffects.StateOutcomeUncertain) {
				t.Fatalf("provider-head fault attempts = %#v, want one outcome_uncertain attempt", attempts)
			}
			if got := loadClaudeAttemptProofProviderHead(t, backend); got != "" {
				t.Fatalf("provider head = %q after injected commit failure, want empty", got)
			}
			if got := readClaudeAttemptProofCount(t, captureDir); got != 1 || calls.Load() != 1 {
				t.Fatalf("after commit failure process_count=%d agent_calls=%d, want one", got, calls.Load())
			}
			if err := manager.ReplayAgentBacklog(context.Background(), claudeAttemptProofAgentConfig().ID); err != nil {
				t.Fatalf("replay terminal provider-head fault delivery: %v", err)
			}
			if got := readClaudeAttemptProofCount(t, captureDir); got != 1 || calls.Load() != 1 {
				t.Fatalf("after replay process_count=%d agent_calls=%d, want no redispatch", got, calls.Load())
			}
			if err := manager.Shutdown(); err != nil {
				t.Fatalf("shutdown provider-head fault manager: %v", err)
			}
		})
	}
}

func TestClaudeAttemptIdentitySelectedStoreModeAndProcessParity(t *testing.T) {
	for _, runtimeMode := range []runtimesessions.RuntimeMode{
		runtimesessions.RuntimeModeTask,
		runtimesessions.RuntimeModeSession,
		runtimesessions.RuntimeModeSessionPerEntity,
	} {
		for _, outputFormat := range []string{"json", "stream-json"} {
			surface := claudeAttemptProofSurface{
				name:         runtimeMode.String() + "_" + outputFormat,
				runtimeMode:  runtimeMode,
				outputFormat: outputFormat,
			}
			switch runtimeMode {
			case runtimesessions.RuntimeModeSession:
				surface.sessionScope = runtimesessions.SessionScopeFlow
			case runtimesessions.RuntimeModeSessionPerEntity:
				surface.sessionScope = runtimesessions.SessionScopeEntity
			}
			for _, backendName := range []string{"sqlite", "postgres"} {
				t.Run(surface.name+"/"+backendName, func(t *testing.T) {
					backend := newClaudeAttemptProofBackend(t, backendName, testutil.PostgresRowState(), testutil.SQLiteFreshFile())
					t.Setenv("SWARM_CLAUDE_USE_MCP", "0")
					captureDir := t.TempDir()
					t.Setenv("SWARM_CLAUDE_ATTEMPT_PROOF_CAPTURE", captureDir)
					t.Setenv("SWARM_CLAUDE_ATTEMPT_PROOF_MODE", "success")
					dockerBin := filepath.Join(t.TempDir(), "docker")
					writeClaudeAttemptProofDocker(t, dockerBin)
					calls := &atomic.Int32{}
					manager, eventBus := newClaudeAttemptProofManager(t, backend, dockerBin, calls, surface)
					cfg := claudeAttemptProofAgentConfig(surface)
					if err := manager.SpawnAgent(cfg); err != nil {
						t.Fatalf("spawn Claude %s proof agent: %v", surface.name, err)
					}
					manager.Run(context.Background())
					t.Cleanup(func() { _ = manager.Shutdown() })

					eventID := publishClaudeAttemptProofEvent(t, eventBus, surface)
					receipt := waitClaudeAttemptProofReceipt(t, backend, eventID, runtimemanager.ReceiptStatusProcessed, calls)
					if receipt.RetryCount != 0 || calls.Load() != 1 {
						t.Fatalf("%s receipt=%#v agent_calls=%d, want one successful invocation", surface.name, receipt, calls.Load())
					}
					attempts := loadClaudeAttemptProofAttempts(t, backend)
					if len(attempts) != 1 || attempts[0].ordinal != 1 || attempts[0].state != string(runtimeeffects.StateSettled) {
						t.Fatalf("%s attempts=%#v, want one settled selected-store attempt", surface.name, attempts)
					}
					if got := readClaudeAttemptProofValue(t, filepath.Join(captureDir, "last_session_id")); got != attempts[0].id {
						t.Fatalf("%s provider child=%q, want durable attempt %q", surface.name, got, attempts[0].id)
					}
					if got := readClaudeAttemptProofValue(t, filepath.Join(captureDir, "last_output_format")); got != outputFormat {
						t.Fatalf("%s output format=%q, want %q", surface.name, got, outputFormat)
					}
					requireClaudeAttemptProofSessionSurface(t, backend, surface, attempts[0].id)
				})
			}
		}
	}
}

type claudeAttemptProofChainDepthAgent struct{ id string }

func (a claudeAttemptProofChainDepthAgent) ID() string { return a.id }

func (claudeAttemptProofChainDepthAgent) Type() string { return "claude-attempt-proof-chain-depth" }

func (claudeAttemptProofChainDepthAgent) Subscriptions() []events.EventType {
	return []events.EventType{claudeAttemptProofEventType}
}

func (claudeAttemptProofChainDepthAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, runtimeengine.ErrChainDepthExceeded
}

func TestAgentManagerDirectDeadLetterPersistsCanonicalEnvelopeSelectedStores(t *testing.T) {
	for _, backendName := range []string{"sqlite", "postgres"} {
		t.Run(backendName, func(t *testing.T) {
			backend := newClaudeAttemptProofBackend(t, backendName, testutil.PostgresRowState(), testutil.SQLiteFreshFile())
			eventBus, err := runtimebus.NewEventBus(backend.store)
			if err != nil {
				t.Fatalf("new chain-depth proof event bus: %v", err)
			}
			manager := runtimemanager.NewAgentManagerWithOptions(eventBus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
				return claudeAttemptProofChainDepthAgent{id: cfg.ID}, nil
			}, runtimemanager.AgentManagerOptions{LifecycleStore: backend.store, Sessions: backend.sessions}, backend.store)
			if err := manager.SpawnAgent(claudeAttemptProofAgentConfig()); err != nil {
				t.Fatalf("spawn chain-depth proof agent: %v", err)
			}
			manager.Run(context.Background())
			t.Cleanup(func() { _ = manager.Shutdown() })

			eventID := publishClaudeAttemptProofEvent(t, eventBus)
			receipt := waitClaudeAttemptProofReceipt(t, backend, eventID, runtimemanager.ReceiptStatusDeadLetter, &atomic.Int32{})
			if receipt.RetryCount != 0 || receipt.Failure == nil || receipt.Failure.Class != runtimefailures.ClassChainDepthExceeded || receipt.Failure.Detail.Code != "chain_depth_limit" {
				t.Fatalf("chain-depth receipt=%#v, want canonical direct dead-letter envelope", receipt)
			}
			requireClaudeAttemptProofDeliveryFailure(t, backend, eventID, "dead_letter", runtimefailures.ClassChainDepthExceeded, "chain_depth_limit")
		})
	}
}

func newClaudeAttemptProofBackend(t *testing.T, name string, requirement testutil.DatabaseRequirement, sqliteRequirement testutil.DatabaseRequirement) claudeAttemptProofBackend {
	t.Helper()
	switch name {
	case "sqlite":
		spec, err := loadServePlatformSpecDocument(filepath.Join(repoRoot(), defaultPlatformSpecPath))
		if err != nil {
			t.Fatalf("load platform spec: %v", err)
		}
		plans, err := store.GeneratePlatformTableDDLs(spec)
		if err != nil {
			t.Fatalf("generate SQLite schema: %v", err)
		}
		sqliteStore, err := store.NewSQLiteRuntimeStore(testutil.SQLiteDeclaredPath(t, sqliteRequirement, filepath.Join(t.TempDir(), "runtime.db")))
		if err != nil {
			t.Fatalf("new SQLite runtime store: %v", err)
		}
		t.Cleanup(func() { _ = sqliteStore.Close() })
		bootstrapSQLiteSchemaForTest(t, context.Background(), sqliteStore, plans)
		return claudeAttemptProofBackend{name: name, store: sqliteStore, db: sqliteStore.DB, sessions: sqliteStore}
	case "postgres":
		_, db, _ := testutil.AcquirePostgres(t, requirement)
		pg := &store.PostgresStore{DB: db}
		return claudeAttemptProofBackend{name: name, store: pg, db: db, sessions: runtimesessions.NewPostgresRegistry(db, time.Minute)}
	default:
		t.Fatalf("unknown Claude proof backend %q", name)
		return claudeAttemptProofBackend{}
	}
}

func newClaudeAttemptProofManager(t *testing.T, backend claudeAttemptProofBackend, dockerBin string, calls *atomic.Int32, surfaces ...claudeAttemptProofSurface) (*runtimemanager.AgentManager, *runtimebus.EventBus) {
	t.Helper()
	surface := defaultClaudeAttemptProofSurface()
	if len(surfaces) > 0 {
		surface = surfaces[0]
	}
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "claude-attempt-proof-token")
	eventBus, err := runtimebus.NewEventBus(backend.store)
	if err != nil {
		t.Fatalf("new Claude proof event bus: %v", err)
	}
	cfg := &config.Config{}
	cfg.Workspace.DockerBin = dockerBin
	cfg.LLM.ClaudeCLI.Command = "claude"
	cfg.LLM.ClaudeCLI.OutputFormat = surface.outputFormat
	runtime := runtimellm.NewClaudeCLIRuntimeWithOptions(
		cfg,
		backend.sessions,
		"claude-proof-worker",
		backend.store,
		nil,
		claudeAttemptProofWorkspace{},
		backend.store,
		eventBus,
		runtimellm.ClaudeCLIRuntimeOptions{
			ProviderCredentials: runtimellm.NewProviderCredentialResolver(runtimecredentials.NewEnvStore()),
		},
	)
	manager := runtimemanager.NewAgentManagerWithOptions(eventBus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return &claudeAttemptProofAgent{runtime: runtime, config: cfg, calls: calls}, nil
	}, runtimemanager.AgentManagerOptions{LifecycleStore: backend.store, Sessions: backend.sessions}, backend.store)
	return manager, eventBus
}

func claudeAttemptProofAgentConfig(surfaces ...claudeAttemptProofSurface) runtimeactors.AgentConfig {
	surface := defaultClaudeAttemptProofSurface()
	if len(surfaces) > 0 {
		surface = surfaces[0]
	}
	cfg := runtimeactors.AgentConfig{
		ID: "claude-attempt-proof-agent", Type: "sonnet", Role: "worker", Mode: "global", Model: "regular",
		LLMBackend: "claude_cli", ConversationMode: surface.runtimeMode.String(), SessionScope: surface.sessionScope.String(), FlowPath: "proof/inst-1",
	}
	if surface.runtimeMode == runtimesessions.RuntimeModeSessionPerEntity {
		cfg.EntityID = claudeAttemptProofEntityID
	}
	return cfg
}

func publishClaudeAttemptProofEvent(t *testing.T, eventBus *runtimebus.EventBus, surfaces ...claudeAttemptProofSurface) string {
	t.Helper()
	surface := defaultClaudeAttemptProofSurface()
	if len(surfaces) > 0 {
		surface = surfaces[0]
	}
	eventID := uuid.NewString()
	envelope := events.EventEnvelope{}
	if surface.runtimeMode == runtimesessions.RuntimeModeSessionPerEntity {
		envelope = events.EnvelopeForEntityID(envelope, claudeAttemptProofEntityID)
	}
	evt := eventtest.RootIngress(eventID, claudeAttemptProofEventType, "proof", "", json.RawMessage(`{"request":"run"}`), 0, "", "", envelope, time.Now().UTC())
	if err := eventBus.PublishDirect(context.Background(), evt, []string{claudeAttemptProofAgentConfig(surface).ID}); err != nil {
		t.Fatalf("publish Claude proof event: %v", err)
	}
	return eventID
}

func waitClaudeAttemptProofReceipt(t *testing.T, backend claudeAttemptProofBackend, eventID string, want runtimemanager.ReceiptStatus, calls *atomic.Int32) runtimemanager.EventReceipt {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		receipt, found, err := backend.store.GetEventReceipt(context.Background(), eventID, claudeAttemptProofAgentConfig().ID)
		if err == nil && found && receipt.Status == want {
			return receipt
		}
		if time.Now().After(deadline) {
			var deliveryStatus string
			query := `SELECT COALESCE(status, '') FROM event_deliveries WHERE event_id=? AND subscriber_id=?`
			if backend.name == "postgres" {
				query = `SELECT COALESCE(status, '') FROM event_deliveries WHERE event_id=$1::uuid AND subscriber_id=$2`
			}
			deliveryErr := backend.db.QueryRowContext(context.Background(), query, eventID, claudeAttemptProofAgentConfig().ID).Scan(&deliveryStatus)
			t.Fatalf("receipt %s did not reach %s: found=%v receipt=%#v err=%v delivery=%q delivery_err=%v agent_calls=%d", eventID, want, found, receipt, err, deliveryStatus, deliveryErr, calls.Load())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type claudeAttemptProofAttempt struct {
	id      string
	ordinal int
	state   string
}

func loadClaudeAttemptProofAttempts(t *testing.T, backend claudeAttemptProofBackend) []claudeAttemptProofAttempt {
	t.Helper()
	query := `SELECT attempt_id, attempt_ordinal, state FROM agent_external_effect_attempts WHERE adapter='claude_cli' ORDER BY attempt_ordinal`
	if backend.name == "postgres" {
		query = `SELECT attempt_id::text, attempt_ordinal, state FROM agent_external_effect_attempts WHERE adapter='claude_cli' ORDER BY attempt_ordinal`
	}
	rows, err := backend.db.QueryContext(context.Background(), query)
	if err != nil {
		t.Fatalf("query Claude attempts: %v", err)
	}
	defer rows.Close()
	var attempts []claudeAttemptProofAttempt
	for rows.Next() {
		var attempt claudeAttemptProofAttempt
		if err := rows.Scan(&attempt.id, &attempt.ordinal, &attempt.state); err != nil {
			t.Fatalf("scan Claude attempt: %v", err)
		}
		attempts = append(attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read Claude attempts: %v", err)
	}
	return attempts
}

func loadClaudeAttemptProofProviderHead(t *testing.T, backend claudeAttemptProofBackend) string {
	t.Helper()
	query := `SELECT COALESCE(json_extract(runtime_state, '$.provider_session_id'), '') FROM agent_sessions WHERE agent_id=? AND status='active'`
	args := []any{claudeAttemptProofAgentConfig().ID}
	if backend.name == "postgres" {
		query = `SELECT COALESCE(runtime_state->>'provider_session_id', '') FROM agent_sessions WHERE agent_id=$1 AND status='active'`
	}
	var head string
	if err := backend.db.QueryRowContext(context.Background(), query, args...).Scan(&head); err != nil {
		t.Fatalf("load Claude provider head: %v", err)
	}
	return head
}

func loadClaudeAttemptProofDeliveryReason(t *testing.T, backend claudeAttemptProofBackend, eventID string) string {
	t.Helper()
	query := `SELECT COALESCE(reason_code, '') FROM event_deliveries WHERE event_id=? AND subscriber_id=?`
	if backend.name == "postgres" {
		query = `SELECT COALESCE(reason_code, '') FROM event_deliveries WHERE event_id=$1::uuid AND subscriber_id=$2`
	}
	var reason string
	if err := backend.db.QueryRowContext(context.Background(), query, eventID, claudeAttemptProofAgentConfig().ID).Scan(&reason); err != nil {
		t.Fatalf("load Claude delivery reason: %v", err)
	}
	return reason
}

func requireClaudeAttemptProofSessionSurface(t *testing.T, backend claudeAttemptProofBackend, surface claudeAttemptProofSurface, attemptID string) {
	t.Helper()
	if surface.runtimeMode == runtimesessions.RuntimeModeTask {
		query := `SELECT COUNT(*) FROM agent_sessions WHERE agent_id=?`
		if backend.name == "postgres" {
			query = `SELECT COUNT(*) FROM agent_sessions WHERE agent_id=$1`
		}
		var count int
		if err := backend.db.QueryRowContext(context.Background(), query, claudeAttemptProofAgentConfig().ID).Scan(&count); err != nil {
			t.Fatalf("load task-mode session count: %v", err)
		}
		if count != 0 {
			t.Fatalf("task mode session rows=%d, want zero", count)
		}
		return
	}

	query := `SELECT runtime_mode, scope, scope_key, COALESCE(json_extract(runtime_state, '$.provider_session_id'), '') FROM agent_sessions WHERE agent_id=? AND status='active'`
	if backend.name == "postgres" {
		query = `SELECT runtime_mode, scope, scope_key, COALESCE(runtime_state->>'provider_session_id', '') FROM agent_sessions WHERE agent_id=$1 AND status='active'`
	}
	var runtimeMode, scope, scopeKey, providerHead string
	if err := backend.db.QueryRowContext(context.Background(), query, claudeAttemptProofAgentConfig().ID).Scan(&runtimeMode, &scope, &scopeKey, &providerHead); err != nil {
		t.Fatalf("load %s session surface: %v", surface.name, err)
	}
	wantScopeKey := "proof/inst-1"
	if surface.runtimeMode == runtimesessions.RuntimeModeSessionPerEntity {
		wantScopeKey = claudeAttemptProofEntityID
	}
	if runtimeMode != surface.runtimeMode.String() || scope != surface.sessionScope.String() || scopeKey != wantScopeKey || providerHead != attemptID {
		t.Fatalf("%s session=(mode=%q scope=%q key=%q head=%q), want (%q %q %q %q)", surface.name, runtimeMode, scope, scopeKey, providerHead, surface.runtimeMode, surface.sessionScope, wantScopeKey, attemptID)
	}
}

func requireClaudeAttemptProofDeliveryFailure(t *testing.T, backend claudeAttemptProofBackend, eventID, wantReason string, wantClass runtimefailures.Class, wantCode string) {
	t.Helper()
	query := `SELECT COALESCE(reason_code, ''), COALESCE(json_extract(failure, '$.class'), ''), COALESCE(json_extract(failure, '$.detail.code'), '') FROM event_deliveries WHERE event_id=? AND subscriber_id=?`
	if backend.name == "postgres" {
		query = `SELECT COALESCE(reason_code, ''), COALESCE(failure->>'class', ''), COALESCE(failure->'detail'->>'code', '') FROM event_deliveries WHERE event_id=$1::uuid AND subscriber_id=$2`
	}
	var reason, class, code string
	if err := backend.db.QueryRowContext(context.Background(), query, eventID, claudeAttemptProofAgentConfig().ID).Scan(&reason, &class, &code); err != nil {
		t.Fatalf("load selected-store delivery failure: %v", err)
	}
	if reason != wantReason || class != string(wantClass) || code != wantCode {
		t.Fatalf("delivery failure=(reason=%q class=%q code=%q), want (%q %q %q)", reason, class, code, wantReason, wantClass, wantCode)
	}
}

func writeClaudeAttemptProofDocker(t *testing.T, path string) {
	t.Helper()
	script := "#!/bin/sh\nSWARM_CLAUDE_ATTEMPT_PROOF_HELPER=1 exec " + strconv.Quote(os.Args[0]) + " -test.run=TestClaudeAttemptProofProcessHelper -- \"$@\"\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write Claude proof Docker shim: %v", err)
	}
}

func TestClaudeAttemptProofProcessHelper(t *testing.T) {
	if os.Getenv("SWARM_CLAUDE_ATTEMPT_PROOF_HELPER") != "1" {
		return
	}
	os.Exit(runClaudeAttemptProofProcessHelper())
}

func runClaudeAttemptProofProcessHelper() int {
	captureDir := strings.TrimSpace(os.Getenv("SWARM_CLAUDE_ATTEMPT_PROOF_CAPTURE"))
	if captureDir == "" {
		fmt.Fprintln(os.Stderr, "SWARM_CLAUDE_ATTEMPT_PROOF_CAPTURE is required")
		return 2
	}
	count := readClaudeAttemptProofCountRaw(captureDir) + 1
	if err := os.WriteFile(filepath.Join(captureDir, "count"), []byte(strconv.Itoa(count)), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	providerSessionID := ""
	outputFormat := ""
	for i, arg := range os.Args {
		if arg == "--session-id" && i+1 < len(os.Args) {
			providerSessionID = strings.TrimSpace(os.Args[i+1])
		}
		if arg == "--output-format" && i+1 < len(os.Args) {
			outputFormat = strings.TrimSpace(os.Args[i+1])
		}
	}
	if providerSessionID == "" {
		fmt.Fprintln(os.Stderr, "--session-id is required")
		return 2
	}
	if err := os.WriteFile(filepath.Join(captureDir, "last_session_id"), []byte(providerSessionID), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if err := os.WriteFile(filepath.Join(captureDir, "last_output_format"), []byte(outputFormat), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if os.Getenv("SWARM_CLAUDE_ATTEMPT_PROOF_MODE") == "postlaunch_failure" {
		fmt.Fprintln(os.Stderr, "injected provider process failure")
		return 1
	}
	fmt.Fprintf(os.Stdout, "{\"type\":\"result\",\"result\":\"ok\",\"session_id\":%q}\n", providerSessionID)
	return 0
}

func readClaudeAttemptProofCount(t *testing.T, captureDir string) int {
	t.Helper()
	return readClaudeAttemptProofCountRaw(captureDir)
}

func readClaudeAttemptProofCountRaw(captureDir string) int {
	raw, err := os.ReadFile(filepath.Join(captureDir, "count"))
	if err != nil {
		return 0
	}
	count, _ := strconv.Atoi(strings.TrimSpace(string(raw)))
	return count
}

func readClaudeAttemptProofValue(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read Claude proof value: %v", err)
	}
	return strings.TrimSpace(string(raw))
}
