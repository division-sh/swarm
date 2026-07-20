package serveapp

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

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const (
	claudeAttemptProofEventType  events.EventType = "claude.attempt.proof.requested"
	claudeAttemptProofRuntimeID                   = "77777777-7777-4777-8777-777777777777"
	claudeAttemptProofBundleHash                  = "bundle-v1:sha256:7777777777777777777777777777777777777777777777777777777777777777"
)

var claudeAttemptProofBundleSourceFact = runtimecorrelation.BundleSourceFact{
	BundleHash:        claudeAttemptProofBundleHash,
	BundleSource:      "ephemeral",
	BundleFingerprint: "sha256:7777777777777777777777777777777777777777777777777777777777777777",
}

func claudeAttemptProofContext() context.Context {
	return runtimeauthoractivity.WithScope(context.Background(), runtimeauthoractivity.BundleScope(
		claudeAttemptProofRuntimeID,
		claudeAttemptProofBundleHash,
	))
}

type claudeAttemptProofSurface struct {
	name         string
	memory       bool
	outputFormat string
}

func defaultClaudeAttemptProofSurface() claudeAttemptProofSurface {
	return claudeAttemptProofSurface{
		name:         "memory_json",
		memory:       true,
		outputFormat: "json",
	}
}

type claudeAttemptProofStore interface {
	runtimebus.EventStore
	runtimemanager.ManagerPersistence
	runtimemanager.AgentLifecyclePersistence
	runtimeeffects.Store
	runtimeeffects.CompletionStore
	runtimeeffects.CompletionHeartbeatStore
	runtimeeffects.RecoveryStore
	runtimellm.ConversationPersistence
	GetEventReceipt(context.Context, string, string) (runtimemanager.EventReceipt, bool, error)
	RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
}

type claudeAttemptProofWorkspace struct{}

func (claudeAttemptProofWorkspace) ResolveWorkspace(context.Context, runtimeactors.AgentConfig) (*workspace.Target, error) {
	return &workspace.Target{Backend: workspace.BackendDocker, Container: "claude-attempt-proof", Workdir: workspace.LogicalWorkspaceMount}, nil
}

type claudeAttemptProofAgent struct {
	runtime      *runtimellm.ClaudeCLIRuntime
	config       runtimeactors.AgentConfig
	calls        *atomic.Int32
	conversation *runtimellm.Conversation
}

type claudeAttemptProofToolExecutor struct{}

func (claudeAttemptProofToolExecutor) Execute(context.Context, string, any) (any, error) {
	return nil, fmt.Errorf("Claude attempt proof declares no callable tools")
}

func (claudeAttemptProofToolExecutor) ToolCapabilitiesForActor(runtimeactors.AgentConfig, []string, map[string]struct{}) toolcapabilities.Set {
	return toolcapabilities.NewSet(nil)
}

func (a *claudeAttemptProofAgent) ID() string { return a.config.ID }

func (*claudeAttemptProofAgent) Type() string { return "claude-attempt-proof" }

func (*claudeAttemptProofAgent) Subscriptions() []events.EventType {
	return []events.EventType{claudeAttemptProofEventType}
}

func (a *claudeAttemptProofAgent) OnEvent(ctx context.Context, evt events.Event) ([]events.Event, error) {
	a.calls.Add(1)
	ctx = runtimeactors.WithActor(ctx, a.config)
	ctx = agentmemory.WithExecution(ctx, a.config.Memory, agentmemory.Identity{
		RunID:        evt.RunID(),
		AgentID:      a.config.ID,
		FlowInstance: a.config.CanonicalFlowPath(),
	})
	if a.conversation == nil {
		a.conversation = runtimellm.NewConversation(a.config.ID, a.config.CanonicalFlowPath(), "Reply exactly ok.", nil, a.config.Memory, 25, a.runtime)
		a.conversation.SetToolExecutor(claudeAttemptProofToolExecutor{})
	}
	_, err := a.conversation.Step(ctx, "Reply exactly ok.")
	return nil, err
}

type claudeAttemptProofBackend struct {
	name     string
	store    claudeAttemptProofStore
	db       *sql.DB
	sessions runtimesessions.Registry
}

type claudeAttemptProofSpendProjection struct{}

func (claudeAttemptProofSpendProjection) ProjectCommittedCompletionSpend(context.Context, runtimeeffects.CompletionSpendProjection) {
}

type claudeAttemptProofProviderHeadFaultStore struct {
	claudeAttemptProofStore
	err error
}

func (s claudeAttemptProofProviderHeadFaultStore) SettleCompletion(context.Context, runtimeeffects.Attempt, runtimeeffects.CompletionSettlement) (runtimeeffects.CompletionSettlementResult, error) {
	return runtimeeffects.CompletionSettlementResult{}, s.err
}

func TestClaudeAttemptStartRejectionRetriesThroughSelectedStore(t *testing.T) {
	for _, backendName := range []string{"sqlite", "postgres"} {
		t.Run(backendName, func(t *testing.T) {
			backend := newClaudeAttemptProofBackend(t, backendName)
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
			runClaudeAttemptProofManager(t, manager)
			t.Cleanup(func() { _ = manager.Shutdown() })

			eventID := publishClaudeAttemptProofEvent(t, eventBus)
			if err := manager.ReplayAgentBacklog(claudeAttemptProofAdmissionContext(t), claudeAttemptProofAgentConfig().ID); err != nil {
				t.Fatalf("process initial Claude proof delivery: %v", err)
			}
			first := waitClaudeAttemptProofReceipt(t, backend, eventID, runtimemanager.ReceiptStatusError, calls)
			if first.RetryCount != 1 || first.Failure == nil || first.Failure.Detail.Code != "claude_cli_process_start_failed" {
				t.Fatalf("first receipt = %#v, want retryable start rejection", first)
			}
			writeClaudeAttemptProofDocker(t, dockerBin)
			makeClaudeAttemptProofDeliveryRetryEligible(t, backend, eventID)
			if err := manager.ReplayAgentBacklog(claudeAttemptProofAdmissionContext(t), claudeAttemptProofAgentConfig().ID); err != nil {
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
	result, err := backend.db.ExecContext(claudeAttemptProofContext(), query, args...)
	if err != nil {
		t.Fatalf("make Claude proof delivery retry eligible: %v", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		t.Fatalf("retry eligibility rows = %d err=%v, want 1", affected, err)
	}
}

func TestClaudePostlaunchFailurePreservesClassificationAndRestartRefusesProviderRedispatch(t *testing.T) {
	for _, backendName := range []string{"sqlite", "postgres"} {
		t.Run(backendName, func(t *testing.T) {
			backend := newClaudeAttemptProofBackend(t, backendName)
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
			runClaudeAttemptProofManager(t, manager)

			eventID := publishClaudeAttemptProofEvent(t, eventBus)
			if err := manager.ReplayAgentBacklog(claudeAttemptProofAdmissionContext(t), claudeAttemptProofAgentConfig().ID); err != nil {
				t.Fatalf("process initial Claude proof delivery: %v", err)
			}
			receipt := waitClaudeAttemptProofReceipt(t, backend, eventID, runtimemanager.ReceiptStatusError, calls)
			if receipt.RetryCount != 1 || receipt.Failure == nil || receipt.Failure.Detail.Code != "claude_cli_process_failed" {
				t.Fatalf("first receipt = %#v, want original retryable connector classification", receipt)
			}
			attempts := loadClaudeAttemptProofAttempts(t, backend)
			if len(attempts) != 1 || attempts[0].state != string(runtimeeffects.StateOutcomeUncertain) {
				t.Fatalf("postlaunch attempts = %#v, want one outcome-uncertain attempt", attempts)
			}
			if err := manager.Shutdown(); err != nil {
				t.Fatalf("shutdown first manager: %v", err)
			}

			makeClaudeAttemptProofDeliveryRetryEligible(t, backend, eventID)
			restarted, _ := newClaudeAttemptProofManager(t, backend, dockerBin, calls)
			recoveryCtx := claudeAttemptProofAdmissionContext(t)
			if _, err := restarted.HydrateForStartup(recoveryCtx); err != nil {
				t.Fatalf("hydrate restarted manager: %v", err)
			}
			if _, err := restarted.ReplayAfterStartupAdmission(recoveryCtx, true); err != nil {
				t.Fatalf("replay restarted manager after admission: %v", err)
			}
			if err := restarted.ReplayAgentBacklog(claudeAttemptProofAdmissionContext(t), claudeAttemptProofAgentConfig().ID); err != nil {
				t.Fatalf("replay postlaunch-failure delivery: %v", err)
			}
			dead := waitClaudeAttemptProofReceipt(t, backend, eventID, runtimemanager.ReceiptStatusDeadLetter, calls)
			if dead.RetryCount != 1 || calls.Load() != 2 || readClaudeAttemptProofCount(t, captureDir) != 1 {
				t.Fatalf("restart replay receipt=%#v agent_calls=%d process_calls=%d, want one refused retry and one provider invocation", dead, calls.Load(), readClaudeAttemptProofCount(t, captureDir))
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
			backend := newClaudeAttemptProofBackend(t, backendName)
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
			runClaudeAttemptProofManager(t, manager)
			eventID := publishClaudeAttemptProofEvent(t, eventBus)
			receipt := waitClaudeAttemptProofReceipt(t, backend, eventID, runtimemanager.ReceiptStatusDeadLetter, calls)
			if receipt.RetryCount != 0 || receipt.Failure == nil || receipt.Failure.Detail.Code != "provider_head_commit_injected" {
				t.Fatalf("provider-head fault receipt = %#v, want original terminal failure", receipt)
			}
			attempts := loadClaudeAttemptProofAttempts(t, backend)
			if len(attempts) != 1 || attempts[0].state != string(runtimeeffects.StateResponseObserved) {
				t.Fatalf("provider-head fault attempts = %#v, want response_observed before recovery", attempts)
			}
			if turns, spend := loadClaudeAttemptProofCompletionRows(t, backend, attempts[0].id); turns != 0 || spend != 0 {
				t.Fatalf("partial atomic settlement turns=%d spend=%d, want 0/0", turns, spend)
			}
			if got := loadClaudeAttemptProofProviderHead(t, backend); got != "" {
				t.Fatalf("provider head = %q after injected commit failure, want empty", got)
			}
			summary, err := baseStore.ReconcileExternalEffectAttempts(claudeAttemptProofContext(), time.Now().UTC().Add(10*time.Minute))
			if err != nil || summary.OutcomeUncertain != 1 {
				t.Fatalf("recover provider-head fault summary=%#v err=%v", summary, err)
			}
			attempts = loadClaudeAttemptProofAttempts(t, backend)
			if len(attempts) != 1 || attempts[0].state != string(runtimeeffects.StateOutcomeUncertain) {
				t.Fatalf("provider-head fault attempts after recovery = %#v, want one outcome_uncertain attempt", attempts)
			}
			if turns, _ := loadClaudeAttemptProofCompletionRows(t, backend, attempts[0].id); turns != 1 {
				t.Fatalf("recovered completion target rows = %d, want 1", turns)
			}
			if got := readClaudeAttemptProofCount(t, captureDir); got != 1 || calls.Load() != 1 {
				t.Fatalf("after commit failure process_count=%d agent_calls=%d, want one", got, calls.Load())
			}
			if err := manager.ReplayAgentBacklog(claudeAttemptProofAdmissionContext(t), claudeAttemptProofAgentConfig().ID); err != nil {
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

func TestClaudeAttemptIdentitySelectedStoreMemoryAndProcessParity(t *testing.T) {
	for _, memory := range []bool{false, true} {
		for _, outputFormat := range []string{"json", "stream-json"} {
			memoryLabel := "stateless"
			if memory {
				memoryLabel = "memory"
			}
			surface := claudeAttemptProofSurface{
				name:         memoryLabel + "_" + outputFormat,
				memory:       memory,
				outputFormat: outputFormat,
			}
			for _, backendName := range []string{"sqlite", "postgres"} {
				t.Run(surface.name+"/"+backendName, func(t *testing.T) {
					backend := newClaudeAttemptProofBackend(t, backendName)
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
					runClaudeAttemptProofManager(t, manager)
					t.Cleanup(func() { _ = manager.Shutdown() })

					eventID := publishClaudeAttemptProofEvent(t, eventBus, surface)
					receipt := waitClaudeAttemptProofReceipt(t, backend, eventID, runtimemanager.ReceiptStatusProcessed, calls)
					if receipt.RetryCount != 0 || calls.Load() != 1 {
						t.Fatalf("%s receipt=%#v failure=%+v agent_calls=%d, want one successful invocation", surface.name, receipt, receipt.Failure, calls.Load())
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
			backend := newClaudeAttemptProofBackend(t, backendName)
			eventBus, workOwner := newClaudeAttemptProofEventBus(t, backend)
			manager := runtimemanager.NewAgentManagerWithOptions(eventBus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
				return claudeAttemptProofChainDepthAgent{id: cfg.ID}, nil
			}, runtimemanager.AgentManagerOptions{BaseContext: claudeAttemptProofContext(), LifecycleStore: backend.store, Sessions: backend.sessions, WorkOwner: workOwner}, backend.store)
			if err := manager.SpawnAgent(claudeAttemptProofAgentConfig()); err != nil {
				t.Fatalf("spawn chain-depth proof agent: %v", err)
			}
			runClaudeAttemptProofManager(t, manager)
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

func newClaudeAttemptProofBackend(t *testing.T, name string) claudeAttemptProofBackend {
	t.Helper()
	switch name {
	case "sqlite":
		spec, err := loadServePlatformSpecDocument(filepath.Join(cliapp.RepoRoot(), defaultPlatformSpecPath))
		if err != nil {
			t.Fatalf("load platform spec: %v", err)
		}
		plans, err := store.GeneratePlatformTableDDLs(spec)
		if err != nil {
			t.Fatalf("generate SQLite schema: %v", err)
		}
		sqliteStore, err := store.NewSQLiteRuntimeStore(filepath.Join(t.TempDir(), "runtime.db"))
		if err != nil {
			t.Fatalf("new SQLite runtime store: %v", err)
		}
		t.Cleanup(func() { _ = sqliteStore.Close() })
		bootstrapSQLiteSchemaForTest(t, claudeAttemptProofContext(), sqliteStore, plans)
		return claudeAttemptProofBackend{name: name, store: sqliteStore, db: sqliteStore.DB, sessions: sqliteStore}
	case "postgres":
		_, db, _ := testutil.StartPostgres(t)
		pg := storetest.AdmitPostgresRuntimeStore(t, db)
		pg.SetSessionLockTTL(time.Minute)
		return claudeAttemptProofBackend{name: name, store: pg, db: db, sessions: pg}
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
	eventBus, workOwner := newClaudeAttemptProofEventBus(t, backend)
	cfg := &config.Config{}
	cfg.Workspace.DockerBin = dockerBin
	cfg.LLM.ClaudeCLI.Command = "claude"
	cfg.LLM.ClaudeCLI.OutputFormat = surface.outputFormat
	runtime := runtimellm.NewClaudeCLIRuntimeWithOptions(
		cfg,
		backend.sessions,
		"claude-proof-worker",
		claudeAttemptProofWorkspace{},
		backend.store,
		eventBus,
		runtimellm.ClaudeCLIRuntimeOptions{
			ProviderCredentials:  runtimellm.NewProviderCredentialResolver(runtimecredentials.NewEnvStore()),
			CompletionController: runtimeeffects.NewCompletionController(backend.store, claudeAttemptProofSpendProjection{}),
		},
	)
	manager := runtimemanager.NewAgentManagerWithOptions(eventBus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return &claudeAttemptProofAgent{runtime: runtime, config: cfg, calls: calls}, nil
	}, runtimemanager.AgentManagerOptions{BaseContext: claudeAttemptProofContext(), LifecycleStore: backend.store, Sessions: backend.sessions, WorkOwner: workOwner}, backend.store)
	return manager, eventBus
}

func claudeAttemptProofAdmissionContext(t testing.TB) context.Context {
	t.Helper()
	admission, err := managedexecution.New(
		managedexecution.KindNormalRuntime,
		"claude-attempt-proof-runtime",
		1,
		"",
		"claude-attempt-proof-actors",
		"claude-attempt-proof-bundle",
		nil,
	)
	if err != nil {
		t.Fatalf("build Claude attempt proof admission: %v", err)
	}
	return managedexecution.WithAdmission(claudeAttemptProofContext(), admission)
}

func runClaudeAttemptProofManager(t testing.TB, manager *runtimemanager.AgentManager) {
	t.Helper()
	if err := manager.Run(claudeAttemptProofAdmissionContext(t)); err != nil {
		t.Fatalf("run Claude attempt proof manager: %v", err)
	}
}

func newClaudeAttemptProofEventBus(t *testing.T, backend claudeAttemptProofBackend) (*runtimebus.EventBus, *worklifetime.RuntimeOccurrence) {
	t.Helper()
	workOwner := newSupervisorTestRuntimeOccurrence(t, claudeAttemptProofBundleHash)
	lease, err := backend.store.RegisterAuthorActivityEventCatalog(
		runtimeauthoractivity.BundleScope(claudeAttemptProofRuntimeID, claudeAttemptProofBundleHash),
		[]runtimeauthoractivity.EventDescriptor{{EventType: string(claudeAttemptProofEventType), Disposition: runtimeauthoractivity.StoryDifferent}},
	)
	if err != nil {
		t.Fatalf("register Claude proof author activity catalog: %v", err)
	}
	t.Cleanup(lease.Release)
	eventBus, err := runtimebus.NewEventBusWithOptions(backend.store, runtimebus.EventBusOptions{
		RuntimeInstanceID: claudeAttemptProofRuntimeID,
		BundleSourceFact:  claudeAttemptProofBundleSourceFact,
		WorkOwner:         workOwner,
	})
	if err != nil {
		t.Fatalf("new Claude proof event bus: %v", err)
	}
	return eventBus, workOwner
}

func claudeAttemptProofAgentConfig(surfaces ...claudeAttemptProofSurface) runtimeactors.AgentConfig {
	surface := defaultClaudeAttemptProofSurface()
	if len(surfaces) > 0 {
		surface = surfaces[0]
	}
	cfg := runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            "claude-attempt-proof-agent", Type: "sonnet", Role: "worker", FlowID: "global", Model: "regular",
		LLMBackend: "claude_cli", Memory: agentmemory.Authored(surface.memory), FlowPath: "proof/inst-1",
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
	evt := eventtest.RunCreatingRootIngress(eventID, claudeAttemptProofEventType, "proof", "", json.RawMessage(`{"request":"run"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	if err := eventBus.PublishDirect(claudeAttemptProofContext(), evt, []string{claudeAttemptProofAgentConfig(surface).ID}); err != nil {
		t.Fatalf("publish Claude proof event: %v", err)
	}
	return eventID
}

func waitClaudeAttemptProofReceipt(t *testing.T, backend claudeAttemptProofBackend, eventID string, want runtimemanager.ReceiptStatus, calls *atomic.Int32) runtimemanager.EventReceipt {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		receipt, found, err := backend.store.GetEventReceipt(claudeAttemptProofContext(), eventID, claudeAttemptProofAgentConfig().ID)
		if err == nil && found && receipt.Status == want {
			return receipt
		}
		if time.Now().After(deadline) {
			var deliveryStatus string
			query := `SELECT COALESCE(status, '') FROM event_deliveries WHERE event_id=? AND subscriber_id=?`
			if backend.name == "postgres" {
				query = `SELECT COALESCE(status, '') FROM event_deliveries WHERE event_id=$1::uuid AND subscriber_id=$2`
			}
			deliveryErr := backend.db.QueryRowContext(claudeAttemptProofContext(), query, eventID, claudeAttemptProofAgentConfig().ID).Scan(&deliveryStatus)
			t.Fatalf("receipt %s did not reach %s: found=%v receipt=%#v failure=%+v err=%v delivery=%q delivery_err=%v agent_calls=%d", eventID, want, found, receipt, receipt.Failure, err, deliveryStatus, deliveryErr, calls.Load())
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
	query := `SELECT attempt_id, attempt_ordinal, state FROM runtime_external_effect_attempts WHERE adapter='claude_cli' ORDER BY attempt_ordinal`
	if backend.name == "postgres" {
		query = `SELECT attempt_id::text, attempt_ordinal, state FROM runtime_external_effect_attempts WHERE adapter='claude_cli' ORDER BY attempt_ordinal`
	}
	rows, err := backend.db.QueryContext(claudeAttemptProofContext(), query)
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

func loadClaudeAttemptProofCompletionRows(t *testing.T, backend claudeAttemptProofBackend, attemptID string) (int, int) {
	t.Helper()
	query := `SELECT (SELECT COUNT(*) FROM agent_turns WHERE completion_attempt_id=?), (SELECT COUNT(*) FROM spend_ledger WHERE external_effect_attempt_id=?)`
	if backend.name == "postgres" {
		query = `SELECT (SELECT COUNT(*) FROM agent_turns WHERE completion_attempt_id=$1::uuid), (SELECT COUNT(*) FROM spend_ledger WHERE external_effect_attempt_id=$1::uuid)`
	}
	var turns, spend int
	args := []any{attemptID, attemptID}
	if backend.name == "postgres" {
		args = args[:1]
	}
	if err := backend.db.QueryRowContext(claudeAttemptProofContext(), query, args...).Scan(&turns, &spend); err != nil {
		t.Fatalf("load Claude completion rows: %v", err)
	}
	return turns, spend
}

func loadClaudeAttemptProofProviderHead(t *testing.T, backend claudeAttemptProofBackend) string {
	t.Helper()
	query := `SELECT COALESCE(json_extract(runtime_state, '$.provider_session_id'), '') FROM agent_sessions WHERE agent_id=? AND status='active'`
	args := []any{claudeAttemptProofAgentConfig().ID}
	if backend.name == "postgres" {
		query = `SELECT COALESCE(runtime_state->>'provider_session_id', '') FROM agent_sessions WHERE agent_id=$1 AND status='active'`
	}
	var head string
	if err := backend.db.QueryRowContext(claudeAttemptProofContext(), query, args...).Scan(&head); err != nil {
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
	if err := backend.db.QueryRowContext(claudeAttemptProofContext(), query, eventID, claudeAttemptProofAgentConfig().ID).Scan(&reason); err != nil {
		t.Fatalf("load Claude delivery reason: %v", err)
	}
	return reason
}

func requireClaudeAttemptProofSessionSurface(t *testing.T, backend claudeAttemptProofBackend, surface claudeAttemptProofSurface, attemptID string) {
	t.Helper()
	if !surface.memory {
		query := `SELECT COUNT(*) FROM agent_sessions WHERE agent_id=?`
		if backend.name == "postgres" {
			query = `SELECT COUNT(*) FROM agent_sessions WHERE agent_id=$1`
		}
		var count int
		if err := backend.db.QueryRowContext(claudeAttemptProofContext(), query, claudeAttemptProofAgentConfig().ID).Scan(&count); err != nil {
			t.Fatalf("load stateless live-memory row count: %v", err)
		}
		if count != 0 {
			t.Fatalf("stateless live-memory rows=%d, want zero", count)
		}
		return
	}

	query := `SELECT flow_instance, memory_enabled, memory_source, COALESCE(json_extract(runtime_state, '$.provider_session_id'), '') FROM agent_sessions WHERE agent_id=? AND status='active'`
	if backend.name == "postgres" {
		query = `SELECT flow_instance, memory_enabled, memory_source, COALESCE(runtime_state->>'provider_session_id', '') FROM agent_sessions WHERE agent_id=$1 AND status='active'`
	}
	var flowInstance, memorySource, providerHead string
	var memoryEnabled bool
	if err := backend.db.QueryRowContext(claudeAttemptProofContext(), query, claudeAttemptProofAgentConfig().ID).Scan(&flowInstance, &memoryEnabled, &memorySource, &providerHead); err != nil {
		t.Fatalf("load %s session surface: %v", surface.name, err)
	}
	if flowInstance != "proof/inst-1" || !memoryEnabled || memorySource != string(agentmemory.SourceAuthored) || providerHead != attemptID {
		t.Fatalf("%s memory=(flow=%q enabled=%v source=%q head=%q), want (proof/inst-1 true authored %q)", surface.name, flowInstance, memoryEnabled, memorySource, providerHead, attemptID)
	}
}

func requireClaudeAttemptProofDeliveryFailure(t *testing.T, backend claudeAttemptProofBackend, eventID, wantReason string, wantClass runtimefailures.Class, wantCode string) {
	t.Helper()
	query := `SELECT COALESCE(reason_code, ''), COALESCE(json_extract(failure, '$.class'), ''), COALESCE(json_extract(failure, '$.detail.code'), '') FROM event_deliveries WHERE event_id=? AND subscriber_id=?`
	if backend.name == "postgres" {
		query = `SELECT COALESCE(reason_code, ''), COALESCE(failure->>'class', ''), COALESCE(failure->'detail'->>'code', '') FROM event_deliveries WHERE event_id=$1::uuid AND subscriber_id=$2`
	}
	var reason, class, code string
	if err := backend.db.QueryRowContext(claudeAttemptProofContext(), query, eventID, claudeAttemptProofAgentConfig().ID).Scan(&reason, &class, &code); err != nil {
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
	fmt.Fprintf(os.Stdout, "{\"type\":\"result\",\"result\":\"ok\",\"session_id\":%q,\"model\":\"claude-proof\",\"total_cost_usd\":0.001,\"usage\":{\"input_tokens\":12,\"output_tokens\":3,\"cache_read_input_tokens\":0,\"cache_creation_input_tokens\":0}}\n", providerSessionID)
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
