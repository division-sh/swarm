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
	runtimeagentframe "github.com/division-sh/swarm/internal/runtime/agentframe"
	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimedeliverycontinuation "github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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

var claudeAttemptProofBundleSourceFact = mustClaudeAttemptProofBundleSourceFact()

func mustClaudeAttemptProofBundleSourceFact() runtimecorrelation.BundleSourceFact {
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact(claudeAttemptProofBundleHash)
	if err != nil {
		panic(err)
	}
	return fact
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
	runtimebus.CommitPublicationOwner
	runtimereplycontext.Store
	runtimerunlifecycle.OperationOwner
	runtimebus.FlowInstanceRoutePersistence
	runtimebus.FlowInstanceRouteRecordReader
	runtimebus.FlowInstanceRouteSetPersistence
	runtimebus.FlowInstanceRouteTopologyPersistence
	runtimebus.FlowInstanceRouteRollbackPersistence
	runtimebus.ActiveAgentDescriptorLister
	runtimebus.ActiveFlowInstanceDescriptorLister
	runtimebus.SelectedRunTargetOwnerLister
	runtimepipeline.WorkflowInstancePersistenceReader
	runtimebus.PreparedPublishEventReader
	runtimebus.TargetFailureDeadLetterRecorder
	runtimebus.RunOriginReader
	runtimemanager.ManagerPersistence
	storetest.AgentFixtureStore
	runtimeeffects.Store
	runtimeeffects.CompletionStore
	runtimeeffects.CompletionHeartbeatStore
	runtimeeffects.RecoveryStore
	runtimesessions.Resetter
	runtimellm.ConversationPersistence
	runtimedelivery.Store
	PipelineObligations() runtimepipelineobligation.Store
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
		RunID: evt.RunID(),
		Agent: a.config.Identity,
	})
	if a.conversation == nil {
		providerPrompt, err := a.config.ProviderPrompt(runtimeagentintent.RuntimeEnvironmentContext())
		if err != nil {
			return nil, err
		}
		providerContract := a.runtime.ProviderContract()
		a.conversation, err = runtimellm.NewManagedConversation(runtimeagentframe.SessionSeed{
			AgentIdentity:  a.config.Identity,
			Role:           a.config.Role,
			FlowID:         a.config.FlowID,
			Intent:         a.config.Intent,
			Criteria:       append([]string{}, a.config.Criteria...),
			ProviderPrompt: providerPrompt,
			RuntimeMode:    providerContract.RuntimeMode,
			Provider:       providerContract.Provider,
			Transport:      string(providerContract.Transport),
			ModelAlias:     a.config.Model,
			Model:          a.config.ResolvedModel,
		}, a.config.CanonicalFlowPath(), nil, a.config.Memory, 25, a.runtime)
		if err != nil {
			return nil, err
		}
		a.conversation.SetToolExecutor(claudeAttemptProofToolExecutor{})
	}
	_, err := a.conversation.RunManaged(ctx, runtimeagentframe.TurnDraft{Kind: runtimeagentframe.TurnInitial, Event: evt})
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
			manager, eventBus, _ := newClaudeAttemptProofManager(t, backend, dockerBin, calls)
			runClaudeAttemptProofManager(t, manager)
			t.Cleanup(func() { _ = manager.Shutdown() })

			eventID := publishClaudeAttemptProofEvent(t, eventBus)
			first := waitClaudeAttemptProofReceipt(t, backend, eventID, runtimemanager.ReceiptStatusError, calls)
			if first.RetryCount != 1 || first.Failure == nil || first.Failure.Detail.Code != "claude_cli_process_start_failed" {
				t.Fatalf("first receipt = %#v, want retryable start rejection", first)
			}
			attempts := loadClaudeAttemptProofAttempts(t, backend)
			if len(attempts) != 1 || attempts[0].ordinal != 1 || attempts[0].state != string(runtimeeffects.StateTerminalFailure) {
				t.Fatalf("attempts after start rejection = %#v, want one terminal attempt", attempts)
			}
			if turns, spend := loadClaudeAttemptProofCompletionRows(t, backend, attempts[0].id); turns != 0 || spend != 0 {
				t.Fatalf("start rejection materialized completion rows turns=%d spend=%d, want 0/0", turns, spend)
			}
			writeClaudeAttemptProofDocker(t, dockerBin)
			makeClaudeAttemptProofDeliveryDueNow(t, backend, eventID)
			eventBus.SignalDeliveryContinuations()
			processed := waitClaudeAttemptProofReceipt(t, backend, eventID, runtimemanager.ReceiptStatusProcessed, calls)
			if processed.RetryCount != 1 || calls.Load() != 2 {
				t.Fatalf("processed receipt=%#v agent_calls=%d, want one real retry", processed, calls.Load())
			}

			attempts = loadClaudeAttemptProofAttempts(t, backend)
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
			if turns, spend := loadClaudeAttemptProofCompletionRows(t, backend, attempts[0].id); turns != 0 || spend != 0 {
				t.Fatalf("prelaunch attempt materialized completion rows turns=%d spend=%d, want 0/0", turns, spend)
			}
			if turns, spend := loadClaudeAttemptProofCompletionRows(t, backend, attempts[1].id); turns != 1 || spend != 1 {
				t.Fatalf("successful retry completion rows turns=%d spend=%d, want exactly 1/1", turns, spend)
			}
		})
	}
}

func makeClaudeAttemptProofDeliveryDueNow(t *testing.T, backend claudeAttemptProofBackend, eventID string) {
	t.Helper()
	eligibleAt := time.Now().UTC().Add(-2 * time.Minute)
	query := `UPDATE event_deliveries SET next_eligible_at = ? WHERE event_id = ? AND subscriber_type = 'agent' AND subscriber_id = ?`
	args := []any{eligibleAt, eventID, claudeAttemptProofAgentConfig().ID}
	if backend.name == "postgres" {
		query = `UPDATE event_deliveries SET next_eligible_at = $1 WHERE event_id = $2 AND subscriber_type = 'agent' AND subscriber_id = $3`
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
			manager, eventBus, coordinator := newClaudeAttemptProofManager(t, backend, dockerBin, calls)
			runClaudeAttemptProofManager(t, manager)

			eventID := publishClaudeAttemptProofEvent(t, eventBus)
			receipt := waitClaudeAttemptProofReceipt(t, backend, eventID, runtimemanager.ReceiptStatusError, calls)
			if receipt.RetryCount != 1 || receipt.Failure == nil || receipt.Failure.Detail.Code != "claude_cli_process_failed" {
				t.Fatalf("first receipt = %#v, want original retryable connector classification", receipt)
			}
			attempts := loadClaudeAttemptProofAttempts(t, backend)
			if len(attempts) != 1 || attempts[0].state != string(runtimeeffects.StateOutcomeUncertain) {
				t.Fatalf("postlaunch attempts = %#v, want one outcome-uncertain attempt", attempts)
			}
			if err := coordinator.Retire(claudeAttemptProofContext()); err != nil {
				t.Fatalf("retire first delivery coordinator: %v", err)
			}
			if err := manager.Shutdown(); err != nil {
				t.Fatalf("shutdown first manager: %v", err)
			}

			makeClaudeAttemptProofDeliveryDueNow(t, backend, eventID)
			restarted, restartedBus, _ := newClaudeAttemptProofManagerForGeneration(t, backend, dockerBin, calls, 2)
			t.Cleanup(func() { _ = restarted.Shutdown() })
			cfg := claudeAttemptProofAgentConfig()
			if _, err := restarted.ResolveAgentConfig(cfg.ID, cfg.CanonicalFlowPath()); err != nil {
				t.Fatalf("restarted manager did not hydrate the Claude proof agent: %v", err)
			}
			runClaudeAttemptProofManager(t, restarted)
			restartedBus.SignalDeliveryContinuations()
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
			manager, eventBus, _ := newClaudeAttemptProofManager(t, backend, dockerBin, calls)
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
			summary, err := baseStore.ReconcileExternalEffectAttempts(
				claudeAttemptProofContext(),
				runtimeeffects.NewRecoveryRequest(time.Now().UTC().Add(10*time.Minute), executionposture.Live),
			)
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
			if got := readClaudeAttemptProofCount(t, captureDir); got != 1 || calls.Load() != 1 {
				t.Fatalf("after terminal settlement process_count=%d agent_calls=%d, want no redispatch", got, calls.Load())
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
					manager, eventBus, _ := newClaudeAttemptProofManager(t, backend, dockerBin, calls, surface)
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
			eventBus, workOwner, _ := newClaudeAttemptProofEventBus(t, backend, 1)
			manager := runtimemanager.NewAgentManagerWithOptions(eventBus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
				return claudeAttemptProofChainDepthAgent{id: cfg.ID}, nil
			}, runtimemanager.AgentManagerOptions{
				ExecutionPosture: executionposture.Live,
				BaseContext:      claudeAttemptProofContext(),
				LifecycleStore:   storetest.AgentLifecycleFixture(t, backend.store),
				DeliveryStore:    backend.store,
				Sessions:         backend.sessions,
				SessionResetter:  backend.store,
				PersistenceRoles: selectedStoreManagerPersistenceRoles(backend.store, eventBus),
				LLMBackend:       "claude_cli",
				WorkOwner:        workOwner, ReceiverExecution: eventreceiver.NormalExecution(),
			}, backend.store)
			installClaudeAttemptProofManagerTopology(t, backend, manager, claudeAttemptProofAgentConfig())
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
		return claudeAttemptProofBackend{name: name, store: sqliteStore, db: storetest.DatabaseForTest(sqliteStore), sessions: sqliteStore}
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

func newClaudeAttemptProofManager(
	t *testing.T,
	backend claudeAttemptProofBackend,
	dockerBin string,
	calls *atomic.Int32,
	surfaces ...claudeAttemptProofSurface,
) (*runtimemanager.AgentManager, *runtimebus.EventBus, *runtimedeliverycontinuation.Coordinator) {
	t.Helper()
	return newClaudeAttemptProofManagerForGeneration(t, backend, dockerBin, calls, 1, surfaces...)
}

func newClaudeAttemptProofManagerForGeneration(
	t *testing.T,
	backend claudeAttemptProofBackend,
	dockerBin string,
	calls *atomic.Int32,
	generation uint64,
	surfaces ...claudeAttemptProofSurface,
) (*runtimemanager.AgentManager, *runtimebus.EventBus, *runtimedeliverycontinuation.Coordinator) {
	t.Helper()
	surface := defaultClaudeAttemptProofSurface()
	if len(surfaces) > 0 {
		surface = surfaces[0]
	}
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "claude-attempt-proof-token")
	eventBus, workOwner, coordinator := newClaudeAttemptProofEventBus(t, backend, generation)
	cfg := &config.Config{Runtime: config.RuntimeConfig{ExecutionPosture: executionposture.Live}}
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
			CompletionController: liveTestCompletionController(backend.store, backend.store, backend.store, claudeAttemptProofSpendProjection{}),
		},
	)
	manager := runtimemanager.NewAgentManagerWithOptions(eventBus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return &claudeAttemptProofAgent{runtime: runtime, config: cfg, calls: calls}, nil
	}, runtimemanager.AgentManagerOptions{
		ExecutionPosture: executionposture.Live,
		BaseContext:      claudeAttemptProofContext(),
		LifecycleStore:   storetest.AgentLifecycleFixture(t, backend.store),
		DeliveryStore:    backend.store,
		SemanticSource:   claudeAttemptProofSemanticSource(),
		Sessions:         backend.sessions,
		SessionResetter:  backend.store,
		PersistenceRoles: selectedStoreManagerPersistenceRoles(backend.store, eventBus),
		LLMBackend:       "claude_cli",
		WorkOwner:        workOwner, ReceiverExecution: eventreceiver.NormalExecution(),
	}, backend.store)
	installClaudeAttemptProofManagerTopology(t, backend, manager, claudeAttemptProofAgentConfig(surface))
	return manager, eventBus, coordinator
}

func installClaudeAttemptProofManagerTopology(t testing.TB, backend claudeAttemptProofBackend, manager *runtimemanager.AgentManager, cfg runtimeactors.AgentConfig) {
	t.Helper()
	registerServeTestDurableAgent(t, backend.store, manager, cfg)
}

func claudeAttemptProofSemanticSource() semanticview.Source {
	cfg := claudeAttemptProofAgentConfig()
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			cfg.ID: {
				ID: cfg.ID, Type: cfg.Type, Role: cfg.Role, Model: cfg.Model,
				ResolvedIntent: cfg.Intent,
			},
		},
	})
}

func claudeAttemptProofAdmissionContext(t testing.TB) context.Context {
	t.Helper()
	return claudeAttemptProofAdmissionContextForGeneration(t, 1)
}

func claudeAttemptProofAdmissionContextForGeneration(t testing.TB, generation uint64) context.Context {
	t.Helper()
	admission, err := managedexecution.New(
		managedexecution.KindNormalRuntime,
		"claude-attempt-proof-runtime",
		generation,
		"",
		"claude-attempt-proof-actors",
		claudeAttemptProofBundleHash,
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

func newClaudeAttemptProofEventBus(
	t *testing.T,
	backend claudeAttemptProofBackend,
	generation uint64,
) (*runtimebus.EventBus, *worklifetime.RuntimeOccurrence, *runtimedeliverycontinuation.Coordinator) {
	t.Helper()
	workOwner := newSupervisorTestRuntimeOccurrence(t, claudeAttemptProofBundleHash)
	authority, err := runtimedelivery.NewNormalExecutionAuthority(
		claudeAttemptProofBundleSourceFact,
		"claude-attempt-proof-runtime",
		generation,
	)
	if err != nil {
		t.Fatalf("construct Claude proof delivery authority: %v", err)
	}
	if err := backend.store.ActivateDeliveryAuthority(claudeAttemptProofContext(), authority); err != nil {
		t.Fatalf("activate Claude proof delivery authority: %v", err)
	}
	lease, err := backend.store.RegisterAuthorActivityEventCatalog(
		runtimeauthoractivity.BundleScope(claudeAttemptProofRuntimeID, claudeAttemptProofBundleHash),
		[]runtimeauthoractivity.EventDescriptor{{EventType: string(claudeAttemptProofEventType), Disposition: runtimeauthoractivity.StoryDifferent}},
	)
	if err != nil {
		t.Fatalf("register Claude proof author activity catalog: %v", err)
	}
	t.Cleanup(lease.Release)
	eventBus, err := runtimebus.NewEventBusWithOptions(backend.store, runtimebus.EventBusOptions{
		ExecutionPosture:    executionposture.Live,
		RuntimeInstanceID:   claudeAttemptProofRuntimeID,
		BundleSourceFact:    claudeAttemptProofBundleSourceFact,
		WorkOwner:           workOwner,
		ReceiverExecution:   eventreceiver.NormalExecution(),
		PipelineObligations: backend.store.PipelineObligations(),
		DeliveryAuthority:   authority,
		Durable: runtimebus.DurableDependencies{
			ReplyContext: backend.store, RunLifecycle: backend.store,
			DeliveryLifecycle: backend.store, FlowRoutes: backend.store, FlowRouteRecords: backend.store,
			FlowRouteSets: backend.store, FlowRouteTopology: backend.store, FlowRouteRollback: backend.store, ActiveAgents: backend.store,
			ActiveFlows: backend.store, TargetOwners: backend.store, WorkflowInstances: backend.store, PreparedEvents: backend.store,
			TargetFailureRecorder: backend.store, RunOrigins: backend.store,
		},
	})
	if err != nil {
		t.Fatalf("new Claude proof event bus: %v", err)
	}
	coordinator, err := runtimedeliverycontinuation.New(
		backend.store,
		authority,
		workOwner,
		eventBus,
		func(_ context.Context, reportErr error) {
			t.Errorf("Claude proof delivery continuation failed: %v", reportErr)
		},
	)
	if err != nil {
		t.Fatalf("construct Claude proof delivery coordinator: %v", err)
	}
	if err := eventBus.SetDeliveryContinuationOwner(coordinator); err != nil {
		t.Fatalf("configure Claude proof delivery coordinator: %v", err)
	}
	if err := coordinator.Start(claudeAttemptProofContext()); err != nil {
		t.Fatalf("start Claude proof delivery coordinator: %v", err)
	}
	t.Cleanup(func() {
		retireCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := coordinator.Retire(retireCtx); err != nil {
			t.Errorf("retire Claude proof delivery coordinator: %v", err)
		}
	})
	return eventBus, workOwner, coordinator
}

func claudeAttemptProofAgentConfig(surfaces ...claudeAttemptProofSurface) runtimeactors.AgentConfig {
	surface := defaultClaudeAttemptProofSurface()
	if len(surfaces) > 0 {
		surface = surfaces[0]
	}
	cfg := runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            "claude-attempt-proof-agent", Type: "sonnet", Role: "worker", FlowID: "global", Model: "regular",
		LLMBackend: "claude_cli", ResolvedLLMBackend: "claude_cli", Memory: agentmemory.Authored(surface.memory), FlowPath: "proof/inst-1",
		Identity: claudeAttemptProofAgentIdentity(),
	}
	return serveTestAgentConfig(cfg)
}

func claudeAttemptProofAgentIdentity() agentidentity.Identity {
	name, err := agentidentity.DeclaredName("claude-attempt-proof-agent", "claude-attempt-proof")
	if err != nil {
		panic(err)
	}
	route, err := agentidentity.PresentRoute("proof", "inst-1", "proof/inst-1")
	if err != nil {
		panic(err)
	}
	identity, err := agentidentity.New(name, route)
	if err != nil {
		panic(err)
	}
	return identity
}

func publishClaudeAttemptProofEvent(t *testing.T, eventBus *runtimebus.EventBus, surfaces ...claudeAttemptProofSurface) string {
	t.Helper()
	surface := defaultClaudeAttemptProofSurface()
	if len(surfaces) > 0 {
		surface = surfaces[0]
	}
	eventID := uuid.NewString()
	evt := eventtest.RunCreatingRootIngress(eventID, claudeAttemptProofEventType, "proof", "", json.RawMessage(`{"request":"run"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	cfg := claudeAttemptProofAgentConfig(surface)
	if err := eventBus.PublishDirectRoutes(claudeAttemptProofContext(), evt, []events.DeliveryRoute{{Recipient: events.MustAgentDeliveryRecipient(cfg.ID), AgentIdentity: cfg.Identity}}); err != nil {
		t.Fatalf("publish Claude proof event: %v", err)
	}
	return eventID
}

func waitClaudeAttemptProofReceipt(t *testing.T, backend claudeAttemptProofBackend, eventID string, want runtimemanager.ReceiptStatus, calls *atomic.Int32) runtimedelivery.Snapshot {
	t.Helper()
	wantStatus := runtimedelivery.StatusPending
	switch want {
	case runtimemanager.ReceiptStatusError:
		wantStatus = runtimedelivery.StatusFailed
	case runtimemanager.ReceiptStatusProcessed:
		wantStatus = runtimedelivery.StatusDelivered
	case runtimemanager.ReceiptStatusDeadLetter, runtimemanager.ReceiptStatusTerminal:
		wantStatus = runtimedelivery.StatusDeadLetter
	default:
		t.Fatalf("unsupported receipt-to-delivery test status %q", want)
	}
	cfg := claudeAttemptProofAgentConfig()
	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(cfg.ID), AgentIdentity: cfg.Identity}
	proof, err := backend.store.ProveHandoff(claudeAttemptProofContext(), eventID, route)
	if err != nil {
		t.Fatalf("prove Claude delivery handoff: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		snapshot, snapshotErr := backend.store.Snapshot(claudeAttemptProofContext(), proof.DeliveryID())
		if snapshotErr == nil && snapshot.Status == wantStatus {
			return snapshot
		}
		if time.Now().After(deadline) {
			t.Fatalf("delivery %s did not reach %s: snapshot=%#v failure=%+v err=%v agent_calls=%d", eventID, wantStatus, snapshot, snapshot.Failure, snapshotErr, calls.Load())
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
