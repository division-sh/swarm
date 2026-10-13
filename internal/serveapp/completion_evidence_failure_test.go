package serveapp

import (
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
)

func TestServedCompletion2270CLIFailureNativeBothStores(t *testing.T) {
	for _, format := range []string{"json", "stream-json"} {
		for _, backendName := range []string{"sqlite", "postgres"} {
			t.Run(format+"/"+backendName, func(t *testing.T) {
				backend, actor, eventID := completion2270NativeCLIResult(t, backendName, format, "valid_result_exit", runtimemanager.ReceiptStatusDeadLetter)
				requireClaudeAttemptProofDeliveryFailure(t, backend, eventID, "terminal_failure", runtimefailures.ClassOutcomeUncertain, "claude_cli_attempt_outcome_unconfirmed")
				turn := completion2270NativeCLIPublicTurn(t, backend, actor)
				if turn.Failure == nil || turn.Failure.Class != runtimefailures.ClassOutcomeUncertain || turn.ResolvedModel != "claude-proof" || turn.Tokens == nil || turn.Tokens.Input != 12 || turn.Tokens.Output != 3 || turn.CompletionEvidence.ProviderReportedCostUSD == nil || *turn.CompletionEvidence.ProviderReportedCostUSD != 0.001 || turn.CompletionEvidence.CostProvenance != runtimeeffects.CompletionCostReported {
					t.Fatalf("native failure lost independent terminal facts: %+v", turn)
				}
				usage, err := backend.store.(operatorread.AgentUsageReader).LoadOperatorAgentUsage(backend.context(), claudeAttemptProofAgentConfig().Identity, operatorread.OperatorAgentUsageOptions{})
				if err != nil || usage.Usage.Exact.LedgerEntries != 1 || usage.Usage.Exact.ProviderReportedCostUSD != 0.001 {
					t.Fatalf("native failure spend was lost/repeated: %+v, %v", usage, err)
				}
			})
		}
	}
}

func TestServedCompletion2270InvalidFinalCLIResultBothStores(t *testing.T) {
	for _, backendName := range []string{"sqlite", "postgres"} {
		t.Run(backendName, func(t *testing.T) {
			backend, actor, _ := completion2270NativeCLIResult(t, backendName, "stream-json", "invalid_final_result", runtimemanager.ReceiptStatusProcessed)
			turn := completion2270NativeCLIPublicTurn(t, backend, actor)
			if turn.ResolvedModel != "" || turn.Tokens != nil || len(turn.CompletionEvidence.Models) != 0 || turn.CompletionEvidence.ProviderReportedCostUSD != nil || len(turn.CompletionEvidence.Problems) != 3 {
				t.Fatalf("streaming settlement resurrected prior terminal facts: %+v", turn)
			}
		})
	}
}

func TestServedCompletion2270CLIProtocolKeysBothStores(t *testing.T) {
	for _, backendName := range []string{"sqlite", "postgres"} {
		t.Run(backendName, func(t *testing.T) {
			backend, actor, _ := completion2270NativeCLIResult(t, backendName, "stream-json", "case_variant_usage", runtimemanager.ReceiptStatusProcessed)
			turn := completion2270NativeCLIPublicTurn(t, backend, actor)
			if turn.Failure != nil || turn.ResolvedModel != "exact-terminal" || turn.Tokens == nil || turn.Tokens.Input != 12 || turn.Tokens.Output != 3 || turn.CompletionEvidence.ProviderReportedCostUSD == nil || *turn.CompletionEvidence.ProviderReportedCostUSD != 0.3 || len(turn.CompletionEvidence.Problems) != 0 {
				t.Fatalf("native settlement adopted case-variant usage metadata: %+v", turn)
			}
			usage, err := backend.store.(operatorread.AgentUsageReader).LoadOperatorAgentUsage(backend.context(), claudeAttemptProofAgentConfig().Identity, operatorread.OperatorAgentUsageOptions{})
			if err != nil || usage.Usage.Exact.LedgerEntries != 1 || usage.Usage.Exact.ProviderReportedCostUSD != 0.3 {
				t.Fatalf("exact token repair changed native price/cardinality: %+v, %v", usage, err)
			}
		})
	}
}

func completion2270NativeCLIResult(t *testing.T, backendName, format, mode string, want runtimemanager.ReceiptStatus) (claudeAttemptProofBackend, string, string) {
	t.Helper()
	backend := newClaudeAttemptProofBackend(t, backendName)
	t.Setenv("SWARM_CLAUDE_USE_MCP", "0")
	capture := t.TempDir()
	t.Setenv("SWARM_CLAUDE_ATTEMPT_PROOF_CAPTURE", capture)
	t.Setenv("SWARM_CLAUDE_ATTEMPT_PROOF_MODE", mode)
	docker := filepath.Join(t.TempDir(), "docker")
	writeClaudeAttemptProofDocker(t, docker)
	var calls atomic.Int32
	surface := claudeAttemptProofSurface{name: format, memory: true, outputFormat: format}
	manager, bus, _ := newClaudeAttemptProofManager(t, backend, docker, &calls, surface)
	runClaudeAttemptProofManager(t, backend, manager)
	t.Cleanup(func() {
		if err := manager.Shutdown(); err != nil {
			t.Errorf("shutdown native CLI proof: %v", err)
		}
	})
	eventID := publishClaudeAttemptProofEvent(t, backend, bus, surface)
	receipt := waitClaudeAttemptProofReceipt(t, backend, eventID, want, &calls)
	if calls.Load() != 1 || readClaudeAttemptProofCount(t, capture) != 1 || receipt.RetryCount != 0 {
		t.Fatalf("CLI result was redispatched: callbacks=%d receipt=%+v", calls.Load(), receipt)
	}
	if mode == "valid_result_exit" && loadClaudeAttemptProofProviderHead(t, backend) != "" {
		t.Fatal("process error adopted a successful provider head")
	}
	return backend, claudeAttemptProofAgentConfig(surface).ID, eventID
}

func completion2270NativeCLIPublicTurn(t *testing.T, backend claudeAttemptProofBackend, agentID string) operatorread.OperatorPublicConversationTurn {
	t.Helper()
	reads := backend.store.(operatorread.ConversationReader)
	conversations, err := reads.ListOperatorConversations(backend.context(), operatorread.OperatorConversationListOptions{AgentID: agentID})
	if err != nil || len(conversations.Conversations) != 1 {
		t.Fatalf("native conversation: %+v, %v", conversations, err)
	}
	page, err := reads.ListOperatorConversationTurns(backend.context(), operatorread.OperatorConversationTurnListOptions{SessionID: conversations.Conversations[0].SessionID})
	if err != nil || len(page.Turns) != 1 {
		t.Fatalf("native turn page: %+v, %v", page, err)
	}
	detail, err := reads.LoadOperatorPublicConversationTurn(backend.context(), conversations.Conversations[0].SessionID, page.Turns[0].TurnID)
	if err != nil {
		t.Fatal(err)
	}
	return detail.Turn
}
