package llm

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// This finite shape ratchet names the execution proofs; it does not substitute
// call-site presence for the request/observation/copy invariants they execute.
func TestCompletion2270EvidenceConsumerCensus(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate evidence consumer census")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "../../.."))
	for _, row := range []struct{ path, function, call, proof string }{
		{"internal/runtime/llm/api_runtime.go", "sendAdmittedRequest", "withCompletionRequest", "TestServedCompletion2270RunServePublicReadbackBothStores"},
		{"internal/runtime/llm/openai_responses_runtime.go", "sendAdmittedRequest", "withCompletionRequest", "TestSelectedCompletion2270EvidenceBothStores"},
		{"internal/runtime/llm/openai_compatible_runtime.go", "sendAdmittedRequest", "withCompletionRequest", "TestSelectedCompletion2270EvidenceBothStores"},
		{"internal/runtime/llm/cli_runtime.go", "continueClaudeTurn", "withCompletionRequest", "TestServedCompletion2270EvidenceBothStores"},
		{"internal/runtime/llm/mock_runtime.go", "executeMockCompletionWithExecutor", "withCompletionRequest", "TestMockCompletionSpendDoesNotConsumeLiveAdmissionCap"},
		{"internal/runtime/llm/api_runtime.go", "continueSession", "apiCompletionUsageFromRaw", "TestCompletion2270APIExecutionEvidence"},
		{"internal/runtime/llm/openai_responses_runtime.go", "continueSession", "apiCompletionUsageFromRaw", "TestCompletion2270APIExecutionEvidence"},
		{"internal/runtime/llm/openai_compatible_runtime.go", "continueSession", "apiCompletionUsageFromRaw", "TestCompletion2270APIExecutionEvidence"},
		{"internal/runtime/llm/cli_runtime.go", "acceptClaudeContinuation", "claudeCompletionUsageFromRaw", "TestCompletion2270AdapterEvidence"},
		{"internal/runtime/llm/cli_runtime.go", "failClaudeContinuation", "claudeFailureUsage", "TestServedCompletion2270CLIFailureNativeBothStores"},
		{"internal/runtime/llm/cli_runtime_process.go", "runWithPreparedInput", "retainClaudeFailureObservations", "TestCompletion2270CLIFailureHandoffBoundaries"},
		{"internal/runtime/llm/cli_runtime_process.go", "runStreamingPrepared", "retainClaudeFailureObservations", "TestServedCompletion2270CLIFailureNativeBothStores"},
		{"internal/runtime/llm/completion_observation.go", "claudeCompletionUsageFromRaw", "selectClaudeCompletionResult", "TestServedCompletion2270BufferedFramingBothStores"},
		{"internal/runtime/llm/completion_observation.go", "selectClaudeCompletionResult", "decodeClaudeCompletionResult", "TestCompletion2270TerminalFraming"},
		{"internal/runtime/llm/completion_observation.go", "decodeClaudeCompletionResult", "completionObservationFieldList", "TestServedCompletion2270InvalidFinalCLIResultBothStores"},
		{"internal/runtime/llm/completion_observation.go", "completionObservationFieldList", "completionObservationDecode", "TestCompletion2270ContributorUnicodeStrictness"},
		{"internal/runtime/llm/completion_observation.go", "observeClaudeTerminalModel", "completionObservationDecode", "TestServedCompletion2270MalformedUnicodeBothStores"},
		{"internal/runtime/llm/completion_observation.go", "observeClaudeContributors", "completionObservationFields", "TestCompletion2270InvalidUTF8LastTerminal"},
		{"internal/runtime/llm/completion_observation.go", "observeClaudeTokens", "decodeClaudeCompletionTokens", "TestServedCompletion2270CLIProtocolKeysBothStores"},
		{"internal/runtime/llm/completion_observation.go", "decodeClaudeCompletionTokens", "completionObservationDecode", "TestCompletion2270TokenProtocolKeysAreExact"},
		{"internal/runtime/llm/completion_authority.go", "settleCompletionTurnWithProviderHead", "Clone", "TestCompletion2270EvidenceCopiesDoNotAlias"},
		{"internal/runtime/llm/completion_authority.go", "completionContinuationForHandle", "completionContinuationForPayload", "TestSettledCompletionRecoveryUsesImmutableProjectedTurn"},
		{"internal/runtime/llm/completion_authority.go", "validateCompletionContinuation", "ValidateEvidence", "TestSettledCompletionRecoveryUsesImmutableProjectedTurn"},
		{"internal/runtime/effects/effects.go", "Controller.Authorize", "BindCompletionRequest", "TestCompletion2270SettlementBothStores"},
		{"internal/runtime/effects/completion.go", "CompletionSettlement.Clone", "Clone", "TestCompletion2270EvidenceCopiesDoNotAlias"},
		{"internal/runtime/effects/completion.go", "ProjectDrainedCompletionSettlement", "Clone", "TestProviderAttemptDrainStartupRecoveryParity"},
		{"internal/runtime/effects/completion.go", "CompletionSettlement.Validate", "ValidateEvidence", "TestCompletion2270EvidenceValidation"},
		{"internal/store/internal/backend/effectpersistence/runtime_external_effects.go", "EffectPostgresOwner.AuthorizeExternalAttempt", "BindCompletionRequest", "TestCompletion2270SettlementBothStores"},
		{"internal/store/internal/backend/effectpersistence/runtime_external_effects.go", "EffectSQLiteOwner.AuthorizeExternalAttempt", "BindCompletionRequest", "TestCompletion2270SettlementBothStores"},
		{"internal/store/internal/backend/effectpersistence/completion_recovery.go", "completionRecoverySettlement", "Validate", "TestProviderAttemptDrainStartupRecoveryParity"},
		{"internal/store/internal/backend/effectpersistence/completion_settlement.go", "insertCompletionSpendRow", "Encode", "TestCompletion2270SettlementBothStores"},
		{"internal/store/internal/backend/effectpersistence/completion_settlement.go", "insertCompletionTargetPostgres", "Encode", "TestCompletion2270SettlementBothStores"},
		{"internal/store/internal/backend/effectpersistence/completion_settlement.go", "insertCompletionTargetSQLite", "Encode", "TestCompletion2270SettlementBothStores"},
		{"internal/store/internal/backend/effectpersistence/completion_settlement.go", "insertForkCompletionPostgres", "Encode", "TestCompletion2270ForkChatReadbackBothStores"},
		{"internal/store/internal/backend/effectpersistence/completion_settlement.go", "insertForkCompletionSQLite", "Encode", "TestCompletion2270ForkChatReadbackBothStores"},
		{"internal/store/internal/operatorsurface/conversation_turn_projection.go", "projectPublicConversationTurn", "DecodeCompletionEvidence", "TestCompletion2270PublicReadbackBothStores"},
		{"internal/store/internal/operatorsurface/conversation_turn_projection.go", "operatorConversationTurnListItemFromPublic", "Clone", "TestCompletion2270PublicReadbackBothStores"},
		{"internal/store/internal/operatorsurface/conversation_projection_types.go", "operatorLiveTurnFromPublic", "Clone", "TestCompletion2270PublicReadbackBothStores"},
		{"internal/store/internal/operatorsurface/conversation_projection_types.go", "enrichOperatorProjectionWithPublicTurn", "Clone", "TestCompletion2270PublicReadbackBothStores"},
		{"internal/store/internal/operatorsurface/operator_agent_usage_read_surface.go", "projectAgentUsageCompletionEvidence", "DecodeCompletionEvidence", "TestServedCompletion2270RunServePublicReadbackBothStores"},
		{"internal/store/internal/operatorsurface/operator_agent_usage_read_surface.go", "projectAgentUsageCompletionEvidence", "ValidateRequest", "TestCompletion2270UsageOwnerContradictionsRefused"},
		{"internal/apiv1/operator_read.go", "validateAgentUsageResult", "ValidateRequest", "TestCompletion2270UsageOwnerContradictionsRefused"},
		{"internal/store/internal/backend/runforkpersistence/conversation_fork_chat.go", "loadConversationForkSourceTurn", "DecodeCompletionEvidence", "TestCompletion2270ForkChatReadbackBothStores"},
		{"internal/store/internal/backend/runforkpersistence/conversation_fork_chat.go", "loadConversationForkCompletions", "DecodeCompletionEvidence", "TestCompletion2270ForkChatReadbackBothStores"},
		{"internal/store/internal/backend/runforkpersistence/conversation_fork_chat.go", "completeConversationForkTurn", "loadConversationForkCompletions", "TestCompletion2270ForkChatReadbackBothStores"},
		{"internal/cliapp/conversations.go", "validateConversationTurn", "validateCLICompletionEvidence", "TestCompletion2270CLIReadback"},
		{"internal/cliapp/conversations.go", "validateConversationTurnListItem", "validateCLICompletionEvidence", "TestCompletion2270CLIReadback"},
		{"internal/cliapp/forkchat.go", "validateForkChatTurn", "ValidateEvidence", "TestCompletion2270CLIReadback"},
		{"internal/cliapp/forkchat.go", "validateForkChatSnapshot", "validateCLICompletionEvidence", "TestCompletion2270CLIForkSnapshotEvidenceRefused"},
	} {
		t.Run(row.path+"/"+row.function, func(t *testing.T) {
			source, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, row.path), nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, declaration := range source.Decls {
				function, ok := declaration.(*ast.FuncDecl)
				if !ok || !completionEvidenceFunctionMatches(function, row.function) {
					continue
				}
				ast.Inspect(function.Body, func(node ast.Node) bool {
					call, ok := node.(*ast.CallExpr)
					if !ok {
						return true
					}
					switch callee := call.Fun.(type) {
					case *ast.Ident:
						found = found || callee.Name == row.call
					case *ast.SelectorExpr:
						found = found || callee.Sel.Name == row.call
					}
					return true
				})
			}
			if !found {
				t.Fatalf("canonical evidence consumer missing %s; reassess owner and rerun %s, not a hash-only refresh", row.call, row.proof)
			}
		})
	}
}

func completionEvidenceFunctionMatches(function *ast.FuncDecl, name string) bool {
	receiver, method, qualified := strings.Cut(name, ".")
	if !qualified {
		return function.Name.Name == name
	}
	if function.Name.Name != method || function.Recv == nil || len(function.Recv.List) != 1 {
		return false
	}
	owner := function.Recv.List[0].Type
	if pointer, ok := owner.(*ast.StarExpr); ok {
		owner = pointer.X
	}
	identifier, ok := owner.(*ast.Ident)
	return ok && identifier.Name == receiver
}
