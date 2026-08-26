package runtimepersistence

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestPlatformSpecRunForkRevisionRegistryIsClosed(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	body, err := os.ReadFile(filepath.Join(root, "platform-spec.yaml"))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	text := string(body)
	start := strings.Index(text, "    fixed_event_revision_and_workset:")
	if start < 0 {
		t.Fatal("platform spec is missing the fixed-event revision/workset contract")
	}
	end := strings.Index(text[start:], "    replay_resume_admission_taxonomy:")
	if end < 0 {
		t.Fatal("platform spec fixed-event revision/workset contract has no boundary")
	}
	contract := text[start : start+end]
	for _, family := range []string{
		"events", "entity_mutations", "entity_metadata", "event_deliveries",
		"committed_replay_scopes", "event_receipts", "dead_letters", "timers",
		"agent_sessions", "agent_turns", "agent_conversation_audits", "reply_contexts",
	} {
		if strings.Count(contract, "      - "+family+"\n") != 1 {
			t.Fatalf("platform spec historical registry does not contain %q exactly once", family)
		}
	}
	for _, forbidden := range []string{"eleven-family", "transaction_id", "source_transaction_id", "txid_current", "xmin"} {
		if strings.Contains(contract, forbidden) {
			t.Fatalf("platform spec fixed-event contract retained %q", forbidden)
		}
	}
	ddlStart := strings.Index(text, "    run_fork_revision_heads:")
	if ddlStart < 0 {
		t.Fatal("platform spec is missing the run-fork revision DDL block")
	}
	ddlEnd := strings.Index(text[ddlStart:], "    operator_principals:")
	if ddlEnd < 0 {
		t.Fatal("platform spec run-fork revision DDL block has no boundary")
	}
	ddl := text[ddlStart : ddlStart+ddlEnd]
	for _, forbidden := range []string{"transaction_id", "source_transaction_id", "txid_current", "xmin"} {
		if strings.Contains(ddl, forbidden) {
			t.Fatalf("platform spec run-fork revision DDL retained %q", forbidden)
		}
	}
}

func TestRunForkRevisionCaptureIsBackendNeutralGuard(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	sharedFiles := []string{"effects.go", "finalizer.go", "projection.go"}
	for _, name := range sharedFiles {
		body, err := os.ReadFile(filepath.Join(root, "internal/store/internal/backend/runforkrevision", name))
		if err != nil {
			t.Fatalf("read shared run-fork revision owner %s: %v", name, err)
		}
		for _, forbidden := range []string{"xmin", "txid_current", "transaction_id", "source_transaction_id", "jsonb", "::", "CaptureCurrentTransaction", "CaptureChanges"} {
			if strings.Contains(string(body), forbidden) {
				t.Fatalf("shared run-fork revision owner %s retained backend-specific or legacy token %q", name, forbidden)
			}
		}
	}
	forbiddenAPIs := []string{"CaptureCurrentTransaction", "CaptureChanges", "CommitRunForkRevisionTx"}
	err := filepath.WalkDir(filepath.Join(root, "internal/store"), func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, forbidden := range forbiddenAPIs {
			if strings.Contains(string(body), forbidden) {
				rel, relErr := filepath.Rel(root, path)
				if relErr != nil {
					return relErr
				}
				t.Fatalf("production store path %s retained legacy revision API %q", filepath.ToSlash(rel), forbidden)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan production revision APIs: %v", err)
	}
}

type runForkRevisionWriterCensusRow struct {
	Path          string
	Symbols       []string
	Tables        []string
	Family        string
	Transaction   string
	Branch        string
	RunDerivation string
	Finalizer     string
	Proof         string
}

func TestRunForkRevisionProductionWriterCensusIsClosed(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	got := scanRunForkRevisionPhysicalWriters(t, root)
	want := make(map[string]struct{})
	for _, row := range runForkRevisionWriterCensus() {
		for field, value := range map[string]string{
			"family": row.Family, "transaction": row.Transaction, "branch": row.Branch,
			"run derivation": row.RunDerivation, "finalizer": row.Finalizer, "proof": row.Proof,
		} {
			if strings.TrimSpace(value) == "" {
				t.Fatalf("writer census %s/%v is missing %s", row.Path, row.Symbols, field)
			}
		}
		for _, symbol := range row.Symbols {
			for _, table := range row.Tables {
				want[row.Path+"|"+symbol+"|"+table] = struct{}{}
			}
		}
	}
	if !reflect.DeepEqual(sortedStringKeys(got), sortedStringKeys(want)) {
		t.Fatalf("run-fork revision production writer census drifted:\ngot  %q\nwant %q", sortedStringKeys(got), sortedStringKeys(want))
	}

	selectedDiscard, err := os.ReadFile(filepath.Join(root, "internal/store/internal/backend/runforkpersistence/run_fork_selected_contract_execution_mutation.go"))
	if err != nil {
		t.Fatalf("read selected-fork discard owner: %v", err)
	}
	selectedText := string(selectedDiscard)
	for _, required := range []string{
		"if !preserveCompletionEvidence",
		"FamilyCommittedReplayScopes",
		"FamilyAgentSessions",
		"FinalizePostgres(ctx, tx, effects)",
		"DeleteMaterializedForkRunTx",
	} {
		if !strings.Contains(selectedText, required) {
			t.Fatalf("selected-fork retained/parent branch census lost %q", required)
		}
	}
	for _, forbidden := range []string{"CaptureCurrentTransaction", "CaptureChanges", "txid_current()", "source_transaction_id"} {
		if strings.Contains(selectedText, forbidden) {
			t.Fatalf("selected-fork owner retained non-authoritative revision path %q", forbidden)
		}
	}

	assertRunForkRevisionContributionPaths(t, root)
}

type runForkRevisionContributionPath struct {
	Path         string
	Writer       string
	WriterTokens []string
	ProofPath    string
	Proof        string
	ProofTokens  []string
}

func assertRunForkRevisionContributionPaths(t *testing.T, root string) {
	t.Helper()
	paths := []runForkRevisionContributionPath{
		{
			Path: "internal/store/internal/backend/delivery/lifecycle.go", Writer: "TerminalizeRunDeliveriesTx",
			WriterTokens: []string{"effects *privaterunforkrevision.Effects", "effects.Add", "FamilyEventDeliveries", "RecordDeadLetterTx"},
			ProofPath:    "internal/store/internal/runtimepersistence/run_fork_revision_operation_proof_test.go", Proof: "TestRunForkRevisionDirectDeliveryTerminalizationIsCompleteOnBothStores",
			ProofTokens: []string{"fixture.store.TerminalizeRun", "requireCompleteRunForkRevision"},
		},
		{
			Path: "internal/store/internal/backend/pipelinepersistence/owner_operations.go", Writer: "TerminalizePipelineObligationTx",
			WriterTokens: []string{"effects *revisionEffects", "terminalizeUnclaimedPipelineObligationTx", "declareEventRevisionFamily", "FamilyEventReceipts"},
			ProofPath:    "internal/store/internal/runtimepersistence/active_run_quiescence_delivery_readback_test.go", Proof: "TestActiveRunDeliveryQuiescenceReadbackParity",
			ProofTokens: []string{"ApplyActiveRunQuiescence", "requireCompleteRunForkRevision"},
		},
		{
			Path: "internal/store/internal/backend/pipelinepersistence/owner_operations.go", Writer: "MarkDecisionProcessed",
			WriterTokens: []string{"effects := newRevisionEffects()", "FamilyEventReceipts", "CommitPipelineHandoff", "FamilyEventDeliveries", "FinalizePostgres"},
			ProofPath:    "internal/store/internal/runtimepersistence/pipeline_obligation_parity_test.go", Proof: "provePipelineDecisionRouteDispositions",
			ProofTokens: []string{"MarkDecisionProcessed", "Settle", "requireCompleteRunForkRevision"},
		},
		{
			Path: "internal/store/internal/backend/pipelinepersistence/owner_operations.go", Writer: "Settle",
			WriterTokens: []string{"effects := newRevisionEffects()", "FamilyEventReceipts", "CommitPipelineHandoff", "FamilyEventDeliveries", "FinalizePostgres"},
			ProofPath:    "internal/store/internal/runtimepersistence/pipeline_obligation_parity_test.go", Proof: "provePipelineExactPayloadRecovery",
			ProofTokens: []string{"PipelineObligations().Settle", "requireCompleteRunForkRevision"},
		},
		{
			Path: "internal/store/internal/backend/eventpersistence/event_commit.go", Writer: "commitInitialSideEffectEvidence",
			WriterTokens: []string{"effects", "CommitInitialPipelineDispositionTx", "RecordDeadLetterTx"},
			ProofPath:    "internal/store/internal/runtimepersistence/run_fork_revision_operation_proof_test.go", Proof: "TestRunForkRevisionTargetFailurePublicationIsCompleteOnBothStores",
			ProofTokens: []string{"store.CommitPublication", "requireCompleteRunForkRevision"},
		},
		{
			Path: "internal/store/internal/backend/runlifecycle/run_lifecycle_state_adapter.go", Writer: "markRunTerminalStateTx",
			WriterTokens: []string{"effects *privaterunforkrevision.Effects", "TerminalizeRunDeliveriesTx", "SupersedeRunTx"},
			ProofPath:    "internal/store/internal/runtimepersistence/decision_cards_test.go", Proof: "TestTerminalDecisionCardSupersessionStateChangeOnlyProducerParity",
			ProofTokens: []string{"markDecisionCardRunTerminalStatus", "stopDecisionCardRun", "quiesceDecisionCardRun", "assertTerminalDecisionCardStateChangeOnly"},
		},
		{
			Path: "internal/store/internal/backend/runlifecycle/run_control.go", Writer: "quiesceStoppedRunWorkTx",
			WriterTokens: []string{"effects *runforkrevision.Effects", "TerminalizeRunTx", "terminateActiveRunSessionsTx", "cancelActiveRunTimerFamiliesTx"},
			ProofPath:    "internal/store/internal/runtimepersistence/decision_cards_test.go", Proof: "TestTerminalDecisionCardSupersessionStateChangeOnlyProducerParity",
			ProofTokens: []string{"run_stop", "stopDecisionCardRun", "assertTerminalDecisionCardStateChangeOnly"},
		},
		{
			Path: "internal/store/internal/backend/runlifecycle/active_run_quiescence.go", Writer: "ApplyActiveRunQuiescence",
			WriterTokens: []string{"effects := runforkrevision.NewEffects()", "TerminalizeRunDeliveriesTx", "TerminalizeRunTx", "FinalizePostgres"},
			ProofPath:    "internal/store/internal/runtimepersistence/active_run_quiescence_delivery_readback_test.go", Proof: "TestActiveRunDeliveryQuiescenceReadbackParity",
			ProofTokens: []string{"ApplyActiveRunQuiescence", "requireCompleteRunForkRevision"},
		},
		{
			Path: "internal/store/internal/backend/runlifecycle/run_lifecycle_candidates.go", Writer: "executeCompletionCandidateTx",
			WriterTokens: []string{"effects *privaterunforkrevision.Effects", "completeRunTx"},
			ProofPath:    "internal/store/internal/runtimepersistence/decision_cards_test.go", Proof: "TestStandaloneCompletionCandidatePublishesChangedGateRevisionParity",
			ProofTokens: []string{"executeStandaloneCompletionCandidateWithCatalog", "requireCompleteRunForkRevision"},
		},
		{
			Path: "internal/store/internal/preservationpersistence/preservation_cleanup.go", Writer: "applyPreservationCleanup",
			WriterTokens: []string{"effects := privaterunforkrevision.NewEffects()", "TerminalizeRunTx", "MarkTerminalTx", "FinalizeRunForkRevisionTx"},
			ProofPath:    "internal/store/internal/runtimepersistence/preservation_cleanup_test.go", Proof: "TestPreservationCleanupPublishesOneCompleteRunForkRevisionPostgres",
			ProofTokens: []string{"ApplyUnavailableBundleStartupPreservationCleanup", "assertPreservationCleanupRunForkRevision"},
		},
		{
			Path: "internal/store/internal/backend/pipelinepersistence/standing_service.go", Writer: "quiesceStandingRunTx",
			WriterTokens: []string{"s.revisionEffects", "TerminalizeRunDeliveriesTx", "TerminalizeRunTx", "FamilyAgentSessions"},
			ProofPath:    "internal/store/internal/runtimepersistence/standing_service_store_test.go", Proof: "TestSQLiteStandingServiceOperatorLifecycleQuiescesAndPersistsDesiredState",
			ProofTokens: []string{"ResetStandingService", "requireCompleteRunForkRevision"},
		},
		{
			Path: "internal/store/internal/backend/runforkpersistence/run_fork_source_freeze.go", Writer: "applyRunForkSourceFreeze",
			WriterTokens: []string{"effects *runforkrevision.Effects", "ForkSourceTx"},
			ProofPath:    "internal/store/internal/runtimepersistence/run_fork_source_freeze_test.go", Proof: "TestRunForkSourceFreezeCommitsCoupledLifecycleDecisionAndActivityOutcome",
			ProofTokens: []string{"commitRunForkSourceFreezeForTest", "ValidateCompletePostgres"},
		},
		{
			Path: "internal/store/internal/backend/runforkpersistence/run_fork_selected_contract_execution_mutation.go", Writer: "ActivateRunForkForSelectedContractExecution",
			WriterTokens: []string{"effects := runforkrevision.NewEffects()", "applyRunForkSourceFreeze", "commitRunForkAuthorActivityTransaction"},
			ProofPath:    "internal/store/internal/runtimepersistence/run_fork_selected_contract_execution_mutation_test.go", Proof: "TestPostTGlobalRoutingRuleDoesNotChangeSelectedContractActivation",
			ProofTokens: []string{"ActivateRunForkForSelectedContractExecution", "ValidateCompletePostgres"},
		},
	}
	for _, path := range paths {
		productionSource, err := os.ReadFile(filepath.Join(root, path.Path))
		if err != nil {
			t.Fatalf("read contribution writer %s: %v", path.Path, err)
		}
		writerBody := productionFunctionBody(t, string(productionSource), path.Writer)
		for _, token := range path.WriterTokens {
			if !strings.Contains(writerBody, token) {
				t.Fatalf("revision contribution path %s/%s cannot prove writer token %q", path.Path, path.Writer, token)
			}
		}
		proofSource, err := os.ReadFile(filepath.Join(root, path.ProofPath))
		if err != nil {
			t.Fatalf("read contribution proof %s: %v", path.ProofPath, err)
		}
		proofBody := productionFunctionBody(t, string(proofSource), path.Proof)
		for _, token := range path.ProofTokens {
			if !strings.Contains(proofBody, token) {
				t.Fatalf("revision contribution proof %s/%s does not execute %q", path.ProofPath, path.Proof, token)
			}
		}
	}
}

func productionFunctionBody(t *testing.T, source, functionName string) string {
	t.Helper()
	start := strings.Index(source, "func "+functionName+"(")
	if start < 0 {
		method := strings.Index(source, ") "+functionName+"(")
		if method >= 0 {
			start = strings.LastIndex(source[:method], "func (")
		}
	}
	if start < 0 {
		t.Fatalf("production function or method %s is missing", functionName)
	}
	bodyStart := strings.Index(source[start:], "{")
	if bodyStart < 0 {
		t.Fatalf("production function %s has no body", functionName)
	}
	bodyStart += start
	depth := 0
	for index := bodyStart; index < len(source); index++ {
		switch source[index] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return source[start : index+1]
			}
		}
	}
	t.Fatalf("production function %s has an unterminated body", functionName)
	return ""
}

func scanRunForkRevisionPhysicalWriters(t *testing.T, root string) map[string]struct{} {
	t.Helper()
	physical := map[string]struct{}{
		"events": {}, "entity_mutations": {}, "entity_state": {},
		"event_deliveries": {}, "event_delivery_attempts": {}, "event_delivery_outcomes": {},
		"committed_replay_scopes": {}, "event_receipts": {}, "dead_letters": {}, "timers": {},
		"agent_sessions": {}, "agent_turns": {}, "agent_conversation_audits": {}, "reply_contexts": {},
	}
	dml := regexp.MustCompile(`(?is)\b(?:INSERT(?:\s+OR\s+[A-Z_]+)?\s+INTO|UPDATE|DELETE\s+FROM)\s+(?:[A-Z_]+\.)?([A-Z_]+)`)
	got := make(map[string]struct{})
	storeRoot := filepath.Join(root, "internal/store")
	err := filepath.WalkDir(storeRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		for _, declaration := range parsed.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				literal, ok := node.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				text, err := strconv.Unquote(literal.Value)
				if err != nil {
					return true
				}
				for _, match := range dml.FindAllStringSubmatch(text, -1) {
					table := strings.ToLower(match[1])
					if _, governed := physical[table]; governed {
						got[rel+"|"+fn.Name.Name+"|"+table] = struct{}{}
					}
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan run-fork revision physical writers: %v", err)
	}
	return got
}

func sortedStringKeys(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func runForkRevisionWriterCensus() []runForkRevisionWriterCensusRow {
	const (
		matrixProof   = "TestRunForkRevisionTwelveFamilySelectedStoreParity"
		sessionProof  = "TestRunForkRevisionSessionProjectionIgnoresExcludedWriterChurnAndTracksStatusPresence"
		discardProof  = "TestSelectedForkRetainedDiscardPublishesHistoricalTombstoneRevisionPostgres"
		rollbackProof = "TestSelectedForkRetainedDiscardRollbackIncludesRevisionPublicationPostgres"
	)
	row := func(path string, symbols, tables []string, family, transaction, branch, runDerivation, finalizer, proof string) runForkRevisionWriterCensusRow {
		return runForkRevisionWriterCensusRow{Path: path, Symbols: symbols, Tables: tables, Family: family, Transaction: transaction, Branch: branch, RunDerivation: runDerivation, Finalizer: finalizer, Proof: proof}
	}
	return []runForkRevisionWriterCensusRow{
		row("internal/store/internal/adminpersistence/destructive_reset_cleanup.go", []string{"destructiveResetCleanupSeverPreservedReferences"}, []string{"agent_sessions", "entity_mutations", "timers"}, "parent-owned cleanup references", "ApplyDestructiveResetCleanup", "whole-parent destructive cleanup", "validated cleanup plan run IDs", "parent deletion cascades complete revision ledger", "TestDestructiveResetCleanup"),
		row("internal/store/internal/adminpersistence/destructive_reset_cleanup.go", []string{"destructiveResetCleanupStatementsForTable"}, []string{"committed_replay_scopes", "dead_letters", "event_deliveries", "event_receipts", "timers"}, "parent-owned cleanup rows", "ApplyDestructiveResetCleanup", "whole-parent destructive cleanup", "validated cleanup plan run IDs", "parent deletion cascades complete revision ledger", "TestDestructiveResetCleanup"),

		row("internal/store/internal/backend/agentpersistence/lifecycle.go", []string{"applyPostgresLifecycleSessionMutation", "applySQLiteLifecycleSubordinate"}, []string{"agent_sessions"}, "agent_sessions", "CommitAgentLifecycleTransitionTx", "projected lifecycle transition", "transition result session run IDs", "lifecycle owner FinalizePostgres/FinalizeSQLite", "TestPostgresLifecycleSessionMutationPublishesRunForkRevision"),
		row("internal/store/internal/backend/decisionpersistence/decision_cards.go", []string{"supersedeRunGateActivations"}, []string{"entity_state"}, "entity_metadata excluded-column no-change", "decision-card named mutation", "accumulator-only update", "validated card run ID", "outer mutation compares declared entity mutation effects; metadata projection is unchanged", matrixProof),

		row("internal/store/internal/backend/delivery/adapter.go", []string{"ActivateNormalAuthority", "prepareProviderOriginRecovery", "RenewClaim"}, []string{"event_deliveries", "event_delivery_attempts"}, "event_deliveries", "delivery lifecycle/effect named mutation", "claim lifecycle", "locked delivery snapshot run ID", "outer delivery/effect owner finalizer", matrixProof),
		row("internal/store/internal/backend/delivery/adapter.go", []string{"BindAgentSession", "closeAttemptForTerminalization", "completeAttempt", "expireAttempt", "insertAttempt", "insertTerminalizedAttempt"}, []string{"event_delivery_attempts"}, "event_deliveries", "delivery lifecycle/effect named mutation", "attempt lifecycle", "locked delivery snapshot run ID", "outer delivery/effect owner finalizer", matrixProof),
		row("internal/store/internal/backend/delivery/adapter.go", []string{"CommitPipelineHandoff", "TerminalizeRun", "claimLocked", "insertExactObligation", "settle"}, []string{"event_deliveries"}, "event_deliveries", "event/pipeline/delivery/run-lifecycle named mutation", "delivery row lifecycle", "event or delivery snapshot run ID", "outer named owner finalizer", matrixProof),
		row("internal/store/internal/backend/delivery/adapter.go", []string{"insertOutcome"}, []string{"event_delivery_outcomes"}, "event_deliveries", "delivery/effect named settlement", "outcome append", "locked delivery snapshot run ID", "outer delivery/effect owner finalizer", matrixProof),
		row("internal/store/internal/backend/delivery/dead_letters.go", []string{"insertPostgresDeadLetterRecord", "insertSQLiteDeadLetterRecord"}, []string{"dead_letters"}, "dead_letters", "PersistDeadLetter", "exact insert or duplicate", "immutable original event run lookup", "dead-letter owner finalizer", matrixProof),

		row("internal/store/internal/backend/effectpersistence/completion_settlement.go", []string{"insertCompletionTargetPostgres", "insertCompletionTargetSQLite"}, []string{"agent_turns"}, "agent_turns", "completion settlement", "memory/stateless completion", "validated completion target run ID", "effect owner finalizer", matrixProof),
		row("internal/store/internal/backend/effectpersistence/runtime_external_effects.go", []string{"promoteProviderHeadPostgres", "promoteProviderHeadSQLiteTx"}, []string{"agent_sessions"}, "agent_sessions excluded-column no-change", "runtime external-effect settlement", "provider-head runtime_state update", "validated completion run ID", "effect owner finalizer compares canonical projection", sessionProof),

		row("internal/store/internal/backend/entityruntime/persistence.go", []string{"CreateEntity"}, []string{"entity_state"}, "entity_metadata", "CreateEntity", "new entity", "validated record run ID", "entity owner finalizer", matrixProof),
		row("internal/store/internal/backend/entityruntime/persistence.go", []string{"SaveEntityField"}, []string{"entity_state"}, "entity_metadata excluded-column no-change", "SaveEntityField", "fields-only update", "validated command run ID", "entity owner finalizer publishes entity_mutations", matrixProof),
		row("internal/store/internal/backend/entityruntime/persistence.go", []string{"InsertSQLiteEntityStateDiff"}, []string{"entity_mutations"}, "entity_mutations", "SQLite entity mutation", "mutation append", "active-run source owner", "entity/pipeline outer finalizer", matrixProof),
		row("internal/store/internal/backend/mutationlog/adapter.go", []string{"InsertWithStory"}, []string{"entity_mutations"}, "entity_mutations", "entity/decision/pipeline/fork named mutation", "mutation append", "active-run source owner", "outer named owner finalizer", matrixProof),

		row("internal/store/internal/backend/eventrecord/postgres/adapter.go", []string{"Insert"}, []string{"events"}, "events", "event/pipeline/fork named commit", "exact append", "admitted record run ID", "outer event/pipeline/fork finalizer", matrixProof),
		row("internal/store/internal/backend/eventrecord/sqlite/adapter.go", []string{"Insert"}, []string{"events"}, "events", "event/pipeline named commit", "exact append", "admitted record run ID", "outer event/pipeline finalizer", matrixProof),
		row("internal/store/internal/backend/eventrecord/postgres/adapter.go", []string{"DeleteSelectedForkRunEvents"}, []string{"events"}, "events", "DiscardMaterializedSelectedContractExecutionFork", "retained tombstone or whole-parent deletion", "locked selected fork run ID", "retained branch finalizes; parent branch cascades", discardProof),

		row("internal/store/internal/backend/genericschedule/occurrence.go", []string{"AdvanceOccurrenceTx"}, []string{"timers"}, "timers", "CommitGenericScheduleOccurrence", "occurrence advance", "loaded activation run ID", "schedule/pipeline outer finalizer", matrixProof),
		row("internal/store/internal/backend/genericschedule/owner.go", []string{"cancelLoadedTx", "failLoadedTx", "failMalformedByIDTx", "insertActivationTx", "stampOccurrenceTx"}, []string{"timers"}, "timers", "generic schedule named mutation", "schedule lifecycle", "loaded/validated activation run ID", "generic schedule owner finalizer", "TestGenericScheduleDuplicateCancellationDoesNotPublishRunForkRevision"),
		row("internal/store/internal/backend/workflowtimer/cancellation.go", []string{"CancelRunsTx"}, []string{"timers"}, "timers", "standing/run-lifecycle/preservation named mutation", "workflow timer cancellation", "locked timer run ID", "outer pipeline/run-lifecycle/preservation finalizer", matrixProof),

		row("internal/store/internal/backend/llmpersistence/postgres.go", []string{"ensurePostgresStatelessAuditTx"}, []string{"agent_conversation_audits"}, "agent_conversation_audits", "completion settlement", "stateless audit ensure", "validated turn run ID", "effect owner finalizer", matrixProof),
		row("internal/store/internal/backend/llmpersistence/sqlite.go", []string{"ensureSQLiteStatelessAuditTx"}, []string{"agent_conversation_audits"}, "agent_conversation_audits", "completion settlement", "stateless audit ensure", "validated turn run ID", "effect owner finalizer", matrixProof),
		row("internal/store/internal/backend/llmpersistence/postgres.go", []string{"EnsureCompletionTurnMemoryTx", "ProjectCompletionConversationTx", "UpdateLiveSessionWatchdog", "UpsertConversation"}, []string{"agent_sessions"}, "agent_sessions excluded-column no-change", "LLM/effect named mutation", "conversation/runtime-state churn", "validated memory identity run ID", "LLM/effect owner finalizer compares canonical projection", sessionProof),
		row("internal/store/internal/backend/llmpersistence/sqlite.go", []string{"EnsureCompletionTurnMemoryTx", "ProjectCompletionConversationTx", "UpdateLiveSessionWatchdog", "UpsertConversation"}, []string{"agent_sessions"}, "agent_sessions excluded-column no-change", "LLM/effect named mutation", "conversation/runtime-state churn", "validated memory identity run ID", "LLM/effect owner finalizer compares canonical projection", sessionProof),
		row("internal/store/internal/backend/llmpersistence/postgres_sessions.go", []string{"acquirePostgresLiveSession", "Release", "Rotate", "IncrementTurn", "AdoptSessionID", "ResetAll"}, []string{"agent_sessions"}, "agent_sessions", "live-session named operation", "presence/status or excluded lease/runtime churn", "validated memory identity or locked session run ID", "LLM owner finalizer", sessionProof),
		row("internal/store/internal/backend/llmpersistence/sqlite_sessions.go", []string{"acquireSQLiteLiveSession", "Release", "Rotate", "IncrementTurn", "AdoptSessionID", "ResetAll"}, []string{"agent_sessions"}, "agent_sessions", "live-session named operation", "presence/status or excluded lease/runtime churn", "validated memory identity or locked session run ID", "LLM owner finalizer", sessionProof),

		row("internal/store/internal/backend/pipelinepersistence/owner_operations.go", []string{"insertCommittedPipelineScopeTx"}, []string{"committed_replay_scopes"}, "committed_replay_scopes", "event/pipeline named commit", "scope insert", "admitted event run ID", "outer pipeline finalizer", matrixProof),
		row("internal/store/internal/backend/pipelinepersistence/owner_operations.go", []string{"writeExactPlatformPipelineReceipt"}, []string{"event_receipts"}, "event_receipts", "pipeline disposition/settlement", "exact receipt insert", "immutable event run lookup", "outer pipeline finalizer", matrixProof),
		row("internal/store/internal/backend/pipelinepersistence/scenario_setup.go", []string{"SetupScenarioEntities"}, []string{"entity_state"}, "entity_metadata + entity_mutations", "SetupScenarioEntities", "scenario materialization", "created run ID", "pipeline owner finalizer", matrixProof),
		row("internal/store/internal/backend/pipelinepersistence/standing_service.go", []string{"copyStandingEntityStateTx"}, []string{"entity_state"}, "entity_metadata + entity_mutations", "standing service reconciliation", "standing run copy", "new standing run ID", "standing pipeline owner finalizer", matrixProof),
		row("internal/store/internal/backend/pipelinepersistence/standing_service.go", []string{"quiesceStandingRunTx"}, []string{"agent_sessions"}, "agent_sessions", "standing service reconciliation", "run quiescence", "locked standing run ID", "standing pipeline owner finalizer", matrixProof),
		row("internal/store/internal/backend/pipelinepersistence/workflow_engine_mutation_commit.go", []string{"commitPostgresWorkflowEngineState", "commitSQLiteWorkflowEngineState"}, []string{"entity_state"}, "entity_metadata + entity_mutations", "CommitWorkflowEngineMutation", "workflow projection plus audit mutation", "validated state run ID", "pipeline owner finalizer", matrixProof),
		row("internal/store/internal/backend/pipelinepersistence/workflow_engine_timer_commit.go", []string{"cancelWorkflowEngineTimerActivation", "insertWorkflowEngineTimerActivation"}, []string{"timers"}, "timers", "CommitWorkflowEngineMutation", "workflow timer mutation", "validated activation run ID", "pipeline owner finalizer", matrixProof),
		row("internal/store/internal/backend/pipelinepersistence/workflow_timer_occurrence_commit.go", []string{"advanceWorkflowEngineTimerOccurrence"}, []string{"timers"}, "timers", "CommitWorkflowTimerOccurrence", "workflow timer occurrence", "loaded activation run ID", "pipeline owner finalizer", matrixProof),

		row("internal/store/internal/backend/replycontext/owner.go", []string{"createPostgresReplyContext", "createSQLiteReplyContextTx", "claimLoadedReplyContextTx"}, []string{"reply_contexts"}, "reply_contexts", "CreateReplyContext/ClaimReplyContext or outer event commit", "create/claim", "validated reply record run ID", "reply-context or outer event finalizer", matrixProof),
		row("internal/store/internal/backend/runforkpersistence/run_fork_materializer.go", []string{"materializeRunForkEntityState"}, []string{"entity_state"}, "entity_metadata", "MaterializeRunFork", "fork entity materialization", "deterministic fork run ID", "run-fork owner finalizer", matrixProof),
		row("internal/store/internal/backend/runforkpersistence/run_fork_selected_contract_execution_mutation.go", []string{"materializeSelectedContractWorkflowState"}, []string{"entity_state"}, "entity_metadata", "MaterializeRunForkForSelectedContractExecution", "selected workflow materialization", "deterministic fork run ID", "run-fork owner finalizer", matrixProof),
		row("internal/store/internal/backend/runforkpersistence/run_fork_selected_contract_execution_mutation.go", []string{"DiscardMaterializedSelectedContractExecutionFork"}, []string{"agent_conversation_audits", "agent_sessions", "agent_turns", "committed_replay_scopes", "dead_letters", "entity_mutations", "entity_state", "event_deliveries", "event_delivery_attempts", "event_delivery_outcomes", "event_receipts", "timers"}, "nine retained families or parent-owned complete state", "DiscardMaterializedSelectedContractExecutionFork", "retained completion-evidence branch and distinct parent-deleting branch", "locked selected fork run ID", "retained branch finalizes nine families; parent branch deletes parent and cascade ledger", discardProof+"; "+rollbackProof),

		row("internal/store/internal/backend/runlifecycle/active_run_quiescence.go", []string{"terminateActiveRunSessionsTx", "sqliteTerminateActiveRunSessionsTx"}, []string{"agent_sessions"}, "agent_sessions", "ApplyActiveRunQuiescence/run control", "run quiescence", "locked target run IDs", "run-lifecycle owner finalizer", matrixProof),
		row("internal/store/internal/preservationpersistence/preservation_cleanup.go", []string{"terminateUnavailableBundlePreservationSessionTx"}, []string{"agent_sessions"}, "agent_sessions", "ApplyPreservationCleanup", "retained-run cleanup", "locked active run IDs", "preservation owner finalizer", matrixProof),

		row("internal/store/storetest/event.go", []string{"insertPipelineScopeFixture"}, []string{"committed_replay_scopes"}, "test fixture only", "storetest semantic event fixture", "test-only insertion", "fixture event run ID", "fixture explicitly invokes selected-store finalizer", matrixProof),
		row("internal/store/storetest/event.go", []string{"insertPipelineDispositionFixture"}, []string{"event_receipts"}, "test fixture only", "storetest semantic event fixture", "test-only insertion", "fixture event run ID", "fixture explicitly invokes selected-store finalizer", matrixProof),
	}
}
