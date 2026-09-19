package runtimepersistence

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// This is structural enforcement, not a claim that syntax proves complete
// runtime write sets. Differential and omitted-contributor execution tests
// remain necessary, especially for generated IDs and joined sibling effects.
func TestRunForkRevisionProjectionContributorCensusIsClosed(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	body, err := os.ReadFile(filepath.Join(root, "internal/store/internal/backend/runforkrevision/projection.go"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := revisionProjectionContributors(string(body))
	if err != nil {
		t.Fatal(err)
	}
	want := revisionProjectionContributorCensus()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("canonical projection contributor drift: got=%v want=%v", got, want)
	}
	if err := validateRevisionPhysicalContributors(got, runForkRevisionPhysicalTables()); err != nil {
		t.Fatal(err)
	}
}

func revisionProjectionContributorCensus() map[string][]string {
	return map[string][]string{
		"FamilyEvents":                  {"events"},
		"FamilyEntityMutations":         {"entity_mutations"},
		"FamilyEntityMetadata":          {"entity_state"},
		"FamilyEventDeliveries":         {"event_deliveries", "event_delivery_attempts", "event_delivery_handler_rule_selections"},
		"FamilyCommittedReplayScopes":   {"committed_replay_scopes"},
		"FamilyEventReceipts":           {"event_receipts", "events"},
		"FamilyDeadLetters":             {"dead_letters", "event_delivery_outcomes", "events"},
		"FamilyTimers":                  {"timers"},
		"FamilyAgentSessions":           {"agent_sessions"},
		"FamilyAgentTurns":              {"agent_turns"},
		"FamilyAgentConversationAudits": {"agent_conversation_audits"},
		"FamilyReplyContexts":           {"reply_contexts"},
		"FamilyFanOutObligations":       {"fan_out_intents", "fan_out_obligation_barriers", "fan_out_outcomes"},
	}
}

func revisionProjectionContributors(source string) (map[string][]string, error) {
	functions, err := revisionGuardFunctions(source)
	if err != nil {
		return nil, err
	}
	fn := functions["canonicalProjectionSpec"]
	if fn == nil {
		return nil, fmt.Errorf("canonicalProjectionSpec missing")
	}
	from := regexp.MustCompile(`(?i)\b(?:FROM|JOIN)\s+([a-z_][a-z_0-9]*)`)
	result := map[string][]string{}
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		clause, ok := node.(*ast.CaseClause)
		if !ok || len(clause.List) != 1 {
			return true
		}
		family, ok := clause.List[0].(*ast.Ident)
		if !ok || !strings.HasPrefix(family.Name, "Family") {
			return true
		}
		tables := map[string]struct{}{}
		ast.Inspect(clause, func(node ast.Node) bool {
			if field, ok := node.(*ast.KeyValueExpr); ok && revisionGuardNode(field.Key) == "source" {
				if literal, ok := field.Value.(*ast.BasicLit); ok && literal.Kind == token.STRING {
					if sql, err := strconv.Unquote(literal.Value); err == nil {
						for _, match := range from.FindAllStringSubmatch("FROM "+sql, -1) {
							tables[strings.ToLower(match[1])] = struct{}{}
						}
					}
				}
			}
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			sql, unquoteErr := strconv.Unquote(literal.Value)
			if unquoteErr != nil {
				return true
			}
			for _, match := range from.FindAllStringSubmatch(sql, -1) {
				tables[strings.ToLower(match[1])] = struct{}{}
			}
			return true
		})
		result[family.Name] = sortedStringKeys(tables)
		return false
	})
	return result, nil
}

func validateRevisionPhysicalContributors(families map[string][]string, physical map[string]struct{}) error {
	used := map[string]struct{}{}
	for family, tables := range families {
		if len(tables) == 0 {
			return fmt.Errorf("%s has no classified projection tables", family)
		}
		for _, table := range tables {
			if _, ok := physical[table]; !ok {
				return fmt.Errorf("%s contributor %s is absent from physical writer census", family, table)
			}
			used[table] = struct{}{}
		}
	}
	if !reflect.DeepEqual(sortedStringKeys(used), sortedStringKeys(physical)) {
		return fmt.Errorf("unclassified physical table: projection=%v physical=%v", sortedStringKeys(used), sortedStringKeys(physical))
	}
	return nil
}

type revisionExactWriterContract struct {
	path, symbol string
	calls        []string
}

func TestRunForkRevisionExactWriterContributions(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	seen := map[string]bool{}
	for _, contract := range revisionExactWriterContracts() {
		key := contract.path + "|" + contract.symbol
		if seen[key] {
			t.Fatalf("duplicate exact writer contract %s", key)
		}
		seen[key] = true
		t.Run(key, func(t *testing.T) {
			body, err := os.ReadFile(filepath.Join(root, "internal/store/internal/backend", contract.path))
			if err != nil {
				t.Fatal(err)
			}
			if err := validateRevisionExactWriter(string(body), contract); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func revisionExactWriterContracts() []revisionExactWriterContract {
	return []revisionExactWriterContract{
		{"mutationlog/adapter.go", "InsertWithStory", []string{"uuid.NewString()", "effects.AddFact(runID, runforkrevision.FamilyEntityMutations, mutationID)"}},
		{"mutationlog/adapter.go", "InsertSQLiteWithStory", []string{"uuid.NewString()", "effects.AddFact(runID, runforkrevision.FamilyEntityMutations, mutationID)"}},
		{"entityruntime/persistence.go", "InsertSQLiteEntityStateDiff", []string{"uuid.NewString()", "effects.AddFact(runID, privaterunforkrevision.FamilyEntityMutations, mutationID)"}},
		{"entityruntime/persistence.go", "EntityPostgresOwner.CreateEntity", []string{"effects.AddFact(storedRunID, privaterunforkrevision.FamilyEntityMetadata, storedEntityID)"}},
		{"entityruntime/persistence.go", "EntitySQLiteOwner.CreateEntity", []string{"effects.AddFact(rec.RunID, privaterunforkrevision.FamilyEntityMetadata, rec.EntityID)"}},
		{"pipelinepersistence/scenario_setup.go", "PipelinePostgresOwner.SetupScenarioEntities", []string{"effects.AddFact(storedRunID, privaterunforkrevision.FamilyEntityMetadata, storedEntityID)"}},
		{"pipelinepersistence/scenario_setup.go", "PipelineSQLiteOwner.SetupScenarioEntities", []string{"effects.AddFact(req.RunID, privaterunforkrevision.FamilyEntityMetadata, entity.EntityID)"}},
		{"runforkpersistence/run_fork_materializer.go", "materializeRunForkEntityState", []string{"effects.AddFact(forkRunID, privaterunforkrevision.FamilyEntityMetadata, entityID)"}},
		{"replycontext/owner.go", "createPostgresReplyContext", []string{
			"record.Normalized()", "record.Validate()", "resolveReplyContextCreateConflict(record, existing, loadErr)",
			"effects.AddFact(record.RunID, runforkrevision.FamilyReplyContexts, record.ID)",
			"effects.AddFact(existing.RunID, runforkrevision.FamilyReplyContexts, existing.ID)",
		}},
		{"replycontext/owner.go", "createSQLiteReplyContextTx", []string{
			"record.Normalized()", "record.Validate()", "resolveReplyContextCreateConflict(record, existing, loadErr)",
			"effects.AddFact(record.RunID, runforkrevision.FamilyReplyContexts, record.ID)",
			"effects.AddFact(existing.RunID, runforkrevision.FamilyReplyContexts, existing.ID)",
		}},
		{"replycontext/owner.go", "ReplyPostgresOwner.ClaimReplyContext", []string{"effects.AddFact(record.RunID, runforkrevision.FamilyReplyContexts, record.ID)"}},
		{"replycontext/owner.go", "ReplySQLiteOwner.ClaimReplyContext", []string{"effects.AddFact(record.RunID, runforkrevision.FamilyReplyContexts, record.ID)"}},
		{"replycontext/owner.go", "ReplyPostgresOwner.ClaimWithinTransaction", []string{"effects.AddFact(loaded.RunID, runforkrevision.FamilyReplyContexts, loaded.ID)"}},
		{"replycontext/owner.go", "ReplySQLiteOwner.ClaimWithinTransaction", []string{"effects.AddFact(loaded.RunID, runforkrevision.FamilyReplyContexts, loaded.ID)"}},
		{"effectpersistence/completion_settlement.go", "insertCompletionTargetPostgres", []string{"effects.AddFact(storedRunID, privaterunforkrevision.FamilyAgentTurns, storedTurnID)"}},
		{"effectpersistence/completion_settlement.go", "insertCompletionTargetSQLite", []string{"effects.AddFact(t.RunID, privaterunforkrevision.FamilyAgentTurns, t.TurnID)"}},
		{"agentpersistence/lifecycle.go", "addLifecycleRevisionEffects", []string{
			"effects.AddFact(session.RunID, privaterunforkrevision.FamilyAgentSessions, session.PreviousSessionID)",
			"effects.AddFact(session.RunID, privaterunforkrevision.FamilyAgentSessions, session.SuccessorSessionID)",
		}},
		{"agentpersistence/lifecycle.go", "commitPostgresAgentLifecycleTransitionTx", []string{"applyPostgresLifecycleSubordinate(ctx, tx, req)", "addLifecycleRevisionEffects(effects, result)"}},
		{"agentpersistence/lifecycle.go", "commitSQLiteAgentLifecycleTransitionTx", []string{"applySQLiteLifecycleSubordinate(ctx, tx, req)", "addLifecycleRevisionEffects(effects, result)"}},
		{"llmpersistence/owner.go", "agentSessionEffects", []string{"addAgentSessionFacts(effects, runID, sessionID, otherSessionIDs...)"}},
		{"llmpersistence/owner.go", "addAgentSessionFacts", []string{
			"effects.AddFact(runID, runforkrevision.FamilyAgentSessions, sessionID)",
			"effects.AddFact(runID, runforkrevision.FamilyAgentSessions, id)",
		}},
		{"llmpersistence/postgres.go", "ensurePostgresStatelessAuditTx", []string{
			"effects.AddFact(previousRunID, runforkrevision.FamilyAgentConversationAudits, storedSessionID)",
			"effects.AddFact(storedRunID, runforkrevision.FamilyAgentConversationAudits, storedSessionID)",
		}},
		{"llmpersistence/sqlite.go", "ensureSQLiteStatelessAuditTx", []string{
			"effects.AddFact(previousRunID, runforkrevision.FamilyAgentConversationAudits, sessionID)",
			"effects.AddFact(identity.RunID, runforkrevision.FamilyAgentConversationAudits, sessionID)",
		}},
		{"llmpersistence/postgres.go", "LLMPostgresOwner.EnsureCompletionTurnMemoryTx", []string{
			"ensurePostgresStatelessAuditTx(ctx, tx, effects, rec, plan, identity)", "addAgentSessionFacts(effects, storedRunID, storedSessionID)",
		}},
		{"llmpersistence/sqlite.go", "LLMSQLiteOwner.EnsureCompletionTurnMemoryTx", []string{
			"ensureSQLiteStatelessAuditTx(ctx, tx, effects, rec, plan, identity, now)", "addAgentSessionFacts(effects, identity.RunID, rec.SessionID)",
		}},
		{"llmpersistence/postgres.go", "LLMPostgresOwner.UpsertConversation", []string{"addAgentSessionFacts(effects, storedRunID, storedSessionID)"}},
		{"llmpersistence/sqlite.go", "LLMSQLiteOwner.UpsertConversation", []string{"addAgentSessionFacts(effects, identity.RunID, rec.SessionID)"}},
		{"llmpersistence/postgres.go", "LLMPostgresOwner.ProjectCompletionConversationTx", []string{"addAgentSessionFacts(effects, storedRunID, storedSessionID)"}},
		{"llmpersistence/sqlite.go", "LLMSQLiteOwner.ProjectCompletionConversationTx", []string{"addAgentSessionFacts(effects, identity.RunID, rec.SessionID)"}},
		{"llmpersistence/postgres.go", "LLMPostgresOwner.UpdateLiveSessionWatchdog", []string{"agentSessionEffects(storedRunID, storedSessionID)"}},
		{"llmpersistence/sqlite.go", "LLMSQLiteOwner.UpdateLiveSessionWatchdog", []string{"addAgentSessionFacts(effects, identity.RunID, update.SessionID)"}},
		{"llmpersistence/postgres_sessions.go", "LLMPostgresOwner.acquirePostgresLiveSession", []string{"agentSessionEffects(current.runID, current.sessionID)"}},
		{"llmpersistence/postgres_sessions.go", "LLMPostgresOwner.Release", []string{"agentSessionEffects(storedRunID, storedSessionID)"}},
		{"llmpersistence/postgres_sessions.go", "LLMPostgresOwner.Rotate", []string{"agentSessionEffects(currentRunID, currentID)", "addAgentSessionFacts(effects, newRunID, newID)"}},
		{"llmpersistence/postgres_sessions.go", "LLMPostgresOwner.IncrementTurn", []string{"agentSessionEffects(storedRunID, storedSessionID)"}},
		{"llmpersistence/postgres_sessions.go", "LLMPostgresOwner.AdoptSessionID", []string{"agentSessionEffects(storedRunID, sessionID)"}},
		{"llmpersistence/sqlite_sessions.go", "LLMSQLiteOwner.acquireSQLiteLiveSession", []string{
			"addAgentSessionFacts(effects, identity.RunID, sessionID)", "addAgentSessionFacts(effects, identity.RunID, rec.sessionID)",
		}},
		{"llmpersistence/sqlite_sessions.go", "LLMSQLiteOwner.Release", []string{"addAgentSessionFacts(effects, identity.RunID, lease.SessionID)"}},
		{"llmpersistence/sqlite_sessions.go", "LLMSQLiteOwner.Rotate", []string{"addAgentSessionFacts(effects, identity.RunID, rec.sessionID, newID)"}},
		{"llmpersistence/sqlite_sessions.go", "LLMSQLiteOwner.IncrementTurn", []string{"addAgentSessionFacts(effects, identity.RunID, sessionID)"}},
		{"llmpersistence/sqlite_sessions.go", "LLMSQLiteOwner.AdoptSessionID", []string{"addAgentSessionFacts(effects, identity.RunID, rec.sessionID)"}},
		{"llmpersistence/postgres_sessions.go", "LLMPostgresOwner.ResetAll", []string{"addAgentSessionFacts(effects, disposition.RunID, disposition.SessionID)"}},
		{"llmpersistence/sqlite_sessions.go", "LLMSQLiteOwner.ResetAll", []string{"addAgentSessionFacts(effects, disposition.RunID, disposition.SessionID)"}},
		{"genericschedule/owner.go", "AdmitTx", []string{"insertActivationTx(ctx, tx, dialectFor(postgres), scope, activation)", "effects.AddFact(storedRunID, privaterunforkrevision.FamilyTimers, activation.ID)"}},
		{"genericschedule/owner.go", "addTimerEffect", []string{"effects.AddFact(runID, privaterunforkrevision.FamilyTimers, activationID)"}},
		{"genericschedule/owner.go", "PostgresOwner.failMalformed", []string{"addTimerEffect(effects, runID, storedTimerID)"}},
		{"genericschedule/owner.go", "SQLiteOwner.failMalformed", []string{"addTimerEffect(effects, runID, storedTimerID)"}},
		{"genericschedule/owner.go", "PrepareOccurrenceTx", []string{
			"addTimerEffect(effects, runID, storedTimerID)",
			"addTimerEffect(effects, activation.Command.RunID, activation.ID)",
			"addTimerEffect(effects, activation.Command.RunID, activation.ID)",
			"addTimerEffect(effects, activation.Command.RunID, activation.ID)",
		}},
		{"genericschedule/owner.go", "CancelTx", []string{"effects.AddFact(activation.Command.RunID, privaterunforkrevision.FamilyTimers, activation.ID)"}},
		{"genericschedule/owner.go", "CancelAdmissionTx", []string{"effects.AddFact(activation.Command.RunID, privaterunforkrevision.FamilyTimers, activation.ID)"}},
		{"genericschedule/owner.go", "CancelRunsTx", []string{"CancelTx(ctx, tx, postgres, effects, runtimegenericschedule.CancelCommand{ActivationID: ref.ActivationID, Cause: cause, CancelledAt: cancelledAt})"}},
		{"workflowtimer/cancellation.go", "CancelRunsTx", []string{"effects.AddFact(ref.RunID, privaterunforkrevision.FamilyTimers, ref.ActivationID)"}},
		{"pipelinepersistence/owner.go", "addTimerRevisionEffects", []string{"effects.AddFact(runID, privaterunforkrevision.FamilyTimers, timerID)"}},
		{"pipelinepersistence/workflow_engine_timer_commit.go", "commitWorkflowEngineTimerMutation", []string{
			"insertWorkflowEngineTimerActivation(ctx, tx, postgres, effects, activation)",
			"cancelWorkflowEngineTimerActivation(ctx, tx, postgres, effects, activation)",
		}},
		{"pipelinepersistence/workflow_engine_timer_commit.go", "insertWorkflowEngineTimerActivation", []string{"effects.AddFact(storedRunID, privaterunforkrevision.FamilyTimers, storedTimerID)"}},
		{"pipelinepersistence/workflow_engine_timer_commit.go", "cancelWorkflowEngineTimerActivation", []string{"effects.AddFact(storedRunID, privaterunforkrevision.FamilyTimers, storedTimerID)"}},
		{"pipelinepersistence/workflow_timer_occurrence_commit.go", "advanceWorkflowEngineTimerOccurrence", []string{"addTimerRevisionEffects(effects, storedRunID, storedTimerID)"}},
		{"pipelinepersistence/generic_schedule_occurrence_commit.go", "commitGenericScheduleOccurrence", []string{
			"addTimerRevisionEffects(effects, persisted.Command.RunID, persisted.ID)",
			"privategenericschedule.AdvanceOccurrenceTx(txctx, tx, postgres, command)",
			"addTimerRevisionEffects(effects, next.Command.RunID, next.ID)",
		}},
		{"pipelinepersistence/workflow_engine_mutation_commit.go", "commitWorkflowEngineState", []string{
			"commitPostgresWorkflowEngineState(ctx, tx, effects, record)", "commitSQLiteWorkflowEngineState(ctx, tx, record)",
			"effects.AddFact(record.Identity.RunID, privaterunforkrevision.FamilyEntityMetadata, record.EntityID)",
		}},
		{"pipelinepersistence/workflow_engine_mutation_commit.go", "commitPostgresWorkflowEngineState", []string{
			"effects.AddFact(storedRunID, privaterunforkrevision.FamilyEntityMetadata, storedEntityID)",
			"effects.AddFact(storedRunID, privaterunforkrevision.FamilyEntityMetadata, storedEntityID)",
		}},
		{"eventrecord/postgres/adapter.go", "Insert", []string{"runforkrevision.NewFactRef(runforkrevision.FamilyEvents, storedEventID)", "effects.AddFacts(storedRunID, ref)"}},
		{"eventrecord/sqlite/adapter.go", "Insert", []string{"runforkrevision.NewFactRef(runforkrevision.FamilyEvents, record.EventID)", "effects.AddFacts(record.RunID, ref)"}},
		{"pipelinepersistence/owner_operations.go", "insertCommittedPipelineScopeTx", []string{
			"declareEventRevisionFact(ctx, tx, effects, eventID, privaterunforkrevision.FamilyCommittedReplayScopes, eventID)",
			"declareEventRevisionFact(ctx, tx, effects, eventID, privaterunforkrevision.FamilyCommittedReplayScopes, eventID)",
		}},
		{"pipelinepersistence/owner_operations.go", "writeExactPlatformPipelineReceipt", []string{"uuid.NewString()", "declareEventRevisionFact(ctx, tx, effects, eventID, privaterunforkrevision.FamilyEventReceipts, receiptID)"}},
		{"pipelinepersistence/owner_operations.go", "declareEventRevisionFact", []string{"privaterunforkrevision.NewFactRef(family, key)", "effects.AddFacts(runID.String, ref)"}},
		{"delivery/adapter.go", "Adapter.insertExactObligation", []string{"effects.AddFact(record.RunID, privaterunforkrevision.FamilyEventDeliveries, record.DeliveryID)"}},
		{"delivery/adapter.go", "Adapter.claimLocked", []string{"effects.AddFact(record.RunID, privaterunforkrevision.FamilyEventDeliveries, record.DeliveryID)"}},
		{"delivery/adapter.go", "Adapter.BindAgentSession", []string{"effects.AddFact(record.RunID, privaterunforkrevision.FamilyEventDeliveries, record.DeliveryID)"}},
		{"delivery/adapter.go", "Adapter.RenewClaim", []string{"effects.AddFact(claim.RunID(), privaterunforkrevision.FamilyEventDeliveries, claim.DeliveryID())"}},
		{"delivery/adapter.go", "Adapter.prepareProviderOriginRecovery", []string{"effects.AddFact(claim.RunID(), privaterunforkrevision.FamilyEventDeliveries, claim.DeliveryID())"}},
		{"delivery/adapter.go", "Adapter.settle", []string{"effects.AddFact(record.RunID, privaterunforkrevision.FamilyEventDeliveries, record.DeliveryID)"}},
		{"delivery/adapter.go", "Adapter.CommitPipelineHandoff", []string{"effects.AddFact(runID, privaterunforkrevision.FamilyEventDeliveries, deliveryID)"}},
		{"delivery/adapter.go", "Adapter.terminalizeDeliveries", []string{"effects.AddFact(record.RunID, privaterunforkrevision.FamilyEventDeliveries, id)"}},
		{"delivery/adapter.go", "Adapter.persistHandlerRuleSelection", []string{"effects.AddFact(runID, privaterunforkrevision.FamilyEventDeliveries, deliveryID)"}},
		{"delivery/adapter.go", "Adapter.insertAttempt", []string{"effects.AddFact(runID, privaterunforkrevision.FamilyEventDeliveries, deliveryID)"}},
		{"delivery/adapter.go", "Adapter.expireAttempt", []string{"effects.AddFact(record.RunID, privaterunforkrevision.FamilyEventDeliveries, record.DeliveryID)"}},
		{"delivery/adapter.go", "Adapter.completeAttempt", []string{"effects.AddFact(claim.RunID(), privaterunforkrevision.FamilyEventDeliveries, claim.DeliveryID())"}},
		{"delivery/adapter.go", "Adapter.closeAttemptForTerminalization", []string{"effects.AddFact(claim.RunID(), privaterunforkrevision.FamilyEventDeliveries, claim.DeliveryID())"}},
		{"delivery/adapter.go", "Adapter.insertTerminalizedAttempt", []string{"effects.AddFact(runID, privaterunforkrevision.FamilyEventDeliveries, deliveryID)"}},
		{"delivery/adapter.go", "Adapter.insertOutcome", []string{"declareOutcomeDeadLetterEffects(ctx, tx, effects, deliveryID, version)"}},
		{"delivery/dead_letters.go", "declareDeadLetterEffect", []string{"privaterunforkrevision.RunIDForEvent(ctx, tx, eventID)", "effects.AddFact(runID, privaterunforkrevision.FamilyDeadLetters, deadLetterID)"}},
		{"delivery/dead_letters.go", "declareOutcomeDeadLetterEffects", []string{"effects.AddFact(runID.String, privaterunforkrevision.FamilyDeadLetters, deadLetterID)"}},
		{"delivery/dead_letters.go", "insertPostgresDeadLetterRecord", []string{"uuid.NewString()", "declareDeadLetterEffect(ctx, tx, effects, rec.OriginalEventID, deadLetterID)"}},
		{"delivery/dead_letters.go", "insertSQLiteDeadLetterRecord", []string{"uuid.NewString()", "declareDeadLetterEffect(ctx, tx, effects, rec.OriginalEventID, deadLetterID)"}},
		{"delivery/lifecycle.go", "DeliveryPostgresOwner.renewClaimTx", []string{"postgresDeliveryAdapter.RenewClaim(ctx, tx, effects, claim, lease)"}},
		{"delivery/lifecycle.go", "DeliverySQLiteOwner.renewClaimTx", []string{"sqliteDeliveryAdapter.RenewClaim(ctx, tx, effects, claim, lease)"}},
		{"delivery/lifecycle.go", "DeliveryPostgresOwner.TerminalizeRunDeliveriesTx", []string{"postgresDeliveryAdapter.TerminalizeRun(ctx, tx, effects, story, runID, reason)", "s.RecordDeadLetterTx(ctx, tx, story, effects, diagnostic, false)"}},
		{"delivery/lifecycle.go", "DeliverySQLiteOwner.TerminalizeRunDeliveriesTx", []string{"sqliteDeliveryAdapter.TerminalizeRun(ctx, tx, effects, story, runID, reason)", "s.RecordDeadLetterTx(ctx, tx, story, effects, diagnostic, false)"}},
		{"delivery/receiver_materialization.go", "Adapter.TerminalizeMaterializationDependents", []string{"a.publicationRecords(ctx, tx, materializer.EventID)", "validateMaterializerAuthority(materializer, record.Snapshot)", "a.terminalizeDeliveries(ctx, tx, effects, story, ids, reason, failure)"}},
		{"pipelinepersistence/publication_settlement_kernel.go", "settlePipelineMemberTx", []string{
			"writePipelineDispositionTx(ctx, tx, effects, claim.EventID(), claim.Purpose(), disposition, postgres, now)",
			"postgresDeliveryAdapter.CommitPipelineHandoff(ctx, tx, effects, claim.EventID())",
			"sqliteDeliveryAdapter.CommitPipelineHandoff(ctx, tx, effects, claim.EventID())",
		}},
		{"pipelinepersistence/fan_out_obligation.go", "insertFanOutEntitySourceRevisionTx", []string{"effects.AddFact(runID, privaterunforkrevision.FamilyEntityMutations, mutationID)"}},
		{"pipelinepersistence/fan_out_obligation.go", "commitFanOutIntentTx", []string{"privaterunforkrevision.FanOutIntentFact(request.Key)", "effects.AddFacts(request.Key.RunID, ref)"}},
		{"pipelinepersistence/fan_out_owner.go", "blockFanOutClaim", []string{"privaterunforkrevision.FanOutIntentFact(request.Claim.Key)", "effects.AddFacts(request.Claim.Key.RunID, ref)"}},
		{"pipelinepersistence/fan_out_owner.go", "commitFanOutChunk", []string{
			"privaterunforkrevision.FanOutOutcomeFact(command.Claim.Key, outcome.Ordinal)", "effects.AddFacts(command.Claim.Key.RunID, ref)",
			"privaterunforkrevision.FanOutIntentFact(command.Claim.Key)", "effects.AddFacts(command.Claim.Key.RunID, ref)",
		}},
		{"pipelinepersistence/fan_out_owner.go", "cancelRunFanOut", []string{"privaterunforkrevision.FanOutIntentFact(intent.Request.Key)", "effects.AddFacts(runID, ref)"}},
		{"pipelinepersistence/fan_out_barrier_owner.go", "addFanOutBarrierRevisionEffect", []string{"privaterunforkrevision.FanOutBarrierFact(key)", "effects.AddFacts(key.RunID, ref)"}},
		{"pipelinepersistence/fan_out_barrier_owner.go", "advanceFanOutDeliveryBarriersTx", []string{"addFanOutBarrierRevisionEffect(effects, registration.IntentKey)"}},
		{"pipelinepersistence/fan_out_barrier_owner.go", "suppressSupersededArmedFanOutBarrierTx", []string{"addFanOutBarrierRevisionEffect(effects, registration.IntentKey)"}},
		{"pipelinepersistence/fan_out_barrier_owner.go", "suppressSupersededPendingFanOutBarriersTx", []string{"addFanOutBarrierRevisionEffect(effects, registration.IntentKey)"}},
		{"pipelinepersistence/fan_out_barrier_owner.go", "terminalizeDeadLetteredFanOutBarrierOutcomesTx", []string{"addFanOutBarrierRevisionEffect(effects, key)"}},
		{"pipelinepersistence/fan_out_barrier_owner.go", "suppressRunTerminalFanOutBarriersTx", []string{"addFanOutBarrierRevisionEffect(effects, candidate.key)"}},
		{"pipelinepersistence/fan_out_barrier_owner.go", "materializeRunForkFanOutBarrierTx", []string{"addFanOutBarrierRevisionEffect(effects, registration.IntentKey)"}},
		{"pipelinepersistence/fan_out_barrier_owner.go", "commitFanOutBarrierRegistrationTx", []string{"addFanOutBarrierRevisionEffect(effects, registration.IntentKey)"}},
		{"pipelinepersistence/fan_out_barrier_owner.go", "commitFanOutBarrierCompletionTx", []string{"addFanOutBarrierRevisionEffect(effects, key)"}},
		{"runforkpersistence/run_fork_fan_out_materializer.go", "materializeRunForkFanOutObligations", []string{
			"runforkrevision.FanOutIntentFact(intent.Request.Key)", "effects.AddFacts(forkRunID, intentRef)",
			"runforkrevision.FanOutOutcomeFact(intent.Request.Key, outcome.Ordinal)", "effects.AddFacts(forkRunID, outcomeRef)",
		}},
		{"runforkpersistence/run_fork_fan_out_materializer.go", "bindRunForkFanOutPendingReplays", []string{"runforkrevision.FanOutOutcomeFact(key, replay.Ordinal)", "effects.AddFacts(forkRunID, ref)"}},
	}
}

func revisionGuardFunctions(source string) (map[string]*ast.FuncDecl, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "writer.go", source, 0)
	if err != nil {
		return nil, err
	}
	result := map[string]*ast.FuncDecl{}
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		name := fn.Name.Name
		if fn.Recv != nil {
			typ := fn.Recv.List[0].Type
			if pointer, ok := typ.(*ast.StarExpr); ok {
				typ = pointer.X
			}
			name = revisionGuardNode(typ) + "." + name
		}
		if result[name] != nil {
			return nil, fmt.Errorf("duplicate writer symbol %s", name)
		}
		result[name] = fn
	}
	return result, nil
}

func revisionGuardNode(node ast.Node) string {
	var out bytes.Buffer
	if err := printer.Fprint(&out, token.NewFileSet(), node); err != nil {
		panic(err)
	}
	return out.String()
}

func validateRevisionExactWriter(source string, contract revisionExactWriterContract) error {
	return validateRevisionWriterCalls(source, contract, true)
}

func validateRevisionWriterCalls(source string, contract revisionExactWriterContract, exact bool) error {
	functions, err := revisionGuardFunctions(source)
	if err != nil {
		return err
	}
	fn := functions[contract.symbol]
	if fn == nil {
		return fmt.Errorf("missing exact writer %s", contract.symbol)
	}
	calls := map[string]int{}
	var whole bool
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		calls[revisionGuardNode(call)]++
		if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "Add" && revisionGuardNode(selector.X) == "effects" {
			whole = true
		}
		return true
	})
	if whole && exact {
		return fmt.Errorf("exact writer %s regressed to whole-family effects.Add", contract.symbol)
	}
	for _, required := range contract.calls {
		expression, err := parser.ParseExpr(required)
		if err != nil {
			return fmt.Errorf("invalid guard contract: %w", err)
		}
		key := revisionGuardNode(expression)
		if calls[key] == 0 {
			return fmt.Errorf("exact writer %s missing actual call %s", contract.symbol, required)
		}
		calls[key]--
	}
	return nil
}

// Run-wide operations which retain only affected runs still declare whole
// families explicitly. This is not an exemption for known-key normal writes.
func TestRunForkRevisionExplicitBroadWriterContributions(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	for _, contract := range []revisionExactWriterContract{
		{"runlifecycle/active_run_quiescence.go", "terminateActiveRunSessionsTx", []string{"effects.Add(runID, runforkrevision.FamilyAgentSessions)"}},
		{"runlifecycle/active_run_quiescence.go", "sqliteTerminateActiveRunSessionsTx", []string{"effects.Add(runID, runforkrevision.FamilyAgentSessions)"}},
		{"pipelinepersistence/standing_service.go", "standingServiceAdapter.quiesceStandingRunTx", []string{
			"s.revisionEffects.Add(runID, privaterunforkrevision.FamilyAgentSessions)",
			"s.revisionEffects.Add(runID, privaterunforkrevision.FamilyAgentSessions)",
		}},
		{"delivery/adapter.go", "Adapter.ActivateNormalAuthority", []string{"declareAuthorityDeliveryRuns(ctx, tx, authority, effects)"}},
		{"delivery/lifecycle.go", "declareAuthorityDeliveryRuns", []string{"effects.Add(runID, privaterunforkrevision.FamilyEventDeliveries)"}},
	} {
		body, err := os.ReadFile(filepath.Join(root, "internal/store/internal/backend", contract.path))
		if err != nil {
			t.Fatal(err)
		}
		if err := validateRevisionWriterCalls(string(body), contract, false); err != nil {
			t.Fatalf("explicit broad owner %s/%s: %v", contract.path, contract.symbol, err)
		}
	}
}

func TestRunForkRevisionEnumeratedWriterContributions(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	for _, row := range []struct {
		path, symbol, rangeOver, call string
	}{
		{"agentpersistence/lifecycle.go", "addLifecycleRevisionEffects", "result.Subordinate.Sessions", "effects.AddFact(session.RunID, privaterunforkrevision.FamilyAgentSessions, session.PreviousSessionID)"},
		{"agentpersistence/lifecycle.go", "addLifecycleRevisionEffects", "result.Subordinate.Sessions", "effects.AddFact(session.RunID, privaterunforkrevision.FamilyAgentSessions, session.SuccessorSessionID)"},
		{"llmpersistence/owner.go", "addAgentSessionFacts", "otherSessionIDs", "effects.AddFact(runID, runforkrevision.FamilyAgentSessions, id)"},
		{"workflowtimer/cancellation.go", "CancelRunsTx", "refs", "effects.AddFact(ref.RunID, privaterunforkrevision.FamilyTimers, ref.ActivationID)"},
		{"genericschedule/owner.go", "CancelRunsTx", "refs", "CancelTx(ctx, tx, postgres, effects, runtimegenericschedule.CancelCommand{ActivationID: ref.ActivationID, Cause: cause, CancelledAt: cancelledAt})"},
	} {
		body, err := os.ReadFile(filepath.Join(root, "internal/store/internal/backend", row.path))
		if err != nil {
			t.Fatal(err)
		}
		if err := validateRevisionEnumeratedContribution(string(body), row.symbol, row.rangeOver, row.call); err != nil {
			t.Fatalf("%s/%s: %v", row.path, row.symbol, err)
		}
	}
	for _, row := range []struct {
		path, symbol string
		sqlite       bool
	}{
		{"llmpersistence/postgres_sessions.go", "LLMPostgresOwner.ResetAll", false},
		{"llmpersistence/sqlite_sessions.go", "LLMSQLiteOwner.ResetAll", true},
	} {
		body, err := os.ReadFile(filepath.Join(root, "internal/store/internal/backend", row.path))
		if err != nil {
			t.Fatal(err)
		}
		if err := validateRevisionResetAllContribution(string(body), row.symbol, row.sqlite); err != nil {
			t.Fatalf("%s/%s: %v", row.path, row.symbol, err)
		}
	}
}

func validateRevisionResetAllContribution(source, symbol string, sqlite bool) error {
	const required = "addAgentSessionFacts(effects, disposition.RunID, disposition.SessionID)"
	if err := validateRevisionExactWriter(source, revisionExactWriterContract{symbol: symbol, calls: []string{required}}); err != nil {
		return err
	}
	functions, err := revisionGuardFunctions(source)
	if err != nil {
		return err
	}
	fn := functions[symbol]
	guardSource := `package p; func f() { if err := ` + required + `; err != nil { return err } }; func reset() { if err := handoff.ResetAttempt(); err != nil { return err } }`
	guards, err := revisionGuardFunctions(guardSource)
	if err != nil {
		return err
	}
	wantFirst := revisionGuardNode(guards["f"].Body.List[0])
	wantReset := revisionGuardNode(guards["reset"].Body.List[0])
	loops, resets := 0, 0
	var whole, invalid bool
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if loop, ok := node.(*ast.RangeStmt); ok && revisionGuardNode(loop.X) == "summary.OrphanedSessions" {
			loops++
			// No deduplication, continue, or conditional may precede this call.
			if len(loop.Body.List) == 0 || revisionGuardNode(loop.Body.List[0]) != wantFirst {
				invalid = true
			}
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if revisionGuardNode(call.Fun) == "addAgentSessionEffect" {
			whole = true
		}
		if sqlite && revisionGuardNode(call.Fun) == "s.runRuntimeMutationOutcome" {
			for _, arg := range call.Args {
				if callback, ok := arg.(*ast.FuncLit); ok {
					resets++
					// Both attempt-local owners reset before any reads/mutations;
					// cancellation failure must abort, not permit stale submission.
					if len(callback.Body.List) < 2 || revisionGuardNode(callback.Body.List[0]) != wantReset || revisionGuardNode(callback.Body.List[1]) != "summary = runtimesessions.ResetSummary{}" {
						invalid = true
					}
				}
			}
		}
		return true
	})
	if loops != 1 || whole || invalid || (sqlite && resets != 1) {
		return fmt.Errorf("%s must contribute every physical session before dedup and reset SQLite handoff then summary before mutation: loops=%d whole=%v invalid=%v callbacks=%d", symbol, loops, whole, invalid, resets)
	}
	return nil
}

func validateRevisionEnumeratedContribution(source, symbol, rangeOver, required string) error {
	functions, err := revisionGuardFunctions(source)
	if err != nil {
		return err
	}
	fn := functions[symbol]
	if fn == nil {
		return fmt.Errorf("missing enumerated writer %s", symbol)
	}
	expression, err := parser.ParseExpr(required)
	if err != nil {
		return err
	}
	matched := 0
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		loop, ok := node.(*ast.RangeStmt)
		if !ok || revisionGuardNode(loop.X) != rangeOver {
			return true
		}
		ast.Inspect(loop.Body, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok && revisionGuardNode(call) == revisionGuardNode(expression) {
				matched++
			}
			return true
		})
		return false
	})
	if matched != 1 {
		return fmt.Errorf("%s must contribute each identity inside full range %s, matches=%d", symbol, rangeOver, matched)
	}
	return nil
}

func TestRunForkRevisionExactContributorGuardHostileControls(t *testing.T) {
	contract := revisionExactWriterContract{symbol: "write", calls: []string{"effects.AddFact(runID, revision.FamilyEntityMutations, mutationID)"}}
	valid := `package p; func write() error { return effects.AddFact(runID, revision.FamilyEntityMutations, mutationID) }`
	for _, tc := range []struct {
		name, source string
		wantError    bool
	}{
		{"exact", valid, false},
		{"missing", `package p; func write() error { return nil }`, true},
		{"comment_is_not_call", "package p\nfunc write() error { // effects.AddFact(runID, revision.FamilyEntityMutations, mutationID)\nreturn nil }", true},
		{"string_is_not_call", `package p; func write() error { _ = "effects.AddFact(runID, revision.FamilyEntityMutations, mutationID)"; return nil }`, true},
		{"wrong_generated_key", strings.Replace(valid, ", mutationID)", ", entityID)", 1), true},
		{"wrong_run", strings.Replace(valid, "(runID,", "(foreignRunID,", 1), true},
		{"wrong_family", strings.Replace(valid, "FamilyEntityMutations", "FamilyEntityMetadata", 1), true},
		{"whole_fallback", strings.Replace(valid, "return effects", "_ = effects.Add(runID, revision.FamilyEntityMutations); return effects", 1), true},
		{"other_function", strings.Replace(valid, "func write", "func unrelated", 1) + `; func write() error { return nil }`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateRevisionExactWriter(tc.source, contract); (err != nil) != tc.wantError {
				t.Fatalf("guard error=%v wantError=%t", err, tc.wantError)
			}
		})
	}
	t.Run("typed_refs", func(t *testing.T) {
		contract := revisionExactWriterContract{symbol: "write", calls: []string{"revision.NewFactRef(revision.FamilyEvents, eventID)", "effects.AddFacts(runID, ref)"}}
		source := `package p; func write() error { ref, err := revision.NewFactRef(revision.FamilyEvents, eventID); if err != nil { return err }; return effects.AddFacts(runID, ref) }`
		if err := validateRevisionExactWriter(source, contract); err != nil {
			t.Fatal(err)
		}
		if err := validateRevisionExactWriter(strings.Replace(source, "return effects.AddFacts(runID, ref)", "return nil", 1), contract); err == nil {
			t.Fatal("unchecked typed ref without contribution passed")
		}
	})
	t.Run("whole_cold_writer_is_not_blanket_forbidden", func(t *testing.T) {
		source := valid + `; func coldBulk() error { return effects.Add(runID, revision.FamilyEntityMutations) }`
		if err := validateRevisionExactWriter(source, contract); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("receiver_is_not_interchangeable", func(t *testing.T) {
		contract := revisionExactWriterContract{symbol: "SQLite.write", calls: []string{"effects.AddFact(runID, revision.FamilyEntityMutations, mutationID)"}}
		source := `package p; func (s *Postgres) write() error { return effects.AddFact(runID, revision.FamilyEntityMutations, mutationID) }; func (s *SQLite) write() error { return nil }`
		if err := validateRevisionExactWriter(source, contract); err == nil {
			t.Fatal("Postgres contribution concealed missing SQLite contribution")
		}
	})
	t.Run("both_backend_branches_required", func(t *testing.T) {
		both := contract
		both.calls = append(append([]string(nil), contract.calls...), contract.calls...)
		if err := validateRevisionExactWriter(valid, both); err == nil {
			t.Fatal("one contribution satisfied two backend branches")
		}
	})
	t.Run("enumeration_not_first_member", func(t *testing.T) {
		call := "effects.AddFact(session.RunID, revision.FamilyAgentSessions, session.SessionID)"
		source := `package p; func write() error { for _, session := range result.Sessions { if err := effects.AddFact(session.RunID, revision.FamilyAgentSessions, session.SessionID); err != nil { return err } }; return nil }`
		if err := validateRevisionEnumeratedContribution(source, "write", "result.Sessions", call); err != nil {
			t.Fatal(err)
		}
		if err := validateRevisionEnumeratedContribution(strings.Replace(source, "range result.Sessions", "range result.Sessions[:1]", 1), "write", "result.Sessions", call); err == nil {
			t.Fatal("first-item-only effect satisfied full enumerated contributor contract")
		}
	})
	t.Run("reset_all_physical_sessions_before_run_dedup", func(t *testing.T) {
		contribution := `if err := addAgentSessionFacts(effects, disposition.RunID, disposition.SessionID); err != nil { return err };`
		dedup := `if _, exists := seenRuns[disposition.RunID]; exists { continue };`
		handoffReset := `if err := handoff.ResetAttempt(); err != nil { return err };`
		summaryReset := `summary = runtimesessions.ResetSummary{};`
		valid := `package p; func (s *SQLite) ResetAll() { s.runRuntimeMutationOutcome(ctx, label, effects, func() error { ` + handoffReset + summaryReset + ` for _, disposition := range summary.OrphanedSessions { ` + contribution + dedup + ` seenRuns[disposition.RunID] = struct{}{} }; return nil }) }`
		for _, tc := range []struct {
			name, source string
			wantError    bool
		}{
			{"exact_before_dedup", valid, false},
			{"after_dedup", strings.Replace(valid, contribution+dedup, dedup+contribution, 1), true},
			{"first_session_only", strings.Replace(valid, "range summary.OrphanedSessions", "range summary.OrphanedSessions[:1]", 1), true},
			{"wrong_physical_key", strings.Replace(valid, "disposition.SessionID", "disposition.RunID", 1), true},
			{"whole_helper", strings.Replace(valid, contribution, contribution+`_ = addAgentSessionEffect(effects, disposition.RunID);`, 1), true},
			{"missing_handoff_reset", strings.Replace(valid, handoffReset, "", 1), true},
			{"late_handoff_reset", strings.Replace(valid, handoffReset+summaryReset, summaryReset+handoffReset, 1), true},
			{"ignored_handoff_error", strings.Replace(valid, handoffReset, `_ = handoff.ResetAttempt();`, 1), true},
			{"swallowed_handoff_error", strings.Replace(valid, handoffReset, `if err := handoff.ResetAttempt(); err != nil { return nil };`, 1), true},
			{"wrong_handoff_owner", strings.Replace(valid, "handoff.ResetAttempt()", "other.ResetAttempt()", 1), true},
			{"mutation_before_handoff_reset", strings.Replace(valid, handoffReset, "mutate();"+handoffReset, 1), true},
			{"missing_summary_reset", strings.Replace(valid, "summary = runtimesessions.ResetSummary{};", "", 1), true},
			{"late_summary_reset", strings.Replace(valid, "summary = runtimesessions.ResetSummary{};", "scan(); summary = runtimesessions.ResetSummary{};", 1), true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if err := validateRevisionResetAllContribution(tc.source, "SQLite.ResetAll", true); (err != nil) != tc.wantError {
					t.Fatalf("reset-all guard error=%v wantError=%v", err, tc.wantError)
				}
			})
		}
	})
	t.Run("omitted_joined_physical_table", func(t *testing.T) {
		physical := runForkRevisionPhysicalTables()
		delete(physical, "event_delivery_handler_rule_selections")
		if err := validateRevisionPhysicalContributors(revisionProjectionContributorCensus(), physical); err == nil {
			t.Fatal("joined selection contributor omitted without guard refusal")
		}
	})
	t.Run("new_join", func(t *testing.T) {
		source := "package p; func canonicalProjectionSpec() { switch family { case FamilyDeadLetters: query := `SELECT * FROM dead_letters d JOIN unclassified_dependency x ON x.id=d.id`; _ = query } }"
		got, err := revisionProjectionContributors(source)
		if err != nil {
			t.Fatal(err)
		}
		if err := validateRevisionPhysicalContributors(got, runForkRevisionPhysicalTables()); err == nil || !strings.Contains(err.Error(), "unclassified_dependency") {
			t.Fatalf("new joined contributor not refused: %v", err)
		}
	})
	t.Run("source_field_includes_base_and_join", func(t *testing.T) {
		source := `package p; func canonicalProjectionSpec() { switch family { case FamilyDeadLetters: spec = projectionSpec{query: "SELECT d.id", source: "dead_letters d JOIN events e ON e.event_id=d.original_event_id"} } }`
		got, err := revisionProjectionContributors(source)
		if err != nil || !reflect.DeepEqual(got["FamilyDeadLetters"], []string{"dead_letters", "events"}) {
			t.Fatalf("source field lost physical contributors: got=%v err=%v", got, err)
		}
	})
}

func TestRunForkRevisionJoinedPhysicalWriterGuardHostileControls(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "internal/store")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	// Include quoted/schema-qualified writes so spelling alone cannot evade
	// the governed contributor table set. This fixture is never executed SQL.
	source := "package store\nfunc unclassifiedContributor() {\n" +
		"_ = `INSERT INTO event_delivery_handler_rule_selections (delivery_id) VALUES (?)`\n" +
		"_ = `UPDATE \"event_delivery_attempts\" SET open_marker = false`\n" +
		"_ = `DELETE FROM \"public\".\"event_delivery_outcomes\" WHERE delivery_id = $1`\n}"
	if err := os.WriteFile(filepath.Join(dir, "future.go"), []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	got := scanRunForkRevisionPhysicalWriters(t, root)
	want := map[string]struct{}{}
	for _, table := range []string{"event_delivery_handler_rule_selections", "event_delivery_attempts", "event_delivery_outcomes"} {
		want["internal/store/future.go|unclassifiedContributor|"+table] = struct{}{}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("joined physical writer escaped inventory: got=%v want=%v", got, want)
	}
	for _, row := range runForkRevisionWriterCensus() {
		for _, symbol := range row.Symbols {
			for _, table := range row.Tables {
				if _, allowed := got[row.Path+"|"+symbol+"|"+table]; allowed {
					t.Fatal("unclassified physical writer matched an existing inventory row")
				}
			}
		}
	}
}
