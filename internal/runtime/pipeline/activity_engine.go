package pipeline

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/eventschema"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/httpresponsesuccess"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const activityRequestEventType = events.EventType("platform.activity_requested")

type pipelineActivityIntentWriter struct {
	coordinator *PipelineCoordinator
}

func (w pipelineActivityIntentWriter) WriteActivityIntents(ctx context.Context, intents []runtimeengine.ActivityIntent) error {
	if len(intents) == 0 || w.coordinator == nil || w.coordinator.bus == nil {
		return nil
	}
	outbox := w.coordinator.bus.EngineOutbox()
	if outbox == nil {
		return fmt.Errorf("activity intent writer requires pipeline outbox")
	}
	immediate := make([]runtimeengine.ActivityIntent, 0, len(intents))
	for _, intent := range intents {
		intent = intent.Normalized()
		if intent.ApprovalDecision == "" {
			immediate = append(immediate, intent)
			continue
		}
		if _, ok := PipelineSQLTxFromContext(ctx); !ok {
			return fmt.Errorf("approved activity %s must be materialized in the workflow mutation", intent.ActivityID)
		}
		store, ok := w.coordinator.decisionCards.(decisioncard.ProposedEffectStore)
		if !ok || store == nil {
			return fmt.Errorf("proposed-effect continuation store is required for approved activity %s", intent.ActivityID)
		}
		card, continuation, err := w.coordinator.buildProposedEffectCard(ctx, intent)
		if err != nil {
			return err
		}
		if err := store.CreateProposedEffectCard(ctx, card, continuation); err != nil {
			return err
		}
	}
	requests, err := activityRequestEmitIntents(immediate)
	if err != nil {
		return err
	}
	if err := outbox.WriteOutbox(ctx, requests); err != nil {
		return err
	}
	for _, intent := range intents {
		intent = intent.Normalized()
		detail := map[string]any{
			"activity_id": intent.ActivityID, "tool": intent.Tool, "effect_class": string(intent.EffectClass),
			"success_event": intent.SuccessEvent, "failure_event": intent.FailureEvent,
			"retry_max_attempts": intent.RetryMaxAttempts, "retry_backoff": intent.RetryBackoff,
			"fork_policy": string(intent.ForkPolicy),
		}
		if intent.Generation.Valid() {
			detail["loop_generation"] = intent.Generation.PayloadValue()
			detail["loop_stage"] = intent.LoopStage
		}
		action := "intent_persisted"
		if intent.ApprovalDecision != "" {
			action = "proposal_persisted"
			detail["approval_decision"] = intent.ApprovalDecision
		}
		entry := RuntimeLogEntry{
			Level:     "info",
			Component: "activity",
			Action:    action,
			EventID:   activityRequestEventID(intent),
			EventType: intent.SuccessEvent,
			EntityID:  intent.EntityID.String(),
			Detail:    detail,
		}
		logCtx := WithoutPipelineSQLTxContext(context.WithoutCancel(ctx))
		logIntent := func() {
			_ = w.coordinator.bus.LogRuntime(logCtx, entry)
		}
		if _, txActive := PipelineSQLTxFromContext(ctx); txActive {
			if !QueuePipelinePostCommitAction(ctx, logIntent) {
				return fmt.Errorf("activity intent runtime log requires a post-commit action queue")
			}
			continue
		}
		if err := w.coordinator.bus.LogRuntime(logCtx, entry); err != nil {
			return err
		}
	}
	return nil
}

type pipelineActivityDispatcher struct {
	coordinator *PipelineCoordinator
	client      *http.Client
}

func (d pipelineActivityDispatcher) DispatchActivities(ctx context.Context, intents []runtimeengine.ActivityIntent) error {
	if len(intents) == 0 {
		return nil
	}
	if d.coordinator == nil || d.coordinator.bus == nil {
		return fmt.Errorf("activity dispatcher requires pipeline bus")
	}
	dispatcher := d.coordinator.bus.EngineDispatcher()
	if dispatcher == nil {
		return fmt.Errorf("activity dispatcher requires pipeline outbox dispatcher")
	}
	immediate := make([]runtimeengine.ActivityIntent, 0, len(intents))
	for _, intent := range intents {
		intent = intent.Normalized()
		if intent.ApprovalDecision == "" {
			immediate = append(immediate, intent)
		}
	}
	requests, err := activityRequestEmitIntents(immediate)
	if err != nil {
		return err
	}
	return dispatcher.DispatchPostCommit(ctx, requests)
}

func (pc *PipelineCoordinator) buildProposedEffectCard(ctx context.Context, intent runtimeengine.ActivityIntent) (decisioncard.Card, decisioncard.ProposedEffectContinuation, error) {
	intent = intent.Normalized()
	executionMode, err := decisioncard.CausalExecutionMode(ctx)
	if err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, err
	}
	if !intent.ExecutionMode.Valid() {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, fmt.Errorf("approved activity %s requires typed causal execution mode", intent.ActivityID)
	}
	if executionMode != intent.ExecutionMode {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, fmt.Errorf("approved activity %s execution mode conflicts with its source event", intent.ActivityID)
	}
	if intent.ApprovalDecision == "" {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, fmt.Errorf("approved activity decision is required")
	}
	if intent.EffectClass != runtimecontracts.ActivityEffectClassNonIdempotentWrite {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, fmt.Errorf("approved activity %s must be non_idempotent_write", intent.ActivityID)
	}
	runID := strings.TrimSpace(intent.SourceRunID)
	requestEventID := activityRequestEventID(intent)
	flowInstance := firstNonEmptyString(intent.FlowInstance, intent.FlowID.String(), "root")
	createdAt := time.Now().UTC()
	bundleHash := workflowGateBundleHash(ctx, pc)
	if bundleHash == "" {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, fmt.Errorf("bundle identity is required before proposing approved activity %s", intent.ActivityID)
	}
	workflowVersion := ""
	if source := pc.SemanticSource(); source != nil {
		workflowVersion = strings.TrimSpace(source.WorkflowVersion())
	}
	if workflowVersion == "" {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, fmt.Errorf("workflow version is required before proposing approved activity %s", intent.ActivityID)
	}
	continuation := decisioncard.ProposedEffectContinuation{
		CardID: decisioncard.ProposedEffectCardID(requestEventID, intent.ApprovalDecision), RunID: runID,
		RequestEventID: requestEventID, ActivityID: intent.ActivityID, Tool: intent.Tool,
		BundleHash: bundleHash, WorkflowVersion: workflowVersion, Input: intent.Input,
		EffectClass: intent.EffectClass, SuccessEvent: intent.SuccessEvent, FailureEvent: intent.FailureEvent,
		RevisionEvent: intent.RevisionEvent, RejectedEvent: intent.RejectedEvent,
		RetryMaxAttempts: intent.RetryMaxAttempts, RetryBackoff: intent.RetryBackoff, ForkPolicy: intent.ForkPolicy,
		EntityID: intent.EntityID.String(), NodeID: intent.NodeID.String(), FlowID: intent.FlowID.String(), FlowInstance: flowInstance,
		HandlerEventKey: intent.HandlerEventKey, SourceEventID: intent.SourceEventID, SourceRunID: intent.SourceRunID,
		SourceTaskID: intent.SourceTaskID, ParentEventID: intent.ParentEventID, ChainDepth: intent.ChainDepth,
		Attempt: intent.Attempt, Generation: intent.Generation, LoopStage: intent.LoopStage,
		ExecutionMode:  intent.ExecutionMode,
		ReplyContextID: intent.Context.ReplyContextID(), State: decisioncard.ProposedEffectPending,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}.Canonical()
	effect, err := continuation.EffectValue()
	if err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, fmt.Errorf("encode proposed activity effect: %w", err)
	}
	effectHash, err := canonicaljson.HashValue(effect)
	if err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, fmt.Errorf("hash proposed activity effect: %w", err)
	}
	continuation.EffectContentHash = effectHash
	anchor, err := decisioncard.NewProposedEffectAnchor(decisioncard.ProposedEffectAnchor{
		RequestEventID: requestEventID, ActivityID: intent.ActivityID, Decision: intent.ApprovalDecision,
		Scope: decisioncard.Scope{Kind: decisioncard.ScopeEntity, FlowInstance: flowInstance, EntityID: intent.EntityID.String()},
	})
	if err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, err
	}
	outcomes := map[string]runtimecontracts.WorkflowGateOutcomePlan{
		"approve": {Verdict: "approve", Label: "Approve"},
		"revise": {
			Verdict: "revise", Label: "Request revision",
			Input: map[string]runtimecontracts.WorkflowGateInputField{"feedback": {Type: "text", Label: "Feedback", Required: true}},
		},
		"reject": {
			Verdict: "reject", Label: "Reject",
			Input: map[string]runtimecontracts.WorkflowGateInputField{"reason": {Type: "text", Label: "Reason"}},
		},
	}
	snapshot, err := decisioncard.FreezeSnapshot(intent.ApprovalDecision, "", map[string]any{
		"activity_id": intent.ActivityID, "tool": intent.Tool, "effect_class": string(intent.EffectClass), "input": intent.Input.Interface(),
	}, outcomes)
	if err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, err
	}
	provenance, err := canonicaljson.FromGo(map[string]any{
		"source_event": intent.SourceEventID, "flow_id": intent.FlowID.String(), "flow_instance": flowInstance, "node_id": intent.NodeID.String(),
		"execution_mode": executionMode,
	})
	if err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, fmt.Errorf("admit proposed-effect provenance: %w", err)
	}
	card, err := decisioncard.New(decisioncard.Card{
		CardID: continuation.CardID, RunID: runID, Anchor: anchor, Snapshot: snapshot,
		ExecutionMode:     executionMode,
		EffectContentHash: effectHash, BundleHash: bundleHash, WorkflowVersion: workflowVersion,
		EffectiveCadence: pc.decisionCardCadence.Stamp(createdAt), Provenance: provenance, CreatedAt: createdAt,
	})
	if err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, err
	}
	if err := continuation.Validate(card); err != nil {
		return decisioncard.Card{}, decisioncard.ProposedEffectContinuation{}, err
	}
	return card, continuation, nil
}

func (d pipelineActivityDispatcher) executeActivityIntent(ctx context.Context, intent runtimeengine.ActivityIntent) error {
	intent = intent.Normalized()
	var err error
	ctx, err = activityExecutionContext(ctx, intent)
	if err != nil {
		return err
	}
	source := d.coordinator.SemanticSource()
	if err := d.admitActivityContractPin(ctx, intent, source); err != nil {
		return err
	}
	if source == nil {
		return runtimefailures.New(runtimefailures.ClassInternalFailure, "activity_semantic_source_missing", "activity-runtime", "execute_activity", nil)
	}
	tool, ok := d.coordinator.channelActivityTools[intent.Tool]
	if !ok {
		tool, ok = source.ToolEntries()[intent.Tool]
	}
	if !ok {
		return d.publishActivityFailure(ctx, intent, runtimefailures.New(runtimefailures.ClassTargetUnreachable, "activity_tool_not_declared", "activity-runtime", "execute_activity", map[string]any{"tool": intent.Tool}))
	}
	toolEffectClass := runtimecontracts.NormalizeActivityEffectClass(tool.EffectClass)
	if toolEffectClass != intent.EffectClass {
		return d.publishActivityFailure(ctx, intent, runtimefailures.New(runtimefailures.ClassSchemaInvalid, "activity_effect_class_changed", "activity-runtime", "execute_activity", map[string]any{
			"tool": intent.Tool, "requested_effect_class": string(intent.EffectClass), "declared_effect_class": string(toolEffectClass),
		}))
	}
	if !runtimecontracts.SupportedActivityEffectClass(toolEffectClass) {
		return d.publishActivityFailure(ctx, intent, runtimefailures.New(runtimefailures.ClassSchemaInvalid, "activity_effect_class_unsupported", "activity-runtime", "execute_activity", map[string]any{
			"tool": intent.Tool, "effect_class": string(toolEffectClass),
		}))
	}
	var mockResponse *providerconnectors.AdmittedMockResponse
	if intent.ExecutionMode == runtimeeffects.ExecutionModeMock {
		if toolEffectClass != runtimecontracts.ActivityEffectClassNonIdempotentWrite {
			return d.publishActivityFailure(ctx, intent, runtimefailures.New(runtimefailures.ClassSchemaInvalid, "mock_activity_effect_class_unsupported", "activity-runtime", "admit_mock_activity", map[string]any{
				"tool": intent.Tool, "effect_class": string(toolEffectClass), "required_effect_class": string(runtimecontracts.ActivityEffectClassNonIdempotentWrite),
			}))
		}
		if reused, err := d.reuseExistingNonIdempotentActivityAttempt(ctx, intent); err != nil || reused {
			return err
		}
		admitted, err := d.coordinator.mockConnectorResponses.Admit(intent.Tool, tool)
		if err != nil {
			return d.publishActivityFailure(ctx, intent, runtimefailures.Wrap(runtimefailures.ClassSchemaInvalid, "mock_connector_response_not_admitted", "activity-runtime", "admit_mock_activity", map[string]any{"tool": intent.Tool}, err))
		}
		mockResponse = &admitted
	}
	if toolEffectClass != runtimecontracts.ActivityEffectClassNonIdempotentWrite {
		if err := d.admitReadOnlyActivityGeneration(ctx, intent); err != nil {
			return d.publishActivityFailure(ctx, intent, err)
		}
	}
	if toolEffectClass == runtimecontracts.ActivityEffectClassNonIdempotentWrite {
		return d.executeNonIdempotentActivityIntent(ctx, intent, tool, mockResponse)
	}
	client := d.client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	if recorded, ok, err := d.recordedActivityResult(ctx, intent); err != nil {
		return err
	} else if ok {
		d.logActivityRuntime(ctx, intent, "result_reused", map[string]any{
			"activity_id":       intent.ActivityID,
			"tool":              intent.Tool,
			"effect_class":      string(intent.EffectClass),
			"result_event_id":   recorded.EventID,
			"result_event_type": recorded.EventType,
		})
		return nil
	}
	maxAttempts := activityRetryMaxAttempts(intent, toolEffectClass)
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		attemptIntent := intent
		attemptIntent.Attempt = attempt
		d.logActivityRuntime(ctx, attemptIntent, "attempt_started", map[string]any{
			"activity_id":        attemptIntent.ActivityID,
			"tool":               attemptIntent.Tool,
			"effect_class":       string(attemptIntent.EffectClass),
			"attempt":            attempt,
			"retry_max_attempts": maxAttempts,
		})
		result, err := d.executeActivityHTTPTool(ctx, client, attemptIntent, tool)
		if err == nil {
			return d.publishActivitySuccess(ctx, attemptIntent, result)
		}
		lastErr = err
		failure := runtimefailures.FromError(err, "activity-runtime", "execute_http_tool")
		d.logActivityRuntime(ctx, attemptIntent, "attempt_failed", map[string]any{
			"activity_id":        attemptIntent.ActivityID,
			"tool":               attemptIntent.Tool,
			"effect_class":       string(attemptIntent.EffectClass),
			"attempt":            attempt,
			"retry_max_attempts": maxAttempts,
			"failure":            failure.Failure,
		})
		if runtimeengine.FailureDispositionFor(failure) != runtimeengine.FailureDispositionRetry {
			break
		}
		if attempt < maxAttempts {
			if err := waitActivityRetryBackoff(ctx, intent.RetryBackoff, attempt); err != nil {
				return err
			}
		}
	}
	failureIntent := intent
	failureIntent.Attempt = maxAttempts
	return d.publishActivityFailure(ctx, failureIntent, lastErr)
}

func (d pipelineActivityDispatcher) reuseExistingNonIdempotentActivityAttempt(ctx context.Context, intent runtimeengine.ActivityIntent) (bool, error) {
	if d.coordinator == nil || d.coordinator.workflowStore == nil || !d.coordinator.workflowStore.Enabled() {
		return false, nil
	}
	intent.Attempt = 1
	expected := activityAttemptStartRecord(intent, activityInputHash(intent.Input))
	existing, ok, err := d.coordinator.workflowStore.LoadActivityAttempt(ctx, expected.RequestEventID)
	if err != nil {
		return true, d.publishActivityFailure(ctx, intent, activityDependencyFailure(err, intent.Tool, "load_activity_attempt"))
	}
	if !ok {
		return false, nil
	}
	if err := validateActivityAttemptClaimIdentity(existing, expected); err != nil {
		return true, err
	}
	return true, d.publishExistingActivityAttempt(ctx, intent, existing)
}

func activityExecutionContext(ctx context.Context, intent runtimeengine.ActivityIntent) (context.Context, error) {
	if !intent.ExecutionMode.Valid() {
		return nil, fmt.Errorf("activity %s requires typed causal execution mode", intent.ActivityID)
	}
	if active, ok := runtimeeffects.ExecutionModeFromContext(ctx); ok && active != intent.ExecutionMode {
		return nil, fmt.Errorf("activity %s execution mode conflicts with dispatch context", intent.ActivityID)
	}
	if authority, ok := runtimeeffects.AuthorityFromContext(ctx); ok && authority.ExecutionMode != intent.ExecutionMode {
		return nil, fmt.Errorf("activity %s execution mode conflicts with completion authority", intent.ActivityID)
	}
	return runtimeeffects.WithExecutionMode(ctx, intent.ExecutionMode), nil
}

func (d pipelineActivityDispatcher) admitActivityContractPin(ctx context.Context, intent runtimeengine.ActivityIntent, source semanticview.Source) error {
	if intent.BundleHash == "" && intent.WorkflowVersion == "" {
		return nil
	}
	if intent.BundleHash == "" || intent.WorkflowVersion == "" {
		return runtimefailures.New(runtimefailures.ClassSchemaInvalid, "activity_contract_pin_incomplete", "activity-runtime", "admit_activity_contract", map[string]any{
			"activity_id": intent.ActivityID, "bundle_hash": intent.BundleHash, "workflow_version": intent.WorkflowVersion,
		})
	}
	currentBundleHash := workflowGateBundleHash(ctx, d.coordinator)
	currentWorkflowVersion := ""
	if source != nil {
		currentWorkflowVersion = strings.TrimSpace(source.WorkflowVersion())
	}
	if currentBundleHash == intent.BundleHash && currentWorkflowVersion == intent.WorkflowVersion {
		return nil
	}
	return DeferPipelineReceipt(runtimefailures.New(
		runtimefailures.ClassDependencyUnavailable,
		"activity_contract_pin_unavailable",
		"activity-runtime",
		"admit_activity_contract",
		map[string]any{
			"activity_id":               intent.ActivityID,
			"required_bundle_hash":      intent.BundleHash,
			"current_bundle_hash":       currentBundleHash,
			"required_workflow_version": intent.WorkflowVersion,
			"current_workflow_version":  currentWorkflowVersion,
		},
	))
}

func (d pipelineActivityDispatcher) admitReadOnlyActivityGeneration(ctx context.Context, intent runtimeengine.ActivityIntent) error {
	if !intent.Generation.Valid() {
		return nil
	}
	if d.coordinator == nil || d.coordinator.workflowStore == nil || !d.coordinator.workflowStore.Enabled() {
		return runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "activity_loop_store_unavailable", "activity-runtime", "admit_read_only_activity", nil)
	}
	unlock := d.coordinator.lockWorkflowEntity(intent.EntityID.String())
	defer unlock()
	return d.coordinator.workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
		instance, ok, err := d.coordinator.workflowStore.Load(txctx, intent.EntityID.String())
		if err != nil {
			return err
		}
		current := false
		if ok {
			current, err = workflowLoopGenerationCurrent(&instance, intent.Generation, intent.LoopStage)
		}
		if err != nil {
			return err
		}
		if !current {
			return runtimefailures.New(runtimefailures.ClassStaleArrival, "activity_loop_generation_stale", "activity-runtime", "admit_read_only_activity", map[string]any{
				"activity_id": intent.ActivityID, "loop_id": intent.Generation.LoopID,
				"revision_id": intent.Generation.RevisionID, "expected_stage": intent.LoopStage,
			})
		}
		return nil
	})
}

func (d pipelineActivityDispatcher) executeNonIdempotentActivityIntent(ctx context.Context, intent runtimeengine.ActivityIntent, tool runtimecontracts.ToolSchemaEntry, mockResponse *providerconnectors.AdmittedMockResponse) error {
	if d.coordinator == nil || d.coordinator.workflowStore == nil || !d.coordinator.workflowStore.Enabled() {
		return d.publishActivityFailure(ctx, intent, runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "activity_journal_unavailable", "activity-runtime", "load_activity_attempt", map[string]any{"tool": strings.TrimSpace(intent.Tool)}))
	}
	intent.Attempt = 1
	startRecord := activityAttemptStartRecord(intent, activityInputHash(intent.Input))
	unlock := d.coordinator.lockWorkflowEntity(intent.EntityID.String())
	started, inserted, err := d.coordinator.workflowStore.ClaimActivityAttemptForLoopGeneration(ctx, startRecord)
	unlock()
	if err != nil {
		if reconciled, ok, loadErr := d.coordinator.workflowStore.LoadActivityAttempt(ctx, startRecord.RequestEventID); loadErr == nil && ok {
			if claimErr := validateActivityAttemptClaimIdentity(reconciled, startRecord); claimErr != nil {
				return claimErr
			}
			return d.publishExistingActivityAttempt(ctx, intent, reconciled)
		}
		return d.publishActivityFailure(ctx, intent, activityDependencyFailure(err, intent.Tool, "start_activity_attempt"))
	}
	if !inserted {
		return d.publishExistingActivityAttempt(ctx, intent, started)
	}
	if mockResponse != nil {
		result, err := mockResponse.Materialize()
		if err != nil {
			return d.publishActivityFailure(ctx, intent, runtimefailures.Wrap(runtimefailures.ClassInternalFailure, "mock_connector_response_materialization_failed", "activity-runtime", "materialize_mock_activity", map[string]any{"tool": intent.Tool}, err))
		}
		terminal := started.withTerminal(
			ActivityAttemptStatusSucceeded,
			activityResultEventID(intent, intent.SuccessEvent),
			intent.SuccessEvent,
			activitySuccessPayload(intent, result),
			nil,
		)
		stored, err := d.coordinator.workflowStore.CompleteActivityAttempt(ctx, terminal)
		if err != nil {
			return activityDependencyFailure(err, intent.Tool, "complete_activity_attempt")
		}
		return d.publishJournaledActivityResult(ctx, intent, stored)
	}
	client := d.client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	prepared, err := d.prepareActivityHTTPTool(ctx, client, intent, tool)
	if err != nil {
		cause := runtimefailures.FromError(err, "activity-runtime", "prepare_non_idempotent_http")
		terminal := started.withTerminal(
			ActivityAttemptStatusFailed,
			activityResultEventID(intent, intent.FailureEvent),
			intent.FailureEvent,
			activityFailurePayload(intent, cause),
			&cause.Failure,
		)
		stored, journalErr := d.coordinator.workflowStore.CompleteActivityAttempt(ctx, terminal)
		if journalErr != nil {
			return activityDependencyFailure(journalErr, intent.Tool, "complete_activity_attempt")
		}
		return d.publishJournaledActivityResult(ctx, intent, stored)
	}
	result, err := executePreparedActivityHTTPTool(ctx, prepared)
	var terminal ActivityAttemptRecord
	if err != nil {
		redacted := runtimemanagedcredentials.RedactString(err.Error(), prepared.secrets...)
		cause := runtimefailures.FromError(err, "activity-runtime", "execute_non_idempotent_http")
		status := ActivityAttemptStatusFailed
		if activityHTTPOutcomeUncertain(err) {
			status = ActivityAttemptStatusUncertain
			cause = runtimefailures.FromError(runtimefailures.Wrap(runtimefailures.ClassOutcomeUncertain, "activity_provider_outcome_uncertain", "activity-runtime", "execute_non_idempotent_http", map[string]any{
				"activity_id": intent.ActivityID, "tool": intent.Tool, "redacted_cause": redacted,
			}, err), "activity-runtime", "execute_non_idempotent_http")
		}
		payload := activityFailurePayload(intent, cause)
		terminal = started.withTerminal(status, activityResultEventID(intent, intent.FailureEvent), intent.FailureEvent, payload, &cause.Failure)
	} else {
		payload := activitySuccessPayload(intent, result)
		terminal = started.withTerminal(ActivityAttemptStatusSucceeded, activityResultEventID(intent, intent.SuccessEvent), intent.SuccessEvent, payload, nil)
	}
	var stored ActivityAttemptRecord
	if terminal.Status == ActivityAttemptStatusUncertain {
		stored, err = d.coordinator.workflowStore.MarkActivityAttemptUncertain(ctx, terminal)
	} else {
		stored, err = d.coordinator.workflowStore.CompleteActivityAttempt(ctx, terminal)
	}
	if err != nil {
		return activityDependencyFailure(err, intent.Tool, "complete_activity_attempt")
	}
	return d.publishJournaledActivityResult(ctx, intent, stored)
}

func activityDependencyFailure(err error, tool, operation string) error {
	if _, ok := runtimefailures.As(err); ok {
		return err
	}
	return runtimefailures.Wrap(runtimefailures.ClassDependencyUnavailable, "activity_journal_operation_failed", "activity-runtime", operation, map[string]any{"tool": strings.TrimSpace(tool)}, err)
}

type activityRecordedResult struct {
	EventID   string
	EventType string
	Payload   json.RawMessage
}

func (d pipelineActivityDispatcher) recordedActivityResult(ctx context.Context, intent runtimeengine.ActivityIntent) (activityRecordedResult, bool, error) {
	if d.coordinator == nil || d.coordinator.db == nil {
		return activityRecordedResult{}, false, nil
	}
	db := d.coordinator.db
	successID := activityResultEventID(intent, intent.SuccessEvent)
	failureID := activityResultEventID(intent, intent.FailureEvent)
	var (
		rows *sql.Rows
		err  error
	)
	if d.coordinator.workflowStore != nil && d.coordinator.workflowStore.isSQLite() {
		rows, err = db.QueryContext(ctx, `
			SELECT event_id, event_name, payload
			FROM events
			WHERE event_id IN (?, ?)
		`, successID, failureID)
	} else {
		rows, err = db.QueryContext(ctx, `
			SELECT event_id::text, event_name, payload::text
			FROM events
			WHERE event_id IN ($1::uuid, $2::uuid)
		`, successID, failureID)
	}
	if err != nil {
		return activityRecordedResult{}, false, fmt.Errorf("lookup recorded activity result %s: %w", intent.ActivityID, err)
	}
	defer rows.Close()
	var found []activityRecordedResult
	for rows.Next() {
		var result activityRecordedResult
		var payload string
		if err := rows.Scan(&result.EventID, &result.EventType, &payload); err != nil {
			return activityRecordedResult{}, false, fmt.Errorf("scan recorded activity result %s: %w", intent.ActivityID, err)
		}
		result.Payload = json.RawMessage(payload)
		found = append(found, result)
	}
	if err := rows.Err(); err != nil {
		return activityRecordedResult{}, false, fmt.Errorf("iterate recorded activity result %s: %w", intent.ActivityID, err)
	}
	switch len(found) {
	case 0:
		return activityRecordedResult{}, false, nil
	case 1:
		return found[0], true, nil
	default:
		return activityRecordedResult{}, false, fmt.Errorf("activity request %s has both success and failure results recorded", activityRequestEventID(intent))
	}
}

func (d pipelineActivityDispatcher) logActivityRuntime(ctx context.Context, intent runtimeengine.ActivityIntent, action string, detail map[string]any) {
	if d.coordinator == nil || d.coordinator.bus == nil {
		return
	}
	intent = intent.Normalized()
	if detail == nil {
		detail = map[string]any{}
	}
	requestEventID := activityRequestEventID(intent)
	detail["request_event_id"] = requestEventID
	lineageEventID := requestEventID
	if inbound, ok := runtimecorrelation.InboundEventFromContext(ctx); ok && inbound.Type() == activityRequestEventType {
		lineageEventID = inbound.ID()
	} else if lineage, ok := runtimecorrelation.RuntimeLineageFromContext(ctx); ok && lineage.SubjectEventType == string(activityRequestEventType) && lineage.SubjectEventID != "" {
		lineageEventID = lineage.SubjectEventID
	}
	if intent.Generation.Valid() {
		detail["loop_generation"] = intent.Generation.PayloadValue()
		detail["loop_stage"] = intent.LoopStage
	}
	_ = d.coordinator.bus.LogRuntime(ctx, RuntimeLogEntry{
		Level:     "info",
		Component: "activity",
		Action:    action,
		EventID:   lineageEventID,
		EventType: intent.SuccessEvent,
		EntityID:  intent.EntityID.String(),
		Detail:    detail,
	})
}

func (pc *PipelineCoordinator) handleActivityRequestEvent(ctx context.Context, evt events.Event) (bool, error) {
	if pc == nil || evt.Type() != activityRequestEventType {
		return false, nil
	}
	intent, err := activityIntentFromRequestEvent(evt)
	if err != nil {
		return true, err
	}
	dispatcher := pipelineActivityDispatcher{coordinator: pc}
	if err := dispatcher.executeActivityIntent(ctx, intent); err != nil {
		return true, err
	}
	return true, nil
}

type activityRequestPayload struct {
	ActivityID       string                       `json:"activity_id"`
	Tool             string                       `json:"tool"`
	BundleHash       string                       `json:"bundle_hash,omitempty"`
	WorkflowVersion  string                       `json:"workflow_version,omitempty"`
	EffectClass      string                       `json:"effect_class"`
	SuccessEvent     string                       `json:"success_event"`
	FailureEvent     string                       `json:"failure_event"`
	RevisionEvent    string                       `json:"revision_event,omitempty"`
	RejectedEvent    string                       `json:"rejected_event,omitempty"`
	RetryMaxAttempts int                          `json:"retry_max_attempts"`
	RetryBackoff     string                       `json:"retry_backoff"`
	ForkPolicy       string                       `json:"fork_policy"`
	EntityID         string                       `json:"entity_id"`
	NodeID           string                       `json:"node_id"`
	FlowID           string                       `json:"flow_id"`
	FlowInstance     string                       `json:"flow_instance,omitempty"`
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

func activityRequestEmitIntents(intents []runtimeengine.ActivityIntent) ([]runtimeengine.EmitIntent, error) {
	if len(intents) == 0 {
		return nil, nil
	}
	out := make([]runtimeengine.EmitIntent, 0, len(intents))
	for _, intent := range intents {
		request, err := activityRequestEmitIntent(intent)
		if err != nil {
			return nil, err
		}
		out = append(out, request)
	}
	return out, nil
}

// ExecuteDurableActivity admits one externally requested activity through the
// same persisted request and attempt journal used by authored activity rows.
// A terminal journal record is authoritative when publish acknowledgment is
// lost, and replay never re-executes a completed non-idempotent activity.
func (pc *PipelineCoordinator) ExecuteDurableActivity(ctx context.Context, intent runtimeengine.ActivityIntent) (ActivityAttemptRecord, error) {
	if pc == nil || pc.bus == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return ActivityAttemptRecord{}, runtimefailures.New(
			runtimefailures.ClassDependencyUnavailable,
			"activity_journal_unavailable",
			"activity-runtime",
			"execute_durable_activity",
			nil,
		)
	}
	intent = intent.Normalized()
	request, err := activityRequestEmitIntent(intent)
	if err != nil {
		return ActivityAttemptRecord{}, err
	}
	publishCtx := events.WithDeliveryContext(ctx, request.Context)
	publishErr := pc.bus.Publish(publishCtx, request.Event)
	var dispatchErr error
	if publishErr == nil {
		_, dispatchErr = pc.handleActivityRequestEvent(publishCtx, request.Event)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 35*time.Second)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		record, ok, loadErr := pc.workflowStore.LoadActivityAttempt(ctx, activityRequestEventID(intent))
		if loadErr != nil {
			return ActivityAttemptRecord{}, activityDependencyFailure(loadErr, intent.Tool, "load_activity_attempt_after_publish")
		}
		if ok {
			if identityErr := validateActivityAttemptClaimIdentity(record, activityAttemptStartRecord(intent, activityInputHash(intent.Input))); identityErr != nil {
				return ActivityAttemptRecord{}, identityErr
			}
			if record.Status != ActivityAttemptStatusStarted || publishErr != nil {
				// A started record after publish failure is the fail-closed
				// acknowledgment-loss outcome; the adapter must not resend it.
				return record, nil
			}
		}
		if publishErr != nil {
			return ActivityAttemptRecord{}, publishErr
		}
		if dispatchErr != nil {
			return ActivityAttemptRecord{}, dispatchErr
		}
		select {
		case <-waitCtx.Done():
			if ok {
				return record, nil
			}
			return ActivityAttemptRecord{}, runtimefailures.Wrap(
				runtimefailures.ClassDependencyUnavailable,
				"activity_attempt_missing_after_publish",
				"activity-runtime",
				"execute_durable_activity",
				map[string]any{"request_event_id": activityRequestEventID(intent), "tool": intent.Tool},
				waitCtx.Err(),
			)
		case <-ticker.C:
		}
	}
}

func activityRequestEmitIntent(intent runtimeengine.ActivityIntent) (runtimeengine.EmitIntent, error) {
	intent = intent.Normalized()
	if !intent.ExecutionMode.Valid() {
		return runtimeengine.EmitIntent{}, fmt.Errorf("activity %s requires typed causal execution mode", intent.ActivityID)
	}
	payload := activityRequestPayloadFromIntent(intent)
	value, err := canonicaljson.FromGo(payload)
	if err != nil {
		return runtimeengine.EmitIntent{}, err
	}
	value, err = value.With("input", intent.Input)
	if err != nil {
		return runtimeengine.EmitIntent{}, fmt.Errorf("attach admitted activity input: %w", err)
	}
	raw, err := canonicaljson.Encode(value)
	if err != nil {
		return runtimeengine.EmitIntent{}, err
	}
	evt := events.NewChildEventWithLineage(
		activityRequestEventID(intent),
		activityRequestEventType,
		events.PlatformProducer(runtimeWorkflowID),
		intent.SourceTaskID,
		raw,
		intent.ChainDepth+1,
		events.EventLineage{
			RunID:         intent.SourceRunID,
			ParentEventID: firstNonEmptyString(intent.SourceEventID, intent.ParentEventID),
			TaskID:        intent.SourceTaskID,
			ExecutionMode: intent.ExecutionMode,
		},
		events.EventEnvelope{
			EntityID: intent.EntityID.String(),
			Source: events.RouteIdentity{
				FlowID:   intent.FlowID.String(),
				EntityID: intent.EntityID.String(),
			},
		},
		time.Now().UTC(),
	)
	return runtimeengine.EmitIntent{Event: evt, Context: intent.Context}, nil
}

func activityRequestEventID(intent runtimeengine.ActivityIntent) string {
	intent = intent.Normalized()
	return activityidentity.RequestEventID(activityIdentityFact(intent))
}

func activityResultEventID(intent runtimeengine.ActivityIntent, eventType string) string {
	intent = intent.Normalized()
	return activityidentity.ResultEventID(activityIdentityFact(intent), eventType)
}

func activityIdentityFact(intent runtimeengine.ActivityIntent) activityidentity.Fact {
	return activityidentity.Fact{
		RunID: intent.SourceRunID, SourceEventID: intent.SourceEventID, ParentEventID: intent.ParentEventID,
		EntityID: intent.EntityID.String(), FlowID: intent.FlowID.String(), NodeID: intent.NodeID.String(),
		HandlerEventKey: intent.HandlerEventKey, ActivityID: intent.ActivityID, Tool: intent.Tool,
		Attempt: intent.Attempt, RevisionID: intent.Generation.RevisionID,
	}
}

func activityRetryMaxAttempts(intent runtimeengine.ActivityIntent, effectClass runtimecontracts.ActivityEffectClass) int {
	if intent.RetryMaxAttempts > 0 {
		return intent.RetryMaxAttempts
	}
	defaults := runtimecontracts.ActivityRetryDefaultsForEffectClass(effectClass)
	if defaults.MaxAttempts > 0 {
		return defaults.MaxAttempts
	}
	return 1
}

func waitActivityRetryBackoff(ctx context.Context, backoff string, completedAttempt int) error {
	delay := activityRetryDelay(backoff, completedAttempt)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func activityRetryDelay(backoff string, completedAttempt int) time.Duration {
	switch strings.TrimSpace(strings.ToLower(backoff)) {
	case "", "none":
		return 0
	case "exponential":
		if completedAttempt < 1 {
			completedAttempt = 1
		}
		delay := 10 * time.Millisecond
		for i := 1; i < completedAttempt && delay < time.Second; i++ {
			delay *= 2
		}
		if delay > time.Second {
			return time.Second
		}
		return delay
	default:
		return 10 * time.Millisecond
	}
}

func activityRequestPayloadFromIntent(intent runtimeengine.ActivityIntent) activityRequestPayload {
	intent = intent.Normalized()
	return activityRequestPayload{
		ActivityID:       intent.ActivityID,
		Tool:             intent.Tool,
		BundleHash:       intent.BundleHash,
		WorkflowVersion:  intent.WorkflowVersion,
		EffectClass:      string(intent.EffectClass),
		SuccessEvent:     intent.SuccessEvent,
		FailureEvent:     intent.FailureEvent,
		RevisionEvent:    intent.RevisionEvent,
		RejectedEvent:    intent.RejectedEvent,
		RetryMaxAttempts: intent.RetryMaxAttempts,
		RetryBackoff:     intent.RetryBackoff,
		ForkPolicy:       string(intent.ForkPolicy),
		EntityID:         intent.EntityID.String(),
		NodeID:           intent.NodeID.String(),
		FlowID:           intent.FlowID.String(),
		FlowInstance:     intent.FlowInstance,
		HandlerEventKey:  intent.HandlerEventKey,
		SourceEventID:    intent.SourceEventID,
		SourceRunID:      intent.SourceRunID,
		SourceTaskID:     intent.SourceTaskID,
		ParentEventID:    intent.ParentEventID,
		ChainDepth:       intent.ChainDepth,
		Attempt:          intent.Attempt,
		Generation:       intent.Generation,
		LoopStage:        intent.LoopStage,
	}
}

func activityIntentFromRequestEvent(evt events.Event) (runtimeengine.ActivityIntent, error) {
	if !evt.ExecutionMode().Valid() {
		return runtimeengine.ActivityIntent{}, fmt.Errorf("activity request %s carries invalid execution mode %q", evt.ID(), evt.ExecutionMode())
	}
	semanticPayload, err := canonicaljson.Decode(evt.Payload())
	if err != nil {
		return runtimeengine.ActivityIntent{}, fmt.Errorf("decode activity request %s: %w", evt.ID(), err)
	}
	var payload activityRequestPayload
	if err := canonicaljson.ValueInto(semanticPayload, &payload); err != nil {
		return runtimeengine.ActivityIntent{}, fmt.Errorf("decode activity request %s: %w", evt.ID(), err)
	}
	input, ok := semanticPayload.Lookup("input")
	if !ok || input.Kind() != semanticvalue.KindObject {
		return runtimeengine.ActivityIntent{}, fmt.Errorf("activity request %s input must be a semantic object", evt.ID())
	}
	intent := runtimeengine.ActivityIntent{
		Context:          evt.DeliveryContext(),
		ActivityID:       payload.ActivityID,
		Tool:             payload.Tool,
		BundleHash:       payload.BundleHash,
		WorkflowVersion:  payload.WorkflowVersion,
		Input:            input,
		EffectClass:      runtimecontracts.NormalizeActivityEffectClass(payload.EffectClass),
		SuccessEvent:     payload.SuccessEvent,
		FailureEvent:     payload.FailureEvent,
		RevisionEvent:    payload.RevisionEvent,
		RejectedEvent:    payload.RejectedEvent,
		RetryMaxAttempts: payload.RetryMaxAttempts,
		RetryBackoff:     payload.RetryBackoff,
		ForkPolicy:       runtimecontracts.ActivityForkPolicy(strings.TrimSpace(payload.ForkPolicy)),
		EntityID:         identity.NormalizeEntityID(payload.EntityID),
		NodeID:           identity.NormalizeNodeID(payload.NodeID),
		FlowID:           identity.NormalizeFlowID(payload.FlowID),
		FlowInstance:     payload.FlowInstance,
		HandlerEventKey:  payload.HandlerEventKey,
		SourceEventID:    payload.SourceEventID,
		SourceRunID:      payload.SourceRunID,
		SourceTaskID:     payload.SourceTaskID,
		ParentEventID:    payload.ParentEventID,
		ChainDepth:       payload.ChainDepth,
		Attempt:          payload.Attempt,
		Generation:       payload.Generation,
		LoopStage:        payload.LoopStage,
		ExecutionMode:    evt.ExecutionMode(),
	}.Normalized()
	if intent.ActivityID == "" || intent.Tool == "" || intent.SuccessEvent == "" || intent.FailureEvent == "" {
		return runtimeengine.ActivityIntent{}, fmt.Errorf("activity request %s is missing required activity identity", evt.ID())
	}
	if (intent.BundleHash == "") != (intent.WorkflowVersion == "") {
		return runtimeengine.ActivityIntent{}, fmt.Errorf("activity request %s carries an incomplete contract pin", evt.ID())
	}
	return intent, nil
}

type preparedActivityHTTPTool struct {
	toolName        string
	method          string
	url             string
	headers         http.Header
	body            []byte
	timeout         time.Duration
	client          *http.Client
	secrets         []string
	managedAuth     *activityManagedHTTPAuth
	success         *runtimecontracts.HTTPResponseSuccess
	responseMapping map[string]any
	outputSchema    runtimecontracts.ToolInputSchema
	compiledResult  *runtimecontracts.CompiledResultProjection
	inputHash       string
}

func (d pipelineActivityDispatcher) executeActivityHTTPTool(ctx context.Context, client *http.Client, intent runtimeengine.ActivityIntent, tool runtimecontracts.ToolSchemaEntry) (any, error) {
	prepared, err := d.prepareActivityHTTPTool(ctx, client, intent, tool)
	if err != nil {
		return nil, err
	}
	return executePreparedActivityHTTPTool(ctx, prepared)
}

func (d pipelineActivityDispatcher) prepareActivityHTTPTool(ctx context.Context, client *http.Client, intent runtimeengine.ActivityIntent, tool runtimecontracts.ToolSchemaEntry) (preparedActivityHTTPTool, error) {
	if tool.HTTP == nil {
		return preparedActivityHTTPTool{}, activityContractFailure(intent.Tool, "http_block_missing")
	}
	if strings.TrimSpace(tool.RateLimit) != "" || strings.TrimSpace(tool.RateLimitMaxWait) != "" {
		return preparedActivityHTTPTool{}, activityContractFailure(intent.Tool, "rate_limit_unsupported")
	}
	credentials := map[string]any{}
	secrets := []string{}
	if len(tool.Credentials) > 0 && tool.ManagedCredential != nil {
		return preparedActivityHTTPTool{}, activityContractFailure(intent.Tool, "credential_owners_conflict")
	}
	if len(tool.Credentials) > 0 {
		if intent.EffectClass != runtimecontracts.ActivityEffectClassNonIdempotentWrite {
			return preparedActivityHTTPTool{}, activityContractFailure(intent.Tool, "static_credential_effect_class_unsupported")
		}
		resolved, secretValues, err := d.resolveActivityToolCredentials(ctx, intent, tool.Credentials)
		if err != nil {
			return preparedActivityHTTPTool{}, activityAuthenticationFailure(err, intent.Tool, "resolve_static_credentials", "activity_credential")
		}
		credentials = resolved
		secrets = secretValues
	}
	if tool.ManagedCredential != nil {
		if intent.EffectClass != runtimecontracts.ActivityEffectClassNonIdempotentWrite || !strings.EqualFold(strings.TrimSpace(tool.Category), "provider_connector") {
			return preparedActivityHTTPTool{}, activityContractFailure(intent.Tool, "managed_credential_effect_class_unsupported")
		}
	}
	input, ok := intent.Input.ObjectMap()
	if !ok {
		return preparedActivityHTTPTool{}, activityContractFailure(intent.Tool, "input_not_object")
	}
	inputDTO := make(map[string]any, len(input))
	for name, value := range input {
		inputDTO[name] = value.Interface()
	}
	env := map[string]any{"input": inputDTO, "credentials": credentials}
	url, err := resolveActivityHTTPURLTemplate(tool.HTTP.URL, env)
	if err != nil {
		return preparedActivityHTTPTool{}, activityTemplateFailure(err, intent.Tool, "url", secrets)
	}
	url = strings.TrimSpace(url)
	if url == "" {
		return preparedActivityHTTPTool{}, runtimefailures.New(runtimefailures.ClassSchemaInvalid, "activity_url_empty", "activity-runtime", "prepare_http_request", map[string]any{"tool": strings.TrimSpace(intent.Tool)})
	}
	var body []byte
	if tool.HTTP.Body != nil {
		resolvedBody, err := resolveActivityTemplateTree(tool.HTTP.Body, env)
		if err != nil {
			return preparedActivityHTTPTool{}, activityTemplateFailure(err, intent.Tool, "body", secrets)
		}
		raw, err := json.Marshal(resolvedBody)
		if err != nil {
			return preparedActivityHTTPTool{}, runtimefailures.Wrap(runtimefailures.ClassSchemaInvalid, "activity_body_invalid", "activity-runtime", "prepare_http_request", map[string]any{"tool": strings.TrimSpace(intent.Tool)}, redactActivityError(err, secrets))
		}
		body = raw
	}
	method := strings.ToUpper(strings.TrimSpace(tool.HTTP.Method))
	if method == "" {
		method = http.MethodGet
	}
	timeout := 30 * time.Second
	if tool.HTTP.TimeoutSeconds > 0 {
		timeout = time.Duration(tool.HTTP.TimeoutSeconds) * time.Second
	}
	headers := make(http.Header, len(tool.HTTP.Headers))
	for key, value := range tool.HTTP.Headers {
		resolved, err := resolveActivityTemplateString(value, env)
		if err != nil {
			return preparedActivityHTTPTool{}, activityTemplateFailure(err, intent.Tool, "header", secrets)
		}
		headers.Set(strings.TrimSpace(key), strings.TrimSpace(resolved))
	}
	if len(body) > 0 && strings.TrimSpace(headers.Get("Content-Type")) == "" {
		headers.Set("Content-Type", "application/json")
	}
	if client == nil {
		client = &http.Client{Timeout: timeout}
	}
	managedAuth, err := d.resolveActivityManagedCredential(ctx, client, intent, tool)
	if err != nil {
		return preparedActivityHTTPTool{}, activityAuthenticationFailure(redactActivityError(err, secrets), intent.Tool, "resolve_managed_credential", "managed_credential")
	}
	if managedAuth != nil {
		if err := runtimemanagedcredentials.ApplyHTTPAuthorization(headers, managedAuth.HTTPAuthorization(), false); err != nil {
			return preparedActivityHTTPTool{}, activityAuthenticationFailure(redactActivityError(err, append(secrets, managedAuth.SecretValues()...)), intent.Tool, "apply_managed_credential", "managed_credential")
		}
		secrets = append(secrets, managedAuth.SecretValues()...)
	}
	return preparedActivityHTTPTool{
		toolName:        intent.Tool,
		method:          method,
		url:             url,
		headers:         headers,
		body:            body,
		timeout:         timeout,
		client:          client,
		secrets:         secrets,
		managedAuth:     managedAuth,
		success:         cloneActivityResponseSuccess(tool.ResponseSuccess),
		responseMapping: cloneActivityTemplateMap(tool.ResponseMapping),
		outputSchema:    tool.OutputSchema,
		compiledResult:  cloneCompiledResultProjection(tool.CompiledResult),
		inputHash:       activityInputHash(intent.Input),
	}, nil
}

func activityContractFailure(tool, reasonCode string) error {
	return runtimefailures.New(runtimefailures.ClassSchemaInvalid, "activity_tool_contract_invalid", "activity-runtime", "prepare_http_request", map[string]any{
		"tool": strings.TrimSpace(tool), "reason_code": strings.TrimSpace(reasonCode),
	})
}

func activityTemplateFailure(err error, tool, field string, secrets []string) error {
	return runtimefailures.Wrap(runtimefailures.ClassSchemaInvalid, "activity_template_invalid", "activity-runtime", "prepare_http_request", map[string]any{
		"tool": strings.TrimSpace(tool), "field": strings.TrimSpace(field),
	}, redactActivityError(err, secrets))
}

func activityAuthenticationFailure(err error, tool, operation, authKind string) error {
	if _, ok := runtimefailures.As(err); ok {
		return err
	}
	return runtimefailures.Wrap(runtimefailures.ClassAuthenticationNeeded, "activity_credential_required", "activity-runtime", operation, map[string]any{
		"auth_kind": strings.TrimSpace(authKind), "tool": strings.TrimSpace(tool),
	}, err)
}

func executePreparedActivityHTTPTool(ctx context.Context, prepared preparedActivityHTTPTool) (any, error) {
	reqCtx, cancel := context.WithTimeout(ctx, prepared.timeout)
	defer cancel()
	refreshedAfterUnauthorized := false
	for {
		var body io.Reader
		if len(prepared.body) > 0 {
			body = bytes.NewReader(prepared.body)
		}
		req, err := http.NewRequestWithContext(reqCtx, prepared.method, prepared.url, body)
		if err != nil {
			return nil, runtimefailures.Wrap(runtimefailures.ClassInternalFailure, "activity_request_construction_failed", "activity-runtime", "construct_http_request", map[string]any{"tool": prepared.toolName}, redactActivityError(err, prepared.secrets))
		}
		for key, values := range prepared.headers {
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}
		resp, err := prepared.client.Do(req)
		if err != nil {
			cause := redactActivityError(err, prepared.secrets)
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(reqCtx.Err(), context.DeadlineExceeded) {
				return nil, runtimefailures.Wrap(runtimefailures.ClassTimeout, "activity_http_timeout", "activity-runtime", "dispatch_http_request", map[string]any{"tool": prepared.toolName}, cause)
			}
			return nil, activityHTTPUncertainError{err: runtimefailures.Wrap(runtimefailures.ClassOutcomeUncertain, "activity_http_transport_uncertain", "activity-runtime", "dispatch_http_request", map[string]any{"tool": prepared.toolName}, cause)}
		}
		raw, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			return nil, activityHTTPUncertainError{err: runtimefailures.Wrap(runtimefailures.ClassOutcomeUncertain, "activity_http_response_read_uncertain", "activity-runtime", "read_http_response", map[string]any{"tool": prepared.toolName}, redactActivityError(readErr, prepared.secrets))}
		}
		parsed := parseHTTPActivityResponse(raw)
		parsed = runtimemanagedcredentials.RedactValue(parsed, prepared.secrets...)
		if prepared.managedAuth != nil && resp.StatusCode == http.StatusUnauthorized && !refreshedAfterUnauthorized {
			refreshedAfterUnauthorized = true
			token, record, refreshErr := prepared.managedAuth.TokenSource.Refresh(ctx, prepared.managedAuth.StoreKey)
			if refreshErr != nil {
				return nil, runtimefailures.Wrap(runtimefailures.ClassAuthenticationNeeded, "managed_credential_refresh_failed", "activity-runtime", "refresh_managed_credential", map[string]any{"auth_kind": "managed_credential", "tool": prepared.toolName}, fmt.Errorf("%s", runtimemanagedcredentials.RedactString(refreshErr.Error(), append(prepared.secrets, record.SecretValues()...)...)))
			}
			prepared.managedAuth.Token = token
			prepared.managedAuth.Record = record
			prepared.secrets = append(prepared.secrets, prepared.managedAuth.SecretValues()...)
			if err := runtimemanagedcredentials.ApplyHTTPAuthorization(prepared.headers, prepared.managedAuth.HTTPAuthorization(), true); err != nil {
				return nil, activityAuthenticationFailure(redactActivityError(err, prepared.secrets), prepared.toolName, "apply_refreshed_managed_credential", "managed_credential")
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, activityHTTPStatusFailure(prepared.toolName, resp.StatusCode)
		}
		responseEnv := map[string]any{
			"response": map[string]any{
				"status":  resp.StatusCode,
				"headers": flattenActivityHTTPHeaders(resp.Header),
				"body":    parsed,
			},
		}
		if err := httpresponsesuccess.Evaluate("activity http tool "+strings.TrimSpace(prepared.toolName), prepared.success, responseEnv, prepared.secrets); err != nil {
			return nil, runtimefailures.Wrap(runtimefailures.ClassConnectorFailure, "provider_response_rejected", "activity-runtime", "validate_http_response", map[string]any{"tool": prepared.toolName, "status": resp.StatusCode}, err)
		}
		result := parsed
		if len(prepared.responseMapping) > 0 {
			mapped, err := resolveActivityTemplateTree(prepared.responseMapping, responseEnv)
			if err != nil {
				return nil, runtimefailures.Wrap(runtimefailures.ClassConnectorFailure, "provider_response_projection_failed", "activity-runtime", "project_http_response", map[string]any{"tool": prepared.toolName, "status": resp.StatusCode}, redactActivityError(err, prepared.secrets))
			}
			result = mapped
		}
		if prepared.compiledResult != nil {
			if err := eventschema.ValidateValueAgainstSchema(runtimecontracts.ToolInputSchemaJSONSchema(prepared.outputSchema), result); err != nil {
				return nil, runtimefailures.Wrap(runtimefailures.ClassConnectorFailure, "provider_response_schema_invalid", "activity-runtime", "validate_projected_response", map[string]any{"tool": prepared.toolName, "status": resp.StatusCode}, redactActivityError(err, prepared.secrets))
			}
			projected, err := projectCompiledActivityResult(result, *prepared.compiledResult)
			if err != nil {
				return nil, runtimefailures.Wrap(runtimefailures.ClassConnectorFailure, "channel_result_projection_failed", "activity-runtime", "project_channel_result", map[string]any{"tool": prepared.toolName, "status": resp.StatusCode}, err)
			}
			if err := eventschema.ValidateValueAgainstSchema(runtimecontracts.ToolInputSchemaJSONSchema(prepared.compiledResult.OutputSchema), projected); err != nil {
				return nil, runtimefailures.Wrap(runtimefailures.ClassConnectorFailure, "channel_result_schema_invalid", "activity-runtime", "validate_channel_result", map[string]any{"tool": prepared.toolName, "status": resp.StatusCode}, err)
			}
			result = projected
		}
		return result, nil
	}
}

func activityHTTPStatusFailure(tool string, status int) error {
	attributes := map[string]any{"tool": strings.TrimSpace(tool), "status": status}
	switch status {
	case http.StatusUnauthorized:
		attributes["auth_kind"] = "provider_credential"
		return runtimefailures.New(runtimefailures.ClassAuthenticationNeeded, "provider_unauthorized", "activity-runtime", "http_status", attributes)
	case http.StatusForbidden:
		attributes["action"] = "provider_request"
		return runtimefailures.New(runtimefailures.ClassAuthorizationDenied, "provider_forbidden", "activity-runtime", "http_status", attributes)
	case http.StatusPaymentRequired:
		return runtimefailures.New(runtimefailures.ClassConnectorFailure, "provider_credit_exhausted", "activity-runtime", "http_status", attributes)
	case http.StatusRequestTimeout:
		return runtimefailures.New(runtimefailures.ClassTimeout, "provider_request_timeout", "activity-runtime", "http_status", attributes)
	default:
		return runtimefailures.New(runtimefailures.ClassConnectorFailure, "provider_http_status", "activity-runtime", "http_status", attributes)
	}
}

func cloneActivityResponseSuccess(check *runtimecontracts.HTTPResponseSuccess) *runtimecontracts.HTTPResponseSuccess {
	if check == nil {
		return nil
	}
	out := *check
	return &out
}

func cloneActivityTemplateMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = cloneActivityTemplateValue(value)
	}
	return out
}

func cloneActivityTemplateValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneActivityTemplateMap(typed)
	case []any:
		out := make([]any, len(typed))
		for index, item := range typed {
			out[index] = cloneActivityTemplateValue(item)
		}
		return out
	default:
		return typed
	}
}

func cloneCompiledResultProjection(in *runtimecontracts.CompiledResultProjection) *runtimecontracts.CompiledResultProjection {
	if in == nil {
		return nil
	}
	out := &runtimecontracts.CompiledResultProjection{
		Fields:       make(map[string]runtimecontracts.CompiledResultField, len(in.Fields)),
		OutputSchema: in.OutputSchema,
	}
	for target, field := range in.Fields {
		out.Fields[target] = field
	}
	return out
}

func projectCompiledActivityResult(result any, projection runtimecontracts.CompiledResultProjection) (map[string]any, error) {
	out := map[string]any{}
	targets := make([]string, 0, len(projection.Fields))
	for target := range projection.Fields {
		targets = append(targets, target)
	}
	sort.Strings(targets)
	for _, target := range targets {
		field := projection.Fields[target]
		value, ok := activityValueAtPath(result, strings.TrimPrefix(strings.TrimSpace(field.From), "result."))
		if !ok {
			return nil, fmt.Errorf("compiled result source %q is missing", field.From)
		}
		converted, err := convertCompiledActivityResult(value, field.Convert)
		if err != nil {
			return nil, fmt.Errorf("compiled result source %q: %w", field.From, err)
		}
		if err := setActivityValueAtPath(out, target, converted); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func activityValueAtPath(value any, path string) (any, bool) {
	current := value
	if strings.TrimSpace(path) == "" {
		return current, true
	}
	for _, segment := range strings.Split(path, ".") {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[segment]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func setActivityValueAtPath(out map[string]any, path string, value any) error {
	parts := strings.Split(strings.TrimSpace(path), ".")
	if len(parts) == 0 || parts[0] == "" {
		return fmt.Errorf("compiled result target is required")
	}
	current := out
	for _, segment := range parts[:len(parts)-1] {
		next, exists := current[segment]
		if !exists {
			object := map[string]any{}
			current[segment] = object
			current = object
			continue
		}
		object, ok := next.(map[string]any)
		if !ok {
			return fmt.Errorf("compiled result target %q overlaps another target", path)
		}
		current = object
	}
	leaf := parts[len(parts)-1]
	if _, exists := current[leaf]; exists {
		return fmt.Errorf("compiled result target %q is assigned more than once", path)
	}
	current[leaf] = value
	return nil
}

func convertCompiledActivityResult(value any, conversion string) (any, error) {
	switch strings.TrimSpace(conversion) {
	case "":
		return value, nil
	case runtimecontracts.FieldProjectionConvertNumberToText:
		number, ok := value.(float64)
		if !ok || math.IsNaN(number) || math.IsInf(number, 0) || math.Trunc(number) != number || math.Abs(number) > semanticvalue.MaxSafeInteger {
			return nil, fmt.Errorf("number_to_text requires an exact I-JSON-safe integer")
		}
		return strconv.FormatFloat(number, 'f', 0, 64), nil
	default:
		return nil, fmt.Errorf("compiled result conversion %q is unsupported", conversion)
	}
}

func flattenActivityHTTPHeaders(headers http.Header) map[string]any {
	out := make(map[string]any, len(headers))
	for key, values := range headers {
		key = strings.ToLower(strings.TrimSpace(key))
		if key == "" || len(values) == 0 {
			continue
		}
		if len(values) == 1 {
			out[key] = values[0]
			continue
		}
		items := make([]any, 0, len(values))
		for _, value := range values {
			items = append(items, value)
		}
		out[key] = items
	}
	return out
}

type activityManagedHTTPAuth struct {
	StoreKey    string
	Token       string
	Record      runtimemanagedcredentials.Record
	Header      string
	Prefix      string
	TokenSource *runtimemanagedcredentials.TokenSource
}

func (a *activityManagedHTTPAuth) SecretValues() []string {
	if a == nil {
		return nil
	}
	secrets := a.Record.SecretValues()
	token := strings.TrimSpace(a.Token)
	if token != "" {
		secrets = append(secrets, token)
	}
	return secrets
}

func (a *activityManagedHTTPAuth) HTTPAuthorization() runtimemanagedcredentials.HTTPAuthorization {
	if a == nil {
		return runtimemanagedcredentials.HTTPAuthorization{}
	}
	return runtimemanagedcredentials.HTTPAuthorization{
		CredentialKey: a.StoreKey,
		AccessToken:   a.Token,
		Header:        a.Header,
		Prefix:        a.Prefix,
	}
}

func (d pipelineActivityDispatcher) resolveActivityManagedCredential(ctx context.Context, client *http.Client, intent runtimeengine.ActivityIntent, tool runtimecontracts.ToolSchemaEntry) (*activityManagedHTTPAuth, error) {
	if tool.ManagedCredential == nil {
		return nil, nil
	}
	ref := *tool.ManagedCredential
	key := strings.TrimSpace(ref.Key)
	if key == "" {
		return nil, fmt.Errorf("activity tool %s managed_credential.key is required", intent.Tool)
	}
	source := semanticview.Source(nil)
	var store runtimemanagedcredentials.Store
	if d.coordinator != nil {
		source = d.coordinator.SemanticSource()
		store = d.coordinator.managedCredentials
	}
	flowID := intent.FlowID.String()
	storeKey, mapped := semanticview.CredentialStoreKeyForFlow(source, flowID, key)
	if mapped && strings.TrimSpace(storeKey) == "" {
		return nil, fmt.Errorf("managed credential %q is not declared and bound for imported package flow %s", key, flowID)
	}
	storeKey = strings.TrimSpace(storeKey)
	if storeKey == "" {
		return nil, fmt.Errorf("managed credential %q does not resolve to a deployment credential key", key)
	}
	tokenSource := &runtimemanagedcredentials.TokenSource{
		Store:          store,
		HTTPClient:     client,
		DifferentOwner: runtimeeffects.OwnerPipelineActivity,
	}
	token, record, err := tokenSource.AccessToken(ctx, runtimemanagedcredentials.AccessTokenRequest{
		Key:            storeKey,
		GrantType:      ref.GrantType,
		Scopes:         ref.Scopes,
		GrantModel:     ref.GrantModel,
		TokenRequest:   ref.TokenRequest,
		InstallationID: activityManagedCredentialInputValue(intent.Input, ref.InstallationIDInput),
	})
	if err != nil {
		redacted := fmt.Errorf("%s", runtimemanagedcredentials.RedactString(err.Error(), record.SecretValues()...))
		return nil, activityAuthenticationFailure(redacted, intent.Tool, "access_managed_credential", "managed_credential")
	}
	return &activityManagedHTTPAuth{
		StoreKey:    storeKey,
		Token:       token,
		Record:      record,
		Header:      ref.Header,
		Prefix:      ref.Prefix,
		TokenSource: tokenSource,
	}, nil
}

func activityManagedCredentialInputValue(input semanticvalue.Value, key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return ""
	}
	value, ok := input.Lookup(key)
	if !ok || value.Kind() == semanticvalue.KindNull {
		return ""
	}
	if text, ok := value.String(); ok {
		return strings.TrimSpace(text)
	}
	if number, ok := value.Number(); ok {
		return strings.TrimSpace(strconv.FormatFloat(number, 'g', -1, 64))
	}
	return strings.TrimSpace(fmt.Sprint(value.Interface()))
}

type activityHTTPUncertainError struct {
	err error
}

func (e activityHTTPUncertainError) Error() string {
	if e.err == nil {
		return "activity http outcome uncertain"
	}
	return e.err.Error()
}

func (e activityHTTPUncertainError) Unwrap() error {
	return e.err
}

func activityHTTPOutcomeUncertain(err error) bool {
	var target activityHTTPUncertainError
	return errors.As(err, &target)
}

func parseHTTPActivityResponse(raw []byte) any {
	if len(bytes.TrimSpace(raw)) == 0 {
		return map[string]any{}
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return string(raw)
	}
	return out
}

func (d pipelineActivityDispatcher) resolveActivityToolCredentials(ctx context.Context, intent runtimeengine.ActivityIntent, keys []string) (map[string]any, []string, error) {
	out := make(map[string]any, len(keys))
	secrets := make([]string, 0, len(keys))
	store := runtimecredentials.Store(nil)
	if d.coordinator != nil {
		store = d.coordinator.credentials
	}
	if store == nil {
		return nil, nil, fmt.Errorf("credential store is not configured")
	}
	source := semanticview.Source(nil)
	if d.coordinator != nil {
		source = d.coordinator.SemanticSource()
	}
	flowID := intent.FlowID.String()
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		storeKey, mapped := semanticview.CredentialStoreKeyForFlow(source, flowID, key)
		if mapped && strings.TrimSpace(storeKey) == "" {
			return nil, nil, fmt.Errorf("credential %q is not declared and bound for imported package flow %s", key, flowID)
		}
		storeKey = strings.TrimSpace(storeKey)
		if storeKey == "" {
			return nil, nil, fmt.Errorf("credential %q does not resolve to a deployment credential key", key)
		}
		value, ok, err := store.Get(ctx, storeKey)
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			return nil, nil, fmt.Errorf("missing credential %q", storeKey)
		}
		out[key] = value
		secrets = append(secrets, value)
	}
	return out, secrets, nil
}

func redactActivityError(err error, secrets []string) error {
	if err == nil {
		return nil
	}
	if _, ok := runtimefailures.As(err); ok {
		return err
	}
	return fmt.Errorf("%s", runtimemanagedcredentials.RedactString(err.Error(), secrets...))
}

func resolveActivityTemplateTree(value any, env map[string]any) (any, error) {
	switch typed := value.(type) {
	case string:
		return resolveActivityTemplateValue(typed, env)
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, value := range typed {
			resolved, err := resolveActivityTemplateTree(value, env)
			if err != nil {
				return nil, err
			}
			out[key] = resolved
		}
		return out, nil
	case []any:
		out := make([]any, len(typed))
		for idx, value := range typed {
			resolved, err := resolveActivityTemplateTree(value, env)
			if err != nil {
				return nil, err
			}
			out[idx] = resolved
		}
		return out, nil
	default:
		return value, nil
	}
}

func resolveActivityTemplateValue(template string, env map[string]any) (any, error) {
	out := strings.TrimSpace(template)
	matches, err := activityTemplateMatches(out)
	if err != nil {
		return nil, err
	}
	if len(matches) == 1 && matches[0].start == 0 && matches[0].end == len(out) {
		value, ok := workflowExpressionLookupPath(env, matches[0].expr)
		if !ok {
			return nil, fmt.Errorf("activity template expression %q did not resolve", matches[0].expr)
		}
		return value, nil
	}
	return resolveActivityTemplateString(out, env)
}

func resolveActivityTemplateString(template string, env map[string]any) (string, error) {
	out := strings.TrimSpace(template)
	matches, err := activityTemplateMatches(out)
	if err != nil {
		return "", err
	}
	if len(matches) == 0 {
		return out, nil
	}
	var builder strings.Builder
	last := 0
	for _, match := range matches {
		builder.WriteString(out[last:match.start])
		value, ok := workflowExpressionLookupPath(env, match.expr)
		if !ok {
			return "", fmt.Errorf("activity template expression %q did not resolve", match.expr)
		}
		builder.WriteString(asString(value))
		last = match.end
	}
	builder.WriteString(out[last:])
	return builder.String(), nil
}

func resolveActivityHTTPURLTemplate(template string, env map[string]any) (string, error) {
	out := strings.TrimSpace(template)
	matches, err := activityTemplateMatches(out)
	if err != nil {
		return "", err
	}
	if len(matches) == 0 {
		return out, nil
	}
	if len(matches) == 1 && matches[0].start == 0 && matches[0].end == len(out) {
		value, ok := workflowExpressionLookupPath(env, matches[0].expr)
		if !ok {
			return "", fmt.Errorf("activity template expression %q did not resolve", matches[0].expr)
		}
		return asString(value), nil
	}
	var builder strings.Builder
	last := 0
	for _, match := range matches {
		builder.WriteString(out[last:match.start])
		value, ok := workflowExpressionLookupPath(env, match.expr)
		if !ok {
			return "", fmt.Errorf("activity template expression %q did not resolve", match.expr)
		}
		builder.WriteString(escapeActivityHTTPURLTemplateComponent(out, match.start, match.end, asString(value)))
		last = match.end
	}
	builder.WriteString(out[last:])
	return builder.String(), nil
}

type activityTemplateMatch struct {
	start int
	end   int
	expr  string
}

func activityTemplateMatches(template string) ([]activityTemplateMatch, error) {
	matches := make([]activityTemplateMatch, 0, 2)
	cursor := 0
	for {
		relativeStart := strings.Index(template[cursor:], "{{")
		if relativeStart < 0 {
			return matches, nil
		}
		start := cursor + relativeStart
		relativeEnd := strings.Index(template[start+2:], "}}")
		if relativeEnd < 0 {
			return nil, fmt.Errorf("unterminated activity template expression in %q", template)
		}
		end := start + 2 + relativeEnd
		expr := strings.TrimSpace(template[start+2 : end])
		matches = append(matches, activityTemplateMatch{start: start, end: end + 2, expr: expr})
		cursor = end + 2
	}
}

func escapeActivityHTTPURLTemplateComponent(raw string, start, end int, value string) string {
	if activityHTTPURLTemplateOffsetInQuery(raw, start) {
		return strings.ReplaceAll(url.QueryEscape(value), "+", "%20")
	}
	if activityHTTPURLTemplatePlaceholderInURLBaseOrAuthority(raw, start, end, value) {
		return value
	}
	return url.PathEscape(value)
}

func activityHTTPURLTemplateOffsetInQuery(raw string, offset int) bool {
	queryStart := strings.Index(raw, "?")
	if queryStart < 0 || queryStart > offset {
		return false
	}
	fragmentStart := strings.Index(raw, "#")
	return fragmentStart < 0 || offset < fragmentStart
}

func activityHTTPURLTemplatePlaceholderInURLBaseOrAuthority(raw string, start, end int, value string) bool {
	prefix := raw[:start]
	suffix := raw[end:]
	if strings.HasPrefix(suffix, "://") {
		return true
	}
	if strings.HasSuffix(prefix, "://") {
		return true
	}
	schemeIndex := strings.LastIndex(prefix, "://")
	if schemeIndex >= 0 {
		authorityPrefix := prefix[schemeIndex+len("://"):]
		return !strings.ContainsAny(authorityPrefix, "/?#")
	}
	if start == 0 {
		return activityHTTPURLTemplateValueHasSchemeAuthority(value)
	}
	return false
}

func activityHTTPURLTemplateValueHasSchemeAuthority(value string) bool {
	parsed, err := url.Parse(strings.TrimSpace(value))
	return err == nil && parsed.Scheme != "" && parsed.Host != ""
}

func (d pipelineActivityDispatcher) publishActivitySuccess(ctx context.Context, intent runtimeengine.ActivityIntent, result any) error {
	return d.publishActivityResult(ctx, intent, intent.SuccessEvent, activitySuccessPayload(intent, result))
}

func activitySuccessPayload(intent runtimeengine.ActivityIntent, result any) map[string]any {
	payload := map[string]any{
		"activity_id":  intent.ActivityID,
		"tool":         intent.Tool,
		"effect_class": string(intent.EffectClass),
		"attempt":      intent.Attempt,
		"result":       result,
	}
	return activityPayloadWithGeneration(intent, payload)
}

func (d pipelineActivityDispatcher) publishActivityFailure(ctx context.Context, intent runtimeengine.ActivityIntent, cause error) error {
	return d.publishActivityResult(ctx, intent, intent.FailureEvent, activityFailurePayload(intent, cause))
}

func activityFailurePayload(intent runtimeengine.ActivityIntent, cause error) map[string]any {
	failure := runtimefailures.Normalize(cause, "activity-runtime", "activity_failure_payload")
	payload := map[string]any{
		"activity_id":  intent.ActivityID,
		"tool":         intent.Tool,
		"effect_class": string(intent.EffectClass),
		"attempt":      intent.Attempt,
		"failure":      failure,
	}
	return activityPayloadWithGeneration(intent, payload)
}

func activityPayloadWithGeneration(intent runtimeengine.ActivityIntent, payload map[string]any) map[string]any {
	if generation := intent.Generation.Normalize(); generation.Valid() {
		payload[generation.RevisionField] = generation.RevisionID
	}
	return payload
}

func (d pipelineActivityDispatcher) publishActivityResult(ctx context.Context, intent runtimeengine.ActivityIntent, eventType string, payload map[string]any) error {
	return d.publishActivityResultWithID(ctx, intent, activityResultEventID(intent, eventType), eventType, payload)
}

func (d pipelineActivityDispatcher) publishActivityResultWithID(ctx context.Context, intent runtimeengine.ActivityIntent, eventID, eventType string, payload map[string]any) error {
	ctx = events.WithDeliveryContext(ctx, intent.Context)
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	evt := events.NewChildEventWithLineage(
		eventID,
		events.EventType(eventType),
		events.NodeProducer(intent.NodeID.String()),
		intent.SourceTaskID,
		raw,
		intent.ChainDepth+1,
		events.EventLineage{
			RunID:         intent.SourceRunID,
			ParentEventID: firstNonEmptyString(intent.SourceEventID, intent.ParentEventID),
			TaskID:        intent.SourceTaskID,
			ExecutionMode: intent.ExecutionMode,
		},
		events.EventEnvelope{
			EntityID: intent.EntityID.String(),
			Source: events.RouteIdentity{
				FlowID:   intent.FlowID.String(),
				EntityID: intent.EntityID.String(),
			},
		},
		time.Now().UTC(),
	)
	if collector, ok := ctx.Value(pipelineEmitCollectorKey{}).(*[]events.Event); ok && collector != nil {
		*collector = append(*collector, evt)
		d.logActivityRuntime(ctx, intent, "result_published", map[string]any{
			"activity_id":       intent.ActivityID,
			"tool":              intent.Tool,
			"effect_class":      string(intent.EffectClass),
			"attempt":           intent.Attempt,
			"result_event_id":   evt.ID(),
			"result_event_type": string(evt.Type()),
		})
		return nil
	}
	if err := d.coordinator.bus.Publish(ctx, evt); err != nil {
		return err
	}
	d.logActivityRuntime(ctx, intent, "result_published", map[string]any{
		"activity_id":       intent.ActivityID,
		"tool":              intent.Tool,
		"effect_class":      string(intent.EffectClass),
		"attempt":           intent.Attempt,
		"result_event_id":   evt.ID(),
		"result_event_type": string(evt.Type()),
	})
	return nil
}

func (d pipelineActivityDispatcher) publishExistingActivityAttempt(ctx context.Context, intent runtimeengine.ActivityIntent, rec ActivityAttemptRecord) error {
	rec = rec.normalized()
	if rec.Status == ActivityAttemptStatusStarted {
		return nil
	}
	return d.publishJournaledActivityResult(ctx, intent, rec)
}

func (d pipelineActivityDispatcher) publishJournaledActivityResult(ctx context.Context, intent runtimeengine.ActivityIntent, rec ActivityAttemptRecord) error {
	rec = rec.normalized()
	if rec.ResultEventID == "" || rec.ResultEventType == "" || rec.ResultPayload == nil {
		return fmt.Errorf("activity attempt %s has no terminal journal result", rec.RequestEventID)
	}
	intent.Attempt = rec.Attempt
	intent.Generation = rec.Generation
	if id := strings.TrimSpace(rec.ReplyContextID); id != "" {
		intent.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: id}}
	}
	return d.publishActivityResultWithID(ctx, intent, rec.ResultEventID, rec.ResultEventType, rec.ResultPayload)
}

func activityAttemptStartRecord(intent runtimeengine.ActivityIntent, inputHash string) ActivityAttemptRecord {
	intent = intent.Normalized()
	return ActivityAttemptRecord{
		RequestEventID:  activityRequestEventID(intent),
		RunID:           intent.SourceRunID,
		ExecutionMode:   intent.ExecutionMode,
		SourceEventID:   intent.SourceEventID,
		ParentEventID:   intent.ParentEventID,
		EntityID:        intent.EntityID.String(),
		FlowInstance:    firstNonEmptyString(intent.FlowInstance, intent.FlowID.String()),
		NodeID:          intent.NodeID.String(),
		HandlerEventKey: intent.HandlerEventKey,
		ActivityID:      intent.ActivityID,
		Tool:            intent.Tool,
		EffectClass:     string(intent.EffectClass),
		Attempt:         1,
		Status:          ActivityAttemptStatusStarted,
		SuccessEvent:    intent.SuccessEvent,
		FailureEvent:    intent.FailureEvent,
		InputHash:       inputHash,
		ReplyContextID:  intent.Context.ReplyContextID(),
		Generation:      intent.Generation,
		LoopStage:       intent.LoopStage,
	}
}

func (rec ActivityAttemptRecord) withTerminal(status, eventID, eventType string, payload map[string]any, failure *runtimefailures.Envelope) ActivityAttemptRecord {
	rec = rec.normalized()
	rec.Status = status
	rec.ResultEventID = strings.TrimSpace(eventID)
	rec.ResultEventType = strings.TrimSpace(eventType)
	rec.ResultPayload = cloneStringAnyMap(payload)
	rec.Failure = runtimefailures.CloneEnvelope(failure)
	return rec
}

func activityInputHash(input semanticvalue.Value) string {
	hash, err := canonicaljson.HashValue(input)
	if err != nil {
		panic(fmt.Sprintf("hash admitted activity input: %v", err))
	}
	return hash
}
