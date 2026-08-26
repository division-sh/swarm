package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/providerconnectors"
	runtimeactivityresult "github.com/division-sh/swarm/internal/runtime/activityresult"
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
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
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
	immediate := make([]runtimeengine.ActivityIntent, 0, len(intents))
	for _, intent := range intents {
		intent = intent.Normalized()
		if intent.ApprovalDecision != "" {
			return fmt.Errorf("approved activity %s requires the selected workflow engine commit owner", intent.ActivityID)
		}
		immediate = append(immediate, intent)
	}
	if _, err := activityRequestEmitIntents(immediate); err != nil {
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
		if err := w.coordinator.bus.LogRuntime(ctx, entry); err != nil {
			return err
		}
	}
	return nil
}

type pipelineActivityDispatcher struct {
	coordinator *PipelineCoordinator
	client      *http.Client
	emissions   *pipelineEmissionPlan
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
	flowInstance := firstNonEmptyString(intent.FlowInstance, intent.ExecutionFlowID.String(), "root")
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
		EntityID: intent.EntityID.String(), NodeID: intent.Owner.Key(), FlowID: intent.ExecutionFlowID.String(), FlowInstance: flowInstance,
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
		Scope: decisioncard.Scope{Kind: decisioncard.ScopeEntity, FlowInstance: flowInstance, EntityID: intent.EntityID.String()}, Source: intent.RoutingSource,
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
		"source_event": intent.SourceEventID, "flow_id": intent.ExecutionFlowID.String(), "flow_instance": flowInstance, "node_id": intent.Owner.Key(),
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
	if err := d.coordinator.executionPosture.Admit(intent.ExecutionMode, "activity attempt claim, credential lookup, and launch"); err != nil {
		return runtimefailures.Wrap(runtimefailures.ClassAuthorizationDenied, "process_execution_posture_rejected", "activity-runtime", "execute_activity", map[string]any{
			"activity_id": intent.ActivityID, "tool": intent.Tool, "execution_mode": intent.ExecutionMode, "execution_posture": d.coordinator.executionPosture,
		}, err)
	}
	var err error
	ctx, err = activityExecutionContext(ctx, intent)
	if err != nil {
		return err
	}
	source := d.coordinator.SemanticSource()
	if source == nil {
		return runtimefailures.New(runtimefailures.ClassInternalFailure, "activity_semantic_source_missing", "activity-runtime", "execute_activity", nil)
	}
	target, privateTarget, activationLease, targetErr := d.coordinator.channelActivityTarget(intent.Tool, intent.ChannelActivationGeneration)
	if targetErr != nil {
		return d.publishActivityFailure(ctx, intent, runtimefailures.Wrap(runtimefailures.ClassSchemaInvalid, "channel_activity_plan_invalid", "activity-runtime", "resolve_private_target", map[string]any{"tool": intent.Tool}, targetErr))
	}
	if activationLease != nil {
		defer activationLease.Release()
	}
	ctx = withActivityChannelTarget(ctx, target, privateTarget)
	tool, ok := target.Tool()
	if privateTarget {
		if !intent.PlanGeneration.Valid() || !intent.PlanGeneration.Equal(target.Generation()) {
			return d.rejectChannelActivityTarget(ctx, intent, runtimefailures.New(runtimefailures.ClassSchemaInvalid, "channel_activity_plan_generation_changed", "activity-runtime", "execute_activity", map[string]any{
				"tool": intent.Tool, "requested_generation": intent.PlanGeneration.Diagnostic(), "available_generation": target.Generation().Diagnostic(),
			}))
		}
	} else if strings.HasPrefix(intent.Tool, runtimecontracts.PrivateChannelActivityPrefix) {
		return d.rejectChannelActivityTarget(ctx, intent, runtimefailures.New(runtimefailures.ClassTargetUnreachable, "channel_activity_plan_generation_unavailable", "activity-runtime", "execute_activity", map[string]any{
			"tool": intent.Tool, "requested_generation": intent.PlanGeneration.Diagnostic(),
		}))
	} else {
		tool, ok = source.ToolEntries()[intent.Tool]
	}
	if !ok {
		return d.publishActivityFailure(ctx, intent, runtimefailures.New(runtimefailures.ClassTargetUnreachable, "activity_tool_not_declared", "activity-runtime", "execute_activity", map[string]any{"tool": intent.Tool}))
	}
	toolEffectClass := tool.Effect()
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
		responsePlan, err := d.coordinator.mockResponsePlanForRun(ctx, intent.SourceRunID, source)
		if err != nil {
			return d.publishActivityFailure(ctx, intent, runtimefailures.Wrap(runtimefailures.ClassSchemaInvalid, "scenario_execution_profile_not_admitted", "activity-runtime", "admit_mock_activity", map[string]any{"tool": intent.Tool, "run_id": intent.SourceRunID}, err))
		}
		if reused, err := d.reuseExistingNonIdempotentActivityAttempt(ctx, intent); err != nil || reused {
			return err
		}
		admitted, err := responsePlan.Admit(intent.Tool, tool)
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

func (pc *PipelineCoordinator) mockResponsePlanForRun(ctx context.Context, runID string, source semanticview.Source) (*providerconnectors.MockResponsePlan, error) {
	if pc.scenarioProfiles == nil {
		return pc.mockConnectorResponses, nil
	}
	profile, found, err := pc.scenarioProfiles.LoadScenarioExecutionProfile(ctx, strings.TrimSpace(runID))
	if err != nil {
		return nil, fmt.Errorf("load scenario execution profile for run %s: %w", runID, err)
	}
	if !found {
		return pc.mockConnectorResponses, nil
	}
	if pc.executionPosture != executionposture.MockOnly {
		return nil, fmt.Errorf("run %s has a scenario execution profile but runtime posture is %q", runID, pc.executionPosture)
	}
	if err := pc.effectiveSource.Validate(); err != nil {
		return nil, fmt.Errorf("runtime effective source identity is unavailable: %w", err)
	}
	if !profile.EffectiveSourceIdentity().Equal(pc.effectiveSource) {
		return nil, fmt.Errorf("run %s scenario execution effective source mismatch: persisted=%s runtime=%s", runID, profile.EffectiveSourceIdentity().Digest(), pc.effectiveSource.Digest())
	}
	return providerconnectors.OverlayMockResponsePlan(pc.mockConnectorResponses, source, profile.ConnectorResponses())
}

func (d pipelineActivityDispatcher) reuseExistingNonIdempotentActivityAttempt(ctx context.Context, intent runtimeengine.ActivityIntent) (bool, error) {
	if d.coordinator == nil || d.coordinator.workflowStore == nil || !d.coordinator.workflowStore.enabled() {
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

func (d pipelineActivityDispatcher) activityContractPinFailure(ctx context.Context, intent runtimeengine.ActivityIntent, source semanticview.Source) *runtimefailures.Envelope {
	if intent.BundleHash == "" && intent.WorkflowVersion == "" {
		return nil
	}
	if intent.BundleHash == "" || intent.WorkflowVersion == "" {
		failure := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassSchemaInvalid, "activity_contract_pin_incomplete", "activity-runtime", "admit_activity_contract", map[string]any{
			"activity_id": intent.ActivityID, "bundle_hash": intent.BundleHash, "workflow_version": intent.WorkflowVersion,
		}), "activity-runtime", "admit_activity_contract")
		return &failure
	}
	currentBundleHash := workflowGateBundleHash(ctx, d.coordinator)
	currentWorkflowVersion := ""
	if source != nil {
		currentWorkflowVersion = strings.TrimSpace(source.WorkflowVersion())
	}
	if currentBundleHash == intent.BundleHash && currentWorkflowVersion == intent.WorkflowVersion {
		return nil
	}
	failure := runtimefailures.Normalize(runtimefailures.New(
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
	), "activity-runtime", "admit_activity_contract")
	return &failure
}

func (d pipelineActivityDispatcher) admitReadOnlyActivityGeneration(ctx context.Context, intent runtimeengine.ActivityIntent) error {
	if !intent.Generation.Valid() {
		return nil
	}
	if d.coordinator == nil || d.coordinator.workflowStore == nil || !d.coordinator.workflowStore.enabled() {
		return runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "activity_loop_store_unavailable", "activity-runtime", "admit_read_only_activity", nil)
	}
	unlock := d.coordinator.lockWorkflowEntity(intent.EntityID.String())
	defer unlock()
	route, err := workflowInstanceRouteForExecution(d.coordinator.SemanticSource(), intent.ExecutionFlowID.String(), intent.FlowInstance)
	if err != nil {
		return fmt.Errorf("activity state route: %w", err)
	}
	instance, ok, err := d.coordinator.workflowStore.Load(ctx, route)
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
}

func (d pipelineActivityDispatcher) executeNonIdempotentActivityIntent(ctx context.Context, intent runtimeengine.ActivityIntent, tool runtimecontracts.ToolSchemaEntry, mockResponse *providerconnectors.AdmittedMockResponse) error {
	if d.coordinator == nil || d.coordinator.workflowStore == nil || !d.coordinator.workflowStore.enabled() {
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

func (d pipelineActivityDispatcher) rejectChannelActivityTarget(ctx context.Context, intent runtimeengine.ActivityIntent, cause error) error {
	if intent.EffectClass != runtimecontracts.ActivityEffectClassNonIdempotentWrite {
		return d.publishActivityFailure(ctx, intent, cause)
	}
	if d.coordinator == nil || d.coordinator.workflowStore == nil || !d.coordinator.workflowStore.enabled() {
		return d.publishActivityFailure(ctx, intent, cause)
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
		return d.publishActivityFailure(ctx, intent, activityDependencyFailure(err, intent.Tool, "reject_channel_activity_target"))
	}
	if !inserted {
		return d.publishExistingActivityAttempt(ctx, intent, started)
	}
	failure := runtimefailures.FromError(cause, "activity-runtime", "reject_channel_activity_target")
	terminal := started.withTerminal(
		ActivityAttemptStatusFailed,
		activityResultEventID(intent, intent.FailureEvent),
		intent.FailureEvent,
		activityFailurePayload(intent, failure),
		&failure.Failure,
	)
	stored, err := d.coordinator.workflowStore.CompleteActivityAttempt(ctx, terminal)
	if err != nil {
		return activityDependencyFailure(err, intent.Tool, "complete_rejected_channel_activity_target")
	}
	return d.publishJournaledActivityResult(ctx, intent, stored)
}

func activityDependencyFailure(err error, tool, operation string) error {
	if _, ok := runtimefailures.As(err); ok {
		return err
	}
	return runtimefailures.Wrap(runtimefailures.ClassDependencyUnavailable, "activity_journal_operation_failed", "activity-runtime", operation, map[string]any{"tool": strings.TrimSpace(tool)}, err)
}

type activityRecordedResult = runtimeactivityresult.Record

func (d pipelineActivityDispatcher) recordedActivityResult(ctx context.Context, intent runtimeengine.ActivityIntent) (activityRecordedResult, bool, error) {
	if d.coordinator == nil || d.coordinator.workflowStore == nil {
		return activityRecordedResult{}, false, nil
	}
	return d.coordinator.workflowStore.recordedActivityResult(ctx, intent)
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

func (pc *PipelineCoordinator) handleActivityRequestEvent(ctx context.Context, evt events.Event) (bool, runtimepipelineobligation.ExecutionOutcome, error) {
	return pc.handleActivityRequestEventWithEmissionPlan(ctx, evt, nil)
}

func (pc *PipelineCoordinator) handleActivityRequestEventWithEmissionPlan(ctx context.Context, evt events.Event, emissions *pipelineEmissionPlan) (bool, runtimepipelineobligation.ExecutionOutcome, error) {
	if pc == nil || evt.Type() != activityRequestEventType {
		return false, runtimepipelineobligation.Continue(), nil
	}
	intent, err := activityIntentFromRequestEvent(evt)
	if err != nil {
		return true, runtimepipelineobligation.Continue(), err
	}
	dispatcher := pipelineActivityDispatcher{coordinator: pc, emissions: emissions}
	if failure := dispatcher.activityContractPinFailure(ctx, intent, pc.SemanticSource()); failure != nil {
		if failure.Class == runtimefailures.ClassDependencyUnavailable {
			return true, runtimepipelineobligation.ReleaseForRetry(failure.Detail.Code, failure), nil
		}
		return true, runtimepipelineobligation.DeadLetterExecution(failure.Detail.Code, failure), nil
	}
	if err := dispatcher.executeActivityIntent(ctx, intent); err != nil {
		return true, runtimepipelineobligation.Continue(), err
	}
	return true, runtimepipelineobligation.Continue(), nil
}

type activityRequestPayload struct {
	ActivityID                  string                                         `json:"activity_id"`
	Tool                        string                                         `json:"tool"`
	PlanGeneration              *plangeneration.Generation                     `json:"plan_generation,omitempty"`
	ChannelActivationGeneration *channelonboarding.ChannelActivationGeneration `json:"channel_activation_generation,omitempty"`
	BundleHash                  string                                         `json:"bundle_hash,omitempty"`
	WorkflowVersion             string                                         `json:"workflow_version,omitempty"`
	EffectClass                 string                                         `json:"effect_class"`
	SuccessEvent                string                                         `json:"success_event"`
	FailureEvent                string                                         `json:"failure_event"`
	RevisionEvent               string                                         `json:"revision_event,omitempty"`
	RejectedEvent               string                                         `json:"rejected_event,omitempty"`
	RetryMaxAttempts            int                                            `json:"retry_max_attempts"`
	RetryBackoff                string                                         `json:"retry_backoff"`
	ForkPolicy                  string                                         `json:"fork_policy"`
	EntityID                    string                                         `json:"entity_id"`
	NodeID                      string                                         `json:"node_id"`
	FlowID                      string                                         `json:"flow_id"`
	FlowInstance                string                                         `json:"flow_instance,omitempty"`
	HandlerEventKey             string                                         `json:"handler_event_key"`
	SourceEventID               string                                         `json:"source_event_id"`
	SourceRunID                 string                                         `json:"source_run_id"`
	SourceTaskID                string                                         `json:"source_task_id"`
	ParentEventID               string                                         `json:"parent_event_id"`
	ChainDepth                  int                                            `json:"chain_depth"`
	Attempt                     int                                            `json:"attempt"`
	Generation                  attemptgeneration.Generation                   `json:"loop_generation,omitempty"`
	LoopStage                   string                                         `json:"loop_stage,omitempty"`
}

func activityRequestEmitIntents(intents []runtimeengine.ActivityIntent) ([]runtimeengine.EmitIntent, error) {
	if len(intents) == 0 {
		return nil, nil
	}
	out := make([]runtimeengine.EmitIntent, 0, len(intents))
	for _, intent := range intents {
		request, err := activityRequestEmitIntentFromAdmittedSource(intent)
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
	if pc == nil || pc.bus == nil || pc.workflowStore == nil || !pc.workflowStore.enabled() {
		return ActivityAttemptRecord{}, runtimefailures.New(
			runtimefailures.ClassDependencyUnavailable,
			"activity_journal_unavailable",
			"activity-runtime",
			"execute_durable_activity",
			nil,
		)
	}
	intent = intent.Normalized()
	request, err := activityRequestEmitIntentFromAdmittedSource(intent)
	if err != nil {
		return ActivityAttemptRecord{}, err
	}
	publishCtx := events.WithDeliveryContext(ctx, request.Context)
	publishErr := pc.bus.Publish(publishCtx, request.Event)
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

func activityRequestEmitIntentFromAdmittedSource(intent runtimeengine.ActivityIntent) (runtimeengine.EmitIntent, error) {
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
	routingSource := intent.RoutingSource
	if routingSource.Empty() {
		return runtimeengine.EmitIntent{}, fmt.Errorf("activity request requires admitted producer source")
	}
	evt, err := events.NewChildEvent(events.ChildEventInput{
		Facts: events.EventFacts{
			ID: activityRequestEventID(intent), Type: activityRequestEventType,
			Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: runtimeWorkflowID},
			TaskID:   intent.SourceTaskID, Payload: raw, ChainDepth: intent.ChainDepth + 1,
			Envelope: events.EventEnvelope{
				EntityID: intent.EntityID.String(), FlowInstance: intent.FlowInstance, Source: routingSource.Route(),
			},
			RoutingSource: routingSource, CreatedAt: time.Now().UTC(),
		},
		Lineage: events.EventLineage{
			RunID:         intent.SourceRunID,
			ParentEventID: firstNonEmptyString(intent.SourceEventID, intent.ParentEventID),
			TaskID:        intent.SourceTaskID,
			ExecutionMode: intent.ExecutionMode,
		},
	})
	if err != nil {
		return runtimeengine.EmitIntent{}, fmt.Errorf("construct activity request event: %w", err)
	}
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
		EntityID: intent.EntityID.String(), Owner: intent.Owner, ExecutionFlowID: intent.ExecutionFlowID.String(),
		HandlerEventKey: intent.HandlerEventKey, ActivityID: intent.ActivityID, Tool: intent.Tool,
		Attempt: intent.Attempt, RevisionID: intent.Generation.RevisionID,
	}
}

func activityEventProducer(owner activityidentity.Owner) events.ProducerClaim {
	if node, ok := owner.Node(); ok {
		return events.ProducerClaim{Type: events.EventProducerNode, ID: node.Key()}
	}
	if agentID := owner.AgentID(); agentID != "" {
		return events.ProducerClaim{Type: events.EventProducerAgent, ID: agentID}
	}
	return events.ProducerClaim{}
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
	var planGeneration *plangeneration.Generation
	if intent.PlanGeneration.Valid() {
		value := intent.PlanGeneration
		planGeneration = &value
	}
	var activationGeneration *channelonboarding.ChannelActivationGeneration
	if intent.ChannelActivationGeneration.Valid() {
		value := intent.ChannelActivationGeneration
		activationGeneration = &value
	}
	return activityRequestPayload{
		ActivityID:                  intent.ActivityID,
		Tool:                        intent.Tool,
		PlanGeneration:              planGeneration,
		ChannelActivationGeneration: activationGeneration,
		BundleHash:                  intent.BundleHash,
		WorkflowVersion:             intent.WorkflowVersion,
		EffectClass:                 string(intent.EffectClass),
		SuccessEvent:                intent.SuccessEvent,
		FailureEvent:                intent.FailureEvent,
		RevisionEvent:               intent.RevisionEvent,
		RejectedEvent:               intent.RejectedEvent,
		RetryMaxAttempts:            intent.RetryMaxAttempts,
		RetryBackoff:                intent.RetryBackoff,
		ForkPolicy:                  string(intent.ForkPolicy),
		EntityID:                    intent.EntityID.String(),
		NodeID:                      intent.Owner.Key(),
		FlowID:                      intent.ExecutionFlowID.String(),
		FlowInstance:                intent.FlowInstance,
		HandlerEventKey:             intent.HandlerEventKey,
		SourceEventID:               intent.SourceEventID,
		SourceRunID:                 intent.SourceRunID,
		SourceTaskID:                intent.SourceTaskID,
		ParentEventID:               intent.ParentEventID,
		ChainDepth:                  intent.ChainDepth,
		Attempt:                     intent.Attempt,
		Generation:                  intent.Generation,
		LoopStage:                   intent.LoopStage,
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
	if strings.HasPrefix(strings.TrimSpace(payload.Tool), runtimecontracts.PrivateChannelActivityPrefix) && (payload.PlanGeneration == nil || payload.ChannelActivationGeneration == nil) {
		return runtimeengine.ActivityIntent{}, fmt.Errorf("activity request %s for private channel target requires plan_generation and channel_activation_generation", evt.ID())
	}
	input, ok := semanticPayload.Lookup("input")
	if !ok || input.Kind() != semanticvalue.KindObject {
		return runtimeengine.ActivityIntent{}, fmt.Errorf("activity request %s input must be a semantic object", evt.ID())
	}
	planGeneration := plangeneration.Generation{}
	if payload.PlanGeneration != nil {
		planGeneration = *payload.PlanGeneration
	}
	owner, err := activityidentity.ParseOwnerKey(payload.NodeID)
	if err != nil {
		return runtimeengine.ActivityIntent{}, fmt.Errorf("activity request %s owner identity: %w", evt.ID(), err)
	}
	intent := runtimeengine.ActivityIntent{
		Context:          evt.DeliveryContext(),
		RoutingSource:    evt.RoutingSource(),
		ActivityID:       payload.ActivityID,
		Tool:             payload.Tool,
		PlanGeneration:   planGeneration,
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
		Owner:            owner,
		ExecutionFlowID:  identity.NormalizeFlowID(payload.FlowID),
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
	if payload.ChannelActivationGeneration != nil {
		intent.ChannelActivationGeneration = *payload.ChannelActivationGeneration
	}
	if intent.ActivityID == "" || intent.Tool == "" || intent.SuccessEvent == "" || intent.FailureEvent == "" {
		return runtimeengine.ActivityIntent{}, fmt.Errorf("activity request %s is missing required activity identity", evt.ID())
	}
	if (intent.BundleHash == "") != (intent.WorkflowVersion == "") {
		return runtimeengine.ActivityIntent{}, fmt.Errorf("activity request %s carries an incomplete contract pin", evt.ID())
	}
	return intent, nil
}

type preparedActivityHTTPTool struct {
	toolName           string
	method             string
	url                string
	headers            http.Header
	body               []byte
	timeout            time.Duration
	client             *http.Client
	secrets            []string
	managedAuth        *activityManagedHTTPAuth
	success            runtimecontracts.ToolResponseSuccessPolicy
	hasSuccess         bool
	responseMapping    runtimecontracts.ToolResponseMapping
	hasResponseMapping bool
	outputSchema       runtimecontracts.ToolInputSchema
	compiledResult     runtimecontracts.ToolCompiledResultProjection
	hasCompiledResult  bool
	inputHash          string
}

func (d pipelineActivityDispatcher) executeActivityHTTPTool(ctx context.Context, client *http.Client, intent runtimeengine.ActivityIntent, tool runtimecontracts.ToolSchemaEntry) (any, error) {
	prepared, err := d.prepareActivityHTTPTool(ctx, client, intent, tool)
	if err != nil {
		return nil, err
	}
	return executePreparedActivityHTTPTool(ctx, prepared)
}

func (d pipelineActivityDispatcher) prepareActivityHTTPTool(ctx context.Context, client *http.Client, intent runtimeengine.ActivityIntent, tool runtimecontracts.ToolSchemaEntry) (preparedActivityHTTPTool, error) {
	httpExecution, hasHTTP := tool.HTTPExecution()
	if !hasHTTP {
		return preparedActivityHTTPTool{}, activityContractFailure(intent.Tool, "http_block_missing")
	}
	if tool.RatePolicy().Enabled() {
		return preparedActivityHTTPTool{}, activityContractFailure(intent.Tool, "rate_limit_unsupported")
	}
	credentials := map[string]any{}
	secrets := []string{}
	staticCredentials := tool.Credentials()
	_, hasManagedCredential := tool.ManagedCredentialExecution()
	if len(staticCredentials) > 0 && hasManagedCredential {
		return preparedActivityHTTPTool{}, activityContractFailure(intent.Tool, "credential_owners_conflict")
	}
	if len(staticCredentials) > 0 {
		if intent.EffectClass != runtimecontracts.ActivityEffectClassNonIdempotentWrite {
			return preparedActivityHTTPTool{}, activityContractFailure(intent.Tool, "static_credential_effect_class_unsupported")
		}
		resolved, secretValues, err := d.resolveActivityToolCredentials(ctx, intent, staticCredentials)
		if err != nil {
			return preparedActivityHTTPTool{}, activityAuthenticationFailure(err, intent.Tool, "resolve_static_credentials", "activity_credential")
		}
		credentials = resolved
		secrets = secretValues
	}
	if hasManagedCredential {
		if intent.EffectClass != runtimecontracts.ActivityEffectClassNonIdempotentWrite || tool.Category() != runtimecontracts.ToolCategoryProviderConnector {
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
	request, err := httpExecution.Prepare(inputDTO, credentials)
	if err != nil {
		return preparedActivityHTTPTool{}, activityTemplateFailure(err, intent.Tool, "request", secrets)
	}
	headers := request.Headers()
	if client == nil {
		client = &http.Client{Timeout: request.Timeout()}
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
	responseSuccess, hasResponseSuccess := tool.ResponseSuccessPolicy()
	responseMapping, hasResponseMapping := tool.CompiledResponseMapping()
	compiledResult, hasCompiledResult := tool.CompiledResultExecution()
	return preparedActivityHTTPTool{
		toolName:           intent.Tool,
		method:             request.Method(),
		url:                request.URL(),
		headers:            headers,
		body:               request.Body(),
		timeout:            request.Timeout(),
		client:             client,
		secrets:            secrets,
		managedAuth:        managedAuth,
		success:            responseSuccess,
		hasSuccess:         hasResponseSuccess,
		responseMapping:    responseMapping,
		hasResponseMapping: hasResponseMapping,
		outputSchema:       tool.OutputSchema(),
		compiledResult:     compiledResult,
		hasCompiledResult:  hasCompiledResult,
		inputHash:          activityInputHash(intent.Input),
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
		if prepared.hasSuccess {
			if err := prepared.success.Evaluate(responseEnv); err != nil {
				return nil, runtimefailures.Wrap(runtimefailures.ClassConnectorFailure, "provider_response_rejected", "activity-runtime", "validate_http_response", map[string]any{"tool": prepared.toolName, "status": resp.StatusCode}, redactActivityError(err, prepared.secrets))
			}
		}
		result := parsed
		if prepared.hasResponseMapping {
			mapped, err := prepared.responseMapping.Render(responseEnv)
			if err != nil {
				return nil, runtimefailures.Wrap(runtimefailures.ClassConnectorFailure, "provider_response_projection_failed", "activity-runtime", "project_http_response", map[string]any{"tool": prepared.toolName, "status": resp.StatusCode}, redactActivityError(err, prepared.secrets))
			}
			result = mapped
		}
		if !prepared.outputSchema.IsZero() {
			if err := prepared.outputSchema.Validate(result); err != nil {
				return nil, runtimefailures.Wrap(runtimefailures.ClassConnectorFailure, "provider_response_schema_invalid", "activity-runtime", "validate_projected_response", map[string]any{"tool": prepared.toolName, "status": resp.StatusCode}, redactActivityError(err, prepared.secrets))
			}
		}
		if prepared.hasCompiledResult {
			projected, err := prepared.compiledResult.Project(result)
			if err != nil {
				return nil, runtimefailures.Wrap(runtimefailures.ClassConnectorFailure, "channel_result_projection_failed", "activity-runtime", "project_channel_result", map[string]any{"tool": prepared.toolName, "status": resp.StatusCode}, err)
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
	ref, ok := tool.ManagedCredentialExecution()
	if !ok {
		return nil, nil
	}
	key := ref.Key()
	source := semanticview.Source(nil)
	var store runtimemanagedcredentials.Store
	if d.coordinator != nil {
		source = d.coordinator.SemanticSource()
		store = d.coordinator.managedCredentials
	}
	flowID := intent.ExecutionFlowID.String()
	storeKey, mapped := semanticview.CredentialStoreKeyForFlow(source, flowID, key)
	if mapped && storeKey == "" {
		return nil, fmt.Errorf("managed credential %q is not declared and bound for imported package flow %s", key, flowID)
	}
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
		GrantType:      ref.GrantType(),
		Scopes:         ref.Scopes(),
		GrantModel:     ref.GrantModel(),
		TokenRequest:   ref.TokenRequest(),
		InstallationID: activityManagedCredentialInputValue(intent.Input, ref.InstallationIDInput()),
	})
	if err != nil {
		redacted := fmt.Errorf("%s", runtimemanagedcredentials.RedactString(err.Error(), record.SecretValues()...))
		return nil, activityAuthenticationFailure(redacted, intent.Tool, "access_managed_credential", "managed_credential")
	}
	return &activityManagedHTTPAuth{
		StoreKey:    storeKey,
		Token:       token,
		Record:      record,
		Header:      ref.Header(),
		Prefix:      ref.Prefix(),
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
	flowID := intent.ExecutionFlowID.String()
	channelTarget, channelPrivate := activityChannelTargetFromContext(ctx)
	for _, key := range keys {
		storeKey := ""
		mapped := false
		if channelPrivate {
			storeKey, mapped = channelTarget.CredentialStoreKey(key)
		}
		if !mapped {
			storeKey, mapped = semanticview.CredentialStoreKeyForFlow(source, flowID, key)
		}
		if mapped && storeKey == "" {
			return nil, nil, fmt.Errorf("credential %q is not declared and bound for imported package flow %s", key, flowID)
		}
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

type activityChannelTargetContextKey struct{}

type activityChannelTargetContextValue struct {
	target  ChannelActivityTarget
	private bool
}

func withActivityChannelTarget(ctx context.Context, target ChannelActivityTarget, private bool) context.Context {
	return context.WithValue(ctx, activityChannelTargetContextKey{}, activityChannelTargetContextValue{target: target, private: private})
}

func activityChannelTargetFromContext(ctx context.Context) (ChannelActivityTarget, bool) {
	value, ok := ctx.Value(activityChannelTargetContextKey{}).(activityChannelTargetContextValue)
	if !ok {
		return ChannelActivityTarget{}, false
	}
	return value.target, value.private
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
	routingSource := intent.RoutingSource
	if routingSource.Empty() {
		return fmt.Errorf("activity result requires admitted producer source")
	}
	evt, err := events.NewChildEvent(events.ChildEventInput{
		Facts: events.EventFacts{
			ID: eventID, Type: events.EventType(eventType),
			Producer: activityEventProducer(intent.Owner),
			TaskID:   intent.SourceTaskID, Payload: raw, ChainDepth: intent.ChainDepth + 1,
			Envelope: events.EventEnvelope{
				EntityID: intent.EntityID.String(), FlowInstance: intent.FlowInstance, Source: routingSource.Route(),
			},
			RoutingSource: routingSource, CreatedAt: time.Now().UTC(),
		},
		Lineage: events.EventLineage{
			RunID:         intent.SourceRunID,
			ParentEventID: firstNonEmptyString(intent.SourceEventID, intent.ParentEventID),
			TaskID:        intent.SourceTaskID,
			ExecutionMode: intent.ExecutionMode,
		},
	})
	if err != nil {
		return fmt.Errorf("construct activity result event: %w", err)
	}
	if d.emissions != nil {
		d.emissions.appendEvent(evt)
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
		FlowInstance:    firstNonEmptyString(intent.FlowInstance, intent.ExecutionFlowID.String()),
		NodeID:          intent.Owner.Key(),
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
