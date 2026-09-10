package contracts

import (
	"fmt"
	"go/ast"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

const transitionGuardModule = "github.com/division-sh/swarm/"

type transitionBoundaryUse struct {
	Caller, Operation string
}

func (u transitionBoundaryUse) key() string { return u.Caller + "::" + u.Operation }

// T22 guards typed ownership boundaries, not variable spellings or entire files.
// Counts also reject additional authority operations inside an approved function.
// T19's execution/mutation tests remain necessary: this is not an execution proof.
func TestCompiledTransitionOwnershipGuard(t *testing.T) {
	uses := loadTransitionBoundaryUses(t, nil)
	if problems := transitionBoundaryProblems(uses); len(problems) != 0 {
		t.Fatalf("compiled transition ownership boundary changed:\n%s", strings.Join(problems, "\n"))
	}
	t.Logf("reviewed %d exact caller/operation boundaries covering %d typed uses", len(allowedTransitionBoundaryUses()), len(uses))
}

func TestCompiledTransitionOwnershipGuardRejectsHostileUses(t *testing.T) {
	root := handlerRuleIdentityGuardRepoRoot(t)
	overlay := map[string][]byte{}
	want := map[string]int{}
	for _, pkg := range []string{
		"internal/runtime/semanticview", "internal/runtime/bootverify", "internal/runtime/pipeline",
		"internal/runtime/engine", "internal/runtime/workflowlifecycle", "internal/runtime/gateruntime",
		"internal/runtime/loopruntime", "internal/runtime/runforkexecution", "internal/runtime/authoringview",
		"internal/runtime/routingtopology", "internal/cliapp", "internal/apiv1", "internal/serveapp",
		"internal/store/internal/backend/runforkpersistence",
	} {
		// The same hostile program must be rejected in every audited consumer
		// family, including consumers with no current transition authority.
		overlay[filepath.Join(root, pkg, "transition_guard_hostile.go")] = []byte(fmt.Sprintf(`package %s
import strangeAlias "github.com/division-sh/swarm/internal/runtime/contracts"
import strangeCorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
type t22GraphAlias = strangeAlias.WorkflowStageTopology
type t22Envelope struct { *t22GraphAlias }
func t22FlowlessRead(unexpected []strangeAlias.WorkflowStageTopologyEdge, a, b string) (strangeAlias.WorkflowStageTopologyEdge, bool) {
    for _, arbitrary := range unexpected {
        if arbitrary.From == a && arbitrary.To == b { return arbitrary, true }
    }
    return strangeAlias.WorkflowStageTopologyEdge{}, false
}
func t22Promoted(x t22Envelope) int { return len(x.Edges) }
func t22TargetOnly(x t22GraphAlias, target string) bool {
    for _, stage := range x.Stages { if stage == target { return true } }
    return false
}
func t22RewriteSite(x strangeAlias.WorkflowTransitionSite) strangeAlias.WorkflowTransitionSite {
    x.HandlerEvent = "another.handler"
    return x
}
func t22LocalAdmission(x t22GraphAlias, site strangeAlias.WorkflowTransitionSite, a, b string) (strangeAlias.CompiledTransition, error) {
    borrowed := x.AdmitTransition
    return borrowed(site, a, b)
}
func t22BorrowAcceptedContext() {
    _ = strangeCorrelation.InboundEventFromContext
    _ = strangeCorrelation.WithInboundEvent
}
// Same identifier spellings on unrelated types are not transition authority.
type t22Decoy struct { Edges []string; From, To, TriggerEventID, TriggerEventType, Transition, RuleSelection string }
func (x t22Decoy) WorkflowTransitions() []string { return x.Edges }
func t22Benign(x t22Decoy) bool { return x.From == x.To && len(x.WorkflowTransitions()) == 0 && x.TriggerEventID == x.TriggerEventType && x.Transition == x.RuleSelection }
`, filepath.Base(pkg)))
		want[pkg+".t22FlowlessRead::carrier fields"] = 2
		want[pkg+".t22Promoted::edge inventory"] = 1
		want[pkg+".t22TargetOnly::graph metadata"] = 1
		want[pkg+".t22RewriteSite::carrier site fields"] = 1
		want[pkg+".t22LocalAdmission::call internal/runtime/contracts.WorkflowStageTopology.AdmitTransition"] = 1
		want[pkg+".t22BorrowAcceptedContext::call internal/runtime/correlation.InboundEventFromContext"] = 1
		want[pkg+".t22BorrowAcceptedContext::call internal/runtime/correlation.WithInboundEvent"] = 1
	}

	// Append helpers to genuinely approved production files, only in memory.
	path, raw := transitionGuardOverlay(t, "internal/runtime/contracts/workflow_transition_admission.go", `
func (unexpected *WorkflowContractBundle) WorkflowTransitions() []WorkflowStageTopologyEdge { return nil }
func deriveRuleTransitions(unexpected HandlerTransitionSemantic) []WorkflowStageTopologyEdge { return nil }
type WorkflowTransitionContract struct { From, To string }
func t22PrivateEvidence(source WorkflowStageTopologyEdge) CompiledTransition {
    var arbitrary CompiledTransition
    arbitrary.edge = source
    return arbitrary
}
type t22ErasedEdge WorkflowStageTopologyEdge
func t22EraseEvidence(x WorkflowStageTopologyEdge) t22ErasedEdge { return t22ErasedEdge(x) }
func t22GuardTarget(unexpected WorkflowStageTopology) { _ = unexpected.GuardTerminationTarget }
`)
	needle := "func (t CompiledTransition) ValidateAgainst(graph WorkflowStageTopology) error {"
	if strings.Count(string(raw), needle) != 1 {
		t.Fatal("validated codec owner changed; update the explicit hostile insertion")
	}
	// Add a second authority reference inside an already allowed function.
	overlay[path] = []byte(strings.Replace(string(raw), needle, needle+"\n\t_ = graph.AdmitTransition", 1))
	want["internal/runtime/contracts.WorkflowContractBundle.WorkflowTransitions::retired declaration"] = 1
	want["internal/runtime/contracts.deriveRuleTransitions::retired declaration"] = 1
	want["internal/runtime/contracts.<package>::retired type WorkflowTransitionContract"] = 1
	want["internal/runtime/contracts.t22PrivateEvidence::compiled evidence fields"] = 1
	want["internal/runtime/contracts.t22EraseEvidence::erase typed evidence internal/runtime/contracts.WorkflowStageTopologyEdge"] = 1
	want["internal/runtime/contracts.CompiledTransition.ValidateAgainst::call internal/runtime/contracts.WorkflowStageTopology.AdmitTransition"] = 2
	want["internal/runtime/contracts.t22GuardTarget::call internal/runtime/contracts.WorkflowStageTopology.GuardTerminationTarget"] = 1

	path, raw = transitionGuardOverlay(t, "internal/runtime/contracts/workflow_contract_types.go", "")
	for _, entry := range []struct{ old, replacement, finding string }{
		{"type WorkflowSemanticView struct {", "type WorkflowSemanticView struct {\nTransitions []WorkflowStageTopologyEdge", "WorkflowSemanticView.Transitions"},
		{"type SystemNodeContract struct {", "type SystemNodeContract struct {\nOwnedTransitions []string", "SystemNodeContract.OwnedTransitions"},
	} {
		if strings.Count(string(raw), entry.old) != 1 {
			t.Fatal("retired-field hostile fixture cannot find its exact type owner")
		}
		raw = []byte(strings.Replace(string(raw), entry.old, entry.replacement, 1))
		want["internal/runtime/contracts.<package>::retired field "+entry.finding] = 1
	}
	overlay[path] = raw

	path, raw = transitionGuardOverlay(t, "internal/runtime/pipeline/workflow_transition_admission.go", `
func t22ValidationHandoff(unexpected pipelineEngineStateRepo) { _ = unexpected.validateMutationTransition }
func t22ReconstructHistory(a, b string) WorkflowTransitionRecord {
    return WorkflowTransitionRecord{TransitionID: a + "_" + b, From: a, To: b}
}
func t22AcceptedEventHandoff(unexpected DeliveryTargetApplication) {
    _ = runtimecorrelation.InboundEventFromContext
    _ = runtimecorrelation.WithInboundEvent
    _ = unexpected.Event
    _ = unexpected.Validate
    _ = deliveryTargetApplicationFromContext
    _ = withDeliveryTargetApplication
    _ = workflowNodeEventHandlerResolutionForDeliveryContext
    _ = workflowNodeDeliveryRoute
}
func t22RewriteTrigger(unexpected runtimeengine.StateMutation) runtimeengine.StateMutation {
    unexpected.TriggerEventID = "borrowed-event"
    unexpected.TriggerEventType = "borrowed-type"
    unexpected.TriggeredAt = unexpected.TriggeredAt.Add(1)
    return unexpected
}
type t22ErasedTrigger runtimeengine.StateMutation
func t22EraseTrigger(unexpected runtimeengine.StateMutation) t22ErasedTrigger { return t22ErasedTrigger(unexpected) }
func t22RestoreTrigger(unexpected t22ErasedTrigger) runtimeengine.StateMutation { return runtimeengine.StateMutation(unexpected) }
`)
	needle = "accepted, hasAccepted := runtimecorrelation.InboundEventFromContext(ctx)"
	if strings.Count(string(raw), needle) != 1 {
		t.Fatal("accepted-event owner changed; update the explicit hostile insertion")
	}
	overlay[path] = []byte(strings.Replace(string(raw), needle, needle+"\n_ = runtimecorrelation.InboundEventFromContext", 1))
	want["internal/runtime/pipeline.t22ValidationHandoff::call internal/runtime/pipeline.pipelineEngineStateRepo.validateMutationTransition"] = 1
	want["internal/runtime/pipeline.t22ReconstructHistory::construct internal/runtime/pipeline.WorkflowTransitionRecord"] = 1
	for _, callee := range []string{
		"internal/runtime/correlation.InboundEventFromContext", "internal/runtime/correlation.WithInboundEvent",
		"internal/runtime/pipeline.DeliveryTargetApplication.Event", "internal/runtime/pipeline.DeliveryTargetApplication.Validate",
		"internal/runtime/pipeline.deliveryTargetApplicationFromContext", "internal/runtime/pipeline.withDeliveryTargetApplication",
		"internal/runtime/pipeline.workflowNodeEventHandlerResolutionForDeliveryContext", "internal/runtime/pipeline.workflowNodeDeliveryRoute",
	} {
		want["internal/runtime/pipeline.t22AcceptedEventHandoff::call "+callee] = 1
	}
	want["internal/runtime/pipeline.pipelineEngineStateRepo.validateMutationTransition::call internal/runtime/correlation.InboundEventFromContext"] = 2
	want["internal/runtime/pipeline.t22RewriteTrigger::accepted trigger TriggerEventID"] = 1
	want["internal/runtime/pipeline.t22RewriteTrigger::accepted trigger TriggerEventType"] = 1
	want["internal/runtime/pipeline.t22RewriteTrigger::accepted trigger TriggeredAt"] = 2
	want["internal/runtime/pipeline.t22EraseTrigger::erase typed evidence internal/runtime/engine.StateMutation"] = 1
	want["internal/runtime/pipeline.t22RestoreTrigger::construct internal/runtime/engine.StateMutation"] = 1
	path, raw = transitionGuardOverlay(t, "internal/runtime/pipeline/workflow_handler_preview.go", `
type t22PreviewAlias = HandlerPreview
type t22PreviewEnvelope struct { *t22PreviewAlias }
func t22RewritePreview(unexpected t22PreviewEnvelope, arbitrary contractHandlerExecutionResult) {
    unexpected.Transition = arbitrary.Transition
    unexpected.RuleSelection = arbitrary.RuleSelection
}
func t22ConstructPreview(unexpected contractHandlerExecutionResult) HandlerPreview {
    return HandlerPreview{Transition: unexpected.Transition, RuleSelection: unexpected.RuleSelection}
}
type t22ErasedPreview HandlerPreview
func t22ErasePreview(unexpected HandlerPreview) t22ErasedPreview { return t22ErasedPreview(unexpected) }
func t22PreviewHandoff() { _ = PreviewContractHandlerExecution; _ = loadPreviewEngineState }
`)
	needle = "previewBundle := semanticview.CloneBundleForPreview(bundle, policyOverrides)"
	if strings.Count(string(raw), needle) != 1 {
		t.Fatal("preview owner changed; update the explicit hostile insertion")
	}
	overlay[path] = []byte(strings.Replace(string(raw), needle, needle+"\n_ = loadPreviewEngineState", 1))
	for _, field := range []string{"Transition", "RuleSelection"} {
		for _, caller := range []string{"t22RewritePreview", "t22ConstructPreview"} {
			want["internal/runtime/pipeline."+caller+"::typed preview "+field] = 1
			want["internal/runtime/pipeline."+caller+"::execution projection "+field] = 1
		}
	}
	want["internal/runtime/pipeline.t22ConstructPreview::construct internal/runtime/pipeline.HandlerPreview"] = 1
	want["internal/runtime/pipeline.t22ErasePreview::erase typed evidence internal/runtime/pipeline.HandlerPreview"] = 1
	want["internal/runtime/pipeline.t22PreviewHandoff::call internal/runtime/pipeline.PreviewContractHandlerExecution"] = 1
	want["internal/runtime/pipeline.t22PreviewHandoff::call internal/runtime/pipeline.loadPreviewEngineState"] = 1
	want["internal/runtime/pipeline.PreviewContractHandlerExecution::call internal/runtime/pipeline.loadPreviewEngineState"] = 1
	path, raw = transitionGuardOverlay(t, "internal/runtime/engine/transition_admission.go", `
func t22EngineHandoff(unexpected EngineMutation) error { return unexpected.ValidateTransitionEvidence() }
func t22SourceStageHandoff(unexpected *Executor) { _ = unexpected.validateSourceStage }
func (unexpected *Executor) killStateTarget(flow string) string { return flow }
func t22RetiredKillTarget(unexpected *Executor) string { return unexpected.killStateTarget(".") }
`)
	overlay[path] = raw
	want["internal/runtime/engine.t22EngineHandoff::call internal/runtime/engine.EngineMutation.ValidateTransitionEvidence"] = 1
	want["internal/runtime/engine.t22SourceStageHandoff::call internal/runtime/engine.Executor.validateSourceStage"] = 1
	want["internal/runtime/engine.Executor.killStateTarget::retired declaration"] = 1
	want["internal/runtime/engine.t22RetiredKillTarget::retired use internal/runtime/engine.Executor.killStateTarget"] = 1
	path, raw = transitionGuardOverlay(t, "internal/runtime/workflowlifecycle/transition.go", `
func t22GuardEvidence(unexpected Transition) {
    _ = unexpected.ValidateHandlerEvidence
    _ = unexpected.HandlerOrigin
}
func (unexpected Transition) ValidateHandlerSelection(handler contracts.SystemNodeEventHandler) error { return nil }
func t22RetiredHandlerSelection(unexpected Transition) { _ = unexpected.ValidateHandlerSelection }
`)
	overlay[path] = raw
	want["internal/runtime/workflowlifecycle.t22GuardEvidence::call internal/runtime/workflowlifecycle.Transition.ValidateHandlerEvidence"] = 1
	want["internal/runtime/workflowlifecycle.t22GuardEvidence::call internal/runtime/workflowlifecycle.Transition.HandlerOrigin"] = 1
	want["internal/runtime/workflowlifecycle.Transition.ValidateHandlerSelection::retired declaration"] = 1
	want["internal/runtime/workflowlifecycle.t22RetiredHandlerSelection::retired use internal/runtime/workflowlifecycle.Transition.ValidateHandlerSelection"] = 1
	path, raw = transitionGuardOverlay(t, "internal/runtime/engine/interfaces.go", `
type TransitionValidator interface { ValidateTransition(currentState, nextState string) error }
func t22RetiredValidatorPort(unexpected TransitionValidator) error { return unexpected.ValidateTransition("from", "to") }
`)
	needle = "type RuntimeDependencies struct {"
	if strings.Count(string(raw), needle) != 1 {
		t.Fatal("retired validator fixture cannot find its exact dependency owner")
	}
	overlay[path] = []byte(strings.Replace(string(raw), needle, needle+"\nTransitionValidator TransitionValidator", 1))
	want["internal/runtime/engine.<package>::retired type TransitionValidator"] = 1
	want["internal/runtime/engine.<package>::retired field RuntimeDependencies.TransitionValidator"] = 1
	want["internal/runtime/engine.<package>::retired declaration internal/runtime/engine.TransitionValidator.ValidateTransition"] = 1
	want["internal/runtime/engine.t22RetiredValidatorPort::retired use internal/runtime/engine.TransitionValidator.ValidateTransition"] = 1

	uses := loadTransitionBoundaryUses(t, overlay)
	counts := map[string]int{}
	for _, use := range uses {
		counts[use.key()]++
		if strings.Contains(use.Caller, ".t22Benign") || strings.Contains(use.Caller, ".t22Decoy.") {
			t.Fatalf("identifier-only false positive: %#v", use)
		}
	}
	problems := strings.Join(transitionBoundaryProblems(uses), "\n")
	for key, count := range want {
		if counts[key] != count || !strings.Contains(problems, key) {
			t.Errorf("hostile boundary %s: found %d, want %d and a rejection; problems:\n%s", key, counts[key], count, problems)
		}
	}
	t.Logf("rejected %d independently asserted hostile boundaries; unrelated same-spelling receivers accepted", len(want))
}

func transitionBoundaryProblems(uses []transitionBoundaryUse) []string {
	counts := map[string]int{}
	for _, use := range uses {
		counts[use.key()]++
	}
	allowed := allowedTransitionBoundaryUses()
	var problems []string
	for key, count := range counts {
		if count != allowed[key] {
			problems = append(problems, fmt.Sprintf("%q: %d, // want %d", key, count, allowed[key]))
		}
	}
	for key, count := range allowed {
		if counts[key] == 0 {
			problems = append(problems, fmt.Sprintf("%s missing reviewed use count %d", key, count))
		}
	}
	sort.Strings(problems)
	return problems
}

func allowedTransitionBoundaryUses() map[string]int {
	return map[string]int{
		// Existing typed-event context producers/readers. These exact diagnostic,
		// lineage and dispatch uses do not grant transition selection authority.
		"internal/runtime/bus.EventBus.activeAgentDescriptors::call internal/runtime/correlation.InboundEventFromContext":                              1,
		"internal/runtime/bus.EventBus.activeTargetDescriptors::call internal/runtime/correlation.InboundEventFromContext":                             1,
		"internal/runtime/bus.EventBus.planSubscribedRoutePlan::call internal/runtime/correlation.WithInboundEvent":                                    1,
		"internal/runtime/pipeline.FreshActivityRequestLineage::call internal/runtime/pipeline.workflowNodeEventHandlerResolutionForDeliveryContext":   1,
		"internal/runtime/pipeline.PipelineCoordinator.notifyTestFlowTerminationCommitted::call internal/runtime/correlation.InboundEventFromContext":  1,
		"internal/runtime/pipeline.PipelineCoordinator.notifyTestWorkflowTerminalCommitted::call internal/runtime/correlation.InboundEventFromContext": 1,
		"internal/runtime.BudgetTracker.evaluateScope::call internal/runtime/correlation.InboundEventFromContext":                                      1,
		"internal/runtime/bus.EventBus.beginReceiverDispatch::call internal/runtime/correlation.WithInboundEvent":                                      1,
		"internal/runtime/bus.EventBus.materializePublishRecipientPlan::call internal/runtime/correlation.WithInboundEvent":                            1,
		"internal/runtime/bus.EventBus.receiverRouteContext::call internal/runtime/correlation.WithInboundEvent":                                       1,
		"internal/runtime/bus.InboundEventFromContext::call internal/runtime/correlation.InboundEventFromContext":                                      1,
		"internal/runtime/bus.WithInboundEvent::call internal/runtime/correlation.WithInboundEvent":                                                    1,
		"internal/runtime/bus.connectRoutePlanResolver.Plan::call internal/runtime/correlation.WithInboundEvent":                                       1,
		"internal/runtime/bus.deliveryPlanner.PlanDirect::call internal/runtime/correlation.WithInboundEvent":                                          1,
		"internal/runtime/bus.deliveryPlanner.PlanExactDirect::call internal/runtime/correlation.WithInboundEvent":                                     1,
		"internal/runtime/bus.deliveryPlanner.planAtGeneration::call internal/runtime/correlation.WithInboundEvent":                                    1,
		"internal/runtime/currentstate.RunIDFromContext::call internal/runtime/correlation.InboundEventFromContext":                                    1,
		"internal/runtime/decisioncard.CausalExecutionMode::call internal/runtime/correlation.InboundEventFromContext":                                 1,
		"internal/runtime/effects.logicalOperationIdentity::call internal/runtime/correlation.InboundEventFromContext":                                 1,
		"internal/runtime/effects.validateManagedAgentFramePrelaunch::call internal/runtime/correlation.InboundEventFromContext":                       1,
		"internal/runtime/ingress.Controller.publishTransitionEvent::call internal/runtime/correlation.InboundEventFromContext":                        1,
		"internal/runtime/llm.newAgentStartedRuntimeDiagnostic::call internal/runtime/correlation.InboundEventFromContext":                             1,
		"internal/runtime/manager.AgentManager.processEventDetailedOwned::call internal/runtime/correlation.WithInboundEvent":                          1,
		"internal/runtime/manager.agentDeliveryExecutionContext::call internal/runtime/correlation.WithInboundEvent":                                   1,
		"internal/runtime/manager.newPlatformContextualRuntimeDiagnosticEvent::call internal/runtime/correlation.InboundEventFromContext":              1,
		"internal/runtime/manager.terminalFlowSelfRetiringAgent::call internal/runtime/correlation.InboundEventFromContext":                            1,
		"internal/runtime/pipeline.PipelineCoordinator.publish::call internal/runtime/correlation.InboundEventFromContext":                             1,
		"internal/runtime/pipeline.PipelineCoordinator.publishDirect::call internal/runtime/correlation.InboundEventFromContext":                       1,
		"internal/runtime/pipeline.PipelineCoordinator.serveFanOutTurn::call internal/runtime/correlation.WithInboundEvent":                            1,
		"internal/runtime/pipeline.pipelineActivityDispatcher.logActivityRuntime::call internal/runtime/correlation.InboundEventFromContext":           1,
		"internal/store/internal/backend/entityruntime.InsertSQLiteEntityStateDiff::call internal/runtime/correlation.InboundEventFromContext":         1,
		"internal/store/internal/backend/mutationlog.InsertSQLiteWithStory::call internal/runtime/correlation.InboundEventFromContext":                 1,
		"internal/store/internal/backend/mutationlog.InsertWithStory::call internal/runtime/correlation.InboundEventFromContext":                       1,
		"internal/store/storetest.PersistManagedAgentTurnFixture::call internal/runtime/correlation.WithInboundEvent":                                  1,
		// T19: accepted execution binding and the existing exact delivery owners.
		"internal/runtime/pipeline.DeclarativeNode.HandleEvent::call internal/runtime/pipeline.workflowNodeEventHandlerResolutionForDeliveryContext":                                      1,
		"internal/runtime/pipeline.PipelineCoordinator.executeAuthoritativeNodeHandler::call internal/runtime/pipeline.workflowNodeEventHandlerResolutionForDeliveryContext":              1,
		"internal/runtime/pipeline.PipelineCoordinator.executeNodeContractHandler::call internal/runtime/correlation.WithInboundEvent":                                                    1,
		"internal/runtime/pipeline.PipelineCoordinator.executeNodeContractHandler::call internal/runtime/pipeline.DeliveryTargetApplication.Event":                                        1,
		"internal/runtime/pipeline.PipelineCoordinator.executeNodeContractHandler::call internal/runtime/pipeline.DeliveryTargetApplication.Validate":                                     1,
		"internal/runtime/pipeline.PipelineCoordinator.executeNodeContractHandler::call internal/runtime/pipeline.deliveryTargetApplicationFromContext":                                   1,
		"internal/runtime/pipeline.PipelineCoordinator.executeNodeContractHandler::call internal/runtime/pipeline.withDeliveryTargetApplication":                                          1,
		"internal/runtime/pipeline.PipelineCoordinator.executeNodeHandlerPlanResultWithEmissionPlan::call internal/runtime/correlation.WithInboundEvent":                                  1,
		"internal/runtime/pipeline.PipelineCoordinator.executeNodeHandlerPlanResultWithEmissionPlan::call internal/runtime/pipeline.DeliveryTargetApplication.Event":                      1,
		"internal/runtime/pipeline.PipelineCoordinator.executeNodeHandlerPlanResultWithEmissionPlan::call internal/runtime/pipeline.withDeliveryTargetApplication":                        1,
		"internal/runtime/pipeline.PipelineCoordinator.executeNodeHandlerPlanResultWithEmissionPlan::call internal/runtime/pipeline.workflowNodeEventHandlerResolutionForDeliveryContext": 1,
		"internal/runtime/pipeline.PipelineCoordinator.loadCurrentDeliveryTargetState::call internal/runtime/pipeline.DeliveryTargetApplication.Event":                                    3,
		"internal/runtime/pipeline.PipelineCoordinator.loadCurrentDeliveryTargetState::call internal/runtime/pipeline.DeliveryTargetApplication.Validate":                                 1,
		"internal/runtime/pipeline.PipelineCoordinator.prepareDeliveryTargetApplication::call internal/runtime/pipeline.DeliveryTargetApplication.Validate":                               3,
		"internal/runtime/pipeline.PipelineCoordinator.workflowNodeConnectedInputFailureApplies::call internal/runtime/pipeline.workflowNodeDeliveryRoute":                                1,
		"internal/runtime/pipeline.PipelineCoordinator.workflowNodeDeliveryRouteMatches::call internal/runtime/pipeline.workflowNodeDeliveryRoute":                                        1,
		"internal/runtime/pipeline.PipelineCoordinator.workflowNodeInterceptPolicy::call internal/runtime/pipeline.workflowNodeDeliveryRoute":                                             1,
		"internal/runtime/pipeline.coordinatorHandlerExecutionEngine.ExecuteHandlerSteps::call internal/runtime/correlation.WithInboundEvent":                                             1,
		"internal/runtime/pipeline.coordinatorHandlerExecutionEngine.ExecuteHandlerSteps::call internal/runtime/pipeline.DeliveryTargetApplication.Event":                                 1,
		"internal/runtime/pipeline.coordinatorHandlerExecutionEngine.ExecuteHandlerSteps::call internal/runtime/pipeline.DeliveryTargetApplication.Validate":                              1,
		"internal/runtime/pipeline.coordinatorHandlerExecutionEngine.ExecuteHandlerSteps::call internal/runtime/pipeline.deliveryTargetApplicationFromContext":                            1,
		"internal/runtime/pipeline.coordinatorHandlerExecutionEngine.ExecuteHandlerSteps::call internal/runtime/pipeline.withDeliveryTargetApplication":                                   1,
		"internal/runtime/pipeline.pipelineEngineMutationOwner.CommitEngineMutation::call internal/runtime/pipeline.DeliveryTargetApplication.Validate":                                   1,
		"internal/runtime/pipeline.pipelineEngineMutationOwner.CommitEngineMutation::call internal/runtime/pipeline.deliveryTargetApplicationFromContext":                                 1,
		"internal/runtime/pipeline.pipelineEngineStateRepo.LoadState::call internal/runtime/pipeline.DeliveryTargetApplication.Validate":                                                  1,
		"internal/runtime/pipeline.pipelineEngineStateRepo.LoadState::call internal/runtime/pipeline.deliveryTargetApplicationFromContext":                                                1,
		"internal/runtime/pipeline.pipelineEngineStateRepo.prepareMutation::call internal/runtime/pipeline.DeliveryTargetApplication.Validate":                                            1,
		"internal/runtime/pipeline.pipelineEngineStateRepo.validateMutationTransition::call internal/runtime/correlation.InboundEventFromContext":                                         1,
		"internal/runtime/pipeline.pipelineEngineStateRepo.validateMutationTransition::call internal/runtime/pipeline.DeliveryTargetApplication.Event":                                    1,
		"internal/runtime/pipeline.pipelineEngineStateRepo.validateMutationTransition::call internal/runtime/pipeline.DeliveryTargetApplication.Validate":                                 1,
		"internal/runtime/pipeline.pipelineEngineStateRepo.validateMutationTransition::call internal/runtime/pipeline.deliveryTargetApplicationFromContext":                               1,
		"internal/runtime/pipeline.pipelineEngineStateRepo.validateMutationTransition::call internal/runtime/pipeline.workflowNodeDeliveryRoute":                                          1,
		"internal/runtime/pipeline.pipelineEngineStateRepo.validateMutationTransition::call internal/runtime/pipeline.workflowNodeEventHandlerResolutionForDeliveryContext":               1,
		"internal/runtime/pipeline.workflowNodeEventHandlerResolutionForDelivery::call internal/runtime/pipeline.workflowNodeEventHandlerResolutionForDeliveryContext":                    1,
		"internal/runtime/pipeline.workflowNodeEventHandlerResolutionForDeliveryContext::call internal/runtime/pipeline.workflowNodeDeliveryRoute":                                        1,
		"internal/runtime/pipeline.workflowNodeHandlerEventKeyForExecution::call internal/runtime/pipeline.workflowNodeEventHandlerResolutionForDeliveryContext":                          1,
		"internal/runtime/pipeline.workflowNodePolicyForDelivery::call internal/runtime/pipeline.workflowNodeEventHandlerResolutionForDeliveryContext":                                    1,
		"internal/runtime/pipeline.workflowNodeProducerSource::call internal/runtime/pipeline.DeliveryTargetApplication.Validate":                                                         1,
		"internal/runtime/pipeline.workflowNodeProducerSource::call internal/runtime/pipeline.deliveryTargetApplicationFromContext":                                                       1,
		// Trigger projections originate at execution/lifecycle producers; validation
		// and persistence consume them without deriving a replacement accepted event.
		"internal/runtime/engine.EngineMutation.ValidateTransitionEvidence::accepted trigger TriggerEventID":              1,
		"internal/runtime/engine.EngineMutation.ValidateTransitionEvidence::accepted trigger TriggerEventType":            1,
		"internal/runtime/engine.EngineMutation.ValidateTransitionEvidence::accepted trigger TriggeredAt":                 1,
		"internal/runtime/engine.Executor.persist::accepted trigger TriggerEventID":                                       1,
		"internal/runtime/engine.Executor.persist::accepted trigger TriggerEventType":                                     1,
		"internal/runtime/engine.Executor.persist::accepted trigger TriggeredAt":                                          1,
		"internal/runtime/pipeline.PipelineCoordinator.handleWorkflowStageTimerFire::accepted trigger TriggerEventID":     1,
		"internal/runtime/pipeline.PipelineCoordinator.handleWorkflowStageTimerFire::accepted trigger TriggerEventType":   1,
		"internal/runtime/pipeline.PipelineCoordinator.handleWorkflowStageTimerFire::accepted trigger TriggeredAt":        1,
		"internal/runtime/pipeline.PipelineCoordinator.planWorkflowLifecycleEffect::accepted trigger TriggerEventID":      1,
		"internal/runtime/pipeline.PipelineCoordinator.planWorkflowLifecycleEffect::accepted trigger TriggerEventType":    1,
		"internal/runtime/pipeline.PipelineCoordinator.planWorkflowLifecycleEffect::accepted trigger TriggeredAt":         1,
		"internal/runtime/pipeline.PipelineCoordinator.projectWorkflowEvidence::accepted trigger TriggerEventID":          1,
		"internal/runtime/pipeline.PipelineCoordinator.projectWorkflowEvidence::accepted trigger TriggerEventType":        1,
		"internal/runtime/pipeline.PipelineCoordinator.projectWorkflowEvidence::accepted trigger TriggeredAt":             1,
		"internal/runtime/pipeline.PipelineCoordinator.routeWorkflowGateDecision::accepted trigger TriggerEventID":        1,
		"internal/runtime/pipeline.PipelineCoordinator.routeWorkflowGateDecision::accepted trigger TriggerEventType":      1,
		"internal/runtime/pipeline.PipelineCoordinator.routeWorkflowGateDecision::accepted trigger TriggeredAt":           1,
		"internal/runtime/pipeline.artifactRepoResultState::accepted trigger TriggerEventID":                              1,
		"internal/runtime/pipeline.artifactRepoResultState::accepted trigger TriggerEventType":                            1,
		"internal/runtime/pipeline.artifactRepoResultState::accepted trigger TriggeredAt":                                 1,
		"internal/runtime/pipeline.pipelineEngineStateRepo.prepareMutation::accepted trigger TriggerEventID":              2,
		"internal/runtime/pipeline.pipelineEngineStateRepo.prepareMutation::accepted trigger TriggerEventType":            1,
		"internal/runtime/pipeline.pipelineEngineStateRepo.prepareMutation::accepted trigger TriggeredAt":                 8,
		"internal/runtime/pipeline.pipelineEngineStateRepo.validateMutationTransition::accepted trigger TriggerEventID":   2,
		"internal/runtime/pipeline.pipelineEngineStateRepo.validateMutationTransition::accepted trigger TriggerEventType": 2,
		"internal/runtime/pipeline.pipelineEngineStateRepo.validateMutationTransition::accepted trigger TriggeredAt":      1,
		// T20: typed result transport, not reconstruction from the display RuleID.
		"internal/runtime/pipeline.PipelineCoordinator.executeNodeContractHandler::construct internal/runtime/pipeline.contractHandlerExecutionResult": 4,
		"internal/runtime/pipeline.PipelineCoordinator.executeNodeContractHandler::execution projection RuleSelection":                                 4,
		"internal/runtime/pipeline.PipelineCoordinator.executeNodeContractHandler::execution projection Transition":                                    3,
		"internal/runtime/pipeline.PipelineCoordinator.executeNodeHandlerPlanResultWithEmissionPlan::execution projection RuleSelection":               2,
		"internal/runtime/pipeline.PreviewContractHandlerExecution::construct internal/runtime/pipeline.HandlerPreview":                                1,
		"internal/runtime/pipeline.PreviewContractHandlerExecution::execution projection RuleSelection":                                                2,
		"internal/runtime/pipeline.PreviewContractHandlerExecution::execution projection Transition":                                                   1,
		"internal/runtime/pipeline.PreviewContractHandlerExecution::typed preview RuleSelection":                                                       1,
		"internal/runtime/pipeline.PreviewContractHandlerExecution::typed preview Transition":                                                          1,
		"internal/runtime/pipeline.pipelineEngineStateRepo.LoadState::call internal/runtime/pipeline.loadPreviewEngineState":                           1,
		// Compiler graph, carrier owners, and validated codec. No other producer is permitted.
		"internal/runtime/contracts.BuildWorkflowStageTopology::call internal/runtime/contracts.HandlerTransitionAdvanceCarriers":              1,
		"internal/runtime/contracts.BuildWorkflowStageTopology::construct internal/runtime/contracts.WorkflowStageTopology":                    1,
		"internal/runtime/contracts.BuildWorkflowStageTopology::construct internal/runtime/contracts.WorkflowStageTopologyEdge":                4,
		"internal/runtime/contracts.BuildWorkflowStageTopology::edge inventory":                                                                11,
		"internal/runtime/contracts.CompiledTransition.Edge::compiled evidence fields":                                                         1,
		"internal/runtime/contracts.CompiledTransition.FlowID::compiled evidence fields":                                                       1,
		"internal/runtime/contracts.CompiledTransition.MarshalJSON::carrier fields":                                                            16,
		"internal/runtime/contracts.CompiledTransition.MarshalJSON::compiled evidence fields":                                                  2,
		"internal/runtime/contracts.CompiledTransition.UnmarshalJSON::carrier fields":                                                          2,
		"internal/runtime/contracts.CompiledTransition.UnmarshalJSON::construct internal/runtime/contracts.CompiledTransition":                 1,
		"internal/runtime/contracts.CompiledTransition.UnmarshalJSON::construct internal/runtime/contracts.WorkflowStageTopologyEdge":          1,
		"internal/runtime/contracts.CompiledTransition.Validate::carrier fields":                                                               60,
		"internal/runtime/contracts.CompiledTransition.Validate::compiled evidence fields":                                                     2,
		"internal/runtime/contracts.CompiledTransition.ValidateAgainst::call internal/runtime/contracts.WorkflowStageTopology.AdmitTransition": 1,
		"internal/runtime/contracts.CompiledTransition.ValidateAgainst::call internal/runtime/contracts.WorkflowStageTopologyEdge.Site":        1,
		"internal/runtime/contracts.CompiledTransition.ValidateAgainst::carrier fields":                                                        2,
		"internal/runtime/contracts.CompiledTransition.ValidateAgainst::compiled evidence fields":                                              6,
		"internal/runtime/contracts.HandlerAdvanceTargets::call internal/runtime/contracts.HandlerAdvanceCarriers":                             1,
		"internal/runtime/contracts.WorkflowContractBundle.WorkflowStageTopology::edge inventory":                                              2,
		"internal/runtime/contracts.WorkflowContractBundle.WorkflowStageTopology::topology inventory":                                          1,
		"internal/runtime/contracts.WorkflowStageTopology.AdmitTransition::call internal/runtime/contracts.WorkflowStageTopologyEdge.Site":     1,
		"internal/runtime/contracts.WorkflowStageTopology.AdmitTransition::carrier fields":                                                     2,
		"internal/runtime/contracts.WorkflowStageTopology.AdmitTransition::construct internal/runtime/contracts.CompiledTransition":            1,
		"internal/runtime/contracts.WorkflowStageTopology.AdmitTransition::edge inventory":                                                     1,
		"internal/runtime/contracts.WorkflowStageTopology.HandlerTargets::carrier fields":                                                      3,
		"internal/runtime/contracts.WorkflowStageTopology.HandlerTargets::edge inventory":                                                      1,
		"internal/runtime/contracts.WorkflowStageTopology.StageCanReenter::carrier fields":                                                     2,
		"internal/runtime/contracts.WorkflowStageTopology.StageCanReenter::edge inventory":                                                     1,
		"internal/runtime/contracts.WorkflowStageTopology.StronglyConnectedComponent::edge inventory":                                          2,
		"internal/runtime/contracts.WorkflowStageTopologyEdge.Site::carrier fields":                                                            10,
		"internal/runtime/contracts.WorkflowStageTopologyEdge.Site::construct internal/runtime/contracts.WorkflowTransitionSite":               1,
		"internal/runtime/contracts.appendTopologyEdge::carrier fields":                                                                        8,
		"internal/runtime/contracts.deriveWorkflowStageTopologies::call internal/runtime/contracts.BuildWorkflowStageTopology":                 1,
		"internal/runtime/contracts.populateWorkflowSemantics::topology inventory":                                                             2,
		"internal/runtime/contracts.topologyEdgeSortKey::carrier fields":                                                                       15,
		"internal/runtime/contracts.topologyReachable::carrier fields":                                                                         2,
		// Read-only source wrappers; no transition legality or selection.
		"internal/runtime/semanticview.WorkflowStageTopology::call internal/runtime/contracts.WorkflowContractBundle.WorkflowStageTopology":                      1,
		"internal/runtime/semanticview.bundleSource.DerivedHandlerTransitions::call internal/runtime/contracts.WorkflowContractBundle.DerivedHandlerTransitions": 1,
		"internal/runtime/semanticview.bundleSource.WorkflowStageTopology::call internal/runtime/contracts.WorkflowContractBundle.WorkflowStageTopology":         1,
		"internal/runtime/semanticview.bundleSource.WorkflowStages::call internal/runtime/contracts.WorkflowContractBundle.WorkflowStages":                       1,
		// Static reference/ownership/reachability checks consume compiled owners.
		"internal/runtime/bootverify.checkLoopValidation::call internal/runtime/semanticview.WorkflowStageTopology":                                  1,
		"internal/runtime/bootverify.checkerContext.configFromPayloadAlignment::call internal/runtime/semanticview.Source.DerivedHandlerTransitions": 1,
		"internal/runtime/bootverify.checkerContext.gateSchemaValidation::call internal/runtime/semanticview.Source.DerivedHandlerTransitions":       1,
		"internal/runtime/bootverify.checkerContext.stateMachineCoherence::call internal/runtime/semanticview.Source.DerivedHandlerTransitions":      1,
		"internal/runtime/bootverify.checkerContext.transitionOwnership::call internal/runtime/contracts.HandlerAdvanceCarriers":                     1,
		"internal/runtime/bootverify.checkerContext.transitionOwnership::call internal/runtime/semanticview.WorkflowStageTopology":                   1,
		"internal/runtime/bootverify.checkerContext.transitionOwnership::call internal/runtime/contracts.WorkflowStageTopology.AdmitTransition":      1,
		"internal/runtime/bootverify.checkerContext.transitionOwnership::call internal/runtime/contracts.WorkflowStageTopologyEdge.Site":             1,
		"internal/runtime/bootverify.checkerContext.transitionOwnership::carrier fields":                                                             16,
		"internal/runtime/bootverify.checkerContext.transitionOwnership::edge inventory":                                                             1,
		"internal/runtime/bootverify.checkerContext.transitionReferences::call internal/runtime/semanticview.WorkflowStageTopology":                  1,
		"internal/runtime/bootverify.checkerContext.transitionReferences::carrier fields":                                                            18,
		"internal/runtime/bootverify.checkerContext.transitionReferences::edge inventory":                                                            1,
		"internal/runtime/bootverify.compiledLoopEscapeOwnsEdge::carrier fields":                                                                     6,
		"internal/runtime/bootverify.joinStageCanReenter::call internal/runtime/semanticview.WorkflowStageTopology":                                  1,
		"internal/runtime/bootverify.timerActivationStates::call internal/runtime/semanticview.WorkflowStageTopology":                                1,
		"internal/runtime/bootverify.timerCancelStateGraphEdges::carrier fields":                                                                     1,
		"internal/runtime/bootverify.workflowStageGraphEdges::call internal/runtime/semanticview.WorkflowStageTopology":                              1,
		"internal/runtime/bootverify.workflowStageGraphEdges::carrier fields":                                                                        2,
		"internal/runtime/bootverify.workflowStageGraphEdges::edge inventory":                                                                        1,
		// Selected handler and explicit guard-disposition admission.
		"internal/runtime/engine.Executor.newExecutionFrame::call internal/runtime/engine.Executor.validateSourceStage":                        1,
		"internal/runtime/engine.Executor.validateSourceStage::call internal/runtime/semanticview.WorkflowStageTopology":                       1,
		"internal/runtime/engine.Executor.validateSourceStage::graph metadata":                                                                 3,
		"internal/runtime/engine.Executor.admitSelectedTransition::call internal/runtime/contracts.WorkflowStageTopology.AdmitTransition":      1,
		"internal/runtime/engine.Executor.admitSelectedTransition::call internal/runtime/semanticview.WorkflowStageTopology":                   1,
		"internal/runtime/engine.Executor.admitSelectedTransition::call internal/runtime/workflowlifecycle.NewCompiledTransition":              1,
		"internal/runtime/engine.Executor.admitSelectedTransition::call internal/runtime/workflowlifecycle.Transition.ValidateHandlerEvidence": 1,
		"internal/runtime/engine.Executor.admitSelectedTransition::construct internal/runtime/contracts.WorkflowTransitionSite":                1,
		"internal/runtime/engine.Executor.applyGuardFailure::call internal/runtime/semanticview.WorkflowStageTopology":                         1,
		"internal/runtime/engine.Executor.applyGuardFailure::call internal/runtime/workflowlifecycle.NewGuardTermination":                      1,
		"internal/runtime/engine.Executor.applyGuardFailure::call internal/runtime/contracts.WorkflowStageTopology.GuardTerminationTarget":     1,
		"internal/runtime/engine.Executor.applyGuardFailure::call internal/runtime/workflowlifecycle.Transition.ValidateHandlerEvidence":       1,
		// Frozen gate carrier validation; not ambient route selection.
		"internal/runtime/gateruntime.validateTransition::carrier fields": 3,
		// Exact production handoffs, stage metadata, history writers and strict hydration.
		"internal/runtime/pipeline.DecodeWorkflowInstancePersistenceRecord::call internal/runtime/pipeline.validateWorkflowTransitionHistoryFlow":                  1,
		"internal/runtime/pipeline.PipelineCoordinator.handleWorkflowStageTimerFire::call internal/runtime/contracts.WorkflowStageTopology.AdmitTransition":        1,
		"internal/runtime/pipeline.PipelineCoordinator.handleWorkflowStageTimerFire::call internal/runtime/semanticview.WorkflowStageTopology":                     1,
		"internal/runtime/pipeline.PipelineCoordinator.handleWorkflowStageTimerFire::call internal/runtime/workflowlifecycle.NewCompiledTransition":                1,
		"internal/runtime/pipeline.PipelineCoordinator.handleWorkflowStageTimerFire::construct internal/runtime/contracts.WorkflowTransitionSite":                  1,
		"internal/runtime/pipeline.PipelineCoordinator.isTerminalFlowState::call internal/runtime/semanticview.WorkflowStageTopology":                              1,
		"internal/runtime/pipeline.PipelineCoordinator.planWorkflowGateEffect::call internal/runtime/contracts.WorkflowStageTopology.AdmitTransition":              1,
		"internal/runtime/pipeline.PipelineCoordinator.planWorkflowGateEffect::call internal/runtime/semanticview.WorkflowStageTopology":                           1,
		"internal/runtime/pipeline.PipelineCoordinator.planWorkflowGateEffect::construct internal/runtime/contracts.WorkflowTransitionSite":                        1,
		"internal/runtime/pipeline.PipelineCoordinator.routeWorkflowGateDecision::call internal/runtime/workflowlifecycle.NewCompiledTransition":                   1,
		"internal/runtime/pipeline.pipelineEngineGuardRunner.EvaluateGuard::call internal/runtime/semanticview.WorkflowStageTopology":                              1,
		"internal/runtime/pipeline.pipelineEngineMutationOwner.CommitEngineMutation::call internal/runtime/engine.EngineMutation.ValidateTransitionEvidence":       1,
		"internal/runtime/pipeline.pipelineEngineStateRepo.prepareMutation::call internal/runtime/pipeline.pipelineEngineStateRepo.validateMutationTransition":     1,
		"internal/runtime/pipeline.pipelineEngineStateRepo.prepareMutation::construct internal/runtime/pipeline.WorkflowTransitionRecord":                          1,
		"internal/runtime/pipeline.pipelineEngineStateRepo.validateMutationTransition::call internal/runtime/semanticview.WorkflowStageTopology":                   1,
		"internal/runtime/pipeline.pipelineEngineStateRepo.validateMutationTransition::call internal/runtime/workflowlifecycle.Transition.ValidateAgainst":         1,
		"internal/runtime/pipeline.pipelineEngineStateRepo.validateMutationTransition::call internal/runtime/workflowlifecycle.Transition.ValidateHandlerEvidence": 1,
		"internal/runtime/pipeline.pipelineEngineStateRepo.validateMutationTransition::call internal/runtime/workflowlifecycle.Transition.HandlerOrigin":           1,
		"internal/runtime/pipeline.pipelineEngineStateRepo.validateMutationTransition::carrier fields":                                                             4,
		"internal/runtime/pipeline.validateWorkflowTransitionHistoryFlow::call internal/runtime/pipeline.validateWorkflowTransitionRecord":                         1,
		"internal/runtime/pipeline.validateWorkflowTransitionHistoryFlow::history evidence fields":                                                                 2,
		"internal/runtime/pipeline.validateWorkflowTransitionRecord::history evidence fields":                                                                      11,
		"internal/runtime/pipeline.workflowInstancePersistedProjectionFromInstance::call internal/runtime/pipeline.validateWorkflowTransitionHistoryFlow":          1,
		"internal/runtime/pipeline.workflowInstanceTransitionHistoryFromConfig::call internal/runtime/pipeline.validateWorkflowTransitionRecord":                   1,
		// Validated lifecycle evidence construction and round-trip.
		"internal/runtime/workflowlifecycle.NewCompiledTransition::call internal/runtime/workflowlifecycle.finishTransition":                          1,
		"internal/runtime/workflowlifecycle.NewCompiledTransition::carrier fields":                                                                    2,
		"internal/runtime/workflowlifecycle.NewCompiledTransition::construct internal/runtime/workflowlifecycle.Transition":                           1,
		"internal/runtime/workflowlifecycle.NewGuardTermination::call internal/runtime/workflowlifecycle.finishTransition":                            1,
		"internal/runtime/workflowlifecycle.NewGuardTermination::construct internal/runtime/workflowlifecycle.Transition":                             1,
		"internal/runtime/workflowlifecycle.Transition.UnmarshalJSON::construct internal/runtime/workflowlifecycle.Transition":                        1,
		"internal/runtime/workflowlifecycle.Transition.Validate::call internal/runtime/workflowlifecycle.finishTransition":                            1,
		"internal/runtime/workflowlifecycle.Transition.ValidateAgainst::call internal/runtime/contracts.CompiledTransition.ValidateAgainst":           1,
		"internal/runtime/workflowlifecycle.Transition.ValidateAgainst::call internal/runtime/contracts.WorkflowStageTopology.GuardTerminationTarget": 1,
		"internal/runtime/workflowlifecycle.Transition.HandlerOrigin::carrier fields":                                                                 3,
		"internal/runtime/workflowlifecycle.Transition.validateCause::carrier fields":                                                                 6,
		// Authoring readback projects graph fields without selecting an execution carrier.
		"internal/runtime/authoringview.buildStageGraphEdgesForFlow::call internal/runtime/semanticview.WorkflowStageTopology": 1,
		"internal/runtime/authoringview.buildStageGraphEdgesForFlow::carrier fields":                                           16,
		"internal/runtime/authoringview.buildStageGraphEdgesForFlow::edge inventory":                                           2,
		// CLI/API scenario gate-name inventory is descriptive, not transition authority.
		"internal/apiv1.declaredTestSetupGateNames::call internal/runtime/contracts.WorkflowContractBundle.DerivedHandlerTransitions":                1,
		"internal/cliapp.scenarioRunner.declaredScenarioGateNames::call internal/runtime/contracts.WorkflowContractBundle.DerivedHandlerTransitions": 1,
		// Exact flow metadata and selected-site construction, never pair-only admission.
		"internal/runtime/pipeline.PipelineCoordinator.planWorkflowLifecycleEffect::call internal/runtime/pipeline.validateWorkflowInitialEntry":                       1,
		"internal/runtime/bootverify.checkerContext.transitionOwnership::graph metadata":                                                                               1,
		"internal/runtime/bootverify.timerActivationStates::graph metadata":                                                                                            1,
		"internal/runtime/contracts.BuildWorkflowStageTopology::graph metadata":                                                                                        5,
		"internal/runtime/contracts.CompiledTransition.ValidateAgainst::graph metadata":                                                                                1,
		"internal/runtime/contracts.WorkflowContractBundle.WorkflowStageTopology::graph metadata":                                                                      9,
		"internal/runtime/contracts.WorkflowStageTopology.AdmitTransition::graph metadata":                                                                             8,
		"internal/runtime/contracts.WorkflowStageTopology.HandlerStages::graph metadata":                                                                               1,
		"internal/runtime/contracts.WorkflowStageTopology.GuardTerminationTarget::graph metadata":                                                                      2,
		"internal/runtime/engine.Executor.admitSelectedTransition::carrier site fields":                                                                                10,
		"internal/runtime/pipeline.PipelineCoordinator.isTerminalFlowState::graph metadata":                                                                            1,
		"internal/runtime/pipeline.PipelineCoordinator.planWorkflowLifecycleEffect::call internal/runtime/pipeline.pipelineEngineStateRepo.validateMutationTransition": 1,
		"internal/runtime/pipeline.pipelineEngineGuardRunner.EvaluateGuard::graph metadata":                                                                            4,
		"internal/runtime/pipeline.pipelineEngineStateRepo.validateMutationTransition::graph metadata":                                                                 1,
		"internal/runtime/pipeline.validateWorkflowInitialEntry::call internal/runtime/semanticview.WorkflowStageTopology":                                             1,
		"internal/runtime/pipeline.validateWorkflowInitialEntry::graph metadata":                                                                                       3,
		"internal/runtime/workflowlifecycle.NewGuardTermination::graph metadata":                                                                                       3,
		"internal/runtime/workflowlifecycle.Transition.ValidateAgainst::graph metadata":                                                                                3,
	}
}

func loadTransitionBoundaryUses(t *testing.T, overlay map[string][]byte) []transitionBoundaryUse {
	t.Helper()
	root := handlerRuleIdentityGuardRepoRoot(t)
	pkgs, err := packages.Load(&packages.Config{
		Dir: root, Tests: false, Overlay: overlay,
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles |
			packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
	}, "./internal/runtime/...", "./internal/cliapp", "./internal/store/...", "./internal/apiv1", "./internal/serveapp", "./internal/operatorread/...")
	if err != nil {
		t.Fatal(err)
	}
	if packages.PrintErrors(pkgs) != 0 {
		t.Fatal("compiler-resolved transition census requires successfully typed packages")
	}
	loaded := map[string]bool{}
	var uses []transitionBoundaryUse
	for _, pkg := range pkgs {
		loaded[strings.TrimPrefix(pkg.PkgPath, transitionGuardModule)] = true
		for _, file := range pkg.Syntax {
			for _, decl := range file.Decls {
				if function, ok := decl.(*ast.FuncDecl); ok {
					object, ok := pkg.TypesInfo.Defs[function.Name].(*types.Func)
					if !ok {
						t.Fatalf("unresolved function %s", function.Name.Name)
					}
					caller := transitionGuardFunction(object)
					if retiredTransitionFunction(object) {
						uses = append(uses, transitionBoundaryUse{caller, "retired declaration"})
					}
					ast.Inspect(function.Body, func(node ast.Node) bool {
						uses = append(uses, transitionNodeUses(caller, node, pkg.TypesInfo)...)
						return true
					})
					continue
				}
				// Include package initializers and type declarations, not only calls.
				ast.Inspect(decl, func(node ast.Node) bool {
					caller := strings.TrimPrefix(pkg.PkgPath, transitionGuardModule) + ".<package>"
					uses = append(uses, transitionNodeUses(caller, node, pkg.TypesInfo)...)
					if spec, ok := node.(*ast.TypeSpec); ok {
						object, _ := pkg.TypesInfo.Defs[spec.Name].(*types.TypeName)
						if object != nil && retiredTransitionType(object) {
							uses = append(uses, transitionBoundaryUse{caller, "retired type " + object.Name()})
						}
						if object != nil {
							if fields, ok := object.Type().Underlying().(*types.Struct); ok {
								for i := 0; i < fields.NumFields(); i++ {
									if retiredTransitionField(transitionGuardType(object.Type()), fields.Field(i).Name()) {
										uses = append(uses, transitionBoundaryUse{caller, "retired field " + object.Name() + "." + fields.Field(i).Name()})
									}
								}
							}
						}
					}
					return true
				})
			}
		}
	}
	for _, required := range []string{
		"internal/runtime/contracts", "internal/runtime/semanticview", "internal/runtime/bootverify",
		"internal/runtime/pipeline", "internal/runtime/engine", "internal/runtime/workflowlifecycle",
		"internal/runtime/gateruntime", "internal/runtime/loopruntime", "internal/runtime/runforkexecution",
		"internal/runtime/authoringview", "internal/runtime/routingtopology", "internal/cliapp",
		"internal/store/internal/backend/runforkpersistence", "internal/apiv1", "internal/serveapp",
	} {
		if !loaded[required] {
			t.Fatalf("audited transition consumer package missing: %s", required)
		}
	}
	return uses
}

func transitionNodeUses(caller string, node ast.Node, info *types.Info) []transitionBoundaryUse {
	var operations []string
	switch n := node.(type) {
	case *ast.Ident:
		if fn, ok := info.Defs[n].(*types.Func); ok && retiredTransitionFunction(fn) {
			operations = append(operations, "retired declaration "+transitionGuardFunction(fn))
		}
		if fn, ok := info.Uses[n].(*types.Func); ok {
			if retiredTransitionFunction(fn) {
				operations = append(operations, "retired use "+transitionGuardFunction(fn))
			} else if operation := transitionGuardCallee(fn); operation != "" {
				operations = append(operations, operation)
			}
		}
	case *ast.SelectorExpr:
		selection := info.Selections[n]
		if selection != nil && selection.Kind() == types.FieldVal {
			owner, field := transitionGuardFieldOwner(selection), selection.Obj().Name()
			if operation := transitionProjectionFieldOperation(owner, field); operation != "" {
				operations = append(operations, operation)
			}
			switch {
			case retiredTransitionField(owner, field):
				operations = append(operations, "retired field "+owner+"."+field)
			case owner == "internal/runtime/contracts.WorkflowStageTopologyEdge":
				operations = append(operations, "carrier fields")
			case owner == "internal/runtime/contracts.WorkflowStageTopology" && field == "Edges":
				operations = append(operations, "edge inventory")
			case owner == "internal/runtime/contracts.WorkflowStageTopology":
				operations = append(operations, "graph metadata")
			case owner == "internal/runtime/contracts.WorkflowTransitionSite":
				operations = append(operations, "carrier site fields")
			case owner == "internal/runtime/contracts.WorkflowSemanticView" && field == "StageTopologies":
				operations = append(operations, "topology inventory")
			case owner == "internal/runtime/pipeline.WorkflowTransitionRecord":
				operations = append(operations, "history evidence fields")
			case owner == "internal/runtime/contracts.CompiledTransition":
				operations = append(operations, "compiled evidence fields")
			}
		}
	case *ast.CompositeLit:
		owner := transitionGuardType(info.TypeOf(n))
		if len(n.Elts) > 0 {
			if protectedTransitionConstruction(owner) {
				operations = append(operations, "construct "+owner)
			}
		}
		fields, isStruct := info.TypeOf(n).Underlying().(*types.Struct)
		for index, element := range n.Elts {
			fieldName := ""
			if keyed, ok := element.(*ast.KeyValueExpr); ok {
				if field, ok := keyed.Key.(*ast.Ident); ok {
					fieldName = field.Name
				}
			} else if isStruct && index < fields.NumFields() {
				fieldName = fields.Field(index).Name()
			}
			if operation := transitionProjectionFieldOperation(owner, fieldName); operation != "" {
				operations = append(operations, operation)
			}
		}
	case *ast.CallExpr:
		if info.Types[n.Fun].IsType() {
			if owner := transitionGuardType(info.TypeOf(n)); protectedTransitionConstruction(owner) || owner == "internal/runtime/engine.StateMutation" {
				operations = append(operations, "construct "+owner)
			} else if len(n.Args) == 1 {
				if source := transitionGuardType(info.TypeOf(n.Args[0])); protectedTransitionConstruction(source) || source == "internal/runtime/engine.StateMutation" {
					operations = append(operations, "erase typed evidence "+source)
				}
			}
		}
	}
	uses := make([]transitionBoundaryUse, 0, len(operations))
	for _, operation := range operations {
		uses = append(uses, transitionBoundaryUse{caller, operation})
	}
	return uses
}

func transitionProjectionFieldOperation(owner, field string) string {
	switch owner {
	case "internal/runtime/engine.StateMutation":
		if field == "TriggerEventID" || field == "TriggerEventType" || field == "TriggeredAt" {
			return "accepted trigger " + field
		}
	case "internal/runtime/pipeline.HandlerPreview":
		if field == "Transition" || field == "RuleSelection" {
			return "typed preview " + field
		}
	case "internal/runtime/pipeline.contractHandlerExecutionResult":
		if field == "Transition" || field == "RuleSelection" {
			return "execution projection " + field
		}
	}
	return ""
}

// Follow promoted fields to their actual declaring type; embedding must not
// turn an edge inventory into an unclassified field on an arbitrary wrapper.
func transitionGuardFieldOwner(selection *types.Selection) string {
	typ := selection.Recv()
	for i, index := range selection.Index() {
		typ = types.Unalias(typ)
		if pointer, ok := typ.(*types.Pointer); ok {
			typ = types.Unalias(pointer.Elem())
		}
		if i == len(selection.Index())-1 {
			return transitionGuardType(typ)
		}
		fields, ok := typ.Underlying().(*types.Struct)
		if !ok {
			return ""
		}
		typ = fields.Field(index).Type()
	}
	return ""
}

func transitionGuardType(typ types.Type) string {
	if typ == nil {
		return ""
	}
	typ = types.Unalias(typ)
	if pointer, ok := typ.(*types.Pointer); ok {
		typ = types.Unalias(pointer.Elem())
	}
	if named, ok := typ.(*types.Named); ok && named.Obj().Pkg() != nil {
		return strings.TrimPrefix(named.Obj().Pkg().Path(), transitionGuardModule) + "." + named.Obj().Name()
	}
	return ""
}

func transitionGuardFunction(fn *types.Func) string {
	signature := fn.Type().(*types.Signature)
	if signature.Recv() != nil {
		return transitionGuardType(signature.Recv().Type()) + "." + fn.Name()
	}
	return strings.TrimPrefix(fn.Pkg().Path(), transitionGuardModule) + "." + fn.Name()
}

func transitionGuardCallee(fn *types.Func) string {
	key := transitionGuardFunction(fn)
	switch key {
	case "internal/runtime/correlation.InboundEventFromContext", "internal/runtime/correlation.WithInboundEvent",
		"internal/runtime/pipeline.DeliveryTargetApplication.Event", "internal/runtime/pipeline.DeliveryTargetApplication.Validate",
		"internal/runtime/pipeline.deliveryTargetApplicationFromContext", "internal/runtime/pipeline.withDeliveryTargetApplication",
		"internal/runtime/pipeline.workflowNodeEventHandlerResolutionForDeliveryContext", "internal/runtime/pipeline.workflowNodeDeliveryRoute",
		"internal/runtime/pipeline.PreviewContractHandlerExecution", "internal/runtime/pipeline.loadPreviewEngineState":
		return "call " + key
	case "internal/runtime/contracts.WorkflowStageTopology.AdmitTransition", "internal/runtime/contracts.CompiledTransition.ValidateAgainst",
		"internal/runtime/contracts.WorkflowStageTopology.GuardTerminationTarget":
		return "call " + key
	case "internal/runtime/engine.EngineMutation.ValidateTransitionEvidence", "internal/runtime/engine.Executor.validateSourceStage",
		"internal/runtime/pipeline.pipelineEngineStateRepo.validateMutationTransition",
		"internal/runtime/workflowlifecycle.Transition.ValidateAgainst", "internal/runtime/workflowlifecycle.Transition.ValidateHandlerEvidence",
		"internal/runtime/workflowlifecycle.Transition.HandlerOrigin",
		"internal/runtime/pipeline.validateWorkflowTransitionHistoryFlow", "internal/runtime/pipeline.validateWorkflowTransitionRecord", "internal/runtime/pipeline.validateWorkflowInitialEntry":
		return "call " + key
	case "internal/runtime/contracts.BuildWorkflowStageTopology", "internal/runtime/contracts.HandlerAdvanceCarriers", "internal/runtime/contracts.HandlerTransitionAdvanceCarriers":
		return "call " + key
	case "internal/runtime/contracts.WorkflowStageTopologyEdge.Site":
		return "call " + key
	case "internal/runtime/workflowlifecycle.NewCompiledTransition", "internal/runtime/workflowlifecycle.NewGuardTermination", "internal/runtime/workflowlifecycle.finishTransition":
		return "call " + key
	case "internal/runtime/contracts.WorkflowContractBundle.WorkflowStageTopology", "internal/runtime/semanticview.WorkflowStageTopology",
		"internal/runtime/contracts.WorkflowContractBundle.DerivedHandlerTransitions", "internal/runtime/semanticview.Source.DerivedHandlerTransitions",
		"internal/runtime/contracts.WorkflowContractBundle.WorkflowStages", "internal/runtime/semanticview.Source.WorkflowStages":
		return "call " + key
	}
	return ""
}

func protectedTransitionConstruction(owner string) bool {
	switch owner {
	case "internal/runtime/contracts.WorkflowStageTopology", "internal/runtime/contracts.WorkflowStageTopologyEdge",
		"internal/runtime/contracts.WorkflowTransitionSite", "internal/runtime/contracts.CompiledTransition",
		"internal/runtime/workflowlifecycle.Transition", "internal/runtime/pipeline.WorkflowTransitionRecord",
		"internal/runtime/pipeline.HandlerPreview", "internal/runtime/pipeline.contractHandlerExecutionResult":
		return true
	}
	return false
}

func retiredTransitionType(object *types.TypeName) bool {
	key := strings.TrimPrefix(object.Pkg().Path(), transitionGuardModule) + "." + object.Name()
	switch key {
	case "internal/runtime/contracts.WorkflowTransitionContract", "internal/runtime/pipeline.WorkflowDefinition", "internal/runtime/pipeline.WorkflowTransition", "internal/runtime/engine.TransitionValidator":
		return true
	}
	return false
}

func retiredTransitionField(owner, field string) bool {
	return (owner == "internal/runtime/contracts.WorkflowSemanticView" && field == "Transitions") ||
		(owner == "internal/runtime/contracts.SystemNodeContract" && field == "OwnedTransitions") ||
		(owner == "internal/runtime/engine.RuntimeDependencies" && field == "TransitionValidator")
}

func retiredTransitionFunction(fn *types.Func) bool {
	key := transitionGuardFunction(fn)
	switch key {
	case "internal/runtime/contracts.deriveWorkflowTransitionContract", "internal/runtime/contracts.deriveRuleTransitions",
		"internal/runtime/contracts.deriveJoinTransitions", "internal/runtime/contracts.deriveStageTimerTransitions",
		"internal/runtime/contracts.WorkflowContractBundle.WorkflowTransitions", "internal/runtime/contracts.WorkflowContractBundle.TransitionIDsByOwner",
		"internal/runtime/semanticview.bundleSource.WorkflowTransitions", "internal/runtime/semanticview.Source.WorkflowTransitions",
		"internal/runtime/pipeline.WorkflowDefinition.Transition", "internal/runtime/pipeline.WorkflowDefinition.CanTransition",
		"internal/runtime/pipeline.WorkflowDefinition.TransitionByTrigger", "internal/runtime/pipeline.CanTransitionWorkflowState",
		"internal/runtime/pipeline.NewWorkflowDefinition", "internal/runtime/pipeline.LoadWorkflowDefinition",
		"internal/runtime/pipeline.workflowTransitionFromStages", "internal/runtime/pipeline.containsWorkflowStateID",
		"internal/runtime/pipeline.WorkflowStateTransition", "internal/runtime/pipeline.workflowTransitionRecord",
		"internal/runtime/pipeline.workflowTransitionIdentity", "internal/runtime/pipeline.workflowTransitionFromHandlerOutcome",
		"internal/runtime/pipeline.terminalStateFlowCandidates", "internal/runtime/pipeline.flowIDForWorkflowState",
		"internal/runtime/bootverify.transitionOwningFlowID", "internal/runtime/bootverify.transitionTriggerIsTimerReference",
		"internal/runtime/engine.TransitionValidator.ValidateTransition", "internal/runtime/engine.Executor.killStateTarget",
		"internal/runtime/workflowlifecycle.Transition.ValidateHandlerSelection":
		return true
	}
	return false
}

// Retained here so hostile overlays never write into the shared worktree.
func transitionGuardOverlay(t *testing.T, relative, appended string) (string, []byte) {
	t.Helper()
	path := filepath.Join(handlerRuleIdentityGuardRepoRoot(t), relative)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return path, append(raw, []byte(appended)...)
}
