package bus

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

var errUnexpectedDurableTestRole = errors.New("unexpected durable EventBus test role")

type unexpectedDurableTestRoles struct {
	runtimerunlifecycle.OperationOwner
}

func (unexpectedDurableTestRoles) CreateReplyContext(context.Context, runtimereplycontext.Record) error {
	return errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) LoadReplyContext(context.Context, string) (runtimereplycontext.Record, error) {
	return runtimereplycontext.Record{}, errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) ClaimReplyContext(context.Context, string, string) (runtimereplycontext.Record, runtimereplycontext.ClaimOutcome, error) {
	return runtimereplycontext.Record{}, "", errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) RequestCompletionCandidate(context.Context, runtimerunlifecycle.CandidateRequest) (runtimerunlifecycle.CandidateRequestDisposition, error) {
	return "", errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) TransitionActiveRun(context.Context, runtimerunlifecycle.ActiveTransitionRequest) (runtimerunlifecycle.MutationDisposition, error) {
	return "", errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) MarkTerminalRun(context.Context, runtimerunlifecycle.TerminalRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	return runtimerunlifecycle.Snapshot{}, "", errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) ActivateDeliveryAuthority(context.Context, runtimedelivery.ExecutionAuthority) error {
	return errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) InspectDeliveryRecovery(context.Context, runtimecorrelation.BundleSourceFact) (runtimedelivery.RecoveryInventory, error) {
	return runtimedelivery.RecoveryInventory{}, errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) ClaimDelivery(context.Context, runtimedelivery.ExecutionAuthority, events.Event, events.DeliveryRoute) (runtimedelivery.ClaimResult, error) {
	return runtimedelivery.ClaimResult{}, errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) ScanDeliveryContinuations(context.Context, runtimedelivery.ExecutionAuthority, runtimedelivery.ContinuationCursor, int) (runtimedelivery.ContinuationPage, error) {
	return runtimedelivery.ContinuationPage{}, errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) ObserveDeliveryContinuation(context.Context, runtimedelivery.ExecutionAuthority, string) (runtimedelivery.ContinuationObservation, error) {
	return runtimedelivery.ContinuationObservation{}, errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) RenewClaim(context.Context, runtimedelivery.Claim) (runtimedelivery.Snapshot, error) {
	return runtimedelivery.Snapshot{}, errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) BindAgentSession(context.Context, runtimedelivery.Claim, string) (runtimedelivery.Snapshot, error) {
	return runtimedelivery.Snapshot{}, errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) SettleSuccess(context.Context, runtimedelivery.Claim, []string, time.Duration) (runtimedelivery.Snapshot, error) {
	return runtimedelivery.Snapshot{}, errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) SettleFailure(context.Context, runtimedelivery.Claim, runtimedelivery.Settlement) (runtimedelivery.Snapshot, error) {
	return runtimedelivery.Snapshot{}, errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) Snapshot(context.Context, string) (runtimedelivery.Snapshot, error) {
	return runtimedelivery.Snapshot{}, errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) Outcomes(context.Context, string) ([]runtimedelivery.Outcome, error) {
	return nil, errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) ProveHandoff(context.Context, string, events.DeliveryRoute) (runtimedelivery.DurableHandoffProof, error) {
	return runtimedelivery.DurableHandoffProof{}, errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) SummarizeRun(context.Context, string) (runtimedelivery.RunSummary, error) {
	return runtimedelivery.RunSummary{}, errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) TerminalizeRun(context.Context, string, string) ([]runtimedelivery.Terminalization, error) {
	return nil, errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) UpsertFlowInstanceRoute(context.Context, FlowInstanceRouteRecord) error {
	return errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) DeleteFlowInstanceRoute(context.Context, runtimeflowidentity.Route) error {
	return errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) ListFlowInstanceRoutes(context.Context) ([]runtimeflowidentity.Route, error) {
	return nil, errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) ReplaceFlowInstanceRouteRecords(context.Context, runtimeflowidentity.Route, []FlowInstanceRouteRecord) error {
	return errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) ReplaceFlowInstanceRouteTopology(context.Context, []FlowInstanceRouteRecordSet) error {
	return errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) ListFlowInstanceRouteRecords(context.Context, runtimeflowidentity.Route) ([]FlowInstanceRouteRecord, error) {
	return nil, errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) RollbackFlowInstanceRoute(context.Context, runtimeflowidentity.Route) error {
	return errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) ListActiveAgentDescriptors(context.Context) ([]ActiveAgentDescriptor, error) {
	return nil, nil
}
func (unexpectedDurableTestRoles) ListActiveFlowInstanceDescriptors(context.Context) ([]ActiveFlowInstanceDescriptor, error) {
	return nil, nil
}
func (unexpectedDurableTestRoles) ListSelectedRunTargetOwners(context.Context) ([]ActiveTargetDescriptor, error) {
	return nil, nil
}
func (unexpectedDurableTestRoles) LoadWorkflowInstance(context.Context, runtimeflowidentity.Route) (runtimepipeline.WorkflowInstance, bool, error) {
	return runtimepipeline.WorkflowInstance{}, false, errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) ListWorkflowInstances(context.Context) ([]runtimepipeline.WorkflowInstance, error) {
	return nil, errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) SelectActiveWorkflowInstances(context.Context, string, []runtimepipeline.WorkflowInstanceFieldSelector, []string) ([]runtimepipeline.WorkflowInstance, error) {
	return nil, errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) LoadWorkflowEntityState(context.Context, runtimeflowidentity.Route, runtimeidentity.EntityID) (runtimepipeline.WorkflowEntityStatePersistenceRecord, bool, error) {
	return runtimepipeline.WorkflowEntityStatePersistenceRecord{}, false, errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) SelectActiveWorkflowEntityStates(context.Context, string, []runtimepipeline.WorkflowInstanceFieldSelector, []string) ([]runtimepipeline.WorkflowEntityStatePersistenceRecord, error) {
	return nil, errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) LoadPreparedPublishEvent(context.Context, string) (PreparedPublishEvent, bool, error) {
	return PreparedPublishEvent{}, false, errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) RecordDeadLetter(context.Context, runtimedeadletters.Record) error {
	return errUnexpectedDurableTestRole
}
func (unexpectedDurableTestRoles) LoadRunOrigin(context.Context, string) (runtimerunlifecycle.RunOrigin, error) {
	return runtimerunlifecycle.ScenarioSetupRunOrigin(), nil
}

// ExactDurableTestDependencies projects synthetic test stores into the same
// explicit role graph used by production constructors. Missing roles fail if
// exercised; they are never inferred by production code.
func ExactDurableTestDependencies(selected any) DurableDependencies {
	defaults := unexpectedDurableTestRoles{}
	deps := DurableTestDependencyProjection(selected)
	if deps.ReplyContext == nil {
		deps.ReplyContext = defaults
	}
	if deps.RunLifecycle == nil {
		deps.RunLifecycle = defaults
	}
	if deps.DeliveryLifecycle == nil {
		deps.DeliveryLifecycle = defaults
	}
	if deps.FlowRoutes == nil {
		deps.FlowRoutes = defaults
	}
	if deps.FlowRouteRecords == nil {
		deps.FlowRouteRecords = defaults
	}
	if deps.FlowRouteSets == nil {
		deps.FlowRouteSets = defaults
	}
	if deps.FlowRouteTopology == nil {
		deps.FlowRouteTopology = defaults
	}
	if deps.FlowRouteRollback == nil {
		deps.FlowRouteRollback = defaults
	}
	if deps.ActiveAgents == nil {
		deps.ActiveAgents = defaults
	}
	if deps.ActiveFlows == nil {
		deps.ActiveFlows = defaults
	}
	if deps.TargetOwners == nil {
		deps.TargetOwners = defaults
	}
	if deps.WorkflowInstances == nil {
		deps.WorkflowInstances = defaults
	}
	if deps.PreparedEvents == nil {
		deps.PreparedEvents = defaults
	}
	if deps.TargetFailureRecorder == nil {
		deps.TargetFailureRecorder = defaults
	}
	if deps.RunOrigins == nil {
		deps.RunOrigins = defaults
	}
	return deps
}

// DurableTestDependencyProjection exposes only roles explicitly implemented by
// a synthetic store. Ephemeral fixtures use it without installing defaults.
func DurableTestDependencyProjection(selected any) DurableDependencies {
	var deps DurableDependencies
	if role, ok := selected.(runtimereplycontext.Store); ok {
		deps.ReplyContext = role
	}
	if role, ok := selected.(runtimerunlifecycle.OperationOwner); ok {
		deps.RunLifecycle = role
	}
	if role, ok := selected.(runtimedelivery.Store); ok {
		deps.DeliveryLifecycle = role
	}
	if role, ok := selected.(FlowInstanceRoutePersistence); ok {
		deps.FlowRoutes = role
	}
	if role, ok := selected.(FlowInstanceRouteRecordReader); ok {
		deps.FlowRouteRecords = role
	}
	if role, ok := selected.(FlowInstanceRouteSetPersistence); ok {
		deps.FlowRouteSets = role
	}
	if role, ok := selected.(FlowInstanceRouteTopologyPersistence); ok {
		deps.FlowRouteTopology = role
	}
	if role, ok := selected.(FlowInstanceRouteRollbackPersistence); ok {
		deps.FlowRouteRollback = role
	}
	if role, ok := selected.(ActiveAgentDescriptorLister); ok {
		deps.ActiveAgents = role
	}
	if role, ok := selected.(ActiveFlowInstanceDescriptorLister); ok {
		deps.ActiveFlows = role
	}
	if role, ok := selected.(SelectedRunTargetOwnerLister); ok {
		deps.TargetOwners = role
	}
	if role, ok := selected.(runtimepipeline.WorkflowInstancePersistenceReader); ok {
		deps.WorkflowInstances = role
	}
	if role, ok := selected.(PreparedPublishEventReader); ok {
		deps.PreparedEvents = role
	}
	if role, ok := selected.(TargetFailureDeadLetterRecorder); ok {
		deps.TargetFailureRecorder = role
	}
	if role, ok := selected.(RunOriginReader); ok {
		deps.RunOrigins = role
	}
	return deps
}

func TestDurableDependenciesRequireWorkflowInstanceStateReaderAtConstruction(t *testing.T) {
	roles := unexpectedDurableTestRoles{}
	store := newTargetRouteMemoryStore()
	deps := DurableDependencies{
		RunLifecycle: roles, DeliveryLifecycle: roles, FlowRoutes: roles, FlowRouteRecords: roles,
		FlowRouteSets: roles, FlowRouteTopology: roles, FlowRouteRollback: roles,
		ActiveAgents: roles, ActiveFlows: roles, TargetOwners: roles,
		PreparedEvents: roles, TargetFailureRecorder: roles, RunOrigins: roles,
	}
	opts := EventBusOptions{
		BundleSourceFact:    authorActivityTestBundleSourceFact,
		Durable:             deps,
		ExecutionPosture:    executionposture.Live,
		PipelineObligations: store.PipelineObligations(),
		ReceiverExecution:   eventreceiver.NormalExecution(),
	}
	if _, err := NewEventBusWithOptions(store, opts); err == nil || !strings.Contains(err.Error(), "workflow instance/state reader") {
		t.Fatalf("missing workflow reader validation error = %v", err)
	}
	opts.Durable.WorkflowInstances = roles
	if _, err := NewEventBusWithOptions(store, opts); err != nil {
		t.Fatalf("construct with complete durable dependencies: %v", err)
	}
}
