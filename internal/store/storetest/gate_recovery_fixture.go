package storetest

import (
	"context"
	"time"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type GateDuplicateMutationDiagnosticStorage = private.GateDuplicateMutationDiagnosticStorage

func ReadGateDuplicateMutationDiagnostics(ctx context.Context, selected any, eventID string) ([]GateDuplicateMutationDiagnosticStorage, error) {
	return private.ReadGateDuplicateMutationDiagnosticsForTest(ctx, selected, eventID)
}

func ReadGateRunRootFieldsStorage(ctx context.Context, selected any, runID string) ([]byte, error) {
	return private.ReadGateRunRootFieldsStorageForTest(ctx, selected, runID)
}

func CountGateRunProposedEffectContinuations(ctx context.Context, selected any, run string) (int, error) {
	return private.CountGateRunProposedEffectContinuationsForTest(ctx, selected, run)
}

func CountA2MembershipRefusalEvents(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountA2MembershipRefusalEventsForTest(ctx, selected, runID)
}

func SetA2ActivationJoinScheduleFailure(ctx context.Context, selected any, enabled bool) error {
	return private.SetA2ActivationJoinScheduleFailureForTest(ctx, selected, enabled)
}

func SetA2JoinCloseScheduleFailure(ctx context.Context, selected any, enabled bool) error {
	return private.SetA2JoinCloseScheduleFailureForTest(ctx, selected, enabled)
}

func SetA2AccumulatorPublicationFailure(ctx context.Context, selected any, enabled bool) error {
	return private.SetA2AccumulatorPublicationFailureForTest(ctx, selected, enabled)
}

func ReadGateNodeDeliveryStatus(ctx context.Context, selected any, event, node string) (string, error) {
	return private.ReadGateNodeDeliveryStatusForTest(ctx, selected, event, node)
}

func CountRunNonterminalDeliveryStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountRunNonterminalDeliveryStorageForTest(ctx, selected, runID)
}

type CanonicalAccumulatorClaimStorage = private.CanonicalAccumulatorClaimStorage
type CanonicalAccumulatorRefusalStorage = private.CanonicalAccumulatorRefusalStorage
type ReceiverAppearanceSettlementStorage = private.ReceiverAppearanceSettlementStorage

func ReadCanonicalAccumulatorClaimStorage(ctx context.Context, selected any, eventID string) (CanonicalAccumulatorClaimStorage, error) {
	return private.ReadCanonicalAccumulatorClaimStorageForTest(ctx, selected, eventID)
}

func ReadCanonicalAccumulatorRefusalStorage(ctx context.Context, selected any, eventID string) (CanonicalAccumulatorRefusalStorage, error) {
	return private.ReadCanonicalAccumulatorRefusalStorageForTest(ctx, selected, eventID)
}

func ReadReceiverAppearanceSettlementStorage(ctx context.Context, selected any, eventID string) (ReceiverAppearanceSettlementStorage, error) {
	return private.ReadReceiverAppearanceSettlementStorageForTest(ctx, selected, eventID)
}

func ReadGateEventFailure(ctx context.Context, selected any, eventID string) (string, error) {
	return private.ReadGateEventFailureForTest(ctx, selected, eventID)
}

func ReadGateSubscriberFailure(ctx context.Context, selected any, eventID, subscriberID string) (string, error) {
	return private.ReadGateSubscriberFailureForTest(ctx, selected, eventID, subscriberID)
}

func ReadGateNodeFailure(ctx context.Context, selected any, eventID, nodeID string) (string, error) {
	return private.ReadGateNodeFailureForTest(ctx, selected, eventID, nodeID)
}

type WorkflowTimerNodeFailureStorage = private.WorkflowTimerNodeFailureStorage
type WorkflowTimerDeliveryTraceStorage = private.WorkflowTimerDeliveryTraceStorage

type ExactJoinNodeDeliveryStorage = private.ExactJoinNodeDeliveryStorage

func ReadExactJoinNodeDeliveryStorage(ctx context.Context, selected any, eventID, subscriberKey string) (ExactJoinNodeDeliveryStorage, error) {
	return private.ReadExactJoinNodeDeliveryStorageForTest(ctx, selected, eventID, subscriberKey)
}

func ReadWorkflowTimerNodeFailure(ctx context.Context, selected any, eventID, nodeID string) (WorkflowTimerNodeFailureStorage, error) {
	return private.ReadWorkflowTimerNodeFailureForTest(ctx, selected, eventID, nodeID)
}

func ReadWorkflowTimerDeliveryTrace(ctx context.Context, selected any, runID string) (WorkflowTimerDeliveryTraceStorage, error) {
	return private.ReadWorkflowTimerDeliveryTraceForTest(ctx, selected, runID)
}

func ReadWorkflowTimerEventIdentity(ctx context.Context, selected any, runID, eventName string) (string, error) {
	return private.ReadWorkflowTimerEventIdentityForTest(ctx, selected, runID, eventName)
}

func ReadLatestWorkflowTimerRuntimeLogText(ctx context.Context, selected any) (string, error) {
	return private.ReadLatestWorkflowTimerRuntimeLogTextForTest(ctx, selected)
}

func ReadGateSubscriberFailureOrEmptyObject(ctx context.Context, selected any, eventID, subscriberID string) (string, error) {
	return private.ReadGateSubscriberFailureOrEmptyObjectForTest(ctx, selected, eventID, subscriberID)
}

func ReadGateDeadLetterFailure(ctx context.Context, selected any, eventID string) (string, error) {
	return private.ReadGateDeadLetterFailureForTest(ctx, selected, eventID)
}

func CountGateEventSubscriberDeliveries(ctx context.Context, selected any, event, subscriber string) (int, error) {
	return private.CountGateEventSubscriberDeliveriesForTest(ctx, selected, event, subscriber)
}

func ReadGateLatestDeadLetterFailure(ctx context.Context, selected any, event string) (string, error) {
	return private.ReadGateLatestDeadLetterFailureForTest(ctx, selected, event)
}

func CountGateSourceNamedEvents(ctx context.Context, selected any, name, source string) (int, error) {
	return private.CountGateSourceNamedEventsForTest(ctx, selected, name, source)
}

type A2WorkerOutputStorage = private.A2WorkerOutputStorage

func ReadA2WorkerOutputs(ctx context.Context, selected any, runID, sourceEventID string) (A2WorkerOutputStorage, error) {
	return private.ReadA2WorkerOutputsForTest(ctx, selected, runID, sourceEventID)
}

func CountSourceEventPublications(ctx context.Context, selected any, sourceEventID string) (int, error) {
	return private.CountSourceEventPublicationsForTest(ctx, selected, sourceEventID)
}

func ReadSourceEventPublicationIdentity(ctx context.Context, selected any, sourceEventID string) (string, error) {
	return private.ReadSourceEventPublicationIdentityForTest(ctx, selected, sourceEventID)
}

func ReadSourceNamedChildEventIDs(ctx context.Context, selected any, parentID, name string) ([]string, error) {
	return private.ReadSourceNamedChildEventIDsForTest(ctx, selected, parentID, name)
}

type A2EarlyRefusalStorage = private.A2EarlyRefusalStorage

func ReadA2EarlyRefusal(ctx context.Context, selected any, eventID, nodeID string) (A2EarlyRefusalStorage, error) {
	return private.ReadA2EarlyRefusalForTest(ctx, selected, eventID, nodeID)
}

func ReadGateNodeDeliveryIdentity(ctx context.Context, selected any, eventID, nodeID string) (string, error) {
	return private.ReadGateNodeDeliveryIdentityForTest(ctx, selected, eventID, nodeID)
}

func CountGateNodeSettledAttempts(ctx context.Context, selected any, eventID, nodeID string) (int, error) {
	return private.CountGateNodeSettledAttemptsForTest(ctx, selected, eventID, nodeID)
}

func CountGateEventNodeDeliveries(ctx context.Context, selected any, eventID string) (int, error) {
	return private.CountGateEventNodeDeliveriesForTest(ctx, selected, eventID)
}

func ReadGateEventNodeDeliveryIdentity(ctx context.Context, selected any, eventID string) (string, error) {
	return private.ReadGateEventNodeDeliveryIdentityForTest(ctx, selected, eventID)
}

func CountGateEventDeliveredRows(ctx context.Context, selected any, eventID string) (int, error) {
	return private.CountGateEventDeliveredRowsForTest(ctx, selected, eventID)
}

func ReadGateRunEventNames(ctx context.Context, selected any, run string) ([]string, error) {
	return private.ReadGateRunEventNamesForTest(ctx, selected, run)
}

func InstallGateProposedEffectCreationFailure(ctx context.Context, selected any) error {
	return private.InstallGateProposedEffectCreationFailureForTest(ctx, selected)
}

func InstallProspectivePublicationCreationFailure(ctx context.Context, selected any) error {
	return private.InstallProspectivePublicationCreationFailureForTest(ctx, selected)
}

func SetGateDecisionRouteAttemptFault(ctx context.Context, selected any, event string, attempt int, due time.Time) error {
	return private.SetGateDecisionRouteAttemptFaultForTest(ctx, selected, event, attempt, due)
}

func SetGateDecisionRouteDueFault(ctx context.Context, selected any, event string, due time.Time) error {
	return private.SetGateDecisionRouteDueFaultForTest(ctx, selected, event, due)
}
