package pipeline

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

// PreparedWorkflowPublicationState is the immutable prospective receiver fact
// of one prepared engine mutation. Only that mutation can discharge it at commit.
type PreparedWorkflowPublicationState struct {
	digest       [32]byte
	source       runtimecorrelation.SourceArtifactFact
	runID        string
	route        events.RouteIdentity
	availability DeliveryTargetAvailability
	fields       string
	instanceID   string
}

type preparedPublicationMutation struct {
	State     WorkflowEngineStateRecord
	Lifecycle WorkflowLifecycleMutationPlan
	Schedules []preparedPublicationSchedule
}

type preparedPublicationSchedule struct {
	Kind        WorkflowScheduleMutationKind
	CommandHash string
	CancelCause string
	CancelledAt time.Time
}

func publicationMutationBytes(record WorkflowEngineStateRecord, lifecycle WorkflowLifecycleMutationPlan) ([]byte, error) {
	projection := preparedPublicationMutation{State: record, Lifecycle: lifecycle}
	// Schedule commands own semantic payload encoding and immutable identity.
	// Bind that identity rather than serialize the command's private value model.
	projection.Lifecycle.Schedules = nil
	for _, schedule := range lifecycle.Schedules {
		if err := schedule.Validate(record.Identity.RunID); err != nil {
			return nil, err
		}
		hash, err := schedule.Command.ImmutableHash()
		if err != nil {
			return nil, err
		}
		projection.Schedules = append(projection.Schedules, preparedPublicationSchedule{
			Kind: schedule.Kind, CommandHash: hash, CancelCause: schedule.CancelCause, CancelledAt: schedule.CancelledAt,
		})
	}
	return canonicaljson.Bytes(projection)
}

func prepareWorkflowPublicationState(record WorkflowEngineStateRecord, lifecycle WorkflowLifecycleMutationPlan, flowID string, source runtimecorrelation.SourceArtifactFact) (PreparedWorkflowPublicationState, error) {
	if err := record.Validate(); err != nil {
		return PreparedWorkflowPublicationState{}, err
	}
	if err := source.Validate(); err != nil {
		return PreparedWorkflowPublicationState{}, err
	}
	if flowID == "" {
		return PreparedWorkflowPublicationState{}, fmt.Errorf("prospective publication requires exact semantic flow")
	}
	raw, err := publicationMutationBytes(record, lifecycle)
	if err != nil {
		return PreparedWorkflowPublicationState{}, err
	}
	return PreparedWorkflowPublicationState{
		digest: sha256.Sum256(raw), source: source, runID: record.Identity.RunID,
		route:        events.RouteIdentity{FlowID: flowID, FlowInstance: record.Identity.Route.InstancePath, EntityID: record.EntityID},
		availability: NewDeliveryTargetAvailability(record.CurrentState, record.Status, !record.TerminatedAt.IsZero()),
		fields:       string(record.Fields), instanceID: record.Identity.Route.InstanceID,
	}, nil
}

func (p PreparedWorkflowPublicationState) Empty() bool {
	return p == PreparedWorkflowPublicationState{}
}

func (p PreparedWorkflowPublicationState) ValidatePublication(source runtimecorrelation.SourceArtifactFact, evt events.Event) error {
	if p.Empty() || p.source.Validate() != nil || source != p.source || evt.RunID() != p.runID {
		return fmt.Errorf("prospective receiver state disagrees with publication run or admitted source")
	}
	return nil
}

func (p PreparedWorkflowPublicationState) ValidateMutation(record WorkflowEngineStateRecord, lifecycle WorkflowLifecycleMutationPlan) error {
	if p.Empty() {
		return fmt.Errorf("prospective receiver state is missing")
	}
	if err := record.Validate(); err != nil {
		return err
	}
	raw, err := publicationMutationBytes(record, lifecycle)
	if err != nil {
		return err
	}
	if sha256.Sum256(raw) != p.digest {
		return fmt.Errorf("publication prospective receiver state disagrees with committing mutation")
	}
	return nil
}

func (p PreparedWorkflowPublicationState) Candidate() DeliveryTargetOwnerCandidate {
	return DeliveryTargetOwnerCandidate{Route: p.route, Availability: p.availability}
}

func (p PreparedWorkflowPublicationState) PinRoutingDescriptor() (runtimepinrouting.Descriptor, error) {
	if p.Empty() {
		return runtimepinrouting.Descriptor{}, fmt.Errorf("prospective receiver state is missing")
	}
	var fields map[string]any
	if err := json.Unmarshal([]byte(p.fields), &fields); err != nil {
		return runtimepinrouting.Descriptor{}, err
	}
	addresses, err := runtimepinrouting.DescriptorAddressFields(fields)
	if err != nil {
		return runtimepinrouting.Descriptor{}, err
	}
	return runtimepinrouting.Descriptor{ID: p.instanceID, EntityID: p.route.EntityID, FlowInstance: p.route.FlowInstance, AddressFields: addresses}, nil
}

func (p PreparedWorkflowPublicationState) ValidateTarget(route events.RouteIdentity) error {
	if p.Empty() || route.FlowInstance != p.route.FlowInstance {
		return nil
	}
	// A blueprint constrains selection; omitted coordinates are not yet an
	// admitted owner. The receiver classifier still validates the complete target.
	if route.FlowID != "" && route.FlowID != p.route.FlowID || route.EntityID != "" && route.EntityID != p.route.EntityID {
		return fmt.Errorf("receiver target contradicts prospective flow/entity ownership: target=%+v prospective=%+v", route, p.route)
	}
	return nil
}
