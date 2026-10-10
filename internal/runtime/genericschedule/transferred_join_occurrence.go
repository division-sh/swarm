package genericschedule

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
)

// TransferredJoinOccurrence is non-executable retained arrival evidence. It
// neither admits a schedule nor proves historical event or delivery membership.
type TransferredJoinOccurrence struct {
	Command     AdmissionCommand
	Publication joinruntime.TransferredPublication
}

func NewTransferredJoinOccurrence(join joinruntime.Activation, record joinruntime.TransferredPublication) (TransferredJoinOccurrence, error) {
	if join.TransferredPublication != nil && *join.TransferredPublication != record {
		return TransferredJoinOccurrence{}, fmt.Errorf("transferred occurrence contradicts its retained arrival publication")
	}
	command, err := WorkflowJoinAdmission(join, record.ExecutionMode)
	if err != nil {
		return TransferredJoinOccurrence{}, err
	}
	occurrence := TransferredJoinOccurrence{Command: command.Canonical(), Publication: record}
	if err := occurrence.Validate(); err != nil {
		return TransferredJoinOccurrence{}, err
	}
	return occurrence, nil
}

func (o TransferredJoinOccurrence) Validate() error {
	if err := o.Command.Validate(); err != nil {
		return err
	}
	command := o.Command.Canonical()
	payload, object := command.Payload.Interface().(map[string]any)
	if !object {
		return fmt.Errorf("transferred occurrence requires an object arrival handle")
	}
	handle, ref, valid := timeridentity.ParseJoinHandle(payload)
	if !valid || ref.Mode() != timeridentity.JoinRefModeArrival || command.OwnerKind != OwnerSystem ||
		command.Due.Kind != DueAbsolute || command.ReplyContext != "" || command.ExecutionMode != o.Publication.ExecutionMode {
		return fmt.Errorf("transferred occurrence requires exact one-shot arrival owner, due and mode without reply authority")
	}
	if err := o.Publication.Validate(ref); err != nil {
		return err
	}
	return validateExactWorkflowJoinAdmissionCommand(command, handle)
}

// Project consumes correspondence only. It does not assert that the source
// publication exists; ProjectTransferredJoinContinuation requires that event.
func (o TransferredJoinOccurrence) Project(child AdmissionCommand, authorityStamp string) (TransferredJoinOccurrence, error) {
	if err := o.Validate(); err != nil {
		return TransferredJoinOccurrence{}, err
	}
	source, child := o.Command.Canonical(), child.Canonical()
	if err := validatePublishedJoinContinuationProjection(source, child); err != nil {
		return TransferredJoinOccurrence{}, err
	}
	projected := TransferredJoinOccurrence{Command: child, Publication: joinruntime.TransferredPublication{
		EventID:     activityidentity.ForkLineageEventID(child.RunID, o.Publication.EventID),
		SourceRunID: source.RunID, SourceEventID: o.Publication.EventID,
		AuthorityStamp: authorityStamp, ExecutionMode: source.ExecutionMode,
	}}
	if err := projected.Validate(); err != nil {
		return TransferredJoinOccurrence{}, err
	}
	return projected, nil
}

// Event returns the expected immutable frame, without schema admission,
// historical membership, prepared targets, or delivery/execution authority.
func (o TransferredJoinOccurrence) Event() (events.Event, error) {
	if err := o.Validate(); err != nil {
		return events.Event{}, err
	}
	command := o.Command.Canonical()
	lineage, err := events.NewSelectedForkLineage(command.RunID, o.Publication.SourceRunID, o.Publication.SourceEventID,
		o.Publication.AuthorityStamp, command.TaskID, command.ExecutionMode)
	if err != nil {
		return events.Event{}, err
	}
	payload, err := occurrencePublicationPayload(command)
	if err != nil {
		return events.Event{}, err
	}
	return events.NewSelectedForkReplayEvent(events.SelectedForkReplayEventInput{
		Facts:   occurrenceEventFacts(command, o.Publication.EventID, command.Due.Absolute, payload),
		Lineage: lineage,
	})
}

func (o TransferredJoinOccurrence) ValidateEvent(event events.Event) error {
	expected, err := o.Event()
	if err != nil {
		return err
	}
	return validateTransferredJoinPublicationEvent(event, expected)
}

func (o TransferredJoinOccurrence) EvidenceDigest() (string, error) {
	if err := o.Validate(); err != nil {
		return "", err
	}
	command, err := o.Command.ImmutableHash()
	if err != nil {
		return "", err
	}
	return canonicaljson.Hash(struct {
		Kind        string
		Command     string
		Publication joinruntime.TransferredPublication
	}{"transferred_join_occurrence", command, o.Publication})
}

func validateTransferredJoinPublicationEvent(actual, expected events.Event) error {
	actualLineage, found := actual.SelectedForkLineage()
	expectedLineage, _ := expected.SelectedForkLineage()
	if !found || actualLineage.DestinationRunID() != expectedLineage.DestinationRunID() ||
		actualLineage.SourceRunID() != expectedLineage.SourceRunID() || actualLineage.SourceEventID() != expectedLineage.SourceEventID() ||
		actualLineage.AuthorityStamp() != expectedLineage.AuthorityStamp() || actualLineage.TaskID() != expectedLineage.TaskID() ||
		actualLineage.ExecutionMode() != expectedLineage.ExecutionMode() {
		return fmt.Errorf("published join continuation event contradicts its exact selected lineage")
	}
	return validateOccurrencePublicationEvent(actual, expected)
}
