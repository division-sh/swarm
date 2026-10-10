package genericschedule

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/google/uuid"
)

// PublishedJoinContinuation retains publication evidence, not fixed-cut,
// delivery, schedule admission, or current execution authority.
type PublishedJoinContinuation struct {
	source   AdmissionCommand
	original events.Event
	child    AdmissionCommand
}

// ProjectPublishedJoinContinuation consumes the caller's canonical entity and
// generation correspondence; it does not reconstruct either correspondence.
func ProjectPublishedJoinContinuation(source Activation, original events.Event, child AdmissionCommand) (PublishedJoinContinuation, error) {
	if _, err := source.ValidatePublishedOccurrence(original); err != nil {
		return PublishedJoinContinuation{}, err
	}
	if source.ClockSuspension != nil {
		return PublishedJoinContinuation{}, fmt.Errorf("published join continuation cannot carry clock authority")
	}
	return projectPublishedJoinContinuation(source.Command, original, child)
}

func ProjectTransferredJoinContinuation(source TransferredJoinOccurrence, original events.Event, child AdmissionCommand) (PublishedJoinContinuation, error) {
	if err := source.ValidateEvent(original); err != nil {
		return PublishedJoinContinuation{}, err
	}
	return projectPublishedJoinContinuation(source.Command, original, child)
}

func projectPublishedJoinContinuation(source AdmissionCommand, original events.Event, child AdmissionCommand) (PublishedJoinContinuation, error) {
	source, child = source.Canonical(), child.Canonical()
	if err := validatePublishedJoinContinuationProjection(source, child); err != nil {
		return PublishedJoinContinuation{}, err
	}
	return PublishedJoinContinuation{source: source, original: original.Clone(), child: child}, nil
}

func validatePublishedJoinContinuationProjection(source, child AdmissionCommand) error {
	if err := validatePublishedJoinContinuationFrame(source, child); err != nil {
		return err
	}
	sourceRef, childRef, err := publishedJoinContinuationRefs(source, child)
	if err != nil {
		return err
	}
	return validatePublishedJoinContinuationSemantics(source, child, sourceRef, childRef)
}

func validatePublishedJoinContinuationFrame(source, child AdmissionCommand) error {
	if err := source.Validate(); err != nil {
		return err
	}
	if err := child.Validate(); err != nil {
		return err
	}
	source, child = source.Canonical(), child.Canonical()
	for _, runID := range []string{source.RunID, child.RunID} {
		id, err := uuid.Parse(runID)
		if err != nil || id == uuid.Nil || id.String() != runID {
			return fmt.Errorf("published join continuation requires canonical source and child runs")
		}
	}
	if source.OwnerKind != OwnerSystem || child.OwnerKind != source.OwnerKind ||
		child.OwnerID != source.OwnerID || source.ReplyContext != "" || child.ReplyContext != "" ||
		child.RunID == source.RunID || source.Due.Kind != DueAbsolute ||
		child.Due.Kind != DueAbsolute || !child.Due.Absolute.Equal(source.Due.Absolute) ||
		child.ExecutionMode != source.ExecutionMode {
		return fmt.Errorf("published join continuation must preserve owner, due and mode with distinct child identity and no reply or clock authority")
	}
	return nil
}

func publishedJoinContinuationRefs(source, child AdmissionCommand) (timeridentity.JoinRef, timeridentity.JoinRef, error) {
	sourcePayload, sourceMap := source.Payload.Interface().(map[string]any)
	childPayload, childMap := child.Payload.Interface().(map[string]any)
	if !sourceMap || !childMap {
		return timeridentity.JoinRef{}, timeridentity.JoinRef{}, fmt.Errorf("published join continuation requires object handle payloads")
	}
	sourceHandle, sourceRef, sourceOK := timeridentity.ParseJoinHandle(sourcePayload)
	childHandle, childRef, childOK := timeridentity.ParseJoinHandle(childPayload)
	if !sourceOK || !childOK || sourceRef.Mode() != timeridentity.JoinRefModeArrival || childRef.Mode() != timeridentity.JoinRefModeArrival ||
		sourceHandle.Kind() != childHandle.Kind() || !sourceRef.Declaration().Equal(childRef.Declaration()) {
		return timeridentity.JoinRef{}, timeridentity.JoinRef{}, fmt.Errorf("published join continuation requires the same exact arrival declaration and handle kind")
	}
	if err := validateExactWorkflowJoinAdmissionCommand(source, sourceHandle); err != nil {
		return timeridentity.JoinRef{}, timeridentity.JoinRef{}, err
	}
	if err := validateExactWorkflowJoinAdmissionCommand(child, childHandle); err != nil {
		return timeridentity.JoinRef{}, timeridentity.JoinRef{}, err
	}
	return sourceRef, childRef, nil
}

func validatePublishedJoinContinuationSemantics(source, child AdmissionCommand, sourceRef, childRef timeridentity.JoinRef) error {
	entry, projected := sourceRef.StageEntry(), childRef.StageEntry()
	origin := entry.OriginRunID
	if origin == "" {
		origin = entry.RunID
	}
	if entry.RunID != source.RunID || entry.EntityID != source.EntityID ||
		projected.RunID != child.RunID || projected.EntityID != child.EntityID || projected.OriginRunID != origin ||
		entry.FlowScope != projected.FlowScope || entry.Stage != projected.Stage || entry.Cause != projected.Cause ||
		entry.EventID != projected.EventID || entry.OccurrenceID != projected.OccurrenceID || entry.TransitionID != projected.TransitionID {
		return fmt.Errorf("published join continuation contradicts retained stage-entry provenance")
	}
	before, after := sourceRef.Generation(), childRef.Generation()
	if before.Valid() != after.Valid() || before.FlowID != after.FlowID || before.LoopID != after.LoopID ||
		before.RevisionField != after.RevisionField || before.Attempt != after.Attempt {
		return fmt.Errorf("published join continuation changes the retained generation meaning")
	}
	return nil
}

func (p PublishedJoinContinuation) Present() bool { return p.source.RunID != "" }

func (p PublishedJoinContinuation) SourceEvent() events.Event {
	if !p.Present() {
		return events.Event{}
	}
	return p.original.Clone()
}

func (p PublishedJoinContinuation) ChildCommand() AdmissionCommand { return p.child }

// RetainedPublication binds the child's immediate source, without claiming that
// the child event has been durably published.
func (p PublishedJoinContinuation) RetainedPublication(authorityStamp string) (joinruntime.TransferredPublication, error) {
	if !p.Present() {
		return joinruntime.TransferredPublication{}, fmt.Errorf("published join continuation is absent")
	}
	publication := joinruntime.TransferredPublication{
		EventID:     activityidentity.ForkLineageEventID(p.child.RunID, p.original.ID()),
		SourceRunID: p.original.RunID(), SourceEventID: p.original.ID(),
		AuthorityStamp: authorityStamp, ExecutionMode: p.original.ExecutionMode(),
	}
	if err := (TransferredJoinOccurrence{Command: p.child, Publication: publication}).Validate(); err != nil {
		return joinruntime.TransferredPublication{}, err
	}
	return publication, nil
}

// Event projects the published cause into the child. It admits no new timer
// occurrence and deliberately keeps producer identity separate from fork authority.
func (p PublishedJoinContinuation) Event(forkEventID, authorityStamp string) (events.Event, error) {
	publication, err := p.RetainedPublication(authorityStamp)
	if err != nil {
		return events.Event{}, err
	}
	if forkEventID != publication.EventID {
		return events.Event{}, fmt.Errorf("published join continuation requires its deterministic child event")
	}
	return (TransferredJoinOccurrence{Command: p.child, Publication: publication}).Event()
}

// ValidateEvent checks the sealed child publication frame. Prepared targets,
// payload schema admission, and delivery authority remain separate evidence.
func (p PublishedJoinContinuation) ValidateEvent(event events.Event, authorityStamp string) error {
	expected, err := p.Event(event.ID(), authorityStamp)
	if err != nil {
		return err
	}
	return validateTransferredJoinPublicationEvent(event, expected)
}
