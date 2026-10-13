package genericschedule

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/google/uuid"
)

// JoinContinuation distinguishes an admitted pending obligation from a physical
// source publication. Neither variant grants current execution authority.
type JoinContinuation struct {
	source   AdmissionCommand
	original events.Event
	child    AdmissionCommand
	cause    joinruntime.CommittedPublicationCause
	pending  *TransferredJoinOccurrence
}

// ProjectPublishedJoinContinuation consumes the caller's canonical entity and
// generation correspondence; it does not reconstruct either correspondence.
func ProjectPublishedJoinContinuation(source Activation, original events.Event, child AdmissionCommand) (JoinContinuation, error) {
	if _, err := source.ValidatePublishedOccurrence(original); err != nil {
		return JoinContinuation{}, err
	}
	if source.ClockSuspension != nil {
		return JoinContinuation{}, fmt.Errorf("published join continuation cannot carry clock authority")
	}
	return projectPublishedJoinContinuation(source.Command, original, child, joinruntime.CommittedPublicationCause{RunID: original.RunID(), EventID: original.ID()})
}

func ProjectTransferredJoinContinuation(source TransferredJoinOccurrence, original events.Event, child AdmissionCommand) (JoinContinuation, error) {
	if err := source.ValidateEvent(original); err != nil {
		return JoinContinuation{}, err
	}
	return projectPublishedJoinContinuation(source.Command, original, child, source.Publication.CommittedCause)
}

// ProjectPendingJoinContinuation consumes the immediate source's retained
// obligation, not a fabricated source event. Native cut/cause admission remains
// the caller's responsibility, as for physical publication membership.
func ProjectPendingJoinContinuation(source TransferredJoinOccurrence, child AdmissionCommand) (JoinContinuation, error) {
	if err := source.Validate(); err != nil {
		return JoinContinuation{}, err
	}
	if err := validatePublishedJoinContinuationProjection(source.Command, child); err != nil {
		return JoinContinuation{}, err
	}
	return JoinContinuation{source: source.Command.Canonical(), child: child.Canonical(), cause: source.Publication.CommittedCause, pending: &source}, nil
}

func projectPublishedJoinContinuation(source AdmissionCommand, original events.Event, child AdmissionCommand, cause joinruntime.CommittedPublicationCause) (JoinContinuation, error) {
	source, child = source.Canonical(), child.Canonical()
	if err := validatePublishedJoinContinuationProjection(source, child); err != nil {
		return JoinContinuation{}, err
	}
	return JoinContinuation{source: source, original: original.Clone(), child: child, cause: cause}, nil
}

func validatePublishedJoinContinuationProjection(source, child AdmissionCommand) error {
	if err := validatePublishedJoinContinuationFrame(source, child); err != nil {
		return err
	}
	sourceRef, childRef, err := publishedJoinContinuationRefs(source, child)
	if err != nil {
		return err
	}
	if sourceRef.Mode() == timeridentity.JoinRefModeFanOutDelivery {
		return validatePublishedBarrierContinuation(source, child, sourceRef, childRef)
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
	if !sourceOK || !childOK || sourceRef.Mode() != childRef.Mode() || sourceHandle.Kind() != childHandle.Kind() {
		return timeridentity.JoinRef{}, timeridentity.JoinRef{}, fmt.Errorf("published join continuation requires the same exact join kind")
	}
	if sourceRef.Mode() == timeridentity.JoinRefModeArrival && !sourceRef.Declaration().Equal(childRef.Declaration()) {
		return timeridentity.JoinRef{}, timeridentity.JoinRef{}, fmt.Errorf("published arrival continuation requires its original declaration")
	}
	if err := validateExactJoinContinuationCommand(source, sourceHandle); err != nil {
		return timeridentity.JoinRef{}, timeridentity.JoinRef{}, err
	}
	if err := validateExactJoinContinuationCommand(child, childHandle); err != nil {
		return timeridentity.JoinRef{}, timeridentity.JoinRef{}, err
	}
	return sourceRef, childRef, nil
}

func validatePublishedBarrierContinuation(source, child AdmissionCommand, before, after timeridentity.JoinRef) error {
	original, originalOK := before.FanOutDelivery()
	projected, projectedOK := after.FanOutDelivery()
	if !originalOK || !projectedOK || before.Node() != after.Node() || before.HandlerEvent() != after.HandlerEvent() || before.JoinID() != after.JoinID() ||
		!original.DeclarationIdentity().Equal(projected.DeclarationIdentity()) || original.TriggeringDeliveryID() != projected.TriggeringDeliveryID() {
		return fmt.Errorf("published barrier continuation changes its exact intent or outcome declaration")
	}
	beforeGeneration, afterGeneration := before.Generation(), after.Generation()
	if beforeGeneration.Valid() != afterGeneration.Valid() || beforeGeneration.FlowID != afterGeneration.FlowID || beforeGeneration.LoopID != afterGeneration.LoopID ||
		beforeGeneration.RevisionField != afterGeneration.RevisionField || beforeGeneration.Attempt != afterGeneration.Attempt {
		return fmt.Errorf("published barrier continuation changes retained generation meaning")
	}
	originalSummary, err := joinContinuationBarrierSummary(source)
	if err != nil {
		return err
	}
	projectedSummary, err := joinContinuationBarrierSummary(child)
	if err != nil || originalSummary != projectedSummary {
		return fmt.Errorf("published barrier continuation changes its frozen terminal summary")
	}
	return nil
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

func (p JoinContinuation) Present() bool { return p.source.RunID != "" }

func (p JoinContinuation) Published() bool { return p.Present() && p.pending == nil }

func (p JoinContinuation) PendingSource() (TransferredJoinOccurrence, bool) {
	if !p.Present() || p.pending == nil {
		return TransferredJoinOccurrence{}, false
	}
	return *p.pending, true
}

func (p JoinContinuation) SourceRunID() string { return p.source.RunID }

func (p JoinContinuation) SourceEventID() string {
	if p.pending != nil {
		return p.pending.Publication.EventID
	}
	return p.original.ID()
}

func (p JoinContinuation) SourceEvent() events.Event {
	if !p.Published() {
		return events.Event{}
	}
	return p.original.Clone()
}

func (p JoinContinuation) ChildCommand() AdmissionCommand { return p.child }

// RetainedPublication binds the child's immediate source, without claiming that
// the child event has been durably published.
func (p JoinContinuation) RetainedPublication(authorityStamp string) (joinruntime.TransferredPublication, error) {
	if !p.Present() {
		return joinruntime.TransferredPublication{}, fmt.Errorf("published join continuation is absent")
	}
	publication := joinruntime.TransferredPublication{
		EventID:     activityidentity.ForkLineageEventID(p.child.RunID, p.SourceEventID()),
		SourceRunID: p.SourceRunID(), SourceEventID: p.SourceEventID(),
		AuthorityStamp: authorityStamp, ExecutionMode: p.child.ExecutionMode,
		CommittedCause: p.cause,
	}
	if err := (TransferredJoinOccurrence{Command: p.child, Publication: publication}).Validate(); err != nil {
		return joinruntime.TransferredPublication{}, err
	}
	return publication, nil
}

// Event projects the published cause into the child. It admits no new timer
// occurrence and deliberately keeps producer identity separate from fork authority.
func (p JoinContinuation) Event(forkEventID, authorityStamp string) (events.Event, error) {
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
func (p JoinContinuation) ValidateEvent(event events.Event, authorityStamp string) error {
	expected, err := p.Event(event.ID(), authorityStamp)
	if err != nil {
		return err
	}
	return validateTransferredJoinPublicationEvent(event, expected)
}
