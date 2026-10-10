package genericschedule

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/google/uuid"
)

// PublishedJoinContinuation retains publication evidence, not fixed-cut,
// delivery, schedule admission, or current execution authority.
type PublishedJoinContinuation struct {
	source   Activation
	original events.Event
	child    AdmissionCommand
}

// ProjectPublishedJoinContinuation consumes the caller's canonical entity and
// generation correspondence; it does not reconstruct either correspondence.
func ProjectPublishedJoinContinuation(source Activation, original events.Event, child AdmissionCommand) (PublishedJoinContinuation, error) {
	if err := validatePublishedJoinContinuationFrame(source, original, child); err != nil {
		return PublishedJoinContinuation{}, err
	}
	source, child = source.Canonical(), child.Canonical()
	sourceRef, childRef, err := publishedJoinContinuationRefs(source.Command, child)
	if err != nil {
		return PublishedJoinContinuation{}, err
	}
	if err := validatePublishedJoinContinuationSemantics(source.Command, child, sourceRef, childRef); err != nil {
		return PublishedJoinContinuation{}, err
	}
	return PublishedJoinContinuation{source: source, original: original.Clone(), child: child}, nil
}

func validatePublishedJoinContinuationFrame(source Activation, original events.Event, child AdmissionCommand) error {
	if _, err := source.ValidatePublishedOccurrence(original); err != nil {
		return err
	}
	if err := child.Validate(); err != nil {
		return err
	}
	source, child = source.Canonical(), child.Canonical()
	for _, runID := range []string{source.Command.RunID, child.RunID} {
		id, err := uuid.Parse(runID)
		if err != nil || id == uuid.Nil || id.String() != runID {
			return fmt.Errorf("published join continuation requires canonical source and child runs")
		}
	}
	if source.Command.OwnerKind != OwnerSystem || child.OwnerKind != source.Command.OwnerKind ||
		child.OwnerID != source.Command.OwnerID || source.ClockSuspension != nil ||
		source.Command.ReplyContext != "" || child.ReplyContext != "" ||
		child.RunID == source.Command.RunID ||
		child.Due.Kind != DueAbsolute || !child.Due.Absolute.Equal(source.CurrentDueAt) ||
		child.ExecutionMode != source.Command.ExecutionMode {
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

func (p PublishedJoinContinuation) Present() bool { return p.source.ID != "" }

func (p PublishedJoinContinuation) SourceEvent() events.Event {
	if !p.Present() {
		return events.Event{}
	}
	return p.original.Clone()
}

func (p PublishedJoinContinuation) ChildCommand() AdmissionCommand { return p.child }

// Event projects the published cause into the child. It admits no new timer
// occurrence and deliberately keeps producer identity separate from fork authority.
func (p PublishedJoinContinuation) Event(forkEventID, authorityStamp string) (events.Event, error) {
	if !p.Present() || forkEventID != activityidentity.ForkLineageEventID(p.child.RunID, p.original.ID()) ||
		strings.TrimSpace(authorityStamp) == "" {
		return events.Event{}, fmt.Errorf("published join continuation requires its deterministic child event and selection authority stamp")
	}
	lineage, err := events.NewSelectedForkLineage(p.child.RunID, p.source.Command.RunID, p.original.ID(), authorityStamp, p.child.TaskID, p.child.ExecutionMode)
	if err != nil {
		return events.Event{}, err
	}
	// Reuse the generic occurrence's numeric and envelope emission projection,
	// without constructing or admitting a child activation row.
	projected, err := occurrencePublicationEvent(Activation{Command: p.child}, Occurrence{EventID: forkEventID, DueAt: p.original.CreatedAt()})
	if err != nil {
		return events.Event{}, err
	}
	return events.NewSelectedForkReplayEvent(events.SelectedForkReplayEventInput{
		Facts: events.EventFacts{
			ID: forkEventID, Type: projected.Type(),
			Producer: events.ProducerClaim{Type: p.original.Producer().Type(), ID: p.original.Producer().ID()},
			TaskID:   p.child.TaskID, Payload: projected.Payload(), Envelope: projected.Envelope(),
			RoutingSource: p.child.RoutingSource, CreatedAt: p.original.CreatedAt(), ExecutionMode: p.child.ExecutionMode,
		},
		Lineage: lineage,
	})
}

// ValidateEvent checks the sealed child publication frame. Prepared targets,
// payload schema admission, and delivery authority remain separate evidence.
func (p PublishedJoinContinuation) ValidateEvent(event events.Event, authorityStamp string) error {
	expected, err := p.Event(event.ID(), authorityStamp)
	if err != nil {
		return err
	}
	actualLineage, found := event.SelectedForkLineage()
	expectedLineage, _ := expected.SelectedForkLineage()
	if !found || actualLineage.DestinationRunID() != expectedLineage.DestinationRunID() ||
		actualLineage.SourceRunID() != expectedLineage.SourceRunID() || actualLineage.SourceEventID() != expectedLineage.SourceEventID() ||
		actualLineage.AuthorityStamp() != expectedLineage.AuthorityStamp() || actualLineage.TaskID() != expectedLineage.TaskID() ||
		actualLineage.ExecutionMode() != expectedLineage.ExecutionMode() {
		return fmt.Errorf("published join continuation event contradicts its exact selected lineage")
	}
	return validateOccurrencePublicationEvent(event, expected)
}
