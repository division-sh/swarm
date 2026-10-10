package genericschedule

import (
	"bytes"
	"errors"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

func occurrencePublicationEvent(activation Activation, occurrence Occurrence) (events.Event, error) {
	projected, err := workflowexpr.ProjectSemanticValue(activation.Command.Payload)
	if err != nil {
		return events.Event{}, err
	}
	payload, err := canonicaljson.MarshalPreservingNumberKinds(projected)
	if err != nil {
		return events.Event{}, err
	}
	return occurrenceEvent(activation, occurrence, payload)
}

// ValidatePublishedOccurrence proves retained publication evidence, never a
// current declaration, execution grant, target route, or delivery settlement.
func (a Activation) ValidatePublishedOccurrence(event events.Event) (Occurrence, error) {
	if err := a.Validate(); err != nil {
		return Occurrence{}, err
	}
	a = a.Canonical()
	occurrence, err := a.publishedOccurrence()
	if err != nil {
		return Occurrence{}, err
	}
	expected, err := occurrencePublicationEvent(a, occurrence)
	if err != nil {
		return Occurrence{}, err
	}
	if err := validateOccurrencePublicationEvent(event, expected); err != nil {
		return Occurrence{}, err
	}
	return occurrence, nil
}

func (a Activation) publishedOccurrence() (Occurrence, error) {
	// Recurrence clears the accepted occurrence and advances CurrentDueAt. Its
	// retained fired/accepted pair cannot prove publication of the current due.
	if a.Command.Due.Recurring() || a.Status != StatusFired || a.CurrentEventID == "" ||
		a.CurrentEventAdmittedAt.IsZero() || a.FiredAt.IsZero() || a.AcceptedAt.IsZero() {
		return Occurrence{}, errors.New("generic schedule publication requires its durably accepted retained one-shot occurrence")
	}
	occurrence := Occurrence{ActivationID: a.ID, DueAt: a.CurrentDueAt,
		EventID: a.CurrentEventID, AdmittedAt: a.CurrentEventAdmittedAt}
	if err := occurrence.Validate(); err != nil {
		return Occurrence{}, err
	}
	if occurrence.AdmittedAt.Before(occurrence.DueAt) {
		return Occurrence{}, errors.New("generic schedule publication admission precedes its retained due coordinate")
	}
	return occurrence, nil
}

func validateOccurrencePublicationEvent(actual, expected events.Event) error {
	if actual.ID() != expected.ID() || actual.AdmissionClass() != expected.AdmissionClass() ||
		actual.RunID() != expected.RunID() || actual.ParentEventID() != expected.ParentEventID() ||
		actual.ExecutionMode() != expected.ExecutionMode() || actual.Type() != expected.Type() ||
		!actual.Producer().Equal(expected.Producer()) || actual.RoutingSource() != expected.RoutingSource() ||
		actual.TaskID() != expected.TaskID() || actual.ChainDepth() != expected.ChainDepth() ||
		!actual.CreatedAt().Equal(expected.CreatedAt()) {
		return errors.New("generic schedule publication does not match its exact retained event facts")
	}
	// Prepared durable routes own targets and their derived envelope fields.
	// Only the immutable emission source belongs to this evidence owner.
	if !events.SameRouteIdentity(actual.Envelope().Source, expected.Envelope().Source) {
		return errors.New("generic schedule publication does not match its exact retained envelope source")
	}
	var payload any
	if _, err := canonicaljson.Decode(actual.Payload()); err != nil {
		return err
	}
	if err := canonicaljson.DecodePreservingNumberLexemes(actual.Payload(), &payload); err != nil {
		return err
	}
	raw, err := canonicaljson.MarshalPreservingNumberKinds(payload)
	if err != nil {
		return err
	}
	if !bytes.Equal(raw, expected.Payload()) {
		return errors.New("generic schedule publication does not match its exact projected payload")
	}
	return nil
}
