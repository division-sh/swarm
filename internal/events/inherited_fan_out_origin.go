package events

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/identity"
)

// InheritedFanOutOrigin names a newly executed ordinal from an inherited intent.
// It is neither an ordinary causal parent nor a replay of its immutable trigger.
// Construction supplies a claim for admission; only the named chunk transaction
// can prove it against the locked intent and commit its outcome relation.
type InheritedFanOutOrigin struct {
	runID                string
	sourceRunID          string
	triggerEventID       string
	triggeringDeliveryID string
	declaration          identity.DeclarationIdentity
	bundleHash           string
	semanticDigest       string
	ordinal              int
}

func NewInheritedFanOutOrigin(runID, sourceRunID, triggerEventID, triggeringDeliveryID string, declaration identity.DeclarationIdentity, bundleHash, semanticDigest string, ordinal int) (InheritedFanOutOrigin, error) {
	origin := InheritedFanOutOrigin{runID, sourceRunID, triggerEventID, triggeringDeliveryID, declaration, bundleHash, semanticDigest, ordinal}
	if err := origin.Validate(); err != nil {
		return InheritedFanOutOrigin{}, err
	}
	return origin, nil
}

func (o InheritedFanOutOrigin) Validate() error {
	for name, value := range map[string]string{"run_id": o.runID, "source_run_id": o.sourceRunID, "trigger_event_id": o.triggerEventID, "triggering_delivery_id": o.triggeringDeliveryID} {
		if value == "" || value != strings.TrimSpace(value) {
			return fmt.Errorf("inherited fan-out origin requires canonical %s", name)
		}
		if err := validateOptionalUUID(name, value); err != nil {
			return err
		}
	}
	if o.runID == o.sourceRunID || !o.declaration.Valid() || o.declaration.Family() != "fan_out" || o.ordinal < 0 ||
		o.bundleHash == "" || o.bundleHash != strings.TrimSpace(o.bundleHash) || o.semanticDigest == "" || o.semanticDigest != strings.TrimSpace(o.semanticDigest) {
		return fmt.Errorf("inherited fan-out origin requires distinct runs and exact declaration, plan and ordinal")
	}
	return nil
}

func (o InheritedFanOutOrigin) RunID() string                             { return o.runID }
func (o InheritedFanOutOrigin) SourceRunID() string                       { return o.sourceRunID }
func (o InheritedFanOutOrigin) TriggerEventID() string                    { return o.triggerEventID }
func (o InheritedFanOutOrigin) TriggeringDeliveryID() string              { return o.triggeringDeliveryID }
func (o InheritedFanOutOrigin) Declaration() identity.DeclarationIdentity { return o.declaration }
func (o InheritedFanOutOrigin) BundleHash() string                        { return o.bundleHash }
func (o InheritedFanOutOrigin) SemanticDigest() string                    { return o.semanticDigest }
func (o InheritedFanOutOrigin) Ordinal() int                              { return o.ordinal }

func (o InheritedFanOutOrigin) MarshalJSON() ([]byte, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		RunID                string                       `json:"run_id"`
		SourceRunID          string                       `json:"source_run_id"`
		TriggerEventID       string                       `json:"trigger_event_id"`
		TriggeringDeliveryID string                       `json:"triggering_delivery_id"`
		Declaration          identity.DeclarationIdentity `json:"declaration"`
		BundleHash           string                       `json:"bundle_hash"`
		SemanticDigest       string                       `json:"semantic_digest"`
		Ordinal              int                          `json:"ordinal"`
	}{o.runID, o.sourceRunID, o.triggerEventID, o.triggeringDeliveryID, o.declaration, o.bundleHash, o.semanticDigest, o.ordinal})
}

type InheritedFanOutEventInput struct {
	Facts  EventFacts
	Origin InheritedFanOutOrigin
}

func NewInheritedFanOutEvent(input InheritedFanOutEventInput) (Event, error) {
	if err := input.Origin.Validate(); err != nil {
		return Event{}, err
	}
	input.Facts.inheritedFanOut = &input.Origin
	return newSemanticEvent(EventAdmissionInheritedFanOut, "", input.Facts, input.Origin.RunID(), "", nil, nil)
}

func (e Event) InheritedFanOutOrigin() (InheritedFanOutOrigin, bool) {
	if e.inheritedFanOut == nil {
		return InheritedFanOutOrigin{}, false
	}
	return *e.inheritedFanOut, true
}
