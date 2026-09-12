package eventrecord

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/store/internal/backend/fanoutorigin"
)

func (r Record) ValidateInheritedFanOutOwner(ctx context.Context, q fanoutorigin.Queryer, postgres bool) error {
	if r.Class != events.EventAdmissionInheritedFanOut {
		return nil
	}
	admitted, err := r.Decode()
	if err != nil {
		return err
	}
	if err := fanoutorigin.ValidateCommitted(ctx, q, postgres, admitted.Event()); err != nil {
		return Corrupt(r.EventID, err)
	}
	return nil
}

// The private record codec restores semantic facts, never publication authority.
func (r Record) decodeInheritedFanOutOrigin() (*events.InheritedFanOutOrigin, error) {
	if r.Class != events.EventAdmissionInheritedFanOut {
		if len(r.InheritedFanOutOrigin) != 0 {
			return nil, fmt.Errorf("event class %q cannot carry inherited fan-out origin", r.Class)
		}
		return nil, nil
	}
	if r.SourceEventID != "" {
		return nil, fmt.Errorf("inherited fan-out origin cannot carry an ordinary causal parent")
	}
	var wire struct {
		RunID                string                       `json:"run_id"`
		SourceRunID          string                       `json:"source_run_id"`
		TriggerEventID       string                       `json:"trigger_event_id"`
		TriggeringDeliveryID string                       `json:"triggering_delivery_id"`
		Declaration          identity.DeclarationIdentity `json:"declaration"`
		BundleHash           string                       `json:"bundle_hash"`
		SemanticDigest       string                       `json:"semantic_digest"`
		Ordinal              *int                         `json:"ordinal"`
	}
	decoder := json.NewDecoder(bytes.NewReader(r.InheritedFanOutOrigin))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return nil, fmt.Errorf("decode inherited fan-out origin: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("inherited fan-out origin requires exactly one JSON object")
	}
	if wire.Ordinal == nil || wire.RunID != r.RunID {
		return nil, fmt.Errorf("inherited fan-out origin requires exact destination run and ordinal presence")
	}
	origin, err := events.NewInheritedFanOutOrigin(wire.RunID, wire.SourceRunID, wire.TriggerEventID, wire.TriggeringDeliveryID,
		wire.Declaration, wire.BundleHash, wire.SemanticDigest, *wire.Ordinal)
	if err != nil {
		return nil, err
	}
	return &origin, nil
}
