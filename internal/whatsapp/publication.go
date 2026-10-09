package whatsapp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
)

var errCaptureMissing = errors.New("WhatsApp original capture is not pending")
var errCapturePublicationPending = errors.New("WhatsApp capture has no verified committed publication")

type pendingCapture struct {
	sequence     int64
	event        capturedEvent
	request      *runtimeinbound.Request
	requestBytes []byte
}

func (e capturedEvent) sameCapture(other capturedEvent) bool {
	return e.Scope == other.Scope && e.OccurrenceID == other.OccurrenceID && e.Conversation == other.Conversation &&
		e.EventID == other.EventID && e.Kind == other.Kind && e.ReceivedAt.Equal(other.ReceivedAt) && bytes.Equal(e.Body, other.Body)
}

func (e capturedEvent) publicationFingerprint() (string, error) {
	if err := e.validate(); err != nil {
		return "", err
	}
	return runtimeinbound.SemanticFingerprint(struct {
		Scope                       captureScope
		Conversation, EventID, Kind string
		Body                        []byte
	}{e.Scope, e.Conversation, e.EventID, e.Kind, e.Body})
}

const captureProvenanceKey = "whatsapp_capture"

// The selected-store request retains original occurrence provenance; only the
// retry-stable content fingerprint excludes that transport occurrence.
func withCaptureProvenance(event capturedEvent, request runtimeinbound.Request) (runtimeinbound.Request, error) {
	if err := event.validate(); err != nil {
		return runtimeinbound.Request{}, err
	}
	if request.Identity().BindingGeneration() != event.Scope.PublicationBinding {
		return runtimeinbound.Request{}, runtimeinbound.ErrRequestIdentityConflict
	}
	metadata := request.Normalized().OriginalTransportMetadata
	if _, err := canonicaljson.Decode(metadata); err != nil {
		return runtimeinbound.Request{}, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(metadata, &fields); err != nil || fields == nil {
		return runtimeinbound.Request{}, fmt.Errorf("WhatsApp publication requires object transport metadata")
	}
	if _, exists := fields[captureProvenanceKey]; exists {
		return runtimeinbound.Request{}, runtimeinbound.ErrRequestIdentityConflict
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return runtimeinbound.Request{}, err
	}
	fields[captureProvenanceKey] = raw
	request.OriginalTransportMetadata, err = json.Marshal(fields)
	return request, err
}

func publicationCaptureProvenance(request runtimeinbound.Request) (capturedEvent, error) {
	var event capturedEvent
	if _, err := canonicaljson.Decode(request.OriginalTransportMetadata); err != nil {
		return event, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(request.OriginalTransportMetadata, &fields); err != nil {
		return event, err
	}
	raw, ok := fields[captureProvenanceKey]
	if !ok {
		return event, fmt.Errorf("WhatsApp publication has no original capture evidence")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&event); err != nil {
		return event, err
	}
	return event, event.validate()
}

func publicationRequestBytes(request runtimeinbound.Request) ([]byte, error) {
	request = request.Normalized()
	if err := request.Validate(); err != nil {
		return nil, err
	}
	// Both selected stores retain timestamps at microsecond precision. Refuse an
	// unrepresentable input instead of silently changing its frozen evidence.
	if !request.OriginalReceivedAt.Equal(request.OriginalReceivedAt.Truncate(time.Microsecond)) {
		return nil, fmt.Errorf("WhatsApp publication receipt time must have microsecond precision")
	}
	raw, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	value, err := canonicaljson.Decode(raw)
	if err != nil {
		return nil, err
	}
	return canonicaljson.Encode(value)
}

func validateCapturePublication(event capturedEvent, request runtimeinbound.Request) error {
	identity, err := event.publicationIdentity()
	if err != nil {
		return err
	}
	fingerprint, err := event.publicationFingerprint()
	if err != nil {
		return err
	}
	publicationID, markerID, err := runtimeinbound.DeterministicIDs(identity)
	if err != nil {
		return err
	}
	if request.Identity() != identity || request.RequestFingerprint != fingerprint ||
		request.PublicationID != publicationID || request.MarkerEventID != markerID || !request.OriginalReceivedAt.Equal(event.ReceivedAt) {
		return runtimeinbound.ErrRequestIdentityConflict
	}
	original, err := publicationCaptureProvenance(request)
	if err != nil {
		return fmt.Errorf("%w: %w", runtimeinbound.ErrRequestIdentityConflict, err)
	}
	if !original.sameCapture(event) {
		return runtimeinbound.ErrRequestIdentityConflict
	}
	return nil
}

func decodePublicationRequest(event capturedEvent, raw, digest []byte) (runtimeinbound.Request, error) {
	var request runtimeinbound.Request
	if got := sha256.Sum256(raw); len(raw) == 0 || !bytes.Equal(got[:], digest) {
		return request, fmt.Errorf("WhatsApp capture publication digest mismatch")
	}
	if _, err := canonicaljson.Decode(raw); err != nil {
		return request, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return request, err
	}
	canonical, err := publicationRequestBytes(request)
	if err != nil {
		return request, err
	}
	if !bytes.Equal(canonical, raw) {
		return request, fmt.Errorf("WhatsApp capture publication request is not canonical")
	}
	return request, validateCapturePublication(event, request)
}

// Staging freezes the request selected by the existing runtime admission owner;
// it does not grant account, target, standing or normalized-output authority.
func (s *captureStore) stagePublication(ctx context.Context, event capturedEvent, request runtimeinbound.Request) error {
	raw, err := publicationRequestBytes(request)
	if err != nil {
		return err
	}
	request = request.Normalized()
	if err := validateCapturePublication(event, request); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stored, err := s.readPendingRows(ctx, tx)
	if err != nil {
		return err
	}
	for _, row := range stored {
		if !row.event.sameCapture(event) {
			continue
		}
		if row.request != nil {
			if !bytes.Equal(row.requestBytes, raw) {
				return runtimeinbound.ErrRequestIdentityConflict
			}
			return tx.Commit()
		}
		digest := sha256.Sum256(raw)
		if _, err := tx.ExecContext(ctx, `UPDATE whatsapp_incoming_capture SET publication_request=?,publication_digest=?
			WHERE sequence=?`, raw, digest[:], row.sequence); err != nil {
			return err
		}
		return tx.Commit()
	}
	return errCaptureMissing
}

type publicationReader interface {
	LoadInboundPublicationByIdentity(context.Context, runtimeinbound.Identity) (runtimeinbound.Record, bool, error)
}

// This reader must be the selected-store owner, whose exact load verifies all
// committed coupling. A callback result or current connection health is not a
// substitute. Result loss before retirement leaves the original request pending.
func (s *captureStore) retirePublished(ctx context.Context, event capturedEvent, reader publicationReader) error {
	found, err := s.reconcilePublished(ctx, event, reader)
	if err != nil {
		return err
	}
	if !found {
		return errCapturePublicationPending
	}
	return nil
}

func verifyHistoricalCapture(event capturedEvent, record runtimeinbound.Record) ([]byte, error) {
	if record.State != "committed" || record.CommittedAt.IsZero() {
		return nil, errCapturePublicationPending
	}
	original, err := publicationCaptureProvenance(record.Request)
	if err != nil {
		return nil, err
	}
	if err := validateCapturePublication(original, record.Request); err != nil {
		return nil, err
	}
	if original.Scope != event.Scope || original.Conversation != event.Conversation || original.EventID != event.EventID ||
		original.Kind != event.Kind || !bytes.Equal(original.Body, event.Body) {
		return nil, runtimeinbound.ErrRequestIdentityConflict
	}
	return publicationRequestBytes(record.Request)
}

// Historical reconciliation precedes new planning and staging. The namespace
// comes from the captured admission, never today's target. A fresh occurrence
// can retire an identical duplicate only after verifying the original evidence.
func (s *captureStore) reconcilePublished(ctx context.Context, event capturedEvent, reader publicationReader) (bool, error) {
	if reader == nil {
		return false, errCapturePublicationPending
	}
	identity, err := event.publicationIdentity()
	if err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	stored, err := s.readPendingRows(ctx, tx)
	if err != nil {
		return false, err
	}
	for _, row := range stored {
		if !row.event.sameCapture(event) {
			continue
		}
		record, found, err := reader.LoadInboundPublicationByIdentity(ctx, identity)
		if err != nil {
			return false, err
		}
		if !found {
			return false, nil
		}
		actual, err := verifyHistoricalCapture(event, record)
		if err != nil {
			return false, err
		}
		if row.request != nil && !bytes.Equal(row.requestBytes, actual) {
			return false, runtimeinbound.ErrRequestIdentityConflict
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM whatsapp_incoming_capture WHERE sequence=?`, row.sequence); err != nil {
			return false, err
		}
		if err := tx.Commit(); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, errCaptureMissing
}
