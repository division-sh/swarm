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
		e.EventID == other.EventID && e.Kind == other.Kind && bytes.Equal(e.Body, other.Body)
}

func (e capturedEvent) publicationFingerprint() (string, error) {
	if err := e.validate(); err != nil {
		return "", err
	}
	return runtimeinbound.SemanticFingerprint(e)
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
	identity, err := event.publicationProviderEventID()
	if err != nil {
		return err
	}
	fingerprint, err := event.publicationFingerprint()
	if err != nil {
		return err
	}
	publicationID, markerID := runtimeinbound.DeterministicIDs("whatsapp", request.EntityID, identity)
	if request.Provider != "whatsapp" || request.ProviderEventID != identity || request.RequestFingerprint != fingerprint ||
		request.PublicationID != publicationID || request.MarkerEventID != markerID {
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
	LoadInboundPublicationByIdentity(context.Context, string, string, string) (runtimeinbound.Record, bool, error)
}

// This reader must be the selected-store owner, whose exact load verifies all
// committed coupling. A callback result or current connection health is not a
// substitute. Result loss before retirement leaves the original request pending.
func (s *captureStore) retirePublished(ctx context.Context, event capturedEvent, reader publicationReader) error {
	if reader == nil {
		return errCapturePublicationPending
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
		if row.request == nil {
			return errCapturePublicationPending
		}
		record, found, err := reader.LoadInboundPublicationByIdentity(ctx, "whatsapp", row.request.EntityID, row.request.ProviderEventID)
		if err != nil {
			return err
		}
		if !found || record.State != "committed" || record.CommittedAt.IsZero() {
			return errCapturePublicationPending
		}
		actual, err := publicationRequestBytes(record.Request)
		if err != nil {
			return err
		}
		if !bytes.Equal(row.requestBytes, actual) {
			return runtimeinbound.ErrRequestIdentityConflict
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM whatsapp_incoming_capture WHERE sequence=?`, row.sequence); err != nil {
			return err
		}
		return tx.Commit()
	}
	return errCaptureMissing
}
