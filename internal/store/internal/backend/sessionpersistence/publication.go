package sessionpersistence

import (
	"bytes"
	"context"
	"crypto/sha256"

	capturedata "github.com/division-sh/swarm/internal/sessioncapture"

	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
)

type pendingCapture struct {
	sequence     int64
	event        capturedEvent
	request      *runtimeinbound.Request
	requestBytes []byte
}

func (s *CaptureStore) StagePublication(ctx context.Context, event capturedEvent, request runtimeinbound.Request) error {
	raw, err := capturedata.PublicationRequestBytes(request)
	if err != nil {
		return err
	}
	request = request.Normalized()
	if err := capturedata.ValidateCapturePublication(event, request); err != nil {
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
		if !row.event.SameCapture(event) {
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

type PublicationReader interface {
	LoadInboundPublicationByIdentity(context.Context, runtimeinbound.Identity) (runtimeinbound.Record, bool, error)
}

// This reader must be the selected-store owner, whose exact load verifies all
// committed coupling. A callback result or current connection health is not a
// substitute. Result loss before retirement leaves the original request pending.
func (s *CaptureStore) RetirePublished(ctx context.Context, event capturedEvent, reader PublicationReader) error {
	found, err := s.ReconcilePublished(ctx, event, reader)
	if err != nil {
		return err
	}
	if !found {
		return errCapturePublicationPending
	}
	return nil
}

// Historical reconciliation precedes new planning and staging. The namespace
// comes from the captured admission, never today's target. A fresh occurrence
// can retire an identical duplicate only after verifying the original evidence.
func (s *CaptureStore) ReconcilePublished(ctx context.Context, event capturedEvent, reader PublicationReader) (bool, error) {
	if reader == nil {
		return false, errCapturePublicationPending
	}
	identity, err := event.PublicationIdentity()
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
		if !row.event.SameCapture(event) {
			continue
		}
		record, found, err := reader.LoadInboundPublicationByIdentity(ctx, identity)
		if err != nil {
			return false, err
		}
		if !found {
			return false, nil
		}
		actual, err := capturedata.VerifyHistoricalCapture(event, record)
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
