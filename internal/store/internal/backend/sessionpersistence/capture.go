package sessionpersistence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/channelonboarding"
	capturedata "github.com/division-sh/swarm/internal/sessioncapture"

	"github.com/google/uuid"
)

type CaptureStore struct {
	connectionID string
	db           *sql.DB
}

func newCaptureStore(ctx context.Context, db *sql.DB, connectionID string) (*CaptureStore, error) {
	if db == nil || uuid.Validate(connectionID) != nil {
		return nil, fmt.Errorf("WhatsApp incoming Capture requires its private database and stable connection")
	}
	db.SetMaxOpenConns(1)
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS whatsapp_incoming_capture (
		sequence INTEGER PRIMARY KEY,
		connection_id TEXT NOT NULL,
		account_ref TEXT NOT NULL,
		conversation_ref TEXT NOT NULL,
		event_id TEXT NOT NULL,
		event_kind TEXT NOT NULL,
		body_bytes INTEGER NOT NULL CHECK(body_bytes > 0 AND body_bytes <= 1048576),
		envelope BLOB NOT NULL,
		digest BLOB NOT NULL,
		publication_request BLOB,
		publication_digest BLOB,
		setup_disposition TEXT CHECK(setup_disposition IS NULL OR setup_disposition = 'non_claim'),
		setup_disposition_digest BLOB,
		CHECK((setup_disposition IS NULL AND setup_disposition_digest IS NULL) OR
		      (setup_disposition IS NOT NULL AND setup_disposition = 'non_claim' AND setup_disposition_digest IS NOT NULL AND length(setup_disposition_digest) = 32 AND publication_request IS NULL)),
		CHECK((publication_request IS NULL AND publication_digest IS NULL) OR
		      (publication_request IS NOT NULL AND publication_digest IS NOT NULL AND length(publication_digest) = 32)),
		UNIQUE(connection_id, account_ref, conversation_ref, event_id, event_kind)
	)`)
	if err != nil {
		return nil, err
	}
	columns, err := db.QueryContext(ctx, `SELECT setup_disposition,setup_disposition_digest FROM whatsapp_incoming_capture LIMIT 0`)
	if err != nil {
		return nil, fmt.Errorf("WhatsApp private capture schema is unsupported: %w", err)
	}
	if err := columns.Close(); err != nil {
		return nil, err
	}
	_, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS whatsapp_callback_failures (
		connection_id TEXT NOT NULL,
		occurrence_id TEXT NOT NULL,
		reason TEXT NOT NULL CHECK(reason IN ('crash','capture_failed')),
		PRIMARY KEY(connection_id,occurrence_id,reason)
	)`)
	if err != nil {
		return nil, err
	}
	return &CaptureStore{connectionID: connectionID, db: db}, nil
}

func (s *CaptureStore) Capture(ctx context.Context, event capturedEvent) error {
	if err := event.Validate(); err != nil {
		return err
	}
	if event.Scope.Session.ConnectionID != s.connectionID {
		return errCaptureScopeChanged
	}
	envelope, err := json.Marshal(event)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(envelope)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stored, err := s.readAllRows(ctx, tx)
	if err != nil {
		return err
	}
	var totalBytes int
	var pending int
	for _, row := range stored {
		prior := row.event
		if !row.nonClaim {
			pending++
			totalBytes += len(prior.Body)
		}
		if prior.Scope.Session.AccountRef != event.Scope.Session.AccountRef || prior.Conversation != event.Conversation ||
			prior.EventID != event.EventID || prior.Kind != event.Kind {
			continue
		}
		// A recovered occurrence may see the same provider delivery again. All
		// stored routing and quota evidence was validated before this success.
		if !prior.SameDelivery(event) && !(row.nonClaim && prior.SameNonClaimDelivery(event)) {
			return errCaptureConflict
		}
		return tx.Commit()
	}
	if pending >= maxPendingCaptureCount || totalBytes+len(event.Body) > maxPendingCaptureBytes {
		return errCaptureCapacity
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO whatsapp_incoming_capture
		(connection_id,account_ref,conversation_ref,event_id,event_kind,body_bytes,envelope,digest)
		VALUES(?,?,?,?,?,?,?,?)`, s.connectionID, event.Scope.Session.AccountRef, event.Conversation,
		event.EventID, event.Kind, len(event.Body), envelope, digest[:])
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *CaptureStore) Pending(ctx context.Context) ([]capturedEvent, error) {
	return s.readCapturedRows(ctx, s.db)
}

type captureRowQuery interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// Every admission and readback consumer validates the same complete rows. A
// corrupt routing index cannot hide a row from duplicate or quota admission.
func (s *CaptureStore) readCapturedRows(ctx context.Context, query captureRowQuery) ([]capturedEvent, error) {
	stored, err := s.readPendingRows(ctx, query)
	if err != nil {
		return nil, err
	}
	result := make([]capturedEvent, 0, len(stored))
	for _, row := range stored {
		result = append(result, row.event)
	}
	return result, nil
}

func (s *CaptureStore) readPendingRows(ctx context.Context, query captureRowQuery) ([]pendingCapture, error) {
	rows, err := s.readAllRows(ctx, query)
	if err != nil {
		return nil, err
	}
	result := make([]pendingCapture, 0, len(rows))
	for _, row := range rows {
		if !row.nonClaim {
			result = append(result, row)
		}
	}
	return result, nil
}

func (s *CaptureStore) readAllRows(ctx context.Context, query captureRowQuery) ([]pendingCapture, error) {
	rows, err := query.QueryContext(ctx, `SELECT sequence,envelope,digest,connection_id,body_bytes,
		account_ref,conversation_ref,event_id,event_kind,publication_request,publication_digest,setup_disposition,setup_disposition_digest
		FROM whatsapp_incoming_capture ORDER BY sequence`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []pendingCapture
	var totalBytes int64
	var pendingCount int
	for rows.Next() {
		var raw, digest, request, requestDigest, dispositionDigest []byte
		var disposition sql.NullString
		var connectionID, account, conversation, eventID, kind string
		var sequence, bodyBytes int64
		if err := rows.Scan(&sequence, &raw, &digest, &connectionID, &bodyBytes, &account, &conversation, &eventID, &kind, &request, &requestDigest, &disposition, &dispositionDigest); err != nil {
			return nil, err
		}
		event, err := capturedata.DecodeCapture(raw, digest)
		if err != nil {
			return nil, err
		}
		if connectionID != s.connectionID || event.Scope.Session.ConnectionID != connectionID || int64(len(event.Body)) != bodyBytes ||
			event.Scope.Session.AccountRef != account || event.Conversation != conversation || event.EventID != eventID || event.Kind != kind {
			return nil, fmt.Errorf("WhatsApp incoming Capture scope or byte accounting mismatch")
		}
		nonClaim := disposition.Valid
		if err := validateNonClaimReceipt(event, raw, disposition, dispositionDigest, request); err != nil {
			return nil, err
		}
		if !nonClaim {
			totalBytes += bodyBytes
			pendingCount++
			if pendingCount > maxPendingCaptureCount || totalBytes > maxPendingCaptureBytes {
				return nil, errCaptureCapacity
			}
		}
		pending := pendingCapture{sequence: sequence, event: event, requestBytes: request, nonClaim: nonClaim}
		if request != nil || requestDigest != nil {
			admitted, err := capturedata.DecodePublicationRequest(event, request, requestDigest)
			if err != nil {
				return nil, err
			}
			pending.request = &admitted
		}
		result = append(result, pending)
	}
	return result, rows.Err()
}

func nonClaimDigest(raw []byte) [32]byte {
	return sha256.Sum256(append([]byte("whatsapp-setup-nonclaim-v1\x00"), raw...))
}

func validateNonClaimReceipt(event capturedEvent, raw []byte, disposition sql.NullString, digest, request []byte) error {
	if !disposition.Valid && digest == nil {
		return nil
	}
	expected := nonClaimDigest(raw)
	if !disposition.Valid || disposition.String != "non_claim" || request != nil ||
		event.Scope.Kind != channelonboarding.SessionInputOnboarding || !bytes.Equal(digest, expected[:]) {
		return fmt.Errorf("WhatsApp non-claim disposition contradicts its original capture")
	}
	return nil
}

func (s *CaptureStore) RecordFailure(ctx context.Context, failure callbackFailure) error {
	if failure.ConnectionID != s.connectionID || uuid.Validate(failure.OccurrenceID) != nil ||
		(failure.Reason != "crash" && failure.Reason != "capture_failed") {
		return fmt.Errorf("WhatsApp callback failure has a different or invalid occurrence")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO whatsapp_callback_failures(connection_id,occurrence_id,reason)
		VALUES(?,?,?) ON CONFLICT(connection_id,occurrence_id,reason) DO NOTHING`,
		failure.ConnectionID, failure.OccurrenceID, failure.Reason)
	return err
}

type capturedEvent = capturedata.Event
type callbackFailure = capturedata.CallbackFailure

const maxPendingCaptureCount = capturedata.MaxPendingCaptureCount
const maxPendingCaptureBytes = capturedata.MaxPendingCaptureBytes

var errCaptureScopeChanged = capturedata.ErrCaptureScopeChanged
var errCaptureConflict = capturedata.ErrCaptureConflict
var errCaptureCapacity = capturedata.ErrCaptureCapacity
var errCaptureMissing = capturedata.ErrCaptureMissing
var errCapturePublicationPending = capturedata.ErrCapturePublicationPending

func (s *CaptureStore) PendingPublications(ctx context.Context) ([]capturedata.PendingCapture, error) {
	rows, err := s.readPendingRows(ctx, s.db)
	if err != nil {
		return nil, err
	}
	result := make([]capturedata.PendingCapture, len(rows))
	for i, row := range rows {
		result[i] = capturedata.PendingCapture{Sequence: row.sequence, Event: row.event, Request: row.request, RequestBytes: append([]byte(nil), row.requestBytes...)}
	}
	return result, nil
}
