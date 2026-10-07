package whatsapp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/google/uuid"
)

const maxCaptureEventBytes = 1 << 20
const maxPendingCaptureCount = 1024
const maxPendingCaptureBytes = 16 << 20

var errCaptureConflict = errors.New("WhatsApp event identity already carries different capture evidence")
var errCaptureCapacity = errors.New("WhatsApp incoming capture capacity exhausted")
var errCaptureScopeChanged = errors.New("WhatsApp capture belongs to a different original admission, principal or source")

// captureScope freezes existing admitted facts. It does not grant execution or
// reconstruct a binding from today's source/principal during recovery.
type captureScope struct {
	Session             operatorchannel.SessionAccountAdmission
	Source              channelonboarding.ChannelDurableContextIdentity
	OnboardingOperation string
	PrincipalID         string
	BindingRevision     int64
}

func (s captureScope) validate() error {
	if err := s.Session.Validate(); err != nil {
		return err
	}
	if err := s.Source.Validate(); err != nil {
		return err
	}
	if s.Session.Provider != "whatsapp" || uuid.Validate(s.OnboardingOperation) != nil ||
		uuid.Validate(s.PrincipalID) != nil || s.BindingRevision < 0 {
		return fmt.Errorf("WhatsApp capture requires its exact existing onboarding/principal/source scope")
	}
	return nil
}

type capturedEvent struct {
	Scope        captureScope
	OccurrenceID string
	Conversation string
	EventID      string
	Kind         string
	Body         []byte
}

func (e capturedEvent) validate() error {
	if err := e.Scope.validate(); err != nil {
		return err
	}
	if uuid.Validate(e.OccurrenceID) != nil || e.Conversation == "" || e.EventID == "" ||
		(e.Kind != "message" && e.Kind != "edit" && e.Kind != "revoke") {
		return fmt.Errorf("WhatsApp capture requires exact occurrence/conversation/event and supported kind")
	}
	if len(e.Body) == 0 || len(e.Body) > maxCaptureEventBytes {
		return errCaptureCapacity
	}
	_, err := canonicaljson.Decode(e.Body)
	return err
}

func (e capturedEvent) requireOriginalScope(current captureScope) error {
	if err := current.validate(); err != nil {
		return err
	}
	if e.Scope != current {
		return errCaptureScopeChanged
	}
	return nil
}

type captureStore struct {
	connectionID string
	db           *sql.DB
}

func newCaptureStore(ctx context.Context, db *sql.DB, connectionID string) (*captureStore, error) {
	if db == nil || uuid.Validate(connectionID) != nil {
		return nil, fmt.Errorf("WhatsApp incoming capture requires its private database and stable connection")
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
		UNIQUE(connection_id, account_ref, conversation_ref, event_id, event_kind)
	)`)
	if err != nil {
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
	return &captureStore{connectionID: connectionID, db: db}, nil
}

func (s *captureStore) capture(ctx context.Context, event capturedEvent) error {
	if err := event.validate(); err != nil {
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
	var previous, previousDigest []byte
	err = tx.QueryRowContext(ctx, `SELECT envelope, digest FROM whatsapp_incoming_capture
		WHERE connection_id=? AND account_ref=? AND conversation_ref=? AND event_id=? AND event_kind=?`,
		s.connectionID, event.Scope.Session.AccountRef, event.Conversation, event.EventID, event.Kind).Scan(&previous, &previousDigest)
	if err == nil {
		stored, err := decodeCapture(previous, previousDigest)
		if err != nil {
			return err
		}
		// A recovered occurrence may see the same provider delivery again. Its
		// original capture facts remain unchanged; a new scope is never adopted.
		if stored.Scope != event.Scope || !bytes.Equal(stored.Body, event.Body) {
			return errCaptureConflict
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var count, totalBytes int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(body_bytes),0)
		FROM whatsapp_incoming_capture WHERE connection_id=?`, s.connectionID).Scan(&count, &totalBytes); err != nil {
		return err
	}
	if count >= maxPendingCaptureCount || totalBytes+int64(len(event.Body)) > maxPendingCaptureBytes {
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

func decodeCapture(raw, digest []byte) (capturedEvent, error) {
	var event capturedEvent
	if got := sha256.Sum256(raw); !bytes.Equal(got[:], digest) {
		return event, fmt.Errorf("WhatsApp incoming capture digest mismatch")
	}
	if _, err := canonicaljson.Decode(raw); err != nil {
		return event, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&event); err != nil {
		return event, err
	}
	return event, event.validate()
}

func (s *captureStore) pending(ctx context.Context) ([]capturedEvent, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT envelope,digest,connection_id,body_bytes,
		account_ref,conversation_ref,event_id,event_kind
		FROM whatsapp_incoming_capture ORDER BY sequence`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []capturedEvent
	var totalBytes int64
	for rows.Next() {
		var raw, digest []byte
		var connectionID, account, conversation, eventID, kind string
		var bodyBytes int64
		if err := rows.Scan(&raw, &digest, &connectionID, &bodyBytes, &account, &conversation, &eventID, &kind); err != nil {
			return nil, err
		}
		event, err := decodeCapture(raw, digest)
		if err != nil {
			return nil, err
		}
		if connectionID != s.connectionID || event.Scope.Session.ConnectionID != connectionID || int64(len(event.Body)) != bodyBytes ||
			event.Scope.Session.AccountRef != account || event.Conversation != conversation || event.EventID != eventID || event.Kind != kind {
			return nil, fmt.Errorf("WhatsApp incoming capture scope or byte accounting mismatch")
		}
		totalBytes += bodyBytes
		if len(result) >= maxPendingCaptureCount || totalBytes > maxPendingCaptureBytes {
			return nil, errCaptureCapacity
		}
		result = append(result, event)
	}
	return result, rows.Err()
}

func (s *captureStore) recordFailure(ctx context.Context, failure callbackFailure) error {
	if failure.ConnectionID != s.connectionID || uuid.Validate(failure.OccurrenceID) != nil ||
		(failure.Reason != "crash" && failure.Reason != "capture_failed") {
		return fmt.Errorf("WhatsApp callback failure has a different or invalid occurrence")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO whatsapp_callback_failures(connection_id,occurrence_id,reason)
		VALUES(?,?,?) ON CONFLICT(connection_id,occurrence_id,reason) DO NOTHING`,
		failure.ConnectionID, failure.OccurrenceID, failure.Reason)
	return err
}
