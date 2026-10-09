package whatsapp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
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
	Kind                channelonboarding.SessionInputScope
	Session             operatorchannel.SessionAccountAdmission
	PublicationBinding  runtimeinbound.BindingGeneration
	Source              channelonboarding.ChannelDurableContextIdentity
	OnboardingOperation string
	OperationRevision   int64
	ActivationRevision  int64
	TargetSelector      string
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
	switch s.Kind {
	case channelonboarding.SessionInputOnboarding:
		if s.PublicationBinding != (runtimeinbound.BindingGeneration{}) || s.ActivationRevision != 0 {
			return fmt.Errorf("WhatsApp onboarding capture cannot carry business publication authority")
		}
	case channelonboarding.SessionInputBusiness:
		if err := s.PublicationBinding.Validate(); err != nil {
			return err
		}
		if s.BindingRevision < 1 || s.ActivationRevision < 1 {
			return fmt.Errorf("WhatsApp business capture requires its confirmed binding")
		}
	default:
		return fmt.Errorf("WhatsApp capture requires explicit onboarding or business scope")
	}
	if s.Session.Provider != "whatsapp" || uuid.Validate(s.OnboardingOperation) != nil ||
		uuid.Validate(s.PrincipalID) != nil || s.BindingRevision < 0 || s.OperationRevision < 1 || s.TargetSelector == "" {
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
	ReceivedAt   time.Time
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
	if e.ReceivedAt.IsZero() || !e.ReceivedAt.Equal(e.ReceivedAt.Truncate(time.Microsecond)) {
		return fmt.Errorf("WhatsApp capture requires its original microsecond receipt time")
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
		publication_request BLOB,
		publication_digest BLOB,
		CHECK((publication_request IS NULL AND publication_digest IS NULL) OR
		      (publication_request IS NOT NULL AND publication_digest IS NOT NULL AND length(publication_digest) = 32)),
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
	stored, err := s.readCapturedRows(ctx, tx)
	if err != nil {
		return err
	}
	var totalBytes int
	for _, prior := range stored {
		totalBytes += len(prior.Body)
		if prior.Scope.Session.AccountRef != event.Scope.Session.AccountRef || prior.Conversation != event.Conversation ||
			prior.EventID != event.EventID || prior.Kind != event.Kind {
			continue
		}
		// A recovered occurrence may see the same provider delivery again. All
		// stored routing and quota evidence was validated before this success.
		if prior.Scope != event.Scope || !bytes.Equal(prior.Body, event.Body) {
			return errCaptureConflict
		}
		return tx.Commit()
	}
	if len(stored) >= maxPendingCaptureCount || totalBytes+len(event.Body) > maxPendingCaptureBytes {
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
	return s.readCapturedRows(ctx, s.db)
}

type captureRowQuery interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// Every admission and readback consumer validates the same complete rows. A
// corrupt routing index cannot hide a row from duplicate or quota admission.
func (s *captureStore) readCapturedRows(ctx context.Context, query captureRowQuery) ([]capturedEvent, error) {
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

func (s *captureStore) readPendingRows(ctx context.Context, query captureRowQuery) ([]pendingCapture, error) {
	rows, err := query.QueryContext(ctx, `SELECT sequence,envelope,digest,connection_id,body_bytes,
		account_ref,conversation_ref,event_id,event_kind,publication_request,publication_digest
		FROM whatsapp_incoming_capture ORDER BY sequence`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []pendingCapture
	var totalBytes int64
	for rows.Next() {
		var raw, digest, request, requestDigest []byte
		var connectionID, account, conversation, eventID, kind string
		var sequence, bodyBytes int64
		if err := rows.Scan(&sequence, &raw, &digest, &connectionID, &bodyBytes, &account, &conversation, &eventID, &kind, &request, &requestDigest); err != nil {
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
		pending := pendingCapture{sequence: sequence, event: event, requestBytes: request}
		if request != nil || requestDigest != nil {
			admitted, err := decodePublicationRequest(event, request, requestDigest)
			if err != nil {
				return nil, err
			}
			pending.request = &admitted
		}
		result = append(result, pending)
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
