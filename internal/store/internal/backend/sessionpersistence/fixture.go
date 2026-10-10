package sessionpersistence

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/sessioncapture"
)

type CaptureCorruption string
type CaptureFault string

const (
	CorruptConnection       CaptureCorruption = "connection"
	CorruptAccount          CaptureCorruption = "account"
	CorruptConversation     CaptureCorruption = "conversation"
	CorruptEvent            CaptureCorruption = "event"
	CorruptKind             CaptureCorruption = "kind"
	CorruptByteCount        CaptureCorruption = "byte_count"
	CorruptEnvelope         CaptureCorruption = "envelope"
	CorruptDigest           CaptureCorruption = "digest"
	CorruptSetupDisposition CaptureCorruption = "setup_disposition"
	CaptureInsertFault      CaptureFault      = "insert"
	CaptureStageFault       CaptureFault      = "stage"
	CaptureRetirementFault  CaptureFault      = "retirement"
)

func (o *Owner) CorruptCaptureIndex(ctx context.Context, dimension CaptureCorruption) error {
	var query string
	switch dimension {
	case CorruptConnection:
		query = `UPDATE whatsapp_incoming_capture SET connection_id='another_connection'`
	case CorruptAccount:
		query = `UPDATE whatsapp_incoming_capture SET account_ref='another_account'`
	case CorruptConversation:
		query = `UPDATE whatsapp_incoming_capture SET conversation_ref='another_conversation'`
	case CorruptEvent:
		query = `UPDATE whatsapp_incoming_capture SET event_id='another_event'`
	case CorruptKind:
		query = `UPDATE whatsapp_incoming_capture SET event_kind='edit'`
	case CorruptByteCount:
		query = `UPDATE whatsapp_incoming_capture SET body_bytes=1`
	case CorruptEnvelope:
		query = `UPDATE whatsapp_incoming_capture SET envelope='{}'`
	case CorruptDigest:
		query = `UPDATE whatsapp_incoming_capture SET digest=x'00'`
	case CorruptSetupDisposition:
		query = `UPDATE whatsapp_incoming_capture SET setup_disposition_digest=zeroblob(32) WHERE setup_disposition='non_claim'`
	default:
		return fmt.Errorf("unknown capture corruption %q", dimension)
	}
	_, err := o.db.ExecContext(ctx, query)
	return err
}

func (o *Owner) CorruptFirstCaptureConversation(ctx context.Context) error {
	_, err := o.db.ExecContext(ctx, `UPDATE whatsapp_incoming_capture SET conversation_ref='tampered' WHERE sequence=1`)
	return err
}

func (o *Owner) ReplaceCaptureEnvelope(ctx context.Context, raw, digest []byte) error {
	_, err := o.db.ExecContext(ctx, `UPDATE whatsapp_incoming_capture SET envelope=?,digest=?`, raw, digest)
	return err
}

func (o *Owner) CorruptPublicationDigest(ctx context.Context) error {
	_, err := o.db.ExecContext(ctx, `UPDATE whatsapp_incoming_capture SET publication_digest=?`, make([]byte, 32))
	return err
}

func (o *Owner) ReplacePublicationRequest(ctx context.Context, raw, digest []byte) error {
	_, err := o.db.ExecContext(ctx, `UPDATE whatsapp_incoming_capture SET publication_request=?,publication_digest=?`, raw, digest)
	return err
}

func (o *Owner) SetCaptureFault(ctx context.Context, fault CaptureFault, enabled bool) error {
	var name, verb string
	switch fault {
	case CaptureInsertFault:
		name, verb = "session_capture_insert_cut", "INSERT"
	case CaptureStageFault:
		name, verb = "session_capture_stage_cut", "UPDATE"
	case CaptureRetirementFault:
		name, verb = "session_capture_retire_cut", "DELETE"
	default:
		return fmt.Errorf("unknown capture fault %q", fault)
	}
	query := `DROP TRIGGER ` + name
	if enabled {
		query = `CREATE TRIGGER ` + name + ` BEFORE ` + verb + ` ON whatsapp_incoming_capture BEGIN SELECT RAISE(ABORT,'private capture fixture write failure'); END`
	}
	_, err := o.db.ExecContext(ctx, query)
	return err
}

func (o *Owner) CaptureCount(ctx context.Context) (int, error) {
	var count int
	err := o.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM whatsapp_incoming_capture`).Scan(&count)
	return count, err
}

func (o *Owner) CallbackFailureReason(ctx context.Context, occurrenceID string) (string, error) {
	var reason string
	err := o.db.QueryRowContext(ctx, `SELECT reason FROM whatsapp_callback_failures WHERE occurrence_id=?`, occurrenceID).Scan(&reason)
	return reason, err
}

func (o *Owner) CallbackFailureCount(ctx context.Context, connectionID, occurrenceID, reason string) (int, error) {
	var count int
	err := o.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM whatsapp_callback_failures WHERE occurrence_id=? AND (?='' OR connection_id=?) AND (?='' OR reason=?)`, occurrenceID, connectionID, connectionID, reason, reason).Scan(&count)
	return count, err
}

func (o *Owner) CaptureRawRows(ctx context.Context) ([][]byte, error) {
	rows, err := o.db.QueryContext(ctx, `SELECT envelope,digest FROM whatsapp_incoming_capture ORDER BY sequence`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result [][]byte
	for rows.Next() {
		var raw, digest []byte
		if err := rows.Scan(&raw, &digest); err != nil {
			return nil, err
		}
		result = append(result, raw, digest)
	}
	return result, rows.Err()
}

func (o *Owner) CaptureStoredRows(ctx context.Context) ([]string, error) {
	rows, err := o.db.QueryContext(ctx, `SELECT json_array(sequence,connection_id,account_ref,conversation_ref,event_id,event_kind,body_bytes,hex(envelope),hex(digest)) FROM whatsapp_incoming_capture ORDER BY sequence`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var row string
		if err := rows.Scan(&row); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func (o *Owner) SeedCaptureQuota(ctx context.Context, event sessioncapture.Event, count int) error {
	if count < 1 || count > sessioncapture.MaxPendingCaptureCount {
		return fmt.Errorf("invalid capture quota fixture count")
	}
	capture, err := o.Captures(ctx, event.Scope.Session.ConnectionID)
	if err != nil {
		return err
	}
	event.EventID = "0"
	if err := capture.Capture(ctx, event); err != nil {
		return err
	}
	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for i := 1; i < count; i++ {
		event.EventID = fmt.Sprint(i)
		raw, err := json.Marshal(event)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(raw)
		if _, err := tx.ExecContext(ctx, `INSERT INTO whatsapp_incoming_capture (connection_id,account_ref,conversation_ref,event_id,event_kind,body_bytes,envelope,digest) VALUES(?,?,?,?,?,?,?,?)`, event.Scope.Session.ConnectionID, event.Scope.Session.AccountRef, event.Conversation, event.EventID, event.Kind, len(event.Body), raw, digest[:]); err != nil {
			return err
		}
	}
	return tx.Commit()
}
