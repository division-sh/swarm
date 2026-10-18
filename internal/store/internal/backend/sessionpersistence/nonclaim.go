package sessionpersistence

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
)

// SettleNonClaim retains the existing capture as a verified non-executable
// receipt. Only the private compiled projection owner can supply this fact.
func (s *CaptureStore) SettleNonClaim(ctx context.Context, fact authority.NonClaim) error {
	if err := fact.Validate(ctx); err != nil {
		return err
	}
	raw := fact.OriginalCapture()
	var event capturedEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return err
	}
	parentID, revision := fact.Parent()
	fingerprint, err := event.PublicationFingerprint()
	if err != nil || event.Scope.Kind != channelonboarding.SessionInputOnboarding ||
		event.Scope.OnboardingOperation != parentID || event.Scope.OperationRevision != revision || fingerprint != fact.Fingerprint() {
		return errCaptureScopeChanged
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := s.readAllRows(ctx, tx)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if !row.event.SameCapture(event) {
			continue
		}
		original, err := json.Marshal(row.event)
		if err != nil || !bytes.Equal(original, raw) || row.request != nil {
			return errCaptureConflict
		}
		if err := fact.Validate(ctx); err != nil {
			return err
		}
		if !row.nonClaim {
			digest := nonClaimDigest(original)
			if _, err := tx.ExecContext(ctx, `UPDATE whatsapp_incoming_capture
				SET setup_disposition='non_claim',setup_disposition_digest=? WHERE sequence=?`, digest[:], row.sequence); err != nil {
				return err
			}
		}
		if err := fact.Validate(ctx); err != nil {
			return err
		}
		return tx.Commit()
	}
	return errCaptureMissing
}

func (s *CaptureStore) NonClaimReceipts(ctx context.Context) ([]capturedEvent, error) {
	rows, err := s.readAllRows(ctx, s.db)
	if err != nil {
		return nil, err
	}
	var receipts []capturedEvent
	for _, row := range rows {
		if row.nonClaim {
			receipts = append(receipts, row.event)
		}
	}
	return receipts, nil
}
