package sessionpersistence

import (
	"context"
	"crypto/sha256"
	"encoding/json"

	"github.com/division-sh/swarm/internal/channelonboarding"
)

// RetainUnclaimed marks the original account capture terminal without assigning
// it a source/run. The old capture constraints and private evidence stay intact.
func (s *CaptureStore) RetainUnclaimed(ctx context.Context, event capturedEvent) error {
	if !event.Scope.DeploymentRegistration || event.Scope.RegisteredBusiness || event.Scope.Kind != channelonboarding.SessionInputOnboarding {
		return errCaptureScopeChanged
	}
	if err := s.Capture(ctx, event); err != nil {
		return err
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
		if !row.event.SameNonClaimDelivery(event) {
			continue
		}
		if row.nonClaim || row.request != nil {
			return errCaptureConflict
		}
		raw, err := json.Marshal(row.event)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(raw)
		if _, err := tx.ExecContext(ctx, `INSERT INTO whatsapp_unclaimed_capture(sequence,reason,digest)
			VALUES(?,'account_unclaimed',?) ON CONFLICT(sequence) DO NOTHING`, row.sequence, digest[:]); err != nil {
			return err
		}
		return tx.Commit()
	}
	return errCaptureMissing
}

func (s *CaptureStore) IsUnclaimed(ctx context.Context, event capturedEvent) (bool, error) {
	rows, err := s.readAllRows(ctx, s.db)
	if err != nil {
		return false, err
	}
	for _, row := range rows {
		if !row.unclaimed || row.event.Scope.Session != event.Scope.Session || row.event.Conversation != event.Conversation || row.event.EventID != event.EventID || row.event.Kind != event.Kind {
			continue
		}
		if !row.event.SameNonClaimDelivery(event) {
			return false, errCaptureConflict
		}
		return true, nil
	}
	return false, nil
}

func (s *CaptureStore) UnclaimedCount(ctx context.Context) (int64, error) {
	rows, err := s.readAllRows(ctx, s.db)
	var count int64
	for _, row := range rows {
		if row.unclaimed {
			count++
		}
	}
	return count, err
}
