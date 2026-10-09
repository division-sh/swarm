package sessionpersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
)

type ClaimReceiptReader interface {
	LoadOperatorChannelClaimReceipt(context.Context, string) (operatorchannel.ClaimReceipt, bool, error)
}

// A committed claim is historical evidence, not a reason to re-admit old input
// under a later activation. Uncommitted captures remain with their original owner.
func (s *CaptureStore) ReconcileSessionClaim(ctx context.Context, event capturedEvent, reader ClaimReceiptReader) (bool, error) {
	if reader == nil || event.Scope.Kind != channelonboarding.SessionInputOnboarding {
		return false, fmt.Errorf("onboarding claim history requires its original scope and receipt owner")
	}
	providerID, err := event.PublicationProviderEventID()
	if err != nil {
		return false, err
	}
	fingerprint, err := event.PublicationFingerprint()
	if err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	rows, err := s.readPendingRows(ctx, tx)
	if err != nil {
		return false, err
	}
	for _, row := range rows {
		if !row.event.SameCapture(event) {
			continue
		}
		id := operatorchannel.SessionClaimReceiptID(event.Scope.OnboardingOperation, providerID)
		receipt, found, err := reader.LoadOperatorChannelClaimReceipt(ctx, id)
		if err != nil || !found {
			return false, err
		}
		if receipt.Provider != event.Scope.Session.Provider || receipt.ProviderEventID != providerID || receipt.NativeCaptureFingerprint != fingerprint {
			return false, operatorchannel.ErrConflict
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
