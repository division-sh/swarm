package operatorchannel

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	domain "github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
)

func (s *PostgresOwner) SettleSessionChannelClaim(ctx context.Context, fact authority.Claim) (domain.ClaimSettlement, error) {
	return settleSessionClaim(ctx, postgresRunner{s}, fact)
}

func (s *SQLiteOwner) SettleSessionChannelClaim(ctx context.Context, fact authority.Claim) (domain.ClaimSettlement, error) {
	return settleSessionClaim(ctx, sqliteRunner{s}, fact)
}

func settleSessionClaim(ctx context.Context, runner transactionRunner, fact authority.Claim) (domain.ClaimSettlement, error) {
	var absent domain.ClaimSettlement
	if err := runner.require(); err != nil {
		return absent, err
	}
	if err := fact.Validate(ctx); err != nil {
		return absent, err
	}
	var claim domain.InboundClaim
	if err := json.Unmarshal(fact.Body(), &claim); err != nil {
		return absent, err
	}
	if err := claim.Validate(); err != nil {
		return absent, err
	}
	parentID, parentRevision := fact.Parent()
	var settlement domain.ClaimSettlement
	err := runner.mutate(ctx, "settle native operator channel claim", func(txctx context.Context, tx *sql.Tx) error {
		if err := fact.Validate(txctx); err != nil {
			return err
		}
		if receipt, found, err := loadClaimReceipt(txctx, tx, runner.dialect(), claim.PublicationID); err != nil {
			return err
		} else if found {
			if !receipt.Matches(claim) || receipt.NativeCaptureFingerprint != fact.Fingerprint() {
				return domain.ErrConflict
			}
			settlement = domain.ClaimSettlement{Consumed: true, Disposition: receipt.Disposition}
			return nil
		}
		if err := requireSessionClaimParent(txctx, tx, runner.dialect(), claim, parentID, parentRevision); err != nil {
			return err
		}
		var err error
		settlement, err = settleInboundClaimTx(txctx, tx, runner.dialect(), claim, fact.ReceivedAt())
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(txctx, runner.dialect().bind(`UPDATE operator_channel_claim_receipts SET native_capture_fingerprint=?
			WHERE publication_id=? AND native_capture_fingerprint IS NULL`), fact.Fingerprint(), claim.PublicationID)
		return err
	})
	return settlement, err
}

func requireSessionClaimParent(ctx context.Context, tx *sql.Tx, d dialect, claim domain.InboundClaim, parentID string, parentRevision int64) error {
	op, found, err := loadOperationByChallenge(ctx, tx, d, claim.Challenge, true)
	if err != nil {
		return err
	}
	query := `SELECT operation_revision,phase FROM channel_onboarding_operations WHERE operation_id=?`
	if d == dialectPostgres {
		query += ` FOR UPDATE`
	}
	var revision int64
	var phase string
	if err := tx.QueryRowContext(ctx, d.bind(query), parentID).Scan(&revision, &phase); err != nil {
		return err
	}
	if revision != parentRevision || phase != "awaiting_external_identity" && phase != "awaiting_operator_confirmation" {
		return domain.ErrRevisionConflict
	}
	if !found {
		return nil
	}
	if op.OnboardingOperationID != parentID {
		return domain.ErrConflict
	}
	return requireActiveOnboardingParent(ctx, tx, d, op)
}

func (s *PostgresOwner) LoadOperatorChannelClaimReceipt(ctx context.Context, id string) (domain.ClaimReceipt, bool, error) {
	if err := s.requireCurrent(); err != nil {
		return domain.ClaimReceipt{}, false, err
	}
	return loadClaimReceipt(ctx, s.backend, dialectPostgres, id)
}

func (s *SQLiteOwner) LoadOperatorChannelClaimReceipt(ctx context.Context, id string) (domain.ClaimReceipt, bool, error) {
	if err := s.requireCurrent(); err != nil {
		return domain.ClaimReceipt{}, false, err
	}
	return loadClaimReceipt(ctx, s.backend, dialectSQLite, id)
}

func loadClaimReceipt(ctx context.Context, db queryer, d dialect, id string) (domain.ClaimReceipt, bool, error) {
	var receipt domain.ClaimReceipt
	var operation sql.NullString
	var fingerprint sql.NullString
	var recorded any
	err := db.QueryRowContext(ctx, d.bind(`SELECT publication_id,provider,provider_event_id,interface_key,challenge,operation_id,disposition,reason,provider_authorization,native_capture_fingerprint,recorded_at
		FROM operator_channel_claim_receipts WHERE publication_id=?`), id).Scan(&receipt.PublicationID, &receipt.Provider, &receipt.ProviderEventID,
		&receipt.InterfaceKey, &receipt.Challenge, &operation, &receipt.Disposition, &receipt.Reason, &receipt.ProviderAuthorization, &fingerprint, &recorded)
	if errors.Is(err, sql.ErrNoRows) {
		return receipt, false, nil
	}
	if err != nil {
		return receipt, false, err
	}
	receipt.RecordedAt, err = timeValue(recorded)
	if err != nil {
		return receipt, false, err
	}
	if receipt.PublicationID != id || receipt.ProviderAuthorization == "" || receipt.RecordedAt.IsZero() {
		return domain.ClaimReceipt{}, false, fmt.Errorf("operator claim receipt has incomplete original evidence")
	}
	receipt.OperationID = operation.String
	receipt.NativeCaptureFingerprint = fingerprint.String
	return receipt, true, nil
}

func requireSessionParent(ctx context.Context, tx *sql.Tx, d dialect, parentID string, admitted domain.ProviderAuthority) error {
	if err := admitted.RequireExecutable(); err != nil {
		return err
	}
	originalID, originalRevision := admitted.SessionParent()
	if originalID != parentID || parentID == "" {
		return domain.ErrRevisionConflict
	}
	query := `SELECT operation_revision,phase FROM channel_onboarding_operations WHERE operation_id=?`
	if d == dialectPostgres {
		query += ` FOR UPDATE`
	}
	var revision int64
	var phase string
	if err := tx.QueryRowContext(ctx, d.bind(query), parentID).Scan(&revision, &phase); err != nil {
		return err
	}
	if revision != originalRevision || phase == "succeeded" || phase == "failed" || phase == "retired" {
		return domain.ErrRevisionConflict
	}
	return admitted.RequireExecutable()
}
