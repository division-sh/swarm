package durabledata

import (
	"context"
	"database/sql"
	"errors"
)

// CapturedResourceReceipt has no public transaction methods or raw carriers.
// Only the domain owner can interpret its private mutation/cleanup operations.
type CapturedResourceReceipt struct{ row capturedResourceReceiptTx }

type capturedResourceReceiptTx interface {
	installTx(context.Context, *sql.Tx, bool) error
	restoreTx(context.Context, *sql.Tx, bool) error
}

func (f *ReceiptCorruption) CapturedReceipt() *CapturedResourceReceipt {
	return &CapturedResourceReceipt{row: f}
}

func (f *receiptAtomicCorruption) CapturedReceipt() *CapturedResourceReceipt {
	return &CapturedResourceReceipt{row: f}
}

func (f *ImportProvenanceRemoval) CapturedReceipt() *CapturedResourceReceipt {
	return &CapturedResourceReceipt{row: f}
}

func (f *ProvenanceReadArmAddition) CapturedReceipt() *CapturedResourceReceipt {
	return &CapturedResourceReceipt{row: f}
}

func (f *NonMutatingHeadHistoryAddition) CapturedReceipt() *CapturedResourceReceipt {
	return &CapturedResourceReceipt{row: f}
}

func InstallCapturedResourceReceiptTx(ctx context.Context, tx *sql.Tx, f *CapturedResourceReceipt, postgres bool) error {
	if f == nil || f.row == nil || tx == nil {
		return errors.New("resource receipt installation requires its exact capture and selected transaction")
	}
	return f.row.installTx(ctx, tx, postgres)
}

func RestoreCapturedResourceReceiptTx(ctx context.Context, tx *sql.Tx, f *CapturedResourceReceipt, postgres bool) error {
	if f == nil || f.row == nil || tx == nil {
		return errors.New("resource receipt restoration requires its exact capture and selected transaction")
	}
	return f.row.restoreTx(ctx, tx, postgres)
}
