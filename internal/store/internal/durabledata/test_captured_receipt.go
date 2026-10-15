package durabledata

import (
	"context"
	"database/sql"
	"errors"
)

// CapturedResourceReceipt has no public transaction methods or raw carriers.
// Only the domain owner can interpret its private captured payload.
type CapturedResourceReceipt struct {
	corruption *ReceiptCorruption
	atomic     *receiptAtomicCorruption
	removal    *ImportProvenanceRemoval
	provenance *ProvenanceReadArmAddition
	head       *NonMutatingHeadHistoryAddition
}

func (f *ReceiptCorruption) CapturedReceipt() *CapturedResourceReceipt {
	return &CapturedResourceReceipt{corruption: f}
}

func (f *receiptAtomicCorruption) CapturedReceipt() *CapturedResourceReceipt {
	return &CapturedResourceReceipt{atomic: f}
}

func (f *ImportProvenanceRemoval) CapturedReceipt() *CapturedResourceReceipt {
	return &CapturedResourceReceipt{removal: f}
}

func (f *ProvenanceReadArmAddition) CapturedReceipt() *CapturedResourceReceipt {
	return &CapturedResourceReceipt{provenance: f}
}

func (f *NonMutatingHeadHistoryAddition) CapturedReceipt() *CapturedResourceReceipt {
	return &CapturedResourceReceipt{head: f}
}

func (f *CapturedResourceReceipt) hasExactlyOnePayload() bool {
	if f == nil {
		return false
	}
	count := 0
	for _, present := range [...]bool{f.corruption != nil, f.atomic != nil, f.removal != nil, f.provenance != nil, f.head != nil} {
		if present {
			count++
		}
	}
	return count == 1
}

func InstallCapturedResourceReceiptTx(ctx context.Context, tx *sql.Tx, f *CapturedResourceReceipt, postgres bool) error {
	if !f.hasExactlyOnePayload() || tx == nil {
		return errors.New("resource receipt installation requires its exact capture and selected transaction")
	}
	switch {
	case f.corruption != nil:
		return f.corruption.installTx(ctx, tx, postgres)
	case f.atomic != nil:
		return f.atomic.installTx(ctx, tx, postgres)
	case f.removal != nil:
		return f.removal.installTx(ctx, tx, postgres)
	case f.provenance != nil:
		return f.provenance.installTx(ctx, tx, postgres)
	default:
		return f.head.installTx(ctx, tx, postgres)
	}
}

func RestoreCapturedResourceReceiptTx(ctx context.Context, tx *sql.Tx, f *CapturedResourceReceipt, postgres bool) error {
	if !f.hasExactlyOnePayload() || tx == nil {
		return errors.New("resource receipt restoration requires its exact capture and selected transaction")
	}
	switch {
	case f.corruption != nil:
		return f.corruption.restoreTx(ctx, tx, postgres)
	case f.atomic != nil:
		return f.atomic.restoreTx(ctx, tx, postgres)
	case f.removal != nil:
		return f.removal.restoreTx(ctx, tx, postgres)
	case f.provenance != nil:
		return f.provenance.restoreTx(ctx, tx, postgres)
	default:
		return f.head.restoreTx(ctx, tx, postgres)
	}
}
